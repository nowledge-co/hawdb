// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;
use hawdb_executor::{QueryMemoryClass, QueryMemoryLedger};

struct OwnedRoot(PathBuf);

impl OwnedRoot {
    fn new(name: &str) -> Self {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        Self(test_dir(&format!("text-seed-{name}-{nonce}")))
    }
}

impl Drop for OwnedRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn expected_scores(
    reader: &SearchOutOfCoreReader,
    documents: &[SearchDocument],
    text: &str,
    limit: usize,
    excluded: Option<&str>,
) -> (BTreeMap<String, f64>, usize) {
    let terms = reader
        .primary_segment()
        .lexical_projection
        .tokenize_query(
            text,
            &reader.analyzer_lexicon,
            reader.lexical_term_policy.max_term_bytes(),
        )
        .unwrap();
    let corpus = crate::TextCorpusStats::from_documents(documents.iter(), &reader.analyzer_lexicon);
    let mut scores = documents
        .iter()
        .filter(|document| excluded != Some(document.id.as_str()))
        .map(|document| {
            (
                document.id.clone(),
                crate::bm25_score(&terms, document, &corpus, &reader.analyzer_lexicon),
            )
        })
        .filter(|(_, score)| *score > 0.0)
        .collect::<Vec<_>>();
    let matches = scores.len();
    scores.sort_by(|(left_id, left_score), (right_id, right_score)| {
        right_score
            .total_cmp(left_score)
            .then_with(|| left_id.cmp(right_id))
    });
    scores.truncate(limit);
    (scores.into_iter().collect(), matches)
}

fn assert_producer_parity(reader: &SearchOutOfCoreReader, documents: &[SearchDocument]) {
    let budget = NonZeroUsize::new(64 * 1024 * 1024).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let account = ledger.account(QueryMemoryClass::ExternalRead, "text seed", budget);
    let task = crate::RuntimeTaskContext::default();
    for text in ["GRAPH storage", "中国 Graph", "missingterm"] {
        for limit in [1, 2, 10] {
            for excluded in [None, Some(documents[0].id.as_str())] {
                let (expected, matches) = expected_scores(reader, documents, text, limit, excluded);
                let output = reader
                    .text_seed_scores_with_context(text, limit, &account, &task, |id| {
                        Ok(excluded != Some(id))
                    })
                    .unwrap();
                let actual_bits = output
                    .scores()
                    .iter()
                    .map(|(id, score)| (id.clone(), score.to_bits()))
                    .collect::<BTreeMap<_, _>>();
                let expected_bits = expected
                    .iter()
                    .map(|(id, score)| (id.clone(), score.to_bits()))
                    .collect::<BTreeMap<_, _>>();
                assert_eq!(
                    actual_bits, expected_bits,
                    "text={text} limit={limit} excluded={excluded:?}"
                );
                assert_eq!(output.report.matching_document_count, matches);
                assert_eq!(output.report.returned_count, expected.len());
                assert_eq!(output.report.corpus_document_count, documents.len());
                assert_eq!(output.report.generation, reader.manifest.generation);
                assert_eq!(
                    output.report.source_graph_commit_epoch,
                    reader.manifest.source_graph_commit_epoch
                );
                let retained = ledger.snapshot().used_bytes;
                assert!(
                    retained > 0,
                    "returned score owner must retain its admission"
                );
                assert!(account.reserve(budget.get() - retained + 1).is_err());
                drop(output);
                assert_eq!(ledger.snapshot().used_bytes, 0);
            }
        }
    }
}

#[test]
fn filtered_out_of_core_text_seeds_charge_metadata_and_keep_external_ids() {
    let _serial = crate::test_allocation::serial();
    let root = OwnedRoot::new("filtered-accounted");
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root.0, Default::default()).unwrap();
    let mut other = document(0, "other");
    other.title = "graph graph graph graph graph graph".into();
    other
        .metadata
        .insert("labels".into(), r#"["planning"]"#.into());
    let mut team = document(1, "team");
    team.title = "graph".into();
    team.metadata
        .insert("labels".into(), r#"["database"]"#.into());
    team.metadata
        .insert("padding".into(), "x".repeat(1024 * 1024));
    writer.push(other).unwrap();
    writer.push(team).unwrap();
    writer.finish().unwrap();
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    let budget = NonZeroUsize::new(64 * 1024 * 1024).unwrap();
    let task = crate::RuntimeTaskContext::default();
    for filters in [
        BTreeMap::new(),
        BTreeMap::from([("space_id".into(), "team".into())]),
        BTreeMap::from([("labels__in".into(), r#"["database"]"#.into())]),
    ] {
        let expected = reader
            .search_with_options(
                "graph",
                None,
                SearchMode::Text,
                SearchQueryOptions {
                    limit: 1,
                    offset: 0,
                    rank_window: None,
                    fusion_weights: SearchFusionWeights::default(),
                    metadata_filters: filters.clone(),
                    policy_epoch: None,
                },
            )
            .unwrap();
        let ledger = QueryMemoryLedger::new(budget);
        let account = ledger.account(QueryMemoryClass::ExternalRead, "filtered text", budget);
        let baseline = crate::test_allocation::live();
        let (actual, peak) = crate::test_allocation::measure(|| {
            reader
                .text_seed_scores_with_metadata_filters("graph", &filters, 1, &account, &task)
                .unwrap()
        });
        let actual_bits = actual
            .scores()
            .iter()
            .map(|(id, score)| (id.clone(), score.to_bits()))
            .collect::<BTreeMap<_, _>>();
        let expected_bits = expected
            .result
            .hits
            .iter()
            .map(|hit| (hit.id.clone(), hit.text_score.to_bits()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(actual_bits, expected_bits, "filters={filters:?}");
        for hit in &expected.result.hits {
            assert_eq!(actual.external_id(&hit.id), hit.external_id.as_deref());
        }
        let allocated = peak.checked_sub(baseline).unwrap();
        assert!(
            allocated <= ledger.snapshot().peak_bytes,
            "filtered text peak exceeded its account: allocated {allocated}, charged {}",
            ledger.snapshot().peak_bytes
        );
        drop(actual);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
    let tiny = NonZeroUsize::new(256).unwrap();
    let ledger = QueryMemoryLedger::new(tiny);
    let account = ledger.account(QueryMemoryClass::ExternalRead, "tiny filtered text", tiny);
    assert!(reader
        .text_seed_scores_with_metadata_filters(
            "graph",
            &BTreeMap::from([("space_id".into(), "team".into())]),
            1,
            &account,
            &task,
        )
        .is_err());
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn out_of_core_text_seed_scores_use_the_complete_live_corpus_and_bounded_window() {
    let mut a = document(0, "team");
    a.id = "a".into();
    a.title = "Graph".into();
    a.content = "storage memory archive foo bar".into();
    let mut b = a.clone();
    b.id = "b".into();
    let mut late = a.clone();
    late.id = "z".into();
    late.title = "中国 Graph Graph Graph Graph".into();
    late.content = "storage Graph Graph Graph".into();
    let documents = vec![a.clone(), b.clone(), late.clone()];
    let root = OwnedRoot::new("two-artifacts");
    publish_two_artifact_manifest_with_initial_documents(
        &root.0,
        vec![a, b],
        Default::default(),
        late,
        Default::default(),
    );
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    assert_eq!(reader.segments.len(), 2);
    let (winner, count) = expected_scores(&reader, &documents, "GRAPH storage", 1, None);
    assert_eq!(count, 3);
    assert_eq!(
        winner.keys().next().unwrap(),
        "z",
        "later artifact must supply the winner"
    );
    assert_producer_parity(&reader, &documents);

    let merged_root = OwnedRoot::new("single-artifact");
    let mut writer =
        SearchOutOfCoreGenerationWriter::create(&merged_root.0, Default::default()).unwrap();
    for document in documents.iter().cloned() {
        writer.push(document).unwrap();
    }
    writer.finish().unwrap();
    let merged = SearchOutOfCoreReader::open(&merged_root.0).unwrap();
    assert_eq!(merged.segments.len(), 1);
    assert_producer_parity(&merged, &documents);

    for replace in [false, true] {
        let root = OwnedRoot::new(&format!("visibility-{replace}"));
        let old = document(0, "old-space");
        let mut live = if replace {
            old.clone()
        } else {
            document(1, "live-space")
        };
        live.title = "中国 Graph replacement".into();
        live.content = "storage Graph current version".into();
        publish_two_artifact_manifest(&root.0, old.clone(), live.clone());
        install_edited_mutation_run(&root.0, &old, 0, 3, |entry| {
            if replace {
                entry.operation = mutation_run::SearchMutationOperation::Replace;
            }
        });
        let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
        assert!(!reader.visibility.is_empty());
        assert_producer_parity(&reader, &[live]);
    }
}

#[test]
fn out_of_core_text_seed_retains_pinned_generation_across_replacement_and_delete() {
    let root = OwnedRoot::new("pinned-replacement-delete");
    let row = |id: &str, body: &str| crate::SearchProjectionRow {
        kind: crate::SearchProjectionKind::Memory,
        external_id: id.into(),
        title: id.into(),
        body: body.into(),
        embedding: None,
        source_id: None,
        metadata: BTreeMap::new(),
    };
    let old = row("a", "legacytoken");
    let deleted = row("b", "deletedtoken");
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root.0, Default::default()).unwrap();
    writer.push(old.clone().into_document()).unwrap();
    writer.push(deleted.clone().into_document()).unwrap();
    writer.finish().unwrap();

    let pinned = SearchOutOfCoreReader::open(&root.0).unwrap();
    let budget = NonZeroUsize::new(64 * 1024 * 1024).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let account = ledger.account(QueryMemoryClass::ExternalRead, "pinned text seed", budget);
    let task = crate::RuntimeTaskContext::default();
    let scores = |reader: &SearchOutOfCoreReader, text: &str| {
        reader
            .text_seed_scores_with_context(text, 2, &account, &task, |_| Ok(true))
            .unwrap()
            .scores()
            .keys()
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(scores(&pinned, "legacytoken"), vec!["memory:a"]);
    assert_eq!(scores(&pinned, "deletedtoken"), vec!["memory:b"]);

    let replacement = row("a", "freshterm");
    SearchOutOfCoreGenerationWriter::prepare_delta(
        &pinned,
        crate::SearchProjectionDelta {
            upserts: vec![replacement],
            deletes: vec!["memory:b".into()],
            ..Default::default()
        },
        Default::default(),
    )
    .unwrap()
    .finish()
    .unwrap();
    let current = SearchOutOfCoreReader::open(&root.0).unwrap();
    assert_ne!(pinned.generation(), current.generation());
    assert_eq!(scores(&pinned, "legacytoken"), vec!["memory:a"]);
    assert_eq!(scores(&pinned, "deletedtoken"), vec!["memory:b"]);
    assert!(scores(&pinned, "freshterm").is_empty());
    assert!(scores(&current, "legacytoken").is_empty());
    assert!(scores(&current, "deletedtoken").is_empty());
    assert_eq!(scores(&current, "freshterm"), vec!["memory:a"]);
    let filtered_scores = |reader: &SearchOutOfCoreReader, text: &str| {
        reader
            .text_seed_scores_with_metadata_filters(text, &BTreeMap::new(), 2, &account, &task)
            .unwrap()
    };
    let pinned_old = filtered_scores(&pinned, "legacytoken");
    assert_eq!(pinned_old.external_id("memory:a"), Some("a"));
    assert_eq!(
        pinned_old.scores().keys().cloned().collect::<Vec<_>>(),
        vec!["memory:a"]
    );
    drop(pinned_old);
    assert!(filtered_scores(&pinned, "freshterm").scores().is_empty());
    assert_eq!(
        filtered_scores(&current, "freshterm")
            .scores()
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        vec!["memory:a"]
    );
    assert!(filtered_scores(&current, "deletedtoken")
        .scores()
        .is_empty());
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn active_out_of_core_text_seed_keeps_its_generation_during_publication() {
    let root = OwnedRoot::new("active-publication");
    let row = |id: &str, body: &str, space: &str| crate::SearchProjectionRow {
        kind: crate::SearchProjectionKind::Memory,
        external_id: id.into(),
        title: id.into(),
        body: body.into(),
        embedding: None,
        source_id: None,
        metadata: BTreeMap::from([("space_id".into(), space.into())]),
    };
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root.0, Default::default()).unwrap();
    writer
        .push(row("a", "sharedterm", "old").into_document())
        .unwrap();
    writer
        .push(row("b", "sharedterm", "old").into_document())
        .unwrap();
    writer.finish().unwrap();

    let pinned = SearchOutOfCoreReader::open(&root.0).unwrap();
    let budget = NonZeroUsize::new(64 * 1024 * 1024).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let account = ledger.account(QueryMemoryClass::ExternalRead, "active text seed", budget);
    let task = crate::RuntimeTaskContext::default();
    let filters = BTreeMap::from([("space_id".into(), "old".into())]);
    let (candidate_tx, candidate_rx) = std::sync::mpsc::channel();
    let (published_tx, published_rx) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        let query_reader = &pinned;
        let query_account = &account;
        let query_task = &task;
        let query_filters = &filters;
        let query = scope.spawn(move || {
            let mut paused = false;
            let output = query_reader.text_seed_scores_with_metadata_filters_and_observer(
                "sharedterm",
                query_filters,
                2,
                query_account,
                query_task,
                |_| {
                    if !paused {
                        paused = true;
                        candidate_tx.send(()).unwrap();
                        published_rx
                            .recv_timeout(std::time::Duration::from_secs(30))
                            .unwrap();
                    }
                    Ok(())
                },
            )?;
            let ids = output.scores().keys().cloned().collect::<Vec<_>>();
            assert_eq!(output.external_id("memory:a"), Some("a"));
            assert_eq!(output.external_id("memory:b"), Some("b"));
            Ok::<_, crate::HawDBError>(ids)
        });
        candidate_rx
            .recv_timeout(std::time::Duration::from_secs(30))
            .unwrap();
        SearchOutOfCoreGenerationWriter::prepare_delta(
            &pinned,
            crate::SearchProjectionDelta {
                upserts: vec![row("a", "freshterm", "new")],
                deletes: vec!["memory:b".into()],
                ..Default::default()
            },
            Default::default(),
        )
        .unwrap()
        .finish()
        .unwrap();
        let current = SearchOutOfCoreReader::open(&root.0).unwrap();
        assert_ne!(pinned.generation(), current.generation());
        published_tx.send(()).unwrap();
        assert_eq!(query.join().unwrap().unwrap(), vec!["memory:a", "memory:b"]);
        assert!(current
            .text_seed_scores_with_metadata_filters("sharedterm", &filters, 2, &account, &task)
            .unwrap()
            .scores()
            .is_empty());
        assert_eq!(
            current
                .text_seed_scores_with_metadata_filters(
                    "freshterm",
                    &BTreeMap::from([("space_id".into(), "new".into())]),
                    2,
                    &account,
                    &task,
                )
                .unwrap()
                .scores()
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            vec!["memory:a"]
        );
    });
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn out_of_core_text_seed_corpus_merge_peak_stays_within_the_shared_root() {
    use crate::lexical_projection::{
        AccountedCorpusStatistics, LexicalCorpusStatistics, QueryContext,
    };

    let _serial = crate::test_allocation::serial();
    let root = OwnedRoot::new("corpus-merge-peak");
    publish_two_artifact_manifest_with_initial_documents(
        &root.0,
        vec![document(0, "team")],
        Default::default(),
        document(1, "team"),
        Default::default(),
    );
    let mut reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    let policy = crate::SearchLexicalTermPolicy::new(NonZeroU64::new(16 * 1024).unwrap()).unwrap();
    reader.set_lexical_term_policy(policy).unwrap();
    let text = "z".repeat(16 * 1024);
    let terms = reader
        .primary_segment()
        .lexical_projection
        .tokenize_query(&text, &reader.analyzer_lexicon, policy.max_term_bytes())
        .unwrap();
    assert_eq!(terms.len(), 1);
    assert_eq!(terms.first().unwrap(), &text);
    assert_eq!(reader.segments.len(), 2);
    let projections = || {
        reader
            .segments
            .iter()
            .map(|segment| &segment.lexical_projection)
    };
    let expected =
        LexicalCorpusStatistics::aggregate(projections(), &terms, policy.max_term_bytes()).unwrap();
    let budget = NonZeroUsize::new(36 * 1024).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let account = ledger.account(QueryMemoryClass::ExternalRead, "text corpus", budget);
    drop(account.reserve(1).unwrap());
    let task = crate::RuntimeTaskContext::default();
    // Readers, borrowed query terms, reference statistics and account registration
    // are outside this measurement. Only actual aggregate-owned allocations count.
    for _ in 0..2 {
        let baseline = crate::test_allocation::live();
        let (output, peak) = crate::test_allocation::measure(|| {
            AccountedCorpusStatistics::aggregate(
                projections(),
                &terms,
                policy.max_term_bytes(),
                QueryContext {
                    execution: None,
                    memory: &account,
                    task: &task,
                },
            )
            .unwrap()
        });
        let allocated = peak.checked_sub(baseline).unwrap();
        let charged = ledger.snapshot().peak_bytes;
        assert!(
            allocated <= charged && allocated <= budget.get(),
            "corpus merge peak must fit its caller root: allocated {allocated}, charged {charged}, budget {}",
            budget.get()
        );
        assert_eq!(output.statistics, expected);
        assert_eq!(output.statistics.document_count(), 2);
        assert_eq!(output.statistics.bytes_read(), 0);
        let retained = ledger.snapshot().used_bytes;
        assert!(retained >= crate::test_allocation::live() - baseline);
        assert!(account.reserve(budget.get() - retained + 1).is_err());
        drop(output);
        assert_eq!(crate::test_allocation::live(), baseline);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn out_of_core_text_seed_shared_budget_scope_failure_and_cancellation_release_and_recover() {
    let root = OwnedRoot::new("scope-and-root");
    let documents = vec![document(0, "team"), document(1, "team")];
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root.0, Default::default()).unwrap();
    for document in documents.iter().cloned() {
        writer.push(document).unwrap();
    }
    writer.finish().unwrap();
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    let budget = NonZeroUsize::new(2 * 1024 * 1024).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let account = ledger.account(QueryMemoryClass::ExternalRead, "text seed", budget);
    let competitor = ledger.account(QueryMemoryClass::PipelineBatch, "other operator", budget);
    let held = competitor.reserve(budget.get() - 64).unwrap();
    let mut calls = 0;
    let task = crate::RuntimeTaskContext::default();
    let error = reader
        .text_seed_scores_with_context("graph", 2, &account, &task, |_| {
            calls += 1;
            Ok(true)
        })
        .unwrap_err();
    assert!(error.to_string().contains("query_memory_bytes"), "{error}");
    assert_eq!(calls, 0, "failed query admission must precede scope calls");
    drop(error);
    assert_eq!(ledger.snapshot().used_bytes, held.bytes());
    drop(held);
    let expected = expected_scores(&reader, &documents, "graph", 2, None).0;
    let output = reader
        .text_seed_scores_with_context("graph", 2, &account, &task, |_| Ok(true))
        .unwrap();
    assert_eq!(output.scores(), &expected);
    drop(output);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    for mode in 0..3 {
        let task = crate::RuntimeTaskContext::default();
        if mode == 0 {
            task.cancellation().cancel();
        }
        let mut calls = 0;
        let error = reader
            .text_seed_scores_with_context("graph", 2, &account, &task, |_| {
                calls += 1;
                if mode == 2 {
                    Err(HawDBError::Execution("text scope refused".into()))
                } else {
                    task.cancellation().cancel();
                    Ok(true)
                }
            })
            .unwrap_err();
        assert_eq!(calls, usize::from(mode != 0));
        assert!(
            error.to_string().contains(if mode == 2 {
                "text scope refused"
            } else {
                "lexical query cancelled"
            }),
            "{error}"
        );
        drop(error);
        assert_eq!(ledger.snapshot().used_bytes, 0);
        let healthy = crate::RuntimeTaskContext::default();
        let output = reader
            .text_seed_scores_with_context("graph", 2, &account, &healthy, |_| Ok(true))
            .unwrap();
        assert_eq!(output.scores(), &expected);
        drop(output);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
    let output = reader
        .text_seed_scores_with_context("中国", 0, &account, &task, |_| {
            panic!("zero seed window must not read the source")
        })
        .unwrap();
    assert!(output.scores().is_empty());
    assert_eq!(output.report.bytes_read, 0);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}
