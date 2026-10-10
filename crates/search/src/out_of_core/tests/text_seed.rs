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
            .map(|segment| segment.lexical_projection.as_ref())
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
