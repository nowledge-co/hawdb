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
use hawdb_core::RuntimeCancellationToken;
use hawdb_executor::{QueryMemoryClass, QueryMemoryLedger};
use std::cell::{Cell, RefCell};

thread_local! {
    static ALLOCATION_COUNT: Cell<Option<usize>> = const { Cell::new(None) };
    static CANCELLATION: RefCell<Option<(usize, RuntimeCancellationToken)>> = const { RefCell::new(None) };
}

fn allocation_observer(bytes: usize) {
    crate::test_allocation::record_allocation(bytes);
    let _ = ALLOCATION_COUNT.try_with(|count| {
        if let Some(value) = count.get() {
            count.set(Some(value.saturating_add(1)));
        }
    });
    // Disarm and release the TLS borrow before cancel. The token is fresh and
    // has no waiters, so cancellation cannot allocate, invoke a waker or panic.
    let token = CANCELLATION
        .try_with(|slot| {
            let mut slot = slot.try_borrow_mut().ok()?;
            let (remaining, _) = slot.as_mut()?;
            *remaining -= 1;
            if *remaining == 0 {
                slot.take().map(|(_, token)| token)
            } else {
                None
            }
        })
        .ok()
        .flatten();
    if let Some(token) = token {
        token.cancel();
    }
}

fn allocation_count() -> usize {
    ALLOCATION_COUNT.with(|count| count.get().unwrap_or_default())
}

fn count_allocations<T>(work: impl FnOnce() -> T) -> (T, usize) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            ALLOCATION_COUNT.with(|count| count.set(None));
        }
    }
    ALLOCATION_COUNT.with(|count| assert!(count.replace(Some(0)).is_none()));
    let reset = Reset;
    // SAFETY: the observer forwards every allocation exactly once and only
    // updates thread-local counters; this healthy measurement has no token.
    let (value, _) =
        unsafe { crate::test_allocation::measure_with_observer(allocation_observer, work) };
    let count = allocation_count();
    drop(reset);
    (value, count)
}

struct AllocationCancellation(std::marker::PhantomData<std::rc::Rc<()>>);

impl Drop for AllocationCancellation {
    fn drop(&mut self) {
        CANCELLATION.with(|slot| *slot.borrow_mut() = None);
    }
}

fn cancel_on_allocation(
    token: RuntimeCancellationToken,
    remaining: usize,
) -> AllocationCancellation {
    assert!(remaining > 0);
    CANCELLATION.with(|slot| {
        assert!(slot.borrow().is_none());
        *slot.borrow_mut() = Some((remaining, token));
    });
    AllocationCancellation(std::marker::PhantomData)
}

struct OwnedDirectory(PathBuf);

impl OwnedDirectory {
    fn new(name: &str) -> Self {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = projection_root(&format!("{name}-{nonce}"));
        fs::create_dir_all(&root).unwrap();
        Self(root)
    }
}

impl Drop for OwnedDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn documents() -> BTreeMap<String, SearchDocument> {
    BTreeMap::from([
        ("a".into(), document("a", "Graph Graph", "storage")),
        ("b".into(), document("b", "Graph", "memory retrieval")),
        (
            "c".into(),
            document("c", "Graph Graph Graph Graph", "storage storage"),
        ),
        ("d".into(), document("d", "Vector", "embedding")),
    ])
}

fn reader(
    root: &OwnedDirectory,
    documents: &BTreeMap<String, SearchDocument>,
) -> Arc<LexicalProjectionReader> {
    LexicalProjectionWriter::new(LexicalProjectionConfig {
        target_block_bytes: NonZeroU64::new(128).unwrap(),
        max_block_bytes: NonZeroU64::new(1024).unwrap(),
        ..Default::default()
    })
    .write(
        &root.0,
        1,
        None,
        11,
        13,
        documents.values(),
        &Default::default(),
    )
    .unwrap()
}

fn inputs<'a>(
    reader: &LexicalProjectionReader,
    delta: &'a LexicalMiniDelta,
    context: QueryContext<'a>,
    retained_score_limit: Option<usize>,
) -> ScoringInputs<'a> {
    ScoringInputs {
        delta,
        max_term_bytes: reader.config.max_term_bytes,
        retained_score_limit,
        global_statistics: None,
        prune_blocks: false,
        context: Some(context),
    }
}

#[test]
fn lexical_query_context_preserves_real_bm25_and_retains_result_charge() {
    let root = OwnedDirectory::new("context-bm25");
    let documents = documents();
    let reader = reader(&root, &documents);
    let terms = BTreeSet::from(["graph".into(), "storage".into()]);
    let delta = LexicalMiniDelta::default();
    let task = RuntimeTaskContext::default();
    let budget = NonZeroUsize::new(1024 * 1024).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let account = ledger.account(QueryMemoryClass::ExternalRead, "text producer", budget);
    let context = QueryContext {
        memory: &account,
        task: &task,
    };
    let corpus = crate::TextCorpusStats::from_documents(documents.values(), &Default::default());
    for retained in [None, Some(1), Some(2)] {
        let expected = reader
            .score(&terms, &delta, retained, |_| Ok(true))
            .unwrap();
        let actual = reader
            .score_accounted(&terms, inputs(&reader, &delta, context, retained), |_| {
                Ok(true)
            })
            .unwrap();
        assert_eq!(actual.report, expected);
        if retained.is_none() {
            for document in documents.values() {
                let expected = crate::bm25_score(&terms, document, &corpus, &Default::default());
                assert_eq!(
                    actual
                        .report
                        .scores
                        .get(&document.id)
                        .copied()
                        .unwrap_or(0.0),
                    expected
                );
            }
        }
        assert!(
            ledger.snapshot().used_bytes > 0,
            "returned raw scores must retain their charge"
        );
        let used = ledger.snapshot().used_bytes;
        assert!(
            account.reserve(budget.get() - used + 1).is_err(),
            "consumer lifetime participates in the same root budget"
        );
        drop(actual);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn lexical_query_context_refuses_competing_budget_before_candidate_callbacks_and_recovers() {
    let root = OwnedDirectory::new("context-competing-budget");
    let documents = documents();
    let reader = reader(&root, &documents);
    let terms = BTreeSet::from(["graph".into(), "storage".into()]);
    let delta = LexicalMiniDelta::default();
    let task = RuntimeTaskContext::default();
    let budget = NonZeroUsize::new(1024 * 1024).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let account = ledger.account(QueryMemoryClass::ExternalRead, "text producer", budget);
    let competitor = ledger.account(QueryMemoryClass::BlockingState, "other operator", budget);
    let context = QueryContext {
        memory: &account,
        task: &task,
    };
    let actual = reader
        .score_accounted(&terms, inputs(&reader, &delta, context, Some(2)), |_| {
            Ok(true)
        })
        .unwrap();
    let retained_bytes = ledger.snapshot().used_bytes;
    let expected = actual.report.clone();
    drop(actual);
    // Leave enough room for the actual complete result, but not for its
    // simultaneously live decoding/statistics workspace. A result-only bound
    // therefore cannot accidentally satisfy this guard.
    let held = competitor.reserve(budget.get() - retained_bytes).unwrap();
    let mut calls = 0;
    let result = reader.score_accounted(&terms, inputs(&reader, &delta, context, Some(2)), |_| {
        calls += 1;
        Ok(true)
    });
    assert!(
        matches!(result, Err(HawDBError::Execution(_))),
        "shared workspace admission must refuse before producing a partial result"
    );
    assert_eq!(calls, 0);
    assert_eq!(ledger.snapshot().used_bytes, held.bytes());
    drop(held);
    let retry = reader
        .score_accounted(&terms, inputs(&reader, &delta, context, Some(2)), |_| {
            Ok(true)
        })
        .unwrap();
    assert_eq!(retry.report, expected);
    drop(retry);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn lexical_query_context_cancellation_before_and_during_scoring_releases_every_owner() {
    let root = OwnedDirectory::new("context-cancel");
    let documents = documents();
    let reader = reader(&root, &documents);
    let terms = BTreeSet::from(["graph".into(), "storage".into()]);
    let delta = LexicalMiniDelta::default();
    let budget = NonZeroUsize::new(1024 * 1024).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let account = ledger.account(QueryMemoryClass::ExternalRead, "text producer", budget);
    for before in [true, false] {
        let cancellation = RuntimeCancellationToken::new();
        let task = RuntimeTaskContext::without_deadline(cancellation.clone());
        let context = QueryContext {
            memory: &account,
            task: &task,
        };
        if before {
            cancellation.cancel();
        }
        let mut calls = 0;
        let result = reader.score_accounted(&terms, inputs(&reader, &delta, context, None), |_| {
            calls += 1;
            cancellation.cancel();
            Ok(true)
        });
        assert!(matches!(result, Err(HawDBError::Execution(_))));
        assert_eq!(calls, usize::from(!before));
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
    let task = RuntimeTaskContext::default();
    let retry = reader
        .score_accounted(
            &terms,
            inputs(
                &reader,
                &delta,
                QueryContext {
                    memory: &account,
                    task: &task,
                },
                None,
            ),
            |_| Ok(true),
        )
        .unwrap();
    assert_eq!(retry.report.scores.len(), 3);
    drop(retry);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn lexical_query_context_delta_ids_admit_before_copy_and_drop_unretained_ids() {
    let _serial = crate::test_allocation::serial();
    let terms = BTreeSet::from(["graph".to_string()]);
    let long_id = "z".repeat(64 * 1024);
    let delta_document = document(&long_id, "", "graph");
    for with_base in [false, true] {
        let root = OwnedDirectory::new("context-delta-id");
        let documents = if with_base {
            BTreeMap::from([(
                "a".to_string(),
                document("a", "Graph Graph Graph Graph", "storage"),
            )])
        } else {
            BTreeMap::new()
        };
        let reader = reader(&root, &documents);
        let mut delta = Arc::new(LexicalMiniDelta::default());
        delta
            .upsert(&delta_document, None, &Default::default(), reader.config)
            .unwrap();
        let budget = NonZeroUsize::new(16 * 1024).unwrap();
        let ledger = QueryMemoryLedger::new(budget);
        let account = ledger.account(QueryMemoryClass::ExternalRead, "text producer", budget);
        // Initialize request-owned class telemetry before measuring scorer owners.
        drop(account.reserve(1).unwrap());
        let task = RuntimeTaskContext::default();
        let context = QueryContext {
            memory: &account,
            task: &task,
        };
        for retained in [None, Some(1), Some(0)] {
            let baseline = crate::test_allocation::live();
            let (result, peak) = crate::test_allocation::measure(|| {
                reader.score_accounted(&terms, inputs(&reader, &delta, context, retained), |_| {
                    Ok(true)
                })
            });
            assert!(
                peak.saturating_sub(baseline) < long_id.len() / 2,
                "unadmitted delta ID must not be cloned: peak increment {}, ID {}",
                peak.saturating_sub(baseline),
                long_id.len()
            );
            if retained == Some(0) || with_base && retained == Some(1) {
                let report = result.unwrap();
                assert_eq!(
                    report.report.matching_document_count,
                    1 + usize::from(with_base)
                );
                assert_eq!(
                    report.report.scores.len(),
                    usize::from(with_base && retained == Some(1))
                );
                assert!(!report.report.scores.contains_key(&long_id));
                drop(report);
            } else {
                assert!(matches!(result, Err(HawDBError::Execution(_))));
                drop(result);
            }
            assert_eq!(ledger.snapshot().used_bytes, 0);
            assert_eq!(crate::test_allocation::live(), baseline);
        }
        let large_budget = NonZeroUsize::new(256 * 1024).unwrap();
        let large_ledger = QueryMemoryLedger::new(large_budget);
        let large_account = large_ledger.account(
            QueryMemoryClass::ExternalRead,
            "text producer",
            large_budget,
        );
        let report = reader
            .score_accounted(
                &terms,
                inputs(
                    &reader,
                    &delta,
                    QueryContext {
                        memory: &large_account,
                        task: &task,
                    },
                    None,
                ),
                |_| Ok(true),
            )
            .unwrap();
        let mut full_documents = documents.clone();
        full_documents.insert(long_id.clone(), delta_document.clone());
        let corpus =
            crate::TextCorpusStats::from_documents(full_documents.values(), &Default::default());
        for document in full_documents.values() {
            let expected = crate::bm25_score(&terms, document, &corpus, &Default::default());
            assert_eq!(
                report
                    .report
                    .scores
                    .get(&document.id)
                    .copied()
                    .unwrap_or(0.0),
                expected
            );
        }
        assert!(large_ledger.snapshot().used_bytes >= long_id.len());
        drop(report);
        assert_eq!(large_ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn lexical_query_context_cancellation_during_final_materialization_refuses_and_recovers() {
    let _serial = crate::test_allocation::serial();
    let root = OwnedDirectory::new("context-final-cancel");
    let reader = reader(&root, &BTreeMap::new());
    let document = document("a", "", "graph");
    let mut delta = Arc::new(LexicalMiniDelta::default());
    delta
        .upsert(&document, None, &Default::default(), reader.config)
        .unwrap();
    let terms = BTreeSet::from(["graph".to_string()]);
    let budget = NonZeroUsize::new(1024 * 1024).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let account = ledger.account(QueryMemoryClass::ExternalRead, "text producer", budget);
    drop(account.reserve(1).unwrap());
    let healthy = RuntimeTaskContext::default();
    let context = QueryContext {
        memory: &account,
        task: &healthy,
    };
    let mut callback_allocations = 0;
    let mut calls = 0;
    let (report, allocations) = count_allocations(|| {
        reader
            .score_accounted(&terms, inputs(&reader, &delta, context, Some(1)), |_| {
                calls += 1;
                callback_allocations = allocation_count();
                Ok(true)
            })
            .unwrap()
    });
    assert_eq!(calls, 1);
    let after_predicate = allocations.checked_sub(callback_allocations).unwrap();
    assert!(
        after_predicate >= 2,
        "the retained ID and final score map both allocate after the predicate"
    );
    let corpus =
        crate::TextCorpusStats::from_documents([&document].into_iter(), &Default::default());
    assert_eq!(
        report.report.scores["a"],
        crate::bm25_score(&terms, &document, &corpus, &Default::default())
    );
    let expected = report.report.clone();
    drop(report);
    assert_eq!(ledger.snapshot().used_bytes, 0);

    // The only predicate precedes retained-ID admission and TopK finalization.
    // Derive its final allocation from the healthy path instead of racing a
    // timer/thread or depending on BTreeMap's internal node representation.
    let cancellation = RuntimeCancellationToken::new();
    let task = RuntimeTaskContext::without_deadline(cancellation.clone());
    let mut scheduled = None;
    calls = 0;
    // SAFETY: the observer forwards each allocation once. The fresh token has
    // no registered waiters; it is taken out of TLS before cancel is called.
    let (result, _) = unsafe {
        crate::test_allocation::measure_with_observer(allocation_observer, || {
            reader.score_accounted(
                &terms,
                inputs(
                    &reader,
                    &delta,
                    QueryContext {
                        memory: &account,
                        task: &task,
                    },
                    Some(1),
                ),
                |_| {
                    calls += 1;
                    scheduled = Some(cancel_on_allocation(cancellation.clone(), after_predicate));
                    Ok(true)
                },
            )
        })
    };
    drop(scheduled);
    assert_eq!(calls, 1);
    assert!(cancellation.is_cancelled(), "final allocation must cancel");
    assert!(
        matches!(&result, Err(HawDBError::Execution(message)) if message == "lexical query cancelled"),
        "cancellation during final materialization must refuse the complete report"
    );
    drop(result);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    let retry = reader
        .score_accounted(&terms, inputs(&reader, &delta, context, Some(1)), |_| {
            Ok(true)
        })
        .unwrap();
    assert_eq!(retry.report, expected);
    drop(retry);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn lexical_raw_query_terms_and_real_bm25_preserve_parity_and_retained_ownership() {
    let root = OwnedDirectory::new("raw-query-parity");
    let mut documents = documents();
    documents.insert(
        "han".into(),
        document("han", "中国 Graph", "龘靐齉 storage"),
    );
    let reader = reader(&root, &documents);
    let analyzer = SearchAnalyzerLexicon::default();
    let delta = LexicalMiniDelta::default();
    let task = RuntimeTaskContext::default();
    let budget = NonZeroUsize::new(64 * 1024 * 1024).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let account = ledger.account(QueryMemoryClass::ExternalRead, "raw text", budget);
    let context = QueryContext {
        memory: &account,
        task: &task,
    };
    let corpus = crate::TextCorpusStats::from_documents(documents.values(), &analyzer);
    for text in [
        "GraphStorage write_ahead_log graph graph",
        "ΟΣ GRAPH storage",
        "中国 Graph",
        "龘靐齉 Graph",
        "\u{20000}\u{20001} storage Graph",
    ] {
        let expected_terms = reader
            .tokenize_query(text, &analyzer, reader.config.max_term_bytes)
            .unwrap();
        let normalized = reader
            .tokenize_accounted(text, &analyzer, reader.config.max_term_bytes, context)
            .unwrap();
        assert_eq!(
            normalized
                .terms
                .iter()
                .map(|term| term.as_str().to_owned())
                .collect::<BTreeSet<_>>(),
            expected_terms
        );
        let retained = ledger.snapshot().used_bytes;
        assert!(
            retained > 0,
            "returned normalized terms must retain payload and set charges"
        );
        assert!(account.reserve(budget.get() - retained + 1).is_err());
        let expected = reader
            .score(&expected_terms, &delta, None, |_| Ok(true))
            .unwrap();
        let scored = reader
            .score_terms_accounted(
                &normalized.terms,
                inputs(&reader, &delta, context, None),
                |_| Ok(true),
            )
            .unwrap();
        assert_eq!(scored.report, expected);
        for document in documents.values() {
            assert_eq!(
                scored
                    .report
                    .scores
                    .get(&document.id)
                    .copied()
                    .unwrap_or(0.0),
                crate::bm25_score(&expected_terms, document, &corpus, &analyzer)
            );
        }
        assert!(ledger.snapshot().used_bytes > retained);
        drop(scored);
        assert_eq!(ledger.snapshot().used_bytes, retained);
        drop(normalized);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn lexical_raw_query_retained_charge_covers_actual_term_and_set_allocations() {
    let _serial = crate::test_allocation::serial();
    let root = OwnedDirectory::new("raw-query-set-allocation");
    let reader = reader(&root, &documents());
    let analyzer = SearchAnalyzerLexicon::default();
    let task = RuntimeTaskContext::default();
    let budget = NonZeroUsize::new(1024 * 1024).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let account = ledger.account(QueryMemoryClass::ExternalRead, "raw text", budget);
    // Register the account's class outside the measured query allocation.
    drop(account.reserve(1).unwrap());
    let many = (b'a'..=b'l')
        .map(|suffix| format!("zz{}", char::from(suffix)))
        .collect::<Vec<_>>()
        .join(" ");
    for text in ["graph graph graph", many.as_str()] {
        let expected = reader
            .tokenize_query(text, &analyzer, reader.config.max_term_bytes)
            .unwrap();
        if text == many {
            assert!(expected.len() > 16, "exercise retained set growth");
        }
        let baseline = crate::test_allocation::live();
        let (normalized, _) = crate::test_allocation::measure(|| {
            reader
                .tokenize_accounted(
                    text,
                    &analyzer,
                    reader.config.max_term_bytes,
                    QueryContext {
                        memory: &account,
                        task: &task,
                    },
                )
                .unwrap()
        });
        let allocated = crate::test_allocation::live()
            .checked_sub(baseline)
            .expect("query retains measured allocations");
        assert!(allocated > 0);
        let retained = ledger.snapshot().used_bytes;
        assert!(
            retained >= allocated,
            "normalized query charge must cover actual term and set allocations: charged {retained}, allocated {allocated}"
        );
        assert_eq!(
            normalized
                .terms
                .iter()
                .map(|term| term.as_str().to_owned())
                .collect::<BTreeSet<_>>(),
            expected
        );
        assert!(account.reserve(budget.get() - retained + 1).is_err());
        drop(normalized);
        assert_eq!(crate::test_allocation::live(), baseline);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn lexical_raw_query_competing_root_refuses_before_token_copy_and_recovers() {
    let _serial = crate::test_allocation::serial();
    let root = OwnedDirectory::new("raw-query-budget");
    let reader = reader(&root, &documents());
    let text = "x".repeat(64 * 1024);
    let analyzer = SearchAnalyzerLexicon::default();
    let task = RuntimeTaskContext::default();
    let budget = NonZeroUsize::new(128 * 1024).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let account = ledger.account(QueryMemoryClass::ExternalRead, "raw text", budget);
    drop(account.reserve(1).unwrap());
    let competitor = ledger.account(QueryMemoryClass::PipelineBatch, "other operator", budget);
    let competing = competitor.reserve(budget.get() - 16 * 1024).unwrap();
    let context = QueryContext {
        memory: &account,
        task: &task,
    };
    let baseline = crate::test_allocation::live();
    let (result, peak) = crate::test_allocation::measure(|| {
        reader.tokenize_accounted(
            &text,
            &analyzer,
            NonZeroU64::new(128 * 1024).unwrap(),
            context,
        )
    });
    let error = result
        .err()
        .expect("shared root must refuse the token before copying it");
    assert!(error.to_string().contains("query_memory_bytes"), "{error}");
    assert!(
        peak.saturating_sub(baseline) < text.len() / 2,
        "unadmitted raw token must not be cloned: peak increment {}",
        peak.saturating_sub(baseline)
    );
    drop(error);
    assert_eq!(crate::test_allocation::live(), baseline);
    assert_eq!(ledger.snapshot().used_bytes, competing.bytes());
    drop(competing);
    let normalized = reader
        .tokenize_accounted(
            &text,
            &analyzer,
            NonZeroU64::new(128 * 1024).unwrap(),
            context,
        )
        .unwrap();
    assert_eq!(normalized.terms.len(), 1);
    assert_eq!(normalized.terms.first().unwrap().as_str(), text);
    assert!(ledger.snapshot().used_bytes >= text.len());
    drop(normalized);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn lexical_raw_query_pre_and_mid_normalization_cancellation_refuse_and_recover() {
    let _serial = crate::test_allocation::serial();
    let root = OwnedDirectory::new("raw-query-cancel");
    let reader = reader(&root, &documents());
    let analyzer = SearchAnalyzerLexicon::default();
    let budget = NonZeroUsize::new(1024 * 1024).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let account = ledger.account(QueryMemoryClass::ExternalRead, "raw text", budget);
    drop(account.reserve(1).unwrap());
    for precancel in [true, false] {
        let task = RuntimeTaskContext::default();
        let context = QueryContext {
            memory: &account,
            task: &task,
        };
        let result = if precancel {
            task.cancellation().cancel();
            reader.tokenize_accounted(
                "Graph storage",
                &analyzer,
                reader.config.max_term_bytes,
                context,
            )
        } else {
            let _cancel = cancel_on_allocation(task.cancellation().clone(), 1);
            count_allocations(|| {
                reader.tokenize_accounted(
                    "Graph storage",
                    &analyzer,
                    reader.config.max_term_bytes,
                    context,
                )
            })
            .0
        };
        let error = result
            .err()
            .expect("cancelled normalization must not return terms");
        assert!(task.cancellation().is_cancelled());
        assert!(
            error.to_string().contains("lexical query cancelled"),
            "{error}"
        );
        drop(error);
        assert_eq!(ledger.snapshot().used_bytes, 0);
        let healthy = RuntimeTaskContext::default();
        let terms = reader
            .tokenize_accounted(
                "Graph storage",
                &analyzer,
                reader.config.max_term_bytes,
                QueryContext {
                    memory: &account,
                    task: &healthy,
                },
            )
            .unwrap();
        assert_eq!(
            terms
                .terms
                .iter()
                .map(|term| term.as_str().to_owned())
                .collect::<BTreeSet<_>>(),
            reader
                .tokenize_query("Graph storage", &analyzer, reader.config.max_term_bytes)
                .unwrap()
        );
        drop(terms);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}

struct QueryTlsExit {
    ledger: QueryMemoryLedger,
    observed: Arc<std::sync::atomic::AtomicUsize>,
    late_cancel: Option<RuntimeCancellationToken>,
}

impl Drop for QueryTlsExit {
    fn drop(&mut self) {
        self.observed.store(
            self.ledger.snapshot().used_bytes,
            std::sync::atomic::Ordering::Relaxed,
        );
        if let Some(token) = &self.late_cancel {
            token.cancel();
        }
    }
}

thread_local! {
    static QUERY_TLS_EXIT: RefCell<Option<QueryTlsExit>> = const { RefCell::new(None) };
}

#[test]
fn lexical_raw_query_cjk_uses_shared_root_and_retains_workspace_until_native_join() {
    let root = OwnedDirectory::new("raw-query-cjk-root");
    let reader = reader(&root, &documents());
    let analyzer = SearchAnalyzerLexicon::default();
    let text = "中国 Graph";
    let budget = NonZeroUsize::new(64 * 1024 * 1024).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let account = ledger.account(QueryMemoryClass::ExternalRead, "raw text", budget);
    let competitor = ledger.account(QueryMemoryClass::PipelineBatch, "other operator", budget);
    let competing = competitor.reserve(budget.get() - 4 * 1024 * 1024).unwrap();
    let task = RuntimeTaskContext::default();
    let error = reader
        .tokenize_accounted(
            text,
            &analyzer,
            reader.config.max_term_bytes,
            QueryContext {
                memory: &account,
                task: &task,
            },
        )
        .err()
        .expect("CJK construction must respect competing root ownership");
    assert!(error.to_string().contains("query_memory_bytes"), "{error}");
    assert_eq!(ledger.snapshot().used_bytes, competing.bytes());
    drop(competing);
    let normalized = reader
        .tokenize_accounted(
            text,
            &analyzer,
            reader.config.max_term_bytes,
            QueryContext {
                memory: &account,
                task: &task,
            },
        )
        .unwrap();
    assert_eq!(
        normalized
            .terms
            .iter()
            .map(|term| term.as_str().to_owned())
            .collect::<BTreeSet<_>>(),
        reader
            .tokenize_query(text, &analyzer, reader.config.max_term_bytes)
            .unwrap()
    );
    // Returning keeps only normalized-term ownership, not the joined worker stack.
    assert!(ledger.snapshot().used_bytes < 2 * 1024 * 1024);
    drop(normalized);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    for mode in 0..5 {
        let task = RuntimeTaskContext::default();
        let observed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let caller = std::thread::current().id();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            crate::analyzer_workspace::run_query(&account, &task, |workspace| {
                assert_ne!(std::thread::current().id(), caller);
                QUERY_TLS_EXIT.with(|exit| {
                    *exit.borrow_mut() = Some(QueryTlsExit {
                        ledger: ledger.clone(),
                        observed: Arc::clone(&observed),
                        late_cancel: (mode == 4).then(|| task.cancellation().clone()),
                    })
                });
                match mode {
                    0 | 4 => Ok(()),
                    1 => Err(HawDBError::Execution("raw query consumer stopped".into())),
                    2 => panic!("raw query worker panic"),
                    _ => {
                        task.cancellation().cancel();
                        workspace.checkpoint()
                    }
                }
            })
        }));
        assert!(
            observed.load(std::sync::atomic::Ordering::Relaxed) >= 2 * 1024 * 1024,
            "query workspace/stack leases must survive worker TLS destruction"
        );
        if mode == 2 {
            assert!(result.is_err());
        } else {
            let result = result.unwrap();
            if mode == 0 {
                result.unwrap();
            } else {
                let error = result.unwrap_err();
                assert!(
                    error.to_string().contains(if mode == 1 {
                        "consumer stopped"
                    } else {
                        "lexical query cancelled"
                    }),
                    "{error}"
                );
            }
        }
        assert_eq!(ledger.snapshot().used_bytes, 0);
        let healthy = RuntimeTaskContext::default();
        crate::analyzer_workspace::run_query(&account, &healthy, |workspace| {
            workspace.checkpoint()
        })
        .unwrap();
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}
