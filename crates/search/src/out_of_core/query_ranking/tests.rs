// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use hawdb_executor::{QueryMemoryClass, QueryMemoryLedger};

#[test]
fn admitted_ranking_preserves_ties_windows_offsets_and_payload_lifetime() {
    let budget = NonZeroUsize::new(256 << 10).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let memory = ledger.account(QueryMemoryClass::BlockingState, "query ranking", budget);
    let task = crate::RuntimeTaskContext::default();
    let context = Some(QueryContext {
        memory: &memory,
        task: &task,
        execution: None,
        io_scope: None,
    });
    let text = BTreeMap::from([("a".into(), 2.0), ("b".into(), 2.0), ("c".into(), 1.0)]);
    let vector = BTreeMap::from([("b".into(), 0.9), ("c".into(), 0.9), ("d".into(), 0.1)]);
    for mode in [SearchMode::Text, SearchMode::Vector, SearchMode::Hybrid] {
        for window in [None, Some(0), Some(1), Some(2)] {
            let text_ranks = Ranks::new(&text, context).unwrap();
            let vector_ranks = Ranks::new(&vector, context).unwrap();
            assert_eq!(text_ranks.values, ranked_scores(&text));
            let text_window = text_ranks.window(window, context).unwrap();
            let vector_window = vector_ranks.window(window, context).unwrap();
            let expected = match (mode, window) {
                (SearchMode::Text, _) => vec!["a", "b", "c"],
                (SearchMode::Vector, _) => vec!["b", "c", "d"],
                (_, Some(0)) => vec![],
                (_, Some(1)) => vec!["a", "b"],
                (_, Some(2)) => vec!["b", "a", "c"],
                _ => vec!["b", "c", "a", "d"],
            };
            for offset in [0, 1, 9] {
                let mut candidates = Candidates::new(
                    Inputs {
                        vector_scores: &vector,
                        text_scores: &text,
                        vector_ranks: &vector_ranks,
                        text_ranks: &text_ranks,
                        vector_window: &vector_window,
                        text_window: &text_window,
                        mode,
                        weights: Default::default(),
                    },
                    context,
                )
                .unwrap();
                assert_eq!(
                    candidates
                        .values
                        .iter()
                        .map(|candidate| candidate.id.as_str())
                        .collect::<Vec<_>>(),
                    expected
                );
                candidates.page(offset, 2).unwrap();
                assert_eq!(
                    candidates
                        .values
                        .iter()
                        .map(|candidate| candidate.id.as_str())
                        .collect::<Vec<_>>(),
                    expected
                        .iter()
                        .copied()
                        .skip(offset)
                        .take(2)
                        .collect::<Vec<_>>()
                );
            }
        }
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn ranking_window_refusal_preserves_existing_ranks_and_refunds() {
    let budget = NonZeroUsize::new(64 << 10).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let memory = ledger.account(QueryMemoryClass::BlockingState, "rank refusal", budget);
    let task = crate::RuntimeTaskContext::default();
    let context = Some(QueryContext {
        memory: &memory,
        task: &task,
        execution: None,
        io_scope: None,
    });
    let scores = BTreeMap::from([("a".into(), 1.0), ("b".into(), 1.0)]);
    let ranks = Ranks::new(&scores, context).unwrap();
    let before = ledger.snapshot().used_bytes;
    let blocked = memory.reserve(memory.available_bytes()).unwrap();
    assert!(matches!(
        ranks.window(Some(1), context),
        Err(HawDBError::ResourceBudgetExceeded(_))
    ));
    assert_eq!(ranks.values["a"], 1);
    drop(blocked);
    assert_eq!(ledger.snapshot().used_bytes, before);
    drop(ranks);
    task.cancellation().cancel();
    assert!(matches!(
        Ranks::new(&BTreeMap::new(), context),
        Err(HawDBError::Execution(_))
    ));
    assert_eq!(ledger.snapshot().used_bytes, 0);
}
