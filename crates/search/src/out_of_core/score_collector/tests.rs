// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use hawdb_executor::{QueryMemoryClass, QueryMemoryLedger};

#[test]
fn score_heap_transfers_funded_ids_to_map_and_refunds_replaced_payloads() {
    let budget = NonZeroUsize::new(64 << 10).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let memory = ledger.account(QueryMemoryClass::BlockingState, "score collector", budget);
    let task = crate::RuntimeTaskContext::default();
    for limit in [Some(0), Some(2), None] {
        let mut collector = BoundedScoreCollector::with_context(
            limit,
            10,
            Some(QueryContext {
                memory: &memory,
                task: &task,
                execution: None,
                io_scope: None,
            }),
        )
        .unwrap();
        for (id, score) in [("long-old-id", 1.0), ("b", 2.0), ("a", 2.0), ("z", 0.5)] {
            collector.push_borrowed(id, score).unwrap();
        }
        assert_eq!(collector.matching_count, 4);
        let scores = collector.finish().unwrap();
        let expected = match limit {
            Some(0) => vec![],
            Some(2) => vec!["a", "b"],
            _ => vec!["a", "b", "long-old-id", "z"],
        };
        assert_eq!(
            scores.values.keys().map(String::as_str).collect::<Vec<_>>(),
            expected
        );
        if !scores.values.is_empty() {
            assert!(ledger.snapshot().used_bytes >= scores.values.len() * MAP_ENTRY_BYTES);
        }
        drop(scores);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn score_heap_map_admission_failure_drops_heap_before_refunding() {
    let budget = NonZeroUsize::new(32 << 10).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let memory = ledger.account(QueryMemoryClass::BlockingState, "score failure", budget);
    let task = crate::RuntimeTaskContext::default();
    let mut collector = BoundedScoreCollector::with_context(
        Some(2),
        10,
        Some(QueryContext {
            memory: &memory,
            task: &task,
            execution: None,
            io_scope: None,
        }),
    )
    .unwrap();
    collector.push_borrowed("a", 1.0).unwrap();
    collector.push_borrowed("b", 2.0).unwrap();
    let blocked = memory.reserve(memory.available_bytes()).unwrap();
    assert!(matches!(
        collector.finish(),
        Err(HawDBError::ResourceBudgetExceeded(_))
    ));
    assert_eq!(ledger.snapshot().used_bytes, blocked.bytes());
    drop(blocked);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}
