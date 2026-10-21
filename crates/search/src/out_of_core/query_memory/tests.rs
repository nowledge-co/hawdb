// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::RuntimeTaskContext;
use hawdb_executor::{QueryMemoryClass, QueryMemoryLedger};
use hawdb_optimizer_predicate::{
    push_search_predicates, SearchPredicateOp, SearchScanPredicateSupport,
};
use std::cell::RefCell;

thread_local! {
    static CANCELLATION: RefCell<Option<(usize, crate::RuntimeCancellationToken)>> = const { RefCell::new(None) };
}

fn cancel_during_allocation(bytes: usize) {
    crate::test_allocation::record_allocation(bytes);
    let token = CANCELLATION
        .try_with(|slot| {
            let mut slot = slot.try_borrow_mut().ok()?;
            if slot
                .as_ref()
                .is_some_and(|(threshold, _)| bytes >= *threshold)
            {
                slot.take().map(|(_, token)| token)
            } else {
                None
            }
        })
        .ok()
        .flatten();
    // The TLS borrow is closed. This fresh token has no registered waiters.
    if let Some(token) = token {
        token.cancel();
    }
}

#[test]
fn metadata_construction_preserves_all_operations_aliases_and_parser_errors() {
    let _serial = crate::test_allocation::serial();
    let budget = NonZeroUsize::new(4 << 20).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let account = ledger.account(QueryMemoryClass::BlockingState, "predicate test", budget);
    drop(account.reserve(1).unwrap());
    let task = RuntimeTaskContext::default();
    let query = Some(QueryContext {
        memory: &account,
        task: &task,
        execution: None,
        io_scope: None,
    });
    for filters in [
        BTreeMap::new(),
        BTreeMap::from([
            ("kind".into(), "\u{2003}Memory\u{2003}".into()),
            ("history".into(), " TRUE ".into()),
            ("latest__in".into(), r#"["1"," FALSE "]"#.into()),
            (
                "labels__not_in".into(),
                r#"["Deleted","Deleted", ""]"#.into(),
            ),
            ("spaces__exists".into(), "1".into()),
            ("temporal_contexts__missing".into(), "false".into()),
            ("event_date_from".into(), "2026-01-01".into()),
            ("recorded_date__gt".into(), "2025-01-01".into()),
            ("created_at__lt".into(), "2027-01-01".into()),
            ("updated_at__lte".into(), "2028-01-01".into()),
        ]),
        BTreeMap::from([("kind__in".into(), "[]".into())]),
        BTreeMap::from([("kind__not_in".into(), "[]".into())]),
        BTreeMap::from([("kind__in".into(), r#"["\uD83D\uDE80","a\"b","\\"]"#.into())]),
        BTreeMap::from([("kind__in".into(), r#"["\uD800"]"#.into())]),
        BTreeMap::from([("kind__in".into(), r#"["value",false]"#.into())]),
        BTreeMap::from([("kind__in".into(), r#""other type""#.into())]),
        BTreeMap::from([("kind__in".into(), r#"["unfinished\"#.into())]),
        BTreeMap::from([("history__in".into(), r#"["true","invalid"]"#.into())]),
        BTreeMap::from([("event_date__not_in".into(), "[]".into())]),
        BTreeMap::from([("latest__gte".into(), "true".into())]),
    ] {
        let legacy = crate::search_metadata_predicate_pushdown(&filters);
        let actual = Predicates::new(&filters, query).unwrap();
        assert_eq!(actual.predicates, legacy.predicates);
        assert_eq!(actual.report, legacy.report);
        let optimizer =
            push_search_predicates(&actual.predicates, SearchScanPredicateSupport::default());
        assert_eq!(optimizer.pushed(), &actual.predicates);
        assert!(optimizer.residual().is_empty());
        drop(actual);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn metadata_construction_funds_decoding_empty_values_duplicates_and_malformed_lists() {
    let _serial = crate::test_allocation::serial();
    let budget = NonZeroUsize::new(16 << 20).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let account = ledger.account(QueryMemoryClass::BlockingState, "predicate peak", budget);
    drop(account.reserve(1).unwrap());
    let task = RuntimeTaskContext::default();
    let query = Some(QueryContext {
        memory: &account,
        task: &task,
        execution: None,
        io_scope: None,
    });
    let escaped = format!(r#"["{}"]"#, r"\u0041\uD83D\uDE80".repeat(4096));
    let unique =
        serde_json::to_string(&(0..2048).map(|n| format!("value-{n}")).collect::<Vec<_>>())
            .unwrap();
    let empty = format!("[{}]", vec![r#""""#; 2048].join(","));
    let whitespace = format!("[{}\"value\"{}]", " ".repeat(65536), " ".repeat(65536));
    for value in [
        escaped,
        unique,
        empty,
        whitespace,
        format!(r#"["{}\uD800"]"#, "x".repeat(65536)),
    ] {
        let filters = BTreeMap::from([("labels__in".into(), value)]);
        let baseline = crate::test_allocation::live();
        let (output, peak) =
            crate::test_allocation::measure(|| Predicates::new(&filters, query).unwrap());
        assert!(
            peak.saturating_sub(baseline) <= ledger.snapshot().used_bytes,
            "construction peak {}, admitted {}",
            peak.saturating_sub(baseline),
            ledger.snapshot().used_bytes
        );
        drop(output);
        assert_eq!(crate::test_allocation::live(), baseline);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn metadata_construction_refuses_before_large_copy_preserves_existing_owner_and_refunds() {
    let _serial = crate::test_allocation::serial();
    let budget = NonZeroUsize::new(2 << 20).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let account = ledger.account(QueryMemoryClass::BlockingState, "predicate refusal", budget);
    drop(account.reserve(1).unwrap());
    let task = RuntimeTaskContext::default();
    let query = Some(QueryContext {
        memory: &account,
        task: &task,
        execution: None,
        io_scope: None,
    });
    let kept = Predicates::new(&BTreeMap::from([("kind".into(), "Memory".into())]), query).unwrap();
    let filters = BTreeMap::from([(
        "labels__in".into(),
        format!(r#"["{}"]"#, "x".repeat(256 << 10)),
    )]);
    let held = account.reserve(account.available_bytes() - 4096).unwrap();
    let used = ledger.snapshot().used_bytes;
    let baseline = crate::test_allocation::live();
    let (refused, peak) = crate::test_allocation::measure(|| Predicates::new(&filters, query));
    assert!(matches!(
        refused,
        Err(HawDBError::ResourceBudgetExceeded(_))
    ));
    assert!(peak.saturating_sub(baseline) < 4096);
    assert_eq!(ledger.snapshot().used_bytes, used);
    assert!(
        matches!(kept.predicates.predicates()[0].op(), SearchPredicateOp::Eq(value) if value.as_str() == "memory")
    );
    drop(refused);
    drop(held);
    let retry = Predicates::new(&filters, query).unwrap();
    drop(retry);
    let before = ledger.snapshot().used_bytes;
    task.cancellation().cancel();
    assert!(matches!(
        Predicates::new(&filters, query),
        Err(HawDBError::Execution(_))
    ));
    assert_eq!(ledger.snapshot().used_bytes, before);
    drop(kept);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn metadata_construction_mid_allocation_cancellation_releases_payload_before_admission() {
    let _serial = crate::test_allocation::serial();
    let filters = BTreeMap::from([(
        "labels__in".into(),
        format!(r#"["{}"]"#, "x".repeat(64 << 10)),
    )]);
    for threshold in [1, 32 << 10] {
        let budget = NonZeroUsize::new(2 << 20).unwrap();
        let ledger = QueryMemoryLedger::new(budget);
        let account = ledger.account(QueryMemoryClass::BlockingState, "predicate cancel", budget);
        drop(account.reserve(1).unwrap());
        let task = RuntimeTaskContext::default();
        // Initialize the platform's lazy waiter mutex before observing inside
        // GlobalAlloc, then unregister the sole waiter. Cancellation itself
        // must not allocate or invoke a waker from the observer.
        let mut waiter = task.cancellation().cancelled();
        assert!(std::future::Future::poll(
            std::pin::Pin::new(&mut waiter),
            &mut std::task::Context::from_waker(std::task::Waker::noop()),
        )
        .is_pending());
        drop(waiter);
        let query = Some(QueryContext {
            memory: &account,
            task: &task,
            execution: None,
            io_scope: None,
        });
        CANCELLATION
            .with(|slot| *slot.borrow_mut() = Some((threshold, task.cancellation().clone())));
        let baseline = crate::test_allocation::live();
        // SAFETY: the observer forwards each size once, never allocates or
        // unwinds, and closes its TLS borrow before canceling a waiter-free token.
        let (result, _) = unsafe {
            crate::test_allocation::measure_with_observer(cancel_during_allocation, || {
                Predicates::new(&filters, query)
            })
        };
        CANCELLATION.with(|slot| *slot.borrow_mut() = None);
        assert!(task.cancellation().is_cancelled());
        assert!(matches!(result, Err(HawDBError::Execution(_))));
        drop(result);
        assert_eq!(ledger.snapshot().used_bytes, 0);
        assert_eq!(crate::test_allocation::live(), baseline);
    }
}
