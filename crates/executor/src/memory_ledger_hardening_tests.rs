use super::*;
use std::panic::catch_unwind;

fn poison_valid_state(ledger: &QueryMemoryLedger) {
    assert!(catch_unwind(|| {
        let _guard = lock_recover(&ledger.inner.state);
        panic!("injected unwind while holding valid ledger state");
    })
    .is_err());
    assert!(ledger.inner.state.is_poisoned());
}

fn assert_hierarchy(ledger: &QueryMemoryLedger, expected: usize) {
    let snapshot = ledger.snapshot();
    assert_eq!(snapshot.used_bytes, expected);
    assert_eq!(
        snapshot
            .classes
            .iter()
            .map(|entry| entry.used_bytes)
            .sum::<usize>(),
        expected
    );
    let state = lock_recover(&ledger.inner.state);
    assert_eq!(
        state
            .accounts
            .values()
            .map(|entry| entry.used_bytes)
            .sum::<usize>(),
        expected
    );
    for (class, entry) in &state.classes {
        assert_eq!(
            entry.used_bytes,
            state
                .accounts
                .values()
                .filter(|account| account.class == *class)
                .map(|account| account.used_bytes)
                .sum::<usize>()
        );
        assert!(entry.peak_bytes >= entry.used_bytes);
    }
}

#[test]
fn poisoned_ledger_supports_reserve_grow_transfer_and_drop() {
    for same_class in [false, true] {
        let budget = NonZeroUsize::new(32).unwrap();
        let ledger = QueryMemoryLedger::new(budget);
        let source = ledger.account(QueryMemoryClass::BlockingState, "source", budget);
        let mut left = source.reserve(16).unwrap();
        poison_valid_state(&ledger);

        let target = ledger.account(
            if same_class {
                QueryMemoryClass::BlockingState
            } else {
                QueryMemoryClass::PipelineBatch
            },
            "target",
            budget,
        );
        let mut right = target.reserve(4).unwrap();
        left.grow(4).unwrap();
        assert_hierarchy(&ledger, 24);
        left.transfer_to(12, &mut right, 16).unwrap();
        assert_eq!((left.bytes(), right.bytes()), (8, 20));
        assert_hierarchy(&ledger, 28);
        left.shrink(usize::MAX);
        assert_hierarchy(&ledger, 20);
        right.reset();
        assert_hierarchy(&ledger, 0);
        left.grow(32).unwrap();
        drop((left, right));
        assert_hierarchy(&ledger, 0);
        assert_eq!(ledger.snapshot().peak_bytes, 32);
    }
}

#[test]
fn poisoned_ledger_failed_reservations_and_transfers_are_atomic() {
    let budget = NonZeroUsize::new(32).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let source = ledger.account(QueryMemoryClass::BlockingState, "source", budget);
    let target = ledger.account(QueryMemoryClass::PipelineBatch, "target", budget);
    let mut left = source.reserve(24).unwrap();
    let mut right = target.reserve(4).unwrap();
    poison_valid_state(&ledger);
    let before = ledger.snapshot();

    for bytes in [9, usize::MAX] {
        assert!(source.reserve(bytes).is_err());
        assert!(left.grow(bytes).is_err());
        assert!(left.transfer_to(0, &mut right, bytes).is_err());
        assert_eq!((left.bytes(), right.bytes()), (24, 4));
        assert_eq!(ledger.snapshot(), before);
        assert_hierarchy(&ledger, 28);
    }
    assert!(left.transfer_to(25, &mut right, 0).is_err());
    assert_eq!(ledger.snapshot(), before);
    left.transfer_to(24, &mut right, 28).unwrap();
    assert_hierarchy(&ledger, 32);
    drop((left, right));
    assert_hierarchy(&ledger, 0);
}

#[test]
fn owner_conversion_runs_outside_the_ledger_lock_and_cannot_poison_it() {
    struct Owner<'a>(&'a QueryMemoryLedger);
    impl From<Owner<'_>> for Arc<str> {
        fn from(owner: Owner<'_>) -> Self {
            // try_lock proves reentrancy is possible without risking a hung regression test.
            assert!(owner.0.inner.state.try_lock().is_ok());
            assert_eq!(owner.0.snapshot().account_count, 0);
            panic!("injected caller conversion panic");
        }
    }

    let budget = NonZeroUsize::new(8).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    assert!(catch_unwind(|| ledger.account(
        QueryMemoryClass::BlockingState,
        Owner(&ledger),
        budget
    ))
    .is_err());
    assert!(!ledger.inner.state.is_poisoned());
    assert_eq!(ledger.snapshot().account_count, 0);
    let account = ledger.account(QueryMemoryClass::BlockingState, "after panic", budget);
    let lease = account.reserve(8).unwrap();
    assert_hierarchy(&ledger, 8);
    drop(lease);
    assert_hierarchy(&ledger, 0);
}

#[test]
fn poisoned_ledger_concurrent_accounts_share_one_budget() {
    const WORKERS: usize = 8;
    let budget = NonZeroUsize::new(4).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    poison_valid_state(&ledger);
    let start = Arc::new(std::sync::Barrier::new(WORKERS + 1));
    let settled = Arc::new(std::sync::Barrier::new(WORKERS + 1));
    let release = Arc::new(std::sync::Barrier::new(WORKERS + 1));
    let workers = (0..WORKERS)
        .map(|index| {
            let account = ledger.account(
                QueryMemoryClass::MorselOutput,
                format!("worker {index}"),
                budget,
            );
            let start = Arc::clone(&start);
            let settled = Arc::clone(&settled);
            let release = Arc::clone(&release);
            std::thread::spawn(move || {
                start.wait();
                let lease = account.reserve(1).ok();
                settled.wait();
                release.wait();
                lease.is_some()
            })
        })
        .collect::<Vec<_>>();
    start.wait();
    settled.wait();
    let used = ledger.snapshot().used_bytes;
    release.wait();
    let admitted = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .filter(|admitted| *admitted)
        .count();
    assert_eq!(used, 4);
    assert_eq!(admitted, 4);
    assert_hierarchy(&ledger, 0);
}
