use super::*;
use crate::build_memory::{reserved::ReservedMemory, BuildMemory};
use skein_core::{RuntimeMemoryReservation, RuntimeTaskContext};
use std::panic::{catch_unwind, AssertUnwindSafe};

const OLD_CAPACITY: usize = 8;
const NEW_CAPACITY: usize = 16;
const OLD_BYTES: usize = OLD_CAPACITY * size_of::<u64>();
const NEW_BYTES: usize = NEW_CAPACITY * size_of::<u64>();
const ORIGINAL: [u64; 3] = [17, 42, 91];

#[derive(Clone, Copy)]
enum Failure {
    Allocation,
    Overgrant,
    Unwind,
}

impl Failure {
    fn allocate(
        self,
        values: &mut Vec<u64>,
        capacity: usize,
    ) -> std::result::Result<(), TryReserveError> {
        match self {
            Self::Allocation => values.try_reserve_exact(usize::MAX),
            Self::Overgrant => values.try_reserve_exact(capacity + 1),
            Self::Unwind => {
                values.try_reserve_exact(capacity)?;
                panic!("allocator callback unwound after allocation");
            }
        }
    }
}

fn memory(bytes: usize) -> BuildMemory {
    BuildMemory::new(
        &RuntimeTaskContext::default()
            .with_memory_reservation(RuntimeMemoryReservation::new(bytes as u64, 0)),
    )
    .unwrap()
}

fn original() -> Vec<u64> {
    let mut values = Vec::with_capacity(OLD_CAPACITY);
    values.extend_from_slice(&ORIGINAL);
    values
}

fn reject(values: &mut Vec<u64>, memory: Memory<'_>, failure: Failure) {
    let address = values.as_ptr();
    let result = catch_unwind(AssertUnwindSafe(|| {
        reserve_with(
            values,
            NEW_CAPACITY,
            memory,
            "test slots",
            |values, size| failure.allocate(values, size),
        )
    }));
    match failure {
        Failure::Allocation => assert!(result
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("test slots allocation failed")),
        Failure::Overgrant => assert!(result
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("test slots exceeded admission")),
        Failure::Unwind => assert!(result.is_err()),
    }
    assert_eq!(values.as_ptr(), address);
    assert_eq!(values.capacity(), OLD_CAPACITY);
    assert_eq!(values, &ORIGINAL);
}

#[test]
fn rejected_replacement_preserves_query_capacity_and_can_be_retried() {
    for failure in [Failure::Allocation, Failure::Overgrant, Failure::Unwind] {
        let memory = memory(OLD_BYTES + NEW_BYTES);
        let mut lease = memory.retained.reserve(OLD_BYTES).unwrap();
        let mut values = original();
        reject(&mut values, Memory::Lease(&mut lease), failure);
        assert_eq!(lease.bytes(), OLD_BYTES);
        assert_eq!(memory.ledger.snapshot().used_bytes, OLD_BYTES);

        super::super::reserve_capacity(&mut values, NEW_CAPACITY, &mut lease).unwrap();
        assert_eq!(values, ORIGINAL);
        assert_eq!(values.capacity(), NEW_CAPACITY);
        assert_eq!(lease.bytes(), NEW_BYTES);
        assert_eq!(memory.ledger.snapshot().used_bytes, NEW_BYTES);
        assert_eq!(memory.ledger.snapshot().peak_bytes, OLD_BYTES + NEW_BYTES);
        drop(values);
        drop(lease);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn rejected_replacement_restores_shared_progress_at_an_exhausted_root() {
    let capacity = OLD_BYTES + NEW_BYTES;
    let root_bytes = capacity + ReservedMemory::metadata_bytes();
    for failure in [Failure::Allocation, Failure::Overgrant, Failure::Unwind] {
        let memory = memory(root_bytes);
        let progress = ReservedMemory::new(&memory.spool, capacity).unwrap();
        let mut grant = progress.reserve(OLD_BYTES).unwrap();
        let mut values = original();
        assert!(memory.retained.reserve(1).is_err());
        reject(&mut values, Memory::Grant(Some(&mut grant)), failure);
        assert_eq!(grant.bytes(), OLD_BYTES);
        let free = progress.reserve(NEW_BYTES).unwrap();
        assert!(progress.reserve(1).is_err());
        drop(free);

        reserve(
            &mut values,
            NEW_CAPACITY,
            Memory::Grant(Some(&mut grant)),
            "search spill slots",
        )
        .unwrap();
        assert_eq!(values, ORIGINAL);
        assert_eq!(values.capacity(), NEW_CAPACITY);
        assert_eq!(grant.bytes(), NEW_BYTES);
        let free = progress.reserve(OLD_BYTES).unwrap();
        assert!(progress.reserve(1).is_err());
        drop(free);
        assert_eq!(memory.ledger.snapshot().used_bytes, root_bytes);
        drop(values);
        drop(grant);
        drop(progress);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn progress_denial_precedes_candidate_allocation_and_preserves_the_old_owner() {
    let capacity = OLD_BYTES + NEW_BYTES - 1;
    let memory = memory(capacity + ReservedMemory::metadata_bytes());
    let progress = ReservedMemory::new(&memory.spool, capacity).unwrap();
    let mut grant = progress.reserve(OLD_BYTES).unwrap();
    let mut values = original();
    let address = values.as_ptr();
    let error = reserve_with(
        &mut values,
        NEW_CAPACITY,
        Memory::Grant(Some(&mut grant)),
        "search spill slots",
        |_, _| panic!("denied candidate must not allocate"),
    )
    .unwrap_err();
    assert!(error.to_string().contains("search spill progress"));
    assert_eq!(values.as_ptr(), address);
    assert_eq!(values, ORIGINAL);
    assert_eq!(grant.bytes(), OLD_BYTES);
    assert!(progress.reserve(NEW_BYTES - 1).is_ok());
    drop(values);
    drop(grant);
    drop(progress);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn untracked_fixture_growth_preserves_the_same_failure_contract() {
    for failure in [Failure::Allocation, Failure::Overgrant, Failure::Unwind] {
        let mut values = original();
        reject(&mut values, Memory::Grant(None), failure);
        reserve(
            &mut values,
            NEW_CAPACITY,
            Memory::Grant(None),
            "search spill slots",
        )
        .unwrap();
        assert_eq!(values, ORIGINAL);
        assert_eq!(values.capacity(), NEW_CAPACITY);
    }
}
