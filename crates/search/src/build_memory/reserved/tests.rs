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
use crate::build_memory::BuildMemory;
use crate::RuntimeTaskContext;
use hawdb_core::RuntimeMemoryReservation;

fn memory(bytes: usize) -> BuildMemory {
    BuildMemory::new(
        &RuntimeTaskContext::default()
            .with_memory_reservation(RuntimeMemoryReservation::new(bytes as u64, 0)),
    )
    .unwrap()
}

#[test]
fn reservation_and_grants_obey_exact_and_one_short_admission() {
    let capacity = 8192;
    let bytes = capacity + ReservedMemory::metadata_bytes();
    let limited = memory(bytes - 1);
    assert!(ReservedMemory::new(&limited.spool, capacity).is_err());
    assert_eq!(limited.ledger.snapshot().used_bytes, 0);
    let memory = memory(bytes);
    let reservation = ReservedMemory::new(&memory.spool, capacity).unwrap();
    let mut first = reservation.reserve(capacity - 1).unwrap();
    assert!(reservation.reserve(2).is_err());
    assert!(first.grow(2).is_err());
    first.grow(1).unwrap();
    assert_eq!(first.bytes(), capacity);
    assert!(reservation.ensure_capacity(capacity + 1).is_err());
    assert_eq!(reservation.state().capacity, capacity);
    assert_eq!(reservation.state().used, capacity);
    first.shrink(1);
    let last = reservation.reserve(1).unwrap();
    assert_eq!(memory.ledger.snapshot().used_bytes, bytes);
    drop((reservation, first));
    assert_eq!(memory.ledger.snapshot().used_bytes, bytes);
    drop(last);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn reserved_progress_survives_a_full_root_and_grows_only_after_admission() {
    let memory = memory(64 * 1024);
    let reservation = ReservedMemory::new(&memory.spool, 8192).unwrap();
    let snapshot = memory.ledger.snapshot();
    let mut input = memory
        .input
        .reserve(snapshot.budget_bytes - snapshot.used_bytes)
        .unwrap();
    assert!(memory.retained.reserve(1).is_err());
    let mut grant = reservation.reserve(4096).unwrap();
    grant.grow(4096).unwrap();
    assert!(reservation.ensure_capacity(16384).is_err());
    assert_eq!(reservation.state().used, 8192);
    input.shrink(8192);
    reservation.ensure_capacity(16384).unwrap();
    grant.grow(8192).unwrap();
    assert_eq!(memory.ledger.snapshot().used_bytes, snapshot.budget_bytes);
    drop((grant, reservation, input));
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn concurrent_grants_and_unwind_return_capacity_to_the_reservation() {
    let memory = memory(64 * 1024);
    let reservation = ReservedMemory::new(&memory.spool, 8192).unwrap();
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let reservation = &reservation;
            scope.spawn(move || {
                for _ in 0..1024 {
                    let mut grant = reservation.reserve(1024).unwrap();
                    grant.grow(1024).unwrap();
                    grant.shrink(512);
                }
            });
        }
    });
    assert_eq!(reservation.state().used, 0);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _grant = reservation.reserve(8192).unwrap();
        panic!("spill consumer unwound");
    }))
    .is_err());
    assert_eq!(reservation.state().used, 0);
    assert_eq!(memory.ledger.snapshot().account_count, 3);
    drop(reservation);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn exclusive_scratch_cannot_be_granted_or_overlap_across_workers() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let memory = memory(32 * 1024);
    let reservation = ReservedMemory::with_scratch_capacity(&memory.spool, 4096, 8192).unwrap();
    let grant = reservation.reserve(4096).unwrap();
    assert!(reservation.reserve(1).is_err());
    let mut entered = false;
    assert!(reservation
        .with_scratch(8193, || {
            entered = true;
            Ok(())
        })
        .is_err());
    assert!(!entered);
    let active = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let (reservation, active) = (&reservation, &active);
            scope.spawn(move || {
                for _ in 0..128 {
                    reservation
                        .with_scratch(8192, || {
                            assert_eq!(active.fetch_add(1, Ordering::SeqCst), 0);
                            std::thread::yield_now();
                            assert_eq!(active.fetch_sub(1, Ordering::SeqCst), 1);
                            Ok(())
                        })
                        .unwrap();
                }
            });
        }
    });
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = reservation.with_scratch(8192, || -> Result<()> {
            panic!("native I/O unwound");
        });
    }))
    .is_err());
    drop(reservation);
    grant.with_scratch(8192, || Ok(())).unwrap();
    assert_eq!(
        memory.ledger.snapshot().used_bytes,
        4096 + 8192 + ReservedMemory::metadata_bytes()
    );
    drop(grant);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}
