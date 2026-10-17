// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

// One word makes admission/revocation atomic: generation:47, open:1, refs:16.
// Refs include the operation token and every admitted mutation. A closed scope
// remains busy until its already-admitted work finishes. Generations never wrap.
const REFS: u64 = (1 << 16) - 1;
const OPEN: u64 = 1 << 16;
const STEP: u64 = 1 << 17;
const GENERATION: u64 = !(STEP - 1);

#[derive(Debug)]
pub(crate) struct Authority {
    _lease: SearchProjectionPublishLease,
    state: AtomicU64,
}

impl Authority {
    pub(super) fn new(lease: SearchProjectionPublishLease) -> Arc<Self> {
        Arc::new(Self {
            _lease: lease,
            state: AtomicU64::new(0),
        })
    }

    pub(super) fn acquire(self: &Arc<Self>) -> Result<ConsumerPreparationLease> {
        let previous = self
            .state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                if state & (REFS | OPEN) != 0 {
                    return None;
                }
                (state & GENERATION)
                    .checked_add(STEP)
                    .map(|generation| generation | 1)
            })
            .map_err(|state| {
                if state & REFS != 0 {
                    HawDBError::ProjectionBusy
                } else {
                    invalid("consumer operation generation exhausted")
                }
            })?;
        Ok(ConsumerPreparationLease {
            authority: Arc::clone(self),
            generation: (previous & GENERATION) + STEP,
        })
    }

    pub(super) fn require_available(&self) -> Result<()> {
        let state = self.state.load(Ordering::Acquire);
        if state & REFS != 0 {
            return Err(HawDBError::ProjectionBusy);
        }
        if state & OPEN != 0 || (state & GENERATION).checked_add(STEP).is_none() {
            return Err(invalid("consumer operation generation exhausted"));
        }
        Ok(())
    }

    pub(super) fn owns(self: &Arc<Self>, index: &SearchIndex) -> bool {
        index.consumer_owner.ptr_eq(&Arc::downgrade(self))
    }

    pub(super) fn require_index(self: &Arc<Self>, index: &SearchIndex) -> Result<()> {
        if !index.consumer_binding_valid
            || !self.owns(index)
            || index.consumer_binding.is_none()
            || index.path.is_none()
        {
            return Err(invalid(
                "consumer index no longer belongs to its native owner",
            ));
        }
        Ok(())
    }
}

/// Exclusive target custody, retaining the actual native lease after owner closure.
/// Does not certify C/E mapping, installation, incarnation or activation. No new
/// heap allocation or governor is created for an operation or admitted mutation.
#[derive(Debug)]
#[must_use = "retain preparation custody until work finishes or is cancelled"]
pub struct ConsumerPreparationLease {
    authority: Arc<Authority>,
    generation: u64,
}

impl ConsumerPreparationLease {
    pub(super) fn owns(&self, index: &SearchIndex) -> bool {
        self.authority.owns(index)
    }
    pub(super) fn generation(&self) -> u64 {
        self.generation
    }
    pub(super) fn open_mutations(&self) -> u64 {
        self.authority.state.fetch_or(OPEN, Ordering::Release);
        self.generation
    }
}

impl Drop for ConsumerPreparationLease {
    fn drop(&mut self) {
        self.authority
            .state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                if state & GENERATION != self.generation || state & REFS == 0 {
                    return None;
                }
                Some((state & !OPEN) - 1)
            })
            .expect("live consumer operation owns one reference");
    }
}

/// Internal, non-cloneable permission retaining the native FD and operation slot
/// across the entire admitted method, even if its callback ends on another thread.
#[derive(Debug)]
pub(crate) struct MutationPermit {
    authority: Arc<Authority>,
    generation: u64,
}

impl MutationPermit {
    pub(super) fn acquire(owner: &Weak<Authority>, generation: u64) -> Result<Self> {
        let authority = owner
            .upgrade()
            .ok_or_else(|| invalid("consumer mutation scope has ended"))?;
        authority
            .state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                if state & GENERATION != generation
                    || state & OPEN == 0
                    || state & REFS == 0
                    || state & REFS == REFS
                {
                    return None;
                }
                Some(state + 1)
            })
            .map_err(|_| invalid("consumer mutation scope ended or exhausted admission"))?;
        Ok(Self {
            authority,
            generation,
        })
    }
}

impl Drop for MutationPermit {
    fn drop(&mut self) {
        self.authority
            .state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                if state & GENERATION != self.generation || state & REFS == 0 {
                    return None;
                }
                Some(state - 1)
            })
            .expect("admitted consumer mutation owns one reference");
    }
}

impl ConsumerProjection {
    /// Kernel entry for the facade's registered-consumer preparation admission.
    /// A completed receipt must match the current clean resident checkpoint.
    #[doc(hidden)]
    pub fn reserve_preparation(&mut self) -> Result<ConsumerPreparationLease> {
        let operation = self.authority.acquire()?;
        self.authority.require_index(&self.index)?;
        let binding = self
            .index
            .consumer_binding
            .as_ref()
            .ok_or_else(|| invalid("preparation requires a bound consumer checkpoint"))?;
        let receipt = self
            .index
            .consumer_receipt
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let receipt = receipt
            .as_ref()
            .ok_or_else(|| invalid("preparation requires a completed consumer checkpoint"))?;
        let durable_epoch = *self
            .index
            .durable_source_graph_commit_epoch
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !self.index.consumer_binding_valid
            || self.index.path.is_none()
            || &receipt.binding != binding
            || self.pending_mutation
            || self.index.source_graph_commit_epoch != durable_epoch
            || self.index.source_graph_commit_epoch != Some(receipt.source_epoch)
        {
            return Err(invalid(
                "preparation requires the matching clean completed checkpoint",
            ));
        }
        Ok(operation)
    }

    /// Includes same-epoch edits and unsuccessful mutation/checkpoint attempts.
    #[doc(hidden)]
    pub fn has_pending_mutation(&self) -> bool {
        self.pending_mutation
    }

    /// Check before source hydration; actual mutation still acquires the slot.
    #[doc(hidden)]
    pub fn require_operation_available(&self) -> Result<()> {
        self.authority.require_available()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (std::path::PathBuf, ConsumerProjection) {
        let root = std::env::temp_dir().join(format!(
            "hawdb-mutation-custody-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        fs::create_dir(&root).unwrap();
        let uuid = hawdb_core::generate_uuidv7().unwrap();
        let binding = ConsumerBinding {
            database_uuid: uuid,
            projection_uuid: uuid,
            consumer_id: "main".into(),
            registration_uuid: uuid,
            checkpoint_uuid: uuid,
        };
        let owner = ConsumerProjection::initialize(&root, binding, 0, |_| Ok(())).unwrap();
        (root, owner)
    }

    #[test]
    fn admitted_work_retains_slot_and_native_lease_after_scope_and_owner_end() {
        let (root, mut owner) = fixture();
        let permit = owner
            .apply(|index| index.borrow_consumer_mutation())
            .unwrap();
        assert!(owner.index().borrow_consumer_mutation().is_err());
        assert!(matches!(
            owner.checkpoint(),
            Err(HawDBError::ProjectionBusy)
        ));
        assert!(matches!(
            owner.reserve_preparation(),
            Err(HawDBError::ProjectionBusy)
        ));
        assert!(matches!(
            owner.apply(|_| Ok(())),
            Err(HawDBError::ProjectionBusy)
        ));
        drop(owner);
        let error = ConsumerProjection::open(&root).unwrap_err();
        assert!(
            matches!(error, HawDBError::Storage(ref message)
            if message == "another search projection publication is active"),
            "{error:?}"
        );
        let deferred = SearchIndex {
            path: Some(root.clone()),
            ..SearchIndex::default()
        }
        .retry_projection_cleanup(Default::default());
        assert!(deferred.retry_required);
        assert_eq!(deferred.scanned_entries, 0);
        assert_eq!(deferred.delete_attempts, 0);
        drop(permit);
        drop(ConsumerProjection::open(&root).unwrap());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn old_generation_cannot_reenter_preparation_or_a_later_mutation() {
        let (root, owner) = fixture();
        let authority = &owner.authority;
        let weak = Arc::downgrade(authority);
        let first = authority.acquire().unwrap();
        let stale = first.open_mutations();
        drop(MutationPermit::acquire(&weak, stale).unwrap());
        drop(first);
        let next = authority.acquire().unwrap();
        assert!(MutationPermit::acquire(&weak, stale).is_err());
        let current = next.open_mutations();
        assert_ne!(stale, current);
        assert!(MutationPermit::acquire(&weak, stale).is_err());
        drop(MutationPermit::acquire(&weak, current).unwrap());
        drop(next);
        drop(owner);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn reference_and_generation_exhaustion_refuse_without_wrapping() {
        let (root, owner) = fixture();
        let authority = &owner.authority;
        let operation = authority.acquire().unwrap();
        let generation = operation.open_mutations();
        let actual = authority.state.load(Ordering::Acquire);
        // Simulate the exact boundary without allocating 65,534 native guards.
        authority
            .state
            .store(generation | OPEN | REFS, Ordering::Release);
        assert!(MutationPermit::acquire(&Arc::downgrade(authority), generation).is_err());
        assert_eq!(
            authority.state.load(Ordering::Acquire),
            generation | OPEN | REFS
        );
        authority.state.store(actual, Ordering::Release);
        drop(operation);
        authority.state.store(GENERATION, Ordering::Release);
        assert!(authority.acquire().is_err());
        assert!(authority.require_available().is_err());
        assert_eq!(authority.state.load(Ordering::Acquire), GENERATION);
        drop(owner);
        drop(ConsumerProjection::open(&root).unwrap());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn concurrent_admission_and_revocation_keep_admitted_work_busy() {
        let (root, owner) = fixture();
        for _ in 0..64 {
            let operation = owner.authority.acquire().unwrap();
            let generation = operation.open_mutations();
            let weak = Arc::downgrade(&owner.authority);
            let start = Arc::new(std::sync::Barrier::new(2));
            let worker_start = Arc::clone(&start);
            let worker = std::thread::spawn(move || {
                worker_start.wait();
                MutationPermit::acquire(&weak, generation)
            });
            start.wait();
            drop(operation);
            let result = worker.join().unwrap();
            assert!(
                MutationPermit::acquire(&Arc::downgrade(&owner.authority), generation).is_err()
            );
            if let Ok(permit) = result {
                assert!(matches!(
                    owner.authority.acquire(),
                    Err(HawDBError::ProjectionBusy)
                ));
                drop(permit);
            }
            drop(owner.authority.acquire().unwrap());
        }
        drop(owner);
        fs::remove_dir_all(root).unwrap();
    }
}
