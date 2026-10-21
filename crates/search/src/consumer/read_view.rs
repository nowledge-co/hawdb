// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Shared version custody and the read exclusion around native selection.

use super::*;
use hawdb_qos::{RuntimePermit, RuntimeRetainedMemory};
use hawdb_storage::consumer_allocation::ConsumerSelectionReceipt;
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Identity {
    source_branch: Uuid,
    database_uuid: Uuid,
    consumer_id_hash: hawdb_integrity::Sha256Digest,
    target_locator_hash: hawdb_integrity::Sha256Digest,
}

impl From<ConsumerCandidateReservation> for Identity {
    fn from(value: ConsumerCandidateReservation) -> Self {
        Self {
            source_branch: value.source_branch,
            database_uuid: value.database_uuid,
            consumer_id_hash: value.consumer_id_hash,
            target_locator_hash: value.target_locator_hash,
        }
    }
}

#[derive(Debug, Clone)]
struct Version {
    reservation: ConsumerCandidateReservation,
    report: ConsumerCandidateViewReport,
    reader: crate::out_of_core::CandidateView,
}

impl Version {
    fn matches(&self, receipt: &ConsumerSelectionReceipt) -> bool {
        self.reservation == ConsumerCandidateReservation::capture(receipt.binding())
            && self.report.source_epoch == receipt.source_epoch()
            && self.report.metadata_reference == receipt.metadata_reference()
    }
}

#[derive(Debug)]
enum View {
    Ready(Option<Version>),
    Pending(Version),
}

#[derive(Debug)]
struct State {
    revision: u64,
    view: View,
}

#[derive(Debug)]
struct SharedSlot {
    identity: Identity,
    state: Mutex<State>,
    // Reader ownership closes before the shared allocation is refunded.
    memory: RuntimeRetainedMemory,
}

impl SharedSlot {
    fn lock(&self) -> Result<MutexGuard<'_, State>> {
        self.state.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => HawDBError::ProjectionBusy,
            TryLockError::Poisoned(_) => invalid("Consumer read slot is poisoned"),
        })
    }

    fn require_governor(&self, permit: &RuntimePermit) -> Result<()> {
        if !self.memory.shares_governor_with(permit) {
            return Err(HawDBError::Execution(
                "Consumer read slot permit belongs to another governor".into(),
            ));
        }
        Ok(())
    }
}

/// Runtime read version for one native Consumer. New pins fail closed while
/// selection is pending; existing pins keep their immutable native reader.
/// This internal kernel handle does not register a Consumer with the facade.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct ConsumerReadSlot(Arc<SharedSlot>);

/// One immutable version admitted before a subsequent publication. Reader
/// memory and native pins survive closure of the candidate or read slot.
#[doc(hidden)]
#[derive(Debug)]
pub struct ConsumerReadPin {
    version: Version,
    _memory: RuntimeRetainedMemory,
}

impl ConsumerReadSlot {
    pub(super) fn empty(
        reservation: ConsumerCandidateReservation,
        permit: &RuntimePermit,
    ) -> Result<Self> {
        Self::with_view(reservation, View::Ready(None), permit)
    }

    pub(super) fn restored(
        reader: crate::out_of_core::CandidateView,
        receipt: &ConsumerSelectionReceipt,
        permit: &RuntimePermit,
    ) -> Result<Self> {
        if !reader.shares_governor_with(permit) {
            return Err(invalid(
                "Consumer reopened reader belongs to another governor",
            ));
        }
        let reservation = ConsumerCandidateReservation::capture(receipt.binding());
        let version = Version {
            reservation,
            report: ConsumerCandidateViewReport {
                source_epoch: receipt.source_epoch(),
                generation: reader.generation(),
                document_count: reader.document_count(),
                metadata_reference: receipt.metadata_reference(),
            },
            reader,
        };
        Self::with_view(reservation, View::Ready(Some(version)), permit)
    }

    fn with_view(
        reservation: ConsumerCandidateReservation,
        view: View,
        permit: &RuntimePermit,
    ) -> Result<Self> {
        let bytes = std::mem::size_of::<SharedSlot>() + 2 * std::mem::size_of::<usize>();
        let memory = permit
            .reserve_retained_memory(bytes as u64)
            .map_err(|error| HawDBError::Execution(error.to_string()))?;
        Ok(Self(Arc::new(SharedSlot {
            identity: reservation.into(),
            state: Mutex::new(State { revision: 0, view }),
            memory,
        })))
    }

    pub fn report(&self) -> Result<ConsumerCandidateViewReport> {
        match &self.0.lock()?.view {
            View::Ready(Some(version)) => Ok(version.report),
            View::Ready(None) | View::Pending(_) => Err(HawDBError::ProjectionBusy),
        }
    }

    pub fn pin(&self, permit: &RuntimePermit) -> Result<ConsumerReadPin> {
        self.0.require_governor(permit)?;
        let memory = permit
            .reserve_retained_memory(std::mem::size_of::<ConsumerReadPin>() as u64)
            .map_err(|error| HawDBError::Execution(error.to_string()))?;
        let state = self.0.lock()?;
        let View::Ready(Some(version)) = &state.view else {
            return Err(HawDBError::ProjectionBusy);
        };
        Ok(ConsumerReadPin {
            version: version.clone(),
            _memory: memory,
        })
    }
}

impl ConsumerReadPin {
    pub fn report(&self) -> ConsumerCandidateViewReport {
        self.version.report
    }

    /// Query a completed registered version with fresh execution, result and
    /// I/O admission from its original host governor. The output owns its grant.
    pub fn search_with_options(
        &self,
        permit: &RuntimePermit,
        task: RuntimeTaskContext,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: crate::SearchMode,
        options: crate::SearchQueryOptions,
    ) -> crate::SearchQueryResult<crate::SearchRetainedOutput> {
        self.probe_query(permit, task, query_text, query_embedding, mode, options)
    }

    /// Validation probe with fresh same-governor working, result and I/O grants.
    /// `result_bytes` must explicitly cover the returned immutable owner, whose
    /// reservation survives query/permit/pin closure. Registered Consumer serving
    /// and complete working-pipeline qualification remain separate contracts.
    pub fn probe_query(
        &self,
        permit: &RuntimePermit,
        task: RuntimeTaskContext,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: crate::SearchMode,
        options: crate::SearchQueryOptions,
    ) -> crate::SearchQueryResult<crate::SearchRetainedOutput> {
        probe_view(
            &self.version.reader,
            permit,
            task,
            query_text,
            query_embedding,
            mode,
            options,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Prepared,
    Pending,
    Published,
    NotSelected,
}

/// All version metadata is built outside E and covered by the selector's
/// retained allocation. Entering E only checks a revision and moves metadata.
#[derive(Debug)]
pub(super) struct SlotPreparation {
    slot: ConsumerReadSlot,
    expected_revision: u64,
    pending_revision: u64,
    settled_revision: u64,
    previous: Option<Version>,
    next: Version,
    retired: Option<View>,
    phase: Phase,
}

impl SlotPreparation {
    pub(super) fn bind(
        candidate: &ConsumerPreparedCandidate,
        slot: &ConsumerReadSlot,
        permit: &RuntimePermit,
    ) -> Result<Self> {
        slot.0.require_governor(permit)?;
        if !candidate.detached.view.shares_governor_with(permit) {
            return Err(HawDBError::Execution(
                "Consumer read candidate belongs to another governor".into(),
            ));
        }
        if slot.0.identity != Identity::from(candidate.detached.reservation) {
            return Err(invalid("Consumer read slot belongs to another target"));
        }
        let selection = &candidate
            .selection
            .as_ref()
            .expect("prepared selector")
            .selection;
        let previous_receipt = selection.previous_receipt();
        let state = slot.0.lock()?;
        let View::Ready(previous) = &state.view else {
            return Err(HawDBError::ProjectionBusy);
        };
        match (previous, previous_receipt.as_ref()) {
            (None, None) => {}
            (Some(version), Some(receipt)) if version.matches(receipt) => {}
            _ => {
                return Err(invalid(
                    "Consumer read version differs from selected previous root",
                ))
            }
        }
        let settled_revision = state
            .revision
            .checked_add(2)
            .ok_or_else(|| invalid("Consumer read slot revision exhausted"))?;
        Ok(Self {
            slot: slot.clone(),
            expected_revision: state.revision,
            pending_revision: settled_revision - 1,
            settled_revision,
            previous: previous.clone(),
            next: Version {
                reservation: candidate.detached.reservation,
                report: candidate.report(),
                reader: candidate.detached.view.clone(),
            },
            retired: None,
            phase: Phase::Prepared,
        })
    }

    pub(super) fn enter_pending(&mut self) -> Result<()> {
        let mut state = self.slot.0.lock()?;
        if self.phase == Phase::Pending {
            return self.require_pending(&state);
        }
        if self.phase != Phase::Prepared {
            return Err(invalid("Consumer read publication is already settled"));
        }
        if state.revision != self.expected_revision || !matches!(state.view, View::Ready(_)) {
            return Err(HawDBError::ProjectionBusy);
        }
        self.retired = Some(std::mem::replace(
            &mut state.view,
            View::Pending(self.next.clone()),
        ));
        state.revision = self.pending_revision;
        self.phase = Phase::Pending;
        Ok(())
    }

    fn require_pending(&self, state: &State) -> Result<()> {
        if state.revision != self.pending_revision
            || !matches!(&state.view, View::Pending(version) if version.reservation == self.next.reservation)
        {
            return Err(invalid("Consumer read pending operation differs"));
        }
        Ok(())
    }

    pub(super) fn complete(&mut self, receipt: &ConsumerSelectionReceipt) -> Result<()> {
        if !self.next.matches(receipt) {
            return Err(invalid("Consumer read receipt differs from candidate"));
        }
        self.settle(true)
    }

    pub(super) fn restore_not_selected(&mut self) -> Result<()> {
        self.settle(false)
    }

    fn settle(&mut self, published: bool) -> Result<()> {
        let completed = if published {
            Phase::Published
        } else {
            Phase::NotSelected
        };
        if self.phase == completed {
            // Historical completion must never overwrite a later successor.
            return Ok(());
        }
        if self.phase == Phase::Prepared && !published {
            self.phase = completed;
            drop(self.previous.take());
            return Ok(());
        }
        if self.phase != Phase::Pending {
            return Err(invalid(
                "Consumer read outcome differs from settled operation",
            ));
        }
        let mut state = self.slot.0.lock()?;
        self.require_pending(&state)?;
        let View::Pending(next) = std::mem::replace(&mut state.view, View::Ready(None)) else {
            unreachable!("validated pending version");
        };
        let mut discarded = None;
        state.view = View::Ready(if published {
            // E already installed this exact prebuilt version. Completion
            // only opens admission; it cannot choose a later replacement.
            Some(next)
        } else {
            discarded = Some(next);
            self.previous.clone()
        });
        state.revision = self.settled_revision;
        self.phase = completed;
        drop(state);
        // Native retirement and allocation refunds occur after the source
        // fence and outside the slot mutex, including no-selection recovery.
        drop(self.retired.take());
        drop(self.previous.take());
        drop(discarded);
        Ok(())
    }
}
