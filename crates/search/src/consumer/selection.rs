// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Fresh selector admission and coordinated durable/read-view completion.

use super::*;

/// Native selection and its prebuilt read-version exchange share one pending
/// result. Finish outside SourceOwner; a lost response keeps new pins blocked
/// until the original operation is resolved with fresh admission.
#[doc(hidden)]
#[derive(Debug)]
#[must_use]
pub struct PendingConsumerPublication<'a> {
    selection: hawdb_storage::consumer_allocation::PendingConsumerSelection<'a>,
    read: &'a mut read_view::SlotPreparation,
}

impl PendingConsumerPublication<'_> {
    pub fn finish(self) -> Result<hawdb_storage::consumer_allocation::ConsumerSelectionReceipt> {
        let receipt = self.selection.finish()?;
        self.read.complete(&receipt)?;
        Ok(receipt)
    }
}

/// Fresh execution and I/O admission for a prepared selector. Acquire before
/// entering the source fence; the borrow retains the born target and reader.
#[doc(hidden)]
#[derive(Debug)]
pub struct ConsumerSelectionAttempt<'a> {
    candidate: &'a mut ConsumerPreparedCandidate,
    task: RuntimeTaskContext,
    _io: Box<dyn hawdb_core::RuntimeIoWavePermit>,
    _permit: std::sync::Arc<hawdb_qos::RuntimePermit>,
    _descriptors: hawdb_storage::file_descriptors::DescriptorReservation,
}

impl ConsumerPreparedCandidate {
    /// Precompute and synchronize the selector outside SourceOwner. Metadata
    /// remains funded after this preparation releases its execution permit.
    pub fn prepare_selection(
        &mut self,
        permit: &hawdb_qos::RuntimePermit,
        task: RuntimeTaskContext,
    ) -> Result<()> {
        if self.selection.is_some() {
            return Err(invalid("consumer selector is already prepared"));
        }
        let task = self.selection_task(permit, task)?;
        let _io = task
            .acquire_io_wave(std::num::NonZeroUsize::MIN)
            .map_err(|error| HawDBError::Execution(error.to_string()))?
            .ok_or_else(|| {
                HawDBError::Execution("consumer selection requires I/O admission".into())
            })?;
        let memory = permit
            .reserve_retained_memory(
                hawdb_storage::consumer_allocation::CONSUMER_SELECTION_MEMORY_BOUND,
            )
            .map_err(|error| HawDBError::Execution(error.to_string()))?;
        let selection = self.birth.prepare_selection()?;
        self.selection = Some(ConsumerCandidateSelection {
            selection,
            read: None,
            _memory: memory,
        });
        Ok(())
    }

    /// Create an empty runtime slot in this candidate's original governor.
    /// Existing selected Consumers must reuse their current slot instead.
    pub fn create_read_slot(&self, permit: &hawdb_qos::RuntimePermit) -> Result<ConsumerReadSlot> {
        if !self.detached.view.shares_governor_with(permit) {
            return Err(HawDBError::Execution(
                "Consumer read slot permit belongs to another governor".into(),
            ));
        }
        ConsumerReadSlot::empty(self.detached.reservation, permit)
    }

    /// Bind the exact previous runtime version before entering the source
    /// fence. A receipt alone cannot replace a missing previous reader.
    pub fn bind_read_slot(
        &mut self,
        slot: &ConsumerReadSlot,
        permit: &hawdb_qos::RuntimePermit,
    ) -> Result<()> {
        let selection = self
            .selection
            .as_ref()
            .ok_or_else(|| invalid("consumer selector is not prepared"))?;
        if selection.read.is_some() {
            return Err(invalid("Consumer selector already has a read slot"));
        }
        let read = read_view::SlotPreparation::bind(self, slot, permit)?;
        self.selection.as_mut().expect("prepared selector").read = Some(read);
        Ok(())
    }

    pub fn admit_selection(
        &mut self,
        permit: std::sync::Arc<hawdb_qos::RuntimePermit>,
        task: RuntimeTaskContext,
    ) -> Result<ConsumerSelectionAttempt<'_>> {
        if self
            .selection
            .as_ref()
            .is_none_or(|selection| selection.read.is_none())
        {
            return Err(invalid("consumer selector requires a prepared read slot"));
        }
        let task = self.selection_task(&permit, task)?;
        let descriptors = self
            .selection
            .as_ref()
            .expect("prepared selector")
            .selection
            .reserve_attempt_descriptors()?;
        let io = task
            .acquire_io_wave(std::num::NonZeroUsize::MIN)
            .map_err(|error| HawDBError::Execution(error.to_string()))?
            .ok_or_else(|| {
                HawDBError::Execution("consumer selection requires I/O admission".into())
            })?;
        Ok(ConsumerSelectionAttempt {
            candidate: self,
            task,
            _io: io,
            _permit: permit,
            _descriptors: descriptors,
        })
    }

    fn selection_task(
        &self,
        permit: &hawdb_qos::RuntimePermit,
        task: RuntimeTaskContext,
    ) -> Result<RuntimeTaskContext> {
        if !self.detached.view.shares_governor_with(permit) {
            return Err(HawDBError::Execution(
                "consumer selection permit belongs to another governor".into(),
            ));
        }
        if task.has_resource_controllers() {
            return Err(invalid(
                "consumer selection task already has a resource controller",
            ));
        }
        Ok(permit.bind_task_context(task))
    }
}

impl ConsumerSelectionAttempt<'_> {
    pub fn candidate(&self) -> &ConsumerPreparedCandidate {
        self.candidate
    }

    /// Called only after the source-E check while the host retains SourceOwner.
    pub fn select(&mut self) -> Result<PendingConsumerPublication<'_>> {
        crate::build_control::checkpoint(&self.task)?;
        let candidate = self
            .candidate
            .selection
            .as_mut()
            .expect("admitted prepared selector");
        let read = candidate.read.as_mut().expect("admitted read slot");
        read.enter_pending()?;
        let selection = candidate.selection.select()?;
        Ok(PendingConsumerPublication { selection, read })
    }

    /// The registered facade explicitly abandons a selector that did not enter
    /// native selection, or resolves the same attempted operation otherwise.
    pub fn resolve_or_cancel_unattempted(
        &mut self,
    ) -> Result<hawdb_storage::consumer_allocation::ConsumerSelectionResolution> {
        if !self
            .candidate
            .selection
            .as_ref()
            .expect("admitted selector")
            .selection
            .native_selection_attempted()
        {
            self.cancel_unattempted()?;
            Ok(hawdb_storage::consumer_allocation::ConsumerSelectionResolution::NotSelected)
        } else {
            self.resolve()
        }
    }

    /// Resolve with fresh admission after a failed or lost selection response.
    /// Resolution uses the original operation rather than a later source epoch.
    pub fn resolve(
        &mut self,
    ) -> Result<hawdb_storage::consumer_allocation::ConsumerSelectionResolution> {
        crate::build_control::checkpoint(&self.task)?;
        let candidate = self
            .candidate
            .selection
            .as_mut()
            .expect("admitted prepared selector");
        let outcome = candidate.selection.resolve()?;
        let read = candidate.read.as_mut().expect("admitted read slot");
        match &outcome {
            hawdb_storage::consumer_allocation::ConsumerSelectionResolution::Published(receipt) => {
                read.complete(receipt)?;
            }
            hawdb_storage::consumer_allocation::ConsumerSelectionResolution::NotSelected => {
                read.restore_not_selected()?;
            }
        }
        Ok(outcome)
    }

    /// Settle a selector that never entered native selection, for example
    /// after source validation refused it. An attempted selector must resolve.
    pub fn cancel_unattempted(&mut self) -> Result<()> {
        crate::build_control::checkpoint(&self.task)?;
        let candidate = self
            .candidate
            .selection
            .as_mut()
            .expect("admitted prepared selector");
        candidate.selection.cancel_unattempted()?;
        candidate
            .read
            .as_mut()
            .expect("admitted read slot")
            .restore_not_selected()
    }
}
