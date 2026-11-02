// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Park verified native records without retaining construction execution.

use super::*;
use hawdb_core::{ResourceBudgetError, ResourceBudgetKind, ResourceBudgetScope};
use hawdb_qos::{RuntimeGovernor, RuntimePermit, RuntimeRetainedMemory, RuntimeWorkRequest};
use std::sync::Arc;

/// Exact compared content, original custody and retained memory only.
#[doc(hidden)]
pub struct SearchComparedConsumerDocuments {
    run: Option<Run>,
    control: Control,
    writer: SearchOutOfCoreGenerationWriter,
    documents: usize,
    encoded_bytes: u64,
    governor: RuntimeGovernor,
    _metadata: QueryMemoryLease,
    _retained: RuntimeRetainedMemory,
    // On active publication or best-effort Drop cleanup, close execution only
    // after the files and their captured cleanup owner have closed.
    execution: Option<Arc<RuntimePermit>>,
}

impl SearchComparedConsumerDocuments {
    pub(super) fn new(
        run: Option<Run>,
        mut control: Control,
        mut writer: SearchOutOfCoreGenerationWriter,
        documents: usize,
        encoded_bytes: u64,
    ) -> Result<Self> {
        checkpoint(&writer.task_context)?;
        let permit = writer.memory.host_admission.clone().ok_or_else(|| {
            HawDBError::Execution("compared Consumer records require originating admission".into())
        })?;
        let metadata = writer
            .memory
            .retained
            .reserve(std::mem::size_of::<Self>())?;
        let bytes = (writer.memory.ledger.snapshot().used_bytes as u64)
            .checked_add(crate::build_memory::ACCOUNT_METADATA_BYTES as u64)
            .ok_or_else(|| memory_limit(u64::MAX, permit.request().memory_bytes))?;
        let retained = retain(&permit, bytes)?;
        // A buffered producer may flush when dropped. Close it while execution
        // is still admitted; only immutable held runs survive the handoff.
        if let Some(mut spool) = writer.spool.take() {
            let _io = crate::out_of_core::scratch_directory::io_scope(&writer.task_context, true)?;
            let flushed = spool.flush();
            drop(spool);
            flushed?;
        }
        writer.held_spool.take();
        let task = RuntimeTaskContext::new(
            writer.task_context.cancellation().clone(),
            writer.task_context.deadline(),
        );
        if let Some(run) = &run {
            run.rebind_task(&task)?;
        }
        writer.stage.rebind_task(&task)?;
        writer.task_context = task.clone();
        writer.memory.host_admission.take();
        control.rebind_execution(&task, None);
        Ok(Self {
            run,
            control,
            writer,
            documents,
            encoded_bytes,
            governor: permit.governor(),
            _metadata: metadata,
            _retained: retained,
            execution: None,
        })
    }

    /// Re-admit read-back and all deterministic resident/reader prerequisites
    /// before the database allocates a reservation or creates a born directory.
    pub fn admit_publication(
        mut self,
        permit: Arc<RuntimePermit>,
        task: RuntimeTaskContext,
        limits: crate::consumer::RegisteredConsumerLimits,
    ) -> Result<SearchAdmittedConsumerDocuments> {
        if task.has_resource_controllers() || !self._retained.shares_governor_with(&permit) {
            return Err(HawDBError::Execution(
                "Consumer read-back requires fresh originating-governor admission".into(),
            ));
        }
        let task = permit.bind_task_context(task);
        self.execution = Some(Arc::clone(&permit));
        self.writer.stage.rebind_task(&task)?;
        self.writer.task_context = task.clone();
        self.writer.memory.host_admission = Some(Arc::clone(&permit));
        self.control
            .rebind_execution(&task, Some(Arc::clone(&permit)));
        if let Some(run) = &self.run {
            run.rebind_task(&task)?;
        }
        checkpoint(&task)?;
        let resident_bytes = self
            .encoded_bytes
            .checked_mul(32)
            .and_then(|n| n.checked_add(64 * 1024))
            .ok_or_else(|| memory_limit(u64::MAX, limits.resident_bytes.get()))?;
        if resident_bytes > limits.resident_bytes.get() {
            return Err(memory_limit(resident_bytes, limits.resident_bytes.get()));
        }
        let resident = retain(&permit, resident_bytes)?;
        let source = self
            .run
            .take()
            .map(|run| Reader::open(run, &mut self.control, &self.writer))
            .transpose()?;
        Ok(SearchAdmittedConsumerDocuments {
            source,
            documents: self,
            _resident: resident,
        })
    }
}

impl Drop for SearchComparedConsumerDocuments {
    fn drop(&mut self) {
        if self.execution.is_some() {
            return;
        }
        // Idle ownership admits no work. Dropping it may obtain one fresh,
        // bounded cleanup operation; refusal preserves the existing funded
        // native cleanup ticket rather than bypassing the governor.
        if let Ok(permit) = self.governor.try_admit(
            RuntimeWorkRequest::background_maintenance(self.writer.stage.cleanup_working_bytes())
                .with_io_wave_slots(1),
        ) {
            let permit = Arc::new(permit);
            let task = permit.bind_task_context(RuntimeTaskContext::default());
            if self.writer.stage.rebind_task(&task).is_ok() {
                self.execution = Some(permit);
            }
        }
    }
}

/// Active read-back prerequisites; cannot be produced by incomplete coverage.
#[doc(hidden)]
pub struct SearchAdmittedConsumerDocuments {
    // Close the held reader before its stage on Windows.
    source: Option<Reader>,
    documents: SearchComparedConsumerDocuments,
    _resident: RuntimeRetainedMemory,
}

impl SearchAdmittedConsumerDocuments {
    pub fn prepare_registered(
        mut self,
        birth: hawdb_storage::consumer_allocation::PendingConsumerGeneration,
        preparation: crate::consumer::RegisteredConsumerPreparation<'_>,
    ) -> std::result::Result<
        crate::consumer::PreparedRegisteredConsumer,
        Box<crate::consumer::RegisteredConsumerError>,
    > {
        // Every fallible initializer prerequisite now lives either before birth
        // or inside the native initializer's cleanup/retirement boundary.
        crate::consumer::ConsumerProjection::prepare_registered(birth, preparation, |index| {
            let mut count = 0usize;
            if let Some(source) = self.source.as_mut() {
                while let Some(entry) = source.next(&mut self.documents.control)? {
                    let run::Entry::Upsert(record, _) = entry else {
                        return Err(unusable());
                    };
                    self.documents.control.read(record.encoded_bytes)?;
                    let admitted = record
                        .into_document(self.documents.writer.options.max_metadata_fields.get())?;
                    index.upsert(admitted.document)?;
                    count = count.checked_add(1).ok_or_else(unusable)?;
                }
            }
            if count != self.documents.documents || index.document_count() != count {
                return Err(unusable());
            }
            checkpoint(&self.documents.writer.task_context)
        })
    }
}

fn memory_limit(requested: u64, budget: u64) -> HawDBError {
    ResourceBudgetError {
        resource: ResourceBudgetKind::MemoryBytes,
        scope: ResourceBudgetScope::Operator,
        owner: "compared Consumer resident documents".into(),
        memory_class: None,
        requested,
        used: 0,
        budget,
        budget_option: "RegisteredConsumerLimits.resident_bytes",
        message: "compared Consumer documents exceed resident admission".into(),
    }
    .into()
}

fn retain(permit: &RuntimePermit, bytes: u64) -> Result<RuntimeRetainedMemory> {
    permit.reserve_retained_memory(bytes).map_err(|error| {
        ResourceBudgetError {
            resource: ResourceBudgetKind::MemoryBytes,
            scope: ResourceBudgetScope::SharedPool,
            owner: "compared Consumer retained memory".into(),
            memory_class: None,
            requested: error.requested,
            used: 0,
            // The observed available pool headroom is this reservation's effective
            // boundary, not an invented total governor memory capacity.
            budget: error.available,
            budget_option: "RuntimeGovernor.available_retained_memory_bytes",
            message: error.to_string(),
        }
        .into()
    })
}
