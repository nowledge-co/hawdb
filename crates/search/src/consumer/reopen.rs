// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Complete native reopen remains separate from registered Consumer adoption.

use super::*;
use crate::build_memory::checked_mul;
use hawdb_storage::consumer_allocation::{ConsumerReopenRequest, CONSUMER_REOPEN_MEMORY_BOUND};
use hawdb_storage::file_io::SourceDirectory;
use std::mem::size_of;
use std::num::NonZeroU64;
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
pub struct ConsumerReopenLimits {
    pub metadata: generation_metadata::MetadataLimits,
    pub artifact_read_bytes: NonZeroU64,
    /// Complete encoded metadata, manifest and artifact closure, counted once.
    /// This is a logical verification bound, not a physical-I/O measurement.
    pub generation_bytes: NonZeroU64,
}

pub(super) struct ReopenedConsumer {
    pub slot: ConsumerReadSlot,
    pub publication: SearchProjectionPublishLease,
    pub selected: hawdb_storage::consumer_allocation::ConsumerSelectionReceipt,
    pub logical: Option<SearchProjectionPublishLease>,
}

impl ConsumerReadSlot {
    /// Reconstruct a completed native version outside SourceOwner. This kernel
    /// path verifies full native birth/metadata/artifact custody; it does not
    /// register a Consumer or acknowledge the facade's registry. Pending intents
    /// refuse Busy until their original operation is resolved separately.
    pub fn reopen(
        request: ConsumerReopenRequest,
        original_parent: SourceDirectory,
        permit: Arc<hawdb_qos::RuntimePermit>,
        task: RuntimeTaskContext,
        limits: ConsumerReopenLimits,
    ) -> Result<Self> {
        Self::reopen_retained(request, original_parent, permit, task, limits, None)
            .map(|reopened| reopened.slot)
    }

    pub(super) fn reopen_retained(
        request: ConsumerReopenRequest,
        original_parent: SourceDirectory,
        permit: Arc<hawdb_qos::RuntimePermit>,
        task: RuntimeTaskContext,
        limits: ConsumerReopenLimits,
        mut logical: Option<SearchProjectionPublishLease>,
    ) -> Result<ReopenedConsumer> {
        let budget = permit.request().memory_bytes;
        if budget < CONSUMER_REOPEN_MEMORY_BOUND {
            return Err(hawdb_core::ResourceBudgetError {
                resource: hawdb_core::ResourceBudgetKind::MemoryBytes,
                scope: hawdb_core::ResourceBudgetScope::Query,
                owner: "Consumer reopen control".into(),
                memory_class: None,
                requested: CONSUMER_REOPEN_MEMORY_BOUND,
                used: 0,
                budget,
                budget_option: "RuntimePermit.request.memory_bytes",
                message: "Consumer reopen control exceeds working admission".into(),
            }
            .into());
        }
        if task.has_resource_controllers() {
            return Err(invalid(
                "Consumer reopen task already has resource controllers",
            ));
        }
        let task = permit.bind_task_context(task);
        let mut memory = BuildMemory::new(&task)?;
        memory.host_admission = Some(Arc::clone(&permit));
        // This is the original operation's account, before any Storage decode
        // or native target open. Payloads close before this envelope is refunded.
        let _control_memory = memory
            .spool
            .reserve(CONSUMER_REOPEN_MEMORY_BOUND as usize)?;
        let io = task
            .acquire_io_wave(std::num::NonZeroUsize::MIN)
            .map_err(|error| HawDBError::Execution(error.to_string()))?
            .ok_or_else(|| invalid("Consumer reopen requires I/O admission"))?;
        let native = request.open(original_parent, &task)?;
        let directory = native.directory(&task)?;
        let mut publication = if logical.as_ref().is_some_and(|lease| {
            lease.with_captured_directory(|original| original.same_directory(directory))
        }) {
            logical.take().expect("same original publication")
        } else {
            SearchProjectionPublishLease::acquire_captured(
                directory.duplicate(&directory.operation_context())?,
                &memory,
                &task,
            )?
        };
        let manifest = {
            let reference = native.receipt().metadata_reference();
            let length = usize::try_from(reference.byte_length)
                .map_err(|_| invalid("Consumer metadata length overflow"))?;
            if length > limits.metadata.encoded_bytes {
                return Err(invalid("Consumer reopen metadata exceeds byte limit"));
            }
            let _bytes_memory = memory.spool.reserve(length)?;
            let mut bytes = Vec::new();
            bytes.try_reserve_exact(length).map_err(|error| {
                HawDBError::Execution(format!(
                    "Consumer reopen metadata allocation failed: {error}"
                ))
            })?;
            if bytes.capacity() > length {
                return Err(invalid("Consumer reopen metadata exceeded admission"));
            }
            let mut scratch = [0u8; crate::build_memory::SPOOL_BUFFER_BYTES];
            verify_candidate_child(
                directory,
                &directory.operation_context(),
                GENERATION_METADATA_FILE,
                reference,
                &task,
                &mut scratch,
                Some(&mut bytes),
            )?;
            let metadata =
                generation_metadata::MetadataView::decode(&bytes, limits.metadata, &task)?;
            let binding = native.receipt().binding();
            let expected = ConsumerBinding {
                database_uuid: binding.database_uuid,
                projection_uuid: binding.projection_uuid,
                consumer_id: binding.consumer_id.to_owned(),
                registration_uuid: binding.registration_uuid,
                checkpoint_uuid: binding.checkpoint_uuid,
            };
            if !metadata.matches_binding(&expected)
                || metadata.target_incarnation() != binding.target_incarnation
                || metadata.generation() != binding.generation
                || metadata.operation() != binding.operation
                || metadata.source_epoch() != native.receipt().source_epoch()
            {
                return Err(integrity(
                    "Consumer reopened metadata differs from selected receipt",
                ));
            }
            let manifest = metadata.search_manifest();
            let total = metadata.artifacts().try_fold(
                reference
                    .byte_length
                    .checked_add(manifest.byte_length)
                    .ok_or_else(|| invalid("Consumer reopen closure byte overflow"))?,
                |bytes, artifact| {
                    if artifact.reference.byte_length > limits.artifact_read_bytes.get() {
                        return Err(invalid("Consumer reopened artifact exceeds read limit"));
                    }
                    bytes
                        .checked_add(artifact.reference.byte_length)
                        .ok_or_else(|| invalid("Consumer reopen closure byte overflow"))
                },
            )?;
            if total > limits.generation_bytes.get()
                || manifest.byte_length > limits.artifact_read_bytes.get()
            {
                return Err(invalid(
                    "Consumer reopened generation exceeds verification limit",
                ));
            }
            let _artifacts_memory = memory.spool.reserve(checked_mul(
                metadata.artifact_count(),
                size_of::<generation_metadata::ArtifactReference<'_>>(),
            )?)?;
            let mut artifacts = Vec::with_capacity(metadata.artifact_count());
            if artifacts.capacity() > metadata.artifact_count() {
                return Err(invalid("Consumer artifact index exceeds admission"));
            }
            artifacts.extend(metadata.artifacts());
            verify_generation_contents(
                directory,
                manifest,
                metadata.source_epoch(),
                &artifacts,
                &memory,
                &task,
                &mut scratch,
            )?;
            manifest
        };
        // Reader construction uses independently scoped waves. Never wait for a
        // second slot while retaining this operation's sole I/O slot.
        drop(io);
        let view = crate::out_of_core::open_stationary_candidate(
            crate::out_of_core::StationaryCandidateOpenContext {
                stationary: crate::out_of_core::StationaryRoot::Reopened(&native),
                publication: &mut publication,
                memory: &memory,
                task: &task,
                permit: &permit,
                expected_manifest: manifest,
                config: crate::SearchOutOfCoreConfig::default(),
                max_artifact_read_bytes: limits.artifact_read_bytes,
            },
        )?;
        if view.source_epoch() != Some(native.receipt().source_epoch()) {
            return Err(integrity("Consumer reopened reader source epoch differs"));
        }
        let io = task
            .acquire_io_wave(std::num::NonZeroUsize::MIN)
            .map_err(|error| HawDBError::Execution(error.to_string()))?
            .ok_or_else(|| invalid("Consumer reopen requires final I/O admission"))?;
        native.require_current(&task)?;
        let slot = Self::restored(view, native.receipt(), &permit)?;
        crate::build_control::checkpoint(&task)?;
        drop(io);
        Ok(ReopenedConsumer {
            slot,
            publication,
            selected: native.receipt().clone(),
            logical,
        })
    }
}

pub(super) fn verify_generation_contents(
    target: &SourceDirectory,
    manifest_reference: ObjectReference,
    source_epoch: u64,
    artifacts: &[generation_metadata::ArtifactReference<'_>],
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
    scratch: &mut [u8; crate::build_memory::SPOOL_BUFFER_BYTES],
) -> Result<()> {
    if manifest_reference.byte_length > crate::out_of_core::MAX_OUT_OF_CORE_MANIFEST_BYTES {
        return Err(invalid("Consumer Search manifest exceeds read limit"));
    }
    let length = usize::try_from(manifest_reference.byte_length)
        .map_err(|_| invalid("Consumer manifest length overflow"))?;
    let _manifest_memory = memory.spool.reserve(length)?;
    let mut manifest = Vec::new();
    manifest.try_reserve_exact(length).map_err(|error| {
        HawDBError::Execution(format!("Consumer manifest allocation failed: {error}"))
    })?;
    if manifest.capacity() > length {
        return Err(invalid("Consumer manifest exceeds admission"));
    }
    let context = target.operation_context();
    verify_candidate_child(
        target,
        &context,
        crate::out_of_core::OUT_OF_CORE_MANIFEST_FILE,
        manifest_reference,
        task,
        scratch,
        Some(&mut manifest),
    )?;
    let _decode_memory =
        memory
            .spool
            .reserve(crate::out_of_core::consumer_manifest_decode_capacity(
                &manifest, task,
            )?)?;
    let absent = crate::out_of_core::validate_consumer_manifest_artifacts(
        &manifest,
        source_epoch,
        artifacts,
        task,
    )?;
    if absent {
        match target.open_child(&context, crate::SEARCH_SNAPSHOT_FILE) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
            Ok(_) => {
                return Err(integrity(
                    "Consumer resident snapshot violates manifest absence proof",
                ))
            }
        }
    }
    for artifact in artifacts {
        verify_candidate_child(
            target,
            &context,
            artifact.name,
            artifact.reference,
            task,
            scratch,
            None,
        )?;
    }
    Ok(())
}
