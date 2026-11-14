// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_qos::RuntimePermit;
use hawdb_storage::consumer_allocation::{ConsumerCleanupRequest, CONSUMER_CLEANUP_MEMORY_BOUND};
use hawdb_storage::file_io::SourceDirectory;
use std::sync::Arc;

pub use hawdb_storage::consumer_allocation::{ConsumerCleanupOptions, ConsumerCleanupReport};

/// Synchronous maintenance with fresh admission and no retained work resources.
/// The ledger and parent-side intent remain the retry evidence on any failure.
#[doc(hidden)]
pub fn reclaim_generations(
    request: ConsumerCleanupRequest,
    original_parent: Option<&SourceDirectory>,
    options: &ConsumerCleanupOptions,
    permit: Arc<RuntimePermit>,
    task: RuntimeTaskContext,
) -> Result<ConsumerCleanupReport> {
    if task.has_resource_controllers() {
        return Err(invalid("Consumer cleanup requires an unbound task"));
    }
    let task = permit.bind_task_context(task);
    let memory = BuildMemory::new(&task)?;
    let _working = memory
        .spool
        .reserve(CONSUMER_CLEANUP_MEMORY_BOUND as usize)?;
    let _io = task
        .acquire_io_wave(std::num::NonZeroUsize::MIN)
        .map_err(|e| HawDBError::Execution(e.to_string()))?
        .ok_or_else(|| invalid("Consumer cleanup requires I/O admission"))?;
    let captured;
    let parent = match original_parent {
        Some(parent) => parent,
        None => {
            captured = request.capture_parent()?;
            &captured
        }
    };
    request.run(
        parent,
        options,
        &task,
        |directory| {
            let lease = SearchProjectionPublishLease::acquire_captured(
                directory.duplicate(&directory.operation_context())?,
                &memory,
                &task,
            )?;
            if lease.has_readers() {
                return Err(HawDBError::ProjectionBusy);
            }
            directory.retire_cached_children();
            Ok(lease)
        },
        known_file,
    )
}

fn known_file(name: &str) -> bool {
    matches!(
        name,
        GENERATION_METADATA_FILE
            | SEARCH_SNAPSHOT_FILE
            | crate::SEARCH_SEGMENT_DESCRIPTOR_FILE
            | crate::SEARCH_SEGMENT_PAYLOAD_FILE
            | crate::out_of_core::OUT_OF_CORE_MANIFEST_FILE
            | crate::lexical_projection::MANIFEST_FILE
    ) || crate::out_of_core::artifact_key::ArtifactKey::from_file_name(name).is_some()
}

impl ConsumerProjection {
    pub fn reclaim_registered_generations(
        &self,
        request: ConsumerCleanupRequest,
        options: &ConsumerCleanupOptions,
        permit: Arc<RuntimePermit>,
        task: RuntimeTaskContext,
    ) -> Result<ConsumerCleanupReport> {
        let native = self
            .stationary
            .as_ref()
            .ok_or_else(|| invalid("Consumer is not stationary"))?;
        reclaim_generations(request, Some(&native.parent), options, permit, task)
    }
}
