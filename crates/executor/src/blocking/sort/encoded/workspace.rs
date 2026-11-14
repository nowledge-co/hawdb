// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_storage::file_descriptors::{DescriptorReservation, FileOpenContext};

pub(super) struct Buffers {
    pub buffer: NonZeroUsize,
    pub files: FileOpenContext,
    pub bytes: usize,
    _memory: Option<crate::QueryMemoryLease>,
}

pub(super) struct Workspace {
    buffers: Buffers,
    descriptors: DescriptorReservation,
}

impl std::ops::Deref for Workspace {
    type Target = Buffers;
    fn deref(&self) -> &Buffers {
        &self.buffers
    }
}

impl Workspace {
    fn metadata_bytes(readers: usize) -> usize {
        readers.saturating_mul(
            size_of::<Entry>()
                .saturating_add(size_of::<spill::SpillReader>())
                .saturating_add(size_of::<&spill::SpillRun>()),
        )
    }

    pub fn minimum_bytes(readers: usize) -> usize {
        Self::metadata_bytes(readers).saturating_add(readers.saturating_add(1))
    }

    pub fn into_parts(self) -> (Buffers, DescriptorReservation) {
        (self.buffers, self.descriptors)
    }

    pub fn new(
        spill: &SpillBudgetTracker,
        account: Option<&QueryMemoryAccount>,
        readers: usize,
        headroom: usize,
        writer: bool,
        limit: Option<usize>,
    ) -> Result<Self> {
        let streams = readers.saturating_add(usize::from(writer)).max(1);
        let metadata = Self::metadata_bytes(readers);
        // Leave room for output/parent batches; buffer size is a throughput
        // choice, while record and key admission remain independent hard limits.
        let available = account
            .map_or(usize::MAX, |account| {
                account
                    .available_bytes()
                    .min(account.query_available_bytes().saturating_sub(headroom))
            })
            .min(limit.unwrap_or(usize::MAX))
            .saturating_sub(metadata);
        // Planned groups already reserve their complete head set. Shared-account
        // pair/output merges leave room for incremental decoded/output batches.
        let share = if limit.is_some() { 1 } else { 4 };
        let buffer =
            NonZeroUsize::new((available / streams / share).clamp(1, spill::SPILL_IO_BUFFER_BYTES))
                .unwrap();
        let bytes = metadata.saturating_add(streams.saturating_mul(buffer.get()));
        let memory = account.map(|account| account.reserve(bytes)).transpose()?;
        let (descriptors, files) = spill.reserve_merge_descriptors(streams)?;
        Ok(Self {
            buffers: Buffers {
                buffer,
                files,
                bytes,
                _memory: memory,
            },
            descriptors,
        })
    }
}

pub(super) fn is_capacity_error(error: &HawDBError) -> bool {
    matches!(error,
        HawDBError::ResourceBudgetExceeded(cause) if cause.resource == hawdb_core::ResourceBudgetKind::MemoryBytes
    ) || matches!(
        error,
        HawDBError::FileDescriptors(hawdb_core::error::FileDescriptorError::BudgetExceeded { .. })
    )
}
