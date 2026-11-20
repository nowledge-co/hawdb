// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Bounded ID-only projection pages used by embedded host reconciliation.

use crate::SearchProjectionKind;
use hawdb_qos::RuntimeRetainedMemory;

/// Identity of the immutable Search projection read for one ID page.
///
/// Callers must restart pagination when this value differs between pages.
/// The token identifies an in-process publication; it is not durable evidence
/// and must not be persisted across database opens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchProjectionPageGeneration {
    Resident(u64),
    OutOfCore { reader: u64, generation: u64 },
}

/// One ordered, kind-filtered page of Search projection document IDs.
///
/// The page retains a runtime charge for the UTF-8 bytes owned by `ids` and
/// `next`. It owns no projection-generation lease, file handle, or payload.
#[derive(Debug)]
pub struct SearchProjectionIdPage {
    ids: Vec<String>,
    /// Exclusive cursor for the next page; `None` proves exhaustion for this
    /// generation and kind set.
    next: Option<String>,
    generation: SearchProjectionPageGeneration,
    retained_id_bytes: u64,
    _memory: Vec<RuntimeRetainedMemory>,
    _cursor_memory: Option<RuntimeRetainedMemory>,
}

impl SearchProjectionIdPage {
    /// Ordered document IDs in this page.
    pub fn ids(&self) -> &[String] {
        &self.ids
    }

    /// Exclusive cursor for the next page, or `None` when this generation is
    /// exhausted for the requested kind set.
    pub fn next(&self) -> Option<&str> {
        self.next.as_deref()
    }

    /// Process-local generation identity read by this page.
    pub const fn generation(&self) -> SearchProjectionPageGeneration {
        self.generation
    }

    /// UTF-8 bytes charged while this page is alive. This includes the copied
    /// `next` cursor when present, but excludes `Vec`/`String` control words.
    pub const fn retained_id_bytes(&self) -> u64 {
        self.retained_id_bytes
    }

    #[doc(hidden)]
    pub fn from_admitted_ids(
        ids: Vec<String>,
        next: Option<String>,
        generation: SearchProjectionPageGeneration,
        retained_id_bytes: u64,
        memory: Vec<RuntimeRetainedMemory>,
        cursor_memory: Option<RuntimeRetainedMemory>,
    ) -> Self {
        Self {
            ids,
            next,
            generation,
            retained_id_bytes,
            _memory: memory,
            _cursor_memory: cursor_memory,
        }
    }
}

/// Kernel-level page contents with the per-ID reservations acquired before
/// copying. The embedded facade adds the copied next-cursor reservation.
#[doc(hidden)]
#[derive(Debug)]
pub struct SearchProjectionIdBatch {
    pub ids: Vec<String>,
    pub has_more: bool,
    pub memory: Vec<RuntimeRetainedMemory>,
}

pub(crate) fn requested_kind(
    id: &str,
    kinds: &std::collections::BTreeSet<SearchProjectionKind>,
) -> bool {
    id.split_once(':')
        .and_then(|(kind, _)| SearchProjectionKind::from_name(kind))
        .is_some_and(|kind| kinds.contains(&kind))
}

pub(crate) fn kind_range_intersects(
    first: &str,
    last: &str,
    kinds: &std::collections::BTreeSet<SearchProjectionKind>,
) -> bool {
    kinds.iter().any(|kind| {
        let (first_for_kind, after_kind) = match kind {
            SearchProjectionKind::Memory => ("memory:", "memory;"),
            SearchProjectionKind::Message => ("message:", "message;"),
            SearchProjectionKind::Entity => ("entity:", "entity;"),
            SearchProjectionKind::Source => ("source:", "source;"),
            SearchProjectionKind::SourceChunk => ("source_chunk:", "source_chunk;"),
            SearchProjectionKind::Community => ("community:", "community;"),
        };
        last >= first_for_kind && first < after_kind
    })
}

pub(crate) fn finish_ids(
    mut entries: Vec<(String, RuntimeRetainedMemory)>,
    limit: usize,
) -> SearchProjectionIdBatch {
    let has_more = entries.len() > limit;
    entries.truncate(limit);
    let (ids, memory) = entries.into_iter().unzip();
    SearchProjectionIdBatch {
        ids,
        has_more,
        memory,
    }
}
