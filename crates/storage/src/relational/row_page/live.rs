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

//! Immutable, bounded live overlays over a generation-pinned row root and
//! optional disk-backed WAL recovery delta.
//!
//! Publication is deliberately separate from SQL selection. A caller stages a
//! complete next-epoch view before its WAL append and installs the returned
//! `Arc` only after that WAL batch is durable.

use super::delta::RelationalRowDeltaRunRangeCursor;
use super::demand::RelationalRowPageProjectedOverlayValue;
use super::{
    RelationalRowDeltaError, RelationalRowDeltaReader, RelationalRowPageRecoveredValue,
    RelationalRowPageRootReader,
};
use crate::relational::{
    estimated_row_change_encoding_bytes, RelationalKey, RelationalRowChange,
    RelationalRowChangeCapture, RelationalRowChangeCaptureLimits, RelationalValue,
};
use hawdb_integrity::Sha256Digest;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    ops::Bound,
    sync::Arc,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalRowPageReadViewIdentity {
    pub base_generation: u64,
    pub delta_generation: Option<u64>,
    pub base_commit_epoch: u64,
    pub visible_commit_epoch: u64,
    pub root_set_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalRowPageLiveError {
    Admission(String),
    Corrupt(String),
    RequiresCheckpoint { tables: Vec<String> },
    Invalidated(String),
}

impl fmt::Display for RelationalRowPageLiveError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(message) => {
                write!(
                    formatter,
                    "relational row live-view admission failed: {message}"
                )
            }
            Self::Corrupt(message) => {
                write!(formatter, "corrupt relational row live view: {message}")
            }
            Self::RequiresCheckpoint { tables } => write!(
                formatter,
                "relational row live view requires a schema checkpoint for tables {}",
                tables.join(",")
            ),
            Self::Invalidated(message) => {
                write!(formatter, "relational row live view invalidated: {message}")
            }
        }
    }
}

impl std::error::Error for RelationalRowPageLiveError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalRowPageCheckpointError {
    Admission(String),
    Corrupt(String),
    Durability(String),
}

impl fmt::Display for RelationalRowPageCheckpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(message) => write!(
                formatter,
                "relational row checkpoint admission failed: {message}"
            ),
            Self::Corrupt(message) => {
                write!(
                    formatter,
                    "corrupt relational row checkpoint view: {message}"
                )
            }
            Self::Durability(message) => write!(
                formatter,
                "relational row checkpoint durability failed: {message}"
            ),
        }
    }
}

impl std::error::Error for RelationalRowPageCheckpointError {}

struct RelationalRowPageLiveBatch {
    commit_epoch: u64,
    changes: Arc<[RelationalRowChange]>,
    previous: Option<Arc<RelationalRowPageLiveBatch>>,
}

impl RelationalRowPageLiveBatch {
    fn overlay_value(
        &self,
        table: &str,
        primary_key: &RelationalKey,
    ) -> Option<RelationalRowPageRecoveredValue> {
        self.changes
            .binary_search_by(|change| {
                change
                    .table
                    .as_str()
                    .cmp(table)
                    .then_with(|| change.primary_key.cmp(primary_key))
            })
            .ok()
            .map(|index| {
                self.changes[index]
                    .row
                    .clone()
                    .map_or(RelationalRowPageRecoveredValue::Deleted, |row| {
                        RelationalRowPageRecoveredValue::Present(row)
                    })
            })
    }

    fn range_cursor(
        &self,
        table: &str,
        lower: Bound<&RelationalKey>,
        upper: Bound<&RelationalKey>,
    ) -> Option<RelationalRowPageLiveRangeCursor<'_>> {
        let start = self
            .changes
            .partition_point(|change| match change.table.as_str().cmp(table) {
                std::cmp::Ordering::Less => true,
                std::cmp::Ordering::Greater => false,
                std::cmp::Ordering::Equal => key_precedes_lower(&change.primary_key, lower),
            });
        let width = self.changes[start..].partition_point(|change| {
            match change.table.as_str().cmp(table) {
                std::cmp::Ordering::Less => true,
                std::cmp::Ordering::Greater => false,
                std::cmp::Ordering::Equal => !key_exceeds_upper(&change.primary_key, upper),
            }
        });
        let end = start.saturating_add(width);
        (start < end).then_some(RelationalRowPageLiveRangeCursor {
            batch: self,
            next: start,
            end,
        })
    }

    fn may_contain_prefix_at_or_after(
        &self,
        table: &str,
        prefix: &RelationalKey,
        lower: &RelationalKey,
    ) -> bool {
        let start = self.changes.partition_point(|change| {
            change
                .table
                .as_str()
                .cmp(table)
                .then_with(|| change.primary_key.cmp(lower))
                .is_lt()
        });
        self.changes.get(start).is_some_and(|change| {
            change.table == table && change.primary_key.0.starts_with(prefix.0.as_slice())
        })
    }
}

#[derive(Debug, Clone)]
struct RelationalRowPageLiveOverlay {
    head: Option<Arc<RelationalRowPageLiveBatch>>,
    batch_count: usize,
    entry_count: usize,
    encoded_bytes: usize,
    resident_bytes: usize,
}

impl fmt::Debug for RelationalRowPageLiveBatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelationalRowPageLiveBatch")
            .field("commit_epoch", &self.commit_epoch)
            .field("changes", &self.changes.len())
            .field("has_previous", &self.previous.is_some())
            .finish()
    }
}

impl RelationalRowPageLiveOverlay {
    fn empty() -> Self {
        Self {
            head: None,
            batch_count: 0,
            entry_count: 0,
            encoded_bytes: 0,
            resident_bytes: 0,
        }
    }

    fn append(
        &self,
        commit_epoch: u64,
        capture: RelationalRowChangeCapture,
        limits: RelationalRowChangeCaptureLimits,
    ) -> Result<Self, RelationalRowPageLiveError> {
        let (changes, declared_bytes) = match capture {
            RelationalRowChangeCapture::Captured {
                changes,
                encoded_bytes,
            } => (changes, encoded_bytes),
            RelationalRowChangeCapture::RequiresCheckpoint { tables } => {
                return Err(RelationalRowPageLiveError::RequiresCheckpoint { tables });
            }
            RelationalRowChangeCapture::Invalidated { reason } => {
                return Err(RelationalRowPageLiveError::Invalidated(reason));
            }
        };
        let capture_resident_bytes = validate_capture(&changes, declared_bytes)?;
        let entry_count = self.entry_count.checked_add(changes.len()).ok_or_else(|| {
            RelationalRowPageLiveError::Admission("live row entry accounting overflow".to_string())
        })?;
        let encoded_bytes = self
            .encoded_bytes
            .checked_add(declared_bytes)
            .ok_or_else(|| {
                RelationalRowPageLiveError::Admission(
                    "live row byte accounting overflow".to_string(),
                )
            })?;
        let resident_bytes = self
            .resident_bytes
            .checked_add(capture_resident_bytes)
            .ok_or_else(|| {
                RelationalRowPageLiveError::Admission(
                    "live row resident byte accounting overflow".to_string(),
                )
            })?;
        if entry_count > limits.max_entries.get() || resident_bytes > limits.max_bytes.get() {
            return Err(RelationalRowPageLiveError::Admission(format!(
                "live row overlay would retain {entry_count} entries/{resident_bytes} resident bytes, exceeding limits {}/{}",
                limits.max_entries, limits.max_bytes
            )));
        }
        if changes.is_empty() {
            return Ok(self.clone());
        }
        let batch_count = self.batch_count.checked_add(1).ok_or_else(|| {
            RelationalRowPageLiveError::Admission("live row batch accounting overflow".to_string())
        })?;
        let head = Arc::new(RelationalRowPageLiveBatch {
            commit_epoch,
            changes: Arc::from(changes),
            previous: self.head.as_ref().map(Arc::clone),
        });
        Ok(Self {
            head: Some(head),
            batch_count,
            entry_count,
            encoded_bytes,
            resident_bytes,
        })
    }

    fn overlay_value(
        &self,
        table: &str,
        primary_key: &RelationalKey,
    ) -> Option<RelationalRowPageRecoveredValue> {
        let mut current = self.head.as_deref();
        while let Some(batch) = current {
            if let Some(value) = batch.overlay_value(table, primary_key) {
                return Some(value);
            }
            current = batch.previous.as_deref();
        }
        None
    }

    fn overlay_value_accounted(
        &self,
        table: &str,
        primary_key: &RelationalKey,
    ) -> (Option<RelationalRowPageRecoveredValue>, usize) {
        let mut current = self.head.as_deref();
        let mut batches_examined = 0usize;
        while let Some(batch) = current {
            batches_examined = batches_examined.saturating_add(1);
            if let Some(value) = batch.overlay_value(table, primary_key) {
                return (Some(value), batches_examined);
            }
            current = batch.previous.as_deref();
        }
        (None, batches_examined)
    }

    fn may_contain_prefix_at_or_after(
        &self,
        table: &str,
        prefix: &RelationalKey,
        lower: &RelationalKey,
    ) -> bool {
        let mut current = self.head.as_deref();
        while let Some(batch) = current {
            if batch.may_contain_prefix_at_or_after(table, prefix, lower) {
                return true;
            }
            current = batch.previous.as_deref();
        }
        false
    }
}

pub(super) struct RelationalRowPageOverlayRangeEntry {
    pub key: RelationalKey,
    pub value: RelationalRowPageOverlayRangeValue,
    pub epoch: u64,
}

pub(super) enum RelationalRowPageOverlayRangeValue {
    Projected(RelationalRowPageProjectedOverlayValue),
    Recovered(RelationalRowPageRecoveredValue),
}

struct RelationalRowPageLiveRangeCursor<'a> {
    batch: &'a RelationalRowPageLiveBatch,
    next: usize,
    end: usize,
}

impl RelationalRowPageLiveRangeCursor<'_> {
    fn next(&mut self) -> Option<RelationalRowPageOverlayRangeEntry> {
        let change = self.batch.changes.get(self.next..self.end)?.first()?;
        self.next += 1;
        Some(RelationalRowPageOverlayRangeEntry {
            key: change.primary_key.clone(),
            value: RelationalRowPageOverlayRangeValue::Recovered(
                change
                    .row
                    .clone()
                    .map_or(RelationalRowPageRecoveredValue::Deleted, |row| {
                        RelationalRowPageRecoveredValue::Present(row)
                    }),
            ),
            epoch: self.batch.commit_epoch,
        })
    }
}

enum RelationalRowPageOverlayRangeSource<'a> {
    Recovery(Box<RelationalRowDeltaRunRangeCursor<'a>>),
    Live(RelationalRowPageLiveRangeCursor<'a>),
}

pub(super) struct RelationalRowPageOverlayRangeSources<'a> {
    sources: Vec<RelationalRowPageOverlayRangeSource<'a>>,
    recovery: super::RelationalRowDeltaReadReport,
    live_entries_visited: usize,
}

impl RelationalRowPageOverlayRangeSources<'_> {
    pub(super) fn len(&self) -> usize {
        self.sources.len()
    }

    pub(super) fn next(
        &mut self,
        source: usize,
    ) -> Result<Option<RelationalRowPageOverlayRangeEntry>, RelationalRowDeltaError> {
        match self.sources.get_mut(source).ok_or_else(|| {
            RelationalRowDeltaError::Corrupt(format!(
                "overlay merge source {source} is out of range"
            ))
        })? {
            RelationalRowPageOverlayRangeSource::Recovery(cursor) => {
                let next =
                    cursor
                        .next()?
                        .map(|(key, value, epoch)| RelationalRowPageOverlayRangeEntry {
                            key,
                            value: RelationalRowPageOverlayRangeValue::Projected(value),
                            epoch,
                        });
                if next.is_some() {
                    self.recovery.entries_visited = self
                        .recovery
                        .entries_visited
                        .checked_add(1)
                        .ok_or_else(|| {
                            RelationalRowDeltaError::Admission(
                                "row delta range entry counter overflow".to_string(),
                            )
                        })?;
                }
                Ok(next)
            }
            RelationalRowPageOverlayRangeSource::Live(cursor) => {
                let next = cursor.next();
                if next.is_some() {
                    self.live_entries_visited =
                        self.live_entries_visited.checked_add(1).ok_or_else(|| {
                            RelationalRowDeltaError::Admission(
                                "live row range entry counter overflow".to_string(),
                            )
                        })?;
                }
                Ok(next)
            }
        }
    }

    pub(super) fn recovery_report(
        &self,
    ) -> Result<super::RelationalRowDeltaReadReport, RelationalRowDeltaError> {
        let mut report = self.recovery.clone();
        if let Some(snapshot) = self.sources.iter().find_map(|source| match source {
            RelationalRowPageOverlayRangeSource::Recovery(cursor) => {
                Some(cursor.file_pool_snapshot())
            }
            RelationalRowPageOverlayRangeSource::Live(_) => None,
        }) {
            snapshot?.apply_to(&mut report);
        }
        report.stopped_early = self.sources.iter().any(|source| {
            matches!(
                source,
                RelationalRowPageOverlayRangeSource::Recovery(cursor) if !cursor.is_exhausted()
            )
        });
        Ok(report)
    }

    pub(super) const fn live_entries_visited(&self) -> usize {
        self.live_entries_visited
    }
}

/// One immutable row view pinned to an exact base generation and visible epoch.
///
/// Live batches retain their payload through `Arc` ownership. Advancing a view
/// installs one new persistent-chain head; pinned readers keep their prior
/// identity and payload without copying earlier batches or a database-sized
/// row set.
#[derive(Debug, Clone)]
pub struct RelationalRowPageReadView {
    identity: RelationalRowPageReadViewIdentity,
    base: Arc<RelationalRowPageRootReader>,
    recovery_delta: Option<Arc<RelationalRowDeltaReader>>,
    live: RelationalRowPageLiveOverlay,
}

impl RelationalRowPageReadView {
    pub fn from_base(base: Arc<RelationalRowPageRootReader>) -> Self {
        let manifest = base.manifest();
        Self {
            identity: RelationalRowPageReadViewIdentity {
                base_generation: manifest.generation,
                delta_generation: None,
                base_commit_epoch: manifest.source_commit_epoch,
                visible_commit_epoch: manifest.source_commit_epoch,
                root_set_digest: manifest.root_set_digest,
            },
            base,
            recovery_delta: None,
            live: RelationalRowPageLiveOverlay::empty(),
        }
    }

    pub fn from_recovery_delta(
        base: Arc<RelationalRowPageRootReader>,
        recovery_delta: Arc<RelationalRowDeltaReader>,
    ) -> Result<Self, RelationalRowDeltaError> {
        let base_manifest = base.manifest();
        let delta_manifest = recovery_delta.manifest();
        if delta_manifest.base.generation != base_manifest.generation
            || delta_manifest.base.source_commit_epoch != base_manifest.source_commit_epoch
            || delta_manifest.base.root_set_digest != base_manifest.root_set_digest
        {
            return Err(RelationalRowDeltaError::Corrupt(
                "row read view cannot combine mismatched base and delta generations".to_string(),
            ));
        }
        Ok(Self {
            identity: RelationalRowPageReadViewIdentity {
                base_generation: base_manifest.generation,
                delta_generation: Some(delta_manifest.delta_generation),
                base_commit_epoch: base_manifest.source_commit_epoch,
                visible_commit_epoch: delta_manifest.visible_commit_epoch,
                root_set_digest: base_manifest.root_set_digest,
            },
            base,
            recovery_delta: Some(recovery_delta),
            live: RelationalRowPageLiveOverlay::empty(),
        })
    }

    pub const fn identity(&self) -> RelationalRowPageReadViewIdentity {
        self.identity
    }

    pub fn base(&self) -> &RelationalRowPageRootReader {
        &self.base
    }

    pub fn pinned_base(&self) -> Arc<RelationalRowPageRootReader> {
        Arc::clone(&self.base)
    }

    pub fn recovery_delta(&self) -> Option<&RelationalRowDeltaReader> {
        self.recovery_delta.as_deref()
    }

    pub(super) fn validate_serving_fence(&self) -> Result<(), RelationalRowDeltaError> {
        let base = self.base.manifest();
        if self.identity.base_generation != base.generation
            || self.identity.base_commit_epoch != base.source_commit_epoch
            || self.identity.root_set_digest != base.root_set_digest
            || self.identity.visible_commit_epoch < self.identity.base_commit_epoch
        {
            return Err(RelationalRowDeltaError::Corrupt(
                "relational row read-view identity differs from its pinned base".to_string(),
            ));
        }
        if self.identity.delta_generation
            != self
                .recovery_delta
                .as_ref()
                .map(|delta| delta.manifest().delta_generation)
        {
            return Err(RelationalRowDeltaError::Corrupt(
                "relational row read-view delta identity differs from its pinned recovery delta"
                    .to_string(),
            ));
        }
        let live_floor = self
            .recovery_delta
            .as_ref()
            .map_or(self.identity.base_commit_epoch, |delta| {
                delta.manifest().visible_commit_epoch
            });
        if live_floor > self.identity.visible_commit_epoch {
            return Err(RelationalRowDeltaError::Corrupt(
                "relational row recovery epoch exceeds the read-view epoch".to_string(),
            ));
        }
        let mut newer_epoch = None;
        let mut current = self.live.head.as_deref();
        while let Some(batch) = current {
            if batch.commit_epoch <= live_floor
                || batch.commit_epoch > self.identity.visible_commit_epoch
                || newer_epoch.is_some_and(|newer| batch.commit_epoch >= newer)
            {
                return Err(RelationalRowDeltaError::Corrupt(format!(
                    "live row batch epoch {} is outside the ordered serving range ({live_floor}, {}]",
                    batch.commit_epoch, self.identity.visible_commit_epoch
                )));
            }
            newer_epoch = Some(batch.commit_epoch);
            current = batch.previous.as_deref();
        }
        Ok(())
    }

    pub fn advance(
        &self,
        next_commit_epoch: u64,
        capture: Option<RelationalRowChangeCapture>,
        limits: RelationalRowChangeCaptureLimits,
    ) -> Result<Self, RelationalRowPageLiveError> {
        let expected = self
            .identity
            .visible_commit_epoch
            .checked_add(1)
            .ok_or_else(|| {
                RelationalRowPageLiveError::Corrupt(
                    "relational row read-view epoch overflow".to_string(),
                )
            })?;
        if next_commit_epoch != expected {
            return Err(RelationalRowPageLiveError::Corrupt(format!(
                "relational row read view expected commit epoch {expected}, got {next_commit_epoch}"
            )));
        }
        if let Some(capture) = capture.as_ref() {
            validate_capture_against_base(&self.base, capture)?;
        }
        let live = capture.map_or_else(
            || Ok(self.live.clone()),
            |capture| self.live.append(next_commit_epoch, capture, limits),
        )?;
        let mut identity = self.identity;
        identity.visible_commit_epoch = next_commit_epoch;
        Ok(Self {
            identity,
            base: Arc::clone(&self.base),
            recovery_delta: self.recovery_delta.as_ref().map(Arc::clone),
            live,
        })
    }

    pub fn live_batch_count(&self) -> usize {
        self.live.batch_count
    }

    pub const fn live_entry_count(&self) -> usize {
        self.live.entry_count
    }

    pub const fn live_encoded_bytes(&self) -> usize {
        self.live.encoded_bytes
    }

    pub const fn live_resident_bytes(&self) -> usize {
        self.live.resident_bytes
    }

    pub(super) fn live_may_contain_prefix_at_or_after(
        &self,
        table: &str,
        prefix: &RelationalKey,
        lower: &RelationalKey,
    ) -> bool {
        self.live
            .may_contain_prefix_at_or_after(table, prefix, lower)
    }

    pub fn overlay_value(
        &self,
        table: &str,
        primary_key: &RelationalKey,
    ) -> Result<Option<RelationalRowPageRecoveredValue>, RelationalRowDeltaError> {
        if let Some(value) = self.live.overlay_value(table, primary_key) {
            return Ok(Some(value));
        }
        self.recovery_delta.as_ref().map_or(Ok(None), |delta| {
            delta
                .lookup(table, primary_key)
                .map(|(value, _report)| value)
        })
    }

    pub(super) fn overlay_value_accounted(
        &self,
        table: &str,
        primary_key: &RelationalKey,
    ) -> Result<
        (
            Option<RelationalRowPageRecoveredValue>,
            super::RelationalRowDeltaReadReport,
            usize,
            bool,
        ),
        RelationalRowDeltaError,
    > {
        let (live, batches_examined) = self.live.overlay_value_accounted(table, primary_key);
        if live.is_some() {
            return Ok((
                live,
                super::RelationalRowDeltaReadReport::default(),
                batches_examined,
                true,
            ));
        }
        self.recovery_delta.as_ref().map_or_else(
            || {
                Ok((
                    None,
                    super::RelationalRowDeltaReadReport::default(),
                    batches_examined,
                    false,
                ))
            },
            |delta| {
                delta
                    .lookup(table, primary_key)
                    .map(|(value, report)| (value, report, batches_examined, false))
            },
        )
    }

    pub(super) fn overlay_range_sources(
        &self,
        table: &str,
        lower: Bound<&RelationalKey>,
        upper: Bound<&RelationalKey>,
        requested_fields: &[usize],
        binds_recovery_overflow: bool,
        max_sources: usize,
    ) -> Result<RelationalRowPageOverlayRangeSources<'_>, RelationalRowDeltaError> {
        let mut live = Vec::new();
        let mut current = self.live.head.as_deref();
        while let Some(batch) = current {
            if let Some(cursor) = batch.range_cursor(table, lower, upper) {
                if live.len() == max_sources {
                    return Err(RelationalRowDeltaError::Admission(format!(
                        "row snapshot range needs more than {max_sources} live merge sources"
                    )));
                }
                live.push(cursor);
            }
            current = batch.previous.as_deref();
        }
        let remaining_sources = max_sources.saturating_sub(live.len());
        let (recovery, recovery_report) = self.recovery_delta.as_ref().map_or_else(
            || Ok((Vec::new(), super::RelationalRowDeltaReadReport::default())),
            |delta| {
                delta.range_sources(
                    table,
                    lower,
                    upper,
                    requested_fields,
                    binds_recovery_overflow,
                    remaining_sources,
                )
            },
        )?;
        let mut sources = Vec::with_capacity(recovery.len().saturating_add(live.len()));
        sources.extend(
            recovery
                .into_iter()
                .map(Box::new)
                .map(RelationalRowPageOverlayRangeSource::Recovery),
        );
        sources.extend(
            live.into_iter()
                .map(RelationalRowPageOverlayRangeSource::Live),
        );
        Ok(RelationalRowPageOverlayRangeSources {
            sources,
            recovery: recovery_report,
            live_entries_visited: 0,
        })
    }

    pub fn latest_live_commit_epoch(&self) -> Option<u64> {
        self.live.head.as_ref().map(|batch| batch.commit_epoch)
    }

    /// Coalesces the keys changed since the pinned base into one bounded,
    /// strictly ordered capture whose row values come from the caller's
    /// current canonical state.
    ///
    /// Recovery runs and live batches may contain repeated versions of one
    /// key. Only the final key is retained, and `current_row` is invoked once
    /// per distinct key after the complete key set has been admitted. The
    /// transient ordered set plus the returned capture share one conservative
    /// byte envelope so checkpoint planning cannot build an unbounded merge
    /// structure before dirty-page admission.
    pub fn checkpoint_capture(
        &self,
        mut current_row: impl FnMut(
            &str,
            &RelationalKey,
        ) -> Result<
            Option<crate::relational::RelationalRow>,
            RelationalRowPageCheckpointError,
        >,
        limits: RelationalRowChangeCaptureLimits,
    ) -> Result<RelationalRowChangeCapture, RelationalRowPageCheckpointError> {
        let mut keys = CheckpointChangeKeys::new(limits);
        if let Some(delta) = self.recovery_delta.as_deref() {
            let mut collection_error = None;
            let report = delta
                .visit_entries(
                    |table, primary_key, _, _| match keys.insert(table, primary_key) {
                        Ok(()) => true,
                        Err(error) => {
                            collection_error = Some(error);
                            false
                        }
                    },
                )
                .map_err(map_delta_checkpoint_error)?;
            if let Some(error) = collection_error {
                return Err(error);
            }
            if report.stopped_early {
                return Err(RelationalRowPageCheckpointError::Corrupt(
                    "recovery-delta traversal stopped without an admission error".to_string(),
                ));
            }
        }

        let live_floor = self
            .recovery_delta
            .as_ref()
            .map_or(self.identity.base_commit_epoch, |delta| {
                delta.manifest().visible_commit_epoch
            });
        let mut newer_live_epoch = None;
        let mut batch = self.live.head.as_deref();
        while let Some(current) = batch {
            if current.commit_epoch <= live_floor
                || current.commit_epoch > self.identity.visible_commit_epoch
                || newer_live_epoch.is_some_and(|newer| current.commit_epoch >= newer)
            {
                return Err(RelationalRowPageCheckpointError::Corrupt(format!(
                    "live batch epoch {} is outside the ordered range ({live_floor}, {}]",
                    current.commit_epoch, self.identity.visible_commit_epoch
                )));
            }
            for change in current.changes.iter() {
                keys.insert(&change.table, &change.primary_key)?;
            }
            newer_live_epoch = Some(current.commit_epoch);
            batch = current.previous.as_deref();
        }

        keys.into_capture(&mut current_row)
    }

    /// Resolves one changed key from the complete recovery/live overlay.
    /// Checkpoint planning uses this when canonical state intentionally owns
    /// only schemas and counts.
    pub fn checkpoint_overlay_row(
        &self,
        table: &str,
        primary_key: &RelationalKey,
    ) -> Result<Option<crate::relational::RelationalRow>, RelationalRowPageCheckpointError> {
        self.overlay_value(table, primary_key)
            .map_err(map_delta_checkpoint_error)?
            .map_or_else(
                || {
                    Err(RelationalRowPageCheckpointError::Corrupt(format!(
                        "checkpoint change key {table}/{primary_key:?} is missing from its overlay"
                    )))
                },
                |value| match value {
                    RelationalRowPageRecoveredValue::Present(row) => Ok(Some(row)),
                    RelationalRowPageRecoveredValue::Deleted => Ok(None),
                },
            )
    }
}

fn key_precedes_lower(key: &RelationalKey, lower: Bound<&RelationalKey>) -> bool {
    match lower {
        Bound::Unbounded => false,
        Bound::Included(lower) => key < lower,
        Bound::Excluded(lower) => key <= lower,
    }
}

fn key_exceeds_upper(key: &RelationalKey, upper: Bound<&RelationalKey>) -> bool {
    match upper {
        Bound::Unbounded => false,
        Bound::Included(upper) => key > upper,
        Bound::Excluded(upper) => key >= upper,
    }
}

struct CheckpointChangeKeys {
    tables: BTreeMap<String, BTreeSet<RelationalKey>>,
    entry_count: usize,
    resident_bytes: usize,
    limits: RelationalRowChangeCaptureLimits,
}

impl CheckpointChangeKeys {
    fn new(limits: RelationalRowChangeCaptureLimits) -> Self {
        Self {
            tables: BTreeMap::new(),
            entry_count: 0,
            resident_bytes: 0,
            limits,
        }
    }

    fn insert(
        &mut self,
        table: &str,
        primary_key: &RelationalKey,
    ) -> Result<(), RelationalRowPageCheckpointError> {
        if self
            .tables
            .get(table)
            .is_some_and(|keys| keys.contains(primary_key))
        {
            return Ok(());
        }
        let next_entries = self.entry_count.checked_add(1).ok_or_else(|| {
            RelationalRowPageCheckpointError::Admission(
                "checkpoint change entry accounting overflow".to_string(),
            )
        })?;
        if next_entries > self.limits.max_entries.get() {
            return Err(RelationalRowPageCheckpointError::Admission(format!(
                "checkpoint change set contains {next_entries} distinct keys, exceeding limit {}",
                self.limits.max_entries
            )));
        }
        let table_bytes = if self.tables.contains_key(table) {
            0
        } else {
            std::mem::size_of::<String>()
                .checked_add(4 * std::mem::size_of::<usize>())
                .and_then(|bytes| bytes.checked_add(table.len()))
                .ok_or_else(|| {
                    RelationalRowPageCheckpointError::Admission(
                        "checkpoint table allocation accounting overflow".to_string(),
                    )
                })?
        };
        let key_bytes = relational_key_resident_bytes(primary_key).ok_or_else(|| {
            RelationalRowPageCheckpointError::Admission(
                "checkpoint change key accounting overflow".to_string(),
            )
        })?;
        let next_resident_bytes = self
            .resident_bytes
            .checked_add(table_bytes)
            .and_then(|bytes| bytes.checked_add(key_bytes))
            .ok_or_else(|| {
                RelationalRowPageCheckpointError::Admission(
                    "checkpoint change resident-byte accounting overflow".to_string(),
                )
            })?;
        if next_resident_bytes > self.limits.max_bytes.get() {
            return Err(RelationalRowPageCheckpointError::Admission(format!(
                "checkpoint change key set retains {next_resident_bytes} bytes, exceeding limit {}",
                self.limits.max_bytes
            )));
        }
        self.tables
            .entry(table.to_string())
            .or_default()
            .insert(primary_key.clone());
        self.entry_count = next_entries;
        self.resident_bytes = next_resident_bytes;
        Ok(())
    }

    fn into_capture(
        self,
        current_row: &mut impl FnMut(
            &str,
            &RelationalKey,
        ) -> Result<
            Option<crate::relational::RelationalRow>,
            RelationalRowPageCheckpointError,
        >,
    ) -> Result<RelationalRowChangeCapture, RelationalRowPageCheckpointError> {
        let capture_vector_bytes = self
            .entry_count
            .checked_mul(std::mem::size_of::<RelationalRowChange>())
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Vec<RelationalRowChange>>()))
            .ok_or_else(|| {
                RelationalRowPageCheckpointError::Admission(
                    "checkpoint capture allocation accounting overflow".to_string(),
                )
            })?;
        let initial_peak = self
            .resident_bytes
            .checked_add(capture_vector_bytes)
            .ok_or_else(|| {
                RelationalRowPageCheckpointError::Admission(
                    "checkpoint capture peak-byte accounting overflow".to_string(),
                )
            })?;
        if initial_peak > self.limits.max_bytes.get() {
            return Err(RelationalRowPageCheckpointError::Admission(format!(
                "checkpoint capture requires {initial_peak} bytes before row resolution, exceeding limit {}",
                self.limits.max_bytes
            )));
        }
        let mut changes = Vec::with_capacity(self.entry_count);
        let mut encoded_bytes = 0usize;
        let mut capture_resident_bytes = capture_vector_bytes;
        for (table, primary_keys) in self.tables {
            for primary_key in primary_keys {
                let change = RelationalRowChange {
                    row: current_row(&table, &primary_key)?,
                    table: table.clone(),
                    primary_key,
                };
                let change_bytes =
                    estimated_row_change_encoding_bytes(&change).ok_or_else(|| {
                        RelationalRowPageCheckpointError::Admission(
                            "checkpoint change encoding-byte accounting overflow".to_string(),
                        )
                    })?;
                encoded_bytes = encoded_bytes.checked_add(change_bytes).ok_or_else(|| {
                    RelationalRowPageCheckpointError::Admission(
                        "checkpoint capture encoding-byte accounting overflow".to_string(),
                    )
                })?;
                let resident_bytes = estimated_change_resident_bytes(&change)
                    .and_then(|bytes| bytes.checked_sub(std::mem::size_of::<RelationalRowChange>()))
                    .ok_or_else(|| {
                        RelationalRowPageCheckpointError::Admission(
                            "checkpoint change resident-byte accounting overflow".to_string(),
                        )
                    })?;
                capture_resident_bytes = capture_resident_bytes
                    .checked_add(resident_bytes)
                    .ok_or_else(|| {
                        RelationalRowPageCheckpointError::Admission(
                            "checkpoint capture resident-byte accounting overflow".to_string(),
                        )
                    })?;
                let conservative_peak = self
                    .resident_bytes
                    .checked_add(capture_resident_bytes)
                    .ok_or_else(|| {
                        RelationalRowPageCheckpointError::Admission(
                            "checkpoint capture peak-byte accounting overflow".to_string(),
                        )
                    })?;
                if encoded_bytes > self.limits.max_bytes.get()
                    || conservative_peak > self.limits.max_bytes.get()
                {
                    return Err(RelationalRowPageCheckpointError::Admission(format!(
                        "checkpoint capture uses {encoded_bytes} encoded bytes/{conservative_peak} conservative peak bytes, exceeding limit {}",
                        self.limits.max_bytes
                    )));
                }
                changes.push(change);
            }
        }
        Ok(RelationalRowChangeCapture::Captured {
            changes,
            encoded_bytes,
        })
    }
}

fn relational_key_resident_bytes(primary_key: &RelationalKey) -> Option<usize> {
    primary_key.0.iter().try_fold(
        std::mem::size_of::<RelationalKey>()
            .checked_add(4 * std::mem::size_of::<usize>())?
            .checked_add(
                primary_key
                    .0
                    .len()
                    .checked_mul(std::mem::size_of::<RelationalValue>())?,
            )?,
        |bytes, value| bytes.checked_add(value.estimated_payload_bytes()),
    )
}

fn estimated_change_resident_bytes(change: &RelationalRowChange) -> Option<usize> {
    let key_bytes = change.primary_key.0.iter().try_fold(
        change
            .primary_key
            .0
            .len()
            .checked_mul(std::mem::size_of::<RelationalValue>())?,
        |bytes, value| bytes.checked_add(value.estimated_payload_bytes()),
    )?;
    let row_bytes = change.row.as_ref().map_or(Some(0), |row| {
        row.values().iter().try_fold(
            row.values()
                .len()
                .checked_mul(std::mem::size_of::<RelationalValue>())?,
            |bytes, value| bytes.checked_add(value.estimated_payload_bytes()),
        )
    })?;
    std::mem::size_of::<RelationalRowChange>()
        .checked_add(change.table.len())?
        .checked_add(key_bytes)?
        .checked_add(row_bytes)
}

fn map_delta_checkpoint_error(error: RelationalRowDeltaError) -> RelationalRowPageCheckpointError {
    match error {
        RelationalRowDeltaError::Admission(message) => {
            RelationalRowPageCheckpointError::Admission(message)
        }
        RelationalRowDeltaError::Durability(message) => {
            RelationalRowPageCheckpointError::Durability(message)
        }
        error => RelationalRowPageCheckpointError::Corrupt(error.to_string()),
    }
}

fn validate_capture_against_base(
    base: &RelationalRowPageRootReader,
    capture: &RelationalRowChangeCapture,
) -> Result<(), RelationalRowPageLiveError> {
    let RelationalRowChangeCapture::Captured { changes, .. } = capture else {
        return Ok(());
    };
    let tables = &base.manifest().tables;
    for change in changes {
        if tables
            .binary_search_by(|table| table.table.cmp(&change.table))
            .is_err()
        {
            return Err(RelationalRowPageLiveError::Corrupt(format!(
                "live row change references table {} outside the pinned row root",
                change.table
            )));
        }
    }
    Ok(())
}

fn validate_capture(
    changes: &[RelationalRowChange],
    declared_bytes: usize,
) -> Result<usize, RelationalRowPageLiveError> {
    let mut actual_bytes = 0usize;
    let mut resident_bytes = if changes.is_empty() {
        0
    } else {
        std::mem::size_of::<RelationalRowPageLiveBatch>()
            .checked_add(4 * std::mem::size_of::<usize>())
            .ok_or_else(|| {
                RelationalRowPageLiveError::Admission(
                    "live row batch allocation accounting overflow".to_string(),
                )
            })?
    };
    let mut previous: Option<(&str, &RelationalKey)> = None;
    for change in changes {
        if change.table.is_empty() {
            return Err(RelationalRowPageLiveError::Corrupt(
                "live row change has an empty table name".to_string(),
            ));
        }
        let current = (change.table.as_str(), &change.primary_key);
        if previous.is_some_and(|previous| previous >= current) {
            return Err(RelationalRowPageLiveError::Corrupt(
                "live row changes are not strictly ordered by table and primary key".to_string(),
            ));
        }
        let bytes = estimated_row_change_encoding_bytes(change).ok_or_else(|| {
            RelationalRowPageLiveError::Admission(
                "live row change byte accounting overflow".to_string(),
            )
        })?;
        actual_bytes = actual_bytes.checked_add(bytes).ok_or_else(|| {
            RelationalRowPageLiveError::Admission(
                "live row capture byte accounting overflow".to_string(),
            )
        })?;
        let change_resident_bytes = estimated_change_resident_bytes(change).ok_or_else(|| {
            RelationalRowPageLiveError::Admission(
                "live row resident byte accounting overflow".to_string(),
            )
        })?;
        resident_bytes = resident_bytes
            .checked_add(change_resident_bytes)
            .ok_or_else(|| {
                RelationalRowPageLiveError::Admission(
                    "live row resident byte accounting overflow".to_string(),
                )
            })?;
        previous = Some(current);
    }
    if actual_bytes != declared_bytes {
        return Err(RelationalRowPageLiveError::Corrupt(format!(
            "live row capture declares {declared_bytes} bytes but requires {actual_bytes}"
        )));
    }
    Ok(resident_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relational::{RelationalRow, RelationalValue};
    use std::num::NonZeroUsize;

    #[test]
    fn capture_validation_rejects_undercharged_and_unordered_changes() {
        let changes = vec![change(2, Some("two")), change(1, Some("one"))];
        assert!(matches!(
            validate_capture(&changes, 0),
            Err(RelationalRowPageLiveError::Corrupt(reason))
                if reason.contains("strictly ordered")
        ));

        let changes = vec![change(1, Some("one"))];
        assert!(matches!(
            validate_capture(&changes, 0),
            Err(RelationalRowPageLiveError::Corrupt(reason))
                if reason.contains("declares")
        ));
        let encoded_bytes = estimated_row_change_encoding_bytes(&changes[0]).unwrap();
        let resident_bytes = validate_capture(&changes, encoded_bytes).unwrap();
        assert!(resident_bytes > encoded_bytes);
        let limits = RelationalRowChangeCaptureLimits {
            max_entries: NonZeroUsize::new(1).unwrap(),
            max_bytes: NonZeroUsize::new(encoded_bytes).unwrap(),
        };
        assert!(matches!(
            RelationalRowPageLiveOverlay::empty().append(2, capture(changes), limits),
            Err(RelationalRowPageLiveError::Admission(reason))
                if reason.contains("resident bytes")
        ));
    }

    #[test]
    fn overlay_admission_is_cumulative_and_atomic() {
        let limits = RelationalRowChangeCaptureLimits {
            max_entries: NonZeroUsize::new(1).unwrap(),
            max_bytes: NonZeroUsize::new(4096).unwrap(),
        };
        let first = capture(vec![change(1, Some("one"))]);
        let overlay = RelationalRowPageLiveOverlay::empty()
            .append(2, first, limits)
            .unwrap();
        let error = overlay
            .append(3, capture(vec![change(2, Some("two"))]), limits)
            .unwrap_err();
        assert!(matches!(error, RelationalRowPageLiveError::Admission(_)));
        assert_eq!(overlay.entry_count, 1);
        assert_eq!(overlay.batch_count, 1);
        assert_eq!(
            overlay.overlay_value("documents", &key(1)),
            Some(RelationalRowPageRecoveredValue::Present(row(1, "one")))
        );
    }

    #[test]
    fn checkpoint_key_capture_coalesces_and_orders_distinct_keys() {
        let limits = RelationalRowChangeCaptureLimits {
            max_entries: NonZeroUsize::new(4).unwrap(),
            max_bytes: NonZeroUsize::new(16 * 1024).unwrap(),
        };
        let mut keys = CheckpointChangeKeys::new(limits);
        keys.insert("documents", &key(2)).unwrap();
        keys.insert("documents", &key(1)).unwrap();
        keys.insert("documents", &key(2)).unwrap();

        let mut resolutions = 0;
        let capture = keys
            .into_capture(&mut |_, primary_key| {
                resolutions += 1;
                Ok((primary_key == &key(1)).then(|| row(1, "current")))
            })
            .unwrap();
        let RelationalRowChangeCapture::Captured {
            changes,
            encoded_bytes,
        } = capture
        else {
            panic!("checkpoint key capture must remain materialized");
        };
        assert_eq!(resolutions, 2);
        assert!(encoded_bytes > 0);
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0].primary_key, key(1));
        assert_eq!(changes[0].row, Some(row(1, "current")));
        assert_eq!(changes[1].primary_key, key(2));
        assert_eq!(changes[1].row, None);
    }

    #[test]
    fn checkpoint_key_capture_rejects_unbounded_transient_state() {
        let mut entry_limited = CheckpointChangeKeys::new(RelationalRowChangeCaptureLimits {
            max_entries: NonZeroUsize::new(1).unwrap(),
            max_bytes: NonZeroUsize::new(16 * 1024).unwrap(),
        });
        entry_limited.insert("documents", &key(1)).unwrap();
        assert!(matches!(
            entry_limited.insert("documents", &key(2)),
            Err(RelationalRowPageCheckpointError::Admission(reason))
                if reason.contains("distinct keys")
        ));

        let mut byte_limited = CheckpointChangeKeys::new(RelationalRowChangeCaptureLimits {
            max_entries: NonZeroUsize::new(1).unwrap(),
            max_bytes: NonZeroUsize::new(1).unwrap(),
        });
        assert!(matches!(
            byte_limited.insert("documents", &key(1)),
            Err(RelationalRowPageCheckpointError::Admission(reason))
                if reason.contains("resident-byte") || reason.contains("retains")
        ));
    }

    fn capture(changes: Vec<RelationalRowChange>) -> RelationalRowChangeCapture {
        let encoded_bytes = changes
            .iter()
            .map(|change| estimated_row_change_encoding_bytes(change).unwrap())
            .sum();
        RelationalRowChangeCapture::Captured {
            changes,
            encoded_bytes,
        }
    }

    fn change(id: i64, body: Option<&str>) -> RelationalRowChange {
        RelationalRowChange {
            table: "documents".to_string(),
            primary_key: key(id),
            row: body.map(|body| row(id, body)),
        }
    }

    fn key(id: i64) -> RelationalKey {
        RelationalKey(vec![RelationalValue::BigInt(id)])
    }

    fn row(id: i64, body: &str) -> RelationalRow {
        RelationalRow::new(vec![
            RelationalValue::BigInt(id),
            RelationalValue::Text(body.to_string()),
        ])
    }
}
