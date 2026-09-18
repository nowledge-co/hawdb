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

//! Immutable, manifest-last relational row delta runs.

use super::{RelationalRowPageLimits, RelationalRowPageRootReader};
use crate::relational::{
    RelationalOverflowRootBinding, RelationalRecoverySourceIdentity,
    RelationalRowChangeCaptureLimits,
};
use hawdb_integrity::{IntegrityDigest, Sha256Digest};
use std::fmt;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::path::Path;

mod builder;
mod codec;
mod reader;

pub use builder::RelationalRowDeltaBuilder;
pub use reader::RelationalRowDeltaReader;
pub(super) use reader::RelationalRowDeltaRunRangeCursor;

pub const RELATIONAL_ROW_DELTA_MANIFEST_FILE: &str = "relational-row-delta.manifest.hawdb";
const RELATIONAL_ROW_DELTA_PUBLICATION_LOCK_FILE: &str = "relational-row-delta.lock";

pub const DEFAULT_RELATIONAL_ROW_DELTA_DIRTY_ENTRIES: usize = 100_000;
pub const DEFAULT_RELATIONAL_ROW_DELTA_DIRTY_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_RELATIONAL_ROW_DELTA_RUNS: usize = 4096;
pub const DEFAULT_RELATIONAL_ROW_DELTA_CHECKPOINT_RUNS: usize = 256;
pub const DEFAULT_RELATIONAL_ROW_DELTA_RANGE_OPEN_FILES: usize = 32;
pub const DEFAULT_RELATIONAL_ROW_DELTA_MANIFEST_BYTES: usize = 1024 * 1024;
pub const DEFAULT_RELATIONAL_ROW_DELTA_RUN_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const DEFAULT_RELATIONAL_ROW_DELTA_TABLES: usize = 4096;

pub fn relational_row_delta_run_file(
    base_generation: u64,
    delta_generation: u64,
    ordinal: u32,
) -> String {
    format!("relational-row-delta-{base_generation}-{delta_generation}-{ordinal}.run.hawdb")
}

pub fn relational_row_delta_manifest_generation_file(
    base_generation: u64,
    delta_generation: u64,
) -> String {
    format!("relational-row-delta-{base_generation}-{delta_generation}.manifest.hawdb")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalRowDeltaConfig {
    pub max_dirty_entries: NonZeroUsize,
    pub max_dirty_bytes: NonZeroUsize,
    pub max_runs: NonZeroUsize,
    /// Soft run count at which a canonical checkpoint fold is recommended.
    pub checkpoint_runs: NonZeroUsize,
    /// Maximum recovery-run files retained by one streaming range merge.
    pub max_range_open_files: NonZeroUsize,
    pub max_manifest_bytes: NonZeroUsize,
    pub max_run_bytes: NonZeroU64,
    pub max_tables: NonZeroUsize,
    pub row_limits: RelationalRowPageLimits,
}

impl Default for RelationalRowDeltaConfig {
    fn default() -> Self {
        Self {
            max_dirty_entries: NonZeroUsize::new(DEFAULT_RELATIONAL_ROW_DELTA_DIRTY_ENTRIES)
                .expect("default row delta dirty entry limit is non-zero"),
            max_dirty_bytes: NonZeroUsize::new(DEFAULT_RELATIONAL_ROW_DELTA_DIRTY_BYTES)
                .expect("default row delta dirty byte limit is non-zero"),
            max_runs: NonZeroUsize::new(DEFAULT_RELATIONAL_ROW_DELTA_RUNS)
                .expect("default row delta run limit is non-zero"),
            checkpoint_runs: NonZeroUsize::new(DEFAULT_RELATIONAL_ROW_DELTA_CHECKPOINT_RUNS)
                .expect("default row delta checkpoint run target is non-zero"),
            max_range_open_files: NonZeroUsize::new(DEFAULT_RELATIONAL_ROW_DELTA_RANGE_OPEN_FILES)
                .expect("default row delta range open-file limit is non-zero"),
            max_manifest_bytes: NonZeroUsize::new(DEFAULT_RELATIONAL_ROW_DELTA_MANIFEST_BYTES)
                .expect("default row delta manifest limit is non-zero"),
            max_run_bytes: NonZeroU64::new(DEFAULT_RELATIONAL_ROW_DELTA_RUN_BYTES)
                .expect("default row delta run-byte limit is non-zero"),
            max_tables: NonZeroUsize::new(DEFAULT_RELATIONAL_ROW_DELTA_TABLES)
                .expect("default row delta table limit is non-zero"),
            row_limits: RelationalRowPageLimits::default(),
        }
    }
}

impl RelationalRowDeltaConfig {
    pub const fn capture_limits(self) -> RelationalRowChangeCaptureLimits {
        RelationalRowChangeCaptureLimits {
            max_entries: self.max_dirty_entries,
            max_bytes: self.max_dirty_bytes,
        }
    }

    pub const fn checkpoint_recommended(self, run_count: usize) -> bool {
        run_count >= self.checkpoint_runs.get()
    }

    fn validate_run_policy(self) -> Result<(), RelationalRowDeltaError> {
        if self.checkpoint_runs > self.max_runs {
            return Err(RelationalRowDeltaError::Admission(format!(
                "row delta checkpoint target {} exceeds hard run limit {}",
                self.checkpoint_runs, self.max_runs
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RelationalRowDeltaGeneration {
    pub base_generation: u64,
    pub delta_generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalRowDeltaBaseBinding {
    pub generation: u64,
    pub source_commit_epoch: u64,
    pub root_set_digest: Sha256Digest,
}

impl RelationalRowDeltaBaseBinding {
    fn from_reader(base: &RelationalRowPageRootReader) -> Self {
        Self {
            generation: base.manifest().generation,
            source_commit_epoch: base.manifest().source_commit_epoch,
            root_set_digest: base.manifest().root_set_digest,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalRowDeltaTableMetadata {
    pub table: String,
    pub schema_digest: Sha256Digest,
    pub column_count: NonZeroU32,
    pub row_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalRowDeltaManifest {
    pub base: RelationalRowDeltaBaseBinding,
    pub delta_generation: u64,
    pub visible_commit_epoch: u64,
    pub recovery_source: RelationalRecoverySourceIdentity,
    pub schema_set_digest: Sha256Digest,
    pub run_set_digest: Sha256Digest,
    pub overflow_root: Option<RelationalOverflowRootBinding>,
    tables: Vec<RelationalRowDeltaTableMetadata>,
    runs: Vec<RowDeltaRunDescriptor>,
    total_entries: u64,
}

impl RelationalRowDeltaManifest {
    pub const fn generation(&self) -> RelationalRowDeltaGeneration {
        RelationalRowDeltaGeneration {
            base_generation: self.base.generation,
            delta_generation: self.delta_generation,
        }
    }

    pub fn tables(&self) -> &[RelationalRowDeltaTableMetadata] {
        &self.tables
    }

    pub fn run_count(&self) -> usize {
        self.runs.len()
    }

    pub const fn total_entries(&self) -> u64 {
        self.total_entries
    }

    pub fn artifact_bytes(&self) -> u64 {
        self.runs
            .iter()
            .fold(0u64, |bytes, run| bytes.saturating_add(run.encoded_len))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalRowDeltaReport {
    pub generation: RelationalRowDeltaGeneration,
    pub base_commit_epoch: u64,
    pub visible_commit_epoch: u64,
    pub runs: usize,
    pub entries: u64,
    pub run_bytes: u64,
    pub manifest_bytes: u64,
    pub replayed_batches: u64,
    pub peak_dirty_entries: usize,
    pub peak_dirty_bytes: usize,
    pub events: [RelationalRowDeltaPublicationPhase; 4],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalRowDeltaPublicationPhase {
    CandidateRunsDurable,
    CandidateManifestDurable,
    BaseRevalidated,
    LatestManifestPublished,
}

const COMPLETE_PUBLICATION_TRACE: [RelationalRowDeltaPublicationPhase; 4] = [
    RelationalRowDeltaPublicationPhase::CandidateRunsDurable,
    RelationalRowDeltaPublicationPhase::CandidateManifestDurable,
    RelationalRowDeltaPublicationPhase::BaseRevalidated,
    RelationalRowDeltaPublicationPhase::LatestManifestPublished,
];

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RelationalRowDeltaReadReport {
    pub runs_read: usize,
    pub bytes_read: u64,
    pub entries_visited: u64,
    pub peak_open_files: usize,
    /// Successful `File::open` calls made by the range cursor pool.
    pub range_file_opens: usize,
    /// Range cursor requests served by an already retained run file.
    pub range_file_pool_hits: usize,
    /// Range cursor requests that had to open a run file.
    pub range_file_pool_misses: usize,
    pub stopped_early: bool,
}

#[derive(Debug)]
pub enum RelationalRowDeltaError {
    Admission(String),
    Corrupt(String),
    Durability(String),
    RequiresCheckpoint {
        tables: Vec<String>,
    },
    Invalidated(String),
    Publication(super::RelationalRowPagePublicationError),
    Row(super::RelationalRowPageError),
    StaleGeneration {
        expected_previous: Option<RelationalRowDeltaGeneration>,
        actual_previous: Option<RelationalRowDeltaGeneration>,
    },
    StaleBase {
        expected: RelationalRowDeltaBaseBinding,
        actual: Option<RelationalRowDeltaBaseBinding>,
    },
}

impl fmt::Display for RelationalRowDeltaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(message) => write!(formatter, "relational row delta admission failed: {message}"),
            Self::Corrupt(message) => write!(formatter, "corrupt relational row delta: {message}"),
            Self::Durability(message) => write!(formatter, "relational row delta durability failed: {message}"),
            Self::RequiresCheckpoint { tables } => write!(
                formatter,
                "relational row delta requires a schema checkpoint for tables {}",
                tables.join(",")
            ),
            Self::Invalidated(message) => write!(formatter, "relational row delta invalidated: {message}"),
            Self::Publication(error) => write!(formatter, "{error}"),
            Self::Row(error) => write!(formatter, "{error}"),
            Self::StaleGeneration {
                expected_previous,
                actual_previous,
            } => write!(
                formatter,
                "relational row delta generation changed: expected {expected_previous:?}, found {actual_previous:?}"
            ),
            Self::StaleBase { expected, actual } => write!(
                formatter,
                "relational row delta base changed: expected {expected:?}, found {actual:?}"
            ),
        }
    }
}

impl std::error::Error for RelationalRowDeltaError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Publication(error) => Some(error),
            Self::Row(error) => Some(error),
            Self::Admission(_)
            | Self::Corrupt(_)
            | Self::Durability(_)
            | Self::RequiresCheckpoint { .. }
            | Self::Invalidated(_)
            | Self::StaleGeneration { .. }
            | Self::StaleBase { .. } => None,
        }
    }
}

impl From<super::RelationalRowPagePublicationError> for RelationalRowDeltaError {
    fn from(error: super::RelationalRowPagePublicationError) -> Self {
        Self::Publication(error)
    }
}

impl From<super::RelationalRowPageError> for RelationalRowDeltaError {
    fn from(error: super::RelationalRowPageError) -> Self {
        Self::Row(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RowDeltaKey {
    table_ordinal: u32,
    encoded_primary_key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RowDeltaValue {
    is_present: bool,
    encoded_row: Vec<u8>,
    last_modified_epoch: u64,
    charged_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RowDeltaBound {
    table_ordinal: u32,
    encoded_primary_key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RowDeltaRunDescriptor {
    ordinal: u32,
    start_epoch: u64,
    end_epoch: u64,
    entry_count: u32,
    encoded_len: u64,
    descriptor_bytes: u64,
    payload_bytes: u64,
    digest: IntegrityDigest,
    lower_bound: RowDeltaBound,
    upper_bound: RowDeltaBound,
}

#[derive(Clone, Copy)]
struct RowDeltaRunContext<'a> {
    directory: &'a Path,
    base: RelationalRowDeltaBaseBinding,
    delta_generation: u64,
    schema_set_digest: Sha256Digest,
    tables: &'a [RelationalRowDeltaTableMetadata],
    config: RelationalRowDeltaConfig,
}

fn durability(context: &'static str) -> impl FnOnce(std::io::Error) -> RelationalRowDeltaError {
    move |error| RelationalRowDeltaError::Durability(format!("{context}: {error}"))
}

#[cfg(test)]
#[path = "delta/tests.rs"]
mod tests;
