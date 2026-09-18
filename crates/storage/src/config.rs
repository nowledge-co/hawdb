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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DurabilityPolicy {
    SyncOnCheckpoint,
    #[default]
    SyncOnEveryWrite,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RecoveryMode {
    #[default]
    Strict,
    /// Repair only a physically incomplete final WAL record on writable open.
    /// The original WAL and doctor audit must be durable before truncation.
    AutoRepairTornTail,
    /// Legacy report value; database open rejects this mode.
    DoctorRepairTornTail,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DurableCompression {
    #[default]
    Zstd,
}

pub const DEFAULT_MAX_WAL_REPLAY_ENTRIES: usize = 1_000_000;
pub const DEFAULT_MAX_WAL_REPLAY_BYTES: u64 = 1024 * 1024 * 1024;
pub const DEFAULT_MAX_WAL_QUARANTINE_BYTES: u64 = DEFAULT_MAX_WAL_REPLAY_BYTES;
pub const DEFAULT_MAX_WAL_RECORD_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_MAX_WAL_BATCH_OPERATIONS: usize = 100_000;
pub const DEFAULT_MAX_CHECKPOINT_ENCODED_BYTES: u64 = 1024 * 1024 * 1024 * 1024;
pub const DEFAULT_MAX_CHECKPOINT_DECODED_BYTES: u64 = 4 * 1024 * 1024 * 1024 * 1024;
pub const DEFAULT_SEGMENT_CACHE_CAPACITY_BYTES: u64 = 256 * 1024 * 1024;
/// Aggregate encoded graph-manifest bytes admitted during one database open.
///
/// Graph payload pages remain demand-paged. This separate bound prevents the
/// non-evictable descriptor manifests from consuming the storage cache or the
/// process memory budget before a query is admitted.
pub const DEFAULT_MAX_GRAPH_MANIFEST_OPEN_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_AUTO_MATERIALIZE_CHECKPOINT_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_MAX_OUT_OF_CORE_DELTA_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StorageResidencyMode {
    #[default]
    Auto,
    Materialized,
    OutOfCore,
}

/// Selects the relational-index implementation used by one database handle.
///
/// Persistent index publication and demand-paged reads advance together
/// through this state machine. Keeping them in one mode prevents callers from
/// selecting a reader that can never have a published generation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RelationalIndexMode {
    /// Keep materialized relational indexes as the only serving path.
    #[default]
    Materialized,
    /// Publish and recover persistent index generations without serving reads.
    Shadow,
    /// Publish persistent generations and use them for eligible SQL reads.
    DemandPaged,
    /// Require a current persistent view for SQL reads and constraint checks.
    ///
    /// This mode is deliberately opt-in. Missing, stale, corrupt, or
    /// admission-unavailable required indexes fail closed instead of falling
    /// back to materialized postings.
    Authoritative,
}

impl RelationalIndexMode {
    pub const fn publishes_persistent_indexes(self) -> bool {
        !matches!(self, Self::Materialized)
    }

    pub const fn serves_demand_paged_reads(self) -> bool {
        matches!(self, Self::DemandPaged | Self::Authoritative)
    }

    pub const fn requires_authoritative_indexes(self) -> bool {
        matches!(self, Self::Authoritative)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalReplayConfig {
    pub recovery_mode: RecoveryMode,
    pub max_entries: Option<usize>,
    pub max_bytes: Option<u64>,
    /// Maximum aggregate bytes retained under the automatic corrupt-WAL
    /// quarantine directory. A corrupt WAL larger than this bound remains in
    /// place and is not copied.
    pub max_quarantine_bytes: u64,
    pub max_record_bytes: Option<usize>,
    pub max_batch_operations: Option<usize>,
    pub max_checkpoint_encoded_bytes: Option<u64>,
    pub max_checkpoint_decoded_bytes: Option<u64>,
    pub segment_cache_capacity_bytes: u64,
    pub max_graph_manifest_open_bytes: u64,
    pub residency_mode: StorageResidencyMode,
    pub auto_materialize_checkpoint_bytes: u64,
    pub max_out_of_core_delta_bytes: Option<u64>,
    /// Derived columnar shadow double-write: checkpoints additionally
    /// publish a column-group catalog under `column-groups/` and recovery
    /// validates it. Off by default; reads are never served from the shadow.
    pub graph_columnar_shadow_checkpoint: bool,
    /// Persistent relational-index publication and read activation mode.
    pub relational_index_mode: RelationalIndexMode,
}

impl Default for WalReplayConfig {
    fn default() -> Self {
        Self {
            recovery_mode: RecoveryMode::default(),
            max_entries: Some(DEFAULT_MAX_WAL_REPLAY_ENTRIES),
            max_bytes: Some(DEFAULT_MAX_WAL_REPLAY_BYTES),
            max_quarantine_bytes: DEFAULT_MAX_WAL_QUARANTINE_BYTES,
            max_record_bytes: Some(DEFAULT_MAX_WAL_RECORD_BYTES),
            max_batch_operations: Some(DEFAULT_MAX_WAL_BATCH_OPERATIONS),
            max_checkpoint_encoded_bytes: Some(DEFAULT_MAX_CHECKPOINT_ENCODED_BYTES),
            max_checkpoint_decoded_bytes: Some(DEFAULT_MAX_CHECKPOINT_DECODED_BYTES),
            segment_cache_capacity_bytes: DEFAULT_SEGMENT_CACHE_CAPACITY_BYTES,
            max_graph_manifest_open_bytes: DEFAULT_MAX_GRAPH_MANIFEST_OPEN_BYTES,
            residency_mode: StorageResidencyMode::Auto,
            auto_materialize_checkpoint_bytes: DEFAULT_AUTO_MATERIALIZE_CHECKPOINT_BYTES,
            max_out_of_core_delta_bytes: Some(DEFAULT_MAX_OUT_OF_CORE_DELTA_BYTES),
            graph_columnar_shadow_checkpoint: false,
            relational_index_mode: RelationalIndexMode::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_bounded_and_recoverable() {
        assert_eq!(
            DurabilityPolicy::default(),
            DurabilityPolicy::SyncOnEveryWrite
        );
        assert_eq!(RecoveryMode::default(), RecoveryMode::Strict);
        assert_eq!(DurableCompression::default(), DurableCompression::Zstd);
        assert_eq!(
            RelationalIndexMode::default(),
            RelationalIndexMode::Materialized
        );
        assert!(!RelationalIndexMode::Materialized.publishes_persistent_indexes());
        assert!(!RelationalIndexMode::Materialized.serves_demand_paged_reads());
        assert!(RelationalIndexMode::Shadow.publishes_persistent_indexes());
        assert!(!RelationalIndexMode::Shadow.serves_demand_paged_reads());
        assert!(RelationalIndexMode::DemandPaged.publishes_persistent_indexes());
        assert!(RelationalIndexMode::DemandPaged.serves_demand_paged_reads());
        assert!(!RelationalIndexMode::DemandPaged.requires_authoritative_indexes());
        assert!(RelationalIndexMode::Authoritative.publishes_persistent_indexes());
        assert!(RelationalIndexMode::Authoritative.serves_demand_paged_reads());
        assert!(RelationalIndexMode::Authoritative.requires_authoritative_indexes());
        let replay = WalReplayConfig::default();
        assert_eq!(replay.max_entries, Some(DEFAULT_MAX_WAL_REPLAY_ENTRIES));
        assert_eq!(replay.max_bytes, Some(DEFAULT_MAX_WAL_REPLAY_BYTES));
        assert_eq!(
            replay.max_quarantine_bytes,
            DEFAULT_MAX_WAL_QUARANTINE_BYTES
        );
        assert_eq!(replay.max_record_bytes, Some(DEFAULT_MAX_WAL_RECORD_BYTES));
        assert_eq!(
            replay.max_batch_operations,
            Some(DEFAULT_MAX_WAL_BATCH_OPERATIONS)
        );
        assert_eq!(
            replay.max_out_of_core_delta_bytes,
            Some(DEFAULT_MAX_OUT_OF_CORE_DELTA_BYTES)
        );
        assert_eq!(
            replay.max_graph_manifest_open_bytes,
            DEFAULT_MAX_GRAPH_MANIFEST_OPEN_BYTES
        );
    }
}
