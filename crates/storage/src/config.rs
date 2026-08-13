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
    DoctorRepairTornTail,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DurableCompression {
    #[default]
    Zstd,
}

pub const DEFAULT_MAX_WAL_REPLAY_ENTRIES: usize = 1_000_000;
pub const DEFAULT_MAX_WAL_REPLAY_BYTES: u64 = 1024 * 1024 * 1024;
pub const DEFAULT_MAX_WAL_RECORD_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_MAX_WAL_BATCH_OPERATIONS: usize = 100_000;
pub const DEFAULT_MAX_CHECKPOINT_ENCODED_BYTES: u64 = 1024 * 1024 * 1024 * 1024;
pub const DEFAULT_MAX_CHECKPOINT_DECODED_BYTES: u64 = 4 * 1024 * 1024 * 1024 * 1024;
pub const DEFAULT_SEGMENT_CACHE_CAPACITY_BYTES: u64 = 256 * 1024 * 1024;
pub const DEFAULT_AUTO_MATERIALIZE_CHECKPOINT_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_MAX_OUT_OF_CORE_DELTA_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StorageResidencyMode {
    #[default]
    Auto,
    Materialized,
    OutOfCore,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalReplayConfig {
    pub recovery_mode: RecoveryMode,
    pub max_entries: Option<usize>,
    pub max_bytes: Option<u64>,
    pub max_record_bytes: Option<usize>,
    pub max_batch_operations: Option<usize>,
    pub max_checkpoint_encoded_bytes: Option<u64>,
    pub max_checkpoint_decoded_bytes: Option<u64>,
    pub segment_cache_capacity_bytes: u64,
    pub residency_mode: StorageResidencyMode,
    pub auto_materialize_checkpoint_bytes: u64,
    pub max_out_of_core_delta_bytes: Option<u64>,
    /// Columnar shadow double-write (spec §3.7): checkpoints additionally
    /// publish a column-group catalog under `column-groups/` and recovery
    /// validates it. Off by default; reads are never served from the shadow.
    pub graph_columnar_shadow_checkpoint: bool,
}

impl Default for WalReplayConfig {
    fn default() -> Self {
        Self {
            recovery_mode: RecoveryMode::default(),
            max_entries: Some(DEFAULT_MAX_WAL_REPLAY_ENTRIES),
            max_bytes: Some(DEFAULT_MAX_WAL_REPLAY_BYTES),
            max_record_bytes: Some(DEFAULT_MAX_WAL_RECORD_BYTES),
            max_batch_operations: Some(DEFAULT_MAX_WAL_BATCH_OPERATIONS),
            max_checkpoint_encoded_bytes: Some(DEFAULT_MAX_CHECKPOINT_ENCODED_BYTES),
            max_checkpoint_decoded_bytes: Some(DEFAULT_MAX_CHECKPOINT_DECODED_BYTES),
            segment_cache_capacity_bytes: DEFAULT_SEGMENT_CACHE_CAPACITY_BYTES,
            residency_mode: StorageResidencyMode::Auto,
            auto_materialize_checkpoint_bytes: DEFAULT_AUTO_MATERIALIZE_CHECKPOINT_BYTES,
            max_out_of_core_delta_bytes: Some(DEFAULT_MAX_OUT_OF_CORE_DELTA_BYTES),
            graph_columnar_shadow_checkpoint: false,
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
        let replay = WalReplayConfig::default();
        assert_eq!(replay.max_entries, Some(DEFAULT_MAX_WAL_REPLAY_ENTRIES));
        assert_eq!(replay.max_bytes, Some(DEFAULT_MAX_WAL_REPLAY_BYTES));
        assert_eq!(replay.max_record_bytes, Some(DEFAULT_MAX_WAL_RECORD_BYTES));
        assert_eq!(
            replay.max_batch_operations,
            Some(DEFAULT_MAX_WAL_BATCH_OPERATIONS)
        );
        assert_eq!(
            replay.max_out_of_core_delta_bytes,
            Some(DEFAULT_MAX_OUT_OF_CORE_DELTA_BYTES)
        );
    }
}
