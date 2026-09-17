//! Storage contracts for the derived graph columnar shadow.
//!
//! The embedded facade owns checkpoint orchestration and retry state. These
//! types describe the durable shadow outcome and can be consumed without a
//! dependency on that facade.

/// Subdirectory of the database root holding the self-contained shadow.
pub const COLUMN_GROUP_SHADOW_DIR: &str = "column-groups";

/// Outcome of the shadow double-write attempted by one checkpoint.
///
/// The canonical checkpoint result reflects canonical publication only. A
/// shadow failure is reported here and leaves the dirty state available for a
/// later retry. Disabled shadows have no report.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ColumnarShadowCheckpointStatus {
    /// The shadow manifest for this checkpoint's epoch was published.
    #[default]
    Published,
    /// The shadow build or publication failed after canonical publication.
    Failed { error: String },
}

/// Write-amplification evidence for one shadow checkpoint.
///
/// A failed report carries only its status and source epoch; its remaining
/// counters are zero.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnarShadowCheckpointReport {
    /// Whether the shadow published or failed for this checkpoint.
    pub status: ColumnarShadowCheckpointStatus,
    /// Shadow manifest generation this checkpoint published.
    pub generation: u64,
    /// Storage commit epoch the checkpoint publishes.
    pub source_commit_epoch: u64,
    /// Tables referenced by the published shadow manifest.
    pub table_count: usize,
    /// Tables rebuilt because they were dirty since the previous checkpoint.
    pub dirty_table_count: usize,
    /// Untouched tables whose directory references were reused byte-for-byte.
    pub reused_table_count: usize,
    /// Immutable column-group artifact bytes written by this checkpoint.
    pub group_bytes_written: u64,
    /// Table-directory, key-dictionary, and manifest bytes written.
    pub metadata_bytes_written: u64,
    /// Peak builder footprint using the same estimates as the resource budget.
    pub peak_builder_bytes: u64,
    /// Builder-lifetime byte allowance admitted for this build, or zero when unmetered.
    pub admitted_budget_bytes: u64,
    /// Column groups flushed by this checkpoint, including budget-driven short groups.
    pub flushed_group_count: usize,
    /// Oversized rows written directly as single-row groups instead of buffered.
    pub oversized_row_group_count: usize,
    /// Superseded shadow files removed after publishing the active manifest.
    pub reclaimed_file_count: usize,
    /// Failed post-publish removals retained for a later retry.
    pub reclaim_failed_count: usize,
    /// Wall-clock time spent building and publishing the shadow.
    pub elapsed_micros: u64,
}

/// What recovery observed about the shadow catalog when it is enabled.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnarShadowRecoveryStatus {
    /// The published shadow catalog opened and validated cleanly.
    pub validated: bool,
    /// A corrupt rebuildable shadow was discarded for a later rebuild.
    pub discarded: bool,
    /// Validation error of the discarded shadow, when any.
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shadow_contract_defaults_to_an_empty_published_observation() {
        assert_eq!(
            ColumnarShadowCheckpointReport::default().status,
            ColumnarShadowCheckpointStatus::Published
        );
        assert_eq!(
            ColumnarShadowRecoveryStatus::default(),
            ColumnarShadowRecoveryStatus {
                validated: false,
                discarded: false,
                error: None,
            }
        );
    }
}
