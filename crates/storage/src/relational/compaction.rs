//! Relational compaction configuration and report contracts.

use super::{
    RelationalOverflowReferenceSortConfig, RelationalRowPagePublicationConfig,
    RelationalRowPageRewriteConfig,
};
use crate::{CanonicalAdjacencyConfig, CanonicalSegmentConfig, PersistentPropertyProjectionConfig};
use skein_core::{Result, SkeinError};
use std::num::{NonZeroU64, NonZeroUsize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalRowPageCompactionConfig {
    pub rewrite: RelationalRowPageRewriteConfig,
    pub max_dirty_pages: NonZeroUsize,
    pub max_dirty_bytes: NonZeroU64,
    /// Allowance for existing materialized checkpoint sidecars, not row-page
    /// relocation. Out-of-core row pages are streamed independently of this cap.
    pub max_materialized_checkpoint_bytes: NonZeroU64,
}

impl Default for RelationalRowPageCompactionConfig {
    fn default() -> Self {
        Self {
            rewrite: RelationalRowPageRewriteConfig::default(),
            max_dirty_pages: NonZeroUsize::new(128).unwrap(),
            max_dirty_bytes: NonZeroU64::new(16 * 1024 * 1024).unwrap(),
            max_materialized_checkpoint_bytes: NonZeroU64::new(64 * 1024 * 1024).unwrap(),
        }
    }
}

impl RelationalRowPageCompactionConfig {
    fn publication_config(self) -> RelationalRowPagePublicationConfig {
        RelationalRowPagePublicationConfig {
            max_dirty_pages: self.max_dirty_pages,
            max_dirty_bytes: self.max_dirty_bytes,
            ..RelationalRowPagePublicationConfig::default()
        }
    }

    /// Estimated transient reservation for row planning and checkpoint writers.
    /// Materialized sidecars and enabled columnar shadow add their own allowance.
    /// This is not allocator/RSS accounting.
    pub fn admission_bytes(self) -> Result<u64> {
        let publication = self.publication_config();
        let adjacency = CanonicalAdjacencyConfig::default();
        let projection = PersistentPropertyProjectionConfig::default();
        let canonical = CanonicalSegmentConfig::default();
        self.max_dirty_bytes
            .get()
            .checked_mul(4)
            .and_then(|bytes| {
                bytes.checked_add((publication.max_manifest_bytes.get() as u64).saturating_mul(4))
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    (publication.page_limits.max_page_bytes.get() as u64).saturating_mul(4),
                )
            })
            .and_then(|bytes| bytes.checked_add(adjacency.memory_budget_bytes.get()))
            .and_then(|bytes| bytes.checked_add(projection.memory_budget_bytes.get()))
            .and_then(|bytes| bytes.checked_add(projection.max_definition_bytes.get()))
            .and_then(|bytes| bytes.checked_add(canonical.max_record_bytes.get().saturating_mul(2)))
            .and_then(|bytes| {
                bytes.checked_add(canonical.target_segment_bytes.get().saturating_mul(2))
            })
            .ok_or_else(|| {
                SkeinError::Storage("row-page compaction admission byte count overflow".to_string())
            })
    }
}

/// Internal bridge for the embedded facade's checkpoint writer.
#[doc(hidden)]
pub fn relational_row_page_compaction_publication_config(
    config: RelationalRowPageCompactionConfig,
) -> RelationalRowPagePublicationConfig {
    config.publication_config()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalRowPageCompactionReport {
    pub source_commit_epoch: u64,
    pub published_generation: u64,
    pub root_pages: u64,
    pub dirty_pages_written: u64,
    pub relocated_pages_written: u64,
    pub reused_pages: u64,
    pub previous_allocated_pages: u64,
    pub allocated_pages: u64,
    pub admitted_memory_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalOverflowCompactionConfig {
    pub max_scan_rows: NonZeroUsize,
    pub max_scan_pages: NonZeroUsize,
    pub max_scan_bytes: NonZeroUsize,
    pub max_overlay_entries: NonZeroUsize,
    pub max_overlay_bytes: NonZeroUsize,
    pub max_rewrite_bytes: NonZeroU64,
    pub reference_sort: RelationalOverflowReferenceSortConfig,
}

impl Default for RelationalOverflowCompactionConfig {
    fn default() -> Self {
        Self {
            max_scan_rows: NonZeroUsize::new(100_000_000)
                .expect("default overflow compaction row limit is non-zero"),
            max_scan_pages: NonZeroUsize::new(1_000_000)
                .expect("default overflow compaction page limit is non-zero"),
            max_scan_bytes: NonZeroUsize::new(1024usize.saturating_mul(1024 * 1024 * 1024))
                .expect("default overflow compaction read-byte limit is non-zero"),
            max_overlay_entries: NonZeroUsize::new(
                crate::DEFAULT_RELATIONAL_ROW_SNAPSHOT_OVERLAY_ENTRIES,
            )
            .expect("default overflow compaction overlay entry limit is non-zero"),
            max_overlay_bytes: NonZeroUsize::new(
                crate::DEFAULT_RELATIONAL_ROW_SNAPSHOT_OVERLAY_BYTES,
            )
            .expect("default overflow compaction overlay byte limit is non-zero"),
            max_rewrite_bytes: NonZeroU64::new(128 * 1024 * 1024 * 1024)
                .expect("default overflow compaction rewrite limit is non-zero"),
            reference_sort: RelationalOverflowReferenceSortConfig::default(),
        }
    }
}

impl RelationalOverflowCompactionConfig {
    pub fn admission_bytes(self) -> Result<u64> {
        let sort_bytes =
            u64::try_from(self.reference_sort.max_memory_bytes.get()).map_err(|_| {
                SkeinError::Storage(
                    "overflow compaction sort memory exceeds this target".to_string(),
                )
            })?;
        let overlay_bytes = u64::try_from(self.max_overlay_bytes.get()).map_err(|_| {
            SkeinError::Storage(
                "overflow compaction overlay memory exceeds this target".to_string(),
            )
        })?;
        let page_bytes = crate::DEFAULT_RELATIONAL_ROW_PAGE_BYTES as u64;
        sort_bytes
            .checked_add(overlay_bytes)
            .and_then(|bytes| bytes.checked_add(page_bytes.saturating_mul(2)))
            .and_then(|bytes| {
                bytes.checked_add(
                    (crate::DEFAULT_MAX_RELATIONAL_HYDRATION_BYTES as u64).saturating_mul(2),
                )
            })
            .ok_or_else(|| {
                SkeinError::Storage("overflow compaction admission byte count overflow".to_string())
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalOverflowCompactionReport {
    pub source_commit_epoch: u64,
    pub published_generation: u64,
    pub tables_scanned: usize,
    pub rows_scanned: usize,
    pub pages_read: usize,
    pub row_bytes_read: usize,
    pub hydrated_values: usize,
    pub overlay_entries: usize,
    pub overlay_bytes: usize,
    pub reference_occurrences: u64,
    pub unique_references: u64,
    pub spill_run_count: usize,
    pub spill_bytes: u64,
    pub peak_sort_memory_bytes: usize,
    pub previous_extent_count: u64,
    pub published_extent_count: u64,
    pub reclaimable_base_extent_count: u64,
    pub new_extent_count: u64,
    pub reused_extent_count: u64,
    pub copied_base_extent_count: u64,
    pub introduced_extent_count: u64,
    pub admitted_memory_bytes: u64,
}
