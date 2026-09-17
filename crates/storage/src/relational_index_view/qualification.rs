//! Relational index view qualification report contracts.

use crate::relational_index_view::RelationalIndexReadViewReport;
use crate::{RelationalIndexReadLimits, RelationalKey};
use std::num::NonZeroUsize;

pub const RELATIONAL_INDEX_VIEW_QUALIFICATION_PROTOCOL: &str =
    "skein-relational-index-view-qualification-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RelationalIndexQualificationProbeKind {
    Exact,
    LeadingPrefix,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalIndexViewQualificationOptions {
    pub max_tables: NonZeroUsize,
    pub max_rows_per_table: NonZeroUsize,
    pub max_probes: NonZeroUsize,
    pub read_limits: RelationalIndexReadLimits,
}

impl Default for RelationalIndexViewQualificationOptions {
    fn default() -> Self {
        Self {
            max_tables: NonZeroUsize::new(64).expect("default table limit is non-zero"),
            max_rows_per_table: NonZeroUsize::new(8).expect("default row sample limit is non-zero"),
            max_probes: NonZeroUsize::new(512).expect("default probe limit is non-zero"),
            read_limits: RelationalIndexReadLimits::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalIndexQualificationProbeReport {
    pub ordinal: usize,
    pub table: String,
    pub index: String,
    pub kind: RelationalIndexQualificationProbeKind,
    pub candidate_rows: usize,
    pub oracle_rows: usize,
    pub candidate_digest: String,
    pub oracle_digest: String,
    pub matched: bool,
    pub read: RelationalIndexReadViewReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalIndexViewQualificationReport {
    pub protocol: &'static str,
    pub base_generation: u64,
    pub delta_generation: Option<u64>,
    pub base_commit_epoch: u64,
    pub visible_commit_epoch: u64,
    pub root_set_digest: String,
    pub tables_discovered: usize,
    pub tables_sampled: usize,
    pub rows_sampled: usize,
    pub indexes_discovered: usize,
    pub indexes_probed: usize,
    pub probes: Vec<RelationalIndexQualificationProbeReport>,
    pub mismatches: usize,
    pub truncated: bool,
    pub ready: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RelationalIndexQualificationProbe {
    pub table: String,
    pub index: String,
    pub kind: RelationalIndexQualificationProbeKind,
    pub key: RelationalKey,
}
