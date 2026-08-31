use super::super::QueryOutput;
pub use crate::relational_sql::{
    RelationalJoinPlanningAttempt, RelationalJoinPlanningBudget, RelationalJoinPlanningCost,
    RelationalJoinPlanningFallbackClass, RelationalJoinPlanningOutcome,
    RelationalJoinPlanningReason, RelationalJoinPlanningStatus, RelationalJoinPlanningStrategy,
    RelationalOperatorCardinalityProfile, RelationalOperatorId, RelationalOperatorKind,
    RelationalSqlStageTimings,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfiledRelationalSqlQueryOutput {
    pub output: QueryOutput,
    pub profile: RelationalSqlReadProfile,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalSqlReadProfile {
    pub stage_timings: RelationalSqlStageTimings,
    pub join_planning: RelationalJoinPlanningOutcome,
    /// Base access followed by join operators in plan execution order.
    pub operator_cardinality_profiles: Vec<RelationalOperatorCardinalityProfile>,
    pub intermediate_rows: usize,
    pub hydrated_rows: usize,
    pub hydrated_compressed_bytes: usize,
    pub hydrated_decompressed_bytes: usize,
    pub index_reads: Vec<RelationalSqlIndexReadProfile>,
    pub row_read: RelationalSqlRowReadProfile,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalSqlIndexReadProfile {
    pub table: String,
    pub index: String,
    pub runtime_path: String,
    pub logical_pages: usize,
    pub logical_bytes: usize,
    pub physical_pages: usize,
    pub physical_bytes: usize,
    pub cache_hits: usize,
    pub cache_misses: usize,
    pub cache_admission_rejections: usize,
    pub rows_visited: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalSqlRowReadProfile {
    pub runtime_path: String,
    pub base_generation: Option<u64>,
    pub delta_generation: Option<u64>,
    pub base_commit_epoch: Option<u64>,
    pub visible_commit_epoch: Option<u64>,
    pub root_set_digest: Option<String>,
    pub descriptor_reads: usize,
    pub logical_pages: usize,
    pub logical_bytes: usize,
    pub physical_pages: usize,
    pub physical_bytes: usize,
    pub cache_hits: usize,
    pub cache_misses: usize,
    pub cache_admission_rejections: usize,
    pub rows_visited: usize,
    pub overlay_entries: usize,
    pub overlay_resident_bytes: usize,
}
