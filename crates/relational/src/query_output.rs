//! Internal relational result and resource-boundary contracts shared by execution and EXPLAIN.

use crate::index_runtime::RelationalIndexExecutionEvidence;
use crate::row_runtime::RelationalRowExecutionEvidence;
use skein_core::{Result, SkeinError};
use skein_executor::binding::map_payload_bytes;
use skein_executor::{BlockingOperatorMemoryReport, QueryRows, Row};
use skein_optimizer::{
    RelationalAccessPathDescriptor, RelationalJoinPlanningOutcome,
    RelationalOperatorCardinalityProfile,
};
use skein_sql::RelationalSqlStageTimings;
use skein_storage::RelationalHydrationBudget;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalQueryLimits {
    pub max_output_rows: usize,
    pub max_output_payload_bytes: usize,
    pub max_intermediate_rows: usize,
    /// Maximum relation-join work units, including probe attempts and rows
    /// considered by a join predicate. This remains separate from rows emitted
    /// at relational operator boundaries.
    pub max_candidate_work: usize,
    pub hydration: RelationalHydrationBudget,
    pub index_read: skein_storage::RelationalIndexReadLimits,
    pub row_read: skein_storage::RelationalRowPageSnapshotReadLimits,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalQueryOutput {
    pub rows: QueryRows,
    pub stage_timings: RelationalSqlStageTimings,
    pub join_planning: RelationalJoinPlanningOutcome,
    pub operator_cardinality_profiles: Vec<RelationalOperatorCardinalityProfile>,
    pub intermediate_rows: usize,
    pub hydration: RelationalHydrationBudget,
    pub access_path: RelationalAccessPathDescriptor,
    pub join_access_paths: Vec<RelationalAccessPathDescriptor>,
    pub index_execution_evidence: Vec<RelationalIndexExecutionEvidence>,
    pub row_execution_evidence: RelationalRowExecutionEvidence,
    pub blocking_operator_memory_reports: Vec<BlockingOperatorMemoryReport>,
}

pub fn push_relational_output(
    row: Row,
    output: &mut Vec<Row>,
    payload_bytes: &mut usize,
    limits: RelationalQueryLimits,
) -> Result<()> {
    if output.len() >= limits.max_output_rows {
        return Err(SkeinError::Execution(format!(
            "relational SQL output exceeds max_output_rows {}",
            limits.max_output_rows
        )));
    }
    *payload_bytes = payload_bytes.saturating_add(map_payload_bytes(&row));
    if *payload_bytes > limits.max_output_payload_bytes {
        return Err(SkeinError::Execution(format!(
            "relational SQL output exceeds max_output_payload_bytes {}",
            limits.max_output_payload_bytes
        )));
    }
    output.push(row);
    Ok(())
}
