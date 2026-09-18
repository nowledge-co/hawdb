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

//! Execution memory defaults and admission estimates.

use hawdb_core::{HawDBError, Result, RuntimeTaskContext};
use hawdb_plan::{PhysicalPlan, PlanChildren, VectorExecutionResourceProfile};
use hawdb_storage::MutationLimits;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::time::Duration;

#[doc(hidden)]
pub fn enforced_query_memory_budget(
    memory: &ExecutionMemoryConfig,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<NonZeroUsize> {
    let Some(reservation) = task_context.and_then(RuntimeTaskContext::memory_reservation) else {
        return Ok(memory.query_memory_bytes);
    };
    // Never widen an undersized admission to an operator-configured floor.
    // The shared root remains the admitted reservation and the operator fails
    // closed when its first charge cannot fit.
    runtime_memory_budget("query memory", reservation.memory_bytes())
}

#[doc(hidden)]
pub fn enforced_result_memory_budget(
    memory: &ExecutionMemoryConfig,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<NonZeroUsize> {
    let Some(reservation) = task_context.and_then(RuntimeTaskContext::memory_reservation) else {
        return Ok(memory.query_memory_bytes);
    };
    let admitted = runtime_memory_budget("query result", reservation.result_bytes())?;
    Ok(admitted.min(memory.query_memory_bytes))
}

fn runtime_memory_budget(owner: &str, bytes: u64) -> Result<NonZeroUsize> {
    let bytes = usize::try_from(bytes).map_err(|_| {
        HawDBError::Execution(format!(
            "runtime-admitted {owner} reservation {bytes} does not fit the executor address space"
        ))
    })?;
    NonZeroUsize::new(bytes).ok_or_else(|| {
        HawDBError::Execution(format!(
            "runtime-admitted {owner} reservation must be non-zero"
        ))
    })
}

#[doc(hidden)]
pub const DEFAULT_EXECUTION_BATCH_ROWS: usize = 256;
const DEFAULT_EXECUTION_BATCH_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_QUERY_MEMORY_BYTES: usize = 256 * 1024 * 1024;
#[doc(hidden)]
pub const DEFAULT_BLOCKING_OPERATOR_MEMORY_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_EXECUTION_MAX_SPILL_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const DEFAULT_EXECUTION_MAX_SPILL_RUNS: usize = 128;
const DEFAULT_EXECUTION_MAX_TOTAL_SPILL_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const DEFAULT_EXECUTION_MAX_TOTAL_SPILL_RUNS: usize = 512;
const DEFAULT_EXECUTION_MIN_SPILL_FREE_BYTES: u64 = 1024 * 1024 * 1024;
const DEFAULT_SPILL_FREE_SPACE_PROBE_INTERVAL_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_SPILL_ORPHAN_GRACE_PERIOD: Duration = Duration::from_secs(24 * 60 * 60);
const MUTATION_OPERATION_BOOKKEEPING_BYTES: u64 = 64;
const MUTATION_AFFECTED_ROW_BOOKKEEPING_BYTES: u64 = 16;
const MUTATION_RESULT_ROW_BOOKKEEPING_BYTES: u64 = 64;
#[doc(hidden)]
pub const SOURCE_SEGMENT_SCAN_MAX_WAVE_BYTES: u64 = 2 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionMemoryConfig {
    /// Maximum tracked resident bytes owned by one query across all operators.
    pub query_memory_bytes: NonZeroUsize,
    /// Maximum row count in an executor-owned transfer batch.
    pub batch_rows: NonZeroUsize,
    /// Maximum estimated resident bytes in an executor-owned transfer batch.
    pub batch_payload_bytes: NonZeroUsize,
    /// Maximum estimated resident bytes retained by one blocking operator.
    pub blocking_operator_bytes: NonZeroUsize,
    /// Maximum cumulative serialized spill bytes, including merge passes.
    pub max_spill_bytes: NonZeroU64,
    /// Maximum cumulative spill runs created, including merge passes.
    pub max_spill_runs: NonZeroUsize,
    /// Maximum live spill bytes shared by all queries using the spill directory.
    pub max_total_spill_bytes: NonZeroU64,
    /// Maximum live spill runs shared by all queries using the spill directory.
    pub max_total_spill_runs: NonZeroUsize,
    /// Free space preserved on the filesystem containing the spill directory.
    pub min_spill_free_bytes: NonZeroU64,
    /// Reserved bytes between filesystem free-space probes for one spill pool.
    pub spill_free_space_probe_interval_bytes: NonZeroU64,
    /// Minimum age before a spill file from an earlier process is removed.
    pub spill_orphan_grace_period: Duration,
    /// Directory governed as one shared spill pool.
    pub spill_directory: PathBuf,
}

impl Default for ExecutionMemoryConfig {
    fn default() -> Self {
        Self {
            query_memory_bytes: NonZeroUsize::new(DEFAULT_QUERY_MEMORY_BYTES)
                .expect("default query memory budget is non-zero"),
            batch_rows: NonZeroUsize::new(DEFAULT_EXECUTION_BATCH_ROWS)
                .expect("default execution batch size is non-zero"),
            batch_payload_bytes: NonZeroUsize::new(DEFAULT_EXECUTION_BATCH_PAYLOAD_BYTES)
                .expect("default execution batch byte size is non-zero"),
            blocking_operator_bytes: NonZeroUsize::new(DEFAULT_BLOCKING_OPERATOR_MEMORY_BYTES)
                .expect("default blocking operator memory budget is non-zero"),
            max_spill_bytes: NonZeroU64::new(DEFAULT_EXECUTION_MAX_SPILL_BYTES)
                .expect("default spill byte budget is non-zero"),
            max_spill_runs: NonZeroUsize::new(DEFAULT_EXECUTION_MAX_SPILL_RUNS)
                .expect("default spill run budget is non-zero"),
            max_total_spill_bytes: NonZeroU64::new(DEFAULT_EXECUTION_MAX_TOTAL_SPILL_BYTES)
                .expect("default total spill byte budget is non-zero"),
            max_total_spill_runs: NonZeroUsize::new(DEFAULT_EXECUTION_MAX_TOTAL_SPILL_RUNS)
                .expect("default total spill run budget is non-zero"),
            min_spill_free_bytes: NonZeroU64::new(DEFAULT_EXECUTION_MIN_SPILL_FREE_BYTES)
                .expect("default spill free-space reserve is non-zero"),
            spill_free_space_probe_interval_bytes: NonZeroU64::new(
                DEFAULT_SPILL_FREE_SPACE_PROBE_INTERVAL_BYTES,
            )
            .expect("default spill free-space probe interval is non-zero"),
            spill_orphan_grace_period: DEFAULT_SPILL_ORPHAN_GRACE_PERIOD,
            spill_directory: std::env::temp_dir().join("hawdb-spill"),
        }
    }
}

impl ExecutionMemoryConfig {
    /// Returns process-wide spill usage for this configured directory.
    pub fn spill_pool_snapshot(&self) -> hawdb_core::Result<crate::SpillPoolSnapshot> {
        crate::spill::spill_pool_snapshot(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct ExecutionMemoryEstimate {
    pub pipeline_batch_count: usize,
    pub blocking_operator_count: usize,
    pub pipeline_bytes: u64,
    pub blocking_bytes: u64,
    pub external_read_bytes: u64,
    pub fixed_operator_bytes: u64,
    pub total_bytes: u64,
}

#[doc(hidden)]
pub fn estimated_execution_memory(
    plan: &PhysicalPlan,
    memory: &ExecutionMemoryConfig,
) -> ExecutionMemoryEstimate {
    let shape = peak_execution_memory_shape(plan, memory);
    let pipeline_bytes = usize_to_u64(shape.pipeline_batch_count)
        .saturating_mul(usize_to_u64(memory.batch_payload_bytes.get()));
    let blocking_bytes = usize_to_u64(shape.blocking_operator_count)
        .saturating_mul(usize_to_u64(memory.blocking_operator_bytes.get()));
    let external_read_bytes = shape.external_read_bytes;
    let fixed_operator_bytes = shape.fixed_operator_bytes;
    ExecutionMemoryEstimate {
        pipeline_batch_count: shape.pipeline_batch_count,
        blocking_operator_count: shape.blocking_operator_count,
        pipeline_bytes,
        blocking_bytes,
        external_read_bytes,
        fixed_operator_bytes,
        total_bytes: pipeline_bytes
            .saturating_add(blocking_bytes)
            .saturating_add(external_read_bytes)
            .saturating_add(fixed_operator_bytes),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct ExternalReadMemoryBudget {
    pub max_working_bytes: NonZeroUsize,
    pub max_result_bytes: NonZeroUsize,
}

impl ExternalReadMemoryBudget {
    pub fn reserved_bytes(self) -> usize {
        self.max_working_bytes
            .get()
            .saturating_add(self.max_result_bytes.get())
    }
}

#[doc(hidden)]
pub fn external_read_memory_budget(
    profile: VectorExecutionResourceProfile,
    memory: &ExecutionMemoryConfig,
) -> ExternalReadMemoryBudget {
    let configured_working_bytes = memory.blocking_operator_bytes.get();
    let planned_working_bytes = profile
        .max_working_memory_bytes
        .map(|bytes| usize::try_from(bytes).unwrap_or(usize::MAX))
        .unwrap_or(configured_working_bytes)
        .max(1);
    ExternalReadMemoryBudget {
        max_working_bytes: NonZeroUsize::new(planned_working_bytes.min(configured_working_bytes))
            .expect("configured external read working budget is non-zero"),
        max_result_bytes: memory.blocking_operator_bytes,
    }
}

#[doc(hidden)]
pub fn max_external_read_parallelism(plan: &PhysicalPlan) -> usize {
    let own_parallelism = match plan {
        PhysicalPlan::VectorSeedScan {
            resource_profile, ..
        } => resource_profile.max_parallelism.max(1),
        _ => 1,
    };
    match plan.children() {
        PlanChildren::None => own_parallelism,
        PlanChildren::Unary(input) => own_parallelism.max(max_external_read_parallelism(input)),
        PlanChildren::Binary(left, right) => own_parallelism
            .max(max_external_read_parallelism(left))
            .max(max_external_read_parallelism(right)),
    }
}

#[doc(hidden)]
pub fn estimated_mutation_memory_bytes(
    limits: MutationLimits,
    max_wal_record_bytes: Option<usize>,
) -> u64 {
    let Some(max_wal_record_bytes) = max_wal_record_bytes else {
        return u64::MAX;
    };
    usize_to_u64(max_wal_record_bytes)
        .saturating_mul(2)
        .saturating_add(
            usize_to_u64(limits.max_operations.get())
                .saturating_mul(MUTATION_OPERATION_BOOKKEEPING_BYTES),
        )
        .saturating_add(
            usize_to_u64(limits.max_affected_rows.get())
                .saturating_mul(MUTATION_AFFECTED_ROW_BOOKKEEPING_BYTES),
        )
        .saturating_add(
            usize_to_u64(limits.max_result_rows.get())
                .saturating_mul(MUTATION_RESULT_ROW_BOOKKEEPING_BYTES),
        )
        .saturating_add(usize_to_u64(limits.max_result_payload_bytes.get()))
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ExecutionMemoryShape {
    pipeline_batch_count: usize,
    blocking_operator_count: usize,
    external_read_bytes: u64,
    fixed_operator_bytes: u64,
}

fn peak_execution_memory_shape(
    plan: &PhysicalPlan,
    memory: &ExecutionMemoryConfig,
) -> ExecutionMemoryShape {
    let mut shape = match plan.children() {
        PlanChildren::None => ExecutionMemoryShape::default(),
        PlanChildren::Unary(input) => peak_execution_memory_shape(input, memory),
        PlanChildren::Binary(left, right) => peak_shape_max(
            peak_execution_memory_shape(left, memory),
            peak_execution_memory_shape(right, memory),
            memory,
        ),
    };
    shape.pipeline_batch_count = shape.pipeline_batch_count.saturating_add(1);
    if retains_blocking_state(plan) {
        shape.blocking_operator_count = shape.blocking_operator_count.saturating_add(1);
    }
    if matches!(plan, PhysicalPlan::SourceSegmentScan { .. }) {
        shape.fixed_operator_bytes = shape
            .fixed_operator_bytes
            .saturating_add(SOURCE_SEGMENT_SCAN_MAX_WAVE_BYTES);
    }
    if let PhysicalPlan::VectorSeedScan {
        resource_profile, ..
    } = plan
    {
        shape.external_read_bytes = shape.external_read_bytes.saturating_add(usize_to_u64(
            external_read_memory_budget(*resource_profile, memory).reserved_bytes(),
        ));
    }
    shape
}

fn peak_shape_max(
    left: ExecutionMemoryShape,
    right: ExecutionMemoryShape,
    memory: &ExecutionMemoryConfig,
) -> ExecutionMemoryShape {
    let shape_bytes = |shape: ExecutionMemoryShape| {
        usize_to_u64(shape.pipeline_batch_count)
            .saturating_mul(usize_to_u64(memory.batch_payload_bytes.get()))
            .saturating_add(
                usize_to_u64(shape.blocking_operator_count)
                    .saturating_mul(usize_to_u64(memory.blocking_operator_bytes.get())),
            )
            .saturating_add(shape.external_read_bytes)
            .saturating_add(shape.fixed_operator_bytes)
    };
    let left_total = shape_bytes(left);
    let right_total = shape_bytes(right);
    if left_total > right_total
        || (left_total == right_total && left.fixed_operator_bytes >= right.fixed_operator_bytes)
    {
        left
    } else {
        right
    }
}

fn retains_blocking_state(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::GraphAlgorithm { .. }
            | PhysicalPlan::VectorSeedScan { .. }
            | PhysicalPlan::SourceSegmentScan { .. }
            | PhysicalPlan::NodeCartesianProductExec { .. }
            | PhysicalPlan::HashJoinExec { .. }
            | PhysicalPlan::AdjacencyExpandExec { .. }
            | PhysicalPlan::OptionalRelationshipCountSumExec { .. }
            | PhysicalPlan::ThreadRepairStatsExec { .. }
            | PhysicalPlan::ShortestPathExec { .. }
            | PhysicalPlan::AggregateExec { .. }
            | PhysicalPlan::DistinctExec { .. }
            | PhysicalPlan::SortExec { .. }
            | PhysicalPlan::TopNExec { .. }
    )
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hawdb_plan::Predicate;

    fn admission_test_config() -> ExecutionMemoryConfig {
        ExecutionMemoryConfig {
            query_memory_bytes: NonZeroUsize::new(16 * 1024).unwrap(),
            batch_rows: NonZeroUsize::new(8).unwrap(),
            batch_payload_bytes: NonZeroUsize::new(1024).unwrap(),
            blocking_operator_bytes: NonZeroUsize::new(4096).unwrap(),
            max_spill_bytes: NonZeroU64::new(1024 * 1024).unwrap(),
            max_spill_runs: NonZeroUsize::new(8).unwrap(),
            max_total_spill_bytes: NonZeroU64::new(4 * 1024 * 1024).unwrap(),
            max_total_spill_runs: NonZeroUsize::new(32).unwrap(),
            min_spill_free_bytes: NonZeroU64::new(1).unwrap(),
            spill_free_space_probe_interval_bytes: NonZeroU64::new(1024).unwrap(),
            spill_orphan_grace_period: Duration::from_secs(60),
            spill_directory: std::env::temp_dir(),
        }
    }

    #[test]
    fn execution_admission_uses_configured_pipeline_and_blocking_budgets() {
        let plan = PhysicalPlan::SortExec {
            items: Vec::new(),
            input: Box::new(PhysicalPlan::DistinctExec {
                input: Box::new(PhysicalPlan::SeqNodeScan {
                    variable: "n".to_string(),
                    label: "Node".to_string(),
                }),
            }),
        };

        let estimate = estimated_execution_memory(&plan, &admission_test_config());

        assert_eq!(estimate.pipeline_batch_count, 3);
        assert_eq!(estimate.blocking_operator_count, 2);
        assert_eq!(estimate.pipeline_bytes, 3 * 1024);
        assert_eq!(estimate.blocking_bytes, 2 * 4096);
        assert_eq!(estimate.external_read_bytes, 0);
        assert_eq!(estimate.fixed_operator_bytes, 0);
        assert_eq!(estimate.total_bytes, 11 * 1024);
    }

    #[test]
    fn binary_admission_uses_the_higher_memory_child_path() {
        let plan = PhysicalPlan::NodeCartesianProductExec {
            left: Box::new(PhysicalPlan::ProjectExec {
                items: Vec::new(),
                input: Box::new(PhysicalPlan::ProjectExec {
                    items: Vec::new(),
                    input: Box::new(PhysicalPlan::SeqNodeScan {
                        variable: "left".to_string(),
                        label: "Left".to_string(),
                    }),
                }),
            }),
            right: Box::new(PhysicalPlan::DistinctExec {
                input: Box::new(PhysicalPlan::SeqNodeScan {
                    variable: "right".to_string(),
                    label: "Right".to_string(),
                }),
            }),
        };

        let estimate = estimated_execution_memory(&plan, &admission_test_config());

        assert_eq!(estimate.pipeline_batch_count, 3);
        assert_eq!(estimate.blocking_operator_count, 2);
        assert_eq!(estimate.total_bytes, 11 * 1024);
    }

    #[test]
    fn source_segment_admission_includes_the_fixed_io_wave() {
        let plan = PhysicalPlan::SourceSegmentScan {
            variable: "n".to_string(),
            predicate: Predicate::ConstantBool(true),
        };

        let estimate = estimated_execution_memory(&plan, &admission_test_config());

        assert_eq!(estimate.pipeline_bytes, 1024);
        assert_eq!(estimate.blocking_operator_count, 1);
        assert_eq!(estimate.blocking_bytes, 4096);
        assert_eq!(estimate.external_read_bytes, 0);
        assert_eq!(
            estimate.fixed_operator_bytes,
            SOURCE_SEGMENT_SCAN_MAX_WAVE_BYTES
        );
        assert_eq!(
            estimate.total_bytes,
            1024 + 4096 + SOURCE_SEGMENT_SCAN_MAX_WAVE_BYTES
        );
    }

    #[test]
    fn vector_seed_admission_reserves_external_working_and_result_memory() {
        let plan = PhysicalPlan::VectorSeedScan {
            embedding_parameter: "embedding".to_string(),
            output_external_id: true,
            metadata_filters: Default::default(),
            resource_profile: VectorExecutionResourceProfile {
                priority: 128,
                max_parallelism: 3,
                max_working_memory_bytes: Some(2048),
            },
            vector_plan: hawdb_plan::VectorPhysicalPlan::TopK {
                limit: 4,
                input: Box::new(hawdb_plan::VectorPhysicalPlan::RawVectorRerank {
                    embedding_dimension: 2,
                    input: Box::new(hawdb_plan::VectorPhysicalPlan::VectorCandidateScan {
                        source: hawdb_plan::VectorCandidateSource::Scalar,
                        embedding_dimension: 2,
                        candidate_limit: 4,
                        input: Box::new(hawdb_plan::VectorPhysicalPlan::Filter {
                            fields: Vec::new(),
                        }),
                    }),
                }),
            },
        };

        let estimate = estimated_execution_memory(&plan, &admission_test_config());

        assert_eq!(estimate.pipeline_bytes, 1024);
        assert_eq!(estimate.blocking_bytes, 4096);
        assert_eq!(estimate.external_read_bytes, 2048 + 4096);
        assert_eq!(estimate.total_bytes, 11 * 1024);
        assert_eq!(max_external_read_parallelism(&plan), 3);
    }

    #[test]
    fn mutation_admission_reserves_wal_staging_and_bounded_results() {
        let limits = MutationLimits {
            max_affected_rows: NonZeroUsize::new(3).unwrap(),
            max_operations: NonZeroUsize::new(5).unwrap(),
            max_result_rows: NonZeroUsize::new(7).unwrap(),
            max_result_payload_bytes: NonZeroUsize::new(11).unwrap(),
        };

        assert_eq!(
            estimated_mutation_memory_bytes(limits, Some(13)),
            2 * 13
                + 5 * MUTATION_OPERATION_BOOKKEEPING_BYTES
                + 3 * MUTATION_AFFECTED_ROW_BOOKKEEPING_BYTES
                + 7 * MUTATION_RESULT_ROW_BOOKKEEPING_BYTES
                + 11
        );
        assert_eq!(estimated_mutation_memory_bytes(limits, None), u64::MAX);
    }
}
