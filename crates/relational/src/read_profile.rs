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

use hawdb_executor::{BlockingOperatorMemoryReport, QueryOutput};
use hawdb_optimizer::{RelationalJoinPlanningOutcome, RelationalOperatorCardinalityProfile};
use hawdb_sql::RelationalSqlStageTimings;

/// Output and execution evidence for a relational SQL read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfiledRelationalSqlQueryOutput {
    pub output: QueryOutput,
    pub profile: RelationalSqlReadProfile,
}

/// Relational execution evidence collected while producing a query result.
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
    /// Memory and spill evidence for blocking relational operators.
    pub blocking_operator_memory_reports: Vec<BlockingOperatorMemoryReport>,
}

/// I/O counters for one relational index access path.
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

/// I/O counters and snapshot identity for relational row-page reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalSqlRowReadProfile {
    pub runtime_path: String,
    pub projection_generation: Option<String>,
    pub projection_source_watermark: Option<u64>,
    pub projection_version: Option<u64>,
    pub projection_publication_commit_epoch: Option<u64>,
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
