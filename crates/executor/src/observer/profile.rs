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

//! Query-owned profile initialization and completion accounting.
//!
//! Process sampling and host policy remain in the embedded facade.

use super::{blocking_operator_kinds, QueryExecutionObserver, QueryExecutionReports};
use crate::result_delivery::OutputMetrics;
use crate::{ExecutionLimit, PipelineMemoryReport, QueryMemoryLedger, ReadExecutionProfile};
use hawdb_core::Result;
use hawdb_plan::PhysicalPlan;
use hawdb_storage::ScanPruningReport;

pub fn read_execution_profile(
    plan: &PhysicalPlan,
    max_rows: Option<usize>,
) -> Result<ReadExecutionProfile<ScanPruningReport>> {
    let execution_limit = ExecutionLimit::from_user_max_rows(max_rows)?;
    Ok(ReadExecutionProfile {
        max_rows,
        detection_row_cap: execution_limit.output_rows,
        row_limit_enforced_before_output: max_rows.is_some(),
        operator_row_cap_enabled: execution_limit.output_rows.is_some(),
        operator_cardinality_profiles: Vec::new(),
        blocking_operator_kinds: blocking_operator_kinds(plan),
        scan_pruning_reports: Vec::new(),
        vector_execution_reports: Vec::new(),
        graph_expansion_reports: Vec::new(),
        blocking_operator_memory_reports: Vec::new(),
        pipeline_memory_report: PipelineMemoryReport::default(),
    })
}

/// Internal lifecycle shared by the facade's existing execution entrypoints.
pub struct ExecutionProfileBuilder {
    profile: ReadExecutionProfile<ScanPruningReport>,
}

impl ExecutionProfileBuilder {
    pub fn start(plan: &PhysicalPlan, max_rows: Option<usize>) -> Result<Self> {
        Ok(Self {
            profile: read_execution_profile(plan, max_rows)?,
        })
    }

    pub fn finish(
        mut self,
        observer: QueryExecutionObserver,
        memory_ledger: &QueryMemoryLedger,
        output: OutputMetrics,
        record_host_metrics: impl FnOnce(&mut PipelineMemoryReport),
    ) -> ReadExecutionProfile<ScanPruningReport> {
        let QueryExecutionReports {
            operator_cardinality,
            scan_pruning,
            vector_execution,
            graph_expansion,
            blocking_memory,
            mut pipeline_memory,
        } = observer.into_reports();
        self.profile.operator_cardinality_profiles = operator_cardinality;
        self.profile.scan_pruning_reports = scan_pruning;
        self.profile.vector_execution_reports = vector_execution;
        self.profile.graph_expansion_reports = graph_expansion;
        self.profile.blocking_operator_memory_reports = blocking_memory;

        pipeline_memory.output_rows = output.rows;
        pipeline_memory.output_payload_bytes = output.payload_bytes;
        let query_memory = memory_ledger.snapshot();
        pipeline_memory.query_memory_budget_bytes = query_memory.budget_bytes;
        pipeline_memory.query_memory_peak_bytes = query_memory.peak_bytes;
        pipeline_memory.query_memory_completion_bytes = query_memory.used_bytes;
        pipeline_memory.query_memory_account_count = query_memory.account_count;
        // Keep the ledger snapshot alive at the host's sampling boundary, as in
        // the original facade lifecycle. The owner never samples the process.
        record_host_metrics(&mut pipeline_memory);
        self.profile.pipeline_memory_report = pipeline_memory;
        self.profile
    }
}

#[cfg(test)]
mod tests;
