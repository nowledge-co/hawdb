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

//! Execution-report observer contract for host integration.

use crate::BlockingOperatorMemoryReport;
use hawdb_storage::ScanPruningReport;

mod profile;
mod query;
mod report_value;

#[doc(hidden)]
pub use report_value::{
    blocking_operator_memory_report_value, graph_expansion_report_value,
    pipeline_memory_report_value, scan_pruning_report_value, vector_execution_report_value,
};

#[doc(hidden)]
pub use profile::{read_execution_profile, ExecutionProfileBuilder};

// Internal collection remains separate from the embedded host's public reports.
#[doc(hidden)]
pub use query::{blocking_operator_kinds, QueryExecutionObserver, QueryExecutionReports};

pub trait ExecutionObserver {
    fn record_scan_pruning_report(&self, _report: ScanPruningReport) {}

    fn record_blocking_memory_report(&self, _report: BlockingOperatorMemoryReport) {}
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopExecutionObserver;

impl ExecutionObserver for NoopExecutionObserver {}
