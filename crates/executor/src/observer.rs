//! Execution-report observer contract for host integration.

use crate::BlockingOperatorMemoryReport;
use skein_storage::ScanPruningReport;

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
