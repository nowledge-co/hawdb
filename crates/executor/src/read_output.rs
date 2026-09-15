//! Public output contracts for bounded and streaming read execution.

use crate::{QueryOutput, ReadExecutionProfile};
use skein_storage::ScanPruningReport;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryStreamOptions {
    pub max_rows: Option<usize>,
    pub max_payload_bytes: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryStreamReport {
    pub fully_streamed: bool,
    pub output_rows: usize,
    pub output_payload_bytes: usize,
    pub execution_profile: ReadExecutionProfile<ScanPruningReport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedReadQueryOutput {
    pub output: QueryOutput,
    pub execution_profile: ReadExecutionProfile<ScanPruningReport>,
}

#[cfg(test)]
mod tests {
    use super::QueryStreamOptions;

    #[test]
    fn stream_options_default_to_unbounded() {
        assert_eq!(
            QueryStreamOptions::default(),
            QueryStreamOptions {
                max_rows: None,
                max_payload_bytes: None,
            }
        );
    }
}
