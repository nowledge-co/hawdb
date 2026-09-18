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

//! Public output contracts for bounded and streaming read execution.

use crate::{QueryOutput, ReadExecutionProfile};
use hawdb_storage::ScanPruningReport;

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
