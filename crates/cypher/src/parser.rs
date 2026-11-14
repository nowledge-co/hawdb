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

use super::ast::Statement;
use hawdb_core::time::Instant;
use hawdb_core::Result;

mod frontend;
#[cfg(test)]
mod pipeline_tests;
mod projected;
#[cfg(test)]
mod tests;

/// Parses ordered query clauses independently of statement-template lowering.
#[doc(hidden)]
pub fn parse_pipeline(input: &str) -> Result<super::ast::QueryPipeline> {
    frontend::parse_pipeline(input)
}

pub(crate) const MAX_CYPHER_INPUT_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_CYPHER_PARSER_DEPTH: usize = 32;

/// Parses one Cypher statement.
///
/// Input is limited to 16 MiB and recursive parser nesting is limited to 32
/// entries. Inputs exceeding either limit return a parse error.
pub fn parse(input: &str) -> Result<Statement> {
    frontend::parse(input)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseMetrics {
    pub input_bytes: usize,
    pub elapsed_nanos: u64,
}

#[derive(Debug)]
pub struct ParseMeasurement {
    pub result: Result<Statement>,
    pub metrics: ParseMetrics,
}

pub fn parse_profiled(input: &str) -> ParseMeasurement {
    let started = Instant::now();
    let result = frontend::parse(input);
    ParseMeasurement {
        result,
        metrics: ParseMetrics {
            input_bytes: input.len(),
            elapsed_nanos: started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
        },
    }
}
