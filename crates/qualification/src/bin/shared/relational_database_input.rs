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

use hawdb::{DatabaseConfig, RelationalIndexMode, StorageResidencyMode};
use serde::Deserialize;
use std::num::NonZeroUsize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DatabaseInput {
    max_read_result_rows: usize,
    max_read_result_payload_bytes: usize,
    execution_batch_rows: usize,
    execution_batch_payload_bytes: usize,
    blocking_operator_bytes: usize,
    segment_cache_capacity_bytes: u64,
    max_relational_index_read_bytes: usize,
    max_relational_hydration_bytes: usize,
}

impl DatabaseInput {
    pub(crate) fn resolve(self, read_only: bool) -> Result<DatabaseConfig, String> {
        let mut config = DatabaseConfig {
            read_only,
            max_read_result_rows: Some(require_nonzero_usize(
                "max_read_result_rows",
                self.max_read_result_rows,
            )?),
            max_read_result_payload_bytes: Some(require_nonzero_usize(
                "max_read_result_payload_bytes",
                self.max_read_result_payload_bytes,
            )?),
            segment_cache_capacity_bytes: require_nonzero_u64(
                "segment_cache_capacity_bytes",
                self.segment_cache_capacity_bytes,
            )?,
            max_relational_index_read_bytes: NonZeroUsize::new(
                self.max_relational_index_read_bytes,
            )
            .ok_or_else(|| "max_relational_index_read_bytes must be non-zero".to_string())?,
            max_relational_hydration_bytes: NonZeroUsize::new(self.max_relational_hydration_bytes)
                .ok_or_else(|| "max_relational_hydration_bytes must be non-zero".to_string())?,
            storage_residency_mode: StorageResidencyMode::OutOfCore,
            relational_index_mode: RelationalIndexMode::Authoritative,
            ..DatabaseConfig::default()
        };
        config.execution_memory.batch_rows = NonZeroUsize::new(self.execution_batch_rows)
            .ok_or_else(|| "execution_batch_rows must be non-zero".to_string())?;
        config.execution_memory.batch_payload_bytes =
            NonZeroUsize::new(self.execution_batch_payload_bytes)
                .ok_or_else(|| "execution_batch_payload_bytes must be non-zero".to_string())?;
        config.execution_memory.blocking_operator_bytes =
            NonZeroUsize::new(self.blocking_operator_bytes)
                .ok_or_else(|| "blocking_operator_bytes must be non-zero".to_string())?;
        Ok(config)
    }
}

fn require_nonzero_usize(name: &str, value: usize) -> Result<usize, String> {
    (value > 0)
        .then_some(value)
        .ok_or_else(|| format!("{name} must be non-zero"))
}

fn require_nonzero_u64(name: &str, value: u64) -> Result<u64, String> {
    (value > 0)
        .then_some(value)
        .ok_or_else(|| format!("{name} must be non-zero"))
}
