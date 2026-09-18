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

use hawdb::{
    DatabaseConfig, RuntimeGovernorConfig, StorageResidencyMode, StorageResourceProfileLimits,
};
use hawdb_qualification::{
    CONTENT_STORE_512_MIB_CAPABILITY_BYTES, CONTENT_STORE_SHARED_HOST_MAX_CAPACITY_BYTES,
};
use serde::Deserialize;
use std::num::NonZeroUsize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub(crate) enum RuntimeProfileInput {
    #[serde(rename = "capability_512_mib")]
    Capability512Mib,
    #[serde(rename = "shared_host_8_gib")]
    SharedHost8Gib,
    ConfiguredWorkload {
        runtime_memory_ceiling_bytes: u64,
    },
}

pub(crate) struct ResolvedRuntimeProfile {
    pub(crate) config: RuntimeGovernorConfig,
    pub(crate) max_resident_bytes: u64,
}

impl RuntimeProfileInput {
    pub(crate) fn resolve(self) -> Result<ResolvedRuntimeProfile, String> {
        let mut config = RuntimeGovernorConfig::shared_host();
        let max_resident_bytes = match self {
            Self::Capability512Mib => {
                config.memory_budget_bytes = Some(CONTENT_STORE_512_MIB_CAPABILITY_BYTES);
                CONTENT_STORE_512_MIB_CAPABILITY_BYTES
            }
            Self::SharedHost8Gib => CONTENT_STORE_SHARED_HOST_MAX_CAPACITY_BYTES,
            Self::ConfiguredWorkload {
                runtime_memory_ceiling_bytes,
            } => {
                if runtime_memory_ceiling_bytes == 0 {
                    return Err(
                        "configured workload runtime memory ceiling must be non-zero".to_string(),
                    );
                }
                config.memory_budget_bytes = Some(runtime_memory_ceiling_bytes);
                runtime_memory_ceiling_bytes
            }
        };
        Ok(ResolvedRuntimeProfile {
            config,
            max_resident_bytes,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GraphDatabaseInput {
    max_read_result_rows: usize,
    max_read_result_payload_bytes: usize,
    execution_batch_rows: usize,
    execution_batch_payload_bytes: usize,
    blocking_operator_bytes: usize,
    segment_cache_capacity_bytes: u64,
}

impl GraphDatabaseInput {
    pub(crate) fn resolve(self, max_resident_bytes: u64) -> Result<DatabaseConfig, String> {
        if self.segment_cache_capacity_bytes == 0 {
            return Err("segment_cache_capacity_bytes must be non-zero".to_string());
        }
        if self.segment_cache_capacity_bytes > max_resident_bytes {
            return Err(
                "segment cache capacity must not exceed the runtime profile ceiling".to_string(),
            );
        }
        let mut config = DatabaseConfig {
            read_only: true,
            max_read_result_rows: Some(require_nonzero_usize(
                "max_read_result_rows",
                self.max_read_result_rows,
            )?),
            max_read_result_payload_bytes: Some(require_nonzero_usize(
                "max_read_result_payload_bytes",
                self.max_read_result_payload_bytes,
            )?),
            segment_cache_capacity_bytes: self.segment_cache_capacity_bytes,
            storage_residency_mode: StorageResidencyMode::OutOfCore,
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

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProcessLimitsInput {
    min_canonical_artifact_bytes: u64,
    max_steady_resident_bytes: u64,
    max_peak_resident_bytes: u64,
    max_total_page_faults: Option<u64>,
    max_minor_page_faults: Option<u64>,
    max_major_page_faults: Option<u64>,
    require_fully_streamed: bool,
}

impl ProcessLimitsInput {
    pub(crate) fn resolve(self, max_resident_bytes: u64) -> Result<ResolvedProcessLimits, String> {
        if self.min_canonical_artifact_bytes == 0
            || self.max_steady_resident_bytes == 0
            || self.max_peak_resident_bytes == 0
        {
            return Err(
                "graph process canonical, steady RSS, and peak RSS limits must be non-zero"
                    .to_string(),
            );
        }
        if self.max_steady_resident_bytes > self.max_peak_resident_bytes {
            return Err("graph steady RSS limit must not exceed peak RSS limit".to_string());
        }
        if self.max_peak_resident_bytes > max_resident_bytes {
            return Err(
                "graph peak RSS limit must not exceed the runtime profile ceiling".to_string(),
            );
        }
        Ok(ResolvedProcessLimits {
            min_canonical_artifact_bytes: self.min_canonical_artifact_bytes,
            max_steady_resident_bytes: self.max_steady_resident_bytes,
            max_peak_resident_bytes: self.max_peak_resident_bytes,
            max_total_page_faults: self.max_total_page_faults,
            max_minor_page_faults: self.max_minor_page_faults,
            max_major_page_faults: self.max_major_page_faults,
            require_fully_streamed: self.require_fully_streamed,
        })
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ResolvedProcessLimits {
    min_canonical_artifact_bytes: u64,
    max_steady_resident_bytes: u64,
    max_peak_resident_bytes: u64,
    max_total_page_faults: Option<u64>,
    max_minor_page_faults: Option<u64>,
    max_major_page_faults: Option<u64>,
    require_fully_streamed: bool,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct QueryLimitsInput {
    max_intermediate_rows: usize,
    max_intermediate_payload_bytes: usize,
    max_output_rows: usize,
    max_output_payload_bytes: usize,
}

impl QueryLimitsInput {
    pub(crate) fn resolve(
        self,
        process: ResolvedProcessLimits,
    ) -> Result<StorageResourceProfileLimits, String> {
        Ok(StorageResourceProfileLimits {
            min_canonical_artifact_bytes: process.min_canonical_artifact_bytes,
            max_steady_resident_bytes: process.max_steady_resident_bytes,
            max_peak_resident_bytes: process.max_peak_resident_bytes,
            max_total_page_faults: process.max_total_page_faults,
            max_minor_page_faults: process.max_minor_page_faults,
            max_major_page_faults: process.max_major_page_faults,
            max_intermediate_rows: require_nonzero_usize(
                "max_intermediate_rows",
                self.max_intermediate_rows,
            )?,
            max_intermediate_payload_bytes: require_nonzero_usize(
                "max_intermediate_payload_bytes",
                self.max_intermediate_payload_bytes,
            )?,
            max_output_rows: require_nonzero_usize("max_output_rows", self.max_output_rows)?,
            max_output_payload_bytes: require_nonzero_usize(
                "max_output_payload_bytes",
                self.max_output_payload_bytes,
            )?,
            require_fully_streamed: process.require_fully_streamed,
        })
    }
}

fn require_nonzero_usize(name: &str, value: usize) -> Result<usize, String> {
    (value > 0)
        .then_some(value)
        .ok_or_else(|| format!("{name} must be non-zero"))
}
