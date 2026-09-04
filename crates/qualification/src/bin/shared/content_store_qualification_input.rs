use super::qualification_value::value_from_json;
use serde::Deserialize;
use skein::RuntimeGovernorConfig;
use skein_qualification::{
    ContentStoreResourceProfileKind, ProductionContentStoreReadCase,
    ProductionContentStoreResourceLimits, CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
    CONTENT_STORE_SHARED_HOST_8_GIB_BYTES,
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub(crate) enum ResourceProfileInput {
    #[serde(rename = "capability_512_mib")]
    Capability512Mib,
    #[serde(rename = "shared_host_8_gib")]
    SharedHost8Gib,
    ConfiguredWorkload {
        available_memory_bytes: u64,
        runtime_memory_ceiling_bytes: u64,
    },
}

impl ResourceProfileInput {
    pub(crate) fn resolve(
        self,
    ) -> Result<(ContentStoreResourceProfileKind, u64, RuntimeGovernorConfig), String> {
        let mut runtime = RuntimeGovernorConfig::shared_host();
        match self {
            Self::Capability512Mib => {
                runtime.memory_budget_bytes = Some(CONTENT_STORE_512_MIB_CAPABILITY_BYTES);
                Ok((
                    ContentStoreResourceProfileKind::Capability512Mib,
                    CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
                    runtime,
                ))
            }
            Self::SharedHost8Gib => Ok((
                ContentStoreResourceProfileKind::SharedHost8Gib,
                CONTENT_STORE_SHARED_HOST_8_GIB_BYTES,
                runtime,
            )),
            Self::ConfiguredWorkload {
                available_memory_bytes,
                runtime_memory_ceiling_bytes,
            } => {
                if available_memory_bytes == 0 || runtime_memory_ceiling_bytes == 0 {
                    return Err(
                        "configured workload memory and runtime ceiling must be non-zero"
                            .to_string(),
                    );
                }
                if runtime_memory_ceiling_bytes > available_memory_bytes {
                    return Err(
                        "configured workload runtime ceiling must not exceed available memory"
                            .to_string(),
                    );
                }
                runtime.memory_budget_bytes = Some(runtime_memory_ceiling_bytes);
                Ok((
                    ContentStoreResourceProfileKind::ConfiguredWorkload,
                    available_memory_bytes,
                    runtime,
                ))
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResourceLimitsInput {
    max_steady_resident_bytes: u64,
    max_peak_resident_bytes: u64,
    max_total_page_faults_per_run: Option<u64>,
    max_minor_page_faults_per_run: Option<u64>,
    max_major_page_faults_per_run: Option<u64>,
}

impl From<ResourceLimitsInput> for ProductionContentStoreResourceLimits {
    fn from(input: ResourceLimitsInput) -> Self {
        Self {
            max_steady_resident_bytes: input.max_steady_resident_bytes,
            max_peak_resident_bytes: input.max_peak_resident_bytes,
            max_total_page_faults_per_run: input.max_total_page_faults_per_run,
            max_minor_page_faults_per_run: input.max_minor_page_faults_per_run,
            max_major_page_faults_per_run: input.max_major_page_faults_per_run,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReadCaseInput {
    case_name: String,
    statement_name: String,
    parameters: Vec<serde_json::Value>,
    expected_output_rows: usize,
    expected_output_sha256: String,
    max_intermediate_rows: u64,
    max_physical_pages_per_run: u64,
    max_physical_bytes_per_run: u64,
}

impl ReadCaseInput {
    pub(crate) fn resolve(self) -> Result<ProductionContentStoreReadCase, String> {
        Ok(ProductionContentStoreReadCase {
            case_name: self.case_name,
            statement_name: self.statement_name,
            parameters: self
                .parameters
                .iter()
                .map(value_from_json)
                .collect::<Result<Vec<_>, _>>()?,
            expected_output_rows: self.expected_output_rows,
            expected_output_sha256: self.expected_output_sha256,
            max_intermediate_rows: self.max_intermediate_rows,
            max_physical_pages_per_run: self.max_physical_pages_per_run,
            max_physical_bytes_per_run: self.max_physical_bytes_per_run,
        })
    }
}
