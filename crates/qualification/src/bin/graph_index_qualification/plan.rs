use super::graph_qualification_input::{
    GraphDatabaseInput, ProcessLimitsInput, QueryLimitsInput, ResolvedProcessLimits,
    RuntimeProfileInput,
};
use super::qualification_input::{
    read_bounded_json, EvidenceBindingInput, ProductionIdentityInput,
};
use super::qualification_value::value_from_json;
use serde::Deserialize;
use skein::{
    NowledgeGraphStatement, NowledgeMemGraphMode, NowledgeMemOpenOptions,
    PersistentGraphIndexClass, RuntimeGovernorConfig,
};
use skein_qualification::{
    PersistentGraphIndexProductionRequirement, ProductionGraphIndexQualificationMatrixConfig,
    ProductionGraphStorageQualificationConfig,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub(crate) const GRAPH_INDEX_QUALIFICATION_PLAN_PROTOCOL: &str =
    "skein-production-graph-index-plan-v1";

pub(crate) fn read_plan(path: &Path) -> Result<GraphIndexQualificationPlan, String> {
    read_bounded_json(path, "graph index qualification plan")
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GraphIndexQualificationPlan {
    protocol: String,
    evidence_binding: EvidenceBindingInput,
    expected_identity: ProductionIdentityInput,
    runtime_profile: RuntimeProfileInput,
    database: GraphDatabaseInput,
    measurement_runs: usize,
    process_limits: ProcessLimitsInput,
    cases: Vec<GraphIndexCaseInput>,
}

impl GraphIndexQualificationPlan {
    pub(crate) fn into_config(
        self,
        database_path: PathBuf,
    ) -> Result<ProductionGraphIndexQualificationMatrixConfig, String> {
        if self.protocol != GRAPH_INDEX_QUALIFICATION_PLAN_PROTOCOL {
            return Err(format!(
                "graph index qualification plan protocol must be {GRAPH_INDEX_QUALIFICATION_PLAN_PROTOCOL}"
            ));
        }
        if self.measurement_runs < 2 {
            return Err(
                "graph index qualification requires at least two measurement runs".to_string(),
            );
        }
        let classes = self
            .cases
            .iter()
            .map(|case| PersistentGraphIndexClass::from(case.class))
            .collect::<Vec<_>>();
        if classes != PersistentGraphIndexClass::ALL.to_vec() {
            return Err(
                "graph index qualification cases must be ordered exactly as node_equality, node_range, node_full_text, node_composite_equality, relationship_equality, relationship_range, forward_adjacency, and reverse_adjacency"
                    .to_string(),
            );
        }

        let runtime = self.runtime_profile.resolve()?;
        let database_config = self.database.resolve(runtime.max_resident_bytes)?;
        let process_limits = self.process_limits.resolve(runtime.max_resident_bytes)?;
        let open_options =
            NowledgeMemOpenOptions::graph_only(database_path, NowledgeMemGraphMode::ShadowReadOnly)
                .with_database_config(database_config);
        let evidence_binding: skein::ProductionEvidenceBinding = self.evidence_binding.into();
        let expected_identity: skein::ProductionQualificationIdentity =
            self.expected_identity.into();
        let cases = self
            .cases
            .into_iter()
            .map(|case| {
                case.resolve(
                    open_options.clone(),
                    runtime.config,
                    process_limits,
                    evidence_binding.clone(),
                    expected_identity.clone(),
                    self.measurement_runs,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ProductionGraphIndexQualificationMatrixConfig { cases })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GraphIndexCaseInput {
    class: GraphIndexClassInput,
    cypher: String,
    parameters: BTreeMap<String, serde_json::Value>,
    query_limits: QueryLimitsInput,
    reference_output_digest: String,
    reference_output_rows: usize,
    max_blocks_read_per_run: u64,
    max_bytes_read_per_run: u64,
    max_cancellation_latency_micros: u64,
}

impl GraphIndexCaseInput {
    fn resolve(
        self,
        open_options: NowledgeMemOpenOptions,
        runtime_governor_config: RuntimeGovernorConfig,
        process_limits: ResolvedProcessLimits,
        evidence_binding: skein::ProductionEvidenceBinding,
        expected_identity: skein::ProductionQualificationIdentity,
        measurement_runs: usize,
    ) -> Result<ProductionGraphStorageQualificationConfig, String> {
        if self.cypher.trim().is_empty() {
            return Err("graph index qualification Cypher must not be empty".to_string());
        }
        if !is_sha256_digest(&self.reference_output_digest) {
            return Err("graph index reference_output_digest must be a sha256 digest".to_string());
        }
        if self.max_blocks_read_per_run == 0
            || self.max_bytes_read_per_run == 0
            || self.max_cancellation_latency_micros == 0
        {
            return Err(
                "graph index block, byte, and cancellation limits must be non-zero".to_string(),
            );
        }
        let parameters = self
            .parameters
            .into_iter()
            .map(|(name, value)| Ok((name, value_from_json(&value)?)))
            .collect::<Result<BTreeMap<_, _>, String>>()?;
        Ok(ProductionGraphStorageQualificationConfig {
            open_options,
            runtime_governor_config,
            statement: NowledgeGraphStatement {
                cypher: self.cypher,
                parameters,
            },
            limits: self.query_limits.resolve(process_limits)?,
            evidence_binding,
            expected_identity,
            measurement_runs,
            persistent_index_requirement: Some(PersistentGraphIndexProductionRequirement {
                class: self.class.into(),
                reference_output_digest: self.reference_output_digest,
                reference_output_rows: self.reference_output_rows,
                max_blocks_read_per_run: self.max_blocks_read_per_run,
                max_bytes_read_per_run: self.max_bytes_read_per_run,
                max_cancellation_latency_micros: self.max_cancellation_latency_micros,
            }),
        })
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum GraphIndexClassInput {
    NodeEquality,
    NodeRange,
    NodeFullText,
    NodeCompositeEquality,
    RelationshipEquality,
    RelationshipRange,
    ForwardAdjacency,
    ReverseAdjacency,
}

impl From<GraphIndexClassInput> for PersistentGraphIndexClass {
    fn from(input: GraphIndexClassInput) -> Self {
        match input {
            GraphIndexClassInput::NodeEquality => Self::NodeEquality,
            GraphIndexClassInput::NodeRange => Self::NodeRange,
            GraphIndexClassInput::NodeFullText => Self::NodeFullText,
            GraphIndexClassInput::NodeCompositeEquality => Self::NodeCompositeEquality,
            GraphIndexClassInput::RelationshipEquality => Self::RelationshipEquality,
            GraphIndexClassInput::RelationshipRange => Self::RelationshipRange,
            GraphIndexClassInput::ForwardAdjacency => Self::ForwardAdjacency,
            GraphIndexClassInput::ReverseAdjacency => Self::ReverseAdjacency,
        }
    }
}

fn is_sha256_digest(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use skein::{
        NowledgeMemGraphMode, StorageResidencyMode, PRODUCTION_QUALIFICATION_POLICY_VERSION,
    };
    use skein_qualification::{
        CONTENT_STORE_512_MIB_CAPABILITY_BYTES, CONTENT_STORE_SHARED_HOST_MAX_CAPACITY_BYTES,
    };

    #[test]
    fn checked_in_example_plan_builds_one_ordered_read_only_matrix() {
        let plan: GraphIndexQualificationPlan = serde_json::from_str(include_str!(
            "../../../fixtures/nowledge_graph/production_graph_index_plan_example_v1.json"
        ))
        .unwrap();
        let config = plan.into_config(PathBuf::from("database")).unwrap();

        assert_eq!(config.cases.len(), PersistentGraphIndexClass::ALL.len());
        assert!(config.cases.iter().all(|case| {
            case.open_options.mode == NowledgeMemGraphMode::ShadowReadOnly
                && case
                    .open_options
                    .database_config
                    .as_ref()
                    .is_some_and(|database| {
                        database.read_only
                            && database.storage_residency_mode == StorageResidencyMode::OutOfCore
                    })
                && case.runtime_governor_config.memory_budget_bytes.is_none()
        }));
        assert_eq!(
            config
                .cases
                .iter()
                .map(|case| case.persistent_index_requirement.as_ref().unwrap().class)
                .collect::<Vec<_>>(),
            PersistentGraphIndexClass::ALL
        );
    }

    #[test]
    fn plan_rejects_reordered_classes_and_profile_overcommit() {
        let mut value = valid_plan_json();
        value["cases"].as_array_mut().unwrap().swap(0, 1);
        let plan: GraphIndexQualificationPlan = serde_json::from_value(value).unwrap();
        assert!(plan
            .into_config(PathBuf::from("database"))
            .unwrap_err()
            .contains("ordered exactly"));

        let mut value = valid_plan_json();
        value["runtime_profile"] = serde_json::json!("capability_512_mib");
        value["process_limits"]["max_steady_resident_bytes"] =
            serde_json::json!(CONTENT_STORE_512_MIB_CAPABILITY_BYTES);
        value["process_limits"]["max_peak_resident_bytes"] =
            serde_json::json!(CONTENT_STORE_512_MIB_CAPABILITY_BYTES + 1);
        let plan: GraphIndexQualificationPlan = serde_json::from_value(value).unwrap();
        assert!(plan
            .into_config(PathBuf::from("database"))
            .unwrap_err()
            .contains("runtime profile ceiling"));
    }

    #[test]
    fn plan_rejects_invalid_digest_and_parameter_integer() {
        let mut value = valid_plan_json();
        value["cases"][0]["reference_output_digest"] = serde_json::json!("not-a-digest");
        let plan: GraphIndexQualificationPlan = serde_json::from_value(value).unwrap();
        assert!(plan
            .into_config(PathBuf::from("database"))
            .unwrap_err()
            .contains("sha256"));

        let mut value = valid_plan_json();
        value["cases"][0]["parameters"]["id"] = serde_json::json!(u64::MAX);
        let plan: GraphIndexQualificationPlan = serde_json::from_value(value).unwrap();
        assert!(plan
            .into_config(PathBuf::from("database"))
            .unwrap_err()
            .contains("exceeds i64"));
    }

    fn valid_plan_json() -> serde_json::Value {
        let identity = serde_json::json!({
            "source_revision": "revision",
            "rust_toolchain": "rustc",
            "target_os": std::env::consts::OS,
            "target_arch": std::env::consts::ARCH,
            "enabled_features": [],
            "durable_format_version": 1,
            "schema_version": 1,
            "configuration_digest": "config",
            "deployment_profile": "shared-host-8-gib",
            "dataset_fingerprint": "dataset",
            "canonical_graph_commit_epoch": 1,
            "policy_version": PRODUCTION_QUALIFICATION_POLICY_VERSION
        });
        let cases = PersistentGraphIndexClass::ALL
            .into_iter()
            .map(|class| {
                serde_json::json!({
                    "class": class.as_str(),
                    "cypher": "MATCH (n:Memory) WHERE n.id = $id RETURN n.id",
                    "parameters": {"id": "memory-1"},
                    "query_limits": {
                        "max_intermediate_rows": 100,
                        "max_intermediate_payload_bytes": 1048576,
                        "max_output_rows": 100,
                        "max_output_payload_bytes": 1048576
                    },
                    "reference_output_digest": format!("sha256:{}", "0".repeat(64)),
                    "reference_output_rows": 1,
                    "max_blocks_read_per_run": 64,
                    "max_bytes_read_per_run": 1048576,
                    "max_cancellation_latency_micros": 1000000
                })
            })
            .collect::<Vec<_>>();
        serde_json::json!({
            "protocol": GRAPH_INDEX_QUALIFICATION_PLAN_PROTOCOL,
            "evidence_binding": {
                "identity": identity.clone(),
                "generated_at_unix_seconds": 1
            },
            "expected_identity": identity,
            "runtime_profile": "shared_host_8_gib",
            "database": {
                "max_read_result_rows": 1000,
                "max_read_result_payload_bytes": 1048576,
                "execution_batch_rows": 256,
                "execution_batch_payload_bytes": 1048576,
                "blocking_operator_bytes": 1048576,
                "segment_cache_capacity_bytes": 1048576
            },
            "measurement_runs": 2,
            "process_limits": {
                "min_canonical_artifact_bytes": 1048577,
                "max_steady_resident_bytes": 1073741824,
                "max_peak_resident_bytes": CONTENT_STORE_SHARED_HOST_MAX_CAPACITY_BYTES,
                "max_total_page_faults": null,
                "max_minor_page_faults": null,
                "max_major_page_faults": null,
                "require_fully_streamed": true
            },
            "cases": cases
        })
    }
}
