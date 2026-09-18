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

use super::graph_qualification_input::{
    GraphDatabaseInput, ProcessLimitsInput, QueryLimitsInput, RuntimeProfileInput,
};
use super::qualification_input::{
    read_bounded_json, EvidenceBindingInput, ProductionIdentityInput,
};
use super::qualification_value::value_from_json;
use hawdb::{NowledgeGraphStatement, NowledgeMemGraphMode, NowledgeMemOpenOptions};
use hawdb_qualification::ProductionGraphStorageQualificationConfig;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub(crate) const GRAPH_STORAGE_QUALIFICATION_PLAN_PROTOCOL: &str =
    "hawdb-production-graph-storage-plan-v1";

pub(crate) fn read_plan(path: &Path) -> Result<GraphStorageQualificationPlan, String> {
    read_bounded_json(path, "graph storage qualification plan")
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GraphStorageQualificationPlan {
    protocol: String,
    evidence_binding: EvidenceBindingInput,
    expected_identity: ProductionIdentityInput,
    runtime_profile: RuntimeProfileInput,
    database: GraphDatabaseInput,
    measurement_runs: usize,
    process_limits: ProcessLimitsInput,
    statement: GraphStatementInput,
}

impl GraphStorageQualificationPlan {
    pub(crate) fn into_config(
        self,
        database_path: PathBuf,
    ) -> Result<ProductionGraphStorageQualificationConfig, String> {
        if self.protocol != GRAPH_STORAGE_QUALIFICATION_PLAN_PROTOCOL {
            return Err(format!(
                "graph storage qualification plan protocol must be {GRAPH_STORAGE_QUALIFICATION_PLAN_PROTOCOL}"
            ));
        }
        if self.measurement_runs < 2 {
            return Err(
                "graph storage qualification requires at least two measurement runs".to_string(),
            );
        }
        let runtime = self.runtime_profile.resolve()?;
        let database_config = self.database.resolve(runtime.max_resident_bytes)?;
        let process_limits = self.process_limits.resolve(runtime.max_resident_bytes)?;
        let statement = self.statement.resolve(process_limits)?;
        let open_options =
            NowledgeMemOpenOptions::graph_only(database_path, NowledgeMemGraphMode::ShadowReadOnly)
                .with_database_config(database_config);
        Ok(ProductionGraphStorageQualificationConfig {
            open_options,
            runtime_governor_config: runtime.config,
            statement: statement.statement,
            limits: statement.limits,
            evidence_binding: self.evidence_binding.into(),
            expected_identity: self.expected_identity.into(),
            measurement_runs: self.measurement_runs,
            persistent_index_requirement: None,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GraphStatementInput {
    cypher: String,
    parameters: BTreeMap<String, serde_json::Value>,
    query_limits: QueryLimitsInput,
}

struct ResolvedGraphStatement {
    statement: NowledgeGraphStatement,
    limits: hawdb::StorageResourceProfileLimits,
}

impl GraphStatementInput {
    fn resolve(
        self,
        process_limits: super::graph_qualification_input::ResolvedProcessLimits,
    ) -> Result<ResolvedGraphStatement, String> {
        if self.cypher.trim().is_empty() {
            return Err("graph storage qualification Cypher must not be empty".to_string());
        }
        let parameters = self
            .parameters
            .into_iter()
            .map(|(name, value)| Ok((name, value_from_json(&value)?)))
            .collect::<Result<BTreeMap<_, _>, String>>()?;
        Ok(ResolvedGraphStatement {
            statement: NowledgeGraphStatement {
                cypher: self.cypher,
                parameters,
            },
            limits: self.query_limits.resolve(process_limits)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hawdb::{StorageResidencyMode, PRODUCTION_QUALIFICATION_POLICY_VERSION};
    use hawdb_qualification::{
        CONTENT_STORE_512_MIB_CAPABILITY_BYTES, CONTENT_STORE_SHARED_HOST_MAX_CAPACITY_BYTES,
    };

    #[test]
    fn checked_in_example_plan_builds_one_bounded_read_only_run() {
        let plan: GraphStorageQualificationPlan = serde_json::from_str(include_str!(
            "../../../fixtures/nowledge_graph/production_graph_storage_plan_example_v1.json"
        ))
        .unwrap();
        let config = plan.into_config(PathBuf::from("database")).unwrap();
        let database = config.open_options.database_config.unwrap();

        assert_eq!(
            config.open_options.mode,
            NowledgeMemGraphMode::ShadowReadOnly
        );
        assert!(database.read_only);
        assert_eq!(
            database.storage_residency_mode,
            StorageResidencyMode::OutOfCore
        );
        assert!(config.runtime_governor_config.memory_budget_bytes.is_none());
        assert!(config.persistent_index_requirement.is_none());
        assert_eq!(config.measurement_runs, 5);
    }

    #[test]
    fn plan_rejects_short_measurement_and_profile_overcommit() {
        let mut value = valid_plan_json();
        value["measurement_runs"] = serde_json::json!(1);
        let plan: GraphStorageQualificationPlan = serde_json::from_value(value).unwrap();
        assert!(plan
            .into_config(PathBuf::from("database"))
            .unwrap_err()
            .contains("at least two"));

        let mut value = valid_plan_json();
        value["runtime_profile"] = serde_json::json!("capability_512_mib");
        value["process_limits"]["max_steady_resident_bytes"] =
            serde_json::json!(CONTENT_STORE_512_MIB_CAPABILITY_BYTES);
        value["process_limits"]["max_peak_resident_bytes"] =
            serde_json::json!(CONTENT_STORE_512_MIB_CAPABILITY_BYTES + 1);
        let plan: GraphStorageQualificationPlan = serde_json::from_value(value).unwrap();
        assert!(plan
            .into_config(PathBuf::from("database"))
            .unwrap_err()
            .contains("runtime profile ceiling"));
    }

    #[test]
    fn plan_rejects_empty_cypher_and_out_of_range_parameter() {
        let mut value = valid_plan_json();
        value["statement"]["cypher"] = serde_json::json!("   ");
        let plan: GraphStorageQualificationPlan = serde_json::from_value(value).unwrap();
        assert!(plan
            .into_config(PathBuf::from("database"))
            .unwrap_err()
            .contains("must not be empty"));

        let mut value = valid_plan_json();
        value["statement"]["parameters"]["limit"] = serde_json::json!(u64::MAX);
        let plan: GraphStorageQualificationPlan = serde_json::from_value(value).unwrap();
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
        serde_json::json!({
            "protocol": GRAPH_STORAGE_QUALIFICATION_PLAN_PROTOCOL,
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
            "statement": {
                "cypher": "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN m.id, e.id LIMIT $limit",
                "parameters": {"limit": 100},
                "query_limits": {
                    "max_intermediate_rows": 100,
                    "max_intermediate_payload_bytes": 1048576,
                    "max_output_rows": 100,
                    "max_output_payload_bytes": 1048576
                }
            }
        })
    }
}
