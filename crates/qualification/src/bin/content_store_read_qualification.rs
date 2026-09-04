#[path = "shared/qualification_input.rs"]
mod qualification_input;

#[path = "shared/content_store_qualification_input.rs"]
mod content_store_qualification_input;
#[path = "shared/qualification_value.rs"]
mod qualification_value;
#[path = "shared/relational_database_input.rs"]
mod relational_database_input;

use content_store_qualification_input::{ReadCaseInput, ResourceLimitsInput, ResourceProfileInput};
use qualification_input::{read_bounded_json, EvidenceBindingInput, ProductionIdentityInput};
use relational_database_input::DatabaseInput;
use serde::Deserialize;
use skein_qualification::{
    run_production_content_store_storage_qualification, ProductionContentStoreOpenCacheLimits,
    ProductionContentStoreStorageQualificationConfig,
    PRODUCTION_CONTENT_STORE_STORAGE_QUALIFICATION_PROTOCOL,
};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

const CONTENT_STORE_READ_QUALIFICATION_PLAN_PROTOCOL: &str =
    "skein-production-content-store-read-plan-v1";

fn main() -> ExitCode {
    match run(std::env::args().skip(1)) {
        Ok(Some(report)) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report.json())
                    .expect("content-store read qualification report must serialize")
            );
            if report.ready {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Ok(None) => {
            println!("{}", usage());
            ExitCode::SUCCESS
        }
        Err(error) => {
            println!(
                "{}",
                serde_json::json!({
                    "protocol": PRODUCTION_CONTENT_STORE_STORAGE_QUALIFICATION_PROTOCOL,
                    "evidence_kind": "representative_production_relational_replica",
                    "production_eligible": true,
                    "ready": false,
                    "blocker_codes": ["qualification_input_invalid"],
                    "errors": ["qualification_failed"],
                })
            );
            eprintln!("skein-content-store-read-qualification: {error}");
            ExitCode::from(2)
        }
    }
}

fn run(
    args: impl IntoIterator<Item = String>,
) -> Result<Option<skein_qualification::ProductionContentStoreStorageQualificationReport>, String> {
    let Some((database_path, plan_path)) = parse_args(args)? else {
        return Ok(None);
    };
    let plan = read_plan(&plan_path)?;
    let config = plan.into_config(database_path)?;
    run_production_content_store_storage_qualification(config)
        .map(Some)
        .map_err(|error| error.to_string())
}

fn parse_args(
    args: impl IntoIterator<Item = String>,
) -> Result<Option<(PathBuf, PathBuf)>, String> {
    let mut database_path = None;
    let mut plan_path = None;
    let mut args = args.into_iter();
    while let Some(argument) = args.next() {
        if matches!(argument.as_str(), "--help" | "-h") {
            return Ok(None);
        }
        let value = args
            .next()
            .ok_or_else(|| format!("missing value for {argument}"))?;
        match argument.as_str() {
            "--database-path" if database_path.is_none() => {
                database_path = Some(PathBuf::from(value));
            }
            "--plan-json" if plan_path.is_none() => {
                plan_path = Some(PathBuf::from(value));
            }
            "--database-path" | "--plan-json" => {
                return Err(format!("duplicate argument '{argument}'"));
            }
            _ => return Err(format!("unknown argument '{argument}'\n{}", usage())),
        }
    }
    Ok(Some((
        database_path.ok_or_else(|| "--database-path is required".to_string())?,
        plan_path.ok_or_else(|| "--plan-json is required".to_string())?,
    )))
}

fn read_plan(path: &Path) -> Result<ContentStoreReadQualificationPlan, String> {
    read_bounded_json(path, "content-store read qualification plan")
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContentStoreReadQualificationPlan {
    protocol: String,
    evidence_binding: EvidenceBindingInput,
    expected_identity: ProductionIdentityInput,
    resource_profile: ResourceProfileInput,
    database: DatabaseInput,
    measurement_runs: usize,
    open_payload_cache_limits: OpenPayloadCacheLimitsInput,
    resource_limits: ResourceLimitsInput,
    read_cases: Vec<ReadCaseInput>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenPayloadCacheLimitsInput {
    max_requests: u64,
    max_resident_bytes: u64,
}

impl From<OpenPayloadCacheLimitsInput> for ProductionContentStoreOpenCacheLimits {
    fn from(input: OpenPayloadCacheLimitsInput) -> Self {
        Self {
            max_requests: input.max_requests,
            max_resident_bytes: input.max_resident_bytes,
        }
    }
}

impl ContentStoreReadQualificationPlan {
    fn into_config(
        self,
        database_path: PathBuf,
    ) -> Result<ProductionContentStoreStorageQualificationConfig, String> {
        if self.protocol != CONTENT_STORE_READ_QUALIFICATION_PLAN_PROTOCOL {
            return Err(format!(
                "content-store read qualification plan protocol must be {CONTENT_STORE_READ_QUALIFICATION_PLAN_PROTOCOL}"
            ));
        }
        let (resource_profile_kind, configured_available_memory_bytes, runtime_governor_config) =
            self.resource_profile.resolve()?;
        let database_config = self.database.resolve(true)?;
        let read_cases = self
            .read_cases
            .into_iter()
            .map(ReadCaseInput::resolve)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ProductionContentStoreStorageQualificationConfig {
            database_path,
            database_config,
            runtime_governor_config,
            resource_profile_kind,
            configured_available_memory_bytes,
            evidence_binding: self.evidence_binding.into(),
            expected_identity: self.expected_identity.into(),
            measurement_runs: self.measurement_runs,
            open_payload_cache_limits: self.open_payload_cache_limits.into(),
            resource_limits: self.resource_limits.into(),
            read_cases,
        })
    }
}

fn usage() -> &'static str {
    "usage: skein-content-store-read-qualification \
     --database-path <existing-read-only-skein-directory> --plan-json <path>"
}

#[cfg(test)]
mod tests {
    use super::*;
    use skein::{
        ProductionQualificationIdentity, RelationalIndexMode, StorageResidencyMode, Value,
        PRODUCTION_QUALIFICATION_POLICY_VERSION,
    };
    use skein_qualification::{
        ContentStoreResourceProfileKind, CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
        CONTENT_STORE_SHARED_HOST_8_GIB_BYTES,
    };

    #[test]
    fn plan_builds_fixed_read_only_authoritative_profile() {
        let plan: ContentStoreReadQualificationPlan =
            serde_json::from_value(valid_plan_json()).unwrap();
        let config = plan
            .into_config(PathBuf::from("representative-copy"))
            .unwrap();

        assert!(config.database_config.read_only);
        assert_eq!(
            config.database_config.storage_residency_mode,
            StorageResidencyMode::OutOfCore
        );
        assert_eq!(
            config.database_config.relational_index_mode,
            RelationalIndexMode::Authoritative
        );
        assert_eq!(
            config.runtime_governor_config.memory_budget_bytes,
            Some(CONTENT_STORE_512_MIB_CAPABILITY_BYTES)
        );
        assert_eq!(
            config.read_cases[0].parameters[0],
            Value::String("thread-1".into())
        );
        assert_eq!(config.read_cases[0].parameters[1], Value::Int(32));
    }

    #[test]
    fn shared_host_profile_keeps_dynamic_governor() {
        let mut json = valid_plan_json();
        json["resource_profile"] = serde_json::json!("shared_host_8_gib");
        let plan: ContentStoreReadQualificationPlan = serde_json::from_value(json).unwrap();
        let config = plan
            .into_config(PathBuf::from("representative-copy"))
            .unwrap();

        assert_eq!(
            config.resource_profile_kind,
            ContentStoreResourceProfileKind::SharedHost8Gib
        );
        assert_eq!(
            config.configured_available_memory_bytes,
            CONTENT_STORE_SHARED_HOST_8_GIB_BYTES
        );
        assert_eq!(config.runtime_governor_config.memory_budget_bytes, None);
    }

    #[test]
    fn configured_profile_requires_a_bounded_nonzero_ceiling() {
        let mut valid = valid_plan_json();
        valid["resource_profile"] = serde_json::json!({
            "configured_workload": {
                "available_memory_bytes": 1_073_741_824_u64,
                "runtime_memory_ceiling_bytes": 536_870_912_u64
            }
        });
        let plan: ContentStoreReadQualificationPlan = serde_json::from_value(valid).unwrap();
        let config = plan
            .into_config(PathBuf::from("representative-copy"))
            .unwrap();
        assert_eq!(
            config.runtime_governor_config.memory_budget_bytes,
            Some(536_870_912)
        );

        let mut invalid = valid_plan_json();
        invalid["resource_profile"] = serde_json::json!({
            "configured_workload": {
                "available_memory_bytes": 536_870_912_u64,
                "runtime_memory_ceiling_bytes": 1_073_741_824_u64
            }
        });
        let plan: ContentStoreReadQualificationPlan = serde_json::from_value(invalid).unwrap();
        assert!(plan
            .into_config(PathBuf::from("representative-copy"))
            .unwrap_err()
            .contains("must not exceed"));
    }

    #[test]
    fn rejects_unknown_protocol_and_oversized_integer_parameter() {
        let mut protocol = valid_plan_json();
        protocol["protocol"] = serde_json::json!("unknown");
        let plan: ContentStoreReadQualificationPlan = serde_json::from_value(protocol).unwrap();
        assert!(plan
            .into_config(PathBuf::from("representative-copy"))
            .unwrap_err()
            .contains("protocol"));

        let mut integer = valid_plan_json();
        integer["read_cases"][0]["parameters"][1] = serde_json::json!(u64::MAX);
        let plan: ContentStoreReadQualificationPlan = serde_json::from_value(integer).unwrap();
        assert!(plan
            .into_config(PathBuf::from("representative-copy"))
            .unwrap_err()
            .contains("exceeds i64"));

        let mut zero = valid_plan_json();
        zero["database"]["segment_cache_capacity_bytes"] = serde_json::json!(0);
        let plan: ContentStoreReadQualificationPlan = serde_json::from_value(zero).unwrap();
        assert!(plan
            .into_config(PathBuf::from("representative-copy"))
            .unwrap_err()
            .contains("segment_cache_capacity_bytes must be non-zero"));
    }

    #[test]
    fn rejects_unknown_nested_identity_fields() {
        let mut json = valid_plan_json();
        json["expected_identity"]["unexpected"] = serde_json::json!(true);

        let error = serde_json::from_value::<ContentStoreReadQualificationPlan>(json)
            .unwrap_err()
            .to_string();

        assert!(error.contains("unknown field `unexpected`"));
    }

    #[test]
    fn checked_in_example_plan_matches_the_parser_contract() {
        let plan: ContentStoreReadQualificationPlan = serde_json::from_str(include_str!(
            "../../fixtures/nowledge_content_store/production_read_plan_example_v1.json"
        ))
        .unwrap();
        let config = plan
            .into_config(PathBuf::from("representative-copy"))
            .unwrap();

        assert_eq!(config.measurement_runs, 5);
        assert_eq!(
            config.resource_profile_kind,
            ContentStoreResourceProfileKind::SharedHost8Gib
        );
        assert_eq!(config.read_cases.len(), 1);
    }

    #[test]
    fn argument_parser_requires_both_paths() {
        assert_eq!(parse_args(["--help".to_string()]).unwrap(), None);
        assert!(
            parse_args(["--database-path".to_string(), "copy".to_string()])
                .unwrap_err()
                .contains("--plan-json")
        );
        assert!(parse_args([
            "--database-path".to_string(),
            "copy-a".to_string(),
            "--database-path".to_string(),
            "copy-b".to_string(),
            "--plan-json".to_string(),
            "plan.json".to_string(),
        ])
        .unwrap_err()
        .contains("duplicate"));
    }

    fn valid_plan_json() -> serde_json::Value {
        let identity = ProductionQualificationIdentity {
            source_revision: "revision".to_string(),
            rust_toolchain: "rustc".to_string(),
            target_os: std::env::consts::OS.to_string(),
            target_arch: std::env::consts::ARCH.to_string(),
            enabled_features: Vec::new(),
            durable_format_version: 1,
            schema_version: 1,
            configuration_digest: "config".to_string(),
            deployment_profile: "representative-copy".to_string(),
            dataset_fingerprint: "dataset".to_string(),
            canonical_graph_commit_epoch: 1,
            policy_version: PRODUCTION_QUALIFICATION_POLICY_VERSION,
        };
        serde_json::json!({
            "protocol": CONTENT_STORE_READ_QUALIFICATION_PLAN_PROTOCOL,
            "evidence_binding": {
                "identity": identity.clone(),
                "generated_at_unix_seconds": 1
            },
            "expected_identity": identity,
            "resource_profile": "capability_512_mib",
            "database": {
                "max_read_result_rows": 1000,
                "max_read_result_payload_bytes": 1048576,
                "execution_batch_rows": 256,
                "execution_batch_payload_bytes": 1048576,
                "blocking_operator_bytes": 1048576,
                "segment_cache_capacity_bytes": 1048576,
                "max_relational_index_read_bytes": 1048576,
                "max_relational_hydration_bytes": 1048576
            },
            "measurement_runs": 2,
            "open_payload_cache_limits": {
                "max_requests": 16,
                "max_resident_bytes": 1048576
            },
            "resource_limits": {
                "max_steady_resident_bytes": CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
                "max_peak_resident_bytes": CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
                "max_total_page_faults_per_run": null,
                "max_minor_page_faults_per_run": null,
                "max_major_page_faults_per_run": null
            },
            "read_cases": [{
                "case_name": "thread-page",
                "statement_name": "thread_messages_page",
                "parameters": ["thread-1", 32],
                "expected_output_rows": 0,
                "expected_output_sha256": "0".repeat(64),
                "max_intermediate_rows": 1000,
                "max_physical_pages_per_run": 100,
                "max_physical_bytes_per_run": 1048576
            }]
        })
    }
}
