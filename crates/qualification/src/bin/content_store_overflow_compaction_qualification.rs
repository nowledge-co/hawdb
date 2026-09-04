#[path = "shared/content_store_qualification_input.rs"]
mod content_store_qualification_input;
#[path = "shared/qualification_input.rs"]
mod qualification_input;
#[path = "shared/qualification_value.rs"]
mod qualification_value;
#[path = "shared/relational_database_input.rs"]
mod relational_database_input;

use content_store_qualification_input::{ReadCaseInput, ResourceLimitsInput, ResourceProfileInput};
use qualification_input::{read_bounded_json, EvidenceBindingInput, ProductionIdentityInput};
use relational_database_input::DatabaseInput;
use serde::Deserialize;
use skein::RelationalOverflowCompactionConfig;
use skein_qualification::{
    run_production_content_store_overflow_compaction_qualification,
    ProductionContentStoreOverflowCompactionLimits,
    ProductionContentStoreOverflowCompactionQualificationConfig,
    PRODUCTION_CONTENT_STORE_OVERFLOW_COMPACTION_QUALIFICATION_PROTOCOL,
};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::process::ExitCode;

const CONTENT_STORE_OVERFLOW_COMPACTION_PLAN_PROTOCOL: &str =
    "skein-production-content-store-overflow-compaction-plan-v1";

fn main() -> ExitCode {
    match run(std::env::args().skip(1)) {
        Ok(Some(report)) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report.json())
                    .expect("overflow compaction qualification report must serialize")
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
                    "protocol": PRODUCTION_CONTENT_STORE_OVERFLOW_COMPACTION_QUALIFICATION_PROTOCOL,
                    "evidence_kind": "representative_production_relational_overflow_compaction",
                    "production_eligible": true,
                    "ready": false,
                    "blocker_codes": ["qualification_input_invalid"],
                    "errors": ["qualification_failed"],
                })
            );
            eprintln!("skein-content-store-overflow-compaction-qualification: {error}");
            ExitCode::from(2)
        }
    }
}

fn run(
    args: impl IntoIterator<Item = String>,
) -> Result<
    Option<skein_qualification::ProductionContentStoreOverflowCompactionQualificationReport>,
    String,
> {
    let Some((replica_path, plan_path)) = parse_args(args)? else {
        return Ok(None);
    };
    let plan: ContentStoreOverflowCompactionQualificationPlan = read_bounded_json(
        &plan_path,
        "content-store overflow compaction qualification plan",
    )?;
    let config = plan.into_config(replica_path)?;
    run_production_content_store_overflow_compaction_qualification(config)
        .map(Some)
        .map_err(|error| error.to_string())
}

fn parse_args(
    args: impl IntoIterator<Item = String>,
) -> Result<Option<(PathBuf, PathBuf)>, String> {
    let mut replica_path = None;
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
            "--replica-path" if replica_path.is_none() => {
                replica_path = Some(PathBuf::from(value));
            }
            "--plan-json" if plan_path.is_none() => {
                plan_path = Some(PathBuf::from(value));
            }
            "--replica-path" | "--plan-json" => {
                return Err(format!("duplicate argument '{argument}'"));
            }
            _ => return Err(format!("unknown argument '{argument}'\n{}", usage())),
        }
    }
    Ok(Some((
        replica_path.ok_or_else(|| "--replica-path is required".to_string())?,
        plan_path.ok_or_else(|| "--plan-json is required".to_string())?,
    )))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContentStoreOverflowCompactionQualificationPlan {
    protocol: String,
    evidence_binding: EvidenceBindingInput,
    expected_identity: ProductionIdentityInput,
    resource_profile: ResourceProfileInput,
    runtime_result_budget_bytes: u64,
    database: DatabaseInput,
    compaction: OverflowCompactionInput,
    limits: OverflowCompactionLimitsInput,
    verification_cases: Vec<ReadCaseInput>,
}

impl ContentStoreOverflowCompactionQualificationPlan {
    fn into_config(
        self,
        replica_path: PathBuf,
    ) -> Result<ProductionContentStoreOverflowCompactionQualificationConfig, String> {
        if self.protocol != CONTENT_STORE_OVERFLOW_COMPACTION_PLAN_PROTOCOL {
            return Err(format!(
                "content-store overflow compaction qualification plan protocol must be {CONTENT_STORE_OVERFLOW_COMPACTION_PLAN_PROTOCOL}"
            ));
        }
        let (resource_profile_kind, configured_available_memory_bytes, mut runtime_governor_config) =
            self.resource_profile.resolve()?;
        if self.runtime_result_budget_bytes == 0
            || self.runtime_result_budget_bytes > configured_available_memory_bytes
        {
            return Err(
                "overflow compaction runtime result budget must be non-zero and within the configured memory profile"
                    .to_string(),
            );
        }
        runtime_governor_config.result_budget_bytes = self.runtime_result_budget_bytes;
        Ok(
            ProductionContentStoreOverflowCompactionQualificationConfig {
                replica_path,
                database_config: self.database.resolve(false)?,
                runtime_governor_config,
                resource_profile_kind,
                configured_available_memory_bytes,
                evidence_binding: self.evidence_binding.into(),
                expected_identity: self.expected_identity.into(),
                compaction: self.compaction.resolve()?,
                limits: self.limits.into(),
                verification_cases: self
                    .verification_cases
                    .into_iter()
                    .map(ReadCaseInput::resolve)
                    .collect::<Result<Vec<_>, _>>()?,
            },
        )
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OverflowCompactionInput {
    max_scan_rows: usize,
    max_scan_pages: usize,
    max_scan_bytes: usize,
    max_overlay_entries: usize,
    max_overlay_bytes: usize,
    max_rewrite_bytes: u64,
    reference_sort: OverflowReferenceSortInput,
}

impl OverflowCompactionInput {
    fn resolve(self) -> Result<RelationalOverflowCompactionConfig, String> {
        let mut config = RelationalOverflowCompactionConfig {
            max_scan_rows: nonzero_usize("max_scan_rows", self.max_scan_rows)?,
            max_scan_pages: nonzero_usize("max_scan_pages", self.max_scan_pages)?,
            max_scan_bytes: nonzero_usize("max_scan_bytes", self.max_scan_bytes)?,
            max_overlay_entries: nonzero_usize("max_overlay_entries", self.max_overlay_entries)?,
            max_overlay_bytes: nonzero_usize("max_overlay_bytes", self.max_overlay_bytes)?,
            max_rewrite_bytes: nonzero_u64("max_rewrite_bytes", self.max_rewrite_bytes)?,
            ..RelationalOverflowCompactionConfig::default()
        };
        self.reference_sort.apply_to(&mut config)?;
        Ok(config)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OverflowReferenceSortInput {
    max_memory_bytes: usize,
    max_spill_bytes: u64,
    max_runs: usize,
    max_reference_occurrences: u64,
}

impl OverflowReferenceSortInput {
    fn apply_to(self, config: &mut RelationalOverflowCompactionConfig) -> Result<(), String> {
        config.reference_sort.max_memory_bytes =
            nonzero_usize("max_memory_bytes", self.max_memory_bytes)?;
        config.reference_sort.max_spill_bytes =
            nonzero_u64("max_spill_bytes", self.max_spill_bytes)?;
        config.reference_sort.max_runs = nonzero_usize("max_runs", self.max_runs)?;
        config.reference_sort.max_reference_occurrences =
            nonzero_u64("max_reference_occurrences", self.max_reference_occurrences)?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OverflowCompactionLimitsInput {
    process: ResourceLimitsInput,
    max_compaction_elapsed_micros: u64,
    max_cleanup_elapsed_micros: u64,
    max_new_generation_artifact_bytes: u64,
    max_new_artifact_write_amplification_per_million: u64,
    min_reclaimable_base_extent_count: u64,
    min_physically_removed_extent_bytes: u64,
}

impl From<OverflowCompactionLimitsInput> for ProductionContentStoreOverflowCompactionLimits {
    fn from(input: OverflowCompactionLimitsInput) -> Self {
        Self {
            process: input.process.into(),
            max_compaction_elapsed_micros: input.max_compaction_elapsed_micros,
            max_cleanup_elapsed_micros: input.max_cleanup_elapsed_micros,
            max_new_generation_artifact_bytes: input.max_new_generation_artifact_bytes,
            max_new_artifact_write_amplification_per_million: input
                .max_new_artifact_write_amplification_per_million,
            min_reclaimable_base_extent_count: input.min_reclaimable_base_extent_count,
            min_physically_removed_extent_bytes: input.min_physically_removed_extent_bytes,
        }
    }
}

fn nonzero_usize(name: &str, value: usize) -> Result<NonZeroUsize, String> {
    NonZeroUsize::new(value).ok_or_else(|| format!("{name} must be non-zero"))
}

fn nonzero_u64(name: &str, value: u64) -> Result<NonZeroU64, String> {
    NonZeroU64::new(value).ok_or_else(|| format!("{name} must be non-zero"))
}

fn usage() -> &'static str {
    "usage: skein-content-store-overflow-compaction-qualification \
     --replica-path <caller-owned-disposable-skein-directory> --plan-json <path>"
}

#[cfg(test)]
mod tests {
    use super::*;
    use skein::{
        RelationalIndexMode, StorageResidencyMode, PRODUCTION_QUALIFICATION_POLICY_VERSION,
    };
    use skein_qualification::{
        ContentStoreResourceProfileKind, CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
        CONTENT_STORE_SHARED_HOST_8_GIB_BYTES,
    };

    #[test]
    fn plan_builds_a_bounded_writable_capability_profile() {
        let plan: ContentStoreOverflowCompactionQualificationPlan =
            serde_json::from_value(valid_plan_json()).unwrap();
        let config = plan
            .into_config(PathBuf::from("disposable-replica"))
            .unwrap();

        assert!(!config.database_config.read_only);
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
            config.compaction.max_scan_rows,
            NonZeroUsize::new(1_000_000).unwrap()
        );
        assert_eq!(config.verification_cases.len(), 1);
    }

    #[test]
    fn shared_host_profile_keeps_dynamic_memory_admission() {
        let mut json = valid_plan_json();
        json["resource_profile"] = serde_json::json!("shared_host_8_gib");
        let plan: ContentStoreOverflowCompactionQualificationPlan =
            serde_json::from_value(json).unwrap();
        let config = plan
            .into_config(PathBuf::from("disposable-replica"))
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
    fn plan_rejects_zero_compaction_bounds_and_unbounded_result_memory() {
        let mut zero = valid_plan_json();
        zero["compaction"]["max_scan_rows"] = serde_json::json!(0);
        let plan: ContentStoreOverflowCompactionQualificationPlan =
            serde_json::from_value(zero).unwrap();
        assert!(plan
            .into_config(PathBuf::from("disposable-replica"))
            .unwrap_err()
            .contains("max_scan_rows must be non-zero"));

        let mut result = valid_plan_json();
        result["runtime_result_budget_bytes"] =
            serde_json::json!(CONTENT_STORE_512_MIB_CAPABILITY_BYTES + 1);
        let plan: ContentStoreOverflowCompactionQualificationPlan =
            serde_json::from_value(result).unwrap();
        assert!(plan
            .into_config(PathBuf::from("disposable-replica"))
            .unwrap_err()
            .contains("within the configured memory profile"));
    }

    #[test]
    fn argument_parser_requires_one_replica_and_plan() {
        assert_eq!(parse_args(["--help".to_string()]).unwrap(), None);
        assert!(
            parse_args(["--replica-path".to_string(), "copy".to_string()])
                .unwrap_err()
                .contains("--plan-json")
        );
        assert!(parse_args([
            "--replica-path".to_string(),
            "copy-a".to_string(),
            "--replica-path".to_string(),
            "copy-b".to_string(),
            "--plan-json".to_string(),
            "plan.json".to_string(),
        ])
        .unwrap_err()
        .contains("duplicate"));
    }

    fn valid_plan_json() -> serde_json::Value {
        let identity = skein::ProductionQualificationIdentity {
            source_revision: "revision".to_string(),
            rust_toolchain: "rustc".to_string(),
            target_os: std::env::consts::OS.to_string(),
            target_arch: std::env::consts::ARCH.to_string(),
            enabled_features: Vec::new(),
            durable_format_version: 1,
            schema_version: 1,
            configuration_digest: "config".to_string(),
            deployment_profile: "disposable-replica".to_string(),
            dataset_fingerprint: "dataset".to_string(),
            canonical_graph_commit_epoch: 7,
            policy_version: PRODUCTION_QUALIFICATION_POLICY_VERSION,
        };
        serde_json::json!({
            "protocol": CONTENT_STORE_OVERFLOW_COMPACTION_PLAN_PROTOCOL,
            "evidence_binding": {
                "identity": identity.clone(),
                "generated_at_unix_seconds": 1
            },
            "expected_identity": identity,
            "resource_profile": "capability_512_mib",
            "runtime_result_budget_bytes": 134217728,
            "database": {
                "max_read_result_rows": 1000,
                "max_read_result_payload_bytes": 134217728,
                "execution_batch_rows": 256,
                "execution_batch_payload_bytes": 1048576,
                "blocking_operator_bytes": 1048576,
                "segment_cache_capacity_bytes": 1048576,
                "max_relational_index_read_bytes": 1048576,
                "max_relational_hydration_bytes": 67108864
            },
            "compaction": {
                "max_scan_rows": 1000000,
                "max_scan_pages": 100000,
                "max_scan_bytes": 1073741824,
                "max_overlay_entries": 100000,
                "max_overlay_bytes": 16777216,
                "max_rewrite_bytes": 137438953472_u64,
                "reference_sort": {
                    "max_memory_bytes": 8388608,
                    "max_spill_bytes": 134217728,
                    "max_runs": 32,
                    "max_reference_occurrences": 100000000
                }
            },
            "limits": {
                "process": {
                    "max_steady_resident_bytes": CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
                    "max_peak_resident_bytes": CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
                    "max_total_page_faults_per_run": null,
                    "max_minor_page_faults_per_run": null,
                    "max_major_page_faults_per_run": null
                },
                "max_compaction_elapsed_micros": 60000000,
                "max_cleanup_elapsed_micros": 60000000,
                "max_new_generation_artifact_bytes": 137438953472_u64,
                "max_new_artifact_write_amplification_per_million": 10000000,
                "min_reclaimable_base_extent_count": 1,
                "min_physically_removed_extent_bytes": 1
            },
            "verification_cases": [{
                "case_name": "thread-page",
                "statement_name": "thread_messages_page",
                "parameters": ["thread-1", 32, 0],
                "expected_output_rows": 32,
                "expected_output_sha256": "0".repeat(64),
                "max_intermediate_rows": 10000,
                "max_physical_pages_per_run": 1000,
                "max_physical_bytes_per_run": 134217728
            }]
        })
    }
}
