#[path = "shared/qualification_input.rs"]
mod qualification_input;

#[path = "shared/qualification_value.rs"]
mod qualification_value;
#[path = "shared/relational_database_input.rs"]
mod relational_database_input;

use qualification_input::{read_bounded_json, EvidenceBindingInput, ProductionIdentityInput};
use qualification_value::value_from_json;
use relational_database_input::DatabaseInput;
use serde::Deserialize;
use skein::{
    WalGroupCommitAdaptiveColdStartEvidence, WalGroupCommitAdaptivePolicyEvidence,
    WalGroupCommitAdaptiveSteadyStateEvidence, WalGroupCommitConfig, WalGroupCommitEvidence,
    WalGroupCommitTailLatencyEvidence,
};
use skein_qualification::{
    run_production_content_store_mutation_qualification, ContentStoreResourceProfileKind,
    ProductionContentStoreMutationKind, ProductionContentStoreMutationLatencyReference,
    ProductionContentStoreMutationMatrixCase, ProductionContentStoreMutationOperation,
    ProductionContentStoreMutationQualificationConfig,
    ProductionContentStoreMutationResourceLimits, ProductionContentStoreMutationVerificationCase,
    ProductionContentStoreMutationWorker, CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
    CONTENT_STORE_SHARED_HOST_8_GIB_BYTES,
    PRODUCTION_CONTENT_STORE_MUTATION_QUALIFICATION_PROTOCOL,
    PRODUCTION_CONTENT_STORE_WRITER_MATRIX,
};
use std::collections::BTreeMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

const CONTENT_STORE_MUTATION_QUALIFICATION_PLAN_PROTOCOL: &str =
    "skein-production-content-store-mutation-plan-v1";

fn main() -> ExitCode {
    match run(std::env::args().skip(1)) {
        Ok(Some(report)) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report.json())
                    .expect("content-store mutation qualification report must serialize")
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
                    "protocol": PRODUCTION_CONTENT_STORE_MUTATION_QUALIFICATION_PROTOCOL,
                    "evidence_kind": "representative_production_relational_mutation_replicas",
                    "production_eligible": true,
                    "ready": false,
                    "blocker_codes": ["qualification_input_invalid"],
                    "errors": ["qualification_failed"],
                })
            );
            eprintln!("skein-content-store-mutation-qualification: {error}");
            ExitCode::from(2)
        }
    }
}

fn run(
    args: impl IntoIterator<Item = String>,
) -> Result<Option<skein_qualification::ProductionContentStoreMutationQualificationReport>, String>
{
    let Some(paths) = parse_args(args)? else {
        return Ok(None);
    };
    let plan: ContentStoreMutationQualificationPlan = read_bounded_json(
        &paths.plan_path,
        "content-store mutation qualification plan",
    )?;
    let config = plan.into_config(paths.source_database_path, paths.replica_paths)?;
    run_production_content_store_mutation_qualification(config)
        .map(Some)
        .map_err(|error| error.to_string())
}

#[derive(Debug, PartialEq, Eq)]
struct CommandPaths {
    source_database_path: PathBuf,
    plan_path: PathBuf,
    replica_paths: BTreeMap<usize, PathBuf>,
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Option<CommandPaths>, String> {
    let mut source_database_path = None;
    let mut plan_path = None;
    let mut replica_paths = BTreeMap::new();
    let mut args = args.into_iter();
    while let Some(argument) = args.next() {
        if matches!(argument.as_str(), "--help" | "-h") {
            return Ok(None);
        }
        let value = args
            .next()
            .ok_or_else(|| format!("missing value for {argument}"))?;
        match argument.as_str() {
            "--source-database-path" if source_database_path.is_none() => {
                source_database_path = Some(PathBuf::from(value));
            }
            "--plan-json" if plan_path.is_none() => {
                plan_path = Some(PathBuf::from(value));
            }
            "--replica-1" => insert_replica_path(&mut replica_paths, 1, value)?,
            "--replica-4" => insert_replica_path(&mut replica_paths, 4, value)?,
            "--replica-8" => insert_replica_path(&mut replica_paths, 8, value)?,
            "--replica-10" => insert_replica_path(&mut replica_paths, 10, value)?,
            "--source-database-path" | "--plan-json" => {
                return Err(format!("duplicate argument '{argument}'"));
            }
            _ => return Err(format!("unknown argument '{argument}'\n{}", usage())),
        }
    }
    let expected = PRODUCTION_CONTENT_STORE_WRITER_MATRIX
        .into_iter()
        .collect::<Vec<_>>();
    let actual = replica_paths.keys().copied().collect::<Vec<_>>();
    if actual != expected {
        return Err(
            "--replica-1, --replica-4, --replica-8, and --replica-10 are required".to_string(),
        );
    }
    Ok(Some(CommandPaths {
        source_database_path: source_database_path
            .ok_or_else(|| "--source-database-path is required".to_string())?,
        plan_path: plan_path.ok_or_else(|| "--plan-json is required".to_string())?,
        replica_paths,
    }))
}

fn insert_replica_path(
    paths: &mut BTreeMap<usize, PathBuf>,
    writer_count: usize,
    value: String,
) -> Result<(), String> {
    if paths.insert(writer_count, PathBuf::from(value)).is_some() {
        return Err(format!("duplicate replica path for {writer_count} writers"));
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContentStoreMutationQualificationPlan {
    protocol: String,
    evidence_binding: EvidenceBindingInput,
    expected_identity: ProductionIdentityInput,
    resource_profile: MutationResourceProfileInput,
    database: DatabaseInput,
    wal_group_commit: WalGroupCommitInput,
    resource_limits: MutationResourceLimitsInput,
    max_commit_p95_regression_per_million: u32,
    latency_reference: MutationLatencyReferenceInput,
    cases: Vec<MutationCaseInput>,
}

impl ContentStoreMutationQualificationPlan {
    fn into_config(
        self,
        source_database_path: PathBuf,
        mut replica_paths: BTreeMap<usize, PathBuf>,
    ) -> Result<ProductionContentStoreMutationQualificationConfig, String> {
        if self.protocol != CONTENT_STORE_MUTATION_QUALIFICATION_PLAN_PROTOCOL {
            return Err(format!(
                "content-store mutation qualification plan protocol must be {CONTENT_STORE_MUTATION_QUALIFICATION_PLAN_PROTOCOL}"
            ));
        }
        let (resource_profile_kind, configured_available_memory_bytes) =
            self.resource_profile.resolve()?;
        let database_config = self.database.resolve(false)?;
        let wal_group_commit = self.wal_group_commit.resolve()?;
        let writer_counts = self
            .cases
            .iter()
            .map(|case| case.writer_count)
            .collect::<Vec<_>>();
        if writer_counts != PRODUCTION_CONTENT_STORE_WRITER_MATRIX {
            return Err(
                "mutation plan cases must be ordered exactly as 1, 4, 8, and 10 writers"
                    .to_string(),
            );
        }
        let cases = self
            .cases
            .into_iter()
            .map(|case| {
                let replica_path = replica_paths.remove(&case.writer_count).ok_or_else(|| {
                    format!("missing replica path for {} writers", case.writer_count)
                })?;
                case.resolve(replica_path)
            })
            .collect::<Result<Vec<_>, String>>()?;
        if !replica_paths.is_empty() {
            return Err("unused mutation replica path".to_string());
        }
        Ok(ProductionContentStoreMutationQualificationConfig {
            source_database_path,
            database_config,
            wal_group_commit,
            resource_profile_kind,
            configured_available_memory_bytes,
            resource_limits: self.resource_limits.into(),
            max_commit_p95_regression_per_million: self.max_commit_p95_regression_per_million,
            latency_reference: self.latency_reference.into(),
            evidence_binding: self.evidence_binding.into(),
            expected_identity: self.expected_identity.into(),
            cases,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
enum MutationResourceProfileInput {
    #[serde(rename = "capability_512_mib")]
    Capability512Mib,
    #[serde(rename = "shared_host_8_gib")]
    SharedHost8Gib,
    ConfiguredWorkload {
        available_memory_bytes: u64,
    },
}

impl MutationResourceProfileInput {
    fn resolve(self) -> Result<(ContentStoreResourceProfileKind, u64), String> {
        match self {
            Self::Capability512Mib => Ok((
                ContentStoreResourceProfileKind::Capability512Mib,
                CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
            )),
            Self::SharedHost8Gib => Ok((
                ContentStoreResourceProfileKind::SharedHost8Gib,
                CONTENT_STORE_SHARED_HOST_8_GIB_BYTES,
            )),
            Self::ConfiguredWorkload {
                available_memory_bytes,
            } if available_memory_bytes > 0 => Ok((
                ContentStoreResourceProfileKind::ConfiguredWorkload,
                available_memory_bytes,
            )),
            Self::ConfiguredWorkload { .. } => {
                Err("configured workload available memory must be non-zero".to_string())
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MutationResourceLimitsInput {
    max_steady_resident_bytes: u64,
    max_peak_resident_bytes: u64,
    max_total_page_faults_per_case: Option<u64>,
    max_minor_page_faults_per_case: Option<u64>,
    max_major_page_faults_per_case: Option<u64>,
}

impl From<MutationResourceLimitsInput> for ProductionContentStoreMutationResourceLimits {
    fn from(input: MutationResourceLimitsInput) -> Self {
        Self {
            max_steady_resident_bytes: input.max_steady_resident_bytes,
            max_peak_resident_bytes: input.max_peak_resident_bytes,
            max_total_page_faults_per_case: input.max_total_page_faults_per_case,
            max_minor_page_faults_per_case: input.max_minor_page_faults_per_case,
            max_major_page_faults_per_case: input.max_major_page_faults_per_case,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MutationLatencyReferenceInput {
    source_revision: String,
    configuration_digest: String,
    dataset_fingerprint: String,
    generated_at_unix_seconds: u64,
}

impl From<MutationLatencyReferenceInput> for ProductionContentStoreMutationLatencyReference {
    fn from(input: MutationLatencyReferenceInput) -> Self {
        Self {
            source_revision: input.source_revision,
            configuration_digest: input.configuration_digest,
            dataset_fingerprint: input.dataset_fingerprint,
            generated_at_unix_seconds: input.generated_at_unix_seconds,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MutationCaseInput {
    writer_count: usize,
    workers: Vec<MutationWorkerInput>,
    verification_cases: Vec<MutationVerificationInput>,
    reference_commit_p95_micros: u64,
    max_commit_p95_micros: u64,
}

impl MutationCaseInput {
    fn resolve(
        self,
        replica_path: PathBuf,
    ) -> Result<ProductionContentStoreMutationMatrixCase, String> {
        Ok(ProductionContentStoreMutationMatrixCase {
            replica_path,
            writer_count: self.writer_count,
            workers: self
                .workers
                .into_iter()
                .map(MutationWorkerInput::resolve)
                .collect::<Result<Vec<_>, _>>()?,
            verification_cases: self
                .verification_cases
                .into_iter()
                .map(MutationVerificationInput::resolve)
                .collect::<Result<Vec<_>, _>>()?,
            reference_commit_p95_micros: self.reference_commit_p95_micros,
            max_commit_p95_micros: self.max_commit_p95_micros,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MutationWorkerInput {
    conflict_domain: String,
    operations: Vec<MutationOperationInput>,
}

impl MutationWorkerInput {
    fn resolve(self) -> Result<ProductionContentStoreMutationWorker, String> {
        Ok(ProductionContentStoreMutationWorker {
            conflict_domain: self.conflict_domain,
            operations: self
                .operations
                .into_iter()
                .map(MutationOperationInput::resolve)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MutationOperationInput {
    statement_name: String,
    parameters: Vec<serde_json::Value>,
    kind: MutationKindInput,
}

impl MutationOperationInput {
    fn resolve(self) -> Result<ProductionContentStoreMutationOperation, String> {
        Ok(ProductionContentStoreMutationOperation {
            statement_name: self.statement_name,
            parameters: self
                .parameters
                .iter()
                .map(value_from_json)
                .collect::<Result<Vec<_>, _>>()?,
            kind: self.kind.into(),
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MutationKindInput {
    Insert,
    Update,
}

impl From<MutationKindInput> for ProductionContentStoreMutationKind {
    fn from(input: MutationKindInput) -> Self {
        match input {
            MutationKindInput::Insert => Self::Insert,
            MutationKindInput::Update => Self::Update,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MutationVerificationInput {
    case_name: String,
    statement_name: String,
    parameters: Vec<serde_json::Value>,
    expected_output_rows: usize,
    expected_output_sha256: String,
}

impl MutationVerificationInput {
    fn resolve(self) -> Result<ProductionContentStoreMutationVerificationCase, String> {
        Ok(ProductionContentStoreMutationVerificationCase {
            case_name: self.case_name,
            statement_name: self.statement_name,
            parameters: self
                .parameters
                .iter()
                .map(value_from_json)
                .collect::<Result<Vec<_>, _>>()?,
            expected_output_rows: self.expected_output_rows,
            expected_output_sha256: self.expected_output_sha256,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WalGroupCommitInput {
    delay_policy: WalGroupDelayPolicyInput,
    max_entries: usize,
    max_bytes: u64,
    max_delay_micros: u64,
    evidence: WalGroupCommitEvidenceInput,
}

impl WalGroupCommitInput {
    fn resolve(self) -> Result<WalGroupCommitConfig, String> {
        let max_entries = NonZeroUsize::new(self.max_entries)
            .ok_or_else(|| "WAL group commit max_entries must be non-zero".to_string())?;
        let max_bytes = NonZeroU64::new(self.max_bytes)
            .ok_or_else(|| "WAL group commit max_bytes must be non-zero".to_string())?;
        let max_delay = Duration::from_micros(self.max_delay_micros);
        let evidence = self.evidence.into();
        match self.delay_policy {
            WalGroupDelayPolicyInput::Fixed => WalGroupCommitConfig::enabled_after_evidence(
                evidence,
                max_entries,
                max_bytes,
                max_delay,
            ),
            WalGroupDelayPolicyInput::AdaptiveFsync => {
                WalGroupCommitConfig::adaptive_enabled_after_evidence(
                    evidence,
                    max_entries,
                    max_bytes,
                    max_delay,
                )
            }
        }
        .map_err(|error| error.to_string())
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum WalGroupDelayPolicyInput {
    Fixed,
    AdaptiveFsync,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WalGroupCommitEvidenceInput {
    measurement_rounds: usize,
    commit_count: usize,
    baseline_elapsed_micros: u64,
    baseline_fsync_count: u64,
    grouped_elapsed_micros: u64,
    grouped_fsync_count: u64,
    concurrent_tail_latency: WalGroupTailLatencyInput,
    single_writer_tail_latency: WalGroupTailLatencyInput,
    single_writer_max_coalescing_wait_count: u64,
    single_writer_max_observed_group_entries: usize,
    adaptive_cold_start_behavior: Option<WalGroupAdaptiveColdStartInput>,
    adaptive_steady_state_behavior: Option<WalGroupAdaptiveSteadyStateInput>,
    strict_recovery_verified: bool,
    wal_order_verified: bool,
}

impl From<WalGroupCommitEvidenceInput> for WalGroupCommitEvidence {
    fn from(input: WalGroupCommitEvidenceInput) -> Self {
        Self {
            measurement_rounds: input.measurement_rounds,
            commit_count: input.commit_count,
            baseline_elapsed_micros: input.baseline_elapsed_micros,
            baseline_fsync_count: input.baseline_fsync_count,
            grouped_elapsed_micros: input.grouped_elapsed_micros,
            grouped_fsync_count: input.grouped_fsync_count,
            concurrent_tail_latency: input.concurrent_tail_latency.into(),
            single_writer_tail_latency: input.single_writer_tail_latency.into(),
            single_writer_max_coalescing_wait_count: input.single_writer_max_coalescing_wait_count,
            single_writer_max_observed_group_entries: input
                .single_writer_max_observed_group_entries,
            adaptive_cold_start_behavior: input.adaptive_cold_start_behavior.map(Into::into),
            adaptive_steady_state_behavior: input.adaptive_steady_state_behavior.map(Into::into),
            strict_recovery_verified: input.strict_recovery_verified,
            wal_order_verified: input.wal_order_verified,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WalGroupTailLatencyInput {
    commit_count: usize,
    paired_p95_regression_micros: i64,
    paired_p95_mad_micros: u64,
    max_accepted_p95_regression_micros: u64,
}

impl From<WalGroupTailLatencyInput> for WalGroupCommitTailLatencyEvidence {
    fn from(input: WalGroupTailLatencyInput) -> Self {
        Self {
            commit_count: input.commit_count,
            paired_p95_regression_micros: input.paired_p95_regression_micros,
            paired_p95_mad_micros: input.paired_p95_mad_micros,
            max_accepted_p95_regression_micros: input.max_accepted_p95_regression_micros,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WalGroupAdaptiveColdStartInput {
    commit_count: usize,
    min_fallback_delay_count: u64,
    min_coalescing_wait_count: u64,
    min_observed_group_entries: usize,
}

impl From<WalGroupAdaptiveColdStartInput> for WalGroupCommitAdaptiveColdStartEvidence {
    fn from(input: WalGroupAdaptiveColdStartInput) -> Self {
        Self {
            commit_count: input.commit_count,
            min_fallback_delay_count: input.min_fallback_delay_count,
            min_coalescing_wait_count: input.min_coalescing_wait_count,
            min_observed_group_entries: input.min_observed_group_entries,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WalGroupAdaptiveSteadyStateInput {
    commit_count: usize,
    max_fallback_delay_count: u64,
    min_fsync_baseline_sample_count: u64,
    min_coalescing_wait_count: u64,
    min_observed_group_entries: usize,
    safety_net: WalGroupAdaptivePolicyInput,
}

impl From<WalGroupAdaptiveSteadyStateInput> for WalGroupCommitAdaptiveSteadyStateEvidence {
    fn from(input: WalGroupAdaptiveSteadyStateInput) -> Self {
        Self {
            commit_count: input.commit_count,
            max_fallback_delay_count: input.max_fallback_delay_count,
            min_fsync_baseline_sample_count: input.min_fsync_baseline_sample_count,
            min_coalescing_wait_count: input.min_coalescing_wait_count,
            min_observed_group_entries: input.min_observed_group_entries,
            safety_net: input.safety_net.into(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WalGroupAdaptivePolicyInput {
    paired_elapsed_regression_micros: i64,
    paired_elapsed_mad_micros: u64,
    max_accepted_elapsed_regression_micros: u64,
    tail_latency: WalGroupTailLatencyInput,
}

impl From<WalGroupAdaptivePolicyInput> for WalGroupCommitAdaptivePolicyEvidence {
    fn from(input: WalGroupAdaptivePolicyInput) -> Self {
        Self {
            paired_elapsed_regression_micros: input.paired_elapsed_regression_micros,
            paired_elapsed_mad_micros: input.paired_elapsed_mad_micros,
            max_accepted_elapsed_regression_micros: input.max_accepted_elapsed_regression_micros,
            tail_latency: input.tail_latency.into(),
        }
    }
}

fn usage() -> &'static str {
    "usage: skein-content-store-mutation-qualification \
     --source-database-path <read-only-source> --replica-1 <path> \
     --replica-4 <path> --replica-8 <path> --replica-10 <path> \
     --plan-json <path>"
}

#[cfg(test)]
mod tests {
    use super::*;
    use skein::{
        ProductionQualificationIdentity, RelationalIndexMode, StorageResidencyMode, Value,
        WalGroupCommitActivation, PRODUCTION_QUALIFICATION_POLICY_VERSION,
    };

    #[test]
    fn plan_binds_exact_writer_paths_and_writable_storage() {
        let plan: ContentStoreMutationQualificationPlan =
            serde_json::from_value(valid_plan_json()).unwrap();
        let config = plan
            .into_config(PathBuf::from("source"), replica_paths())
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
            config.wal_group_commit.activation(),
            WalGroupCommitActivation::EvidenceValidated
        );
        assert_eq!(
            config
                .cases
                .iter()
                .map(|case| case.writer_count)
                .collect::<Vec<_>>(),
            PRODUCTION_CONTENT_STORE_WRITER_MATRIX
        );
        assert_eq!(config.cases[3].replica_path, PathBuf::from("replica-10"));
        assert_eq!(
            config.cases[0].workers[0].operations[0].parameters[0],
            Value::String("document-1-0".to_string())
        );
    }

    #[test]
    fn checked_in_example_plan_matches_the_parser_contract() {
        let plan: ContentStoreMutationQualificationPlan = serde_json::from_str(include_str!(
            "../../fixtures/nowledge_content_store/production_mutation_plan_example_v1.json"
        ))
        .unwrap();
        let config = plan
            .into_config(PathBuf::from("source"), replica_paths())
            .unwrap();

        assert_eq!(
            config
                .cases
                .iter()
                .map(|case| case.writer_count)
                .collect::<Vec<_>>(),
            PRODUCTION_CONTENT_STORE_WRITER_MATRIX
        );
        assert_eq!(config.cases[3].workers.len(), 10);
    }

    #[test]
    fn plan_rejects_writer_order_and_invalid_wal_evidence() {
        let mut order = valid_plan_json();
        order["cases"].as_array_mut().unwrap().swap(0, 1);
        let plan: ContentStoreMutationQualificationPlan = serde_json::from_value(order).unwrap();
        assert!(plan
            .into_config(PathBuf::from("source"), replica_paths())
            .unwrap_err()
            .contains("ordered exactly"));

        let mut wal = valid_plan_json();
        wal["wal_group_commit"]["evidence"]["grouped_fsync_count"] =
            wal["wal_group_commit"]["evidence"]["baseline_fsync_count"].clone();
        let plan: ContentStoreMutationQualificationPlan = serde_json::from_value(wal).unwrap();
        assert!(plan
            .into_config(PathBuf::from("source"), replica_paths())
            .unwrap_err()
            .contains("fsync_reduction_not_proven"));
    }

    #[test]
    fn argument_parser_requires_complete_replica_slots() {
        assert_eq!(parse_args(["--help".to_string()]).unwrap(), None);
        assert!(parse_args([
            "--source-database-path".to_string(),
            "source".to_string(),
            "--plan-json".to_string(),
            "plan.json".to_string(),
            "--replica-1".to_string(),
            "one".to_string(),
        ])
        .unwrap_err()
        .contains("--replica-4"));
        assert!(parse_args([
            "--source-database-path".to_string(),
            "source".to_string(),
            "--plan-json".to_string(),
            "plan.json".to_string(),
            "--replica-1".to_string(),
            "one".to_string(),
            "--replica-1".to_string(),
            "again".to_string(),
        ])
        .unwrap_err()
        .contains("duplicate"));
    }

    fn replica_paths() -> BTreeMap<usize, PathBuf> {
        PRODUCTION_CONTENT_STORE_WRITER_MATRIX
            .into_iter()
            .map(|writers| (writers, PathBuf::from(format!("replica-{writers}"))))
            .collect()
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
        let cases = PRODUCTION_CONTENT_STORE_WRITER_MATRIX
            .into_iter()
            .map(mutation_case_json)
            .collect::<Vec<_>>();
        serde_json::json!({
            "protocol": CONTENT_STORE_MUTATION_QUALIFICATION_PLAN_PROTOCOL,
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
            "wal_group_commit": qualified_wal_json(),
            "resource_limits": {
                "max_steady_resident_bytes": CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
                "max_peak_resident_bytes": CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
                "max_total_page_faults_per_case": null,
                "max_minor_page_faults_per_case": null,
                "max_major_page_faults_per_case": null
            },
            "max_commit_p95_regression_per_million": 50000,
            "latency_reference": {
                "source_revision": "accepted-revision",
                "configuration_digest": "config",
                "dataset_fingerprint": "dataset",
                "generated_at_unix_seconds": 1
            },
            "cases": cases
        })
    }

    fn mutation_case_json(writer_count: usize) -> serde_json::Value {
        let workers = (0..writer_count)
            .map(|worker| {
                serde_json::json!({
                    "conflict_domain": format!("writer-{worker}"),
                    "operations": [{
                        "statement_name": "upsert_content_document",
                        "parameters": [
                            format!("document-{writer_count}-{worker}"),
                            "thread",
                            format!("owner-{writer_count}-{worker}"),
                            "default",
                            "application/x-nowledge-thread",
                            1,
                            "2026-08-16T00:00:00Z",
                            "2026-08-16T00:00:00Z"
                        ],
                        "kind": "insert"
                    }]
                })
            })
            .collect::<Vec<_>>();
        serde_json::json!({
            "writer_count": writer_count,
            "workers": workers,
            "verification_cases": [{
                "case_name": format!("verify-{writer_count}"),
                "statement_name": "thread_owned_document_ids",
                "parameters": [format!("owner-{writer_count}-0")],
                "expected_output_rows": 1,
                "expected_output_sha256": "0".repeat(64)
            }],
            "reference_commit_p95_micros": 1000000,
            "max_commit_p95_micros": 1000000
        })
    }

    fn qualified_wal_json() -> serde_json::Value {
        let tail = serde_json::json!({
            "commit_count": 16,
            "paired_p95_regression_micros": 1,
            "paired_p95_mad_micros": 1,
            "max_accepted_p95_regression_micros": 10
        });
        serde_json::json!({
            "delay_policy": "fixed",
            "max_entries": 16,
            "max_bytes": 1048576,
            "max_delay_micros": 250,
            "evidence": {
                "measurement_rounds": 9,
                "commit_count": 16,
                "baseline_elapsed_micros": 200,
                "baseline_fsync_count": 16,
                "grouped_elapsed_micros": 100,
                "grouped_fsync_count": 2,
                "concurrent_tail_latency": tail.clone(),
                "single_writer_tail_latency": tail,
                "single_writer_max_coalescing_wait_count": 0,
                "single_writer_max_observed_group_entries": 1,
                "adaptive_cold_start_behavior": null,
                "adaptive_steady_state_behavior": null,
                "strict_recovery_verified": true,
                "wal_order_verified": true
            }
        })
    }
}
