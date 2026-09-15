//! Previous-wrapper readiness preflight reduction and its developer file adapter.
//!
//! The embedded facade retains command dispatch. This owner evaluates supplied
//! evidence and provides the same typed and preflight contracts to that facade.

use skein_core::{Result, SkeinError, GRAPH_RAG_SCHEMA_CONTEXT_PROTOCOL};
use skein_evidence::replacement_contract::{
    NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL, NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE, NOWLEDGE_MEM_SEARCH_CANDIDATE_PRIMARY_ENGINE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
    NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS,
};
use skein_route_ownership::graph::{
    nowledge_mem_graph_read_route_catalog_digest, NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION,
    REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

pub const NOWLEDGE_PREVIOUS_WRAPPER_PREFLIGHT_PROTOCOL: &str =
    "skein-nowledge-previous-wrapper-preflight-check";
pub use crate::bounded_read_evidence::NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL;
pub const NOWLEDGE_MEM_CUTOVER_CONTROLS_PROTOCOL: &str = "skein-nowledge-mem-cutover-controls-v1";
pub const NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL: &str = "skein-nowledge-mem-library-readiness-v1";
pub const NOWLEDGE_MEM_OPERATIONS_READINESS_PROTOCOL: &str =
    "skein-nowledge-mem-operations-readiness-v1";
pub const NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL: &str =
    "skein-nowledge-query-runtime-preflight-v1";
const SKEIN_NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-search-projection-evidence";
const SKEIN_NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-search-projection-shadow-evidence";
const SKEIN_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE: &str = "skein-rust-library";

#[derive(Debug, Clone)]
pub struct NowledgePreviousWrapperPreflightInputs {
    pub wrapper_identity: String,
    pub contract_evidence: serde_json::Value,
    pub adapter_smoke: serde_json::Value,
    pub migration_gate: serde_json::Value,
    pub replacement_summary: serde_json::Value,
    pub query_runtime_preflight: serde_json::Value,
    pub library_readiness: serde_json::Value,
    pub cutover_controls: serde_json::Value,
    pub operations_readiness: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct NowledgePreviousWrapperPreflightArtifacts {
    pub contract_evidence: serde_json::Value,
    pub adapter_smoke: serde_json::Value,
    pub migration_gate: serde_json::Value,
    pub replacement_summary: serde_json::Value,
    pub query_runtime_preflight: serde_json::Value,
    pub library_readiness: serde_json::Value,
    pub cutover_controls: serde_json::Value,
    pub operations_readiness: serde_json::Value,
}

impl NowledgePreviousWrapperPreflightInputs {
    pub fn new(
        wrapper_identity: impl Into<String>,
        artifacts: NowledgePreviousWrapperPreflightArtifacts,
    ) -> Result<Self> {
        let wrapper_identity = wrapper_identity.into();
        if wrapper_identity.trim().is_empty() {
            return Err(SkeinError::Semantic(
                "previous-wrapper preflight wrapper identity must not be empty".to_string(),
            ));
        }
        Ok(Self {
            wrapper_identity,
            contract_evidence: artifacts.contract_evidence,
            adapter_smoke: artifacts.adapter_smoke,
            migration_gate: artifacts.migration_gate,
            replacement_summary: artifacts.replacement_summary,
            query_runtime_preflight: artifacts.query_runtime_preflight,
            library_readiness: artifacts.library_readiness,
            cutover_controls: artifacts.cutover_controls,
            operations_readiness: artifacts.operations_readiness,
        })
    }
}

pub trait IntoNowledgePreviousWrapperPreflightInputs {
    fn into_nowledge_previous_wrapper_preflight_inputs(
        self,
    ) -> Result<NowledgePreviousWrapperPreflightInputs>;
}

impl IntoNowledgePreviousWrapperPreflightInputs for NowledgePreviousWrapperPreflightInputs {
    fn into_nowledge_previous_wrapper_preflight_inputs(
        self,
    ) -> Result<NowledgePreviousWrapperPreflightInputs> {
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgePreviousWrapperPreflightCheckReport {
    pub name: String,
    pub ready: bool,
    pub evidence_fields: Vec<String>,
    pub failed_evidence_fields: Vec<String>,
    pub blocker_codes: Vec<String>,
}

impl NowledgePreviousWrapperPreflightCheckReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "ready": self.ready,
            "evidence_fields": self.evidence_fields,
            "failed_evidence_fields": self.failed_evidence_fields,
            "blocker_codes": self.blocker_codes,
        })
    }
}

#[derive(Debug, Clone)]
pub struct NowledgePreviousWrapperPreflightReport {
    pub protocol: String,
    pub ready: bool,
    pub wrapper_identity: String,
    pub failed_checks: Vec<String>,
    pub release_summary: serde_json::Value,
    pub checks: Vec<NowledgePreviousWrapperPreflightCheckReport>,
}

impl NowledgePreviousWrapperPreflightReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "ready": self.ready,
            "wrapper_identity": self.wrapper_identity,
            "failed_checks": self.failed_checks,
            "release_summary": self.release_summary,
            "checks": self.checks.iter().map(NowledgePreviousWrapperPreflightCheckReport::json).collect::<Vec<_>>(),
        })
    }
}

pub fn nowledge_previous_wrapper_preflight_check_usage() -> String {
    "nowledge-previous-wrapper-preflight-check requires [--require-ready] --wrapper-identity <id> (--bundle-dir <dir> | --contract-evidence-json <path> --adapter-smoke-json <path> --migration-gate-json <path> --replacement-summary-json <path> --query-runtime-preflight-json <path> --library-readiness-json <path> --cutover-controls-json <path> --operations-readiness-json <path>)".to_string()
}

#[derive(Debug, Clone, Default)]
struct CliPreviousWrapperPreflightCheckInputs {
    wrapper_identity: Option<String>,
    bundle_dir: Option<String>,
    contract_evidence: Option<serde_json::Value>,
    adapter_smoke: Option<serde_json::Value>,
    migration_gate: Option<serde_json::Value>,
    replacement_summary: Option<serde_json::Value>,
    query_runtime_preflight: Option<serde_json::Value>,
    library_readiness: Option<serde_json::Value>,
    cutover_controls: Option<serde_json::Value>,
    operations_readiness: Option<serde_json::Value>,
}

#[cfg(test)]
type PreviousWrapperPreflightCheckInputs = CliPreviousWrapperPreflightCheckInputs;

impl IntoNowledgePreviousWrapperPreflightInputs for CliPreviousWrapperPreflightCheckInputs {
    fn into_nowledge_previous_wrapper_preflight_inputs(
        self,
    ) -> Result<NowledgePreviousWrapperPreflightInputs> {
        cli_inputs_to_typed(self)
    }
}

pub fn run_nowledge_previous_wrapper_preflight_check(
    mut args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let mut require_ready = false;
    let mut inputs = CliPreviousWrapperPreflightCheckInputs::default();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--require-ready" => {
                require_ready = true;
            }
            "--wrapper-identity" => {
                let value = args.next().ok_or_else(|| {
                    SkeinError::Semantic(nowledge_previous_wrapper_preflight_check_usage())
                })?;
                if value.trim().is_empty() {
                    return Err(SkeinError::Semantic(
                        "--wrapper-identity must not be empty".to_string(),
                    ));
                }
                inputs.wrapper_identity = Some(value);
            }
            "--bundle-dir" => {
                let value = args.next().ok_or_else(|| {
                    SkeinError::Semantic(nowledge_previous_wrapper_preflight_check_usage())
                })?;
                if value.trim().is_empty() {
                    return Err(SkeinError::Semantic(
                        "--bundle-dir must not be empty".to_string(),
                    ));
                }
                inputs.bundle_dir = Some(value);
            }
            "--contract-evidence-json" => {
                inputs.contract_evidence = Some(read_json_arg(&mut args)?);
            }
            "--adapter-smoke-json" => {
                inputs.adapter_smoke = Some(read_json_arg(&mut args)?);
            }
            "--migration-gate-json" => {
                inputs.migration_gate = Some(read_json_arg(&mut args)?);
            }
            "--replacement-summary-json" => {
                inputs.replacement_summary = Some(read_json_arg(&mut args)?);
            }
            "--query-runtime-preflight-json" => {
                inputs.query_runtime_preflight = Some(read_json_arg(&mut args)?);
            }
            "--library-readiness-json" => {
                inputs.library_readiness = Some(read_json_arg(&mut args)?);
            }
            "--cutover-controls-json" => {
                inputs.cutover_controls = Some(read_json_arg(&mut args)?);
            }
            "--operations-readiness-json" => {
                inputs.operations_readiness = Some(read_json_arg(&mut args)?);
            }
            _ => {
                return Err(SkeinError::Semantic(
                    nowledge_previous_wrapper_preflight_check_usage(),
                ));
            }
        }
    }
    fill_bundle_dir_inputs(&mut inputs)?;
    let report = nowledge_previous_wrapper_preflight_check_json(cli_inputs_to_typed(inputs)?)?;
    Ok((report, require_ready))
}

fn fill_bundle_dir_inputs(inputs: &mut CliPreviousWrapperPreflightCheckInputs) -> Result<()> {
    let Some(bundle_dir) = inputs.bundle_dir.as_deref() else {
        return Ok(());
    };
    let bundle_dir = Path::new(bundle_dir);
    if inputs.contract_evidence.is_none() {
        inputs.contract_evidence =
            Some(read_json_file(&bundle_dir.join("contract-evidence.json"))?);
    }
    if inputs.adapter_smoke.is_none() {
        inputs.adapter_smoke = Some(read_json_file(&bundle_dir.join("adapter-smoke.json"))?);
    }
    if inputs.migration_gate.is_none() {
        inputs.migration_gate = Some(read_json_file(&bundle_dir.join("migration-gate.json"))?);
    }
    if inputs.replacement_summary.is_none() {
        inputs.replacement_summary = Some(read_json_file(
            &bundle_dir.join("replacement-summary.json"),
        )?);
    }
    if inputs.query_runtime_preflight.is_none() {
        inputs.query_runtime_preflight = Some(read_json_file(
            &bundle_dir.join("query-runtime-preflight.json"),
        )?);
    }
    if inputs.library_readiness.is_none() {
        inputs.library_readiness =
            Some(read_json_file(&bundle_dir.join("library-readiness.json"))?);
    }
    if inputs.cutover_controls.is_none() {
        inputs.cutover_controls = Some(read_json_file(&bundle_dir.join("cutover-controls.json"))?);
    }
    if inputs.operations_readiness.is_none() {
        inputs.operations_readiness = Some(read_json_file(
            &bundle_dir.join("operations-readiness.json"),
        )?);
    }
    Ok(())
}

fn read_json_arg(args: &mut impl Iterator<Item = String>) -> Result<serde_json::Value> {
    let path = args
        .next()
        .ok_or_else(|| SkeinError::Semantic(nowledge_previous_wrapper_preflight_check_usage()))?;
    read_json_file(Path::new(&path))
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let raw = std::fs::read_to_string(path).map_err(|_| {
        SkeinError::Execution(
            "failed to read previous-wrapper preflight JSON: io_error".to_string(),
        )
    })?;
    serde_json::from_str(&raw).map_err(|_| {
        SkeinError::Execution(
            "failed to parse previous-wrapper preflight JSON: invalid_json".to_string(),
        )
    })
}

fn cli_inputs_to_typed(
    inputs: CliPreviousWrapperPreflightCheckInputs,
) -> Result<NowledgePreviousWrapperPreflightInputs> {
    let wrapper_identity = inputs
        .wrapper_identity
        .ok_or_else(|| SkeinError::Semantic(nowledge_previous_wrapper_preflight_check_usage()))?;
    let contract_evidence = inputs
        .contract_evidence
        .ok_or_else(|| SkeinError::Semantic(nowledge_previous_wrapper_preflight_check_usage()))?;
    let adapter_smoke = inputs
        .adapter_smoke
        .ok_or_else(|| SkeinError::Semantic(nowledge_previous_wrapper_preflight_check_usage()))?;
    let migration_gate = inputs
        .migration_gate
        .ok_or_else(|| SkeinError::Semantic(nowledge_previous_wrapper_preflight_check_usage()))?;
    let replacement_summary = inputs
        .replacement_summary
        .ok_or_else(|| SkeinError::Semantic(nowledge_previous_wrapper_preflight_check_usage()))?;
    let query_runtime_preflight = inputs
        .query_runtime_preflight
        .ok_or_else(|| SkeinError::Semantic(nowledge_previous_wrapper_preflight_check_usage()))?;
    let library_readiness = inputs
        .library_readiness
        .ok_or_else(|| SkeinError::Semantic(nowledge_previous_wrapper_preflight_check_usage()))?;
    let cutover_controls = inputs
        .cutover_controls
        .ok_or_else(|| SkeinError::Semantic(nowledge_previous_wrapper_preflight_check_usage()))?;
    let operations_readiness = inputs
        .operations_readiness
        .ok_or_else(|| SkeinError::Semantic(nowledge_previous_wrapper_preflight_check_usage()))?;

    NowledgePreviousWrapperPreflightInputs::new(
        wrapper_identity,
        NowledgePreviousWrapperPreflightArtifacts {
            contract_evidence,
            adapter_smoke,
            migration_gate,
            replacement_summary,
            query_runtime_preflight,
            library_readiness,
            cutover_controls,
            operations_readiness,
        },
    )
}

pub fn nowledge_previous_wrapper_preflight_check_json(
    inputs: impl IntoNowledgePreviousWrapperPreflightInputs,
) -> Result<serde_json::Value> {
    Ok(nowledge_previous_wrapper_preflight_check(
        inputs.into_nowledge_previous_wrapper_preflight_inputs()?,
    )?
    .json())
}

pub fn nowledge_previous_wrapper_preflight_check(
    inputs: NowledgePreviousWrapperPreflightInputs,
) -> Result<NowledgePreviousWrapperPreflightReport> {
    let wrapper_identity = inputs.wrapper_identity;
    let contract_evidence = inputs.contract_evidence;
    let adapter_smoke = inputs.adapter_smoke;
    let migration_gate = inputs.migration_gate;
    let replacement_summary = inputs.replacement_summary;
    let query_runtime_preflight = inputs.query_runtime_preflight;
    let library_readiness = inputs.library_readiness;
    let cutover_controls = inputs.cutover_controls;
    let operations_readiness = inputs.operations_readiness;

    let checks = vec![
        preflight_check(
            "full_contract",
            [
                bool_path(&contract_evidence, &["required_contract_ready"]) == Some(true),
                bool_path(&contract_evidence, &["full_contract_checked"]) == Some(true),
                bool_path(&contract_evidence, &["full_contract_ready"]) == Some(true),
                full_contract_check_count_ready(&contract_evidence),
                selected_checks_match_check_count(&contract_evidence),
                bool_path(
                    &contract_evidence,
                    &["previous_wrapper_contract_evidence", "ready"],
                ) == Some(true),
                str_path(
                    &contract_evidence,
                    &["previous_wrapper_contract_evidence", "wrapper_identity"],
                ) == Some(wrapper_identity.as_str()),
            ],
            [
                "required_contract_ready",
                "full_contract_checked",
                "full_contract_ready",
                "check_count",
                "selected_checks",
                "previous_wrapper_contract_evidence.ready",
                "previous_wrapper_contract_evidence.wrapper_identity",
            ],
            blocker_codes(
                &contract_evidence,
                &[
                    &["required_contract_blocker_codes"][..],
                    &["previous_wrapper_contract_evidence", "blocker_codes"][..],
                ],
            ),
        ),
        preflight_check(
            "adapter_smoke",
            [
                bool_path(&adapter_smoke, &["adapter_smoke_ready"]) == Some(true),
                str_path(&adapter_smoke, &["engine_kind"]) == Some("previous_wrapper"),
                str_path(&adapter_smoke, &["wrapper_identity"]) == Some(wrapper_identity.as_str()),
                u64_path(&adapter_smoke, &["primary_only_checks"]) == Some(0),
                bool_path(&adapter_smoke, &["dual_engine_evidence", "ready"]) == Some(true),
                dual_engine_primary_count_ready(&adapter_smoke, &["dual_engine_evidence"]),
                dual_engine_primary_shadow_counts_match(&adapter_smoke, &["dual_engine_evidence"]),
                dual_engine_matched_shadow_counts_match(&adapter_smoke, &["dual_engine_evidence"]),
                dual_engine_primary_only_count_clear(&adapter_smoke, &["dual_engine_evidence"]),
            ],
            [
                "adapter_smoke_ready",
                "engine_kind",
                "wrapper_identity",
                "primary_only_checks",
                "dual_engine_evidence.ready",
                "dual_engine_evidence.primary_check_count",
                "dual_engine_evidence.shadow_check_count",
                "dual_engine_evidence.matched_check_count",
                "dual_engine_evidence.primary_only_check_count",
            ],
            blocker_codes(&adapter_smoke, &[&["blocker_codes"][..]]),
        ),
        preflight_check(
            "migration_gate",
            [
                str_path(&migration_gate, &["migration_gate", "decision"]) == Some("ready"),
                str_path(&migration_gate, &["cutover", "decision"]) == Some("ready"),
                bool_path(&migration_gate, &["cutover_evidence", "eligible"]) == Some(true),
                str_path(&migration_gate, &["cutover_evidence", "ready_engine_kind"])
                    == Some("previous_wrapper"),
                str_path(
                    &migration_gate,
                    &["cutover_evidence", "ready_wrapper_identity"],
                ) == Some(wrapper_identity.as_str()),
                bool_path(
                    &migration_gate,
                    &["previous_wrapper_contract_evidence", "ready"],
                ) == Some(true),
                u64_path(&migration_gate, &["replacement_readiness_per_million"])
                    == Some(1_000_000),
            ],
            [
                "migration_gate.decision",
                "cutover.decision",
                "cutover_evidence.eligible",
                "cutover_evidence.ready_engine_kind",
                "cutover_evidence.ready_wrapper_identity",
                "previous_wrapper_contract_evidence.ready",
                "replacement_readiness_per_million",
            ],
            blocker_codes(
                &migration_gate,
                &[
                    &["migration_gate", "blockers"][..],
                    &["cutover", "blockers"][..],
                    &["cutover_evidence", "blockers"][..],
                ],
            ),
        ),
        preflight_check(
            "storage_recovery",
            [
                bool_path(
                    &migration_gate,
                    &["cutover_evidence", "storage_recovery_required"],
                ) == Some(true),
                bool_path(
                    &migration_gate,
                    &["cutover_evidence", "storage_recovery_present"],
                ) == Some(true),
                bool_path(
                    &migration_gate,
                    &["cutover_evidence", "storage_recovery_ready"],
                ) == Some(true),
                bool_path(
                    &migration_gate,
                    &["cutover_evidence", "storage_recovery_protocol_matches"],
                ) == Some(true),
                bool_path(
                    &migration_gate,
                    &["cutover_evidence", "storage_recovery_durable"],
                ) == Some(true),
                bool_path(
                    &migration_gate,
                    &[
                        "cutover_evidence",
                        "storage_recovery_checkpoint_boundary_present",
                    ],
                ) == Some(true),
                bool_path(
                    &migration_gate,
                    &["cutover_evidence", "storage_recovery_wal_replay_bounded"],
                ) == Some(true),
                bool_path(
                    &migration_gate,
                    &[
                        "cutover_evidence",
                        "storage_recovery_replay_boundary_consistent",
                    ],
                ) == Some(true),
                bool_path(
                    &migration_gate,
                    &["cutover_evidence", "storage_recovery_torn_tail_clean"],
                ) == Some(true),
            ],
            [
                "cutover_evidence.storage_recovery_required",
                "cutover_evidence.storage_recovery_present",
                "cutover_evidence.storage_recovery_ready",
                "cutover_evidence.storage_recovery_protocol_matches",
                "cutover_evidence.storage_recovery_durable",
                "cutover_evidence.storage_recovery_checkpoint_boundary_present",
                "cutover_evidence.storage_recovery_wal_replay_bounded",
                "cutover_evidence.storage_recovery_replay_boundary_consistent",
                "cutover_evidence.storage_recovery_torn_tail_clean",
            ],
            blocker_codes(
                &migration_gate,
                &[
                    &["cutover_evidence", "storage_recovery_blocker_codes"][..],
                    &["cutover_evidence", "storage_recovery_blockers"][..],
                ],
            ),
        ),
        preflight_check(
            "background_maintenance",
            [
                bool_path(
                    &migration_gate,
                    &["cutover_evidence", "background_maintenance_required"],
                ) == Some(true),
                bool_path(
                    &migration_gate,
                    &["cutover_evidence", "background_maintenance_present"],
                ) == Some(true),
                bool_path(
                    &migration_gate,
                    &["cutover_evidence", "background_maintenance_ready"],
                ) == Some(true),
                bool_path(
                    &migration_gate,
                    &[
                        "cutover_evidence",
                        "background_maintenance_protocol_matches",
                    ],
                ) == Some(true),
                replacement_summary_background_maintenance_graph_delta_ready(&replacement_summary),
                bool_path(
                    &replacement_summary,
                    &[
                        "cutover_evidence",
                        "background_maintenance_foreground_admission_probe_ready",
                    ],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &[
                        "cutover_evidence",
                        "background_maintenance_memory_pressure_ready",
                    ],
                ) == Some(true),
                u64_path(
                    &replacement_summary,
                    &[
                        "cutover_evidence",
                        "background_maintenance_memory_budget_bytes",
                    ],
                )
                .is_some_and(|value| value > 0),
                u64_path(
                    &replacement_summary,
                    &[
                        "cutover_evidence",
                        "background_maintenance_estimated_memory_bytes",
                    ],
                )
                .is_some(),
            ],
            [
                "cutover_evidence.background_maintenance_required",
                "cutover_evidence.background_maintenance_present",
                "cutover_evidence.background_maintenance_ready",
                "cutover_evidence.background_maintenance_protocol_matches",
                "replacement_summary.cutover_evidence.background_maintenance_graph_delta_qos",
                "replacement_summary.cutover_evidence.background_maintenance_foreground_admission_probe_ready",
                "replacement_summary.cutover_evidence.background_maintenance_memory_pressure_ready",
                "replacement_summary.cutover_evidence.background_maintenance_memory_budget_bytes",
                "replacement_summary.cutover_evidence.background_maintenance_estimated_memory_bytes",
            ],
            blocker_codes(
                &migration_gate,
                &[
                    &["cutover_evidence", "background_maintenance_blocker_codes"][..],
                    &["cutover_evidence", "background_maintenance_blockers"][..],
                ],
            )
            .into_iter()
            .chain(blocker_codes(
                &replacement_summary,
                &[
                    &["cutover_evidence", "background_maintenance_blocker_codes"][..],
                    &["cutover_evidence", "background_maintenance_blockers"][..],
                ],
            ))
            .collect::<Vec<_>>(),
        ),
        preflight_check(
            "replacement_summary",
            [
                bool_path(&replacement_summary, &["production_cutover_ready"]) == Some(true),
                u64_path(
                    &replacement_summary,
                    &["production_replacement_per_million"],
                ) == Some(1_000_000),
                empty_array_path(&replacement_summary, &["blocking_categories"]),
                empty_array_path(&replacement_summary, &["missing_evidence"]),
                empty_array_path(&replacement_summary, &["next_actions"]),
                bool_path(&replacement_summary, &["shadow_evidence", "ready"]) == Some(true),
                str_path(
                    &replacement_summary,
                    &["shadow_evidence", "ready_wrapper_identity"],
                ) == Some(wrapper_identity.as_str()),
                str_path(
                    &replacement_summary,
                    &["shadow_evidence", "contract_wrapper_identity"],
                ) == Some(wrapper_identity.as_str()),
                str_path(
                    &replacement_summary,
                    &["shadow_evidence", "cutover_ready_wrapper_identity"],
                ) == Some(wrapper_identity.as_str()),
                bool_path(&replacement_summary, &["dual_engine_evidence", "present"]) == Some(true),
                bool_path(&replacement_summary, &["dual_engine_evidence", "ready"]) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["dual_engine_evidence", "consistent"],
                ) == Some(true),
                dual_engine_primary_count_ready(&replacement_summary, &["dual_engine_evidence"]),
                dual_engine_primary_shadow_counts_match(
                    &replacement_summary,
                    &["dual_engine_evidence"],
                ),
                dual_engine_matched_shadow_counts_match(
                    &replacement_summary,
                    &["dual_engine_evidence"],
                ),
                dual_engine_primary_only_count_clear(
                    &replacement_summary,
                    &["dual_engine_evidence"],
                ),
                bool_path(
                    &replacement_summary,
                    &["search_projection_evidence", "present"],
                ) == Some(true),
                str_path(
                    &replacement_summary,
                    &["search_projection_evidence", "protocol"],
                ) == Some(SKEIN_NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL),
                bool_path(
                    &replacement_summary,
                    &["search_projection_evidence", "ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_evidence", "derived_projection"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_evidence", "all_tables_covered"],
                ) == Some(true),
                search_projection_table_counts_match(&replacement_summary),
                bool_path(
                    &replacement_summary,
                    &["search_projection_evidence", "fts_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_evidence", "vector_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_evidence", "document_identity_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_evidence", "embedding_identity_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_evidence", "fail_soft_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_evidence", "rebuild_marker_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_evidence", "metadata_repair_marker_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_evidence", "incremental_update_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_evidence", "source_chunk_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_evidence", "predicate_pushdown_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &[
                        "search_projection_evidence",
                        "production_filter_pruning_ready",
                    ],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_shadow_evidence", "present"],
                ) == Some(true),
                str_path(
                    &replacement_summary,
                    &["search_projection_shadow_evidence", "protocol"],
                ) == Some(SKEIN_NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL),
                str_path(
                    &replacement_summary,
                    &["search_projection_shadow_evidence", "evidence_source"],
                ) == Some(SKEIN_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE),
                bool_path(
                    &replacement_summary,
                    &["search_projection_shadow_evidence", "ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_shadow_evidence", "primary_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_shadow_evidence", "shadow_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_shadow_evidence", "document_count_parity"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &[
                        "search_projection_shadow_evidence",
                        "document_identity_parity",
                    ],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_shadow_evidence", "table_parity_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &[
                        "search_projection_shadow_evidence",
                        "embedding_identity_parity",
                    ],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_shadow_evidence", "lifecycle_parity"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &[
                        "search_projection_shadow_evidence",
                        "incremental_watermark_parity",
                    ],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &[
                        "search_projection_shadow_evidence",
                        "pushdown_evidence",
                        "ready",
                    ],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &[
                        "search_projection_shadow_evidence",
                        "pushdown_evidence",
                        "predicate_pushdown_parity",
                    ],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &[
                        "search_projection_shadow_evidence",
                        "pushdown_evidence",
                        "shadow_persisted_segment_descriptor_ready",
                    ],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &[
                        "search_projection_shadow_evidence",
                        "pushdown_evidence",
                        "shadow_segment_descriptor_scan_filter_fields_ready",
                    ],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &[
                        "search_projection_shadow_evidence",
                        "pushdown_evidence",
                        "shadow_segment_document_pruning_ready",
                    ],
                ) == Some(true),
                search_projection_shadow_segment_pruning_candidate_count_ready(
                    &replacement_summary,
                ),
                search_projection_shadow_segment_pruning_count_is_positive(
                    &replacement_summary,
                    "shadow_segment_pruned_document_count",
                ),
                search_projection_shadow_segment_pruning_count_is_positive(
                    &replacement_summary,
                    "shadow_segment_scanned_document_count",
                ),
                search_projection_shadow_segment_descriptor_summaries_ready(&replacement_summary),
                str_path(
                    &replacement_summary,
                    &["search_candidate_shadow_evidence", "protocol"],
                ) == Some(NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL),
                str_path(
                    &replacement_summary,
                    &["search_candidate_shadow_evidence", "evidence_source"],
                ) == Some(NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE),
                str_path(
                    &replacement_summary,
                    &["search_candidate_shadow_evidence", "route"],
                ) == Some(NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE),
                bool_path(
                    &replacement_summary,
                    &["search_candidate_shadow_evidence", "present"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_candidate_shadow_evidence", "ready"],
                ) == Some(true),
                str_path(
                    &replacement_summary,
                    &[
                        "search_candidate_shadow_evidence",
                        "candidate_primary_engine",
                    ],
                ) == Some(NOWLEDGE_MEM_SEARCH_CANDIDATE_PRIMARY_ENGINE),
                search_candidate_shadow_counts_ready(&replacement_summary),
                bool_path(
                    &replacement_summary,
                    &["search_candidate_shadow_evidence", "text_retriever_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_candidate_shadow_evidence", "vector_retriever_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &[
                        "search_candidate_shadow_evidence",
                        "fts_top_k_overlap_ready",
                    ],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &[
                        "search_candidate_shadow_evidence",
                        "vector_top_k_overlap_ready",
                    ],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &[
                        "search_candidate_shadow_evidence",
                        "source_chunk_identity_ready",
                    ],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_candidate_shadow_evidence", "fail_soft_observed"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &[
                        "search_candidate_shadow_evidence",
                        "projection_marker_status_visible",
                    ],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &[
                        "search_candidate_shadow_evidence",
                        "projection_watermark_ready",
                    ],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &[
                        "search_candidate_shadow_evidence",
                        "embedding_identity_ready",
                    ],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &[
                        "search_candidate_shadow_evidence",
                        "candidate_identity_ready",
                    ],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &[
                        "search_candidate_shadow_evidence",
                        "candidate_identity_parity",
                    ],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_candidate_shadow_evidence", "filter_pushdown_ready"],
                ) == Some(true),
                u64_path(
                    &replacement_summary,
                    &[
                        "search_candidate_shadow_evidence",
                        "filter_pushdown_field_summary_count",
                    ],
                )
                .is_some_and(|count| count > 0),
                search_candidate_missing_required_fields_ready(&replacement_summary),
                bool_path(
                    &replacement_summary,
                    &[
                        "search_candidate_shadow_evidence",
                        "filter_pushdown_field_capabilities_ready",
                    ],
                ) == Some(true),
                search_candidate_missing_capability_fields_ready(
                    &replacement_summary,
                    "filter_pushdown_missing_value_summary_fields",
                ),
                search_candidate_missing_capability_fields_ready(
                    &replacement_summary,
                    "filter_pushdown_missing_numeric_range_fields",
                ),
                search_candidate_missing_capability_fields_ready(
                    &replacement_summary,
                    "filter_pushdown_missing_timestamp_range_fields",
                ),
                str_path(
                    &replacement_summary,
                    &["workload_fixture_evidence", "protocol"],
                ) == Some(NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL),
                bool_path(
                    &replacement_summary,
                    &["workload_fixture_evidence", "present"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["workload_fixture_evidence", "ready"],
                ) == Some(true),
                u64_path(
                    &replacement_summary,
                    &["workload_fixture_evidence", "route_count"],
                )
                .is_some_and(|count| count > 0),
                u64_path(
                    &replacement_summary,
                    &["workload_fixture_evidence", "query_count"],
                )
                .is_some_and(|count| count > 0),
                u64_path(
                    &replacement_summary,
                    &["workload_fixture_evidence", "failed_query_count"],
                ) == Some(0),
                u64_path(
                    &replacement_summary,
                    &["workload_fixture_evidence", "bounded_expansion_probe_count"],
                )
                .is_some_and(|count| count > 0),
                u64_path(
                    &replacement_summary,
                    &[
                        "workload_fixture_evidence",
                        "failed_bounded_expansion_probe_count",
                    ],
                ) == Some(0),
                u64_path(
                    &replacement_summary,
                    &["workload_fixture_evidence", "search_metadata_probe_count"],
                )
                .is_some_and(|count| count > 0),
                u64_path(
                    &replacement_summary,
                    &[
                        "workload_fixture_evidence",
                        "failed_search_metadata_probe_count",
                    ],
                ) == Some(0),
                u64_path(
                    &replacement_summary,
                    &["workload_fixture_evidence", "source_projection_probe_count"],
                )
                .is_some_and(|count| count > 0),
                u64_path(
                    &replacement_summary,
                    &[
                        "workload_fixture_evidence",
                        "failed_source_projection_probe_count",
                    ],
                ) == Some(0),
                workload_source_projection_ready(&replacement_summary, &["workload_fixture_evidence"]),
            ],
            [
                "production_cutover_ready",
                "production_replacement_per_million",
                "blocking_categories",
                "missing_evidence",
                "next_actions",
                "shadow_evidence.ready",
                "shadow_evidence.ready_wrapper_identity",
                "shadow_evidence.contract_wrapper_identity",
                "shadow_evidence.cutover_ready_wrapper_identity",
                "dual_engine_evidence.present",
                "dual_engine_evidence.ready",
                "dual_engine_evidence.consistent",
                "dual_engine_evidence.primary_check_count",
                "dual_engine_evidence.shadow_check_count",
                "dual_engine_evidence.matched_check_count",
                "dual_engine_evidence.primary_only_check_count",
                "search_projection_evidence.present",
                "search_projection_evidence.protocol",
                "search_projection_evidence.ready",
                "search_projection_evidence.derived_projection",
                "search_projection_evidence.all_tables_covered",
                "search_projection_evidence.covered_table_count",
                "search_projection_evidence.fts_ready",
                "search_projection_evidence.vector_ready",
                "search_projection_evidence.document_identity_ready",
                "search_projection_evidence.embedding_identity_ready",
                "search_projection_evidence.fail_soft_ready",
                "search_projection_evidence.rebuild_marker_ready",
                "search_projection_evidence.metadata_repair_marker_ready",
                "search_projection_evidence.incremental_update_ready",
                "search_projection_evidence.source_chunk_ready",
                "search_projection_evidence.predicate_pushdown_ready",
                "search_projection_evidence.production_filter_pruning_ready",
                "search_projection_shadow_evidence.present",
                "search_projection_shadow_evidence.protocol",
                "search_projection_shadow_evidence.evidence_source",
                "search_projection_shadow_evidence.ready",
                "search_projection_shadow_evidence.primary_ready",
                "search_projection_shadow_evidence.shadow_ready",
                "search_projection_shadow_evidence.document_count_parity",
                "search_projection_shadow_evidence.document_identity_parity",
                "search_projection_shadow_evidence.table_parity_ready",
                "search_projection_shadow_evidence.embedding_identity_parity",
                "search_projection_shadow_evidence.lifecycle_parity",
                "search_projection_shadow_evidence.incremental_watermark_parity",
                "search_projection_shadow_evidence.pushdown_evidence.ready",
                "search_projection_shadow_evidence.pushdown_evidence.predicate_pushdown_parity",
                "search_projection_shadow_evidence.pushdown_evidence.shadow_persisted_segment_descriptor_ready",
                "search_projection_shadow_evidence.pushdown_evidence.shadow_segment_descriptor_scan_filter_fields_ready",
                "search_projection_shadow_evidence.pushdown_evidence.shadow_segment_document_pruning_ready",
                "search_projection_shadow_evidence.pushdown_evidence.shadow_segment_pruning_candidate_document_count",
                "search_projection_shadow_evidence.pushdown_evidence.shadow_segment_pruned_document_count",
                "search_projection_shadow_evidence.pushdown_evidence.shadow_segment_scanned_document_count",
                "search_projection_shadow_evidence.pushdown_evidence.shadow_segment_descriptor_field_summaries",
                "search_candidate_shadow_evidence.protocol",
                "search_candidate_shadow_evidence.evidence_source",
                "search_candidate_shadow_evidence.route",
                "search_candidate_shadow_evidence.present",
                "search_candidate_shadow_evidence.ready",
                "search_candidate_shadow_evidence.candidate_primary_engine",
                "search_candidate_shadow_evidence.candidate_counts",
                "search_candidate_shadow_evidence.text_retriever_ready",
                "search_candidate_shadow_evidence.vector_retriever_ready",
                "search_candidate_shadow_evidence.fts_top_k_overlap_ready",
                "search_candidate_shadow_evidence.vector_top_k_overlap_ready",
                "search_candidate_shadow_evidence.source_chunk_identity_ready",
                "search_candidate_shadow_evidence.fail_soft_observed",
                "search_candidate_shadow_evidence.projection_marker_status_visible",
                "search_candidate_shadow_evidence.projection_watermark_ready",
                "search_candidate_shadow_evidence.embedding_identity_ready",
                "search_candidate_shadow_evidence.candidate_identity_ready",
                "search_candidate_shadow_evidence.candidate_identity_parity",
                "search_candidate_shadow_evidence.filter_pushdown_ready",
                "search_candidate_shadow_evidence.filter_pushdown_field_summary_count",
                "search_candidate_shadow_evidence.filter_pushdown_missing_required_fields",
                "search_candidate_shadow_evidence.filter_pushdown_field_capabilities_ready",
                "search_candidate_shadow_evidence.filter_pushdown_missing_value_summary_fields",
                "search_candidate_shadow_evidence.filter_pushdown_missing_numeric_range_fields",
                "search_candidate_shadow_evidence.filter_pushdown_missing_timestamp_range_fields",
                "workload_fixture_evidence.protocol",
                "workload_fixture_evidence.present",
                "workload_fixture_evidence.ready",
                "workload_fixture_evidence.route_count",
                "workload_fixture_evidence.query_count",
                "workload_fixture_evidence.failed_query_count",
                "workload_fixture_evidence.bounded_expansion_probe_count",
                "workload_fixture_evidence.failed_bounded_expansion_probe_count",
                "workload_fixture_evidence.search_metadata_probe_count",
                "workload_fixture_evidence.failed_search_metadata_probe_count",
                "workload_fixture_evidence.source_projection_probe_count",
                "workload_fixture_evidence.failed_source_projection_probe_count",
                "workload_fixture_evidence.source_projection_reports",
            ],
            blocker_codes(
                &replacement_summary,
                &[
                    &["blocking_categories"][..],
                    &["missing_evidence"][..],
                    &["next_actions"][..],
                    &["search_projection_evidence", "blocker_codes"][..],
                    &["search_projection_shadow_evidence", "blocker_codes"][..],
                    &["search_candidate_shadow_evidence", "blocker_codes"][..],
                    &["workload_fixture_evidence", "blocker_codes"][..],
                ],
            ),
        ),
        preflight_check(
            "replacement_summary_route_catalog",
            [
                route_catalog_metadata_ready(&replacement_summary, &["bounded_read_evidence"]),
                route_catalog_metadata_ready(&replacement_summary, &["graph_route_readiness"]),
                route_catalog_metadata_ready(&replacement_summary, &["query_runtime_preflight"]),
            ],
            [
                "replacement_summary.bounded_read_evidence.route_catalog",
                "replacement_summary.graph_route_readiness.route_catalog",
                "replacement_summary.query_runtime_preflight.route_catalog",
            ],
            blocker_codes(
                &replacement_summary,
                &[
                    &["bounded_read_evidence", "blocker_codes"][..],
                    &["graph_route_readiness", "blocker_codes"][..],
                    &["query_runtime_preflight", "blocker_codes"][..],
                ],
            ),
        ),
        preflight_check(
            "replacement_summary_graph_route",
            [
                bool_path(&replacement_summary, &["graph_route_readiness", "present"])
                    == Some(true),
                bool_path(&replacement_summary, &["graph_route_readiness", "ready"]) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["graph_route_readiness", "evidence_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["graph_route_readiness", "route_coverage_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["graph_route_readiness", "evidence_route_coverage_matches"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["graph_route_readiness", "route_query_runtime_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["graph_route_readiness", "route_query_plan_evidence_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &[
                        "graph_route_readiness",
                        "route_query_profile_evidence_ready",
                    ],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &[
                        "graph_route_readiness",
                        "route_query_api_behavior_evidence_ready",
                    ],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &[
                        "graph_route_readiness",
                        "route_relationship_property_pruning_evidence_ready",
                    ],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["graph_route_readiness", "route_primary_ready"],
                ) == Some(true),
                u64_path(
                    &replacement_summary,
                    &["graph_route_readiness", "primary_ready_route_count"],
                )
                .is_some_and(|count| {
                    count == REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64
                }),
            ],
            [
                "replacement_summary.graph_route_readiness.present",
                "replacement_summary.graph_route_readiness.ready",
                "replacement_summary.graph_route_readiness.evidence_ready",
                "replacement_summary.graph_route_readiness.route_coverage_ready",
                "replacement_summary.graph_route_readiness.evidence_route_coverage_matches",
                "replacement_summary.graph_route_readiness.route_query_runtime_ready",
                "replacement_summary.graph_route_readiness.route_query_plan_evidence_ready",
                "replacement_summary.graph_route_readiness.route_query_profile_evidence_ready",
                "replacement_summary.graph_route_readiness.route_query_api_behavior_evidence_ready",
                "replacement_summary.graph_route_readiness.route_relationship_property_pruning_evidence_ready",
                "replacement_summary.graph_route_readiness.route_primary_ready",
                "replacement_summary.graph_route_readiness.primary_ready_route_count",
            ],
            blocker_codes(
                &replacement_summary,
                &[&["graph_route_readiness", "blocker_codes"][..]],
            ),
        ),
        preflight_check(
            "replacement_summary_bounded_read",
            [
                str_path(&replacement_summary, &["bounded_read_evidence", "protocol"])
                    == Some(NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL),
                bool_path(&replacement_summary, &["bounded_read_evidence", "present"])
                    == Some(true),
                bool_path(&replacement_summary, &["bounded_read_evidence", "ready"]) == Some(true),
                str_path(&replacement_summary, &["bounded_read_evidence", "mode"])
                    == Some("shadow_read_only"),
                bounded_read_execution_row_cap_ready(&replacement_summary),
                u64_path(
                    &replacement_summary,
                    &["bounded_read_evidence", "estimated_payload_bytes"],
                )
                .is_some(),
                u64_path(
                    &replacement_summary,
                    &["bounded_read_evidence", "max_estimated_payload_bytes"],
                )
                .is_some_and(|value| value > 0),
                bool_path(
                    &replacement_summary,
                    &["bounded_read_evidence", "payload_budget_exceeded"],
                ) == Some(false),
                bool_path(
                    &replacement_summary,
                    &["bounded_read_evidence", "row_limit_enforced_before_output"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["bounded_read_evidence", "operator_row_cap_enabled"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["bounded_read_evidence", "row_budget_exceeded"],
                ) == Some(false),
                bool_path(
                    &replacement_summary,
                    &["bounded_read_evidence", "streaming"],
                ) == Some(false),
                u64_path(
                    &replacement_summary,
                    &["bounded_read_evidence", "blocking_operator_count"],
                ) == Some(0),
                empty_array_path(
                    &replacement_summary,
                    &["bounded_read_evidence", "missing_covered_routes"],
                ),
                bool_path(
                    &replacement_summary,
                    &["bounded_read_evidence", "route_primary_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["bounded_read_evidence", "route_query_plan_evidence_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &[
                        "bounded_read_evidence",
                        "route_query_profile_evidence_ready",
                    ],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &[
                        "bounded_read_evidence",
                        "route_query_api_behavior_evidence_ready",
                    ],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &[
                        "bounded_read_evidence",
                        "route_relationship_property_pruning_evidence_ready",
                    ],
                ) == Some(true),
                bounded_read_relationship_property_pruning_ready(&replacement_summary),
            ],
            [
                "replacement_summary.bounded_read_evidence.protocol",
                "replacement_summary.bounded_read_evidence.present",
                "replacement_summary.bounded_read_evidence.ready",
                "replacement_summary.bounded_read_evidence.mode",
                "replacement_summary.bounded_read_evidence.execution_row_cap",
                "replacement_summary.bounded_read_evidence.estimated_payload_bytes",
                "replacement_summary.bounded_read_evidence.max_estimated_payload_bytes",
                "replacement_summary.bounded_read_evidence.payload_budget_exceeded",
                "replacement_summary.bounded_read_evidence.row_limit_enforced_before_output",
                "replacement_summary.bounded_read_evidence.operator_row_cap_enabled",
                "replacement_summary.bounded_read_evidence.row_budget_exceeded",
                "replacement_summary.bounded_read_evidence.streaming",
                "replacement_summary.bounded_read_evidence.blocking_operator_count",
                "replacement_summary.bounded_read_evidence.missing_covered_routes",
                "replacement_summary.bounded_read_evidence.route_primary_ready",
                "replacement_summary.bounded_read_evidence.route_query_plan_evidence_ready",
                "replacement_summary.bounded_read_evidence.route_query_profile_evidence_ready",
                "replacement_summary.bounded_read_evidence.route_query_api_behavior_evidence_ready",
                "replacement_summary.bounded_read_evidence.route_relationship_property_pruning_evidence_ready",
                "replacement_summary.bounded_read_evidence.relationship_property_pruning",
            ],
            blocker_codes(
                &replacement_summary,
                &[&["bounded_read_evidence", "blocker_codes"][..]],
            ),
        ),
        preflight_check(
            "query_runtime_preflight",
            [
                str_path(&query_runtime_preflight, &["protocol"])
                    == Some(NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL),
                bool_path(&query_runtime_preflight, &["ready"]) == Some(true),
                bool_path(&query_runtime_preflight, &["database_opened"]) == Some(true),
                bool_path(&query_runtime_preflight, &["redaction", "ready"]) == Some(true),
                bool_path(&query_runtime_preflight, &["redaction", "rows_copied"]) == Some(false),
                bool_path(
                    &query_runtime_preflight,
                    &["redaction", "parameters_copied"],
                ) == Some(false),
                bool_path(
                    &query_runtime_preflight,
                    &["redaction", "local_paths_copied"],
                ) == Some(false),
                bool_path(
                    &query_runtime_preflight,
                    &["redaction", "raw_errors_copied"],
                ) == Some(false),
                u64_path(&query_runtime_preflight, &["probe_count"]).is_some_and(|value| value > 0),
                query_runtime_preflight_counts_match(&query_runtime_preflight),
                u64_path(&query_runtime_preflight, &["failed_probe_count"]) == Some(0),
                query_runtime_preflight_route_coverage_ready(&query_runtime_preflight),
                str_path(&query_runtime_preflight, &["route_catalog_version"])
                    == Some(NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION),
                str_path(&query_runtime_preflight, &["route_catalog_digest"])
                    == Some(nowledge_mem_graph_read_route_catalog_digest().as_str()),
                query_runtime_preflight_probe_details_ready(&query_runtime_preflight),
            ],
            [
                "query_runtime_preflight.protocol",
                "query_runtime_preflight.ready",
                "query_runtime_preflight.database_opened",
                "query_runtime_preflight.redaction.ready",
                "query_runtime_preflight.redaction.rows_copied",
                "query_runtime_preflight.redaction.parameters_copied",
                "query_runtime_preflight.redaction.local_paths_copied",
                "query_runtime_preflight.redaction.raw_errors_copied",
                "query_runtime_preflight.probe_count",
                "query_runtime_preflight.passed_probe_count",
                "query_runtime_preflight.failed_probe_count",
                "query_runtime_preflight.route_coverage",
                "query_runtime_preflight.route_catalog_version",
                "query_runtime_preflight.route_catalog_digest",
                "query_runtime_preflight.probes",
            ],
            blocker_codes(
                &query_runtime_preflight,
                &[
                    &["blocker_codes"][..],
                    &["failed_checks"][..],
                    &["route_coverage_blocker_codes"][..],
                ],
            ),
        ),
        preflight_check(
            "library_readiness",
            [
                str_path(&library_readiness, &["protocol"])
                    == Some(NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL),
                bool_path(&library_readiness, &["present"]) == Some(true),
                bool_path(&library_readiness, &["ready"]) == Some(true),
                u64_path(&library_readiness, &["ready_area_count"]).is_some_and(|value| value > 0),
                u64_path(&library_readiness, &["blocked_area_count"]) == Some(0),
                bool_path(&library_readiness, &["redaction", "ready"]) == Some(true),
                bool_path(&library_readiness, &["redaction", "query_text_copied"]) == Some(false),
                bool_path(&library_readiness, &["redaction", "parameters_copied"]) == Some(false),
                bool_path(&library_readiness, &["redaction", "local_paths_copied"]) == Some(false),
                bool_path(&library_readiness, &["open_report", "graph_opened"]) == Some(true),
                bool_path(
                    &library_readiness,
                    &["open_report", "search_projection_opened"],
                ) == Some(true),
                library_readiness_area_ready(&library_readiness, "graph"),
                library_readiness_area_ready(&library_readiness, "query"),
                library_readiness_area_ready(&library_readiness, "storage"),
                library_readiness_area_ready(&library_readiness, "background"),
                library_readiness_area_ready(&library_readiness, "query_family"),
                library_readiness_area_ready(&library_readiness, "graph_route"),
                library_readiness_area_ready(&library_readiness, "search_route_ownership"),
                library_readiness_area_ready(&library_readiness, "search_projection"),
                library_readiness_area_ready(&library_readiness, "search_projection_shadow"),
                library_readiness_area_ready(&library_readiness, "search_candidate_shadow"),
                library_readiness_area_ready(&library_readiness, "workload_fixture"),
                library_readiness_graph_rag_workload_ready(&library_readiness),
                workload_source_projection_ready(&library_readiness, &["workload_fixture_evidence"]),
            ],
            [
                "library_readiness.protocol",
                "library_readiness.present",
                "library_readiness.ready",
                "library_readiness.ready_area_count",
                "library_readiness.blocked_area_count",
                "library_readiness.redaction.ready",
                "library_readiness.redaction.query_text_copied",
                "library_readiness.redaction.parameters_copied",
                "library_readiness.redaction.local_paths_copied",
                "library_readiness.open_report.graph_opened",
                "library_readiness.open_report.search_projection_opened",
                "library_readiness.readiness_by_area.graph.ready",
                "library_readiness.readiness_by_area.query.ready",
                "library_readiness.readiness_by_area.storage.ready",
                "library_readiness.readiness_by_area.background.ready",
                "library_readiness.readiness_by_area.query_family.ready",
                "library_readiness.readiness_by_area.graph_route.ready",
                "library_readiness.readiness_by_area.search_route_ownership.ready",
                "library_readiness.readiness_by_area.search_projection.ready",
                "library_readiness.readiness_by_area.search_projection_shadow.ready",
                "library_readiness.readiness_by_area.search_candidate_shadow.ready",
                "library_readiness.readiness_by_area.workload_fixture.ready",
                "library_readiness.workload_fixture_evidence.graph_rag_reports",
                "library_readiness.workload_fixture_evidence.source_projection_reports",
            ],
            blocker_codes(
                &library_readiness,
                &[
                    &["blocker_codes"][..],
                    &["readiness_by_area", "graph", "blocker_codes"][..],
                    &["readiness_by_area", "query", "blocker_codes"][..],
                    &["readiness_by_area", "storage", "blocker_codes"][..],
                    &["readiness_by_area", "background", "blocker_codes"][..],
                    &["readiness_by_area", "query_family", "blocker_codes"][..],
                    &["readiness_by_area", "graph_route", "blocker_codes"][..],
                    &["readiness_by_area", "search_route_ownership", "blocker_codes"][..],
                    &["readiness_by_area", "search_projection", "blocker_codes"][..],
                    &[
                        "readiness_by_area",
                        "search_projection_shadow",
                        "blocker_codes",
                    ][..],
                    &[
                        "readiness_by_area",
                        "search_candidate_shadow",
                        "blocker_codes",
                    ][..],
                    &["readiness_by_area", "workload_fixture", "blocker_codes"][..],
                    &["workload_fixture_evidence", "blocker_codes"][..],
                ],
            ),
        ),
        preflight_check(
            "cutover_controls",
            [
                str_path(&cutover_controls, &["protocol"])
                    == Some(NOWLEDGE_MEM_CUTOVER_CONTROLS_PROTOCOL),
                bool_path(&cutover_controls, &["ready"]) == Some(true),
                str_path(&cutover_controls, &["controls", "graph_reads"]) == Some("skein"),
                str_path(&cutover_controls, &["controls", "search_reads"]) == Some("skein"),
                str_path(&cutover_controls, &["controls", "dual_writes"]) == Some("enabled"),
                str_path(&cutover_controls, &["controls", "projection_catch_up"])
                    == Some("enabled"),
                bool_path(&cutover_controls, &["graph", "read_selected_skein"]) == Some(true),
                bool_path(&cutover_controls, &["graph", "read_effective"]) == Some(true),
                bool_path(&cutover_controls, &["search", "read_selected_skein"]) == Some(true),
                bool_path(&cutover_controls, &["search", "read_effective"]) == Some(true),
                bool_path(&cutover_controls, &["work", "dual_writes_enabled"]) == Some(true),
                bool_path(&cutover_controls, &["work", "projection_catch_up_enabled"])
                    == Some(true),
                bool_path(
                    &cutover_controls,
                    &["work", "initial_import_inactive_for_cutover"],
                ) == Some(true)
                    || bool_path(
                        &cutover_controls,
                        &["work", "initial_import_cutover_catch_up_ready"],
                    ) == Some(true),
                bool_path(
                    &cutover_controls,
                    &["production_status", "graph", "skein_cutover_effective"],
                ) == Some(true),
                bool_path(
                    &cutover_controls,
                    &["production_status", "search", "skein_cutover_effective"],
                ) == Some(true),
                bool_path(&cutover_controls, &["redaction", "query_text_copied"]) == Some(false),
                bool_path(&cutover_controls, &["redaction", "parameters_copied"]) == Some(false),
                bool_path(&cutover_controls, &["redaction", "local_paths_copied"]) == Some(false),
            ],
            [
                "cutover_controls.protocol",
                "cutover_controls.ready",
                "cutover_controls.controls.graph_reads",
                "cutover_controls.controls.search_reads",
                "cutover_controls.controls.dual_writes",
                "cutover_controls.controls.projection_catch_up",
                "cutover_controls.graph.read_selected_skein",
                "cutover_controls.graph.read_effective",
                "cutover_controls.search.read_selected_skein",
                "cutover_controls.search.read_effective",
                "cutover_controls.work.dual_writes_enabled",
                "cutover_controls.work.projection_catch_up_enabled",
                "cutover_controls.work.initial_import_safe_for_read_cutover",
                "cutover_controls.production_status.graph.skein_cutover_effective",
                "cutover_controls.production_status.search.skein_cutover_effective",
                "cutover_controls.redaction.query_text_copied",
                "cutover_controls.redaction.parameters_copied",
                "cutover_controls.redaction.local_paths_copied",
            ],
            blocker_codes(&cutover_controls, &[&["blocker_codes"][..]]),
        ),
        preflight_check(
            "operations_readiness",
            [
                str_path(&operations_readiness, &["protocol"])
                    == Some(NOWLEDGE_MEM_OPERATIONS_READINESS_PROTOCOL),
                bool_path(&operations_readiness, &["present"]) == Some(true),
                bool_path(&operations_readiness, &["ready"]) == Some(true),
                bool_path(&operations_readiness, &["graph", "open"]) == Some(true),
                bool_path(&operations_readiness, &["graph", "read_only"]) == Some(false),
                bool_path(&operations_readiness, &["search_projection", "open"]) == Some(true),
                bool_path(&operations_readiness, &["search_projection", "stale"]) == Some(false),
                bool_path(&operations_readiness, &["storage_lifecycle", "ready"]) == Some(true),
                str_path(&operations_readiness, &["storage_lifecycle", "action"])
                    == Some("ready"),
                bool_path(
                    &operations_readiness,
                    &["readiness", "storage_lifecycle_ready"],
                ) == Some(true),
                bool_path(
                    &operations_readiness,
                    &["readiness", "storage_recovery_ready"],
                ) == Some(true),
                bool_path(&operations_readiness, &["readiness", "slow_query_ready"]) == Some(true),
                bool_path(
                    &operations_readiness,
                    &["readiness", "background_maintenance_ready"],
                ) == Some(true),
                bool_path(&operations_readiness, &["redaction", "query_text_copied"])
                    == Some(false),
                bool_path(&operations_readiness, &["redaction", "parameters_copied"])
                    == Some(false),
                bool_path(&operations_readiness, &["redaction", "local_paths_copied"])
                    == Some(false),
            ],
            [
                "operations_readiness.protocol",
                "operations_readiness.present",
                "operations_readiness.ready",
                "operations_readiness.graph.open",
                "operations_readiness.graph.read_only",
                "operations_readiness.search_projection.open",
                "operations_readiness.search_projection.stale",
                "operations_readiness.storage_lifecycle.ready",
                "operations_readiness.storage_lifecycle.action",
                "operations_readiness.readiness.storage_lifecycle_ready",
                "operations_readiness.readiness.storage_recovery_ready",
                "operations_readiness.readiness.slow_query_ready",
                "operations_readiness.readiness.background_maintenance_ready",
                "operations_readiness.redaction.query_text_copied",
                "operations_readiness.redaction.parameters_copied",
                "operations_readiness.redaction.local_paths_copied",
            ],
            blocker_codes(&operations_readiness, &[&["blocker_codes"][..]]),
        ),
    ];
    let ready = checks.iter().all(|check| check.ready);
    let failed_checks = checks
        .iter()
        .filter(|check| !check.ready)
        .map(|check| check.name.clone())
        .collect::<Vec<_>>();
    let release_summary = previous_wrapper_preflight_release_summary(PreflightReleaseInputs {
        wrapper_identity: &wrapper_identity,
        contract_evidence: &contract_evidence,
        adapter_smoke: &adapter_smoke,
        migration_gate: &migration_gate,
        replacement_summary: &replacement_summary,
        query_runtime_preflight: &query_runtime_preflight,
        library_readiness: &library_readiness,
        cutover_controls: &cutover_controls,
        operations_readiness: &operations_readiness,
    });

    Ok(NowledgePreviousWrapperPreflightReport {
        protocol: NOWLEDGE_PREVIOUS_WRAPPER_PREFLIGHT_PROTOCOL.to_string(),
        ready,
        wrapper_identity,
        failed_checks,
        release_summary,
        checks,
    })
}

struct PreflightReleaseInputs<'a> {
    wrapper_identity: &'a str,
    contract_evidence: &'a serde_json::Value,
    adapter_smoke: &'a serde_json::Value,
    migration_gate: &'a serde_json::Value,
    replacement_summary: &'a serde_json::Value,
    query_runtime_preflight: &'a serde_json::Value,
    library_readiness: &'a serde_json::Value,
    cutover_controls: &'a serde_json::Value,
    operations_readiness: &'a serde_json::Value,
}

fn previous_wrapper_preflight_release_summary(
    inputs: PreflightReleaseInputs<'_>,
) -> serde_json::Value {
    let mut summary = serde_json::Map::new();
    let contract_evidence = inputs.contract_evidence;
    let adapter_smoke = inputs.adapter_smoke;
    let migration_gate = inputs.migration_gate;
    let replacement_summary = inputs.replacement_summary;
    let query_runtime_preflight = inputs.query_runtime_preflight;
    let library_readiness = inputs.library_readiness;
    let cutover_controls = inputs.cutover_controls;
    let operations_readiness = inputs.operations_readiness;
    insert_json_value(&mut summary, "wrapper_identity", inputs.wrapper_identity);
    insert_json_value(
        &mut summary,
        "required_contract_ready",
        bool_path(contract_evidence, &["required_contract_ready"]),
    );
    insert_json_value(
        &mut summary,
        "full_contract_checked",
        bool_path(contract_evidence, &["full_contract_checked"]),
    );
    insert_json_value(
        &mut summary,
        "full_contract_ready",
        bool_path(contract_evidence, &["full_contract_ready"]),
    );
    insert_json_value(
        &mut summary,
        "selected_checks",
        u64_path(contract_evidence, &["selected_checks"]),
    );
    insert_json_value(
        &mut summary,
        "check_count",
        u64_path(contract_evidence, &["check_count"]),
    );
    insert_json_value(
        &mut summary,
        "adapter_smoke_ready",
        bool_path(adapter_smoke, &["adapter_smoke_ready"]),
    );
    insert_json_value(
        &mut summary,
        "adapter_request_count",
        u64_path(adapter_smoke, &["request_count"]),
    );
    insert_json_value(
        &mut summary,
        "adapter_primary_only_checks",
        u64_path(adapter_smoke, &["primary_only_checks"]),
    );
    insert_json_value(
        &mut summary,
        "adapter_dual_engine_ready",
        bool_path(adapter_smoke, &["dual_engine_evidence", "ready"]),
    );
    insert_json_value(
        &mut summary,
        "adapter_dual_engine_counts_consistent",
        dual_engine_count_evidence_consistent(adapter_smoke, &["dual_engine_evidence"]),
    );
    insert_json_value(
        &mut summary,
        "adapter_dual_engine_primary_check_count",
        u64_path(
            adapter_smoke,
            &["dual_engine_evidence", "primary_check_count"],
        ),
    );
    insert_json_value(
        &mut summary,
        "adapter_dual_engine_shadow_check_count",
        u64_path(
            adapter_smoke,
            &["dual_engine_evidence", "shadow_check_count"],
        ),
    );
    insert_json_value(
        &mut summary,
        "adapter_dual_engine_matched_check_count",
        u64_path(
            adapter_smoke,
            &["dual_engine_evidence", "matched_check_count"],
        ),
    );
    insert_json_value(
        &mut summary,
        "adapter_dual_engine_primary_only_check_count",
        u64_path(
            adapter_smoke,
            &["dual_engine_evidence", "primary_only_check_count"],
        ),
    );
    insert_json_value(
        &mut summary,
        "migration_gate_decision",
        str_path(migration_gate, &["migration_gate", "decision"]),
    );
    insert_json_value(
        &mut summary,
        "cutover_decision",
        str_path(migration_gate, &["cutover", "decision"]),
    );
    insert_json_value(
        &mut summary,
        "cutover_eligible",
        bool_path(migration_gate, &["cutover_evidence", "eligible"]),
    );
    insert_json_value(
        &mut summary,
        "ready_engine_kind",
        str_path(migration_gate, &["cutover_evidence", "ready_engine_kind"]),
    );
    insert_json_value(
        &mut summary,
        "ready_wrapper_identity",
        str_path(
            migration_gate,
            &["cutover_evidence", "ready_wrapper_identity"],
        ),
    );
    insert_json_value(
        &mut summary,
        "replacement_readiness_per_million",
        u64_path(migration_gate, &["replacement_readiness_per_million"])
            .or_else(|| u64_path(replacement_summary, &["replacement_readiness_per_million"])),
    );
    insert_json_value(
        &mut summary,
        "production_cutover_ready",
        bool_path(replacement_summary, &["production_cutover_ready"]),
    );
    insert_json_value(
        &mut summary,
        "production_replacement_per_million",
        u64_path(replacement_summary, &["production_replacement_per_million"]),
    );
    insert_json_value(
        &mut summary,
        "shadow_evidence_ready",
        bool_path(replacement_summary, &["shadow_evidence", "ready"]),
    );
    insert_json_value(
        &mut summary,
        "shadow_evidence_ready_wrapper_identity",
        str_path(
            replacement_summary,
            &["shadow_evidence", "ready_wrapper_identity"],
        ),
    );
    insert_json_value(
        &mut summary,
        "shadow_evidence_contract_wrapper_identity",
        str_path(
            replacement_summary,
            &["shadow_evidence", "contract_wrapper_identity"],
        ),
    );
    insert_json_value(
        &mut summary,
        "shadow_evidence_cutover_ready_wrapper_identity",
        str_path(
            replacement_summary,
            &["shadow_evidence", "cutover_ready_wrapper_identity"],
        ),
    );
    insert_json_value(
        &mut summary,
        "bounded_read_evidence_ready",
        bool_path(replacement_summary, &["bounded_read_evidence", "ready"]),
    );
    insert_json_value(
        &mut summary,
        "bounded_read_evidence_mode",
        str_path(replacement_summary, &["bounded_read_evidence", "mode"]),
    );
    insert_json_value(
        &mut summary,
        "bounded_read_max_rows",
        u64_path(replacement_summary, &["bounded_read_evidence", "max_rows"]),
    );
    insert_json_value(
        &mut summary,
        "bounded_read_execution_row_cap",
        u64_path(
            replacement_summary,
            &["bounded_read_evidence", "execution_row_cap"],
        ),
    );
    insert_json_value(
        &mut summary,
        "bounded_read_estimated_payload_bytes",
        u64_path(
            replacement_summary,
            &["bounded_read_evidence", "estimated_payload_bytes"],
        ),
    );
    insert_json_value(
        &mut summary,
        "bounded_read_max_estimated_payload_bytes",
        u64_path(
            replacement_summary,
            &["bounded_read_evidence", "max_estimated_payload_bytes"],
        ),
    );
    insert_json_value(
        &mut summary,
        "bounded_read_payload_budget_exceeded",
        bool_path(
            replacement_summary,
            &["bounded_read_evidence", "payload_budget_exceeded"],
        ),
    );
    insert_json_value(
        &mut summary,
        "bounded_read_row_limit_enforced_before_output",
        bool_path(
            replacement_summary,
            &["bounded_read_evidence", "row_limit_enforced_before_output"],
        ),
    );
    insert_json_value(
        &mut summary,
        "bounded_read_operator_row_cap_enabled",
        bool_path(
            replacement_summary,
            &["bounded_read_evidence", "operator_row_cap_enabled"],
        ),
    );
    insert_json_value(
        &mut summary,
        "bounded_read_streaming",
        bool_path(replacement_summary, &["bounded_read_evidence", "streaming"]),
    );
    insert_json_value(
        &mut summary,
        "bounded_read_blocking_operator_count",
        u64_path(
            replacement_summary,
            &["bounded_read_evidence", "blocking_operator_count"],
        ),
    );
    insert_json_value(
        &mut summary,
        "bounded_read_blocking_operator_memory_reports_complete",
        bool_path(
            replacement_summary,
            &[
                "bounded_read_evidence",
                "blocking_operator_memory_reports_complete",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "bounded_read_blocking_operator_memory_within_budget",
        bool_path(
            replacement_summary,
            &[
                "bounded_read_evidence",
                "blocking_operator_memory_within_budget",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "bounded_read_spill_within_budget",
        bool_path(
            replacement_summary,
            &["bounded_read_evidence", "spill_within_budget"],
        ),
    );
    insert_json_value(
        &mut summary,
        "bounded_read_route_catalog_version",
        str_path(
            replacement_summary,
            &["bounded_read_evidence", "route_catalog_version"],
        ),
    );
    insert_json_value(
        &mut summary,
        "bounded_read_route_catalog_digest",
        str_path(
            replacement_summary,
            &["bounded_read_evidence", "route_catalog_digest"],
        ),
    );
    insert_json_value(
        &mut summary,
        "storage_recovery_ready",
        bool_path(
            migration_gate,
            &["cutover_evidence", "storage_recovery_ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "storage_recovery_durable",
        bool_path(
            migration_gate,
            &["cutover_evidence", "storage_recovery_durable"],
        ),
    );
    insert_json_value(
        &mut summary,
        "storage_recovery_checkpoint_boundary_present",
        bool_path(
            migration_gate,
            &[
                "cutover_evidence",
                "storage_recovery_checkpoint_boundary_present",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "storage_recovery_wal_replay_bounded",
        bool_path(
            migration_gate,
            &["cutover_evidence", "storage_recovery_wal_replay_bounded"],
        ),
    );
    insert_json_value(
        &mut summary,
        "storage_recovery_replay_boundary_consistent",
        bool_path(
            migration_gate,
            &[
                "cutover_evidence",
                "storage_recovery_replay_boundary_consistent",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "storage_recovery_torn_tail_clean",
        bool_path(
            migration_gate,
            &["cutover_evidence", "storage_recovery_torn_tail_clean"],
        ),
    );
    insert_json_value(
        &mut summary,
        "background_maintenance_ready",
        bool_path(
            migration_gate,
            &["cutover_evidence", "background_maintenance_ready"],
        ),
    );
    for (field, path) in [
        (
            "background_maintenance_executable_search_projection_graph_delta_count",
            "background_maintenance_executable_search_projection_graph_delta_count",
        ),
        (
            "background_maintenance_admitted_search_projection_graph_delta_count",
            "background_maintenance_admitted_search_projection_graph_delta_count",
        ),
        (
            "background_maintenance_deferred_search_projection_graph_delta_count",
            "background_maintenance_deferred_search_projection_graph_delta_count",
        ),
        (
            "background_maintenance_rejected_search_projection_graph_delta_count",
            "background_maintenance_rejected_search_projection_graph_delta_count",
        ),
        (
            "background_maintenance_executable_search_projection_graph_delta_operations",
            "background_maintenance_executable_search_projection_graph_delta_operations",
        ),
        (
            "background_maintenance_admitted_search_projection_graph_delta_operations",
            "background_maintenance_admitted_search_projection_graph_delta_operations",
        ),
        (
            "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch",
            "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch",
        ),
        (
            "background_maintenance_memory_budget_bytes",
            "background_maintenance_memory_budget_bytes",
        ),
        (
            "background_maintenance_estimated_memory_bytes",
            "background_maintenance_estimated_memory_bytes",
        ),
    ] {
        insert_json_value(
            &mut summary,
            field,
            u64_path(replacement_summary, &["cutover_evidence", path]),
        );
    }
    insert_json_value(
        &mut summary,
        "background_maintenance_foreground_admission_probe_ready",
        bool_path(
            replacement_summary,
            &[
                "cutover_evidence",
                "background_maintenance_foreground_admission_probe_ready",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "background_maintenance_memory_pressure_ready",
        bool_path(
            replacement_summary,
            &[
                "cutover_evidence",
                "background_maintenance_memory_pressure_ready",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "dual_engine_evidence_present",
        bool_path(replacement_summary, &["dual_engine_evidence", "present"]),
    );
    insert_json_value(
        &mut summary,
        "dual_engine_evidence_ready",
        bool_path(replacement_summary, &["dual_engine_evidence", "ready"]),
    );
    insert_json_value(
        &mut summary,
        "dual_engine_evidence_consistent",
        bool_path(replacement_summary, &["dual_engine_evidence", "consistent"]).or(Some(
            dual_engine_count_evidence_consistent(replacement_summary, &["dual_engine_evidence"]),
        )),
    );
    for (field, path) in [
        ("dual_engine_primary_check_count", "primary_check_count"),
        ("dual_engine_shadow_check_count", "shadow_check_count"),
        ("dual_engine_matched_check_count", "matched_check_count"),
        (
            "dual_engine_primary_only_check_count",
            "primary_only_check_count",
        ),
        ("dual_engine_matched_per_million", "matched_per_million"),
    ] {
        insert_json_value(
            &mut summary,
            field,
            u64_path(replacement_summary, &["dual_engine_evidence", path]),
        );
    }
    insert_json_value(
        &mut summary,
        "search_projection_evidence_ready",
        bool_path(
            replacement_summary,
            &["search_projection_evidence", "ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_evidence_protocol",
        str_path(
            replacement_summary,
            &["search_projection_evidence", "protocol"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_all_tables_covered",
        bool_path(
            replacement_summary,
            &["search_projection_evidence", "all_tables_covered"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_covered_table_count",
        u64_path(
            replacement_summary,
            &["search_projection_evidence", "covered_table_count"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_required_table_count",
        u64_path(
            replacement_summary,
            &["search_projection_evidence", "required_table_count"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_fts_ready",
        bool_path(
            replacement_summary,
            &["search_projection_evidence", "fts_ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_vector_ready",
        bool_path(
            replacement_summary,
            &["search_projection_evidence", "vector_ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_document_identity_ready",
        bool_path(
            replacement_summary,
            &["search_projection_evidence", "document_identity_ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_embedding_identity_ready",
        bool_path(
            replacement_summary,
            &["search_projection_evidence", "embedding_identity_ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_fail_soft_ready",
        bool_path(
            replacement_summary,
            &["search_projection_evidence", "fail_soft_ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_incremental_update_ready",
        bool_path(
            replacement_summary,
            &["search_projection_evidence", "incremental_update_ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_predicate_pushdown_ready",
        bool_path(
            replacement_summary,
            &["search_projection_evidence", "predicate_pushdown_ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_production_filter_pruning_ready",
        bool_path(
            replacement_summary,
            &[
                "search_projection_evidence",
                "production_filter_pruning_ready",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_shadow_evidence_ready",
        bool_path(
            replacement_summary,
            &["search_projection_shadow_evidence", "ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_shadow_evidence_protocol",
        str_path(
            replacement_summary,
            &["search_projection_shadow_evidence", "protocol"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_shadow_evidence_source",
        str_path(
            replacement_summary,
            &["search_projection_shadow_evidence", "evidence_source"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_shadow_primary_ready",
        bool_path(
            replacement_summary,
            &["search_projection_shadow_evidence", "primary_ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_shadow_shadow_ready",
        bool_path(
            replacement_summary,
            &["search_projection_shadow_evidence", "shadow_ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_shadow_document_count_parity",
        bool_path(
            replacement_summary,
            &["search_projection_shadow_evidence", "document_count_parity"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_shadow_document_identity_parity",
        bool_path(
            replacement_summary,
            &[
                "search_projection_shadow_evidence",
                "document_identity_parity",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_shadow_table_parity_ready",
        bool_path(
            replacement_summary,
            &["search_projection_shadow_evidence", "table_parity_ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_shadow_embedding_identity_parity",
        bool_path(
            replacement_summary,
            &[
                "search_projection_shadow_evidence",
                "embedding_identity_parity",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_shadow_lifecycle_parity",
        bool_path(
            replacement_summary,
            &["search_projection_shadow_evidence", "lifecycle_parity"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_shadow_incremental_watermark_parity",
        bool_path(
            replacement_summary,
            &[
                "search_projection_shadow_evidence",
                "incremental_watermark_parity",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_shadow_pushdown_ready",
        bool_path(
            replacement_summary,
            &[
                "search_projection_shadow_evidence",
                "pushdown_evidence",
                "ready",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_shadow_predicate_pushdown_parity",
        bool_path(
            replacement_summary,
            &[
                "search_projection_shadow_evidence",
                "pushdown_evidence",
                "predicate_pushdown_parity",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_shadow_persisted_segment_descriptor_ready",
        bool_path(
            replacement_summary,
            &[
                "search_projection_shadow_evidence",
                "pushdown_evidence",
                "shadow_persisted_segment_descriptor_ready",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_shadow_segment_descriptor_scan_filter_fields_ready",
        bool_path(
            replacement_summary,
            &[
                "search_projection_shadow_evidence",
                "pushdown_evidence",
                "shadow_segment_descriptor_scan_filter_fields_ready",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_shadow_segment_document_pruning_ready",
        bool_path(
            replacement_summary,
            &[
                "search_projection_shadow_evidence",
                "pushdown_evidence",
                "shadow_segment_document_pruning_ready",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_shadow_segment_pruning_candidate_document_count",
        u64_path(
            replacement_summary,
            &[
                "search_projection_shadow_evidence",
                "pushdown_evidence",
                "shadow_segment_pruning_candidate_document_count",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_shadow_segment_pruned_document_count",
        u64_path(
            replacement_summary,
            &[
                "search_projection_shadow_evidence",
                "pushdown_evidence",
                "shadow_segment_pruned_document_count",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_shadow_segment_scanned_document_count",
        u64_path(
            replacement_summary,
            &[
                "search_projection_shadow_evidence",
                "pushdown_evidence",
                "shadow_segment_scanned_document_count",
            ],
        ),
    );
    summary.insert(
        "search_projection_shadow_segment_descriptor_field_summaries_ready".to_string(),
        serde_json::json!(search_projection_shadow_segment_descriptor_summaries_ready(
            replacement_summary
        )),
    );
    insert_json_value(
        &mut summary,
        "search_candidate_shadow_evidence_ready",
        bool_path(
            replacement_summary,
            &["search_candidate_shadow_evidence", "ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_candidate_shadow_candidate_counts_ready",
        bool_path(
            replacement_summary,
            &["search_candidate_shadow_evidence", "candidate_counts_ready"],
        )
        .or(Some(search_candidate_shadow_counts_ready(
            replacement_summary,
        ))),
    );
    insert_json_value(
        &mut summary,
        "search_candidate_shadow_text_retriever_ready",
        bool_path(
            replacement_summary,
            &["search_candidate_shadow_evidence", "text_retriever_ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_candidate_shadow_vector_retriever_ready",
        bool_path(
            replacement_summary,
            &["search_candidate_shadow_evidence", "vector_retriever_ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_candidate_shadow_fts_top_k_overlap_ready",
        bool_path(
            replacement_summary,
            &[
                "search_candidate_shadow_evidence",
                "fts_top_k_overlap_ready",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_candidate_shadow_vector_top_k_overlap_ready",
        bool_path(
            replacement_summary,
            &[
                "search_candidate_shadow_evidence",
                "vector_top_k_overlap_ready",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_candidate_shadow_source_chunk_identity_ready",
        bool_path(
            replacement_summary,
            &[
                "search_candidate_shadow_evidence",
                "source_chunk_identity_ready",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_candidate_shadow_fail_soft_observed",
        bool_path(
            replacement_summary,
            &["search_candidate_shadow_evidence", "fail_soft_observed"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_candidate_shadow_projection_marker_status_visible",
        bool_path(
            replacement_summary,
            &[
                "search_candidate_shadow_evidence",
                "projection_marker_status_visible",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_candidate_shadow_projection_watermark_ready",
        bool_path(
            replacement_summary,
            &[
                "search_candidate_shadow_evidence",
                "projection_watermark_ready",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_candidate_shadow_embedding_identity_ready",
        bool_path(
            replacement_summary,
            &[
                "search_candidate_shadow_evidence",
                "embedding_identity_ready",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_candidate_shadow_candidate_identity_ready",
        bool_path(
            replacement_summary,
            &[
                "search_candidate_shadow_evidence",
                "candidate_identity_ready",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_candidate_shadow_filter_pushdown_ready",
        bool_path(
            replacement_summary,
            &["search_candidate_shadow_evidence", "filter_pushdown_ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_candidate_shadow_filter_pushdown_field_summary_count",
        u64_path(
            replacement_summary,
            &[
                "search_candidate_shadow_evidence",
                "filter_pushdown_field_summary_count",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_candidate_shadow_filter_pushdown_field_capabilities_ready",
        bool_path(
            replacement_summary,
            &[
                "search_candidate_shadow_evidence",
                "filter_pushdown_field_capabilities_ready",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_candidate_shadow_filter_pushdown_missing_value_summary_fields",
        value_path(
            replacement_summary,
            &[
                "search_candidate_shadow_evidence",
                "filter_pushdown_missing_value_summary_fields",
            ],
        )
        .cloned(),
    );
    insert_json_value(
        &mut summary,
        "search_candidate_shadow_filter_pushdown_missing_numeric_range_fields",
        value_path(
            replacement_summary,
            &[
                "search_candidate_shadow_evidence",
                "filter_pushdown_missing_numeric_range_fields",
            ],
        )
        .cloned(),
    );
    insert_json_value(
        &mut summary,
        "search_candidate_shadow_filter_pushdown_missing_timestamp_range_fields",
        value_path(
            replacement_summary,
            &[
                "search_candidate_shadow_evidence",
                "filter_pushdown_missing_timestamp_range_fields",
            ],
        )
        .cloned(),
    );
    insert_json_value(
        &mut summary,
        "query_runtime_preflight_ready",
        bool_path(query_runtime_preflight, &["ready"]),
    );
    insert_json_value(
        &mut summary,
        "query_runtime_preflight_database_opened",
        bool_path(query_runtime_preflight, &["database_opened"]),
    );
    insert_json_value(
        &mut summary,
        "query_runtime_preflight_redaction_ready",
        bool_path(query_runtime_preflight, &["redaction", "ready"]),
    );
    insert_json_value(
        &mut summary,
        "query_runtime_preflight_rows_copied",
        bool_path(query_runtime_preflight, &["redaction", "rows_copied"]),
    );
    insert_json_value(
        &mut summary,
        "query_runtime_preflight_parameters_copied",
        bool_path(query_runtime_preflight, &["redaction", "parameters_copied"]),
    );
    insert_json_value(
        &mut summary,
        "query_runtime_preflight_local_paths_copied",
        bool_path(
            query_runtime_preflight,
            &["redaction", "local_paths_copied"],
        ),
    );
    insert_json_value(
        &mut summary,
        "query_runtime_preflight_raw_errors_copied",
        bool_path(query_runtime_preflight, &["redaction", "raw_errors_copied"]),
    );
    insert_json_value(
        &mut summary,
        "query_runtime_preflight_probe_count",
        u64_path(query_runtime_preflight, &["probe_count"]),
    );
    insert_json_value(
        &mut summary,
        "query_runtime_preflight_passed_probe_count",
        u64_path(query_runtime_preflight, &["passed_probe_count"]),
    );
    insert_json_value(
        &mut summary,
        "query_runtime_preflight_failed_probe_count",
        u64_path(query_runtime_preflight, &["failed_probe_count"]),
    );
    insert_json_value(
        &mut summary,
        "library_readiness_ready",
        bool_path(library_readiness, &["ready"]),
    );
    insert_json_value(
        &mut summary,
        "library_readiness_ready_area_count",
        u64_path(library_readiness, &["ready_area_count"]),
    );
    insert_json_value(
        &mut summary,
        "library_readiness_blocked_area_count",
        u64_path(library_readiness, &["blocked_area_count"]),
    );
    insert_json_value(
        &mut summary,
        "library_readiness_redaction_ready",
        bool_path(library_readiness, &["redaction", "ready"]),
    );
    insert_json_value(
        &mut summary,
        "library_readiness_query_text_copied",
        bool_path(library_readiness, &["redaction", "query_text_copied"]),
    );
    insert_json_value(
        &mut summary,
        "library_readiness_parameters_copied",
        bool_path(library_readiness, &["redaction", "parameters_copied"]),
    );
    insert_json_value(
        &mut summary,
        "library_readiness_local_paths_copied",
        bool_path(library_readiness, &["redaction", "local_paths_copied"]),
    );
    insert_json_value(
        &mut summary,
        "library_readiness_graph_opened",
        bool_path(library_readiness, &["open_report", "graph_opened"]),
    );
    insert_json_value(
        &mut summary,
        "library_readiness_search_projection_opened",
        bool_path(
            library_readiness,
            &["open_report", "search_projection_opened"],
        ),
    );
    insert_json_value(
        &mut summary,
        "library_readiness_workload_fixture_graph_rag_ready",
        Some(library_readiness_graph_rag_workload_ready(
            library_readiness,
        )),
    );
    insert_json_value(
        &mut summary,
        "library_readiness_workload_fixture_graph_rag_probe_count",
        u64_path(
            library_readiness,
            &["workload_fixture_evidence", "graph_rag_probe_count"],
        ),
    );
    insert_json_value(
        &mut summary,
        "library_readiness_workload_fixture_failed_graph_rag_probe_count",
        u64_path(
            library_readiness,
            &["workload_fixture_evidence", "failed_graph_rag_probe_count"],
        ),
    );
    insert_json_value(
        &mut summary,
        "library_readiness_workload_fixture_source_projection_ready",
        Some(workload_source_projection_ready(
            library_readiness,
            &["workload_fixture_evidence"],
        )),
    );
    insert_json_value(
        &mut summary,
        "library_readiness_workload_fixture_source_projection_probe_count",
        u64_path(
            library_readiness,
            &["workload_fixture_evidence", "source_projection_probe_count"],
        ),
    );
    insert_json_value(
        &mut summary,
        "library_readiness_workload_fixture_failed_source_projection_probe_count",
        u64_path(
            library_readiness,
            &[
                "workload_fixture_evidence",
                "failed_source_projection_probe_count",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "cutover_controls_ready",
        bool_path(cutover_controls, &["ready"]),
    );
    insert_json_value(
        &mut summary,
        "cutover_controls_graph_reads",
        str_path(cutover_controls, &["controls", "graph_reads"]),
    );
    insert_json_value(
        &mut summary,
        "cutover_controls_search_reads",
        str_path(cutover_controls, &["controls", "search_reads"]),
    );
    insert_json_value(
        &mut summary,
        "cutover_controls_dual_writes_enabled",
        bool_path(cutover_controls, &["work", "dual_writes_enabled"]),
    );
    insert_json_value(
        &mut summary,
        "cutover_controls_projection_catch_up_enabled",
        bool_path(cutover_controls, &["work", "projection_catch_up_enabled"]),
    );
    insert_json_value(
        &mut summary,
        "cutover_controls_initial_import_inactive_for_cutover",
        bool_path(
            cutover_controls,
            &["work", "initial_import_inactive_for_cutover"],
        ),
    );
    insert_json_value(
        &mut summary,
        "cutover_controls_initial_import_cutover_catch_up_ready",
        bool_path(
            cutover_controls,
            &["work", "initial_import_cutover_catch_up_ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "cutover_controls_initial_import_safe_for_read_cutover",
        bool_path(
            cutover_controls,
            &["work", "initial_import_safe_for_read_cutover"],
        ),
    );
    insert_json_value(
        &mut summary,
        "cutover_controls_graph_read_effective",
        bool_path(cutover_controls, &["graph", "read_effective"]),
    );
    insert_json_value(
        &mut summary,
        "cutover_controls_search_read_effective",
        bool_path(cutover_controls, &["search", "read_effective"]),
    );
    insert_json_value(
        &mut summary,
        "operations_readiness_ready",
        bool_path(operations_readiness, &["ready"]),
    );
    insert_json_value(
        &mut summary,
        "operations_readiness_graph_open",
        bool_path(operations_readiness, &["graph", "open"]),
    );
    insert_json_value(
        &mut summary,
        "operations_readiness_search_projection_open",
        bool_path(operations_readiness, &["search_projection", "open"]),
    );
    insert_json_value(
        &mut summary,
        "operations_readiness_search_projection_stale",
        bool_path(operations_readiness, &["search_projection", "stale"]),
    );
    insert_json_value(
        &mut summary,
        "operations_readiness_storage_lifecycle_action",
        str_path(operations_readiness, &["storage_lifecycle", "action"]),
    );
    insert_json_value(
        &mut summary,
        "operations_readiness_storage_recovery_ready",
        bool_path(
            operations_readiness,
            &["readiness", "storage_recovery_ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "operations_readiness_slow_query_ready",
        bool_path(operations_readiness, &["readiness", "slow_query_ready"]),
    );
    insert_json_value(
        &mut summary,
        "operations_readiness_background_maintenance_ready",
        bool_path(
            operations_readiness,
            &["readiness", "background_maintenance_ready"],
        ),
    );
    serde_json::Value::Object(summary)
}

fn insert_json_value<T: serde::Serialize>(
    object: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: T,
) {
    object.insert(key.to_string(), serde_json::json!(value));
}

fn preflight_check(
    name: &str,
    conditions: impl IntoIterator<Item = bool>,
    evidence_fields: impl IntoIterator<Item = &'static str>,
    blocker_codes: Vec<String>,
) -> NowledgePreviousWrapperPreflightCheckReport {
    let conditions = conditions.into_iter().collect::<Vec<_>>();
    let evidence_fields = evidence_fields
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let failed_evidence_fields = conditions
        .iter()
        .zip(evidence_fields.iter())
        .filter_map(|(condition, field)| (!*condition).then_some(field.clone()))
        .collect::<Vec<_>>();
    let ready = failed_evidence_fields.is_empty();
    NowledgePreviousWrapperPreflightCheckReport {
        name: name.to_string(),
        ready,
        evidence_fields,
        failed_evidence_fields,
        blocker_codes,
    }
}

fn value_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    Some(current)
}

fn bool_path(value: &serde_json::Value, path: &[&str]) -> Option<bool> {
    value_path(value, path).and_then(serde_json::Value::as_bool)
}

fn str_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a str> {
    value_path(value, path).and_then(serde_json::Value::as_str)
}

fn string_array_path(value: &serde_json::Value, path: &[&str]) -> Vec<String> {
    value_path(value, path)
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn u64_path(value: &serde_json::Value, path: &[&str]) -> Option<u64> {
    value_path(value, path).and_then(serde_json::Value::as_u64)
}

fn replacement_summary_background_maintenance_graph_delta_ready(
    replacement_summary: &serde_json::Value,
) -> bool {
    [
        &[
            "cutover_evidence",
            "background_maintenance_executable_search_projection_graph_delta_count",
        ][..],
        &[
            "cutover_evidence",
            "background_maintenance_admitted_search_projection_graph_delta_count",
        ][..],
        &[
            "cutover_evidence",
            "background_maintenance_deferred_search_projection_graph_delta_count",
        ][..],
        &[
            "cutover_evidence",
            "background_maintenance_rejected_search_projection_graph_delta_count",
        ][..],
        &[
            "cutover_evidence",
            "background_maintenance_executable_search_projection_graph_delta_operations",
        ][..],
        &[
            "cutover_evidence",
            "background_maintenance_admitted_search_projection_graph_delta_operations",
        ][..],
    ]
    .iter()
    .all(|path| u64_path(replacement_summary, path).is_some())
        && value_path(
            replacement_summary,
            &[
                "cutover_evidence",
                "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch",
            ],
        )
        .is_some_and(|value| value.is_u64() || value.is_null())
        && bool_path(
            replacement_summary,
            &[
                "cutover_evidence",
                "background_maintenance_foreground_admission_probe_ready",
            ],
        ) == Some(true)
}

fn library_readiness_area_ready(value: &serde_json::Value, area: &str) -> bool {
    value_path(value, &["readiness_by_area", area, "ready"]).and_then(serde_json::Value::as_bool)
        == Some(true)
}

fn library_readiness_graph_rag_workload_ready(value: &serde_json::Value) -> bool {
    let Some(evidence) = value_path(value, &["workload_fixture_evidence"]) else {
        return false;
    };
    str_path(evidence, &["protocol"]) == Some(NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL)
        && bool_path(evidence, &["ready"]) == Some(true)
        && u64_path(evidence, &["graph_rag_probe_count"]).is_some_and(|count| count > 0)
        && u64_path(evidence, &["failed_graph_rag_probe_count"]) == Some(0)
        && value_path(evidence, &["graph_rag_reports"])
            .and_then(serde_json::Value::as_array)
            .is_some_and(|reports| reports.iter().any(graph_rag_workload_report_ready))
}

fn graph_rag_workload_report_ready(report: &serde_json::Value) -> bool {
    bool_path(report, &["ready"]) == Some(true)
        && str_path(report, &["schema_protocol"]) == Some(GRAPH_RAG_SCHEMA_CONTEXT_PROTOCOL)
        && u64_path(report, &["label_count"]).is_some_and(|count| count > 0)
        && u64_path(report, &["relationship_type_count"]).is_some_and(|count| count > 0)
        && u64_path(report, &["route_count"]).is_some_and(|count| count > 0)
        && u64_path(report, &["parameter_requirement_count"]).is_some_and(|count| count > 0)
        && u64_path(report, &["row_count"]).is_some_and(|count| count > 0)
        && bool_path(report, &["row_budget_exceeded"]) == Some(false)
        && bool_path(report, &["payload_budget_exceeded"]) == Some(false)
        && u64_path(report, &["blocking_operator_count"]) == Some(0)
        && bool_path(report, &["streaming"]) == Some(false)
        && value_path(report, &["error_class"]).is_none_or(serde_json::Value::is_null)
}

fn workload_source_projection_ready(value: &serde_json::Value, path: &[&str]) -> bool {
    let Some(evidence) = value_path(value, path) else {
        return false;
    };
    u64_path(evidence, &["source_projection_probe_count"]).is_some_and(|count| count > 0)
        && u64_path(evidence, &["failed_source_projection_probe_count"]) == Some(0)
        && value_path(evidence, &["source_projection_reports"])
            .and_then(serde_json::Value::as_array)
            .is_some_and(|reports| reports.iter().any(source_projection_workload_report_ready))
}

fn source_projection_workload_report_ready(report: &serde_json::Value) -> bool {
    bool_path(report, &["ready"]) == Some(true)
        && bool_path(report, &["too_small_batch_failed_closed"]) == Some(true)
        && u64_path(report, &["operation_count"]) == Some(2)
        && u64_path(report, &["upserted_documents"]) == Some(2)
        && u64_path(report, &["deleted_documents"]) == Some(0)
        && u64_path(report, &["source_document_count"]) == Some(2)
        && bool_path(report, &["indexed_source_document_ready"]) == Some(true)
        && u64_path(report, &["source_graph_commit_epoch"]).is_some()
        && u64_path(report, &["complete_through_graph_commit_epoch"]).is_some()
        && value_path(report, &["error_class"]).is_none_or(serde_json::Value::is_null)
}

fn full_contract_check_count_ready(value: &serde_json::Value) -> bool {
    u64_path(value, &["check_count"]).is_some_and(|count| count > 0)
}

fn selected_checks_match_check_count(value: &serde_json::Value) -> bool {
    let Some(selected_checks) = u64_path(value, &["selected_checks"]) else {
        return false;
    };
    let Some(check_count) = u64_path(value, &["check_count"]) else {
        return false;
    };
    check_count > 0 && selected_checks == check_count
}

fn dual_engine_primary_count_ready(value: &serde_json::Value, path: &[&str]) -> bool {
    dual_engine_u64_path(value, path, "primary_check_count").is_some_and(|count| count > 0)
}

fn dual_engine_primary_shadow_counts_match(value: &serde_json::Value, path: &[&str]) -> bool {
    let Some(primary_check_count) = dual_engine_u64_path(value, path, "primary_check_count") else {
        return false;
    };
    let Some(shadow_check_count) = dual_engine_u64_path(value, path, "shadow_check_count") else {
        return false;
    };
    primary_check_count > 0 && primary_check_count == shadow_check_count
}

fn dual_engine_matched_shadow_counts_match(value: &serde_json::Value, path: &[&str]) -> bool {
    let Some(matched_check_count) = dual_engine_u64_path(value, path, "matched_check_count") else {
        return false;
    };
    let Some(shadow_check_count) = dual_engine_u64_path(value, path, "shadow_check_count") else {
        return false;
    };
    shadow_check_count > 0 && matched_check_count == shadow_check_count
}

fn dual_engine_primary_only_count_clear(value: &serde_json::Value, path: &[&str]) -> bool {
    dual_engine_u64_path(value, path, "primary_only_check_count") == Some(0)
}

fn dual_engine_count_evidence_consistent(value: &serde_json::Value, path: &[&str]) -> bool {
    dual_engine_primary_count_ready(value, path)
        && dual_engine_primary_shadow_counts_match(value, path)
        && dual_engine_matched_shadow_counts_match(value, path)
        && dual_engine_primary_only_count_clear(value, path)
}

fn search_projection_table_counts_match(value: &serde_json::Value) -> bool {
    let Some(covered_table_count) = u64_path(
        value,
        &["search_projection_evidence", "covered_table_count"],
    ) else {
        return false;
    };
    let Some(required_table_count) = u64_path(
        value,
        &["search_projection_evidence", "required_table_count"],
    ) else {
        return false;
    };
    required_table_count > 0 && covered_table_count == required_table_count
}

fn search_candidate_shadow_counts_ready(value: &serde_json::Value) -> bool {
    let Some(request_count) = u64_path(
        value,
        &["search_candidate_shadow_evidence", "request_count"],
    ) else {
        return false;
    };
    let Some(primary_candidate_count) = u64_path(
        value,
        &[
            "search_candidate_shadow_evidence",
            "primary_candidate_count",
        ],
    ) else {
        return false;
    };
    let Some(shadow_candidate_count) = u64_path(
        value,
        &["search_candidate_shadow_evidence", "shadow_candidate_count"],
    ) else {
        return false;
    };
    let Some(matched_candidate_count) = u64_path(
        value,
        &[
            "search_candidate_shadow_evidence",
            "matched_candidate_count",
        ],
    ) else {
        return false;
    };
    request_count > 0
        && primary_candidate_count == shadow_candidate_count
        && matched_candidate_count == shadow_candidate_count
        && u64_path(
            value,
            &[
                "search_candidate_shadow_evidence",
                "primary_only_candidate_count",
            ],
        ) == Some(0)
}

fn search_projection_shadow_segment_pruning_candidate_count_ready(
    value: &serde_json::Value,
) -> bool {
    let Some(candidate_count) = u64_path(
        value,
        &[
            "search_projection_shadow_evidence",
            "pushdown_evidence",
            "shadow_segment_pruning_candidate_document_count",
        ],
    ) else {
        return false;
    };
    let Some(pruned_count) = u64_path(
        value,
        &[
            "search_projection_shadow_evidence",
            "pushdown_evidence",
            "shadow_segment_pruned_document_count",
        ],
    ) else {
        return false;
    };
    let Some(scanned_count) = u64_path(
        value,
        &[
            "search_projection_shadow_evidence",
            "pushdown_evidence",
            "shadow_segment_scanned_document_count",
        ],
    ) else {
        return false;
    };
    candidate_count > 0 && pruned_count.checked_add(scanned_count) == Some(candidate_count)
}

fn search_projection_shadow_segment_pruning_count_is_positive(
    value: &serde_json::Value,
    field: &str,
) -> bool {
    u64_path(
        value,
        &[
            "search_projection_shadow_evidence",
            "pushdown_evidence",
            field,
        ],
    )
    .is_some_and(|count| count > 0)
}

fn search_projection_shadow_segment_descriptor_summaries_ready(value: &serde_json::Value) -> bool {
    let path = &[
        "search_projection_shadow_evidence",
        "pushdown_evidence",
        "shadow_segment_descriptor_field_summaries",
    ];
    let Some(summaries) = value_path(value, path).and_then(serde_json::Value::as_array) else {
        return false;
    };
    if summaries.is_empty() {
        return false;
    }
    let fields = summaries
        .iter()
        .filter_map(|summary| str_path(summary, &["field"]))
        .collect::<BTreeSet<_>>();
    NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
        .iter()
        .all(|required| fields.contains(required))
        && ["importance", "confidence"].iter().all(|field| {
            summaries.iter().any(|summary| {
                str_path(summary, &["field"]) == Some(*field)
                    && bool_path(summary, &["numeric_range_summary_used"]) == Some(true)
                    && u64_path(summary, &["numeric_range_segment_count"])
                        .is_some_and(|count| count > 0)
            })
        })
        && ["created_at", "updated_at", "event_start", "event_end"]
            .iter()
            .all(|field| {
                summaries.iter().any(|summary| {
                    str_path(summary, &["field"]) == Some(*field)
                        && bool_path(summary, &["timestamp_range_summary_used"]) == Some(true)
                        && u64_path(summary, &["timestamp_range_segment_count"])
                            .is_some_and(|count| count > 0)
                })
            })
        && summaries.iter().any(|summary| {
            str_path(summary, &["field"]) == Some("document_id")
                && bool_path(summary, &["unique_key_summary_used"]) == Some(true)
                && u64_path(summary, &["unique_key_summary_segment_count"])
                    .is_some_and(|count| count > 0)
        })
}

fn search_candidate_missing_required_fields_ready(value: &serde_json::Value) -> bool {
    let path = &[
        "search_candidate_shadow_evidence",
        "filter_pushdown_missing_required_fields",
    ];
    value_path(value, path).is_some_and(serde_json::Value::is_array)
        && string_array_path(value, path).is_empty()
}

fn search_candidate_missing_capability_fields_ready(
    value: &serde_json::Value,
    field: &str,
) -> bool {
    let path = &["search_candidate_shadow_evidence", field];
    value_path(value, path).is_some_and(serde_json::Value::is_array)
        && string_array_path(value, path).is_empty()
}

fn query_runtime_preflight_counts_match(value: &serde_json::Value) -> bool {
    let Some(probe_count) = u64_path(value, &["probe_count"]) else {
        return false;
    };
    let Some(passed_probe_count) = u64_path(value, &["passed_probe_count"]) else {
        return false;
    };
    let Some(failed_probe_count) = u64_path(value, &["failed_probe_count"]) else {
        return false;
    };
    probe_count > 0 && passed_probe_count == probe_count && failed_probe_count == 0
}

fn route_catalog_metadata_ready(value: &serde_json::Value, path: &[&str]) -> bool {
    let Some(value) = value_path(value, path) else {
        return false;
    };
    str_path(value, &["route_catalog_version"])
        == Some(NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION)
        && str_path(value, &["route_catalog_digest"])
            == Some(nowledge_mem_graph_read_route_catalog_digest().as_str())
}

fn bounded_read_execution_row_cap_ready(value: &serde_json::Value) -> bool {
    let Some(max_rows) = u64_path(value, &["bounded_read_evidence", "max_rows"]) else {
        return false;
    };
    max_rows > 0
        && max_rows.checked_add(1).is_some_and(|expected| {
            u64_path(value, &["bounded_read_evidence", "execution_row_cap"]) == Some(expected)
        })
}

fn bounded_read_relationship_property_pruning_ready(value: &serde_json::Value) -> bool {
    let Some(required_count) = u64_path(
        value,
        &[
            "bounded_read_evidence",
            "relationship_property_pruning_required_count",
        ],
    ) else {
        return false;
    };
    required_count
        == u64_path(
            value,
            &[
                "bounded_read_evidence",
                "relationship_property_pruning_report_count",
            ],
        )
        .unwrap_or_default()
}

fn query_runtime_preflight_probe_details_ready(value: &serde_json::Value) -> bool {
    let Some(probes) = value
        .get("probes")
        .and_then(serde_json::Value::as_array)
        .filter(|probes| !probes.is_empty())
    else {
        return false;
    };
    probes.iter().all(query_runtime_preflight_probe_ready)
}

fn query_runtime_preflight_route_coverage_ready(value: &serde_json::Value) -> bool {
    let Some(probes) = value
        .get("probes")
        .and_then(serde_json::Value::as_array)
        .filter(|probes| !probes.is_empty())
    else {
        return false;
    };
    let observed_routes = probes
        .iter()
        .filter_map(|probe| str_path(probe, &["route"]))
        .collect::<Vec<_>>();
    let observed_route_set = observed_routes.iter().copied().collect::<BTreeSet<_>>();
    let duplicate_routes = duplicate_routes(&observed_routes);
    let unknown_routes = observed_route_set
        .iter()
        .filter(|route| !REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.contains(route))
        .count();
    u64_path(value, &["required_route_count"])
        == Some(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64)
        && u64_path(value, &["covered_route_count"])
            == Some(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64)
        && bool_path(value, &["required_routes_covered"]) == Some(true)
        && empty_array_path(value, &["missing_required_routes"])
        && empty_array_path(value, &["unknown_routes"])
        && empty_array_path(value, &["duplicate_routes"])
        && str_path(value, &["route_catalog_version"])
            == Some(NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION)
        && str_path(value, &["route_catalog_digest"])
            == Some(nowledge_mem_graph_read_route_catalog_digest().as_str())
        && bool_path(value, &["route_coverage_ready"]) == Some(true)
        && empty_array_path(value, &["route_coverage_blocker_codes"])
        && unknown_routes == 0
        && duplicate_routes.is_empty()
        && REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .all(|route| observed_route_set.contains(route))
}

fn duplicate_routes(routes: &[&str]) -> Vec<String> {
    let mut counts = BTreeMap::<&str, usize>::new();
    for route in routes {
        *counts.entry(*route).or_default() += 1;
    }
    counts
        .into_iter()
        .filter(|(_, count)| *count > 1)
        .map(|(route, _)| route.to_string())
        .collect()
}

fn query_runtime_preflight_probe_ready(probe: &serde_json::Value) -> bool {
    bool_path(probe, &["ready"]) == Some(true)
        && bool_path(probe, &["success"]) == Some(true)
        && str_path(probe, &["selected_plan_fingerprint"])
            .is_some_and(|value| !value.trim().is_empty())
        && value_path(probe, &["output_row_count"]).is_some()
        && json_object_path_is_non_empty(probe, &["selected_plan_operator_counts"])
        && json_object_path_is_non_empty(probe, &["selected_plan_class_counts"])
        && u64_path(probe, &["optimizer_decision_count"]).is_some()
        && u64_path(probe, &["optimizer_rule_event_count"]).is_some()
        && query_runtime_preflight_probe_plan_cache_ready(probe)
        && query_runtime_preflight_probe_scan_pruning_ready(probe)
        && empty_array_path(probe, &["blocker_codes"])
}

fn query_runtime_preflight_probe_plan_cache_ready(probe: &serde_json::Value) -> bool {
    str_path(probe, &["plan_cache_lookup"]).is_some_and(|value| !value.trim().is_empty())
        && str_path(probe, &["plan_cache", "lookup"]).is_some_and(|value| !value.trim().is_empty())
        && bool_path(probe, &["plan_cache", "cacheable"]).is_some()
        && bool_path(probe, &["plan_cache", "hit"]).is_some()
        && bool_path(probe, &["plan_cache", "miss"]).is_some()
        && bool_path(probe, &["plan_cache", "bypassed"]) == Some(false)
}

fn query_runtime_preflight_probe_scan_pruning_ready(probe: &serde_json::Value) -> bool {
    let Some(report_count) = u64_path(probe, &["execution_profile", "scan_pruning_report_count"])
    else {
        return false;
    };
    let Some(reports) = value_path(probe, &["execution_profile", "scan_pruning_reports"])
        .and_then(serde_json::Value::as_array)
    else {
        return false;
    };
    report_count == reports.len() as u64
        && u64_path(probe, &["execution_profile", "pruned_scan_count"]).is_some()
        && reports.iter().all(query_runtime_scan_pruning_report_ready)
}

fn query_runtime_scan_pruning_report_ready(report: &serde_json::Value) -> bool {
    scan_pruning_target_kind_ready(report)
        && value_path(report, &["strategy"]).is_some_and(serde_json::Value::is_object)
        && bool_path(report, &["pruned"]).is_some()
        && bool_path(report, &["exact_empty"]).is_some()
        && u64_path(report, &["candidate_count_before_pruning"]).is_some()
        && u64_path(report, &["pruned_candidate_count"]).is_some()
        && u64_path(report, &["candidate_count_before_filter"]).is_some()
        && u64_path(report, &["output_count"]).is_some()
        && u64_path(report, &["filtered_out_count"]).is_some()
}

fn scan_pruning_target_kind_ready(report: &serde_json::Value) -> bool {
    matches!(
        str_path(report, &["target_kind"]),
        Some("node" | "relationship")
    )
}

fn dual_engine_u64_path(value: &serde_json::Value, path: &[&str], field: &str) -> Option<u64> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    current.get(field).and_then(serde_json::Value::as_u64)
}

fn empty_array_path(value: &serde_json::Value, path: &[&str]) -> bool {
    value_path(value, path)
        .and_then(serde_json::Value::as_array)
        .is_some_and(Vec::is_empty)
}

fn json_object_path_is_non_empty(value: &serde_json::Value, path: &[&str]) -> bool {
    value_path(value, path)
        .and_then(serde_json::Value::as_object)
        .is_some_and(|object| !object.is_empty())
}

fn blocker_codes(value: &serde_json::Value, paths: &[&[&str]]) -> Vec<String> {
    let mut codes = Vec::new();
    for path in paths {
        if let Some(items) = value_path(value, path).and_then(serde_json::Value::as_array) {
            for item in items {
                if let Some(code) = item.as_str() {
                    codes.push(code.to_string());
                } else if let Some(action) = item.get("action").and_then(serde_json::Value::as_str)
                {
                    codes.push(action.to_string());
                }
            }
        }
    }
    codes.sort();
    codes.dedup();
    codes
}

#[cfg(test)]
fn check_by_name<'a>(report: &'a serde_json::Value, name: &str) -> &'a serde_json::Value {
    report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["name"] == name)
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::{
        check_by_name, nowledge_previous_wrapper_preflight_check_json,
        run_nowledge_previous_wrapper_preflight_check, PreviousWrapperPreflightCheckInputs,
        GRAPH_RAG_SCHEMA_CONTEXT_PROTOCOL, SKEIN_NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL,
        SKEIN_NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL,
        SKEIN_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE,
    };
    use super::{
        nowledge_mem_graph_read_route_catalog_digest,
        NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL,
        NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL, NOWLEDGE_MEM_CUTOVER_CONTROLS_PROTOCOL,
        NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION, NOWLEDGE_MEM_OPERATIONS_READINESS_PROTOCOL,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_PRIMARY_ENGINE,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
        NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS, REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
    };
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn preflight_check_reports_ready_when_all_artifacts_are_ready() {
        let report = nowledge_previous_wrapper_preflight_check_json(ready_inputs()).unwrap();

        assert_eq!(report["ready"], true);
        assert_eq!(report["failed_checks"], serde_json::json!([]));
        assert_eq!(report["wrapper_identity"], "nowledge-previous-wrapper:test");
        let summary = &report["release_summary"];
        assert_release_summary_field(
            summary,
            "wrapper_identity",
            "nowledge-previous-wrapper:test",
        );
        assert_release_summary_field(summary, "required_contract_ready", true);
        assert_release_summary_field(summary, "full_contract_checked", true);
        assert_release_summary_field(summary, "full_contract_ready", true);
        assert_release_summary_field(summary, "selected_checks", 2);
        assert_release_summary_field(summary, "check_count", 2);
        assert_release_summary_field(summary, "adapter_smoke_ready", true);
        assert_release_summary_field(summary, "adapter_dual_engine_counts_consistent", true);
        assert_release_summary_field(summary, "migration_gate_decision", "ready");
        assert_release_summary_field(summary, "cutover_decision", "ready");
        assert_release_summary_field(summary, "ready_engine_kind", "previous_wrapper");
        assert_release_summary_field(
            summary,
            "ready_wrapper_identity",
            "nowledge-previous-wrapper:test",
        );
        assert_release_summary_field(summary, "production_cutover_ready", true);
        assert_release_summary_field(summary, "production_replacement_per_million", 1_000_000);
        assert_release_summary_field(summary, "shadow_evidence_ready", true);
        assert_release_summary_field(
            summary,
            "shadow_evidence_ready_wrapper_identity",
            "nowledge-previous-wrapper:test",
        );
        assert_release_summary_field(
            summary,
            "shadow_evidence_contract_wrapper_identity",
            "nowledge-previous-wrapper:test",
        );
        assert_release_summary_field(
            summary,
            "shadow_evidence_cutover_ready_wrapper_identity",
            "nowledge-previous-wrapper:test",
        );
        assert_release_summary_field(summary, "bounded_read_evidence_ready", true);
        assert_release_summary_field(summary, "bounded_read_evidence_mode", "shadow_read_only");
        assert_release_summary_field(summary, "bounded_read_max_rows", 512);
        assert_release_summary_field(summary, "bounded_read_execution_row_cap", 513);
        assert_release_summary_field(summary, "bounded_read_estimated_payload_bytes", 2048);
        assert_release_summary_field(summary, "bounded_read_max_estimated_payload_bytes", 4096);
        assert_release_summary_field(summary, "bounded_read_payload_budget_exceeded", false);
        assert_release_summary_field(
            summary,
            "bounded_read_row_limit_enforced_before_output",
            true,
        );
        assert_release_summary_field(summary, "bounded_read_operator_row_cap_enabled", true);
        assert_release_summary_field(summary, "bounded_read_streaming", false);
        assert_release_summary_field(summary, "bounded_read_blocking_operator_count", 0);
        assert_release_summary_field(
            summary,
            "bounded_read_blocking_operator_memory_reports_complete",
            true,
        );
        assert_release_summary_field(
            summary,
            "bounded_read_blocking_operator_memory_within_budget",
            true,
        );
        assert_release_summary_field(summary, "bounded_read_spill_within_budget", true);
        assert_release_summary_field(
            summary,
            "bounded_read_route_catalog_version",
            NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION,
        );
        assert_release_summary_field(
            summary,
            "bounded_read_route_catalog_digest",
            nowledge_mem_graph_read_route_catalog_digest(),
        );
        assert_release_summary_field(summary, "storage_recovery_ready", true);
        assert_release_summary_field(summary, "storage_recovery_durable", true);
        assert_release_summary_field(
            summary,
            "storage_recovery_checkpoint_boundary_present",
            true,
        );
        assert_release_summary_field(summary, "storage_recovery_wal_replay_bounded", true);
        assert_release_summary_field(summary, "storage_recovery_replay_boundary_consistent", true);
        assert_release_summary_field(summary, "storage_recovery_torn_tail_clean", true);
        assert_release_summary_field(summary, "background_maintenance_ready", true);
        assert_release_summary_field(
            summary,
            "background_maintenance_executable_search_projection_graph_delta_count",
            2,
        );
        assert_release_summary_field(
            summary,
            "background_maintenance_admitted_search_projection_graph_delta_count",
            1,
        );
        assert_release_summary_field(summary, "dual_engine_evidence_present", true);
        assert_release_summary_field(summary, "dual_engine_evidence_ready", true);
        assert_release_summary_field(summary, "dual_engine_evidence_consistent", true);
        assert_release_summary_field(summary, "dual_engine_primary_check_count", 2);
        assert_release_summary_field(summary, "dual_engine_shadow_check_count", 2);
        assert_release_summary_field(summary, "dual_engine_matched_check_count", 2);
        assert_release_summary_field(summary, "dual_engine_primary_only_check_count", 0);
        assert_release_summary_field(summary, "dual_engine_matched_per_million", 1_000_000);
        assert_release_summary_field(summary, "search_projection_evidence_ready", true);
        assert_release_summary_field(
            summary,
            "search_projection_evidence_protocol",
            SKEIN_NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL,
        );
        assert_release_summary_field(summary, "search_projection_all_tables_covered", true);
        assert_release_summary_field(summary, "search_projection_covered_table_count", 6);
        assert_release_summary_field(summary, "search_projection_required_table_count", 6);
        assert_release_summary_field(summary, "search_projection_fts_ready", true);
        assert_release_summary_field(summary, "search_projection_vector_ready", true);
        assert_release_summary_field(summary, "search_projection_document_identity_ready", true);
        assert_release_summary_field(summary, "search_projection_embedding_identity_ready", true);
        assert_release_summary_field(summary, "search_projection_fail_soft_ready", true);
        assert_release_summary_field(summary, "search_projection_incremental_update_ready", true);
        assert_release_summary_field(summary, "search_projection_predicate_pushdown_ready", true);
        assert_release_summary_field(
            summary,
            "search_projection_production_filter_pruning_ready",
            true,
        );
        assert_release_summary_field(summary, "search_projection_shadow_evidence_ready", true);
        assert_release_summary_field(
            summary,
            "search_projection_shadow_evidence_protocol",
            SKEIN_NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL,
        );
        assert_release_summary_field(
            summary,
            "search_projection_shadow_evidence_source",
            SKEIN_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE,
        );
        assert_release_summary_field(summary, "search_projection_shadow_primary_ready", true);
        assert_release_summary_field(summary, "search_projection_shadow_shadow_ready", true);
        assert_release_summary_field(
            summary,
            "search_projection_shadow_document_count_parity",
            true,
        );
        assert_release_summary_field(
            summary,
            "search_projection_shadow_document_identity_parity",
            true,
        );
        assert_release_summary_field(summary, "search_projection_shadow_table_parity_ready", true);
        assert_release_summary_field(
            summary,
            "search_projection_shadow_embedding_identity_parity",
            true,
        );
        assert_release_summary_field(summary, "search_projection_shadow_lifecycle_parity", true);
        assert_release_summary_field(
            summary,
            "search_projection_shadow_incremental_watermark_parity",
            true,
        );
        assert_release_summary_field(summary, "search_projection_shadow_pushdown_ready", true);
        assert_release_summary_field(
            summary,
            "search_projection_shadow_predicate_pushdown_parity",
            true,
        );
        assert_release_summary_field(
            summary,
            "search_projection_shadow_persisted_segment_descriptor_ready",
            true,
        );
        assert_release_summary_field(
            summary,
            "search_projection_shadow_segment_descriptor_scan_filter_fields_ready",
            true,
        );
        assert_release_summary_field(
            summary,
            "search_projection_shadow_segment_document_pruning_ready",
            true,
        );
        assert_release_summary_field(
            summary,
            "search_projection_shadow_segment_pruning_candidate_document_count",
            4,
        );
        assert_release_summary_field(
            summary,
            "search_projection_shadow_segment_pruned_document_count",
            2,
        );
        assert_release_summary_field(
            summary,
            "search_projection_shadow_segment_scanned_document_count",
            2,
        );
        assert_release_summary_field(
            summary,
            "search_projection_shadow_segment_descriptor_field_summaries_ready",
            true,
        );
        assert_release_summary_field(summary, "search_candidate_shadow_evidence_ready", true);
        assert_release_summary_field(
            summary,
            "search_candidate_shadow_candidate_counts_ready",
            true,
        );
        assert_release_summary_field(
            summary,
            "search_candidate_shadow_text_retriever_ready",
            true,
        );
        assert_release_summary_field(
            summary,
            "search_candidate_shadow_vector_retriever_ready",
            true,
        );
        assert_release_summary_field(
            summary,
            "search_candidate_shadow_fts_top_k_overlap_ready",
            true,
        );
        assert_release_summary_field(
            summary,
            "search_candidate_shadow_vector_top_k_overlap_ready",
            true,
        );
        assert_release_summary_field(
            summary,
            "search_candidate_shadow_source_chunk_identity_ready",
            true,
        );
        assert_release_summary_field(summary, "search_candidate_shadow_fail_soft_observed", true);
        assert_release_summary_field(
            summary,
            "search_candidate_shadow_projection_marker_status_visible",
            true,
        );
        assert_release_summary_field(
            summary,
            "search_candidate_shadow_projection_watermark_ready",
            true,
        );
        assert_release_summary_field(
            summary,
            "search_candidate_shadow_embedding_identity_ready",
            true,
        );
        assert_release_summary_field(
            summary,
            "search_candidate_shadow_candidate_identity_ready",
            true,
        );
        assert_release_summary_field(
            summary,
            "search_candidate_shadow_filter_pushdown_ready",
            true,
        );
        assert_release_summary_field(
            summary,
            "search_candidate_shadow_filter_pushdown_field_summary_count",
            NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len(),
        );
        assert_release_summary_field(
            summary,
            "search_candidate_shadow_filter_pushdown_field_capabilities_ready",
            true,
        );
        assert_release_summary_field(
            summary,
            "search_candidate_shadow_filter_pushdown_missing_value_summary_fields",
            serde_json::json!([]),
        );
        assert_release_summary_field(
            summary,
            "search_candidate_shadow_filter_pushdown_missing_numeric_range_fields",
            serde_json::json!([]),
        );
        assert_release_summary_field(
            summary,
            "search_candidate_shadow_filter_pushdown_missing_timestamp_range_fields",
            serde_json::json!([]),
        );
        assert_release_summary_field(summary, "query_runtime_preflight_ready", true);
        assert_release_summary_field(summary, "query_runtime_preflight_database_opened", true);
        assert_release_summary_field(summary, "query_runtime_preflight_redaction_ready", true);
        assert_release_summary_field(summary, "query_runtime_preflight_rows_copied", false);
        assert_release_summary_field(summary, "query_runtime_preflight_parameters_copied", false);
        assert_release_summary_field(summary, "query_runtime_preflight_local_paths_copied", false);
        assert_release_summary_field(summary, "query_runtime_preflight_raw_errors_copied", false);
        assert_release_summary_field(
            summary,
            "query_runtime_preflight_probe_count",
            REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
        );
        assert_release_summary_field(
            summary,
            "query_runtime_preflight_passed_probe_count",
            REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
        );
        assert_release_summary_field(summary, "query_runtime_preflight_failed_probe_count", 0);
        assert_release_summary_field(summary, "library_readiness_ready", true);
        assert_release_summary_field(summary, "library_readiness_ready_area_count", 11);
        assert_release_summary_field(summary, "library_readiness_blocked_area_count", 0);
        assert_release_summary_field(summary, "library_readiness_redaction_ready", true);
        assert_release_summary_field(summary, "library_readiness_query_text_copied", false);
        assert_release_summary_field(summary, "library_readiness_parameters_copied", false);
        assert_release_summary_field(summary, "library_readiness_local_paths_copied", false);
        assert_release_summary_field(summary, "library_readiness_graph_opened", true);
        assert_release_summary_field(summary, "library_readiness_search_projection_opened", true);
        assert_release_summary_field(
            summary,
            "library_readiness_workload_fixture_graph_rag_ready",
            true,
        );
        assert_release_summary_field(
            summary,
            "library_readiness_workload_fixture_graph_rag_probe_count",
            1,
        );
        assert_release_summary_field(
            summary,
            "library_readiness_workload_fixture_failed_graph_rag_probe_count",
            0,
        );
        assert_release_summary_field(
            summary,
            "library_readiness_workload_fixture_source_projection_ready",
            true,
        );
        assert_release_summary_field(
            summary,
            "library_readiness_workload_fixture_source_projection_probe_count",
            1,
        );
        assert_release_summary_field(
            summary,
            "library_readiness_workload_fixture_failed_source_projection_probe_count",
            0,
        );
        assert_release_summary_field(summary, "cutover_controls_ready", true);
        assert_release_summary_field(summary, "cutover_controls_graph_reads", "skein");
        assert_release_summary_field(summary, "cutover_controls_search_reads", "skein");
        assert_release_summary_field(summary, "cutover_controls_dual_writes_enabled", true);
        assert_release_summary_field(
            summary,
            "cutover_controls_projection_catch_up_enabled",
            true,
        );
        assert_release_summary_field(
            summary,
            "cutover_controls_initial_import_inactive_for_cutover",
            true,
        );
        assert_release_summary_field(summary, "cutover_controls_graph_read_effective", true);
        assert_release_summary_field(summary, "cutover_controls_search_read_effective", true);
        assert_release_summary_field(summary, "operations_readiness_ready", true);
        assert_release_summary_field(summary, "operations_readiness_graph_open", true);
        assert_release_summary_field(summary, "operations_readiness_search_projection_open", true);
        assert_release_summary_field(
            summary,
            "operations_readiness_search_projection_stale",
            false,
        );
        assert_release_summary_field(
            summary,
            "operations_readiness_storage_lifecycle_action",
            "ready",
        );
        assert_release_summary_field(summary, "operations_readiness_storage_recovery_ready", true);
        assert_release_summary_field(summary, "operations_readiness_slow_query_ready", true);
        assert_release_summary_field(
            summary,
            "operations_readiness_background_maintenance_ready",
            true,
        );
        assert!(report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .all(|check| check["ready"] == true));
        assert!(report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .all(|check| check["failed_evidence_fields"] == serde_json::json!([])));
    }

    #[test]
    fn typed_preflight_api_reports_structured_readiness() {
        let report = super::nowledge_previous_wrapper_preflight_check(
            super::cli_inputs_to_typed(ready_inputs()).unwrap(),
        )
        .unwrap();

        assert!(report.ready);
        assert_eq!(
            report.protocol,
            super::NOWLEDGE_PREVIOUS_WRAPPER_PREFLIGHT_PROTOCOL
        );
        assert_eq!(report.wrapper_identity, "nowledge-previous-wrapper:test");
        assert!(report.failed_checks.is_empty());
        assert!(report.checks.iter().all(|check| check.ready));
        assert!(report
            .checks
            .iter()
            .any(|check| check.name == "query_runtime_preflight"));
        assert_eq!(report.json()["ready"], true);
    }

    #[test]
    fn preflight_check_fails_closed_on_partial_evidence() {
        let mut inputs = ready_inputs();
        let replacement_summary = serde_json::json!({
            "production_cutover_ready": false,
            "production_replacement_per_million": 0,
            "blocking_categories": ["cutover_evidence"],
            "missing_evidence": [],
            "next_actions": [
                {
                    "action": "provide_eligible_cutover_evidence"
                }
            ]
        });
        inputs.replacement_summary = Some(replacement_summary);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!([
                "background_maintenance",
                "replacement_summary",
                "replacement_summary_route_catalog",
                "replacement_summary_graph_route",
                "replacement_summary_bounded_read"
            ])
        );
        assert_eq!(
            check_by_name(&report, "background_maintenance")["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.cutover_evidence.background_maintenance_graph_delta_qos",
                "replacement_summary.cutover_evidence.background_maintenance_foreground_admission_probe_ready",
                "replacement_summary.cutover_evidence.background_maintenance_memory_pressure_ready",
                "replacement_summary.cutover_evidence.background_maintenance_memory_budget_bytes",
                "replacement_summary.cutover_evidence.background_maintenance_estimated_memory_bytes"
            ])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["blocker_codes"],
            serde_json::json!(["cutover_evidence", "provide_eligible_cutover_evidence"])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["failed_evidence_fields"],
            serde_json::json!([
                "production_cutover_ready",
                "production_replacement_per_million",
                "blocking_categories",
                "next_actions",
                "shadow_evidence.ready",
                "shadow_evidence.ready_wrapper_identity",
                "shadow_evidence.contract_wrapper_identity",
                "shadow_evidence.cutover_ready_wrapper_identity",
                "dual_engine_evidence.present",
                "dual_engine_evidence.ready",
                "dual_engine_evidence.consistent",
                "dual_engine_evidence.primary_check_count",
                "dual_engine_evidence.shadow_check_count",
                "dual_engine_evidence.matched_check_count",
                "dual_engine_evidence.primary_only_check_count",
                "search_projection_evidence.present",
                "search_projection_evidence.protocol",
                "search_projection_evidence.ready",
                "search_projection_evidence.derived_projection",
                "search_projection_evidence.all_tables_covered",
                "search_projection_evidence.covered_table_count",
                "search_projection_evidence.fts_ready",
                "search_projection_evidence.vector_ready",
                "search_projection_evidence.document_identity_ready",
                "search_projection_evidence.embedding_identity_ready",
                "search_projection_evidence.fail_soft_ready",
                "search_projection_evidence.rebuild_marker_ready",
                "search_projection_evidence.metadata_repair_marker_ready",
                "search_projection_evidence.incremental_update_ready",
                "search_projection_evidence.source_chunk_ready",
                "search_projection_evidence.predicate_pushdown_ready",
                "search_projection_evidence.production_filter_pruning_ready",
                "search_projection_shadow_evidence.present",
                "search_projection_shadow_evidence.protocol",
                "search_projection_shadow_evidence.evidence_source",
                "search_projection_shadow_evidence.ready",
                "search_projection_shadow_evidence.primary_ready",
                "search_projection_shadow_evidence.shadow_ready",
                "search_projection_shadow_evidence.document_count_parity",
                "search_projection_shadow_evidence.document_identity_parity",
                "search_projection_shadow_evidence.table_parity_ready",
                "search_projection_shadow_evidence.embedding_identity_parity",
                "search_projection_shadow_evidence.lifecycle_parity",
                "search_projection_shadow_evidence.incremental_watermark_parity",
                "search_projection_shadow_evidence.pushdown_evidence.ready",
                "search_projection_shadow_evidence.pushdown_evidence.predicate_pushdown_parity",
                "search_projection_shadow_evidence.pushdown_evidence.shadow_persisted_segment_descriptor_ready",
                "search_projection_shadow_evidence.pushdown_evidence.shadow_segment_descriptor_scan_filter_fields_ready",
                "search_projection_shadow_evidence.pushdown_evidence.shadow_segment_document_pruning_ready",
                "search_projection_shadow_evidence.pushdown_evidence.shadow_segment_pruning_candidate_document_count",
                "search_projection_shadow_evidence.pushdown_evidence.shadow_segment_pruned_document_count",
                "search_projection_shadow_evidence.pushdown_evidence.shadow_segment_scanned_document_count",
                "search_projection_shadow_evidence.pushdown_evidence.shadow_segment_descriptor_field_summaries",
                "search_candidate_shadow_evidence.protocol",
                "search_candidate_shadow_evidence.evidence_source",
                "search_candidate_shadow_evidence.route",
                "search_candidate_shadow_evidence.present",
                "search_candidate_shadow_evidence.ready",
                "search_candidate_shadow_evidence.candidate_primary_engine",
                "search_candidate_shadow_evidence.candidate_counts",
                "search_candidate_shadow_evidence.text_retriever_ready",
                "search_candidate_shadow_evidence.vector_retriever_ready",
                "search_candidate_shadow_evidence.fts_top_k_overlap_ready",
                "search_candidate_shadow_evidence.vector_top_k_overlap_ready",
                "search_candidate_shadow_evidence.source_chunk_identity_ready",
                "search_candidate_shadow_evidence.fail_soft_observed",
                "search_candidate_shadow_evidence.projection_marker_status_visible",
                "search_candidate_shadow_evidence.projection_watermark_ready",
                "search_candidate_shadow_evidence.embedding_identity_ready",
                "search_candidate_shadow_evidence.candidate_identity_ready",
                "search_candidate_shadow_evidence.candidate_identity_parity",
                "search_candidate_shadow_evidence.filter_pushdown_ready",
                "search_candidate_shadow_evidence.filter_pushdown_field_summary_count",
                "search_candidate_shadow_evidence.filter_pushdown_missing_required_fields",
                "search_candidate_shadow_evidence.filter_pushdown_field_capabilities_ready",
                "search_candidate_shadow_evidence.filter_pushdown_missing_value_summary_fields",
                "search_candidate_shadow_evidence.filter_pushdown_missing_numeric_range_fields",
                "search_candidate_shadow_evidence.filter_pushdown_missing_timestamp_range_fields",
                "workload_fixture_evidence.protocol",
                "workload_fixture_evidence.present",
                "workload_fixture_evidence.ready",
                "workload_fixture_evidence.route_count",
                "workload_fixture_evidence.query_count",
                "workload_fixture_evidence.failed_query_count",
                "workload_fixture_evidence.bounded_expansion_probe_count",
                "workload_fixture_evidence.failed_bounded_expansion_probe_count",
                "workload_fixture_evidence.search_metadata_probe_count",
                "workload_fixture_evidence.failed_search_metadata_probe_count",
                "workload_fixture_evidence.source_projection_probe_count",
                "workload_fixture_evidence.failed_source_projection_probe_count",
                "workload_fixture_evidence.source_projection_reports"
            ])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary_graph_route")["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.graph_route_readiness.present",
                "replacement_summary.graph_route_readiness.ready",
                "replacement_summary.graph_route_readiness.evidence_ready",
                "replacement_summary.graph_route_readiness.route_coverage_ready",
                "replacement_summary.graph_route_readiness.evidence_route_coverage_matches",
                "replacement_summary.graph_route_readiness.route_query_runtime_ready",
                "replacement_summary.graph_route_readiness.route_query_plan_evidence_ready",
                "replacement_summary.graph_route_readiness.route_query_profile_evidence_ready",
                "replacement_summary.graph_route_readiness.route_query_api_behavior_evidence_ready",
                "replacement_summary.graph_route_readiness.route_relationship_property_pruning_evidence_ready",
                "replacement_summary.graph_route_readiness.route_primary_ready",
                "replacement_summary.graph_route_readiness.primary_ready_route_count"
            ])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary_bounded_read")["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.bounded_read_evidence.protocol",
                "replacement_summary.bounded_read_evidence.present",
                "replacement_summary.bounded_read_evidence.ready",
                "replacement_summary.bounded_read_evidence.mode",
                "replacement_summary.bounded_read_evidence.execution_row_cap",
                "replacement_summary.bounded_read_evidence.estimated_payload_bytes",
                "replacement_summary.bounded_read_evidence.max_estimated_payload_bytes",
                "replacement_summary.bounded_read_evidence.payload_budget_exceeded",
                "replacement_summary.bounded_read_evidence.row_limit_enforced_before_output",
                "replacement_summary.bounded_read_evidence.operator_row_cap_enabled",
                "replacement_summary.bounded_read_evidence.row_budget_exceeded",
                "replacement_summary.bounded_read_evidence.streaming",
                "replacement_summary.bounded_read_evidence.blocking_operator_count",
                "replacement_summary.bounded_read_evidence.missing_covered_routes",
                "replacement_summary.bounded_read_evidence.route_primary_ready",
                "replacement_summary.bounded_read_evidence.route_query_plan_evidence_ready",
                "replacement_summary.bounded_read_evidence.route_query_profile_evidence_ready",
                "replacement_summary.bounded_read_evidence.route_query_api_behavior_evidence_ready",
                "replacement_summary.bounded_read_evidence.route_relationship_property_pruning_evidence_ready",
                "replacement_summary.bounded_read_evidence.relationship_property_pruning"
            ])
        );
    }

    #[test]
    fn preflight_check_requires_full_contract_evidence() {
        let mut inputs = ready_inputs();
        let contract_evidence = inputs.contract_evidence.as_mut().unwrap();
        contract_evidence["full_contract_checked"] = serde_json::json!(false);
        contract_evidence["full_contract_ready"] = serde_json::json!(false);
        contract_evidence["selected_checks"] = serde_json::json!(1);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["full_contract"])
        );
        assert_eq!(
            check_by_name(&report, "full_contract")["failed_evidence_fields"],
            serde_json::json!([
                "full_contract_checked",
                "full_contract_ready",
                "selected_checks"
            ])
        );
    }

    #[test]
    fn preflight_check_requires_summary_dual_engine_evidence() {
        let mut inputs = ready_inputs();
        inputs.replacement_summary.as_mut().unwrap()["dual_engine_evidence"]["ready"] =
            serde_json::json!(false);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["replacement_summary"])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["failed_evidence_fields"],
            serde_json::json!(["dual_engine_evidence.ready"])
        );
    }

    #[test]
    fn preflight_check_requires_summary_search_projection_evidence() {
        let mut inputs = ready_inputs();
        let replacement_summary = inputs.replacement_summary.as_mut().unwrap();
        replacement_summary["search_projection_evidence"]["protocol"] =
            serde_json::json!("legacy-search-projection-evidence");
        replacement_summary["search_projection_evidence"]["ready"] = serde_json::json!(true);
        replacement_summary["search_projection_evidence"]["covered_table_count"] =
            serde_json::json!(5);
        replacement_summary["search_projection_evidence"]["document_identity_ready"] =
            serde_json::json!(false);
        replacement_summary["search_projection_evidence"]["source_chunk_ready"] =
            serde_json::json!(false);
        replacement_summary["search_projection_evidence"]["production_filter_pruning_ready"] =
            serde_json::json!(false);
        replacement_summary["search_projection_evidence"]["blocker_codes"] =
            serde_json::json!(["missing_source_chunks_index"]);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["replacement_summary"])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["failed_evidence_fields"],
            serde_json::json!([
                "search_projection_evidence.protocol",
                "search_projection_evidence.covered_table_count",
                "search_projection_evidence.document_identity_ready",
                "search_projection_evidence.source_chunk_ready",
                "search_projection_evidence.production_filter_pruning_ready"
            ])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["blocker_codes"],
            serde_json::json!(["missing_source_chunks_index"])
        );
    }

    #[test]
    fn preflight_check_requires_summary_search_projection_shadow_evidence() {
        let mut inputs = ready_inputs();
        let replacement_summary = inputs.replacement_summary.as_mut().unwrap();
        replacement_summary["search_projection_shadow_evidence"]["protocol"] =
            serde_json::json!("legacy-search-projection-shadow-evidence");
        replacement_summary["search_projection_shadow_evidence"]["evidence_source"] =
            serde_json::json!("cli-wrapper");
        replacement_summary["search_projection_shadow_evidence"]["ready"] = serde_json::json!(true);
        replacement_summary["search_projection_shadow_evidence"]["table_parity_ready"] =
            serde_json::json!(false);
        replacement_summary["search_projection_shadow_evidence"]["document_identity_parity"] =
            serde_json::json!(false);
        replacement_summary["search_projection_shadow_evidence"]["incremental_watermark_parity"] =
            serde_json::json!(false);
        replacement_summary["search_projection_shadow_evidence"]["pushdown_evidence"]["ready"] =
            serde_json::json!(false);
        replacement_summary["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["shadow_segment_descriptor_scan_filter_fields_ready"] = serde_json::json!(false);
        replacement_summary["search_projection_shadow_evidence"]["blocker_codes"] =
            serde_json::json!([
                "table_parity_mismatch",
                "incremental_watermark_mismatch",
                "skein_search_projection_segment_descriptor_fields_missing"
            ]);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["replacement_summary"])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["failed_evidence_fields"],
            serde_json::json!([
                "search_projection_shadow_evidence.protocol",
                "search_projection_shadow_evidence.evidence_source",
                "search_projection_shadow_evidence.document_identity_parity",
                "search_projection_shadow_evidence.table_parity_ready",
                "search_projection_shadow_evidence.incremental_watermark_parity",
                "search_projection_shadow_evidence.pushdown_evidence.ready",
                "search_projection_shadow_evidence.pushdown_evidence.shadow_segment_descriptor_scan_filter_fields_ready"
            ])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["blocker_codes"],
            serde_json::json!([
                "incremental_watermark_mismatch",
                "skein_search_projection_segment_descriptor_fields_missing",
                "table_parity_mismatch"
            ])
        );
    }

    #[test]
    fn preflight_check_requires_search_projection_shadow_descriptor_summaries() {
        let mut inputs = ready_inputs();
        let replacement_summary = inputs.replacement_summary.as_mut().unwrap();
        replacement_summary["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["shadow_segment_descriptor_field_summaries"] = serde_json::json!([]);
        replacement_summary["search_projection_shadow_evidence"]["blocker_codes"] =
            serde_json::json!(["skein_search_projection_segment_descriptor_fields_missing"]);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["replacement_summary"])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["failed_evidence_fields"],
            serde_json::json!([
                "search_projection_shadow_evidence.pushdown_evidence.shadow_segment_descriptor_field_summaries"
            ])
        );
        assert_eq!(
            report["release_summary"]
                ["search_projection_shadow_segment_descriptor_field_summaries_ready"],
            serde_json::json!(false)
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["blocker_codes"],
            serde_json::json!(["skein_search_projection_segment_descriptor_fields_missing"])
        );
    }

    #[test]
    fn preflight_check_requires_summary_search_candidate_shadow_evidence() {
        let mut inputs = ready_inputs();
        let replacement_summary = inputs.replacement_summary.as_mut().unwrap();
        replacement_summary["search_candidate_shadow_evidence"]["ready"] = serde_json::json!(true);
        replacement_summary["search_candidate_shadow_evidence"]["candidate_identity_ready"] =
            serde_json::json!(false);
        replacement_summary["search_candidate_shadow_evidence"]["filter_pushdown_ready"] =
            serde_json::json!(false);
        replacement_summary["search_candidate_shadow_evidence"]
            ["filter_pushdown_field_summary_count"] = serde_json::json!(0);
        replacement_summary["search_candidate_shadow_evidence"]
            ["filter_pushdown_missing_required_fields"] = serde_json::json!(["lifecycle_state"]);
        replacement_summary["search_candidate_shadow_evidence"]
            ["filter_pushdown_field_capabilities_ready"] = serde_json::json!(false);
        replacement_summary["search_candidate_shadow_evidence"]
            ["filter_pushdown_missing_value_summary_fields"] =
            serde_json::json!(["lifecycle_state"]);
        replacement_summary["search_candidate_shadow_evidence"]["blocker_codes"] =
            serde_json::json!([
                "search_candidate_field_pruning_missing",
                "search_candidate_field_pruning_capability_missing"
            ]);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["replacement_summary"])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["failed_evidence_fields"],
            serde_json::json!([
                "search_candidate_shadow_evidence.candidate_identity_ready",
                "search_candidate_shadow_evidence.filter_pushdown_ready",
                "search_candidate_shadow_evidence.filter_pushdown_field_summary_count",
                "search_candidate_shadow_evidence.filter_pushdown_missing_required_fields",
                "search_candidate_shadow_evidence.filter_pushdown_field_capabilities_ready",
                "search_candidate_shadow_evidence.filter_pushdown_missing_value_summary_fields"
            ])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["blocker_codes"],
            serde_json::json!([
                "search_candidate_field_pruning_capability_missing",
                "search_candidate_field_pruning_missing"
            ])
        );
    }

    #[test]
    fn preflight_check_requires_summary_search_candidate_top_k_overlap_evidence() {
        let mut inputs = ready_inputs();
        let replacement_summary = inputs.replacement_summary.as_mut().unwrap();
        replacement_summary["search_candidate_shadow_evidence"]["ready"] = serde_json::json!(true);
        replacement_summary["search_candidate_shadow_evidence"]["fts_top_k_overlap_ready"] =
            serde_json::json!(false);
        replacement_summary["search_candidate_shadow_evidence"]["vector_top_k_overlap_ready"] =
            serde_json::json!(false);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["replacement_summary"])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["failed_evidence_fields"],
            serde_json::json!([
                "search_candidate_shadow_evidence.fts_top_k_overlap_ready",
                "search_candidate_shadow_evidence.vector_top_k_overlap_ready"
            ])
        );
    }

    #[test]
    fn preflight_check_requires_summary_search_candidate_retriever_leg_evidence() {
        let mut inputs = ready_inputs();
        let replacement_summary = inputs.replacement_summary.as_mut().unwrap();
        replacement_summary["search_candidate_shadow_evidence"]["ready"] = serde_json::json!(true);
        replacement_summary["search_candidate_shadow_evidence"]["text_retriever_ready"] =
            serde_json::json!(false);
        replacement_summary["search_candidate_shadow_evidence"]["vector_retriever_ready"] =
            serde_json::json!(false);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["replacement_summary"])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["failed_evidence_fields"],
            serde_json::json!([
                "search_candidate_shadow_evidence.text_retriever_ready",
                "search_candidate_shadow_evidence.vector_retriever_ready"
            ])
        );
    }

    #[test]
    fn preflight_check_requires_summary_search_candidate_readiness_evidence() {
        let mut inputs = ready_inputs();
        let replacement_summary = inputs.replacement_summary.as_mut().unwrap();
        replacement_summary["search_candidate_shadow_evidence"]["ready"] = serde_json::json!(true);
        replacement_summary["search_candidate_shadow_evidence"]["source_chunk_identity_ready"] =
            serde_json::json!(false);
        replacement_summary["search_candidate_shadow_evidence"]["fail_soft_observed"] =
            serde_json::json!(false);
        replacement_summary["search_candidate_shadow_evidence"]
            ["projection_marker_status_visible"] = serde_json::json!(false);
        replacement_summary["search_candidate_shadow_evidence"]["projection_watermark_ready"] =
            serde_json::json!(false);
        replacement_summary["search_candidate_shadow_evidence"]["embedding_identity_ready"] =
            serde_json::json!(false);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["replacement_summary"])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["failed_evidence_fields"],
            serde_json::json!([
                "search_candidate_shadow_evidence.source_chunk_identity_ready",
                "search_candidate_shadow_evidence.fail_soft_observed",
                "search_candidate_shadow_evidence.projection_marker_status_visible",
                "search_candidate_shadow_evidence.projection_watermark_ready",
                "search_candidate_shadow_evidence.embedding_identity_ready"
            ])
        );
    }

    #[test]
    fn preflight_check_requires_summary_workload_fixture_evidence() {
        let mut inputs = ready_inputs();
        let replacement_summary = inputs.replacement_summary.as_mut().unwrap();
        replacement_summary["workload_fixture_evidence"]["ready"] = serde_json::json!(false);
        replacement_summary["workload_fixture_evidence"]["failed_query_count"] =
            serde_json::json!(1);
        replacement_summary["workload_fixture_evidence"]["blocker_codes"] =
            serde_json::json!(["workload_fixture_route_queries_not_ready"]);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["replacement_summary"])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["failed_evidence_fields"],
            serde_json::json!([
                "workload_fixture_evidence.ready",
                "workload_fixture_evidence.failed_query_count"
            ])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["blocker_codes"],
            serde_json::json!(["workload_fixture_route_queries_not_ready"])
        );
    }

    #[test]
    fn preflight_check_requires_storage_recovery_evidence() {
        let mut inputs = ready_inputs();
        let cutover_evidence = inputs
            .migration_gate
            .as_mut()
            .unwrap()
            .get_mut("cutover_evidence")
            .unwrap();
        cutover_evidence["storage_recovery_ready"] = serde_json::json!(false);
        cutover_evidence["storage_recovery_blocker_codes"] =
            serde_json::json!(["wal_replay_unbounded"]);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["storage_recovery"])
        );
        assert_eq!(
            check_by_name(&report, "storage_recovery")["blocker_codes"],
            serde_json::json!(["wal_replay_unbounded"])
        );
        assert_eq!(
            check_by_name(&report, "storage_recovery")["failed_evidence_fields"],
            serde_json::json!(["cutover_evidence.storage_recovery_ready"])
        );
    }

    #[test]
    fn preflight_check_recomputes_storage_recovery_raw_fields() {
        let mut inputs = ready_inputs();
        let cutover_evidence = inputs
            .migration_gate
            .as_mut()
            .unwrap()
            .get_mut("cutover_evidence")
            .unwrap();
        cutover_evidence["storage_recovery_ready"] = serde_json::json!(true);
        cutover_evidence["storage_recovery_durable"] = serde_json::json!(false);
        cutover_evidence["storage_recovery_checkpoint_boundary_present"] = serde_json::json!(false);
        cutover_evidence["storage_recovery_replay_boundary_consistent"] = serde_json::json!(false);
        cutover_evidence["storage_recovery_torn_tail_clean"] = serde_json::json!(false);
        cutover_evidence["storage_recovery_blocker_codes"] = serde_json::json!([
            "durable_recovery_not_observed",
            "checkpoint_boundary_missing",
            "replay_boundary_inconsistent",
            "torn_tail_observed"
        ]);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["storage_recovery"])
        );
        assert_eq!(
            check_by_name(&report, "storage_recovery")["failed_evidence_fields"],
            serde_json::json!([
                "cutover_evidence.storage_recovery_durable",
                "cutover_evidence.storage_recovery_checkpoint_boundary_present",
                "cutover_evidence.storage_recovery_replay_boundary_consistent",
                "cutover_evidence.storage_recovery_torn_tail_clean"
            ])
        );
        assert_eq!(
            check_by_name(&report, "storage_recovery")["blocker_codes"],
            serde_json::json!([
                "checkpoint_boundary_missing",
                "durable_recovery_not_observed",
                "replay_boundary_inconsistent",
                "torn_tail_observed"
            ])
        );
    }

    #[test]
    fn preflight_check_requires_background_maintenance_evidence() {
        let mut inputs = ready_inputs();
        let cutover_evidence = inputs
            .migration_gate
            .as_mut()
            .unwrap()
            .get_mut("cutover_evidence")
            .unwrap();
        cutover_evidence["background_maintenance_present"] = serde_json::json!(false);
        cutover_evidence["background_maintenance_ready"] = serde_json::json!(false);
        cutover_evidence["background_maintenance_blocker_codes"] =
            serde_json::json!(["missing_evidence"]);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["background_maintenance"])
        );
        assert_eq!(
            check_by_name(&report, "background_maintenance")["blocker_codes"],
            serde_json::json!(["missing_evidence"])
        );
    }

    #[test]
    fn preflight_check_requires_background_maintenance_qos_evidence() {
        let mut inputs = ready_inputs();
        let cutover_evidence = inputs
            .replacement_summary
            .as_mut()
            .unwrap()
            .get_mut("cutover_evidence")
            .unwrap();
        cutover_evidence["background_maintenance_deferred_search_projection_graph_delta_count"] =
            serde_json::Value::Null;
        cutover_evidence["background_maintenance_memory_pressure_ready"] = serde_json::json!(false);
        cutover_evidence["background_maintenance_memory_budget_bytes"] = serde_json::Value::Null;
        cutover_evidence["background_maintenance_blocker_codes"] =
            serde_json::json!(["memory_budget_exceeded"]);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["background_maintenance"])
        );
        assert_eq!(
            check_by_name(&report, "background_maintenance")["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.cutover_evidence.background_maintenance_graph_delta_qos",
                "replacement_summary.cutover_evidence.background_maintenance_memory_pressure_ready",
                "replacement_summary.cutover_evidence.background_maintenance_memory_budget_bytes"
            ])
        );
        assert_eq!(
            check_by_name(&report, "background_maintenance")["blocker_codes"],
            serde_json::json!(["memory_budget_exceeded"])
        );
    }

    #[test]
    fn preflight_check_requires_library_readiness() {
        let mut inputs = ready_inputs();
        let library_readiness = inputs.library_readiness.as_mut().unwrap();
        library_readiness["ready"] = serde_json::json!(false);
        library_readiness["blocked_area_count"] = serde_json::json!(1);
        library_readiness["blocker_codes"] =
            serde_json::json!(["search_projection_evidence_not_ready"]);
        library_readiness["readiness_by_area"]["search_projection"]["ready"] =
            serde_json::json!(false);
        library_readiness["readiness_by_area"]["search_projection"]["blocker_codes"] =
            serde_json::json!(["search_projection_probe_missing"]);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["library_readiness"])
        );
        assert_eq!(
            check_by_name(&report, "library_readiness")["failed_evidence_fields"],
            serde_json::json!([
                "library_readiness.ready",
                "library_readiness.blocked_area_count",
                "library_readiness.readiness_by_area.search_projection.ready"
            ])
        );
        assert_eq!(
            check_by_name(&report, "library_readiness")["blocker_codes"],
            serde_json::json!([
                "search_projection_evidence_not_ready",
                "search_projection_probe_missing"
            ])
        );
    }

    #[test]
    fn preflight_check_requires_query_runtime_preflight_redaction() {
        let mut inputs = ready_inputs();
        let query_runtime_preflight = inputs.query_runtime_preflight.as_mut().unwrap();
        query_runtime_preflight["redaction"]["ready"] = serde_json::json!(false);
        query_runtime_preflight["redaction"]["rows_copied"] = serde_json::json!(true);
        query_runtime_preflight["redaction"]["parameters_copied"] = serde_json::json!(true);
        query_runtime_preflight["redaction"]["local_paths_copied"] = serde_json::json!(true);
        query_runtime_preflight["redaction"]["raw_errors_copied"] = serde_json::json!(true);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["query_runtime_preflight"])
        );
        assert_eq!(
            check_by_name(&report, "query_runtime_preflight")["failed_evidence_fields"],
            serde_json::json!([
                "query_runtime_preflight.redaction.ready",
                "query_runtime_preflight.redaction.rows_copied",
                "query_runtime_preflight.redaction.parameters_copied",
                "query_runtime_preflight.redaction.local_paths_copied",
                "query_runtime_preflight.redaction.raw_errors_copied"
            ])
        );
        assert_release_summary_field(
            &report["release_summary"],
            "query_runtime_preflight_redaction_ready",
            false,
        );
        assert_release_summary_field(
            &report["release_summary"],
            "query_runtime_preflight_raw_errors_copied",
            true,
        );
    }

    #[test]
    fn preflight_check_requires_library_readiness_redaction() {
        let mut inputs = ready_inputs();
        let library_readiness = inputs.library_readiness.as_mut().unwrap();
        library_readiness["redaction"]["ready"] = serde_json::json!(false);
        library_readiness["redaction"]["query_text_copied"] = serde_json::json!(true);
        library_readiness["redaction"]["parameters_copied"] = serde_json::json!(true);
        library_readiness["redaction"]["local_paths_copied"] = serde_json::json!(true);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["library_readiness"])
        );
        assert_eq!(
            check_by_name(&report, "library_readiness")["failed_evidence_fields"],
            serde_json::json!([
                "library_readiness.redaction.ready",
                "library_readiness.redaction.query_text_copied",
                "library_readiness.redaction.parameters_copied",
                "library_readiness.redaction.local_paths_copied"
            ])
        );
        assert_release_summary_field(
            &report["release_summary"],
            "library_readiness_redaction_ready",
            false,
        );
        assert_release_summary_field(
            &report["release_summary"],
            "library_readiness_query_text_copied",
            true,
        );
    }

    #[test]
    fn preflight_check_requires_library_graph_route_readiness_area() {
        let mut inputs = ready_inputs();
        let library_readiness = inputs.library_readiness.as_mut().unwrap();
        library_readiness["ready"] = serde_json::json!(false);
        library_readiness["ready_area_count"] = serde_json::json!(8);
        library_readiness["blocked_area_count"] = serde_json::json!(1);
        library_readiness["blocker_codes"] = serde_json::json!(["graph_route_readiness_not_ready"]);
        library_readiness["readiness_by_area"]["graph_route"]["ready"] = serde_json::json!(false);
        library_readiness["readiness_by_area"]["graph_route"]["blocker_codes"] =
            serde_json::json!(["graph_route_readiness_missing"]);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["library_readiness"])
        );
        assert_eq!(
            check_by_name(&report, "library_readiness")["failed_evidence_fields"],
            serde_json::json!([
                "library_readiness.ready",
                "library_readiness.blocked_area_count",
                "library_readiness.readiness_by_area.graph_route.ready"
            ])
        );
        assert_eq!(
            check_by_name(&report, "library_readiness")["blocker_codes"],
            serde_json::json!([
                "graph_route_readiness_missing",
                "graph_route_readiness_not_ready"
            ])
        );
    }

    #[test]
    fn preflight_check_requires_library_search_route_ownership_area() {
        let mut inputs = ready_inputs();
        let library_readiness = inputs.library_readiness.as_mut().unwrap();
        library_readiness["ready"] = serde_json::json!(false);
        library_readiness["ready_area_count"] = serde_json::json!(10);
        library_readiness["blocked_area_count"] = serde_json::json!(1);
        library_readiness["blocker_codes"] =
            serde_json::json!(["search_route_ownership_not_ready"]);
        library_readiness["readiness_by_area"]["search_route_ownership"]["ready"] =
            serde_json::json!(false);
        library_readiness["readiness_by_area"]["search_route_ownership"]["blocker_codes"] =
            serde_json::json!(["search_routes_still_lancedb"]);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["library_readiness"])
        );
        assert_eq!(
            check_by_name(&report, "library_readiness")["failed_evidence_fields"],
            serde_json::json!([
                "library_readiness.ready",
                "library_readiness.blocked_area_count",
                "library_readiness.readiness_by_area.search_route_ownership.ready"
            ])
        );
        assert_eq!(
            check_by_name(&report, "library_readiness")["blocker_codes"],
            serde_json::json!([
                "search_route_ownership_not_ready",
                "search_routes_still_lancedb"
            ])
        );
    }

    #[test]
    fn preflight_check_requires_library_workload_fixture_readiness_area() {
        let mut inputs = ready_inputs();
        let library_readiness = inputs.library_readiness.as_mut().unwrap();
        library_readiness["ready"] = serde_json::json!(false);
        library_readiness["ready_area_count"] = serde_json::json!(10);
        library_readiness["blocked_area_count"] = serde_json::json!(1);
        library_readiness["blocker_codes"] =
            serde_json::json!(["workload_fixture_evidence_not_ready"]);
        library_readiness["readiness_by_area"]["workload_fixture"]["ready"] =
            serde_json::json!(false);
        library_readiness["readiness_by_area"]["workload_fixture"]["blocker_codes"] =
            serde_json::json!(["workload_fixture_bounded_expansion_not_ready"]);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["library_readiness"])
        );
        assert_eq!(
            check_by_name(&report, "library_readiness")["failed_evidence_fields"],
            serde_json::json!([
                "library_readiness.ready",
                "library_readiness.blocked_area_count",
                "library_readiness.readiness_by_area.workload_fixture.ready"
            ])
        );
        assert_eq!(
            check_by_name(&report, "library_readiness")["blocker_codes"],
            serde_json::json!([
                "workload_fixture_bounded_expansion_not_ready",
                "workload_fixture_evidence_not_ready"
            ])
        );
    }

    #[test]
    fn preflight_check_requires_library_graph_rag_workload_evidence() {
        let mut inputs = ready_inputs();
        let library_readiness = inputs.library_readiness.as_mut().unwrap();
        library_readiness["workload_fixture_evidence"]["graph_rag_probe_count"] =
            serde_json::json!(1);
        library_readiness["workload_fixture_evidence"]["failed_graph_rag_probe_count"] =
            serde_json::json!(1);
        library_readiness["workload_fixture_evidence"]["graph_rag_reports"] = serde_json::json!([
            {
                "name": "memory-to-entity",
                "ready": false,
                "schema_protocol": GRAPH_RAG_SCHEMA_CONTEXT_PROTOCOL,
                "label_count": 2,
                "relationship_type_count": 1,
                "route_count": 1,
                "parameter_requirement_count": 1,
                "row_count": 1,
                "row_budget_exceeded": false,
                "payload_budget_exceeded": false,
                "blocking_operator_count": 0,
                "streaming": false,
                "error_class": "execution"
            }
        ]);
        library_readiness["workload_fixture_evidence"]["blocker_codes"] =
            serde_json::json!(["workload_fixture_graph_rag_not_ready"]);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["library_readiness"])
        );
        assert_eq!(
            check_by_name(&report, "library_readiness")["failed_evidence_fields"],
            serde_json::json!(["library_readiness.workload_fixture_evidence.graph_rag_reports"])
        );
        assert_eq!(
            check_by_name(&report, "library_readiness")["blocker_codes"],
            serde_json::json!(["workload_fixture_graph_rag_not_ready"])
        );
    }

    #[test]
    fn preflight_check_requires_library_source_projection_workload_evidence() {
        let mut inputs = ready_inputs();
        let library_readiness = inputs.library_readiness.as_mut().unwrap();
        library_readiness["workload_fixture_evidence"]["source_projection_probe_count"] =
            serde_json::json!(1);
        library_readiness["workload_fixture_evidence"]["failed_source_projection_probe_count"] =
            serde_json::json!(1);
        library_readiness["workload_fixture_evidence"]["source_projection_reports"] = serde_json::json!([
            {
                "name": "source-ingest-composite-changefeed",
                "ready": false,
                "source_graph_commit_epoch": 43,
                "complete_through_graph_commit_epoch": 43,
                "too_small_batch_failed_closed": false,
                "operation_count": 1,
                "upserted_documents": 1,
                "deleted_documents": 0,
                "source_document_count": 1,
                "indexed_source_document_ready": false,
                "error_class": "batch_split"
            }
        ]);
        library_readiness["workload_fixture_evidence"]["blocker_codes"] =
            serde_json::json!(["workload_fixture_source_projection_not_ready"]);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["library_readiness"])
        );
        assert_eq!(
            check_by_name(&report, "library_readiness")["failed_evidence_fields"],
            serde_json::json!([
                "library_readiness.workload_fixture_evidence.source_projection_reports"
            ])
        );
        assert_eq!(
            check_by_name(&report, "library_readiness")["blocker_codes"],
            serde_json::json!(["workload_fixture_source_projection_not_ready"])
        );
        assert_release_summary_field(
            &report["release_summary"],
            "library_readiness_workload_fixture_source_projection_ready",
            false,
        );
    }

    #[test]
    fn preflight_check_requires_effective_cutover_controls() {
        let mut inputs = ready_inputs();
        let cutover_controls = inputs.cutover_controls.as_mut().unwrap();
        cutover_controls["ready"] = serde_json::json!(false);
        cutover_controls["graph"]["read_effective"] = serde_json::json!(false);
        cutover_controls["production_status"]["graph"]["skein_cutover_effective"] =
            serde_json::json!(false);
        cutover_controls["blocker_codes"] =
            serde_json::json!(["graph_read_selected_skein_but_not_effective"]);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["cutover_controls"])
        );
        assert_eq!(
            check_by_name(&report, "cutover_controls")["failed_evidence_fields"],
            serde_json::json!([
                "cutover_controls.ready",
                "cutover_controls.graph.read_effective",
                "cutover_controls.production_status.graph.skein_cutover_effective"
            ])
        );
        assert_eq!(
            check_by_name(&report, "cutover_controls")["blocker_codes"],
            serde_json::json!(["graph_read_selected_skein_but_not_effective"])
        );
        assert_release_summary_field(
            &report["release_summary"],
            "cutover_controls_graph_read_effective",
            false,
        );
    }

    #[test]
    fn preflight_check_blocks_active_initial_import_for_read_cutover() {
        let mut inputs = ready_inputs();
        let cutover_controls = inputs.cutover_controls.as_mut().unwrap();
        cutover_controls["ready"] = serde_json::json!(false);
        cutover_controls["controls"]["initial_import"] = serde_json::json!("enabled");
        cutover_controls["work"]["initial_import_enabled"] = serde_json::json!(true);
        cutover_controls["work"]["initial_import_inactive_for_cutover"] = serde_json::json!(false);
        cutover_controls["blocker_codes"] =
            serde_json::json!(["initial_import_active_blocks_read_cutover"]);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["cutover_controls"])
        );
        assert_eq!(
            check_by_name(&report, "cutover_controls")["failed_evidence_fields"],
            serde_json::json!([
                "cutover_controls.ready",
                "cutover_controls.work.initial_import_safe_for_read_cutover"
            ])
        );
        assert_eq!(
            check_by_name(&report, "cutover_controls")["blocker_codes"],
            serde_json::json!(["initial_import_active_blocks_read_cutover"])
        );
        assert_release_summary_field(
            &report["release_summary"],
            "cutover_controls_initial_import_inactive_for_cutover",
            false,
        );
    }

    #[test]
    fn preflight_check_accepts_active_initial_import_with_cutover_catch_up_proof() {
        let mut inputs = ready_inputs();
        let cutover_controls = inputs.cutover_controls.as_mut().unwrap();
        cutover_controls["controls"]["initial_import"] = serde_json::json!("enabled");
        cutover_controls["work"]["initial_import_enabled"] = serde_json::json!(true);
        cutover_controls["work"]["initial_import_inactive_for_cutover"] = serde_json::json!(false);
        cutover_controls["work"]["initial_import_cutover_catch_up_ready"] = serde_json::json!(true);
        cutover_controls["work"]["initial_import_safe_for_read_cutover"] = serde_json::json!(true);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], true);
        assert_release_summary_field(
            &report["release_summary"],
            "cutover_controls_initial_import_inactive_for_cutover",
            false,
        );
        assert_release_summary_field(
            &report["release_summary"],
            "cutover_controls_initial_import_cutover_catch_up_ready",
            true,
        );
        assert_release_summary_field(
            &report["release_summary"],
            "cutover_controls_initial_import_safe_for_read_cutover",
            true,
        );
    }

    #[test]
    fn preflight_check_requires_operations_readiness() {
        let mut inputs = ready_inputs();
        let operations_readiness = inputs.operations_readiness.as_mut().unwrap();
        operations_readiness["ready"] = serde_json::json!(false);
        operations_readiness["search_projection"]["stale"] = serde_json::json!(true);
        operations_readiness["storage_lifecycle"]["action"] = serde_json::json!("repair_wal_tail");
        operations_readiness["readiness"]["storage_recovery_ready"] = serde_json::json!(false);
        operations_readiness["blocker_codes"] =
            serde_json::json!(["search_projection_stale", "storage_recovery_not_ready"]);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["operations_readiness"])
        );
        assert_eq!(
            check_by_name(&report, "operations_readiness")["failed_evidence_fields"],
            serde_json::json!([
                "operations_readiness.ready",
                "operations_readiness.search_projection.stale",
                "operations_readiness.storage_lifecycle.action",
                "operations_readiness.readiness.storage_recovery_ready"
            ])
        );
        assert_eq!(
            check_by_name(&report, "operations_readiness")["blocker_codes"],
            serde_json::json!(["search_projection_stale", "storage_recovery_not_ready"])
        );
        assert_release_summary_field(
            &report["release_summary"],
            "operations_readiness_search_projection_stale",
            true,
        );
    }

    #[test]
    fn preflight_check_requires_matching_wrapper_identity() {
        let mut inputs = ready_inputs();
        inputs.wrapper_identity = Some("nowledge-previous-wrapper:other".to_string());

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!([
                "full_contract",
                "adapter_smoke",
                "migration_gate",
                "replacement_summary"
            ])
        );
        assert_eq!(
            check_by_name(&report, "full_contract")["failed_evidence_fields"],
            serde_json::json!(["previous_wrapper_contract_evidence.wrapper_identity"])
        );
        assert_eq!(
            check_by_name(&report, "adapter_smoke")["failed_evidence_fields"],
            serde_json::json!(["wrapper_identity"])
        );
        assert_eq!(
            check_by_name(&report, "migration_gate")["failed_evidence_fields"],
            serde_json::json!(["cutover_evidence.ready_wrapper_identity"])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["failed_evidence_fields"],
            serde_json::json!([
                "shadow_evidence.ready_wrapper_identity",
                "shadow_evidence.contract_wrapper_identity",
                "shadow_evidence.cutover_ready_wrapper_identity"
            ])
        );
    }

    #[test]
    fn preflight_check_requires_ready_dual_engine_evidence_when_present() {
        let mut inputs = ready_inputs();
        let adapter_smoke = inputs.adapter_smoke.as_mut().unwrap();
        adapter_smoke["dual_engine_evidence"]["ready"] = serde_json::json!(false);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["adapter_smoke"])
        );
        assert_eq!(
            check_by_name(&report, "adapter_smoke")["failed_evidence_fields"],
            serde_json::json!(["dual_engine_evidence.ready"])
        );
    }

    #[test]
    fn preflight_check_requires_adapter_dual_engine_counts_to_match() {
        let mut inputs = ready_inputs();
        let adapter_smoke = inputs.adapter_smoke.as_mut().unwrap();
        adapter_smoke["dual_engine_evidence"]["ready"] = serde_json::json!(true);
        adapter_smoke["dual_engine_evidence"]["matched_check_count"] = serde_json::json!(1);
        adapter_smoke["dual_engine_evidence"]["primary_only_check_count"] = serde_json::json!(1);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["adapter_smoke"])
        );
        assert_eq!(
            check_by_name(&report, "adapter_smoke")["failed_evidence_fields"],
            serde_json::json!([
                "dual_engine_evidence.matched_check_count",
                "dual_engine_evidence.primary_only_check_count"
            ])
        );
    }

    #[test]
    fn preflight_check_requires_adapter_dual_engine_evidence() {
        let mut inputs = ready_inputs();
        inputs
            .adapter_smoke
            .as_mut()
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove("dual_engine_evidence");

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["adapter_smoke"])
        );
        assert_eq!(
            check_by_name(&report, "adapter_smoke")["failed_evidence_fields"],
            serde_json::json!([
                "dual_engine_evidence.ready",
                "dual_engine_evidence.primary_check_count",
                "dual_engine_evidence.shadow_check_count",
                "dual_engine_evidence.matched_check_count",
                "dual_engine_evidence.primary_only_check_count"
            ])
        );
    }

    #[test]
    fn preflight_check_requires_query_runtime_preflight() {
        let mut inputs = ready_inputs();
        let preflight = inputs.query_runtime_preflight.as_mut().unwrap();
        preflight["ready"] = serde_json::json!(false);
        preflight["failed_probe_count"] = serde_json::json!(1);
        preflight["blocker_codes"] = serde_json::json!(["query_runtime_probe_failed"]);
        preflight["probes"][0]["selected_plan_fingerprint"] = serde_json::json!("");

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["query_runtime_preflight"])
        );
        assert_eq!(
            check_by_name(&report, "query_runtime_preflight")["failed_evidence_fields"],
            serde_json::json!([
                "query_runtime_preflight.ready",
                "query_runtime_preflight.passed_probe_count",
                "query_runtime_preflight.failed_probe_count",
                "query_runtime_preflight.probes"
            ])
        );
        assert_eq!(
            check_by_name(&report, "query_runtime_preflight")["blocker_codes"],
            serde_json::json!(["query_runtime_probe_failed"])
        );
    }

    #[test]
    fn preflight_check_rejects_weak_query_runtime_probe_profile() {
        let mut inputs = ready_inputs();
        let preflight = inputs.query_runtime_preflight.as_mut().unwrap();
        preflight["probes"][0]["execution_profile"]
            .as_object_mut()
            .unwrap()
            .remove("scan_pruning_reports");

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["query_runtime_preflight"])
        );
        assert_eq!(
            check_by_name(&report, "query_runtime_preflight")["failed_evidence_fields"],
            serde_json::json!(["query_runtime_preflight.probes"])
        );
    }

    #[test]
    fn preflight_check_rejects_query_runtime_preflight_without_route_coverage() {
        let mut inputs = ready_inputs();
        let preflight = inputs.query_runtime_preflight.as_mut().unwrap();
        preflight["probes"].as_array_mut().unwrap().pop();

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["query_runtime_preflight"])
        );
        assert_eq!(
            check_by_name(&report, "query_runtime_preflight")["failed_evidence_fields"],
            serde_json::json!(["query_runtime_preflight.route_coverage"])
        );
    }

    #[test]
    fn preflight_check_rejects_stale_query_runtime_route_catalog() {
        let mut inputs = ready_inputs();
        let preflight = inputs.query_runtime_preflight.as_mut().unwrap();
        preflight["route_catalog_digest"] = serde_json::json!("fnv1a64:stale");

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["query_runtime_preflight"])
        );
        assert_eq!(
            check_by_name(&report, "query_runtime_preflight")["failed_evidence_fields"],
            serde_json::json!([
                "query_runtime_preflight.route_coverage",
                "query_runtime_preflight.route_catalog_digest"
            ])
        );
    }

    #[test]
    fn preflight_check_rejects_stale_replacement_summary_route_catalog() {
        let mut inputs = ready_inputs();
        let replacement_summary = inputs.replacement_summary.as_mut().unwrap();
        replacement_summary["graph_route_readiness"]["route_catalog_digest"] =
            serde_json::json!("fnv1a64:stale");

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["replacement_summary_route_catalog"])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary_route_catalog")["failed_evidence_fields"],
            serde_json::json!(["replacement_summary.graph_route_readiness.route_catalog"])
        );
    }

    #[test]
    fn preflight_check_requires_graph_route_api_behavior_evidence() {
        let mut inputs = ready_inputs();
        let replacement_summary = inputs.replacement_summary.as_mut().unwrap();
        replacement_summary["graph_route_readiness"]
            .as_object_mut()
            .unwrap()
            .remove("route_query_api_behavior_evidence_ready");
        replacement_summary["graph_route_readiness"]["blocker_codes"] =
            serde_json::json!(["route_query_api_behavior_evidence_not_ready"]);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["replacement_summary_graph_route"])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary_graph_route")["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.graph_route_readiness.route_query_api_behavior_evidence_ready"
            ])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary_graph_route")["blocker_codes"],
            serde_json::json!(["route_query_api_behavior_evidence_not_ready"])
        );
    }

    #[test]
    fn preflight_check_rejects_bounded_read_payload_budget_overrun() {
        let mut inputs = ready_inputs();
        let replacement_summary = inputs.replacement_summary.as_mut().unwrap();
        replacement_summary["bounded_read_evidence"]["payload_budget_exceeded"] =
            serde_json::json!(true);
        replacement_summary["bounded_read_evidence"]["blocker_codes"] =
            serde_json::json!(["bounded_read_payload_budget_exceeded"]);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["replacement_summary_bounded_read"])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary_bounded_read")["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.bounded_read_evidence.payload_budget_exceeded"
            ])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary_bounded_read")["blocker_codes"],
            serde_json::json!(["bounded_read_payload_budget_exceeded"])
        );
        assert_eq!(
            report["release_summary"]["bounded_read_payload_budget_exceeded"],
            serde_json::json!(true)
        );
    }

    #[test]
    fn preflight_check_requires_bounded_read_api_behavior_evidence() {
        let mut inputs = ready_inputs();
        let replacement_summary = inputs.replacement_summary.as_mut().unwrap();
        replacement_summary["bounded_read_evidence"]
            .as_object_mut()
            .unwrap()
            .remove("route_query_api_behavior_evidence_ready");
        replacement_summary["bounded_read_evidence"]["blocker_codes"] =
            serde_json::json!(["bounded_read_graph_route_readiness_not_ready"]);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["replacement_summary_bounded_read"])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary_bounded_read")["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.bounded_read_evidence.route_query_api_behavior_evidence_ready"
            ])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary_bounded_read")["blocker_codes"],
            serde_json::json!(["bounded_read_graph_route_readiness_not_ready"])
        );
    }

    #[test]
    fn preflight_check_rejects_query_runtime_preflight_with_unknown_route() {
        let mut inputs = ready_inputs();
        let preflight = inputs.query_runtime_preflight.as_mut().unwrap();
        let mut probe = preflight["probes"][0].clone();
        probe["route"] = serde_json::json!("/graph/stale-route");
        preflight["probes"].as_array_mut().unwrap().push(probe);
        preflight["unknown_routes"] = serde_json::json!(["/graph/stale-route"]);
        preflight["route_coverage_ready"] = serde_json::json!(false);
        preflight["route_coverage_blocker_codes"] =
            serde_json::json!(["query_runtime_unknown_routes"]);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["query_runtime_preflight"])
        );
        assert_eq!(
            check_by_name(&report, "query_runtime_preflight")["failed_evidence_fields"],
            serde_json::json!(["query_runtime_preflight.route_coverage"])
        );
        assert!(
            check_by_name(&report, "query_runtime_preflight")["blocker_codes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|code| code == "query_runtime_unknown_routes")
        );
    }

    #[test]
    fn preflight_check_rejects_query_runtime_preflight_with_duplicate_route() {
        let mut inputs = ready_inputs();
        let preflight = inputs.query_runtime_preflight.as_mut().unwrap();
        let probe = preflight["probes"][0].clone();
        preflight["probes"].as_array_mut().unwrap().push(probe);
        preflight["duplicate_routes"] =
            serde_json::json!([REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES[0]]);
        preflight["route_coverage_ready"] = serde_json::json!(false);
        preflight["route_coverage_blocker_codes"] =
            serde_json::json!(["query_runtime_duplicate_routes"]);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["query_runtime_preflight"])
        );
        assert_eq!(
            check_by_name(&report, "query_runtime_preflight")["failed_evidence_fields"],
            serde_json::json!(["query_runtime_preflight.route_coverage"])
        );
        assert!(
            check_by_name(&report, "query_runtime_preflight")["blocker_codes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|code| code == "query_runtime_duplicate_routes")
        );
    }

    #[test]
    fn preflight_check_requires_replacement_dual_engine_consistency() {
        let mut inputs = ready_inputs();
        let replacement_summary = inputs.replacement_summary.as_mut().unwrap();
        replacement_summary["dual_engine_evidence"]["ready"] = serde_json::json!(true);
        replacement_summary["dual_engine_evidence"]["consistent"] = serde_json::json!(false);
        replacement_summary["dual_engine_evidence"]["matched_check_count"] = serde_json::json!(1);
        replacement_summary["dual_engine_evidence"]["primary_only_check_count"] =
            serde_json::json!(1);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["replacement_summary"])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["failed_evidence_fields"],
            serde_json::json!([
                "dual_engine_evidence.consistent",
                "dual_engine_evidence.matched_check_count",
                "dual_engine_evidence.primary_only_check_count"
            ])
        );
    }

    #[test]
    fn preflight_check_requires_replacement_shadow_provenance() {
        let mut inputs = ready_inputs();
        let replacement_summary = inputs.replacement_summary.as_mut().unwrap();
        replacement_summary["shadow_evidence"]["ready"] = serde_json::json!(false);
        replacement_summary["shadow_evidence"]["ready_wrapper_identity"] =
            serde_json::json!(serde_json::Value::Null);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["replacement_summary"])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["failed_evidence_fields"],
            serde_json::json!([
                "shadow_evidence.ready",
                "shadow_evidence.ready_wrapper_identity"
            ])
        );
    }

    #[test]
    fn preflight_check_can_load_standard_bundle_dir() {
        let inputs = ready_inputs();
        let bundle_dir = unique_test_dir("previous-wrapper-preflight-bundle");
        std::fs::create_dir_all(&bundle_dir).unwrap();
        write_json(
            bundle_dir.join("contract-evidence.json"),
            inputs.contract_evidence.as_ref().unwrap(),
        );
        write_json(
            bundle_dir.join("adapter-smoke.json"),
            inputs.adapter_smoke.as_ref().unwrap(),
        );
        write_json(
            bundle_dir.join("migration-gate.json"),
            inputs.migration_gate.as_ref().unwrap(),
        );
        write_json(
            bundle_dir.join("replacement-summary.json"),
            inputs.replacement_summary.as_ref().unwrap(),
        );
        write_json(
            bundle_dir.join("query-runtime-preflight.json"),
            inputs.query_runtime_preflight.as_ref().unwrap(),
        );
        write_json(
            bundle_dir.join("library-readiness.json"),
            inputs.library_readiness.as_ref().unwrap(),
        );
        write_json(
            bundle_dir.join("cutover-controls.json"),
            inputs.cutover_controls.as_ref().unwrap(),
        );
        write_json(
            bundle_dir.join("operations-readiness.json"),
            inputs.operations_readiness.as_ref().unwrap(),
        );

        let (report, require_ready) = run_nowledge_previous_wrapper_preflight_check(
            vec![
                "--require-ready".to_string(),
                "--wrapper-identity".to_string(),
                "nowledge-previous-wrapper:test".to_string(),
                "--bundle-dir".to_string(),
                bundle_dir.to_string_lossy().into_owned(),
            ]
            .into_iter(),
        )
        .unwrap();

        assert!(require_ready);
        assert_eq!(report["ready"], true);
        assert_eq!(report["failed_checks"], serde_json::json!([]));
        std::fs::remove_dir_all(bundle_dir).unwrap();
    }

    #[test]
    fn preflight_json_input_read_errors_are_redacted_by_default() {
        let secret_path = unique_test_dir("previous-wrapper-secret-token-do-not-emit")
            .join("missing-secret-file.json");

        let error = super::read_json_file(&secret_path).unwrap_err().to_string();

        assert_eq!(
            error,
            "execution error: failed to read previous-wrapper preflight JSON: io_error"
        );
        assert!(!error.contains("secret-token-do-not-emit"));
        assert!(!error.contains("missing-secret-file"));
    }

    #[test]
    fn preflight_json_input_parse_errors_are_redacted_by_default() {
        let root = unique_test_dir("previous-wrapper-parse-redaction");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("secret-json-path-do-not-emit.json");
        std::fs::write(
            &path,
            "{ \"secret\": \"parse-secret-do-not-emit\", \"unterminated\": ",
        )
        .unwrap();

        let error = super::read_json_file(&path).unwrap_err().to_string();

        assert_eq!(
            error,
            "execution error: failed to parse previous-wrapper preflight JSON: invalid_json"
        );
        assert!(!error.contains("secret-json-path-do-not-emit"));
        assert!(!error.contains("parse-secret-do-not-emit"));
        std::fs::remove_dir_all(root).unwrap();
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein-{name}-{nanos}"))
    }

    fn write_json(path: PathBuf, value: &serde_json::Value) {
        std::fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
    }

    fn assert_release_summary_field<T: serde::Serialize>(
        summary: &serde_json::Value,
        field: &str,
        expected: T,
    ) {
        assert_eq!(summary[field], serde_json::json!(expected));
    }

    fn ready_inputs() -> PreviousWrapperPreflightCheckInputs {
        PreviousWrapperPreflightCheckInputs {
            wrapper_identity: Some("nowledge-previous-wrapper:test".to_string()),
            bundle_dir: None,
            contract_evidence: Some(serde_json::json!({
                "required_contract_ready": true,
                "full_contract_checked": true,
                "full_contract_ready": true,
                "selected_checks": 2,
                "check_count": 2,
                "previous_wrapper_contract_evidence": {
                    "ready": true,
                    "wrapper_identity": "nowledge-previous-wrapper:test",
                    "blocker_codes": []
                },
                "required_contract_blocker_codes": []
            })),
            adapter_smoke: Some(serde_json::json!({
                "adapter_smoke_ready": true,
                "engine_kind": "previous_wrapper",
                "wrapper_identity": "nowledge-previous-wrapper:test",
                "primary_only_checks": 0,
                "request_count": 4,
                "dual_engine_evidence": {
                    "ready": true,
                    "primary_check_count": 2,
                    "shadow_check_count": 2,
                    "matched_check_count": 2,
                    "primary_only_check_count": 0
                },
                "blocker_codes": []
            })),
            migration_gate: Some(serde_json::json!({
                "migration_gate": {
                    "decision": "ready",
                    "blockers": []
                },
                "cutover": {
                    "decision": "ready",
                    "blockers": []
                },
                "cutover_evidence": {
                    "eligible": true,
                    "ready_engine_kind": "previous_wrapper",
                    "ready_wrapper_identity": "nowledge-previous-wrapper:test",
                    "storage_recovery_required": true,
                    "storage_recovery_present": true,
                    "storage_recovery_ready": true,
                    "storage_recovery_protocol_matches": true,
                    "storage_recovery_durable": true,
                    "storage_recovery_checkpoint_boundary_present": true,
                    "storage_recovery_wal_replay_bounded": true,
                    "storage_recovery_replay_boundary_consistent": true,
                    "storage_recovery_torn_tail_clean": true,
                    "storage_recovery_blocker_codes": [],
                    "background_maintenance_required": true,
                    "background_maintenance_present": true,
                    "background_maintenance_ready": true,
                    "background_maintenance_protocol_matches": true,
                    "background_maintenance_blocker_codes": [],
                    "blockers": []
                },
                "previous_wrapper_contract_evidence": {
                    "ready": true
                },
                "replacement_readiness_per_million": 1_000_000
            })),
            replacement_summary: Some(serde_json::json!({
                "production_cutover_ready": true,
                "production_replacement_per_million": 1_000_000,
                "blocking_categories": [],
                "missing_evidence": [],
                "next_actions": [],
                "shadow_evidence": {
                    "ready": true,
                    "ready_wrapper_identity": "nowledge-previous-wrapper:test",
                    "contract_wrapper_identity": "nowledge-previous-wrapper:test",
                    "cutover_ready_wrapper_identity": "nowledge-previous-wrapper:test"
                },
                "cutover_evidence": {
                    "background_maintenance_executable_search_projection_graph_delta_count": 2,
                    "background_maintenance_admitted_search_projection_graph_delta_count": 1,
                    "background_maintenance_deferred_search_projection_graph_delta_count": 1,
                    "background_maintenance_rejected_search_projection_graph_delta_count": 0,
                    "background_maintenance_executable_search_projection_graph_delta_operations": 8,
                    "background_maintenance_admitted_search_projection_graph_delta_operations": 3,
                    "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch": 42,
                    "background_maintenance_foreground_admission_probe_ready": true,
                    "background_maintenance_foreground_admission_probe_admission": "admit",
                    "background_maintenance_memory_pressure_ready": true,
                    "background_maintenance_memory_budget_bytes": 4096,
                    "background_maintenance_estimated_memory_bytes": 1024
                },
                "dual_engine_evidence": {
                    "present": true,
                    "ready": true,
                    "consistent": true,
                    "primary_check_count": 2,
                    "shadow_check_count": 2,
                    "matched_check_count": 2,
                    "primary_only_check_count": 0,
                    "matched_per_million": 1_000_000
                },
                "search_projection_evidence": ready_search_projection_evidence(),
                "search_projection_shadow_evidence": ready_search_projection_shadow_evidence(),
                "search_candidate_shadow_evidence": ready_search_candidate_shadow_evidence(),
                "bounded_read_evidence": ready_bounded_read_evidence(),
                "workload_fixture_evidence": ready_workload_fixture_evidence(),
                "graph_route_readiness": ready_graph_route_readiness(),
                "query_runtime_preflight": ready_route_catalog_metadata()
            })),
            query_runtime_preflight: Some(ready_query_runtime_preflight()),
            library_readiness: Some(ready_library_readiness()),
            cutover_controls: Some(ready_cutover_controls()),
            operations_readiness: Some(ready_operations_readiness()),
        }
    }

    fn ready_cutover_controls() -> serde_json::Value {
        serde_json::json!({
            "protocol": NOWLEDGE_MEM_CUTOVER_CONTROLS_PROTOCOL,
            "ready": true,
            "controls": {
                "graph_reads": "skein",
                "search_reads": "skein",
                "dual_writes": "enabled",
                "initial_import": "disabled",
                "projection_catch_up": "enabled"
            },
            "graph": {
                "read_selected_skein": true,
                "read_effective": true
            },
            "search": {
                "read_selected_skein": true,
                "read_effective": true
            },
            "work": {
                "dual_writes_enabled": true,
                "initial_import_enabled": false,
                "initial_import_inactive_for_cutover": true,
                "initial_import_cutover_catch_up_ready": false,
                "initial_import_safe_for_read_cutover": true,
                "projection_catch_up_enabled": true
            },
            "production_status": {
                "graph": {
                    "skein_cutover_effective": true
                },
                "search": {
                    "skein_cutover_effective": true
                }
            },
            "redaction": {
                "query_text_copied": false,
                "parameters_copied": false,
                "local_paths_copied": false
            },
            "blocker_codes": []
        })
    }

    fn ready_operations_readiness() -> serde_json::Value {
        serde_json::json!({
            "protocol": NOWLEDGE_MEM_OPERATIONS_READINESS_PROTOCOL,
            "present": true,
            "ready": true,
            "mode": "writable_cutover",
            "graph": {
                "open": true,
                "read_only": false,
                "commit_epoch": 42
            },
            "search_projection": {
                "open": true,
                "commit_lag": 0,
                "stale": false
            },
            "storage_lifecycle": {
                "ready": true,
                "action": "ready"
            },
            "readiness": {
                "storage_lifecycle_ready": true,
                "storage_recovery_ready": true,
                "slow_query_ready": true,
                "background_maintenance_ready": true
            },
            "redaction": {
                "query_text_copied": false,
                "parameters_copied": false,
                "local_paths_copied": false
            },
            "blocker_codes": []
        })
    }

    fn ready_route_catalog_metadata() -> serde_json::Value {
        serde_json::json!({
            "route_catalog_version": NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION,
            "route_catalog_digest": nowledge_mem_graph_read_route_catalog_digest(),
            "blocker_codes": []
        })
    }

    fn ready_graph_route_readiness() -> serde_json::Value {
        serde_json::json!({
            "route_catalog_version": NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION,
            "route_catalog_digest": nowledge_mem_graph_read_route_catalog_digest(),
            "present": true,
            "ready": true,
            "evidence_ready": true,
            "route_coverage_ready": true,
            "evidence_route_coverage_matches": true,
            "route_query_runtime_ready": true,
            "route_query_plan_evidence_ready": true,
            "route_query_profile_evidence_ready": true,
            "route_query_api_behavior_evidence_ready": true,
            "route_relationship_property_pruning_evidence_ready": true,
            "route_primary_ready": true,
            "primary_ready_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "blocker_codes": []
        })
    }

    fn ready_workload_fixture_evidence() -> serde_json::Value {
        serde_json::json!({
            "protocol": NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL,
            "present": true,
            "ready": true,
            "route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "query_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "failed_query_count": 0,
            "bounded_expansion_probe_count": 2,
            "failed_bounded_expansion_probe_count": 0,
            "search_metadata_probe_count": 3,
            "failed_search_metadata_probe_count": 0,
            "graph_rag_probe_count": 1,
            "failed_graph_rag_probe_count": 0,
            "graph_rag_reports": [
                {
                    "name": "memory-to-entity",
                    "ready": true,
                    "schema_protocol": GRAPH_RAG_SCHEMA_CONTEXT_PROTOCOL,
                    "context_epoch": 42,
                    "schema_fingerprint": 4242,
                    "label_count": 2,
                    "relationship_type_count": 1,
                    "property_count": 3,
                    "route_count": 1,
                    "common_path_count": 1,
                    "parameter_requirement_count": 1,
                    "row_count": 1,
                    "max_rows": 4,
                    "execution_row_cap": 5,
                    "estimated_payload_bytes": 128,
                    "row_budget_exceeded": false,
                    "payload_budget_exceeded": false,
                    "blocking_operator_count": 0,
                    "streaming": false,
                    "error_class": null
                }
            ],
            "source_projection_probe_count": 1,
            "failed_source_projection_probe_count": 0,
            "source_projection_reports": [
                {
                    "name": "source-ingest-composite-changefeed",
                    "ready": true,
                    "source_graph_commit_epoch": 43,
                    "complete_through_graph_commit_epoch": 43,
                    "too_small_batch_failed_closed": true,
                    "operation_count": 2,
                    "upserted_documents": 2,
                    "deleted_documents": 0,
                    "source_document_count": 2,
                    "indexed_source_document_ready": true,
                    "error_class": null
                }
            ],
            "blocker_codes": []
        })
    }

    fn ready_bounded_read_evidence() -> serde_json::Value {
        serde_json::json!({
            "protocol": NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL,
            "present": true,
            "ready": true,
            "mode": "shadow_read_only",
            "max_rows": 512,
            "execution_row_cap": 513,
            "estimated_payload_bytes": 2048,
            "max_estimated_payload_bytes": 4096,
            "payload_budget_exceeded": false,
            "row_limit_enforced_before_output": true,
            "operator_row_cap_enabled": true,
            "row_budget_exceeded": false,
            "streaming": false,
            "blocking_operator_count": 0,
            "blocking_operator_kinds": [],
            "blocking_operator_memory_reports_complete": true,
            "blocking_operator_memory_within_budget": true,
            "spill_within_budget": true,
            "covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "required_covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "missing_covered_routes": [],
            "route_catalog_version": NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION,
            "route_catalog_digest": nowledge_mem_graph_read_route_catalog_digest(),
            "route_primary_ready": true,
            "primary_ready_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "route_query_plan_evidence_ready": true,
            "route_query_profile_evidence_ready": true,
            "route_query_api_behavior_evidence_ready": true,
            "route_relationship_property_pruning_evidence_ready": true,
            "relationship_property_pruning_required_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "relationship_property_pruning_report_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "blocker_codes": []
        })
    }

    fn ready_query_runtime_preflight() -> serde_json::Value {
        let probes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| ready_query_runtime_preflight_probe(route))
            .collect::<Vec<_>>();
        serde_json::json!({
            "protocol": "skein-nowledge-query-runtime-preflight-v1",
            "ready": true,
            "database_opened": true,
            "redaction": {
                "ready": true,
                "rows_copied": false,
                "parameters_copied": false,
                "local_paths_copied": false,
                "raw_errors_copied": false
            },
            "probe_count": probes.len(),
            "passed_probe_count": probes.len(),
            "failed_probe_count": 0,
            "required_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "covered_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "missing_required_routes": [],
            "required_routes_covered": true,
            "unknown_routes": [],
            "duplicate_routes": [],
            "route_catalog_version": NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION,
            "route_catalog_digest": nowledge_mem_graph_read_route_catalog_digest(),
            "route_coverage_ready": true,
            "route_coverage_blocker_codes": [],
            "blocker_codes": [],
            "failed_checks": [],
            "probes": probes
        })
    }

    fn ready_query_runtime_preflight_probe(route: &str) -> serde_json::Value {
        serde_json::json!({
            "name": format!("probe:{route}"),
            "route": route,
            "query_family": "memory_lookup",
            "ready": true,
            "success": true,
            "output_row_count": 1,
            "selected_plan_fingerprint": "ProjectExec(IndexNodeSeek)",
            "selected_plan_operator_counts": {
                "IndexNodeSeek": 1,
                "ProjectExec": 1
            },
            "selected_plan_class_counts": {
                "access": 1,
                "relational": 1
            },
            "optimizer_decision_count": 2,
            "optimizer_rule_event_count": 1,
            "plan_cache_lookup": "miss",
            "plan_cache": {
                "lookup": "miss",
                "bypass_reason": null,
                "cacheable": true,
                "hit": false,
                "miss": true,
                "bypassed": false
            },
            "execution_profile": {
                "scan_pruning_report_count": 1,
                "pruned_scan_count": 1,
                "scan_pruning_reports": [
                    {
                        "target_kind": "node",
                        "label_id": 1,
                        "rel_type_id": null,
                        "strategy": {
                            "kind": "property_eq",
                            "property": "id"
                        },
                        "pruned": true,
                        "exact_empty": false,
                        "candidate_count_before_pruning": 2,
                        "pruned_candidate_count": 1,
                        "candidate_count_before_filter": 1,
                        "output_count": 1,
                        "filtered_out_count": 0
                    }
                ]
            },
            "blocker_codes": []
        })
    }

    fn ready_library_readiness() -> serde_json::Value {
        serde_json::json!({
            "protocol": "skein-nowledge-mem-library-readiness-v1",
            "present": true,
            "ready": true,
            "mode": "shadow_read_only",
            "ready_area_count": 11,
            "blocked_area_count": 0,
            "blocker_codes": [],
            "redaction": {
                "ready": true,
                "query_text_copied": false,
                "parameters_copied": false,
                "local_paths_copied": false
            },
            "open_report": {
                "protocol": "skein-nowledge-mem-open-report",
                "mode": "shadow_read_only",
                "graph_opened": true,
                "search_projection_opened": true
            },
            "readiness_by_area": {
                "graph": {
                    "ready": true,
                    "blocker_codes": []
                },
                "query": {
                    "ready": true,
                    "blocker_codes": []
                },
                "storage": {
                    "ready": true,
                    "blocker_codes": []
                },
                "background": {
                    "ready": true,
                    "blocker_codes": []
                },
                "query_family": {
                    "ready": true,
                    "blocker_codes": []
                },
                "graph_route": {
                    "ready": true,
                    "blocker_codes": []
                },
                "search_route_ownership": {
                    "ready": true,
                    "blocker_codes": []
                },
                "search_projection": {
                    "ready": true,
                    "blocker_codes": []
                },
                "search_projection_shadow": {
                    "ready": true,
                    "blocker_codes": []
                },
                "search_candidate_shadow": {
                    "ready": true,
                    "blocker_codes": []
                },
                "workload_fixture": {
                    "ready": true,
                    "blocker_codes": []
                }
            },
            "workload_fixture_evidence": ready_workload_fixture_evidence()
        })
    }

    fn ready_search_projection_evidence() -> serde_json::Value {
        serde_json::json!({
            "protocol": SKEIN_NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL,
            "present": true,
            "ready": true,
            "derived_projection": true,
            "all_tables_covered": true,
            "covered_table_count": 6,
            "required_table_count": 6,
            "fts_ready": true,
            "vector_ready": true,
            "document_identity_ready": true,
            "embedding_identity_ready": true,
            "fail_soft_ready": true,
            "rebuild_marker_ready": true,
            "metadata_repair_marker_ready": true,
            "incremental_update_ready": true,
            "source_chunk_ready": true,
            "predicate_pushdown_ready": true,
            "production_filter_pruning_ready": true,
            "blocker_codes": []
        })
    }

    fn ready_search_projection_shadow_evidence() -> serde_json::Value {
        serde_json::json!({
            "protocol": SKEIN_NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL,
            "evidence_source": SKEIN_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE,
            "present": true,
            "ready": true,
            "primary_engine": "lancedb",
            "shadow_engine": "skein",
            "primary_ready": true,
            "shadow_ready": true,
            "document_count_parity": true,
            "document_identity_parity": true,
            "table_parity_ready": true,
            "embedding_identity_parity": true,
            "lifecycle_parity": true,
            "incremental_watermark_parity": true,
            "pushdown_evidence": {
                "ready": true,
                "predicate_pushdown_parity": true,
                "primary_predicate_pushdown_ready": true,
                "shadow_predicate_pushdown_ready": true,
                "shadow_persisted_segment_descriptor_ready": true,
                "shadow_segment_descriptor_scan_filter_fields_ready": true,
                "shadow_segment_document_pruning_ready": true,
                "shadow_segment_pruning_candidate_document_count": 4,
                "shadow_segment_pruned_document_count": 2,
                "shadow_segment_scanned_document_count": 2,
                "primary_scan_filter_fields": [
                    "kind",
                    "external_id",
                    "source_id",
                    "space_id",
                    "unit_type",
                    "lifecycle_state",
                    "importance",
                    "confidence",
                    "created_at",
                    "updated_at",
                    "event_start",
                    "event_end",
                    "is_latest"
                ],
                "shadow_scan_filter_fields": [
                    "kind",
                    "external_id",
                    "source_id",
                    "space_id",
                    "unit_type",
                    "lifecycle_state",
                    "importance",
                    "confidence",
                    "created_at",
                    "updated_at",
                    "event_start",
                    "event_end",
                    "is_latest"
                ],
                "shadow_segment_descriptor_field_summaries": scan_filter_field_summaries_json()
            },
            "blocker_codes": []
        })
    }

    fn scan_filter_field_summaries_json() -> serde_json::Value {
        let mut summaries = NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
            .iter()
            .map(|field| descriptor_field_summary_json(field))
            .collect::<Vec<_>>();
        summaries.push(descriptor_field_summary_json("document_id"));
        serde_json::json!(summaries)
    }

    fn descriptor_field_summary_json(field: &str) -> serde_json::Value {
        let numeric_range_summary_used = matches!(field, "importance" | "confidence");
        let timestamp_range_summary_used = matches!(
            field,
            "created_at" | "updated_at" | "event_start" | "event_end"
        );
        let unique_key_summary_used = field == "document_id";
        serde_json::json!({
            "field": field,
            "segment_count": 1,
            "present_document_count": 2,
            "value_summary_used": true,
            "value_summary_segment_count": 1,
            "numeric_range_summary_used": numeric_range_summary_used,
            "numeric_range_segment_count": usize::from(numeric_range_summary_used),
            "timestamp_range_summary_used": timestamp_range_summary_used,
            "timestamp_range_segment_count": usize::from(timestamp_range_summary_used),
            "unique_key_summary_used": unique_key_summary_used,
            "unique_key_summary_segment_count": usize::from(unique_key_summary_used),
        })
    }

    fn ready_search_candidate_shadow_evidence() -> serde_json::Value {
        serde_json::json!({
            "protocol": NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
            "route": NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE,
            "evidence_source": NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE,
            "present": true,
            "ready": true,
            "candidate_primary_engine": NOWLEDGE_MEM_SEARCH_CANDIDATE_PRIMARY_ENGINE,
            "request_count": 2,
            "primary_candidate_count": 3,
            "shadow_candidate_count": 3,
            "matched_candidate_count": 3,
            "primary_only_candidate_count": 0,
            "candidate_counts_ready": true,
            "text_retriever_ready": true,
            "vector_retriever_ready": true,
            "fts_top_k_overlap_ready": true,
            "vector_top_k_overlap_ready": true,
            "source_chunk_identity_ready": true,
            "fail_soft_observed": true,
            "projection_marker_status_visible": true,
            "projection_watermark_ready": true,
            "embedding_identity_ready": true,
            "candidate_identity_ready": true,
            "candidate_identity_parity": true,
            "filter_pushdown_ready": true,
            "filter_pushdown_field_summary_count": NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len(),
            "filter_pushdown_missing_required_fields": [],
            "filter_pushdown_field_capabilities_ready": true,
            "filter_pushdown_missing_value_summary_fields": [],
            "filter_pushdown_missing_numeric_range_fields": [],
            "filter_pushdown_missing_timestamp_range_fields": [],
            "blocker_codes": []
        })
    }
}
