use skein_core::{Result, SkeinError};
use std::collections::BTreeSet;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub const BLACKBOX_REPORT_PROTOCOL: &str = "skein-blackbox-report-v1";
pub const BLACKBOX_EVENT_PROTOCOL: &str = "skein-blackbox-event-v1";

const KNOWN_ARTIFACTS: &[&str] = &[
    "contract.json",
    "contract-evidence.json",
    "previous-wrapper-contract-evidence.json",
    "adapter-smoke.json",
    "adapter-shadow.jsonl",
    "slow-query-log.jsonl",
    "skein-log.jsonl",
    "skein-demo.out",
    "storage-recovery.json",
    "storage-recovery-evidence.json",
    "background-maintenance.json",
    "background-maintenance-evidence.json",
    "migration-shadow.jsonl",
    "migration-gate.json",
    "query-family-evidence.json",
    "graph-route-evidence.json",
    "graph-route-readiness.json",
    "bounded-read-report.json",
    "bounded-read-evidence.json",
    "search-projection-evidence.json",
    "search-projection-shadow-evidence.json",
    "search-candidate-shadow-evidence.json",
    "query-runtime-preflight.json",
    "replacement-summary.json",
    "library-readiness.json",
    "preflight-check.json",
    "integration-bundle.json",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlackboxReportOptions {
    pub artifact_dir: PathBuf,
    pub output_dir: PathBuf,
    pub run_id: Option<String>,
    pub run_status: BlackboxRunStatus,
    pub exit_code: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlackboxRunStatus {
    Completed,
    Failed,
    Running,
}

impl BlackboxRunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Running => "running",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlackboxRedactionReport {
    pub raw_query_text_copied: bool,
    pub raw_parameters_copied: bool,
    pub raw_artifact_payloads_copied: bool,
    pub artifact_paths_are_relative: bool,
}

impl Default for BlackboxRedactionReport {
    fn default() -> Self {
        Self {
            raw_query_text_copied: false,
            raw_parameters_copied: false,
            raw_artifact_payloads_copied: false,
            artifact_paths_are_relative: true,
        }
    }
}

impl BlackboxRedactionReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "raw_query_text_copied": self.raw_query_text_copied,
            "raw_parameters_copied": self.raw_parameters_copied,
            "raw_artifact_payloads_copied": self.raw_artifact_payloads_copied,
            "artifact_paths_are_relative": self.artifact_paths_are_relative,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlackboxJsonArtifactSummary {
    pub parse_ready: bool,
    pub parse_error_kind: Option<String>,
    pub protocol: Option<String>,
    pub ready: Option<bool>,
    pub decision: Option<String>,
    pub blocker_codes: Vec<String>,
    pub blocking_categories: Vec<String>,
    pub failed_checks: Vec<String>,
    pub missing_evidence: Vec<String>,
    pub production_cutover_ready: Option<bool>,
    pub production_replacement_per_million: Option<u64>,
}

impl BlackboxJsonArtifactSummary {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "parse_ready": self.parse_ready,
            "parse_error_kind": self.parse_error_kind,
            "protocol": self.protocol,
            "ready": self.ready,
            "decision": self.decision,
            "blocker_codes": self.blocker_codes,
            "blocking_categories": self.blocking_categories,
            "failed_checks": self.failed_checks,
            "missing_evidence": self.missing_evidence,
            "production_cutover_ready": self.production_cutover_ready,
            "production_replacement_per_million": self.production_replacement_per_million,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlackboxJsonlArtifactSummary {
    pub line_count: usize,
    pub nonempty_line_count: usize,
    pub vector_query_count: usize,
    pub vector_execution_report_count: usize,
    pub vector_backends: Vec<String>,
    pub vector_compression_modes: Vec<String>,
    pub vector_candidate_sources: Vec<String>,
    pub vector_backend_selection_reasons: Vec<String>,
    pub estimated_raw_vector_bytes: u64,
    pub max_filter_selectivity_per_million: u32,
    pub generated_candidate_count: u64,
    pub descriptor_pruned_count: u64,
    pub scalar_filtered_count: u64,
    pub residual_filtered_count: u64,
    pub reranked_candidate_count: u64,
    pub returned_count: u64,
    pub raw_vector_bytes_read: u64,
    pub index_covered_document_count: u64,
    pub index_candidate_document_count: u64,
    pub index_coverage_incomplete_report_count: usize,
    pub index_coverage_unknown_report_count: usize,
    pub vector_fallback_reason_codes: Vec<String>,
}

impl BlackboxJsonlArtifactSummary {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "line_count": self.line_count,
            "nonempty_line_count": self.nonempty_line_count,
            "vector_query_count": self.vector_query_count,
            "vector_execution_report_count": self.vector_execution_report_count,
            "vector_backends": self.vector_backends,
            "vector_compression_modes": self.vector_compression_modes,
            "vector_candidate_sources": self.vector_candidate_sources,
            "vector_backend_selection_reasons": self.vector_backend_selection_reasons,
            "estimated_raw_vector_bytes": self.estimated_raw_vector_bytes,
            "max_filter_selectivity_per_million": self.max_filter_selectivity_per_million,
            "generated_candidate_count": self.generated_candidate_count,
            "descriptor_pruned_count": self.descriptor_pruned_count,
            "scalar_filtered_count": self.scalar_filtered_count,
            "residual_filtered_count": self.residual_filtered_count,
            "reranked_candidate_count": self.reranked_candidate_count,
            "returned_count": self.returned_count,
            "raw_vector_bytes_read": self.raw_vector_bytes_read,
            "index_covered_document_count": self.index_covered_document_count,
            "index_candidate_document_count": self.index_candidate_document_count,
            "index_coverage_incomplete_report_count": self.index_coverage_incomplete_report_count,
            "index_coverage_unknown_report_count": self.index_coverage_unknown_report_count,
            "vector_fallback_reason_codes": self.vector_fallback_reason_codes,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlackboxBackgroundQosSummary {
    pub protocol: Option<String>,
    pub ready: Option<bool>,
    pub total_candidates: Option<u64>,
    pub admitted_count: Option<u64>,
    pub deferred_count: Option<u64>,
    pub rejected_count: Option<u64>,
    pub executable_search_projection_graph_delta_count: Option<u64>,
    pub admitted_search_projection_graph_delta_count: Option<u64>,
    pub deferred_search_projection_graph_delta_count: Option<u64>,
    pub rejected_search_projection_graph_delta_count: Option<u64>,
    pub executable_search_projection_graph_delta_operations: Option<u64>,
    pub admitted_search_projection_graph_delta_operations: Option<u64>,
    pub max_search_projection_graph_delta_complete_through_graph_commit_epoch: Option<u64>,
    pub memory_pressure_ready: Option<bool>,
    pub memory_budget_bytes: Option<u64>,
    pub estimated_memory_bytes: Option<u64>,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlackboxReadinessReport {
    pub protocol: String,
    pub present: bool,
    pub ready: bool,
    pub redaction_ready: bool,
    pub operational_evidence_ready: bool,
    pub protocol_ready: bool,
    pub artifact_dir_present: bool,
    pub artifact_count_present: bool,
    pub events_path_ready: bool,
    pub raw_query_text_redacted: bool,
    pub raw_parameters_redacted: bool,
    pub raw_artifact_payloads_redacted: bool,
    pub artifact_paths_relative: bool,
    pub artifact_count: usize,
    pub slow_query_log_present: bool,
    pub slow_query_log_jsonl_summary_present: bool,
    pub background_maintenance_present: bool,
    pub background_qos_summary_ready: bool,
    pub blocker_codes: Vec<String>,
}

impl BlackboxReadinessReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "present": self.present,
            "ready": self.ready,
            "redaction_ready": self.redaction_ready,
            "operational_evidence_ready": self.operational_evidence_ready,
            "protocol_ready": self.protocol_ready,
            "artifact_dir_present": self.artifact_dir_present,
            "artifact_count_present": self.artifact_count_present,
            "events_path_ready": self.events_path_ready,
            "raw_query_text_redacted": self.raw_query_text_redacted,
            "raw_parameters_redacted": self.raw_parameters_redacted,
            "raw_artifact_payloads_redacted": self.raw_artifact_payloads_redacted,
            "artifact_paths_relative": self.artifact_paths_relative,
            "artifact_count": self.artifact_count,
            "slow_query_log_present": self.slow_query_log_present,
            "slow_query_log_jsonl_summary_present": self.slow_query_log_jsonl_summary_present,
            "background_maintenance_present": self.background_maintenance_present,
            "background_qos_summary_ready": self.background_qos_summary_ready,
            "blocker_codes": self.blocker_codes,
        })
    }
}

impl BlackboxBackgroundQosSummary {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "ready": self.ready,
            "total_candidates": self.total_candidates,
            "admitted_count": self.admitted_count,
            "deferred_count": self.deferred_count,
            "rejected_count": self.rejected_count,
            "executable_search_projection_graph_delta_count": self.executable_search_projection_graph_delta_count,
            "admitted_search_projection_graph_delta_count": self.admitted_search_projection_graph_delta_count,
            "deferred_search_projection_graph_delta_count": self.deferred_search_projection_graph_delta_count,
            "rejected_search_projection_graph_delta_count": self.rejected_search_projection_graph_delta_count,
            "executable_search_projection_graph_delta_operations": self.executable_search_projection_graph_delta_operations,
            "admitted_search_projection_graph_delta_operations": self.admitted_search_projection_graph_delta_operations,
            "max_search_projection_graph_delta_complete_through_graph_commit_epoch": self.max_search_projection_graph_delta_complete_through_graph_commit_epoch,
            "memory_pressure_ready": self.memory_pressure_ready,
            "memory_budget_bytes": self.memory_budget_bytes,
            "estimated_memory_bytes": self.estimated_memory_bytes,
            "blocker_codes": self.blocker_codes,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlackboxArtifactReport {
    pub name: String,
    pub format: String,
    pub byte_len: usize,
    pub checksum: u64,
    pub json: Option<BlackboxJsonArtifactSummary>,
    pub jsonl: Option<BlackboxJsonlArtifactSummary>,
    pub background_qos: Option<BlackboxBackgroundQosSummary>,
}

impl BlackboxArtifactReport {
    pub fn json(&self) -> serde_json::Value {
        let mut artifact = serde_json::Map::new();
        artifact.insert("name".to_string(), serde_json::json!(self.name));
        artifact.insert("format".to_string(), serde_json::json!(self.format));
        artifact.insert("byte_len".to_string(), serde_json::json!(self.byte_len));
        artifact.insert("checksum".to_string(), serde_json::json!(self.checksum));
        if let Some(summary) = self.json.as_ref() {
            artifact.insert("json".to_string(), summary.json());
        }
        if let Some(summary) = self.jsonl.as_ref() {
            artifact.insert("jsonl".to_string(), summary.json());
        }
        if let Some(summary) = self.background_qos.as_ref() {
            artifact.insert("background_qos".to_string(), summary.json());
        }
        serde_json::Value::Object(artifact)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlackboxEventReport {
    pub protocol: String,
    pub protocol_version: u64,
    pub run_id: String,
    pub sequence: usize,
    pub event: String,
    pub artifact: BlackboxArtifactReport,
    pub background_qos: Option<BlackboxBackgroundQosSummary>,
}

impl BlackboxEventReport {
    pub fn json(&self) -> serde_json::Value {
        let mut event = serde_json::json!({
            "protocol": self.protocol,
            "protocol_version": self.protocol_version,
            "run_id": self.run_id,
            "sequence": self.sequence,
            "event": self.event,
            "artifact": self.artifact.json(),
        });
        if let Some(background_qos) = self.background_qos.as_ref() {
            event["background_qos"] = background_qos.json();
        }
        event
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlackboxReport {
    pub protocol: String,
    pub protocol_version: u64,
    pub run_id: String,
    pub run_status: BlackboxRunStatus,
    pub exit_code: Option<i64>,
    pub generated_unix_seconds: u64,
    pub artifact_dir_present: bool,
    pub artifact_count: usize,
    pub events_path: String,
    pub artifacts: Vec<BlackboxArtifactReport>,
    pub redaction: BlackboxRedactionReport,
}

impl BlackboxReport {
    pub fn readiness(&self) -> BlackboxReadinessReport {
        let slow_query_log_present = self.artifact("slow-query-log.jsonl").is_some();
        let slow_query_log_jsonl_summary_present =
            self.jsonl_artifact_summary_present("slow-query-log.jsonl");
        let background_maintenance_present = self.artifact("background-maintenance.json").is_some();
        let background_qos_summary_ready =
            self.background_qos_summary_ready("background-maintenance.json");
        let protocol_ready = self.protocol == BLACKBOX_REPORT_PROTOCOL;
        let artifact_dir_present = self.artifact_dir_present;
        let artifact_count_present = self.artifact_count > 0;
        let events_path_ready = self.events_path == "events.jsonl";
        let raw_query_text_redacted = !self.redaction.raw_query_text_copied;
        let raw_parameters_redacted = !self.redaction.raw_parameters_copied;
        let raw_artifact_payloads_redacted = !self.redaction.raw_artifact_payloads_copied;
        let artifact_paths_relative = self.redaction.artifact_paths_are_relative;
        let redaction_ready = protocol_ready
            && artifact_dir_present
            && artifact_count_present
            && events_path_ready
            && raw_query_text_redacted
            && raw_parameters_redacted
            && raw_artifact_payloads_redacted
            && artifact_paths_relative;
        let operational_evidence_ready = slow_query_log_present
            && slow_query_log_jsonl_summary_present
            && background_maintenance_present
            && background_qos_summary_ready;
        let mut blocker_codes = Vec::new();
        if !redaction_ready {
            blocker_codes.push("blackbox_redaction_not_ready".to_string());
        }
        if !slow_query_log_present {
            blocker_codes.push("blackbox_slow_query_log_missing".to_string());
        }
        if !slow_query_log_jsonl_summary_present {
            blocker_codes.push("blackbox_slow_query_log_summary_missing".to_string());
        }
        if !background_maintenance_present {
            blocker_codes.push("blackbox_background_maintenance_missing".to_string());
        }
        if !background_qos_summary_ready {
            blocker_codes.push("blackbox_background_qos_summary_missing".to_string());
        }

        BlackboxReadinessReport {
            protocol: BLACKBOX_REPORT_PROTOCOL.to_string(),
            present: true,
            ready: blocker_codes.is_empty(),
            redaction_ready,
            operational_evidence_ready,
            protocol_ready,
            artifact_dir_present,
            artifact_count_present,
            events_path_ready,
            raw_query_text_redacted,
            raw_parameters_redacted,
            raw_artifact_payloads_redacted,
            artifact_paths_relative,
            artifact_count: self.artifact_count,
            slow_query_log_present,
            slow_query_log_jsonl_summary_present,
            background_maintenance_present,
            background_qos_summary_ready,
            blocker_codes,
        }
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "protocol_version": self.protocol_version,
            "run_id": self.run_id,
            "run_status": self.run_status.as_str(),
            "exit_code": self.exit_code,
            "generated_unix_seconds": self.generated_unix_seconds,
            "artifact_dir_present": self.artifact_dir_present,
            "artifact_count": self.artifact_count,
            "events_path": self.events_path,
            "artifacts": self.artifacts.iter().map(BlackboxArtifactReport::json).collect::<Vec<_>>(),
            "redaction": self.redaction.json(),
        })
    }

    fn jsonl_artifact_summary_present(&self, name: &str) -> bool {
        self.artifact(name)
            .filter(|artifact| artifact.format == "jsonl")
            .and_then(|artifact| artifact.jsonl.as_ref())
            .is_some()
    }

    fn background_qos_summary_ready(&self, name: &str) -> bool {
        let Some(artifact) = self.artifact(name) else {
            return false;
        };
        let Some(background_qos) = artifact.background_qos.as_ref() else {
            return false;
        };
        artifact.format == "json"
            && background_qos.protocol.as_deref() == Some("skein-background-maintenance-report")
            && background_qos.ready == Some(true)
            && background_qos.total_candidates.is_some()
            && background_qos.admitted_count.is_some()
            && background_qos.deferred_count.is_some()
            && background_qos.rejected_count.is_some()
            && background_qos
                .executable_search_projection_graph_delta_count
                .is_some()
            && background_qos
                .admitted_search_projection_graph_delta_count
                .is_some()
            && background_qos
                .deferred_search_projection_graph_delta_count
                .is_some()
            && background_qos
                .rejected_search_projection_graph_delta_count
                .is_some()
            && background_qos
                .executable_search_projection_graph_delta_operations
                .is_some()
            && background_qos
                .admitted_search_projection_graph_delta_operations
                .is_some()
            && background_qos
                .max_search_projection_graph_delta_complete_through_graph_commit_epoch
                .is_some()
            && background_qos.memory_pressure_ready == Some(true)
            && background_qos.memory_budget_bytes.is_some()
            && background_qos.estimated_memory_bytes.is_some()
            && background_qos.blocker_codes.is_empty()
    }

    fn artifact(&self, name: &str) -> Option<&BlackboxArtifactReport> {
        self.artifacts.iter().find(|artifact| artifact.name == name)
    }

    pub fn events(&self) -> Vec<BlackboxEventReport> {
        let mut events = Vec::new();
        for artifact in &self.artifacts {
            let sequence = events.len() + 1;
            events.push(BlackboxEventReport {
                protocol: BLACKBOX_EVENT_PROTOCOL.to_string(),
                protocol_version: 1,
                run_id: self.run_id.clone(),
                sequence,
                event: "artifact_observed".to_string(),
                artifact: artifact.clone(),
                background_qos: None,
            });
            if let Some(background_qos) = artifact.background_qos.as_ref() {
                let sequence = events.len() + 1;
                events.push(BlackboxEventReport {
                    protocol: BLACKBOX_EVENT_PROTOCOL.to_string(),
                    protocol_version: 1,
                    run_id: self.run_id.clone(),
                    sequence,
                    event: "background_qos_summary".to_string(),
                    artifact: artifact.clone(),
                    background_qos: Some(background_qos.clone()),
                });
            }
        }
        events
    }
}

impl std::str::FromStr for BlackboxRunStatus {
    type Err = SkeinError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "running" => Ok(Self::Running),
            _ => Err(SkeinError::Semantic(
                "blackbox run status must be completed, failed, or running".to_string(),
            )),
        }
    }
}

pub fn blackbox_report(options: &BlackboxReportOptions) -> Result<BlackboxReport> {
    if !options.artifact_dir.is_dir() {
        return Err(SkeinError::Semantic(
            "blackbox artifact_dir does not exist or is not a directory".to_string(),
        ));
    }
    let generated_unix_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| SkeinError::Execution(format!("system clock before unix epoch: {error}")))?
        .as_secs();
    let run_id = options
        .run_id
        .clone()
        .unwrap_or_else(|| format!("skein-blackbox-{generated_unix_seconds}"));
    let artifacts = collect_blackbox_artifacts(&options.artifact_dir)?;
    Ok(BlackboxReport {
        protocol: BLACKBOX_REPORT_PROTOCOL.to_string(),
        protocol_version: 1,
        run_id,
        run_status: options.run_status,
        exit_code: options.exit_code,
        generated_unix_seconds,
        artifact_dir_present: true,
        artifact_count: artifacts.len(),
        events_path: "events.jsonl".to_string(),
        artifacts,
        redaction: BlackboxRedactionReport::default(),
    })
}

pub fn blackbox_report_json(options: &BlackboxReportOptions) -> Result<serde_json::Value> {
    Ok(blackbox_report(options)?.json())
}

pub fn blackbox_readiness_from_manifest_json(value: &serde_json::Value) -> BlackboxReadinessReport {
    let artifact_count = value
        .get("artifact_count")
        .and_then(serde_json::Value::as_u64)
        .and_then(|count| usize::try_from(count).ok())
        .unwrap_or_default();
    let slow_query_log_present = blackbox_json_artifact(value, "slow-query-log.jsonl").is_some();
    let slow_query_log_jsonl_summary_present =
        blackbox_jsonl_artifact_summary_present(value, "slow-query-log.jsonl");
    let background_maintenance_present =
        blackbox_json_artifact(value, "background-maintenance.json").is_some();
    let background_qos_summary_ready =
        blackbox_json_background_qos_summary_ready(value, "background-maintenance.json");
    let protocol_ready =
        value.get("protocol").and_then(serde_json::Value::as_str) == Some(BLACKBOX_REPORT_PROTOCOL);
    let artifact_dir_present = value
        .get("artifact_dir_present")
        .and_then(serde_json::Value::as_bool)
        == Some(true);
    let artifact_count_present = value
        .get("artifact_count")
        .and_then(serde_json::Value::as_u64)
        .is_some_and(|count| count > 0);
    let events_path_ready =
        value.get("events_path").and_then(serde_json::Value::as_str) == Some("events.jsonl");
    let raw_query_text_redacted =
        blackbox_json_nested_bool(value, &["redaction", "raw_query_text_copied"]) == Some(false);
    let raw_parameters_redacted =
        blackbox_json_nested_bool(value, &["redaction", "raw_parameters_copied"]) == Some(false);
    let raw_artifact_payloads_redacted =
        blackbox_json_nested_bool(value, &["redaction", "raw_artifact_payloads_copied"])
            == Some(false);
    let artifact_paths_relative =
        blackbox_json_nested_bool(value, &["redaction", "artifact_paths_are_relative"])
            == Some(true);
    let redaction_ready = protocol_ready
        && artifact_dir_present
        && artifact_count_present
        && events_path_ready
        && raw_query_text_redacted
        && raw_parameters_redacted
        && raw_artifact_payloads_redacted
        && artifact_paths_relative;
    let operational_evidence_ready = slow_query_log_present
        && slow_query_log_jsonl_summary_present
        && background_maintenance_present
        && background_qos_summary_ready;
    let mut blocker_codes = Vec::new();
    if !redaction_ready {
        blocker_codes.push("blackbox_redaction_not_ready".to_string());
    }
    if !slow_query_log_present {
        blocker_codes.push("blackbox_slow_query_log_missing".to_string());
    }
    if !slow_query_log_jsonl_summary_present {
        blocker_codes.push("blackbox_slow_query_log_summary_missing".to_string());
    }
    if !background_maintenance_present {
        blocker_codes.push("blackbox_background_maintenance_missing".to_string());
    }
    if !background_qos_summary_ready {
        blocker_codes.push("blackbox_background_qos_summary_missing".to_string());
    }
    blocker_codes.extend(string_array_field(value, "blocker_codes"));

    BlackboxReadinessReport {
        protocol: BLACKBOX_REPORT_PROTOCOL.to_string(),
        present: value.is_object(),
        ready: blocker_codes.is_empty(),
        redaction_ready,
        operational_evidence_ready,
        protocol_ready,
        artifact_dir_present,
        artifact_count_present,
        events_path_ready,
        raw_query_text_redacted,
        raw_parameters_redacted,
        raw_artifact_payloads_redacted,
        artifact_paths_relative,
        artifact_count,
        slow_query_log_present,
        slow_query_log_jsonl_summary_present,
        background_maintenance_present,
        background_qos_summary_ready,
        blocker_codes,
    }
}

pub fn write_blackbox_report(options: &BlackboxReportOptions) -> Result<serde_json::Value> {
    Ok(write_blackbox_report_typed(options)?.json())
}

pub fn write_blackbox_report_typed(options: &BlackboxReportOptions) -> Result<BlackboxReport> {
    fs::create_dir_all(&options.output_dir)?;
    let manifest = blackbox_report(options)?;
    write_blackbox_events(&options.output_dir.join("events.jsonl"), &manifest.events())?;
    let manifest_json = manifest.json();
    let manifest_bytes = serde_json::to_vec_pretty(&manifest_json).map_err(|_| {
        SkeinError::Execution("blackbox manifest JSON error: serialization_error".to_string())
    })?;
    fs::write(options.output_dir.join("manifest.json"), manifest_bytes)?;
    Ok(manifest)
}

fn collect_blackbox_artifacts(artifact_dir: &Path) -> Result<Vec<BlackboxArtifactReport>> {
    let mut artifacts = Vec::new();
    for artifact_name in KNOWN_ARTIFACTS {
        let path = artifact_dir.join(artifact_name);
        if !path.is_file() {
            continue;
        }
        let bytes = fs::read(&path)?;
        let json = artifact_name
            .ends_with(".json")
            .then(|| json_artifact_summary(&bytes));
        let jsonl = artifact_name
            .ends_with(".jsonl")
            .then(|| jsonl_artifact_summary(&bytes));
        artifacts.push(BlackboxArtifactReport {
            name: (*artifact_name).to_string(),
            format: artifact_format(artifact_name).to_string(),
            byte_len: bytes.len(),
            checksum: checksum_bytes(&bytes),
            json,
            jsonl,
            background_qos: background_qos_summary_for_artifact(artifact_name, &bytes),
        });
    }
    Ok(artifacts)
}

fn background_qos_summary_for_artifact(
    artifact_name: &str,
    bytes: &[u8],
) -> Option<BlackboxBackgroundQosSummary> {
    if artifact_name != "background-maintenance.json"
        && artifact_name != "background-maintenance-evidence.json"
    {
        return None;
    }
    serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .map(|value| background_qos_summary_from_json(&value))
}

fn background_qos_summary_from_json(value: &serde_json::Value) -> BlackboxBackgroundQosSummary {
    let memory_pressure = value.get("memory_pressure");
    BlackboxBackgroundQosSummary {
        protocol: string_field(value, "protocol"),
        ready: first_bool_field(value, &["ready", "background_maintenance_ready"]),
        total_candidates: first_u64_field(
            value,
            &[
                "total_candidates",
                "background_maintenance_total_candidates",
            ],
        ),
        admitted_count: first_u64_field(value, &["admitted_count"]),
        deferred_count: first_u64_field(value, &["deferred_count"]),
        rejected_count: first_u64_field(value, &["rejected_count"]),
        executable_search_projection_graph_delta_count: first_u64_field(
            value,
            &[
                "executable_search_projection_graph_delta_count",
                "background_maintenance_executable_search_projection_graph_delta_count",
            ],
        ),
        admitted_search_projection_graph_delta_count: first_u64_field(
            value,
            &[
                "admitted_search_projection_graph_delta_count",
                "background_maintenance_admitted_search_projection_graph_delta_count",
            ],
        ),
        deferred_search_projection_graph_delta_count: first_u64_field(
            value,
            &[
                "deferred_search_projection_graph_delta_count",
                "background_maintenance_deferred_search_projection_graph_delta_count",
            ],
        ),
        rejected_search_projection_graph_delta_count: first_u64_field(
            value,
            &[
                "rejected_search_projection_graph_delta_count",
                "background_maintenance_rejected_search_projection_graph_delta_count",
            ],
        ),
        executable_search_projection_graph_delta_operations: first_u64_field(
            value,
            &[
                "executable_search_projection_graph_delta_operations",
                "background_maintenance_executable_search_projection_graph_delta_operations",
            ],
        ),
        admitted_search_projection_graph_delta_operations: first_u64_field(
            value,
            &[
                "admitted_search_projection_graph_delta_operations",
                "background_maintenance_admitted_search_projection_graph_delta_operations",
            ],
        ),
        max_search_projection_graph_delta_complete_through_graph_commit_epoch: first_u64_field(
            value,
            &[
                "max_search_projection_graph_delta_complete_through_graph_commit_epoch",
                "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch",
            ],
        ),
        memory_pressure_ready: memory_pressure
            .and_then(|memory_pressure| memory_pressure.get("ready"))
            .and_then(serde_json::Value::as_bool)
            .or_else(|| {
                value
                    .get("memory_pressure_ready")
                    .or_else(|| value.get("background_maintenance_memory_pressure_ready"))
                    .and_then(serde_json::Value::as_bool)
            }),
        memory_budget_bytes: memory_pressure
            .and_then(|memory_pressure| memory_pressure.get("budget_bytes"))
            .and_then(serde_json::Value::as_u64)
            .or_else(|| {
                value
                    .get("memory_budget_bytes")
                    .or_else(|| value.get("background_maintenance_memory_budget_bytes"))
                    .and_then(serde_json::Value::as_u64)
            }),
        estimated_memory_bytes: memory_pressure
            .and_then(|memory_pressure| memory_pressure.get("estimated_bytes"))
            .and_then(serde_json::Value::as_u64)
            .or_else(|| {
                value
                    .get("estimated_memory_bytes")
                    .or_else(|| value.get("background_maintenance_estimated_memory_bytes"))
                    .and_then(serde_json::Value::as_u64)
            }),
        blocker_codes: string_array_field(value, "blocker_codes")
            .into_iter()
            .chain(string_array_field(
                value,
                "background_maintenance_blocker_codes",
            ))
            .collect(),
    }
}

fn artifact_format(name: &str) -> &'static str {
    if name.ends_with(".json") {
        "json"
    } else if name.ends_with(".jsonl") {
        "jsonl"
    } else {
        "opaque"
    }
}

fn json_artifact_summary(bytes: &[u8]) -> BlackboxJsonArtifactSummary {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        return BlackboxJsonArtifactSummary {
            parse_ready: false,
            parse_error_kind: Some("invalid_json".to_string()),
            protocol: None,
            ready: None,
            decision: None,
            blocker_codes: Vec::new(),
            blocking_categories: Vec::new(),
            failed_checks: Vec::new(),
            missing_evidence: Vec::new(),
            production_cutover_ready: None,
            production_replacement_per_million: None,
        };
    };
    BlackboxJsonArtifactSummary {
        parse_ready: true,
        parse_error_kind: None,
        protocol: string_field(&value, "protocol"),
        ready: first_bool_field(
            &value,
            &[
                "ready",
                "production_cutover_ready",
                "route_primary_ready",
                "required_contract_ready",
                "library_ready",
                "integration_ready",
            ],
        ),
        decision: string_field(&value, "decision"),
        blocker_codes: string_array_field(&value, "blocker_codes"),
        blocking_categories: string_array_field(&value, "blocking_categories"),
        failed_checks: string_array_field(&value, "failed_checks"),
        missing_evidence: string_array_field(&value, "missing_evidence"),
        production_cutover_ready: value
            .get("production_cutover_ready")
            .and_then(serde_json::Value::as_bool),
        production_replacement_per_million: value
            .get("production_replacement_per_million")
            .and_then(serde_json::Value::as_u64),
    }
}

fn jsonl_artifact_summary(bytes: &[u8]) -> BlackboxJsonlArtifactSummary {
    let text = String::from_utf8_lossy(bytes);
    let line_count = text.lines().count();
    let nonempty_line_count = text.lines().filter(|line| !line.trim().is_empty()).count();
    let mut vector_query_count = 0usize;
    let mut vector_execution_report_count = 0usize;
    let mut vector_backends = BTreeSet::new();
    let mut vector_compression_modes = BTreeSet::new();
    let mut vector_candidate_sources = BTreeSet::new();
    let mut vector_backend_selection_reasons = BTreeSet::new();
    let mut estimated_raw_vector_bytes = 0u64;
    let mut max_filter_selectivity_per_million = 0u32;
    let mut generated_candidate_count = 0u64;
    let mut descriptor_pruned_count = 0u64;
    let mut scalar_filtered_count = 0u64;
    let mut residual_filtered_count = 0u64;
    let mut reranked_candidate_count = 0u64;
    let mut returned_count = 0u64;
    let mut raw_vector_bytes_read = 0u64;
    let mut index_covered_document_count = 0u64;
    let mut index_candidate_document_count = 0u64;
    let mut index_coverage_incomplete_report_count = 0usize;
    let mut index_coverage_unknown_report_count = 0usize;
    let mut vector_fallback_reason_codes = BTreeSet::new();
    for value in text
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
    {
        let Some(reports) = value
            .get("vector_execution_reports")
            .and_then(serde_json::Value::as_array)
        else {
            continue;
        };
        if !reports.is_empty() {
            vector_query_count += 1;
        }
        vector_execution_report_count = vector_execution_report_count.saturating_add(reports.len());
        for report in reports {
            if let Some(backend) = report.get("backend").and_then(serde_json::Value::as_str) {
                vector_backends.insert(backend.to_string());
            }
            if let Some(mode) = report
                .get("compression_mode")
                .and_then(serde_json::Value::as_str)
            {
                vector_compression_modes.insert(mode.to_string());
            }
            if let Some(source) = report
                .get("candidate_source")
                .and_then(serde_json::Value::as_str)
            {
                vector_candidate_sources.insert(source.to_string());
            }
            if let Some(reason) = report
                .get("backend_selection_reason")
                .and_then(serde_json::Value::as_str)
            {
                vector_backend_selection_reasons.insert(reason.to_string());
            }
            estimated_raw_vector_bytes = estimated_raw_vector_bytes.saturating_add(
                report
                    .get("estimated_raw_vector_bytes")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
            );
            max_filter_selectivity_per_million = max_filter_selectivity_per_million.max(
                report
                    .get("filter_selectivity_per_million")
                    .and_then(serde_json::Value::as_u64)
                    .and_then(|value| u32::try_from(value).ok())
                    .unwrap_or(0),
            );
            generated_candidate_count = generated_candidate_count.saturating_add(
                report
                    .get("generated_candidate_count")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
            );
            descriptor_pruned_count = descriptor_pruned_count.saturating_add(
                report
                    .get("descriptor_pruned_count")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
            );
            scalar_filtered_count = scalar_filtered_count.saturating_add(
                report
                    .get("scalar_filtered_count")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
            );
            residual_filtered_count = residual_filtered_count.saturating_add(
                report
                    .get("residual_filtered_count")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
            );
            reranked_candidate_count = reranked_candidate_count.saturating_add(
                report
                    .get("reranked_candidate_count")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
            );
            returned_count = returned_count.saturating_add(
                report
                    .get("returned_count")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
            );
            raw_vector_bytes_read = raw_vector_bytes_read.saturating_add(
                report
                    .get("raw_vector_bytes_read")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
            );
            index_covered_document_count = index_covered_document_count.saturating_add(
                report
                    .get("index_covered_document_count")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
            );
            index_candidate_document_count = index_candidate_document_count.saturating_add(
                report
                    .get("index_candidate_document_count")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
            );
            match report
                .get("index_coverage_complete")
                .and_then(serde_json::Value::as_bool)
            {
                Some(false) => {
                    index_coverage_incomplete_report_count =
                        index_coverage_incomplete_report_count.saturating_add(1);
                }
                Some(true) => {}
                None => {
                    index_coverage_unknown_report_count =
                        index_coverage_unknown_report_count.saturating_add(1);
                }
            }
            if let Some(codes) = report
                .get("fallback_reason_codes")
                .and_then(serde_json::Value::as_array)
            {
                vector_fallback_reason_codes.extend(
                    codes
                        .iter()
                        .filter_map(serde_json::Value::as_str)
                        .map(ToString::to_string),
                );
            }
        }
    }
    BlackboxJsonlArtifactSummary {
        line_count,
        nonempty_line_count,
        vector_query_count,
        vector_execution_report_count,
        vector_backends: vector_backends.into_iter().collect(),
        vector_compression_modes: vector_compression_modes.into_iter().collect(),
        vector_candidate_sources: vector_candidate_sources.into_iter().collect(),
        vector_backend_selection_reasons: vector_backend_selection_reasons.into_iter().collect(),
        estimated_raw_vector_bytes,
        max_filter_selectivity_per_million,
        generated_candidate_count,
        descriptor_pruned_count,
        scalar_filtered_count,
        residual_filtered_count,
        reranked_candidate_count,
        returned_count,
        raw_vector_bytes_read,
        index_covered_document_count,
        index_candidate_document_count,
        index_coverage_incomplete_report_count,
        index_coverage_unknown_report_count,
        vector_fallback_reason_codes: vector_fallback_reason_codes.into_iter().collect(),
    }
}

fn string_field(value: &serde_json::Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

fn first_bool_field(value: &serde_json::Value, keys: &[&str]) -> Option<bool> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(serde_json::Value::as_bool))
}

fn first_u64_field(value: &serde_json::Value, keys: &[&str]) -> Option<u64> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(serde_json::Value::as_u64))
}

fn string_array_field(value: &serde_json::Value, key: &str) -> Vec<String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(str::to_string)
        .collect()
}

fn blackbox_jsonl_artifact_summary_present(value: &serde_json::Value, name: &str) -> bool {
    let Some(artifact) = blackbox_json_artifact(value, name) else {
        return false;
    };
    artifact.get("format").and_then(serde_json::Value::as_str) == Some("jsonl")
        && blackbox_json_nested_u64(artifact, &["jsonl", "line_count"]).is_some()
        && blackbox_json_nested_u64(artifact, &["jsonl", "nonempty_line_count"]).is_some()
}

fn blackbox_json_background_qos_summary_ready(value: &serde_json::Value, name: &str) -> bool {
    let Some(artifact) = blackbox_json_artifact(value, name) else {
        return false;
    };
    artifact.get("format").and_then(serde_json::Value::as_str) == Some("json")
        && blackbox_json_nested_str(artifact, &["background_qos", "protocol"])
            == Some("skein-background-maintenance-report")
        && blackbox_json_nested_bool(artifact, &["background_qos", "ready"]) == Some(true)
        && blackbox_json_nested_u64(artifact, &["background_qos", "total_candidates"]).is_some()
        && blackbox_json_nested_u64(artifact, &["background_qos", "admitted_count"]).is_some()
        && blackbox_json_nested_u64(artifact, &["background_qos", "deferred_count"]).is_some()
        && blackbox_json_nested_u64(artifact, &["background_qos", "rejected_count"]).is_some()
        && blackbox_json_nested_u64(
            artifact,
            &[
                "background_qos",
                "executable_search_projection_graph_delta_count",
            ],
        )
        .is_some()
        && blackbox_json_nested_u64(
            artifact,
            &[
                "background_qos",
                "admitted_search_projection_graph_delta_count",
            ],
        )
        .is_some()
        && blackbox_json_nested_u64(
            artifact,
            &[
                "background_qos",
                "deferred_search_projection_graph_delta_count",
            ],
        )
        .is_some()
        && blackbox_json_nested_u64(
            artifact,
            &[
                "background_qos",
                "rejected_search_projection_graph_delta_count",
            ],
        )
        .is_some()
        && blackbox_json_nested_u64(
            artifact,
            &[
                "background_qos",
                "executable_search_projection_graph_delta_operations",
            ],
        )
        .is_some()
        && blackbox_json_nested_u64(
            artifact,
            &[
                "background_qos",
                "admitted_search_projection_graph_delta_operations",
            ],
        )
        .is_some()
        && blackbox_json_nested_u64(
            artifact,
            &[
                "background_qos",
                "max_search_projection_graph_delta_complete_through_graph_commit_epoch",
            ],
        )
        .is_some()
        && blackbox_json_nested_bool(artifact, &["background_qos", "memory_pressure_ready"])
            == Some(true)
        && blackbox_json_nested_u64(artifact, &["background_qos", "memory_budget_bytes"]).is_some()
        && blackbox_json_nested_u64(artifact, &["background_qos", "estimated_memory_bytes"])
            .is_some()
        && blackbox_json_nested_value(artifact, &["background_qos", "blocker_codes"])
            .and_then(serde_json::Value::as_array)
            .is_some_and(Vec::is_empty)
}

fn blackbox_json_artifact<'a>(
    value: &'a serde_json::Value,
    name: &str,
) -> Option<&'a serde_json::Value> {
    value
        .get("artifacts")?
        .as_array()?
        .iter()
        .find(|artifact| artifact.get("name").and_then(serde_json::Value::as_str) == Some(name))
}

fn blackbox_json_nested_value<'a>(
    value: &'a serde_json::Value,
    path: &[&str],
) -> Option<&'a serde_json::Value> {
    path.iter()
        .try_fold(value, |current, key| current.get(*key))
}

fn blackbox_json_nested_bool(value: &serde_json::Value, path: &[&str]) -> Option<bool> {
    blackbox_json_nested_value(value, path).and_then(serde_json::Value::as_bool)
}

fn blackbox_json_nested_u64(value: &serde_json::Value, path: &[&str]) -> Option<u64> {
    blackbox_json_nested_value(value, path).and_then(serde_json::Value::as_u64)
}

fn blackbox_json_nested_str<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a str> {
    blackbox_json_nested_value(value, path).and_then(serde_json::Value::as_str)
}

fn write_blackbox_events(events_path: &Path, events: &[BlackboxEventReport]) -> Result<()> {
    let mut file = fs::File::create(events_path)?;
    for event in events {
        let event_line = serde_json::to_string(&event.json()).map_err(|_| {
            SkeinError::Execution("blackbox event JSON error: serialization_error".to_string())
        })?;
        writeln!(file, "{event_line}")?;
    }
    Ok(())
}

fn checksum_bytes(bytes: &[u8]) -> u64 {
    skein_integrity::checksum_u64(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn blackbox_report_writes_redacted_manifest_and_events() {
        let root = unique_test_dir("blackbox-report");
        let artifact_dir = root.join("artifacts");
        let output_dir = root.join("blackbox");
        fs::create_dir_all(&artifact_dir).unwrap();
        fs::write(
            artifact_dir.join("query-runtime-preflight.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "protocol": "skein-nowledge-query-runtime-preflight-v1",
                "ready": false,
                "blocker_codes": ["query_runtime_probe_failed"],
                "query": "MATCH (secret {token: $token}) RETURN secret",
                "parameters": {"token": "secret-token"}
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            artifact_dir.join("adapter-shadow.jsonl"),
            "{\"event\":\"request\",\"payload\":{\"query\":\"secret\"}}\n",
        )
        .unwrap();
        fs::write(
            artifact_dir.join("slow-query-log.jsonl"),
            "{\"event\":\"slow_query\",\"digest\":\"digest-1\"}\n",
        )
        .unwrap();

        let manifest = write_blackbox_report(&BlackboxReportOptions {
            artifact_dir,
            output_dir: output_dir.clone(),
            run_id: Some("run-1".to_string()),
            run_status: BlackboxRunStatus::Failed,
            exit_code: Some(7),
        })
        .unwrap();

        assert_eq!(manifest["protocol"], BLACKBOX_REPORT_PROTOCOL);
        assert_eq!(manifest["run_id"], "run-1");
        assert_eq!(manifest["run_status"], "failed");
        assert_eq!(manifest["exit_code"], 7);
        assert_eq!(manifest["artifact_count"], 3);
        let rendered = serde_json::to_string(&manifest).unwrap();
        assert!(!rendered.contains("MATCH"));
        assert!(!rendered.contains("secret-token"));
        assert!(rendered.contains("query_runtime_probe_failed"));
        assert!(output_dir.join("manifest.json").is_file());
        let events = fs::read_to_string(output_dir.join("events.jsonl")).unwrap();
        assert_eq!(events.lines().count(), 3);
        assert!(events.contains(BLACKBOX_EVENT_PROTOCOL));
        assert!(events.contains("slow-query-log.jsonl"));
    }

    #[test]
    fn blackbox_report_typed_api_exposes_redacted_manifest_and_events() {
        let root = unique_test_dir("blackbox-report-typed");
        let artifact_dir = root.join("artifacts");
        fs::create_dir_all(&artifact_dir).unwrap();
        fs::write(
            artifact_dir.join("replacement-summary.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "protocol": "skein-nowledge-replacement-summary",
                "production_cutover_ready": false,
                "production_replacement_per_million": 500_000,
                "blocking_categories": ["query_runtime_preflight"],
                "query": "MATCH (secret {token: $token}) RETURN secret",
                "parameters": {"token": "secret-token"}
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            artifact_dir.join("skein-log.jsonl"),
            "{\"event\":\"started\"}\n\n",
        )
        .unwrap();

        let report = blackbox_report(&BlackboxReportOptions {
            artifact_dir,
            output_dir: root.join("blackbox"),
            run_id: Some("run-typed".to_string()),
            run_status: BlackboxRunStatus::Running,
            exit_code: None,
        })
        .unwrap();

        assert_eq!(report.protocol, BLACKBOX_REPORT_PROTOCOL);
        assert_eq!(report.run_id, "run-typed");
        assert_eq!(report.run_status, BlackboxRunStatus::Running);
        assert_eq!(report.artifact_count, 2);
        assert!(report.redaction.artifact_paths_are_relative);
        assert!(!report.redaction.raw_query_text_copied);
        assert!(!report.redaction.raw_parameters_copied);
        let replacement = report
            .artifacts
            .iter()
            .find(|artifact| artifact.name == "replacement-summary.json")
            .unwrap();
        let summary = replacement.json.as_ref().unwrap();
        assert!(summary.parse_ready);
        assert_eq!(
            summary.protocol.as_deref(),
            Some("skein-nowledge-replacement-summary")
        );
        assert_eq!(summary.ready, Some(false));
        assert_eq!(
            summary.blocking_categories,
            vec!["query_runtime_preflight".to_string()]
        );
        assert_eq!(summary.production_replacement_per_million, Some(500_000));
        let log = report
            .artifacts
            .iter()
            .find(|artifact| artifact.name == "skein-log.jsonl")
            .unwrap();
        assert_eq!(log.jsonl.as_ref().unwrap().line_count, 2);
        assert_eq!(log.jsonl.as_ref().unwrap().nonempty_line_count, 1);
        let events = report.events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].protocol, BLACKBOX_EVENT_PROTOCOL);
        assert_eq!(events[0].run_id, "run-typed");
        let rendered = serde_json::to_string(&report.json()).unwrap();
        assert!(!rendered.contains("MATCH"));
        assert!(!rendered.contains("secret-token"));
        assert!(!rendered.contains(root.to_string_lossy().as_ref()));
    }

    #[test]
    fn jsonl_summary_aggregates_redacted_vector_execution_metrics() {
        let summary = jsonl_artifact_summary(
            br#"{"vector_execution_reports":[{"backend":"quantized_projection","compression_mode":"preferred","candidate_source":"quantized","backend_selection_reason":"quantized_preferred","estimated_raw_vector_bytes":307200,"filter_selectivity_per_million":800000,"generated_candidate_count":5,"descriptor_pruned_count":3,"scalar_filtered_count":2,"residual_filtered_count":1,"reranked_candidate_count":4,"returned_count":2,"raw_vector_bytes_read":128,"index_covered_document_count":8,"index_candidate_document_count":10,"index_coverage_complete":false,"fallback_reason_codes":["vector_index_empty"]}]}
{"vector_execution_reports":[]}
"#,
        );

        assert_eq!(summary.vector_query_count, 1);
        assert_eq!(summary.vector_execution_report_count, 1);
        assert_eq!(summary.vector_backends, vec!["quantized_projection"]);
        assert_eq!(summary.vector_compression_modes, vec!["preferred"]);
        assert_eq!(summary.vector_candidate_sources, vec!["quantized"]);
        assert_eq!(
            summary.vector_backend_selection_reasons,
            vec!["quantized_preferred"]
        );
        assert_eq!(summary.estimated_raw_vector_bytes, 307_200);
        assert_eq!(summary.max_filter_selectivity_per_million, 800_000);
        assert_eq!(summary.generated_candidate_count, 5);
        assert_eq!(summary.descriptor_pruned_count, 3);
        assert_eq!(summary.scalar_filtered_count, 2);
        assert_eq!(summary.residual_filtered_count, 1);
        assert_eq!(summary.reranked_candidate_count, 4);
        assert_eq!(summary.returned_count, 2);
        assert_eq!(summary.raw_vector_bytes_read, 128);
        assert_eq!(summary.index_covered_document_count, 8);
        assert_eq!(summary.index_candidate_document_count, 10);
        assert_eq!(summary.index_coverage_incomplete_report_count, 1);
        assert_eq!(summary.index_coverage_unknown_report_count, 0);
        assert_eq!(
            summary.vector_fallback_reason_codes,
            vec!["vector_index_empty"]
        );
    }

    #[test]
    fn blackbox_report_records_compact_background_qos_event() {
        let root = unique_test_dir("blackbox-background-qos");
        let artifact_dir = root.join("artifacts");
        let output_dir = root.join("blackbox");
        fs::create_dir_all(&artifact_dir).unwrap();
        fs::write(
            artifact_dir.join("background-maintenance.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "protocol": "skein-background-maintenance-report",
                "ready": false,
                "total_candidates": 7,
                "admitted_count": 3,
                "deferred_count": 4,
                "rejected_count": 0,
                "executable_search_projection_graph_delta_count": 5,
                "admitted_search_projection_graph_delta_count": 2,
                "deferred_search_projection_graph_delta_count": 3,
                "rejected_search_projection_graph_delta_count": 0,
                "executable_search_projection_graph_delta_operations": 13,
                "admitted_search_projection_graph_delta_operations": 8,
                "max_search_projection_graph_delta_complete_through_graph_commit_epoch": 42,
                "memory_pressure": {
                    "ready": false,
                    "budget_bytes": 4096,
                    "estimated_bytes": 8192
                },
                "blocker_codes": ["memory_budget_exceeded"],
                "query": "MATCH (secret {token: $token}) RETURN secret",
                "parameters": {"token": "secret-token"}
            }))
            .unwrap(),
        )
        .unwrap();

        let report = write_blackbox_report_typed(&BlackboxReportOptions {
            artifact_dir,
            output_dir: output_dir.clone(),
            run_id: Some("run-background-qos".to_string()),
            run_status: BlackboxRunStatus::Completed,
            exit_code: Some(0),
        })
        .unwrap();

        let background = report
            .artifacts
            .iter()
            .find(|artifact| artifact.name == "background-maintenance.json")
            .and_then(|artifact| artifact.background_qos.as_ref())
            .unwrap();
        assert_eq!(background.ready, Some(false));
        assert_eq!(background.total_candidates, Some(7));
        assert_eq!(background.admitted_count, Some(3));
        assert_eq!(background.deferred_count, Some(4));
        assert_eq!(
            background.executable_search_projection_graph_delta_count,
            Some(5)
        );
        assert_eq!(
            background.max_search_projection_graph_delta_complete_through_graph_commit_epoch,
            Some(42)
        );
        assert_eq!(background.memory_pressure_ready, Some(false));
        assert_eq!(background.memory_budget_bytes, Some(4096));
        assert_eq!(background.estimated_memory_bytes, Some(8192));
        assert_eq!(
            background.blocker_codes,
            vec!["memory_budget_exceeded".to_string()]
        );

        let events = report.events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event, "artifact_observed");
        assert_eq!(events[1].event, "background_qos_summary");
        assert_eq!(events[1].background_qos.as_ref().unwrap(), background);
        let rendered_events = fs::read_to_string(output_dir.join("events.jsonl")).unwrap();
        assert!(rendered_events.contains("background_qos_summary"));
        assert!(rendered_events.contains("memory_budget_exceeded"));
        assert!(!rendered_events.contains("MATCH"));
        assert!(!rendered_events.contains("secret-token"));
        assert!(!rendered_events.contains(root.to_string_lossy().as_ref()));
    }

    #[test]
    fn blackbox_readiness_report_uses_typed_manifest_fields() {
        let root = unique_test_dir("blackbox-readiness");
        let artifact_dir = root.join("artifacts");
        fs::create_dir_all(&artifact_dir).unwrap();
        fs::write(
            artifact_dir.join("slow-query-log.jsonl"),
            "{\"event\":\"slow_query\",\"digest\":\"digest-1\"}\n",
        )
        .unwrap();
        fs::write(
            artifact_dir.join("background-maintenance.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "protocol": "skein-background-maintenance-report",
                "ready": true,
                "total_candidates": 1,
                "admitted_count": 1,
                "deferred_count": 0,
                "rejected_count": 0,
                "executable_search_projection_graph_delta_count": 1,
                "admitted_search_projection_graph_delta_count": 1,
                "deferred_search_projection_graph_delta_count": 0,
                "rejected_search_projection_graph_delta_count": 0,
                "executable_search_projection_graph_delta_operations": 2,
                "admitted_search_projection_graph_delta_operations": 2,
                "max_search_projection_graph_delta_complete_through_graph_commit_epoch": 7,
                "memory_pressure": {
                    "ready": true,
                    "budget_bytes": 4096,
                    "estimated_bytes": 1024
                },
                "blocker_codes": []
            }))
            .unwrap(),
        )
        .unwrap();

        let report = blackbox_report(&BlackboxReportOptions {
            artifact_dir,
            output_dir: root.join("blackbox"),
            run_id: Some("run-readiness".to_string()),
            run_status: BlackboxRunStatus::Completed,
            exit_code: Some(0),
        })
        .unwrap();
        let readiness = report.readiness();
        let json = readiness.json();

        assert_eq!(readiness.protocol, BLACKBOX_REPORT_PROTOCOL);
        assert!(readiness.present);
        assert!(readiness.ready);
        assert!(readiness.redaction_ready);
        assert!(readiness.operational_evidence_ready);
        assert!(readiness.slow_query_log_present);
        assert!(readiness.slow_query_log_jsonl_summary_present);
        assert!(readiness.background_maintenance_present);
        assert!(readiness.background_qos_summary_ready);
        assert!(readiness.blocker_codes.is_empty());
        assert_eq!(json["ready"], true);
        assert_eq!(json["blocker_codes"], serde_json::json!([]));

        let manifest_readiness = blackbox_readiness_from_manifest_json(&report.json());
        assert_eq!(manifest_readiness, readiness);
    }

    #[test]
    fn blackbox_readiness_report_requires_memory_pressure_qos_evidence() {
        let root = unique_test_dir("blackbox-readiness-memory-pressure");
        let artifact_dir = root.join("artifacts");
        fs::create_dir_all(&artifact_dir).unwrap();
        fs::write(
            artifact_dir.join("slow-query-log.jsonl"),
            "{\"event\":\"slow_query\",\"digest\":\"digest-1\"}\n",
        )
        .unwrap();
        fs::write(
            artifact_dir.join("background-maintenance.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "protocol": "skein-background-maintenance-report",
                "ready": true,
                "total_candidates": 1,
                "admitted_count": 1,
                "deferred_count": 0,
                "rejected_count": 0,
                "executable_search_projection_graph_delta_count": 1,
                "admitted_search_projection_graph_delta_count": 1,
                "deferred_search_projection_graph_delta_count": 0,
                "rejected_search_projection_graph_delta_count": 0,
                "executable_search_projection_graph_delta_operations": 2,
                "admitted_search_projection_graph_delta_operations": 2,
                "max_search_projection_graph_delta_complete_through_graph_commit_epoch": 7,
                "blocker_codes": []
            }))
            .unwrap(),
        )
        .unwrap();

        let report = blackbox_report(&BlackboxReportOptions {
            artifact_dir,
            output_dir: root.join("blackbox"),
            run_id: Some("run-memory-pressure".to_string()),
            run_status: BlackboxRunStatus::Completed,
            exit_code: Some(0),
        })
        .unwrap();
        let readiness = report.readiness();

        assert!(!readiness.ready);
        assert!(!readiness.operational_evidence_ready);
        assert!(!readiness.background_qos_summary_ready);
        assert_eq!(
            readiness.blocker_codes,
            vec!["blackbox_background_qos_summary_missing".to_string()]
        );
        let manifest_readiness = blackbox_readiness_from_manifest_json(&report.json());
        assert_eq!(manifest_readiness, readiness);
    }

    #[test]
    fn blackbox_readiness_report_fails_closed_without_operational_evidence() {
        let root = unique_test_dir("blackbox-readiness-missing");
        let artifact_dir = root.join("artifacts");
        fs::create_dir_all(&artifact_dir).unwrap();
        fs::write(
            artifact_dir.join("background-maintenance.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "protocol": "skein-background-maintenance-report",
                "ready": true,
                "total_candidates": 1,
                "admitted_count": 1,
                "deferred_count": 0,
                "rejected_count": 0,
                "blocker_codes": []
            }))
            .unwrap(),
        )
        .unwrap();

        let report = blackbox_report(&BlackboxReportOptions {
            artifact_dir,
            output_dir: root.join("blackbox"),
            run_id: Some("run-readiness-missing".to_string()),
            run_status: BlackboxRunStatus::Completed,
            exit_code: Some(0),
        })
        .unwrap();
        let readiness = report.readiness();

        assert!(!readiness.ready);
        assert!(readiness.redaction_ready);
        assert!(!readiness.operational_evidence_ready);
        assert!(!readiness.slow_query_log_present);
        assert!(!readiness.slow_query_log_jsonl_summary_present);
        assert!(readiness.background_maintenance_present);
        assert!(!readiness.background_qos_summary_ready);
        assert_eq!(
            readiness.blocker_codes,
            vec![
                "blackbox_slow_query_log_missing".to_string(),
                "blackbox_slow_query_log_summary_missing".to_string(),
                "blackbox_background_qos_summary_missing".to_string()
            ]
        );
    }

    fn unique_test_dir(prefix: &str) -> PathBuf {
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("skein-{prefix}-{}-{id}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }
}
