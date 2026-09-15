#![deny(unsafe_code)]

//! Contracts between the embedded database and external artifact runtimes.

use skein_core::Value;
use skein_executor::{QueryOutput, Row};
use skein_qos::{WorkClass, WorkRequest};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DerivedArtifactJobStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
}

impl DerivedArtifactJobStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedArtifactJob {
    pub id: u64,
    pub artifact_type: String,
    pub name: String,
    pub action: String,
    pub payload: BTreeMap<String, Value>,
    pub status: DerivedArtifactJobStatus,
    pub attempts: u32,
    pub last_error: Option<String>,
    pub last_output: Option<QueryOutput>,
}

impl DerivedArtifactJob {
    pub fn background_work_request(&self, estimated_operations: usize) -> WorkRequest {
        let class = if self.artifact_type == "projected_graph" {
            WorkClass::Projection
        } else {
            WorkClass::Import
        };
        WorkRequest::background(class, estimated_operations)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedArtifactJobReport {
    pub job: DerivedArtifactJob,
    pub output: QueryOutput,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalContentArtifactJobCompletion {
    pub runtime_name: String,
    pub runtime_version: Option<String>,
    pub input_ref: Option<String>,
    pub input_checksum: Option<String>,
    pub output_ref: Option<String>,
    pub output_checksum: Option<String>,
    pub projection_kind: Option<String>,
    pub projection_ref: Option<String>,
    pub source_graph_commit_epoch: Option<u64>,
    pub rows_produced: Option<usize>,
    pub metadata: BTreeMap<String, Value>,
}

impl ExternalContentArtifactJobCompletion {
    pub fn new(runtime_name: impl Into<String>) -> Self {
        Self {
            runtime_name: runtime_name.into(),
            runtime_version: None,
            input_ref: None,
            input_checksum: None,
            output_ref: None,
            output_checksum: None,
            projection_kind: None,
            projection_ref: None,
            source_graph_commit_epoch: None,
            rows_produced: None,
            metadata: BTreeMap::new(),
        }
    }

    pub fn with_runtime_version(mut self, runtime_version: impl Into<String>) -> Self {
        self.runtime_version = Some(runtime_version.into());
        self
    }

    pub fn with_input_ref(mut self, input_ref: impl Into<String>) -> Self {
        self.input_ref = Some(input_ref.into());
        self
    }

    pub fn with_input_checksum(mut self, input_checksum: impl Into<String>) -> Self {
        self.input_checksum = Some(input_checksum.into());
        self
    }

    pub fn with_output_ref(mut self, output_ref: impl Into<String>) -> Self {
        self.output_ref = Some(output_ref.into());
        self
    }

    pub fn with_output_checksum(mut self, output_checksum: impl Into<String>) -> Self {
        self.output_checksum = Some(output_checksum.into());
        self
    }

    pub fn with_projection(
        mut self,
        projection_kind: impl Into<String>,
        projection_ref: impl Into<String>,
    ) -> Self {
        self.projection_kind = Some(projection_kind.into());
        self.projection_ref = Some(projection_ref.into());
        self
    }

    pub fn with_source_graph_commit_epoch(mut self, commit_epoch: u64) -> Self {
        self.source_graph_commit_epoch = Some(commit_epoch);
        self
    }

    pub fn with_rows_produced(mut self, rows_produced: usize) -> Self {
        self.rows_produced = Some(rows_produced);
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ExternalContentArtifactJobSummary {
    pub total: usize,
    pub pending: usize,
    pub running: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub pending_by_action: BTreeMap<String, usize>,
    pub failed_by_action: BTreeMap<String, usize>,
    pub next_pending_job_id: Option<u64>,
    pub oldest_failed_job_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalContentArtifactRuntimeManifest {
    pub runtime_name: String,
    pub runtime_version: Option<String>,
    pub supported_actions: BTreeSet<String>,
    pub required_payload_keys: BTreeSet<String>,
    pub estimated_operations: usize,
}

impl ExternalContentArtifactRuntimeManifest {
    pub fn new(runtime_name: impl Into<String>) -> Self {
        Self {
            runtime_name: runtime_name.into(),
            runtime_version: None,
            supported_actions: BTreeSet::new(),
            required_payload_keys: BTreeSet::new(),
            estimated_operations: 1,
        }
    }

    pub fn with_runtime_version(mut self, runtime_version: impl Into<String>) -> Self {
        self.runtime_version = Some(runtime_version.into());
        self
    }

    pub fn with_supported_action(mut self, action: impl Into<String>) -> Self {
        self.supported_actions.insert(action.into());
        self
    }

    pub fn with_required_payload_key(mut self, key: impl Into<String>) -> Self {
        self.required_payload_keys.insert(key.into());
        self
    }

    pub fn with_estimated_operations(mut self, estimated_operations: usize) -> Self {
        self.estimated_operations = estimated_operations;
        self
    }
}

#[doc(hidden)]
pub fn derived_artifact_job_failure_row(job: &DerivedArtifactJob, error: &str) -> Row {
    BTreeMap::from([
        ("job_id".to_string(), Value::Int(job.id as i64)),
        (
            "artifact_type".to_string(),
            Value::String(job.artifact_type.clone()),
        ),
        ("name".to_string(), Value::String(job.name.clone())),
        ("action".to_string(), Value::String(job.action.clone())),
        ("payload".to_string(), Value::Map(job.payload.clone())),
        (
            "status".to_string(),
            Value::String(job.status.as_str().to_string()),
        ),
        ("attempts".to_string(), Value::Int(job.attempts as i64)),
        ("error".to_string(), Value::String(error.to_string())),
    ])
}

#[doc(hidden)]
pub fn external_content_artifact_completion_output(
    job: &DerivedArtifactJob,
    completion: ExternalContentArtifactJobCompletion,
) -> QueryOutput {
    QueryOutput {
        rows: vec![external_content_artifact_completion_row(job, completion)].into(),
    }
}

#[doc(hidden)]
pub fn is_external_content_artifact_job(artifact_type: &str) -> bool {
    matches!(
        artifact_type,
        "content_artifact" | "artifact_parse" | "content_parse" | "blob_parse" | "crawler"
    )
}

#[doc(hidden)]
pub fn external_content_runtime_can_claim(
    manifest: &ExternalContentArtifactRuntimeManifest,
    job: &DerivedArtifactJob,
) -> bool {
    is_external_content_artifact_job(&job.artifact_type)
        && manifest.supported_actions.contains(&job.action)
        && manifest
            .required_payload_keys
            .iter()
            .all(|key| job.payload.contains_key(key))
}

#[doc(hidden)]
pub fn summarize_external_content_artifact_job(
    summary: &mut ExternalContentArtifactJobSummary,
    job: &DerivedArtifactJob,
) {
    summary.total += 1;
    match job.status {
        DerivedArtifactJobStatus::Pending => {
            summary.pending += 1;
            *summary
                .pending_by_action
                .entry(job.action.clone())
                .or_default() += 1;
            summary.next_pending_job_id.get_or_insert(job.id);
        }
        DerivedArtifactJobStatus::Running => {
            summary.running += 1;
        }
        DerivedArtifactJobStatus::Succeeded => {
            summary.succeeded += 1;
        }
        DerivedArtifactJobStatus::Failed => {
            summary.failed += 1;
            *summary
                .failed_by_action
                .entry(job.action.clone())
                .or_default() += 1;
            summary.oldest_failed_job_id.get_or_insert(job.id);
        }
    }
}

fn external_content_artifact_completion_row(
    job: &DerivedArtifactJob,
    completion: ExternalContentArtifactJobCompletion,
) -> Row {
    BTreeMap::from([
        ("job_id".to_string(), Value::Int(job.id as i64)),
        (
            "artifact_type".to_string(),
            Value::String(job.artifact_type.clone()),
        ),
        ("name".to_string(), Value::String(job.name.clone())),
        ("action".to_string(), Value::String(job.action.clone())),
        (
            "runtime_name".to_string(),
            Value::String(completion.runtime_name),
        ),
        (
            "runtime_version".to_string(),
            optional_string_value(completion.runtime_version),
        ),
        (
            "input_ref".to_string(),
            optional_string_value(completion.input_ref),
        ),
        (
            "input_checksum".to_string(),
            optional_string_value(completion.input_checksum),
        ),
        (
            "output_ref".to_string(),
            optional_string_value(completion.output_ref),
        ),
        (
            "output_checksum".to_string(),
            optional_string_value(completion.output_checksum),
        ),
        (
            "projection_kind".to_string(),
            optional_string_value(completion.projection_kind),
        ),
        (
            "projection_ref".to_string(),
            optional_string_value(completion.projection_ref),
        ),
        (
            "source_graph_commit_epoch".to_string(),
            completion
                .source_graph_commit_epoch
                .map_or(Value::Null, |value| Value::Int(value as i64)),
        ),
        (
            "rows_produced".to_string(),
            completion
                .rows_produced
                .map_or(Value::Null, |value| Value::Int(value as i64)),
        ),
        ("metadata".to_string(), Value::Map(completion.metadata)),
    ])
}

fn optional_string_value(value: Option<String>) -> Value {
    value.map(Value::String).unwrap_or(Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(
        id: u64,
        artifact_type: &str,
        action: &str,
        status: DerivedArtifactJobStatus,
    ) -> DerivedArtifactJob {
        DerivedArtifactJob {
            id,
            artifact_type: artifact_type.to_string(),
            name: format!("job-{id}"),
            action: action.to_string(),
            payload: BTreeMap::new(),
            status,
            attempts: 0,
            last_error: None,
            last_output: None,
        }
    }

    #[test]
    fn completion_builder_preserves_provenance_fields() {
        let completion = ExternalContentArtifactJobCompletion::new("parser")
            .with_runtime_version("1.2.3")
            .with_input_ref("file:///input.md")
            .with_input_checksum("sha256:input")
            .with_output_ref("file:///output.json")
            .with_output_checksum("sha256:output")
            .with_projection("search", "search:input")
            .with_source_graph_commit_epoch(7)
            .with_rows_produced(3)
            .with_metadata("format", Value::String("markdown".to_string()));

        assert_eq!(completion.runtime_name, "parser");
        assert_eq!(completion.runtime_version.as_deref(), Some("1.2.3"));
        assert_eq!(completion.source_graph_commit_epoch, Some(7));
        assert_eq!(completion.rows_produced, Some(3));
        assert_eq!(
            completion.metadata.get("format"),
            Some(&Value::String("markdown".to_string()))
        );
    }

    #[test]
    fn runtime_manifest_defaults_and_collects_capabilities() {
        let manifest = ExternalContentArtifactRuntimeManifest::new("parser")
            .with_runtime_version("1.2.3")
            .with_supported_action("parse")
            .with_required_payload_key("content_uri")
            .with_estimated_operations(5);

        assert_eq!(manifest.estimated_operations, 5);
        assert!(manifest.supported_actions.contains("parse"));
        assert!(manifest.required_payload_keys.contains("content_uri"));
        assert_eq!(DerivedArtifactJobStatus::Succeeded.as_str(), "succeeded");
    }

    #[test]
    fn runtime_claim_requires_an_external_type_supported_action_and_payload() {
        let mut parse = job(
            7,
            "content_artifact",
            "parse",
            DerivedArtifactJobStatus::Pending,
        );
        parse.payload.insert(
            "content_uri".to_string(),
            Value::String("file:///input.md".to_string()),
        );
        let manifest = ExternalContentArtifactRuntimeManifest::new("parser")
            .with_supported_action("parse")
            .with_required_payload_key("content_uri");

        assert!(external_content_runtime_can_claim(&manifest, &parse));
        assert_eq!(parse.background_work_request(3).class, WorkClass::Import);

        let projected = job(
            8,
            "projected_graph",
            "rebuild",
            DerivedArtifactJobStatus::Pending,
        );
        assert!(!external_content_runtime_can_claim(&manifest, &projected));
        assert_eq!(
            projected.background_work_request(3).class,
            WorkClass::Projection
        );
    }

    #[test]
    fn external_summary_retains_first_pending_and_failed_job_in_queue_order() {
        let pending = job(
            11,
            "content_artifact",
            "parse",
            DerivedArtifactJobStatus::Pending,
        );
        let failed = job(
            4,
            "content_artifact",
            "crawl",
            DerivedArtifactJobStatus::Failed,
        );
        let mut summary = ExternalContentArtifactJobSummary::default();

        summarize_external_content_artifact_job(&mut summary, &pending);
        summarize_external_content_artifact_job(&mut summary, &failed);

        assert_eq!(summary.total, 2);
        assert_eq!(summary.pending, 1);
        assert_eq!(summary.failed, 1);
        assert_eq!(summary.next_pending_job_id, Some(11));
        assert_eq!(summary.oldest_failed_job_id, Some(4));
        assert_eq!(summary.pending_by_action.get("parse"), Some(&1));
        assert_eq!(summary.failed_by_action.get("crawl"), Some(&1));
    }

    #[test]
    fn completion_and_failure_rows_preserve_the_external_runtime_protocol() {
        let mut completed = job(
            9,
            "content_artifact",
            "parse",
            DerivedArtifactJobStatus::Succeeded,
        );
        completed.payload.insert(
            "content_uri".to_string(),
            Value::String("file:///input.md".to_string()),
        );
        let output = external_content_artifact_completion_output(
            &completed,
            ExternalContentArtifactJobCompletion::new("parser")
                .with_output_ref("artifact://parsed/9")
                .with_rows_produced(2),
        );
        let row = &output.rows[0];
        assert_eq!(
            row.get("runtime_name"),
            Some(&Value::String("parser".to_string()))
        );
        assert_eq!(
            row.get("output_ref"),
            Some(&Value::String("artifact://parsed/9".to_string()))
        );
        assert_eq!(row.get("rows_produced"), Some(&Value::Int(2)));
        assert_eq!(row.get("input_ref"), Some(&Value::Null));

        let mut failed = completed.clone();
        failed.status = DerivedArtifactJobStatus::Failed;
        failed.attempts = 1;
        let failure = derived_artifact_job_failure_row(&failed, "runtime unavailable");
        assert_eq!(failure.get("job_id"), Some(&Value::Int(9)));
        assert_eq!(
            failure.get("status"),
            Some(&Value::String("failed".to_string()))
        );
        assert_eq!(failure.get("attempts"), Some(&Value::Int(1)));
        assert_eq!(
            failure.get("error"),
            Some(&Value::String("runtime unavailable".to_string()))
        );
    }
}
