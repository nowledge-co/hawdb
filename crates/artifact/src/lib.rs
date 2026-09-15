#![deny(unsafe_code)]

//! Contracts between the embedded database and external artifact runtimes.

use skein_core::Value;
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
