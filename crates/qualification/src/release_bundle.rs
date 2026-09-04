use crate::ProductionMorselMatrixPolicy;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use skein::{ProductionEvidenceBinding, ProductionQualificationIdentity};
use std::collections::BTreeSet;

#[path = "release_bundle/content_store.rs"]
mod content_store;
#[path = "release_bundle/durability.rs"]
mod durability;
#[path = "release_bundle/graph_index.rs"]
mod graph_index;
#[path = "release_bundle/graph_resource.rs"]
mod graph_resource;
#[path = "release_bundle/graph_search.rs"]
mod graph_search;
#[path = "release_bundle/memory.rs"]
mod memory;
#[path = "release_bundle/overflow_compaction.rs"]
mod overflow_compaction;
#[path = "release_bundle/runtime.rs"]
mod runtime;
#[path = "release_bundle/vector.rs"]
mod vector;

pub const PRODUCTION_RELEASE_QUALIFICATION_BUNDLE_PROTOCOL: &str =
    "skein-production-release-qualification-bundle-v1";
pub const PRODUCTION_RELEASE_CONTROL_EVIDENCE_PROTOCOL: &str =
    "skein-production-release-control-evidence-v1";

pub const REQUIRED_PRODUCTION_RELEASE_CONTROLS: [&str; 9] = [
    "workspace_fmt",
    "workspace_tests",
    "workspace_clippy",
    "cross_platform_linux",
    "cross_platform_macos",
    "cross_platform_windows",
    "concurrency_models",
    "bazel_parity",
    "storage_tla_model_check",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct ProductionReleaseControlCheck {
    pub name: String,
    pub source_revision: String,
    pub conclusion: String,
    pub artifact_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct ProductionReleaseControlEvidence {
    pub evidence_binding: ProductionEvidenceBinding,
    pub source_revision: String,
    pub checks: Vec<ProductionReleaseControlCheck>,
}

impl ProductionReleaseControlEvidence {
    pub fn blocker_codes(&self) -> Vec<String> {
        let mut blockers = self
            .evidence_binding
            .validate_for(&self.evidence_binding.identity)
            .err()
            .map(|_| vec!["release_control_evidence_binding_invalid".to_string()])
            .unwrap_or_default();
        if self.source_revision != self.evidence_binding.identity.source_revision
            || !durability::valid_full_source_revision(&self.source_revision)
        {
            blockers.push("release_control_revision_invalid".to_string());
        }
        let mut observed = BTreeSet::new();
        for check in &self.checks {
            if !observed.insert(check.name.as_str()) {
                blockers.push("release_control_duplicate".to_string());
            }
            if check.source_revision != self.source_revision {
                blockers.push("release_control_check_revision_mismatch".to_string());
            }
            if check.conclusion != "success" {
                blockers.push("release_control_check_failed".to_string());
            }
            if !durability::valid_sha256(&check.artifact_sha256) {
                blockers.push("release_control_artifact_digest_invalid".to_string());
            }
        }
        for required in REQUIRED_PRODUCTION_RELEASE_CONTROLS {
            if !observed.contains(required) {
                blockers.push(format!("release_control_{required}_missing"));
            }
        }
        blockers.sort();
        blockers.dedup();
        blockers
    }

    pub fn json(&self) -> Value {
        let blocker_codes = self.blocker_codes();
        serde_json::json!({
            "protocol": PRODUCTION_RELEASE_CONTROL_EVIDENCE_PROTOCOL,
            "evidence_kind": "exact_revision_release_controls",
            "production_eligible": true,
            "ready": blocker_codes.is_empty(),
            "blocker_codes": blocker_codes,
            "evidence_binding": self.evidence_binding,
            "source_revision": self.source_revision,
            "checks": self.checks,
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ProductionReleaseQualificationArtifacts {
    pub content_store_memory_profiles: Option<Value>,
    pub content_store_read: Option<Value>,
    pub content_store_512_mib_read: Option<Value>,
    pub content_store_512_mib_overflow_compaction: Option<Value>,
    pub content_store_shared_host_overflow_compaction: Option<Value>,
    pub content_store_mutation_matrix: Option<Value>,
    pub graph_storage: Option<Value>,
    pub graph_index_matrix: Option<Value>,
    pub search: Option<Value>,
    pub vector_targets: Vec<Value>,
    pub morsel_profiles: Vec<Value>,
    pub blocking_operators: Option<Value>,
    pub storage_crash_recovery: Option<Value>,
    pub release_controls: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ProductionReleaseQualificationPolicy {
    pub morsel: ProductionMorselMatrixPolicy,
    pub require_rabitq_reference_verification: bool,
}

impl Default for ProductionReleaseQualificationPolicy {
    fn default() -> Self {
        Self {
            morsel: ProductionMorselMatrixPolicy {
                min_throughput_gain_per_million: 1_000_001,
                max_p99_regression_per_million: 1_100_000,
                max_peak_rss_regression_per_million: 1_100_000,
                max_cancellation_latency_micros: 100_000,
            },
            require_rabitq_reference_verification: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProductionArtifactAssessment {
    pub evidence_kind: String,
    pub artifact_sha256: Option<String>,
    pub ready: bool,
    pub blocker_codes: Vec<String>,
}

impl ProductionArtifactAssessment {
    fn missing(evidence_kind: &str) -> Self {
        Self {
            evidence_kind: evidence_kind.to_string(),
            artifact_sha256: None,
            ready: false,
            blocker_codes: vec!["artifact_missing".to_string()],
        }
    }

    fn evaluated(evidence_kind: &str, artifact: &Value, mut blocker_codes: Vec<String>) -> Self {
        blocker_codes.sort();
        blocker_codes.dedup();
        Self {
            evidence_kind: evidence_kind.to_string(),
            artifact_sha256: Some(artifact_digest(artifact)),
            ready: blocker_codes.is_empty(),
            blocker_codes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProductionVectorMatrixArtifactAssessment {
    pub ready: bool,
    pub blocker_codes: Vec<String>,
    pub target_reports: Vec<ProductionArtifactAssessment>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProductionMorselMatrixArtifactAssessment {
    pub ready: bool,
    pub blocker_codes: Vec<String>,
    pub profile_reports: Vec<ProductionArtifactAssessment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionReleaseQualificationBundleReport {
    pub ready: bool,
    pub blocker_codes: Vec<String>,
    pub expected_identity: ProductionQualificationIdentity,
    pub policy: ProductionReleaseQualificationPolicy,
    pub content_store_memory_profiles: ProductionArtifactAssessment,
    pub content_store_read: ProductionArtifactAssessment,
    pub content_store_512_mib_read: ProductionArtifactAssessment,
    pub content_store_512_mib_overflow_compaction: ProductionArtifactAssessment,
    pub content_store_shared_host_overflow_compaction: ProductionArtifactAssessment,
    pub content_store_mutation_matrix: ProductionArtifactAssessment,
    pub graph_storage: ProductionArtifactAssessment,
    pub graph_index_matrix: ProductionArtifactAssessment,
    pub search: ProductionArtifactAssessment,
    pub vector_matrix: ProductionVectorMatrixArtifactAssessment,
    pub morsel_matrix: ProductionMorselMatrixArtifactAssessment,
    pub blocking_operators: ProductionArtifactAssessment,
    pub storage_crash_recovery: ProductionArtifactAssessment,
    pub release_controls: ProductionArtifactAssessment,
}

impl ProductionReleaseQualificationBundleReport {
    pub fn json(&self) -> Value {
        serde_json::json!({
            "protocol": PRODUCTION_RELEASE_QUALIFICATION_BUNDLE_PROTOCOL,
            "production_eligible": true,
            "ready": self.ready,
            "blocker_codes": self.blocker_codes,
            "expected_identity": self.expected_identity.json(),
            "policy": self.policy,
            "content_store_memory_profiles": self.content_store_memory_profiles,
            "content_store_read": self.content_store_read,
            "content_store_512_mib_read": self.content_store_512_mib_read,
            "content_store_512_mib_overflow_compaction": self.content_store_512_mib_overflow_compaction,
            "content_store_shared_host_overflow_compaction": self.content_store_shared_host_overflow_compaction,
            "content_store_mutation_matrix": self.content_store_mutation_matrix,
            "graph_storage": self.graph_storage,
            "graph_index_matrix": self.graph_index_matrix,
            "search": self.search,
            "vector_matrix": self.vector_matrix,
            "morsel_matrix": self.morsel_matrix,
            "blocking_operators": self.blocking_operators,
            "storage_crash_recovery": self.storage_crash_recovery,
            "release_controls": self.release_controls,
        })
    }
}

pub fn evaluate_production_release_qualification_bundle(
    artifacts: ProductionReleaseQualificationArtifacts,
    expected_identity: ProductionQualificationIdentity,
    policy: ProductionReleaseQualificationPolicy,
) -> ProductionReleaseQualificationBundleReport {
    let content_store_memory_profiles = evaluate_optional(
        "production_content_store_memory_profiles",
        artifacts.content_store_memory_profiles.as_ref(),
        |artifact| memory::validate_memory_profiles(artifact, &expected_identity),
    );
    let content_store_read = evaluate_optional(
        "representative_production_relational_replica",
        artifacts.content_store_read.as_ref(),
        |artifact| content_store::validate_production_read_storage(artifact, &expected_identity),
    );
    let content_store_512_mib_read = evaluate_optional(
        "content_store_512_mib_read_capability",
        artifacts.content_store_512_mib_read.as_ref(),
        |artifact| content_store::validate_512_mib_read_storage(artifact, &expected_identity),
    );
    let content_store_512_mib_overflow_compaction = evaluate_optional(
        "representative_production_relational_overflow_compaction",
        artifacts.content_store_512_mib_overflow_compaction.as_ref(),
        |artifact| {
            overflow_compaction::validate_512_mib_overflow_compaction(artifact, &expected_identity)
        },
    );
    let content_store_shared_host_overflow_compaction = evaluate_optional(
        "representative_production_relational_overflow_compaction",
        artifacts
            .content_store_shared_host_overflow_compaction
            .as_ref(),
        |artifact| {
            overflow_compaction::validate_shared_host_overflow_compaction(
                artifact,
                &expected_identity,
            )
        },
    );
    let content_store_mutation_matrix = evaluate_optional(
        "representative_production_relational_mutation_replicas",
        artifacts.content_store_mutation_matrix.as_ref(),
        |artifact| content_store::validate_mutation_matrix(artifact, &expected_identity),
    );
    let graph_storage = evaluate_optional(
        "representative_production_replica",
        artifacts.graph_storage.as_ref(),
        |artifact| graph_search::validate_graph(artifact, &expected_identity),
    );
    let graph_index_matrix = evaluate_optional(
        "representative_production_graph_index_matrix",
        artifacts.graph_index_matrix.as_ref(),
        |artifact| graph_index::validate_matrix(artifact, &expected_identity),
    );
    let search = evaluate_optional(
        "representative_production_search_replica",
        artifacts.search.as_ref(),
        |artifact| graph_search::validate_search(artifact, &expected_identity),
    );
    let vector_matrix = vector::evaluate_matrix(
        &artifacts.vector_targets,
        artifacts.search.as_ref(),
        &expected_identity,
        policy,
    );
    let morsel_matrix =
        runtime::evaluate_morsel_matrix(&artifacts.morsel_profiles, &expected_identity, policy);
    let blocking_operators = evaluate_optional(
        "active_route_blocking_operators",
        artifacts.blocking_operators.as_ref(),
        |artifact| runtime::validate_blocking(artifact, &expected_identity),
    );
    let storage_crash_recovery = evaluate_optional(
        "storage_crash_recovery_matrix",
        artifacts.storage_crash_recovery.as_ref(),
        |artifact| durability::validate_crash_recovery(artifact, &expected_identity),
    );
    let release_controls = evaluate_optional(
        "exact_revision_release_controls",
        artifacts.release_controls.as_ref(),
        |artifact| durability::validate_release_controls(artifact, &expected_identity),
    );

    let mut blocker_codes = expected_identity
        .validate()
        .err()
        .map(|_| vec!["expected_identity_invalid".to_string()])
        .unwrap_or_default();
    for (prefix, ready) in [
        (
            "content_store_memory_profiles",
            content_store_memory_profiles.ready,
        ),
        ("content_store_read", content_store_read.ready),
        (
            "content_store_512_mib_read",
            content_store_512_mib_read.ready,
        ),
        (
            "content_store_512_mib_overflow_compaction",
            content_store_512_mib_overflow_compaction.ready,
        ),
        (
            "content_store_shared_host_overflow_compaction",
            content_store_shared_host_overflow_compaction.ready,
        ),
        (
            "content_store_mutation_matrix",
            content_store_mutation_matrix.ready,
        ),
        ("graph_storage", graph_storage.ready),
        ("graph_index_matrix", graph_index_matrix.ready),
        ("search", search.ready),
        ("vector_matrix", vector_matrix.ready),
        ("morsel_matrix", morsel_matrix.ready),
        ("blocking_operators", blocking_operators.ready),
        ("storage_crash_recovery", storage_crash_recovery.ready),
        ("release_controls", release_controls.ready),
    ] {
        if !ready {
            blocker_codes.push(format!("{prefix}_not_ready"));
        }
    }
    blocker_codes.sort();
    blocker_codes.dedup();

    ProductionReleaseQualificationBundleReport {
        ready: blocker_codes.is_empty(),
        blocker_codes,
        expected_identity,
        policy,
        content_store_memory_profiles,
        content_store_read,
        content_store_512_mib_read,
        content_store_512_mib_overflow_compaction,
        content_store_shared_host_overflow_compaction,
        content_store_mutation_matrix,
        graph_storage,
        graph_index_matrix,
        search,
        vector_matrix,
        morsel_matrix,
        blocking_operators,
        storage_crash_recovery,
        release_controls,
    }
}

fn evaluate_optional(
    evidence_kind: &str,
    artifact: Option<&Value>,
    validate: impl FnOnce(&Value) -> Vec<String>,
) -> ProductionArtifactAssessment {
    match artifact {
        Some(artifact) => {
            ProductionArtifactAssessment::evaluated(evidence_kind, artifact, validate(artifact))
        }
        None => ProductionArtifactAssessment::missing(evidence_kind),
    }
}

pub(super) fn validate_common_artifact(
    artifact: &Value,
    protocol: &str,
    evidence_kind: &str,
    blockers: &mut Vec<String>,
) {
    require_string(
        artifact,
        "/protocol",
        protocol,
        "protocol_mismatch",
        blockers,
    );
    require_string(
        artifact,
        "/evidence_kind",
        evidence_kind,
        "evidence_kind_mismatch",
        blockers,
    );
    require_bool(
        artifact,
        "/production_eligible",
        true,
        "production_eligible_missing",
        blockers,
    );
    require_bool(artifact, "/ready", true, "reported_not_ready", blockers);
    require_empty_array(
        artifact,
        "/blocker_codes",
        "reported_blockers_present",
        blockers,
    );
}

pub(super) fn validate_exact_binding(
    artifact: &Value,
    pointer: &str,
    expected: &ProductionQualificationIdentity,
    blockers: &mut Vec<String>,
) -> Option<ProductionEvidenceBinding> {
    let Some(value) = artifact.pointer(pointer) else {
        blockers.push("evidence_binding_missing".to_string());
        return None;
    };
    match serde_json::from_value::<ProductionEvidenceBinding>(value.clone()) {
        Ok(binding) => {
            if binding.validate_for(expected).is_err() {
                blockers.push("evidence_binding_mismatch".to_string());
            }
            Some(binding)
        }
        Err(_) => {
            blockers.push("evidence_binding_invalid".to_string());
            None
        }
    }
}

pub(super) fn parse_binding(
    artifact: &Value,
    pointer: &str,
    blockers: &mut Vec<String>,
) -> Option<ProductionEvidenceBinding> {
    let Some(value) = artifact.pointer(pointer) else {
        blockers.push("evidence_binding_missing".to_string());
        return None;
    };
    match serde_json::from_value::<ProductionEvidenceBinding>(value.clone()) {
        Ok(binding) => {
            if binding.identity.validate().is_err() || binding.generated_at_unix_seconds == 0 {
                blockers.push("evidence_binding_invalid".to_string());
            }
            Some(binding)
        }
        Err(_) => {
            blockers.push("evidence_binding_invalid".to_string());
            None
        }
    }
}

pub(super) fn shared_identity_matches(
    left: &ProductionQualificationIdentity,
    right: &ProductionQualificationIdentity,
) -> bool {
    left.source_revision == right.source_revision
        && left.rust_toolchain == right.rust_toolchain
        && left.enabled_features == right.enabled_features
        && left.durable_format_version == right.durable_format_version
        && left.schema_version == right.schema_version
        && left.configuration_digest == right.configuration_digest
        && left.deployment_profile == right.deployment_profile
        && left.dataset_fingerprint == right.dataset_fingerprint
        && left.canonical_graph_commit_epoch == right.canonical_graph_commit_epoch
        && left.policy_version == right.policy_version
}

pub(super) fn require_bool(
    artifact: &Value,
    pointer: &str,
    expected: bool,
    code: &str,
    blockers: &mut Vec<String>,
) {
    if artifact.pointer(pointer).and_then(Value::as_bool) != Some(expected) {
        blockers.push(code.to_string());
    }
}

pub(super) fn require_string(
    artifact: &Value,
    pointer: &str,
    expected: &str,
    code: &str,
    blockers: &mut Vec<String>,
) {
    if artifact.pointer(pointer).and_then(Value::as_str) != Some(expected) {
        blockers.push(code.to_string());
    }
}

pub(super) fn require_nonzero(
    artifact: &Value,
    pointer: &str,
    code: &str,
    blockers: &mut Vec<String>,
) {
    if artifact.pointer(pointer).and_then(Value::as_u64) == Some(0)
        || artifact.pointer(pointer).and_then(Value::as_u64).is_none()
    {
        blockers.push(code.to_string());
    }
}

pub(super) fn require_unsigned(
    artifact: &Value,
    pointer: &str,
    code: &str,
    blockers: &mut Vec<String>,
) {
    if artifact.pointer(pointer).and_then(Value::as_u64).is_none() {
        blockers.push(code.to_string());
    }
}

pub(super) fn require_empty_array(
    artifact: &Value,
    pointer: &str,
    code: &str,
    blockers: &mut Vec<String>,
) {
    if !artifact
        .pointer(pointer)
        .and_then(Value::as_array)
        .is_some_and(Vec::is_empty)
    {
        blockers.push(code.to_string());
    }
}

pub(super) fn required_features(expected: &ProductionQualificationIdentity) -> BTreeSet<&str> {
    expected
        .enabled_features
        .iter()
        .map(String::as_str)
        .collect()
}

fn artifact_digest(artifact: &Value) -> String {
    let bytes = serde_json::to_vec(artifact).expect("JSON value serialization cannot fail");
    format!("sha256:{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
#[path = "release_bundle/tests.rs"]
mod tests;
