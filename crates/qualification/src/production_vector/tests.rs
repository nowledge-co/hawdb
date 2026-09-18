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

use super::*;
use hawdb::{
    SearchEmbeddingManifest, SearchOutOfCoreGenerationBuildOptions,
    SearchOutOfCoreGenerationWriter, SearchProjectionDelta, SearchProjectionKind,
    SearchProjectionRow, PRODUCTION_QUALIFICATION_POLICY_VERSION,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[test]
fn representative_runner_collects_recall_execution_and_lifecycle_evidence() {
    let root = test_root("runner");
    let source = root.join("source");
    let serving = root.join("serving");
    let replicas = (0..3)
        .map(|index| root.join(format!("replica-{index}")))
        .collect::<Vec<_>>();
    let corruption = root.join("corruption");
    build_projection(&source);
    build_serving_projection(&serving);
    for path in replicas.iter().chain(std::iter::once(&corruption)) {
        copy_projection(&source, path);
    }
    let identity = production_identity(42);
    let report = run_production_vector_qualification(ProductionVectorQualificationConfig {
        projection_path: source.clone(),
        search_projection_path: serving,
        query_cases: query_cases(),
        lifecycle: ProductionVectorLifecycleConfig {
            replica_paths: replicas,
            corruption_replica_path: corruption,
            delta: SearchProjectionDelta {
                upserts: vec![projection_row("added", unit_x(), "a")],
                deletes: vec!["memory:delete".to_string()],
                max_operations: Some(2),
                source_graph_commit_epoch: Some(42),
            },
            verification_case: ProductionVectorQueryCase {
                name: "lifecycle".to_string(),
                kind: ProductionVectorCaseKind::MetadataFiltered,
                query_embedding: unit_x().to_vec(),
                metadata_filters: BTreeMap::from([("group".to_string(), "a".to_string())]),
                access_control: None,
            },
            expected_upsert_document_id: "memory:added".to_string(),
            expected_deleted_document_id: "memory:delete".to_string(),
            mixed_load_probe_runs: 4,
        },
        warmup_runs: 1,
        measurement_runs: 2,
        recall_samples: 2,
        top_k: 2,
        candidate_limit: 4,
        minimum_recall_per_million: 0,
        require_rabitq_reference_verification: true,
        max_parallelism: NonZeroUsize::new(2).unwrap(),
        max_working_bytes: 64 * 1024 * 1024,
        evidence_binding: ProductionEvidenceBinding {
            identity: identity.clone(),
            generated_at_unix_seconds: 1,
        },
        expected_identity: identity,
    })
    .expect("qualification runner should collect evidence");

    assert_eq!(report.query_evidence.len(), 2);
    assert!(report
        .query_evidence
        .iter()
        .all(|evidence| evidence.auto_scalar_candidate_parity));
    assert!(report
        .query_evidence
        .iter()
        .all(|evidence| evidence.serving_auto_final_parity
            && evidence.serving_metrics.backend
                == "hawdb_rabitq_out_of_core_candidate_projection"));
    assert!(report
        .recall_evidence
        .iter()
        .all(|evidence| evidence.report.validates_required_approximate_backend()));
    assert!(report.lifecycle.incremental_fallback_safe);
    assert!(report.lifecycle.checkpoint_reopen_restores_projection);
    assert!(report.lifecycle.stale_generation_isolated);
    assert!(report.lifecycle.corrupt_projection_rejected);
    assert!(report.lifecycle.cancellation_propagated);
    assert!(report.lifecycle.serving_cancellation_propagated);
    assert!(report.lifecycle.mixed_foreground_background);
    assert!(
        report.rabitq_reference_verification.ready,
        "{:?}",
        report.rabitq_reference_verification
    );
    assert_eq!(report.rabitq_reference_verification.cases.len(), 2);
    assert!(report
        .blocker_codes
        .contains(&"dataset_too_small".to_string()));
    assert_eq!(
        report.json()["protocol"],
        PRODUCTION_VECTOR_QUALIFICATION_PROTOCOL
    );

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn acl_case_is_required_only_for_acl_release_identity() {
    let cases = query_cases();
    assert!(validate_query_cases(&cases, &production_identity(42)).is_ok());
    let mut acl_identity = production_identity(42);
    acl_identity.enabled_features.push("acl".to_string());
    acl_identity.enabled_features.sort();

    let error = validate_query_cases(&cases, &acl_identity).unwrap_err();

    assert!(error.to_string().contains("requires an ACL case"));
}

#[test]
fn matrix_requires_every_supported_production_target() {
    let identity = production_identity(42);
    let matrix = ProductionVectorQualificationMatrixReport::evaluate(
        Vec::new(),
        ProductionVectorMatrixExpectation::from_identity(&identity),
    );

    assert!(!matrix.ready);
    assert!(matrix
        .blocker_codes
        .contains(&"missing_target_linux_aarch64".to_string()));
    assert!(matrix
        .blocker_codes
        .contains(&"missing_target_linux_x86_64".to_string()));
    assert!(matrix
        .blocker_codes
        .contains(&"missing_target_macos_aarch64".to_string()));
    assert!(matrix
        .blocker_codes
        .contains(&"missing_target_windows_x86_64".to_string()));
}

#[test]
fn lifecycle_rejects_source_path_alias_for_destructive_corruption_probe() {
    let root = test_root("source-alias");
    let source = root.join("source");
    let replicas = (0..3)
        .map(|index| root.join(format!("replica-{index}")))
        .collect::<Vec<_>>();
    for path in std::iter::once(&source).chain(replicas.iter()) {
        std::fs::create_dir_all(path).unwrap();
    }
    let lifecycle = ProductionVectorLifecycleConfig {
        replica_paths: replicas,
        corruption_replica_path: source.join("..").join("source"),
        delta: SearchProjectionDelta {
            upserts: vec![projection_row("added", unit_x(), "a")],
            deletes: vec!["memory:delete".to_string()],
            max_operations: Some(2),
            source_graph_commit_epoch: Some(42),
        },
        verification_case: ProductionVectorQueryCase {
            name: "lifecycle".to_string(),
            kind: ProductionVectorCaseKind::MetadataFiltered,
            query_embedding: unit_x().to_vec(),
            metadata_filters: BTreeMap::from([("group".to_string(), "a".to_string())]),
            access_control: None,
        },
        expected_upsert_document_id: "memory:added".to_string(),
        expected_deleted_document_id: "memory:delete".to_string(),
        mixed_load_probe_runs: 1,
    };

    let error = lifecycle
        .validate(&source, &production_identity(42))
        .unwrap_err();

    assert!(error.to_string().contains("separate from the source"));
    std::fs::remove_dir_all(root).unwrap();
}

fn build_projection(path: &Path) {
    let mut index = SearchIndex::open(path).unwrap();
    index
        .apply_embedding_manifest(SearchEmbeddingManifest {
            model: "test-embedding".to_string(),
            version: Some("v1".to_string()),
            dimension: 8,
        })
        .unwrap();
    index
        .apply_projection_delta(SearchProjectionDelta {
            upserts: projection_rows(),
            deletes: Vec::new(),
            max_operations: Some(4),
            source_graph_commit_epoch: Some(42),
        })
        .unwrap();
    index.checkpoint().unwrap();
}

fn build_serving_projection(path: &Path) {
    let embedding_manifest = SearchEmbeddingManifest {
        model: "test-embedding".to_string(),
        version: Some("v1".to_string()),
        dimension: 8,
    };
    let mut documents = projection_rows()
        .into_iter()
        .map(SearchProjectionRow::into_document)
        .collect::<Vec<_>>();
    documents.sort_unstable_by(|left, right| left.id.cmp(&right.id));
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        path,
        SearchOutOfCoreGenerationBuildOptions {
            source_graph_commit_epoch: Some(42),
            embedding_manifest: Some(embedding_manifest),
            ..SearchOutOfCoreGenerationBuildOptions::default()
        },
    )
    .unwrap();
    for document in documents {
        writer.push(document).unwrap();
    }
    writer.finish().unwrap();
}

fn projection_rows() -> Vec<SearchProjectionRow> {
    vec![
        projection_row("delete", unit_x(), "a"),
        projection_row("keep-a", [0.95, 0.05, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], "a"),
        projection_row("keep-b", unit_y(), "b"),
        projection_row("other-b", [0.1, 0.9, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], "b"),
    ]
}

fn copy_projection(source: &Path, destination: &Path) {
    std::fs::create_dir_all(destination).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() {
            std::fs::copy(entry.path(), destination.join(entry.file_name())).unwrap();
        }
    }
}

fn query_cases() -> Vec<ProductionVectorQueryCase> {
    vec![
        ProductionVectorQueryCase {
            name: "unfiltered".to_string(),
            kind: ProductionVectorCaseKind::Unfiltered,
            query_embedding: unit_x().to_vec(),
            metadata_filters: BTreeMap::new(),
            access_control: None,
        },
        ProductionVectorQueryCase {
            name: "metadata".to_string(),
            kind: ProductionVectorCaseKind::MetadataFiltered,
            query_embedding: unit_x().to_vec(),
            metadata_filters: BTreeMap::from([("group".to_string(), "a".to_string())]),
            access_control: None,
        },
    ]
}

fn projection_row(external_id: &str, embedding: [f32; 8], group: &str) -> SearchProjectionRow {
    SearchProjectionRow {
        kind: SearchProjectionKind::Memory,
        external_id: external_id.to_string(),
        title: external_id.to_string(),
        body: "representative vector qualification".to_string(),
        embedding: Some(embedding.to_vec()),
        source_id: None,
        metadata: BTreeMap::from([("group".to_string(), group.to_string())]),
    }
}

fn unit_x() -> [f32; 8] {
    [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
}

fn unit_y() -> [f32; 8] {
    [0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
}

fn production_identity(epoch: u64) -> ProductionQualificationIdentity {
    ProductionQualificationIdentity {
        source_revision: "test-revision".to_string(),
        rust_toolchain: "test-toolchain".to_string(),
        target_os: std::env::consts::OS.to_string(),
        target_arch: std::env::consts::ARCH.to_string(),
        enabled_features: vec!["full-text-search".to_string(), "vector-search".to_string()],
        durable_format_version: 1,
        schema_version: 1,
        configuration_digest: "test-configuration".to_string(),
        deployment_profile: "test".to_string(),
        dataset_fingerprint: "test-dataset".to_string(),
        canonical_graph_commit_epoch: epoch,
        policy_version: PRODUCTION_QUALIFICATION_POLICY_VERSION,
    }
}

fn test_root(name: &str) -> PathBuf {
    let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "hawdb-production-vector-qualification-{name}-{}-{sequence}",
        std::process::id()
    ))
}
