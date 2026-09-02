use super::*;
use crate::evidence_digest::hash_bytes;
use crate::{
    nowledge_content_store_schema_identity, nowledge_content_store_sql_corpus,
    CONTENT_STORE_512_MIB_CAPABILITY_BYTES, CONTENT_STORE_DESKTOP_8_GIB_BYTES,
    CONTENT_STORE_DESKTOP_MAX_CAPACITY_BYTES,
    PRODUCTION_CONTENT_STORE_MEMORY_QUALIFICATION_PROTOCOL,
    PRODUCTION_CONTENT_STORE_MUTATION_QUALIFICATION_PROTOCOL,
    PRODUCTION_CONTENT_STORE_OVERFLOW_COMPACTION_QUALIFICATION_PROTOCOL,
    PRODUCTION_CONTENT_STORE_STORAGE_QUALIFICATION_PROTOCOL,
    PRODUCTION_CONTENT_STORE_WRITER_MATRIX,
};
use sha2::{Digest, Sha256};
use skein::PRODUCTION_QUALIFICATION_POLICY_VERSION;

#[test]
fn complete_raw_artifact_bundle_is_ready() {
    let expected = identity("linux", "x86_64");
    let artifacts = ProductionReleaseQualificationArtifacts {
        content_store_memory_profiles: Some(content_store_memory_profiles(&expected)),
        content_store_read: Some(content_store_read(&expected)),
        content_store_512_mib_read: Some(content_store_512_mib_read(&expected)),
        content_store_512_mib_overflow_compaction: Some(content_store_overflow_compaction(
            &expected, true,
        )),
        content_store_desktop_overflow_compaction: Some(content_store_overflow_compaction(
            &expected, false,
        )),
        content_store_mutation_matrix: Some(content_store_mutation_matrix(&expected)),
        graph_storage: Some(graph(&expected)),
        graph_index_matrix: Some(graph_index_matrix(&expected)),
        search: Some(search(&expected)),
        vector_targets: [
            ("linux", "x86_64"),
            ("linux", "aarch64"),
            ("macos", "aarch64"),
            ("windows", "x86_64"),
        ]
        .into_iter()
        .map(|(target_os, target_arch)| vector(&identity(target_os, target_arch)))
        .collect(),
        morsel_profiles: [4, 8, 16]
            .into_iter()
            .enumerate()
            .map(|(index, workers)| morsel(&expected, workers, (index + 1) as u64))
            .collect(),
        blocking_operators: Some(blocking(&expected)),
        storage_crash_recovery: Some(crash_recovery(&expected)),
        release_controls: Some(release_controls(&expected)),
    };

    let report = evaluate_production_release_qualification_bundle(
        artifacts,
        expected,
        ProductionReleaseQualificationPolicy::default(),
    );

    assert!(report.ready, "{:?}", report.blocker_codes);
    assert!(report.content_store_memory_profiles.ready);
    assert!(report.content_store_read.ready);
    assert!(report.content_store_512_mib_read.ready);
    assert!(report.content_store_512_mib_overflow_compaction.ready);
    assert!(report.content_store_desktop_overflow_compaction.ready);
    assert!(report.content_store_mutation_matrix.ready);
    assert!(report.graph_storage.ready);
    assert!(report.graph_index_matrix.ready);
    assert!(report.search.ready);
    assert!(report.vector_matrix.ready);
    assert!(report.morsel_matrix.ready);
    assert!(report.blocking_operators.ready);
    assert!(report.storage_crash_recovery.ready);
    assert!(report.release_controls.ready);
    assert_eq!(
        report.json()["protocol"],
        PRODUCTION_RELEASE_QUALIFICATION_BUNDLE_PROTOCOL
    );
}

#[test]
fn memory_profile_top_level_ready_cannot_hide_a_wrong_desktop_budget() {
    let expected = identity("linux", "x86_64");
    let mut artifact = content_store_memory_profiles(&expected);
    artifact["desktop_bound_8_gib"]["memory_budget_bytes"] = serde_json::json!(1);
    let report = evaluate_production_release_qualification_bundle(
        ProductionReleaseQualificationArtifacts {
            content_store_memory_profiles: Some(artifact),
            ..ProductionReleaseQualificationArtifacts::default()
        },
        expected,
        ProductionReleaseQualificationPolicy::default(),
    );

    assert!(!report.content_store_memory_profiles.ready);
    assert!(report
        .content_store_memory_profiles
        .blocker_codes
        .contains(&"content_store_desktop_memory_policy_invalid".to_string()));
}

#[test]
fn memory_profile_matrix_rejects_different_resource_snapshots() {
    let expected = identity("linux", "x86_64");
    let mut artifact = content_store_memory_profiles(&expected);
    artifact["capability_512_mib"]["observed_effective_available_bytes"] =
        serde_json::json!(5_368_709_120_u64);
    let report = evaluate_production_release_qualification_bundle(
        ProductionReleaseQualificationArtifacts {
            content_store_memory_profiles: Some(artifact),
            ..ProductionReleaseQualificationArtifacts::default()
        },
        expected,
        ProductionReleaseQualificationPolicy::default(),
    );

    assert!(!report.content_store_memory_profiles.ready);
    assert!(report
        .content_store_memory_profiles
        .blocker_codes
        .contains(&"content_store_memory_profile_snapshot_mismatch".to_string()));
}

#[test]
fn production_read_cannot_substitute_the_512_mib_capability_run() {
    let expected = identity("linux", "x86_64");
    let report = evaluate_production_release_qualification_bundle(
        ProductionReleaseQualificationArtifacts {
            content_store_read: Some(content_store_512_mib_read(&expected)),
            ..ProductionReleaseQualificationArtifacts::default()
        },
        expected,
        ProductionReleaseQualificationPolicy::default(),
    );

    assert!(!report.content_store_read.ready);
    assert!(report
        .content_store_read
        .blocker_codes
        .contains(&"content_store_production_read_uses_512_mib_capability".to_string()));
}

#[test]
fn configured_production_read_cannot_substitute_the_512_mib_capability_run() {
    let expected = identity("linux", "x86_64");
    let report = evaluate_production_release_qualification_bundle(
        ProductionReleaseQualificationArtifacts {
            content_store_512_mib_read: Some(content_store_read(&expected)),
            ..ProductionReleaseQualificationArtifacts::default()
        },
        expected,
        ProductionReleaseQualificationPolicy::default(),
    );

    assert!(!report.content_store_512_mib_read.ready);
    assert!(report
        .content_store_512_mib_read
        .blocker_codes
        .contains(&"content_store_512_mib_read_profile_missing".to_string()));
}

#[test]
fn overflow_compaction_top_level_ready_cannot_hide_raw_budget_drift() {
    let expected = identity("linux", "x86_64");
    let mut artifact = content_store_overflow_compaction(&expected, true);
    artifact["compaction"]["hydrated_values"] = serde_json::json!(1);
    artifact["artifacts"]["new_artifact_write_amplification_per_million"] = serde_json::json!(1);
    let report = evaluate_production_release_qualification_bundle(
        ProductionReleaseQualificationArtifacts {
            content_store_512_mib_overflow_compaction: Some(artifact),
            ..ProductionReleaseQualificationArtifacts::default()
        },
        expected,
        ProductionReleaseQualificationPolicy::default(),
    );

    assert!(!report.content_store_512_mib_overflow_compaction.ready);
    assert!(report
        .content_store_512_mib_overflow_compaction
        .blocker_codes
        .contains(&"overflow_compaction_rewrite_shape_invalid".to_string()));
    assert!(report
        .content_store_512_mib_overflow_compaction
        .blocker_codes
        .contains(&"overflow_compaction_artifact_budget_invalid".to_string()));
}

#[test]
fn overflow_compaction_profiles_cannot_substitute_for_each_other() {
    let expected = identity("linux", "x86_64");
    let report = evaluate_production_release_qualification_bundle(
        ProductionReleaseQualificationArtifacts {
            content_store_512_mib_overflow_compaction: Some(content_store_overflow_compaction(
                &expected, false,
            )),
            content_store_desktop_overflow_compaction: Some(content_store_overflow_compaction(
                &expected, true,
            )),
            ..ProductionReleaseQualificationArtifacts::default()
        },
        expected,
        ProductionReleaseQualificationPolicy::default(),
    );

    assert!(report
        .content_store_512_mib_overflow_compaction
        .blocker_codes
        .contains(&"overflow_compaction_512_mib_profile_invalid".to_string()));
    assert!(report
        .content_store_desktop_overflow_compaction
        .blocker_codes
        .contains(&"overflow_compaction_desktop_profile_invalid".to_string()));
}

#[test]
fn content_store_read_top_level_ready_cannot_hide_a_raw_io_budget_violation() {
    let expected = identity("linux", "x86_64");
    let mut artifact = content_store_read(&expected);
    artifact["runs"][0]["read"]["execution"]["physical_bytes"] = serde_json::json!(10_001);
    let report = evaluate_production_release_qualification_bundle(
        ProductionReleaseQualificationArtifacts {
            content_store_read: Some(artifact),
            ..ProductionReleaseQualificationArtifacts::default()
        },
        expected,
        ProductionReleaseQualificationPolicy::default(),
    );

    assert!(!report.content_store_read.ready);
    assert!(report
        .content_store_read
        .blocker_codes
        .contains(&"content_store_read_run_budget_exceeded".to_string()));
}

#[test]
fn content_store_mutation_top_level_ready_cannot_hide_recovery_or_latency_drift() {
    let expected = identity("linux", "x86_64");
    let mut artifact = content_store_mutation_matrix(&expected);
    artifact["cases"][0]["wal_replay_open"]["replayed_wal_entries"] = serde_json::json!(0);
    artifact["cases"][0]["overall_commit_latency"]["p95_micros"] = serde_json::json!(99);
    let report = evaluate_production_release_qualification_bundle(
        ProductionReleaseQualificationArtifacts {
            content_store_mutation_matrix: Some(artifact),
            ..ProductionReleaseQualificationArtifacts::default()
        },
        expected,
        ProductionReleaseQualificationPolicy::default(),
    );

    assert!(!report.content_store_mutation_matrix.ready);
    assert!(report
        .content_store_mutation_matrix
        .blocker_codes
        .contains(&"content_store_mutation_wal_replay_invalid".to_string()));
    assert!(report
        .content_store_mutation_matrix
        .blocker_codes
        .contains(&"content_store_mutation_latency_summary_mismatch".to_string()));
}

#[test]
fn content_store_release_gate_recomputes_open_timing_partitions() {
    let expected = identity("linux", "x86_64");
    let mut read = content_store_read(&expected);
    read["opens"][0]["open_timings"]["accounted_micros"] = serde_json::json!(9);
    let mut mutation = content_store_mutation_matrix(&expected);
    mutation["cases"][0]["wal_replay_open"]["open_timings"]["total_open_micros"] =
        serde_json::json!(101);
    let report = evaluate_production_release_qualification_bundle(
        ProductionReleaseQualificationArtifacts {
            content_store_read: Some(read),
            content_store_mutation_matrix: Some(mutation),
            ..ProductionReleaseQualificationArtifacts::default()
        },
        expected,
        ProductionReleaseQualificationPolicy::default(),
    );

    assert!(report
        .content_store_read
        .blocker_codes
        .contains(&"content_store_read_open_timing_invalid".to_string()));
    assert!(report
        .content_store_mutation_matrix
        .blocker_codes
        .contains(&"content_store_mutation_wal_open_timing_invalid".to_string()));
}

#[test]
fn content_store_release_gate_recomputes_bounded_open_payload_cache() {
    let expected = identity("linux", "x86_64");
    let mut read = content_store_read(&expected);
    read["opens"][0]["payload_cache"]["miss_count"] = serde_json::json!(16);
    let report = evaluate_production_release_qualification_bundle(
        ProductionReleaseQualificationArtifacts {
            content_store_read: Some(read),
            ..ProductionReleaseQualificationArtifacts::default()
        },
        expected,
        ProductionReleaseQualificationPolicy::default(),
    );

    assert!(!report.content_store_read.ready);
    assert!(report
        .content_store_read
        .blocker_codes
        .contains(&"content_store_read_open_payload_cache_invalid".to_string()));
}

#[test]
fn content_store_mutation_matrix_rejects_duplicate_writer_shapes() {
    let expected = identity("linux", "x86_64");
    let mut artifact = content_store_mutation_matrix(&expected);
    artifact["cases"][3]["writer_count"] = serde_json::json!(8);
    let report = evaluate_production_release_qualification_bundle(
        ProductionReleaseQualificationArtifacts {
            content_store_mutation_matrix: Some(artifact),
            ..ProductionReleaseQualificationArtifacts::default()
        },
        expected,
        ProductionReleaseQualificationPolicy::default(),
    );

    assert!(!report.content_store_mutation_matrix.ready);
    assert!(report
        .content_store_mutation_matrix
        .blocker_codes
        .contains(&"content_store_mutation_writer_count_duplicate".to_string()));
    assert!(report
        .content_store_mutation_matrix
        .blocker_codes
        .contains(&"content_store_mutation_writer_matrix_incomplete".to_string()));
}

#[test]
fn top_level_ready_cannot_hide_invalid_raw_storage_evidence() {
    let expected = identity("linux", "x86_64");
    let mut artifact = graph(&expected);
    artifact["storage_resource_profile"]["storage"]["canonical_exceeds_cache"] =
        serde_json::json!(false);
    let report = evaluate_production_release_qualification_bundle(
        ProductionReleaseQualificationArtifacts {
            graph_storage: Some(artifact),
            ..ProductionReleaseQualificationArtifacts::default()
        },
        expected,
        ProductionReleaseQualificationPolicy::default(),
    );

    assert!(!report.ready);
    assert!(!report.graph_storage.ready);
    assert!(report
        .graph_storage
        .blocker_codes
        .contains(&"canonical_does_not_exceed_cache".to_string()));
}

#[test]
fn graph_resource_series_cannot_drop_the_cold_run() {
    let expected = identity("linux", "x86_64");
    let mut artifact = graph(&expected);
    artifact["resource_runs"]
        .as_array_mut()
        .expect("resource runs are an array")
        .remove(0);
    let report = evaluate_production_release_qualification_bundle(
        ProductionReleaseQualificationArtifacts {
            graph_storage: Some(artifact),
            ..ProductionReleaseQualificationArtifacts::default()
        },
        expected,
        ProductionReleaseQualificationPolicy::default(),
    );

    assert!(!report.graph_storage.ready);
    assert!(report
        .graph_storage
        .blocker_codes
        .contains(&"graph_resource_run_count_mismatch".to_string()));
}

#[test]
fn graph_resource_summary_cannot_hide_a_cold_run_limit_violation() {
    let expected = identity("linux", "x86_64");
    let mut artifact = graph(&expected);
    artifact["resource_runs"][0]["steady_resident_bytes"] = serde_json::json!(10_001);
    artifact["resource_summary"]["max_steady_resident_bytes"] = serde_json::json!(10_001);
    let report = evaluate_production_release_qualification_bundle(
        ProductionReleaseQualificationArtifacts {
            graph_storage: Some(artifact),
            ..ProductionReleaseQualificationArtifacts::default()
        },
        expected,
        ProductionReleaseQualificationPolicy::default(),
    );

    assert!(!report.graph_storage.ready);
    assert!(report
        .graph_storage
        .blocker_codes
        .contains(&"graph_resource_run_steady_rss_limit_exceeded".to_string()));
}

#[test]
fn windows_graph_resource_evidence_uses_total_page_faults_without_unix_split() {
    let expected = identity("windows", "x86_64");
    let report = evaluate_production_release_qualification_bundle(
        ProductionReleaseQualificationArtifacts {
            graph_storage: Some(graph(&expected)),
            ..ProductionReleaseQualificationArtifacts::default()
        },
        expected,
        ProductionReleaseQualificationPolicy::default(),
    );

    assert!(
        report.graph_storage.ready,
        "{:?}",
        report.graph_storage.blocker_codes
    );
}

#[test]
fn graph_index_matrix_rejects_duplicate_classes_despite_top_level_ready() {
    let expected = identity("linux", "x86_64");
    let mut matrix = graph_index_matrix(&expected);
    matrix["cases"][1]["persistent_index_evidence"]["class"] =
        matrix["cases"][0]["persistent_index_evidence"]["class"].clone();
    let report = evaluate_production_release_qualification_bundle(
        ProductionReleaseQualificationArtifacts {
            graph_index_matrix: Some(matrix),
            ..ProductionReleaseQualificationArtifacts::default()
        },
        expected,
        ProductionReleaseQualificationPolicy::default(),
    );

    assert!(!report.graph_index_matrix.ready);
    assert!(report
        .graph_index_matrix
        .blocker_codes
        .iter()
        .any(|blocker| blocker.contains("duplicate_class")));
}

#[test]
fn graph_index_matrix_revalidates_raw_case_runs() {
    let expected = identity("linux", "x86_64");
    let mut matrix = graph_index_matrix(&expected);
    matrix["cases"][0]["persistent_index_evidence"]["runs"][0]["operation_count"] =
        serde_json::json!(0);
    matrix["cases"][0]["persistent_index_evidence"]["runs"][0]["blocks_read"] =
        serde_json::json!(3);
    matrix["cases"][0]["persistent_index_evidence"]["runs"][0]["bytes_read"] =
        serde_json::json!(257);
    let report = evaluate_production_release_qualification_bundle(
        ProductionReleaseQualificationArtifacts {
            graph_index_matrix: Some(matrix),
            ..ProductionReleaseQualificationArtifacts::default()
        },
        expected,
        ProductionReleaseQualificationPolicy::default(),
    );

    assert!(!report.graph_index_matrix.ready);
    assert!(report
        .graph_index_matrix
        .blocker_codes
        .iter()
        .any(|blocker| blocker.contains("run_operation_missing")));
    assert!(report
        .graph_index_matrix
        .blocker_codes
        .iter()
        .any(|blocker| blocker.contains("run_block_budget_exceeded")));
    assert!(report
        .graph_index_matrix
        .blocker_codes
        .iter()
        .any(|blocker| blocker.contains("run_byte_budget_exceeded")));
}

#[test]
fn vector_matrix_rejects_stale_shared_release_identity() {
    let expected = identity("linux", "x86_64");
    let mut stale = identity("linux", "aarch64");
    stale.source_revision = "stale".to_string();
    let report = evaluate_production_release_qualification_bundle(
        ProductionReleaseQualificationArtifacts {
            vector_targets: vec![vector(&stale)],
            ..ProductionReleaseQualificationArtifacts::default()
        },
        expected,
        ProductionReleaseQualificationPolicy::default(),
    );

    assert!(!report.vector_matrix.ready);
    assert!(report.vector_matrix.target_reports[0]
        .blocker_codes
        .contains(&"release_identity_mismatch".to_string()));
}

#[test]
fn vector_matrix_rejects_a_different_search_generation() {
    let expected = identity("linux", "x86_64");
    let mut vector = vector(&expected);
    vector["search_projection_identity"]["projection_generation"] = serde_json::json!(8);
    let report = evaluate_production_release_qualification_bundle(
        ProductionReleaseQualificationArtifacts {
            search: Some(search(&expected)),
            vector_targets: vec![vector],
            ..ProductionReleaseQualificationArtifacts::default()
        },
        expected,
        ProductionReleaseQualificationPolicy::default(),
    );

    assert!(report.vector_matrix.target_reports[0]
        .blocker_codes
        .contains(&"search_vector_document_identity_mismatch".to_string()));
    assert!(report.vector_matrix.target_reports[0]
        .blocker_codes
        .contains(&"search_vector_generation_mismatch".to_string()));
}

#[test]
fn vector_matrix_rejects_an_unrelated_offline_oracle() {
    let expected = identity("linux", "x86_64");
    let mut vector = vector(&expected);
    vector["oracle_projection_identity"]["transform_seed"] = serde_json::json!(2);
    let report = evaluate_production_release_qualification_bundle(
        ProductionReleaseQualificationArtifacts {
            search: Some(search(&expected)),
            vector_targets: vec![vector],
            ..ProductionReleaseQualificationArtifacts::default()
        },
        expected,
        ProductionReleaseQualificationPolicy::default(),
    );

    assert!(report.vector_matrix.target_reports[0]
        .blocker_codes
        .contains(&"serving_vector_oracle_identity_mismatch".to_string()));
}

#[test]
fn release_controls_reject_a_stale_revision() {
    let expected = identity("linux", "x86_64");
    let mut controls = release_controls(&expected);
    controls["checks"][0]["source_revision"] = serde_json::json!("stale");
    let report = evaluate_production_release_qualification_bundle(
        ProductionReleaseQualificationArtifacts {
            release_controls: Some(controls),
            ..ProductionReleaseQualificationArtifacts::default()
        },
        expected,
        ProductionReleaseQualificationPolicy::default(),
    );

    assert!(report
        .release_controls
        .blocker_codes
        .contains(&"release_control_check_revision_mismatch".to_string()));
}

#[test]
fn crash_recovery_top_level_ready_cannot_hide_an_incomplete_matrix() {
    let expected = identity("linux", "x86_64");
    let mut crash = crash_recovery(&expected);
    crash["cases"].as_array_mut().unwrap().pop();
    crash["case_count"] = serde_json::json!(4);
    let report = evaluate_production_release_qualification_bundle(
        ProductionReleaseQualificationArtifacts {
            storage_crash_recovery: Some(crash),
            ..ProductionReleaseQualificationArtifacts::default()
        },
        expected,
        ProductionReleaseQualificationPolicy::default(),
    );

    assert!(!report.storage_crash_recovery.ready);
    assert!(report
        .storage_crash_recovery
        .blocker_codes
        .contains(&"crash_recovery_matrix_incomplete".to_string()));
}

fn identity(target_os: &str, target_arch: &str) -> ProductionQualificationIdentity {
    ProductionQualificationIdentity {
        source_revision: "a".repeat(40),
        rust_toolchain: "1.97.1".to_string(),
        target_os: target_os.to_string(),
        target_arch: target_arch.to_string(),
        enabled_features: vec![
            "full-text-search".to_string(),
            "tokio-runtime".to_string(),
            "vector-search".to_string(),
        ],
        durable_format_version: 1,
        schema_version: 1,
        configuration_digest: "sha256:config".to_string(),
        deployment_profile: "production".to_string(),
        dataset_fingerprint: "sha256:dataset".to_string(),
        canonical_graph_commit_epoch: 42,
        policy_version: PRODUCTION_QUALIFICATION_POLICY_VERSION,
    }
}

fn binding(identity: &ProductionQualificationIdentity) -> Value {
    serde_json::to_value(ProductionEvidenceBinding {
        identity: identity.clone(),
        generated_at_unix_seconds: 1,
    })
    .unwrap()
}

fn content_store_process() -> Value {
    serde_json::json!({
        "resident_memory_supported": true,
        "total_page_faults_supported": true,
        "split_page_faults_supported": true,
        "start_resident_bytes": 1_000,
        "steady_resident_bytes": 5_000,
        "peak_resident_bytes": 7_000,
        "steady_resident_growth_bytes": 4_000,
        "lifetime_peak_resident_growth_bytes": 6_000,
        "total_page_faults": 10,
        "minor_page_faults": 9,
        "major_page_faults": 1,
    })
}

fn content_store_contract_identity() -> (Value, Value) {
    let corpus = nowledge_content_store_sql_corpus().unwrap();
    (
        serde_json::to_value(corpus.identity()).unwrap(),
        serde_json::to_value(nowledge_content_store_schema_identity()).unwrap(),
    )
}

fn content_store_statement_digest(statement_name: &str, mutation: bool) -> String {
    let corpus = nowledge_content_store_sql_corpus().unwrap();
    let statement = corpus.statement(statement_name).unwrap();
    let domain = if mutation {
        b"skein-production-content-store-mutation-statement-v1".as_slice()
    } else {
        b"skein-production-content-store-statement-v1".as_slice()
    };
    let mut hasher = Sha256::new();
    hash_bytes(&mut hasher, domain);
    hash_bytes(&mut hasher, statement.sql.as_bytes());
    format!("sha256:{:x}", hasher.finalize())
}

fn content_store_memory_profiles(identity: &ProductionQualificationIdentity) -> Value {
    let available_bytes = 6 * 1024 * 1024 * 1024_u64;
    let desktop_budget_bytes = available_bytes / 4;
    serde_json::json!({
        "protocol": PRODUCTION_CONTENT_STORE_MEMORY_QUALIFICATION_PROTOCOL,
        "evidence_kind": "production_content_store_memory_profiles",
        "production_eligible": true,
        "ready": true,
        "blocker_codes": [],
        "evidence_binding": binding(identity),
        "desktop_bound_8_gib": {
            "profile_kind": "desktop_bound8_gib",
            "ready": true,
            "blocker_codes": [],
            "required_effective_limit_bytes": CONTENT_STORE_DESKTOP_8_GIB_BYTES,
            "configured_memory_ceiling_bytes": null,
            "nominal_available_threshold_bytes": 4 * 1024 * 1024 * 1024_u64,
            "nominal_budget_range_observed": true,
            "observed_effective_limit_bytes": CONTENT_STORE_DESKTOP_8_GIB_BYTES,
            "observed_effective_available_bytes": available_bytes,
            "memory_fraction_per_million": 250_000,
            "memory_capacity_bytes": CONTENT_STORE_DESKTOP_MAX_CAPACITY_BYTES,
            "memory_budget_bytes": desktop_budget_bytes,
            "expected_capacity_bytes": CONTENT_STORE_DESKTOP_MAX_CAPACITY_BYTES,
            "expected_dynamic_budget_bytes": desktop_budget_bytes,
        },
        "capability_512_mib": {
            "profile_kind": "capability512_mib",
            "ready": true,
            "blocker_codes": [],
            "required_effective_limit_bytes": null,
            "configured_memory_ceiling_bytes": CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
            "nominal_available_threshold_bytes": null,
            "nominal_budget_range_observed": false,
            "observed_effective_limit_bytes": CONTENT_STORE_DESKTOP_8_GIB_BYTES,
            "observed_effective_available_bytes": available_bytes,
            "memory_fraction_per_million": 250_000,
            "memory_capacity_bytes": CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
            "memory_budget_bytes": CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
            "expected_capacity_bytes": CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
            "expected_dynamic_budget_bytes": CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
        },
    })
}

fn content_store_read(identity: &ProductionQualificationIdentity) -> Value {
    let (corpus, schema) = content_store_contract_identity();
    let digest = "1".repeat(64);
    let statement_digest = content_store_statement_digest("thread_message_anchor_lookup", false);
    let evidence_digest = format!("sha256:{}", "2".repeat(64));
    let residency = serde_json::json!({
        "database_commit_epoch": identity.canonical_graph_commit_epoch,
        "row_serving": true,
        "row_base_generation": 7,
        "row_recovery_delta_generation": null,
        "row_base_commit_epoch": identity.canonical_graph_commit_epoch,
        "row_visible_commit_epoch": identity.canonical_graph_commit_epoch,
        "row_page_artifact_bytes": 2_048,
        "row_root_descriptor_artifact_bytes": 128,
        "row_root_key_artifact_bytes": 128,
        "row_overflow_extent_artifact_bytes": 0,
        "row_overflow_descriptor_artifact_bytes": 0,
        "row_canonical_artifact_bytes": 2_304,
        "row_recovery_delta_artifact_bytes": 0,
        "row_live_entries": 0,
        "row_live_encoded_bytes": 0,
        "row_live_resident_bytes": 0,
        "index_serving": true,
        "index_base_generation": 7,
        "index_recovery_delta_generation": null,
        "index_base_commit_epoch": identity.canonical_graph_commit_epoch,
        "index_visible_commit_epoch": identity.canonical_graph_commit_epoch,
        "index_base_page_count": 4,
        "index_canonical_artifact_bytes": 2_048,
        "index_recovery_delta_artifact_bytes": 0,
        "index_live_entries": 0,
        "index_live_encoded_bytes": 0,
        "segment_cache_capacity_bytes": 1_024,
        "segment_cache_resident_bytes": 512,
        "segment_cache_pinned_bytes": 0,
        "row_index_epoch_aligned": true,
        "row_artifact_exceeds_cache": true,
        "index_artifact_exceeds_cache": true,
    });
    let run = |index: u64, phase: &str, hits: u64, misses: u64| {
        let execution = serde_json::json!({
            "index_runtime_path": "authoritative",
            "row_runtime_path": "row_pages",
            "base_generation": 7,
            "delta_generation": null,
            "base_commit_epoch": identity.canonical_graph_commit_epoch,
            "visible_commit_epoch": identity.canonical_graph_commit_epoch,
            "root_set_digest": evidence_digest,
            "logical_pages": 2,
            "logical_bytes": 256,
            "physical_pages": 1,
            "physical_bytes": 128,
            "cache_hits": hits,
            "cache_misses": misses,
            "cache_admission_rejections": 0,
            "index_logical_pages": 1,
            "index_logical_bytes": 128,
            "index_physical_pages": 1,
            "index_physical_bytes": 128,
            "index_cache_hits": hits,
            "index_cache_misses": misses,
            "index_cache_admission_rejections": 0,
            "overlay_entries": 0,
            "overlay_bytes": 0,
            "rows_visited": 1,
            "intermediate_rows": 1,
            "hydrated_rows": 1,
            "hydrated_compressed_bytes": 128,
            "hydrated_decompressed_bytes": 256,
        });
        let read = serde_json::json!({
            "statement_name": "thread_message_anchor_lookup",
            "phase": if phase == "cold" { "production_cold" } else { "production_warm" },
            "max_rows": 1_000,
            "max_payload_bytes": 8_388_608,
            "output_rows": 1,
            "output_payload_bytes": 128,
            "output_sha256": digest,
            "cache": {
                "hits": hits,
                "misses": misses,
                "insertions": misses,
                "evictions": 0,
                "admission_rejections": 0,
                "resident_bytes_after": 512,
                "pinned_bytes_after": 0,
            },
            "execution": execution,
        });
        serde_json::json!({
            "case_name": "message_lookup",
            "statement_name": "thread_message_anchor_lookup",
            "statement_sha256": statement_digest,
            "parameter_sha256": evidence_digest,
            "expected_output_sha256": digest,
            "run": index,
            "phase": phase,
            "latency_micros": 10,
            "read": read,
            "process": content_store_process(),
        })
    };
    serde_json::json!({
        "protocol": PRODUCTION_CONTENT_STORE_STORAGE_QUALIFICATION_PROTOCOL,
        "evidence_kind": "representative_production_relational_replica",
        "production_eligible": true,
        "ready": true,
        "blocker_codes": [],
        "evidence_binding": binding(identity),
        "corpus": corpus,
        "schema": schema,
        "resource_profile_kind": "configured_workload",
        "configured_available_memory_bytes": 67_108_864,
        "max_relational_hydration_bytes": 1_000,
        "measurement_runs": 2,
        "open_payload_cache_limits": {
            "max_requests": 16,
            "max_resident_bytes": 1_024,
        },
        "resource_limits": {
            "max_steady_resident_bytes": 10_000,
            "max_peak_resident_bytes": 20_000,
            "max_total_page_faults_per_run": 100,
            "max_minor_page_faults_per_run": 100,
            "max_major_page_faults_per_run": 10,
        },
        "read_contracts": [{
            "case_name": "message_lookup",
            "statement_name": "thread_message_anchor_lookup",
            "statement_sha256": statement_digest,
            "parameter_sha256": evidence_digest,
            "expected_output_rows": 1,
            "expected_output_sha256": digest,
            "max_output_rows": 1_000,
            "max_output_payload_bytes": 8_388_608,
            "max_intermediate_rows": 10,
            "max_physical_pages_per_run": 10,
            "max_physical_bytes_per_run": 10_000,
        }],
        "runtime_memory": {
            "host_total_bytes": 67_108_864,
            "host_available_bytes": 50_331_648,
            "cgroup_limit_bytes": null,
            "cgroup_high_bytes": null,
            "cgroup_current_bytes": null,
            "effective_limit_bytes": 67_108_864,
            "effective_available_bytes": 50_331_648,
            "pressure": "normal",
        },
        "runtime_governor": {
            "configured_memory_ceiling_bytes": 33_554_432,
            "memory_fraction_per_million": 800_000,
            "effective_memory_limit_bytes": 67_108_864,
            "effective_available_memory_bytes": 50_331_648,
            "memory_capacity_bytes": 33_554_432,
            "memory_budget_bytes": 25_165_824,
            "result_budget_bytes": 8_388_608,
            "effective_cpu_slots": 4,
            "foreground_io_depth": 2,
            "admissions_delta": 2,
            "admission_waits_delta": 0,
            "admission_rejections_delta": 0,
            "completions_delta": 2,
            "final_active_foreground_tasks": 0,
            "final_active_background_tasks": 0,
            "final_active_blocking_tasks": 0,
            "final_active_cpu_slots": 0,
            "final_active_foreground_io_slots": 0,
            "final_active_background_io_slots": 0,
            "final_admitted_memory_bytes": 0,
            "final_overcommitted": false,
        },
        "lifecycle_process": content_store_process(),
        "initial_residency": residency,
        "final_residency": residency,
        "opens": [{
            "case_name": "message_lookup",
            "latency_micros": 10,
            "open_timings": content_store_open_timings(8, 1),
            "payload_cache": {
                "capacity_bytes": 1_024,
                "resident_bytes": 512,
                "pinned_bytes": 0,
                "hit_count": 1,
                "miss_count": 1,
                "eviction_count": 0,
                "admission_rejection_count": 0,
                "digest_mismatch_count": 0,
                "within_limits": true,
            },
            "recovered_commit_epoch": identity.canonical_graph_commit_epoch,
            "replayed_wal_entries": 0,
            "replayed_wal_bytes": 0,
            "process": content_store_process(),
        }],
        "runs": [run(0, "cold", 0, 1), run(1, "warm", 1, 0)],
    })
}

fn content_store_512_mib_read(identity: &ProductionQualificationIdentity) -> Value {
    let mut artifact = content_store_read(identity);
    artifact["resource_profile_kind"] = serde_json::json!("capability512_mib");
    artifact["configured_available_memory_bytes"] =
        serde_json::json!(CONTENT_STORE_512_MIB_CAPABILITY_BYTES);
    artifact["runtime_memory"]["host_total_bytes"] =
        serde_json::json!(CONTENT_STORE_DESKTOP_8_GIB_BYTES);
    artifact["runtime_memory"]["host_available_bytes"] =
        serde_json::json!(6 * 1024 * 1024 * 1024_u64);
    artifact["runtime_memory"]["effective_limit_bytes"] =
        serde_json::json!(CONTENT_STORE_DESKTOP_8_GIB_BYTES);
    artifact["runtime_memory"]["effective_available_bytes"] =
        serde_json::json!(6 * 1024 * 1024 * 1024_u64);
    artifact["runtime_governor"]["configured_memory_ceiling_bytes"] =
        serde_json::json!(CONTENT_STORE_512_MIB_CAPABILITY_BYTES);
    artifact["runtime_governor"]["effective_memory_limit_bytes"] =
        serde_json::json!(CONTENT_STORE_DESKTOP_8_GIB_BYTES);
    artifact["runtime_governor"]["effective_available_memory_bytes"] =
        serde_json::json!(6 * 1024 * 1024 * 1024_u64);
    artifact["runtime_governor"]["memory_capacity_bytes"] =
        serde_json::json!(CONTENT_STORE_512_MIB_CAPABILITY_BYTES);
    artifact["runtime_governor"]["memory_budget_bytes"] =
        serde_json::json!(CONTENT_STORE_512_MIB_CAPABILITY_BYTES);
    artifact
}

fn overflow_residency(commit_epoch: u64, generation: u64, base_commit_epoch: u64) -> Value {
    serde_json::json!({
        "database_commit_epoch": commit_epoch,
        "row_serving": true,
        "row_materialized_rows_resident": false,
        "row_checkpoint_state_metadata_only": true,
        "row_base_generation": generation,
        "row_recovery_delta_generation": null,
        "row_base_commit_epoch": base_commit_epoch,
        "row_visible_commit_epoch": commit_epoch,
        "row_page_artifact_bytes": 16_384,
        "row_root_descriptor_artifact_bytes": 128,
        "row_root_key_artifact_bytes": 128,
        "row_overflow_extent_artifact_bytes": 4_096,
        "row_overflow_descriptor_artifact_bytes": 128,
        "row_overflow_extent_count": 2,
        "row_canonical_artifact_bytes": 20_736,
        "row_recovery_delta_artifact_bytes": 0,
        "row_live_entries": 0,
        "row_live_encoded_bytes": 0,
        "row_live_resident_bytes": 0,
        "index_serving": true,
        "index_base_generation": generation,
        "index_recovery_delta_generation": null,
        "index_base_commit_epoch": base_commit_epoch,
        "index_visible_commit_epoch": commit_epoch,
        "index_base_page_count": 4,
        "index_canonical_artifact_bytes": 8_192,
        "index_recovery_delta_artifact_bytes": 0,
        "index_live_entries": 0,
        "index_live_encoded_bytes": 0,
        "segment_cache_capacity_bytes": 1_024,
        "segment_cache_resident_bytes": 512,
        "segment_cache_pinned_bytes": 0,
        "row_index_epoch_aligned": true,
        "row_artifact_exceeds_cache": true,
        "index_artifact_exceeds_cache": true,
    })
}

fn overflow_read(
    identity: &ProductionQualificationIdentity,
    phase: &str,
    generation: u64,
    base_commit_epoch: u64,
) -> Value {
    let statement_digest = content_store_statement_digest("thread_message_anchor_lookup", false);
    let parameter_digest = format!("sha256:{}", "2".repeat(64));
    let output_digest = "1".repeat(64);
    let visible_commit_epoch = if phase == "wal_recovery" {
        identity.canonical_graph_commit_epoch + 1
    } else {
        identity.canonical_graph_commit_epoch
    };
    let execution = serde_json::json!({
        "index_runtime_path": "authoritative",
        "row_runtime_path": "snapshot_rows",
        "base_generation": generation,
        "delta_generation": null,
        "base_commit_epoch": base_commit_epoch,
        "visible_commit_epoch": visible_commit_epoch,
        "root_set_digest": parameter_digest,
        "logical_pages": 2,
        "logical_bytes": 256,
        "physical_pages": 1,
        "physical_bytes": 128,
        "cache_hits": 1,
        "cache_misses": 1,
        "cache_admission_rejections": 0,
        "index_logical_pages": 1,
        "index_logical_bytes": 128,
        "index_physical_pages": 1,
        "index_physical_bytes": 128,
        "index_cache_hits": 1,
        "index_cache_misses": 1,
        "index_cache_admission_rejections": 0,
        "overlay_entries": 0,
        "overlay_bytes": 0,
        "rows_visited": 1,
        "intermediate_rows": 1,
        "hydrated_rows": 1,
        "hydrated_compressed_bytes": 128,
        "hydrated_decompressed_bytes": 256,
    });
    let read = serde_json::json!({
        "statement_name": "thread_message_anchor_lookup",
        "phase": phase,
        "max_rows": 1_000,
        "max_payload_bytes": 8_388_608,
        "output_rows": 1,
        "output_payload_bytes": 128,
        "output_sha256": output_digest,
        "cache": {
            "hits": 1,
            "misses": 1,
            "insertions": 1,
            "evictions": 0,
            "admission_rejections": 0,
            "resident_bytes_after": 512,
            "pinned_bytes_after": 0,
        },
        "execution": execution,
    });
    serde_json::json!({
        "case_name": "message_lookup",
        "statement_sha256": statement_digest,
        "parameter_sha256": parameter_digest,
        "expected_output_sha256": output_digest,
        "read": read,
    })
}

fn content_store_overflow_compaction(
    identity: &ProductionQualificationIdentity,
    capability_512_mib: bool,
) -> Value {
    let (corpus, schema) = content_store_contract_identity();
    let profile_kind = if capability_512_mib {
        "capability512_mib"
    } else {
        "desktop_bound8_gib"
    };
    let configured_available_memory_bytes = if capability_512_mib {
        CONTENT_STORE_512_MIB_CAPABILITY_BYTES
    } else {
        CONTENT_STORE_DESKTOP_8_GIB_BYTES
    };
    let memory_capacity_bytes = if capability_512_mib {
        CONTENT_STORE_512_MIB_CAPABILITY_BYTES
    } else {
        CONTENT_STORE_DESKTOP_MAX_CAPACITY_BYTES
    };
    let memory_budget_bytes = if capability_512_mib {
        CONTENT_STORE_512_MIB_CAPABILITY_BYTES
    } else {
        3 * 512 * 1024 * 1024_u64
    };
    let initial_generation = 7;
    let published_generation = 8;
    let cleanup_generation = 9;
    let cleanup_epoch = identity.canonical_graph_commit_epoch + 1;
    let statement_digest = content_store_statement_digest("thread_message_anchor_lookup", false);
    let parameter_digest = format!("sha256:{}", "2".repeat(64));
    let output_digest = "1".repeat(64);
    serde_json::json!({
        "protocol": PRODUCTION_CONTENT_STORE_OVERFLOW_COMPACTION_QUALIFICATION_PROTOCOL,
        "evidence_kind": "representative_production_relational_overflow_compaction",
        "production_eligible": true,
        "ready": true,
        "blocker_codes": [],
        "evidence_binding": binding(identity),
        "corpus": corpus,
        "schema": schema,
        "resource_profile_kind": profile_kind,
        "configured_available_memory_bytes": configured_available_memory_bytes,
        "limits": {
            "process": {
                "max_steady_resident_bytes": memory_capacity_bytes,
                "max_peak_resident_bytes": memory_capacity_bytes,
                "max_total_page_faults_per_run": 100,
                "max_minor_page_faults_per_run": 100,
                "max_major_page_faults_per_run": 10,
            },
            "max_compaction_elapsed_micros": 1_000_000,
            "max_cleanup_elapsed_micros": 1_000_000,
            "max_new_generation_artifact_bytes": 16_384,
            "max_new_artifact_write_amplification_per_million": 3_000_000,
            "min_reclaimable_base_extent_count": 1,
            "min_physically_removed_extent_bytes": 1_024,
        },
        "read_contracts": [{
            "case_name": "message_lookup",
            "statement_name": "thread_message_anchor_lookup",
            "statement_sha256": statement_digest,
            "parameter_sha256": parameter_digest,
            "expected_output_rows": 1,
            "expected_output_sha256": output_digest,
            "max_output_rows": 1_000,
            "max_output_payload_bytes": 8_388_608,
            "max_intermediate_rows": 10,
            "max_physical_pages_per_run": 10,
            "max_physical_bytes_per_run": 10_000,
        }],
        "runtime_memory": {
            "host_total_bytes": CONTENT_STORE_DESKTOP_8_GIB_BYTES,
            "host_available_bytes": 6 * 1024 * 1024 * 1024_u64,
            "cgroup_limit_bytes": null,
            "cgroup_high_bytes": null,
            "cgroup_current_bytes": null,
            "effective_limit_bytes": CONTENT_STORE_DESKTOP_8_GIB_BYTES,
            "effective_available_bytes": 6 * 1024 * 1024 * 1024_u64,
            "pressure": "normal",
        },
        "runtime_governor": {
            "configured_memory_ceiling_bytes": if capability_512_mib {
                Some(CONTENT_STORE_512_MIB_CAPABILITY_BYTES)
            } else {
                None
            },
            "memory_fraction_per_million": 250_000,
            "effective_memory_limit_bytes": CONTENT_STORE_DESKTOP_8_GIB_BYTES,
            "effective_available_memory_bytes": 6 * 1024 * 1024 * 1024_u64,
            "memory_capacity_bytes": memory_capacity_bytes,
            "memory_budget_bytes": memory_budget_bytes,
            "result_budget_bytes": 8_388_608,
            "effective_cpu_slots": 4,
            "foreground_io_depth": 2,
            "admissions_delta": 4,
            "admission_waits_delta": 0,
            "admission_rejections_delta": 0,
            "completions_delta": 4,
            "final_active_foreground_tasks": 0,
            "final_active_background_tasks": 0,
            "final_active_blocking_tasks": 0,
            "final_active_cpu_slots": 0,
            "final_active_foreground_io_slots": 0,
            "final_active_background_io_slots": 0,
            "final_admitted_memory_bytes": 0,
            "final_overcommitted": false,
        },
        "compaction_elapsed_micros": 100,
        "cleanup_elapsed_micros": 100,
        "compaction_process": content_store_process(),
        "compaction_policy": {
            "max_scan_rows": 1_000,
            "max_scan_pages": 1_000,
            "max_scan_bytes": 1_000_000,
            "max_overlay_entries": 1_000,
            "max_overlay_bytes": 16_777_216,
            "max_rewrite_bytes": 16_384,
            "max_sort_memory_bytes": 8_388_608,
            "max_spill_bytes": 134_217_728,
            "max_spill_runs": 32,
            "max_reference_occurrences": 1_000,
            "admission_bytes": 64 * 1024 * 1024_u64,
        },
        "compaction": {
            "source_commit_epoch": identity.canonical_graph_commit_epoch,
            "published_generation": published_generation,
            "tables_scanned": 1,
            "rows_scanned": 10,
            "pages_read": 2,
            "row_bytes_read": 1_024,
            "hydrated_values": 0,
            "overlay_entries": 0,
            "overlay_bytes": 0,
            "reference_occurrences": 4,
            "unique_references": 2,
            "spill_run_count": 1,
            "spill_bytes": 1_024,
            "peak_sort_memory_bytes": 4_096,
            "previous_extent_count": 3,
            "published_extent_count": 2,
            "reclaimable_base_extent_count": 1,
            "new_extent_count": 2,
            "reused_extent_count": 0,
            "copied_base_extent_count": 1,
            "introduced_extent_count": 1,
            "admitted_memory_bytes": 64 * 1024 * 1024_u64,
        },
        "artifacts": {
            "files_before": 10,
            "files_after_compaction": 12,
            "files_after_cleanup": 10,
            "new_generation_artifact_bytes": 8_192,
            "published_live_overflow_extent_bytes": 4_096,
            "new_artifact_write_amplification_per_million": 2_000_000,
            "physically_removed_extent_files": 1,
            "physically_removed_extent_bytes": 4_096,
        },
        "initial_residency": overflow_residency(
            identity.canonical_graph_commit_epoch,
            initial_generation,
            identity.canonical_graph_commit_epoch,
        ),
        "compacted_residency": overflow_residency(
            identity.canonical_graph_commit_epoch,
            published_generation,
            identity.canonical_graph_commit_epoch,
        ),
        "final_residency": overflow_residency(
            cleanup_epoch,
            cleanup_generation,
            cleanup_epoch,
        ),
        "reopened_residency": overflow_residency(
            cleanup_epoch,
            cleanup_generation,
            cleanup_epoch,
        ),
        "reclamation": {
            "current_commit_epoch": cleanup_epoch,
            "checkpoint_epoch": cleanup_epoch,
            "checkpoint_commit_epoch": cleanup_epoch,
            "oldest_reader_commit_epoch": null,
            "safe_reclaim_commit_epoch": identity.canonical_graph_commit_epoch,
            "durable": true,
        },
        "scrub": {
            "generation": cleanup_generation,
            "checked_file_count": 10,
            "checked_bytes": 32_768,
            "sha256_verified_file_count": 10,
            "wal_record_count": 0,
            "wal_bytes": 0,
        },
        "before_reads": [overflow_read(
            identity,
            "production_cold",
            initial_generation,
            identity.canonical_graph_commit_epoch,
        )],
        "compacted_reads": [overflow_read(
            identity,
            "production_warm",
            published_generation,
            identity.canonical_graph_commit_epoch,
        )],
        "reopened_reads": [overflow_read(
            identity,
            "wal_recovery",
            cleanup_generation,
            cleanup_epoch,
        )],
    })
}

fn content_store_open_timings(total_open_micros: u64, wal_replay_micros: u64) -> Value {
    let durable_manifest_open_micros = 1;
    let checkpoint_root_open_micros = 1;
    let post_replay_open_micros = 1;
    let accounted_micros = durable_manifest_open_micros
        + checkpoint_root_open_micros
        + wal_replay_micros
        + post_replay_open_micros;
    serde_json::json!({
        "durable_manifest_open_micros": durable_manifest_open_micros,
        "checkpoint_root_open_micros": checkpoint_root_open_micros,
        "wal_replay_micros": wal_replay_micros,
        "post_replay_open_micros": post_replay_open_micros,
        "accounted_micros": accounted_micros,
        "unaccounted_micros": total_open_micros.saturating_sub(accounted_micros),
        "total_open_micros": total_open_micros,
        "consistent": true,
    })
}

fn content_store_latency(sample_count: usize) -> Value {
    serde_json::json!({
        "sample_count": sample_count,
        "min_micros": 10,
        "p50_micros": 10,
        "p95_micros": 10,
        "p99_micros": 10,
        "max_micros": 10,
    })
}

fn content_store_mutation_storage(commit_epoch: u64, phase: &str) -> Value {
    let dirty = phase == "dirty";
    let recovered = phase == "recovered";
    serde_json::json!({
        "pressure_state": "normal",
        "commit_epoch": commit_epoch,
        "checkpoint_commit_epoch": if phase == "final" { commit_epoch } else { 42 },
        "wal_bytes": if dirty { 1_024 } else { 64 },
        "delta_bytes": if dirty || recovered { 512 } else { 0 },
        "cache_capacity_bytes": 1_024,
        "cache_resident_bytes": 512,
        "cache_pinned_bytes": 0,
        "row_canonical_artifact_bytes": 2_048,
        "row_recovery_delta_entries": if recovered { 1 } else { 0 },
        "row_live_entries": if dirty { 1 } else { 0 },
        "row_live_encoded_bytes": if dirty { 128 } else { 0 },
        "row_live_resident_bytes": if dirty { 128 } else { 0 },
        "index_canonical_artifact_bytes": 2_048,
        "index_recovery_delta_entries": if recovered { 1 } else { 0 },
        "index_live_entries": if dirty { 1 } else { 0 },
        "index_live_encoded_bytes": if dirty { 128 } else { 0 },
        "row_visible_commit_epoch": commit_epoch,
        "index_visible_commit_epoch": commit_epoch,
    })
}

fn content_store_mutation_case(
    identity: &ProductionQualificationIdentity,
    writer_count: usize,
) -> Value {
    let evidence_digest = format!("sha256:{}", "3".repeat(64));
    let verification_statement_digest =
        content_store_statement_digest("thread_message_anchor_lookup", true);
    let output_digest = "4".repeat(64);
    let mut runs = Vec::new();
    for writer_index in 0..writer_count {
        for (operation_index, kind) in ["insert", "update"].into_iter().enumerate() {
            let statement_name = if kind == "insert" {
                "upsert_content_document"
            } else {
                "update_content_document_summary"
            };
            runs.push(serde_json::json!({
                "writer_index": writer_index,
                "operation_index": operation_index,
                "kind": kind,
                "statement_name": statement_name,
                "statement_sha256": content_store_statement_digest(statement_name, true),
                "parameter_sha256": evidence_digest,
                "statement_latency_micros": 10,
                "commit_latency_micros": 10,
            }));
        }
    }
    let operation_count = runs.len();
    let committed_epoch = identity
        .canonical_graph_commit_epoch
        .saturating_add(operation_count as u64);
    let verification = serde_json::json!({
        "case_name": "message_lookup",
        "statement_name": "thread_message_anchor_lookup",
        "statement_sha256": verification_statement_digest,
        "parameter_sha256": evidence_digest,
        "output_rows": 1,
        "output_payload_bytes": 128,
        "output_sha256": output_digest,
    });
    serde_json::json!({
        "writer_count": writer_count,
        "operation_count": operation_count,
        "initial_commit_epoch": identity.canonical_graph_commit_epoch,
        "committed_epoch": committed_epoch,
        "insert_statement_latency": content_store_latency(writer_count),
        "update_statement_latency": content_store_latency(writer_count),
        "insert_commit_latency": content_store_latency(writer_count),
        "update_commit_latency": content_store_latency(writer_count),
        "overall_commit_latency": content_store_latency(operation_count),
        "reference_commit_p95_micros": 10,
        "commit_p95_regression_per_million": 0,
        "max_commit_p95_micros": 20,
        "mutation_elapsed_micros": 100,
        "checkpoint_latency_micros": 100,
        "process": content_store_process(),
        "initial_storage": content_store_mutation_storage(identity.canonical_graph_commit_epoch, "initial"),
        "dirty_storage": content_store_mutation_storage(committed_epoch, "dirty"),
        "recovered_storage": content_store_mutation_storage(committed_epoch, "recovered"),
        "final_storage": content_store_mutation_storage(committed_epoch, "final"),
        "wal_group": {
            "activation": "evidence_validated",
            "delay_policy": "adaptive_fsync",
            "submitted_commits": operation_count,
            "completed_commits": operation_count,
            "group_count": 1,
            "coalescing_wait_count": 0,
            "shared_sync_count": 1,
            "grouped_wal_entries": operation_count,
            "grouped_wal_bytes": 1_024,
            "max_observed_group_entries": operation_count,
            "max_observed_group_bytes": 1_024,
            "total_fsync_micros": 10,
        },
        "wal_replay_open": {
            "total_open_latency_micros": 100,
            "open_timings": content_store_open_timings(80, 50),
            "checkpoint_commit_epoch": identity.canonical_graph_commit_epoch,
            "recovered_commit_epoch": committed_epoch,
            "replayed_wal_entries": operation_count,
            "replayed_wal_bytes": 1_024,
            "torn_tail_ignored": false,
            "torn_tail_repaired": false,
        },
        "manifest_only_open": {
            "total_open_latency_micros": 50,
            "open_timings": content_store_open_timings(40, 0),
            "checkpoint_commit_epoch": committed_epoch,
            "recovered_commit_epoch": committed_epoch,
            "replayed_wal_entries": 0,
            "replayed_wal_bytes": 0,
            "torn_tail_ignored": false,
            "torn_tail_repaired": false,
        },
        "runs": runs,
        "verification_contracts": [{
            "case_name": "message_lookup",
            "statement_name": "thread_message_anchor_lookup",
            "statement_sha256": verification_statement_digest,
            "parameter_sha256": evidence_digest,
            "expected_output_rows": 1,
            "expected_output_sha256": output_digest,
        }],
        "replay_verification": [verification.clone()],
        "checkpoint_verification": [verification],
        "blocker_codes": [],
    })
}

fn content_store_mutation_matrix(identity: &ProductionQualificationIdentity) -> Value {
    let (corpus, schema) = content_store_contract_identity();
    serde_json::json!({
        "protocol": PRODUCTION_CONTENT_STORE_MUTATION_QUALIFICATION_PROTOCOL,
        "evidence_kind": "representative_production_relational_mutation_replicas",
        "production_eligible": true,
        "ready": true,
        "blocker_codes": [],
        "evidence_binding": binding(identity),
        "corpus": corpus,
        "schema": schema,
        "resource_profile_kind": "configured_workload",
        "configured_available_memory_bytes": 67_108_864,
        "resource_limits": {
            "max_steady_resident_bytes": 10_000,
            "max_peak_resident_bytes": 20_000,
            "max_total_page_faults_per_case": 100,
            "max_minor_page_faults_per_case": 100,
            "max_major_page_faults_per_case": 10,
        },
        "max_commit_p95_regression_per_million": 50_000,
        "latency_reference": {
            "source_revision": "accepted-revision",
            "configuration_digest": identity.configuration_digest,
            "dataset_fingerprint": identity.dataset_fingerprint,
            "generated_at_unix_seconds": 1,
        },
        "cases": PRODUCTION_CONTENT_STORE_WRITER_MATRIX
            .into_iter()
            .map(|writers| content_store_mutation_case(identity, writers))
            .collect::<Vec<_>>(),
    })
}

fn release_controls(identity: &ProductionQualificationIdentity) -> Value {
    let digest = format!("sha256:{}", "a".repeat(64));
    serde_json::json!({
        "protocol": PRODUCTION_RELEASE_CONTROL_EVIDENCE_PROTOCOL,
        "evidence_kind": "exact_revision_release_controls",
        "production_eligible": true,
        "ready": true,
        "blocker_codes": [],
        "evidence_binding": binding(identity),
        "source_revision": identity.source_revision,
        "checks": REQUIRED_PRODUCTION_RELEASE_CONTROLS.into_iter().map(|name| {
            serde_json::json!({
                "name": name,
                "source_revision": identity.source_revision,
                "conclusion": "success",
                "artifact_sha256": digest,
            })
        }).collect::<Vec<_>>(),
    })
}

fn crash_recovery(identity: &ProductionQualificationIdentity) -> Value {
    let cases = [
        "before_wal_append",
        "after_wal_append",
        "after_wal_sync",
        "during_checkpoint_publication",
        "after_manifest_publication",
    ]
    .into_iter()
    .map(|point| {
        serde_json::json!({
            "point": point,
            "repetition": 0,
            "process_terminated": true,
            "recovered_batch_present": point != "before_wal_append",
            "whole_batch_recovered": true,
            "commit_epoch": 42,
            "recovered_commit_epoch": 42,
            "replay_lsn_present": true,
            "relationship_endpoints_valid": true,
            "projection_watermark_valid": true,
            "artifact_generation_valid": true,
            "ready": true,
        })
    })
    .collect::<Vec<_>>();
    serde_json::json!({
        "protocol": "skein-storage-crash-recovery-evidence-v1",
        "protocol_version": 1,
        "evidence_binding": binding(identity),
        "expected_identity": identity,
        "required_repetitions": 1,
        "case_count": cases.len(),
        "cases": cases,
        "blocker_codes": [],
        "ready": true,
    })
}

fn runtime() -> Value {
    serde_json::json!({
        "admissions_delta": 100,
        "admission_waits_delta": 0,
        "admission_rejections_delta": 0,
        "completions_delta": 100,
        "cancellations_delta": 1,
        "deadline_exceeded_delta": 0,
        "final_active_foreground_tasks": 0,
        "final_active_background_tasks": 0,
        "final_active_blocking_tasks": 0,
        "final_admitted_memory_bytes": 0,
        "final_overcommitted": false,
    })
}

fn latency() -> Value {
    serde_json::json!({
        "sample_count": 100,
        "min_micros": 1,
        "p50_micros": 10,
        "p95_micros": 20,
        "p99_micros": 30,
        "max_micros": 40,
    })
}

fn graph(identity: &ProductionQualificationIdentity) -> Value {
    let split_page_faults = identity.target_os != "windows";
    let lifecycle_minor_page_faults = split_page_faults.then_some(48u64);
    let lifecycle_major_page_faults = split_page_faults.then_some(2u64);
    let cold_minor_page_faults = split_page_faults.then_some(10u64);
    let run_major_page_faults = split_page_faults.then_some(0u64);
    let summary_major_page_faults = split_page_faults.then_some(0u64);
    let profile_minor_page_faults = split_page_faults.then_some(10u64);
    let profile_major_page_faults = split_page_faults.then_some(0u64);
    serde_json::json!({
        "protocol": "skein-production-graph-storage-qualification-v1",
        "evidence_kind": "representative_production_replica",
        "production_eligible": true,
        "ready": true,
        "blocker_codes": [],
        "evidence_binding": binding(identity),
        "execution": {
            "measurement_runs": 2,
            "output_rows": 20,
            "output_payload_bytes": 2_000,
            "intermediate_rows": 200_000,
            "intermediate_payload_bytes": 2_000_000,
        },
        "runtime": runtime(),
        "lifecycle_process_memory": {
            "resident_memory_available": true,
            "total_page_faults_available": true,
            "split_page_faults_available": split_page_faults,
            "start_resident_bytes": 1_000,
            "start_peak_resident_bytes": 2_000,
            "steady_resident_bytes": 6_000,
            "peak_resident_bytes": 8_000,
            "steady_resident_growth_bytes": 5_000,
            "lifetime_peak_resident_growth_bytes": 6_000,
            "total_page_faults": 50,
            "minor_page_faults": lifecycle_minor_page_faults,
            "major_page_faults": lifecycle_major_page_faults,
        },
        "resource_runs": [
            {
                "run": 0,
                "phase": "cold",
                "fully_streamed": true,
                "output_rows": 10,
                "output_payload_bytes": 1_000,
                "intermediate_rows": 100_000,
                "intermediate_payload_bytes": 1_000_000,
                "steady_resident_bytes": 5_000,
                "peak_resident_bytes": 7_000,
                "steady_resident_growth_bytes": 500,
                "lifetime_peak_resident_growth_bytes": 1_000,
                "total_page_faults": 10,
                "minor_page_faults": cold_minor_page_faults,
                "major_page_faults": run_major_page_faults,
                "segment_cache_resident_bytes_before": 0,
                "segment_cache_resident_bytes_after": 512,
                "segment_cache_hit_count": 0,
                "segment_cache_miss_count": 1,
                "segment_cache_eviction_count": 0,
                "segment_cache_admission_rejection_count": 0,
            },
            {
                "run": 1,
                "phase": "warm",
                "fully_streamed": true,
                "output_rows": 10,
                "output_payload_bytes": 1_000,
                "intermediate_rows": 100_000,
                "intermediate_payload_bytes": 1_000_000,
                "steady_resident_bytes": 5_000,
                "peak_resident_bytes": 7_000,
                "steady_resident_growth_bytes": 0,
                "lifetime_peak_resident_growth_bytes": 0,
                "total_page_faults": 10,
                "minor_page_faults": cold_minor_page_faults,
                "major_page_faults": run_major_page_faults,
                "segment_cache_resident_bytes_before": 512,
                "segment_cache_resident_bytes_after": 512,
                "segment_cache_hit_count": 1,
                "segment_cache_miss_count": 0,
                "segment_cache_eviction_count": 0,
                "segment_cache_admission_rejection_count": 0,
            }
        ],
        "resource_summary": {
            "measurement_runs": 2,
            "max_steady_resident_bytes": 5_000,
            "max_peak_resident_bytes": 7_000,
            "max_steady_resident_growth_bytes": 500,
            "max_lifetime_peak_resident_growth_bytes": 1_000,
            "total_page_faults": 20,
            "minor_page_faults": split_page_faults.then_some(20u64),
            "major_page_faults": summary_major_page_faults,
            "max_segment_cache_resident_bytes": 512,
            "segment_cache_hit_count": 1,
            "segment_cache_miss_count": 1,
            "segment_cache_eviction_count": 0,
            "segment_cache_admission_rejection_count": 0,
        },
        "storage_resource_profile": {
            "protocol": "skein-storage-resource-profile-v2",
            "resource_ready": true,
            "ready": true,
            "blocker_codes": [],
            "identity_matches_expected": true,
            "limits": {
                "min_canonical_artifact_bytes": 1_000,
                "max_steady_resident_bytes": 10_000,
                "max_peak_resident_bytes": 20_000,
                "max_total_page_faults": 1_000,
                "max_minor_page_faults": null,
                "max_major_page_faults": null,
                "max_intermediate_rows": 200_000,
                "max_intermediate_payload_bytes": 2_000_000,
                "max_output_rows": 100,
                "max_output_payload_bytes": 10_000,
                "require_fully_streamed": true,
            },
            "storage": {
                "durable": true,
                "out_of_core": true,
                "canonical_artifact_bytes": 2_000,
                "canonical_exceeds_cache": true,
                "delta_within_budget": true,
                "segment_cache_capacity_bytes": 1_024,
                "segment_cache_resident_bytes_before": 512,
                "segment_cache_resident_bytes_after": 512,
                "segment_cache_hit_count_delta": 1,
                "segment_cache_miss_count_delta": 0,
                "segment_cache_eviction_count_delta": 0,
                "segment_cache_admission_rejection_count_delta": 0,
            },
            "execution": {
                "fully_streamed": true,
                "steady_resident_bytes": 5_000,
                "peak_resident_bytes": 7_000,
                "total_page_faults": 10,
                "minor_page_faults": profile_minor_page_faults,
                "major_page_faults": profile_major_page_faults,
                "intermediate_rows": 100_000,
                "intermediate_payload_bytes": 1_000_000,
                "output_rows": 10,
                "output_payload_bytes": 1_000,
                "metric_capabilities": {
                    "resident_memory": true,
                    "total_page_faults": true,
                    "split_page_faults": split_page_faults,
                },
            },
        },
    })
}

fn graph_index_matrix(identity: &ProductionQualificationIdentity) -> Value {
    let cases = skein::PersistentGraphIndexClass::ALL
        .into_iter()
        .map(|class| graph_index_case(identity, class.as_str()))
        .collect::<Vec<_>>();
    serde_json::json!({
        "protocol": "skein-production-graph-index-qualification-matrix-v1",
        "evidence_kind": "representative_production_replica",
        "production_eligible": true,
        "ready": true,
        "blocker_codes": [],
        "qualified_class_count": cases.len(),
        "required_class_count": skein::PersistentGraphIndexClass::ALL.len(),
        "cases": cases,
    })
}

fn graph_index_case(identity: &ProductionQualificationIdentity, class: &str) -> Value {
    let mut case = graph(identity);
    let digest = format!("sha256:{}", "0".repeat(64));
    case["persistent_index_evidence"] = serde_json::json!({
        "class": class,
        "reference_output_digest": digest.clone(),
        "observed_output_digest": digest,
        "reference_output_rows": 10,
        "observed_output_rows": 10,
        "exact_result_parity": true,
        "digest_runs": 1,
        "measurement_runs": 2,
        "runs_using_required_class": 2,
        "operation_count": 2,
        "max_blocks_read": 1,
        "max_bytes_read": 128,
        "max_blocks_read_per_run": 2,
        "max_bytes_read_per_run": 256,
        "required_artifact_bytes": 2_048,
        "segment_cache_capacity_bytes": 1_024,
        "artifact_exceeds_cache": true,
        "cold_read_observed": true,
        "warm_read_observed": true,
        "runs": [
            {
                "run": 0,
                "phase": "cold",
                "operation_count": 1,
                "blocks_read": 1,
                "bytes_read": 128,
                "index_cache_hits": 0,
                "index_cache_misses": 1,
                "segment_cache_evictions": 0,
            },
            {
                "run": 1,
                "phase": "warm",
                "operation_count": 1,
                "blocks_read": 1,
                "bytes_read": 128,
                "index_cache_hits": 1,
                "index_cache_misses": 0,
                "segment_cache_evictions": 0,
            }
        ],
        "cancellation": {
            "cancellation_observed": true,
            "latency_micros": 10,
            "max_latency_micros": 100,
            "pinned_bytes_before": 0,
            "pinned_bytes_after": 0,
            "handle_poisoned_after": false,
            "subsequent_read_succeeded": true,
        },
    });
    case
}

fn search(identity: &ProductionQualificationIdentity) -> Value {
    let coverage = serde_json::json!({
        "selective_identifier": true,
        "cjk_text": true,
        "common_term": true,
        "no_hit": true,
        "metadata_filter": true,
        "acl_filter": false,
        "hybrid_rrf": true,
        "bounded_generation_update": true,
        "bounded_rabitq_serving": true,
        "incremental_upsert_delete": true,
        "checkpoint_reopen": true,
        "corrupt_artifact": true,
        "stale_manifest": true,
        "mixed_foreground_background": true,
        "larger_than_memory": true,
    });
    let query = |kind: &str| {
        serde_json::json!({
            "kind": kind,
            "exact_topk_score_parity": true,
            "out_of_core_latency": latency(),
        })
    };
    serde_json::json!({
        "protocol": "skein-production-search-out-of-core-qualification-v1",
        "evidence_kind": "representative_production_search_replica",
        "production_eligible": true,
        "ready": true,
        "blocker_codes": [],
        "qualification": {
            "protocol": "skein-search-lexical-production-qualification",
            "protocol_version": 2,
            "ready": true,
            "blocker_codes": [],
            "evidence_binding": binding(identity),
            "projection_identity": {
                "projection_generation": 7,
                "source_graph_commit_epoch": 42,
                "document_count": 100_000,
                "documents_digest": 1,
                "analyzer_digest": 2,
                "embedding_model": "model",
                "embedding_version": "v1",
                "embedding_dimension": 3,
            },
            "topk_score_parity": {"text": true, "vector": true, "hybrid": true},
            "exact_topk_score_parity": true,
            "coverage": coverage,
            "metrics": {
                "canonical_dataset_bytes": 2_000,
                "storage_memory_budget_bytes": 1_000,
                "process_memory_capabilities": {
                    "resident_memory": true,
                    "total_page_faults": true,
                    "split_page_faults": true,
                },
            },
        },
        "query_evidence": [
            query("selective_identifier"), query("cjk_text"), query("common_term"),
            query("no_hit"), query("metadata_filter"), query("vector"), query("hybrid")
        ],
        "lifecycle": {
            "bounded_generation_update": true,
            "rabitq_serving": true,
            "rabitq_preferred_serving": true,
            "rabitq_raw_rerank": true,
            "rabitq_metadata_filter_pushdown": true,
            "rabitq_payload_bytes_read": 1,
            "incremental_upsert_delete": true,
            "checkpoint_reopen": true,
            "stale_generation": true,
            "corrupt_artifact_rejected": true,
            "mixed_foreground_background": true,
            "max_update_resident_document_count": 0,
            "max_update_peak_segment_document_bytes": 1,
            "process_memory": {
                "capabilities": {"resident_memory": true, "total_page_faults": true},
                "peak_resident_bytes": 1,
                "total_page_faults": 0,
            },
        },
        "process_memory": {
            "capabilities": {"resident_memory": true, "total_page_faults": true},
        },
        "out_of_core_metrics": {
            "segment_range_reads": 1,
            "segment_bytes_read": 1,
            "hydrated_bytes": 1,
        },
    })
}

fn vector(identity: &ProductionQualificationIdentity) -> Value {
    let metrics = |kernel: &str| {
        serde_json::json!({
            "backend": "skein_rabitq_candidate_projection",
            "candidate_score_source": "quantized_projection",
            "final_score_source": "raw_vector",
            "kernel": kernel,
            "max_admitted_workers": 1,
            "segment_count": 1,
            "peak_admitted_working_bytes": 1,
            "fallback_count": 0,
        })
    };
    let serving_metrics = serde_json::json!({
        "backend": "skein_rabitq_out_of_core_candidate_projection",
        "candidate_score_source": "quantized_projection",
        "final_score_source": "raw_vector",
        "kernel": "portable",
        "max_admitted_workers": 1,
        "segment_count": 1,
        "projection_payload_bytes_read": 1,
        "peak_admitted_working_bytes": 1,
        "fallback_count": 0,
    });
    let projection_identity = serde_json::json!({
        "projection_generation": 7,
        "source_graph_commit_epoch": 42,
        "document_count": 100_000,
        "source_digest": 1,
        "payload_bytes": 1,
        "payload_checksum": 1,
        "format_version": 1,
        "algorithm": "rabitq",
        "bit_width": 4,
        "dimension": 3,
        "transform_seed": 1,
        "embedding_model": "model",
        "embedding_version": "v1",
        "file_backed": true,
    });
    serde_json::json!({
        "protocol": "skein-production-vector-qualification-v1",
        "evidence_kind": "representative_production_vector_replica",
        "production_eligible": true,
        "ready": true,
        "blocker_codes": [],
        "evidence_binding": binding(identity),
        "expected_identity": serde_json::to_value(identity).unwrap(),
        "projection_identity": projection_identity.clone(),
        "oracle_projection_identity": projection_identity,
        "search_projection_identity": {
            "projection_generation": 7,
            "source_graph_commit_epoch": 42,
            "document_count": 100_000,
            "documents_digest": 1,
            "analyzer_digest": 2,
            "embedding_model": "model",
            "embedding_version": "v1",
            "embedding_dimension": 3,
        },
        "projection_resources": {
            "segment_count": 1,
            "configured_build_working_bytes": 1,
            "peak_build_working_bytes": 1,
            "raw_vector_bytes": 1,
            "projection_payload_bytes": 1,
        },
        "recall_evidence": [{
            "report": {
                "protocol": "skein-vector-recall-validation-v1",
                "ready": true,
                "approximate_backend": "skein_rabitq_candidate_projection",
                "requested_sample_count": 1,
                "executed_sample_count": 1,
                "minimum_recall_per_million": 950_000,
                "candidate_recall_at_k_per_million": 950_000,
                "recall_at_k_per_million": 950_000,
                "exact_hit_count": 1,
                "candidate_hit_count": 1,
                "fallback_count": 0,
                "index_coverage_incomplete_count": 0,
                "blocker_codes": [],
            },
        }],
        "query_evidence": [{
            "auto_scalar_candidate_parity": true,
            "auto_scalar_final_parity": true,
            "serving_auto_final_parity": true,
            "auto_final_matches_exact": true,
            "scalar_candidate_final_matches_exact": true,
            "exact_latency": latency(),
            "auto_latency": latency(),
            "scalar_candidate_latency": latency(),
            "serving_latency": latency(),
            "auto_metrics": metrics("portable"),
            "scalar_candidate_metrics": metrics("scalar"),
            "serving_metrics": serving_metrics,
        }],
        "rabitq_reference_verification": {
            "required": true,
            "compiled": true,
            "available": true,
            "ready": true,
            "implementation": "native_rabitq_scalar_reference",
            "role": "native_scalar_reference_for_dispatch_parity",
            "bit_width": 4,
            "cases": [{}],
        },
        "lifecycle": {
            "incremental_fallback_safe": true,
            "checkpoint_reopen_restores_projection": true,
            "stale_generation_isolated": true,
            "corrupt_projection_rejected": true,
            "cancellation_propagated": true,
            "serving_cancellation_propagated": true,
            "mixed_foreground_background": true,
            "update_latency": latency(),
            "checkpoint_latency": latency(),
            "reopen_latency": latency(),
            "cancellation_latency": latency(),
            "serving_cancellation_latency": latency(),
        },
        "process_memory": {
            "capabilities": {"resident_memory": true, "total_page_faults": true},
        },
    })
}

fn morsel(identity: &ProductionQualificationIdentity, workers: usize, process_id: u64) -> Value {
    let throughput = u64::try_from(workers).unwrap() * 100;
    serde_json::json!({
        "protocol": "skein-production-morsel-profile-v1",
        "evidence_kind": "representative_production_morsel_profile",
        "production_eligible": true,
        "ready": true,
        "blocker_codes": [],
        "process_id": process_id,
        "expected_workers": workers,
        "query_identity": {"query_digest": "query", "parameter_digest": "params"},
        "evidence_binding": binding(identity),
        "runtime_shape": {"effective_cpu_slots": workers},
        "execution": {
            "warmup_runs": 3,
            "measurement_runs": 100,
            "fully_streamed_runs": 100,
            "morsel_max_admitted_workers": workers,
            "morsel_peak_active_workers": workers,
            "rows_per_second": throughput,
            "peak_resident_bytes": 1_000,
            "latency": latency(),
        },
        "cancellation": {"cancellation_observed": true, "latency_micros": 1},
        "runtime": runtime(),
    })
}

fn blocking(identity: &ProductionQualificationIdentity) -> Value {
    let case = |kind: &str, route: &str| {
        serde_json::json!({
            "route_name": route,
            "operator_kind": kind,
            "ready": true,
            "blocker_codes": [],
            "disposition": "external_spill_observed",
            "input_rows": 100_000,
            "minimum_input_rows": 100_000,
            "budget_bytes": 1_000,
            "peak_tracked_bytes": 900,
            "max_spill_bytes": 10_000,
            "max_spill_runs": 10,
            "spilled_bytes": 5_000,
            "spill_run_count": 5,
            "fully_streamed": true,
        })
    };
    serde_json::json!({
        "protocol": "skein-production-blocking-qualification-v1",
        "evidence_kind": "active_route_blocking_operators",
        "production_eligible": true,
        "ready": true,
        "blocker_codes": [],
        "evidence_binding": binding(identity),
        "cases": [case("distinct", "distinct_route"), case("cartesian_build", "cartesian_route")],
        "spill_pool": {
            "active_bytes": 0,
            "pending_write_bytes": 0,
            "active_runs": 0,
            "orphan_cleanup_failures": 0,
            "run_delete_failures": 0,
        },
        "runtime": runtime(),
    })
}
