use super::{
    require_bool, require_empty_array, require_nonzero, require_string, required_features,
    validate_common_artifact, validate_exact_binding,
};
use serde_json::Value;
use skein::ProductionQualificationIdentity;
use std::collections::BTreeSet;

const GRAPH_PROTOCOL: &str = "skein-production-graph-storage-qualification-v1";
const SEARCH_PROTOCOL: &str = "skein-production-search-out-of-core-qualification-v1";
const SEARCH_QUALIFICATION_PROTOCOL: &str = "skein-search-lexical-production-qualification";
const STORAGE_PROFILE_PROTOCOL: &str = "skein-storage-resource-profile-v2";
const MINIMUM_SEARCH_DOCUMENT_COUNT: u64 = 100_000;

pub(super) fn validate_graph(
    artifact: &Value,
    expected: &ProductionQualificationIdentity,
) -> Vec<String> {
    let mut blockers = Vec::new();
    validate_common_artifact(
        artifact,
        GRAPH_PROTOCOL,
        "representative_production_replica",
        &mut blockers,
    );
    validate_exact_binding(artifact, "/evidence_binding", expected, &mut blockers);
    require_nonzero(
        artifact,
        "/execution/measurement_runs",
        "graph_measurements_missing",
        &mut blockers,
    );
    super::require_unsigned(
        artifact,
        "/execution/intermediate_rows",
        "graph_intermediate_rows_missing",
        &mut blockers,
    );
    validate_runtime(artifact, "/runtime", &mut blockers);

    let profile = artifact.pointer("/storage_resource_profile");
    let Some(profile) = profile else {
        blockers.push("storage_resource_profile_missing".to_string());
        return deduplicate(blockers);
    };
    require_string(
        profile,
        "/protocol",
        STORAGE_PROFILE_PROTOCOL,
        "storage_profile_protocol_mismatch",
        &mut blockers,
    );
    for (pointer, code) in [
        ("/resource_ready", "storage_resource_not_ready"),
        ("/ready", "storage_production_not_ready"),
        ("/identity_matches_expected", "storage_identity_mismatch"),
        ("/storage/durable", "storage_not_durable"),
        ("/storage/out_of_core", "storage_not_out_of_core"),
        (
            "/storage/canonical_exceeds_cache",
            "canonical_does_not_exceed_cache",
        ),
        ("/storage/delta_within_budget", "storage_delta_over_budget"),
        ("/execution/fully_streamed", "graph_query_not_streamed"),
        (
            "/execution/metric_capabilities/resident_memory",
            "graph_rss_metric_unavailable",
        ),
        (
            "/execution/metric_capabilities/total_page_faults",
            "graph_page_fault_metric_unavailable",
        ),
    ] {
        require_bool(profile, pointer, true, code, &mut blockers);
    }
    require_empty_array(
        profile,
        "/blocker_codes",
        "storage_profile_blockers_present",
        &mut blockers,
    );
    validate_profile_limits(profile, &mut blockers);
    super::graph_resource::validate(artifact, profile, expected, &mut blockers);
    deduplicate(blockers)
}

pub(super) fn validate_search(
    artifact: &Value,
    expected: &ProductionQualificationIdentity,
) -> Vec<String> {
    let mut blockers = Vec::new();
    validate_common_artifact(
        artifact,
        SEARCH_PROTOCOL,
        "representative_production_search_replica",
        &mut blockers,
    );
    let Some(qualification) = artifact.pointer("/qualification") else {
        blockers.push("search_qualification_missing".to_string());
        return deduplicate(blockers);
    };
    require_string(
        qualification,
        "/protocol",
        SEARCH_QUALIFICATION_PROTOCOL,
        "search_qualification_protocol_mismatch",
        &mut blockers,
    );
    if qualification
        .pointer("/protocol_version")
        .and_then(Value::as_u64)
        != Some(2)
    {
        blockers.push("search_qualification_version_mismatch".to_string());
    }
    require_bool(
        qualification,
        "/ready",
        true,
        "search_qualification_not_ready",
        &mut blockers,
    );
    require_empty_array(
        qualification,
        "/blocker_codes",
        "search_qualification_blockers_present",
        &mut blockers,
    );
    validate_exact_binding(qualification, "/evidence_binding", expected, &mut blockers);
    validate_search_identity(qualification, expected, &mut blockers);
    validate_search_coverage(qualification, expected, &mut blockers);
    validate_search_metrics(qualification, &mut blockers);
    validate_search_queries(artifact, expected, &mut blockers);

    for (pointer, code) in [
        (
            "/lifecycle/bounded_generation_update",
            "search_bounded_generation_update_failed",
        ),
        ("/lifecycle/rabitq_serving", "search_rabitq_serving_failed"),
        (
            "/lifecycle/rabitq_preferred_serving",
            "search_rabitq_preferred_serving_failed",
        ),
        (
            "/lifecycle/rabitq_raw_rerank",
            "search_rabitq_raw_rerank_failed",
        ),
        (
            "/lifecycle/rabitq_metadata_filter_pushdown",
            "search_rabitq_filter_pushdown_failed",
        ),
        (
            "/lifecycle/incremental_upsert_delete",
            "search_incremental_lifecycle_failed",
        ),
        (
            "/lifecycle/checkpoint_reopen",
            "search_checkpoint_reopen_failed",
        ),
        (
            "/lifecycle/stale_generation",
            "search_stale_generation_failed",
        ),
        (
            "/lifecycle/corrupt_artifact_rejected",
            "search_corruption_probe_failed",
        ),
        (
            "/lifecycle/mixed_foreground_background",
            "search_mixed_load_failed",
        ),
        (
            "/process_memory/capabilities/resident_memory",
            "search_rss_metric_unavailable",
        ),
        (
            "/process_memory/capabilities/total_page_faults",
            "search_page_fault_metric_unavailable",
        ),
        (
            "/lifecycle/process_memory/capabilities/resident_memory",
            "search_update_rss_metric_unavailable",
        ),
        (
            "/lifecycle/process_memory/capabilities/total_page_faults",
            "search_update_page_fault_metric_unavailable",
        ),
    ] {
        require_bool(artifact, pointer, true, code, &mut blockers);
    }
    if artifact
        .pointer("/lifecycle/max_update_resident_document_count")
        .and_then(Value::as_u64)
        != Some(0)
    {
        blockers.push("search_update_retained_documents".to_string());
    }
    require_nonzero(
        artifact,
        "/lifecycle/max_update_peak_segment_document_bytes",
        "search_update_segment_memory_missing",
        &mut blockers,
    );
    require_nonzero(
        artifact,
        "/lifecycle/rabitq_payload_bytes_read",
        "search_rabitq_payload_bytes_missing",
        &mut blockers,
    );
    require_nonzero(
        artifact,
        "/lifecycle/process_memory/peak_resident_bytes",
        "search_update_peak_rss_missing",
        &mut blockers,
    );
    super::require_unsigned(
        artifact,
        "/lifecycle/process_memory/total_page_faults",
        "search_update_page_faults_missing",
        &mut blockers,
    );
    for (pointer, code) in [
        (
            "/out_of_core_metrics/segment_range_reads",
            "search_segment_reads_missing",
        ),
        (
            "/out_of_core_metrics/segment_bytes_read",
            "search_segment_bytes_missing",
        ),
        (
            "/out_of_core_metrics/hydrated_bytes",
            "search_hydration_bytes_missing",
        ),
    ] {
        require_nonzero(artifact, pointer, code, &mut blockers);
    }
    deduplicate(blockers)
}

fn validate_profile_limits(profile: &Value, blockers: &mut Vec<String>) {
    let pairs = [
        (
            "/storage/canonical_artifact_bytes",
            "/limits/min_canonical_artifact_bytes",
            false,
            "canonical_artifact_below_minimum",
        ),
        (
            "/execution/steady_resident_bytes",
            "/limits/max_steady_resident_bytes",
            true,
            "steady_rss_limit_exceeded",
        ),
        (
            "/execution/peak_resident_bytes",
            "/limits/max_peak_resident_bytes",
            true,
            "peak_rss_limit_exceeded",
        ),
        (
            "/execution/intermediate_rows",
            "/limits/max_intermediate_rows",
            true,
            "intermediate_row_limit_exceeded",
        ),
        (
            "/execution/intermediate_payload_bytes",
            "/limits/max_intermediate_payload_bytes",
            true,
            "intermediate_payload_limit_exceeded",
        ),
        (
            "/execution/output_rows",
            "/limits/max_output_rows",
            true,
            "output_row_limit_exceeded",
        ),
        (
            "/execution/output_payload_bytes",
            "/limits/max_output_payload_bytes",
            true,
            "output_payload_limit_exceeded",
        ),
    ];
    for (actual_pointer, limit_pointer, upper_bound, code) in pairs {
        let actual = profile.pointer(actual_pointer).and_then(Value::as_u64);
        let limit = profile.pointer(limit_pointer).and_then(Value::as_u64);
        if !actual.zip(limit).is_some_and(|(actual, limit)| {
            if upper_bound {
                actual <= limit
            } else {
                actual >= limit
            }
        }) {
            blockers.push(code.to_string());
        }
    }
    for (actual_pointer, limit_pointer, code) in [
        (
            "/execution/total_page_faults",
            "/limits/max_total_page_faults",
            "total_page_fault_limit_exceeded",
        ),
        (
            "/execution/minor_page_faults",
            "/limits/max_minor_page_faults",
            "minor_page_fault_limit_exceeded",
        ),
        (
            "/execution/major_page_faults",
            "/limits/max_major_page_faults",
            "major_page_fault_limit_exceeded",
        ),
    ] {
        if let Some(limit) = profile.pointer(limit_pointer).and_then(Value::as_u64)
            && !profile
                .pointer(actual_pointer)
                .and_then(Value::as_u64)
                .is_some_and(|actual| actual <= limit)
        {
            blockers.push(code.to_string());
        }
    }
}

fn validate_search_identity(
    qualification: &Value,
    expected: &ProductionQualificationIdentity,
    blockers: &mut Vec<String>,
) {
    let count = qualification
        .pointer("/projection_identity/document_count")
        .and_then(Value::as_u64);
    if !count.is_some_and(|count| count >= MINIMUM_SEARCH_DOCUMENT_COUNT) {
        blockers.push("search_dataset_too_small".to_string());
    }
    for (pointer, code) in [
        (
            "/projection_identity/projection_generation",
            "search_projection_generation_missing",
        ),
        (
            "/projection_identity/documents_digest",
            "search_documents_digest_missing",
        ),
        (
            "/projection_identity/analyzer_digest",
            "search_analyzer_digest_missing",
        ),
        (
            "/projection_identity/embedding_dimension",
            "search_embedding_dimension_missing",
        ),
    ] {
        require_nonzero(qualification, pointer, code, blockers);
    }
    for (pointer, code) in [
        (
            "/projection_identity/embedding_model",
            "search_embedding_model_missing",
        ),
        (
            "/projection_identity/embedding_version",
            "search_embedding_version_missing",
        ),
    ] {
        if !qualification
            .pointer(pointer)
            .and_then(Value::as_str)
            .is_some_and(|value| !value.trim().is_empty())
        {
            blockers.push(code.to_string());
        }
    }
    if qualification
        .pointer("/projection_identity/source_graph_commit_epoch")
        .and_then(Value::as_u64)
        != Some(expected.canonical_graph_commit_epoch)
    {
        blockers.push("search_source_graph_epoch_mismatch".to_string());
    }
    for pointer in [
        "/topk_score_parity/text",
        "/topk_score_parity/vector",
        "/topk_score_parity/hybrid",
        "/exact_topk_score_parity",
    ] {
        require_bool(
            qualification,
            pointer,
            true,
            "search_topk_parity_failed",
            blockers,
        );
    }
}

fn validate_search_coverage(
    qualification: &Value,
    expected: &ProductionQualificationIdentity,
    blockers: &mut Vec<String>,
) {
    let mut fields = vec![
        "selective_identifier",
        "cjk_text",
        "common_term",
        "no_hit",
        "metadata_filter",
        "hybrid_rrf",
        "incremental_upsert_delete",
        "checkpoint_reopen",
        "corrupt_artifact",
        "stale_manifest",
        "mixed_foreground_background",
        "larger_than_memory",
    ];
    if required_features(expected).contains("acl") {
        fields.push("acl_filter");
    }
    for field in fields {
        require_bool(
            qualification,
            &format!("/coverage/{field}"),
            true,
            &format!("search_coverage_{field}_missing"),
            blockers,
        );
    }
}

fn validate_search_metrics(qualification: &Value, blockers: &mut Vec<String>) {
    let dataset = qualification
        .pointer("/metrics/canonical_dataset_bytes")
        .and_then(Value::as_u64);
    let budget = qualification
        .pointer("/metrics/storage_memory_budget_bytes")
        .and_then(Value::as_u64);
    if !dataset
        .zip(budget)
        .is_some_and(|(dataset, budget)| budget > 0 && dataset > budget)
    {
        blockers.push("search_corpus_not_larger_than_memory".to_string());
    }
    for (pointer, code) in [
        (
            "/metrics/process_memory_capabilities/resident_memory",
            "search_qualification_rss_metric_unavailable",
        ),
        (
            "/metrics/process_memory_capabilities/total_page_faults",
            "search_qualification_page_fault_metric_unavailable",
        ),
    ] {
        require_bool(qualification, pointer, true, code, blockers);
    }
}

fn validate_search_queries(
    artifact: &Value,
    expected: &ProductionQualificationIdentity,
    blockers: &mut Vec<String>,
) {
    let Some(cases) = artifact
        .pointer("/query_evidence")
        .and_then(Value::as_array)
    else {
        blockers.push("search_query_evidence_missing".to_string());
        return;
    };
    let mut kinds = BTreeSet::new();
    for case in cases {
        if let Some(kind) = case.pointer("/kind").and_then(Value::as_str) {
            kinds.insert(kind);
        }
        require_bool(
            case,
            "/exact_topk_score_parity",
            true,
            "search_query_parity_failed",
            blockers,
        );
        require_nonzero(
            case,
            "/out_of_core_latency/sample_count",
            "search_latency_samples_missing",
            blockers,
        );
    }
    let mut required = vec![
        "selective_identifier",
        "cjk_text",
        "common_term",
        "no_hit",
        "metadata_filter",
        "vector",
        "hybrid",
    ];
    if required_features(expected).contains("acl") {
        required.push("acl_filter");
    }
    for kind in required {
        if !kinds.contains(kind) {
            blockers.push(format!("search_query_case_{kind}_missing"));
        }
    }
}

pub(super) fn validate_runtime(artifact: &Value, pointer: &str, blockers: &mut Vec<String>) {
    let Some(runtime) = artifact.pointer(pointer) else {
        blockers.push("runtime_report_missing".to_string());
        return;
    };
    for field in [
        "admission_waits_delta",
        "admission_rejections_delta",
        "final_active_foreground_tasks",
        "final_active_background_tasks",
        "final_active_blocking_tasks",
        "final_admitted_memory_bytes",
    ] {
        if runtime
            .pointer(&format!("/{field}"))
            .and_then(Value::as_u64)
            != Some(0)
        {
            blockers.push(format!("runtime_{field}_nonzero"));
        }
    }
    require_bool(
        runtime,
        "/final_overcommitted",
        false,
        "runtime_overcommitted",
        blockers,
    );
}

pub(super) fn deduplicate(mut blockers: Vec<String>) -> Vec<String> {
    blockers.sort();
    blockers.dedup();
    blockers
}
