use super::{
    parse_binding, require_bool, require_empty_array, require_nonzero, require_string,
    require_unsigned, shared_identity_matches, validate_common_artifact,
    ProductionArtifactAssessment, ProductionReleaseQualificationPolicy,
    ProductionVectorMatrixArtifactAssessment,
};
use crate::PRODUCTION_VECTOR_QUALIFICATION_PROTOCOL;
use serde_json::Value;
use skein::ProductionQualificationIdentity;
use std::collections::BTreeSet;

const REQUIRED_TARGETS: [(&str, &str); 4] = [
    ("linux", "aarch64"),
    ("linux", "x86_64"),
    ("macos", "aarch64"),
    ("windows", "x86_64"),
];
const MINIMUM_VECTOR_DOCUMENT_COUNT: u64 = 100_000;
const RECALL_PROTOCOL: &str = "skein-vector-recall-validation-v1";
const RABITQ_BACKEND: &str = "skein_rabitq_candidate_projection";

pub(super) fn evaluate_matrix(
    artifacts: &[Value],
    search_artifact: Option<&Value>,
    expected: &ProductionQualificationIdentity,
    policy: ProductionReleaseQualificationPolicy,
) -> ProductionVectorMatrixArtifactAssessment {
    let mut matrix_blockers = Vec::new();
    let mut targets = BTreeSet::new();
    let mut projection_identity = None;
    let mut target_reports = Vec::with_capacity(artifacts.len());
    for artifact in artifacts {
        let mut blockers = validate_target(artifact, search_artifact, expected, policy);
        let target = artifact
            .pointer("/evidence_binding/identity")
            .and_then(|identity| {
                Some((
                    identity.pointer("/target_os")?.as_str()?.to_string(),
                    identity.pointer("/target_arch")?.as_str()?.to_string(),
                ))
            });
        match target {
            Some(target) if targets.insert(target.clone()) => {}
            Some(_) => blockers.push("duplicate_target_evidence".to_string()),
            None => blockers.push("target_identity_missing".to_string()),
        }
        if let Some(identity) = artifact.pointer("/projection_identity") {
            match &projection_identity {
                Some(expected) if expected != identity => {
                    blockers.push("projection_identity_mismatch".to_string());
                }
                None => projection_identity = Some(identity.clone()),
                Some(_) => {}
            }
        }
        let assessment = ProductionArtifactAssessment::evaluated(
            "representative_production_vector_replica",
            artifact,
            blockers,
        );
        if !assessment.ready {
            matrix_blockers.push("target_not_ready".to_string());
        }
        target_reports.push(assessment);
    }
    for (target_os, target_arch) in REQUIRED_TARGETS {
        if !targets.contains(&(target_os.to_string(), target_arch.to_string())) {
            matrix_blockers.push(format!("missing_target_{target_os}_{target_arch}"));
        }
    }
    matrix_blockers.sort();
    matrix_blockers.dedup();
    ProductionVectorMatrixArtifactAssessment {
        ready: matrix_blockers.is_empty(),
        blocker_codes: matrix_blockers,
        target_reports,
    }
}

fn validate_target(
    artifact: &Value,
    search_artifact: Option<&Value>,
    expected: &ProductionQualificationIdentity,
    policy: ProductionReleaseQualificationPolicy,
) -> Vec<String> {
    let mut blockers = Vec::new();
    validate_common_artifact(
        artifact,
        PRODUCTION_VECTOR_QUALIFICATION_PROTOCOL,
        "representative_production_vector_replica",
        &mut blockers,
    );
    let binding = parse_binding(artifact, "/evidence_binding", &mut blockers);
    if let Some(binding) = &binding {
        if !shared_identity_matches(&binding.identity, expected) {
            blockers.push("release_identity_mismatch".to_string());
        }
        if !REQUIRED_TARGETS.contains(&(
            binding.identity.target_os.as_str(),
            binding.identity.target_arch.as_str(),
        )) {
            blockers.push("target_not_required".to_string());
        }
    }
    let expected_in_report = artifact.pointer("/expected_identity").and_then(|value| {
        serde_json::from_value::<ProductionQualificationIdentity>(value.clone()).ok()
    });
    if expected_in_report.as_ref() != binding.as_ref().map(|binding| &binding.identity) {
        blockers.push("report_expected_identity_mismatch".to_string());
    }
    validate_projection(artifact, expected, &mut blockers);
    validate_serving_oracle_identity(artifact, &mut blockers);
    validate_search_vector_identity(artifact, search_artifact, &mut blockers);
    validate_recall(artifact, &mut blockers);
    validate_queries(artifact, &mut blockers);
    validate_oracle(artifact, policy, &mut blockers);
    validate_lifecycle(artifact, &mut blockers);
    blockers.sort();
    blockers.dedup();
    blockers
}

fn validate_serving_oracle_identity(artifact: &Value, blockers: &mut Vec<String>) {
    let Some(serving) = artifact.pointer("/projection_identity") else {
        blockers.push("vector_projection_identity_missing".to_string());
        return;
    };
    let Some(oracle) = artifact.pointer("/oracle_projection_identity") else {
        blockers.push("vector_oracle_projection_identity_missing".to_string());
        return;
    };
    for pointer in [
        "/projection_generation",
        "/document_count",
        "/source_digest",
        "/payload_bytes",
        "/format_version",
        "/bit_width",
        "/dimension",
    ] {
        if !oracle
            .pointer(pointer)
            .and_then(Value::as_u64)
            .is_some_and(|value| value > 0)
        {
            blockers.push("vector_oracle_projection_identity_incomplete".to_string());
        }
    }
    for pointer in [
        "/source_graph_commit_epoch",
        "/document_count",
        "/format_version",
        "/algorithm",
        "/bit_width",
        "/dimension",
        "/transform_seed",
        "/embedding_model",
        "/embedding_version",
        "/file_backed",
    ] {
        if serving.pointer(pointer) != oracle.pointer(pointer) {
            blockers.push("serving_vector_oracle_identity_mismatch".to_string());
            break;
        }
    }
}

fn validate_search_vector_identity(
    vector_artifact: &Value,
    search_artifact: Option<&Value>,
    blockers: &mut Vec<String>,
) {
    let search_identity =
        search_artifact.and_then(|artifact| artifact.pointer("/qualification/projection_identity"));
    let vector_search_identity = vector_artifact.pointer("/search_projection_identity");
    if search_identity.is_none() || vector_search_identity.is_none() {
        blockers.push("search_vector_identity_missing".to_string());
        return;
    }
    if search_identity != vector_search_identity {
        blockers.push("search_vector_document_identity_mismatch".to_string());
    }
    let vector_generation = vector_artifact
        .pointer("/projection_identity/projection_generation")
        .and_then(Value::as_u64);
    let search_generation = vector_search_identity
        .and_then(|identity| identity.pointer("/projection_generation"))
        .and_then(Value::as_u64);
    if vector_generation != search_generation {
        blockers.push("search_vector_generation_mismatch".to_string());
    }
}

fn validate_projection(
    artifact: &Value,
    expected: &ProductionQualificationIdentity,
    blockers: &mut Vec<String>,
) {
    let count = artifact
        .pointer("/projection_identity/document_count")
        .and_then(Value::as_u64);
    if !count.is_some_and(|count| count >= MINIMUM_VECTOR_DOCUMENT_COUNT) {
        blockers.push("vector_dataset_too_small".to_string());
    }
    for (pointer, code) in [
        (
            "/projection_identity/projection_generation",
            "vector_projection_generation_missing",
        ),
        (
            "/projection_identity/source_digest",
            "vector_source_digest_missing",
        ),
        (
            "/projection_identity/payload_bytes",
            "vector_payload_bytes_missing",
        ),
        (
            "/projection_identity/format_version",
            "vector_format_version_missing",
        ),
        ("/projection_identity/bit_width", "vector_bit_width_missing"),
        ("/projection_identity/dimension", "vector_dimension_missing"),
        (
            "/projection_resources/segment_count",
            "vector_segment_count_missing",
        ),
        (
            "/projection_resources/configured_build_working_bytes",
            "vector_build_budget_missing",
        ),
        (
            "/projection_resources/peak_build_working_bytes",
            "vector_peak_build_memory_missing",
        ),
        (
            "/projection_resources/raw_vector_bytes",
            "vector_raw_bytes_missing",
        ),
        (
            "/projection_resources/projection_payload_bytes",
            "vector_projection_bytes_missing",
        ),
    ] {
        require_nonzero(artifact, pointer, code, blockers);
    }
    for (pointer, code) in [
        (
            "/projection_identity/payload_checksum",
            "vector_payload_checksum_missing",
        ),
        (
            "/projection_identity/transform_seed",
            "vector_transform_seed_missing",
        ),
    ] {
        require_unsigned(artifact, pointer, code, blockers);
    }
    require_bool(
        artifact,
        "/projection_identity/file_backed",
        true,
        "vector_projection_not_file_backed",
        blockers,
    );
    if artifact
        .pointer("/projection_identity/source_graph_commit_epoch")
        .and_then(Value::as_u64)
        != Some(expected.canonical_graph_commit_epoch)
    {
        blockers.push("vector_source_graph_epoch_mismatch".to_string());
    }
    if !artifact
        .pointer("/projection_identity/bit_width")
        .and_then(Value::as_u64)
        .is_some_and(|bit_width| matches!(bit_width, 1 | 4))
    {
        blockers.push("vector_projection_unsupported_bit_width".to_string());
    }
    for (pointer, code) in [
        ("/projection_identity/algorithm", "vector_algorithm_missing"),
        (
            "/projection_identity/embedding_model",
            "vector_embedding_model_missing",
        ),
        (
            "/projection_identity/embedding_version",
            "vector_embedding_version_missing",
        ),
    ] {
        if !artifact
            .pointer(pointer)
            .and_then(Value::as_str)
            .is_some_and(|value| !value.trim().is_empty())
        {
            blockers.push(code.to_string());
        }
    }
    for (pointer, code) in [
        (
            "/process_memory/capabilities/resident_memory",
            "vector_rss_metric_unavailable",
        ),
        (
            "/process_memory/capabilities/total_page_faults",
            "vector_page_fault_metric_unavailable",
        ),
    ] {
        require_bool(artifact, pointer, true, code, blockers);
    }
}

fn validate_recall(artifact: &Value, blockers: &mut Vec<String>) {
    let Some(reports) = artifact
        .pointer("/recall_evidence")
        .and_then(Value::as_array)
    else {
        blockers.push("vector_recall_evidence_missing".to_string());
        return;
    };
    if reports.is_empty() {
        blockers.push("vector_recall_evidence_missing".to_string());
    }
    for evidence in reports {
        let Some(report) = evidence.pointer("/report") else {
            blockers.push("vector_recall_report_missing".to_string());
            continue;
        };
        require_string(
            report,
            "/protocol",
            RECALL_PROTOCOL,
            "vector_recall_protocol_mismatch",
            blockers,
        );
        require_string(
            report,
            "/approximate_backend",
            RABITQ_BACKEND,
            "vector_recall_backend_mismatch",
            blockers,
        );
        require_bool(report, "/ready", true, "vector_recall_not_ready", blockers);
        require_empty_array(
            report,
            "/blocker_codes",
            "vector_recall_blockers_present",
            blockers,
        );
        let requested = report
            .pointer("/requested_sample_count")
            .and_then(Value::as_u64);
        let executed = report
            .pointer("/executed_sample_count")
            .and_then(Value::as_u64);
        if !requested
            .zip(executed)
            .is_some_and(|(requested, executed)| requested > 0 && requested == executed)
        {
            blockers.push("vector_recall_samples_incomplete".to_string());
        }
        let minimum = report
            .pointer("/minimum_recall_per_million")
            .and_then(Value::as_u64);
        let candidate = report
            .pointer("/candidate_recall_at_k_per_million")
            .and_then(Value::as_u64);
        let final_recall = report
            .pointer("/recall_at_k_per_million")
            .and_then(Value::as_u64);
        if !minimum.zip(candidate).zip(final_recall).is_some_and(
            |((minimum, candidate), final_recall)| candidate >= minimum && final_recall >= minimum,
        ) {
            blockers.push("vector_recall_below_threshold".to_string());
        }
        for (pointer, code) in [
            ("/exact_hit_count", "vector_ground_truth_empty"),
            ("/candidate_hit_count", "vector_candidate_hits_empty"),
        ] {
            require_nonzero(report, pointer, code, blockers);
        }
        for (pointer, code) in [
            ("/fallback_count", "vector_recall_fallback_observed"),
            (
                "/index_coverage_incomplete_count",
                "vector_index_coverage_incomplete",
            ),
        ] {
            if report.pointer(pointer).and_then(Value::as_u64) != Some(0) {
                blockers.push(code.to_string());
            }
        }
    }
}

fn validate_queries(artifact: &Value, blockers: &mut Vec<String>) {
    let Some(cases) = artifact
        .pointer("/query_evidence")
        .and_then(Value::as_array)
    else {
        blockers.push("vector_query_evidence_missing".to_string());
        return;
    };
    if cases.is_empty() {
        blockers.push("vector_query_evidence_missing".to_string());
    }
    for case in cases {
        for pointer in [
            "/auto_scalar_candidate_parity",
            "/auto_scalar_final_parity",
            "/serving_auto_final_parity",
            "/auto_final_matches_exact",
            "/scalar_candidate_final_matches_exact",
        ] {
            require_bool(case, pointer, true, "vector_query_parity_failed", blockers);
        }
        for pointer in [
            "/exact_latency/sample_count",
            "/auto_latency/sample_count",
            "/scalar_candidate_latency/sample_count",
            "/serving_latency/sample_count",
            "/auto_metrics/max_admitted_workers",
            "/auto_metrics/segment_count",
            "/auto_metrics/peak_admitted_working_bytes",
            "/scalar_candidate_metrics/max_admitted_workers",
            "/serving_metrics/max_admitted_workers",
            "/serving_metrics/projection_payload_bytes_read",
        ] {
            require_nonzero(case, pointer, "vector_query_measurement_missing", blockers);
        }
        if !case
            .pointer("/auto_metrics/kernel")
            .and_then(Value::as_str)
            .is_some_and(|kernel| !kernel.trim().is_empty())
        {
            blockers.push("vector_auto_kernel_missing".to_string());
        }
        require_string(
            case,
            "/scalar_candidate_metrics/kernel",
            "scalar",
            "vector_scalar_kernel_mismatch",
            blockers,
        );
        require_string(
            case,
            "/serving_metrics/backend",
            "skein_rabitq_out_of_core_candidate_projection",
            "vector_serving_backend_mismatch",
            blockers,
        );
        require_string(
            case,
            "/serving_metrics/candidate_score_source",
            "quantized_projection",
            "vector_serving_candidate_source_mismatch",
            blockers,
        );
        require_string(
            case,
            "/serving_metrics/final_score_source",
            "raw_vector",
            "vector_serving_final_source_mismatch",
            blockers,
        );
        if !case
            .pointer("/serving_metrics/kernel")
            .and_then(Value::as_str)
            .is_some_and(|kernel| !kernel.trim().is_empty())
        {
            blockers.push("vector_serving_kernel_missing".to_string());
        }
        for pointer in [
            "/auto_metrics/fallback_count",
            "/scalar_candidate_metrics/fallback_count",
            "/serving_metrics/fallback_count",
        ] {
            if case.pointer(pointer).and_then(Value::as_u64) != Some(0) {
                blockers.push("vector_query_fallback_observed".to_string());
            }
        }
    }
}

fn validate_oracle(
    artifact: &Value,
    policy: ProductionReleaseQualificationPolicy,
    blockers: &mut Vec<String>,
) {
    let Some(oracle) = artifact.pointer("/rabitq_reference_verification") else {
        blockers.push("rabitq_reference_verification_missing".to_string());
        return;
    };
    require_string(
        oracle,
        "/role",
        "native_scalar_reference_for_dispatch_parity",
        "rabitq_reference_verification_role_mismatch",
        blockers,
    );
    if policy.require_rabitq_reference_verification {
        for (pointer, code) in [
            (
                "/required",
                "rabitq_reference_verification_not_required_by_run",
            ),
            ("/compiled", "rabitq_reference_verification_not_compiled"),
            ("/available", "rabitq_reference_verification_unavailable"),
            ("/ready", "rabitq_reference_verification_not_ready"),
        ] {
            require_bool(oracle, pointer, true, code, blockers);
        }
        if !oracle
            .pointer("/cases")
            .and_then(Value::as_array)
            .is_some_and(|cases| !cases.is_empty())
        {
            blockers.push("rabitq_reference_verification_cases_missing".to_string());
        }
    }
}

fn validate_lifecycle(artifact: &Value, blockers: &mut Vec<String>) {
    for (pointer, code) in [
        (
            "/lifecycle/incremental_fallback_safe",
            "vector_incremental_lifecycle_failed",
        ),
        (
            "/lifecycle/checkpoint_reopen_restores_projection",
            "vector_checkpoint_reopen_failed",
        ),
        (
            "/lifecycle/stale_generation_isolated",
            "vector_stale_generation_failed",
        ),
        (
            "/lifecycle/corrupt_projection_rejected",
            "vector_corruption_probe_failed",
        ),
        (
            "/lifecycle/cancellation_propagated",
            "vector_cancellation_probe_failed",
        ),
        (
            "/lifecycle/serving_cancellation_propagated",
            "vector_serving_cancellation_probe_failed",
        ),
        (
            "/lifecycle/mixed_foreground_background",
            "vector_mixed_load_failed",
        ),
    ] {
        require_bool(artifact, pointer, true, code, blockers);
    }
    for pointer in [
        "/lifecycle/update_latency/sample_count",
        "/lifecycle/checkpoint_latency/sample_count",
        "/lifecycle/reopen_latency/sample_count",
        "/lifecycle/cancellation_latency/sample_count",
        "/lifecycle/serving_cancellation_latency/sample_count",
    ] {
        require_nonzero(
            artifact,
            pointer,
            "vector_lifecycle_samples_missing",
            blockers,
        );
    }
}
