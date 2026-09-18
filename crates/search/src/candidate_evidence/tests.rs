use super::*;
use crate::NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS;

use super::probe_fixtures::ready_probe;

fn ready_structured_probe() -> serde_json::Value {
    let field_summaries = NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
        .iter()
        .map(|field| {
            serde_json::json!({
                "field": field,
                "source": "search_candidate_shadow_probe",
                "segment_count": 1,
                "value_summary_used": true,
                "value_summary_segment_count": 1,
                "numeric_range_summary_used": matches!(*field, "importance" | "confidence"),
                "numeric_range_segment_count": usize::from(matches!(
                    *field,
                    "importance" | "confidence"
                )),
                "timestamp_range_summary_used": matches!(
                    *field,
                    "created_at" | "updated_at" | "event_start" | "event_end"
                ),
                "timestamp_range_segment_count": usize::from(matches!(
                    *field,
                    "created_at" | "updated_at" | "event_start" | "event_end"
                )),
            })
        })
        .collect::<Vec<_>>();
    let mut probe = ready_probe();
    probe["filter_pushdown"]
        .as_object_mut()
        .unwrap()
        .remove("fields");
    probe["filter_pushdown"]["field_summaries"] = serde_json::json!(field_summaries);
    probe
}

#[test]
fn search_candidate_shadow_evidence_reports_ready_counts() {
    let mut accumulator = NowledgeMemSearchCandidateShadowAccumulator::new();
    accumulator.record_compare_candidate_ids(&["mem_1", "mem_2"], &["mem_1", "mem_2"]);
    accumulator
        .record_compare_candidate_ids(&["mem_3", "mem_4", "mem_5"], &["mem_3", "mem_4", "mem_5"]);
    accumulator.record_filter_pushdown_fields(
        2,
        NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
            .iter()
            .copied(),
    );
    let evidence = accumulator.json();

    assert_eq!(
        evidence["protocol"],
        NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL
    );
    assert_eq!(
        evidence["route"],
        NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE
    );
    assert_eq!(
        evidence["evidence_source"],
        NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE
    );
    assert_eq!(evidence["ready"], true);
    assert_eq!(evidence["request_count"], 2);
    assert_eq!(evidence["primary_candidate_count"], 5);
    assert_eq!(evidence["shadow_candidate_count"], 5);
    assert_eq!(evidence["matched_candidate_count"], 5);
    assert_eq!(evidence["primary_only_candidate_count"], 0);
    assert_eq!(evidence["row_count_parity"], true);
    assert_eq!(evidence["text_retriever_ready"], false);
    assert_eq!(evidence["vector_retriever_ready"], false);
    assert_eq!(evidence["fts_top_k_overlap_ready"], false);
    assert_eq!(evidence["vector_top_k_overlap_ready"], false);
    assert_eq!(
        evidence["candidate_readiness"]["source_chunk_identity_ready"],
        false
    );
    assert_eq!(evidence["candidate_readiness"]["fail_soft_observed"], false);
    assert_eq!(
        evidence["candidate_readiness"]["projection_marker_status_visible"],
        false
    );
    assert_eq!(
        evidence["candidate_readiness"]["projection_watermark_ready"],
        false
    );
    assert_eq!(
        evidence["candidate_readiness"]["embedding_identity_ready"],
        false
    );
    assert_eq!(evidence["candidate_identity"]["ready"], true);
    assert_eq!(evidence["candidate_identity"]["parity"], true);
    assert_eq!(evidence["shadow_scan_present"], true);
    assert_eq!(evidence["shadow_scan_filter_pushdown_ready"], true);
    assert_eq!(evidence["shadow_scan_field_pruning_ready"], true);
    assert_eq!(
        evidence["shadow_scan_field_summary_count"],
        serde_json::json!(NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len())
    );
    assert_eq!(evidence["filter_pushdown_ready"], true);
    assert_eq!(evidence["filter_pushdown"]["ready"], true);
    assert_eq!(evidence["filter_pushdown"]["pushed_predicate_count"], 2);
    assert_eq!(
        evidence["filter_pushdown"]["field_capabilities_ready"],
        true
    );
    assert_eq!(
        evidence["filter_pushdown"]["missing_value_summary_fields"],
        serde_json::json!([])
    );
    assert_eq!(
        evidence["filter_pushdown"]["missing_numeric_range_fields"],
        serde_json::json!([])
    );
    assert_eq!(
        evidence["filter_pushdown"]["missing_timestamp_range_fields"],
        serde_json::json!([])
    );
    assert_eq!(
        evidence["filter_pushdown"]["field_summary_count"],
        serde_json::json!(NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len())
    );
    assert!(evidence["filter_pushdown"]["field_summaries"]
        .as_array()
        .unwrap()
        .iter()
        .any(|summary| summary["field"] == "importance"
            && summary["source"] == "persisted_segment_descriptor_contract"
            && summary["numeric_range_summary_used"] == true));
    assert!(evidence["candidate_identity"]
        .get("candidate_ids")
        .is_none());
    assert_eq!(evidence["blocker_codes"], serde_json::json!([]));
}

#[test]
fn search_candidate_shadow_evidence_fails_closed_on_weak_counts() {
    let evidence = nowledge_mem_search_candidate_shadow_evidence_json(
        &NowledgeMemSearchCandidateShadowEvidence {
            request_count: 0,
            primary_candidate_count: 3,
            shadow_candidate_count: 2,
            matched_candidate_count: 1,
            primary_only_candidate_count: 1,
            text_retriever_available: false,
            vector_retriever_available: false,
            text_retriever_candidate_count: 0,
            vector_retriever_candidate_count: 0,
            fts_top_k_overlap_observed: false,
            fts_top_k_overlap_ready: false,
            vector_top_k_overlap_observed: false,
            vector_top_k_overlap_ready: false,
            source_chunk_identity_ready: false,
            fail_soft_observed: false,
            projection_marker_status_visible: false,
            projection_watermark_ready: false,
            embedding_identity_ready: false,
            primary_candidate_identity_checksum: None,
            shadow_candidate_identity_checksum: None,
            matched_candidate_identity_checksum: None,
            filter_pushdown: None,
            blocker_codes: vec!["bridge_timeout".to_string()],
        },
    );

    assert_eq!(evidence["ready"], false);
    assert_eq!(
        evidence["blocker_codes"],
        serde_json::json!([
            "bridge_timeout",
            "search_candidate_filter_pushdown_missing",
            "search_candidate_identity_missing",
            "search_candidate_mismatch",
            "search_candidate_primary_only",
            "search_candidate_shadow_no_requests"
        ])
    );
}

#[test]
fn search_candidate_shadow_accumulator_generates_bridge_evidence() {
    let mut accumulator = NowledgeMemSearchCandidateShadowAccumulator::new();
    accumulator.record_compare_candidate_ids(&["mem_1", "mem_2"], &["mem_1", "mem_2"]);
    accumulator.record_compare_candidate_ids(&["mem_3"], &["mem_3"]);
    accumulator.record_filter_pushdown_fields(
        1,
        NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
            .iter()
            .copied(),
    );

    let evidence = accumulator.json();

    assert_eq!(evidence["ready"], true);
    assert_eq!(evidence["request_count"], 2);
    assert_eq!(evidence["primary_candidate_count"], 3);
    assert_eq!(evidence["shadow_candidate_count"], 3);
    assert_eq!(evidence["matched_candidate_count"], 3);
    assert_eq!(evidence["primary_only_candidate_count"], 0);
    assert_eq!(evidence["row_count_parity"], true);
    assert_eq!(evidence["text_retriever_ready"], false);
    assert_eq!(evidence["vector_retriever_ready"], false);
    assert_eq!(evidence["fts_top_k_overlap_ready"], false);
    assert_eq!(evidence["vector_top_k_overlap_ready"], false);
    assert_eq!(
        evidence["candidate_readiness"]["source_chunk_identity_ready"],
        false
    );
    assert_eq!(
        evidence["candidate_readiness"]["projection_watermark_ready"],
        false
    );
    assert_eq!(
        evidence["candidate_readiness"]["embedding_identity_ready"],
        false
    );
    assert_eq!(evidence["candidate_identity"]["ready"], true);
    assert_eq!(evidence["shadow_scan_filter_pushdown_ready"], true);
    assert_eq!(evidence["shadow_scan_field_pruning_ready"], true);
    assert_eq!(evidence["filter_pushdown_ready"], true);
    assert_eq!(evidence["blocker_codes"], serde_json::json!([]));
}

#[test]
fn search_candidate_shadow_accumulator_preserves_request_blockers() {
    let mut accumulator = NowledgeMemSearchCandidateShadowAccumulator::new();
    accumulator.record_compare_candidate_ids(&["mem_1", "mem_2"], &["mem_1"]);
    accumulator.record_filter_pushdown_fields(
        1,
        NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
            .iter()
            .copied(),
    );
    accumulator.add_blocker_code("bridge_error");

    let evidence = accumulator.json();

    assert_eq!(evidence["ready"], false);
    assert_eq!(evidence["request_count"], 1);
    assert_eq!(evidence["primary_candidate_count"], 2);
    assert_eq!(evidence["shadow_candidate_count"], 1);
    assert_eq!(evidence["matched_candidate_count"], 1);
    assert_eq!(evidence["primary_only_candidate_count"], 1);
    assert_eq!(
        evidence["blocker_codes"],
        serde_json::json!([
            "bridge_error",
            "search_candidate_identity_mismatch",
            "search_candidate_mismatch",
            "search_candidate_primary_only"
        ])
    );
}

#[test]
fn search_candidate_shadow_evidence_requires_filter_pushdown_fields() {
    let mut accumulator = NowledgeMemSearchCandidateShadowAccumulator::new();
    accumulator.record_compare_candidate_ids(&["mem_1"], &["mem_1"]);

    let missing = accumulator.json();

    assert_eq!(missing["ready"], false);
    assert_eq!(missing["filter_pushdown_ready"], false);
    assert!(missing["blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "search_candidate_filter_pushdown_missing"));

    accumulator.record_filter_pushdown_fields(1, ["unit_type"]);
    let partial = accumulator.json();

    assert_eq!(partial["ready"], false);
    assert!(!partial["filter_pushdown"]["missing_required_fields"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(partial["blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "search_candidate_field_pruning_missing"));
}

#[test]
fn search_candidate_shadow_probe_parser_is_available_to_library_callers() {
    let accumulator = parse_search_candidate_shadow_probe(&ready_probe()).unwrap();
    let evidence = nowledge_mem_search_candidate_shadow_evidence_json(&accumulator.evidence());

    assert_eq!(evidence["ready"], true);
    assert_eq!(evidence["request_count"], 2);
    assert_eq!(evidence["text_retriever_ready"], true);
    assert_eq!(evidence["vector_retriever_ready"], true);
    assert_eq!(evidence["fts_top_k_overlap_ready"], true);
    assert_eq!(evidence["vector_top_k_overlap_ready"], true);
    assert_eq!(
        evidence["candidate_readiness"]["source_chunk_identity_ready"],
        true
    );
    assert_eq!(evidence["candidate_readiness"]["fail_soft_observed"], true);
    assert_eq!(
        evidence["candidate_readiness"]["projection_marker_status_visible"],
        true
    );
    assert_eq!(
        evidence["candidate_readiness"]["projection_watermark_ready"],
        true
    );
    assert_eq!(
        evidence["candidate_readiness"]["embedding_identity_ready"],
        true
    );
    assert_eq!(evidence["filter_pushdown"]["ready"], true);
}

#[test]
fn search_candidate_shadow_probe_accepts_structured_field_summaries() {
    let accumulator = parse_search_candidate_shadow_probe(&ready_structured_probe()).unwrap();
    let evidence = nowledge_mem_search_candidate_shadow_evidence_json(&accumulator.evidence());

    assert_eq!(evidence["ready"], true);
    assert_eq!(
        evidence["filter_pushdown"]["field_capabilities_ready"],
        true
    );
    assert!(evidence["filter_pushdown"]["field_summaries"]
        .as_array()
        .unwrap()
        .iter()
        .any(|summary| summary["field"] == "importance"
            && summary["source"] == "search_candidate_shadow_probe"
            && summary["numeric_range_segment_count"] == 1));
}

#[test]
fn search_candidate_shadow_probe_rejects_zero_summary_counts() {
    let mut probe = ready_structured_probe();
    let summaries = probe["filter_pushdown"]["field_summaries"]
        .as_array_mut()
        .unwrap();
    let lifecycle_state = summaries
        .iter_mut()
        .find(|summary| summary["field"] == "lifecycle_state")
        .unwrap();
    lifecycle_state["value_summary_used"] = serde_json::json!(true);
    lifecycle_state["value_summary_segment_count"] = serde_json::json!(0);

    let accumulator = parse_search_candidate_shadow_probe(&probe).unwrap();
    let evidence = nowledge_mem_search_candidate_shadow_evidence_json(&accumulator.evidence());

    assert_eq!(evidence["ready"], false);
    assert_eq!(
        evidence["filter_pushdown"]["field_capabilities_ready"],
        false
    );
    assert_eq!(
        evidence["filter_pushdown"]["missing_value_summary_fields"],
        serde_json::json!(["lifecycle_state"])
    );
    assert!(evidence["blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "search_candidate_field_pruning_capability_missing"));
}

#[test]
fn search_candidate_shadow_probe_requires_retriever_leg_evidence() {
    let mut probe = ready_probe();
    probe.as_object_mut().unwrap().remove("retriever_legs");

    let error = parse_search_candidate_shadow_probe(&probe).unwrap_err();

    assert_eq!(
        error.to_string(),
        "semantic error: search candidate shadow probe field 'retriever_legs' must be a object"
    );
}

#[test]
fn search_candidate_shadow_probe_requires_top_k_overlap_evidence() {
    let mut probe = ready_probe();
    probe.as_object_mut().unwrap().remove("top_k_overlap");

    let error = parse_search_candidate_shadow_probe(&probe).unwrap_err();

    assert_eq!(
        error.to_string(),
        "semantic error: search candidate shadow probe field 'top_k_overlap' must be a object"
    );
}

#[test]
fn search_candidate_shadow_probe_requires_candidate_readiness_evidence() {
    let mut probe = ready_probe();
    probe.as_object_mut().unwrap().remove("candidate_readiness");

    let error = parse_search_candidate_shadow_probe(&probe).unwrap_err();

    assert_eq!(
        error.to_string(),
        "semantic error: search candidate shadow probe field 'candidate_readiness' must be a object"
    );
}

#[test]
fn search_candidate_shadow_probe_requires_typed_candidate_readiness_signals() {
    let mut probe = ready_probe();
    probe["candidate_readiness"]
        .as_object_mut()
        .unwrap()
        .remove("embedding_identity_ready");

    let error = parse_search_candidate_shadow_probe(&probe).unwrap_err();

    assert_eq!(
        error.to_string(),
        "semantic error: search candidate shadow probe field 'embedding_identity_ready' must be a boolean"
    );
}

#[test]
fn search_candidate_shadow_probe_fails_closed_for_unavailable_retriever_leg() {
    let mut probe = ready_probe();
    probe["retriever_legs"]["vector"]["available"] = serde_json::json!(false);
    probe["retriever_legs"]["vector"]["candidate_count"] = serde_json::json!(0);

    let accumulator = parse_search_candidate_shadow_probe(&probe).unwrap();
    let evidence = nowledge_mem_search_candidate_shadow_evidence_json(&accumulator.evidence());

    assert_eq!(evidence["ready"], false);
    assert_eq!(evidence["text_retriever_ready"], true);
    assert_eq!(evidence["vector_retriever_ready"], false);
    assert!(evidence["blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "search_candidate_vector_retriever_unavailable"));
}
