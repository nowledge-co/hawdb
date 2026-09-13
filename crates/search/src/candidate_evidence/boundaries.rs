use super::test_report::ready_report;
use super::*;
use crate::{CompressedVectorSearchMode, SearchMode, VECTOR_RECALL_VALIDATION_PROTOCOL};

#[test]
fn request_builders_and_advisor_preserve_compression_admission() {
    for mask in 0_u32..32 {
        let advisor = NowledgeMemRetrievalProjectionAdvisor {
            recall_evidence_protocol: (mask & 1 != 0)
                .then(|| VECTOR_RECALL_VALIDATION_PROTOCOL.to_string()),
            recall_sample_count: usize::from(mask & 2 != 0),
            recall_evidence_ready: mask & 4 != 0,
            parity_evidence_ready: mask & 8 != 0,
            cold_or_constrained_local_segment: mask & 16 != 0,
            recall_at_k_per_million: Some(1_000_000),
        };
        for mode in [
            CompressedVectorSearchMode::Disabled,
            CompressedVectorSearchMode::Preferred,
            CompressedVectorSearchMode::Required,
        ] {
            for request in [
                NowledgeMemSearchCandidateRequest::text("query", 4),
                NowledgeMemSearchCandidateRequest::vector(vec![1.0, 0.0], 4),
                NowledgeMemSearchCandidateRequest::hybrid("query", vec![1.0, 0.0], 4),
            ] {
                assert_eq!(request.limit, 4);
                assert_eq!(request.offset, 0);
                assert_eq!(request.rank_window, None);
                assert!(!request.recall_validation_probe);
                assert!(request.metadata_filters.is_empty());
                assert_eq!(
                    request.query_embedding.is_some(),
                    request.mode != SearchMode::Text
                );
                assert_eq!(
                    request.query_text.is_empty(),
                    request.mode == SearchMode::Vector
                );
                let request = request
                    .with_compressed_vector_search_mode(mode)
                    .with_retrieval_projection_advisor(advisor.clone())
                    .with_offset(2)
                    .with_rank_window(Some(8))
                    .as_recall_validation_probe();
                assert_eq!(request.offset, 2);
                assert_eq!(request.rank_window, Some(8));
                assert!(request.recall_validation_probe);
                let expected = if mask == 31 {
                    mode
                } else {
                    CompressedVectorSearchMode::Disabled
                };
                assert_eq!(
                    effective_search_candidate_mode(&request),
                    expected,
                    "mask={mask}"
                );
                assert_eq!(advisor.ready(), mask == 31);
            }
        }
    }
}

#[test]
fn report_json_preserves_complete_field_set_and_provenance() {
    let report = ready_report();
    let json = report.json();
    let mut keys = json
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let mut expected = vec![
        "protocol",
        "compressed_vector_search_mode",
        "requested_compressed_vector_search_mode",
        "retrieval_projection_advisor",
        "retrieval_projection_advisor_blocker_codes",
        "mode",
        "query_embedding_dimension",
        "limit",
        "offset",
        "rank_window",
        "document_count",
        "filtered_document_count",
        "total_hits",
        "returned_hit_count",
        "returned_kind_counts",
        "returned_missing_external_id_count",
        "returned_missing_source_id_count",
        "truncated",
        "candidate_set",
        "filtered_out_count",
        "metadata_filter_count",
        "pushed_predicate_count",
        "residual_predicate_count",
        "segment_count",
        "pruned_segment_count",
        "scanned_segment_count",
        "segment_pruning_candidate_document_count",
        "segment_pruned_document_count",
        "segment_scanned_document_count",
        "persisted_segment_descriptor_used",
        "physical_range_read_count",
        "physical_bytes_read",
        "retriever_backends",
        "retriever_backend_selection_reasons",
        "retriever_estimated_raw_vector_bytes",
        "retriever_filter_selectivity_per_million",
        "retriever_available",
        "retriever_candidate_counts",
        "retriever_candidate_score_sources",
        "retriever_final_score_sources",
        "fallback_reason_codes",
        "empty_reason_codes",
        "truncation_reason_codes",
        "projection_full_reindex_needed",
        "projection_metadata_repair_needed",
        "projection_source_graph_commit_epoch",
        "projection_durable_source_graph_commit_epoch",
        "projection_embedding_model",
        "projection_embedding_version",
        "projection_embedding_dimension",
    ];
    keys.sort_unstable();
    expected.sort_unstable();
    assert_eq!(keys, expected);
    assert_eq!(json["offset"], 2);
    assert_eq!(json["rank_window"], 8);
    assert_eq!(json["physical_bytes_read"], 256);
    assert_eq!(json["physical_range_read_count"], 2);
    assert_eq!(json["projection_source_graph_commit_epoch"], 8);
    assert_eq!(json["projection_durable_source_graph_commit_epoch"], 7);
    assert_eq!(
        json["candidate_set"]["snapshot_source_graph_commit_epoch"],
        7
    );
    assert_eq!(json["candidate_set"]["policy_epoch"], 2);
    assert_eq!(json["projection_embedding_model"], "model");
    assert_eq!(json["retriever_available"]["text"], true);
    assert_eq!(json["retrieval_projection_advisor"]["ready"], false);
}

#[test]
fn accumulator_saturates_counts_and_retains_invalid_match_blocker() {
    let mut accumulator = NowledgeMemSearchCandidateShadowAccumulator::new();
    accumulator.record_compare(u64::MAX, u64::MAX, u64::MAX);
    accumulator.record_compare(1, 0, 2);
    accumulator.record_retriever_leg("text", true, u64::MAX);
    accumulator.record_retriever_leg("text", false, 1);
    let evidence = accumulator.evidence();
    assert_eq!(evidence.request_count, 2);
    assert_eq!(evidence.primary_candidate_count, u64::MAX);
    assert_eq!(evidence.shadow_candidate_count, u64::MAX);
    assert_eq!(evidence.matched_candidate_count, u64::MAX);
    assert_eq!(evidence.text_retriever_candidate_count, u64::MAX);
    assert!(evidence.text_retriever_available);
    assert_eq!(
        evidence.blocker_codes,
        ["search_candidate_invalid_match_count"]
    );
}

#[test]
fn identity_is_order_insensitive_within_requests_but_keeps_request_boundaries() {
    let mut single = NowledgeMemSearchCandidateShadowAccumulator::new();
    single.record_compare_candidate_ids(&["b", "a", "a"], &["a", "b"]);
    let single = single.evidence();
    assert_eq!(single.primary_candidate_count, 2);
    assert_eq!(
        single.primary_candidate_identity_checksum,
        single.shadow_candidate_identity_checksum
    );
    assert_eq!(
        single.matched_candidate_identity_checksum,
        single.shadow_candidate_identity_checksum
    );
    let mut split = NowledgeMemSearchCandidateShadowAccumulator::new();
    split.record_compare_candidate_ids(&["a"], &["a"]);
    split.record_compare_candidate_ids(&["b"], &["b"]);
    assert_ne!(
        single.primary_candidate_identity_checksum,
        split.evidence().primary_candidate_identity_checksum
    );
}

#[test]
fn field_summary_merge_preserves_max_capabilities_and_scan_absence() {
    let summary = |source: &str, count, used| NowledgeMemSearchCandidateFieldSummary {
        field: "lifecycle_state".to_string(),
        source: source.to_string(),
        segment_count: count,
        value_summary_used: used,
        value_summary_segment_count: usize::from(used) * count,
        numeric_range_summary_used: false,
        numeric_range_segment_count: 0,
        timestamp_range_summary_used: false,
        timestamp_range_segment_count: 0,
    };
    let mut accumulator = NowledgeMemSearchCandidateShadowAccumulator::new();
    accumulator.record_filter_pushdown_summaries(1, false, vec![summary("first", 2, true)]);
    accumulator.record_filter_pushdown_summaries(2, true, vec![summary("second", 3, false)]);
    let evidence = accumulator.evidence();
    let filter = evidence.filter_pushdown.unwrap();
    assert_eq!(filter.pushed_predicate_count, 3);
    assert!(!filter.shadow_scan_present);
    assert_eq!(filter.field_summaries.len(), 1);
    assert_eq!(filter.field_summaries[0].source, "merged");
    assert_eq!(filter.field_summaries[0].segment_count, 3);
    assert_eq!(filter.field_summaries[0].value_summary_segment_count, 2);
    assert!(filter.field_summaries[0].value_summary_used);
}
