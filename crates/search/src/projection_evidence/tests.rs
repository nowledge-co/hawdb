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

#[test]
fn search_projection_evidence_reports_ready_for_complete_probe() {
    let report = nowledge_search_projection_evidence_json(&ready_probe());

    assert_eq!(report["ready"], true);
    assert_eq!(report["derived_projection"], true);
    assert_eq!(report["all_tables_covered"], true);
    assert_eq!(report["covered_table_count"], 6);
    assert_eq!(report["required_table_count"], 6);
    assert_eq!(report["fts_ready"], true);
    assert_eq!(report["vector_ready"], true);
    assert_eq!(report["embedding_identity_ready"], true);
    assert_eq!(report["document_identity_ready"], true);
    assert_eq!(report["fail_soft_ready"], true);
    assert_eq!(report["rebuild_marker_ready"], true);
    assert_eq!(report["metadata_repair_marker_ready"], true);
    assert_eq!(report["incremental_update_ready"], true);
    assert_eq!(report["source_chunk_ready"], true);
    assert_eq!(report["predicate_pushdown_ready"], true);
    assert_eq!(report["production_filter_pruning_ready"], true);
    assert_eq!(report["compressed_vector_projection_required"], true);
    assert_eq!(report["compressed_vector_projection_ready"], true);
    assert_eq!(report["blocker_codes"], serde_json::json!([]));
}

#[test]
fn search_projection_evidence_report_exposes_typed_summary() {
    let report = NowledgeSearchProjectionEvidenceReport::from_probe(&ready_probe());

    assert_eq!(report.protocol, "hawdb-nowledge-search-projection-evidence");
    assert!(report.ready);
    assert!(report.derived_projection);
    assert!(report.all_tables_covered);
    assert_eq!(report.covered_table_count, 6);
    assert_eq!(report.required_table_count, 6);
    assert!(report.fts_ready);
    assert!(report.vector_ready);
    assert!(report.document_identity_ready);
    assert!(report.embedding_identity_ready);
    assert!(report.fail_soft_ready);
    assert!(report.rebuild_marker_ready);
    assert!(report.metadata_repair_marker_ready);
    assert!(report.incremental_update_ready);
    assert!(report.source_chunk_ready);
    assert!(report.predicate_pushdown_ready);
    assert!(report.hawdb_predicate_pushdown_ready);
    assert!(report.production_filter_pruning_ready);
    assert!(report.compressed_vector_projection_required);
    assert!(report.compressed_vector_projection_ready);
    assert!(report.blocker_codes.is_empty());
    assert_eq!(report.json()["ready"], true);
}

#[test]
fn probe_contract_example_feeds_search_projection_evidence() {
    let contract = nowledge_search_projection_probe_contract_json();
    let example = &contract["example_primary_probe"];
    let hawdb_example = &contract["example_hawdb_probe"];
    let evidence = nowledge_search_projection_evidence_json(example);
    let hawdb_evidence = nowledge_search_projection_evidence_json(hawdb_example);

    assert_eq!(
        contract["protocol"],
        "hawdb-nowledge-search-projection-probe-contract-v1"
    );
    assert_eq!(example["engine"], "lancedb");
    assert_eq!(
        contract["required_tables"],
        serde_json::json!([
            "memories_index",
            "messages_index",
            "communities_index",
            "entities_index",
            "sources_index",
            "source_chunks_index"
        ])
    );
    assert_eq!(
        contract["required_hawdb_scan_filter_fields"],
        serde_json::json!(NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS)
    );
    assert!(contract["predicate_pushdown_fields"]
        .as_array()
        .unwrap()
        .iter()
        .any(|field| field == "segment_descriptor_field_summaries"));
    assert!(contract["predicate_pushdown_fields"]
        .as_array()
        .unwrap()
        .iter()
        .any(|field| field == "segment_descriptor_scan_filter_fields_ready"));
    assert_eq!(evidence["ready"], true);
    assert_eq!(evidence["predicate_pushdown_ready"], true);
    assert_eq!(evidence["compressed_vector_projection_required"], false);
    assert_eq!(evidence["compressed_vector_projection_ready"], true);
    assert_eq!(hawdb_example["engine"], "hawdb");
    assert_eq!(hawdb_evidence["ready"], true);
    assert_eq!(hawdb_evidence["hawdb_predicate_pushdown_ready"], true);
    assert_eq!(
        hawdb_evidence["predicate_pushdown"]["segment_descriptor_scan_filter_fields_ready"],
        true
    );
}

#[test]
fn search_projection_evidence_requires_compressed_vector_projection_for_hawdb_probe() {
    let mut probe = ready_probe();
    probe["compressed_vector_projection"]["ready"] = serde_json::json!(false);
    probe["compressed_vector_projection"]["compiled"] = serde_json::json!(false);
    probe["compressed_vector_projection"]["blocker_codes"] =
        serde_json::json!(["rabitq_projection_unavailable"]);

    let report = nowledge_search_projection_evidence_json(&probe);

    assert_eq!(report["ready"], false);
    assert_eq!(report["compressed_vector_projection_required"], true);
    assert_eq!(report["compressed_vector_projection_ready"], false);
    assert_eq!(
        report["compressed_vector_projection"]["blocker_codes"],
        serde_json::json!(["rabitq_projection_unavailable"])
    );
    assert!(report["blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "compressed_vector_projection_not_ready"));
}

#[test]
fn search_projection_evidence_fails_closed_for_missing_source_chunks() {
    let mut probe = ready_probe();
    probe["tables"]
        .as_array_mut()
        .unwrap()
        .retain(|table| table["name"].as_str() != Some("source_chunks_index"));

    let report = nowledge_search_projection_evidence_json(&probe);

    assert_eq!(report["ready"], false);
    assert_eq!(report["all_tables_covered"], false);
    assert_eq!(report["covered_table_count"], 5);
    assert_eq!(report["source_chunk_ready"], false);
    assert_eq!(
        report["blocker_codes"],
        serde_json::json!([
            "fts_not_ready",
            "missing_required_search_tables",
            "source_chunks_index_not_ready",
            "vector_not_ready"
        ])
    );
}

#[test]
fn search_projection_evidence_recomputes_embedding_identity() {
    let mut probe = ready_probe();
    probe["embedding_manifest"]["active_dimension"] = serde_json::json!(1536);

    let report = nowledge_search_projection_evidence_json(&probe);

    assert_eq!(report["ready"], false);
    assert_eq!(report["embedding_identity_ready"], false);
    assert_eq!(
        report["blocker_codes"],
        serde_json::json!(["embedding_identity_not_ready"])
    );
}

#[test]
fn search_projection_evidence_recomputes_document_identity() {
    let mut probe = ready_probe();
    probe["document_identity"]["document_count"] = serde_json::json!(5);

    let report = nowledge_search_projection_evidence_json(&probe);

    assert_eq!(report["ready"], false);
    assert_eq!(report["document_identity_ready"], false);
    assert_eq!(report["document_identity"]["document_count_matches"], false);
    assert_eq!(
        report["blocker_codes"],
        serde_json::json!(["document_identity_not_ready"])
    );
}

#[test]
fn search_projection_evidence_recomputes_incremental_watermark_readiness() {
    let mut probe = ready_probe();
    probe["incremental_update"]
        .as_object_mut()
        .unwrap()
        .remove("source_graph_commit_epoch");

    let report = nowledge_search_projection_evidence_json(&probe);

    assert_eq!(report["ready"], false);
    assert_eq!(report["incremental_update_ready"], false);
    assert_eq!(report["incremental_update"]["reported_ready"], true);
    assert_eq!(report["incremental_update"]["watermark_ready"], true);
    assert_eq!(
        report["incremental_update"]["source_graph_commit_epoch"],
        serde_json::Value::Null
    );
    assert_eq!(
        report["blocker_codes"],
        serde_json::json!(["incremental_update_not_ready"])
    );
}

#[test]
fn search_projection_evidence_fails_closed_for_missing_predicate_pushdown() {
    let mut probe = ready_probe();
    probe.as_object_mut().unwrap().remove("predicate_pushdown");

    let report = nowledge_search_projection_evidence_json(&probe);

    assert_eq!(report["ready"], false);
    assert_eq!(report["predicate_pushdown_ready"], false);
    assert_eq!(
        report["blocker_codes"],
        serde_json::json!([
            "hawdb_predicate_pushdown_descriptor_not_ready",
            "predicate_pushdown_not_ready"
        ])
    );
}

#[test]
fn hawdb_search_projection_evidence_requires_production_filter_pruning() {
    let mut probe = ready_probe();
    probe
        .as_object_mut()
        .unwrap()
        .remove("production_filter_pruning");

    let report = nowledge_search_projection_evidence_json(&probe);

    assert_eq!(report["ready"], false);
    assert_eq!(report["production_filter_pruning_ready"], false);
    assert_eq!(
        report["production_filter_pruning"]["required_fields_ready"],
        false
    );
    assert_eq!(
        report["blocker_codes"],
        serde_json::json!(["hawdb_production_filter_pruning_not_ready"])
    );
}

#[test]
fn hawdb_search_projection_evidence_rejects_incomplete_production_filter_pruning() {
    let mut probe = ready_probe();
    probe["production_filter_pruning"]["ready_field_count"] = serde_json::json!(12);
    probe["production_filter_pruning"]["missing_fields"] = serde_json::json!(["event_end"]);

    let report = nowledge_search_projection_evidence_json(&probe);

    assert_eq!(report["ready"], false);
    assert_eq!(report["production_filter_pruning_ready"], false);
    assert_eq!(
        report["production_filter_pruning"]["required_fields_ready"],
        false
    );
    assert_eq!(
        report["production_filter_pruning"]["missing_fields"],
        serde_json::json!(["event_end"])
    );
    assert_eq!(
        report["blocker_codes"],
        serde_json::json!(["hawdb_production_filter_pruning_not_ready"])
    );
}

#[test]
fn hawdb_search_projection_evidence_requires_production_filter_explain_analyze() {
    let mut probe = ready_probe();
    probe["production_filter_pruning"]["explain_analyze_ready"] = serde_json::json!(false);

    let report = nowledge_search_projection_evidence_json(&probe);

    assert_eq!(report["ready"], false);
    assert_eq!(report["production_filter_pruning_ready"], false);
    assert_eq!(
        report["production_filter_pruning"]["explain_analyze_ready"],
        false
    );
    assert_eq!(
        report["blocker_codes"],
        serde_json::json!(["hawdb_production_filter_pruning_not_ready"])
    );
}

#[test]
fn hawdb_search_projection_evidence_requires_payload_avoidance_samples() {
    let mut probe = ready_probe();
    for sample in probe["production_filter_pruning"]["samples"]
        .as_array_mut()
        .unwrap()
    {
        sample["explain_analyze"]["payload_read_avoidance"] = serde_json::json!(false);
    }

    let report = nowledge_search_projection_evidence_json(&probe);

    assert_eq!(report["ready"], false);
    assert_eq!(report["production_filter_pruning_ready"], false);
    assert_eq!(
        report["production_filter_pruning"]["sample_evidence_ready"],
        false
    );
    assert_eq!(
        report["production_filter_pruning"]["sample_payload_read_avoidance_count"],
        0
    );
    assert_eq!(
        report["production_filter_pruning"]["missing_sample_fields"],
        serde_json::json!([])
    );
    assert_eq!(
        report["blocker_codes"],
        serde_json::json!(["hawdb_production_filter_pruning_not_ready"])
    );
}

#[test]
fn hawdb_search_projection_evidence_requires_filter_operation_families() {
    let mut probe = ready_probe();
    probe["production_filter_pruning"]["samples"]
        .as_array_mut()
        .unwrap()
        .retain(|sample| sample["operation_family"].as_str() != Some("unique_key"));
    let sample_count = probe["production_filter_pruning"]["samples"]
        .as_array()
        .unwrap()
        .len();
    probe["production_filter_pruning"]["sample_count"] = serde_json::json!(sample_count);

    let report = nowledge_search_projection_evidence_json(&probe);

    assert_eq!(report["ready"], false);
    assert_eq!(report["production_filter_pruning_ready"], false);
    assert_eq!(
        report["production_filter_pruning"]["sample_evidence_ready"],
        false
    );
    assert_eq!(
        report["production_filter_pruning"]["operation_family_evidence_ready"],
        false
    );
    assert_eq!(
        report["production_filter_pruning"]["missing_operation_families"],
        serde_json::json!(["unique_key"])
    );
    assert_eq!(
        report["blocker_codes"],
        serde_json::json!(["hawdb_production_filter_pruning_not_ready"])
    );
}

#[test]
fn hawdb_search_projection_evidence_requires_normalized_default_equality_flag() {
    let mut probe = ready_probe();
    let samples = probe["production_filter_pruning"]["samples"]
        .as_array_mut()
        .unwrap();
    let sample = samples
        .iter_mut()
        .find(|sample| sample["operation_family"].as_str() == Some("normalized_default_equality"))
        .unwrap();
    sample["normalized_default_equality"] = serde_json::json!(false);

    let report = nowledge_search_projection_evidence_json(&probe);

    assert_eq!(report["ready"], false);
    assert_eq!(report["production_filter_pruning_ready"], false);
    assert_eq!(
        report["production_filter_pruning"]["missing_sample_fields"],
        serde_json::json!(["is_latest"])
    );
    assert_eq!(
        report["production_filter_pruning"]["missing_operation_families"],
        serde_json::json!(["normalized_default_equality"])
    );
    assert_eq!(
        report["blocker_codes"],
        serde_json::json!(["hawdb_production_filter_pruning_not_ready"])
    );
}

#[test]
fn hawdb_search_projection_evidence_requires_descriptor_field_summaries() {
    let mut probe = ready_probe();
    probe["predicate_pushdown"]
        .as_object_mut()
        .unwrap()
        .remove("segment_descriptor_field_summaries");

    let report = nowledge_search_projection_evidence_json(&probe);

    assert_eq!(report["ready"], false);
    assert_eq!(report["predicate_pushdown_ready"], true);
    assert_eq!(report["hawdb_predicate_pushdown_ready"], false);
    assert_eq!(
        report["predicate_pushdown"]["segment_descriptor_scan_filter_fields_ready"],
        false
    );
    assert!(report["blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "hawdb_predicate_pushdown_descriptor_not_ready"));
}

#[test]
fn hawdb_search_projection_evidence_requires_descriptor_field_capabilities() {
    let mut probe = ready_probe();
    let summaries = probe["predicate_pushdown"]["segment_descriptor_field_summaries"]
        .as_array_mut()
        .unwrap();
    let importance = summaries
        .iter_mut()
        .find(|summary| summary["field"] == "importance")
        .unwrap();
    importance["numeric_range_summary_used"] = serde_json::json!(false);

    let report = nowledge_search_projection_evidence_json(&probe);

    assert_eq!(report["ready"], false);
    assert_eq!(report["predicate_pushdown_ready"], true);
    assert_eq!(report["hawdb_predicate_pushdown_ready"], false);
    assert_eq!(
        report["predicate_pushdown"]["segment_descriptor_scan_filter_fields_ready"],
        false
    );
    assert_eq!(
        report["predicate_pushdown"]["segment_descriptor_capabilities_ready"],
        false
    );
    assert_eq!(
        report["predicate_pushdown"]["missing_numeric_range_fields"],
        serde_json::json!(["importance"])
    );
    assert!(report["blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "hawdb_predicate_pushdown_descriptor_not_ready"));
}

#[test]
fn hawdb_search_projection_evidence_requires_descriptor_summary_counts() {
    let mut probe = ready_probe();
    let summaries = probe["predicate_pushdown"]["segment_descriptor_field_summaries"]
        .as_array_mut()
        .unwrap();
    let lifecycle_state = summaries
        .iter_mut()
        .find(|summary| summary["field"] == "lifecycle_state")
        .unwrap();
    lifecycle_state["value_summary_used"] = serde_json::json!(true);
    lifecycle_state["value_summary_segment_count"] = serde_json::json!(0);

    let report = nowledge_search_projection_evidence_json(&probe);

    assert_eq!(report["ready"], false);
    assert_eq!(report["predicate_pushdown_ready"], true);
    assert_eq!(report["hawdb_predicate_pushdown_ready"], false);
    assert_eq!(
        report["predicate_pushdown"]["segment_descriptor_capabilities_ready"],
        false
    );
    assert_eq!(
        report["predicate_pushdown"]["missing_value_summary_fields"],
        serde_json::json!(["lifecycle_state"])
    );
    assert!(report["blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "hawdb_predicate_pushdown_descriptor_not_ready"));
}

#[test]
fn hawdb_search_projection_evidence_requires_unique_key_descriptor_summary() {
    let mut probe = ready_probe();
    let summaries = probe["predicate_pushdown"]["segment_descriptor_field_summaries"]
        .as_array_mut()
        .unwrap();
    let document_id = summaries
        .iter_mut()
        .find(|summary| summary["field"] == "document_id")
        .unwrap();
    document_id["unique_key_summary_used"] = serde_json::json!(false);

    let report = nowledge_search_projection_evidence_json(&probe);

    assert_eq!(report["ready"], false);
    assert_eq!(report["predicate_pushdown_ready"], true);
    assert_eq!(report["hawdb_predicate_pushdown_ready"], false);
    assert_eq!(
        report["predicate_pushdown"]["segment_descriptor_capabilities_ready"],
        false
    );
    assert_eq!(
        report["predicate_pushdown"]["missing_unique_key_summary_fields"],
        serde_json::json!(["document_id"])
    );
    assert!(report["blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "hawdb_predicate_pushdown_descriptor_not_ready"));
}

#[test]
fn search_projection_shadow_evidence_reports_ready_for_matching_probes() {
    let primary = ready_probe();
    let shadow = ready_probe();

    let report = nowledge_search_projection_shadow_evidence_json(&primary, &shadow);

    assert_eq!(report["ready"], true);
    assert_eq!(report["evidence_source"], "hawdb-rust-library");
    assert_eq!(report["primary_ready"], true);
    assert_eq!(report["shadow_ready"], true);
    assert_eq!(report["document_count_parity"], true);
    assert_eq!(report["document_identity_parity"], true);
    assert_eq!(report["table_parity"]["ready"], true);
    assert_eq!(report["embedding_identity_parity"], true);
    assert_eq!(report["lifecycle_parity"], true);
    assert_eq!(report["incremental_watermark_parity"], true);
    assert_eq!(report["pushdown_evidence"]["ready"], true);
    assert_eq!(
        report["pushdown_evidence"]["shadow_persisted_segment_descriptor_ready"],
        true
    );
    assert_eq!(
        report["pushdown_evidence"]["shadow_segment_document_pruning_ready"],
        true
    );
    assert_eq!(
        report["pushdown_evidence"]["shadow_segment_pruning_candidate_document_count"],
        6
    );
    assert_eq!(
        report["pushdown_evidence"]["shadow_segment_pruned_document_count"],
        4
    );
    assert_eq!(
        report["pushdown_evidence"]["shadow_segment_scanned_document_count"],
        2
    );
    assert_eq!(report["blocker_codes"], serde_json::json!([]));
}

#[test]
fn search_projection_shadow_evidence_keeps_predicate_parity_separate_from_readiness() {
    let mut primary = ready_probe();
    primary["predicate_pushdown"]["persisted_segment_descriptor_ready"] = serde_json::json!(false);
    let shadow = ready_probe();

    let report = nowledge_search_projection_shadow_evidence_json(&primary, &shadow);

    assert_eq!(report["ready"], false);
    assert_eq!(report["predicate_pushdown_parity"], true);
    assert_eq!(report["pushdown_evidence"]["ready"], true);
    assert!(report["blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "primary_not_ready"));
}

#[test]
fn search_projection_shadow_evidence_requires_shadow_segment_descriptor() {
    let primary = ready_probe();
    let mut shadow = ready_probe();
    shadow["predicate_pushdown"]["persisted_segment_descriptor_ready"] = serde_json::json!(false);

    let report = nowledge_search_projection_shadow_evidence_json(&primary, &shadow);

    assert_eq!(report["ready"], false);
    assert_eq!(report["predicate_pushdown_parity"], true);
    assert_eq!(report["pushdown_evidence"]["ready"], false);
    assert_eq!(
        report["pushdown_evidence"]["shadow_persisted_segment_descriptor_ready"],
        false
    );
    assert!(report["blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "search_projection_shadow_pushdown_evidence_not_ready"));
    assert!(report["blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "hawdb_search_projection_segment_descriptor_missing"));
}

#[test]
fn search_projection_shadow_evidence_requires_shadow_descriptor_field_summaries() {
    let primary = ready_probe();
    let mut shadow = ready_probe();
    shadow["predicate_pushdown"]
        .as_object_mut()
        .unwrap()
        .remove("segment_descriptor_field_summaries");

    let report = nowledge_search_projection_shadow_evidence_json(&primary, &shadow);

    assert_eq!(report["ready"], false);
    assert_eq!(report["pushdown_evidence"]["ready"], false);
    assert_eq!(
        report["pushdown_evidence"]["shadow_persisted_segment_descriptor_ready"],
        true
    );
    assert_eq!(
        report["pushdown_evidence"]["shadow_segment_descriptor_scan_filter_fields_ready"],
        false
    );
    assert!(report["blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "search_projection_shadow_pushdown_evidence_not_ready"));
    assert!(report["blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == HAWDB_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_FIELDS_MISSING));
}

#[test]
fn search_projection_shadow_evidence_fails_closed_on_table_mismatch() {
    let primary = ready_probe();
    let mut shadow = ready_probe();
    shadow["tables"]
        .as_array_mut()
        .unwrap()
        .retain(|table| table["name"].as_str() != Some("source_chunks_index"));

    let report = nowledge_search_projection_shadow_evidence_json(&primary, &shadow);

    assert_eq!(report["ready"], false);
    assert_eq!(report["shadow_ready"], false);
    assert_eq!(report["table_parity"]["ready"], false);
    assert!(report["blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "table_parity_mismatch"));
    assert!(report["blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "shadow_source_chunks_index_not_ready"));
}

#[test]
fn search_projection_shadow_evidence_fails_closed_on_document_identity_mismatch() {
    let primary = ready_probe();
    let mut shadow = ready_probe();
    shadow["document_identity"]["checksum"] = serde_json::json!(999);

    let report = nowledge_search_projection_shadow_evidence_json(&primary, &shadow);

    assert_eq!(report["ready"], false);
    assert_eq!(report["document_count_parity"], true);
    assert_eq!(report["document_identity_parity"], false);
    assert!(report["blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "document_identity_mismatch"));
}

fn ready_probe() -> serde_json::Value {
    serde_json::json!({
        "engine": "hawdb",
        "derived_projection": true,
        "document_count": 6,
        "document_identity": {
            "ready": true,
            "id_space": "search_projection_document_id",
            "representation": "sorted_document_ids",
            "document_count": 6,
            "checksum": 42
        },
        "tables": [
            table("memories_index", true),
            table("messages_index", false),
            table("communities_index", true),
            table("entities_index", true),
            table("sources_index", true),
            table("source_chunks_index", true)
        ],
        "embedding_manifest": {
            "model": "bge-m3",
            "dimension": 1024,
            "active_model": "bge-m3",
            "active_dimension": 1024
        },
        "fail_soft": {
            "fts_to_vector_ready": true,
            "vector_to_fts_ready": true,
            "no_500_on_leg_failure": true
        },
        "lifecycle": {
            "rebuild_marker_ready": true,
            "metadata_repair_marker_ready": true
        },
        "incremental_update": {
            "ready": true,
            "upsert_ready": true,
            "delete_ready": true,
            "watermark_ready": true,
            "source_graph_commit_epoch": 7
        },
        "predicate_pushdown": {
            "ready": true,
            "equality_ready": true,
            "in_list_ready": true,
            "not_in_list_ready": true,
            "range_ready": true,
            "row_filter_ready": true,
            "segment_pruning_ready": true,
            "numeric_min_max_ready": true,
            "timestamp_min_max_ready": true,
            "persisted_segment_descriptor_ready": true,
            "segment_document_pruning_ready": true,
            "segment_pruning_candidate_document_count": 6,
            "segment_pruned_document_count": 4,
            "segment_scanned_document_count": 2,
            "supported_ops": ["eq", "in", "not_in", "gt", "gte", "lt", "lte"],
            "scan_filter_fields": NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS,
            "segment_descriptor_field_summaries": ready_segment_descriptor_field_summaries()
        },
        "production_filter_pruning": ready_production_filter_pruning_template(),
        "compressed_vector_projection": {
            "engine": "hawdb_rabitq_scan",
            "algorithm": "rabitq",
            "compiled": true,
            "ready": true,
            "bit_width": 4,
            "quantizer": "rabitq_sign_then_refinement_scalar_4bit_v1",
            "calibration": "none",
            "dimension": 1024,
            "document_count": 5,
            "supports_allowlist": true,
            "persisted_artifact_used": true,
            "artifact_rebuilt_from_snapshot": false,
            "blocker_codes": []
        }
    })
}

fn table(name: &str, vector_ready: bool) -> serde_json::Value {
    serde_json::json!({
        "name": name,
        "fts_ready": true,
        "vector_ready": vector_ready,
        "row_count": 1,
        "blocker_codes": []
    })
}

fn ready_segment_descriptor_field_summaries() -> serde_json::Value {
    let mut fields = NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
        .iter()
        .map(|field| {
            descriptor_field(
                field,
                true,
                matches!(field, &"importance" | &"confidence"),
                matches!(
                    field,
                    &"created_at" | &"updated_at" | &"event_start" | &"event_end"
                ),
            )
        })
        .collect::<Vec<_>>();
    fields.push(descriptor_field("document_id", true, false, false));
    serde_json::Value::Array(fields)
}

fn descriptor_field(
    field: &str,
    value_summary_used: bool,
    numeric_range_summary_used: bool,
    timestamp_range_summary_used: bool,
) -> serde_json::Value {
    serde_json::json!({
        "field": field,
        "segment_count": 1,
        "present_document_count": 1,
        "value_summary_used": value_summary_used,
        "value_summary_segment_count": usize::from(value_summary_used),
        "numeric_range_summary_used": numeric_range_summary_used,
        "numeric_range_segment_count": usize::from(numeric_range_summary_used),
        "timestamp_range_summary_used": timestamp_range_summary_used,
        "timestamp_range_segment_count": usize::from(timestamp_range_summary_used),
        "unique_key_summary_used": field == "document_id",
        "unique_key_summary_segment_count": usize::from(field == "document_id"),
    })
}
