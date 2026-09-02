use crate::{
    Result, SearchIndex, SearchProjectionProbeOptions, SkeinError,
    NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS,
};
use std::collections::BTreeSet;
use std::path::Path;

const REQUIRED_TABLES: &[&str] = &[
    "memories_index",
    "messages_index",
    "communities_index",
    "entities_index",
    "sources_index",
    "source_chunks_index",
];

const VECTOR_TABLES: &[&str] = &[
    "memories_index",
    "communities_index",
    "entities_index",
    "sources_index",
    "source_chunks_index",
];

const SKEIN_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_FIELDS_MISSING: &str =
    "skein_search_projection_segment_descriptor_fields_missing";
const SKEIN_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE: &str = "skein-rust-library";
const REQUIRED_VALUE_SUMMARY_FIELDS: &[&str] = &[
    "kind",
    "external_id",
    "source_id",
    "space_id",
    "unit_type",
    "lifecycle_state",
    "is_latest",
];
const REQUIRED_NUMERIC_RANGE_FIELDS: &[&str] = &["importance", "confidence"];
const REQUIRED_TIMESTAMP_RANGE_FIELDS: &[&str] =
    &["created_at", "updated_at", "event_start", "event_end"];
const REQUIRED_UNIQUE_KEY_SUMMARY_FIELDS: &[&str] = &["document_id"];
const REQUIRED_PRODUCTION_FILTER_OPERATION_FAMILIES: &[&str] = &[
    "equality",
    "enum_in_list",
    "numeric_range",
    "timestamp_range",
    "normalized_default_equality",
    "unique_key",
];

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeSearchProjectionEvidenceReport {
    pub protocol: String,
    pub ready: bool,
    pub derived_projection: bool,
    pub all_tables_covered: bool,
    pub covered_table_count: u64,
    pub required_table_count: u64,
    pub fts_ready: bool,
    pub vector_ready: bool,
    pub document_identity_ready: bool,
    pub embedding_identity_ready: bool,
    pub fail_soft_ready: bool,
    pub rebuild_marker_ready: bool,
    pub metadata_repair_marker_ready: bool,
    pub incremental_update_ready: bool,
    pub source_chunk_ready: bool,
    pub predicate_pushdown_ready: bool,
    pub skein_predicate_pushdown_ready: bool,
    pub production_filter_pruning_ready: bool,
    pub compressed_vector_projection_required: bool,
    pub compressed_vector_projection_ready: bool,
    pub blocker_codes: Vec<String>,
    evidence: serde_json::Value,
}

impl NowledgeSearchProjectionEvidenceReport {
    pub fn from_probe(probe: &serde_json::Value) -> Self {
        Self::from_evidence(nowledge_search_projection_evidence_json(probe))
    }

    pub fn from_evidence(evidence: serde_json::Value) -> Self {
        Self {
            protocol: str_path(&evidence, &["protocol"])
                .unwrap_or("skein-nowledge-search-projection-evidence")
                .to_string(),
            ready: bool_path(&evidence, &["ready"]).unwrap_or(false),
            derived_projection: bool_path(&evidence, &["derived_projection"]).unwrap_or(false),
            all_tables_covered: bool_path(&evidence, &["all_tables_covered"]).unwrap_or(false),
            covered_table_count: u64_path(&evidence, &["covered_table_count"]).unwrap_or(0),
            required_table_count: u64_path(&evidence, &["required_table_count"])
                .unwrap_or(REQUIRED_TABLES.len() as u64),
            fts_ready: bool_path(&evidence, &["fts_ready"]).unwrap_or(false),
            vector_ready: bool_path(&evidence, &["vector_ready"]).unwrap_or(false),
            document_identity_ready: bool_path(&evidence, &["document_identity_ready"])
                .unwrap_or(false),
            embedding_identity_ready: bool_path(&evidence, &["embedding_identity_ready"])
                .unwrap_or(false),
            fail_soft_ready: bool_path(&evidence, &["fail_soft_ready"]).unwrap_or(false),
            rebuild_marker_ready: bool_path(&evidence, &["rebuild_marker_ready"]).unwrap_or(false),
            metadata_repair_marker_ready: bool_path(&evidence, &["metadata_repair_marker_ready"])
                .unwrap_or(false),
            incremental_update_ready: bool_path(&evidence, &["incremental_update_ready"])
                .unwrap_or(false),
            source_chunk_ready: bool_path(&evidence, &["source_chunk_ready"]).unwrap_or(false),
            predicate_pushdown_ready: bool_path(&evidence, &["predicate_pushdown_ready"])
                .unwrap_or(false),
            skein_predicate_pushdown_ready: bool_path(
                &evidence,
                &["skein_predicate_pushdown_ready"],
            )
            .unwrap_or(false),
            production_filter_pruning_ready: bool_path(
                &evidence,
                &["production_filter_pruning_ready"],
            )
            .unwrap_or(false),
            compressed_vector_projection_required: bool_path(
                &evidence,
                &["compressed_vector_projection_required"],
            )
            .unwrap_or(false),
            compressed_vector_projection_ready: bool_path(
                &evidence,
                &["compressed_vector_projection_ready"],
            )
            .unwrap_or(false),
            blocker_codes: array_path(&evidence, &["blocker_codes"]).unwrap_or_default(),
            evidence,
        }
    }

    pub fn json(&self) -> serde_json::Value {
        self.evidence.clone()
    }
}

pub fn nowledge_search_projection_probe_contract_json() -> serde_json::Value {
    serde_json::json!({
        "protocol": "skein-nowledge-search-projection-probe-contract-v1",
        "purpose": "primary LanceDB and shadow Skein probes must use this shape before search projection shadow evidence can pass",
        "required_tables": REQUIRED_TABLES,
        "vector_tables": VECTOR_TABLES,
        "required_predicate_pushdown_ops": ["eq", "in", "not_in", "gt", "gte", "lt", "lte"],
        "required_top_level_fields": [
            "engine",
            "derived_projection",
            "document_count",
            "document_identity",
            "tables",
            "embedding_manifest",
            "fail_soft",
            "lifecycle",
            "incremental_update",
            "predicate_pushdown",
            "production_filter_pruning"
        ],
        "table_fields": [
            "name",
            "present",
            "fts_ready",
            "vector_ready",
            "row_count",
            "blocker_codes"
        ],
        "embedding_manifest_fields": [
            "model",
            "dimension",
            "active_model",
            "active_dimension"
        ],
        "fail_soft_fields": [
            "fts_to_vector_ready",
            "vector_to_fts_ready",
            "no_500_on_leg_failure"
        ],
        "lifecycle_fields": [
            "rebuild_marker_ready",
            "metadata_repair_marker_ready"
        ],
        "incremental_update_fields": [
            "ready",
            "upsert_ready",
            "delete_ready",
            "watermark_ready",
            "source_graph_commit_epoch"
        ],
        "predicate_pushdown_fields": [
            "equality_ready",
            "in_list_ready",
            "not_in_list_ready",
            "range_ready",
            "row_filter_ready",
            "segment_pruning_ready",
            "numeric_min_max_ready",
            "timestamp_min_max_ready",
            "persisted_segment_descriptor_ready",
            "segment_descriptor_scan_filter_fields_ready",
            "segment_document_pruning_ready",
            "segment_pruning_candidate_document_count",
            "segment_pruned_document_count",
            "segment_scanned_document_count",
            "supported_ops",
            "scan_filter_fields",
            "segment_descriptor_field_count",
            "segment_descriptor_field_summaries"
        ],
        "production_filter_pruning_fields": [
            "ready",
            "persisted_segment_descriptor_used",
            "payload_read_avoidance_ready",
            "explain_analyze_ready",
            "sample_evidence_ready",
            "sample_count",
            "ready_field_count",
            "required_field_count",
            "missing_fields",
            "samples"
        ],
        "required_skein_scan_filter_fields": NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS,
        "required_segment_descriptor_capabilities": {
            "value_summary_fields": REQUIRED_VALUE_SUMMARY_FIELDS,
            "numeric_range_fields": REQUIRED_NUMERIC_RANGE_FIELDS,
            "timestamp_range_fields": REQUIRED_TIMESTAMP_RANGE_FIELDS,
            "unique_key_fields": REQUIRED_UNIQUE_KEY_SUMMARY_FIELDS
        },
        "segment_descriptor_field_summary_fields": [
            "field",
            "segment_count",
            "value_summary_used",
            "numeric_range_summary_used",
            "timestamp_range_summary_used",
            "unique_key_summary_used"
        ],
        "document_identity_fields": [
            "ready",
            "id_space",
            "representation",
            "document_count",
            "checksum"
        ],
        "example_primary_probe": ready_probe_template("lancedb"),
        "example_skein_probe": ready_probe_template("skein"),
    })
}

pub fn nowledge_search_projection_evidence_usage() -> String {
    "nowledge-search-projection-evidence requires [--require-ready] <search-projection-probe-json>"
        .to_string()
}

pub fn skein_search_projection_probe_usage() -> String {
    "skein-search-projection-probe requires [--active-model <model>] [--active-dimension <dimension>] <search-index-dir>"
        .to_string()
}

pub fn nowledge_search_projection_shadow_evidence_usage() -> String {
    "nowledge-search-projection-shadow-evidence requires [--require-ready] --primary-probe-json <path> --shadow-probe-json <path>"
        .to_string()
}

pub fn nowledge_search_projection_probe_contract_usage() -> String {
    "nowledge-search-projection-probe-contract requires no arguments".to_string()
}

pub fn run_nowledge_search_projection_evidence(
    mut args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let mut require_ready = false;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--require-ready" => {
                require_ready = true;
            }
            path => {
                if args.next().is_some() {
                    return Err(SkeinError::Semantic(
                        nowledge_search_projection_evidence_usage(),
                    ));
                }
                let probe = read_json_file(Path::new(path))?;
                return Ok((
                    nowledge_search_projection_evidence_json(&probe),
                    require_ready,
                ));
            }
        }
    }
    Err(SkeinError::Semantic(
        nowledge_search_projection_evidence_usage(),
    ))
}

pub fn run_skein_search_projection_probe(
    mut args: impl Iterator<Item = String>,
) -> Result<serde_json::Value> {
    let mut options = SearchProjectionProbeOptions::default();
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--active-model" => {
                options.active_embedding_model =
                    Some(args.next().ok_or_else(|| {
                        SkeinError::Semantic(skein_search_projection_probe_usage())
                    })?);
            }
            "--active-dimension" => {
                let raw_dimension = args
                    .next()
                    .ok_or_else(|| SkeinError::Semantic(skein_search_projection_probe_usage()))?;
                options.active_embedding_dimension =
                    Some(parse_positive_usize("--active-dimension", &raw_dimension)?);
            }
            path => {
                if args.next().is_some() {
                    return Err(SkeinError::Semantic(skein_search_projection_probe_usage()));
                }
                let index = SearchIndex::open(path)?;
                return Ok(index.nowledge_search_projection_probe_json(options));
            }
        }
    }
    Err(SkeinError::Semantic(skein_search_projection_probe_usage()))
}

pub fn run_nowledge_search_projection_shadow_evidence(
    mut args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let mut require_ready = false;
    let mut primary_probe = None;
    let mut shadow_probe = None;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--require-ready" => {
                require_ready = true;
            }
            "--primary-probe-json" => {
                let path = args.next().ok_or_else(|| {
                    SkeinError::Semantic(nowledge_search_projection_shadow_evidence_usage())
                })?;
                primary_probe = Some(read_json_file(Path::new(&path))?);
            }
            "--shadow-probe-json" => {
                let path = args.next().ok_or_else(|| {
                    SkeinError::Semantic(nowledge_search_projection_shadow_evidence_usage())
                })?;
                shadow_probe = Some(read_json_file(Path::new(&path))?);
            }
            _ => {
                return Err(SkeinError::Semantic(
                    nowledge_search_projection_shadow_evidence_usage(),
                ));
            }
        }
    }
    let primary_probe = primary_probe
        .ok_or_else(|| SkeinError::Semantic(nowledge_search_projection_shadow_evidence_usage()))?;
    let shadow_probe = shadow_probe
        .ok_or_else(|| SkeinError::Semantic(nowledge_search_projection_shadow_evidence_usage()))?;
    Ok((
        nowledge_search_projection_shadow_evidence_json(&primary_probe, &shadow_probe),
        require_ready,
    ))
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let content = std::fs::read_to_string(path).map_err(|_| {
        SkeinError::Execution(
            "failed to read search projection evidence JSON: io_error".to_string(),
        )
    })?;
    serde_json::from_str(&content).map_err(|_| {
        SkeinError::Semantic(
            "failed to parse search projection evidence JSON: invalid_json".to_string(),
        )
    })
}

fn parse_positive_usize(flag: &str, value: &str) -> Result<usize> {
    let parsed = value.parse::<usize>().map_err(|error| {
        SkeinError::Semantic(format!("invalid {flag} value '{value}': {error}"))
    })?;
    if parsed == 0 {
        return Err(SkeinError::Semantic(format!(
            "invalid {flag} value '{value}': expected a positive integer"
        )));
    }
    Ok(parsed)
}

pub fn nowledge_search_projection_evidence_json(probe: &serde_json::Value) -> serde_json::Value {
    let table_reports = required_table_reports(probe);
    let covered_table_count = table_reports
        .iter()
        .filter(|table| bool_path(table, &["present"]) == Some(true))
        .count() as u64;
    let required_table_count = REQUIRED_TABLES.len() as u64;
    let all_tables_covered = covered_table_count == required_table_count;
    let fts_ready = table_reports
        .iter()
        .all(|table| bool_path(table, &["fts_ready"]) == Some(true));
    let vector_ready = table_reports.iter().all(|table| {
        str_path(table, &["name"]).is_some_and(|name| {
            !VECTOR_TABLES.contains(&name) || bool_path(table, &["vector_ready"]) == Some(true)
        })
    });
    let source_chunk_ready = table_reports.iter().any(|table| {
        str_path(table, &["name"]) == Some("source_chunks_index")
            && bool_path(table, &["present"]) == Some(true)
            && bool_path(table, &["fts_ready"]) == Some(true)
            && bool_path(table, &["vector_ready"]) == Some(true)
    });
    let document_identity = document_identity_report(probe);
    let document_identity_ready = bool_path(&document_identity, &["ready"]) == Some(true);
    let embedding_identity = embedding_identity_report(probe);
    let embedding_identity_ready = bool_path(&embedding_identity, &["ready"]) == Some(true);
    let fail_soft = fail_soft_report(probe);
    let fail_soft_ready = bool_path(&fail_soft, &["ready"]) == Some(true);
    let lifecycle = lifecycle_report(probe);
    let rebuild_marker_ready = bool_path(&lifecycle, &["rebuild_marker_ready"]) == Some(true);
    let metadata_repair_marker_ready =
        bool_path(&lifecycle, &["metadata_repair_marker_ready"]) == Some(true);
    let incremental_update = incremental_update_report(probe);
    let incremental_update_ready = bool_path(&incremental_update, &["ready"]) == Some(true);
    let predicate_pushdown = predicate_pushdown_report(probe);
    let predicate_pushdown_ready = bool_path(&predicate_pushdown, &["ready"]) == Some(true);
    let skein_probe = is_skein_search_projection_probe(probe);
    let skein_predicate_pushdown_ready = !skein_probe
        || bool_path(&predicate_pushdown, &["persisted_segment_descriptor_ready"]) == Some(true)
            && bool_path(
                &predicate_pushdown,
                &["segment_descriptor_scan_filter_fields_ready"],
            ) == Some(true)
            && bool_path(
                &predicate_pushdown,
                &["segment_descriptor_capabilities_ready"],
            ) == Some(true)
            && bool_path(&predicate_pushdown, &["segment_document_pruning_ready"]) == Some(true);
    let production_filter_pruning = production_filter_pruning_report(probe);
    let production_filter_pruning_ready =
        !skein_probe || bool_path(&production_filter_pruning, &["ready"]) == Some(true);
    let compressed_vector_projection = compressed_vector_projection_report(probe);
    let compressed_vector_projection_required = vector_ready && skein_probe;
    let compressed_vector_projection_ready = !compressed_vector_projection_required
        || bool_path(&compressed_vector_projection, &["ready"]) == Some(true);
    let derived_projection = bool_path(probe, &["derived_projection"])
        .or_else(|| bool_path(probe, &["projection", "derived"]))
        == Some(true);

    let mut blocker_codes = BTreeSet::new();
    collect_probe_blockers(probe, &mut blocker_codes);
    if !derived_projection {
        blocker_codes.insert("not_derived_projection".to_string());
    }
    if !all_tables_covered {
        blocker_codes.insert("missing_required_search_tables".to_string());
    }
    if !fts_ready {
        blocker_codes.insert("fts_not_ready".to_string());
    }
    if !vector_ready {
        blocker_codes.insert("vector_not_ready".to_string());
    }
    if !embedding_identity_ready {
        blocker_codes.insert("embedding_identity_not_ready".to_string());
    }
    if !fail_soft_ready {
        blocker_codes.insert("fail_soft_not_ready".to_string());
    }
    if !rebuild_marker_ready {
        blocker_codes.insert("rebuild_marker_not_ready".to_string());
    }
    if !metadata_repair_marker_ready {
        blocker_codes.insert("metadata_repair_marker_not_ready".to_string());
    }
    if !incremental_update_ready {
        blocker_codes.insert("incremental_update_not_ready".to_string());
    }
    if !source_chunk_ready {
        blocker_codes.insert("source_chunks_index_not_ready".to_string());
    }
    if !document_identity_ready {
        blocker_codes.insert("document_identity_not_ready".to_string());
    }
    if !predicate_pushdown_ready {
        blocker_codes.insert("predicate_pushdown_not_ready".to_string());
    }
    if !skein_predicate_pushdown_ready {
        blocker_codes.insert("skein_predicate_pushdown_descriptor_not_ready".to_string());
    }
    if !production_filter_pruning_ready {
        blocker_codes.insert("skein_production_filter_pruning_not_ready".to_string());
    }
    if !compressed_vector_projection_ready {
        blocker_codes.insert("compressed_vector_projection_not_ready".to_string());
    }

    let ready = blocker_codes.is_empty();
    serde_json::json!({
        "protocol": "skein-nowledge-search-projection-evidence",
        "ready": ready,
        "derived_projection": derived_projection,
        "all_tables_covered": all_tables_covered,
        "covered_table_count": covered_table_count,
        "required_table_count": required_table_count,
        "required_tables": REQUIRED_TABLES,
        "fts_ready": fts_ready,
        "vector_ready": vector_ready,
        "document_identity_ready": document_identity_ready,
        "embedding_identity_ready": embedding_identity_ready,
        "fail_soft_ready": fail_soft_ready,
        "rebuild_marker_ready": rebuild_marker_ready,
        "metadata_repair_marker_ready": metadata_repair_marker_ready,
        "incremental_update_ready": incremental_update_ready,
        "source_chunk_ready": source_chunk_ready,
        "predicate_pushdown_ready": predicate_pushdown_ready,
        "skein_predicate_pushdown_ready": skein_predicate_pushdown_ready,
        "production_filter_pruning_ready": production_filter_pruning_ready,
        "compressed_vector_projection_required": compressed_vector_projection_required,
        "compressed_vector_projection_ready": compressed_vector_projection_ready,
        "tables": table_reports,
        "document_identity": document_identity,
        "embedding_identity": embedding_identity,
        "fail_soft": fail_soft,
        "lifecycle": lifecycle,
        "incremental_update": incremental_update,
        "predicate_pushdown": predicate_pushdown,
        "production_filter_pruning": production_filter_pruning,
        "compressed_vector_projection": compressed_vector_projection,
        "blocker_codes": blocker_codes.into_iter().collect::<Vec<_>>(),
    })
}

pub fn nowledge_search_projection_shadow_evidence_json(
    primary_probe: &serde_json::Value,
    shadow_probe: &serde_json::Value,
) -> serde_json::Value {
    let primary_evidence = nowledge_search_projection_evidence_json(primary_probe);
    let shadow_evidence = nowledge_search_projection_evidence_json(shadow_probe);
    let table_parity = search_projection_table_parity(&primary_evidence, &shadow_evidence);
    let document_count_parity =
        u64_path(primary_probe, &["document_count"]) == u64_path(shadow_probe, &["document_count"]);
    let document_identity_parity = value_path(&primary_evidence, &["document_identity"])
        == value_path(&shadow_evidence, &["document_identity"]);
    let embedding_identity_parity = value_path(&primary_evidence, &["embedding_identity"])
        == value_path(&shadow_evidence, &["embedding_identity"]);
    let lifecycle_parity = value_path(&primary_evidence, &["lifecycle"])
        == value_path(&shadow_evidence, &["lifecycle"]);
    let predicate_pushdown_parity =
        predicate_pushdown_parity_matches(&primary_evidence, &shadow_evidence);
    let pushdown_evidence =
        search_projection_shadow_pushdown_evidence(&primary_evidence, &shadow_evidence);
    let pushdown_ready = bool_path(&pushdown_evidence, &["ready"]).unwrap_or(false);
    let incremental_watermark_parity = u64_path(
        primary_probe,
        &["incremental_update", "source_graph_commit_epoch"],
    ) == u64_path(
        shadow_probe,
        &["incremental_update", "source_graph_commit_epoch"],
    );
    let mut blocker_codes = BTreeSet::new();
    collect_prefixed_evidence_blockers("primary", &primary_evidence, &mut blocker_codes);
    collect_prefixed_evidence_blockers("shadow", &shadow_evidence, &mut blocker_codes);
    if bool_path(&primary_evidence, &["ready"]) != Some(true) {
        blocker_codes.insert("primary_not_ready".to_string());
    }
    if bool_path(&shadow_evidence, &["ready"]) != Some(true) {
        blocker_codes.insert("shadow_not_ready".to_string());
    }
    if !bool_path(&table_parity, &["ready"]).unwrap_or(false) {
        blocker_codes.insert("table_parity_mismatch".to_string());
    }
    if !document_count_parity {
        blocker_codes.insert("document_count_mismatch".to_string());
    }
    if !document_identity_parity {
        blocker_codes.insert("document_identity_mismatch".to_string());
    }
    if !embedding_identity_parity {
        blocker_codes.insert("embedding_identity_mismatch".to_string());
    }
    if !lifecycle_parity {
        blocker_codes.insert("lifecycle_mismatch".to_string());
    }
    if !incremental_watermark_parity {
        blocker_codes.insert("incremental_watermark_mismatch".to_string());
    }
    if !predicate_pushdown_parity {
        blocker_codes.insert("predicate_pushdown_mismatch".to_string());
    }
    if !pushdown_ready {
        blocker_codes.insert("search_projection_shadow_pushdown_evidence_not_ready".to_string());
    }
    if bool_path(
        &pushdown_evidence,
        &["shadow_persisted_segment_descriptor_ready"],
    ) != Some(true)
    {
        blocker_codes.insert("skein_search_projection_segment_descriptor_missing".to_string());
    }
    if bool_path(
        &pushdown_evidence,
        &["shadow_segment_descriptor_scan_filter_fields_ready"],
    ) != Some(true)
    {
        blocker_codes.insert(SKEIN_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_FIELDS_MISSING.to_string());
    }
    let ready = blocker_codes.is_empty();
    serde_json::json!({
        "protocol": "skein-nowledge-search-projection-shadow-evidence",
        "evidence_source": SKEIN_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE,
        "ready": ready,
        "primary_engine": str_path(primary_probe, &["engine"]).unwrap_or("lancedb"),
        "shadow_engine": str_path(shadow_probe, &["engine"]).unwrap_or("skein"),
        "primary_ready": bool_path(&primary_evidence, &["ready"]).unwrap_or(false),
        "shadow_ready": bool_path(&shadow_evidence, &["ready"]).unwrap_or(false),
        "document_count_parity": document_count_parity,
        "document_identity_parity": document_identity_parity,
        "primary_document_count": u64_path(primary_probe, &["document_count"]),
        "shadow_document_count": u64_path(shadow_probe, &["document_count"]),
        "table_parity": table_parity,
        "embedding_identity_parity": embedding_identity_parity,
        "lifecycle_parity": lifecycle_parity,
        "incremental_watermark_parity": incremental_watermark_parity,
        "predicate_pushdown_parity": predicate_pushdown_parity,
        "pushdown_evidence": pushdown_evidence,
        "primary_evidence": primary_evidence,
        "shadow_evidence": shadow_evidence,
        "blocker_codes": blocker_codes.into_iter().collect::<Vec<_>>(),
    })
}

fn search_projection_shadow_pushdown_evidence(
    primary_evidence: &serde_json::Value,
    shadow_evidence: &serde_json::Value,
) -> serde_json::Value {
    let predicate_pushdown_parity =
        predicate_pushdown_parity_matches(primary_evidence, shadow_evidence);
    let primary_predicate_pushdown_ready =
        bool_path(primary_evidence, &["predicate_pushdown", "ready"]).unwrap_or(false);
    let shadow_predicate_pushdown_ready =
        bool_path(shadow_evidence, &["predicate_pushdown", "ready"]).unwrap_or(false);
    let shadow_persisted_segment_descriptor_ready = bool_path(
        shadow_evidence,
        &["predicate_pushdown", "persisted_segment_descriptor_ready"],
    )
    .unwrap_or(false);
    let shadow_segment_descriptor_scan_filter_fields_ready = bool_path(
        shadow_evidence,
        &[
            "predicate_pushdown",
            "segment_descriptor_scan_filter_fields_ready",
        ],
    )
    .unwrap_or(false);
    let shadow_segment_document_pruning_ready = bool_path(
        shadow_evidence,
        &["predicate_pushdown", "segment_document_pruning_ready"],
    )
    .unwrap_or(false);
    let ready = predicate_pushdown_parity
        && primary_predicate_pushdown_ready
        && shadow_predicate_pushdown_ready
        && shadow_persisted_segment_descriptor_ready
        && shadow_segment_descriptor_scan_filter_fields_ready
        && shadow_segment_document_pruning_ready;
    serde_json::json!({
        "ready": ready,
        "predicate_pushdown_parity": predicate_pushdown_parity,
        "primary_predicate_pushdown_ready": primary_predicate_pushdown_ready,
        "shadow_predicate_pushdown_ready": shadow_predicate_pushdown_ready,
        "shadow_persisted_segment_descriptor_ready": shadow_persisted_segment_descriptor_ready,
        "shadow_segment_descriptor_scan_filter_fields_ready": shadow_segment_descriptor_scan_filter_fields_ready,
        "shadow_segment_document_pruning_ready": shadow_segment_document_pruning_ready,
        "shadow_segment_pruning_candidate_document_count": u64_path(shadow_evidence, &["predicate_pushdown", "segment_pruning_candidate_document_count"]),
        "shadow_segment_pruned_document_count": u64_path(shadow_evidence, &["predicate_pushdown", "segment_pruned_document_count"]),
        "shadow_segment_scanned_document_count": u64_path(shadow_evidence, &["predicate_pushdown", "segment_scanned_document_count"]),
        "primary_scan_filter_fields": array_path(primary_evidence, &["predicate_pushdown", "scan_filter_fields"]).unwrap_or_default(),
        "shadow_scan_filter_fields": array_path(shadow_evidence, &["predicate_pushdown", "scan_filter_fields"]).unwrap_or_default(),
        "shadow_segment_descriptor_field_summaries": value_path(shadow_evidence, &["predicate_pushdown", "segment_descriptor_field_summaries"]).cloned().unwrap_or_else(|| serde_json::json!([])),
    })
}

fn search_projection_table_parity(
    primary_evidence: &serde_json::Value,
    shadow_evidence: &serde_json::Value,
) -> serde_json::Value {
    let tables = REQUIRED_TABLES
        .iter()
        .map(|table| {
            let primary = find_table(primary_evidence, table).unwrap_or(&serde_json::Value::Null);
            let shadow = find_table(shadow_evidence, table).unwrap_or(&serde_json::Value::Null);
            let row_count_matches =
                u64_path(primary, &["row_count"]) == u64_path(shadow, &["row_count"]);
            let ready_matches = bool_path(primary, &["present"]) == bool_path(shadow, &["present"])
                && bool_path(primary, &["fts_ready"]) == bool_path(shadow, &["fts_ready"])
                && bool_path(primary, &["vector_ready"]) == bool_path(shadow, &["vector_ready"]);
            serde_json::json!({
                "name": table,
                "ready": row_count_matches && ready_matches,
                "row_count_matches": row_count_matches,
                "ready_matches": ready_matches,
                "primary_row_count": u64_path(primary, &["row_count"]),
                "shadow_row_count": u64_path(shadow, &["row_count"]),
                "primary_present": bool_path(primary, &["present"]),
                "shadow_present": bool_path(shadow, &["present"]),
                "primary_fts_ready": bool_path(primary, &["fts_ready"]),
                "shadow_fts_ready": bool_path(shadow, &["fts_ready"]),
                "primary_vector_ready": bool_path(primary, &["vector_ready"]),
                "shadow_vector_ready": bool_path(shadow, &["vector_ready"]),
            })
        })
        .collect::<Vec<_>>();
    let ready = tables
        .iter()
        .all(|table| bool_path(table, &["ready"]) == Some(true));
    serde_json::json!({
        "ready": ready,
        "tables": tables,
    })
}

fn predicate_pushdown_parity_matches(
    primary_evidence: &serde_json::Value,
    shadow_evidence: &serde_json::Value,
) -> bool {
    let fields = [
        "ready",
        "equality_ready",
        "in_list_ready",
        "not_in_list_ready",
        "range_ready",
        "row_filter_ready",
        "segment_pruning_ready",
        "numeric_min_max_ready",
        "timestamp_min_max_ready",
        "required_ops_ready",
    ];
    fields.iter().all(|field| {
        bool_path(primary_evidence, &["predicate_pushdown", field])
            == bool_path(shadow_evidence, &["predicate_pushdown", field])
    }) && array_path(primary_evidence, &["predicate_pushdown", "required_ops"])
        == array_path(shadow_evidence, &["predicate_pushdown", "required_ops"])
        && array_path(primary_evidence, &["predicate_pushdown", "supported_ops"])
            == array_path(shadow_evidence, &["predicate_pushdown", "supported_ops"])
}

fn collect_prefixed_evidence_blockers(
    prefix: &str,
    evidence: &serde_json::Value,
    blockers: &mut BTreeSet<String>,
) {
    for code in array_path(evidence, &["blocker_codes"]).unwrap_or_default() {
        blockers.insert(format!("{prefix}_{code}"));
    }
}

fn is_skein_search_projection_probe(probe: &serde_json::Value) -> bool {
    str_path(probe, &["protocol"]) == Some("skein-nowledge-search-projection-probe")
        || str_path(probe, &["engine"]) == Some("skein")
}

fn compressed_vector_projection_report(probe: &serde_json::Value) -> serde_json::Value {
    let projection =
        value_path(probe, &["compressed_vector_projection"]).unwrap_or(&serde_json::Value::Null);
    let ready = bool_path(projection, &["ready"]).unwrap_or(false);
    serde_json::json!({
        "ready": ready,
        "engine": str_path(projection, &["engine"]),
        "compiled": bool_path(projection, &["compiled"]),
        "bit_width": u64_path(projection, &["bit_width"]),
        "dimension": u64_path(projection, &["dimension"]),
        "document_count": u64_path(projection, &["document_count"]),
        "supports_allowlist": bool_path(projection, &["supports_allowlist"]).unwrap_or(false),
        "persisted_artifact_used": bool_path(projection, &["persisted_artifact_used"]).unwrap_or(false),
        "artifact_rebuilt_from_snapshot": bool_path(projection, &["artifact_rebuilt_from_snapshot"]).unwrap_or(false),
        "blocker_codes": array_path(projection, &["blocker_codes"]).unwrap_or_default(),
    })
}

fn ready_probe_template(engine: &str) -> serde_json::Value {
    serde_json::json!({
        "engine": engine,
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
            ready_probe_table("memories_index", true),
            ready_probe_table("messages_index", false),
            ready_probe_table("communities_index", true),
            ready_probe_table("entities_index", true),
            ready_probe_table("sources_index", true),
            ready_probe_table("source_chunks_index", true)
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
            "segment_descriptor_field_summaries": ready_segment_descriptor_field_summaries_template()
        },
        "production_filter_pruning": ready_production_filter_pruning_template(),
        "compressed_vector_projection": {
            "engine": "skein_rabitq_scan",
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
        },
        "blocker_codes": []
    })
}

fn ready_production_filter_pruning_template() -> serde_json::Value {
    let mut samples = NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
        .iter()
        .map(|field| match *field {
            "unit_type" | "lifecycle_state" | "temporal_context" => {
                ready_production_filter_pruning_sample_template(field, "in", "enum_in_list")
            }
            "importance" | "confidence" => {
                ready_production_filter_pruning_sample_template(field, "gte", "numeric_range")
            }
            "created_at" | "updated_at" | "event_start" | "event_end" => {
                ready_production_filter_pruning_sample_template(field, "gte", "timestamp_range")
            }
            "is_latest" => ready_production_filter_pruning_sample_template(
                field,
                "eq",
                "normalized_default_equality",
            ),
            _ => ready_production_filter_pruning_sample_template(field, "eq", "equality"),
        })
        .collect::<Vec<_>>();
    samples.push(ready_production_filter_pruning_sample_template(
        "document_id",
        "eq",
        "unique_key",
    ));
    serde_json::json!({
        "ready": true,
        "persisted_segment_descriptor_used": true,
        "payload_read_avoidance_ready": true,
        "explain_analyze_ready": true,
        "sample_count": samples.len(),
        "ready_field_count": NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len(),
        "required_field_count": NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len(),
        "missing_fields": [],
        "samples": samples,
    })
}

fn ready_production_filter_pruning_sample_template(
    field: &str,
    operation: &str,
    operation_family: &str,
) -> serde_json::Value {
    serde_json::json!({
        "field": field,
        "operation": operation,
        "operation_family": operation_family,
        "ready": true,
        "capability_ready": true,
        "persisted_segment_descriptor_used": true,
        "segment_count": 2,
        "scanned_segment_count": 1,
        "pruned_segment_count": 1,
        "segment_pruning_candidate_document_count": 6,
        "segment_scanned_document_count": 2,
        "segment_pruned_document_count": 4,
        "field_report_count": 1,
        "value_summary_used": matches!(
            operation_family,
            "equality" | "enum_in_list" | "normalized_default_equality" | "unique_key"
        ),
        "numeric_range_summary_used": operation_family == "numeric_range",
        "timestamp_range_summary_used": operation_family == "timestamp_range",
        "normalized_default_equality": operation_family == "normalized_default_equality",
        "unique_key_lookup": operation_family == "unique_key",
        "explain_analyze": {
            "ready": true,
            "operator": "search_projection_segment_scan",
            "segment_count": 2,
            "scanned_segment_count": 1,
            "pruned_segment_count": 1,
            "candidate_document_count": 6,
            "scanned_document_count": 2,
            "pruned_document_count": 4,
            "payload_read_avoidance": true,
        },
    })
}

fn ready_segment_descriptor_field_summaries_template() -> serde_json::Value {
    let mut fields = NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
        .iter()
        .map(|field| {
            ready_segment_descriptor_field_summary_template(
                field,
                true,
                matches!(*field, "importance" | "confidence"),
                matches!(
                    *field,
                    "created_at" | "updated_at" | "event_start" | "event_end"
                ),
            )
        })
        .collect::<Vec<_>>();
    fields.push(ready_segment_descriptor_field_summary_template(
        "document_id",
        true,
        false,
        false,
    ));
    serde_json::Value::Array(fields)
}

fn ready_segment_descriptor_field_summary_template(
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

fn ready_probe_table(name: &str, vector_ready: bool) -> serde_json::Value {
    serde_json::json!({
        "name": name,
        "present": true,
        "fts_ready": true,
        "vector_ready": vector_ready,
        "row_count": 1,
        "blocker_codes": []
    })
}

fn document_identity_report(probe: &serde_json::Value) -> serde_json::Value {
    let identity = value_path(probe, &["document_identity"])
        .or_else(|| value_path(probe, &["projection_identity"]))
        .unwrap_or(&serde_json::Value::Null);
    let document_count = u64_path(probe, &["document_count"]);
    let identity_document_count = u64_path(identity, &["document_count"]);
    let document_count_matches =
        document_count.is_some() && identity_document_count == document_count;
    let ready = bool_path(identity, &["ready"]) == Some(true)
        && str_path(identity, &["id_space"]) == Some("search_projection_document_id")
        && str_path(identity, &["representation"]) == Some("sorted_document_ids")
        && u64_path(identity, &["checksum"]).is_some()
        && document_count_matches;
    serde_json::json!({
        "ready": ready,
        "id_space": str_path(identity, &["id_space"]),
        "representation": str_path(identity, &["representation"]),
        "document_count": identity_document_count,
        "top_level_document_count": document_count,
        "document_count_matches": document_count_matches,
        "checksum": u64_path(identity, &["checksum"]),
    })
}

fn required_table_reports(probe: &serde_json::Value) -> Vec<serde_json::Value> {
    REQUIRED_TABLES
        .iter()
        .map(|name| {
            let table = find_table(probe, name);
            let present = table.is_some();
            let fts_ready = table
                .and_then(|table| bool_path(table, &["fts_ready"]))
                .unwrap_or(false);
            let vector_ready = if VECTOR_TABLES.contains(name) {
                table
                    .and_then(|table| bool_path(table, &["vector_ready"]))
                    .unwrap_or(false)
            } else {
                table
                    .and_then(|table| bool_path(table, &["vector_ready"]))
                    .unwrap_or(true)
            };
            let row_count = table.and_then(|table| u64_path(table, &["row_count"]));
            let blocker_codes = table
                .and_then(|table| array_path(table, &["blocker_codes"]))
                .unwrap_or_default();
            serde_json::json!({
                "name": name,
                "present": present,
                "fts_ready": fts_ready,
                "vector_ready": vector_ready,
                "row_count": row_count,
                "blocker_codes": blocker_codes,
            })
        })
        .collect()
}

fn embedding_identity_report(probe: &serde_json::Value) -> serde_json::Value {
    let manifest = value_path(probe, &["embedding_manifest"])
        .or_else(|| value_path(probe, &["embedding_identity"]))
        .unwrap_or(&serde_json::Value::Null);
    let model = str_path(manifest, &["model"]).or_else(|| str_path(manifest, &["model_id"]));
    let dimension =
        u64_path(manifest, &["dimension"]).or_else(|| u64_path(manifest, &["embedding_dimension"]));
    let active_model =
        str_path(manifest, &["active_model"]).or_else(|| str_path(manifest, &["active_model_id"]));
    let active_dimension = u64_path(manifest, &["active_dimension"])
        .or_else(|| u64_path(manifest, &["active_embedding_dimension"]));
    let model_matches = active_model
        .map(|active| model == Some(active))
        .or_else(|| bool_path(manifest, &["model_matches"]))
        .unwrap_or(false);
    let dimension_matches = active_dimension
        .map(|active| dimension == Some(active))
        .or_else(|| bool_path(manifest, &["dimension_matches"]))
        .unwrap_or(false);
    let ready = model.is_some()
        && dimension.is_some_and(|dimension| dimension > 0)
        && model_matches
        && dimension_matches;
    serde_json::json!({
        "ready": ready,
        "model": model,
        "dimension": dimension,
        "active_model": active_model,
        "active_dimension": active_dimension,
        "model_matches": model_matches,
        "dimension_matches": dimension_matches,
    })
}

fn fail_soft_report(probe: &serde_json::Value) -> serde_json::Value {
    let fail_soft = value_path(probe, &["fail_soft"])
        .or_else(|| value_path(probe, &["degradation"]))
        .unwrap_or(&serde_json::Value::Null);
    let fts_to_vector_ready = bool_path(fail_soft, &["fts_to_vector_ready"]).unwrap_or(false);
    let vector_to_fts_ready = bool_path(fail_soft, &["vector_to_fts_ready"]).unwrap_or(false);
    let no_500_on_leg_failure = bool_path(fail_soft, &["no_500_on_leg_failure"]).unwrap_or(false);
    serde_json::json!({
        "ready": fts_to_vector_ready && vector_to_fts_ready && no_500_on_leg_failure,
        "fts_to_vector_ready": fts_to_vector_ready,
        "vector_to_fts_ready": vector_to_fts_ready,
        "no_500_on_leg_failure": no_500_on_leg_failure,
    })
}

fn lifecycle_report(probe: &serde_json::Value) -> serde_json::Value {
    let lifecycle = value_path(probe, &["lifecycle"])
        .or_else(|| value_path(probe, &["projection_lifecycle"]))
        .unwrap_or(&serde_json::Value::Null);
    serde_json::json!({
        "rebuild_marker_ready": bool_path(lifecycle, &["rebuild_marker_ready"]).unwrap_or(false),
        "metadata_repair_marker_ready": bool_path(lifecycle, &["metadata_repair_marker_ready"]).unwrap_or(false),
    })
}

fn incremental_update_report(probe: &serde_json::Value) -> serde_json::Value {
    let incremental = value_path(probe, &["incremental_update"])
        .or_else(|| value_path(probe, &["incremental"]))
        .unwrap_or(&serde_json::Value::Null);
    let upsert_ready = bool_path(incremental, &["upsert_ready"]).unwrap_or(false);
    let delete_ready = bool_path(incremental, &["delete_ready"]).unwrap_or(false);
    let watermark_ready = bool_path(incremental, &["watermark_ready"]).unwrap_or(false);
    let source_graph_commit_epoch = u64_path(incremental, &["source_graph_commit_epoch"]);
    let reported_ready = bool_path(incremental, &["ready"]).unwrap_or(false);
    let ready = reported_ready
        && upsert_ready
        && delete_ready
        && watermark_ready
        && source_graph_commit_epoch.is_some();
    serde_json::json!({
        "ready": ready,
        "reported_ready": reported_ready,
        "upsert_ready": upsert_ready,
        "delete_ready": delete_ready,
        "watermark_ready": watermark_ready,
        "source_graph_commit_epoch": source_graph_commit_epoch,
    })
}

fn predicate_pushdown_report(probe: &serde_json::Value) -> serde_json::Value {
    let predicate = value_path(probe, &["predicate_pushdown"])
        .or_else(|| value_path(probe, &["scan_filter"]))
        .unwrap_or(&serde_json::Value::Null);
    let equality_ready = bool_path(predicate, &["equality_ready"]).unwrap_or(false);
    let in_list_ready = bool_path(predicate, &["in_list_ready"]).unwrap_or(false);
    let not_in_list_ready = bool_path(predicate, &["not_in_list_ready"]).unwrap_or(false);
    let range_ready = bool_path(predicate, &["range_ready"]).unwrap_or(false);
    let row_filter_ready = bool_path(predicate, &["row_filter_ready"]).unwrap_or(false);
    let segment_pruning_ready = bool_path(predicate, &["segment_pruning_ready"]).unwrap_or(false);
    let numeric_min_max_ready = bool_path(predicate, &["numeric_min_max_ready"]).unwrap_or(false);
    let timestamp_min_max_ready =
        bool_path(predicate, &["timestamp_min_max_ready"]).unwrap_or(false);
    let persisted_segment_descriptor_ready =
        bool_path(predicate, &["persisted_segment_descriptor_ready"]).unwrap_or(false);
    let supported_ops = array_path(predicate, &["supported_ops"]).unwrap_or_default();
    let required_ops = ["eq", "in", "not_in", "gt", "gte", "lt", "lte"];
    let required_ops_ready = required_ops
        .iter()
        .all(|required| supported_ops.iter().any(|op| op == required));
    let scan_filter_fields = array_path(predicate, &["scan_filter_fields"]).unwrap_or_default();
    let segment_descriptor_field_summaries =
        value_path(predicate, &["segment_descriptor_field_summaries"])
            .cloned()
            .unwrap_or_else(|| serde_json::json!([]));
    let segment_descriptor_field_count = segment_descriptor_field_summaries
        .as_array()
        .map(Vec::len)
        .unwrap_or_default();
    let segment_document_pruning_ready =
        bool_path(predicate, &["segment_document_pruning_ready"]).unwrap_or(false);
    let segment_pruning_candidate_document_count =
        u64_path(predicate, &["segment_pruning_candidate_document_count"]);
    let segment_pruned_document_count = u64_path(predicate, &["segment_pruned_document_count"]);
    let segment_scanned_document_count = u64_path(predicate, &["segment_scanned_document_count"]);
    let segment_descriptor_scan_filter_fields_ready = segment_descriptor_fields_cover_scan_filters(
        &scan_filter_fields,
        &segment_descriptor_field_summaries,
    );
    let segment_descriptor_capabilities =
        segment_descriptor_capability_report(&segment_descriptor_field_summaries);
    let ready = equality_ready
        && in_list_ready
        && not_in_list_ready
        && range_ready
        && row_filter_ready
        && segment_pruning_ready
        && numeric_min_max_ready
        && timestamp_min_max_ready
        && required_ops_ready;
    serde_json::json!({
        "ready": ready,
        "equality_ready": equality_ready,
        "in_list_ready": in_list_ready,
        "not_in_list_ready": not_in_list_ready,
        "range_ready": range_ready,
        "row_filter_ready": row_filter_ready,
        "segment_pruning_ready": segment_pruning_ready,
        "numeric_min_max_ready": numeric_min_max_ready,
        "timestamp_min_max_ready": timestamp_min_max_ready,
        "persisted_segment_descriptor_ready": persisted_segment_descriptor_ready,
        "required_ops_ready": required_ops_ready,
        "required_ops": required_ops,
        "supported_ops": supported_ops,
        "scan_filter_fields": scan_filter_fields,
        "required_scan_filter_fields": NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS,
        "segment_descriptor_field_count": segment_descriptor_field_count,
        "segment_descriptor_scan_filter_fields_ready": segment_descriptor_scan_filter_fields_ready,
        "segment_descriptor_capabilities_ready": segment_descriptor_capabilities.ready,
        "segment_document_pruning_ready": segment_document_pruning_ready,
        "segment_pruning_candidate_document_count": segment_pruning_candidate_document_count,
        "segment_pruned_document_count": segment_pruned_document_count,
        "segment_scanned_document_count": segment_scanned_document_count,
        "missing_value_summary_fields": segment_descriptor_capabilities.missing_value_summary_fields,
        "missing_numeric_range_fields": segment_descriptor_capabilities.missing_numeric_range_fields,
        "missing_timestamp_range_fields": segment_descriptor_capabilities.missing_timestamp_range_fields,
        "missing_unique_key_summary_fields": segment_descriptor_capabilities.missing_unique_key_summary_fields,
        "segment_descriptor_field_summaries": segment_descriptor_field_summaries,
    })
}

fn production_filter_pruning_report(probe: &serde_json::Value) -> serde_json::Value {
    let pruning =
        value_path(probe, &["production_filter_pruning"]).unwrap_or(&serde_json::Value::Null);
    let ready = bool_path(pruning, &["ready"]).unwrap_or(false);
    let persisted_segment_descriptor_used =
        bool_path(pruning, &["persisted_segment_descriptor_used"]).unwrap_or(false);
    let payload_read_avoidance_ready =
        bool_path(pruning, &["payload_read_avoidance_ready"]).unwrap_or(false);
    let explain_analyze_ready = bool_path(pruning, &["explain_analyze_ready"]).unwrap_or(false);
    let sample_count = u64_path(pruning, &["sample_count"]).unwrap_or(0);
    let ready_field_count = u64_path(pruning, &["ready_field_count"]).unwrap_or(0);
    let required_field_count = u64_path(pruning, &["required_field_count"])
        .unwrap_or(NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len() as u64);
    let missing_fields = array_path(pruning, &["missing_fields"]).unwrap_or_default();
    let samples = value_path(pruning, &["samples"])
        .cloned()
        .unwrap_or_else(|| serde_json::json!([]));
    let sample_evidence = production_filter_pruning_sample_evidence(&samples);
    let required_fields_ready = sample_count >= required_field_count
        && ready_field_count >= required_field_count
        && required_field_count == NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len() as u64
        && missing_fields.is_empty();
    let ready = ready
        && persisted_segment_descriptor_used
        && payload_read_avoidance_ready
        && explain_analyze_ready
        && required_fields_ready;
    let ready = ready && sample_evidence.ready;
    serde_json::json!({
        "ready": ready,
        "persisted_segment_descriptor_used": persisted_segment_descriptor_used,
        "payload_read_avoidance_ready": payload_read_avoidance_ready,
        "explain_analyze_ready": explain_analyze_ready,
        "sample_evidence_ready": sample_evidence.ready,
        "sample_payload_read_avoidance_count": sample_evidence.payload_read_avoidance_count,
        "operation_family_evidence_ready": sample_evidence.operation_family_evidence_ready,
        "required_operation_families": REQUIRED_PRODUCTION_FILTER_OPERATION_FAMILIES,
        "observed_operation_families": sample_evidence.observed_operation_families,
        "missing_operation_families": sample_evidence.missing_operation_families,
        "sample_count": sample_count,
        "ready_field_count": ready_field_count,
        "required_field_count": required_field_count,
        "required_fields_ready": required_fields_ready,
        "missing_sample_fields": sample_evidence.missing_fields,
        "missing_fields": missing_fields,
        "samples": samples,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProductionFilterPruningSampleEvidence {
    ready: bool,
    payload_read_avoidance_count: u64,
    missing_fields: Vec<&'static str>,
    operation_family_evidence_ready: bool,
    observed_operation_families: Vec<String>,
    missing_operation_families: Vec<&'static str>,
}

fn production_filter_pruning_sample_evidence(
    samples: &serde_json::Value,
) -> ProductionFilterPruningSampleEvidence {
    let sample_items = samples.as_array().map(Vec::as_slice).unwrap_or_default();
    let payload_read_avoidance_count = sample_items
        .iter()
        .filter(|sample| production_filter_pruning_sample_ready(sample))
        .count() as u64;
    let missing_fields = NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
        .iter()
        .copied()
        .filter(|field| {
            !sample_items.iter().any(|sample| {
                str_path(sample, &["field"]) == Some(*field)
                    && production_filter_pruning_sample_capability_ready(sample)
            })
        })
        .collect::<Vec<_>>();
    let observed_operation_families = sample_items
        .iter()
        .filter(|sample| production_filter_pruning_sample_capability_ready(sample))
        .filter_map(|sample| str_path(sample, &["operation_family"]))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let missing_operation_families = REQUIRED_PRODUCTION_FILTER_OPERATION_FAMILIES
        .iter()
        .copied()
        .filter(|family| {
            !sample_items.iter().any(|sample| {
                str_path(sample, &["operation_family"]) == Some(*family)
                    && production_filter_pruning_sample_capability_ready(sample)
            })
        })
        .collect::<Vec<_>>();
    let operation_family_evidence_ready = missing_operation_families.is_empty();
    ProductionFilterPruningSampleEvidence {
        ready: missing_fields.is_empty()
            && operation_family_evidence_ready
            && payload_read_avoidance_count > 0,
        payload_read_avoidance_count,
        missing_fields,
        operation_family_evidence_ready,
        observed_operation_families,
        missing_operation_families,
    }
}

fn production_filter_pruning_sample_ready(sample: &serde_json::Value) -> bool {
    if !production_filter_pruning_sample_capability_ready(sample) {
        return false;
    }
    let Some(segment_count) = u64_path(sample, &["segment_count"]) else {
        return false;
    };
    let Some(scanned_segment_count) = u64_path(sample, &["scanned_segment_count"]) else {
        return false;
    };
    let Some(pruned_segment_count) = u64_path(sample, &["pruned_segment_count"]) else {
        return false;
    };
    if segment_count == 0
        || scanned_segment_count == 0
        || pruned_segment_count == 0
        || scanned_segment_count.checked_add(pruned_segment_count) != Some(segment_count)
    {
        return false;
    }
    let Some(candidate_document_count) =
        u64_path(sample, &["segment_pruning_candidate_document_count"])
    else {
        return false;
    };
    let Some(scanned_document_count) = u64_path(sample, &["segment_scanned_document_count"]) else {
        return false;
    };
    let Some(pruned_document_count) = u64_path(sample, &["segment_pruned_document_count"]) else {
        return false;
    };
    if candidate_document_count == 0
        || scanned_document_count == 0
        || pruned_document_count == 0
        || scanned_document_count.checked_add(pruned_document_count)
            != Some(candidate_document_count)
    {
        return false;
    }
    production_filter_pruning_explain_analyze_ready(
        sample,
        segment_count,
        scanned_segment_count,
        pruned_segment_count,
        candidate_document_count,
        scanned_document_count,
        pruned_document_count,
    )
}

fn production_filter_pruning_sample_capability_ready(sample: &serde_json::Value) -> bool {
    bool_path(sample, &["ready"]) == Some(true)
        && bool_path(sample, &["capability_ready"]) == Some(true)
        && bool_path(sample, &["persisted_segment_descriptor_used"]) == Some(true)
        && production_filter_pruning_sample_operation_ready(sample)
}

fn production_filter_pruning_sample_operation_ready(sample: &serde_json::Value) -> bool {
    let Some(operation) = str_path(sample, &["operation"]) else {
        return false;
    };
    let Some(operation_family) = str_path(sample, &["operation_family"]) else {
        return false;
    };
    match operation_family {
        "equality" => operation == "eq",
        "enum_in_list" => operation == "in",
        "numeric_range" | "timestamp_range" => {
            matches!(operation, "gt" | "gte" | "lt" | "lte" | "between" | "range")
        }
        "normalized_default_equality" => {
            operation == "eq" && bool_path(sample, &["normalized_default_equality"]) == Some(true)
        }
        "unique_key" => {
            operation == "eq" && bool_path(sample, &["unique_key_lookup"]) == Some(true)
        }
        _ => false,
    }
}

fn production_filter_pruning_explain_analyze_ready(
    sample: &serde_json::Value,
    segment_count: u64,
    scanned_segment_count: u64,
    pruned_segment_count: u64,
    candidate_document_count: u64,
    scanned_document_count: u64,
    pruned_document_count: u64,
) -> bool {
    let Some(explain) = value_path(sample, &["explain_analyze"]) else {
        return false;
    };
    bool_path(explain, &["ready"]) == Some(true)
        && str_path(explain, &["operator"]) == Some("search_projection_segment_scan")
        && u64_path(explain, &["segment_count"]) == Some(segment_count)
        && u64_path(explain, &["scanned_segment_count"]) == Some(scanned_segment_count)
        && u64_path(explain, &["pruned_segment_count"]) == Some(pruned_segment_count)
        && u64_path(explain, &["candidate_document_count"]) == Some(candidate_document_count)
        && u64_path(explain, &["scanned_document_count"]) == Some(scanned_document_count)
        && u64_path(explain, &["pruned_document_count"]) == Some(pruned_document_count)
        && bool_path(explain, &["payload_read_avoidance"]) == Some(true)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SegmentDescriptorCapabilityReport {
    ready: bool,
    missing_value_summary_fields: Vec<&'static str>,
    missing_numeric_range_fields: Vec<&'static str>,
    missing_timestamp_range_fields: Vec<&'static str>,
    missing_unique_key_summary_fields: Vec<&'static str>,
}

fn segment_descriptor_fields_cover_scan_filters(
    scan_filter_fields: &[String],
    segment_descriptor_field_summaries: &serde_json::Value,
) -> bool {
    let Some(summaries) = segment_descriptor_field_summaries.as_array() else {
        return false;
    };
    if summaries.is_empty() {
        return false;
    }
    let summary_fields = summaries
        .iter()
        .filter_map(|summary| str_path(summary, &["field"]))
        .collect::<BTreeSet<_>>();
    NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
        .iter()
        .all(|required| scan_filter_fields.iter().any(|field| field == required))
        && NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
            .iter()
            .all(|required| summary_fields.contains(required))
        && segment_descriptor_capability_report(segment_descriptor_field_summaries).ready
}

fn segment_descriptor_capability_report(
    segment_descriptor_field_summaries: &serde_json::Value,
) -> SegmentDescriptorCapabilityReport {
    let summaries = segment_descriptor_field_summaries
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let missing_value_summary_fields = required_capability_missing_fields(
        summaries,
        REQUIRED_VALUE_SUMMARY_FIELDS,
        "value_summary_used",
        None,
    );
    let missing_numeric_range_fields = required_capability_missing_fields(
        summaries,
        REQUIRED_NUMERIC_RANGE_FIELDS,
        "numeric_range_summary_used",
        None,
    );
    let missing_timestamp_range_fields = required_capability_missing_fields(
        summaries,
        REQUIRED_TIMESTAMP_RANGE_FIELDS,
        "timestamp_range_summary_used",
        Some("numeric_range_summary_used"),
    );
    let missing_unique_key_summary_fields = required_capability_missing_fields(
        summaries,
        REQUIRED_UNIQUE_KEY_SUMMARY_FIELDS,
        "unique_key_summary_used",
        None,
    );
    SegmentDescriptorCapabilityReport {
        ready: missing_value_summary_fields.is_empty()
            && missing_numeric_range_fields.is_empty()
            && missing_timestamp_range_fields.is_empty()
            && missing_unique_key_summary_fields.is_empty(),
        missing_value_summary_fields,
        missing_numeric_range_fields,
        missing_timestamp_range_fields,
        missing_unique_key_summary_fields,
    }
}

fn required_capability_missing_fields(
    summaries: &[serde_json::Value],
    required_fields: &'static [&'static str],
    capability: &str,
    alternative_capability: Option<&str>,
) -> Vec<&'static str> {
    required_fields
        .iter()
        .copied()
        .filter(|field| {
            !summaries.iter().any(|summary| {
                str_path(summary, &["field"]) == Some(*field)
                    && segment_descriptor_summary_base_ready(summary)
                    && (segment_descriptor_summary_capability_ready(summary, capability)
                        || alternative_capability.is_some_and(|capability| {
                            segment_descriptor_summary_capability_ready(summary, capability)
                        }))
            })
        })
        .collect()
}

fn segment_descriptor_summary_base_ready(summary: &serde_json::Value) -> bool {
    u64_path(summary, &["segment_count"]).is_some_and(|count| count > 0)
        && u64_path(summary, &["present_document_count"]).is_some()
}

fn segment_descriptor_summary_capability_ready(
    summary: &serde_json::Value,
    capability: &str,
) -> bool {
    bool_path(summary, &[capability]) == Some(true)
        && segment_descriptor_summary_capability_count(summary, capability)
            .is_some_and(|count| count > 0)
}

fn segment_descriptor_summary_capability_count(
    summary: &serde_json::Value,
    capability: &str,
) -> Option<u64> {
    match capability {
        "value_summary_used" => u64_path(summary, &["value_summary_segment_count"]),
        "numeric_range_summary_used" => u64_path(summary, &["numeric_range_segment_count"]),
        "timestamp_range_summary_used" => u64_path(summary, &["timestamp_range_segment_count"]),
        "unique_key_summary_used" => u64_path(summary, &["unique_key_summary_segment_count"]),
        _ => None,
    }
}

fn collect_probe_blockers(probe: &serde_json::Value, blockers: &mut BTreeSet<String>) {
    for code in array_path(probe, &["blocker_codes"]).unwrap_or_default() {
        blockers.insert(code);
    }
    if let Some(tables) = value_path(probe, &["tables"]).and_then(serde_json::Value::as_array) {
        for table in tables {
            for code in array_path(table, &["blocker_codes"]).unwrap_or_default() {
                blockers.insert(code);
            }
        }
    }
}

fn find_table<'a>(probe: &'a serde_json::Value, name: &str) -> Option<&'a serde_json::Value> {
    value_path(probe, &["tables"])?
        .as_array()?
        .iter()
        .find(|table| str_path(table, &["name"]) == Some(name))
}

fn value_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    Some(current)
}

fn bool_path(value: &serde_json::Value, path: &[&str]) -> Option<bool> {
    value_path(value, path).and_then(serde_json::Value::as_bool)
}

fn str_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a str> {
    value_path(value, path).and_then(serde_json::Value::as_str)
}

fn u64_path(value: &serde_json::Value, path: &[&str]) -> Option<u64> {
    value_path(value, path).and_then(serde_json::Value::as_u64)
}

fn array_path(value: &serde_json::Value, path: &[&str]) -> Option<Vec<String>> {
    value_path(value, path)
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(str::to_string))
                .collect()
        })
}

#[cfg(test)]
mod tests {
    use super::{
        nowledge_search_projection_evidence_json, nowledge_search_projection_probe_contract_json,
        nowledge_search_projection_shadow_evidence_json, ready_production_filter_pruning_template,
        NowledgeSearchProjectionEvidenceReport,
        SKEIN_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_FIELDS_MISSING,
    };
    use crate::{
        SearchEmbeddingManifest, SearchIndex, SearchProjectionDelta, SearchProjectionKind,
        SearchProjectionRow, NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS,
    };
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

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
    fn search_projection_evidence_read_errors_are_redacted_by_default() {
        let secret_path =
            unique_test_dir("search_projection_secret_path_do_not_emit").join("missing-probe.json");

        let error = super::read_json_file(&secret_path).unwrap_err().to_string();

        assert_eq!(
            error,
            "execution error: failed to read search projection evidence JSON: io_error"
        );
        assert!(!error.contains("search_projection_secret_path_do_not_emit"));
        assert!(!error.contains("missing-probe"));
    }

    #[test]
    fn search_projection_evidence_parse_errors_are_redacted_by_default() {
        let root = unique_test_dir("search_projection_parse_redaction");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("secret-projection-path-do-not-emit.json");
        std::fs::write(
            &path,
            "{ \"external_id\": \"secret-projection-doc-do-not-emit\", \"unterminated\": ",
        )
        .unwrap();

        let error = super::read_json_file(&path).unwrap_err().to_string();

        assert_eq!(
            error,
            "semantic error: failed to parse search projection evidence JSON: invalid_json"
        );
        assert!(!error.contains("secret-projection-path-do-not-emit"));
        assert!(!error.contains("secret-projection-doc-do-not-emit"));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn search_projection_evidence_report_exposes_typed_summary() {
        let report = NowledgeSearchProjectionEvidenceReport::from_probe(&ready_probe());

        assert_eq!(report.protocol, "skein-nowledge-search-projection-evidence");
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
        assert!(report.skein_predicate_pushdown_ready);
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
        let skein_example = &contract["example_skein_probe"];
        let evidence = nowledge_search_projection_evidence_json(example);
        let skein_evidence = nowledge_search_projection_evidence_json(skein_example);

        assert_eq!(
            contract["protocol"],
            "skein-nowledge-search-projection-probe-contract-v1"
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
            contract["required_skein_scan_filter_fields"],
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
        assert_eq!(skein_example["engine"], "skein");
        assert_eq!(skein_evidence["ready"], true);
        assert_eq!(skein_evidence["skein_predicate_pushdown_ready"], true);
        assert_eq!(
            skein_evidence["predicate_pushdown"]["segment_descriptor_scan_filter_fields_ready"],
            true
        );
    }

    #[test]
    fn search_projection_evidence_requires_compressed_vector_projection_for_skein_probe() {
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
                "predicate_pushdown_not_ready",
                "skein_predicate_pushdown_descriptor_not_ready"
            ])
        );
    }

    #[test]
    fn skein_search_projection_evidence_requires_production_filter_pruning() {
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
            serde_json::json!(["skein_production_filter_pruning_not_ready"])
        );
    }

    #[test]
    fn skein_search_projection_evidence_rejects_incomplete_production_filter_pruning() {
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
            serde_json::json!(["skein_production_filter_pruning_not_ready"])
        );
    }

    #[test]
    fn skein_search_projection_evidence_requires_production_filter_explain_analyze() {
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
            serde_json::json!(["skein_production_filter_pruning_not_ready"])
        );
    }

    #[test]
    fn skein_search_projection_evidence_requires_payload_avoidance_samples() {
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
            serde_json::json!(["skein_production_filter_pruning_not_ready"])
        );
    }

    #[test]
    fn skein_search_projection_evidence_requires_filter_operation_families() {
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
            serde_json::json!(["skein_production_filter_pruning_not_ready"])
        );
    }

    #[test]
    fn skein_search_projection_evidence_requires_normalized_default_equality_flag() {
        let mut probe = ready_probe();
        let samples = probe["production_filter_pruning"]["samples"]
            .as_array_mut()
            .unwrap();
        let sample = samples
            .iter_mut()
            .find(|sample| {
                sample["operation_family"].as_str() == Some("normalized_default_equality")
            })
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
            serde_json::json!(["skein_production_filter_pruning_not_ready"])
        );
    }

    #[test]
    fn skein_search_projection_evidence_requires_descriptor_field_summaries() {
        let mut probe = ready_probe();
        probe["predicate_pushdown"]
            .as_object_mut()
            .unwrap()
            .remove("segment_descriptor_field_summaries");

        let report = nowledge_search_projection_evidence_json(&probe);

        assert_eq!(report["ready"], false);
        assert_eq!(report["predicate_pushdown_ready"], true);
        assert_eq!(report["skein_predicate_pushdown_ready"], false);
        assert_eq!(
            report["predicate_pushdown"]["segment_descriptor_scan_filter_fields_ready"],
            false
        );
        assert!(report["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "skein_predicate_pushdown_descriptor_not_ready"));
    }

    #[test]
    fn skein_search_projection_evidence_requires_descriptor_field_capabilities() {
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
        assert_eq!(report["skein_predicate_pushdown_ready"], false);
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
            .any(|code| code == "skein_predicate_pushdown_descriptor_not_ready"));
    }

    #[test]
    fn skein_search_projection_evidence_requires_descriptor_summary_counts() {
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
        assert_eq!(report["skein_predicate_pushdown_ready"], false);
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
            .any(|code| code == "skein_predicate_pushdown_descriptor_not_ready"));
    }

    #[test]
    fn skein_search_projection_evidence_requires_unique_key_descriptor_summary() {
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
        assert_eq!(report["skein_predicate_pushdown_ready"], false);
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
            .any(|code| code == "skein_predicate_pushdown_descriptor_not_ready"));
    }

    #[test]
    fn skein_probe_output_feeds_search_projection_evidence() {
        let path = unique_test_dir("search_projection_probe_command");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            index
                .apply_embedding_manifest(SearchEmbeddingManifest {
                    model: "bge-m3".to_string(),
                    version: None,
                    dimension: 8,
                })
                .unwrap();
            index
                .apply_projection_delta(SearchProjectionDelta {
                    upserts: nowledge_probe_rows(),
                    deletes: Vec::new(),
                    max_operations: None,
                    source_graph_commit_epoch: Some(11),
                })
                .unwrap();
            index.checkpoint().unwrap();
        }

        let probe = SearchIndex::open(&path)
            .unwrap()
            .nowledge_search_projection_probe_json(crate::SearchProjectionProbeOptions {
                active_embedding_model: Some("bge-m3".to_string()),
                active_embedding_dimension: Some(8),
            });
        let evidence = nowledge_search_projection_evidence_json(&probe);

        assert_eq!(probe["protocol"], "skein-nowledge-search-projection-probe");
        assert_eq!(
            evidence["ready"], true,
            "probe={probe:#}\nevidence={evidence:#}"
        );
        assert_eq!(evidence["covered_table_count"], 6);
        assert_eq!(evidence["source_chunk_ready"], true);
        assert_eq!(evidence["predicate_pushdown_ready"], true);
        assert_eq!(evidence["skein_predicate_pushdown_ready"], true);
        assert_eq!(
            evidence["predicate_pushdown"]["segment_descriptor_scan_filter_fields_ready"],
            true
        );
        assert_eq!(evidence["compressed_vector_projection_required"], true);
        assert_eq!(evidence["compressed_vector_projection_ready"], true);
        assert_eq!(
            probe["predicate_pushdown"]["supported_ops"],
            serde_json::json!(["eq", "in", "not_in", "gt", "gte", "lt", "lte"])
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn search_projection_shadow_evidence_reports_ready_for_matching_probes() {
        let primary = ready_probe();
        let shadow = ready_probe();

        let report = nowledge_search_projection_shadow_evidence_json(&primary, &shadow);

        assert_eq!(report["ready"], true);
        assert_eq!(report["evidence_source"], "skein-rust-library");
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
        primary["predicate_pushdown"]["persisted_segment_descriptor_ready"] =
            serde_json::json!(false);
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
        shadow["predicate_pushdown"]["persisted_segment_descriptor_ready"] =
            serde_json::json!(false);

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
            .any(|code| code == "skein_search_projection_segment_descriptor_missing"));
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
            .any(|code| code == SKEIN_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_FIELDS_MISSING));
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
            "engine": "skein",
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
                "engine": "skein_rabitq_scan",
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

    fn nowledge_probe_rows() -> Vec<SearchProjectionRow> {
        let mut rows = vec![
            nowledge_probe_row(SearchProjectionKind::Memory, "mem_1"),
            nowledge_probe_row_without_embedding(SearchProjectionKind::Message, "msg_1"),
            nowledge_probe_row(SearchProjectionKind::Community, "community_1"),
            nowledge_probe_row(SearchProjectionKind::Entity, "entity_1"),
            nowledge_probe_row(SearchProjectionKind::Source, "source_1"),
            nowledge_probe_row(SearchProjectionKind::SourceChunk, "chunk_1"),
        ];
        rows.extend((0..124).map(|ordinal| {
            nowledge_probe_row(
                SearchProjectionKind::Memory,
                &format!("mem_filler_{ordinal:03}"),
            )
        }));
        rows
    }

    fn nowledge_probe_row(kind: SearchProjectionKind, external_id: &str) -> SearchProjectionRow {
        SearchProjectionRow {
            kind,
            external_id: external_id.to_string(),
            title: format!("{external_id} title"),
            body: format!("{external_id} body"),
            embedding: Some(vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            source_id: Some("source_1".to_string()),
            metadata: BTreeMap::from([
                ("space_id".to_string(), "default".to_string()),
                ("unit_type".to_string(), "fact".to_string()),
                ("lifecycle_state".to_string(), "active".to_string()),
                ("temporal_context".to_string(), "current".to_string()),
                ("importance".to_string(), "0.8".to_string()),
                ("confidence".to_string(), "0.9".to_string()),
                ("created_at".to_string(), "11".to_string()),
                ("updated_at".to_string(), "12".to_string()),
                ("event_start".to_string(), "10".to_string()),
                ("event_end".to_string(), "20".to_string()),
                ("is_latest".to_string(), "true".to_string()),
            ]),
        }
    }

    fn nowledge_probe_row_without_embedding(
        kind: SearchProjectionKind,
        external_id: &str,
    ) -> SearchProjectionRow {
        SearchProjectionRow {
            embedding: None,
            ..nowledge_probe_row(kind, external_id)
        }
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein_{name}_{}_{nanos}", std::process::id()))
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
}
