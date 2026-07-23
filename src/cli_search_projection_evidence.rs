use crate::nowledge_contract::{
    SEARCH_PROJECTION_EVIDENCE_SOURCE, SEARCH_PROJECTION_SHADOW_EVIDENCE_ROUTE,
    SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE, SKEIN_NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL,
    SKEIN_NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL,
};
use crate::{Result, SearchIndex, SearchProjectionProbeOptions, SkeinError};
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

const REQUIRED_PREDICATE_PUSHDOWN_OPS: &[&str] = &["eq", "in", "not_in", "gt", "gte", "lt", "lte"];
const REQUIRED_SCAN_FILTER_FIELDS: &[&str] = &[
    "space_id",
    "unit_type",
    "importance",
    "confidence",
    "created_at",
    "event_start",
    "event_end",
    "is_latest",
];

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

pub fn nowledge_search_projection_probe_contract_json() -> serde_json::Value {
    serde_json::json!({
        "protocol": "skein-nowledge-search-projection-probe-contract-v1",
        "purpose": "primary LanceDB and shadow Skein probes must use this shape before search projection shadow evidence can pass",
        "required_tables": REQUIRED_TABLES,
        "vector_tables": VECTOR_TABLES,
        "required_predicate_pushdown_ops": REQUIRED_PREDICATE_PUSHDOWN_OPS,
        "required_scan_filter_fields": REQUIRED_SCAN_FILTER_FIELDS,
        "required_top_level_fields": [
            "engine",
            "derived_projection",
            "document_count",
            "tables",
            "embedding_manifest",
            "fail_soft",
            "lifecycle",
            "incremental_update",
            "predicate_pushdown"
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
            "required_ops_ready",
            "required_scan_filter_fields_ready",
            "required_ops",
            "required_scan_filter_fields",
            "supported_ops",
            "scan_filter_fields"
        ],
        "example_primary_probe": ready_probe_template("lancedb"),
    })
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
    let compressed_vector_projection = compressed_vector_projection_report(probe);
    let compressed_vector_projection_required =
        vector_ready && is_skein_search_projection_probe(probe);
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
    if !predicate_pushdown_ready {
        blocker_codes.insert("predicate_pushdown_not_ready".to_string());
    }
    if !compressed_vector_projection_ready {
        blocker_codes.insert("compressed_vector_projection_not_ready".to_string());
    }

    let ready = blocker_codes.is_empty();
    serde_json::json!({
        "protocol": SKEIN_NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL,
        "evidence_source": SEARCH_PROJECTION_EVIDENCE_SOURCE,
        "ready": ready,
        "derived_projection": derived_projection,
        "all_tables_covered": all_tables_covered,
        "covered_table_count": covered_table_count,
        "required_table_count": required_table_count,
        "required_tables": REQUIRED_TABLES,
        "fts_ready": fts_ready,
        "vector_ready": vector_ready,
        "embedding_identity_ready": embedding_identity_ready,
        "fail_soft_ready": fail_soft_ready,
        "rebuild_marker_ready": rebuild_marker_ready,
        "metadata_repair_marker_ready": metadata_repair_marker_ready,
        "incremental_update_ready": incremental_update_ready,
        "source_chunk_ready": source_chunk_ready,
        "predicate_pushdown_ready": predicate_pushdown_ready,
        "compressed_vector_projection_required": compressed_vector_projection_required,
        "compressed_vector_projection_ready": compressed_vector_projection_ready,
        "tables": table_reports,
        "embedding_identity": embedding_identity,
        "fail_soft": fail_soft,
        "lifecycle": lifecycle,
        "incremental_update": incremental_update,
        "predicate_pushdown": predicate_pushdown,
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
    let embedding_identity_parity = value_path(&primary_evidence, &["embedding_identity"])
        == value_path(&shadow_evidence, &["embedding_identity"]);
    let lifecycle_parity = value_path(&primary_evidence, &["lifecycle"])
        == value_path(&shadow_evidence, &["lifecycle"]);
    let predicate_pushdown_parity =
        predicate_pushdown_parity_matches(&primary_evidence, &shadow_evidence);
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
    let ready = blocker_codes.is_empty();
    serde_json::json!({
        "protocol": SKEIN_NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL,
        "evidence_source": SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE,
        "route": SEARCH_PROJECTION_SHADOW_EVIDENCE_ROUTE,
        "ready": ready,
        "primary_engine": str_path(primary_probe, &["engine"]).unwrap_or("lancedb"),
        "shadow_engine": str_path(shadow_probe, &["engine"]).unwrap_or("skein"),
        "primary_ready": bool_path(&primary_evidence, &["ready"]).unwrap_or(false),
        "shadow_ready": bool_path(&shadow_evidence, &["ready"]).unwrap_or(false),
        "document_count_parity": document_count_parity,
        "primary_document_count": u64_path(primary_probe, &["document_count"]),
        "shadow_document_count": u64_path(shadow_probe, &["document_count"]),
        "table_parity": table_parity,
        "embedding_identity_parity": embedding_identity_parity,
        "lifecycle_parity": lifecycle_parity,
        "incremental_watermark_parity": incremental_watermark_parity,
        "predicate_pushdown_parity": predicate_pushdown_parity,
        "primary_evidence": primary_evidence,
        "shadow_evidence": shadow_evidence,
        "blocker_codes": blocker_codes.into_iter().collect::<Vec<_>>(),
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
        "required_ops_ready",
        "required_scan_filter_fields_ready",
    ];
    fields.iter().all(|field| {
        bool_path(primary_evidence, &["predicate_pushdown", field])
            == bool_path(shadow_evidence, &["predicate_pushdown", field])
    }) && array_path(primary_evidence, &["predicate_pushdown", "required_ops"])
        == array_path(shadow_evidence, &["predicate_pushdown", "required_ops"])
        && array_path(
            primary_evidence,
            &["predicate_pushdown", "required_scan_filter_fields"],
        ) == array_path(
            shadow_evidence,
            &["predicate_pushdown", "required_scan_filter_fields"],
        )
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
            "supported_ops": REQUIRED_PREDICATE_PUSHDOWN_OPS,
            "scan_filter_fields": [
                "space_id",
                "unit_type",
                "importance",
                "confidence",
                "created_at",
                "event_start",
                "event_end",
                "is_latest"
            ]
        },
        "compressed_vector_projection": {
            "engine": "turbovec",
            "compiled": true,
            "ready": true,
            "bit_width": 4,
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
    serde_json::json!({
        "ready": bool_path(incremental, &["ready"]).unwrap_or(false),
        "upsert_ready": bool_path(incremental, &["upsert_ready"]).unwrap_or(false),
        "delete_ready": bool_path(incremental, &["delete_ready"]).unwrap_or(false),
        "watermark_ready": bool_path(incremental, &["watermark_ready"]).unwrap_or(false),
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
    let persisted_segment_descriptor_ready =
        bool_path(predicate, &["persisted_segment_descriptor_ready"]).unwrap_or(false);
    let supported_ops = array_path(predicate, &["supported_ops"]).unwrap_or_default();
    let required_ops_ready = REQUIRED_PREDICATE_PUSHDOWN_OPS
        .iter()
        .all(|required| supported_ops.iter().any(|op| op == required));
    let scan_filter_fields = array_path(predicate, &["scan_filter_fields"]).unwrap_or_default();
    let required_scan_filter_fields_ready = REQUIRED_SCAN_FILTER_FIELDS
        .iter()
        .all(|required| scan_filter_fields.iter().any(|field| field == required));
    let ready = equality_ready
        && in_list_ready
        && not_in_list_ready
        && range_ready
        && row_filter_ready
        && segment_pruning_ready
        && numeric_min_max_ready
        && required_ops_ready
        && required_scan_filter_fields_ready;
    serde_json::json!({
        "ready": ready,
        "equality_ready": equality_ready,
        "in_list_ready": in_list_ready,
        "not_in_list_ready": not_in_list_ready,
        "range_ready": range_ready,
        "row_filter_ready": row_filter_ready,
        "segment_pruning_ready": segment_pruning_ready,
        "numeric_min_max_ready": numeric_min_max_ready,
        "persisted_segment_descriptor_ready": persisted_segment_descriptor_ready,
        "required_ops_ready": required_ops_ready,
        "required_scan_filter_fields_ready": required_scan_filter_fields_ready,
        "required_ops": REQUIRED_PREDICATE_PUSHDOWN_OPS,
        "required_scan_filter_fields": REQUIRED_SCAN_FILTER_FIELDS,
        "supported_ops": supported_ops,
        "scan_filter_fields": scan_filter_fields,
    })
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

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let content = std::fs::read_to_string(path).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to read search projection evidence JSON: {error}"
        ))
    })?;
    serde_json::from_str(&content).map_err(|error| {
        SkeinError::Semantic(format!(
            "failed to parse search projection evidence JSON: {error}"
        ))
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
        nowledge_search_projection_shadow_evidence_json, run_skein_search_projection_probe,
    };
    use crate::nowledge_contract::SEARCH_PROJECTION_SHADOW_EVIDENCE_ROUTE;
    use crate::{
        SearchEmbeddingManifest, SearchIndex, SearchProjectionDelta, SearchProjectionKind,
        SearchProjectionRow,
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
        assert_eq!(report["fail_soft_ready"], true);
        assert_eq!(report["rebuild_marker_ready"], true);
        assert_eq!(report["metadata_repair_marker_ready"], true);
        assert_eq!(report["incremental_update_ready"], true);
        assert_eq!(report["source_chunk_ready"], true);
        assert_eq!(report["predicate_pushdown_ready"], true);
        assert_eq!(
            report["predicate_pushdown"]["required_scan_filter_fields_ready"],
            true
        );
        assert_eq!(
            report["predicate_pushdown"]["required_scan_filter_fields"],
            serde_json::json!([
                "space_id",
                "unit_type",
                "importance",
                "confidence",
                "created_at",
                "event_start",
                "event_end",
                "is_latest"
            ])
        );
        assert_eq!(report["compressed_vector_projection_required"], true);
        assert_eq!(report["compressed_vector_projection_ready"], true);
        assert_eq!(report["blocker_codes"], serde_json::json!([]));
    }

    #[test]
    fn probe_contract_example_feeds_search_projection_evidence() {
        let contract = nowledge_search_projection_probe_contract_json();
        let example = &contract["example_primary_probe"];
        let evidence = nowledge_search_projection_evidence_json(example);

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
        assert_eq!(evidence["ready"], true);
        assert_eq!(evidence["predicate_pushdown_ready"], true);
        assert_eq!(evidence["compressed_vector_projection_required"], false);
        assert_eq!(evidence["compressed_vector_projection_ready"], true);
    }

    #[test]
    fn search_projection_evidence_requires_compressed_vector_projection_for_skein_probe() {
        let mut probe = ready_probe();
        probe["compressed_vector_projection"]["ready"] = serde_json::json!(false);
        probe["compressed_vector_projection"]["compiled"] = serde_json::json!(false);
        probe["compressed_vector_projection"]["blocker_codes"] =
            serde_json::json!(["turbovec_feature_disabled"]);

        let report = nowledge_search_projection_evidence_json(&probe);

        assert_eq!(report["ready"], false);
        assert_eq!(report["compressed_vector_projection_required"], true);
        assert_eq!(report["compressed_vector_projection_ready"], false);
        assert_eq!(
            report["compressed_vector_projection"]["blocker_codes"],
            serde_json::json!(["turbovec_feature_disabled"])
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
    fn search_projection_evidence_fails_closed_for_missing_predicate_pushdown() {
        let mut probe = ready_probe();
        probe.as_object_mut().unwrap().remove("predicate_pushdown");

        let report = nowledge_search_projection_evidence_json(&probe);

        assert_eq!(report["ready"], false);
        assert_eq!(report["predicate_pushdown_ready"], false);
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["predicate_pushdown_not_ready"])
        );
    }

    #[test]
    fn search_projection_evidence_fails_closed_for_missing_scan_filter_field() {
        let mut probe = ready_probe();
        probe["predicate_pushdown"]["scan_filter_fields"]
            .as_array_mut()
            .unwrap()
            .retain(|field| field.as_str() != Some("created_at"));

        let report = nowledge_search_projection_evidence_json(&probe);

        assert_eq!(report["ready"], false);
        assert_eq!(report["predicate_pushdown_ready"], false);
        assert_eq!(
            report["predicate_pushdown"]["required_scan_filter_fields_ready"],
            false
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["predicate_pushdown_not_ready"])
        );
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

        let probe = run_skein_search_projection_probe(
            [
                "--active-model",
                "bge-m3",
                "--active-dimension",
                "8",
                path.to_str().unwrap(),
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();
        let evidence = nowledge_search_projection_evidence_json(&probe);

        assert_eq!(probe["protocol"], "skein-nowledge-search-projection-probe");
        #[cfg(feature = "turbovec")]
        assert_eq!(evidence["ready"], true);
        #[cfg(not(feature = "turbovec"))]
        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["covered_table_count"], 6);
        assert_eq!(evidence["source_chunk_ready"], true);
        assert_eq!(evidence["predicate_pushdown_ready"], true);
        assert_eq!(evidence["compressed_vector_projection_required"], true);
        #[cfg(feature = "turbovec")]
        assert_eq!(evidence["compressed_vector_projection_ready"], true);
        #[cfg(not(feature = "turbovec"))]
        assert_eq!(evidence["compressed_vector_projection_ready"], false);
        assert_eq!(
            probe["predicate_pushdown"]["supported_ops"],
            serde_json::json!(["eq", "in", "not_in", "gt", "gte", "lt", "lte"])
        );
        #[cfg(not(feature = "turbovec"))]
        assert!(evidence["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "compressed_vector_projection_not_ready"));
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn search_projection_shadow_evidence_reports_ready_for_matching_probes() {
        let primary = ready_probe();
        let shadow = ready_probe();

        let report = nowledge_search_projection_shadow_evidence_json(&primary, &shadow);

        assert_eq!(report["route"], SEARCH_PROJECTION_SHADOW_EVIDENCE_ROUTE);
        assert_eq!(report["ready"], true);
        assert_eq!(report["primary_ready"], true);
        assert_eq!(report["shadow_ready"], true);
        assert_eq!(report["document_count_parity"], true);
        assert_eq!(report["table_parity"]["ready"], true);
        assert_eq!(report["embedding_identity_parity"], true);
        assert_eq!(report["lifecycle_parity"], true);
        assert_eq!(report["incremental_watermark_parity"], true);
        assert_eq!(report["blocker_codes"], serde_json::json!([]));
    }

    #[test]
    fn search_projection_shadow_evidence_ignores_descriptor_state_for_predicate_parity() {
        let mut primary = ready_probe();
        primary["predicate_pushdown"]["persisted_segment_descriptor_ready"] =
            serde_json::json!(false);
        let shadow = ready_probe();

        let report = nowledge_search_projection_shadow_evidence_json(&primary, &shadow);

        assert_eq!(report["ready"], true);
        assert_eq!(report["predicate_pushdown_parity"], true);
    }

    #[test]
    fn search_projection_shadow_evidence_fails_closed_on_scan_filter_field_mismatch() {
        let primary = ready_probe();
        let mut shadow = ready_probe();
        shadow["predicate_pushdown"]["scan_filter_fields"]
            .as_array_mut()
            .unwrap()
            .retain(|field| field.as_str() != Some("event_start"));

        let report = nowledge_search_projection_shadow_evidence_json(&primary, &shadow);

        assert_eq!(report["ready"], false);
        assert_eq!(report["shadow_ready"], false);
        assert_eq!(report["predicate_pushdown_parity"], false);
        assert!(report["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "predicate_pushdown_mismatch"));
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
    fn search_projection_evidence_read_errors_do_not_echo_paths() {
        let path = PathBuf::from("missing-search-projection-evidence-redaction.json");

        let error = super::read_json_file(&path).unwrap_err();

        assert!(!error.to_string().contains("missing-search-projection"));
    }

    fn ready_probe() -> serde_json::Value {
        serde_json::json!({
            "engine": "skein",
            "derived_projection": true,
            "document_count": 6,
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
                "persisted_segment_descriptor_ready": true,
                "supported_ops": ["eq", "in", "not_in", "gt", "gte", "lt", "lte"],
                "scan_filter_fields": [
                    "kind",
                    "external_id",
                    "source_id",
                    "space_id",
                    "unit_type",
                    "importance",
                    "confidence",
                    "created_at",
                    "updated_at",
                    "event_start",
                    "event_end",
                    "is_latest"
                ]
            },
            "compressed_vector_projection": {
                "engine": "turbovec",
                "compiled": true,
                "ready": true,
                "bit_width": 4,
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
        vec![
            nowledge_probe_row(SearchProjectionKind::Memory, "mem_1"),
            nowledge_probe_row_without_embedding(SearchProjectionKind::Message, "msg_1"),
            nowledge_probe_row(SearchProjectionKind::Community, "community_1"),
            nowledge_probe_row(SearchProjectionKind::Entity, "entity_1"),
            nowledge_probe_row(SearchProjectionKind::Source, "source_1"),
            nowledge_probe_row(SearchProjectionKind::SourceChunk, "chunk_1"),
        ]
    }

    fn nowledge_probe_row(kind: SearchProjectionKind, external_id: &str) -> SearchProjectionRow {
        SearchProjectionRow {
            kind,
            external_id: external_id.to_string(),
            title: format!("{external_id} title"),
            body: format!("{external_id} body"),
            embedding: Some(vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            source_id: Some("source_1".to_string()),
            metadata: BTreeMap::from([("space_id".to_string(), "default".to_string())]),
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
}
