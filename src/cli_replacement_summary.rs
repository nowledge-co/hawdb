use std::collections::BTreeSet;

pub fn nowledge_replacement_summary_usage() -> String {
    "nowledge-replacement-summary requires [--require-production-ready] [--compact] [--max-family-items <n>] [--max-blockers <n>] <migration-gate-json>"
        .to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NowledgeReplacementSummaryOptions {
    pub include_family_details: bool,
    pub max_family_items: Option<usize>,
    pub include_blocker_details: bool,
    pub max_blockers: Option<usize>,
}

impl Default for NowledgeReplacementSummaryOptions {
    fn default() -> Self {
        Self {
            include_family_details: true,
            max_family_items: None,
            include_blocker_details: true,
            max_blockers: None,
        }
    }
}

pub fn nowledge_replacement_summary_json(bundle: &serde_json::Value) -> serde_json::Value {
    nowledge_replacement_summary_json_with_options(
        bundle,
        NowledgeReplacementSummaryOptions::default(),
    )
}

pub fn nowledge_replacement_summary_json_with_options(
    bundle: &serde_json::Value,
    options: NowledgeReplacementSummaryOptions,
) -> serde_json::Value {
    let covered_business_surface_per_million =
        json_get_u64_path(bundle, &["inventory_gate", "coverage_per_million"])
            .or_else(|| json_get_u64_path(bundle, &["coverage", "coverage_per_million"]));
    let shadow_parity_per_million = json_get_u64_path(bundle, &["cutover", "matched_per_million"])
        .or_else(|| json_get_u64_path(bundle, &["migration_gate", "shadow_matched_per_million"]));
    let replacement_readiness_per_million = replacement_readiness_per_million_from_families(bundle)
        .or_else(|| json_get_u64_path(bundle, &["replacement_readiness_per_million"]));
    let migration_gate_decision = json_get_str_path(bundle, &["migration_gate", "decision"]);
    let cutover_decision = json_get_str_path(bundle, &["cutover", "decision"]);
    let cutover_evidence_eligible =
        json_get_bool_path(bundle, &["cutover_evidence", "eligible"]).unwrap_or(false);
    let shadow_evidence = shadow_evidence_summary(bundle);
    let shadow_evidence_ready = shadow_evidence.ready;
    let previous_wrapper_contract_ready =
        json_get_bool_path(bundle, &["previous_wrapper_contract_evidence", "ready"])
            .unwrap_or(false);
    let full_contract_evidence = full_contract_evidence_summary(bundle);
    let full_contract_evidence_ready = full_contract_evidence.ready;
    let dual_engine_evidence = dual_engine_evidence_summary(bundle);
    let dual_engine_evidence_present = dual_engine_evidence.present;
    let dual_engine_evidence_ready = dual_engine_evidence.ready;
    let dual_engine_evidence_consistent = dual_engine_evidence.consistent;
    let search_projection_evidence = search_projection_evidence_summary(bundle);
    let search_projection_evidence_ready = search_projection_evidence.ready;
    let search_projection_shadow_evidence = search_projection_shadow_evidence_summary(bundle);
    let search_projection_shadow_evidence_ready = search_projection_shadow_evidence.ready;
    let bounded_read_evidence = bounded_read_evidence_summary(bundle);
    let bounded_read_evidence_ready = bounded_read_evidence.ready;
    let background_graph_delta_evidence_missing =
        background_maintenance_graph_delta_evidence_missing(bundle);
    let production_cutover_ready = migration_gate_decision == Some("ready")
        && cutover_decision == Some("ready")
        && cutover_evidence_eligible
        && shadow_evidence_ready
        && previous_wrapper_contract_ready
        && full_contract_evidence_ready
        && dual_engine_evidence_present
        && dual_engine_evidence_ready == Some(true)
        && dual_engine_evidence_consistent
        && search_projection_evidence_ready
        && search_projection_shadow_evidence_ready
        && bounded_read_evidence_ready
        && !background_graph_delta_evidence_missing
        && replacement_readiness_per_million == Some(1_000_000);
    let production_replacement_per_million = if production_cutover_ready {
        1_000_000
    } else {
        0
    };
    let blocking_categories = nowledge_replacement_blocking_categories(
        bundle,
        ReplacementReadinessInputs {
            covered_business_surface_per_million,
            shadow_parity_per_million,
            replacement_readiness_per_million,
            migration_gate_decision,
            cutover_decision,
            cutover_evidence_eligible,
            shadow_evidence_ready,
            previous_wrapper_contract_ready,
            full_contract_evidence_ready,
            dual_engine_evidence_present,
            dual_engine_evidence_ready,
            dual_engine_evidence_consistent,
            search_projection_evidence_ready,
            search_projection_shadow_evidence_ready,
            bounded_read_evidence_ready,
            background_graph_delta_evidence_missing,
        },
    );
    let blockers = nowledge_replacement_blockers(bundle);
    let blocker_details = nowledge_replacement_blocker_details(&blockers, options);
    let missing_evidence = nowledge_replacement_missing_evidence(bundle);
    let family_details = replacement_readiness_family_details(bundle, options);
    let family_summary = replacement_readiness_family_summary(bundle, family_details.omitted_count);
    let next_actions = nowledge_replacement_next_actions(
        bundle,
        NextActionInputs {
            covered_business_surface_per_million,
            shadow_parity_per_million,
            replacement_readiness_per_million,
            migration_gate_decision,
            cutover_decision,
            cutover_evidence_eligible,
            shadow_evidence_ready,
            previous_wrapper_contract_ready,
            full_contract_evidence_ready,
            dual_engine_evidence_present,
            dual_engine_evidence_ready,
            dual_engine_evidence_consistent,
            search_projection_evidence_ready,
            search_projection_shadow_evidence_ready,
            bounded_read_evidence_ready,
            background_graph_delta_evidence_missing,
            production_cutover_ready,
        },
    );

    serde_json::json!({
        "protocol": "skein-nowledge-replacement-summary",
        "business_surface": {
            "covered_per_million": covered_business_surface_per_million,
            "ready": covered_business_surface_per_million == Some(1_000_000),
        },
        "shadow_parity": {
            "matched_per_million": shadow_parity_per_million,
            "decision": cutover_decision,
            "ready": cutover_decision == Some("ready") && shadow_parity_per_million == Some(1_000_000),
        },
        "replacement_readiness_per_million": replacement_readiness_per_million,
        "production_replacement_per_million": production_replacement_per_million,
        "production_cutover_ready": production_cutover_ready,
        "migration_gate_decision": migration_gate_decision,
        "dual_engine_evidence": {
            "present": dual_engine_evidence.present,
            "ready": dual_engine_evidence.ready,
            "consistent": dual_engine_evidence.consistent,
            "primary_engine": dual_engine_evidence.primary_engine,
            "shadow_engine": dual_engine_evidence.shadow_engine,
            "primary_check_count": dual_engine_evidence.primary_check_count,
            "shadow_check_count": dual_engine_evidence.shadow_check_count,
            "matched_check_count": dual_engine_evidence.matched_check_count,
            "primary_only_check_count": dual_engine_evidence.primary_only_check_count,
            "matched_per_million": dual_engine_evidence.matched_per_million,
        },
        "search_projection_evidence": {
            "present": search_projection_evidence.present,
            "ready": search_projection_evidence.ready,
            "derived_projection": search_projection_evidence.derived_projection,
            "all_tables_covered": search_projection_evidence.all_tables_covered,
            "covered_table_count": search_projection_evidence.covered_table_count,
            "required_table_count": search_projection_evidence.required_table_count,
            "fts_ready": search_projection_evidence.fts_ready,
            "vector_ready": search_projection_evidence.vector_ready,
            "embedding_identity_ready": search_projection_evidence.embedding_identity_ready,
            "fail_soft_ready": search_projection_evidence.fail_soft_ready,
            "rebuild_marker_ready": search_projection_evidence.rebuild_marker_ready,
            "metadata_repair_marker_ready": search_projection_evidence.metadata_repair_marker_ready,
            "incremental_update_ready": search_projection_evidence.incremental_update_ready,
            "source_chunk_ready": search_projection_evidence.source_chunk_ready,
            "blocker_codes": search_projection_evidence.blocker_codes,
        },
        "search_projection_shadow_evidence": {
            "present": search_projection_shadow_evidence.present,
            "ready": search_projection_shadow_evidence.ready,
            "primary_ready": search_projection_shadow_evidence.primary_ready,
            "shadow_ready": search_projection_shadow_evidence.shadow_ready,
            "document_count_parity": search_projection_shadow_evidence.document_count_parity,
            "table_parity_ready": search_projection_shadow_evidence.table_parity_ready,
            "embedding_identity_parity": search_projection_shadow_evidence.embedding_identity_parity,
            "lifecycle_parity": search_projection_shadow_evidence.lifecycle_parity,
            "incremental_watermark_parity": search_projection_shadow_evidence.incremental_watermark_parity,
            "primary_engine": search_projection_shadow_evidence.primary_engine,
            "shadow_engine": search_projection_shadow_evidence.shadow_engine,
            "blocker_codes": search_projection_shadow_evidence.blocker_codes,
        },
        "bounded_read_evidence": {
            "present": bounded_read_evidence.present,
            "ready": bounded_read_evidence.ready,
            "max_rows": bounded_read_evidence.max_rows,
            "execution_row_cap": bounded_read_evidence.execution_row_cap,
            "row_limit_enforced_before_output": bounded_read_evidence.row_limit_enforced_before_output,
            "operator_row_cap_enabled": bounded_read_evidence.operator_row_cap_enabled,
            "streaming": bounded_read_evidence.streaming,
            "blocking_operator_count": bounded_read_evidence.blocking_operator_count,
            "blocker_codes": bounded_read_evidence.blocker_codes,
        },
        "cutover_evidence": {
            "eligible": cutover_evidence_eligible,
            "evidence_kind": json_get_str_path(bundle, &["cutover_evidence", "evidence_kind"]),
            "ready_engine_kind": json_get_str_path(bundle, &["cutover_evidence", "ready_engine_kind"]),
            "ready_wrapper_identity": json_get_str_path(bundle, &["cutover_evidence", "ready_wrapper_identity"]),
            "storage_recovery_required": json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_required"]),
            "storage_recovery_ready": json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_ready"]),
            "storage_recovery_protocol_matches": json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_protocol_matches"]),
            "storage_recovery_blocker_codes": json_get_array_path(bundle, &["cutover_evidence", "storage_recovery_blocker_codes"]),
            "background_maintenance_required": json_get_bool_path(bundle, &["cutover_evidence", "background_maintenance_required"]),
            "background_maintenance_ready": json_get_bool_path(bundle, &["cutover_evidence", "background_maintenance_ready"]),
            "background_maintenance_protocol_matches": json_get_bool_path(bundle, &["cutover_evidence", "background_maintenance_protocol_matches"]),
            "background_maintenance_executable_search_projection_graph_delta_count": json_get_u64_path(bundle, &["cutover_evidence", "background_maintenance_executable_search_projection_graph_delta_count"]),
            "background_maintenance_admitted_search_projection_graph_delta_count": json_get_u64_path(bundle, &["cutover_evidence", "background_maintenance_admitted_search_projection_graph_delta_count"]),
            "background_maintenance_deferred_search_projection_graph_delta_count": json_get_u64_path(bundle, &["cutover_evidence", "background_maintenance_deferred_search_projection_graph_delta_count"]),
            "background_maintenance_rejected_search_projection_graph_delta_count": json_get_u64_path(bundle, &["cutover_evidence", "background_maintenance_rejected_search_projection_graph_delta_count"]),
            "background_maintenance_executable_search_projection_graph_delta_operations": json_get_u64_path(bundle, &["cutover_evidence", "background_maintenance_executable_search_projection_graph_delta_operations"]),
            "background_maintenance_admitted_search_projection_graph_delta_operations": json_get_u64_path(bundle, &["cutover_evidence", "background_maintenance_admitted_search_projection_graph_delta_operations"]),
            "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch": json_get_u64_path(bundle, &["cutover_evidence", "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch"]),
            "background_maintenance_blocker_codes": json_get_array_path(bundle, &["cutover_evidence", "background_maintenance_blocker_codes"]),
            "replacement_readiness_min_per_million": json_get_u64_path(bundle, &["cutover_evidence", "replacement_readiness_min_per_million"]),
        },
        "shadow_evidence": {
            "ready": shadow_evidence.ready,
            "run_evidence_kind": shadow_evidence.run_evidence_kind,
            "ready_engine_kind": shadow_evidence.ready_engine_kind,
            "ready_wrapper_identity": shadow_evidence.ready_wrapper_identity,
            "contract_wrapper_identity": shadow_evidence.contract_wrapper_identity,
            "cutover_ready_wrapper_identity": shadow_evidence.cutover_ready_wrapper_identity,
        },
        "previous_wrapper_contract_evidence": {
            "ready": previous_wrapper_contract_ready,
            "evidence_kind": json_get_str_path(bundle, &["previous_wrapper_contract_evidence", "evidence_kind"]),
            "wrapper_identity": json_get_str_path(bundle, &["previous_wrapper_contract_evidence", "wrapper_identity"]),
            "requires_full_contract_ready": json_get_bool_path(bundle, &["previous_wrapper_contract_evidence", "requires_full_contract_ready"]),
            "requires_wrapper_identity": json_get_bool_path(bundle, &["previous_wrapper_contract_evidence", "requires_wrapper_identity"]),
            "blocker_codes": json_get_array_path(bundle, &["previous_wrapper_contract_evidence", "blocker_codes"]),
        },
        "full_contract_evidence": {
            "ready": full_contract_evidence.ready,
            "required_contract_ready": full_contract_evidence.required_contract_ready,
            "full_contract_checked": full_contract_evidence.full_contract_checked,
            "full_contract_ready": full_contract_evidence.full_contract_ready,
            "selected_checks": full_contract_evidence.selected_checks,
            "check_count": full_contract_evidence.check_count,
        },
        "blocking_categories": blocking_categories,
        "blocker_summary": {
            "total_count": blockers.len(),
            "omitted_count": blocker_details.omitted_count,
        },
        "blockers": blocker_details.blockers,
        "missing_evidence": missing_evidence,
        "next_actions": next_actions,
        "replacement_readiness_family_summary": family_summary,
        "replacement_readiness_by_query_family": family_details.families,
    })
}

struct NowledgeReplacementBlockerDetails {
    blockers: Vec<String>,
    omitted_count: usize,
}

fn nowledge_replacement_blocker_details(
    blockers: &[String],
    options: NowledgeReplacementSummaryOptions,
) -> NowledgeReplacementBlockerDetails {
    if !options.include_blocker_details {
        return NowledgeReplacementBlockerDetails {
            blockers: Vec::new(),
            omitted_count: blockers.len(),
        };
    }
    let limit = options.max_blockers.unwrap_or(blockers.len());
    NowledgeReplacementBlockerDetails {
        blockers: blockers.iter().take(limit).cloned().collect(),
        omitted_count: blockers.len().saturating_sub(limit),
    }
}

struct ReplacementReadinessFamilyDetails {
    families: serde_json::Value,
    omitted_count: usize,
}

fn replacement_readiness_family_details(
    bundle: &serde_json::Value,
    options: NowledgeReplacementSummaryOptions,
) -> ReplacementReadinessFamilyDetails {
    let families = bundle
        .get("replacement_readiness_by_query_family")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    if !options.include_family_details {
        return ReplacementReadinessFamilyDetails {
            families: serde_json::json!([]),
            omitted_count: families.len(),
        };
    }
    let limit = options.max_family_items.unwrap_or(families.len());
    let omitted_count = families.len().saturating_sub(limit);
    ReplacementReadinessFamilyDetails {
        families: serde_json::Value::Array(families.into_iter().take(limit).collect()),
        omitted_count,
    }
}

fn replacement_readiness_family_summary(
    bundle: &serde_json::Value,
    omitted_count: usize,
) -> serde_json::Value {
    let families = bundle
        .get("replacement_readiness_by_query_family")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let total_count = families.len();
    let ready_count = families
        .iter()
        .filter(|family| {
            family
                .get("replacement_readiness_per_million")
                .and_then(serde_json::Value::as_u64)
                == Some(1_000_000)
        })
        .count();
    let blocked_count = total_count.saturating_sub(ready_count);
    let min_replacement_readiness_per_million = families
        .iter()
        .filter_map(|family| {
            family
                .get("replacement_readiness_per_million")
                .and_then(serde_json::Value::as_u64)
        })
        .min();
    let blocked_query_families = families
        .iter()
        .filter(|family| {
            family
                .get("replacement_readiness_per_million")
                .and_then(serde_json::Value::as_u64)
                != Some(1_000_000)
        })
        .filter_map(|family| {
            family
                .get("query_family")
                .and_then(serde_json::Value::as_str)
        })
        .map(str::to_string)
        .collect::<Vec<_>>();
    serde_json::json!({
        "total_count": total_count,
        "ready_count": ready_count,
        "blocked_count": blocked_count,
        "omitted_count": omitted_count,
        "min_replacement_readiness_per_million": min_replacement_readiness_per_million,
        "blocked_query_families": blocked_query_families,
    })
}

fn replacement_readiness_per_million_from_families(bundle: &serde_json::Value) -> Option<u64> {
    bundle
        .get("replacement_readiness_by_query_family")
        .and_then(serde_json::Value::as_array)?
        .iter()
        .map(|family| {
            family
                .get("replacement_readiness_per_million")
                .and_then(serde_json::Value::as_u64)
        })
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        .min()
}

struct ReplacementReadinessInputs<'a> {
    covered_business_surface_per_million: Option<u64>,
    shadow_parity_per_million: Option<u64>,
    replacement_readiness_per_million: Option<u64>,
    migration_gate_decision: Option<&'a str>,
    cutover_decision: Option<&'a str>,
    cutover_evidence_eligible: bool,
    shadow_evidence_ready: bool,
    previous_wrapper_contract_ready: bool,
    full_contract_evidence_ready: bool,
    dual_engine_evidence_present: bool,
    dual_engine_evidence_ready: Option<bool>,
    dual_engine_evidence_consistent: bool,
    search_projection_evidence_ready: bool,
    search_projection_shadow_evidence_ready: bool,
    bounded_read_evidence_ready: bool,
    background_graph_delta_evidence_missing: bool,
}

struct NextActionInputs<'a> {
    covered_business_surface_per_million: Option<u64>,
    shadow_parity_per_million: Option<u64>,
    replacement_readiness_per_million: Option<u64>,
    migration_gate_decision: Option<&'a str>,
    cutover_decision: Option<&'a str>,
    cutover_evidence_eligible: bool,
    shadow_evidence_ready: bool,
    previous_wrapper_contract_ready: bool,
    full_contract_evidence_ready: bool,
    dual_engine_evidence_present: bool,
    dual_engine_evidence_ready: Option<bool>,
    dual_engine_evidence_consistent: bool,
    search_projection_evidence_ready: bool,
    search_projection_shadow_evidence_ready: bool,
    bounded_read_evidence_ready: bool,
    background_graph_delta_evidence_missing: bool,
    production_cutover_ready: bool,
}

struct DualEngineEvidenceSummary<'a> {
    present: bool,
    ready: Option<bool>,
    consistent: bool,
    primary_engine: Option<&'a str>,
    shadow_engine: Option<&'a str>,
    primary_check_count: Option<u64>,
    shadow_check_count: Option<u64>,
    matched_check_count: Option<u64>,
    primary_only_check_count: Option<u64>,
    matched_per_million: Option<u64>,
}

struct ShadowEvidenceSummary<'a> {
    ready: bool,
    run_evidence_kind: Option<&'a str>,
    ready_engine_kind: Option<&'a str>,
    ready_wrapper_identity: Option<&'a str>,
    contract_wrapper_identity: Option<&'a str>,
    cutover_ready_wrapper_identity: Option<&'a str>,
}

struct SearchProjectionEvidenceSummary {
    present: bool,
    ready: bool,
    derived_projection: Option<bool>,
    all_tables_covered: Option<bool>,
    covered_table_count: Option<u64>,
    required_table_count: Option<u64>,
    fts_ready: Option<bool>,
    vector_ready: Option<bool>,
    embedding_identity_ready: Option<bool>,
    fail_soft_ready: Option<bool>,
    rebuild_marker_ready: Option<bool>,
    metadata_repair_marker_ready: Option<bool>,
    incremental_update_ready: Option<bool>,
    source_chunk_ready: Option<bool>,
    blocker_codes: serde_json::Value,
}

struct SearchProjectionShadowEvidenceSummary<'a> {
    present: bool,
    ready: bool,
    primary_ready: Option<bool>,
    shadow_ready: Option<bool>,
    document_count_parity: Option<bool>,
    table_parity_ready: Option<bool>,
    embedding_identity_parity: Option<bool>,
    lifecycle_parity: Option<bool>,
    incremental_watermark_parity: Option<bool>,
    primary_engine: Option<&'a str>,
    shadow_engine: Option<&'a str>,
    blocker_codes: serde_json::Value,
}

struct BoundedReadEvidenceSummary {
    present: bool,
    ready: bool,
    max_rows: Option<u64>,
    execution_row_cap: Option<u64>,
    row_limit_enforced_before_output: Option<bool>,
    operator_row_cap_enabled: Option<bool>,
    streaming: Option<bool>,
    blocking_operator_count: Option<u64>,
    blocker_codes: serde_json::Value,
}

fn shadow_evidence_summary(bundle: &serde_json::Value) -> ShadowEvidenceSummary<'_> {
    let run_evidence_kind = json_get_str_path(bundle, &["shadow_run", "evidence_kind"]);
    let ready_engine_kind = json_get_str_path(bundle, &["shadow_ready", "engine_kind"]);
    let ready_wrapper_identity = json_get_str_path(bundle, &["shadow_ready", "wrapper_identity"]);
    let contract_wrapper_identity = json_get_str_path(
        bundle,
        &["previous_wrapper_contract_evidence", "wrapper_identity"],
    );
    let cutover_ready_wrapper_identity =
        json_get_str_path(bundle, &["cutover_evidence", "ready_wrapper_identity"]);
    ShadowEvidenceSummary {
        ready: run_evidence_kind == Some("previous_wrapper")
            && ready_engine_kind == Some("previous_wrapper")
            && ready_wrapper_identity.is_some()
            && ready_wrapper_identity == contract_wrapper_identity
            && ready_wrapper_identity == cutover_ready_wrapper_identity,
        run_evidence_kind,
        ready_engine_kind,
        ready_wrapper_identity,
        contract_wrapper_identity,
        cutover_ready_wrapper_identity,
    }
}

struct FullContractEvidenceSummary {
    ready: bool,
    required_contract_ready: Option<bool>,
    full_contract_checked: Option<bool>,
    full_contract_ready: Option<bool>,
    selected_checks: Option<u64>,
    check_count: Option<u64>,
}

fn full_contract_evidence_summary(bundle: &serde_json::Value) -> FullContractEvidenceSummary {
    let path = if json_get_path(bundle, &["contract_evidence"]).is_some() {
        &["contract_evidence"][..]
    } else {
        &[][..]
    };
    let required_contract_ready =
        json_get_bool_path_from_dynamic(bundle, path, "required_contract_ready");
    let full_contract_checked =
        json_get_bool_path_from_dynamic(bundle, path, "full_contract_checked");
    let full_contract_ready = json_get_bool_path_from_dynamic(bundle, path, "full_contract_ready");
    let selected_checks = json_get_u64_path_from_dynamic(bundle, path, "selected_checks");
    let check_count = json_get_u64_path_from_dynamic(bundle, path, "check_count")
        .or_else(|| json_get_u64_path_from_dynamic(bundle, path, "total_checks"));
    FullContractEvidenceSummary {
        ready: required_contract_ready == Some(true)
            && full_contract_checked == Some(true)
            && full_contract_ready == Some(true)
            && check_count.is_some_and(|count| count > 0)
            && selected_checks == check_count,
        required_contract_ready,
        full_contract_checked,
        full_contract_ready,
        selected_checks,
        check_count,
    }
}

fn dual_engine_evidence_summary(bundle: &serde_json::Value) -> DualEngineEvidenceSummary<'_> {
    let path = if json_get_path(bundle, &["dual_engine_evidence"]).is_some() {
        &["dual_engine_evidence"][..]
    } else {
        &["cutover", "dual_engine_evidence"][..]
    };
    let present = json_get_path(bundle, path).is_some();
    let primary_check_count = json_get_u64_path_from_dynamic(bundle, path, "primary_check_count");
    let shadow_check_count = json_get_u64_path_from_dynamic(bundle, path, "shadow_check_count");
    let matched_check_count = json_get_u64_path_from_dynamic(bundle, path, "matched_check_count");
    let primary_only_check_count =
        json_get_u64_path_from_dynamic(bundle, path, "primary_only_check_count");
    let matched_per_million = json_get_u64_path_from_dynamic(bundle, path, "matched_per_million");
    DualEngineEvidenceSummary {
        present,
        ready: json_get_bool_path_from_dynamic(bundle, path, "ready"),
        consistent: primary_check_count.is_some_and(|count| count > 0)
            && primary_check_count == shadow_check_count
            && matched_check_count == shadow_check_count
            && primary_only_check_count == Some(0)
            && matched_per_million == Some(1_000_000),
        primary_engine: json_get_str_path_from_dynamic(bundle, path, "primary_engine"),
        shadow_engine: json_get_str_path_from_dynamic(bundle, path, "shadow_engine"),
        primary_check_count,
        shadow_check_count,
        matched_check_count,
        primary_only_check_count,
        matched_per_million,
    }
}

fn search_projection_evidence_summary(
    bundle: &serde_json::Value,
) -> SearchProjectionEvidenceSummary {
    let path = if json_get_path(bundle, &["search_projection_evidence"]).is_some() {
        &["search_projection_evidence"][..]
    } else {
        &["cutover_evidence", "search_projection_evidence"][..]
    };
    let present = json_get_path(bundle, path).is_some();
    let covered_table_count = json_get_u64_path_from_dynamic(bundle, path, "covered_table_count");
    let required_table_count = json_get_u64_path_from_dynamic(bundle, path, "required_table_count");
    let derived_projection = json_get_bool_path_from_dynamic(bundle, path, "derived_projection");
    let all_tables_covered = json_get_bool_path_from_dynamic(bundle, path, "all_tables_covered");
    let fts_ready = json_get_bool_path_from_dynamic(bundle, path, "fts_ready");
    let vector_ready = json_get_bool_path_from_dynamic(bundle, path, "vector_ready");
    let embedding_identity_ready =
        json_get_bool_path_from_dynamic(bundle, path, "embedding_identity_ready");
    let fail_soft_ready = json_get_bool_path_from_dynamic(bundle, path, "fail_soft_ready");
    let rebuild_marker_ready =
        json_get_bool_path_from_dynamic(bundle, path, "rebuild_marker_ready");
    let metadata_repair_marker_ready =
        json_get_bool_path_from_dynamic(bundle, path, "metadata_repair_marker_ready");
    let incremental_update_ready =
        json_get_bool_path_from_dynamic(bundle, path, "incremental_update_ready");
    let source_chunk_ready = json_get_bool_path_from_dynamic(bundle, path, "source_chunk_ready");
    let ready = present
        && derived_projection == Some(true)
        && all_tables_covered == Some(true)
        && covered_table_count.is_some_and(|count| count > 0)
        && covered_table_count == required_table_count
        && fts_ready == Some(true)
        && vector_ready == Some(true)
        && embedding_identity_ready == Some(true)
        && fail_soft_ready == Some(true)
        && rebuild_marker_ready == Some(true)
        && metadata_repair_marker_ready == Some(true)
        && incremental_update_ready == Some(true)
        && source_chunk_ready == Some(true);
    SearchProjectionEvidenceSummary {
        present,
        ready,
        derived_projection,
        all_tables_covered,
        covered_table_count,
        required_table_count,
        fts_ready,
        vector_ready,
        embedding_identity_ready,
        fail_soft_ready,
        rebuild_marker_ready,
        metadata_repair_marker_ready,
        incremental_update_ready,
        source_chunk_ready,
        blocker_codes: json_get_array_path_from_dynamic(bundle, path, "blocker_codes"),
    }
}

fn search_projection_shadow_evidence_summary(
    bundle: &serde_json::Value,
) -> SearchProjectionShadowEvidenceSummary<'_> {
    let path = if json_get_path(bundle, &["search_projection_shadow_evidence"]).is_some() {
        &["search_projection_shadow_evidence"][..]
    } else {
        &["cutover_evidence", "search_projection_shadow_evidence"][..]
    };
    let present = json_get_path(bundle, path).is_some();
    let primary_ready = json_get_bool_path_from_dynamic(bundle, path, "primary_ready");
    let shadow_ready = json_get_bool_path_from_dynamic(bundle, path, "shadow_ready");
    let document_count_parity =
        json_get_bool_path_from_dynamic(bundle, path, "document_count_parity");
    let table_parity_ready = {
        let mut table_parity_path = path.to_vec();
        table_parity_path.extend(["table_parity", "ready"]);
        json_get_bool_path(bundle, &table_parity_path)
            .or_else(|| json_get_bool_path_from_dynamic(bundle, path, "table_parity_ready"))
    };
    let embedding_identity_parity =
        json_get_bool_path_from_dynamic(bundle, path, "embedding_identity_parity");
    let lifecycle_parity = json_get_bool_path_from_dynamic(bundle, path, "lifecycle_parity");
    let incremental_watermark_parity =
        json_get_bool_path_from_dynamic(bundle, path, "incremental_watermark_parity");
    let ready = present
        && primary_ready == Some(true)
        && shadow_ready == Some(true)
        && document_count_parity == Some(true)
        && table_parity_ready == Some(true)
        && embedding_identity_parity == Some(true)
        && lifecycle_parity == Some(true)
        && incremental_watermark_parity == Some(true);
    SearchProjectionShadowEvidenceSummary {
        present,
        ready,
        primary_ready,
        shadow_ready,
        document_count_parity,
        table_parity_ready,
        embedding_identity_parity,
        lifecycle_parity,
        incremental_watermark_parity,
        primary_engine: json_get_str_path_from_dynamic(bundle, path, "primary_engine"),
        shadow_engine: json_get_str_path_from_dynamic(bundle, path, "shadow_engine"),
        blocker_codes: json_get_array_path_from_dynamic(bundle, path, "blocker_codes"),
    }
}

fn bounded_read_evidence_summary(bundle: &serde_json::Value) -> BoundedReadEvidenceSummary {
    let path = if json_get_path(bundle, &["bounded_read_evidence"]).is_some() {
        &["bounded_read_evidence"][..]
    } else {
        &["cutover_evidence", "bounded_read_evidence"][..]
    };
    let present = json_get_path(bundle, path).is_some();
    let max_rows = json_get_u64_path_from_dynamic(bundle, path, "max_rows");
    let execution_row_cap = json_get_u64_path_from_dynamic(bundle, path, "execution_row_cap");
    let row_limit_enforced_before_output =
        json_get_bool_path_from_dynamic(bundle, path, "row_limit_enforced_before_output");
    let operator_row_cap_enabled =
        json_get_bool_path_from_dynamic(bundle, path, "operator_row_cap_enabled");
    let streaming = json_get_bool_path_from_dynamic(bundle, path, "streaming");
    let blocking_operator_count =
        json_get_u64_path_from_dynamic(bundle, path, "blocking_operator_count");
    let ready = present
        && max_rows.is_some_and(|value| value > 0)
        && execution_row_cap == max_rows.and_then(|value| value.checked_add(1))
        && row_limit_enforced_before_output == Some(true)
        && operator_row_cap_enabled == Some(true);
    BoundedReadEvidenceSummary {
        present,
        ready,
        max_rows,
        execution_row_cap,
        row_limit_enforced_before_output,
        operator_row_cap_enabled,
        streaming,
        blocking_operator_count,
        blocker_codes: json_get_array_path_from_dynamic(bundle, path, "blocker_codes"),
    }
}

fn json_get_array_path_from_dynamic(
    value: &serde_json::Value,
    base_path: &[&str],
    field: &str,
) -> serde_json::Value {
    let mut path = base_path.to_vec();
    path.push(field);
    json_get_array_path(value, &path)
}

fn nowledge_replacement_blocking_categories(
    bundle: &serde_json::Value,
    inputs: ReplacementReadinessInputs<'_>,
) -> Vec<String> {
    let mut categories = BTreeSet::new();
    if inputs.covered_business_surface_per_million != Some(1_000_000) {
        categories.insert("scanner_coverage".to_string());
    }
    if inputs.shadow_parity_per_million != Some(1_000_000)
        || inputs.cutover_decision != Some("ready")
        || !inputs.shadow_evidence_ready
    {
        categories.insert("shadow_parity".to_string());
    }
    if inputs.replacement_readiness_per_million != Some(1_000_000) {
        categories.insert("query_family_readiness".to_string());
    }
    if inputs.migration_gate_decision != Some("ready") {
        categories.insert("migration_gate".to_string());
    }
    if !inputs.cutover_evidence_eligible {
        categories.insert("cutover_evidence".to_string());
    }
    if !inputs.previous_wrapper_contract_ready || !inputs.full_contract_evidence_ready {
        categories.insert("previous_wrapper_contract".to_string());
    }
    if !inputs.dual_engine_evidence_present
        || inputs.dual_engine_evidence_ready != Some(true)
        || !inputs.dual_engine_evidence_consistent
    {
        categories.insert("dual_engine_evidence".to_string());
    }
    if !inputs.search_projection_evidence_ready {
        categories.insert("search_projection_evidence".to_string());
    }
    if !inputs.search_projection_shadow_evidence_ready {
        categories.insert("search_projection_shadow_evidence".to_string());
    }
    if !inputs.bounded_read_evidence_ready {
        categories.insert("bounded_read_evidence".to_string());
    }
    if json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_required"]) == Some(true)
        && json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_ready"]) != Some(true)
    {
        categories.insert("storage_recovery".to_string());
    }
    if json_get_bool_path(
        bundle,
        &["cutover_evidence", "background_maintenance_required"],
    ) == Some(true)
        && (json_get_bool_path(
            bundle,
            &["cutover_evidence", "background_maintenance_ready"],
        ) != Some(true)
            || inputs.background_graph_delta_evidence_missing)
    {
        categories.insert("background_maintenance".to_string());
    }
    if json_get_u64_path(
        bundle,
        &[
            "cutover_evidence",
            "replacement_readiness_invalid_family_count",
        ],
    )
    .unwrap_or(0)
        > 0
    {
        categories.insert("query_family_readiness".to_string());
    }
    categories.into_iter().collect()
}

fn nowledge_replacement_next_actions(
    bundle: &serde_json::Value,
    inputs: NextActionInputs<'_>,
) -> Vec<serde_json::Value> {
    if inputs.production_cutover_ready {
        return Vec::new();
    }

    let mut actions = Vec::new();
    if inputs.covered_business_surface_per_million != Some(1_000_000) {
        actions.push(next_action(
            "refresh_nowledge_cypher_inventory",
            "scanner coverage is below full Nowledge business-surface coverage",
            [
                "inventory_gate.coverage_per_million",
                "coverage.coverage_per_million",
            ],
        ));
    }
    if inputs.replacement_readiness_per_million != Some(1_000_000)
        || has_blocked_replacement_family(bundle)
    {
        actions.push(next_action(
            "close_blocked_query_families",
            "one or more query families are below full replacement readiness",
            [
                "replacement_readiness_per_million",
                "replacement_readiness_by_query_family",
            ],
        ));
    }
    if inputs.shadow_parity_per_million != Some(1_000_000)
        || inputs.cutover_decision != Some("ready")
        || inputs.migration_gate_decision != Some("ready")
        || !inputs.shadow_evidence_ready
    {
        actions.push(next_action(
            "run_previous_wrapper_shadow_gate",
            "shadow parity or migration gate decision is not ready",
            [
                "cutover.matched_per_million",
                "cutover.decision",
                "migration_gate.decision",
                "shadow_run.evidence_kind",
                "shadow_ready.engine_kind",
                "shadow_ready.wrapper_identity",
                "cutover_evidence.ready_wrapper_identity",
                "previous_wrapper_contract_evidence.wrapper_identity",
            ],
        ));
    }
    if !inputs.cutover_evidence_eligible {
        actions.push(next_action(
            "provide_eligible_cutover_evidence",
            "cutover evidence is missing or not eligible for production replacement",
            [
                "cutover_evidence.eligible",
                "cutover_evidence.evidence_kind",
                "cutover_evidence.ready_engine_kind",
                "cutover_evidence.ready_wrapper_identity",
            ],
        ));
    }
    if !inputs.previous_wrapper_contract_ready || !inputs.full_contract_evidence_ready {
        actions.push(next_action(
            "run_full_previous_wrapper_contract_check",
            "previous-wrapper contract evidence is missing or not ready",
            [
                "required_contract_ready",
                "full_contract_checked",
                "full_contract_ready",
                "selected_checks",
                "check_count",
                "previous_wrapper_contract_evidence.ready",
                "previous_wrapper_contract_evidence.wrapper_identity",
                "previous_wrapper_contract_evidence.blocker_codes",
            ],
        ));
    }
    if !inputs.dual_engine_evidence_present
        || inputs.dual_engine_evidence_ready != Some(true)
        || !inputs.dual_engine_evidence_consistent
    {
        actions.push(next_action(
            "rerun_dual_engine_shadow_gate",
            "side-by-side dual-engine evidence is missing or not ready",
            [
                "dual_engine_evidence.present",
                "dual_engine_evidence.ready",
                "dual_engine_evidence.consistent",
                "dual_engine_evidence.primary_check_count",
                "dual_engine_evidence.shadow_check_count",
                "dual_engine_evidence.matched_check_count",
                "dual_engine_evidence.primary_only_check_count",
                "dual_engine_evidence.matched_per_million",
            ],
        ));
    }
    if !inputs.search_projection_evidence_ready {
        actions.push(next_action(
            "attach_search_projection_replacement_evidence",
            "LanceDB replacement evidence is missing or not ready",
            [
                "search_projection_evidence.present",
                "search_projection_evidence.ready",
                "search_projection_evidence.derived_projection",
                "search_projection_evidence.all_tables_covered",
                "search_projection_evidence.covered_table_count",
                "search_projection_evidence.required_table_count",
                "search_projection_evidence.fts_ready",
                "search_projection_evidence.vector_ready",
                "search_projection_evidence.embedding_identity_ready",
                "search_projection_evidence.fail_soft_ready",
                "search_projection_evidence.rebuild_marker_ready",
                "search_projection_evidence.metadata_repair_marker_ready",
                "search_projection_evidence.incremental_update_ready",
                "search_projection_evidence.source_chunk_ready",
                "search_projection_evidence.blocker_codes",
            ],
        ));
    }
    if !inputs.search_projection_shadow_evidence_ready {
        actions.push(next_action(
            "run_search_projection_shadow_evidence",
            "LanceDB/Skein search projection side-by-side evidence is missing or not ready",
            [
                "search_projection_shadow_evidence.present",
                "search_projection_shadow_evidence.ready",
                "search_projection_shadow_evidence.primary_ready",
                "search_projection_shadow_evidence.shadow_ready",
                "search_projection_shadow_evidence.document_count_parity",
                "search_projection_shadow_evidence.table_parity.ready",
                "search_projection_shadow_evidence.embedding_identity_parity",
                "search_projection_shadow_evidence.lifecycle_parity",
                "search_projection_shadow_evidence.incremental_watermark_parity",
                "search_projection_shadow_evidence.blocker_codes",
            ],
        ));
    }
    if !inputs.bounded_read_evidence_ready {
        actions.push(next_action(
            "attach_bounded_read_profile",
            "bounded read execution profile is missing or not ready",
            [
                "bounded_read_evidence.present",
                "bounded_read_evidence.ready",
                "bounded_read_evidence.max_rows",
                "bounded_read_evidence.execution_row_cap",
                "bounded_read_evidence.row_limit_enforced_before_output",
                "bounded_read_evidence.operator_row_cap_enabled",
                "bounded_read_evidence.blocking_operator_count",
                "bounded_read_evidence.blocker_codes",
            ],
        ));
    }
    if json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_required"]) == Some(true)
        && json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_ready"]) != Some(true)
    {
        actions.push(next_action(
            "attach_storage_recovery_report",
            "required storage recovery evidence is missing or blocked",
            [
                "cutover_evidence.storage_recovery_present",
                "cutover_evidence.storage_recovery_ready",
                "cutover_evidence.storage_recovery_blocker_codes",
            ],
        ));
    }
    if json_get_bool_path(
        bundle,
        &["cutover_evidence", "background_maintenance_required"],
    ) == Some(true)
        && (json_get_bool_path(
            bundle,
            &["cutover_evidence", "background_maintenance_ready"],
        ) != Some(true)
            || inputs.background_graph_delta_evidence_missing)
    {
        actions.push(next_action(
            "attach_background_maintenance_report",
            "required background maintenance QoS or graph-delta evidence is missing or blocked",
            [
                "cutover_evidence.background_maintenance_present",
                "cutover_evidence.background_maintenance_ready",
                "cutover_evidence.background_maintenance_executable_search_projection_graph_delta_count",
                "cutover_evidence.background_maintenance_admitted_search_projection_graph_delta_count",
                "cutover_evidence.background_maintenance_blocker_codes",
            ],
        ));
    }

    actions
}

fn has_blocked_replacement_family(bundle: &serde_json::Value) -> bool {
    bundle
        .get("replacement_readiness_by_query_family")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .any(|family| {
            family
                .get("replacement_readiness_per_million")
                .and_then(serde_json::Value::as_u64)
                != Some(1_000_000)
        })
}

fn next_action(
    action: &str,
    reason: &str,
    evidence_fields: impl IntoIterator<Item = &'static str>,
) -> serde_json::Value {
    serde_json::json!({
        "action": action,
        "reason": reason,
        "evidence_fields": evidence_fields.into_iter().collect::<Vec<_>>(),
    })
}

fn nowledge_replacement_missing_evidence(bundle: &serde_json::Value) -> Vec<String> {
    let mut missing = Vec::new();
    if bundle.get("cutover_evidence").is_none() {
        missing.push("cutover_evidence".to_string());
    }
    if json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_required"]) == Some(true)
        && json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_present"])
            != Some(true)
    {
        missing.push("storage_recovery".to_string());
    }
    if json_get_bool_path(
        bundle,
        &["cutover_evidence", "background_maintenance_required"],
    ) == Some(true)
        && json_get_bool_path(
            bundle,
            &["cutover_evidence", "background_maintenance_present"],
        ) != Some(true)
    {
        missing.push("background_maintenance".to_string());
    }
    if background_maintenance_graph_delta_evidence_missing(bundle) {
        missing.push("background_maintenance_search_projection_graph_delta".to_string());
    }
    if bundle
        .get("replacement_readiness_by_query_family")
        .is_none()
    {
        missing.push("replacement_readiness_by_query_family".to_string());
    }
    if bundle.get("previous_wrapper_contract_evidence").is_none() {
        missing.push("previous_wrapper_contract_evidence".to_string());
    }
    if !full_contract_evidence_summary(bundle).ready {
        missing.push("full_contract_evidence".to_string());
    }
    if bundle.get("dual_engine_evidence").is_none()
        && json_get_path(bundle, &["cutover", "dual_engine_evidence"]).is_none()
    {
        missing.push("dual_engine_evidence".to_string());
    }
    if bundle.get("search_projection_evidence").is_none()
        && json_get_path(bundle, &["cutover_evidence", "search_projection_evidence"]).is_none()
    {
        missing.push("search_projection_evidence".to_string());
    } else if !search_projection_evidence_summary(bundle).ready {
        missing.push("search_projection_evidence_ready".to_string());
    }
    if bundle.get("search_projection_shadow_evidence").is_none()
        && json_get_path(
            bundle,
            &["cutover_evidence", "search_projection_shadow_evidence"],
        )
        .is_none()
    {
        missing.push("search_projection_shadow_evidence".to_string());
    } else if !search_projection_shadow_evidence_summary(bundle).ready {
        missing.push("search_projection_shadow_evidence_ready".to_string());
    }
    if bundle.get("bounded_read_evidence").is_none()
        && json_get_path(bundle, &["cutover_evidence", "bounded_read_evidence"]).is_none()
    {
        missing.push("bounded_read_evidence".to_string());
    } else if !bounded_read_evidence_summary(bundle).ready {
        missing.push("bounded_read_evidence_ready".to_string());
    }
    if bundle.get("shadow_run").is_none() {
        missing.push("shadow_run".to_string());
    }
    if bundle.get("shadow_ready").is_none() {
        missing.push("shadow_ready".to_string());
    }
    missing
}

fn background_maintenance_graph_delta_evidence_missing(bundle: &serde_json::Value) -> bool {
    if json_get_bool_path(
        bundle,
        &["cutover_evidence", "background_maintenance_required"],
    ) != Some(true)
        || json_get_bool_path(
            bundle,
            &["cutover_evidence", "background_maintenance_present"],
        ) != Some(true)
    {
        return false;
    }

    [
        "background_maintenance_executable_search_projection_graph_delta_count",
        "background_maintenance_admitted_search_projection_graph_delta_count",
        "background_maintenance_deferred_search_projection_graph_delta_count",
        "background_maintenance_rejected_search_projection_graph_delta_count",
        "background_maintenance_executable_search_projection_graph_delta_operations",
        "background_maintenance_admitted_search_projection_graph_delta_operations",
        "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch",
    ]
    .into_iter()
    .any(|field| json_get_u64_path_from_dynamic(bundle, &["cutover_evidence"], field).is_none())
}

fn nowledge_replacement_blockers(bundle: &serde_json::Value) -> Vec<String> {
    let mut blockers = BTreeSet::new();
    for path in [
        &["inventory_gate", "blockers"][..],
        &["cutover", "blockers"][..],
        &["migration_gate", "blockers"][..],
        &["cutover_evidence", "blockers"][..],
        &["cutover_evidence", "storage_recovery_blockers"][..],
        &["cutover_evidence", "background_maintenance_blockers"][..],
        &["cutover_evidence", "replacement_readiness_blockers"][..],
        &["previous_wrapper_contract_evidence", "blockers"][..],
        &["contract_evidence", "full_contract_blockers"][..],
        &["contract_evidence", "required_contract_blockers"][..],
        &["full_contract_blockers"][..],
        &["required_contract_blockers"][..],
        &["search_projection_shadow_evidence", "blocker_codes"][..],
        &[
            "cutover_evidence",
            "search_projection_shadow_evidence",
            "blocker_codes",
        ][..],
        &["bounded_read_evidence", "blocker_codes"][..],
        &["cutover_evidence", "bounded_read_evidence", "blocker_codes"][..],
    ] {
        for blocker in json_get_string_array_path(bundle, path) {
            blockers.insert(blocker);
        }
    }
    blockers.into_iter().collect()
}

fn json_get_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    Some(current)
}

fn json_get_u64_path(value: &serde_json::Value, path: &[&str]) -> Option<u64> {
    json_get_path(value, path).and_then(serde_json::Value::as_u64)
}

fn json_get_bool_path(value: &serde_json::Value, path: &[&str]) -> Option<bool> {
    json_get_path(value, path).and_then(serde_json::Value::as_bool)
}

fn json_get_str_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a str> {
    json_get_path(value, path).and_then(serde_json::Value::as_str)
}

fn json_get_bool_path_from_dynamic(
    value: &serde_json::Value,
    prefix: &[&str],
    field: &str,
) -> Option<bool> {
    json_get_path_from_dynamic(value, prefix, field).and_then(serde_json::Value::as_bool)
}

fn json_get_str_path_from_dynamic<'a>(
    value: &'a serde_json::Value,
    prefix: &[&str],
    field: &str,
) -> Option<&'a str> {
    json_get_path_from_dynamic(value, prefix, field).and_then(serde_json::Value::as_str)
}

fn json_get_u64_path_from_dynamic(
    value: &serde_json::Value,
    prefix: &[&str],
    field: &str,
) -> Option<u64> {
    json_get_path_from_dynamic(value, prefix, field).and_then(serde_json::Value::as_u64)
}

fn json_get_path_from_dynamic<'a>(
    value: &'a serde_json::Value,
    prefix: &[&str],
    field: &str,
) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for key in prefix {
        current = current.get(*key)?;
    }
    current.get(field)
}

fn json_get_array_path(value: &serde_json::Value, path: &[&str]) -> serde_json::Value {
    json_get_path(value, path)
        .filter(|value| value.is_array())
        .cloned()
        .unwrap_or(serde_json::Value::Null)
}

fn json_get_string_array_path(value: &serde_json::Value, path: &[&str]) -> Vec<String> {
    json_get_path(value, path)
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| item.as_str().map(str::to_string))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        nowledge_replacement_summary_json, nowledge_replacement_summary_json_with_options,
        nowledge_replacement_summary_usage, NowledgeReplacementSummaryOptions,
    };

    #[test]
    fn replacement_summary_keeps_production_replacement_zero_without_cutover_evidence() {
        let bundle = serde_json::json!({
            "coverage": {
                "coverage_per_million": 1_000_000
            },
            "inventory_gate": {
                "coverage_per_million": 1_000_000,
                "blockers": []
            },
            "cutover": {
                "decision": "ready",
                "matched_per_million": 1_000_000,
                "blockers": []
            },
            "migration_gate": {
                "decision": "ready",
                "blockers": []
            },
            "replacement_readiness_per_million": 1_000_000,
            "replacement_readiness_by_query_family": []
        });

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["business_surface"]["ready"], true);
        assert_eq!(summary["shadow_parity"]["ready"], true);
        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(
            summary["blocking_categories"],
            serde_json::json!([
                "bounded_read_evidence",
                "cutover_evidence",
                "dual_engine_evidence",
                "previous_wrapper_contract",
                "search_projection_evidence",
                "search_projection_shadow_evidence",
                "shadow_parity"
            ])
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "cutover_evidence"));
    }

    #[test]
    fn replacement_summary_reports_production_ready_when_all_evidence_is_ready() {
        let bundle = production_ready_bundle();

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], true);
        assert_eq!(summary["production_replacement_per_million"], 1_000_000);
        assert_eq!(summary["blocking_categories"], serde_json::json!([]));
        assert_eq!(summary["missing_evidence"], serde_json::json!([]));
        assert_eq!(summary["next_actions"], serde_json::json!([]));
        assert_eq!(summary["bounded_read_evidence"]["present"], true);
        assert_eq!(summary["bounded_read_evidence"]["ready"], true);
        assert_eq!(summary["bounded_read_evidence"]["execution_row_cap"], 513);
        assert_eq!(
            summary["cutover_evidence"]["storage_recovery_protocol_matches"],
            true
        );
        assert_eq!(
            summary["cutover_evidence"]["background_maintenance_protocol_matches"],
            true
        );
        assert_eq!(
            summary["cutover_evidence"]
                ["background_maintenance_executable_search_projection_graph_delta_count"],
            2
        );
        assert_eq!(
            summary["cutover_evidence"]
                ["background_maintenance_admitted_search_projection_graph_delta_count"],
            1
        );
        assert_eq!(
            summary["cutover_evidence"]
                ["background_maintenance_deferred_search_projection_graph_delta_count"],
            1
        );
        assert_eq!(
            summary["cutover_evidence"]
                ["background_maintenance_executable_search_projection_graph_delta_operations"],
            8
        );
        assert_eq!(
            summary["cutover_evidence"]
                ["background_maintenance_admitted_search_projection_graph_delta_operations"],
            3
        );
        assert_eq!(
            summary["cutover_evidence"]
                ["background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch"],
            42
        );
        assert_eq!(summary["shadow_evidence"]["ready"], true);
        assert_eq!(
            summary["shadow_evidence"]["ready_wrapper_identity"],
            "nowledge-previous-wrapper:test"
        );
        assert_eq!(summary["previous_wrapper_contract_evidence"]["ready"], true);
        assert_eq!(summary["full_contract_evidence"]["ready"], true);
        assert_eq!(
            summary["full_contract_evidence"]["full_contract_checked"],
            true
        );
        assert_eq!(
            summary["full_contract_evidence"]["full_contract_ready"],
            true
        );
        assert_eq!(summary["full_contract_evidence"]["selected_checks"], 2);
        assert_eq!(summary["full_contract_evidence"]["check_count"], 2);
        assert_eq!(
            summary["previous_wrapper_contract_evidence"]["wrapper_identity"],
            "nowledge-previous-wrapper:test"
        );
        assert_eq!(summary["dual_engine_evidence"]["present"], true);
        assert_eq!(summary["dual_engine_evidence"]["ready"], true);
        assert_eq!(summary["dual_engine_evidence"]["consistent"], true);
        assert_eq!(summary["dual_engine_evidence"]["primary_engine"], "skein");
        assert_eq!(
            summary["dual_engine_evidence"]["shadow_engine"],
            "previous-wrapper"
        );
        assert_eq!(summary["search_projection_evidence"]["present"], true);
        assert_eq!(summary["search_projection_evidence"]["ready"], true);
        assert_eq!(
            summary["search_projection_evidence"]["covered_table_count"],
            6
        );
        assert_eq!(
            summary["search_projection_evidence"]["required_table_count"],
            6
        );
        assert_eq!(
            summary["search_projection_shadow_evidence"]["present"],
            true
        );
        assert_eq!(summary["search_projection_shadow_evidence"]["ready"], true);
        assert_eq!(
            summary["search_projection_shadow_evidence"]["primary_engine"],
            "lancedb"
        );
        assert_eq!(
            summary["search_projection_shadow_evidence"]["shadow_engine"],
            "skein"
        );
        assert_eq!(
            summary["search_projection_shadow_evidence"]["table_parity_ready"],
            true
        );
        assert_eq!(
            summary["replacement_readiness_by_query_family"][0]["query_family"],
            "memory_lookup"
        );
    }

    #[test]
    fn replacement_summary_blocks_production_without_search_projection_evidence() {
        let mut bundle = production_ready_bundle();
        bundle
            .as_object_mut()
            .unwrap()
            .remove("search_projection_evidence");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(summary["search_projection_evidence"]["present"], false);
        assert_eq!(summary["search_projection_evidence"]["ready"], false);
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_projection_evidence"));
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_projection_evidence"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| {
                action["action"] == "attach_search_projection_replacement_evidence"
                    && action["evidence_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| field == "search_projection_evidence.fts_ready")
            }));
    }

    #[test]
    fn replacement_summary_recomputes_search_projection_evidence_readiness() {
        let mut bundle = production_ready_bundle();
        bundle["search_projection_evidence"]["ready"] = serde_json::json!(true);
        bundle["search_projection_evidence"]["covered_table_count"] = serde_json::json!(5);
        bundle["search_projection_evidence"]["blocker_codes"] =
            serde_json::json!(["missing_source_chunks_index"]);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(summary["search_projection_evidence"]["ready"], false);
        assert_eq!(
            summary["search_projection_evidence"]["covered_table_count"],
            5
        );
        assert_eq!(
            summary["search_projection_evidence"]["blocker_codes"],
            serde_json::json!(["missing_source_chunks_index"])
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_projection_evidence_ready"));
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_projection_evidence"));
    }

    #[test]
    fn replacement_summary_blocks_production_without_search_projection_shadow_evidence() {
        let mut bundle = production_ready_bundle();
        bundle
            .as_object_mut()
            .unwrap()
            .remove("search_projection_shadow_evidence");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(
            summary["search_projection_shadow_evidence"]["present"],
            false
        );
        assert_eq!(summary["search_projection_shadow_evidence"]["ready"], false);
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_projection_shadow_evidence"));
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_projection_shadow_evidence"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| {
                action["action"] == "run_search_projection_shadow_evidence"
                    && action["evidence_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| {
                            field == "search_projection_shadow_evidence.table_parity.ready"
                        })
            }));
    }

    #[test]
    fn replacement_summary_recomputes_search_projection_shadow_evidence_readiness() {
        let mut bundle = production_ready_bundle();
        bundle["search_projection_shadow_evidence"]["ready"] = serde_json::json!(true);
        bundle["search_projection_shadow_evidence"]["table_parity"]["ready"] =
            serde_json::json!(false);
        bundle["search_projection_shadow_evidence"]["blocker_codes"] =
            serde_json::json!(["table_parity_mismatch"]);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(summary["search_projection_shadow_evidence"]["ready"], false);
        assert_eq!(
            summary["search_projection_shadow_evidence"]["table_parity_ready"],
            false
        );
        assert_eq!(
            summary["search_projection_shadow_evidence"]["blocker_codes"],
            serde_json::json!(["table_parity_mismatch"])
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_projection_shadow_evidence_ready"));
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_projection_shadow_evidence"));
    }

    #[test]
    fn replacement_summary_blocks_production_without_bounded_read_evidence() {
        let mut bundle = production_ready_bundle();
        bundle
            .as_object_mut()
            .unwrap()
            .remove("bounded_read_evidence");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(summary["bounded_read_evidence"]["present"], false);
        assert_eq!(summary["bounded_read_evidence"]["ready"], false);
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "bounded_read_evidence"));
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "bounded_read_evidence"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_bounded_read_profile"));
    }

    #[test]
    fn replacement_summary_blocks_production_without_graph_delta_aggregate_evidence() {
        let mut bundle = production_ready_bundle();
        bundle["cutover_evidence"]
            .as_object_mut()
            .unwrap()
            .remove("background_maintenance_admitted_search_projection_graph_delta_count");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "background_maintenance"));
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "background_maintenance_search_projection_graph_delta"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| {
                item["action"] == "attach_background_maintenance_report"
                    && item["evidence_fields"].as_array().unwrap().iter().any(|field| {
                        field
                            == "cutover_evidence.background_maintenance_admitted_search_projection_graph_delta_count"
                    })
            }));
    }

    #[test]
    fn replacement_summary_blocks_production_when_dual_engine_evidence_is_not_ready() {
        let mut bundle = production_ready_bundle();
        bundle["dual_engine_evidence"]["ready"] = serde_json::json!(false);
        bundle["dual_engine_evidence"]["primary_only_check_count"] = serde_json::json!(1);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(summary["dual_engine_evidence"]["present"], true);
        assert_eq!(summary["dual_engine_evidence"]["ready"], false);
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "dual_engine_evidence"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "rerun_dual_engine_shadow_gate"));
    }

    #[test]
    fn replacement_summary_blocks_production_when_dual_engine_counts_are_inconsistent() {
        let mut bundle = production_ready_bundle();
        bundle["dual_engine_evidence"]["ready"] = serde_json::json!(true);
        bundle["dual_engine_evidence"]["matched_check_count"] = serde_json::json!(0);
        bundle["dual_engine_evidence"]["primary_only_check_count"] = serde_json::json!(1);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(summary["dual_engine_evidence"]["ready"], true);
        assert_eq!(summary["dual_engine_evidence"]["consistent"], false);
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "dual_engine_evidence"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| {
                action["action"] == "rerun_dual_engine_shadow_gate"
                    && action["evidence_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| field == "dual_engine_evidence.consistent")
            }));
    }

    #[test]
    fn replacement_summary_blocks_production_without_dual_engine_evidence() {
        let mut bundle = production_ready_bundle();
        bundle
            .as_object_mut()
            .unwrap()
            .remove("dual_engine_evidence");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(summary["dual_engine_evidence"]["present"], false);
        assert_eq!(
            summary["dual_engine_evidence"]["ready"],
            serde_json::Value::Null
        );
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "dual_engine_evidence"));
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "dual_engine_evidence"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| {
                action["action"] == "rerun_dual_engine_shadow_gate"
                    && action["evidence_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| field == "dual_engine_evidence.present")
            }));
    }

    #[test]
    fn replacement_summary_blocks_production_without_previous_wrapper_contract_evidence() {
        let mut bundle = production_ready_bundle();
        bundle
            .as_object_mut()
            .unwrap()
            .remove("previous_wrapper_contract_evidence");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(
            summary["previous_wrapper_contract_evidence"]["ready"],
            false
        );
        assert_eq!(
            summary["missing_evidence"],
            serde_json::json!(["previous_wrapper_contract_evidence"])
        );
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "run_full_previous_wrapper_contract_check"));
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "previous_wrapper_contract"));
    }

    #[test]
    fn replacement_summary_blocks_production_without_shadow_wrapper_identity() {
        let mut bundle = production_ready_bundle();
        bundle["shadow_ready"]
            .as_object_mut()
            .unwrap()
            .remove("wrapper_identity");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(summary["shadow_evidence"]["ready"], false);
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "shadow_parity"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| {
                action["action"] == "run_previous_wrapper_shadow_gate"
                    && action["evidence_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| field == "shadow_ready.wrapper_identity")
            }));
    }

    #[test]
    fn replacement_summary_blocks_production_without_full_contract_evidence() {
        let mut bundle = production_ready_bundle();
        bundle["full_contract_checked"] = serde_json::json!(false);
        bundle["full_contract_ready"] = serde_json::json!(false);
        bundle["selected_checks"] = serde_json::json!(1);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(summary["previous_wrapper_contract_evidence"]["ready"], true);
        assert_eq!(summary["full_contract_evidence"]["ready"], false);
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "full_contract_evidence"));
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "previous_wrapper_contract"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| {
                action["action"] == "run_full_previous_wrapper_contract_check"
                    && action["evidence_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| field == "full_contract_ready")
            }));
    }

    #[test]
    fn replacement_summary_accepts_total_checks_contract_evidence() {
        let mut bundle = production_ready_bundle();
        bundle["contract_evidence"] = serde_json::json!({
            "required_contract_ready": true,
            "full_contract_checked": true,
            "full_contract_ready": true,
            "selected_checks": 2,
            "total_checks": 2,
        });

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["full_contract_evidence"]["ready"], true);
        assert_eq!(summary["full_contract_evidence"]["check_count"], 2);
        assert_eq!(summary["production_cutover_ready"], true);
    }

    #[test]
    fn replacement_summary_can_omit_family_details_for_compact_output() {
        let bundle = production_ready_bundle();

        let summary = nowledge_replacement_summary_json_with_options(
            &bundle,
            NowledgeReplacementSummaryOptions {
                include_family_details: false,
                max_family_items: None,
                include_blocker_details: true,
                max_blockers: None,
            },
        );

        assert_eq!(
            summary["replacement_readiness_by_query_family"],
            serde_json::json!([])
        );
        assert_eq!(
            summary["replacement_readiness_family_summary"]["total_count"],
            1
        );
        assert_eq!(
            summary["replacement_readiness_family_summary"]["ready_count"],
            1
        );
        assert_eq!(
            summary["replacement_readiness_family_summary"]["omitted_count"],
            1
        );
        assert_eq!(summary["production_cutover_ready"], true);
    }

    #[test]
    fn replacement_summary_can_limit_family_details() {
        let bundle = serde_json::json!({
            "inventory_gate": {
                "coverage_per_million": 1_000_000,
                "blockers": []
            },
            "cutover": {
                "decision": "ready",
                "matched_per_million": 1_000_000,
                "blockers": []
            },
            "migration_gate": {
                "decision": "ready",
                "blockers": []
            },
            "cutover_evidence": {
                "eligible": true,
                "replacement_readiness_invalid_family_count": 0,
                "blockers": []
            },
            "shadow_run": {},
            "shadow_ready": {},
            "replacement_readiness_per_million": 1_000_000,
            "replacement_readiness_by_query_family": [
                {
                    "query_family": "memory_lookup",
                    "replacement_readiness_per_million": 1_000_000
                },
                {
                    "query_family": "search_projection",
                    "replacement_readiness_per_million": 0
                }
            ]
        });

        let summary = nowledge_replacement_summary_json_with_options(
            &bundle,
            NowledgeReplacementSummaryOptions {
                include_family_details: true,
                max_family_items: Some(1),
                include_blocker_details: true,
                max_blockers: None,
            },
        );

        assert_eq!(
            summary["replacement_readiness_by_query_family"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            summary["replacement_readiness_family_summary"]["total_count"],
            2
        );
        assert_eq!(
            summary["replacement_readiness_family_summary"]["ready_count"],
            1
        );
        assert_eq!(
            summary["replacement_readiness_family_summary"]["blocked_count"],
            1
        );
        assert_eq!(
            summary["replacement_readiness_family_summary"]["omitted_count"],
            1
        );
        assert_eq!(
            summary["replacement_readiness_family_summary"]["blocked_query_families"],
            serde_json::json!(["search_projection"])
        );
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "close_blocked_query_families"));
    }

    #[test]
    fn replacement_summary_uses_family_minimum_over_stale_top_level_readiness() {
        let mut bundle = production_ready_bundle();
        bundle["replacement_readiness_per_million"] = serde_json::json!(947_740);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["replacement_readiness_per_million"], 1_000_000);
        assert_eq!(
            summary["replacement_readiness_family_summary"]
                ["min_replacement_readiness_per_million"],
            1_000_000
        );
        assert_eq!(summary["production_cutover_ready"], true);
        assert!(!summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|category| category == "query_family_readiness"));
    }

    #[test]
    fn replacement_summary_can_omit_blocker_details_for_compact_output() {
        let bundle = blocked_bundle();

        let summary = nowledge_replacement_summary_json_with_options(
            &bundle,
            NowledgeReplacementSummaryOptions {
                include_family_details: false,
                max_family_items: None,
                include_blocker_details: false,
                max_blockers: None,
            },
        );

        assert_eq!(summary["blockers"], serde_json::json!([]));
        assert_eq!(summary["blocker_summary"]["total_count"], 5);
        assert_eq!(summary["blocker_summary"]["omitted_count"], 5);
        assert_eq!(
            summary["replacement_readiness_by_query_family"],
            serde_json::json!([])
        );
    }

    #[test]
    fn replacement_summary_can_limit_blocker_details() {
        let bundle = blocked_bundle();

        let summary = nowledge_replacement_summary_json_with_options(
            &bundle,
            NowledgeReplacementSummaryOptions {
                include_family_details: true,
                max_family_items: None,
                include_blocker_details: true,
                max_blockers: Some(2),
            },
        );

        assert_eq!(summary["blockers"].as_array().unwrap().len(), 2);
        assert_eq!(summary["blocker_summary"]["total_count"], 5);
        assert_eq!(summary["blocker_summary"]["omitted_count"], 3);
    }

    #[test]
    fn replacement_summary_groups_storage_and_background_blockers() {
        let bundle = serde_json::json!({
            "inventory_gate": {
                "coverage_per_million": 1_000_000,
                "blockers": []
            },
            "cutover": {
                "decision": "blocked",
                "matched_per_million": 500_000,
                "blockers": ["primary-only projected graph"]
            },
            "migration_gate": {
                "decision": "blocked",
                "blockers": ["shadow parity blocked"]
            },
            "cutover_evidence": {
                "eligible": false,
                "storage_recovery_required": true,
                "storage_recovery_present": false,
                "storage_recovery_ready": false,
                "storage_recovery_blocker_codes": ["wal_replay_unbounded"],
                "storage_recovery_blockers": [
                    "WAL replay was not opened with a configured entry bound"
                ],
                "background_maintenance_required": true,
                "background_maintenance_present": false,
                "background_maintenance_ready": false,
                "background_maintenance_blocker_codes": ["missing_evidence"],
                "background_maintenance_blockers": [
                    "background maintenance summary is missing"
                ],
                "replacement_readiness_min_per_million": 500_000,
                "replacement_readiness_invalid_family_count": 0,
                "replacement_readiness_blockers": [
                    "query family search is below full replacement readiness"
                ],
                "blockers": ["cutover evidence blocked"]
            },
            "replacement_readiness_per_million": 500_000,
            "replacement_readiness_by_query_family": []
        });

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(
            summary["blocking_categories"],
            serde_json::json!([
                "background_maintenance",
                "bounded_read_evidence",
                "cutover_evidence",
                "dual_engine_evidence",
                "migration_gate",
                "previous_wrapper_contract",
                "query_family_readiness",
                "search_projection_evidence",
                "search_projection_shadow_evidence",
                "shadow_parity",
                "storage_recovery"
            ])
        );
        assert_eq!(
            summary["missing_evidence"],
            serde_json::json!([
                "storage_recovery",
                "background_maintenance",
                "previous_wrapper_contract_evidence",
                "full_contract_evidence",
                "dual_engine_evidence",
                "search_projection_evidence",
                "search_projection_shadow_evidence",
                "bounded_read_evidence",
                "shadow_run",
                "shadow_ready"
            ])
        );
        assert!(summary["blockers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "background maintenance summary is missing"));
        assert_eq!(
            summary["cutover_evidence"]["storage_recovery_blocker_codes"],
            serde_json::json!(["wal_replay_unbounded"])
        );
        assert_eq!(
            summary["cutover_evidence"]["background_maintenance_blocker_codes"],
            serde_json::json!(["missing_evidence"])
        );
        assert_eq!(
            summary["next_actions"],
            serde_json::json!([
                {
                    "action": "close_blocked_query_families",
                    "reason": "one or more query families are below full replacement readiness",
                    "evidence_fields": [
                        "replacement_readiness_per_million",
                        "replacement_readiness_by_query_family"
                    ]
                },
                {
                    "action": "run_previous_wrapper_shadow_gate",
                    "reason": "shadow parity or migration gate decision is not ready",
                    "evidence_fields": [
                        "cutover.matched_per_million",
                        "cutover.decision",
                        "migration_gate.decision",
                        "shadow_run.evidence_kind",
                        "shadow_ready.engine_kind",
                        "shadow_ready.wrapper_identity",
                        "cutover_evidence.ready_wrapper_identity",
                        "previous_wrapper_contract_evidence.wrapper_identity"
                    ]
                },
                {
                    "action": "provide_eligible_cutover_evidence",
                    "reason": "cutover evidence is missing or not eligible for production replacement",
                    "evidence_fields": [
                        "cutover_evidence.eligible",
                        "cutover_evidence.evidence_kind",
                        "cutover_evidence.ready_engine_kind",
                        "cutover_evidence.ready_wrapper_identity"
                    ]
                },
                {
                    "action": "run_full_previous_wrapper_contract_check",
                    "reason": "previous-wrapper contract evidence is missing or not ready",
                    "evidence_fields": [
                        "required_contract_ready",
                        "full_contract_checked",
                        "full_contract_ready",
                        "selected_checks",
                        "check_count",
                        "previous_wrapper_contract_evidence.ready",
                        "previous_wrapper_contract_evidence.wrapper_identity",
                        "previous_wrapper_contract_evidence.blocker_codes"
                    ]
                },
                {
                    "action": "rerun_dual_engine_shadow_gate",
                    "reason": "side-by-side dual-engine evidence is missing or not ready",
                    "evidence_fields": [
                        "dual_engine_evidence.present",
                        "dual_engine_evidence.ready",
                        "dual_engine_evidence.consistent",
                        "dual_engine_evidence.primary_check_count",
                        "dual_engine_evidence.shadow_check_count",
                        "dual_engine_evidence.matched_check_count",
                        "dual_engine_evidence.primary_only_check_count",
                        "dual_engine_evidence.matched_per_million"
                    ]
                },
                {
                    "action": "attach_search_projection_replacement_evidence",
                    "reason": "LanceDB replacement evidence is missing or not ready",
                    "evidence_fields": [
                        "search_projection_evidence.present",
                        "search_projection_evidence.ready",
                        "search_projection_evidence.derived_projection",
                        "search_projection_evidence.all_tables_covered",
                        "search_projection_evidence.covered_table_count",
                        "search_projection_evidence.required_table_count",
                        "search_projection_evidence.fts_ready",
                        "search_projection_evidence.vector_ready",
                        "search_projection_evidence.embedding_identity_ready",
                        "search_projection_evidence.fail_soft_ready",
                        "search_projection_evidence.rebuild_marker_ready",
                        "search_projection_evidence.metadata_repair_marker_ready",
                        "search_projection_evidence.incremental_update_ready",
                        "search_projection_evidence.source_chunk_ready",
                        "search_projection_evidence.blocker_codes"
                    ]
                },
                {
                    "action": "run_search_projection_shadow_evidence",
                    "reason": "LanceDB/Skein search projection side-by-side evidence is missing or not ready",
                    "evidence_fields": [
                        "search_projection_shadow_evidence.present",
                        "search_projection_shadow_evidence.ready",
                        "search_projection_shadow_evidence.primary_ready",
                        "search_projection_shadow_evidence.shadow_ready",
                        "search_projection_shadow_evidence.document_count_parity",
                        "search_projection_shadow_evidence.table_parity.ready",
                        "search_projection_shadow_evidence.embedding_identity_parity",
                        "search_projection_shadow_evidence.lifecycle_parity",
                        "search_projection_shadow_evidence.incremental_watermark_parity",
                        "search_projection_shadow_evidence.blocker_codes"
                    ]
                },
                {
                    "action": "attach_bounded_read_profile",
                    "reason": "bounded read execution profile is missing or not ready",
                    "evidence_fields": [
                        "bounded_read_evidence.present",
                        "bounded_read_evidence.ready",
                        "bounded_read_evidence.max_rows",
                        "bounded_read_evidence.execution_row_cap",
                        "bounded_read_evidence.row_limit_enforced_before_output",
                        "bounded_read_evidence.operator_row_cap_enabled",
                        "bounded_read_evidence.blocking_operator_count",
                        "bounded_read_evidence.blocker_codes"
                    ]
                },
                {
                    "action": "attach_storage_recovery_report",
                    "reason": "required storage recovery evidence is missing or blocked",
                    "evidence_fields": [
                        "cutover_evidence.storage_recovery_present",
                        "cutover_evidence.storage_recovery_ready",
                        "cutover_evidence.storage_recovery_blocker_codes"
                    ]
                },
                {
                    "action": "attach_background_maintenance_report",
                    "reason": "required background maintenance QoS or graph-delta evidence is missing or blocked",
                    "evidence_fields": [
                        "cutover_evidence.background_maintenance_present",
                        "cutover_evidence.background_maintenance_ready",
                        "cutover_evidence.background_maintenance_executable_search_projection_graph_delta_count",
                        "cutover_evidence.background_maintenance_admitted_search_projection_graph_delta_count",
                        "cutover_evidence.background_maintenance_blocker_codes"
                    ]
                }
            ])
        );
        assert_eq!(summary["production_replacement_per_million"], 0);
    }

    #[test]
    fn validates_nowledge_replacement_summary_usage_text() {
        assert!(nowledge_replacement_summary_usage().contains("<migration-gate-json>"));
        assert!(nowledge_replacement_summary_usage().contains("--require-production-ready"));
        assert!(nowledge_replacement_summary_usage().contains("--compact"));
        assert!(nowledge_replacement_summary_usage().contains("--max-family-items"));
        assert!(nowledge_replacement_summary_usage().contains("--max-blockers"));
    }

    fn production_ready_bundle() -> serde_json::Value {
        serde_json::json!({
            "required_contract_ready": true,
            "full_contract_checked": true,
            "full_contract_ready": true,
            "selected_checks": 2,
            "check_count": 2,
            "inventory_gate": {
                "coverage_per_million": 1_000_000,
                "blockers": []
            },
            "cutover": {
                "decision": "ready",
                "matched_per_million": 1_000_000,
                "blockers": []
            },
            "migration_gate": {
                "decision": "ready",
                "blockers": []
            },
            "cutover_evidence": {
                "eligible": true,
                "evidence_kind": "previous_wrapper",
                "ready_engine_kind": "previous_wrapper",
                "ready_wrapper_identity": "nowledge-previous-wrapper:test",
                "storage_recovery_required": true,
                "storage_recovery_present": true,
                "storage_recovery_ready": true,
                "storage_recovery_protocol_matches": true,
                "storage_recovery_blocker_codes": [],
                "storage_recovery_blockers": [],
                "background_maintenance_required": true,
                "background_maintenance_present": true,
                "background_maintenance_ready": true,
                "background_maintenance_protocol_matches": true,
                "background_maintenance_executable_search_projection_graph_delta_count": 2,
                "background_maintenance_admitted_search_projection_graph_delta_count": 1,
                "background_maintenance_deferred_search_projection_graph_delta_count": 1,
                "background_maintenance_rejected_search_projection_graph_delta_count": 0,
                "background_maintenance_executable_search_projection_graph_delta_operations": 8,
                "background_maintenance_admitted_search_projection_graph_delta_operations": 3,
                "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch": 42,
                "background_maintenance_blocker_codes": [],
                "background_maintenance_blockers": [],
                "replacement_readiness_min_per_million": 1_000_000,
                "replacement_readiness_invalid_family_count": 0,
                "replacement_readiness_blockers": [],
                "blockers": []
            },
            "shadow_run": {
                "evidence_kind": "previous_wrapper"
            },
            "shadow_ready": {
                "engine_kind": "previous_wrapper",
                "wrapper_identity": "nowledge-previous-wrapper:test"
            },
            "dual_engine_evidence": {
                "ready": true,
                "primary_engine": "skein",
                "shadow_engine": "previous-wrapper",
                "primary_check_count": 1,
                "shadow_check_count": 1,
                "matched_check_count": 1,
                "primary_only_check_count": 0,
                "matched_per_million": 1_000_000
            },
            "search_projection_evidence": {
                "ready": true,
                "derived_projection": true,
                "all_tables_covered": true,
                "covered_table_count": 6,
                "required_table_count": 6,
                "fts_ready": true,
                "vector_ready": true,
                "embedding_identity_ready": true,
                "fail_soft_ready": true,
                "rebuild_marker_ready": true,
                "metadata_repair_marker_ready": true,
                "incremental_update_ready": true,
                "source_chunk_ready": true,
                "blocker_codes": []
            },
            "search_projection_shadow_evidence": {
                "ready": true,
                "primary_engine": "lancedb",
                "shadow_engine": "skein",
                "primary_ready": true,
                "shadow_ready": true,
                "document_count_parity": true,
                "table_parity": {
                    "ready": true
                },
                "embedding_identity_parity": true,
                "lifecycle_parity": true,
                "incremental_watermark_parity": true,
                "blocker_codes": []
            },
            "bounded_read_evidence": {
                "max_rows": 512,
                "execution_row_cap": 513,
                "row_limit_enforced_before_output": true,
                "operator_row_cap_enabled": true,
                "streaming": false,
                "blocking_operator_count": 0,
                "blocker_codes": []
            },
            "previous_wrapper_contract_evidence": {
                "ready": true,
                "evidence_kind": "previous_wrapper_contract",
                "wrapper_identity": "nowledge-previous-wrapper:test",
                "requires_full_contract_ready": true,
                "requires_wrapper_identity": true,
                "blocker_codes": [],
                "blockers": []
            },
            "replacement_readiness_per_million": 1_000_000,
            "replacement_readiness_by_query_family": [
                {
                    "query_family": "memory_lookup",
                    "replacement_readiness_per_million": 1_000_000
                }
            ]
        })
    }

    fn blocked_bundle() -> serde_json::Value {
        serde_json::json!({
            "inventory_gate": {
                "coverage_per_million": 1_000_000,
                "blockers": ["inventory blocked"]
            },
            "cutover": {
                "decision": "blocked",
                "matched_per_million": 500_000,
                "blockers": ["primary-only projected graph"]
            },
            "migration_gate": {
                "decision": "blocked",
                "blockers": ["shadow parity blocked"]
            },
            "cutover_evidence": {
                "eligible": false,
                "storage_recovery_required": true,
                "storage_recovery_ready": false,
                "storage_recovery_blocker_codes": ["wal_replay_unbounded"],
                "storage_recovery_blockers": [
                    "WAL replay was not opened with a configured entry bound"
                ],
                "background_maintenance_required": true,
                "background_maintenance_ready": false,
                "background_maintenance_blocker_codes": ["missing_evidence"],
                "background_maintenance_blockers": [
                    "background maintenance summary is missing"
                ],
                "replacement_readiness_min_per_million": 500_000,
                "replacement_readiness_invalid_family_count": 0,
                "replacement_readiness_blockers": [],
                "blockers": []
            },
            "replacement_readiness_per_million": 500_000,
            "replacement_readiness_by_query_family": []
        })
    }
}
