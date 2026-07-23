use skein::{Result, SkeinError};
use std::path::Path;

pub fn nowledge_previous_wrapper_preflight_check_usage() -> String {
    "nowledge-previous-wrapper-preflight-check requires [--require-ready] --wrapper-identity <id> (--bundle-dir <dir> | --contract-evidence-json <path> --adapter-smoke-json <path> --migration-gate-json <path> --replacement-summary-json <path>)".to_string()
}

#[derive(Debug, Clone, Default)]
struct PreviousWrapperPreflightCheckInputs {
    wrapper_identity: Option<String>,
    bundle_dir: Option<String>,
    contract_evidence: Option<serde_json::Value>,
    adapter_smoke: Option<serde_json::Value>,
    migration_gate: Option<serde_json::Value>,
    replacement_summary: Option<serde_json::Value>,
}

pub fn run_nowledge_previous_wrapper_preflight_check(
    mut args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let mut require_ready = false;
    let mut inputs = PreviousWrapperPreflightCheckInputs::default();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--require-ready" => {
                require_ready = true;
            }
            "--wrapper-identity" => {
                let value = args.next().ok_or_else(|| {
                    SkeinError::Semantic(nowledge_previous_wrapper_preflight_check_usage())
                })?;
                if value.trim().is_empty() {
                    return Err(SkeinError::Semantic(
                        "--wrapper-identity must not be empty".to_string(),
                    ));
                }
                inputs.wrapper_identity = Some(value);
            }
            "--bundle-dir" => {
                let value = args.next().ok_or_else(|| {
                    SkeinError::Semantic(nowledge_previous_wrapper_preflight_check_usage())
                })?;
                if value.trim().is_empty() {
                    return Err(SkeinError::Semantic(
                        "--bundle-dir must not be empty".to_string(),
                    ));
                }
                inputs.bundle_dir = Some(value);
            }
            "--contract-evidence-json" => {
                inputs.contract_evidence = Some(read_json_arg(&mut args)?);
            }
            "--adapter-smoke-json" => {
                inputs.adapter_smoke = Some(read_json_arg(&mut args)?);
            }
            "--migration-gate-json" => {
                inputs.migration_gate = Some(read_json_arg(&mut args)?);
            }
            "--replacement-summary-json" => {
                inputs.replacement_summary = Some(read_json_arg(&mut args)?);
            }
            _ => {
                return Err(SkeinError::Semantic(
                    nowledge_previous_wrapper_preflight_check_usage(),
                ));
            }
        }
    }
    fill_bundle_dir_inputs(&mut inputs)?;
    let report = nowledge_previous_wrapper_preflight_check_json(inputs)?;
    Ok((report, require_ready))
}

fn fill_bundle_dir_inputs(inputs: &mut PreviousWrapperPreflightCheckInputs) -> Result<()> {
    let Some(bundle_dir) = inputs.bundle_dir.as_deref() else {
        return Ok(());
    };
    let bundle_dir = Path::new(bundle_dir);
    if inputs.contract_evidence.is_none() {
        inputs.contract_evidence =
            Some(read_json_file(&bundle_dir.join("contract-evidence.json"))?);
    }
    if inputs.adapter_smoke.is_none() {
        inputs.adapter_smoke = Some(read_json_file(&bundle_dir.join("adapter-smoke.json"))?);
    }
    if inputs.migration_gate.is_none() {
        inputs.migration_gate = Some(read_json_file(&bundle_dir.join("migration-gate.json"))?);
    }
    if inputs.replacement_summary.is_none() {
        inputs.replacement_summary = Some(read_json_file(
            &bundle_dir.join("replacement-summary.json"),
        )?);
    }
    Ok(())
}

fn read_json_arg(args: &mut impl Iterator<Item = String>) -> Result<serde_json::Value> {
    let path = args
        .next()
        .ok_or_else(|| SkeinError::Semantic(nowledge_previous_wrapper_preflight_check_usage()))?;
    read_json_file(Path::new(&path))
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let raw = std::fs::read_to_string(path).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to read previous-wrapper preflight JSON '{}': {error}",
            path.display()
        ))
    })?;
    serde_json::from_str(&raw).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to parse previous-wrapper preflight JSON '{}': {error}",
            path.display()
        ))
    })
}

fn nowledge_previous_wrapper_preflight_check_json(
    inputs: PreviousWrapperPreflightCheckInputs,
) -> Result<serde_json::Value> {
    let wrapper_identity = inputs
        .wrapper_identity
        .ok_or_else(|| SkeinError::Semantic(nowledge_previous_wrapper_preflight_check_usage()))?;
    let contract_evidence = inputs
        .contract_evidence
        .ok_or_else(|| SkeinError::Semantic(nowledge_previous_wrapper_preflight_check_usage()))?;
    let adapter_smoke = inputs
        .adapter_smoke
        .ok_or_else(|| SkeinError::Semantic(nowledge_previous_wrapper_preflight_check_usage()))?;
    let migration_gate = inputs
        .migration_gate
        .ok_or_else(|| SkeinError::Semantic(nowledge_previous_wrapper_preflight_check_usage()))?;
    let replacement_summary = inputs
        .replacement_summary
        .ok_or_else(|| SkeinError::Semantic(nowledge_previous_wrapper_preflight_check_usage()))?;

    let checks = vec![
        preflight_check(
            "full_contract",
            [
                bool_path(&contract_evidence, &["required_contract_ready"]) == Some(true),
                bool_path(&contract_evidence, &["full_contract_checked"]) == Some(true),
                bool_path(&contract_evidence, &["full_contract_ready"]) == Some(true),
                full_contract_check_count_ready(&contract_evidence),
                selected_checks_match_check_count(&contract_evidence),
                bool_path(
                    &contract_evidence,
                    &["previous_wrapper_contract_evidence", "ready"],
                ) == Some(true),
                str_path(
                    &contract_evidence,
                    &["previous_wrapper_contract_evidence", "wrapper_identity"],
                ) == Some(wrapper_identity.as_str()),
            ],
            [
                "required_contract_ready",
                "full_contract_checked",
                "full_contract_ready",
                "check_count",
                "selected_checks",
                "previous_wrapper_contract_evidence.ready",
                "previous_wrapper_contract_evidence.wrapper_identity",
            ],
            blocker_codes(
                &contract_evidence,
                &[
                    &["required_contract_blocker_codes"][..],
                    &["previous_wrapper_contract_evidence", "blocker_codes"][..],
                ],
            ),
        ),
        preflight_check(
            "adapter_smoke",
            [
                bool_path(&adapter_smoke, &["adapter_smoke_ready"]) == Some(true),
                str_path(&adapter_smoke, &["engine_kind"]) == Some("previous_wrapper"),
                str_path(&adapter_smoke, &["wrapper_identity"]) == Some(wrapper_identity.as_str()),
                u64_path(&adapter_smoke, &["primary_only_checks"]) == Some(0),
                bool_path(&adapter_smoke, &["dual_engine_evidence", "ready"]) == Some(true),
                dual_engine_primary_count_ready(&adapter_smoke, &["dual_engine_evidence"]),
                dual_engine_primary_shadow_counts_match(&adapter_smoke, &["dual_engine_evidence"]),
                dual_engine_matched_shadow_counts_match(&adapter_smoke, &["dual_engine_evidence"]),
                dual_engine_primary_only_count_clear(&adapter_smoke, &["dual_engine_evidence"]),
            ],
            [
                "adapter_smoke_ready",
                "engine_kind",
                "wrapper_identity",
                "primary_only_checks",
                "dual_engine_evidence.ready",
                "dual_engine_evidence.primary_check_count",
                "dual_engine_evidence.shadow_check_count",
                "dual_engine_evidence.matched_check_count",
                "dual_engine_evidence.primary_only_check_count",
            ],
            blocker_codes(&adapter_smoke, &[&["blocker_codes"][..]]),
        ),
        preflight_check(
            "migration_gate",
            [
                str_path(&migration_gate, &["migration_gate", "decision"]) == Some("ready"),
                str_path(&migration_gate, &["cutover", "decision"]) == Some("ready"),
                bool_path(&migration_gate, &["cutover_evidence", "eligible"]) == Some(true),
                str_path(&migration_gate, &["cutover_evidence", "ready_engine_kind"])
                    == Some("previous_wrapper"),
                str_path(
                    &migration_gate,
                    &["cutover_evidence", "ready_wrapper_identity"],
                ) == Some(wrapper_identity.as_str()),
                bool_path(
                    &migration_gate,
                    &["previous_wrapper_contract_evidence", "ready"],
                ) == Some(true),
                u64_path(&migration_gate, &["replacement_readiness_per_million"])
                    == Some(1_000_000),
            ],
            [
                "migration_gate.decision",
                "cutover.decision",
                "cutover_evidence.eligible",
                "cutover_evidence.ready_engine_kind",
                "cutover_evidence.ready_wrapper_identity",
                "previous_wrapper_contract_evidence.ready",
                "replacement_readiness_per_million",
            ],
            blocker_codes(
                &migration_gate,
                &[
                    &["migration_gate", "blockers"][..],
                    &["cutover", "blockers"][..],
                    &["cutover_evidence", "blockers"][..],
                ],
            ),
        ),
        preflight_check(
            "storage_recovery",
            [
                bool_path(
                    &migration_gate,
                    &["cutover_evidence", "storage_recovery_required"],
                ) == Some(true),
                bool_path(
                    &migration_gate,
                    &["cutover_evidence", "storage_recovery_present"],
                ) == Some(true),
                bool_path(
                    &migration_gate,
                    &["cutover_evidence", "storage_recovery_ready"],
                ) == Some(true),
                bool_path(
                    &migration_gate,
                    &["cutover_evidence", "storage_recovery_protocol_matches"],
                ) == Some(true),
            ],
            [
                "cutover_evidence.storage_recovery_required",
                "cutover_evidence.storage_recovery_present",
                "cutover_evidence.storage_recovery_ready",
                "cutover_evidence.storage_recovery_protocol_matches",
            ],
            blocker_codes(
                &migration_gate,
                &[
                    &["cutover_evidence", "storage_recovery_blocker_codes"][..],
                    &["cutover_evidence", "storage_recovery_blockers"][..],
                ],
            ),
        ),
        preflight_check(
            "background_maintenance",
            [
                bool_path(
                    &migration_gate,
                    &["cutover_evidence", "background_maintenance_required"],
                ) == Some(true),
                bool_path(
                    &migration_gate,
                    &["cutover_evidence", "background_maintenance_present"],
                ) == Some(true),
                bool_path(
                    &migration_gate,
                    &["cutover_evidence", "background_maintenance_ready"],
                ) == Some(true),
                bool_path(
                    &migration_gate,
                    &[
                        "cutover_evidence",
                        "background_maintenance_protocol_matches",
                    ],
                ) == Some(true),
            ],
            [
                "cutover_evidence.background_maintenance_required",
                "cutover_evidence.background_maintenance_present",
                "cutover_evidence.background_maintenance_ready",
                "cutover_evidence.background_maintenance_protocol_matches",
            ],
            blocker_codes(
                &migration_gate,
                &[
                    &["cutover_evidence", "background_maintenance_blocker_codes"][..],
                    &["cutover_evidence", "background_maintenance_blockers"][..],
                ],
            ),
        ),
        preflight_check(
            "replacement_summary",
            [
                bool_path(&replacement_summary, &["production_cutover_ready"]) == Some(true),
                u64_path(
                    &replacement_summary,
                    &["production_replacement_per_million"],
                ) == Some(1_000_000),
                empty_array_path(&replacement_summary, &["blocking_categories"]),
                empty_array_path(&replacement_summary, &["missing_evidence"]),
                empty_array_path(&replacement_summary, &["next_actions"]),
                bool_path(&replacement_summary, &["shadow_evidence", "ready"]) == Some(true),
                str_path(
                    &replacement_summary,
                    &["shadow_evidence", "ready_wrapper_identity"],
                ) == Some(wrapper_identity.as_str()),
                str_path(
                    &replacement_summary,
                    &["shadow_evidence", "contract_wrapper_identity"],
                ) == Some(wrapper_identity.as_str()),
                str_path(
                    &replacement_summary,
                    &["shadow_evidence", "cutover_ready_wrapper_identity"],
                ) == Some(wrapper_identity.as_str()),
                bool_path(&replacement_summary, &["dual_engine_evidence", "present"]) == Some(true),
                bool_path(&replacement_summary, &["dual_engine_evidence", "ready"]) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["dual_engine_evidence", "consistent"],
                ) == Some(true),
                dual_engine_primary_count_ready(&replacement_summary, &["dual_engine_evidence"]),
                dual_engine_primary_shadow_counts_match(
                    &replacement_summary,
                    &["dual_engine_evidence"],
                ),
                dual_engine_matched_shadow_counts_match(
                    &replacement_summary,
                    &["dual_engine_evidence"],
                ),
                dual_engine_primary_only_count_clear(
                    &replacement_summary,
                    &["dual_engine_evidence"],
                ),
                bool_path(
                    &replacement_summary,
                    &["search_projection_evidence", "present"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_evidence", "ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_evidence", "derived_projection"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_evidence", "all_tables_covered"],
                ) == Some(true),
                search_projection_table_counts_match(&replacement_summary),
                bool_path(
                    &replacement_summary,
                    &["search_projection_evidence", "fts_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_evidence", "vector_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_evidence", "embedding_identity_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_evidence", "fail_soft_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_evidence", "rebuild_marker_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_evidence", "metadata_repair_marker_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_evidence", "incremental_update_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_evidence", "source_chunk_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_evidence", "predicate_pushdown_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_shadow_evidence", "present"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_shadow_evidence", "ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_shadow_evidence", "primary_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_shadow_evidence", "shadow_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_shadow_evidence", "document_count_parity"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_shadow_evidence", "table_parity_ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &[
                        "search_projection_shadow_evidence",
                        "embedding_identity_parity",
                    ],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_projection_shadow_evidence", "lifecycle_parity"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &[
                        "search_projection_shadow_evidence",
                        "incremental_watermark_parity",
                    ],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_candidate_shadow_evidence", "present"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_candidate_shadow_evidence", "ready"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &["search_candidate_shadow_evidence", "row_count_parity"],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &[
                        "search_candidate_shadow_evidence",
                        "vector_top_k_overlap_ready",
                    ],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &[
                        "search_candidate_shadow_evidence",
                        "fts_top_k_overlap_ready",
                    ],
                ) == Some(true),
                bool_path(
                    &replacement_summary,
                    &[
                        "search_candidate_shadow_evidence",
                        "shadow_scan_filter_pushdown_ready",
                    ],
                ) == Some(true),
            ],
            [
                "production_cutover_ready",
                "production_replacement_per_million",
                "blocking_categories",
                "missing_evidence",
                "next_actions",
                "shadow_evidence.ready",
                "shadow_evidence.ready_wrapper_identity",
                "shadow_evidence.contract_wrapper_identity",
                "shadow_evidence.cutover_ready_wrapper_identity",
                "dual_engine_evidence.present",
                "dual_engine_evidence.ready",
                "dual_engine_evidence.consistent",
                "dual_engine_evidence.primary_check_count",
                "dual_engine_evidence.shadow_check_count",
                "dual_engine_evidence.matched_check_count",
                "dual_engine_evidence.primary_only_check_count",
                "search_projection_evidence.present",
                "search_projection_evidence.ready",
                "search_projection_evidence.derived_projection",
                "search_projection_evidence.all_tables_covered",
                "search_projection_evidence.covered_table_count",
                "search_projection_evidence.fts_ready",
                "search_projection_evidence.vector_ready",
                "search_projection_evidence.embedding_identity_ready",
                "search_projection_evidence.fail_soft_ready",
                "search_projection_evidence.rebuild_marker_ready",
                "search_projection_evidence.metadata_repair_marker_ready",
                "search_projection_evidence.incremental_update_ready",
                "search_projection_evidence.source_chunk_ready",
                "search_projection_evidence.predicate_pushdown_ready",
                "search_projection_shadow_evidence.present",
                "search_projection_shadow_evidence.ready",
                "search_projection_shadow_evidence.primary_ready",
                "search_projection_shadow_evidence.shadow_ready",
                "search_projection_shadow_evidence.document_count_parity",
                "search_projection_shadow_evidence.table_parity_ready",
                "search_projection_shadow_evidence.embedding_identity_parity",
                "search_projection_shadow_evidence.lifecycle_parity",
                "search_projection_shadow_evidence.incremental_watermark_parity",
                "search_candidate_shadow_evidence.present",
                "search_candidate_shadow_evidence.ready",
                "search_candidate_shadow_evidence.row_count_parity",
                "search_candidate_shadow_evidence.vector_top_k_overlap_ready",
                "search_candidate_shadow_evidence.fts_top_k_overlap_ready",
                "search_candidate_shadow_evidence.shadow_scan_filter_pushdown_ready",
            ],
            blocker_codes(
                &replacement_summary,
                &[
                    &["blocking_categories"][..],
                    &["missing_evidence"][..],
                    &["next_actions"][..],
                    &["search_projection_evidence", "blocker_codes"][..],
                    &["search_projection_shadow_evidence", "blocker_codes"][..],
                    &["search_candidate_shadow_evidence", "blocker_codes"][..],
                ],
            ),
        ),
    ];
    let ready = checks.iter().all(|check| {
        check
            .get("ready")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    });
    let failed_checks = checks
        .iter()
        .filter(|check| check.get("ready").and_then(serde_json::Value::as_bool) != Some(true))
        .filter_map(|check| check.get("name").and_then(serde_json::Value::as_str))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let release_summary = previous_wrapper_preflight_release_summary(
        &wrapper_identity,
        &contract_evidence,
        &adapter_smoke,
        &migration_gate,
        &replacement_summary,
    );

    Ok(serde_json::json!({
        "protocol": "skein-nowledge-previous-wrapper-preflight-check",
        "ready": ready,
        "wrapper_identity": wrapper_identity,
        "failed_checks": failed_checks,
        "release_summary": release_summary,
        "checks": checks,
    }))
}

fn previous_wrapper_preflight_release_summary(
    wrapper_identity: &str,
    contract_evidence: &serde_json::Value,
    adapter_smoke: &serde_json::Value,
    migration_gate: &serde_json::Value,
    replacement_summary: &serde_json::Value,
) -> serde_json::Value {
    let mut summary = serde_json::Map::new();
    insert_json_value(&mut summary, "wrapper_identity", wrapper_identity);
    insert_json_value(
        &mut summary,
        "required_contract_ready",
        bool_path(contract_evidence, &["required_contract_ready"]),
    );
    insert_json_value(
        &mut summary,
        "full_contract_checked",
        bool_path(contract_evidence, &["full_contract_checked"]),
    );
    insert_json_value(
        &mut summary,
        "full_contract_ready",
        bool_path(contract_evidence, &["full_contract_ready"]),
    );
    insert_json_value(
        &mut summary,
        "selected_checks",
        u64_path(contract_evidence, &["selected_checks"]),
    );
    insert_json_value(
        &mut summary,
        "check_count",
        u64_path(contract_evidence, &["check_count"]),
    );
    insert_json_value(
        &mut summary,
        "adapter_smoke_ready",
        bool_path(adapter_smoke, &["adapter_smoke_ready"]),
    );
    insert_json_value(
        &mut summary,
        "adapter_request_count",
        u64_path(adapter_smoke, &["request_count"]),
    );
    insert_json_value(
        &mut summary,
        "adapter_primary_only_checks",
        u64_path(adapter_smoke, &["primary_only_checks"]),
    );
    insert_json_value(
        &mut summary,
        "adapter_dual_engine_ready",
        bool_path(adapter_smoke, &["dual_engine_evidence", "ready"]),
    );
    insert_json_value(
        &mut summary,
        "adapter_dual_engine_counts_consistent",
        dual_engine_count_evidence_consistent(adapter_smoke, &["dual_engine_evidence"]),
    );
    insert_json_value(
        &mut summary,
        "adapter_dual_engine_primary_check_count",
        u64_path(
            adapter_smoke,
            &["dual_engine_evidence", "primary_check_count"],
        ),
    );
    insert_json_value(
        &mut summary,
        "adapter_dual_engine_shadow_check_count",
        u64_path(
            adapter_smoke,
            &["dual_engine_evidence", "shadow_check_count"],
        ),
    );
    insert_json_value(
        &mut summary,
        "adapter_dual_engine_matched_check_count",
        u64_path(
            adapter_smoke,
            &["dual_engine_evidence", "matched_check_count"],
        ),
    );
    insert_json_value(
        &mut summary,
        "adapter_dual_engine_primary_only_check_count",
        u64_path(
            adapter_smoke,
            &["dual_engine_evidence", "primary_only_check_count"],
        ),
    );
    insert_json_value(
        &mut summary,
        "migration_gate_decision",
        str_path(migration_gate, &["migration_gate", "decision"]),
    );
    insert_json_value(
        &mut summary,
        "cutover_decision",
        str_path(migration_gate, &["cutover", "decision"]),
    );
    insert_json_value(
        &mut summary,
        "cutover_eligible",
        bool_path(migration_gate, &["cutover_evidence", "eligible"]),
    );
    insert_json_value(
        &mut summary,
        "ready_engine_kind",
        str_path(migration_gate, &["cutover_evidence", "ready_engine_kind"]),
    );
    insert_json_value(
        &mut summary,
        "ready_wrapper_identity",
        str_path(
            migration_gate,
            &["cutover_evidence", "ready_wrapper_identity"],
        ),
    );
    insert_json_value(
        &mut summary,
        "replacement_readiness_per_million",
        u64_path(migration_gate, &["replacement_readiness_per_million"])
            .or_else(|| u64_path(replacement_summary, &["replacement_readiness_per_million"])),
    );
    insert_json_value(
        &mut summary,
        "production_cutover_ready",
        bool_path(replacement_summary, &["production_cutover_ready"]),
    );
    insert_json_value(
        &mut summary,
        "production_replacement_per_million",
        u64_path(replacement_summary, &["production_replacement_per_million"]),
    );
    insert_json_value(
        &mut summary,
        "shadow_evidence_ready",
        bool_path(replacement_summary, &["shadow_evidence", "ready"]),
    );
    insert_json_value(
        &mut summary,
        "shadow_evidence_ready_wrapper_identity",
        str_path(
            replacement_summary,
            &["shadow_evidence", "ready_wrapper_identity"],
        ),
    );
    insert_json_value(
        &mut summary,
        "shadow_evidence_contract_wrapper_identity",
        str_path(
            replacement_summary,
            &["shadow_evidence", "contract_wrapper_identity"],
        ),
    );
    insert_json_value(
        &mut summary,
        "shadow_evidence_cutover_ready_wrapper_identity",
        str_path(
            replacement_summary,
            &["shadow_evidence", "cutover_ready_wrapper_identity"],
        ),
    );
    insert_json_value(
        &mut summary,
        "storage_recovery_ready",
        bool_path(
            migration_gate,
            &["cutover_evidence", "storage_recovery_ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "background_maintenance_ready",
        bool_path(
            migration_gate,
            &["cutover_evidence", "background_maintenance_ready"],
        ),
    );
    for (field, path) in [
        (
            "background_maintenance_executable_search_projection_graph_delta_count",
            "background_maintenance_executable_search_projection_graph_delta_count",
        ),
        (
            "background_maintenance_admitted_search_projection_graph_delta_count",
            "background_maintenance_admitted_search_projection_graph_delta_count",
        ),
        (
            "background_maintenance_deferred_search_projection_graph_delta_count",
            "background_maintenance_deferred_search_projection_graph_delta_count",
        ),
        (
            "background_maintenance_rejected_search_projection_graph_delta_count",
            "background_maintenance_rejected_search_projection_graph_delta_count",
        ),
        (
            "background_maintenance_executable_search_projection_graph_delta_operations",
            "background_maintenance_executable_search_projection_graph_delta_operations",
        ),
        (
            "background_maintenance_admitted_search_projection_graph_delta_operations",
            "background_maintenance_admitted_search_projection_graph_delta_operations",
        ),
        (
            "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch",
            "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch",
        ),
    ] {
        insert_json_value(
            &mut summary,
            field,
            u64_path(replacement_summary, &["cutover_evidence", path]),
        );
    }
    insert_json_value(
        &mut summary,
        "dual_engine_evidence_present",
        bool_path(replacement_summary, &["dual_engine_evidence", "present"]),
    );
    insert_json_value(
        &mut summary,
        "dual_engine_evidence_ready",
        bool_path(replacement_summary, &["dual_engine_evidence", "ready"]),
    );
    insert_json_value(
        &mut summary,
        "dual_engine_evidence_consistent",
        bool_path(replacement_summary, &["dual_engine_evidence", "consistent"]).or(Some(
            dual_engine_count_evidence_consistent(replacement_summary, &["dual_engine_evidence"]),
        )),
    );
    for (field, path) in [
        ("dual_engine_primary_check_count", "primary_check_count"),
        ("dual_engine_shadow_check_count", "shadow_check_count"),
        ("dual_engine_matched_check_count", "matched_check_count"),
        (
            "dual_engine_primary_only_check_count",
            "primary_only_check_count",
        ),
        ("dual_engine_matched_per_million", "matched_per_million"),
    ] {
        insert_json_value(
            &mut summary,
            field,
            u64_path(replacement_summary, &["dual_engine_evidence", path]),
        );
    }
    insert_json_value(
        &mut summary,
        "search_projection_evidence_ready",
        bool_path(
            replacement_summary,
            &["search_projection_evidence", "ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_all_tables_covered",
        bool_path(
            replacement_summary,
            &["search_projection_evidence", "all_tables_covered"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_covered_table_count",
        u64_path(
            replacement_summary,
            &["search_projection_evidence", "covered_table_count"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_required_table_count",
        u64_path(
            replacement_summary,
            &["search_projection_evidence", "required_table_count"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_fts_ready",
        bool_path(
            replacement_summary,
            &["search_projection_evidence", "fts_ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_vector_ready",
        bool_path(
            replacement_summary,
            &["search_projection_evidence", "vector_ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_embedding_identity_ready",
        bool_path(
            replacement_summary,
            &["search_projection_evidence", "embedding_identity_ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_fail_soft_ready",
        bool_path(
            replacement_summary,
            &["search_projection_evidence", "fail_soft_ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_incremental_update_ready",
        bool_path(
            replacement_summary,
            &["search_projection_evidence", "incremental_update_ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_predicate_pushdown_ready",
        bool_path(
            replacement_summary,
            &["search_projection_evidence", "predicate_pushdown_ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_shadow_evidence_ready",
        bool_path(
            replacement_summary,
            &["search_projection_shadow_evidence", "ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_shadow_primary_ready",
        bool_path(
            replacement_summary,
            &["search_projection_shadow_evidence", "primary_ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_shadow_shadow_ready",
        bool_path(
            replacement_summary,
            &["search_projection_shadow_evidence", "shadow_ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_shadow_document_count_parity",
        bool_path(
            replacement_summary,
            &["search_projection_shadow_evidence", "document_count_parity"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_shadow_table_parity_ready",
        bool_path(
            replacement_summary,
            &["search_projection_shadow_evidence", "table_parity_ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_shadow_embedding_identity_parity",
        bool_path(
            replacement_summary,
            &[
                "search_projection_shadow_evidence",
                "embedding_identity_parity",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_shadow_lifecycle_parity",
        bool_path(
            replacement_summary,
            &["search_projection_shadow_evidence", "lifecycle_parity"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_projection_shadow_incremental_watermark_parity",
        bool_path(
            replacement_summary,
            &[
                "search_projection_shadow_evidence",
                "incremental_watermark_parity",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_candidate_shadow_evidence_ready",
        bool_path(
            replacement_summary,
            &["search_candidate_shadow_evidence", "ready"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_candidate_shadow_row_count_parity",
        bool_path(
            replacement_summary,
            &["search_candidate_shadow_evidence", "row_count_parity"],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_candidate_shadow_vector_top_k_overlap_ready",
        bool_path(
            replacement_summary,
            &[
                "search_candidate_shadow_evidence",
                "vector_top_k_overlap_ready",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_candidate_shadow_fts_top_k_overlap_ready",
        bool_path(
            replacement_summary,
            &[
                "search_candidate_shadow_evidence",
                "fts_top_k_overlap_ready",
            ],
        ),
    );
    insert_json_value(
        &mut summary,
        "search_candidate_shadow_scan_filter_pushdown_ready",
        bool_path(
            replacement_summary,
            &[
                "search_candidate_shadow_evidence",
                "shadow_scan_filter_pushdown_ready",
            ],
        ),
    );
    serde_json::Value::Object(summary)
}

fn insert_json_value<T: serde::Serialize>(
    object: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: T,
) {
    object.insert(key.to_string(), serde_json::json!(value));
}

fn preflight_check(
    name: &str,
    conditions: impl IntoIterator<Item = bool>,
    evidence_fields: impl IntoIterator<Item = &'static str>,
    blocker_codes: Vec<String>,
) -> serde_json::Value {
    let conditions = conditions.into_iter().collect::<Vec<_>>();
    let evidence_fields = evidence_fields.into_iter().collect::<Vec<_>>();
    let failed_evidence_fields = conditions
        .iter()
        .zip(evidence_fields.iter())
        .filter_map(|(condition, field)| (!*condition).then_some(*field))
        .collect::<Vec<_>>();
    let ready = failed_evidence_fields.is_empty();
    serde_json::json!({
        "name": name,
        "ready": ready,
        "evidence_fields": evidence_fields,
        "failed_evidence_fields": failed_evidence_fields,
        "blocker_codes": blocker_codes,
    })
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

fn full_contract_check_count_ready(value: &serde_json::Value) -> bool {
    u64_path(value, &["check_count"]).is_some_and(|count| count > 0)
}

fn selected_checks_match_check_count(value: &serde_json::Value) -> bool {
    let Some(selected_checks) = u64_path(value, &["selected_checks"]) else {
        return false;
    };
    let Some(check_count) = u64_path(value, &["check_count"]) else {
        return false;
    };
    check_count > 0 && selected_checks == check_count
}

fn dual_engine_primary_count_ready(value: &serde_json::Value, path: &[&str]) -> bool {
    dual_engine_u64_path(value, path, "primary_check_count").is_some_and(|count| count > 0)
}

fn dual_engine_primary_shadow_counts_match(value: &serde_json::Value, path: &[&str]) -> bool {
    let Some(primary_check_count) = dual_engine_u64_path(value, path, "primary_check_count") else {
        return false;
    };
    let Some(shadow_check_count) = dual_engine_u64_path(value, path, "shadow_check_count") else {
        return false;
    };
    primary_check_count > 0 && primary_check_count == shadow_check_count
}

fn dual_engine_matched_shadow_counts_match(value: &serde_json::Value, path: &[&str]) -> bool {
    let Some(matched_check_count) = dual_engine_u64_path(value, path, "matched_check_count") else {
        return false;
    };
    let Some(shadow_check_count) = dual_engine_u64_path(value, path, "shadow_check_count") else {
        return false;
    };
    shadow_check_count > 0 && matched_check_count == shadow_check_count
}

fn dual_engine_primary_only_count_clear(value: &serde_json::Value, path: &[&str]) -> bool {
    dual_engine_u64_path(value, path, "primary_only_check_count") == Some(0)
}

fn dual_engine_count_evidence_consistent(value: &serde_json::Value, path: &[&str]) -> bool {
    dual_engine_primary_count_ready(value, path)
        && dual_engine_primary_shadow_counts_match(value, path)
        && dual_engine_matched_shadow_counts_match(value, path)
        && dual_engine_primary_only_count_clear(value, path)
}

fn search_projection_table_counts_match(value: &serde_json::Value) -> bool {
    let Some(covered_table_count) = u64_path(
        value,
        &["search_projection_evidence", "covered_table_count"],
    ) else {
        return false;
    };
    let Some(required_table_count) = u64_path(
        value,
        &["search_projection_evidence", "required_table_count"],
    ) else {
        return false;
    };
    required_table_count > 0 && covered_table_count == required_table_count
}

fn dual_engine_u64_path(value: &serde_json::Value, path: &[&str], field: &str) -> Option<u64> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    current.get(field).and_then(serde_json::Value::as_u64)
}

fn empty_array_path(value: &serde_json::Value, path: &[&str]) -> bool {
    value_path(value, path)
        .and_then(serde_json::Value::as_array)
        .is_some_and(Vec::is_empty)
}

fn blocker_codes(value: &serde_json::Value, paths: &[&[&str]]) -> Vec<String> {
    let mut codes = Vec::new();
    for path in paths {
        if let Some(items) = value_path(value, path).and_then(serde_json::Value::as_array) {
            for item in items {
                if let Some(code) = item.as_str() {
                    codes.push(code.to_string());
                } else if let Some(action) = item.get("action").and_then(serde_json::Value::as_str)
                {
                    codes.push(action.to_string());
                }
            }
        }
    }
    codes.sort();
    codes.dedup();
    codes
}

#[cfg(test)]
fn check_by_name<'a>(report: &'a serde_json::Value, name: &str) -> &'a serde_json::Value {
    report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["name"] == name)
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::{
        check_by_name, nowledge_previous_wrapper_preflight_check_json,
        run_nowledge_previous_wrapper_preflight_check, PreviousWrapperPreflightCheckInputs,
    };
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn preflight_check_reports_ready_when_all_artifacts_are_ready() {
        let report = nowledge_previous_wrapper_preflight_check_json(ready_inputs()).unwrap();

        assert_eq!(report["ready"], true);
        assert_eq!(report["failed_checks"], serde_json::json!([]));
        assert_eq!(report["wrapper_identity"], "nowledge-previous-wrapper:test");
        let summary = &report["release_summary"];
        assert_release_summary_field(
            summary,
            "wrapper_identity",
            "nowledge-previous-wrapper:test",
        );
        assert_release_summary_field(summary, "required_contract_ready", true);
        assert_release_summary_field(summary, "full_contract_checked", true);
        assert_release_summary_field(summary, "full_contract_ready", true);
        assert_release_summary_field(summary, "selected_checks", 2);
        assert_release_summary_field(summary, "check_count", 2);
        assert_release_summary_field(summary, "adapter_smoke_ready", true);
        assert_release_summary_field(summary, "adapter_dual_engine_counts_consistent", true);
        assert_release_summary_field(summary, "migration_gate_decision", "ready");
        assert_release_summary_field(summary, "cutover_decision", "ready");
        assert_release_summary_field(summary, "ready_engine_kind", "previous_wrapper");
        assert_release_summary_field(
            summary,
            "ready_wrapper_identity",
            "nowledge-previous-wrapper:test",
        );
        assert_release_summary_field(summary, "production_cutover_ready", true);
        assert_release_summary_field(summary, "production_replacement_per_million", 1_000_000);
        assert_release_summary_field(summary, "shadow_evidence_ready", true);
        assert_release_summary_field(
            summary,
            "shadow_evidence_ready_wrapper_identity",
            "nowledge-previous-wrapper:test",
        );
        assert_release_summary_field(
            summary,
            "shadow_evidence_contract_wrapper_identity",
            "nowledge-previous-wrapper:test",
        );
        assert_release_summary_field(
            summary,
            "shadow_evidence_cutover_ready_wrapper_identity",
            "nowledge-previous-wrapper:test",
        );
        assert_release_summary_field(summary, "storage_recovery_ready", true);
        assert_release_summary_field(summary, "background_maintenance_ready", true);
        assert_release_summary_field(
            summary,
            "background_maintenance_executable_search_projection_graph_delta_count",
            2,
        );
        assert_release_summary_field(
            summary,
            "background_maintenance_admitted_search_projection_graph_delta_count",
            1,
        );
        assert_release_summary_field(summary, "dual_engine_evidence_present", true);
        assert_release_summary_field(summary, "dual_engine_evidence_ready", true);
        assert_release_summary_field(summary, "dual_engine_evidence_consistent", true);
        assert_release_summary_field(summary, "dual_engine_primary_check_count", 2);
        assert_release_summary_field(summary, "dual_engine_shadow_check_count", 2);
        assert_release_summary_field(summary, "dual_engine_matched_check_count", 2);
        assert_release_summary_field(summary, "dual_engine_primary_only_check_count", 0);
        assert_release_summary_field(summary, "dual_engine_matched_per_million", 1_000_000);
        assert_release_summary_field(summary, "search_projection_evidence_ready", true);
        assert_release_summary_field(summary, "search_projection_all_tables_covered", true);
        assert_release_summary_field(summary, "search_projection_covered_table_count", 6);
        assert_release_summary_field(summary, "search_projection_required_table_count", 6);
        assert_release_summary_field(summary, "search_projection_fts_ready", true);
        assert_release_summary_field(summary, "search_projection_vector_ready", true);
        assert_release_summary_field(summary, "search_projection_embedding_identity_ready", true);
        assert_release_summary_field(summary, "search_projection_fail_soft_ready", true);
        assert_release_summary_field(summary, "search_projection_incremental_update_ready", true);
        assert_release_summary_field(summary, "search_projection_predicate_pushdown_ready", true);
        assert_release_summary_field(summary, "search_projection_shadow_evidence_ready", true);
        assert_release_summary_field(summary, "search_projection_shadow_primary_ready", true);
        assert_release_summary_field(summary, "search_projection_shadow_shadow_ready", true);
        assert_release_summary_field(
            summary,
            "search_projection_shadow_document_count_parity",
            true,
        );
        assert_release_summary_field(summary, "search_projection_shadow_table_parity_ready", true);
        assert_release_summary_field(
            summary,
            "search_projection_shadow_embedding_identity_parity",
            true,
        );
        assert_release_summary_field(summary, "search_projection_shadow_lifecycle_parity", true);
        assert_release_summary_field(
            summary,
            "search_projection_shadow_incremental_watermark_parity",
            true,
        );
        assert_release_summary_field(summary, "search_candidate_shadow_evidence_ready", true);
        assert_release_summary_field(summary, "search_candidate_shadow_row_count_parity", true);
        assert_release_summary_field(
            summary,
            "search_candidate_shadow_vector_top_k_overlap_ready",
            true,
        );
        assert_release_summary_field(
            summary,
            "search_candidate_shadow_fts_top_k_overlap_ready",
            true,
        );
        assert_release_summary_field(
            summary,
            "search_candidate_shadow_scan_filter_pushdown_ready",
            true,
        );
        assert!(report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .all(|check| check["ready"] == true));
        assert!(report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .all(|check| check["failed_evidence_fields"] == serde_json::json!([])));
    }

    #[test]
    fn preflight_check_fails_closed_on_partial_evidence() {
        let mut inputs = ready_inputs();
        let replacement_summary = serde_json::json!({
            "production_cutover_ready": false,
            "production_replacement_per_million": 0,
            "blocking_categories": ["cutover_evidence"],
            "missing_evidence": [],
            "next_actions": [
                {
                    "action": "provide_eligible_cutover_evidence"
                }
            ]
        });
        inputs.replacement_summary = Some(replacement_summary);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["replacement_summary"])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["blocker_codes"],
            serde_json::json!(["cutover_evidence", "provide_eligible_cutover_evidence"])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["failed_evidence_fields"],
            serde_json::json!([
                "production_cutover_ready",
                "production_replacement_per_million",
                "blocking_categories",
                "next_actions",
                "shadow_evidence.ready",
                "shadow_evidence.ready_wrapper_identity",
                "shadow_evidence.contract_wrapper_identity",
                "shadow_evidence.cutover_ready_wrapper_identity",
                "dual_engine_evidence.present",
                "dual_engine_evidence.ready",
                "dual_engine_evidence.consistent",
                "dual_engine_evidence.primary_check_count",
                "dual_engine_evidence.shadow_check_count",
                "dual_engine_evidence.matched_check_count",
                "dual_engine_evidence.primary_only_check_count",
                "search_projection_evidence.present",
                "search_projection_evidence.ready",
                "search_projection_evidence.derived_projection",
                "search_projection_evidence.all_tables_covered",
                "search_projection_evidence.covered_table_count",
                "search_projection_evidence.fts_ready",
                "search_projection_evidence.vector_ready",
                "search_projection_evidence.embedding_identity_ready",
                "search_projection_evidence.fail_soft_ready",
                "search_projection_evidence.rebuild_marker_ready",
                "search_projection_evidence.metadata_repair_marker_ready",
                "search_projection_evidence.incremental_update_ready",
                "search_projection_evidence.source_chunk_ready",
                "search_projection_evidence.predicate_pushdown_ready",
                "search_projection_shadow_evidence.present",
                "search_projection_shadow_evidence.ready",
                "search_projection_shadow_evidence.primary_ready",
                "search_projection_shadow_evidence.shadow_ready",
                "search_projection_shadow_evidence.document_count_parity",
                "search_projection_shadow_evidence.table_parity_ready",
                "search_projection_shadow_evidence.embedding_identity_parity",
                "search_projection_shadow_evidence.lifecycle_parity",
                "search_projection_shadow_evidence.incremental_watermark_parity",
                "search_candidate_shadow_evidence.present",
                "search_candidate_shadow_evidence.ready",
                "search_candidate_shadow_evidence.row_count_parity",
                "search_candidate_shadow_evidence.vector_top_k_overlap_ready",
                "search_candidate_shadow_evidence.fts_top_k_overlap_ready",
                "search_candidate_shadow_evidence.shadow_scan_filter_pushdown_ready"
            ])
        );
    }

    #[test]
    fn preflight_check_requires_full_contract_evidence() {
        let mut inputs = ready_inputs();
        let contract_evidence = inputs.contract_evidence.as_mut().unwrap();
        contract_evidence["full_contract_checked"] = serde_json::json!(false);
        contract_evidence["full_contract_ready"] = serde_json::json!(false);
        contract_evidence["selected_checks"] = serde_json::json!(1);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["full_contract"])
        );
        assert_eq!(
            check_by_name(&report, "full_contract")["failed_evidence_fields"],
            serde_json::json!([
                "full_contract_checked",
                "full_contract_ready",
                "selected_checks"
            ])
        );
    }

    #[test]
    fn preflight_check_requires_summary_dual_engine_evidence() {
        let mut inputs = ready_inputs();
        inputs.replacement_summary = Some(serde_json::json!({
            "production_cutover_ready": true,
            "production_replacement_per_million": 1_000_000,
            "blocking_categories": [],
            "missing_evidence": [],
            "next_actions": [],
            "shadow_evidence": {
                "ready": true,
                "ready_wrapper_identity": "nowledge-previous-wrapper:test",
                "contract_wrapper_identity": "nowledge-previous-wrapper:test",
                "cutover_ready_wrapper_identity": "nowledge-previous-wrapper:test"
            },
            "dual_engine_evidence": {
                "present": true,
                "ready": false,
                "consistent": true,
                "primary_check_count": 2,
                "shadow_check_count": 2,
                "matched_check_count": 2,
                "primary_only_check_count": 0
            },
            "search_projection_evidence": ready_search_projection_evidence(),
            "search_projection_shadow_evidence": ready_search_projection_shadow_evidence(),
            "search_candidate_shadow_evidence": ready_search_candidate_shadow_evidence()
        }));

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["replacement_summary"])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["failed_evidence_fields"],
            serde_json::json!(["dual_engine_evidence.ready"])
        );
    }

    #[test]
    fn preflight_check_requires_summary_search_projection_evidence() {
        let mut inputs = ready_inputs();
        let replacement_summary = inputs.replacement_summary.as_mut().unwrap();
        replacement_summary["search_projection_evidence"]["ready"] = serde_json::json!(true);
        replacement_summary["search_projection_evidence"]["covered_table_count"] =
            serde_json::json!(5);
        replacement_summary["search_projection_evidence"]["source_chunk_ready"] =
            serde_json::json!(false);
        replacement_summary["search_projection_evidence"]["blocker_codes"] =
            serde_json::json!(["missing_source_chunks_index"]);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["replacement_summary"])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["failed_evidence_fields"],
            serde_json::json!([
                "search_projection_evidence.covered_table_count",
                "search_projection_evidence.source_chunk_ready"
            ])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["blocker_codes"],
            serde_json::json!(["missing_source_chunks_index"])
        );
    }

    #[test]
    fn preflight_check_requires_summary_search_projection_shadow_evidence() {
        let mut inputs = ready_inputs();
        let replacement_summary = inputs.replacement_summary.as_mut().unwrap();
        replacement_summary["search_projection_shadow_evidence"]["ready"] = serde_json::json!(true);
        replacement_summary["search_projection_shadow_evidence"]["table_parity_ready"] =
            serde_json::json!(false);
        replacement_summary["search_projection_shadow_evidence"]["incremental_watermark_parity"] =
            serde_json::json!(false);
        replacement_summary["search_projection_shadow_evidence"]["blocker_codes"] =
            serde_json::json!(["table_parity_mismatch", "incremental_watermark_mismatch"]);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["replacement_summary"])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["failed_evidence_fields"],
            serde_json::json!([
                "search_projection_shadow_evidence.table_parity_ready",
                "search_projection_shadow_evidence.incremental_watermark_parity"
            ])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["blocker_codes"],
            serde_json::json!(["incremental_watermark_mismatch", "table_parity_mismatch"])
        );
    }

    #[test]
    fn preflight_check_requires_summary_search_candidate_shadow_evidence() {
        let mut inputs = ready_inputs();
        let replacement_summary = inputs.replacement_summary.as_mut().unwrap();
        replacement_summary["search_candidate_shadow_evidence"]["ready"] = serde_json::json!(true);
        replacement_summary["search_candidate_shadow_evidence"]["row_count_parity"] =
            serde_json::json!(false);
        replacement_summary["search_candidate_shadow_evidence"]
            ["shadow_scan_filter_pushdown_ready"] = serde_json::json!(false);
        replacement_summary["search_candidate_shadow_evidence"]["blocker_codes"] =
            serde_json::json!(["candidate_filter_residual", "candidate_row_count_mismatch"]);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["replacement_summary"])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["failed_evidence_fields"],
            serde_json::json!([
                "search_candidate_shadow_evidence.row_count_parity",
                "search_candidate_shadow_evidence.shadow_scan_filter_pushdown_ready"
            ])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["blocker_codes"],
            serde_json::json!(["candidate_filter_residual", "candidate_row_count_mismatch"])
        );
    }

    #[test]
    fn preflight_check_requires_storage_recovery_evidence() {
        let mut inputs = ready_inputs();
        let cutover_evidence = inputs
            .migration_gate
            .as_mut()
            .unwrap()
            .get_mut("cutover_evidence")
            .unwrap();
        cutover_evidence["storage_recovery_ready"] = serde_json::json!(false);
        cutover_evidence["storage_recovery_blocker_codes"] =
            serde_json::json!(["wal_replay_unbounded"]);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["storage_recovery"])
        );
        assert_eq!(
            check_by_name(&report, "storage_recovery")["blocker_codes"],
            serde_json::json!(["wal_replay_unbounded"])
        );
        assert_eq!(
            check_by_name(&report, "storage_recovery")["failed_evidence_fields"],
            serde_json::json!(["cutover_evidence.storage_recovery_ready"])
        );
    }

    #[test]
    fn preflight_check_requires_background_maintenance_evidence() {
        let mut inputs = ready_inputs();
        let cutover_evidence = inputs
            .migration_gate
            .as_mut()
            .unwrap()
            .get_mut("cutover_evidence")
            .unwrap();
        cutover_evidence["background_maintenance_present"] = serde_json::json!(false);
        cutover_evidence["background_maintenance_ready"] = serde_json::json!(false);
        cutover_evidence["background_maintenance_blocker_codes"] =
            serde_json::json!(["missing_evidence"]);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["background_maintenance"])
        );
        assert_eq!(
            check_by_name(&report, "background_maintenance")["blocker_codes"],
            serde_json::json!(["missing_evidence"])
        );
    }

    #[test]
    fn preflight_check_requires_matching_wrapper_identity() {
        let mut inputs = ready_inputs();
        inputs.wrapper_identity = Some("nowledge-previous-wrapper:other".to_string());

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!([
                "full_contract",
                "adapter_smoke",
                "migration_gate",
                "replacement_summary"
            ])
        );
        assert_eq!(
            check_by_name(&report, "full_contract")["failed_evidence_fields"],
            serde_json::json!(["previous_wrapper_contract_evidence.wrapper_identity"])
        );
        assert_eq!(
            check_by_name(&report, "adapter_smoke")["failed_evidence_fields"],
            serde_json::json!(["wrapper_identity"])
        );
        assert_eq!(
            check_by_name(&report, "migration_gate")["failed_evidence_fields"],
            serde_json::json!(["cutover_evidence.ready_wrapper_identity"])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["failed_evidence_fields"],
            serde_json::json!([
                "shadow_evidence.ready_wrapper_identity",
                "shadow_evidence.contract_wrapper_identity",
                "shadow_evidence.cutover_ready_wrapper_identity"
            ])
        );
    }

    #[test]
    fn preflight_check_requires_ready_dual_engine_evidence_when_present() {
        let mut inputs = ready_inputs();
        let adapter_smoke = inputs.adapter_smoke.as_mut().unwrap();
        adapter_smoke["dual_engine_evidence"]["ready"] = serde_json::json!(false);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["adapter_smoke"])
        );
        assert_eq!(
            check_by_name(&report, "adapter_smoke")["failed_evidence_fields"],
            serde_json::json!(["dual_engine_evidence.ready"])
        );
    }

    #[test]
    fn preflight_check_requires_adapter_dual_engine_counts_to_match() {
        let mut inputs = ready_inputs();
        let adapter_smoke = inputs.adapter_smoke.as_mut().unwrap();
        adapter_smoke["dual_engine_evidence"]["ready"] = serde_json::json!(true);
        adapter_smoke["dual_engine_evidence"]["matched_check_count"] = serde_json::json!(1);
        adapter_smoke["dual_engine_evidence"]["primary_only_check_count"] = serde_json::json!(1);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["adapter_smoke"])
        );
        assert_eq!(
            check_by_name(&report, "adapter_smoke")["failed_evidence_fields"],
            serde_json::json!([
                "dual_engine_evidence.matched_check_count",
                "dual_engine_evidence.primary_only_check_count"
            ])
        );
    }

    #[test]
    fn preflight_check_requires_adapter_dual_engine_evidence() {
        let mut inputs = ready_inputs();
        inputs
            .adapter_smoke
            .as_mut()
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove("dual_engine_evidence");

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["adapter_smoke"])
        );
        assert_eq!(
            check_by_name(&report, "adapter_smoke")["failed_evidence_fields"],
            serde_json::json!([
                "dual_engine_evidence.ready",
                "dual_engine_evidence.primary_check_count",
                "dual_engine_evidence.shadow_check_count",
                "dual_engine_evidence.matched_check_count",
                "dual_engine_evidence.primary_only_check_count"
            ])
        );
    }

    #[test]
    fn preflight_check_requires_replacement_dual_engine_consistency() {
        let mut inputs = ready_inputs();
        let replacement_summary = inputs.replacement_summary.as_mut().unwrap();
        replacement_summary["dual_engine_evidence"]["ready"] = serde_json::json!(true);
        replacement_summary["dual_engine_evidence"]["consistent"] = serde_json::json!(false);
        replacement_summary["dual_engine_evidence"]["matched_check_count"] = serde_json::json!(1);
        replacement_summary["dual_engine_evidence"]["primary_only_check_count"] =
            serde_json::json!(1);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["replacement_summary"])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["failed_evidence_fields"],
            serde_json::json!([
                "dual_engine_evidence.consistent",
                "dual_engine_evidence.matched_check_count",
                "dual_engine_evidence.primary_only_check_count"
            ])
        );
    }

    #[test]
    fn preflight_check_requires_replacement_shadow_provenance() {
        let mut inputs = ready_inputs();
        let replacement_summary = inputs.replacement_summary.as_mut().unwrap();
        replacement_summary["shadow_evidence"]["ready"] = serde_json::json!(false);
        replacement_summary["shadow_evidence"]["ready_wrapper_identity"] =
            serde_json::json!(serde_json::Value::Null);

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["replacement_summary"])
        );
        assert_eq!(
            check_by_name(&report, "replacement_summary")["failed_evidence_fields"],
            serde_json::json!([
                "shadow_evidence.ready",
                "shadow_evidence.ready_wrapper_identity"
            ])
        );
    }

    #[test]
    fn preflight_check_can_load_standard_bundle_dir() {
        let inputs = ready_inputs();
        let bundle_dir = unique_test_dir("previous-wrapper-preflight-bundle");
        std::fs::create_dir_all(&bundle_dir).unwrap();
        write_json(
            bundle_dir.join("contract-evidence.json"),
            inputs.contract_evidence.as_ref().unwrap(),
        );
        write_json(
            bundle_dir.join("adapter-smoke.json"),
            inputs.adapter_smoke.as_ref().unwrap(),
        );
        write_json(
            bundle_dir.join("migration-gate.json"),
            inputs.migration_gate.as_ref().unwrap(),
        );
        write_json(
            bundle_dir.join("replacement-summary.json"),
            inputs.replacement_summary.as_ref().unwrap(),
        );

        let (report, require_ready) = run_nowledge_previous_wrapper_preflight_check(
            vec![
                "--require-ready".to_string(),
                "--wrapper-identity".to_string(),
                "nowledge-previous-wrapper:test".to_string(),
                "--bundle-dir".to_string(),
                bundle_dir.to_string_lossy().into_owned(),
            ]
            .into_iter(),
        )
        .unwrap();

        assert!(require_ready);
        assert_eq!(report["ready"], true);
        assert_eq!(report["failed_checks"], serde_json::json!([]));
        std::fs::remove_dir_all(bundle_dir).unwrap();
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein-{name}-{nanos}"))
    }

    fn write_json(path: PathBuf, value: &serde_json::Value) {
        std::fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
    }

    fn assert_release_summary_field<T: serde::Serialize>(
        summary: &serde_json::Value,
        field: &str,
        expected: T,
    ) {
        assert_eq!(summary[field], serde_json::json!(expected));
    }

    fn ready_inputs() -> PreviousWrapperPreflightCheckInputs {
        PreviousWrapperPreflightCheckInputs {
            wrapper_identity: Some("nowledge-previous-wrapper:test".to_string()),
            bundle_dir: None,
            contract_evidence: Some(serde_json::json!({
                "required_contract_ready": true,
                "full_contract_checked": true,
                "full_contract_ready": true,
                "selected_checks": 2,
                "check_count": 2,
                "previous_wrapper_contract_evidence": {
                    "ready": true,
                    "wrapper_identity": "nowledge-previous-wrapper:test",
                    "blocker_codes": []
                },
                "required_contract_blocker_codes": []
            })),
            adapter_smoke: Some(serde_json::json!({
                "adapter_smoke_ready": true,
                "engine_kind": "previous_wrapper",
                "wrapper_identity": "nowledge-previous-wrapper:test",
                "primary_only_checks": 0,
                "request_count": 4,
                "dual_engine_evidence": {
                    "ready": true,
                    "primary_check_count": 2,
                    "shadow_check_count": 2,
                    "matched_check_count": 2,
                    "primary_only_check_count": 0
                },
                "blocker_codes": []
            })),
            migration_gate: Some(serde_json::json!({
                "migration_gate": {
                    "decision": "ready",
                    "blockers": []
                },
                "cutover": {
                    "decision": "ready",
                    "blockers": []
                },
                "cutover_evidence": {
                    "eligible": true,
                    "ready_engine_kind": "previous_wrapper",
                    "ready_wrapper_identity": "nowledge-previous-wrapper:test",
                    "storage_recovery_required": true,
                    "storage_recovery_present": true,
                    "storage_recovery_ready": true,
                    "storage_recovery_protocol_matches": true,
                    "storage_recovery_blocker_codes": [],
                    "background_maintenance_required": true,
                    "background_maintenance_present": true,
                    "background_maintenance_ready": true,
                    "background_maintenance_protocol_matches": true,
                    "background_maintenance_blocker_codes": [],
                    "blockers": []
                },
                "previous_wrapper_contract_evidence": {
                    "ready": true
                },
                "replacement_readiness_per_million": 1_000_000
            })),
            replacement_summary: Some(serde_json::json!({
                "production_cutover_ready": true,
                "production_replacement_per_million": 1_000_000,
                "blocking_categories": [],
                "missing_evidence": [],
                "next_actions": [],
                "shadow_evidence": {
                    "ready": true,
                    "ready_wrapper_identity": "nowledge-previous-wrapper:test",
                    "contract_wrapper_identity": "nowledge-previous-wrapper:test",
                    "cutover_ready_wrapper_identity": "nowledge-previous-wrapper:test"
                },
                "cutover_evidence": {
                    "background_maintenance_executable_search_projection_graph_delta_count": 2,
                    "background_maintenance_admitted_search_projection_graph_delta_count": 1,
                    "background_maintenance_deferred_search_projection_graph_delta_count": 1,
                    "background_maintenance_rejected_search_projection_graph_delta_count": 0,
                    "background_maintenance_executable_search_projection_graph_delta_operations": 8,
                    "background_maintenance_admitted_search_projection_graph_delta_operations": 3,
                    "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch": 42
                },
                "dual_engine_evidence": {
                    "present": true,
                    "ready": true,
                    "consistent": true,
                    "primary_check_count": 2,
                    "shadow_check_count": 2,
                    "matched_check_count": 2,
                    "primary_only_check_count": 0,
                    "matched_per_million": 1_000_000
                },
                "search_projection_evidence": ready_search_projection_evidence(),
                "search_projection_shadow_evidence": ready_search_projection_shadow_evidence(),
                "search_candidate_shadow_evidence": ready_search_candidate_shadow_evidence()
            })),
        }
    }

    fn ready_search_projection_evidence() -> serde_json::Value {
        serde_json::json!({
            "present": true,
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
            "predicate_pushdown_ready": true,
            "blocker_codes": []
        })
    }

    fn ready_search_projection_shadow_evidence() -> serde_json::Value {
        serde_json::json!({
            "present": true,
            "ready": true,
            "primary_engine": "lancedb",
            "shadow_engine": "skein",
            "primary_ready": true,
            "shadow_ready": true,
            "document_count_parity": true,
            "table_parity_ready": true,
            "embedding_identity_parity": true,
            "lifecycle_parity": true,
            "incremental_watermark_parity": true,
            "blocker_codes": []
        })
    }

    fn ready_search_candidate_shadow_evidence() -> serde_json::Value {
        serde_json::json!({
            "present": true,
            "ready": true,
            "row_count_parity": true,
            "vector_top_k_overlap_ready": true,
            "fts_top_k_overlap_ready": true,
            "shadow_scan_filter_pushdown_ready": true,
            "blocker_codes": []
        })
    }
}
