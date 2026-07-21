use skein::{Result, SkeinError};
use std::path::Path;

pub fn nowledge_mem_integration_readiness_usage() -> String {
    "nowledge-mem-integration-readiness requires [--require-ready] <integration-bundle-json>"
        .to_string()
}

pub fn run_nowledge_mem_integration_readiness(
    mut args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let mut require_ready = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--require-ready" => {
                require_ready = true;
            }
            path => {
                if args.next().is_some() {
                    return Err(SkeinError::Semantic(
                        nowledge_mem_integration_readiness_usage(),
                    ));
                }
                let bundle = read_json_file(Path::new(path))?;
                return Ok((
                    nowledge_mem_integration_readiness_json(&bundle),
                    require_ready,
                ));
            }
        }
    }
    Err(SkeinError::Semantic(
        nowledge_mem_integration_readiness_usage(),
    ))
}

pub fn nowledge_mem_integration_readiness_json(bundle: &serde_json::Value) -> serde_json::Value {
    let checks = vec![
        check(
            "skein_submodule",
            [
                bool_path(bundle, &["submodule", "present"]) == Some(true),
                non_empty_str_path(bundle, &["submodule", "path"]),
                non_empty_str_path(bundle, &["submodule", "commit"]),
            ],
            [
                "submodule.present",
                "submodule.path",
                "submodule.commit",
            ],
            blocker_codes(bundle, &[&["submodule", "blocker_codes"][..]]),
        ),
        check(
            "legacy_coexistence",
            [
                bool_path(bundle, &["coexistence", "old_database_retained"]) == Some(true),
                coexistence_mode_is_safe(bundle),
                bool_path(bundle, &["coexistence", "old_database_deleted"]) != Some(true),
            ],
            [
                "coexistence.old_database_retained",
                "coexistence.mode",
                "coexistence.old_database_deleted",
            ],
            blocker_codes(bundle, &[&["coexistence", "blocker_codes"][..]]),
        ),
        check(
            "content_store_boundary",
            [
                bool_path(bundle, &["content_store", "present"]) == Some(true),
                str_path(bundle, &["content_store", "engine"]) == Some("sqlite"),
                bool_path(bundle, &["content_store", "messages_available"]) == Some(true),
                bool_path(bundle, &["content_store", "source_chunks_available"]) == Some(true),
            ],
            [
                "content_store.present",
                "content_store.engine",
                "content_store.messages_available",
                "content_store.source_chunks_available",
            ],
            blocker_codes(bundle, &[&["content_store", "blocker_codes"][..]]),
        ),
        check(
            "previous_wrapper_preflight",
            [bool_path(bundle, &["previous_wrapper_preflight", "ready"]) == Some(true)],
            ["previous_wrapper_preflight.ready"],
            blocker_codes(
                bundle,
                &[
                    &["previous_wrapper_preflight", "blocker_codes"][..],
                    &["previous_wrapper_preflight", "failed_checks"][..],
                ],
            ),
        ),
        check(
            "cypher_coverage_evidence",
            [
                str_path(bundle, &["cypher_coverage_summary", "protocol"])
                    == Some("nowledge-mem-skein-cypher-coverage-summary"),
                bool_path(bundle, &["cypher_coverage_summary", "ready"]) == Some(true),
                u64_path(bundle, &["cypher_coverage_summary", "coverage_per_million"])
                    == Some(1_000_000),
                u64_path(bundle, &["cypher_coverage_summary", "required_checks"])
                    .is_some_and(|value| value > 0),
                u64_path(bundle, &["cypher_coverage_summary", "covered_checks"])
                    .is_some_and(|value| value > 0),
                u64_path(bundle, &["cypher_coverage_summary", "missing_checks_count"]) == Some(0),
                u64_path(
                    bundle,
                    &["cypher_coverage_summary", "blocked_query_family_count"],
                ) == Some(0),
            ],
            [
                "cypher_coverage_summary.protocol",
                "cypher_coverage_summary.ready",
                "cypher_coverage_summary.coverage_per_million",
                "cypher_coverage_summary.required_checks",
                "cypher_coverage_summary.covered_checks",
                "cypher_coverage_summary.missing_checks_count",
                "cypher_coverage_summary.blocked_query_family_count",
            ],
            blocker_codes(bundle, &[&["cypher_coverage_summary", "blocker_codes"][..]]),
        ),
        check(
            "graph_replacement_evidence",
            [
                bool_path(bundle, &["replacement_summary", "production_cutover_ready"])
                    == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary", "shadow_evidence", "ready"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary", "dual_engine_evidence", "present"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary", "dual_engine_evidence", "ready"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary", "dual_engine_evidence", "consistent"],
                ) == Some(true),
            ],
            [
                "replacement_summary.production_cutover_ready",
                "replacement_summary.shadow_evidence.ready",
                "replacement_summary.dual_engine_evidence.present",
                "replacement_summary.dual_engine_evidence.ready",
                "replacement_summary.dual_engine_evidence.consistent",
            ],
            blocker_codes(
                bundle,
                &[
                    &["replacement_summary", "blocking_categories"][..],
                    &["replacement_summary", "missing_evidence"][..],
                    &["replacement_summary", "dual_engine_evidence", "blocker_codes"][..],
                ],
            ),
        ),
        check(
            "search_projection_replacement_evidence",
            [
                bool_path(
                    bundle,
                    &["replacement_summary", "search_projection_evidence", "ready"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_shadow_evidence",
                        "present",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_shadow_evidence",
                        "ready",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_shadow_evidence",
                        "document_count_parity",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_shadow_evidence",
                        "table_parity_ready",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_shadow_evidence",
                        "embedding_identity_parity",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_shadow_evidence",
                        "incremental_watermark_parity",
                    ],
                ) == Some(true),
            ],
            [
                "replacement_summary.search_projection_evidence.ready",
                "replacement_summary.search_projection_shadow_evidence.present",
                "replacement_summary.search_projection_shadow_evidence.ready",
                "replacement_summary.search_projection_shadow_evidence.document_count_parity",
                "replacement_summary.search_projection_shadow_evidence.table_parity_ready",
                "replacement_summary.search_projection_shadow_evidence.embedding_identity_parity",
                "replacement_summary.search_projection_shadow_evidence.incremental_watermark_parity",
            ],
            blocker_codes(
                bundle,
                &[
                    &[
                        "replacement_summary",
                        "search_projection_evidence",
                        "blocker_codes",
                    ][..],
                    &[
                        "replacement_summary",
                        "search_projection_shadow_evidence",
                        "blocker_codes",
                    ][..],
                ],
            ),
        ),
        check(
            "bounded_read_evidence",
            [
                bool_path(
                    bundle,
                    &["replacement_summary", "bounded_read_evidence", "present"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary", "bounded_read_evidence", "ready"],
                ) == Some(true),
                u64_path(
                    bundle,
                    &["replacement_summary", "bounded_read_evidence", "max_rows"],
                )
                .is_some_and(|value| value > 0),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "bounded_read_evidence",
                        "row_limit_enforced_before_output",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "bounded_read_evidence",
                        "operator_row_cap_enabled",
                    ],
                ) == Some(true),
            ],
            [
                "replacement_summary.bounded_read_evidence.present",
                "replacement_summary.bounded_read_evidence.ready",
                "replacement_summary.bounded_read_evidence.max_rows",
                "replacement_summary.bounded_read_evidence.row_limit_enforced_before_output",
                "replacement_summary.bounded_read_evidence.operator_row_cap_enabled",
            ],
            blocker_codes(
                bundle,
                &[&[
                    "replacement_summary",
                    "bounded_read_evidence",
                    "blocker_codes",
                ][..]],
            ),
        ),
        check(
            "background_maintenance_evidence",
            [
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_required",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_ready",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_protocol_matches",
                    ],
                ) == Some(true),
                u64_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_executable_search_projection_graph_delta_count",
                    ],
                )
                .is_some(),
                u64_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_admitted_search_projection_graph_delta_count",
                    ],
                )
                .is_some(),
                u64_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_executable_search_projection_graph_delta_operations",
                    ],
                )
                .is_some(),
                u64_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_admitted_search_projection_graph_delta_operations",
                    ],
                )
                .is_some(),
            ],
            [
                "replacement_summary.cutover_evidence.background_maintenance_required",
                "replacement_summary.cutover_evidence.background_maintenance_ready",
                "replacement_summary.cutover_evidence.background_maintenance_protocol_matches",
                "replacement_summary.cutover_evidence.background_maintenance_executable_search_projection_graph_delta_count",
                "replacement_summary.cutover_evidence.background_maintenance_admitted_search_projection_graph_delta_count",
                "replacement_summary.cutover_evidence.background_maintenance_executable_search_projection_graph_delta_operations",
                "replacement_summary.cutover_evidence.background_maintenance_admitted_search_projection_graph_delta_operations",
            ],
            blocker_codes(
                bundle,
                &[
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_blocker_codes",
                    ][..],
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_blockers",
                    ][..],
                ],
            ),
        ),
        check(
            "storage_recovery_evidence",
            [
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "storage_recovery_required",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary", "cutover_evidence", "storage_recovery_ready"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "storage_recovery_protocol_matches",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary", "cutover_evidence", "storage_recovery_durable"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "storage_recovery_checkpoint_boundary_present",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "storage_recovery_wal_replay_bounded",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "storage_recovery_torn_tail_clean",
                    ],
                ) == Some(true),
            ],
            [
                "replacement_summary.cutover_evidence.storage_recovery_required",
                "replacement_summary.cutover_evidence.storage_recovery_ready",
                "replacement_summary.cutover_evidence.storage_recovery_protocol_matches",
                "replacement_summary.cutover_evidence.storage_recovery_durable",
                "replacement_summary.cutover_evidence.storage_recovery_checkpoint_boundary_present",
                "replacement_summary.cutover_evidence.storage_recovery_wal_replay_bounded",
                "replacement_summary.cutover_evidence.storage_recovery_torn_tail_clean",
            ],
            blocker_codes(
                bundle,
                &[
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "storage_recovery_blocker_codes",
                    ][..],
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "storage_recovery_blockers",
                    ][..],
                ],
            ),
        ),
    ];
    let ready = checks
        .iter()
        .all(|check| bool_path(check, &["ready"]) == Some(true));
    let failed_checks = checks
        .iter()
        .filter(|check| bool_path(check, &["ready"]) != Some(true))
        .filter_map(|check| str_path(check, &["name"]))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let blocker_codes = checks
        .iter()
        .flat_map(|check| string_array_path(check, &["blocker_codes"]))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    serde_json::json!({
        "protocol": "skein-nowledge-mem-integration-readiness",
        "ready": ready,
        "failed_checks": failed_checks,
        "checks": checks,
        "blocker_codes": blocker_codes,
        "next_actions": next_actions(bundle, ready),
    })
}

fn check(
    name: &'static str,
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
    serde_json::json!({
        "name": name,
        "ready": failed_evidence_fields.is_empty(),
        "evidence_fields": evidence_fields,
        "failed_evidence_fields": failed_evidence_fields,
        "blocker_codes": blocker_codes,
    })
}

fn next_actions(bundle: &serde_json::Value, ready: bool) -> Vec<serde_json::Value> {
    if ready {
        return Vec::new();
    }
    let mut actions = Vec::new();
    if bool_path(bundle, &["submodule", "present"]) != Some(true) {
        actions.push(next_action(
            "add_skein_submodule",
            "Nowledge Mem must depend on Skein as a submodule instead of copying sources",
            ["submodule.present", "submodule.path", "submodule.commit"],
        ));
    }
    if bool_path(bundle, &["coexistence", "old_database_retained"]) != Some(true)
        || !coexistence_mode_is_safe(bundle)
        || bool_path(bundle, &["coexistence", "old_database_deleted"]) == Some(true)
    {
        actions.push(next_action(
            "enable_side_by_side_coexistence",
            "Kuzu/Ladybug and LanceDB must remain available while Skein runs in shadow",
            [
                "coexistence.old_database_retained",
                "coexistence.mode",
                "coexistence.old_database_deleted",
            ],
        ));
    }
    if bool_path(bundle, &["content_store", "present"]) != Some(true) {
        actions.push(next_action(
            "attach_content_store_evidence",
            "messages and source chunks still come from content.db during replacement validation",
            [
                "content_store.present",
                "content_store.engine",
                "content_store.messages_available",
                "content_store.source_chunks_available",
            ],
        ));
    }
    if bool_path(bundle, &["previous_wrapper_preflight", "ready"]) != Some(true) {
        actions.push(next_action(
            "run_previous_wrapper_preflight",
            "the previous-wrapper release bundle must pass before Mem cutover",
            ["previous_wrapper_preflight.ready"],
        ));
    }
    if bool_path(bundle, &["cypher_coverage_summary", "ready"]) != Some(true)
        || u64_path(bundle, &["cypher_coverage_summary", "coverage_per_million"]) != Some(1_000_000)
        || u64_path(
            bundle,
            &["cypher_coverage_summary", "blocked_query_family_count"],
        ) != Some(0)
    {
        actions.push(next_action(
            "attach_cypher_coverage_summary",
            "scanned Nowledge Cypher business-surface coverage must be complete by query family",
            [
                "cypher_coverage_summary.protocol",
                "cypher_coverage_summary.ready",
                "cypher_coverage_summary.coverage_per_million",
                "cypher_coverage_summary.required_checks",
                "cypher_coverage_summary.covered_checks",
                "cypher_coverage_summary.blocked_query_family_count",
                "cypher_coverage_summary.blocker_codes",
            ],
        ));
    }
    if bool_path(bundle, &["replacement_summary", "production_cutover_ready"]) != Some(true) {
        actions.push(next_action(
            "produce_replacement_summary",
            "graph and search replacement evidence must be production-ready",
            [
                "replacement_summary.production_cutover_ready",
                "replacement_summary.blocking_categories",
                "replacement_summary.missing_evidence",
            ],
        ));
    }
    if bool_path(
        bundle,
        &[
            "replacement_summary",
            "search_projection_shadow_evidence",
            "ready",
        ],
    ) != Some(true)
    {
        actions.push(next_action(
            "run_search_projection_shadow_evidence",
            "LanceDB and Skein search projection parity must be proven side-by-side",
            [
                "replacement_summary.search_projection_shadow_evidence.ready",
                "replacement_summary.search_projection_shadow_evidence.blocker_codes",
            ],
        ));
    }
    if bool_path(
        bundle,
        &["replacement_summary", "bounded_read_evidence", "ready"],
    ) != Some(true)
    {
        actions.push(next_action(
            "attach_bounded_read_profile",
            "Skein read replacement must prove bounded execution before Mem cutover",
            [
                "replacement_summary.bounded_read_evidence.present",
                "replacement_summary.bounded_read_evidence.ready",
                "replacement_summary.bounded_read_evidence.max_rows",
                "replacement_summary.bounded_read_evidence.execution_row_cap",
                "replacement_summary.bounded_read_evidence.row_limit_enforced_before_output",
                "replacement_summary.bounded_read_evidence.operator_row_cap_enabled",
                "replacement_summary.bounded_read_evidence.blocker_codes",
            ],
        ));
    }
    if bool_path(
        bundle,
        &[
            "replacement_summary",
            "cutover_evidence",
            "storage_recovery_required",
        ],
    ) != Some(true)
        || bool_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "storage_recovery_ready",
            ],
        ) != Some(true)
        || bool_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "storage_recovery_protocol_matches",
            ],
        ) != Some(true)
        || bool_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "storage_recovery_wal_replay_bounded",
            ],
        ) != Some(true)
    {
        actions.push(next_action(
            "attach_storage_recovery_report",
            "storage recovery evidence must prove durable bounded WAL replay before Mem cutover",
            [
                "replacement_summary.cutover_evidence.storage_recovery_required",
                "replacement_summary.cutover_evidence.storage_recovery_ready",
                "replacement_summary.cutover_evidence.storage_recovery_protocol_matches",
                "replacement_summary.cutover_evidence.storage_recovery_durable",
                "replacement_summary.cutover_evidence.storage_recovery_checkpoint_boundary_present",
                "replacement_summary.cutover_evidence.storage_recovery_wal_replay_bounded",
                "replacement_summary.cutover_evidence.storage_recovery_torn_tail_clean",
                "replacement_summary.cutover_evidence.storage_recovery_blocker_codes",
            ],
        ));
    }
    if bool_path(
        bundle,
        &[
            "replacement_summary",
            "cutover_evidence",
            "background_maintenance_required",
        ],
    ) != Some(true)
        || bool_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_ready",
            ],
        ) != Some(true)
        || bool_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_protocol_matches",
            ],
        ) != Some(true)
        || u64_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_executable_search_projection_graph_delta_count",
            ],
        )
        .is_none()
        || u64_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_admitted_search_projection_graph_delta_count",
            ],
        )
        .is_none()
    {
        actions.push(next_action(
            "attach_background_maintenance_report",
            "background maintenance QoS and search-projection graph-delta evidence must be ready before Mem cutover",
            [
                "replacement_summary.cutover_evidence.background_maintenance_required",
                "replacement_summary.cutover_evidence.background_maintenance_ready",
                "replacement_summary.cutover_evidence.background_maintenance_protocol_matches",
                "replacement_summary.cutover_evidence.background_maintenance_executable_search_projection_graph_delta_count",
                "replacement_summary.cutover_evidence.background_maintenance_admitted_search_projection_graph_delta_count",
                "replacement_summary.cutover_evidence.background_maintenance_blocker_codes",
            ],
        ));
    }
    actions
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

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let raw = std::fs::read_to_string(path).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to read Nowledge Mem integration bundle: {error}",
        ))
    })?;
    serde_json::from_str(&raw).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to parse Nowledge Mem integration bundle: {error}",
        ))
    })
}

fn coexistence_mode_is_safe(bundle: &serde_json::Value) -> bool {
    matches!(
        str_path(bundle, &["coexistence", "mode"]),
        Some("shadow") | Some("side_by_side")
    )
}

fn blocker_codes(value: &serde_json::Value, paths: &[&[&str]]) -> Vec<String> {
    paths
        .iter()
        .flat_map(|path| string_array_path(value, path))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn string_array_path(value: &serde_json::Value, path: &[&str]) -> Vec<String> {
    json_get_path(value, path)
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(str::to_string)
        .collect()
}

fn non_empty_str_path(value: &serde_json::Value, path: &[&str]) -> bool {
    str_path(value, path).is_some_and(|s| !s.trim().is_empty())
}

fn bool_path(value: &serde_json::Value, path: &[&str]) -> Option<bool> {
    json_get_path(value, path).and_then(serde_json::Value::as_bool)
}

fn u64_path(value: &serde_json::Value, path: &[&str]) -> Option<u64> {
    json_get_path(value, path).and_then(serde_json::Value::as_u64)
}

fn str_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a str> {
    json_get_path(value, path).and_then(serde_json::Value::as_str)
}

fn json_get_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    Some(current)
}

#[cfg(test)]
mod tests {
    use super::nowledge_mem_integration_readiness_json;

    #[test]
    fn reports_ready_when_mem_integration_evidence_is_complete() {
        let report = nowledge_mem_integration_readiness_json(&ready_bundle());

        assert_eq!(report["ready"], true);
        assert_eq!(report["failed_checks"], serde_json::json!([]));
        assert_eq!(report["blocker_codes"], serde_json::json!([]));
        assert_eq!(report["next_actions"], serde_json::json!([]));
    }

    #[test]
    fn fails_closed_without_submodule_and_coexistence() {
        let mut bundle = ready_bundle();
        bundle["submodule"]["present"] = serde_json::json!(false);
        bundle["submodule"]["commit"] = serde_json::json!("");
        bundle["coexistence"]["old_database_retained"] = serde_json::json!(false);
        bundle["coexistence"]["old_database_deleted"] = serde_json::json!(true);
        bundle["coexistence"]["mode"] = serde_json::json!("replace_in_place");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["skein_submodule", "legacy_coexistence"])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "add_skein_submodule"));
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "enable_side_by_side_coexistence"));
    }

    #[test]
    fn requires_search_projection_shadow_parity() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["ready"] =
            serde_json::json!(false);
        bundle["replacement_summary"]["search_projection_shadow_evidence"]
            ["document_count_parity"] = serde_json::json!(false);
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["blocker_codes"] =
            serde_json::json!(["document_count_mismatch"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_projection_replacement_evidence"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["document_count_mismatch"])
        );
        let search_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_projection_replacement_evidence")
            .unwrap();
        assert_eq!(
            search_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.search_projection_shadow_evidence.ready",
                "replacement_summary.search_projection_shadow_evidence.document_count_parity"
            ])
        );
    }

    #[test]
    fn requires_cypher_coverage_summary() {
        let mut bundle = ready_bundle();
        bundle["cypher_coverage_summary"] = serde_json::json!(null);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["cypher_coverage_evidence"])
        );
        let coverage_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "cypher_coverage_evidence")
            .unwrap();
        assert_eq!(
            coverage_check["failed_evidence_fields"],
            serde_json::json!([
                "cypher_coverage_summary.protocol",
                "cypher_coverage_summary.ready",
                "cypher_coverage_summary.coverage_per_million",
                "cypher_coverage_summary.required_checks",
                "cypher_coverage_summary.covered_checks",
                "cypher_coverage_summary.missing_checks_count",
                "cypher_coverage_summary.blocked_query_family_count"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_cypher_coverage_summary"));
    }

    #[test]
    fn blocks_incomplete_cypher_coverage_summary() {
        let mut bundle = ready_bundle();
        bundle["cypher_coverage_summary"]["ready"] = serde_json::json!(false);
        bundle["cypher_coverage_summary"]["coverage_per_million"] = serde_json::json!(999_000);
        bundle["cypher_coverage_summary"]["missing_checks_count"] = serde_json::json!(1);
        bundle["cypher_coverage_summary"]["blocked_query_family_count"] = serde_json::json!(1);
        bundle["cypher_coverage_summary"]["blocker_codes"] =
            serde_json::json!(["cypher_coverage_incomplete"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["cypher_coverage_evidence"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["cypher_coverage_incomplete"])
        );
        let coverage_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "cypher_coverage_evidence")
            .unwrap();
        assert_eq!(
            coverage_check["failed_evidence_fields"],
            serde_json::json!([
                "cypher_coverage_summary.ready",
                "cypher_coverage_summary.coverage_per_million",
                "cypher_coverage_summary.missing_checks_count",
                "cypher_coverage_summary.blocked_query_family_count"
            ])
        );
    }

    #[test]
    fn requires_bounded_read_evidence() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["bounded_read_evidence"]["ready"] = serde_json::json!(false);
        bundle["replacement_summary"]["bounded_read_evidence"]
            ["row_limit_enforced_before_output"] = serde_json::json!(false);
        bundle["replacement_summary"]["bounded_read_evidence"]["blocker_codes"] =
            serde_json::json!(["row_cap_not_enforced"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["bounded_read_evidence"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["row_cap_not_enforced"])
        );
        let read_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "bounded_read_evidence")
            .unwrap();
        assert_eq!(
            read_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.bounded_read_evidence.ready",
                "replacement_summary.bounded_read_evidence.row_limit_enforced_before_output"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_bounded_read_profile"));
    }

    #[test]
    fn requires_background_maintenance_evidence() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["cutover_evidence"]["background_maintenance_ready"] =
            serde_json::json!(false);
        bundle["replacement_summary"]["cutover_evidence"]
            ["background_maintenance_protocol_matches"] = serde_json::json!(false);
        bundle["replacement_summary"]["cutover_evidence"]
            ["background_maintenance_admitted_search_projection_graph_delta_count"] =
            serde_json::Value::Null;
        bundle["replacement_summary"]["cutover_evidence"]["background_maintenance_blocker_codes"] =
            serde_json::json!(["background_disabled"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["background_maintenance_evidence"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["background_disabled"])
        );
        let maintenance_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "background_maintenance_evidence")
            .unwrap();
        assert_eq!(
            maintenance_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.cutover_evidence.background_maintenance_ready",
                "replacement_summary.cutover_evidence.background_maintenance_protocol_matches",
                "replacement_summary.cutover_evidence.background_maintenance_admitted_search_projection_graph_delta_count"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_background_maintenance_report"));
    }

    #[test]
    fn requires_storage_recovery_evidence() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["cutover_evidence"]["storage_recovery_ready"] =
            serde_json::json!(false);
        bundle["replacement_summary"]["cutover_evidence"]["storage_recovery_wal_replay_bounded"] =
            serde_json::json!(false);
        bundle["replacement_summary"]["cutover_evidence"]["storage_recovery_blocker_codes"] =
            serde_json::json!(["wal_replay_unbounded"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["storage_recovery_evidence"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["wal_replay_unbounded"])
        );
        let storage_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "storage_recovery_evidence")
            .unwrap();
        assert_eq!(
            storage_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.cutover_evidence.storage_recovery_ready",
                "replacement_summary.cutover_evidence.storage_recovery_wal_replay_bounded"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_storage_recovery_report"));
    }

    fn ready_bundle() -> serde_json::Value {
        serde_json::json!({
            "submodule": {
                "present": true,
                "path": "vendor/skein",
                "commit": "46f8bfb",
                "blocker_codes": []
            },
            "coexistence": {
                "old_database_retained": true,
                "old_database_deleted": false,
                "mode": "shadow",
                "blocker_codes": []
            },
            "content_store": {
                "present": true,
                "engine": "sqlite",
                "messages_available": true,
                "source_chunks_available": true,
                "blocker_codes": []
            },
            "previous_wrapper_preflight": {
                "ready": true,
                "blocker_codes": [],
                "failed_checks": []
            },
            "cypher_coverage_summary": {
                "protocol": "nowledge-mem-skein-cypher-coverage-summary",
                "ready": true,
                "coverage_per_million": 1_000_000,
                "required_checks": 708,
                "covered_checks": 708,
                "missing_checks_count": 0,
                "extra_fixture_checks_count": 0,
                "query_family_count": 4,
                "blocked_query_family_count": 0,
                "blocked_query_families": [],
                "coverage_by_query_family": [
                    {
                        "query_family": "read",
                        "coverage_per_million": 1_000_000,
                        "required_checks": 529,
                        "covered_checks": 529,
                        "missing_checks_count": 0
                    }
                ],
                "blocker_codes": []
            },
            "replacement_summary": {
                "production_cutover_ready": true,
                "blocking_categories": [],
                "missing_evidence": [],
                "shadow_evidence": {
                    "ready": true
                },
                "dual_engine_evidence": {
                    "present": true,
                    "ready": true,
                    "consistent": true
                },
                "search_projection_evidence": {
                    "ready": true,
                    "blocker_codes": []
                },
                "search_projection_shadow_evidence": {
                    "present": true,
                    "ready": true,
                    "document_count_parity": true,
                    "table_parity_ready": true,
                    "embedding_identity_parity": true,
                    "incremental_watermark_parity": true,
                    "blocker_codes": []
                },
                "bounded_read_evidence": {
                    "present": true,
                    "ready": true,
                    "max_rows": 512,
                    "execution_row_cap": 513,
                    "row_limit_enforced_before_output": true,
                    "operator_row_cap_enabled": true,
                    "blocking_operator_count": 0,
                    "blocker_codes": []
                },
                "cutover_evidence": {
                    "storage_recovery_required": true,
                    "storage_recovery_ready": true,
                    "storage_recovery_protocol_matches": true,
                    "storage_recovery_durable": true,
                    "storage_recovery_checkpoint_boundary_present": true,
                    "storage_recovery_wal_replay_bounded": true,
                    "storage_recovery_torn_tail_clean": true,
                    "storage_recovery_blocker_codes": [],
                    "storage_recovery_blockers": [],
                    "background_maintenance_required": true,
                    "background_maintenance_ready": true,
                    "background_maintenance_protocol_matches": true,
                    "background_maintenance_executable_search_projection_graph_delta_count": 1,
                    "background_maintenance_admitted_search_projection_graph_delta_count": 1,
                    "background_maintenance_deferred_search_projection_graph_delta_count": 0,
                    "background_maintenance_rejected_search_projection_graph_delta_count": 0,
                    "background_maintenance_executable_search_projection_graph_delta_operations": 2,
                    "background_maintenance_admitted_search_projection_graph_delta_operations": 2,
                    "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch": 7,
                    "background_maintenance_blocker_codes": [],
                    "background_maintenance_blockers": []
                }
            }
        })
    }
}
