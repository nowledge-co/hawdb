mod cli;

use cli::fixture_contract::{nowledge_fixture_contract_json, nowledge_fixture_contract_usage};
use cli::fixture_contract_check::run_nowledge_fixture_contract_command_check;
use skein::background_maintenance_evidence::run_nowledge_background_maintenance_evidence;
use skein::bounded_read_evidence::run_nowledge_bounded_read_evidence;
use skein::graph_route_evidence::run_nowledge_graph_route_evidence;
use skein::graph_route_readiness::run_nowledge_graph_route_readiness;
use skein::mem_integration_bundle::run_nowledge_mem_integration_bundle;
use skein::mem_integration_readiness::{
    nowledge_mem_integration_readiness_json, run_nowledge_mem_integration_readiness,
};
use skein::mem_library_readiness::run_nowledge_mem_library_readiness;
use skein::nowledge_inventory::background_maintenance_summary_to_json;
use skein::previous_wrapper_preflight::run_nowledge_previous_wrapper_preflight_check;
use skein::query_family_evidence::run_nowledge_query_family_evidence;
use skein::query_runtime_preflight::run_nowledge_query_runtime_preflight;
use skein::replacement_summary::{
    nowledge_replacement_summary_json, nowledge_replacement_summary_json_with_options,
    nowledge_replacement_summary_usage, NowledgeReplacementSummaryOptions,
};
use skein::search_candidate_shadow_evidence::run_nowledge_search_candidate_shadow_evidence;
use skein::search_projection_evidence::{
    nowledge_search_projection_probe_contract_json,
    nowledge_search_projection_probe_contract_usage, run_nowledge_search_projection_evidence,
    run_nowledge_search_projection_shadow_evidence, run_skein_search_projection_probe,
};
use skein::storage_recovery_evidence::run_nowledge_storage_recovery_evidence;
use skein::{
    background_maintenance_evidence_health_from_bundle, external_shadow_ready_missing_capabilities,
    external_shadow_trace_health_from_bundle, external_shadow_trace_report_json,
    replacement_readiness_family_evidence_health_from_bundle,
    scan_nowledge_query_inventory_cypher_coverage_detail_to_json,
    scan_nowledge_query_inventory_cypher_coverage_to_json,
    scan_nowledge_query_inventory_cypher_migration_gate_with_options_to_json,
    scan_nowledge_query_inventory_to_json, storage_recovery_evidence_health_from_bundle,
    CanonicalGraphSnapshotValidation, CanonicalSnapshotIdentityAudit, CompatibilityCheck,
    CompatibilityRollbackEvidence, CompatibilityShadowReport, CompatibilityShadowStatus,
    CypherFixtureCheck, CypherFixtureStatement, Database, DatabaseConfig, ExpectedRows,
    ExternalShadowCommand, ExternalShadowReady, GraphLightningBootstrapManifest,
    NowledgeCypherMigrationGateJsonOptions, NowledgeMemGraph, NowledgeMemGraphMode,
    NowledgeMemReadOptions, ProjectedGraphFixtureCheck, RecoveryMode, Result, SearchIndex,
    SkeinError, StorageRecoveryReport, StorageResidencyMode, StorageResourceProfileLimits, Value,
    GRAPH_LIGHTNING_BOOTSTRAP_PROTOCOL_VERSION, REQUIRED_EXTERNAL_SHADOW_CAPABILITIES,
};
use skein::{
    nowledge_memory_core_fixture, run_compatibility_fixture_with_shadow,
    BackgroundMaintenanceOptions, CompatibilityFixture, LocalQosPolicy, LocalQosState, WorkClass,
    WORK_CLASS_COUNT,
};
use skein_integrity::checksum_u64;
use skein_storage::{durable_replace_file, sync_directory as sync_storage_directory};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::time::Duration;

const GRAPH_LIGHTNING_STAGING_CATALOG_PROTOCOL_VERSION: u64 = 1;
const SKEIN_ENABLE_COMPATIBILITY_TOOLS_ENV: &str = "SKEIN_ENABLE_COMPATIBILITY_TOOLS";

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1).peekable();
    if let Some(command) = args.next() {
        if command == "scan-nowledge-inventory" {
            let root = args.next().unwrap_or_else(|| ".".to_string());
            let json = scan_nowledge_query_inventory_to_json(root)?;
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
            return Ok(());
        }
        if command == "scan-nowledge-cypher-coverage" {
            let root = args.next().unwrap_or_else(|| ".".to_string());
            let json = scan_nowledge_query_inventory_cypher_coverage_to_json(root)?;
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
            return Ok(());
        }
        if command == "scan-nowledge-cypher-coverage-detail" {
            let root = args.next().unwrap_or_else(|| ".".to_string());
            let json = scan_nowledge_query_inventory_cypher_coverage_detail_to_json(root)?;
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
            return Ok(());
        }
        if command == "nowledge-fixture-contract" {
            let fixture_name = args
                .next()
                .unwrap_or_else(|| "nowledge-memory-core".to_string());
            if args.next().is_some() || fixture_name != "nowledge-memory-core" {
                return Err(SkeinError::Semantic(nowledge_fixture_contract_usage()));
            }
            let fixture = nowledge_memory_core_fixture();
            let json = nowledge_fixture_contract_json(&fixture);
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
            return Ok(());
        }
        if command == "nowledge-fixture-contract-command-check" {
            let json = run_nowledge_fixture_contract_command_check(args)?;
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
            if json
                .get("required_contract_ready")
                .or_else(|| json.get("contract_command_check_ready"))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
            {
                return Ok(());
            }
            return Err(SkeinError::Execution(
                "fixture contract command check failed".to_string(),
            ));
        }
        if command == "nowledge-graph-route-readiness" {
            let (json, require_ready) = run_nowledge_graph_route_readiness(args)?;
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
            if require_ready
                && json
                    .get("route_primary_ready")
                    .and_then(serde_json::Value::as_bool)
                    != Some(true)
            {
                return Err(SkeinError::Execution(
                    "nowledge graph route readiness is not ready".to_string(),
                ));
            }
            return Ok(());
        }
        if command == "nowledge-graph-route-evidence" {
            let (json, require_ready) = run_nowledge_graph_route_evidence(args)?;
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
            if require_ready && json.get("ready").and_then(serde_json::Value::as_bool) != Some(true)
            {
                return Err(SkeinError::Execution(
                    "nowledge graph route evidence is not ready".to_string(),
                ));
            }
            return Ok(());
        }
        if command == "nowledge-previous-wrapper-preflight-check" {
            let (json, require_ready) = run_nowledge_previous_wrapper_preflight_check(args)?;
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
            if require_ready && json.get("ready").and_then(serde_json::Value::as_bool) != Some(true)
            {
                return Err(SkeinError::Execution(
                    "nowledge previous-wrapper preflight is not ready".to_string(),
                ));
            }
            return Ok(());
        }
        if command == "nowledge-mem-integration-readiness" {
            let (json, require_ready) = run_nowledge_mem_integration_readiness(args)?;
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
            if require_ready && json.get("ready").and_then(serde_json::Value::as_bool) != Some(true)
            {
                return Err(SkeinError::Execution(
                    "nowledge mem integration readiness is not ready".to_string(),
                ));
            }
            return Ok(());
        }
        if command == "nowledge-mem-integration-bundle" {
            let (json, require_ready) = run_nowledge_mem_integration_bundle(args)?;
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
            if require_ready
                && nowledge_mem_integration_readiness_json(&json)
                    .get("ready")
                    .and_then(serde_json::Value::as_bool)
                    != Some(true)
            {
                return Err(SkeinError::Execution(
                    "nowledge mem integration bundle is not ready".to_string(),
                ));
            }
            return Ok(());
        }
        if command == "nowledge-mem-library-readiness" {
            let (json, require_ready) = run_nowledge_mem_library_readiness(args)?;
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
            if require_ready && json.get("ready").and_then(serde_json::Value::as_bool) != Some(true)
            {
                return Err(SkeinError::Execution(
                    "nowledge mem library readiness is not ready".to_string(),
                ));
            }
            return Ok(());
        }
        if command == "nowledge-search-projection-evidence" {
            let (json, require_ready) = run_nowledge_search_projection_evidence(args)?;
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
            if require_ready && json.get("ready").and_then(serde_json::Value::as_bool) != Some(true)
            {
                return Err(SkeinError::Execution(
                    "nowledge search projection evidence is not ready".to_string(),
                ));
            }
            return Ok(());
        }
        if command == "nowledge-search-projection-probe-contract" {
            if args.next().is_some() {
                return Err(SkeinError::Semantic(
                    nowledge_search_projection_probe_contract_usage(),
                ));
            }
            let json = nowledge_search_projection_probe_contract_json();
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
            return Ok(());
        }
        if command == "skein-search-projection-probe" {
            let json = run_skein_search_projection_probe(args)?;
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
            return Ok(());
        }
        if command == "nowledge-search-projection-shadow-evidence" {
            let (json, require_ready) = run_nowledge_search_projection_shadow_evidence(args)?;
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
            if require_ready && json.get("ready").and_then(serde_json::Value::as_bool) != Some(true)
            {
                return Err(SkeinError::Execution(
                    "nowledge search projection shadow evidence is not ready".to_string(),
                ));
            }
            return Ok(());
        }
        if command == "nowledge-search-candidate-shadow-evidence" {
            let (json, require_ready) = run_nowledge_search_candidate_shadow_evidence(args)?;
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
            if require_ready && json.get("ready").and_then(serde_json::Value::as_bool) != Some(true)
            {
                return Err(SkeinError::Execution(
                    "nowledge search candidate shadow evidence is not ready".to_string(),
                ));
            }
            return Ok(());
        }
        if command == "nowledge-bounded-read-evidence" {
            let (json, require_ready) = run_nowledge_bounded_read_evidence(args)?;
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
            if require_ready && json.get("ready").and_then(serde_json::Value::as_bool) != Some(true)
            {
                return Err(SkeinError::Execution(
                    "nowledge bounded read evidence is not ready".to_string(),
                ));
            }
            return Ok(());
        }
        if command == "nowledge-bounded-read-report" {
            let mut parameters = BTreeMap::new();
            let mut options = NowledgeMemReadOptions::default();
            while let Some(flag) = args.peek() {
                match flag.as_str() {
                    "--params-json" => {
                        args.next();
                        let raw_parameters = args.next().ok_or_else(|| {
                            SkeinError::Semantic(nowledge_bounded_read_report_usage())
                        })?;
                        parameters = parse_parameters_json(&raw_parameters)?;
                    }
                    "--max-rows" => {
                        args.next();
                        let raw_limit = args.next().ok_or_else(|| {
                            SkeinError::Semantic(nowledge_bounded_read_report_usage())
                        })?;
                        options.max_rows = Some(parse_positive_usize("--max-rows", &raw_limit)?);
                    }
                    "--max-estimated-payload-bytes" => {
                        args.next();
                        let raw_limit = args.next().ok_or_else(|| {
                            SkeinError::Semantic(nowledge_bounded_read_report_usage())
                        })?;
                        options.max_estimated_payload_bytes = Some(parse_positive_usize(
                            "--max-estimated-payload-bytes",
                            &raw_limit,
                        )?);
                    }
                    _ => break,
                }
            }
            let path = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(nowledge_bounded_read_report_usage()))?;
            let query = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(nowledge_bounded_read_report_usage()))?;
            if args.next().is_some() {
                return Err(SkeinError::Semantic(nowledge_bounded_read_report_usage()));
            }
            let json = nowledge_bounded_read_report_json(&path, &query, &parameters, &options)?;
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
            return Ok(());
        }
        if command == "nowledge-storage-recovery-evidence" {
            let (json, require_ready) = run_nowledge_storage_recovery_evidence(args)?;
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
            if require_ready && json.get("ready").and_then(serde_json::Value::as_bool) != Some(true)
            {
                return Err(SkeinError::Execution(
                    "nowledge storage recovery evidence is not ready".to_string(),
                ));
            }
            return Ok(());
        }
        if command == "nowledge-background-maintenance-evidence" {
            let (json, require_ready) = run_nowledge_background_maintenance_evidence(args)?;
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
            if require_ready && json.get("ready").and_then(serde_json::Value::as_bool) != Some(true)
            {
                return Err(SkeinError::Execution(
                    "nowledge background maintenance evidence is not ready".to_string(),
                ));
            }
            return Ok(());
        }
        if command == "nowledge-query-family-evidence" {
            let (json, require_ready) = run_nowledge_query_family_evidence(args)?;
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
            if require_ready && json.get("ready").and_then(serde_json::Value::as_bool) != Some(true)
            {
                return Err(SkeinError::Execution(
                    "nowledge query family evidence is not ready".to_string(),
                ));
            }
            return Ok(());
        }
        if command == "nowledge-query-runtime-preflight" {
            let (json, require_ready) = run_nowledge_query_runtime_preflight(args)?;
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
            if require_ready && json.get("ready").and_then(serde_json::Value::as_bool) != Some(true)
            {
                return Err(SkeinError::Execution(
                    "nowledge query runtime preflight is not ready".to_string(),
                ));
            }
            return Ok(());
        }
        if command == "explain" || command == "explain-analyze" {
            let mut parameters = BTreeMap::new();
            while let Some(flag) = args.peek() {
                match flag.as_str() {
                    "--params-json" => {
                        args.next();
                        let raw_parameters = args
                            .next()
                            .ok_or_else(|| SkeinError::Semantic(explain_table_usage(&command)))?;
                        parameters = parse_parameters_json(&raw_parameters)?;
                    }
                    _ => break,
                }
            }
            let path = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(explain_table_usage(&command)))?;
            let query = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(explain_table_usage(&command)))?;
            if args.next().is_some() {
                return Err(SkeinError::Semantic(explain_table_usage(&command)));
            }
            let mut db = Database::open_with_config(
                path,
                DatabaseConfig {
                    read_only: true,
                    ..DatabaseConfig::default()
                },
            )?;
            if command == "explain" {
                println!("{}", db.explain_query_with_params(&query, &parameters)?);
            } else {
                println!(
                    "{}",
                    db.explain_analyze_query_with_params(&query, &parameters)?
                );
            }
            return Ok(());
        }
        if command == "explain-json" {
            let mut parameters = BTreeMap::new();
            while let Some(flag) = args.peek() {
                match flag.as_str() {
                    "--params-json" => {
                        args.next();
                        let raw_parameters = args
                            .next()
                            .ok_or_else(|| SkeinError::Semantic(explain_json_usage()))?;
                        parameters = parse_parameters_json(&raw_parameters)?;
                    }
                    _ => break,
                }
            }
            let path = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(explain_json_usage()))?;
            let query = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(explain_json_usage()))?;
            if args.next().is_some() {
                return Err(SkeinError::Semantic(explain_json_usage()));
            }
            let db = Database::open_with_config(
                path,
                DatabaseConfig {
                    read_only: true,
                    ..DatabaseConfig::default()
                },
            )?;
            let explain = db.explain_query_with_params(&query, &parameters)?;
            let rendered =
                explain_output_json(&query, &parameters, &explain, &db.plan_cache_stats());
            println!("{}", serde_json::to_string_pretty(&rendered).unwrap());
            return Ok(());
        }
        if command == "explain-analyze-json" {
            let mut parameters = BTreeMap::new();
            while let Some(flag) = args.peek() {
                match flag.as_str() {
                    "--params-json" => {
                        args.next();
                        let raw_parameters = args
                            .next()
                            .ok_or_else(|| SkeinError::Semantic(explain_analyze_json_usage()))?;
                        parameters = parse_parameters_json(&raw_parameters)?;
                    }
                    _ => break,
                }
            }
            let path = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(explain_analyze_json_usage()))?;
            let query = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(explain_analyze_json_usage()))?;
            if args.next().is_some() {
                return Err(SkeinError::Semantic(explain_analyze_json_usage()));
            }
            let mut db = Database::open_with_config(
                path,
                DatabaseConfig {
                    read_only: true,
                    ..DatabaseConfig::default()
                },
            )?;
            let explain = db.explain_analyze_query_with_params(&query, &parameters)?;
            let rendered =
                explain_analyze_output_json(&query, &parameters, &explain, &db.plan_cache_stats());
            println!("{}", serde_json::to_string_pretty(&rendered).unwrap());
            return Ok(());
        }
        if command == "external-shadow-adapter-smoke" {
            require_developer_compatibility_tool(&command)?;
            let mut require_previous_wrapper = false;
            let mut shadow_trace = None;
            let mut shadow_timeout = None;
            while let Some(flag) = args.peek() {
                match flag.as_str() {
                    "--require-previous-wrapper" => {
                        require_previous_wrapper = true;
                        args.next();
                    }
                    "--shadow-trace" => {
                        args.next();
                        shadow_trace = Some(args.next().ok_or_else(|| {
                            SkeinError::Semantic(external_shadow_adapter_smoke_usage())
                        })?);
                    }
                    "--shadow-timeout-ms" => {
                        args.next();
                        let raw_timeout = args.next().ok_or_else(|| {
                            SkeinError::Semantic(external_shadow_adapter_smoke_usage())
                        })?;
                        shadow_timeout = Some(parse_shadow_timeout_ms(&raw_timeout)?);
                    }
                    _ => break,
                }
            }
            let shadow_name = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(external_shadow_adapter_smoke_usage()))?;
            let program = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(external_shadow_adapter_smoke_usage()))?;
            let program_args = args.collect::<Vec<_>>();
            let shadow_trace_report = shadow_trace.clone();
            let mut shadow = match (shadow_trace, shadow_timeout) {
                (Some(trace_path), Some(timeout)) => {
                    ExternalShadowCommand::spawn_with_trace_path_and_request_timeout(
                        shadow_name,
                        program,
                        program_args,
                        trace_path,
                        timeout,
                    )?
                }
                (Some(trace_path), None) => ExternalShadowCommand::spawn_with_trace_path(
                    shadow_name,
                    program,
                    program_args,
                    trace_path,
                )?,
                (None, Some(timeout)) => ExternalShadowCommand::spawn_with_request_timeout(
                    shadow_name,
                    program,
                    program_args,
                    timeout,
                )?,
                (None, None) => ExternalShadowCommand::spawn(shadow_name, program, program_args)?,
            };
            let ready = shadow.require_ready()?;
            let mut db = Database::new();
            let fixture = external_shadow_adapter_smoke_fixture();
            let report = run_compatibility_fixture_with_shadow(&mut db, &fixture, &mut shadow)?;
            let json = external_shadow_adapter_smoke_report_json(
                &ready,
                &report,
                shadow.request_count(),
                shadow_trace_report.as_deref(),
            );
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
            enforce_external_shadow_adapter_smoke_requirements(
                &ready,
                &report,
                require_previous_wrapper,
            )?;
            return Ok(());
        }
        if command == "nowledge-cypher-migration-gate" {
            require_developer_compatibility_tool(&command)?;
            let mut require_ready = false;
            let mut require_cutover_evidence = false;
            let mut allow_self_shadow = false;
            let mut shadow_ready = false;
            let mut shadow_trace = None;
            let mut shadow_timeout = None;
            let mut rollback_required = false;
            let mut rollback_evidence = None;
            let mut storage_recovery_required = false;
            let mut storage_recovery = None;
            let mut background_maintenance_required = false;
            let mut background_maintenance = None;
            let mut previous_wrapper_contract_evidence = None;
            while let Some(flag) = args.peek() {
                match flag.as_str() {
                    "--require-ready" => {
                        require_ready = true;
                        args.next();
                    }
                    "--require-cutover-evidence" => {
                        require_cutover_evidence = true;
                        args.next();
                    }
                    "--allow-self-shadow" => {
                        allow_self_shadow = true;
                        args.next();
                    }
                    "--shadow-ready" => {
                        shadow_ready = true;
                        args.next();
                    }
                    "--shadow-trace" => {
                        args.next();
                        shadow_trace = Some(args.next().ok_or_else(|| {
                            SkeinError::Semantic(nowledge_cypher_migration_gate_usage())
                        })?);
                    }
                    "--shadow-timeout-ms" => {
                        args.next();
                        let raw_timeout = args.next().ok_or_else(|| {
                            SkeinError::Semantic(nowledge_cypher_migration_gate_usage())
                        })?;
                        shadow_timeout = Some(parse_shadow_timeout_ms(&raw_timeout)?);
                    }
                    "--require-rollback-evidence" => {
                        rollback_required = true;
                        args.next();
                    }
                    "--rollback-evidence" => {
                        args.next();
                        rollback_evidence = Some(args.next().ok_or_else(|| {
                            SkeinError::Semantic(nowledge_cypher_migration_gate_usage())
                        })?);
                    }
                    "--require-storage-recovery-evidence" => {
                        storage_recovery_required = true;
                        args.next();
                    }
                    "--require-background-maintenance-evidence" => {
                        background_maintenance_required = true;
                        args.next();
                    }
                    "--storage-recovery-report-json" => {
                        args.next();
                        let path = args.next().ok_or_else(|| {
                            SkeinError::Semantic(nowledge_cypher_migration_gate_usage())
                        })?;
                        storage_recovery = Some(read_json_file(Path::new(&path))?);
                    }
                    "--background-maintenance-report-json" => {
                        args.next();
                        let path = args.next().ok_or_else(|| {
                            SkeinError::Semantic(nowledge_cypher_migration_gate_usage())
                        })?;
                        background_maintenance = Some(read_json_file(Path::new(&path))?);
                    }
                    "--previous-wrapper-contract-evidence-json" => {
                        args.next();
                        let path = args.next().ok_or_else(|| {
                            SkeinError::Semantic(nowledge_cypher_migration_gate_usage())
                        })?;
                        previous_wrapper_contract_evidence =
                            Some(read_json_file(Path::new(&path))?);
                    }
                    _ => break,
                }
            }
            let root = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(nowledge_cypher_migration_gate_usage()))?;
            let shadow_name = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(nowledge_cypher_migration_gate_usage()))?;
            let program = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(nowledge_cypher_migration_gate_usage()))?;
            let program_args = args.collect::<Vec<_>>();
            let is_self_shadow = is_self_shadow_command(&shadow_name, &program, &program_args);
            if (require_ready || require_cutover_evidence) && !allow_self_shadow && is_self_shadow {
                return Err(SkeinError::Execution(
                    "nowledge migration gate requires a previous-wrapper shadow for required cutover gates; pass --allow-self-shadow only for protocol smoke tests"
                    .to_string(),
                ));
            }
            let shadow_name_report = shadow_name.clone();
            let shadow_trace_report = shadow_trace.clone();
            let mut shadow = match (shadow_trace, shadow_timeout) {
                (Some(trace_path), Some(timeout)) => {
                    ExternalShadowCommand::spawn_with_trace_path_and_request_timeout(
                        shadow_name,
                        program,
                        program_args,
                        trace_path,
                        timeout,
                    )?
                }
                (Some(trace_path), None) => ExternalShadowCommand::spawn_with_trace_path(
                    shadow_name,
                    program,
                    program_args,
                    trace_path,
                )?,
                (None, Some(timeout)) => ExternalShadowCommand::spawn_with_request_timeout(
                    shadow_name,
                    program,
                    program_args,
                    timeout,
                )?,
                (None, None) => ExternalShadowCommand::spawn(shadow_name, program, program_args)?,
            };
            let shadow_ready_report =
                if should_run_shadow_ready(require_ready, require_cutover_evidence, shadow_ready) {
                    Some(shadow.require_ready()?)
                } else {
                    None
                };
            let mut json =
                scan_nowledge_query_inventory_cypher_migration_gate_with_options_to_json(
                    root,
                    &mut shadow,
                    NowledgeCypherMigrationGateJsonOptions {
                        rollback: CompatibilityRollbackEvidence {
                            required: rollback_required,
                            ready: rollback_evidence.is_some(),
                            evidence: rollback_evidence,
                            blockers: Vec::new(),
                        },
                        storage_recovery_required,
                        storage_recovery,
                        background_maintenance_required,
                        background_maintenance,
                        previous_wrapper_contract_evidence,
                        ..NowledgeCypherMigrationGateJsonOptions::default()
                    },
                )?;
            add_shadow_run_report(&mut json, &shadow_name_report, is_self_shadow)?;
            if let Some(ready) = shadow_ready_report.as_ref() {
                add_shadow_ready_report(&mut json, ready)?;
            }
            if let Some(trace_path) = shadow_trace_report {
                add_shadow_trace_report(&mut json, &trace_path, shadow.request_count())?;
            }
            add_cutover_evidence_report(
                &mut json,
                is_self_shadow,
                shadow_ready_report.as_ref(),
                storage_recovery_required,
                background_maintenance_required,
            )?;
            let rendered = serde_json::to_string_pretty(&json).unwrap();
            println!("{rendered}");
            if require_cutover_evidence && !cutover_evidence_is_eligible(&json) {
                return Err(SkeinError::Execution(
                    "nowledge migration gate lacks eligible cutover evidence".to_string(),
                ));
            }
            if require_ready
                && json
                    .get("migration_gate")
                    .and_then(|gate| gate.get("decision"))
                    .and_then(serde_json::Value::as_str)
                    != Some("ready")
            {
                return Err(SkeinError::Execution(
                    "nowledge migration gate is blocked".to_string(),
                ));
            }
            return Ok(());
        }
        if command == "nowledge-replacement-summary" {
            let mut require_production_ready = false;
            let mut options = NowledgeReplacementSummaryOptions::default();
            let mut custom_summary_options = false;
            let mut search_projection_evidence_path = None;
            let mut search_projection_shadow_evidence_path = None;
            let mut search_candidate_shadow_evidence_path = None;
            let mut bounded_read_evidence_path = None;
            let mut query_runtime_preflight_path = None;
            let mut query_family_evidence_path = None;
            while let Some(flag) = args.peek() {
                match flag.as_str() {
                    "--require-production-ready" => {
                        require_production_ready = true;
                        args.next();
                    }
                    "--compact" => {
                        options.include_family_details = false;
                        options.include_blocker_details = false;
                        custom_summary_options = true;
                        args.next();
                    }
                    "--max-family-items" => {
                        args.next();
                        let raw_limit = args.next().ok_or_else(|| {
                            SkeinError::Semantic(nowledge_replacement_summary_usage())
                        })?;
                        options.max_family_items = Some(parse_max_family_items(&raw_limit)?);
                        custom_summary_options = true;
                    }
                    "--max-blockers" => {
                        args.next();
                        let raw_limit = args.next().ok_or_else(|| {
                            SkeinError::Semantic(nowledge_replacement_summary_usage())
                        })?;
                        options.max_blockers = Some(parse_max_blockers(&raw_limit)?);
                        custom_summary_options = true;
                    }
                    "--search-projection-evidence-json" => {
                        args.next();
                        search_projection_evidence_path = Some(args.next().ok_or_else(|| {
                            SkeinError::Semantic(nowledge_replacement_summary_usage())
                        })?);
                    }
                    "--search-projection-shadow-evidence-json" => {
                        args.next();
                        search_projection_shadow_evidence_path =
                            Some(args.next().ok_or_else(|| {
                                SkeinError::Semantic(nowledge_replacement_summary_usage())
                            })?);
                    }
                    "--search-candidate-shadow-evidence-json" => {
                        args.next();
                        search_candidate_shadow_evidence_path =
                            Some(args.next().ok_or_else(|| {
                                SkeinError::Semantic(nowledge_replacement_summary_usage())
                            })?);
                    }
                    "--bounded-read-evidence-json" => {
                        args.next();
                        bounded_read_evidence_path = Some(args.next().ok_or_else(|| {
                            SkeinError::Semantic(nowledge_replacement_summary_usage())
                        })?);
                    }
                    "--query-runtime-preflight-json" => {
                        args.next();
                        query_runtime_preflight_path = Some(args.next().ok_or_else(|| {
                            SkeinError::Semantic(nowledge_replacement_summary_usage())
                        })?);
                    }
                    "--query-family-evidence-json" => {
                        args.next();
                        query_family_evidence_path = Some(args.next().ok_or_else(|| {
                            SkeinError::Semantic(nowledge_replacement_summary_usage())
                        })?);
                    }
                    _ => break,
                }
            }
            let bundle_path = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(nowledge_replacement_summary_usage()))?;
            if args.next().is_some() {
                return Err(SkeinError::Semantic(nowledge_replacement_summary_usage()));
            }
            let mut bundle = read_json_file(Path::new(&bundle_path))?;
            merge_replacement_summary_evidence(
                &mut bundle,
                search_projection_evidence_path.as_deref(),
                search_projection_shadow_evidence_path.as_deref(),
                search_candidate_shadow_evidence_path.as_deref(),
                bounded_read_evidence_path.as_deref(),
                query_runtime_preflight_path.as_deref(),
                query_family_evidence_path.as_deref(),
            )?;
            let summary = if custom_summary_options {
                nowledge_replacement_summary_json_with_options(&bundle, options)
            } else {
                nowledge_replacement_summary_json(&bundle)
            };
            println!("{}", serde_json::to_string_pretty(&summary).unwrap());
            if require_production_ready
                && summary
                    .get("production_cutover_ready")
                    .and_then(serde_json::Value::as_bool)
                    != Some(true)
            {
                return Err(SkeinError::Execution(
                    "nowledge replacement summary is not production cutover ready".to_string(),
                ));
            }
            return Ok(());
        }
        if command == "background-maintenance-report" {
            let mut require_cutover_ready = false;
            let mut options = BackgroundMaintenanceReportOptions::default();
            while let Some(flag) = args.peek() {
                match flag.as_str() {
                    "--require-cutover-ready" => {
                        require_cutover_ready = true;
                        args.next();
                    }
                    "--disable-background" => {
                        options.policy.background_enabled = false;
                        args.next();
                    }
                    "--max-background-operations" => {
                        args.next();
                        let raw_limit = args.next().ok_or_else(|| {
                            SkeinError::Semantic(background_maintenance_report_usage())
                        })?;
                        options.policy.max_background_operations =
                            Some(parse_background_maintenance_limit(
                                "--max-background-operations",
                                &raw_limit,
                            )?);
                    }
                    "--max-total-background-operations" => {
                        args.next();
                        let raw_limit = args.next().ok_or_else(|| {
                            SkeinError::Semantic(background_maintenance_report_usage())
                        })?;
                        options.policy.max_total_background_operations =
                            Some(parse_background_maintenance_limit(
                                "--max-total-background-operations",
                                &raw_limit,
                            )?);
                    }
                    "--max-projection-background-operations" => {
                        args.next();
                        let raw_limit = args.next().ok_or_else(|| {
                            SkeinError::Semantic(background_maintenance_report_usage())
                        })?;
                        options.policy.max_background_operations_by_class
                            [WorkClass::Projection.as_index()] =
                            Some(parse_background_maintenance_limit(
                                "--max-projection-background-operations",
                                &raw_limit,
                            )?);
                    }
                    _ => break,
                }
            }
            let path = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(background_maintenance_report_usage()))?;
            if args.next().is_some() {
                return Err(SkeinError::Semantic(background_maintenance_report_usage()));
            }
            let db = Database::open_with_config(
                path,
                DatabaseConfig {
                    read_only: true,
                    ..DatabaseConfig::default()
                },
            )?;
            let report = background_maintenance_report_json_with_options(&db, &options);
            println!("{}", serde_json::to_string_pretty(&report).unwrap());
            if require_cutover_ready {
                let health = background_maintenance_evidence_health_from_bundle(
                    &serde_json::json!({ "background_maintenance": report }),
                    true,
                );
                if !health.ready {
                    let reason = if health.blocker_codes.is_empty() {
                        health.blockers.join("; ")
                    } else {
                        health.blocker_codes.join(",")
                    };
                    return Err(SkeinError::Execution(format!(
                        "background maintenance report is not cutover ready: {reason}"
                    )));
                }
            }
            return Ok(());
        }
        if command == "storage-resource-profile" {
            let mut require_ready = false;
            let mut require_fully_streamed = false;
            let mut parameters = BTreeMap::new();
            let mut segment_cache_capacity_bytes = None;
            let mut min_canonical_artifact_bytes = None;
            let mut max_steady_resident_bytes = None;
            let mut max_peak_resident_bytes = None;
            let mut max_total_page_faults = None;
            let mut max_minor_page_faults = None;
            let mut max_major_page_faults = None;
            let mut max_intermediate_rows = None;
            let mut max_intermediate_payload_bytes = None;
            let mut max_output_rows = None;
            let mut max_output_payload_bytes = None;
            while let Some(flag) = args.peek() {
                match flag.as_str() {
                    "--require-ready" => {
                        require_ready = true;
                        args.next();
                    }
                    "--require-fully-streamed" => {
                        require_fully_streamed = true;
                        args.next();
                    }
                    "--params-json" => {
                        args.next();
                        let raw = args.next().ok_or_else(|| {
                            SkeinError::Semantic(storage_resource_profile_usage())
                        })?;
                        parameters = parse_parameters_json(&raw)?;
                    }
                    "--segment-cache-bytes" => {
                        segment_cache_capacity_bytes =
                            Some(parse_next_u64_flag(&mut args, "--segment-cache-bytes")?);
                    }
                    "--min-canonical-bytes" => {
                        min_canonical_artifact_bytes =
                            Some(parse_next_u64_flag(&mut args, "--min-canonical-bytes")?);
                    }
                    "--max-steady-rss-bytes" => {
                        max_steady_resident_bytes =
                            Some(parse_next_u64_flag(&mut args, "--max-steady-rss-bytes")?);
                    }
                    "--max-peak-rss-bytes" => {
                        max_peak_resident_bytes =
                            Some(parse_next_u64_flag(&mut args, "--max-peak-rss-bytes")?);
                    }
                    "--max-total-page-faults" => {
                        max_total_page_faults =
                            Some(parse_next_u64_flag(&mut args, "--max-total-page-faults")?);
                    }
                    "--max-minor-page-faults" => {
                        max_minor_page_faults =
                            Some(parse_next_u64_flag(&mut args, "--max-minor-page-faults")?);
                    }
                    "--max-major-page-faults" => {
                        max_major_page_faults =
                            Some(parse_next_u64_flag(&mut args, "--max-major-page-faults")?);
                    }
                    "--max-intermediate-rows" => {
                        max_intermediate_rows =
                            Some(parse_next_usize_flag(&mut args, "--max-intermediate-rows")?);
                    }
                    "--max-intermediate-payload-bytes" => {
                        max_intermediate_payload_bytes = Some(parse_next_usize_flag(
                            &mut args,
                            "--max-intermediate-payload-bytes",
                        )?);
                    }
                    "--max-output-rows" => {
                        max_output_rows =
                            Some(parse_next_usize_flag(&mut args, "--max-output-rows")?);
                    }
                    "--max-output-payload-bytes" => {
                        max_output_payload_bytes = Some(parse_next_usize_flag(
                            &mut args,
                            "--max-output-payload-bytes",
                        )?);
                    }
                    _ => break,
                }
            }
            let path = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(storage_resource_profile_usage()))?;
            let cypher = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(storage_resource_profile_usage()))?;
            if args.next().is_some() {
                return Err(SkeinError::Semantic(storage_resource_profile_usage()));
            }
            let segment_cache_capacity_bytes = required_positive_profile_u64(
                segment_cache_capacity_bytes,
                "--segment-cache-bytes",
            )?;
            let limits = StorageResourceProfileLimits {
                min_canonical_artifact_bytes: required_positive_profile_u64(
                    min_canonical_artifact_bytes,
                    "--min-canonical-bytes",
                )?,
                max_steady_resident_bytes: required_positive_profile_u64(
                    max_steady_resident_bytes,
                    "--max-steady-rss-bytes",
                )?,
                max_peak_resident_bytes: required_positive_profile_u64(
                    max_peak_resident_bytes,
                    "--max-peak-rss-bytes",
                )?,
                max_total_page_faults,
                max_minor_page_faults,
                max_major_page_faults,
                max_intermediate_rows: required_profile_usize(
                    max_intermediate_rows,
                    "--max-intermediate-rows",
                )?,
                max_intermediate_payload_bytes: required_profile_usize(
                    max_intermediate_payload_bytes,
                    "--max-intermediate-payload-bytes",
                )?,
                max_output_rows: required_profile_usize(max_output_rows, "--max-output-rows")?,
                max_output_payload_bytes: required_profile_usize(
                    max_output_payload_bytes,
                    "--max-output-payload-bytes",
                )?,
                require_fully_streamed,
            };
            let db = Database::open_with_config(
                path,
                DatabaseConfig {
                    read_only: true,
                    segment_cache_capacity_bytes,
                    storage_residency_mode: StorageResidencyMode::OutOfCore,
                    max_read_result_rows: Some(limits.max_output_rows),
                    max_read_result_payload_bytes: Some(limits.max_output_payload_bytes),
                    ..DatabaseConfig::default()
                },
            )?;
            let report = db.storage_resource_profile(&cypher, &parameters, limits)?;
            println!("{}", serde_json::to_string_pretty(&report.json()).unwrap());
            if require_ready && !report.production_ready() {
                return Err(SkeinError::Execution(format!(
                    "storage resource profile is not ready: {}",
                    report.blocker_codes.join(",")
                )));
            }
            return Ok(());
        }
        if command == "storage-recovery-report" {
            let mut recovery_mode = RecoveryMode::default();
            let mut max_wal_replay_entries = None;
            let mut require_durable = false;
            let mut require_checkpoint_boundary = false;
            let mut require_bounded_wal_replay = false;
            let mut require_clean_tail = false;
            while let Some(flag) = args.peek() {
                match flag.as_str() {
                    "--strict" => {
                        recovery_mode = RecoveryMode::Strict;
                        args.next();
                    }
                    "--max-wal-replay-entries" => {
                        args.next();
                        let raw_limit = args
                            .next()
                            .ok_or_else(|| SkeinError::Semantic(storage_recovery_report_usage()))?;
                        max_wal_replay_entries = Some(parse_max_wal_replay_entries(&raw_limit)?);
                    }
                    "--require-durable" => {
                        require_durable = true;
                        args.next();
                    }
                    "--require-checkpoint-boundary" => {
                        require_checkpoint_boundary = true;
                        args.next();
                    }
                    "--require-bounded-wal-replay" => {
                        require_bounded_wal_replay = true;
                        args.next();
                    }
                    "--require-clean-tail" => {
                        require_clean_tail = true;
                        args.next();
                    }
                    _ => break,
                }
            }
            let path = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(storage_recovery_report_usage()))?;
            if args.next().is_some() {
                return Err(SkeinError::Semantic(storage_recovery_report_usage()));
            }
            let db = Database::open_with_config(
                path,
                DatabaseConfig {
                    read_only: true,
                    recovery_mode,
                    max_wal_replay_entries,
                    ..DatabaseConfig::default()
                },
            )?;
            let rendered =
                storage_recovery_report_json(db.storage_version(), &db.storage_recovery_report());
            println!("{}", serde_json::to_string_pretty(&rendered).unwrap());
            enforce_storage_recovery_requirements(
                &db.storage_recovery_report(),
                StorageRecoveryRequirements {
                    require_durable,
                    require_checkpoint_boundary,
                    require_bounded_wal_replay,
                    require_clean_tail,
                },
            )?;
            return Ok(());
        }
        if command == "validate-canonical-snapshot" {
            let mut require_valid = false;
            let mut require_import_ready = false;
            while let Some(flag) = args.peek() {
                match flag.as_str() {
                    "--require-valid" => {
                        require_valid = true;
                        args.next();
                    }
                    "--require-import-ready" => {
                        require_import_ready = true;
                        args.next();
                    }
                    _ => break,
                }
            }
            let path = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(validate_canonical_snapshot_usage()))?;
            if args.next().is_some() {
                return Err(SkeinError::Semantic(validate_canonical_snapshot_usage()));
            }
            let db = Database::open_with_config(
                path,
                DatabaseConfig {
                    read_only: true,
                    ..DatabaseConfig::default()
                },
            )?;
            let snapshot = db.try_export_canonical_graph_snapshot()?;
            let validation = snapshot.validate();
            let rendered = canonical_snapshot_validation_json(
                snapshot.graph_commit_epoch,
                snapshot.logical_checksum,
                snapshot.nodes.len(),
                snapshot.relationships.len(),
                &validation,
            );
            println!("{}", serde_json::to_string_pretty(&rendered).unwrap());
            if require_valid && !validation.is_valid {
                return Err(SkeinError::Execution(
                    "canonical snapshot validation failed".to_string(),
                ));
            }
            if require_import_ready && !validation.is_import_ready {
                return Err(SkeinError::Execution(
                    "canonical snapshot import readiness failed".to_string(),
                ));
            }
            return Ok(());
        }
        if command == "graph-lightning-bootstrap-manifest" {
            let mut require_ready = false;
            while let Some(flag) = args.peek() {
                match flag.as_str() {
                    "--require-ready" => {
                        require_ready = true;
                        args.next();
                    }
                    _ => break,
                }
            }
            let path = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(graph_lightning_bootstrap_manifest_usage()))?;
            if args.next().is_some() {
                return Err(SkeinError::Semantic(
                    graph_lightning_bootstrap_manifest_usage(),
                ));
            }
            let mut db = Database::open(path)?;
            let export = db.prepare_graph_lightning_bootstrap_export()?;
            let rendered = graph_lightning_bootstrap_manifest_json(&export.manifest);
            println!("{}", serde_json::to_string_pretty(&rendered).unwrap());
            if require_ready && !export.manifest.validation.is_import_ready {
                return Err(SkeinError::Execution(
                    "graph lightning bootstrap manifest is not import ready".to_string(),
                ));
            }
            return Ok(());
        }
        if command == "graph-lightning-bootstrap-bundle" {
            let mut require_ready = false;
            while let Some(flag) = args.peek() {
                match flag.as_str() {
                    "--require-ready" => {
                        require_ready = true;
                        args.next();
                    }
                    _ => break,
                }
            }
            let path = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(graph_lightning_bootstrap_bundle_usage()))?;
            if args.next().is_some() {
                return Err(SkeinError::Semantic(
                    graph_lightning_bootstrap_bundle_usage(),
                ));
            }
            let mut db = Database::open(path)?;
            let export = db.prepare_graph_lightning_bootstrap_export()?;
            let rendered = graph_lightning_bootstrap_bundle_json_with_storage_recovery(
                &export,
                db.storage_version(),
                &db.storage_recovery_report(),
            );
            println!("{}", serde_json::to_string_pretty(&rendered).unwrap());
            if require_ready
                && rendered
                    .get("export_gate")
                    .and_then(|gate| gate.get("decision"))
                    .and_then(serde_json::Value::as_str)
                    != Some("ready")
            {
                return Err(SkeinError::Execution(
                    "graph lightning bootstrap bundle is not ready".to_string(),
                ));
            }
            return Ok(());
        }
        if command == "graph-lightning-stage-bootstrap" {
            let mut require_ready = false;
            while let Some(flag) = args.peek() {
                match flag.as_str() {
                    "--require-ready" => {
                        require_ready = true;
                        args.next();
                    }
                    _ => break,
                }
            }
            let database_path = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(graph_lightning_stage_bootstrap_usage()))?;
            let staging_dir = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(graph_lightning_stage_bootstrap_usage()))?;
            if args.next().is_some() {
                return Err(SkeinError::Semantic(graph_lightning_stage_bootstrap_usage()));
            }
            let mut db = Database::open(database_path)?;
            let export = db.prepare_graph_lightning_bootstrap_export()?;
            let catalog = stage_graph_lightning_bootstrap_export_with_storage_recovery(
                &export,
                staging_dir,
                db.storage_version(),
                &db.storage_recovery_report(),
            )?;
            println!("{}", serde_json::to_string_pretty(&catalog).unwrap());
            if require_ready
                && catalog
                    .get("export_gate")
                    .and_then(|gate| gate.get("decision"))
                    .and_then(serde_json::Value::as_str)
                    != Some("ready")
            {
                return Err(SkeinError::Execution(
                    "graph lightning staged bootstrap is not ready".to_string(),
                ));
            }
            return Ok(());
        }
        if command == "graph-lightning-verify-staging" {
            let mut require_ready = false;
            while let Some(flag) = args.peek() {
                match flag.as_str() {
                    "--require-ready" => {
                        require_ready = true;
                        args.next();
                    }
                    _ => break,
                }
            }
            let staging_dir = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(graph_lightning_verify_staging_usage()))?;
            if args.next().is_some() {
                return Err(SkeinError::Semantic(graph_lightning_verify_staging_usage()));
            }
            let report = verify_graph_lightning_staging_catalog(staging_dir)?;
            println!("{}", serde_json::to_string_pretty(&report).unwrap());
            if require_ready
                && report
                    .get("validation_gate")
                    .and_then(|gate| gate.get("decision"))
                    .and_then(serde_json::Value::as_str)
                    != Some("ready")
            {
                return Err(SkeinError::Execution(
                    "graph lightning staging verification is not ready".to_string(),
                ));
            }
            return Ok(());
        }
        if command == "graph-lightning-publish-staging" {
            let (options, staging_dir, publish_dir) =
                parse_graph_lightning_publish_staging_args(args)?;
            let report = if options == PublishGraphLightningOptions::default() {
                publish_graph_lightning_staging_catalog(staging_dir, publish_dir)?
            } else {
                publish_graph_lightning_staging_catalog_with_options(
                    staging_dir,
                    publish_dir,
                    options,
                )?
            };
            println!("{}", serde_json::to_string_pretty(&report).unwrap());
            return Ok(());
        }
        if command == "graph-lightning-verify-published" {
            let staging_dir = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(graph_lightning_verify_published_usage()))?;
            let publish_dir = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(graph_lightning_verify_published_usage()))?;
            if args.next().is_some() {
                return Err(SkeinError::Semantic(
                    graph_lightning_verify_published_usage(),
                ));
            }
            let report = verify_graph_lightning_published_manifest(staging_dir, publish_dir)?;
            println!("{}", serde_json::to_string_pretty(&report).unwrap());
            return Ok(());
        }
        if command == "graph-lightning-gc-staging-report" {
            let staging_dir = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(graph_lightning_gc_staging_report_usage()))?;
            let publish_dir = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(graph_lightning_gc_staging_report_usage()))?;
            if args.next().is_some() {
                return Err(SkeinError::Semantic(
                    graph_lightning_gc_staging_report_usage(),
                ));
            }
            let report = graph_lightning_gc_staging_report(staging_dir, publish_dir)?;
            println!("{}", serde_json::to_string_pretty(&report).unwrap());
            return Ok(());
        }
        if command == "graph-lightning-import-status" {
            let staging_dir = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(graph_lightning_import_status_usage()))?;
            let publish_dir = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(graph_lightning_import_status_usage()))?;
            if args.next().is_some() {
                return Err(SkeinError::Semantic(graph_lightning_import_status_usage()));
            }
            let report = graph_lightning_import_status(staging_dir, publish_dir)?;
            println!("{}", serde_json::to_string_pretty(&report).unwrap());
            return Ok(());
        }
        if command == "graph-lightning-graph-stream" {
            let mut require_ready = false;
            while let Some(flag) = args.peek() {
                match flag.as_str() {
                    "--require-ready" => {
                        require_ready = true;
                        args.next();
                    }
                    _ => break,
                }
            }
            let path = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(graph_lightning_graph_stream_usage()))?;
            if args.next().is_some() {
                return Err(SkeinError::Semantic(graph_lightning_graph_stream_usage()));
            }
            let mut db = Database::open(path)?;
            let export = db.prepare_graph_lightning_bootstrap_export()?;
            if require_ready && !export.manifest.validation.is_import_ready {
                return Err(SkeinError::Execution(
                    "graph lightning graph stream is not import ready".to_string(),
                ));
            }
            print!("{}", export.graph_stream.encoded);
            return Ok(());
        }
        if command == "graph-lightning-verify-export" {
            let mut require_valid = false;
            while let Some(flag) = args.peek() {
                match flag.as_str() {
                    "--require-valid" => {
                        require_valid = true;
                        args.next();
                    }
                    _ => break,
                }
            }
            let path = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(graph_lightning_verify_export_usage()))?;
            if args.next().is_some() {
                return Err(SkeinError::Semantic(graph_lightning_verify_export_usage()));
            }
            let mut db = Database::open(path)?;
            let export = db.prepare_graph_lightning_bootstrap_export()?;
            let validation = export
                .graph_stream
                .validate_against_manifest(&export.manifest);
            let rendered = graph_lightning_graph_stream_validation_json(&validation);
            println!("{}", serde_json::to_string_pretty(&rendered).unwrap());
            if require_valid && !validation.is_valid {
                return Err(SkeinError::Execution(
                    "graph lightning graph stream validation failed".to_string(),
                ));
            }
            return Ok(());
        }
        return Err(SkeinError::Semantic(format!("unknown command '{command}'")));
    }

    let path = std::env::temp_dir().join("skein-demo");
    let _ = std::fs::remove_dir_all(&path);
    let mut db = Database::open(&path)?;
    db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")?;
    db.query("CREATE (:Memory {id: 2, title: 'Runtime strategy'})")?;
    db.checkpoint()?;

    let query = "MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title";
    let explain = db.explain_query(query)?;
    println!("{}", explain.trace.selected_plan);

    drop(db);
    let mut db = Database::open(&path)?;
    let output = db.query(query)?;
    for row in output.rows {
        println!("{row:?}");
    }

    Ok(())
}

fn nowledge_cypher_migration_gate_usage() -> String {
    "nowledge-cypher-migration-gate is a developer compatibility tool; set SKEIN_ENABLE_COMPATIBILITY_TOOLS=1. Usage: nowledge-cypher-migration-gate requires [--require-ready] [--require-cutover-evidence] [--allow-self-shadow] [--shadow-ready] [--shadow-trace <path>] [--shadow-timeout-ms <ms>] [--require-rollback-evidence] [--rollback-evidence <text>] [--require-storage-recovery-evidence] [--storage-recovery-report-json <path>] [--require-background-maintenance-evidence] [--background-maintenance-report-json <path>] [--previous-wrapper-contract-evidence-json <path>] <root> <shadow-name> <program> [args...]"
        .to_string()
}

fn external_shadow_adapter_smoke_usage() -> String {
    "external-shadow-adapter-smoke is a developer compatibility tool; set SKEIN_ENABLE_COMPATIBILITY_TOOLS=1. Usage: external-shadow-adapter-smoke requires [--require-previous-wrapper] [--shadow-trace <path>] [--shadow-timeout-ms <ms>] <shadow-name> <program> [args...]"
        .to_string()
}

fn require_developer_compatibility_tool(command: &str) -> Result<()> {
    if compatibility_tools_enabled_from_value(
        std::env::var(SKEIN_ENABLE_COMPATIBILITY_TOOLS_ENV)
            .ok()
            .as_deref(),
    ) {
        return Ok(());
    }
    Err(SkeinError::Semantic(format!(
        "{command} is quarantined as a developer compatibility tool; production callers must use Skein library APIs and typed readiness gates. Set {SKEIN_ENABLE_COMPATIBILITY_TOOLS_ENV}=1 only for isolated preflight or CI validation."
    )))
}

fn compatibility_tools_enabled_from_value(value: Option<&str>) -> bool {
    value
        .map(str::trim)
        .is_some_and(|value| matches!(value, "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"))
}

fn explain_json_usage() -> String {
    "explain-json requires [--params-json <json-object>] <database-path> <cypher>".to_string()
}

fn explain_table_usage(command: &str) -> String {
    format!("{command} requires [--params-json <json-object>] <database-path> <cypher>")
}

fn explain_analyze_json_usage() -> String {
    "explain-analyze-json requires [--params-json <json-object>] <database-path> <cypher>"
        .to_string()
}

fn validate_canonical_snapshot_usage() -> String {
    "validate-canonical-snapshot requires [--require-valid] [--require-import-ready] <database-path>"
        .to_string()
}

fn storage_recovery_report_usage() -> String {
    "storage-recovery-report requires [--strict] [--max-wal-replay-entries <n>] [--require-durable] [--require-checkpoint-boundary] [--require-bounded-wal-replay] [--require-clean-tail] <database-path>"
        .to_string()
}

fn storage_resource_profile_usage() -> String {
    "storage-resource-profile requires [--require-ready] [--require-fully-streamed] [--params-json <json-object>] --segment-cache-bytes <n> --min-canonical-bytes <n> --max-steady-rss-bytes <n> --max-peak-rss-bytes <n> [--max-total-page-faults <n>] [--max-minor-page-faults <n>] [--max-major-page-faults <n>] --max-intermediate-rows <n> --max-intermediate-payload-bytes <n> --max-output-rows <n> --max-output-payload-bytes <n> <database-path> <cypher>"
        .to_string()
}

fn nowledge_bounded_read_report_usage() -> String {
    "nowledge-bounded-read-report requires [--params-json <json-object>] [--max-rows <n>] [--max-estimated-payload-bytes <n>] <database-path> <cypher>".to_string()
}

fn nowledge_bounded_read_report_json(
    path: impl AsRef<Path>,
    query: &str,
    parameters: &BTreeMap<String, Value>,
    options: &NowledgeMemReadOptions,
) -> Result<serde_json::Value> {
    let graph = NowledgeMemGraph::open(path, NowledgeMemGraphMode::ShadowReadOnly)?;
    let read = graph.read_query_with_params(query, parameters, options)?;
    Ok(read.report.json())
}

fn background_maintenance_report_usage() -> String {
    "background-maintenance-report requires [--require-cutover-ready] [--disable-background] [--max-background-operations <n>] [--max-total-background-operations <n>] [--max-projection-background-operations <n>] <database-path>"
        .to_string()
}

fn graph_lightning_bootstrap_manifest_usage() -> String {
    "graph-lightning-bootstrap-manifest requires [--require-ready] <database-path>".to_string()
}

fn graph_lightning_bootstrap_bundle_usage() -> String {
    "graph-lightning-bootstrap-bundle requires [--require-ready] <database-path>".to_string()
}

fn graph_lightning_stage_bootstrap_usage() -> String {
    "graph-lightning-stage-bootstrap requires [--require-ready] <database-path> <staging-dir>"
        .to_string()
}

fn graph_lightning_verify_staging_usage() -> String {
    "graph-lightning-verify-staging requires [--require-ready] <staging-dir>".to_string()
}

fn graph_lightning_publish_staging_usage() -> String {
    "graph-lightning-publish-staging requires [--require-state-marker] [--fencing-token <token>] [--expected-graph-epoch <epoch>] <staging-dir> <publish-dir>".to_string()
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct PublishGraphLightningOptions {
    require_state_marker: bool,
    fencing_token: Option<String>,
    expected_graph_epoch: Option<u64>,
}

fn parse_graph_lightning_publish_staging_args(
    args: impl Iterator<Item = String>,
) -> Result<(PublishGraphLightningOptions, String, String)> {
    let mut options = PublishGraphLightningOptions::default();
    let mut positional = Vec::new();
    let mut args = args.peekable();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--require-state-marker" => {
                options.require_state_marker = true;
            }
            "--fencing-token" => {
                let Some(value) = args.next() else {
                    return Err(SkeinError::Semantic(graph_lightning_publish_staging_usage()));
                };
                options.fencing_token = Some(value);
            }
            "--expected-graph-epoch" => {
                let Some(value) = args.next() else {
                    return Err(SkeinError::Semantic(graph_lightning_publish_staging_usage()));
                };
                let epoch = value
                    .parse::<u64>()
                    .map_err(|_| SkeinError::Semantic(graph_lightning_publish_staging_usage()))?;
                options.expected_graph_epoch = Some(epoch);
            }
            value if value.starts_with("--") => {
                return Err(SkeinError::Semantic(graph_lightning_publish_staging_usage()));
            }
            value => positional.push(value.to_string()),
        }
    }
    if positional.len() != 2 {
        return Err(SkeinError::Semantic(graph_lightning_publish_staging_usage()));
    }
    Ok((options, positional.remove(0), positional.remove(0)))
}

fn graph_lightning_verify_published_usage() -> String {
    "graph-lightning-verify-published requires <staging-dir> <publish-dir>".to_string()
}

fn graph_lightning_gc_staging_report_usage() -> String {
    "graph-lightning-gc-staging-report requires <staging-dir> <publish-dir>".to_string()
}

fn graph_lightning_import_status_usage() -> String {
    "graph-lightning-import-status requires <staging-dir> <publish-dir>".to_string()
}

fn graph_lightning_graph_stream_usage() -> String {
    "graph-lightning-graph-stream requires [--require-ready] <database-path>".to_string()
}

fn graph_lightning_verify_export_usage() -> String {
    "graph-lightning-verify-export requires [--require-valid] <database-path>".to_string()
}

fn parse_shadow_timeout_ms(raw_timeout: &str) -> Result<Duration> {
    let timeout_ms = raw_timeout.parse::<u64>().map_err(|error| {
        SkeinError::Semantic(format!(
            "invalid --shadow-timeout-ms '{raw_timeout}': {error}"
        ))
    })?;
    if timeout_ms == 0 {
        return Err(SkeinError::Semantic(
            "--shadow-timeout-ms must be greater than zero".to_string(),
        ));
    }
    Ok(Duration::from_millis(timeout_ms))
}

fn parse_max_wal_replay_entries(raw_limit: &str) -> Result<usize> {
    let limit = raw_limit.parse::<usize>().map_err(|error| {
        SkeinError::Semantic(format!(
            "invalid --max-wal-replay-entries '{raw_limit}': {error}"
        ))
    })?;
    if limit == 0 {
        return Err(SkeinError::Semantic(
            "--max-wal-replay-entries must be greater than zero".to_string(),
        ));
    }
    Ok(limit)
}

fn parse_max_family_items(raw_limit: &str) -> Result<usize> {
    let limit = raw_limit.parse::<usize>().map_err(|error| {
        SkeinError::Semantic(format!("invalid --max-family-items '{raw_limit}': {error}"))
    })?;
    if limit == 0 {
        return Err(SkeinError::Semantic(
            "--max-family-items must be greater than zero".to_string(),
        ));
    }
    Ok(limit)
}

fn parse_max_blockers(raw_limit: &str) -> Result<usize> {
    let limit = raw_limit.parse::<usize>().map_err(|error| {
        SkeinError::Semantic(format!("invalid --max-blockers '{raw_limit}': {error}"))
    })?;
    if limit == 0 {
        return Err(SkeinError::Semantic(
            "--max-blockers must be greater than zero".to_string(),
        ));
    }
    Ok(limit)
}

fn parse_positive_usize(flag: &str, raw_value: &str) -> Result<usize> {
    let value = raw_value
        .parse::<usize>()
        .map_err(|error| SkeinError::Semantic(format!("invalid {flag} '{raw_value}': {error}")))?;
    if value == 0 {
        return Err(SkeinError::Semantic(format!(
            "{flag} must be greater than zero"
        )));
    }
    Ok(value)
}

fn parse_next_u64_flag<I>(args: &mut std::iter::Peekable<I>, expected_flag: &str) -> Result<u64>
where
    I: Iterator<Item = String>,
{
    let flag = args
        .next()
        .ok_or_else(|| SkeinError::Semantic(storage_resource_profile_usage()))?;
    debug_assert_eq!(flag, expected_flag);
    let raw = args
        .next()
        .ok_or_else(|| SkeinError::Semantic(storage_resource_profile_usage()))?;
    raw.parse::<u64>()
        .map_err(|error| SkeinError::Semantic(format!("invalid {expected_flag} '{raw}': {error}")))
}

fn parse_next_usize_flag<I>(args: &mut std::iter::Peekable<I>, expected_flag: &str) -> Result<usize>
where
    I: Iterator<Item = String>,
{
    let flag = args
        .next()
        .ok_or_else(|| SkeinError::Semantic(storage_resource_profile_usage()))?;
    debug_assert_eq!(flag, expected_flag);
    let raw = args
        .next()
        .ok_or_else(|| SkeinError::Semantic(storage_resource_profile_usage()))?;
    parse_positive_usize(expected_flag, &raw)
}

fn required_positive_profile_u64(value: Option<u64>, flag: &str) -> Result<u64> {
    match value {
        Some(value) if value > 0 => Ok(value),
        Some(_) => Err(SkeinError::Semantic(format!(
            "{flag} must be greater than zero"
        ))),
        None => Err(SkeinError::Semantic(format!(
            "storage-resource-profile is missing {flag}"
        ))),
    }
}

fn required_profile_usize(value: Option<usize>, flag: &str) -> Result<usize> {
    value.ok_or_else(|| SkeinError::Semantic(format!("storage-resource-profile is missing {flag}")))
}

fn merge_replacement_summary_evidence(
    bundle: &mut serde_json::Value,
    search_projection_evidence_path: Option<&str>,
    search_projection_shadow_evidence_path: Option<&str>,
    search_candidate_shadow_evidence_path: Option<&str>,
    bounded_read_evidence_path: Option<&str>,
    query_runtime_preflight_path: Option<&str>,
    query_family_evidence_path: Option<&str>,
) -> Result<()> {
    if let Some(path) = search_projection_evidence_path {
        insert_replacement_summary_artifact(
            bundle,
            "search_projection_evidence",
            read_json_file(Path::new(path))?,
        )?;
    }
    if let Some(path) = search_projection_shadow_evidence_path {
        insert_replacement_summary_artifact(
            bundle,
            "search_projection_shadow_evidence",
            read_json_file(Path::new(path))?,
        )?;
    }
    if let Some(path) = search_candidate_shadow_evidence_path {
        insert_replacement_summary_artifact(
            bundle,
            "search_candidate_shadow_evidence",
            read_json_file(Path::new(path))?,
        )?;
    }
    if let Some(path) = bounded_read_evidence_path {
        insert_replacement_summary_artifact(
            bundle,
            "bounded_read_evidence",
            read_json_file(Path::new(path))?,
        )?;
    }
    if let Some(path) = query_runtime_preflight_path {
        insert_replacement_summary_artifact(
            bundle,
            "query_runtime_preflight",
            read_json_file(Path::new(path))?,
        )?;
    }
    if let Some(path) = query_family_evidence_path {
        let query_family_evidence = read_json_file(Path::new(path))?;
        let families = query_family_evidence
            .get("replacement_readiness_by_query_family")
            .cloned()
            .ok_or_else(|| {
                SkeinError::Semantic(
                    "query family evidence missing replacement_readiness_by_query_family"
                        .to_string(),
                )
            })?;
        insert_replacement_summary_artifact(
            bundle,
            "query_family_evidence",
            query_family_evidence,
        )?;
        insert_replacement_summary_artifact(
            bundle,
            "replacement_readiness_by_query_family",
            families,
        )?;
    }
    Ok(())
}

fn insert_replacement_summary_artifact(
    bundle: &mut serde_json::Value,
    key: &str,
    value: serde_json::Value,
) -> Result<()> {
    bundle
        .as_object_mut()
        .ok_or_else(|| {
            SkeinError::Semantic("replacement summary bundle must be a JSON object".to_string())
        })?
        .insert(key.to_string(), value);
    Ok(())
}

fn parse_background_maintenance_limit(flag: &str, raw_limit: &str) -> Result<usize> {
    let limit = raw_limit
        .parse::<usize>()
        .map_err(|error| SkeinError::Semantic(format!("invalid {flag} '{raw_limit}': {error}")))?;
    if limit == 0 {
        return Err(SkeinError::Semantic(format!(
            "{flag} must be greater than zero"
        )));
    }
    Ok(limit)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct StorageRecoveryRequirements {
    require_durable: bool,
    require_checkpoint_boundary: bool,
    require_bounded_wal_replay: bool,
    require_clean_tail: bool,
}

fn enforce_storage_recovery_requirements(
    report: &StorageRecoveryReport,
    requirements: StorageRecoveryRequirements,
) -> Result<()> {
    let mut blockers = Vec::new();
    if requirements.require_durable && !report.durable {
        blockers.push("durable recovery was not observed");
    }
    if requirements.require_checkpoint_boundary && report.checkpoint_epoch.is_none() {
        blockers.push("checkpoint boundary is missing");
    }
    if requirements.require_bounded_wal_replay && report.max_wal_replay_entries.is_none() {
        blockers.push("WAL replay was not opened with a configured entry bound");
    }
    if requirements.require_clean_tail && report.torn_tail_ignored {
        blockers.push("torn WAL tail was ignored during recovery");
    }
    if blockers.is_empty() {
        return Ok(());
    }
    Err(SkeinError::Execution(format!(
        "storage recovery report requirements failed: {}",
        blockers.join("; ")
    )))
}

fn insert_json<T: serde::Serialize>(
    object: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: T,
) {
    object.insert(
        key.to_string(),
        serde_json::to_value(value).expect("cutover evidence values must serialize"),
    );
}

fn add_shadow_ready_report(
    bundle: &mut serde_json::Value,
    ready: &ExternalShadowReady,
) -> Result<()> {
    let object = bundle.as_object_mut().ok_or_else(|| {
        SkeinError::Execution("migration gate bundle must be a JSON object".to_string())
    })?;
    object.insert(
        "shadow_ready".to_string(),
        serde_json::json!({
            "protocol_version": ready.protocol_version,
            "capabilities": &ready.capabilities,
            "engine_kind": &ready.engine_kind,
            "wrapper_identity": &ready.wrapper_identity,
        }),
    );
    Ok(())
}

fn add_shadow_run_report(
    bundle: &mut serde_json::Value,
    shadow_name: &str,
    self_shadow: bool,
) -> Result<()> {
    let object = bundle.as_object_mut().ok_or_else(|| {
        SkeinError::Execution("migration gate bundle must be a JSON object".to_string())
    })?;
    object.insert(
        "shadow_run".to_string(),
        serde_json::json!({
            "shadow_name": shadow_name,
            "self_shadow": self_shadow,
            "evidence_kind": if self_shadow {
                "protocol_smoke"
            } else {
                "previous_wrapper"
            },
        }),
    );
    Ok(())
}

fn add_cutover_evidence_report(
    bundle: &mut serde_json::Value,
    self_shadow: bool,
    shadow_ready: Option<&ExternalShadowReady>,
    storage_recovery_required: bool,
    background_maintenance_required: bool,
) -> Result<()> {
    let evidence_kind = if self_shadow {
        "protocol_smoke"
    } else {
        "previous_wrapper"
    };
    let ready_preflight = shadow_ready.is_some();
    let ready_engine_kind = shadow_ready.and_then(|ready| ready.engine_kind.as_deref());
    let ready_wrapper_identity = shadow_ready.and_then(|ready| ready.wrapper_identity.as_deref());
    let ready_missing_capabilities = external_shadow_ready_missing_capabilities(shadow_ready);
    let migration_gate = bundle
        .get("migration_gate")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| {
            SkeinError::Execution("migration gate bundle missing migration_gate".to_string())
        })?;
    let migration_gate_ready = migration_gate
        .get("decision")
        .and_then(serde_json::Value::as_str)
        == Some("ready");
    let shadow_evidence_present = migration_gate
        .get("shadow_evidence_present")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let shadow_trace_health = external_shadow_trace_health_from_bundle(bundle);
    let storage_recovery_health =
        storage_recovery_evidence_health_from_bundle(bundle, storage_recovery_required);
    let background_maintenance_health =
        background_maintenance_evidence_health_from_bundle(bundle, background_maintenance_required);
    let replacement_family_health =
        replacement_readiness_family_evidence_health_from_bundle(bundle);
    let mut blockers = Vec::new();
    if self_shadow {
        blockers.push("shadow run is protocol smoke, not previous-wrapper evidence".to_string());
    }
    if !ready_preflight {
        blockers.push("shadow ready preflight was not executed".to_string());
    }
    if ready_preflight && ready_engine_kind.is_none() {
        blockers.push("shadow ready response missing engine_kind".to_string());
    }
    if let Some(engine_kind) = ready_engine_kind
        && engine_kind != "previous_wrapper"
    {
        blockers.push("shadow ready engine_kind is not previous_wrapper".to_string());
    }
    if ready_preflight
        && ready_engine_kind == Some("previous_wrapper")
        && ready_wrapper_identity.is_none()
    {
        blockers.push("shadow ready response missing wrapper_identity".to_string());
    }
    if ready_preflight && !ready_missing_capabilities.is_empty() {
        blockers.push("shadow ready response missing required capabilities".to_string());
    }
    if !shadow_evidence_present {
        blockers.push("no matched shadow checks are present".to_string());
    }
    if shadow_trace_health.present && !shadow_trace_health.complete {
        blockers.push("shadow trace is incomplete or unavailable".to_string());
    }
    if !storage_recovery_health.ready {
        blockers.extend(storage_recovery_health.blockers.iter().cloned());
    }
    if !background_maintenance_health.ready {
        blockers.extend(background_maintenance_health.blockers.iter().cloned());
    }
    if !replacement_family_health.ready {
        blockers.extend(replacement_family_health.blockers.iter().cloned());
    }
    if !migration_gate_ready {
        blockers.push("migration gate decision is not ready".to_string());
    }

    let object = bundle.as_object_mut().ok_or_else(|| {
        SkeinError::Execution("migration gate bundle must be a JSON object".to_string())
    })?;
    let mut evidence = serde_json::Map::new();
    insert_json(&mut evidence, "eligible", blockers.is_empty());
    insert_json(&mut evidence, "evidence_kind", evidence_kind);
    insert_json(&mut evidence, "requires_previous_wrapper", true);
    insert_json(&mut evidence, "requires_ready_preflight", true);
    insert_json(
        &mut evidence,
        "requires_ready_engine_kind",
        "previous_wrapper",
    );
    insert_json(&mut evidence, "requires_ready_wrapper_identity", true);
    insert_json(
        &mut evidence,
        "requires_ready_capabilities",
        REQUIRED_EXTERNAL_SHADOW_CAPABILITIES,
    );
    insert_json(&mut evidence, "requires_shadow_evidence", true);
    insert_json(&mut evidence, "ready_preflight", ready_preflight);
    insert_json(&mut evidence, "ready_engine_kind", ready_engine_kind);
    insert_json(
        &mut evidence,
        "ready_wrapper_identity",
        ready_wrapper_identity,
    );
    insert_json(
        &mut evidence,
        "ready_missing_capabilities",
        ready_missing_capabilities,
    );
    insert_json(
        &mut evidence,
        "shadow_evidence_present",
        shadow_evidence_present,
    );
    insert_json(
        &mut evidence,
        "shadow_trace_present",
        shadow_trace_health.present,
    );
    insert_json(
        &mut evidence,
        "shadow_trace_complete",
        shadow_trace_health.complete,
    );
    insert_json(
        &mut evidence,
        "shadow_trace_summary_available",
        shadow_trace_health.summary_available,
    );
    insert_json(
        &mut evidence,
        "shadow_trace_request_count_matches",
        shadow_trace_health.request_count_matches,
    );
    insert_json(
        &mut evidence,
        "shadow_trace_pending_request_count",
        shadow_trace_health.pending_request_count,
    );
    insert_json(
        &mut evidence,
        "storage_recovery_required",
        storage_recovery_health.required,
    );
    insert_json(
        &mut evidence,
        "storage_recovery_present",
        storage_recovery_health.present,
    );
    insert_json(
        &mut evidence,
        "storage_recovery_ready",
        storage_recovery_health.ready,
    );
    insert_json(
        &mut evidence,
        "storage_recovery_protocol_matches",
        storage_recovery_health.protocol_matches,
    );
    insert_json(
        &mut evidence,
        "storage_recovery_durable",
        storage_recovery_health.durable_recovery_observed,
    );
    insert_json(
        &mut evidence,
        "storage_recovery_checkpoint_boundary_present",
        storage_recovery_health.checkpoint_boundary_present,
    );
    insert_json(
        &mut evidence,
        "storage_recovery_wal_replay_bounded",
        storage_recovery_health.wal_replay_bounded,
    );
    insert_json(
        &mut evidence,
        "storage_recovery_replay_boundary_consistent",
        storage_recovery_health.replay_boundary_consistent,
    );
    insert_json(
        &mut evidence,
        "storage_recovery_torn_tail_clean",
        storage_recovery_health.torn_tail_clean,
    );
    insert_json(
        &mut evidence,
        "storage_recovery_blocker_codes",
        storage_recovery_health.blocker_codes,
    );
    insert_json(
        &mut evidence,
        "storage_recovery_blockers",
        storage_recovery_health.blockers,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_required",
        background_maintenance_health.required,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_present",
        background_maintenance_health.present,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_ready",
        background_maintenance_health.ready,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_protocol_matches",
        background_maintenance_health.protocol_matches,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_total_candidates",
        background_maintenance_health.total_candidates,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_ranked_count",
        background_maintenance_health.ranked_count,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_executable_search_projection_graph_delta_count",
        background_maintenance_health.executable_search_projection_graph_delta_count,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_admitted_search_projection_graph_delta_count",
        background_maintenance_health.admitted_search_projection_graph_delta_count,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_deferred_search_projection_graph_delta_count",
        background_maintenance_health.deferred_search_projection_graph_delta_count,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_rejected_search_projection_graph_delta_count",
        background_maintenance_health.rejected_search_projection_graph_delta_count,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_executable_search_projection_graph_delta_operations",
        background_maintenance_health.executable_search_projection_graph_delta_operations,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_admitted_search_projection_graph_delta_operations",
        background_maintenance_health.admitted_search_projection_graph_delta_operations,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch",
        background_maintenance_health
            .max_search_projection_graph_delta_complete_through_graph_commit_epoch,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_foreground_admission_probe_ready",
        background_maintenance_health.foreground_admission_probe_ready,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_foreground_admission_probe_admission",
        background_maintenance_health
            .foreground_admission_probe_admission_name
            .as_deref(),
    );
    insert_json(
        &mut evidence,
        "background_maintenance_memory_pressure_ready",
        background_maintenance_health.memory_pressure_ready,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_memory_budget_bytes",
        background_maintenance_health.memory_budget_bytes,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_estimated_memory_bytes",
        background_maintenance_health.estimated_memory_bytes,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_qos_snapshot_ready",
        background_maintenance_health.qos_snapshot_ready,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_qos_snapshot_foreground_admitted",
        background_maintenance_health.qos_snapshot_foreground_admitted,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_qos_snapshot_background_bounded",
        background_maintenance_health.qos_snapshot_background_bounded,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_qos_snapshot_total_background_over_budget",
        background_maintenance_health.qos_snapshot_total_background_over_budget,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_qos_snapshot_blocker_codes",
        background_maintenance_health.qos_snapshot_blocker_codes,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_foreground_ranked_count",
        background_maintenance_health.foreground_ranked_count,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_unknown_admission_count",
        background_maintenance_health.unknown_admission_count,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_blocker_codes",
        background_maintenance_health.blocker_codes,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_blockers",
        background_maintenance_health.blockers,
    );
    insert_json(
        &mut evidence,
        "replacement_readiness_family_report_present",
        replacement_family_health.present,
    );
    insert_json(
        &mut evidence,
        "replacement_readiness_min_per_million",
        replacement_family_health.min_replacement_readiness_per_million,
    );
    insert_json(
        &mut evidence,
        "replacement_readiness_invalid_family_count",
        replacement_family_health.invalid_family_count,
    );
    insert_json(
        &mut evidence,
        "replacement_readiness_blocked_query_families",
        replacement_family_health.blocked_query_families,
    );
    insert_json(
        &mut evidence,
        "replacement_readiness_missing_required_query_families",
        replacement_family_health.missing_required_query_families,
    );
    insert_json(
        &mut evidence,
        "replacement_readiness_blockers",
        replacement_family_health.blockers,
    );
    insert_json(&mut evidence, "migration_gate_ready", migration_gate_ready);
    insert_json(&mut evidence, "blockers", blockers);
    object.insert(
        "cutover_evidence".to_string(),
        serde_json::Value::Object(evidence),
    );
    Ok(())
}

fn add_shadow_trace_report(
    bundle: &mut serde_json::Value,
    trace_path: &str,
    request_count: u64,
) -> Result<()> {
    let object = bundle.as_object_mut().ok_or_else(|| {
        SkeinError::Execution("migration gate bundle must be a JSON object".to_string())
    })?;
    object.insert(
        "shadow_trace".to_string(),
        external_shadow_trace_report_json(trace_path, request_count),
    );
    Ok(())
}

fn external_shadow_adapter_smoke_fixture() -> CompatibilityFixture {
    CompatibilityFixture {
        name: "external-shadow-adapter-smoke".to_string(),
        setup: vec![CypherFixtureStatement::new(
            "CREATE (:Memory {id: 1, stable_id: 'smoke-memory', title: 'Adapter Smoke'})",
        )],
        checks: vec![
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "session query returns seeded memory",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory) WHERE m.stable_id = $stable_id RETURN m.title AS title",
                        BTreeMap::from([(
                            "stable_id".to_string(),
                            Value::String("smoke-memory".to_string()),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![BTreeMap::from([(
                        "title".to_string(),
                        Value::String("Adapter Smoke".to_string()),
                    )])]),
                )
                .with_session_execution(),
            ),
            CompatibilityCheck::ProjectedGraph(ProjectedGraphFixtureCheck {
                name: "single memory projection".to_string(),
                rel_type: None,
                expected_node_count: 1,
                expected_edge_count: 0,
                expected_incoming: Vec::new(),
                expected_communities: Vec::new(),
                expected_hierarchical_communities: Vec::new(),
                expected_page_rank_scores: Vec::new(),
                page_rank_top_node: None,
                tolerance: Default::default(),
            }),
        ],
    }
}

fn external_shadow_adapter_smoke_report_json(
    ready: &ExternalShadowReady,
    report: &CompatibilityShadowReport,
    request_count: u64,
    trace_path: Option<&str>,
) -> serde_json::Value {
    let missing_capabilities = external_shadow_ready_missing_capabilities(Some(ready));
    let matched_checks = report
        .shadow_checks
        .iter()
        .filter(|check| check.status == CompatibilityShadowStatus::Matched)
        .count();
    let primary_only_checks = report
        .shadow_checks
        .iter()
        .filter(|check| check.status == CompatibilityShadowStatus::PrimaryOnly)
        .count();
    let primary_only_reasons = report
        .shadow_checks
        .iter()
        .filter_map(|check| {
            check
                .primary_only_reason
                .as_ref()
                .map(|reason| (check.name.clone(), reason.clone()))
        })
        .collect::<BTreeMap<_, _>>();
    let primary_check_count = report.primary_checks.len();
    let shadow_check_count = report.shadow_checks.len();
    let dual_engine_ready = primary_check_count == shadow_check_count
        && shadow_check_count > 0
        && matched_checks == shadow_check_count
        && primary_only_checks == 0;
    let mut json = serde_json::json!({
        "protocol": "skein-external-shadow-adapter-smoke",
        "ready": {
            "protocol_version": ready.protocol_version,
            "engine_kind": ready.engine_kind,
            "wrapper_identity": ready.wrapper_identity,
            "capabilities": ready.capabilities,
            "missing_capabilities": missing_capabilities,
        },
        "fixture": report.fixture,
        "shadow_engine": report.shadow_engine,
        "total_checks": report.shadow_checks.len(),
        "matched_checks": matched_checks,
        "primary_only_checks": primary_only_checks,
        "primary_only_reasons": primary_only_reasons,
        "dual_engine_evidence": {
            "ready": dual_engine_ready,
            "primary_engine": "skein",
            "shadow_engine": report.shadow_engine,
            "primary_check_count": primary_check_count,
            "shadow_check_count": shadow_check_count,
            "matched_check_count": matched_checks,
            "primary_only_check_count": primary_only_checks,
        },
        "request_count": request_count,
        "operation_expectations": {
            "ready": true,
            "execute_session": true,
            "project_graph": true,
        },
        "adapter_smoke_ready": missing_capabilities.is_empty() && dual_engine_ready,
    });
    if let Some(trace_path) = trace_path
        && let Some(object) = json.as_object_mut()
    {
        object.insert(
            "shadow_trace".to_string(),
            external_shadow_trace_report_json(trace_path, request_count),
        );
    }
    json
}

fn enforce_external_shadow_adapter_smoke_requirements(
    ready: &ExternalShadowReady,
    report: &CompatibilityShadowReport,
    require_previous_wrapper: bool,
) -> Result<()> {
    let missing_capabilities = external_shadow_ready_missing_capabilities(Some(ready));
    if !missing_capabilities.is_empty() {
        return Err(SkeinError::Execution(format!(
            "external shadow adapter smoke missing required capabilities: {}",
            missing_capabilities.join(", ")
        )));
    }
    if require_previous_wrapper && ready.engine_kind.as_deref() != Some("previous_wrapper") {
        return Err(SkeinError::Execution(
            "external shadow adapter smoke requires engine_kind 'previous_wrapper'".to_string(),
        ));
    }
    if !report
        .shadow_checks
        .iter()
        .any(|check| check.status == CompatibilityShadowStatus::Matched)
    {
        return Err(SkeinError::Execution(
            "external shadow adapter smoke did not match any shadow checks".to_string(),
        ));
    }
    let primary_only_checks = report
        .shadow_checks
        .iter()
        .filter(|check| check.status == CompatibilityShadowStatus::PrimaryOnly)
        .map(|check| check.name.as_str())
        .collect::<Vec<_>>();
    if require_previous_wrapper && !primary_only_checks.is_empty() {
        return Err(SkeinError::Execution(format!(
            "external shadow adapter smoke requires all checks to run on previous-wrapper; primary-only checks: {}",
            primary_only_checks.join(", ")
        )));
    }
    if !report
        .shadow_checks
        .iter()
        .any(|check| check.name == "single memory projection")
    {
        return Err(SkeinError::Execution(
            "external shadow adapter smoke did not exercise project_graph".to_string(),
        ));
    }
    Ok(())
}

fn cutover_evidence_is_eligible(bundle: &serde_json::Value) -> bool {
    bundle
        .get("cutover_evidence")
        .and_then(|evidence| evidence.get("eligible"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

fn should_run_shadow_ready(
    require_ready: bool,
    require_cutover_evidence: bool,
    shadow_ready: bool,
) -> bool {
    require_ready || require_cutover_evidence || shadow_ready
}

fn is_self_shadow_command(shadow_name: &str, program: &str, program_args: &[String]) -> bool {
    shadow_name == "self"
        || program.ends_with("skein-shadow-self")
        || program_args.iter().any(|arg| arg == "skein-shadow-self")
}

fn canonical_snapshot_validation_json(
    graph_commit_epoch: u64,
    logical_checksum: u64,
    node_count: usize,
    relationship_count: usize,
    validation: &CanonicalGraphSnapshotValidation,
) -> serde_json::Value {
    serde_json::json!({
        "graph_commit_epoch": graph_commit_epoch,
        "logical_checksum": logical_checksum,
        "node_count": node_count,
        "relationship_count": relationship_count,
        "validation": {
            "is_valid": validation.is_valid,
            "is_import_ready": validation.is_import_ready,
            "checksum_matches": validation.checksum_matches,
            "expected_logical_checksum": validation.expected_logical_checksum,
            "stable_identity_matches": validation.stable_identity_matches,
            "stable_identity_ready": validation.stable_identity_ready,
            "expected_stable_identity": stable_identity_audit_json(&validation.expected_stable_identity),
            "duplicate_node_ids": validation.duplicate_node_ids,
            "duplicate_relationship_ids": validation.duplicate_relationship_ids,
            "missing_sources": endpoint_violations_json(&validation.missing_sources),
            "missing_targets": endpoint_violations_json(&validation.missing_targets),
        }
    })
}

fn storage_recovery_report_json(
    storage_version: &str,
    report: &StorageRecoveryReport,
) -> serde_json::Value {
    serde_json::json!({
        "protocol": "skein-storage-recovery-report",
        "storage_version": storage_version,
        "durable": report.durable,
        "recovery_mode": recovery_mode_name(report.recovery_mode),
        "max_wal_replay_entries": report.max_wal_replay_entries,
        "max_wal_replay_bytes": report.max_wal_replay_bytes,
        "max_wal_record_bytes": report.max_wal_record_bytes,
        "checkpoint_epoch": report.checkpoint_epoch,
        "checkpoint_commit_epoch": report.checkpoint_commit_epoch,
        "wal_present": report.wal_present,
        "wal_generation": report.wal_generation,
        "wal_replay_start_lsn": report.wal_replay_start_lsn,
        "next_lsn_after_replay": report.next_lsn_after_replay,
        "replayed_wal_entries": report.replayed_wal_entries,
        "replayed_wal_bytes": report.replayed_wal_bytes,
        "torn_tail_ignored": report.torn_tail_ignored,
        "torn_tail_repaired": report.torn_tail_repaired,
        "discarded_wal_tail_bytes": report.discarded_wal_tail_bytes,
        "torn_tail_reason": &report.torn_tail_reason,
        "recovered_commit_epoch": report.recovered_commit_epoch,
        "readiness": {
            "durable_recovery_observed": report.durable,
            "checkpoint_boundary_present": report.checkpoint_epoch.is_some(),
            "wal_replay_bounded": report.max_wal_replay_entries.is_some()
                && report.max_wal_replay_bytes.is_some()
                && report.max_wal_record_bytes.is_some(),
            "torn_tail_clean": !report.torn_tail_ignored || report.torn_tail_repaired,
        },
    })
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct BackgroundMaintenanceReportOptions {
    policy: LocalQosPolicy,
    state: LocalQosState,
    maintenance: BackgroundMaintenanceOptions,
}

fn background_maintenance_report_json_with_options(
    database: &Database,
    options: &BackgroundMaintenanceReportOptions,
) -> serde_json::Value {
    let search_index = SearchIndex::in_memory();
    let summary = database.background_maintenance_summary(
        Some(&search_index),
        &options.policy,
        &options.state,
        options.maintenance.clone(),
    );
    let mut report = background_maintenance_summary_to_json(&summary);
    if let Some(object) = report.as_object_mut() {
        object.insert(
            "protocol".to_string(),
            serde_json::Value::String("skein-background-maintenance-report".to_string()),
        );
        object.insert(
            "slow_query".to_string(),
            background_maintenance_slow_query_json(database),
        );
        object.insert(
            "qos_policy".to_string(),
            background_maintenance_qos_policy_json(&options.policy),
        );
        object.insert(
            "qos_state".to_string(),
            background_maintenance_qos_state_json(&options.state),
        );
    }
    report
}

fn background_maintenance_slow_query_json(database: &Database) -> serde_json::Value {
    let records = database.slow_query_log_snapshot();
    let latest_sequence = records.iter().map(|record| record.sequence).max();
    let max_elapsed_micros = records.iter().map(|record| record.elapsed_micros).max();
    serde_json::json!({
        "ready": true,
        "capacity": database.config().slow_query_log_capacity,
        "threshold_micros": database.config().slow_query_log_threshold_micros,
        "record_count": records.len(),
        "latest_sequence": latest_sequence,
        "max_elapsed_micros": max_elapsed_micros,
        "redaction": {
            "query_text_copied": false,
            "parameters_copied": false,
            "local_paths_copied": false,
        },
    })
}

fn background_maintenance_qos_policy_json(policy: &LocalQosPolicy) -> serde_json::Value {
    serde_json::json!({
        "background_enabled": policy.background_enabled,
        "max_background_operations": policy.max_background_operations,
        "max_total_background_operations": policy.max_total_background_operations,
        "max_background_operations_by_class": background_maintenance_class_limits_json(
            &policy.max_background_operations_by_class,
        ),
    })
}

fn background_maintenance_qos_state_json(state: &LocalQosState) -> serde_json::Value {
    serde_json::json!({
        "running_background_operations": state.running_background_operations,
        "running_background_operations_by_class": background_maintenance_class_running_json(
            &state.running_background_operations_by_class,
        ),
    })
}

fn background_maintenance_class_limits_json(
    limits: &[Option<usize>; WORK_CLASS_COUNT],
) -> serde_json::Value {
    let mut object = serde_json::Map::new();
    for class in background_maintenance_work_classes() {
        object.insert(
            class.as_str().to_string(),
            serde_json::to_value(limits[class.as_index()])
                .expect("background maintenance class limit must serialize"),
        );
    }
    serde_json::Value::Object(object)
}

fn background_maintenance_class_running_json(
    running: &[usize; WORK_CLASS_COUNT],
) -> serde_json::Value {
    let mut object = serde_json::Map::new();
    for class in background_maintenance_work_classes() {
        object.insert(
            class.as_str().to_string(),
            serde_json::to_value(running[class.as_index()])
                .expect("background maintenance class running count must serialize"),
        );
    }
    serde_json::Value::Object(object)
}

fn background_maintenance_work_classes() -> [WorkClass; WORK_CLASS_COUNT] {
    [
        WorkClass::Query,
        WorkClass::Mutation,
        WorkClass::Projection,
        WorkClass::Import,
        WorkClass::Analytics,
        WorkClass::Shadow,
    ]
}

fn recovery_mode_name(recovery_mode: RecoveryMode) -> &'static str {
    match recovery_mode {
        RecoveryMode::Strict => "strict",
        RecoveryMode::DoctorRepairTornTail => "doctor_repair_torn_tail",
    }
}

fn graph_lightning_bootstrap_manifest_json(
    manifest: &GraphLightningBootstrapManifest,
) -> serde_json::Value {
    serde_json::json!({
        "protocol": "graph-lightning-bootstrap",
        "protocol_version": manifest.protocol_version,
        "graph_commit_epoch": manifest.graph_commit_epoch,
        "logical_checksum": manifest.logical_checksum,
        "graph_stream_checksum": manifest.graph_stream_checksum,
        "graph_stream_byte_len": manifest.graph_stream_byte_len,
        "schema_checksum": manifest.schema_checksum,
        "node_count": manifest.node_count,
        "relationship_count": manifest.relationship_count,
        "label_count": manifest.label_count,
        "relationship_type_count": manifest.relationship_type_count,
        "node_property_count": manifest.node_property_count,
        "relationship_property_count": manifest.relationship_property_count,
        "validation": {
            "is_valid": manifest.validation.is_valid,
            "is_import_ready": manifest.validation.is_import_ready,
            "checksum_matches": manifest.validation.checksum_matches,
            "expected_logical_checksum": manifest.validation.expected_logical_checksum,
            "stable_identity_matches": manifest.validation.stable_identity_matches,
            "stable_identity_ready": manifest.validation.stable_identity_ready,
            "expected_stable_identity": stable_identity_audit_json(&manifest.validation.expected_stable_identity),
            "duplicate_node_ids": manifest.validation.duplicate_node_ids,
            "duplicate_relationship_ids": manifest.validation.duplicate_relationship_ids,
            "missing_sources": endpoint_violations_json(&manifest.validation.missing_sources),
            "missing_targets": endpoint_violations_json(&manifest.validation.missing_targets),
        }
    })
}

#[cfg(test)]
fn graph_lightning_bootstrap_bundle_json(
    export: &skein::GraphLightningBootstrapExport,
) -> serde_json::Value {
    graph_lightning_bootstrap_bundle_json_with_optional_storage_recovery(export, None)
}

fn graph_lightning_bootstrap_bundle_json_with_storage_recovery(
    export: &skein::GraphLightningBootstrapExport,
    storage_version: &str,
    storage_recovery: &StorageRecoveryReport,
) -> serde_json::Value {
    let storage_recovery_json = storage_recovery_report_json(storage_version, storage_recovery);
    graph_lightning_bootstrap_bundle_json_with_optional_storage_recovery(
        export,
        Some(storage_recovery_json),
    )
}

fn graph_lightning_bootstrap_bundle_json_with_optional_storage_recovery(
    export: &skein::GraphLightningBootstrapExport,
    storage_recovery: Option<serde_json::Value>,
) -> serde_json::Value {
    let graph_stream_validation = export
        .graph_stream
        .validate_against_manifest(&export.manifest);
    let mut blockers = Vec::new();
    let mut manifest_blocker_messages = Vec::new();
    if !export.manifest.validation.is_import_ready {
        manifest_blocker_messages.push("manifest validation is not import ready");
    }
    blockers.extend(manifest_blocker_messages.iter().copied());
    let mut graph_stream_blocker_messages = Vec::new();
    if !graph_stream_validation.is_valid {
        graph_stream_blocker_messages.push("graph stream validation failed");
    }
    blockers.extend(graph_stream_blocker_messages.iter().copied());
    let decision = if blockers.is_empty() {
        "ready"
    } else {
        "blocked"
    };
    let mut bundle = serde_json::json!({
        "protocol": "graph-lightning-bootstrap-bundle",
        "manifest": graph_lightning_bootstrap_manifest_json(&export.manifest),
        "graph_stream_validation": graph_lightning_graph_stream_validation_json(&graph_stream_validation),
        "export_gate": {
            "decision": decision,
            "manifest_blockers": manifest_blocker_messages.len(),
            "graph_stream_blockers": graph_stream_blocker_messages.len(),
            "manifest_blocker_messages": manifest_blocker_messages,
            "graph_stream_blocker_messages": graph_stream_blocker_messages,
            "blockers": blockers,
        },
    });
    if let Some(storage_recovery) = storage_recovery {
        bundle
            .as_object_mut()
            .expect("bootstrap bundle JSON must be an object")
            .insert("storage_recovery".to_string(), storage_recovery);
    }
    bundle
}

#[cfg(test)]
fn stage_graph_lightning_bootstrap_export(
    export: &skein::GraphLightningBootstrapExport,
    staging_dir: impl AsRef<Path>,
) -> Result<serde_json::Value> {
    stage_graph_lightning_bootstrap_export_with_optional_storage_recovery(export, staging_dir, None)
}

fn stage_graph_lightning_bootstrap_export_with_storage_recovery(
    export: &skein::GraphLightningBootstrapExport,
    staging_dir: impl AsRef<Path>,
    storage_version: &str,
    storage_recovery: &StorageRecoveryReport,
) -> Result<serde_json::Value> {
    let storage_recovery_json = storage_recovery_report_json(storage_version, storage_recovery);
    stage_graph_lightning_bootstrap_export_with_optional_storage_recovery(
        export,
        staging_dir,
        Some(storage_recovery_json),
    )
}

fn stage_graph_lightning_bootstrap_export_with_optional_storage_recovery(
    export: &skein::GraphLightningBootstrapExport,
    staging_dir: impl AsRef<Path>,
    storage_recovery: Option<serde_json::Value>,
) -> Result<serde_json::Value> {
    let staging_dir = staging_dir.as_ref();
    fs::create_dir_all(staging_dir)?;
    let bundle = graph_lightning_bootstrap_bundle_json_with_optional_storage_recovery(
        export,
        storage_recovery,
    );
    let manifest = graph_lightning_bootstrap_manifest_json(&export.manifest);
    let graph_stream_validation = export
        .graph_stream
        .validate_against_manifest(&export.manifest);
    let stage_state = if bundle
        .get("export_gate")
        .and_then(|gate| gate.get("decision"))
        .and_then(serde_json::Value::as_str)
        == Some("ready")
    {
        "READY"
    } else {
        "QUARANTINED"
    };
    let manifest_bytes = serde_json::to_vec_pretty(&manifest).unwrap();
    let graph_stream_bytes = export.graph_stream.encoded.as_bytes();
    let bundle_bytes = serde_json::to_vec_pretty(&bundle).unwrap();
    let manifest_artifact = write_staging_artifact(
        staging_dir,
        "graph_lightning_bootstrap_manifest.json",
        &manifest_bytes,
    )?;
    let graph_stream_artifact = write_staging_artifact(
        staging_dir,
        "graph_lightning_graph_stream.txt",
        graph_stream_bytes,
    )?;
    let bundle_artifact = write_staging_artifact(
        staging_dir,
        "graph_lightning_bootstrap_bundle.json",
        &bundle_bytes,
    )?;
    let artifacts = vec![manifest_artifact, graph_stream_artifact, bundle_artifact];
    let artifact_summary = graph_lightning_artifact_summary(&artifacts, "byte_len");
    let catalog = serde_json::json!({
        "protocol": "graph-lightning-staging-catalog",
        "protocol_version": GRAPH_LIGHTNING_STAGING_CATALOG_PROTOCOL_VERSION,
        "stage_state": stage_state,
        "graph_commit_epoch": export.manifest.graph_commit_epoch,
        "logical_checksum": export.manifest.logical_checksum,
        "schema_checksum": export.manifest.schema_checksum,
        "export_gate": bundle["export_gate"].clone(),
        "artifact_summary": artifact_summary,
        "artifacts": artifacts,
        "graph_stream_validation": graph_lightning_graph_stream_validation_json(&graph_stream_validation),
    });
    let catalog_bytes = serde_json::to_vec_pretty(&catalog).unwrap();
    write_staging_artifact(
        staging_dir,
        "graph_lightning_staging_catalog.json",
        &catalog_bytes,
    )?;
    sync_directory(staging_dir)?;
    Ok(catalog)
}

fn verify_graph_lightning_staging_catalog(
    staging_dir: impl AsRef<Path>,
) -> Result<serde_json::Value> {
    let staging_dir = staging_dir.as_ref();
    let catalog_path = staging_dir.join("graph_lightning_staging_catalog.json");
    let catalog = read_json_file(&catalog_path)?;
    let mut errors = Vec::new();
    let mut artifact_errors = Vec::new();
    let mut manifest_errors = Vec::new();
    let mut graph_stream_errors = Vec::new();
    let mut bundle_errors = Vec::new();
    let mut catalog_errors = Vec::new();
    let mut artifact_reports = Vec::new();
    let mut manifest = None;
    let mut graph_stream = None;
    let mut bundle = None;

    let catalog_protocol_matches = catalog.get("protocol").and_then(serde_json::Value::as_str)
        == Some("graph-lightning-staging-catalog");
    if !catalog_protocol_matches {
        push_grouped_error(
            &mut errors,
            &mut catalog_errors,
            "staging catalog protocol mismatch",
        );
    }
    let catalog_protocol_version_matches = catalog
        .get("protocol_version")
        .and_then(serde_json::Value::as_u64)
        == Some(GRAPH_LIGHTNING_STAGING_CATALOG_PROTOCOL_VERSION);
    if !catalog_protocol_version_matches {
        push_grouped_error(
            &mut errors,
            &mut catalog_errors,
            "staging catalog protocol version mismatch",
        );
    }

    let artifacts = catalog
        .get("artifacts")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_else(|| {
            push_grouped_error(
                &mut errors,
                &mut catalog_errors,
                "staging catalog missing artifacts array",
            );
            Vec::new()
        });
    for artifact in &artifacts {
        let kind = artifact
            .get("kind")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let Some(path) = artifact.get("path").and_then(serde_json::Value::as_str) else {
            push_grouped_error(
                &mut errors,
                &mut artifact_errors,
                format!("staging artifact {kind} missing path"),
            );
            continue;
        };
        if path.contains('/') || path.contains('\\') {
            push_grouped_error(
                &mut errors,
                &mut artifact_errors,
                format!("staging artifact {kind} uses non-local path {path}"),
            );
            continue;
        }
        let artifact_path = staging_dir.join(path);
        let expected_byte_len = artifact.get("byte_len").and_then(serde_json::Value::as_u64);
        let expected_checksum = artifact.get("checksum").and_then(serde_json::Value::as_u64);
        match fs::read(&artifact_path) {
            Ok(bytes) => {
                let actual_byte_len = bytes.len() as u64;
                let actual_checksum = checksum_bytes(&bytes);
                let byte_len_matches = expected_byte_len == Some(actual_byte_len);
                let checksum_matches = expected_checksum == Some(actual_checksum);
                if !byte_len_matches {
                    push_grouped_error(
                        &mut errors,
                        &mut artifact_errors,
                        format!("staging artifact {kind} byte length mismatch"),
                    );
                }
                if !checksum_matches {
                    push_grouped_error(
                        &mut errors,
                        &mut artifact_errors,
                        format!("staging artifact {kind} checksum mismatch"),
                    );
                }
                match kind {
                    "manifest" => match serde_json::from_slice::<serde_json::Value>(&bytes) {
                        Ok(value) => manifest = Some(value),
                        Err(error) => {
                            push_grouped_error(
                                &mut errors,
                                &mut manifest_errors,
                                format!("invalid manifest artifact JSON: {error}"),
                            );
                        }
                    },
                    "graph_stream" => match String::from_utf8(bytes.clone()) {
                        Ok(value) => graph_stream = Some(value),
                        Err(error) => push_grouped_error(
                            &mut errors,
                            &mut graph_stream_errors,
                            format!("invalid GraphStream UTF-8: {error}"),
                        ),
                    },
                    "bundle" => match serde_json::from_slice::<serde_json::Value>(&bytes) {
                        Ok(value) => bundle = Some(value),
                        Err(error) => push_grouped_error(
                            &mut errors,
                            &mut bundle_errors,
                            format!("invalid bundle artifact JSON: {error}"),
                        ),
                    },
                    _ => push_grouped_error(
                        &mut errors,
                        &mut artifact_errors,
                        format!("unknown staging artifact kind {kind}"),
                    ),
                }
                artifact_reports.push(serde_json::json!({
                    "kind": kind,
                    "path": path,
                    "expected_byte_len": expected_byte_len,
                    "actual_byte_len": actual_byte_len,
                    "byte_len_matches": byte_len_matches,
                    "expected_checksum": expected_checksum,
                    "actual_checksum": actual_checksum,
                    "checksum_matches": checksum_matches,
                }));
            }
            Err(error) => {
                push_grouped_error(
                    &mut errors,
                    &mut artifact_errors,
                    format!("missing staging artifact {kind} at {path}: {error}"),
                );
                artifact_reports.push(serde_json::json!({
                    "kind": kind,
                    "path": path,
                    "expected_byte_len": expected_byte_len,
                    "actual_byte_len": serde_json::Value::Null,
                    "byte_len_matches": false,
                    "expected_checksum": expected_checksum,
                    "actual_checksum": serde_json::Value::Null,
                    "checksum_matches": false,
                }));
            }
        }
    }

    let manifest_protocol_version_matches = manifest
        .as_ref()
        .and_then(|manifest| manifest.get("protocol_version"))
        .and_then(serde_json::Value::as_u64)
        == Some(GRAPH_LIGHTNING_BOOTSTRAP_PROTOCOL_VERSION);
    if !manifest_protocol_version_matches {
        push_grouped_error(
            &mut errors,
            &mut manifest_errors,
            "manifest protocol version mismatch",
        );
    }

    let graph_stream_validation = graph_stream
        .as_ref()
        .map(|encoded| skein::validate_graph_lightning_graph_stream(encoded, None));
    let graph_stream_validation_json = graph_stream_validation
        .as_ref()
        .map(graph_lightning_graph_stream_validation_json);
    let manifest_matches_graph_stream = match (&manifest, &graph_stream, &graph_stream_validation) {
        (Some(manifest), Some(graph_stream), Some(validation)) => {
            let matches = manifest
                .get("graph_stream_checksum")
                .and_then(serde_json::Value::as_u64)
                == validation.expected_stream_checksum
                && manifest
                    .get("graph_stream_byte_len")
                    .and_then(serde_json::Value::as_u64)
                    == Some(graph_stream.len() as u64)
                && manifest
                    .get("graph_commit_epoch")
                    .and_then(serde_json::Value::as_u64)
                    == validation.graph_commit_epoch
                && manifest
                    .get("logical_checksum")
                    .and_then(serde_json::Value::as_u64)
                    == validation.logical_checksum
                && manifest
                    .get("node_count")
                    .and_then(serde_json::Value::as_u64)
                    == Some(validation.node_count as u64)
                && manifest
                    .get("relationship_count")
                    .and_then(serde_json::Value::as_u64)
                    == Some(validation.relationship_count as u64);
            if !matches {
                push_grouped_error(
                    &mut errors,
                    &mut manifest_errors,
                    "manifest does not match GraphStream artifact",
                );
            }
            matches
        }
        _ => {
            push_grouped_error(
                &mut errors,
                &mut manifest_errors,
                "manifest or GraphStream artifact missing",
            );
            false
        }
    };
    let bundle_matches_artifacts = match (&bundle, &manifest, &graph_stream_validation_json) {
        (Some(bundle), Some(manifest), Some(validation)) => {
            let matches = bundle.get("manifest") == Some(manifest)
                && bundle.get("graph_stream_validation") == Some(validation)
                && bundle.get("export_gate") == catalog.get("export_gate");
            if !matches {
                push_grouped_error(
                    &mut errors,
                    &mut bundle_errors,
                    "bundle does not match staged manifest, GraphStream validation, or catalog gate",
                );
            }
            matches
        }
        _ => {
            push_grouped_error(&mut errors, &mut bundle_errors, "bundle artifact missing");
            false
        }
    };
    let storage_recovery_evidence = match (&bundle, &manifest) {
        (Some(bundle), Some(manifest)) => verify_bundle_storage_recovery_evidence(
            bundle,
            manifest,
            &mut errors,
            &mut bundle_errors,
        ),
        _ => StorageRecoveryEvidenceVerification::default(),
    };
    let artifact_integrity = artifact_reports.iter().all(|report| {
        report
            .get("byte_len_matches")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
            && report
                .get("checksum_matches")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
    });
    let artifact_summary = graph_lightning_artifact_summary(&artifact_reports, "actual_byte_len");
    let catalog_state_ready = catalog
        .get("stage_state")
        .and_then(serde_json::Value::as_str)
        == Some("READY")
        && catalog
            .get("export_gate")
            .and_then(|gate| gate.get("decision"))
            .and_then(serde_json::Value::as_str)
            == Some("ready");
    if !catalog_state_ready {
        push_grouped_error(
            &mut errors,
            &mut catalog_errors,
            "staging catalog is not READY",
        );
    }
    let graph_stream_valid = graph_stream_validation
        .as_ref()
        .is_some_and(|validation| validation.is_valid);
    if !graph_stream_valid {
        push_grouped_error(
            &mut errors,
            &mut graph_stream_errors,
            "GraphStream validation failed",
        );
    }
    let decision = if errors.is_empty()
        && artifact_integrity
        && catalog_protocol_matches
        && catalog_protocol_version_matches
        && manifest_protocol_version_matches
        && manifest_matches_graph_stream
        && bundle_matches_artifacts
        && storage_recovery_evidence.valid
        && catalog_state_ready
        && graph_stream_valid
    {
        "ready"
    } else {
        "blocked"
    };
    Ok(serde_json::json!({
        "protocol": "graph-lightning-staging-verification",
        "protocol_version": GRAPH_LIGHTNING_STAGING_CATALOG_PROTOCOL_VERSION,
        "catalog_path": "graph_lightning_staging_catalog.json",
        "artifact_integrity": artifact_integrity,
        "catalog_protocol_matches": catalog_protocol_matches,
        "catalog_protocol_version_matches": catalog_protocol_version_matches,
        "manifest_protocol_version_matches": manifest_protocol_version_matches,
        "manifest_matches_graph_stream": manifest_matches_graph_stream,
        "bundle_matches_artifacts": bundle_matches_artifacts,
        "storage_recovery_evidence": {
            "present": storage_recovery_evidence.present,
            "valid": storage_recovery_evidence.valid,
            "protocol_matches": storage_recovery_evidence.protocol_matches,
            "storage_version_present": storage_recovery_evidence.storage_version_present,
            "recovered_commit_epoch_matches_manifest": storage_recovery_evidence.recovered_commit_epoch_matches_manifest,
        },
        "catalog_state_ready": catalog_state_ready,
        "graph_stream_validation": graph_stream_validation_json,
        "artifact_summary": artifact_summary,
        "artifacts": artifact_reports,
        "validation_gate": {
            "decision": decision,
            "artifact_errors": artifact_errors.len(),
            "manifest_errors": manifest_errors.len(),
            "graph_stream_errors": graph_stream_errors.len(),
            "bundle_errors": bundle_errors.len(),
            "catalog_errors": catalog_errors.len(),
            "artifact_error_messages": artifact_errors,
            "manifest_error_messages": manifest_errors,
            "graph_stream_error_messages": graph_stream_errors,
            "bundle_error_messages": bundle_errors,
            "catalog_error_messages": catalog_errors,
            "errors": errors,
        },
    }))
}

fn push_grouped_error(
    errors: &mut Vec<String>,
    group: &mut Vec<String>,
    message: impl Into<String>,
) {
    let message = message.into();
    errors.push(message.clone());
    group.push(message);
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct StorageRecoveryEvidenceVerification {
    present: bool,
    valid: bool,
    protocol_matches: bool,
    storage_version_present: bool,
    recovered_commit_epoch_matches_manifest: bool,
}

fn verify_bundle_storage_recovery_evidence(
    bundle: &serde_json::Value,
    manifest: &serde_json::Value,
    errors: &mut Vec<String>,
    bundle_errors: &mut Vec<String>,
) -> StorageRecoveryEvidenceVerification {
    let Some(storage_recovery) = bundle.get("storage_recovery") else {
        return StorageRecoveryEvidenceVerification {
            present: false,
            valid: true,
            protocol_matches: false,
            storage_version_present: false,
            recovered_commit_epoch_matches_manifest: false,
        };
    };
    let protocol_matches = storage_recovery
        .get("protocol")
        .and_then(serde_json::Value::as_str)
        == Some("skein-storage-recovery-report");
    if !protocol_matches {
        push_grouped_error(
            errors,
            bundle_errors,
            "bundle storage_recovery protocol mismatch",
        );
    }
    let storage_version_present = storage_recovery
        .get("storage_version")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|version| !version.is_empty());
    if !storage_version_present {
        push_grouped_error(
            errors,
            bundle_errors,
            "bundle storage_recovery missing storage_version",
        );
    }
    let recovered_commit_epoch_matches_manifest = storage_recovery
        .get("recovered_commit_epoch")
        .and_then(serde_json::Value::as_u64)
        == manifest
            .get("graph_commit_epoch")
            .and_then(serde_json::Value::as_u64);
    if !recovered_commit_epoch_matches_manifest {
        push_grouped_error(
            errors,
            bundle_errors,
            "bundle storage_recovery recovered commit epoch does not match manifest graph epoch",
        );
    }

    StorageRecoveryEvidenceVerification {
        present: true,
        valid: protocol_matches
            && storage_version_present
            && recovered_commit_epoch_matches_manifest,
        protocol_matches,
        storage_version_present,
        recovered_commit_epoch_matches_manifest,
    }
}

fn publish_graph_lightning_staging_catalog(
    staging_dir: impl AsRef<Path>,
    publish_dir: impl AsRef<Path>,
) -> Result<serde_json::Value> {
    publish_graph_lightning_staging_catalog_with_options(
        staging_dir,
        publish_dir,
        PublishGraphLightningOptions::default(),
    )
}

fn publish_graph_lightning_staging_catalog_with_options(
    staging_dir: impl AsRef<Path>,
    publish_dir: impl AsRef<Path>,
    options: PublishGraphLightningOptions,
) -> Result<serde_json::Value> {
    let staging_dir = staging_dir.as_ref();
    let publish_dir = publish_dir.as_ref();
    let verification = verify_graph_lightning_staging_catalog(staging_dir)?;
    if verification
        .get("validation_gate")
        .and_then(|gate| gate.get("decision"))
        .and_then(serde_json::Value::as_str)
        != Some("ready")
    {
        return Err(SkeinError::Execution(
            "graph lightning staging verification is not ready".to_string(),
        ));
    }

    let catalog_path = staging_dir.join("graph_lightning_staging_catalog.json");
    let catalog_bytes = fs::read(&catalog_path)?;
    let catalog_checksum = checksum_bytes(&catalog_bytes);
    let catalog = serde_json::from_slice::<serde_json::Value>(&catalog_bytes)
        .map_err(|_| SkeinError::Execution("invalid JSON file: invalid_json".to_string()))?;
    let manifest = read_staging_artifact_json(&catalog, staging_dir, "manifest")?;
    let publish_preflight = graph_lightning_publish_preflight(staging_dir, &manifest, &options)?;
    let pointer = serde_json::json!({
        "protocol": "graph-lightning-published-manifest",
        "protocol_version": 1,
        "state": "PUBLISHED",
        "graph_commit_epoch": manifest["graph_commit_epoch"].clone(),
        "logical_checksum": manifest["logical_checksum"].clone(),
        "schema_checksum": manifest["schema_checksum"].clone(),
        "graph_stream_checksum": manifest["graph_stream_checksum"].clone(),
        "graph_stream_byte_len": manifest["graph_stream_byte_len"].clone(),
        "node_count": manifest["node_count"].clone(),
        "relationship_count": manifest["relationship_count"].clone(),
        "staging_catalog": {
            "path": "graph_lightning_staging_catalog.json",
            "checksum": catalog_checksum,
            "byte_len": catalog_bytes.len(),
        },
    });

    fs::create_dir_all(publish_dir)?;
    let pointer_path = publish_dir.join("graph_lightning_published_manifest.json");
    if pointer_path.exists() {
        let existing = read_json_file(&pointer_path)?;
        if same_published_manifest_identity(&existing, &pointer) {
            let mut report = pointer;
            if let Some(object) = report.as_object_mut() {
                object.insert(
                    "publish_gate".to_string(),
                    serde_json::json!({
                        "decision": "idempotent",
                        "preflight": publish_preflight,
                        "errors": [],
                    }),
                );
            }
            return Ok(report);
        }
        return Err(SkeinError::Execution(
            "published graph lightning manifest already points to a different snapshot".to_string(),
        ));
    }

    let mut report = pointer;
    if let Some(object) = report.as_object_mut() {
        object.insert(
            "publish_gate".to_string(),
            serde_json::json!({
                "decision": "published",
                "preflight": publish_preflight,
                "errors": [],
            }),
        );
    }
    let pointer_bytes = serde_json::to_vec_pretty(&report).unwrap();
    write_atomic_file(
        publish_dir,
        "graph_lightning_published_manifest.json",
        &pointer_bytes,
    )?;
    sync_directory(publish_dir)?;
    Ok(report)
}

fn graph_lightning_publish_preflight(
    staging_dir: &Path,
    manifest: &serde_json::Value,
    options: &PublishGraphLightningOptions,
) -> Result<serde_json::Value> {
    let mut errors = Vec::new();
    let mut state_errors = Vec::new();
    let state_marker =
        graph_lightning_import_state_marker(staging_dir, &mut errors, &mut state_errors);
    let marker_present = state_marker
        .get("present")
        .and_then(serde_json::Value::as_bool)
        == Some(true);
    let marker_state = state_marker
        .get("import_state")
        .and_then(serde_json::Value::as_str);
    if options.require_state_marker && !marker_present {
        push_grouped_error(
            &mut errors,
            &mut state_errors,
            "publish requires graph lightning import state marker",
        );
    }
    if marker_present && marker_state != Some("VALIDATING") {
        push_grouped_error(
            &mut errors,
            &mut state_errors,
            format!(
                "publish requires VALIDATING import state marker, found {}",
                marker_state.unwrap_or("missing")
            ),
        );
    }

    let manifest_epoch = manifest
        .get("graph_commit_epoch")
        .and_then(serde_json::Value::as_u64);
    let expected_graph_epoch_matches = options
        .expected_graph_epoch
        .is_none_or(|expected| manifest_epoch == Some(expected));
    if !expected_graph_epoch_matches {
        push_grouped_error(
            &mut errors,
            &mut state_errors,
            format!(
                "expected graph epoch {:?} did not match staged manifest epoch {:?}",
                options.expected_graph_epoch, manifest_epoch
            ),
        );
    }

    let marker_fencing_token = state_marker
        .get("idempotency_key")
        .and_then(|key| key.get("fencing_token"))
        .and_then(serde_json::Value::as_str);
    let fencing_token_matches = options
        .fencing_token
        .as_deref()
        .is_none_or(|expected| marker_fencing_token == Some(expected));
    if !fencing_token_matches {
        push_grouped_error(
            &mut errors,
            &mut state_errors,
            "publish fencing token did not match import state marker",
        );
    }

    if !errors.is_empty() {
        return Err(SkeinError::Execution(format!(
            "graph lightning publish preflight blocked: {}",
            errors.join("; ")
        )));
    }

    Ok(serde_json::json!({
        "decision": "ready",
        "require_state_marker": options.require_state_marker,
        "expected_graph_epoch": options.expected_graph_epoch,
        "manifest_graph_epoch": manifest_epoch,
        "expected_graph_epoch_matches": expected_graph_epoch_matches,
        "fencing_token_required": options.fencing_token.is_some(),
        "fencing_token_matches": fencing_token_matches,
        "state_marker": state_marker,
        "state_errors": state_errors.len(),
        "state_error_messages": state_errors,
        "errors": errors,
    }))
}

fn verify_graph_lightning_published_manifest(
    staging_dir: impl AsRef<Path>,
    publish_dir: impl AsRef<Path>,
) -> Result<serde_json::Value> {
    let staging_dir = staging_dir.as_ref();
    let publish_dir = publish_dir.as_ref();
    let published_path = publish_dir.join("graph_lightning_published_manifest.json");
    let published = read_json_file(&published_path)?;
    let staging_verification = verify_graph_lightning_staging_catalog(staging_dir)?;
    let catalog_path = staging_dir.join("graph_lightning_staging_catalog.json");
    let catalog_bytes = fs::read(&catalog_path)?;
    let actual_catalog_checksum = checksum_bytes(&catalog_bytes);
    let actual_catalog_byte_len = catalog_bytes.len() as u64;
    let expected_catalog_checksum = published
        .get("staging_catalog")
        .and_then(|catalog| catalog.get("checksum"))
        .and_then(serde_json::Value::as_u64);
    let expected_catalog_byte_len = published
        .get("staging_catalog")
        .and_then(|catalog| catalog.get("byte_len"))
        .and_then(serde_json::Value::as_u64);
    let catalog_checksum_matches = expected_catalog_checksum == Some(actual_catalog_checksum);
    let catalog_byte_len_matches = expected_catalog_byte_len == Some(actual_catalog_byte_len);
    let pointer_state_published =
        published.get("state").and_then(serde_json::Value::as_str) == Some("PUBLISHED");
    let staging_ready = staging_verification
        .get("validation_gate")
        .and_then(|gate| gate.get("decision"))
        .and_then(serde_json::Value::as_str)
        == Some("ready");
    let storage_recovery_evidence = staging_verification
        .get("storage_recovery_evidence")
        .cloned()
        .unwrap_or_else(|| {
            serde_json::json!({
                "present": false,
                "valid": true,
                "protocol_matches": false,
                "storage_version_present": false,
                "recovered_commit_epoch_matches_manifest": false,
            })
        });
    let catalog = serde_json::from_slice::<serde_json::Value>(&catalog_bytes)
        .map_err(|_| SkeinError::Execution("invalid JSON file: invalid_json".to_string()))?;
    let manifest = read_staging_artifact_json(&catalog, staging_dir, "manifest")?;
    let pointer_matches_manifest = published.get("graph_commit_epoch")
        == manifest.get("graph_commit_epoch")
        && published.get("logical_checksum") == manifest.get("logical_checksum")
        && published.get("schema_checksum") == manifest.get("schema_checksum")
        && published.get("graph_stream_checksum") == manifest.get("graph_stream_checksum")
        && published.get("graph_stream_byte_len") == manifest.get("graph_stream_byte_len")
        && published.get("node_count") == manifest.get("node_count")
        && published.get("relationship_count") == manifest.get("relationship_count");
    let mut errors = Vec::new();
    let mut pointer_errors = Vec::new();
    let mut catalog_errors = Vec::new();
    let mut staging_errors = Vec::new();
    if !pointer_state_published {
        push_grouped_error(
            &mut errors,
            &mut pointer_errors,
            "published pointer is not PUBLISHED",
        );
    }
    if !catalog_checksum_matches {
        push_grouped_error(
            &mut errors,
            &mut catalog_errors,
            "published pointer staging catalog checksum mismatch",
        );
    }
    if !catalog_byte_len_matches {
        push_grouped_error(
            &mut errors,
            &mut catalog_errors,
            "published pointer staging catalog byte length mismatch",
        );
    }
    if !staging_ready {
        push_grouped_error(
            &mut errors,
            &mut staging_errors,
            "published staging catalog is not ready",
        );
    }
    if !pointer_matches_manifest {
        push_grouped_error(
            &mut errors,
            &mut pointer_errors,
            "published pointer does not match staged manifest",
        );
    }
    let decision = if errors.is_empty() {
        "ready"
    } else {
        "blocked"
    };
    Ok(serde_json::json!({
        "protocol": "graph-lightning-published-verification",
        "protocol_version": 1,
        "pointer_state_published": pointer_state_published,
        "catalog_checksum_matches": catalog_checksum_matches,
        "catalog_byte_len_matches": catalog_byte_len_matches,
        "staging_ready": staging_ready,
        "pointer_matches_manifest": pointer_matches_manifest,
        "storage_recovery_evidence": storage_recovery_evidence,
        "published_manifest": published,
        "staging_verification": staging_verification,
        "validation_gate": {
            "decision": decision,
            "pointer_errors": pointer_errors.len(),
            "catalog_errors": catalog_errors.len(),
            "staging_errors": staging_errors.len(),
            "pointer_error_messages": pointer_errors,
            "catalog_error_messages": catalog_errors,
            "staging_error_messages": staging_errors,
            "errors": errors,
        },
    }))
}

fn graph_lightning_gc_staging_report(
    staging_dir: impl AsRef<Path>,
    publish_dir: impl AsRef<Path>,
) -> Result<serde_json::Value> {
    let staging_dir = staging_dir.as_ref();
    let publish_dir = publish_dir.as_ref();
    let catalog_path = staging_dir.join("graph_lightning_staging_catalog.json");
    let catalog_bytes = fs::read(&catalog_path)?;
    let catalog = serde_json::from_slice::<serde_json::Value>(&catalog_bytes)
        .map_err(|_| SkeinError::Execution("invalid JSON file: invalid_json".to_string()))?;
    let candidates = graph_lightning_staging_gc_candidates(&catalog, &catalog_bytes)?;
    let published_path = publish_dir.join("graph_lightning_published_manifest.json");
    let mut errors = Vec::new();
    let mut published_pointer_errors = Vec::new();
    let mut pinned_paths = BTreeSet::new();
    let pointer_state = if published_path.exists() {
        let verification = verify_graph_lightning_published_manifest(staging_dir, publish_dir)?;
        if verification
            .get("validation_gate")
            .and_then(|gate| gate.get("decision"))
            .and_then(serde_json::Value::as_str)
            == Some("ready")
        {
            pinned_paths = candidates
                .iter()
                .filter_map(|candidate| {
                    candidate
                        .get("path")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string)
                })
                .collect();
            "verified"
        } else {
            push_grouped_error(
                &mut errors,
                &mut published_pointer_errors,
                "published pointer verification failed; refusing to mark staging artifacts deletable",
            );
            if let Some(verification_errors) = verification
                .get("validation_gate")
                .and_then(|gate| gate.get("errors"))
                .and_then(serde_json::Value::as_array)
            {
                for error in verification_errors
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                {
                    push_grouped_error(&mut errors, &mut published_pointer_errors, error);
                }
            }
            "verification_failed"
        }
    } else {
        "missing"
    };

    let fail_closed = pointer_state == "verification_failed";
    let candidate_reports = candidates
        .into_iter()
        .map(|candidate| {
            let path = candidate
                .get("path")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            let pinned_by_published_pointer = pinned_paths.contains(path);
            let deletable = !fail_closed && !pinned_by_published_pointer;
            let reason = if pinned_by_published_pointer {
                "published_pointer"
            } else if fail_closed {
                "published_pointer_unverified"
            } else {
                "not_pinned"
            };
            serde_json::json!({
                "kind": candidate["kind"].clone(),
                "path": candidate["path"].clone(),
                "byte_len": candidate["byte_len"].clone(),
                "checksum": candidate["checksum"].clone(),
                "pinned_by_published_pointer": pinned_by_published_pointer,
                "deletable": deletable,
                "reason": reason,
            })
        })
        .collect::<Vec<_>>();
    let deletable_count = candidate_reports
        .iter()
        .filter(|candidate| {
            candidate
                .get("deletable")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
        })
        .count();
    let pinned_count = candidate_reports
        .iter()
        .filter(|candidate| {
            candidate
                .get("pinned_by_published_pointer")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
        })
        .count();
    let total_bytes = graph_lightning_sum_artifact_bytes(&candidate_reports, |_| true);
    let deletable_bytes = graph_lightning_sum_artifact_bytes(&candidate_reports, |candidate| {
        candidate
            .get("deletable")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
    });
    let pinned_bytes = graph_lightning_sum_artifact_bytes(&candidate_reports, |candidate| {
        candidate
            .get("pinned_by_published_pointer")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
    });
    let artifact_summary = graph_lightning_artifact_summary(&candidate_reports, "byte_len");
    let decision = if errors.is_empty() {
        "ready"
    } else {
        "blocked"
    };
    Ok(serde_json::json!({
        "protocol": "graph-lightning-staging-gc-report",
        "protocol_version": 1,
        "published_pointer_state": pointer_state,
        "candidate_count": candidate_reports.len(),
        "pinned_count": pinned_count,
        "deletable_count": deletable_count,
        "total_bytes": total_bytes,
        "pinned_bytes": pinned_bytes,
        "deletable_bytes": deletable_bytes,
        "artifact_summary": artifact_summary,
        "candidates": candidate_reports,
        "gc_gate": {
            "decision": decision,
            "published_pointer_errors": published_pointer_errors.len(),
            "published_pointer_error_messages": published_pointer_errors,
            "errors": errors,
        },
    }))
}

fn graph_lightning_staging_gc_candidates(
    catalog: &serde_json::Value,
    catalog_bytes: &[u8],
) -> Result<Vec<serde_json::Value>> {
    let mut candidates = Vec::new();
    candidates.push(serde_json::json!({
        "kind": "staging_catalog",
        "path": "graph_lightning_staging_catalog.json",
        "byte_len": catalog_bytes.len(),
        "checksum": checksum_bytes(catalog_bytes),
    }));
    let artifacts = catalog
        .get("artifacts")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            SkeinError::Execution("staging catalog missing artifacts array".to_string())
        })?;
    for artifact in artifacts {
        let kind = artifact
            .get("kind")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| SkeinError::Execution("staging artifact missing kind".to_string()))?;
        let path = artifact
            .get("path")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| SkeinError::Execution("staging artifact missing path".to_string()))?;
        if path.contains('/') || path.contains('\\') {
            return Err(SkeinError::Execution(format!(
                "staging artifact {kind} uses non-local path {path}"
            )));
        }
        candidates.push(serde_json::json!({
            "kind": kind,
            "path": path,
            "byte_len": artifact.get("byte_len").cloned().unwrap_or(serde_json::Value::Null),
            "checksum": artifact.get("checksum").cloned().unwrap_or(serde_json::Value::Null),
        }));
    }
    Ok(candidates)
}

fn graph_lightning_artifact_summary(
    artifacts: &[serde_json::Value],
    byte_len_field: &str,
) -> serde_json::Value {
    let mut kind_counts = BTreeMap::new();
    let mut total_byte_len = 0u64;
    let mut measured_object_count = 0usize;
    for artifact in artifacts {
        if let Some(kind) = artifact.get("kind").and_then(serde_json::Value::as_str) {
            *kind_counts.entry(kind.to_string()).or_insert(0usize) += 1;
        }
        if let Some(byte_len) = artifact
            .get(byte_len_field)
            .and_then(serde_json::Value::as_u64)
        {
            total_byte_len = total_byte_len.saturating_add(byte_len);
            measured_object_count += 1;
        }
    }
    let average_byte_len = if measured_object_count == 0 {
        serde_json::Value::Null
    } else {
        serde_json::json!(total_byte_len as f64 / measured_object_count as f64)
    };
    serde_json::json!({
        "object_count": artifacts.len(),
        "measured_object_count": measured_object_count,
        "missing_byte_len_count": artifacts.len().saturating_sub(measured_object_count),
        "total_byte_len": total_byte_len,
        "average_byte_len": average_byte_len,
        "kind_counts": kind_counts,
    })
}

fn graph_lightning_sum_artifact_bytes(
    artifacts: &[serde_json::Value],
    predicate: impl Fn(&serde_json::Value) -> bool,
) -> u64 {
    artifacts
        .iter()
        .filter(|artifact| predicate(artifact))
        .filter_map(|artifact| artifact.get("byte_len").and_then(serde_json::Value::as_u64))
        .sum()
}

fn graph_lightning_import_status(
    staging_dir: impl AsRef<Path>,
    publish_dir: impl AsRef<Path>,
) -> Result<serde_json::Value> {
    let staging_dir = staging_dir.as_ref();
    let publish_dir = publish_dir.as_ref();
    let catalog_path = staging_dir.join("graph_lightning_staging_catalog.json");
    let published_path = publish_dir.join("graph_lightning_published_manifest.json");
    let staging_catalog_present = catalog_path.exists();
    let published_pointer_present = published_path.exists();
    let mut errors = Vec::new();
    let mut presence_errors = Vec::new();
    let mut staging_errors = Vec::new();
    let mut published_errors = Vec::new();
    let mut resource_errors = Vec::new();
    let mut state_errors = Vec::new();
    let mut checkpoint_errors = Vec::new();
    let mut staging_verification = None;
    let mut published_verification = None;
    let state_marker =
        graph_lightning_import_state_marker(staging_dir, &mut errors, &mut state_errors);
    let checkpoint_log =
        graph_lightning_import_checkpoint_log(staging_dir, &mut errors, &mut checkpoint_errors);
    let artifact_state = if !staging_catalog_present && published_pointer_present {
        push_grouped_error(
            &mut errors,
            &mut presence_errors,
            "published pointer exists without a matching staging catalog; refusing to treat import as created",
        );
        "QUARANTINED"
    } else if !staging_catalog_present {
        "CREATED"
    } else {
        let staging_report = verify_graph_lightning_staging_catalog(staging_dir)?;
        let staging_ready = gate_decision(&staging_report, "validation_gate") == Some("ready");
        if !staging_ready {
            for error in gate_errors(&staging_report, "validation_gate") {
                push_grouped_error(&mut errors, &mut staging_errors, error);
            }
        }
        staging_verification = Some(staging_report);
        if !staging_ready {
            "QUARANTINED"
        } else if published_pointer_present {
            let published_report =
                verify_graph_lightning_published_manifest(staging_dir, publish_dir)?;
            let published_ready =
                gate_decision(&published_report, "validation_gate") == Some("ready");
            if !published_ready {
                for error in gate_errors(&published_report, "validation_gate") {
                    push_grouped_error(&mut errors, &mut published_errors, error);
                }
            }
            published_verification = Some(published_report);
            if published_ready {
                "PUBLISHED"
            } else {
                "QUARANTINED"
            }
        } else {
            "READY"
        }
    };
    let import_state = graph_lightning_effective_import_state(
        artifact_state,
        state_marker
            .get("import_state")
            .and_then(serde_json::Value::as_str),
    );
    let resume_action = graph_lightning_import_resume_action(import_state);
    let resource_retention = graph_lightning_import_resource_retention(
        import_state,
        staging_catalog_present,
        staging_dir,
        publish_dir,
        &mut errors,
        &mut resource_errors,
    );
    let storage_recovery_evidence = graph_lightning_import_storage_recovery_evidence(
        staging_verification.as_ref(),
        published_verification.as_ref(),
    );
    let decision = if import_state == "QUARANTINED"
        || !resource_errors.is_empty()
        || !state_errors.is_empty()
        || !checkpoint_errors.is_empty()
    {
        "blocked"
    } else {
        "ready"
    };
    Ok(serde_json::json!({
        "protocol": "graph-lightning-import-status",
        "protocol_version": 1,
        "import_state": import_state,
        "artifact_state": artifact_state,
        "state_marker": state_marker,
        "checkpoint_log": checkpoint_log,
        "resume_action": resume_action,
        "resource_retention": resource_retention,
        "staging_catalog_present": staging_catalog_present,
        "published_pointer_present": published_pointer_present,
        "storage_recovery_evidence": storage_recovery_evidence,
        "staging_verification": staging_verification,
        "published_verification": published_verification,
        "status_gate": {
            "decision": decision,
            "presence_errors": presence_errors.len(),
            "staging_errors": staging_errors.len(),
            "published_errors": published_errors.len(),
            "resource_errors": resource_errors.len(),
            "state_errors": state_errors.len(),
            "checkpoint_errors": checkpoint_errors.len(),
            "presence_error_messages": presence_errors,
            "staging_error_messages": staging_errors,
            "published_error_messages": published_errors,
            "resource_error_messages": resource_errors,
            "state_error_messages": state_errors,
            "checkpoint_error_messages": checkpoint_errors,
            "errors": errors,
        },
    }))
}

fn graph_lightning_import_storage_recovery_evidence(
    staging_verification: Option<&serde_json::Value>,
    published_verification: Option<&serde_json::Value>,
) -> serde_json::Value {
    published_verification
        .and_then(|verification| verification.get("storage_recovery_evidence"))
        .or_else(|| {
            staging_verification
                .and_then(|verification| verification.get("storage_recovery_evidence"))
        })
        .cloned()
        .unwrap_or_else(|| {
            serde_json::json!({
                "present": false,
                "valid": true,
                "protocol_matches": false,
                "storage_version_present": false,
                "recovered_commit_epoch_matches_manifest": false,
            })
        })
}

fn graph_lightning_import_checkpoint_log(
    staging_dir: &Path,
    errors: &mut Vec<String>,
    checkpoint_errors: &mut Vec<String>,
) -> serde_json::Value {
    let checkpoint_path = staging_dir.join("graph_lightning_import_checkpoints.jsonl");
    if !checkpoint_path.exists() {
        return serde_json::json!({
            "present": false,
            "path": "graph_lightning_import_checkpoints.jsonl",
            "entry_count": 0,
            "idempotency_key_count": 0,
            "idempotency_conflicts": 0,
            "idempotency_conflict_messages": [],
            "last_checkpoint": serde_json::Value::Null,
            "failed_checkpoints": [],
            "checkpoint_summary": {
                "stage_counts": {},
                "status_counts": {},
                "failure_rule_counts": {},
                "failure_partition_counts": {},
            },
            "resume_summary": {
                "last_stage": serde_json::Value::Null,
                "last_source_range": serde_json::Value::Null,
                "last_object_digest": serde_json::Value::Null,
                "last_partition": serde_json::Value::Null,
                "failed_source_ranges": [],
                "failed_object_digests": [],
                "failed_partitions": [],
                "failed_validation_rules": [],
            },
            "checkpoint_gate": {
                "decision": "ready",
                "errors": [],
            },
        });
    }

    let content = match fs::read_to_string(&checkpoint_path) {
        Ok(content) => content,
        Err(error) => {
            push_grouped_error(
                errors,
                checkpoint_errors,
                format!("checkpoint log could not be read: {error}"),
            );
            return graph_lightning_import_checkpoint_blocked_json(checkpoint_errors);
        }
    };

    let mut entries = Vec::new();
    let mut failed = Vec::new();
    let mut failed_source_ranges = BTreeSet::new();
    let mut failed_object_digests = BTreeSet::new();
    let mut failed_partitions = BTreeSet::new();
    let mut failed_validation_rules = BTreeSet::new();
    let mut idempotency_fingerprints = BTreeMap::new();
    let mut idempotency_conflicts = Vec::new();
    let mut stage_counts = BTreeMap::new();
    let mut status_counts = BTreeMap::new();
    let mut failure_rule_counts = BTreeMap::new();
    let mut failure_partition_counts = BTreeMap::new();
    for (line_index, line) in content.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let entry = match serde_json::from_str::<serde_json::Value>(trimmed) {
            Ok(entry) => entry,
            Err(error) => {
                push_grouped_error(
                    errors,
                    checkpoint_errors,
                    format!(
                        "checkpoint log line {} is invalid JSON: {error}",
                        line_index + 1
                    ),
                );
                continue;
            }
        };
        graph_lightning_validate_checkpoint_entry(
            &entry,
            line_index + 1,
            errors,
            checkpoint_errors,
        );
        increment_string_field(&entry, "stage", &mut stage_counts);
        increment_string_field(&entry, "status", &mut status_counts);
        if let Some((key, fingerprint)) = graph_lightning_checkpoint_idempotency_fingerprint(&entry)
        {
            if let Some(previous) = idempotency_fingerprints.get(&key) {
                if previous != &fingerprint {
                    let message = format!(
                        "checkpoint log line {} reuses idempotency key for conflicting checkpoint coordinates",
                        line_index + 1
                    );
                    push_grouped_error(errors, checkpoint_errors, message.clone());
                    idempotency_conflicts.push(message);
                }
            } else {
                idempotency_fingerprints.insert(key, fingerprint);
            }
        }
        if entry.get("status").and_then(serde_json::Value::as_str) == Some("failed") {
            collect_string_field(&entry, "source_range", &mut failed_source_ranges);
            collect_string_field(&entry, "object_digest", &mut failed_object_digests);
            collect_string_field(&entry, "partition", &mut failed_partitions);
            collect_string_field(&entry, "validation_rule", &mut failed_validation_rules);
            increment_string_field(&entry, "partition", &mut failure_partition_counts);
            increment_string_field(&entry, "validation_rule", &mut failure_rule_counts);
            failed.push(entry.clone());
        }
        entries.push(entry);
    }

    let last_checkpoint = entries.last().cloned().unwrap_or(serde_json::Value::Null);
    let decision = if checkpoint_errors.is_empty() {
        "ready"
    } else {
        "blocked"
    };
    serde_json::json!({
        "present": true,
        "path": "graph_lightning_import_checkpoints.jsonl",
        "entry_count": entries.len(),
        "idempotency_key_count": idempotency_fingerprints.len(),
        "idempotency_conflicts": idempotency_conflicts.len(),
        "idempotency_conflict_messages": idempotency_conflicts,
        "last_checkpoint": last_checkpoint,
        "failed_checkpoints": failed,
        "checkpoint_summary": {
            "stage_counts": stage_counts,
            "status_counts": status_counts,
            "failure_rule_counts": failure_rule_counts,
            "failure_partition_counts": failure_partition_counts,
        },
        "resume_summary": {
            "last_stage": last_checkpoint.get("stage").cloned().unwrap_or(serde_json::Value::Null),
            "last_source_range": last_checkpoint.get("source_range").cloned().unwrap_or(serde_json::Value::Null),
            "last_object_digest": last_checkpoint.get("object_digest").cloned().unwrap_or(serde_json::Value::Null),
            "last_partition": last_checkpoint.get("partition").cloned().unwrap_or(serde_json::Value::Null),
            "failed_source_ranges": failed_source_ranges.into_iter().collect::<Vec<_>>(),
            "failed_object_digests": failed_object_digests.into_iter().collect::<Vec<_>>(),
            "failed_partitions": failed_partitions.into_iter().collect::<Vec<_>>(),
            "failed_validation_rules": failed_validation_rules.into_iter().collect::<Vec<_>>(),
        },
        "checkpoint_gate": {
            "decision": decision,
            "errors": checkpoint_errors,
        },
    })
}

fn graph_lightning_checkpoint_idempotency_fingerprint(
    entry: &serde_json::Value,
) -> Option<(String, serde_json::Value)> {
    let import_id = marker_string_field(entry, "import_id")?;
    let task_id = marker_string_field(entry, "task_id")?;
    let fencing_token = marker_string_field(entry, "fencing_token")?;
    let object_digest = marker_string_field(entry, "object_digest")?;
    let key = serde_json::json!({
        "import_id": import_id,
        "task_id": task_id,
        "fencing_token": fencing_token,
        "object_digest": object_digest,
    })
    .to_string();
    let fingerprint = serde_json::json!({
        "source_range": entry.get("source_range").cloned().unwrap_or(serde_json::Value::Null),
        "partition": entry.get("partition").cloned().unwrap_or(serde_json::Value::Null),
        "manifest_digest": entry.get("manifest_digest").cloned().unwrap_or(serde_json::Value::Null),
    });
    Some((key, fingerprint))
}

fn graph_lightning_import_checkpoint_blocked_json(
    checkpoint_errors: &[String],
) -> serde_json::Value {
    serde_json::json!({
        "present": true,
        "path": "graph_lightning_import_checkpoints.jsonl",
        "entry_count": 0,
        "idempotency_key_count": 0,
        "idempotency_conflicts": 0,
        "idempotency_conflict_messages": [],
        "last_checkpoint": serde_json::Value::Null,
        "failed_checkpoints": [],
        "checkpoint_summary": {
            "stage_counts": {},
            "status_counts": {},
            "failure_rule_counts": {},
            "failure_partition_counts": {},
        },
        "resume_summary": {
            "last_stage": serde_json::Value::Null,
            "last_source_range": serde_json::Value::Null,
            "last_object_digest": serde_json::Value::Null,
            "last_partition": serde_json::Value::Null,
            "failed_source_ranges": [],
            "failed_object_digests": [],
            "failed_partitions": [],
            "failed_validation_rules": [],
        },
        "checkpoint_gate": {
            "decision": "blocked",
            "errors": checkpoint_errors,
        },
    })
}

fn graph_lightning_validate_checkpoint_entry(
    entry: &serde_json::Value,
    line_number: usize,
    errors: &mut Vec<String>,
    checkpoint_errors: &mut Vec<String>,
) {
    let stage = marker_string_field(entry, "stage");
    if stage.is_none() {
        push_grouped_error(
            errors,
            checkpoint_errors,
            format!("checkpoint log line {line_number} missing stage"),
        );
    }
    let status = marker_string_field(entry, "status");
    if !matches!(status, Some("completed" | "failed")) {
        push_grouped_error(
            errors,
            checkpoint_errors,
            format!("checkpoint log line {line_number} has unsupported status"),
        );
    }
    if status == Some("failed") {
        for field in [
            "source_range",
            "object_digest",
            "partition",
            "validation_rule",
        ] {
            if marker_string_field(entry, field).is_none() {
                push_grouped_error(
                    errors,
                    checkpoint_errors,
                    format!("checkpoint log line {line_number} failed entry missing {field}"),
                );
            }
        }
    }
    if matches!(
        stage,
        Some("object_uploaded" | "object_verified" | "merge_range_committed")
    ) {
        for field in ["import_id", "task_id", "fencing_token", "object_digest"] {
            if marker_string_field(entry, field).is_none() {
                push_grouped_error(
                    errors,
                    checkpoint_errors,
                    format!("checkpoint log line {line_number} missing idempotency field {field}"),
                );
            }
        }
    }
}

fn collect_string_field(entry: &serde_json::Value, field: &str, output: &mut BTreeSet<String>) {
    if let Some(value) = marker_string_field(entry, field) {
        output.insert(value.to_string());
    }
}

fn increment_string_field(
    entry: &serde_json::Value,
    field: &str,
    output: &mut BTreeMap<String, usize>,
) {
    if let Some(value) = marker_string_field(entry, field) {
        *output.entry(value.to_string()).or_insert(0) += 1;
    }
}

fn graph_lightning_import_state_marker(
    staging_dir: &Path,
    errors: &mut Vec<String>,
    state_errors: &mut Vec<String>,
) -> serde_json::Value {
    let state_path = staging_dir.join("graph_lightning_import_state.json");
    if !state_path.exists() {
        return serde_json::json!({
            "present": false,
            "path": "graph_lightning_import_state.json",
            "import_state": serde_json::Value::Null,
            "idempotency_ready": false,
            "idempotency_key": serde_json::Value::Null,
            "raw": serde_json::Value::Null,
        });
    }

    let marker = match read_json_file(&state_path) {
        Ok(marker) => marker,
        Err(error) => {
            push_grouped_error(
                errors,
                state_errors,
                format!("import state marker could not be read: {error}"),
            );
            return serde_json::json!({
                "present": true,
                "path": "graph_lightning_import_state.json",
                "import_state": "QUARANTINED",
                "idempotency_ready": false,
                "idempotency_key": serde_json::Value::Null,
                "raw": serde_json::Value::Null,
            });
        }
    };
    let protocol_valid = marker.get("protocol").and_then(serde_json::Value::as_str)
        == Some("graph-lightning-import-state");
    let version_valid = marker
        .get("protocol_version")
        .and_then(serde_json::Value::as_u64)
        == Some(1);
    let import_state = marker
        .get("import_state")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("QUARANTINED");

    if !protocol_valid {
        push_grouped_error(
            errors,
            state_errors,
            "import state marker protocol mismatch",
        );
    }
    if !version_valid {
        push_grouped_error(
            errors,
            state_errors,
            "import state marker protocol version mismatch",
        );
    }
    if !graph_lightning_import_marker_state_allowed(import_state) {
        push_grouped_error(
            errors,
            state_errors,
            format!("import state marker uses unsupported state {import_state}"),
        );
    }
    let idempotency_key =
        graph_lightning_import_marker_idempotency_key(&marker, import_state, errors, state_errors);

    serde_json::json!({
        "present": true,
        "path": "graph_lightning_import_state.json",
        "import_state": if state_errors.is_empty() { import_state } else { "QUARANTINED" },
        "idempotency_ready": state_errors.is_empty() && idempotency_key.is_some(),
        "idempotency_key": idempotency_key,
        "raw": marker,
    })
}

fn graph_lightning_import_marker_state_allowed(import_state: &str) -> bool {
    matches!(
        import_state,
        "EXPORTING" | "UPLOADING" | "MERGING" | "VALIDATING" | "FAILED" | "CANCELED"
    )
}

fn graph_lightning_import_marker_idempotency_key(
    marker: &serde_json::Value,
    import_state: &str,
    errors: &mut Vec<String>,
    state_errors: &mut Vec<String>,
) -> Option<serde_json::Value> {
    let import_id = marker_string_field(marker, "import_id");
    let task_id = marker_string_field(marker, "task_id");
    let fencing_token = marker_string_field(marker, "fencing_token");
    let object_digest = marker_string_field(marker, "object_digest");
    if graph_lightning_import_marker_state_is_active(import_state) {
        for missing in [
            ("import_id", import_id),
            ("task_id", task_id),
            ("fencing_token", fencing_token),
            ("object_digest", object_digest),
        ]
        .into_iter()
        .filter_map(|(field, value)| value.is_none().then_some(field))
        {
            push_grouped_error(
                errors,
                state_errors,
                format!("active import state marker missing idempotency field {missing}"),
            );
        }
    }

    Some(serde_json::json!({
        "import_id": import_id?,
        "task_id": task_id?,
        "fencing_token": fencing_token?,
        "object_digest": object_digest?,
    }))
}

fn graph_lightning_import_marker_state_is_active(import_state: &str) -> bool {
    matches!(
        import_state,
        "EXPORTING" | "UPLOADING" | "MERGING" | "VALIDATING"
    )
}

fn marker_string_field<'a>(marker: &'a serde_json::Value, field: &str) -> Option<&'a str> {
    marker
        .get(field)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
}

fn graph_lightning_effective_import_state<'a>(
    artifact_state: &'a str,
    marker_state: Option<&'a str>,
) -> &'a str {
    if artifact_state == "PUBLISHED" || artifact_state == "QUARANTINED" {
        return artifact_state;
    }
    marker_state.unwrap_or(artifact_state)
}

fn graph_lightning_import_resource_retention(
    import_state: &str,
    staging_catalog_present: bool,
    staging_dir: &Path,
    publish_dir: &Path,
    errors: &mut Vec<String>,
    resource_errors: &mut Vec<String>,
) -> serde_json::Value {
    if !staging_catalog_present {
        return serde_json::json!({
            "action": "none",
            "safe_to_collect": false,
            "protected_count": 0,
            "deletable_count": 0,
            "reason": "staging catalog is missing",
            "gc_report": serde_json::Value::Null,
        });
    }

    let gc_report = match graph_lightning_gc_staging_report(staging_dir, publish_dir) {
        Ok(report) => report,
        Err(error) => {
            push_grouped_error(
                errors,
                resource_errors,
                format!("resource retention report failed: {error}"),
            );
            return serde_json::json!({
                "action": "hold_for_inspection",
                "safe_to_collect": false,
                "protected_count": 0,
                "deletable_count": 0,
                "reason": "resource retention could not verify staging artifacts",
                "gc_report": serde_json::Value::Null,
            });
        }
    };
    let candidate_count = gc_report
        .get("candidate_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let pinned_count = gc_report
        .get("pinned_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let gc_deletable_count = gc_report
        .get("deletable_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let gc_ready = gate_decision(&gc_report, "gc_gate") == Some("ready");

    match import_state {
        "READY" => serde_json::json!({
            "action": "retain_for_publish",
            "safe_to_collect": false,
            "protected_count": candidate_count,
            "deletable_count": 0,
            "gc_deletable_count": gc_deletable_count,
            "reason": "staging artifacts are required for publishing",
            "gc_report": gc_report,
        }),
        "PUBLISHED" => serde_json::json!({
            "action": "follow_gc_report",
            "safe_to_collect": gc_ready && gc_deletable_count > 0,
            "protected_count": pinned_count,
            "deletable_count": if gc_ready { gc_deletable_count } else { 0 },
            "gc_deletable_count": gc_deletable_count,
            "reason": "published pointer verification controls staging retention",
            "gc_report": gc_report,
        }),
        "EXPORTING" | "UPLOADING" | "MERGING" | "VALIDATING" => serde_json::json!({
            "action": "retain_for_active_import",
            "safe_to_collect": false,
            "protected_count": candidate_count,
            "deletable_count": 0,
            "gc_deletable_count": gc_deletable_count,
            "reason": "import state marker reports active import work",
            "gc_report": gc_report,
        }),
        "FAILED" | "CANCELED" => serde_json::json!({
            "action": "hold_for_inspection",
            "safe_to_collect": false,
            "protected_count": candidate_count,
            "deletable_count": 0,
            "gc_deletable_count": gc_deletable_count,
            "reason": "import state marker reports terminal import work",
            "gc_report": gc_report,
        }),
        "QUARANTINED" => serde_json::json!({
            "action": "hold_for_inspection",
            "safe_to_collect": false,
            "protected_count": candidate_count,
            "deletable_count": 0,
            "gc_deletable_count": gc_deletable_count,
            "reason": "status gate has blocking errors",
            "gc_report": gc_report,
        }),
        _ => serde_json::json!({
            "action": "hold_for_inspection",
            "safe_to_collect": false,
            "protected_count": candidate_count,
            "deletable_count": 0,
            "gc_deletable_count": gc_deletable_count,
            "reason": "unknown import state",
            "gc_report": gc_report,
        }),
    }
}

fn graph_lightning_import_resume_action(import_state: &str) -> serde_json::Value {
    match import_state {
        "CREATED" => serde_json::json!({
            "operation": "stage_bootstrap",
            "safe_to_retry": true,
            "terminal": false,
            "reason": "staging catalog is missing",
        }),
        "READY" => serde_json::json!({
            "operation": "publish_staging",
            "safe_to_retry": true,
            "terminal": false,
            "reason": "staging catalog verified but no published pointer exists",
        }),
        "EXPORTING" => serde_json::json!({
            "operation": "continue_export",
            "safe_to_retry": true,
            "terminal": false,
            "reason": "import state marker reports export in progress",
        }),
        "UPLOADING" => serde_json::json!({
            "operation": "continue_upload",
            "safe_to_retry": true,
            "terminal": false,
            "reason": "import state marker reports upload in progress",
        }),
        "MERGING" => serde_json::json!({
            "operation": "continue_merge",
            "safe_to_retry": true,
            "terminal": false,
            "reason": "import state marker reports merge in progress",
        }),
        "VALIDATING" => serde_json::json!({
            "operation": "continue_validation",
            "safe_to_retry": true,
            "terminal": false,
            "reason": "import state marker reports validation in progress",
        }),
        "PUBLISHED" => serde_json::json!({
            "operation": "none",
            "safe_to_retry": false,
            "terminal": true,
            "reason": "published pointer verified",
        }),
        "FAILED" => serde_json::json!({
            "operation": "inspect_errors",
            "safe_to_retry": false,
            "terminal": true,
            "reason": "import state marker reports failed import",
        }),
        "CANCELED" => serde_json::json!({
            "operation": "none",
            "safe_to_retry": false,
            "terminal": true,
            "reason": "import state marker reports canceled import",
        }),
        "QUARANTINED" => serde_json::json!({
            "operation": "inspect_errors",
            "safe_to_retry": false,
            "terminal": true,
            "reason": "status gate has blocking errors",
        }),
        _ => serde_json::json!({
            "operation": "inspect_errors",
            "safe_to_retry": false,
            "terminal": true,
            "reason": "unknown import state",
        }),
    }
}

fn gate_decision<'a>(report: &'a serde_json::Value, gate: &str) -> Option<&'a str> {
    report
        .get(gate)
        .and_then(|gate| gate.get("decision"))
        .and_then(serde_json::Value::as_str)
}

fn gate_errors(report: &serde_json::Value, gate: &str) -> Vec<String> {
    report
        .get(gate)
        .and_then(|gate| gate.get("errors"))
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(str::to_string)
        .collect()
}

fn read_staging_artifact_json(
    catalog: &serde_json::Value,
    staging_dir: &Path,
    kind: &str,
) -> Result<serde_json::Value> {
    let artifacts = catalog
        .get("artifacts")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            SkeinError::Execution("staging catalog missing artifacts array".to_string())
        })?;
    let artifact = artifacts
        .iter()
        .find(|artifact| artifact.get("kind").and_then(serde_json::Value::as_str) == Some(kind))
        .ok_or_else(|| SkeinError::Execution(format!("staging catalog missing {kind} artifact")))?;
    let path = artifact
        .get("path")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| SkeinError::Execution(format!("staging {kind} artifact missing path")))?;
    if path.contains('/') || path.contains('\\') {
        return Err(SkeinError::Execution(format!(
            "staging {kind} artifact uses non-local path {path}"
        )));
    }
    read_json_file(&staging_dir.join(path))
}

fn same_published_manifest_identity(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    [
        "graph_commit_epoch",
        "logical_checksum",
        "schema_checksum",
        "graph_stream_checksum",
        "graph_stream_byte_len",
        "node_count",
        "relationship_count",
    ]
    .iter()
    .all(|key| left.get(*key) == right.get(*key))
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let bytes = fs::read(path)?;
    serde_json::from_slice(&bytes)
        .map_err(|_| SkeinError::Execution("invalid JSON file: invalid_json".to_string()))
}

fn write_staging_artifact(
    staging_dir: &Path,
    file_name: &str,
    bytes: &[u8],
) -> Result<serde_json::Value> {
    let path = staging_dir.join(file_name);
    let tmp_path = staging_dir.join(format!("{file_name}.tmp"));
    write_atomic_path(&path, &tmp_path, bytes)?;
    Ok(serde_json::json!({
        "kind": graph_lightning_artifact_kind(file_name),
        "path": file_name,
        "byte_len": bytes.len(),
        "checksum": checksum_bytes(bytes),
    }))
}

fn write_atomic_file(dir: &Path, file_name: &str, bytes: &[u8]) -> Result<()> {
    let path = dir.join(file_name);
    let tmp_path = dir.join(format!("{file_name}.tmp"));
    write_atomic_path(&path, &tmp_path, bytes)
}

fn write_atomic_path(path: &Path, tmp_path: &Path, bytes: &[u8]) -> Result<()> {
    {
        let mut file = File::create(tmp_path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    durable_replace_file(tmp_path, path)?;
    Ok(())
}

fn graph_lightning_artifact_kind(file_name: &str) -> &'static str {
    match file_name {
        "graph_lightning_bootstrap_manifest.json" => "manifest",
        "graph_lightning_graph_stream.txt" => "graph_stream",
        "graph_lightning_bootstrap_bundle.json" => "bundle",
        "graph_lightning_staging_catalog.json" => "staging_catalog",
        _ => "unknown",
    }
}

fn checksum_bytes(bytes: &[u8]) -> u64 {
    checksum_u64(bytes)
}

fn sync_directory(path: &Path) -> Result<()> {
    sync_storage_directory(path)?;
    Ok(())
}

fn graph_lightning_graph_stream_validation_json(
    validation: &skein::GraphLightningGraphStreamValidation,
) -> serde_json::Value {
    serde_json::json!({
        "is_valid": validation.is_valid,
        "checksum_matches": validation.checksum_matches,
        "format_version_matches": validation.format_version_matches,
        "count_matches": validation.count_matches,
        "endpoint_integrity": validation.endpoint_integrity,
        "manifest_matches": validation.manifest_matches,
        "expected_stream_checksum": validation.expected_stream_checksum,
        "actual_stream_checksum": validation.actual_stream_checksum,
        "format_version": validation.format_version,
        "graph_commit_epoch": validation.graph_commit_epoch,
        "logical_checksum": validation.logical_checksum,
        "node_count": validation.node_count,
        "relationship_count": validation.relationship_count,
        "duplicate_node_ids": validation.duplicate_node_ids,
        "duplicate_relationship_ids": validation.duplicate_relationship_ids,
        "missing_sources": endpoint_violations_json(&validation.missing_sources),
        "missing_targets": endpoint_violations_json(&validation.missing_targets),
        "errors": validation.errors,
    })
}

fn stable_identity_audit_json(audit: &CanonicalSnapshotIdentityAudit) -> serde_json::Value {
    serde_json::json!({
        "requires_stable_id_mapping": audit.requires_stable_id_mapping,
        "nodes_without_stable_id": audit.nodes_without_stable_id,
        "relationships_without_stable_id": audit.relationships_without_stable_id,
        "duplicate_node_stable_ids": audit.duplicate_node_stable_ids.iter().map(value_json).collect::<Vec<_>>(),
        "duplicate_relationship_stable_ids": audit.duplicate_relationship_stable_ids.iter().map(value_json).collect::<Vec<_>>(),
    })
}

fn explain_output_json(
    query: &str,
    parameters: &BTreeMap<String, Value>,
    output: &skein::api::ExplainOutput,
    plan_cache_stats: &skein::PlanCacheStats,
) -> serde_json::Value {
    explain_diagnostics_json(ExplainDiagnosticsJsonInput {
        protocol: "skein-explain",
        query,
        statement_kind: output.statement_kind,
        parameters,
        trace: &output.trace,
        work_request: &output.work_request,
        plan_cache_lookup: output.plan_cache_lookup,
        plan_cache_stats,
    })
}

fn explain_analyze_output_json(
    query: &str,
    parameters: &BTreeMap<String, Value>,
    output: &skein::api::ExplainAnalyzeOutput,
    plan_cache_stats: &skein::PlanCacheStats,
) -> serde_json::Value {
    let mut json = explain_diagnostics_json(ExplainDiagnosticsJsonInput {
        protocol: "skein-explain-analyze",
        query,
        statement_kind: output.statement_kind,
        parameters,
        trace: &output.trace,
        work_request: &output.work_request,
        plan_cache_lookup: output.plan_cache_lookup,
        plan_cache_stats,
    });
    if let serde_json::Value::Object(object) = &mut json {
        object.insert(
            "output_row_count".to_string(),
            serde_json::json!(output.output.rows.len()),
        );
        object.insert(
            "execution_profile".to_string(),
            read_execution_profile_json(&output.execution_profile),
        );
    }
    json
}

struct ExplainDiagnosticsJsonInput<'a> {
    protocol: &'a str,
    query: &'a str,
    statement_kind: &'static str,
    parameters: &'a BTreeMap<String, Value>,
    trace: &'a skein::optimizer::OptimizerTrace,
    work_request: &'a skein::WorkRequest,
    plan_cache_lookup: skein::PlanCacheLookup,
    plan_cache_stats: &'a skein::PlanCacheStats,
}

fn explain_diagnostics_json(input: ExplainDiagnosticsJsonInput<'_>) -> serde_json::Value {
    serde_json::json!({
        "protocol": input.protocol,
        "protocol_version": 1,
        "query": input.query,
        "statement_kind": input.statement_kind,
        "parameters": serde_json::Value::Object(
            input.parameters
                .iter()
                .map(|(key, value)| (key.clone(), value_json(value)))
                .collect()
        ),
        "groups": input.trace.groups,
        "search_mode": input.trace.search_mode.as_str(),
        "query_digest": input.trace.query_digest,
        "selected_plan": input.trace.selected_plan,
        "selected_plan_fingerprint": input.trace.selected_plan_fingerprint,
        "selected_plan_cost": {
            "estimated_rows": input.trace.selected_plan_cost.estimated_rows,
            "cost": input.trace.selected_plan_cost.cost,
        },
        "selected_plan_cost_breakdown": {
            "estimated_rows": input.trace.selected_plan_cost_breakdown.estimated_rows,
            "cost": input.trace.selected_plan_cost_breakdown.cost,
            "cpu": input.trace.selected_plan_cost_breakdown.cpu,
            "random_io": input.trace.selected_plan_cost_breakdown.random_io,
            "sequential_io": input.trace.selected_plan_cost_breakdown.sequential_io,
            "output_rows": input.trace.selected_plan_cost_breakdown.output_rows,
        },
        "selected_plan_properties": physical_properties_json(&input.trace.selected_plan_properties),
        "selected_plan_operator_counts": input.trace.selected_plan_operator_counts,
        "selected_plan_class_counts": input.trace.selected_plan_class_counts,
        "optimizer_stages": input.trace
            .stage_events
            .iter()
            .map(optimizer_stage_json)
            .collect::<Vec<_>>(),
        "work_request": {
            "priority": input.work_request.priority.as_str(),
            "class": input.work_request.class.as_str(),
            "estimated_operations": input.work_request.estimated_operations,
        },
        "plan_cache_lookup": {
            "event": input.plan_cache_lookup.as_str(),
            "bypass_reason": input.plan_cache_lookup
                .bypass_reason()
                .map(|reason| reason.as_str()),
        },
        "plan_cache_stats": {
            "max_entries": input.plan_cache_stats.max_entries,
            "entries": input.plan_cache_stats.entries,
            "hits": input.plan_cache_stats.hits,
            "misses": input.plan_cache_stats.misses,
            "admissions": input.plan_cache_stats.admissions,
            "disabled_misses": input.plan_cache_stats.disabled_misses,
            "bypasses": input.plan_cache_stats.bypasses,
            "evictions": input.plan_cache_stats.evictions,
            "memory_pressure_events": input.plan_cache_stats.memory_pressure_events,
        },
        "warnings": input.trace.warnings,
        "decisions": input.trace.decisions,
        "rule_events": input.trace
            .rule_events
            .iter()
            .map(rule_event_json)
            .collect::<Vec<_>>(),
    })
}

fn read_execution_profile_json(
    profile: &skein::executor::ReadExecutionProfile,
) -> serde_json::Value {
    serde_json::json!({
        "max_rows": profile.max_rows,
        "detection_row_cap": profile.detection_row_cap,
        "row_limit_enforced_before_output": profile.row_limit_enforced_before_output,
        "operator_row_cap_enabled": profile.operator_row_cap_enabled,
        "blocking_operator_kinds": profile.blocking_operator_kinds,
        "blocking_operator_memory_reports": profile.blocking_operator_memory_reports.iter().map(|report| serde_json::json!({
            "operator": report.operator,
            "budget_bytes": report.budget_bytes,
            "peak_tracked_bytes": report.peak_tracked_bytes,
            "input_rows": report.input_rows,
            "max_spill_bytes": report.max_spill_bytes,
            "max_spill_runs": report.max_spill_runs,
            "spilled_bytes": report.spilled_bytes,
            "spill_run_count": report.spill_run_count,
            "spilled_rows": report.spilled_rows,
        })).collect::<Vec<_>>(),
        "pipeline_memory_report": {
            "intermediate_rows": profile.pipeline_memory_report.intermediate_rows,
            "intermediate_payload_bytes": profile.pipeline_memory_report.intermediate_payload_bytes,
            "peak_batch_rows": profile.pipeline_memory_report.peak_batch_rows,
            "peak_batch_payload_bytes": profile.pipeline_memory_report.peak_batch_payload_bytes,
            "output_rows": profile.pipeline_memory_report.output_rows,
            "output_payload_bytes": profile.pipeline_memory_report.output_payload_bytes,
            "start_resident_bytes": profile.pipeline_memory_report.start_resident_bytes,
            "start_peak_resident_bytes": profile.pipeline_memory_report.start_peak_resident_bytes,
            "steady_resident_bytes": profile.pipeline_memory_report.steady_resident_bytes,
            "peak_resident_bytes": profile.pipeline_memory_report.peak_resident_bytes,
            "steady_resident_growth_bytes": profile.pipeline_memory_report.steady_resident_growth_bytes,
            "lifetime_peak_resident_growth_bytes": profile.pipeline_memory_report.lifetime_peak_resident_growth_bytes,
            "total_page_faults": profile.pipeline_memory_report.total_page_faults,
            "minor_page_faults": profile.pipeline_memory_report.minor_page_faults,
            "major_page_faults": profile.pipeline_memory_report.major_page_faults,
        },
        "scan_pruning_report_count": profile.scan_pruning_reports.len(),
        "scan_pruning_reports": profile
            .scan_pruning_reports
            .iter()
            .map(scan_pruning_report_json)
            .collect::<Vec<_>>(),
    })
}

fn scan_pruning_report_json(report: &skein::store::ScanPruningReport) -> serde_json::Value {
    serde_json::json!({
        "target_kind": report.target_kind.as_str(),
        "label_id": report.label_id.map(|label_id| label_id.0),
        "rel_type_id": report.rel_type_id.map(|rel_type_id| rel_type_id.0),
        "strategy": scan_pruning_strategy_json(&report.strategy),
        "pruned": report.pruned,
        "exact_empty": report.exact_empty,
        "candidate_count_before_pruning": report.candidate_count_before_pruning,
        "pruned_candidate_count": report.pruned_candidate_count,
        "candidate_count_before_filter": report.candidate_count_before_filter,
        "output_count": report.output_count,
        "filtered_out_count": report.filtered_out_count,
    })
}

fn scan_pruning_strategy_json(strategy: &skein::store::ScanPruningStrategy) -> serde_json::Value {
    match strategy {
        skein::store::ScanPruningStrategy::FullLabelScan => {
            serde_json::json!({"kind": "full_label_scan"})
        }
        skein::store::ScanPruningStrategy::Empty => serde_json::json!({"kind": "empty"}),
        skein::store::ScanPruningStrategy::IdEq => serde_json::json!({"kind": "id_eq"}),
        skein::store::ScanPruningStrategy::IdIn => serde_json::json!({"kind": "id_in"}),
        skein::store::ScanPruningStrategy::IdRange => serde_json::json!({"kind": "id_range"}),
        skein::store::ScanPruningStrategy::PropertyEq { property } => {
            serde_json::json!({"kind": "property_eq", "property": property})
        }
        skein::store::ScanPruningStrategy::PropertyNotEq { property } => {
            serde_json::json!({"kind": "property_not_eq", "property": property})
        }
        skein::store::ScanPruningStrategy::PropertyMissingOrNull { property } => {
            serde_json::json!({"kind": "property_missing_or_null", "property": property})
        }
        skein::store::ScanPruningStrategy::PropertyExists { property } => {
            serde_json::json!({"kind": "property_exists", "property": property})
        }
        skein::store::ScanPruningStrategy::PropertyDefaultIfNullEq { property } => {
            serde_json::json!({"kind": "property_default_if_null_eq", "property": property})
        }
        skein::store::ScanPruningStrategy::PropertyDefaultIfNullNotEq { property } => {
            serde_json::json!({"kind": "property_default_if_null_not_eq", "property": property})
        }
        skein::store::ScanPruningStrategy::PropertyIn { property } => {
            serde_json::json!({"kind": "property_in", "property": property})
        }
        skein::store::ScanPruningStrategy::PropertyRange { property } => {
            serde_json::json!({"kind": "property_range", "property": property})
        }
        skein::store::ScanPruningStrategy::OrUnion => serde_json::json!({"kind": "or_union"}),
    }
}

fn physical_properties_json(
    properties: &skein::optimizer::PhysicalProperties,
) -> serde_json::Value {
    serde_json::json!({
        "distribution": distribution_json(&properties.distribution),
        "ordering": properties.ordering,
        "covering_fields": properties.covering_fields,
        "scan_pruning": properties.scan_pruning.as_str(),
        "vector_precision": properties.vector_precision.as_str(),
        "memory_budget": properties.memory_budget.as_str(),
    })
}

fn distribution_json(distribution: &skein::optimizer::Distribution) -> serde_json::Value {
    match distribution {
        skein::optimizer::Distribution::Any | skein::optimizer::Distribution::Single => {
            serde_json::json!({
                "kind": distribution.as_str(),
                "keys": [],
            })
        }
        skein::optimizer::Distribution::Hash(keys) => {
            serde_json::json!({
                "kind": distribution.as_str(),
                "keys": keys,
            })
        }
    }
}

fn optimizer_stage_json(stage: &skein::optimizer::StageTrace) -> serde_json::Value {
    let stats = stage.stats();
    serde_json::json!({
        "name": stage.name(),
        "apply_order": stage.apply_order().as_str(),
        "input_count": stats.input_count,
        "output_count": stats.output_count,
        "applied_rules": stats.applied_rules,
        "skipped_rules": stats.skipped_rules,
    })
}

fn rule_event_json(event: &skein::optimizer::RuleEvent) -> serde_json::Value {
    serde_json::json!({
        "rule": event.rule(),
        "outcome": event.outcome().as_str(),
        "detail": event.detail(),
    })
}

fn parse_parameters_json(raw_parameters: &str) -> Result<BTreeMap<String, Value>> {
    let value = serde_json::from_str::<serde_json::Value>(raw_parameters)
        .map_err(|error| SkeinError::Semantic(format!("invalid --params-json object: {error}")))?;
    let object = value
        .as_object()
        .ok_or_else(|| SkeinError::Semantic("--params-json must be a JSON object".to_string()))?;
    object
        .iter()
        .map(|(key, value)| Ok((key.clone(), value_from_json(value)?)))
        .collect()
}

fn endpoint_violations_json(
    violations: &[skein::CanonicalSnapshotEndpointViolation],
) -> serde_json::Value {
    serde_json::Value::Array(
        violations
            .iter()
            .map(|violation| {
                serde_json::json!({
                    "relationship_id": violation.relationship_id,
                    "missing_node_id": violation.missing_node_id,
                })
            })
            .collect(),
    )
}

fn value_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(value) => serde_json::Value::Bool(*value),
        Value::Int(value) => serde_json::json!(value),
        Value::Float(value) => serde_json::json!(value),
        Value::String(value) => serde_json::Value::String(value.clone()),
        Value::List(values) => {
            serde_json::Value::Array(values.iter().map(value_json).collect::<Vec<_>>())
        }
        Value::Map(values) => serde_json::Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), value_json(value)))
                .collect(),
        ),
    }
}

fn value_from_json(value: &serde_json::Value) -> Result<Value> {
    match value {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(value) => Ok(Value::Bool(*value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(Value::Int(value))
            } else if let Some(value) = value.as_f64() {
                Ok(Value::Float(value))
            } else {
                Err(SkeinError::Semantic(format!(
                    "unsupported JSON number in --params-json: {value}"
                )))
            }
        }
        serde_json::Value::String(value) => Ok(Value::String(value.clone())),
        serde_json::Value::Array(values) => values
            .iter()
            .map(value_from_json)
            .collect::<Result<Vec<_>>>()
            .map(Value::List),
        serde_json::Value::Object(values) => values
            .iter()
            .map(|(key, value)| Ok((key.clone(), value_from_json(value)?)))
            .collect::<Result<BTreeMap<_, _>>>()
            .map(Value::Map),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        add_cutover_evidence_report, add_shadow_ready_report, add_shadow_run_report,
        add_shadow_trace_report, background_maintenance_report_json_with_options,
        background_maintenance_report_usage, canonical_snapshot_validation_json,
        cutover_evidence_is_eligible, enforce_external_shadow_adapter_smoke_requirements,
        enforce_storage_recovery_requirements, explain_analyze_json_usage,
        explain_analyze_output_json, explain_json_usage, explain_output_json, explain_table_usage,
        external_shadow_adapter_smoke_fixture, external_shadow_adapter_smoke_report_json,
        graph_lightning_bootstrap_bundle_json,
        graph_lightning_bootstrap_bundle_json_with_storage_recovery,
        graph_lightning_bootstrap_bundle_usage, graph_lightning_bootstrap_manifest_json,
        graph_lightning_bootstrap_manifest_usage, graph_lightning_gc_staging_report,
        graph_lightning_graph_stream_usage, graph_lightning_graph_stream_validation_json,
        graph_lightning_import_status, graph_lightning_publish_staging_usage,
        graph_lightning_stage_bootstrap_usage, graph_lightning_verify_export_usage,
        graph_lightning_verify_published_usage, graph_lightning_verify_staging_usage,
        is_self_shadow_command, merge_replacement_summary_evidence,
        nowledge_bounded_read_report_json, nowledge_bounded_read_report_usage,
        nowledge_cypher_migration_gate_usage, parse_background_maintenance_limit,
        parse_max_blockers, parse_max_family_items, parse_max_wal_replay_entries,
        parse_parameters_json, parse_positive_usize, parse_shadow_timeout_ms,
        publish_graph_lightning_staging_catalog,
        publish_graph_lightning_staging_catalog_with_options, read_json_file,
        should_run_shadow_ready, stable_identity_audit_json,
        stage_graph_lightning_bootstrap_export,
        stage_graph_lightning_bootstrap_export_with_storage_recovery, storage_recovery_report_json,
        validate_canonical_snapshot_usage, value_json, verify_graph_lightning_published_manifest,
        verify_graph_lightning_staging_catalog, BackgroundMaintenanceReportOptions,
        PublishGraphLightningOptions, StorageRecoveryRequirements,
    };
    use skein::{
        api::ExplainOutput,
        optimizer::{OptimizerTrace, PhysicalPlan, PlanCost, PlanCostBreakdown},
    };
    use skein::{
        BackgroundMaintenanceOptions, CanonicalGraphSnapshotValidation,
        CanonicalSnapshotEndpointViolation, CanonicalSnapshotIdentityAudit, CompatibilityCheck,
        CompatibilityCheckReport, CompatibilityShadowCheckReport, CompatibilityShadowReport,
        CompatibilityShadowStatus, Database, ExternalShadowReady, GraphLightningBootstrapManifest,
        GraphLightningGraphStreamValidation, LocalQosPolicy, NowledgeMemReadOptions,
        PlanCacheStats, RecoveryMode, StorageRecoveryReport, Value, WorkClass, WorkRequest,
    };
    use std::collections::BTreeMap;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[test]
    fn detects_direct_self_shadow_binary() {
        assert!(is_self_shadow_command(
            "oracle",
            "target/debug/skein-shadow-self",
            &[]
        ));
    }

    #[test]
    fn detects_cargo_run_self_shadow() {
        assert!(is_self_shadow_command(
            "oracle",
            "cargo",
            &[
                "run".to_string(),
                "--quiet".to_string(),
                "--bin".to_string(),
                "skein-shadow-self".to_string(),
                "--".to_string(),
            ],
        ));
    }

    #[test]
    fn detects_self_shadow_name() {
        assert!(is_self_shadow_command(
            "self",
            "/usr/bin/legacy-wrapper",
            &[]
        ));
    }

    #[test]
    fn does_not_reject_named_external_shadow() {
        assert!(!is_self_shadow_command(
            "legacy-wrapper",
            "/usr/bin/nmem-graph-shadow",
            &[]
        ));
    }

    #[test]
    fn parses_shadow_timeout_ms() {
        assert_eq!(
            parse_shadow_timeout_ms("250").unwrap(),
            Duration::from_millis(250)
        );
    }

    #[test]
    fn rejects_zero_shadow_timeout_ms() {
        let error = parse_shadow_timeout_ms("0").unwrap_err();

        assert!(error
            .to_string()
            .contains("--shadow-timeout-ms must be greater than zero"));
    }

    #[test]
    fn parses_max_wal_replay_entries() {
        assert_eq!(parse_max_wal_replay_entries("3").unwrap(), 3);
    }

    #[test]
    fn rejects_zero_max_wal_replay_entries() {
        let error = parse_max_wal_replay_entries("0").unwrap_err();

        assert!(error
            .to_string()
            .contains("--max-wal-replay-entries must be greater than zero"));
    }

    #[test]
    fn parses_max_family_items() {
        assert_eq!(parse_max_family_items("3").unwrap(), 3);
    }

    #[test]
    fn rejects_zero_max_family_items() {
        let error = parse_max_family_items("0").unwrap_err();

        assert!(error
            .to_string()
            .contains("--max-family-items must be greater than zero"));
    }

    #[test]
    fn parses_max_blockers() {
        assert_eq!(parse_max_blockers("5").unwrap(), 5);
    }

    #[test]
    fn rejects_zero_max_blockers() {
        let error = parse_max_blockers("0").unwrap_err();

        assert!(error
            .to_string()
            .contains("--max-blockers must be greater than zero"));
    }

    #[test]
    fn nowledge_bounded_read_report_json_reads_shadow_database() {
        let db_path = unique_main_test_dir("bounded-read-report");
        {
            let mut db = Database::open(&db_path).unwrap();
            db.query("CREATE (:Memory {id: 'mem-read-report', title: 'bounded'})")
                .unwrap();
        }

        let report = nowledge_bounded_read_report_json(
            &db_path,
            "MATCH (m:Memory {id: $id}) RETURN m.title AS title",
            &BTreeMap::from([(
                "id".to_string(),
                Value::String("mem-read-report".to_string()),
            )]),
            &NowledgeMemReadOptions {
                max_rows: Some(4),
                max_estimated_payload_bytes: Some(1024),
            },
        )
        .unwrap();

        assert_eq!(report["protocol"], "skein-nowledge-mem-read-report");
        assert_eq!(report["mode"], "shadow_read_only");
        assert_eq!(report["row_count"], 1);
        assert_eq!(report["max_rows"], 4);
        assert_eq!(report["execution_row_cap"], 5);
        assert_eq!(report["row_limit_enforced_before_output"], true);
        assert_eq!(report["operator_row_cap_enabled"], true);
        assert!(report["intermediate_rows"].as_u64().unwrap() >= 1);
        assert!(report["intermediate_payload_bytes"].as_u64().unwrap() > 0);
        assert!(report["output_payload_bytes"].as_u64().unwrap() > 0);
        assert!(report["steady_resident_bytes"].as_u64().unwrap() > 0);
        assert!(report["peak_resident_bytes"].as_u64().unwrap() > 0);
        assert!(report["total_page_faults"].is_u64());
        assert_eq!(report["minor_page_faults"].is_u64(), cfg!(unix));
        assert_eq!(report["major_page_faults"].is_u64(), cfg!(unix));
        assert!(report.get("rows").is_none());
        std::fs::remove_dir_all(db_path).unwrap();
    }

    #[test]
    fn validates_nowledge_bounded_read_report_usage_text() {
        assert!(nowledge_bounded_read_report_usage().contains("--params-json"));
        assert!(nowledge_bounded_read_report_usage().contains("--max-rows"));
        assert!(nowledge_bounded_read_report_usage().contains("--max-estimated-payload-bytes"));
        assert!(nowledge_bounded_read_report_usage().contains("<database-path>"));
        assert!(nowledge_bounded_read_report_usage().contains("<cypher>"));
    }

    #[test]
    fn parses_positive_usize() {
        assert_eq!(parse_positive_usize("--limit", "7").unwrap(), 7);
        let error = parse_positive_usize("--limit", "0").unwrap_err();
        assert!(error
            .to_string()
            .contains("--limit must be greater than zero"));
    }

    #[test]
    fn merges_replacement_summary_evidence_artifacts() {
        let search_path = unique_json_file("search_projection_evidence");
        let shadow_path = unique_json_file("search_projection_shadow_evidence");
        let candidate_shadow_path = unique_json_file("search_candidate_shadow_evidence");
        let bounded_path = unique_json_file("bounded_read_evidence");
        let query_runtime_path = unique_json_file("query_runtime_preflight");
        let family_path = unique_json_file("query_family_evidence");
        std::fs::write(
            &search_path,
            serde_json::json!({
                "protocol": "skein-nowledge-search-projection-evidence-v1",
                "ready": true
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            &shadow_path,
            serde_json::json!({
                "protocol": "skein-nowledge-search-projection-shadow-evidence",
                "ready": true
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            &bounded_path,
            serde_json::json!({
                "protocol": "skein-nowledge-mem-bounded-read-evidence-v2",
                "ready": true
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            &candidate_shadow_path,
            serde_json::json!({
                "protocol": "skein-nowledge-search-candidate-shadow-evidence",
                "ready": true
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            &query_runtime_path,
            serde_json::json!({
                "protocol": "skein-nowledge-query-runtime-preflight-v1",
                "ready": true
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            &family_path,
            serde_json::json!({
                "protocol": "skein-nowledge-query-family-evidence-v1",
                "replacement_readiness_by_query_family": [
                    {
                        "query_family": "read",
                        "replacement_readiness_per_million": 1000000
                    }
                ]
            })
            .to_string(),
        )
        .unwrap();
        let mut bundle = serde_json::json!({
            "protocol": "skein-nowledge-cypher-migration-gate"
        });

        merge_replacement_summary_evidence(
            &mut bundle,
            Some(search_path.to_str().unwrap()),
            Some(shadow_path.to_str().unwrap()),
            Some(candidate_shadow_path.to_str().unwrap()),
            Some(bounded_path.to_str().unwrap()),
            Some(query_runtime_path.to_str().unwrap()),
            Some(family_path.to_str().unwrap()),
        )
        .unwrap();

        assert_eq!(bundle["search_projection_evidence"]["ready"], true);
        assert_eq!(bundle["search_projection_shadow_evidence"]["ready"], true);
        assert_eq!(bundle["search_candidate_shadow_evidence"]["ready"], true);
        assert_eq!(bundle["bounded_read_evidence"]["ready"], true);
        assert_eq!(bundle["query_runtime_preflight"]["ready"], true);
        assert_eq!(
            bundle["query_family_evidence"]["protocol"],
            "skein-nowledge-query-family-evidence-v1"
        );
        assert_eq!(
            bundle["replacement_readiness_by_query_family"][0]["query_family"],
            "read"
        );
        std::fs::remove_file(search_path).unwrap();
        std::fs::remove_file(shadow_path).unwrap();
        std::fs::remove_file(candidate_shadow_path).unwrap();
        std::fs::remove_file(bounded_path).unwrap();
        std::fs::remove_file(query_runtime_path).unwrap();
        std::fs::remove_file(family_path).unwrap();
    }

    #[test]
    fn require_ready_runs_shadow_ready_preflight() {
        assert!(should_run_shadow_ready(true, false, false));
    }

    #[test]
    fn require_cutover_evidence_runs_shadow_ready_preflight() {
        assert!(should_run_shadow_ready(false, true, false));
    }

    #[test]
    fn shadow_ready_runs_preflight_without_requiring_ready_decision() {
        assert!(should_run_shadow_ready(false, false, true));
    }

    #[test]
    fn skips_shadow_ready_preflight_by_default() {
        assert!(!should_run_shadow_ready(false, false, false));
    }

    #[test]
    fn adds_shadow_ready_report_to_migration_gate_bundle() {
        let mut bundle = serde_json::json!({
            "migration_gate": {
                "decision": "ready"
            }
        });
        let ready = ExternalShadowReady {
            protocol_version: 1,
            capabilities: vec![
                "execute".to_string(),
                "execute_session".to_string(),
                "project_graph".to_string(),
            ],
            engine_kind: Some("previous_wrapper".to_string()),
            wrapper_identity: Some("nowledge-previous-wrapper:test".to_string()),
        };

        add_shadow_ready_report(&mut bundle, &ready).unwrap();

        assert_eq!(bundle["shadow_ready"]["protocol_version"], 1);
        assert_eq!(
            bundle["shadow_ready"]["capabilities"],
            serde_json::json!(["execute", "execute_session", "project_graph"])
        );
        assert_eq!(bundle["shadow_ready"]["engine_kind"], "previous_wrapper");
    }

    #[test]
    fn adds_previous_wrapper_shadow_run_report_to_migration_gate_bundle() {
        let mut bundle = serde_json::json!({
            "migration_gate": {
                "decision": "ready"
            }
        });

        add_shadow_run_report(&mut bundle, "legacy-wrapper", false).unwrap();

        assert_eq!(bundle["shadow_run"]["shadow_name"], "legacy-wrapper");
        assert_eq!(bundle["shadow_run"]["self_shadow"], false);
        assert_eq!(bundle["shadow_run"]["evidence_kind"], "previous_wrapper");
    }

    #[test]
    fn marks_self_shadow_run_as_protocol_smoke() {
        let mut bundle = serde_json::json!({
            "migration_gate": {
                "decision": "ready"
            }
        });

        add_shadow_run_report(&mut bundle, "skein-shadow-self", true).unwrap();

        assert_eq!(bundle["shadow_run"]["shadow_name"], "skein-shadow-self");
        assert_eq!(bundle["shadow_run"]["self_shadow"], true);
        assert_eq!(bundle["shadow_run"]["evidence_kind"], "protocol_smoke");
    }

    #[test]
    fn adapter_smoke_fixture_exercises_session_and_project_graph() {
        let fixture = external_shadow_adapter_smoke_fixture();

        assert_eq!(fixture.name, "external-shadow-adapter-smoke");
        assert_eq!(fixture.setup.len(), 1);
        assert!(fixture.checks.iter().any(|check| matches!(
            check,
            CompatibilityCheck::Cypher(cypher)
                if cypher.name == "session query returns seeded memory"
                    && cypher.execution_mode == skein::compat::CypherExecutionMode::Session
        )));
        assert!(fixture.checks.iter().any(|check| matches!(
            check,
            CompatibilityCheck::ProjectedGraph(projected)
                if projected.name == "single memory projection"
                    && projected.expected_node_count == 1
                    && projected.expected_edge_count == 0
        )));
    }

    #[test]
    fn adapter_smoke_requires_previous_wrapper_when_requested() {
        let ready = ExternalShadowReady {
            protocol_version: 1,
            capabilities: vec![
                "execute".to_string(),
                "execute_session".to_string(),
                "project_graph".to_string(),
            ],
            engine_kind: Some("protocol_smoke".to_string()),
            wrapper_identity: None,
        };
        let report = adapter_smoke_report(vec![
            adapter_smoke_shadow_check(
                "session query returns seeded memory",
                CompatibilityShadowStatus::Matched,
                None,
            ),
            adapter_smoke_shadow_check(
                "single memory projection",
                CompatibilityShadowStatus::PrimaryOnly,
                Some("projection metadata is not exposed"),
            ),
        ]);

        let error =
            enforce_external_shadow_adapter_smoke_requirements(&ready, &report, true).unwrap_err();

        assert!(error
            .to_string()
            .contains("requires engine_kind 'previous_wrapper'"));
    }

    #[test]
    fn adapter_smoke_blocks_primary_only_projection_for_required_previous_wrapper() {
        let ready = ExternalShadowReady {
            protocol_version: 1,
            capabilities: vec![
                "execute".to_string(),
                "execute_session".to_string(),
                "project_graph".to_string(),
            ],
            engine_kind: Some("previous_wrapper".to_string()),
            wrapper_identity: Some("nowledge-previous-wrapper:test".to_string()),
        };
        let report = adapter_smoke_report(vec![
            adapter_smoke_shadow_check(
                "session query returns seeded memory",
                CompatibilityShadowStatus::Matched,
                None,
            ),
            adapter_smoke_shadow_check(
                "single memory projection",
                CompatibilityShadowStatus::PrimaryOnly,
                Some("projection metadata is not exposed"),
            ),
        ]);

        let error =
            enforce_external_shadow_adapter_smoke_requirements(&ready, &report, true).unwrap_err();
        assert!(error.to_string().contains(
            "requires all checks to run on previous-wrapper; primary-only checks: single memory projection"
        ));
        let json = external_shadow_adapter_smoke_report_json(&ready, &report, 3, None);

        assert_eq!(json["adapter_smoke_ready"], false);
        assert_eq!(json["dual_engine_evidence"]["ready"], false);
        assert_eq!(
            json["dual_engine_evidence"]["primary_engine"],
            serde_json::json!("skein")
        );
        assert_eq!(
            json["dual_engine_evidence"]["shadow_engine"],
            serde_json::json!("legacy-wrapper")
        );
        assert_eq!(json["dual_engine_evidence"]["primary_check_count"], 2);
        assert_eq!(json["dual_engine_evidence"]["shadow_check_count"], 2);
        assert_eq!(json["dual_engine_evidence"]["matched_check_count"], 1);
        assert_eq!(json["dual_engine_evidence"]["primary_only_check_count"], 1);
        assert_eq!(json["matched_checks"], 1);
        assert_eq!(json["primary_only_checks"], 1);
        assert_eq!(
            json["primary_only_reasons"]["single memory projection"],
            "projection metadata is not exposed"
        );
    }

    #[test]
    fn marks_previous_wrapper_ready_bundle_as_cutover_evidence() {
        let mut bundle = serde_json::json!({
            "migration_gate": {
                "decision": "ready",
                "shadow_evidence_present": true
            }
        });

        let ready = ExternalShadowReady {
            protocol_version: 1,
            capabilities: vec![
                "execute".to_string(),
                "execute_session".to_string(),
                "project_graph".to_string(),
            ],
            engine_kind: Some("previous_wrapper".to_string()),
            wrapper_identity: Some("nowledge-previous-wrapper:test".to_string()),
        };

        add_cutover_evidence_report(&mut bundle, false, Some(&ready), false, false).unwrap();

        assert_eq!(bundle["cutover_evidence"]["eligible"], true);
        assert!(cutover_evidence_is_eligible(&bundle));
        assert_eq!(
            bundle["cutover_evidence"]["evidence_kind"],
            "previous_wrapper"
        );
        assert_eq!(
            bundle["cutover_evidence"]["requires_ready_wrapper_identity"],
            true
        );
        assert_eq!(
            bundle["cutover_evidence"]["ready_wrapper_identity"],
            "nowledge-previous-wrapper:test"
        );
        assert_eq!(
            bundle["cutover_evidence"]["ready_missing_capabilities"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            bundle["cutover_evidence"]["blockers"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
        assert_eq!(bundle["cutover_evidence"]["shadow_trace_present"], false);
        assert_eq!(bundle["cutover_evidence"]["shadow_trace_complete"], true);
        assert_eq!(
            bundle["cutover_evidence"]["storage_recovery_present"],
            false
        );
        assert_eq!(bundle["cutover_evidence"]["storage_recovery_ready"], true);
        assert_eq!(
            bundle["cutover_evidence"]["background_maintenance_required"],
            false
        );
        assert_eq!(
            bundle["cutover_evidence"]["background_maintenance_present"],
            false
        );
        assert_eq!(
            bundle["cutover_evidence"]["background_maintenance_ready"],
            true
        );
        assert_eq!(
            bundle["cutover_evidence"]["replacement_readiness_family_report_present"],
            false
        );
        assert!(bundle["cutover_evidence"]["replacement_readiness_min_per_million"].is_null());
    }

    #[test]
    fn requires_previous_wrapper_identity_for_cutover_evidence() {
        let mut bundle = serde_json::json!({
            "migration_gate": {
                "decision": "ready",
                "shadow_evidence_present": true
            }
        });

        let ready = ExternalShadowReady {
            protocol_version: 1,
            capabilities: vec![
                "execute".to_string(),
                "execute_session".to_string(),
                "project_graph".to_string(),
            ],
            engine_kind: Some("previous_wrapper".to_string()),
            wrapper_identity: None,
        };

        add_cutover_evidence_report(&mut bundle, false, Some(&ready), false, false).unwrap();

        assert_eq!(bundle["cutover_evidence"]["eligible"], false);
        assert!(!cutover_evidence_is_eligible(&bundle));
        assert_eq!(
            bundle["cutover_evidence"]["requires_ready_wrapper_identity"],
            true
        );
        assert!(bundle["cutover_evidence"]["ready_wrapper_identity"].is_null());
        assert_eq!(
            bundle["cutover_evidence"]["blockers"][0],
            "shadow ready response missing wrapper_identity"
        );
    }

    #[test]
    fn blocks_cutover_when_ready_missing_required_capabilities() {
        let mut bundle = serde_json::json!({
            "migration_gate": {
                "decision": "ready",
                "shadow_evidence_present": true
            }
        });

        let ready = ExternalShadowReady {
            protocol_version: 1,
            capabilities: vec!["execute".to_string(), "project_graph".to_string()],
            engine_kind: Some("previous_wrapper".to_string()),
            wrapper_identity: Some("nowledge-previous-wrapper:test".to_string()),
        };

        add_cutover_evidence_report(&mut bundle, false, Some(&ready), false, false).unwrap();

        assert_eq!(bundle["cutover_evidence"]["eligible"], false);
        assert!(!cutover_evidence_is_eligible(&bundle));
        assert_eq!(
            bundle["cutover_evidence"]["ready_missing_capabilities"][0],
            "execute_session"
        );
        assert_eq!(
            bundle["cutover_evidence"]["blockers"][0],
            "shadow ready response missing required capabilities"
        );
    }

    #[test]
    fn blocks_cutover_when_required_storage_recovery_evidence_is_missing() {
        let mut bundle = serde_json::json!({
            "migration_gate": {
                "decision": "ready",
                "shadow_evidence_present": true
            }
        });

        let ready = ExternalShadowReady {
            protocol_version: 1,
            capabilities: vec![
                "execute".to_string(),
                "execute_session".to_string(),
                "project_graph".to_string(),
            ],
            engine_kind: Some("previous_wrapper".to_string()),
            wrapper_identity: Some("nowledge-previous-wrapper:test".to_string()),
        };

        add_cutover_evidence_report(&mut bundle, false, Some(&ready), true, false).unwrap();

        assert_eq!(bundle["cutover_evidence"]["eligible"], false);
        assert!(!cutover_evidence_is_eligible(&bundle));
        assert_eq!(
            bundle["cutover_evidence"]["storage_recovery_required"],
            true
        );
        assert_eq!(
            bundle["cutover_evidence"]["storage_recovery_present"],
            false
        );
        assert_eq!(
            bundle["cutover_evidence"]["storage_recovery_blockers"][0],
            "storage recovery evidence is required before cutover"
        );
        assert_eq!(
            bundle["cutover_evidence"]["storage_recovery_blocker_codes"][0],
            "missing_evidence"
        );
        assert_eq!(
            bundle["cutover_evidence"]["blockers"][0],
            "storage recovery evidence is required before cutover"
        );
    }

    #[test]
    fn blocks_cutover_when_required_background_maintenance_evidence_is_missing() {
        let mut bundle = serde_json::json!({
            "migration_gate": {
                "decision": "ready",
                "shadow_evidence_present": true
            }
        });

        let ready = ExternalShadowReady {
            protocol_version: 1,
            capabilities: vec![
                "execute".to_string(),
                "execute_session".to_string(),
                "project_graph".to_string(),
            ],
            engine_kind: Some("previous_wrapper".to_string()),
            wrapper_identity: Some("nowledge-previous-wrapper:test".to_string()),
        };

        add_cutover_evidence_report(&mut bundle, false, Some(&ready), false, true).unwrap();

        assert_eq!(bundle["cutover_evidence"]["eligible"], false);
        assert!(!cutover_evidence_is_eligible(&bundle));
        assert_eq!(
            bundle["cutover_evidence"]["background_maintenance_required"],
            true
        );
        assert_eq!(
            bundle["cutover_evidence"]["background_maintenance_present"],
            false
        );
        assert_eq!(
            bundle["cutover_evidence"]["background_maintenance_blockers"][0],
            "background maintenance evidence is required before cutover"
        );
        assert_eq!(
            bundle["cutover_evidence"]["background_maintenance_blocker_codes"][0],
            "missing_evidence"
        );
        assert_eq!(
            bundle["cutover_evidence"]["blockers"][0],
            "background maintenance evidence is required before cutover"
        );
    }

    #[test]
    fn blocks_cutover_when_background_maintenance_ranks_foreground_work() {
        let mut bundle = serde_json::json!({
            "migration_gate": {
                "decision": "ready",
                "shadow_evidence_present": true
            },
            "background_maintenance": {
                "total_candidates": 1,
                "foreground_admission_probe_ready": true,
                "foreground_admission_probe_admission": "admit",
                "ranked": [
                    {
                        "kind": "schema_maintenance",
                        "work_class": "mutation",
                        "priority": "foreground",
                        "admission": "admit"
                    }
                ]
            }
        });

        let ready = ExternalShadowReady {
            protocol_version: 1,
            capabilities: vec![
                "execute".to_string(),
                "execute_session".to_string(),
                "project_graph".to_string(),
            ],
            engine_kind: Some("previous_wrapper".to_string()),
            wrapper_identity: Some("nowledge-previous-wrapper:test".to_string()),
        };

        add_cutover_evidence_report(&mut bundle, false, Some(&ready), false, true).unwrap();

        assert_eq!(bundle["cutover_evidence"]["eligible"], false);
        assert!(!cutover_evidence_is_eligible(&bundle));
        assert_eq!(
            bundle["cutover_evidence"]["background_maintenance_present"],
            true
        );
        assert_eq!(
            bundle["cutover_evidence"]["background_maintenance_foreground_ranked_count"],
            1
        );
        assert_eq!(
            bundle["cutover_evidence"]["background_maintenance_blockers"][0],
            "background maintenance evidence ranked foreground work"
        );
        assert_eq!(
            bundle["cutover_evidence"]["background_maintenance_blocker_codes"][0],
            "foreground_ranked_work"
        );
    }

    #[test]
    fn blocks_cutover_when_query_family_replacement_readiness_is_incomplete() {
        let mut bundle = serde_json::json!({
            "migration_gate": {
                "decision": "ready",
                "shadow_evidence_present": true
            },
            "replacement_readiness_by_query_family": [
                {
                    "query_family": "mutation",
                    "required_checks": 1,
                    "covered_checks": 1,
                    "shadow_matched_checks": 0,
                    "shadow_primary_only_checks": 1,
                    "replacement_readiness_per_million": 0
                },
                {
                    "query_family": "read",
                    "required_checks": 1,
                    "covered_checks": 1,
                    "shadow_matched_checks": 1,
                    "shadow_primary_only_checks": 0,
                    "replacement_readiness_per_million": 1_000_000
                }
            ]
        });

        let ready = ExternalShadowReady {
            protocol_version: 1,
            capabilities: vec![
                "execute".to_string(),
                "execute_session".to_string(),
                "project_graph".to_string(),
            ],
            engine_kind: Some("previous_wrapper".to_string()),
            wrapper_identity: Some("nowledge-previous-wrapper:test".to_string()),
        };

        add_cutover_evidence_report(&mut bundle, false, Some(&ready), false, false).unwrap();

        assert_eq!(bundle["cutover_evidence"]["eligible"], false);
        assert!(!cutover_evidence_is_eligible(&bundle));
        assert_eq!(
            bundle["cutover_evidence"]["replacement_readiness_family_report_present"],
            true
        );
        assert_eq!(
            bundle["cutover_evidence"]["replacement_readiness_min_per_million"],
            0
        );
        assert_eq!(
            bundle["cutover_evidence"]["replacement_readiness_invalid_family_count"],
            0
        );
        assert_eq!(
            bundle["cutover_evidence"]["replacement_readiness_blocked_query_families"][0],
            "mutation"
        );
        assert_eq!(
            bundle["cutover_evidence"]["replacement_readiness_blockers"][0],
            "replacement readiness is incomplete for query families: mutation"
        );
    }

    #[test]
    fn blocks_cutover_when_present_shadow_trace_is_incomplete() {
        let mut bundle = serde_json::json!({
            "migration_gate": {
                "decision": "ready",
                "shadow_evidence_present": true
            },
            "shadow_trace": {
                "summary_available": true,
                "request_count": 2,
                "request_events": 2,
                "pending_request_count": 1
            }
        });

        let ready = ExternalShadowReady {
            protocol_version: 1,
            capabilities: vec![
                "execute".to_string(),
                "execute_session".to_string(),
                "project_graph".to_string(),
            ],
            engine_kind: Some("previous_wrapper".to_string()),
            wrapper_identity: Some("nowledge-previous-wrapper:test".to_string()),
        };

        add_cutover_evidence_report(&mut bundle, false, Some(&ready), false, false).unwrap();

        assert_eq!(bundle["cutover_evidence"]["eligible"], false);
        assert!(!cutover_evidence_is_eligible(&bundle));
        assert_eq!(bundle["cutover_evidence"]["shadow_trace_present"], true);
        assert_eq!(bundle["cutover_evidence"]["shadow_trace_complete"], false);
        assert_eq!(
            bundle["cutover_evidence"]["shadow_trace_pending_request_count"],
            1
        );
        assert_eq!(
            bundle["cutover_evidence"]["blockers"][0],
            "shadow trace is incomplete or unavailable"
        );
    }

    #[test]
    fn rejects_protocol_smoke_as_cutover_evidence() {
        let mut bundle = serde_json::json!({
            "migration_gate": {
                "decision": "ready",
                "shadow_evidence_present": true
            }
        });

        let ready = ExternalShadowReady {
            protocol_version: 1,
            capabilities: vec![
                "execute".to_string(),
                "execute_session".to_string(),
                "project_graph".to_string(),
            ],
            engine_kind: Some("protocol_smoke".to_string()),
            wrapper_identity: None,
        };

        add_cutover_evidence_report(&mut bundle, true, Some(&ready), false, false).unwrap();

        assert_eq!(bundle["cutover_evidence"]["eligible"], false);
        assert!(!cutover_evidence_is_eligible(&bundle));
        assert_eq!(
            bundle["cutover_evidence"]["evidence_kind"],
            "protocol_smoke"
        );
        assert_eq!(
            bundle["cutover_evidence"]["blockers"][0],
            "shadow run is protocol smoke, not previous-wrapper evidence"
        );
    }

    #[test]
    fn requires_ready_preflight_for_cutover_evidence() {
        let mut bundle = serde_json::json!({
            "migration_gate": {
                "decision": "ready",
                "shadow_evidence_present": true
            }
        });

        add_cutover_evidence_report(&mut bundle, false, None, false, false).unwrap();

        assert_eq!(bundle["cutover_evidence"]["eligible"], false);
        assert!(!cutover_evidence_is_eligible(&bundle));
        assert_eq!(
            bundle["cutover_evidence"]["blockers"][0],
            "shadow ready preflight was not executed"
        );
    }

    #[test]
    fn requires_previous_wrapper_ready_engine_kind_for_cutover_evidence() {
        let mut bundle = serde_json::json!({
            "migration_gate": {
                "decision": "ready",
                "shadow_evidence_present": true
            }
        });
        let ready = ExternalShadowReady {
            protocol_version: 1,
            capabilities: vec![
                "execute".to_string(),
                "execute_session".to_string(),
                "project_graph".to_string(),
            ],
            engine_kind: None,
            wrapper_identity: None,
        };

        add_cutover_evidence_report(&mut bundle, false, Some(&ready), false, false).unwrap();

        assert_eq!(bundle["cutover_evidence"]["eligible"], false);
        assert!(!cutover_evidence_is_eligible(&bundle));
        assert_eq!(
            bundle["cutover_evidence"]["blockers"][0],
            "shadow ready response missing engine_kind"
        );
    }

    #[test]
    fn missing_cutover_evidence_is_not_eligible() {
        let bundle = serde_json::json!({
            "migration_gate": {
                "decision": "ready",
                "shadow_evidence_present": true
            }
        });

        assert!(!cutover_evidence_is_eligible(&bundle));
    }

    #[test]
    fn adds_shadow_trace_report_to_migration_gate_bundle() {
        let mut bundle = serde_json::json!({
            "migration_gate": {
                "decision": "ready"
            }
        });
        let trace_path = std::env::temp_dir().join(format!(
            "skein-shadow-trace-report-{}.jsonl",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &trace_path,
            r#"{"sequence":1,"event":"request","payload":{"op":"ready"}}"#.to_string()
                + "\n"
                + r#"{"sequence":1,"event":"response","payload":{"ready":true}}"#
                + "\n"
                + r#"{"sequence":2,"event":"request","payload":{"op":"execute"}}"#
                + "\n"
                + r#"{"sequence":2,"event":"error","payload":{"message":"failed"}}"#
                + "\n"
                + r#"{"sequence":3,"event":"request","payload":{"op":"project_graph"}}"#
                + "\n",
        )
        .unwrap();

        add_shadow_trace_report(&mut bundle, trace_path.to_str().unwrap(), 3).unwrap();

        assert_eq!(bundle["shadow_trace"]["path"], "<redacted>");
        assert_eq!(bundle["shadow_trace"]["path_redacted"], true);
        assert_eq!(bundle["shadow_trace"]["request_count"], 3);
        assert_eq!(bundle["shadow_trace"]["summary_available"], true);
        assert_eq!(bundle["shadow_trace"]["trace_record_count"], 5);
        assert_eq!(bundle["shadow_trace"]["request_events"], 3);
        assert_eq!(bundle["shadow_trace"]["response_events"], 1);
        assert_eq!(bundle["shadow_trace"]["error_events"], 1);
        assert_eq!(bundle["shadow_trace"]["completed_request_count"], 2);
        assert_eq!(bundle["shadow_trace"]["pending_request_count"], 1);
        assert_eq!(bundle["shadow_trace"]["request_op_counts"]["ready"], 1);
        assert_eq!(bundle["shadow_trace"]["request_op_counts"]["execute"], 1);
        assert_eq!(
            bundle["shadow_trace"]["request_op_counts"]["project_graph"],
            1
        );
        assert_eq!(bundle["shadow_trace"]["response_op_counts"]["ready"], 1);
        assert_eq!(bundle["shadow_trace"]["error_op_counts"]["execute"], 1);
        assert_eq!(
            bundle["shadow_trace"]["pending_op_counts"]["project_graph"],
            1
        );
        std::fs::remove_file(trace_path).unwrap();
    }

    #[test]
    fn renders_canonical_snapshot_validation_json() {
        let validation = CanonicalGraphSnapshotValidation {
            is_valid: false,
            is_import_ready: false,
            checksum_matches: false,
            expected_logical_checksum: 77,
            stable_identity_matches: false,
            stable_identity_ready: false,
            expected_stable_identity: CanonicalSnapshotIdentityAudit {
                requires_stable_id_mapping: true,
                nodes_without_stable_id: vec![1],
                relationships_without_stable_id: vec![2],
                duplicate_node_stable_ids: vec![Value::String("dup-node".to_string())],
                duplicate_relationship_stable_ids: vec![Value::String("dup-rel".to_string())],
            },
            duplicate_node_ids: vec![1],
            duplicate_relationship_ids: vec![2],
            missing_sources: vec![CanonicalSnapshotEndpointViolation {
                relationship_id: 2,
                missing_node_id: 10,
            }],
            missing_targets: vec![CanonicalSnapshotEndpointViolation {
                relationship_id: 3,
                missing_node_id: 11,
            }],
        };

        let json = canonical_snapshot_validation_json(5, 99, 3, 2, &validation);

        assert_eq!(json["graph_commit_epoch"], 5);
        assert_eq!(json["logical_checksum"], 99);
        assert_eq!(json["node_count"], 3);
        assert_eq!(json["relationship_count"], 2);
        assert_eq!(json["validation"]["is_valid"], false);
        assert_eq!(json["validation"]["is_import_ready"], false);
        assert_eq!(json["validation"]["expected_logical_checksum"], 77);
        assert_eq!(json["validation"]["stable_identity_ready"], false);
        assert_eq!(
            json["validation"]["expected_stable_identity"]["duplicate_node_stable_ids"],
            serde_json::json!(["dup-node"])
        );
        assert_eq!(
            json["validation"]["missing_sources"][0]["missing_node_id"],
            10
        );
        assert_eq!(
            json["validation"]["missing_targets"][0]["relationship_id"],
            3
        );
    }

    #[test]
    fn renders_storage_recovery_report_json() {
        let report = StorageRecoveryReport {
            durable: true,
            recovery_mode: RecoveryMode::Strict,
            max_wal_replay_entries: Some(64),
            max_wal_replay_bytes: Some(4096),
            max_wal_record_bytes: Some(1024),
            checkpoint_epoch: Some(2),
            checkpoint_commit_epoch: Some(8),
            wal_present: true,
            wal_replay_start_lsn: Some(9),
            next_lsn_after_replay: Some(11),
            replayed_wal_entries: 2,
            torn_tail_ignored: true,
            torn_tail_reason: Some("checksum mismatch".to_string()),
            recovered_commit_epoch: 10,
            ..StorageRecoveryReport::default()
        };

        let json = storage_recovery_report_json("skein-storage-v1", &report);

        assert_eq!(json["protocol"], "skein-storage-recovery-report");
        assert_eq!(json["storage_version"], "skein-storage-v1");
        assert_eq!(json["recovery_mode"], "strict");
        assert_eq!(json["max_wal_replay_entries"], 64);
        assert_eq!(json["checkpoint_epoch"], 2);
        assert_eq!(json["checkpoint_commit_epoch"], 8);
        assert_eq!(json["wal_replay_start_lsn"], 9);
        assert_eq!(json["next_lsn_after_replay"], 11);
        assert_eq!(json["replayed_wal_entries"], 2);
        assert_eq!(json["torn_tail_ignored"], true);
        assert_eq!(json["torn_tail_reason"], "checksum mismatch");
        assert_eq!(json["recovered_commit_epoch"], 10);
        assert_eq!(json["readiness"]["durable_recovery_observed"], true);
        assert_eq!(json["readiness"]["checkpoint_boundary_present"], true);
        assert_eq!(json["readiness"]["wal_replay_bounded"], true);
        assert_eq!(json["readiness"]["torn_tail_clean"], false);
    }

    #[test]
    fn renders_background_maintenance_report_json() {
        let db = Database::new();

        let json = background_maintenance_report_json_with_options(
            &db,
            &BackgroundMaintenanceReportOptions::default(),
        );

        assert_eq!(json["protocol"], "skein-background-maintenance-report");
        assert_eq!(json["qos_policy"]["background_enabled"], true);
        assert_eq!(json["qos_policy"]["max_background_operations"], 1024);
        assert_eq!(json["qos_state"]["running_background_operations"], 0);
        assert_eq!(json["total_candidates"], 0);
        assert_eq!(json["admitted_count"], 0);
        assert_eq!(json["deferred_count"], 0);
        assert_eq!(json["rejected_count"], 0);
        assert!(json["top_admitted_kind"].is_null());
        assert!(json["ranked"].as_array().unwrap().is_empty());
    }

    #[test]
    fn background_maintenance_report_can_prove_projection_qos_deferral() {
        let mut db = Database::new();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Graph delta one'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 2, title: 'Graph delta two'})")
            .unwrap();
        let mut policy = LocalQosPolicy::default();
        policy.max_background_operations_by_class[WorkClass::Projection.as_index()] = Some(1);
        let json = background_maintenance_report_json_with_options(
            &db,
            &BackgroundMaintenanceReportOptions {
                policy,
                maintenance: BackgroundMaintenanceOptions {
                    include_schema_maintenance: false,
                    include_property_index_projection: false,
                    include_search_projection_rebuild: false,
                    include_search_projection_metadata_repair: false,
                    include_graph_lightning_bootstrap_export: false,
                    include_external_content_artifact_jobs: false,
                    ..BackgroundMaintenanceOptions::default()
                },
                ..BackgroundMaintenanceReportOptions::default()
            },
        );

        assert_eq!(json["protocol"], "skein-background-maintenance-report");
        assert_eq!(
            json["qos_policy"]["max_background_operations_by_class"]["projection"],
            1
        );
        assert_eq!(json["total_candidates"], 1);
        assert_eq!(json["admitted_count"], 0);
        assert_eq!(json["deferred_count"], 1);
        assert_eq!(json["executable_search_projection_graph_delta_count"], 1);
        assert_eq!(json["deferred_search_projection_graph_delta_count"], 1);
        assert_eq!(json["ranked"][0]["kind"], "search_projection_graph_delta");
        assert_eq!(json["ranked"][0]["work_class"], "projection");
        assert_eq!(json["ranked"][0]["priority"], "background");
        assert_eq!(json["ranked"][0]["admission"], "defer");
        assert_eq!(
            json["ranked"][0]["admission_code"],
            "class_background_limit_exceeded"
        );
        assert_eq!(
            json["ranked"][0]["search_projection_graph_delta_operation_count"],
            2
        );
    }

    #[test]
    fn background_maintenance_report_usage_mentions_cutover_ready_gate() {
        assert!(background_maintenance_report_usage().contains("--require-cutover-ready"));
        assert!(background_maintenance_report_usage().contains("--disable-background"));
        assert!(background_maintenance_report_usage().contains("--max-background-operations"));
        assert!(background_maintenance_report_usage().contains("--max-total-background-operations"));
        assert!(background_maintenance_report_usage()
            .contains("--max-projection-background-operations"));
        assert!(background_maintenance_report_usage().contains("<database-path>"));
    }

    #[test]
    fn background_maintenance_limit_parser_rejects_zero() {
        assert_eq!(
            parse_background_maintenance_limit("--limit", "3").unwrap(),
            3
        );
        let error = parse_background_maintenance_limit("--limit", "0").unwrap_err();
        assert!(error.to_string().contains("must be greater than zero"));
    }

    #[test]
    fn migration_gate_usage_mentions_background_report_input() {
        assert!(
            nowledge_cypher_migration_gate_usage().contains("--background-maintenance-report-json")
        );
        assert!(nowledge_cypher_migration_gate_usage()
            .contains("--previous-wrapper-contract-evidence-json"));
    }

    #[test]
    fn empty_background_maintenance_report_fails_cutover_health_with_codes() {
        let db = Database::new();
        let report = background_maintenance_report_json_with_options(
            &db,
            &BackgroundMaintenanceReportOptions::default(),
        );
        let bundle = serde_json::json!({
            "background_maintenance": report
        });

        let health = skein::background_maintenance_evidence_health_from_bundle(&bundle, true);

        assert!(!health.ready);
        assert_eq!(health.protocol_matches, Some(true));
        assert_eq!(
            health.blocker_codes,
            vec!["no_candidates".to_string(), "no_ranked_work".to_string()]
        );
        assert_eq!(health.slow_query_ready, Some(true));
        assert_eq!(health.slow_query_record_count, Some(0));
        assert_eq!(health.slow_query_redaction_ready, Some(true));
    }

    #[test]
    fn renders_explain_output_json_with_structured_plan_summary() {
        let mut operator_counts = BTreeMap::new();
        operator_counts.insert("SeqNodeScan".to_string(), 1);
        let mut class_counts = BTreeMap::new();
        class_counts.insert("access".to_string(), 1);
        let output = ExplainOutput {
            physical_plan: PhysicalPlan::SeqNodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            },
            work_request: WorkRequest::background(WorkClass::Analytics, 64),
            plan_cache_lookup: skein::PlanCacheLookup::Miss,
            statement_kind: "match_return",
            trace: OptimizerTrace {
                groups: 1,
                search_mode: skein::optimizer::SearchMode::Memo,
                query_digest: Some("q1:fixture".to_string()),
                selected_plan: "SeqNodeScan variable=m label=Memory".to_string(),
                selected_plan_fingerprint: "SeqNodeScan".to_string(),
                selected_plan_cost: PlanCost {
                    estimated_rows: 42,
                    cost: 42,
                },
                selected_plan_cost_breakdown: PlanCostBreakdown {
                    estimated_rows: 42,
                    cost: 42,
                    cpu: 10,
                    random_io: 20,
                    sequential_io: 12,
                    output_rows: 0,
                },
                selected_plan_properties: skein::optimizer::PhysicalProperties {
                    distribution: skein::optimizer::Distribution::Single,
                    ordering: vec!["title asc".to_string()],
                    covering_fields: vec!["Memory.title".to_string()],
                    scan_pruning: skein::optimizer::ScanPruningSupport::Index,
                    vector_precision: skein::optimizer::VectorPrecision::NotVector,
                    memory_budget: skein::optimizer::MemoryBudgetClass::RowLinear,
                },
                selected_plan_operator_counts: operator_counts,
                selected_plan_class_counts: class_counts,
                warnings: vec!["diagnostic warning".to_string()],
                decisions: vec!["diagnostic decision".to_string()],
                rule_events: vec![skein::optimizer::RuleEvent::applied(
                    "implementation:node_equality_index_seek",
                    "priority=100 property=id",
                )],
                stage_events: vec![skein::optimizer::OptimizationStage::new(
                    "physical_search",
                    skein::optimizer::ApplyOrder::BottomUp,
                )
                .trace(skein::optimizer::StageStats::new(2, 1).with_rule_counts(1, 3))],
            },
        };

        let parameters = BTreeMap::from([("id".to_string(), Value::Int(42))]);
        let plan_cache_stats = PlanCacheStats {
            max_entries: Some(128),
            entries: 1,
            hits: 2,
            misses: 3,
            admissions: 6,
            disabled_misses: 1,
            bypasses: 5,
            evictions: 4,
            memory_pressure_events: 2,
        };
        let json = explain_output_json(
            "MATCH (m:Memory {id: $id}) RETURN m",
            &parameters,
            &output,
            &plan_cache_stats,
        );

        assert_eq!(json["protocol"], "skein-explain");
        assert_eq!(json["protocol_version"], 1);
        assert_eq!(json["search_mode"], "memo");
        assert_eq!(json["statement_kind"], "match_return");
        assert_eq!(json["parameters"]["id"], 42);
        assert_eq!(json["selected_plan_fingerprint"], "SeqNodeScan");
        assert_eq!(json["query_digest"], "q1:fixture");
        assert_eq!(json["selected_plan_cost"]["estimated_rows"], 42);
        assert_eq!(json["selected_plan_cost_breakdown"]["cost"], 42);
        assert_eq!(json["selected_plan_cost_breakdown"]["cpu"], 10);
        assert_eq!(json["selected_plan_cost_breakdown"]["random_io"], 20);
        assert_eq!(json["selected_plan_cost_breakdown"]["sequential_io"], 12);
        assert_eq!(
            json["selected_plan_properties"]["distribution"]["kind"],
            "single"
        );
        assert_eq!(json["selected_plan_properties"]["ordering"][0], "title asc");
        assert_eq!(
            json["selected_plan_properties"]["covering_fields"][0],
            "Memory.title"
        );
        assert_eq!(json["selected_plan_properties"]["scan_pruning"], "index");
        assert_eq!(
            json["selected_plan_properties"]["vector_precision"],
            "not_vector"
        );
        assert_eq!(
            json["selected_plan_properties"]["memory_budget"],
            "row_linear"
        );
        assert_eq!(json["selected_plan_operator_counts"]["SeqNodeScan"], 1);
        assert_eq!(json["selected_plan_class_counts"]["access"], 1);
        assert_eq!(json["optimizer_stages"][0]["name"], "physical_search");
        assert_eq!(json["optimizer_stages"][0]["apply_order"], "bottom_up");
        assert_eq!(json["optimizer_stages"][0]["input_count"], 2);
        assert_eq!(json["optimizer_stages"][0]["output_count"], 1);
        assert_eq!(json["optimizer_stages"][0]["applied_rules"], 1);
        assert_eq!(json["optimizer_stages"][0]["skipped_rules"], 3);
        assert_eq!(json["work_request"]["priority"], "background");
        assert_eq!(json["work_request"]["class"], "analytics");
        assert_eq!(json["work_request"]["estimated_operations"], 64);
        assert_eq!(json["plan_cache_lookup"]["event"], "miss");
        assert_eq!(
            json["plan_cache_lookup"]["bypass_reason"],
            serde_json::Value::Null
        );
        assert_eq!(json["plan_cache_stats"]["max_entries"], 128);
        assert_eq!(json["plan_cache_stats"]["entries"], 1);
        assert_eq!(json["plan_cache_stats"]["hits"], 2);
        assert_eq!(json["plan_cache_stats"]["misses"], 3);
        assert_eq!(json["plan_cache_stats"]["admissions"], 6);
        assert_eq!(json["plan_cache_stats"]["disabled_misses"], 1);
        assert_eq!(json["plan_cache_stats"]["bypasses"], 5);
        assert_eq!(json["plan_cache_stats"]["evictions"], 4);
        assert_eq!(json["plan_cache_stats"]["memory_pressure_events"], 2);
        assert_eq!(json["warnings"][0], "diagnostic warning");
        assert_eq!(json["decisions"][0], "diagnostic decision");
        assert_eq!(
            json["rule_events"][0]["rule"],
            "implementation:node_equality_index_seek"
        );
        assert_eq!(json["rule_events"][0]["outcome"], "apply");
        assert_eq!(json["rule_events"][0]["detail"], "priority=100 property=id");
    }

    #[test]
    fn renders_explain_analyze_output_json_with_scan_pruning_reports() {
        let db_path = unique_main_test_dir("explain-analyze-json");
        let mut db = Database::open(&db_path).unwrap();
        db.query("CREATE (:Memory {id: 'mem-a', kind: 'note', title: 'A'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'mem-b', kind: 'task', title: 'B'})")
            .unwrap();

        let query = "MATCH (m:Memory) WHERE m.kind = $kind RETURN m.title AS title";
        let parameters = BTreeMap::from([("kind".to_string(), Value::String("note".to_string()))]);
        let output = db
            .explain_analyze_query_with_params(query, &parameters)
            .unwrap();
        let json = explain_analyze_output_json(query, &parameters, &output, &db.plan_cache_stats());

        assert_eq!(json["protocol"], "skein-explain-analyze");
        assert_eq!(json["protocol_version"], 1);
        assert_eq!(json["statement_kind"], "match_return");
        assert_eq!(json["parameters"]["kind"], "note");
        assert_eq!(json["output_row_count"], 1);
        assert_eq!(json["execution_profile"]["scan_pruning_report_count"], 1);
        assert_eq!(
            json["execution_profile"]["scan_pruning_reports"][0]["strategy"]["kind"],
            "property_eq"
        );
        assert_eq!(
            json["execution_profile"]["scan_pruning_reports"][0]["strategy"]["property"],
            "kind"
        );
        assert!(json["execution_profile"]["scan_pruning_reports"][0]
            .get("value")
            .is_none());
    }

    #[test]
    fn parses_explain_json_parameters_as_skein_values() {
        let parameters = parse_parameters_json(
            r#"{"id":42,"needle":"graph","tags":["a","b"],"meta":{"ok":true}}"#,
        )
        .unwrap();

        assert_eq!(parameters.get("id"), Some(&Value::Int(42)));
        assert_eq!(
            parameters.get("needle"),
            Some(&Value::String("graph".to_string()))
        );
        assert_eq!(
            parameters.get("tags"),
            Some(&Value::List(vec![
                Value::String("a".to_string()),
                Value::String("b".to_string())
            ]))
        );
        assert_eq!(
            parameters.get("meta"),
            Some(&Value::Map(BTreeMap::from([(
                "ok".to_string(),
                Value::Bool(true)
            )])))
        );

        let error = parse_parameters_json(r#"["not", "an", "object"]"#).unwrap_err();
        assert!(error
            .to_string()
            .contains("--params-json must be a JSON object"));
    }

    #[test]
    fn cli_json_file_parse_errors_are_redacted_by_default() {
        let path = unique_json_file("secret-cli-json-path-do-not-emit");
        std::fs::write(&path, "{\"secret\":\"cli-json-payload-do-not-emit\",").unwrap();

        let error = read_json_file(&path).unwrap_err();
        let message = error.to_string();

        assert_eq!(message, "execution error: invalid JSON file: invalid_json");
        assert!(!message.contains(path.to_str().unwrap()));
        assert!(!message.contains("secret-cli-json-path-do-not-emit"));
        assert!(!message.contains("cli-json-payload-do-not-emit"));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn storage_recovery_requirements_reject_unbounded_wal_replay() {
        let report = StorageRecoveryReport {
            durable: true,
            recovery_mode: RecoveryMode::Strict,
            max_wal_replay_entries: None,
            checkpoint_epoch: Some(1),
            checkpoint_commit_epoch: Some(1),
            wal_present: true,
            wal_replay_start_lsn: Some(1),
            next_lsn_after_replay: Some(1),
            replayed_wal_entries: 0,
            torn_tail_ignored: false,
            torn_tail_reason: None,
            recovered_commit_epoch: 1,
            ..StorageRecoveryReport::default()
        };

        let error = enforce_storage_recovery_requirements(
            &report,
            StorageRecoveryRequirements {
                require_bounded_wal_replay: true,
                ..StorageRecoveryRequirements::default()
            },
        )
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("WAL replay was not opened with a configured entry bound"));
    }

    #[test]
    fn renders_graph_lightning_bootstrap_manifest_json() {
        let validation = CanonicalGraphSnapshotValidation {
            is_valid: true,
            is_import_ready: true,
            checksum_matches: true,
            expected_logical_checksum: 99,
            stable_identity_matches: true,
            stable_identity_ready: true,
            expected_stable_identity: CanonicalSnapshotIdentityAudit {
                requires_stable_id_mapping: false,
                nodes_without_stable_id: Vec::new(),
                relationships_without_stable_id: Vec::new(),
                duplicate_node_stable_ids: Vec::new(),
                duplicate_relationship_stable_ids: Vec::new(),
            },
            duplicate_node_ids: Vec::new(),
            duplicate_relationship_ids: Vec::new(),
            missing_sources: Vec::new(),
            missing_targets: Vec::new(),
        };
        let manifest = GraphLightningBootstrapManifest {
            protocol_version: 1,
            graph_commit_epoch: 5,
            logical_checksum: 99,
            graph_stream_checksum: 101,
            graph_stream_byte_len: 4096,
            schema_checksum: 77,
            node_count: 3,
            relationship_count: 2,
            label_count: 2,
            relationship_type_count: 1,
            node_property_count: 6,
            relationship_property_count: 2,
            validation,
        };

        let json = graph_lightning_bootstrap_manifest_json(&manifest);

        assert_eq!(json["protocol"], "graph-lightning-bootstrap");
        assert_eq!(json["protocol_version"], 1);
        assert_eq!(json["graph_commit_epoch"], 5);
        assert_eq!(json["logical_checksum"], 99);
        assert_eq!(json["graph_stream_checksum"], 101);
        assert_eq!(json["graph_stream_byte_len"], 4096);
        assert_eq!(json["schema_checksum"], 77);
        assert_eq!(json["node_count"], 3);
        assert_eq!(json["relationship_count"], 2);
        assert_eq!(json["validation"]["is_import_ready"], true);
    }

    #[test]
    fn renders_graph_lightning_graph_stream_validation_json() {
        let validation = GraphLightningGraphStreamValidation {
            is_valid: false,
            checksum_matches: false,
            format_version_matches: true,
            count_matches: true,
            endpoint_integrity: false,
            manifest_matches: false,
            expected_stream_checksum: Some(11),
            actual_stream_checksum: 22,
            format_version: Some(1),
            graph_commit_epoch: Some(5),
            logical_checksum: Some(99),
            node_count: 3,
            relationship_count: 2,
            duplicate_node_ids: vec![7],
            duplicate_relationship_ids: vec![8],
            missing_sources: vec![CanonicalSnapshotEndpointViolation {
                relationship_id: 2,
                missing_node_id: 10,
            }],
            missing_targets: vec![CanonicalSnapshotEndpointViolation {
                relationship_id: 3,
                missing_node_id: 11,
            }],
            errors: vec!["graph stream checksum mismatch".to_string()],
        };

        let json = graph_lightning_graph_stream_validation_json(&validation);

        assert_eq!(json["is_valid"], false);
        assert_eq!(json["checksum_matches"], false);
        assert_eq!(json["expected_stream_checksum"], 11);
        assert_eq!(json["actual_stream_checksum"], 22);
        assert_eq!(json["duplicate_node_ids"], serde_json::json!([7]));
        assert_eq!(json["missing_targets"][0]["missing_node_id"], 11);
        assert_eq!(
            json["errors"],
            serde_json::json!(["graph stream checksum mismatch"])
        );
    }

    #[test]
    fn renders_graph_lightning_bootstrap_bundle_json() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();

        let json = graph_lightning_bootstrap_bundle_json(&export);

        assert_eq!(json["protocol"], "graph-lightning-bootstrap-bundle");
        assert_eq!(json["manifest"]["protocol"], "graph-lightning-bootstrap");
        assert_eq!(json["manifest"]["validation"]["is_import_ready"], true);
        assert_eq!(json["graph_stream_validation"]["is_valid"], true);
        assert_eq!(json["export_gate"]["decision"], "ready");
        assert_eq!(json["export_gate"]["manifest_blockers"], 0);
        assert_eq!(json["export_gate"]["graph_stream_blockers"], 0);
        assert!(json["export_gate"]["manifest_blocker_messages"]
            .as_array()
            .unwrap()
            .is_empty());
        assert!(json["export_gate"]["graph_stream_blocker_messages"]
            .as_array()
            .unwrap()
            .is_empty());
        assert!(json["export_gate"]["blockers"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn bootstrap_bundle_can_include_storage_recovery_evidence() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'root', title: 'Root'})")
            .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        let recovery = test_storage_recovery_report(export.manifest.graph_commit_epoch);

        let json = graph_lightning_bootstrap_bundle_json_with_storage_recovery(
            &export,
            "skein-storage-v1",
            &recovery,
        );

        assert_eq!(
            json["storage_recovery"]["protocol"],
            "skein-storage-recovery-report"
        );
        assert_eq!(
            json["storage_recovery"]["storage_version"],
            "skein-storage-v1"
        );
        assert_eq!(json["storage_recovery"]["max_wal_replay_entries"], 32);
        assert_eq!(
            json["storage_recovery"]["recovered_commit_epoch"],
            json["manifest"]["graph_commit_epoch"]
        );
        assert_eq!(
            json["storage_recovery"]["readiness"]["wal_replay_bounded"],
            true
        );
        assert_eq!(json["manifest"]["protocol"], "graph-lightning-bootstrap");
        assert_eq!(json["export_gate"]["decision"], "ready");
    }

    #[test]
    fn graph_lightning_bootstrap_bundle_groups_export_gate_blockers() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();
        let mut export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        export.manifest.validation.is_import_ready = false;
        export.graph_stream.encoded.push_str("corrupt");

        let json = graph_lightning_bootstrap_bundle_json(&export);

        assert_eq!(json["export_gate"]["decision"], "blocked");
        assert_eq!(json["export_gate"]["manifest_blockers"], 1);
        assert_eq!(json["export_gate"]["graph_stream_blockers"], 1);
        assert_eq!(
            json["export_gate"]["manifest_blocker_messages"][0],
            "manifest validation is not import ready"
        );
        assert_eq!(
            json["export_gate"]["graph_stream_blocker_messages"][0],
            "graph stream validation failed"
        );
        assert_eq!(json["export_gate"]["blockers"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn stages_graph_lightning_bootstrap_export_artifacts() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        let staging_dir = unique_main_test_dir("graph_lightning_stage_bootstrap");

        let catalog = stage_graph_lightning_bootstrap_export(&export, &staging_dir).unwrap();

        assert_eq!(catalog["protocol"], "graph-lightning-staging-catalog");
        assert_eq!(catalog["stage_state"], "READY");
        assert_eq!(catalog["export_gate"]["decision"], "ready");
        assert_eq!(catalog["artifacts"].as_array().unwrap().len(), 3);
        assert_eq!(catalog["artifact_summary"]["object_count"], 3);
        assert_eq!(catalog["artifact_summary"]["measured_object_count"], 3);
        assert_eq!(catalog["artifact_summary"]["missing_byte_len_count"], 0);
        assert!(
            catalog["artifact_summary"]["total_byte_len"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert_eq!(catalog["artifact_summary"]["kind_counts"]["manifest"], 1);
        assert_eq!(
            catalog["artifact_summary"]["kind_counts"]["graph_stream"],
            1
        );
        assert_eq!(catalog["artifact_summary"]["kind_counts"]["bundle"], 1);
        assert!(staging_dir
            .join("graph_lightning_bootstrap_manifest.json")
            .exists());
        assert!(staging_dir
            .join("graph_lightning_graph_stream.txt")
            .exists());
        assert!(staging_dir
            .join("graph_lightning_bootstrap_bundle.json")
            .exists());
        assert!(staging_dir
            .join("graph_lightning_staging_catalog.json")
            .exists());
        let persisted_catalog =
            std::fs::read_to_string(staging_dir.join("graph_lightning_staging_catalog.json"))
                .unwrap();
        let persisted_catalog =
            serde_json::from_str::<serde_json::Value>(&persisted_catalog).unwrap();
        assert_eq!(persisted_catalog, catalog);

        std::fs::remove_dir_all(staging_dir).unwrap();
    }

    #[test]
    fn verifies_graph_lightning_staging_catalog() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        let staging_dir = unique_main_test_dir("graph_lightning_verify_staging");
        stage_graph_lightning_bootstrap_export(&export, &staging_dir).unwrap();

        let report = verify_graph_lightning_staging_catalog(&staging_dir).unwrap();

        assert_eq!(report["protocol"], "graph-lightning-staging-verification");
        assert_eq!(report["validation_gate"]["decision"], "ready");
        assert_eq!(report["artifact_integrity"], true);
        assert_eq!(report["catalog_protocol_matches"], true);
        assert_eq!(report["catalog_protocol_version_matches"], true);
        assert_eq!(report["manifest_protocol_version_matches"], true);
        assert_eq!(report["manifest_matches_graph_stream"], true);
        assert_eq!(report["bundle_matches_artifacts"], true);
        assert_eq!(report["artifact_summary"]["object_count"], 3);
        assert_eq!(report["artifact_summary"]["measured_object_count"], 3);
        assert_eq!(report["artifact_summary"]["missing_byte_len_count"], 0);
        assert!(
            report["artifact_summary"]["total_byte_len"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert_eq!(report["validation_gate"]["artifact_errors"], 0);
        assert_eq!(report["validation_gate"]["manifest_errors"], 0);
        assert_eq!(report["validation_gate"]["graph_stream_errors"], 0);
        assert_eq!(report["validation_gate"]["bundle_errors"], 0);
        assert_eq!(report["validation_gate"]["catalog_errors"], 0);

        std::fs::remove_dir_all(staging_dir).unwrap();
    }

    #[test]
    fn staging_verification_validates_storage_recovery_evidence() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'root', title: 'Root'})")
            .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        let recovery = test_storage_recovery_report(export.manifest.graph_commit_epoch);
        let staging_dir = unique_main_test_dir("graph_lightning_verify_staging_recovery");
        stage_graph_lightning_bootstrap_export_with_storage_recovery(
            &export,
            &staging_dir,
            "skein-storage-v1",
            &recovery,
        )
        .unwrap();

        let report = verify_graph_lightning_staging_catalog(&staging_dir).unwrap();

        assert_eq!(report["validation_gate"]["decision"], "ready");
        assert_eq!(report["storage_recovery_evidence"]["present"], true);
        assert_eq!(report["storage_recovery_evidence"]["valid"], true);
        assert_eq!(
            report["storage_recovery_evidence"]["protocol_matches"],
            true
        );
        assert_eq!(
            report["storage_recovery_evidence"]["storage_version_present"],
            true
        );
        assert_eq!(
            report["storage_recovery_evidence"]["recovered_commit_epoch_matches_manifest"],
            true
        );

        std::fs::remove_dir_all(staging_dir).unwrap();
    }

    #[test]
    fn staging_verification_blocks_tampered_storage_recovery_epoch() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'root', title: 'Root'})")
            .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        let recovery = test_storage_recovery_report(export.manifest.graph_commit_epoch);
        let staging_dir = unique_main_test_dir("graph_lightning_verify_staging_recovery_tampered");
        stage_graph_lightning_bootstrap_export_with_storage_recovery(
            &export,
            &staging_dir,
            "skein-storage-v1",
            &recovery,
        )
        .unwrap();
        let bundle_path = staging_dir.join("graph_lightning_bootstrap_bundle.json");
        let bundle = std::fs::read_to_string(&bundle_path).unwrap().replace(
            "\"recovered_commit_epoch\": 1",
            "\"recovered_commit_epoch\": 99",
        );
        std::fs::write(&bundle_path, bundle).unwrap();

        let report = verify_graph_lightning_staging_catalog(&staging_dir).unwrap();

        assert_eq!(report["validation_gate"]["decision"], "blocked");
        assert_eq!(report["artifact_integrity"], false);
        assert_eq!(report["storage_recovery_evidence"]["present"], true);
        assert_eq!(report["storage_recovery_evidence"]["valid"], false);
        assert_eq!(
            report["storage_recovery_evidence"]["recovered_commit_epoch_matches_manifest"],
            false
        );
        assert!(report["validation_gate"]["bundle_error_messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|error| error
                .as_str()
                .unwrap()
                .contains("recovered commit epoch does not match manifest graph epoch")));

        std::fs::remove_dir_all(staging_dir).unwrap();
    }

    #[test]
    fn staging_verification_blocks_unsupported_catalog_version() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        let staging_dir = unique_main_test_dir("graph_lightning_verify_staging_catalog_version");
        stage_graph_lightning_bootstrap_export(&export, &staging_dir).unwrap();
        let catalog_path = staging_dir.join("graph_lightning_staging_catalog.json");
        let catalog = std::fs::read_to_string(&catalog_path)
            .unwrap()
            .replace("\"protocol_version\": 1", "\"protocol_version\": 99");
        std::fs::write(&catalog_path, catalog).unwrap();

        let report = verify_graph_lightning_staging_catalog(&staging_dir).unwrap();

        assert_eq!(report["validation_gate"]["decision"], "blocked");
        assert_eq!(report["catalog_protocol_matches"], true);
        assert_eq!(report["catalog_protocol_version_matches"], false);
        assert_eq!(report["validation_gate"]["catalog_errors"], 1);
        assert!(report["validation_gate"]["catalog_error_messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|error| error
                .as_str()
                .unwrap()
                .contains("protocol version mismatch")));

        std::fs::remove_dir_all(staging_dir).unwrap();
    }

    #[test]
    fn staging_verification_blocks_unsupported_manifest_version() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        let staging_dir = unique_main_test_dir("graph_lightning_verify_staging_manifest_version");
        stage_graph_lightning_bootstrap_export(&export, &staging_dir).unwrap();
        let manifest_path = staging_dir.join("graph_lightning_bootstrap_manifest.json");
        let manifest = std::fs::read_to_string(&manifest_path)
            .unwrap()
            .replace("\"protocol_version\": 1", "\"protocol_version\": 99");
        std::fs::write(&manifest_path, manifest).unwrap();

        let report = verify_graph_lightning_staging_catalog(&staging_dir).unwrap();

        assert_eq!(report["validation_gate"]["decision"], "blocked");
        assert_eq!(report["manifest_protocol_version_matches"], false);
        assert!(report["validation_gate"]["manifest_error_messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|error| error
                .as_str()
                .unwrap()
                .contains("manifest protocol version mismatch")));

        std::fs::remove_dir_all(staging_dir).unwrap();
    }

    #[test]
    fn staging_verification_reports_tampered_graph_stream() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        let staging_dir = unique_main_test_dir("graph_lightning_verify_staging_tampered");
        stage_graph_lightning_bootstrap_export(&export, &staging_dir).unwrap();
        let graph_stream_path = staging_dir.join("graph_lightning_graph_stream.txt");
        let tampered = std::fs::read_to_string(&graph_stream_path)
            .unwrap()
            .replace("relationship\t0\t0\t1", "relationship\t0\t0\t99");
        std::fs::write(&graph_stream_path, tampered).unwrap();

        let report = verify_graph_lightning_staging_catalog(&staging_dir).unwrap();

        assert_eq!(report["validation_gate"]["decision"], "blocked");
        assert_eq!(report["artifact_integrity"], false);
        assert_eq!(report["manifest_matches_graph_stream"], false);
        assert_eq!(report["validation_gate"]["artifact_errors"], 2);
        assert_eq!(report["validation_gate"]["manifest_errors"], 1);
        assert_eq!(report["validation_gate"]["graph_stream_errors"], 1);
        assert_eq!(report["validation_gate"]["bundle_errors"], 1);
        assert_eq!(report["validation_gate"]["catalog_errors"], 0);
        assert!(report["validation_gate"]["artifact_error_messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|error| error
                .as_str()
                .unwrap()
                .contains("graph_stream checksum mismatch")));
        assert_eq!(
            report["validation_gate"]["graph_stream_error_messages"][0],
            "GraphStream validation failed"
        );
        assert!(report["validation_gate"]["errors"]
            .as_array()
            .unwrap()
            .iter()
            .any(|error| error
                .as_str()
                .unwrap()
                .contains("graph_stream checksum mismatch")));

        std::fs::remove_dir_all(staging_dir).unwrap();
    }

    #[test]
    fn publishes_graph_lightning_staging_catalog_idempotently() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        let staging_dir = unique_main_test_dir("graph_lightning_publish_staging");
        let publish_dir = unique_main_test_dir("graph_lightning_publish_target");
        stage_graph_lightning_bootstrap_export(&export, &staging_dir).unwrap();

        let published =
            publish_graph_lightning_staging_catalog(&staging_dir, &publish_dir).unwrap();
        let idempotent =
            publish_graph_lightning_staging_catalog(&staging_dir, &publish_dir).unwrap();

        assert_eq!(published["protocol"], "graph-lightning-published-manifest");
        assert_eq!(published["state"], "PUBLISHED");
        assert_eq!(published["publish_gate"]["decision"], "published");
        assert_eq!(idempotent["publish_gate"]["decision"], "idempotent");
        assert!(publish_dir
            .join("graph_lightning_published_manifest.json")
            .exists());

        std::fs::remove_dir_all(staging_dir).unwrap();
        std::fs::remove_dir_all(publish_dir).unwrap();
    }

    #[test]
    fn publish_staging_with_preflight_accepts_matching_fencing_and_epoch() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        let staging_dir = unique_main_test_dir("graph_lightning_publish_preflight_staging");
        let publish_dir = unique_main_test_dir("graph_lightning_publish_preflight_target");
        stage_graph_lightning_bootstrap_export(&export, &staging_dir).unwrap();
        std::fs::write(
            staging_dir.join("graph_lightning_import_state.json"),
            serde_json::json!({
                "protocol": "graph-lightning-import-state",
                "protocol_version": 1,
                "import_state": "VALIDATING",
                "import_id": "import-1",
                "task_id": "task-1",
                "fencing_token": "fence-1",
                "object_digest": "digest-1"
            })
            .to_string(),
        )
        .unwrap();

        let published = publish_graph_lightning_staging_catalog_with_options(
            &staging_dir,
            &publish_dir,
            PublishGraphLightningOptions {
                require_state_marker: true,
                fencing_token: Some("fence-1".to_string()),
                expected_graph_epoch: Some(export.manifest.graph_commit_epoch),
            },
        )
        .unwrap();

        assert_eq!(published["publish_gate"]["decision"], "published");
        assert_eq!(published["publish_gate"]["preflight"]["decision"], "ready");
        assert_eq!(
            published["publish_gate"]["preflight"]["state_marker"]["import_state"],
            "VALIDATING"
        );
        assert_eq!(
            published["publish_gate"]["preflight"]["state_marker"]["idempotency_key"]
                ["fencing_token"],
            "fence-1"
        );
        assert_eq!(
            published["publish_gate"]["preflight"]["expected_graph_epoch"],
            export.manifest.graph_commit_epoch
        );
        assert_eq!(
            published["publish_gate"]["preflight"]["expected_graph_epoch_matches"],
            true
        );
        assert_eq!(
            published["publish_gate"]["preflight"]["fencing_token_matches"],
            true
        );

        std::fs::remove_dir_all(staging_dir).unwrap();
        std::fs::remove_dir_all(publish_dir).unwrap();
    }

    #[test]
    fn publish_staging_rejects_missing_required_state_marker() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        let staging_dir = unique_main_test_dir("graph_lightning_publish_missing_marker_staging");
        let publish_dir = unique_main_test_dir("graph_lightning_publish_missing_marker_target");
        stage_graph_lightning_bootstrap_export(&export, &staging_dir).unwrap();

        let error = publish_graph_lightning_staging_catalog_with_options(
            &staging_dir,
            &publish_dir,
            PublishGraphLightningOptions {
                require_state_marker: true,
                fencing_token: None,
                expected_graph_epoch: None,
            },
        )
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("publish requires graph lightning import state marker"));
        assert!(!publish_dir
            .join("graph_lightning_published_manifest.json")
            .exists());

        std::fs::remove_dir_all(staging_dir).unwrap();
    }

    #[test]
    fn publish_staging_rejects_stale_fencing_token() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        let staging_dir = unique_main_test_dir("graph_lightning_publish_stale_fence_staging");
        let publish_dir = unique_main_test_dir("graph_lightning_publish_stale_fence_target");
        stage_graph_lightning_bootstrap_export(&export, &staging_dir).unwrap();
        std::fs::write(
            staging_dir.join("graph_lightning_import_state.json"),
            serde_json::json!({
                "protocol": "graph-lightning-import-state",
                "protocol_version": 1,
                "import_state": "VALIDATING",
                "import_id": "import-1",
                "task_id": "task-1",
                "fencing_token": "fresh-fence",
                "object_digest": "digest-1"
            })
            .to_string(),
        )
        .unwrap();

        let error = publish_graph_lightning_staging_catalog_with_options(
            &staging_dir,
            &publish_dir,
            PublishGraphLightningOptions {
                require_state_marker: true,
                fencing_token: Some("stale-fence".to_string()),
                expected_graph_epoch: Some(export.manifest.graph_commit_epoch),
            },
        )
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("publish fencing token did not match"));
        assert!(!publish_dir
            .join("graph_lightning_published_manifest.json")
            .exists());

        std::fs::remove_dir_all(staging_dir).unwrap();
    }

    #[test]
    fn publish_staging_rejects_unexpected_graph_epoch() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        let staging_dir = unique_main_test_dir("graph_lightning_publish_epoch_staging");
        let publish_dir = unique_main_test_dir("graph_lightning_publish_epoch_target");
        stage_graph_lightning_bootstrap_export(&export, &staging_dir).unwrap();

        let error = publish_graph_lightning_staging_catalog_with_options(
            &staging_dir,
            &publish_dir,
            PublishGraphLightningOptions {
                require_state_marker: false,
                fencing_token: None,
                expected_graph_epoch: Some(export.manifest.graph_commit_epoch + 1),
            },
        )
        .unwrap_err();

        assert!(error.to_string().contains("expected graph epoch"));
        assert!(!publish_dir
            .join("graph_lightning_published_manifest.json")
            .exists());

        std::fs::remove_dir_all(staging_dir).unwrap();
    }

    #[test]
    fn publish_staging_rejects_different_manifest_overwrite() {
        let staging_dir = unique_main_test_dir("graph_lightning_publish_staging_conflict_a");
        let second_staging_dir = unique_main_test_dir("graph_lightning_publish_staging_conflict_b");
        let publish_dir = unique_main_test_dir("graph_lightning_publish_target_conflict");
        let mut first = Database::new();
        first
            .query(
                "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
            )
            .unwrap();
        let first_export = first.prepare_graph_lightning_bootstrap_export().unwrap();
        stage_graph_lightning_bootstrap_export(&first_export, &staging_dir).unwrap();
        publish_graph_lightning_staging_catalog(&staging_dir, &publish_dir).unwrap();

        let mut second = Database::new();
        second
            .query(
                "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
            )
            .unwrap();
        second
            .query("CREATE (:Source {id: 'source-1', path: '/tmp/source.md'})")
            .unwrap();
        let second_export = second.prepare_graph_lightning_bootstrap_export().unwrap();
        stage_graph_lightning_bootstrap_export(&second_export, &second_staging_dir).unwrap();

        let error =
            publish_graph_lightning_staging_catalog(&second_staging_dir, &publish_dir).unwrap_err();

        assert!(error.to_string().contains("different snapshot"));

        std::fs::remove_dir_all(staging_dir).unwrap();
        std::fs::remove_dir_all(second_staging_dir).unwrap();
        std::fs::remove_dir_all(publish_dir).unwrap();
    }

    #[test]
    fn verifies_graph_lightning_published_manifest() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        let staging_dir = unique_main_test_dir("graph_lightning_verify_published_staging");
        let publish_dir = unique_main_test_dir("graph_lightning_verify_published_target");
        stage_graph_lightning_bootstrap_export(&export, &staging_dir).unwrap();
        publish_graph_lightning_staging_catalog(&staging_dir, &publish_dir).unwrap();

        let report = verify_graph_lightning_published_manifest(&staging_dir, &publish_dir).unwrap();

        assert_eq!(report["protocol"], "graph-lightning-published-verification");
        assert_eq!(report["validation_gate"]["decision"], "ready");
        assert_eq!(report["catalog_checksum_matches"], true);
        assert_eq!(report["catalog_byte_len_matches"], true);
        assert_eq!(report["pointer_matches_manifest"], true);
        assert_eq!(report["staging_ready"], true);
        assert_eq!(report["validation_gate"]["pointer_errors"], 0);
        assert_eq!(report["validation_gate"]["catalog_errors"], 0);
        assert_eq!(report["validation_gate"]["staging_errors"], 0);

        std::fs::remove_dir_all(staging_dir).unwrap();
        std::fs::remove_dir_all(publish_dir).unwrap();
    }

    #[test]
    fn published_verification_exposes_storage_recovery_evidence() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'root', title: 'Root'})")
            .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        let recovery = test_storage_recovery_report(export.manifest.graph_commit_epoch);
        let staging_dir = unique_main_test_dir("graph_lightning_verify_published_recovery");
        let publish_dir = unique_main_test_dir("graph_lightning_verify_published_recovery_target");
        stage_graph_lightning_bootstrap_export_with_storage_recovery(
            &export,
            &staging_dir,
            "skein-storage-v1",
            &recovery,
        )
        .unwrap();
        publish_graph_lightning_staging_catalog(&staging_dir, &publish_dir).unwrap();

        let report = verify_graph_lightning_published_manifest(&staging_dir, &publish_dir).unwrap();

        assert_eq!(report["validation_gate"]["decision"], "ready");
        assert_eq!(report["storage_recovery_evidence"]["present"], true);
        assert_eq!(report["storage_recovery_evidence"]["valid"], true);
        assert_eq!(
            report["storage_recovery_evidence"],
            report["staging_verification"]["storage_recovery_evidence"]
        );

        std::fs::remove_dir_all(staging_dir).unwrap();
        std::fs::remove_dir_all(publish_dir).unwrap();
    }

    #[test]
    fn verify_published_reports_tampered_staging_catalog() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        let staging_dir = unique_main_test_dir("graph_lightning_verify_published_tampered_staging");
        let publish_dir = unique_main_test_dir("graph_lightning_verify_published_tampered_target");
        stage_graph_lightning_bootstrap_export(&export, &staging_dir).unwrap();
        publish_graph_lightning_staging_catalog(&staging_dir, &publish_dir).unwrap();
        let catalog_path = staging_dir.join("graph_lightning_staging_catalog.json");
        let tampered = std::fs::read_to_string(&catalog_path).unwrap().replace(
            "\"stage_state\": \"READY\"",
            "\"stage_state\": \"QUARANTINED\"",
        );
        std::fs::write(&catalog_path, tampered).unwrap();

        let report = verify_graph_lightning_published_manifest(&staging_dir, &publish_dir).unwrap();

        assert_eq!(report["validation_gate"]["decision"], "blocked");
        assert_eq!(report["catalog_checksum_matches"], false);
        assert_eq!(report["staging_ready"], false);
        assert_eq!(report["validation_gate"]["pointer_errors"], 0);
        assert_eq!(report["validation_gate"]["catalog_errors"], 2);
        assert_eq!(report["validation_gate"]["staging_errors"], 1);
        assert_eq!(
            report["validation_gate"]["catalog_error_messages"][0],
            "published pointer staging catalog checksum mismatch"
        );
        assert_eq!(
            report["validation_gate"]["staging_error_messages"][0],
            "published staging catalog is not ready"
        );
        assert!(report["validation_gate"]["errors"]
            .as_array()
            .unwrap()
            .iter()
            .any(|error| error
                .as_str()
                .unwrap()
                .contains("staging catalog checksum mismatch")));

        std::fs::remove_dir_all(staging_dir).unwrap();
        std::fs::remove_dir_all(publish_dir).unwrap();
    }

    #[test]
    fn gc_staging_report_pins_published_artifacts() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        let staging_dir = unique_main_test_dir("graph_lightning_gc_published_staging");
        let publish_dir = unique_main_test_dir("graph_lightning_gc_published_target");
        stage_graph_lightning_bootstrap_export(&export, &staging_dir).unwrap();
        publish_graph_lightning_staging_catalog(&staging_dir, &publish_dir).unwrap();

        let report = graph_lightning_gc_staging_report(&staging_dir, &publish_dir).unwrap();

        assert_eq!(report["protocol"], "graph-lightning-staging-gc-report");
        assert_eq!(report["published_pointer_state"], "verified");
        assert_eq!(report["candidate_count"], 4);
        assert_eq!(report["pinned_count"], 4);
        assert_eq!(report["deletable_count"], 0);
        assert_eq!(report["artifact_summary"]["object_count"], 4);
        assert_eq!(report["artifact_summary"]["measured_object_count"], 4);
        assert!(report["total_bytes"].as_u64().unwrap() > 0);
        assert_eq!(report["pinned_bytes"], report["total_bytes"]);
        assert_eq!(report["deletable_bytes"], 0);
        assert_eq!(report["gc_gate"]["decision"], "ready");
        assert_eq!(report["gc_gate"]["published_pointer_errors"], 0);
        assert!(report["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .all(|candidate| {
                candidate["pinned_by_published_pointer"] == true && candidate["deletable"] == false
            }));

        std::fs::remove_dir_all(staging_dir).unwrap();
        std::fs::remove_dir_all(publish_dir).unwrap();
    }

    #[test]
    fn gc_staging_report_allows_unpublished_artifacts() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        let staging_dir = unique_main_test_dir("graph_lightning_gc_unpublished_staging");
        let publish_dir = unique_main_test_dir("graph_lightning_gc_unpublished_target");
        stage_graph_lightning_bootstrap_export(&export, &staging_dir).unwrap();

        let report = graph_lightning_gc_staging_report(&staging_dir, &publish_dir).unwrap();

        assert_eq!(report["published_pointer_state"], "missing");
        assert_eq!(report["candidate_count"], 4);
        assert_eq!(report["pinned_count"], 0);
        assert_eq!(report["deletable_count"], 4);
        assert_eq!(report["artifact_summary"]["object_count"], 4);
        assert_eq!(report["artifact_summary"]["measured_object_count"], 4);
        assert!(report["total_bytes"].as_u64().unwrap() > 0);
        assert_eq!(report["pinned_bytes"], 0);
        assert_eq!(report["deletable_bytes"], report["total_bytes"]);
        assert_eq!(report["gc_gate"]["decision"], "ready");
        assert_eq!(report["gc_gate"]["published_pointer_errors"], 0);
        assert!(report["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .all(|candidate| {
                candidate["pinned_by_published_pointer"] == false && candidate["deletable"] == true
            }));

        std::fs::remove_dir_all(staging_dir).unwrap();
    }

    #[test]
    fn gc_staging_report_fails_closed_when_published_pointer_cannot_verify() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        let staging_dir = unique_main_test_dir("graph_lightning_gc_tampered_staging");
        let publish_dir = unique_main_test_dir("graph_lightning_gc_tampered_target");
        stage_graph_lightning_bootstrap_export(&export, &staging_dir).unwrap();
        publish_graph_lightning_staging_catalog(&staging_dir, &publish_dir).unwrap();
        let catalog_path = staging_dir.join("graph_lightning_staging_catalog.json");
        let tampered = std::fs::read_to_string(&catalog_path).unwrap().replace(
            "\"stage_state\": \"READY\"",
            "\"stage_state\": \"QUARANTINED\"",
        );
        std::fs::write(&catalog_path, tampered).unwrap();

        let report = graph_lightning_gc_staging_report(&staging_dir, &publish_dir).unwrap();

        assert_eq!(report["published_pointer_state"], "verification_failed");
        assert_eq!(report["candidate_count"], 4);
        assert_eq!(report["pinned_count"], 0);
        assert_eq!(report["deletable_count"], 0);
        assert!(report["total_bytes"].as_u64().unwrap() > 0);
        assert_eq!(report["pinned_bytes"], 0);
        assert_eq!(report["deletable_bytes"], 0);
        assert_eq!(report["gc_gate"]["decision"], "blocked");
        assert_eq!(report["gc_gate"]["published_pointer_errors"], 4);
        assert!(report["gc_gate"]["published_pointer_error_messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|error| error
                .as_str()
                .unwrap()
                .contains("staging catalog checksum mismatch")));
        assert!(report["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .all(|candidate| candidate["deletable"] == false));
        assert!(report["gc_gate"]["errors"]
            .as_array()
            .unwrap()
            .iter()
            .any(|error| error
                .as_str()
                .unwrap()
                .contains("refusing to mark staging artifacts deletable")));

        std::fs::remove_dir_all(staging_dir).unwrap();
        std::fs::remove_dir_all(publish_dir).unwrap();
    }

    #[test]
    fn import_status_reports_created_without_staging_catalog() {
        let staging_dir = unique_main_test_dir("graph_lightning_status_created_staging");
        let publish_dir = unique_main_test_dir("graph_lightning_status_created_target");
        std::fs::create_dir_all(&staging_dir).unwrap();

        let report = graph_lightning_import_status(&staging_dir, &publish_dir).unwrap();

        assert_eq!(report["protocol"], "graph-lightning-import-status");
        assert_eq!(report["import_state"], "CREATED");
        assert_eq!(report["staging_catalog_present"], false);
        assert_eq!(report["published_pointer_present"], false);
        assert_eq!(report["status_gate"]["decision"], "ready");
        assert_eq!(report["resume_action"]["operation"], "stage_bootstrap");
        assert_eq!(report["resume_action"]["safe_to_retry"], true);
        assert_eq!(report["resume_action"]["terminal"], false);
        assert_eq!(report["resource_retention"]["action"], "none");
        assert_eq!(report["resource_retention"]["safe_to_collect"], false);
        assert_eq!(report["resource_retention"]["protected_count"], 0);
        assert_eq!(report["resource_retention"]["deletable_count"], 0);
        assert_eq!(report["status_gate"]["presence_errors"], 0);
        assert_eq!(report["status_gate"]["staging_errors"], 0);
        assert_eq!(report["status_gate"]["published_errors"], 0);
        assert_eq!(report["status_gate"]["resource_errors"], 0);

        std::fs::remove_dir_all(staging_dir).unwrap();
    }

    #[test]
    fn import_status_reports_active_state_marker_without_staging_catalog() {
        let staging_dir = unique_main_test_dir("graph_lightning_status_exporting_marker_staging");
        let publish_dir = unique_main_test_dir("graph_lightning_status_exporting_marker_target");
        std::fs::create_dir_all(&staging_dir).unwrap();
        std::fs::write(
            staging_dir.join("graph_lightning_import_state.json"),
            serde_json::json!({
                "protocol": "graph-lightning-import-state",
                "protocol_version": 1,
                "import_state": "EXPORTING",
                "import_id": "import-1",
                "task_id": "task-1",
                "fencing_token": "fence-1",
                "object_digest": "digest-1"
            })
            .to_string(),
        )
        .unwrap();

        let report = graph_lightning_import_status(&staging_dir, &publish_dir).unwrap();

        assert_eq!(report["artifact_state"], "CREATED");
        assert_eq!(report["import_state"], "EXPORTING");
        assert_eq!(report["state_marker"]["present"], true);
        assert_eq!(report["state_marker"]["import_state"], "EXPORTING");
        assert_eq!(report["state_marker"]["raw"]["import_id"], "import-1");
        assert_eq!(report["state_marker"]["idempotency_ready"], true);
        assert_eq!(
            report["state_marker"]["idempotency_key"]["import_id"],
            "import-1"
        );
        assert_eq!(
            report["state_marker"]["idempotency_key"]["task_id"],
            "task-1"
        );
        assert_eq!(
            report["state_marker"]["idempotency_key"]["fencing_token"],
            "fence-1"
        );
        assert_eq!(
            report["state_marker"]["idempotency_key"]["object_digest"],
            "digest-1"
        );
        assert_eq!(report["resume_action"]["operation"], "continue_export");
        assert_eq!(report["resume_action"]["safe_to_retry"], true);
        assert_eq!(report["resume_action"]["terminal"], false);
        assert_eq!(report["resource_retention"]["action"], "none");
        assert_eq!(report["status_gate"]["decision"], "ready");
        assert_eq!(report["status_gate"]["state_errors"], 0);

        std::fs::remove_dir_all(staging_dir).unwrap();
    }

    #[test]
    fn import_status_summarizes_checkpoint_log_failures() {
        let staging_dir = unique_main_test_dir("graph_lightning_status_checkpoint_log_staging");
        let publish_dir = unique_main_test_dir("graph_lightning_status_checkpoint_log_target");
        std::fs::create_dir_all(&staging_dir).unwrap();
        std::fs::write(
            staging_dir.join("graph_lightning_import_state.json"),
            serde_json::json!({
                "protocol": "graph-lightning-import-state",
                "protocol_version": 1,
                "import_state": "UPLOADING",
                "import_id": "import-1",
                "task_id": "task-1",
                "fencing_token": "fence-1",
                "object_digest": "digest-1"
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            staging_dir.join("graph_lightning_import_checkpoints.jsonl"),
            [
                serde_json::json!({
                    "stage": "source_range_scanned",
                    "status": "completed",
                    "source_range": "node:0..10",
                    "import_id": "import-1",
                    "task_id": "task-1"
                })
                .to_string(),
                serde_json::json!({
                    "stage": "object_uploaded",
                    "status": "failed",
                    "source_range": "node:10..20",
                    "partition": "p0",
                    "object_digest": "digest-failed",
                    "validation_rule": "multipart_checksum",
                    "import_id": "import-1",
                    "task_id": "task-1",
                    "fencing_token": "fence-1"
                })
                .to_string(),
            ]
            .join("\n"),
        )
        .unwrap();

        let report = graph_lightning_import_status(&staging_dir, &publish_dir).unwrap();

        assert_eq!(report["import_state"], "UPLOADING");
        assert_eq!(report["checkpoint_log"]["present"], true);
        assert_eq!(report["checkpoint_log"]["entry_count"], 2);
        assert_eq!(report["checkpoint_log"]["idempotency_key_count"], 1);
        assert_eq!(report["checkpoint_log"]["idempotency_conflicts"], 0);
        assert_eq!(
            report["checkpoint_log"]["checkpoint_summary"]["stage_counts"],
            serde_json::json!({
                "object_uploaded": 1,
                "source_range_scanned": 1
            })
        );
        assert_eq!(
            report["checkpoint_log"]["checkpoint_summary"]["status_counts"],
            serde_json::json!({
                "completed": 1,
                "failed": 1
            })
        );
        assert_eq!(
            report["checkpoint_log"]["checkpoint_summary"]["failure_rule_counts"],
            serde_json::json!({
                "multipart_checksum": 1
            })
        );
        assert_eq!(
            report["checkpoint_log"]["checkpoint_summary"]["failure_partition_counts"],
            serde_json::json!({
                "p0": 1
            })
        );
        assert_eq!(
            report["checkpoint_log"]["last_checkpoint"]["stage"],
            "object_uploaded"
        );
        assert_eq!(
            report["checkpoint_log"]["resume_summary"]["last_source_range"],
            "node:10..20"
        );
        assert_eq!(
            report["checkpoint_log"]["resume_summary"]["failed_source_ranges"],
            serde_json::json!(["node:10..20"])
        );
        assert_eq!(
            report["checkpoint_log"]["resume_summary"]["failed_object_digests"],
            serde_json::json!(["digest-failed"])
        );
        assert_eq!(
            report["checkpoint_log"]["resume_summary"]["failed_partitions"],
            serde_json::json!(["p0"])
        );
        assert_eq!(
            report["checkpoint_log"]["resume_summary"]["failed_validation_rules"],
            serde_json::json!(["multipart_checksum"])
        );
        assert_eq!(
            report["checkpoint_log"]["checkpoint_gate"]["decision"],
            "ready"
        );
        assert_eq!(report["status_gate"]["checkpoint_errors"], 0);
        assert_eq!(report["status_gate"]["decision"], "ready");

        std::fs::remove_dir_all(staging_dir).unwrap();
    }

    #[test]
    fn import_status_blocks_checkpoint_log_idempotency_conflict() {
        let staging_dir =
            unique_main_test_dir("graph_lightning_status_checkpoint_conflict_staging");
        let publish_dir = unique_main_test_dir("graph_lightning_status_checkpoint_conflict_target");
        std::fs::create_dir_all(&staging_dir).unwrap();
        std::fs::write(
            staging_dir.join("graph_lightning_import_checkpoints.jsonl"),
            [
                serde_json::json!({
                    "stage": "object_uploaded",
                    "status": "completed",
                    "source_range": "node:0..10",
                    "partition": "p0",
                    "object_digest": "digest-1",
                    "import_id": "import-1",
                    "task_id": "task-1",
                    "fencing_token": "fence-1"
                })
                .to_string(),
                serde_json::json!({
                    "stage": "object_verified",
                    "status": "completed",
                    "source_range": "node:10..20",
                    "partition": "p0",
                    "object_digest": "digest-1",
                    "import_id": "import-1",
                    "task_id": "task-1",
                    "fencing_token": "fence-1"
                })
                .to_string(),
            ]
            .join("\n"),
        )
        .unwrap();

        let report = graph_lightning_import_status(&staging_dir, &publish_dir).unwrap();

        assert_eq!(
            report["checkpoint_log"]["checkpoint_gate"]["decision"],
            "blocked"
        );
        assert_eq!(report["checkpoint_log"]["idempotency_key_count"], 1);
        assert_eq!(report["checkpoint_log"]["idempotency_conflicts"], 1);
        assert_eq!(report["status_gate"]["decision"], "blocked");
        assert_eq!(report["status_gate"]["checkpoint_errors"], 1);
        assert!(report["status_gate"]["checkpoint_error_messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|error| error.as_str().unwrap().contains("reuses idempotency key")));

        std::fs::remove_dir_all(staging_dir).unwrap();
    }

    #[test]
    fn import_status_blocks_checkpoint_log_without_object_idempotency_key() {
        let staging_dir =
            unique_main_test_dir("graph_lightning_status_checkpoint_missing_key_staging");
        let publish_dir =
            unique_main_test_dir("graph_lightning_status_checkpoint_missing_key_target");
        std::fs::create_dir_all(&staging_dir).unwrap();
        std::fs::write(
            staging_dir.join("graph_lightning_import_checkpoints.jsonl"),
            serde_json::json!({
                "stage": "object_uploaded",
                "status": "completed",
                "object_digest": "digest-1"
            })
            .to_string(),
        )
        .unwrap();

        let report = graph_lightning_import_status(&staging_dir, &publish_dir).unwrap();

        assert_eq!(report["artifact_state"], "CREATED");
        assert_eq!(
            report["checkpoint_log"]["checkpoint_gate"]["decision"],
            "blocked"
        );
        assert_eq!(report["status_gate"]["decision"], "blocked");
        assert_eq!(report["status_gate"]["checkpoint_errors"], 3);
        assert!(report["status_gate"]["checkpoint_error_messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|error| error
                .as_str()
                .unwrap()
                .contains("missing idempotency field import_id")));
        assert!(report["status_gate"]["checkpoint_error_messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|error| error
                .as_str()
                .unwrap()
                .contains("missing idempotency field fencing_token")));

        std::fs::remove_dir_all(staging_dir).unwrap();
    }

    #[test]
    fn import_status_quarantines_pointer_without_staging_catalog() {
        let staging_dir = unique_main_test_dir("graph_lightning_status_pointer_only_staging");
        let publish_dir = unique_main_test_dir("graph_lightning_status_pointer_only_target");
        std::fs::create_dir_all(&publish_dir).unwrap();
        std::fs::write(
            publish_dir.join("graph_lightning_published_manifest.json"),
            "{}",
        )
        .unwrap();

        let report = graph_lightning_import_status(&staging_dir, &publish_dir).unwrap();

        assert_eq!(report["import_state"], "QUARANTINED");
        assert_eq!(report["staging_catalog_present"], false);
        assert_eq!(report["published_pointer_present"], true);
        assert_eq!(report["status_gate"]["decision"], "blocked");
        assert_eq!(report["resume_action"]["operation"], "inspect_errors");
        assert_eq!(report["resume_action"]["safe_to_retry"], false);
        assert_eq!(report["resume_action"]["terminal"], true);
        assert_eq!(report["resource_retention"]["action"], "none");
        assert_eq!(report["resource_retention"]["safe_to_collect"], false);
        assert_eq!(report["resource_retention"]["protected_count"], 0);
        assert_eq!(report["resource_retention"]["deletable_count"], 0);
        assert_eq!(report["status_gate"]["presence_errors"], 1);
        assert_eq!(report["status_gate"]["staging_errors"], 0);
        assert_eq!(report["status_gate"]["published_errors"], 0);
        assert_eq!(report["status_gate"]["resource_errors"], 0);
        assert!(report["status_gate"]["presence_error_messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|error| error
                .as_str()
                .unwrap()
                .contains("published pointer exists without a matching staging catalog")));
        assert!(report["status_gate"]["errors"]
            .as_array()
            .unwrap()
            .iter()
            .any(|error| error
                .as_str()
                .unwrap()
                .contains("published pointer exists without a matching staging catalog")));

        std::fs::remove_dir_all(publish_dir).unwrap();
    }

    #[test]
    fn import_status_reports_ready_after_staging_verifies() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        let staging_dir = unique_main_test_dir("graph_lightning_status_ready_staging");
        let publish_dir = unique_main_test_dir("graph_lightning_status_ready_target");
        stage_graph_lightning_bootstrap_export(&export, &staging_dir).unwrap();

        let report = graph_lightning_import_status(&staging_dir, &publish_dir).unwrap();

        assert_eq!(report["import_state"], "READY");
        assert_eq!(report["staging_catalog_present"], true);
        assert_eq!(report["published_pointer_present"], false);
        assert_eq!(
            report["staging_verification"]["validation_gate"]["decision"],
            "ready"
        );
        assert_eq!(report["published_verification"], serde_json::Value::Null);
        assert_eq!(report["status_gate"]["decision"], "ready");
        assert_eq!(report["resume_action"]["operation"], "publish_staging");
        assert_eq!(report["resume_action"]["safe_to_retry"], true);
        assert_eq!(report["resume_action"]["terminal"], false);
        assert_eq!(report["resource_retention"]["action"], "retain_for_publish");
        assert_eq!(report["resource_retention"]["safe_to_collect"], false);
        assert_eq!(report["resource_retention"]["protected_count"], 4);
        assert_eq!(report["resource_retention"]["deletable_count"], 0);
        assert_eq!(report["resource_retention"]["gc_deletable_count"], 4);
        assert_eq!(
            report["resource_retention"]["gc_report"]["gc_gate"]["decision"],
            "ready"
        );
        assert_eq!(report["status_gate"]["presence_errors"], 0);
        assert_eq!(report["status_gate"]["staging_errors"], 0);
        assert_eq!(report["status_gate"]["published_errors"], 0);
        assert_eq!(report["status_gate"]["resource_errors"], 0);

        std::fs::remove_dir_all(staging_dir).unwrap();
    }

    #[test]
    fn import_status_exposes_ready_storage_recovery_evidence() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'root', title: 'Root'})")
            .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        let recovery = test_storage_recovery_report(export.manifest.graph_commit_epoch);
        let staging_dir = unique_main_test_dir("graph_lightning_status_ready_recovery");
        let publish_dir = unique_main_test_dir("graph_lightning_status_ready_recovery_target");
        stage_graph_lightning_bootstrap_export_with_storage_recovery(
            &export,
            &staging_dir,
            "skein-storage-v1",
            &recovery,
        )
        .unwrap();

        let report = graph_lightning_import_status(&staging_dir, &publish_dir).unwrap();

        assert_eq!(report["import_state"], "READY");
        assert_eq!(report["storage_recovery_evidence"]["present"], true);
        assert_eq!(report["storage_recovery_evidence"]["valid"], true);
        assert_eq!(
            report["storage_recovery_evidence"],
            report["staging_verification"]["storage_recovery_evidence"]
        );

        std::fs::remove_dir_all(staging_dir).unwrap();
    }

    #[test]
    fn import_status_reports_published_after_pointer_verifies() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        let staging_dir = unique_main_test_dir("graph_lightning_status_published_staging");
        let publish_dir = unique_main_test_dir("graph_lightning_status_published_target");
        stage_graph_lightning_bootstrap_export(&export, &staging_dir).unwrap();
        publish_graph_lightning_staging_catalog(&staging_dir, &publish_dir).unwrap();

        let report = graph_lightning_import_status(&staging_dir, &publish_dir).unwrap();

        assert_eq!(report["import_state"], "PUBLISHED");
        assert_eq!(report["published_pointer_present"], true);
        assert_eq!(
            report["published_verification"]["validation_gate"]["decision"],
            "ready"
        );
        assert_eq!(report["status_gate"]["decision"], "ready");
        assert_eq!(report["resume_action"]["operation"], "none");
        assert_eq!(report["resume_action"]["safe_to_retry"], false);
        assert_eq!(report["resume_action"]["terminal"], true);
        assert_eq!(report["resource_retention"]["action"], "follow_gc_report");
        assert_eq!(report["resource_retention"]["safe_to_collect"], false);
        assert_eq!(report["resource_retention"]["protected_count"], 4);
        assert_eq!(report["resource_retention"]["deletable_count"], 0);
        assert_eq!(report["resource_retention"]["gc_deletable_count"], 0);
        assert_eq!(
            report["resource_retention"]["gc_report"]["published_pointer_state"],
            "verified"
        );
        assert_eq!(report["status_gate"]["presence_errors"], 0);
        assert_eq!(report["status_gate"]["staging_errors"], 0);
        assert_eq!(report["status_gate"]["published_errors"], 0);
        assert_eq!(report["status_gate"]["resource_errors"], 0);

        std::fs::remove_dir_all(staging_dir).unwrap();
        std::fs::remove_dir_all(publish_dir).unwrap();
    }

    #[test]
    fn import_status_exposes_published_storage_recovery_evidence() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'root', title: 'Root'})")
            .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        let recovery = test_storage_recovery_report(export.manifest.graph_commit_epoch);
        let staging_dir = unique_main_test_dir("graph_lightning_status_published_recovery");
        let publish_dir = unique_main_test_dir("graph_lightning_status_published_recovery_target");
        stage_graph_lightning_bootstrap_export_with_storage_recovery(
            &export,
            &staging_dir,
            "skein-storage-v1",
            &recovery,
        )
        .unwrap();
        publish_graph_lightning_staging_catalog(&staging_dir, &publish_dir).unwrap();

        let report = graph_lightning_import_status(&staging_dir, &publish_dir).unwrap();

        assert_eq!(report["import_state"], "PUBLISHED");
        assert_eq!(report["storage_recovery_evidence"]["present"], true);
        assert_eq!(report["storage_recovery_evidence"]["valid"], true);
        assert_eq!(
            report["storage_recovery_evidence"],
            report["published_verification"]["storage_recovery_evidence"]
        );

        std::fs::remove_dir_all(staging_dir).unwrap();
        std::fs::remove_dir_all(publish_dir).unwrap();
    }

    #[test]
    fn import_status_reports_quarantined_when_staging_fails() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        let staging_dir = unique_main_test_dir("graph_lightning_status_quarantined_staging");
        let publish_dir = unique_main_test_dir("graph_lightning_status_quarantined_target");
        stage_graph_lightning_bootstrap_export(&export, &staging_dir).unwrap();
        let catalog_path = staging_dir.join("graph_lightning_staging_catalog.json");
        let tampered = std::fs::read_to_string(&catalog_path).unwrap().replace(
            "\"stage_state\": \"READY\"",
            "\"stage_state\": \"QUARANTINED\"",
        );
        std::fs::write(&catalog_path, tampered).unwrap();

        let report = graph_lightning_import_status(&staging_dir, &publish_dir).unwrap();

        assert_eq!(report["import_state"], "QUARANTINED");
        assert_eq!(report["status_gate"]["decision"], "blocked");
        assert_eq!(report["resume_action"]["operation"], "inspect_errors");
        assert_eq!(report["resume_action"]["safe_to_retry"], false);
        assert_eq!(report["resume_action"]["terminal"], true);
        assert_eq!(
            report["resource_retention"]["action"],
            "hold_for_inspection"
        );
        assert_eq!(report["resource_retention"]["safe_to_collect"], false);
        assert_eq!(report["resource_retention"]["protected_count"], 4);
        assert_eq!(report["resource_retention"]["deletable_count"], 0);
        assert_eq!(report["resource_retention"]["gc_deletable_count"], 4);
        assert_eq!(report["status_gate"]["presence_errors"], 0);
        assert_eq!(report["status_gate"]["staging_errors"], 1);
        assert_eq!(report["status_gate"]["published_errors"], 0);
        assert_eq!(report["status_gate"]["resource_errors"], 0);
        assert!(report["status_gate"]["staging_error_messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|error| error
                .as_str()
                .unwrap()
                .contains("staging catalog is not READY")));
        assert!(report["status_gate"]["errors"]
            .as_array()
            .unwrap()
            .iter()
            .any(|error| error
                .as_str()
                .unwrap()
                .contains("staging catalog is not READY")));

        std::fs::remove_dir_all(staging_dir).unwrap();
    }

    #[test]
    fn import_status_reports_canceled_state_marker_over_ready_staging() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        let staging_dir = unique_main_test_dir("graph_lightning_status_canceled_marker_staging");
        let publish_dir = unique_main_test_dir("graph_lightning_status_canceled_marker_target");
        stage_graph_lightning_bootstrap_export(&export, &staging_dir).unwrap();
        std::fs::write(
            staging_dir.join("graph_lightning_import_state.json"),
            serde_json::json!({
                "protocol": "graph-lightning-import-state",
                "protocol_version": 1,
                "import_state": "CANCELED",
                "import_id": "import-canceled"
            })
            .to_string(),
        )
        .unwrap();

        let report = graph_lightning_import_status(&staging_dir, &publish_dir).unwrap();

        assert_eq!(report["artifact_state"], "READY");
        assert_eq!(report["import_state"], "CANCELED");
        assert_eq!(report["state_marker"]["import_state"], "CANCELED");
        assert_eq!(report["state_marker"]["idempotency_ready"], false);
        assert_eq!(
            report["state_marker"]["idempotency_key"],
            serde_json::Value::Null
        );
        assert_eq!(report["resume_action"]["operation"], "none");
        assert_eq!(report["resume_action"]["safe_to_retry"], false);
        assert_eq!(report["resume_action"]["terminal"], true);
        assert_eq!(
            report["resource_retention"]["action"],
            "hold_for_inspection"
        );
        assert_eq!(report["resource_retention"]["protected_count"], 4);
        assert_eq!(report["resource_retention"]["safe_to_collect"], false);
        assert_eq!(report["status_gate"]["decision"], "ready");
        assert_eq!(report["status_gate"]["state_errors"], 0);

        std::fs::remove_dir_all(staging_dir).unwrap();
    }

    #[test]
    fn import_status_blocks_when_resource_retention_cannot_read_candidates() {
        let staging_dir = unique_main_test_dir("graph_lightning_status_resource_blocked_staging");
        let publish_dir = unique_main_test_dir("graph_lightning_status_resource_blocked_target");
        std::fs::create_dir_all(&staging_dir).unwrap();
        std::fs::write(
            staging_dir.join("graph_lightning_staging_catalog.json"),
            serde_json::json!({
                "protocol": "graph-lightning-staging-catalog",
                "stage_state": "READY",
                "export_gate": {
                    "decision": "ready"
                }
            })
            .to_string(),
        )
        .unwrap();

        let report = graph_lightning_import_status(&staging_dir, &publish_dir).unwrap();

        assert_eq!(report["import_state"], "QUARANTINED");
        assert_eq!(report["status_gate"]["decision"], "blocked");
        assert_eq!(
            report["resource_retention"]["action"],
            "hold_for_inspection"
        );
        assert_eq!(report["resource_retention"]["safe_to_collect"], false);
        assert_eq!(report["resource_retention"]["protected_count"], 0);
        assert_eq!(report["resource_retention"]["deletable_count"], 0);
        assert_eq!(
            report["resource_retention"]["gc_report"],
            serde_json::Value::Null
        );
        assert_eq!(report["status_gate"]["resource_errors"], 1);
        assert!(report["status_gate"]["resource_error_messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|error| error
                .as_str()
                .unwrap()
                .contains("resource retention report failed")));

        std::fs::remove_dir_all(staging_dir).unwrap();
    }

    #[test]
    fn import_status_quarantines_active_state_marker_without_idempotency_key() {
        let staging_dir =
            unique_main_test_dir("graph_lightning_status_missing_idempotency_marker_staging");
        let publish_dir =
            unique_main_test_dir("graph_lightning_status_missing_idempotency_marker_target");
        std::fs::create_dir_all(&staging_dir).unwrap();
        std::fs::write(
            staging_dir.join("graph_lightning_import_state.json"),
            serde_json::json!({
                "protocol": "graph-lightning-import-state",
                "protocol_version": 1,
                "import_state": "UPLOADING",
                "import_id": "import-1",
                "task_id": "task-1"
            })
            .to_string(),
        )
        .unwrap();

        let report = graph_lightning_import_status(&staging_dir, &publish_dir).unwrap();

        assert_eq!(report["artifact_state"], "CREATED");
        assert_eq!(report["import_state"], "QUARANTINED");
        assert_eq!(report["state_marker"]["import_state"], "QUARANTINED");
        assert_eq!(report["state_marker"]["idempotency_ready"], false);
        assert_eq!(
            report["state_marker"]["idempotency_key"],
            serde_json::Value::Null
        );
        assert_eq!(report["status_gate"]["decision"], "blocked");
        assert_eq!(report["status_gate"]["state_errors"], 2);
        assert!(report["status_gate"]["state_error_messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|error| error
                .as_str()
                .unwrap()
                .contains("missing idempotency field fencing_token")));
        assert!(report["status_gate"]["state_error_messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|error| error
                .as_str()
                .unwrap()
                .contains("missing idempotency field object_digest")));

        std::fs::remove_dir_all(staging_dir).unwrap();
    }

    #[test]
    fn import_status_quarantines_invalid_state_marker() {
        let staging_dir = unique_main_test_dir("graph_lightning_status_invalid_marker_staging");
        let publish_dir = unique_main_test_dir("graph_lightning_status_invalid_marker_target");
        std::fs::create_dir_all(&staging_dir).unwrap();
        std::fs::write(
            staging_dir.join("graph_lightning_import_state.json"),
            serde_json::json!({
                "protocol": "wrong-protocol",
                "protocol_version": 99,
                "import_state": "UNKNOWN"
            })
            .to_string(),
        )
        .unwrap();

        let report = graph_lightning_import_status(&staging_dir, &publish_dir).unwrap();

        assert_eq!(report["artifact_state"], "CREATED");
        assert_eq!(report["import_state"], "QUARANTINED");
        assert_eq!(report["state_marker"]["present"], true);
        assert_eq!(report["state_marker"]["import_state"], "QUARANTINED");
        assert_eq!(report["resume_action"]["operation"], "inspect_errors");
        assert_eq!(report["status_gate"]["decision"], "blocked");
        assert_eq!(report["status_gate"]["state_errors"], 3);
        assert!(report["status_gate"]["state_error_messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|error| error.as_str().unwrap().contains("protocol mismatch")));
        assert!(report["status_gate"]["state_error_messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|error| error
                .as_str()
                .unwrap()
                .contains("unsupported state UNKNOWN")));

        std::fs::remove_dir_all(staging_dir).unwrap();
    }

    #[test]
    fn renders_stable_identity_audit_values() {
        let audit = CanonicalSnapshotIdentityAudit {
            requires_stable_id_mapping: true,
            nodes_without_stable_id: vec![7],
            relationships_without_stable_id: vec![9],
            duplicate_node_stable_ids: vec![Value::Int(42)],
            duplicate_relationship_stable_ids: vec![Value::Bool(true)],
        };

        let json = stable_identity_audit_json(&audit);

        assert_eq!(json["requires_stable_id_mapping"], true);
        assert_eq!(json["nodes_without_stable_id"], serde_json::json!([7]));
        assert_eq!(json["duplicate_node_stable_ids"], serde_json::json!([42]));
        assert_eq!(
            json["duplicate_relationship_stable_ids"],
            serde_json::json!([true])
        );
    }

    #[test]
    fn renders_nested_values_as_json() {
        let value = Value::Map(
            [(
                "items".to_string(),
                Value::List(vec![
                    Value::Null,
                    Value::Int(1),
                    Value::String("two".to_string()),
                ]),
            )]
            .into_iter()
            .collect(),
        );

        assert_eq!(
            value_json(&value),
            serde_json::json!({
                "items": [null, 1, "two"]
            })
        );
    }

    #[test]
    fn validates_canonical_snapshot_usage_text() {
        assert!(validate_canonical_snapshot_usage().contains("<database-path>"));
        assert!(validate_canonical_snapshot_usage().contains("--require-import-ready"));
    }

    #[test]
    fn validates_explain_json_usage_text() {
        assert!(explain_json_usage().contains("<database-path>"));
        assert!(explain_json_usage().contains("<cypher>"));
        assert!(explain_json_usage().contains("--params-json"));
    }

    #[test]
    fn validates_printable_explain_usage_text() {
        let usage = explain_table_usage("explain-analyze");
        assert!(usage.contains("explain-analyze"));
        assert!(usage.contains("<database-path>"));
        assert!(usage.contains("<cypher>"));
        assert!(usage.contains("--params-json"));
    }

    #[test]
    fn validates_explain_analyze_json_usage_text() {
        assert!(explain_analyze_json_usage().contains("<database-path>"));
        assert!(explain_analyze_json_usage().contains("<cypher>"));
        assert!(explain_analyze_json_usage().contains("--params-json"));
    }

    #[test]
    fn validates_graph_lightning_bootstrap_manifest_usage_text() {
        assert!(graph_lightning_bootstrap_manifest_usage().contains("<database-path>"));
        assert!(graph_lightning_bootstrap_manifest_usage().contains("--require-ready"));
    }

    #[test]
    fn validates_graph_lightning_bootstrap_bundle_usage_text() {
        assert!(graph_lightning_bootstrap_bundle_usage().contains("<database-path>"));
        assert!(graph_lightning_bootstrap_bundle_usage().contains("--require-ready"));
    }

    #[test]
    fn validates_graph_lightning_stage_bootstrap_usage_text() {
        assert!(graph_lightning_stage_bootstrap_usage().contains("<database-path>"));
        assert!(graph_lightning_stage_bootstrap_usage().contains("<staging-dir>"));
        assert!(graph_lightning_stage_bootstrap_usage().contains("--require-ready"));
    }

    #[test]
    fn validates_graph_lightning_verify_staging_usage_text() {
        assert!(graph_lightning_verify_staging_usage().contains("<staging-dir>"));
        assert!(graph_lightning_verify_staging_usage().contains("--require-ready"));
    }

    #[test]
    fn validates_graph_lightning_publish_staging_usage_text() {
        assert!(graph_lightning_publish_staging_usage().contains("<staging-dir>"));
        assert!(graph_lightning_publish_staging_usage().contains("<publish-dir>"));
        assert!(graph_lightning_publish_staging_usage().contains("--require-state-marker"));
        assert!(graph_lightning_publish_staging_usage().contains("--fencing-token"));
        assert!(graph_lightning_publish_staging_usage().contains("--expected-graph-epoch"));
    }

    #[test]
    fn validates_graph_lightning_verify_published_usage_text() {
        assert!(graph_lightning_verify_published_usage().contains("<staging-dir>"));
        assert!(graph_lightning_verify_published_usage().contains("<publish-dir>"));
    }

    #[test]
    fn validates_graph_lightning_graph_stream_usage_text() {
        assert!(graph_lightning_graph_stream_usage().contains("<database-path>"));
        assert!(graph_lightning_graph_stream_usage().contains("--require-ready"));
    }

    #[test]
    fn validates_graph_lightning_verify_export_usage_text() {
        assert!(graph_lightning_verify_export_usage().contains("<database-path>"));
        assert!(graph_lightning_verify_export_usage().contains("--require-valid"));
    }

    #[test]
    fn compatibility_tools_are_opt_in_for_developer_paths() {
        assert!(!crate::compatibility_tools_enabled_from_value(None));
        assert!(!crate::compatibility_tools_enabled_from_value(Some("")));
        assert!(!crate::compatibility_tools_enabled_from_value(Some("0")));
        assert!(crate::compatibility_tools_enabled_from_value(Some("1")));
        assert!(crate::compatibility_tools_enabled_from_value(Some("true")));
        assert!(crate::compatibility_tools_enabled_from_value(Some("YES")));
        assert!(crate::compatibility_tools_enabled_from_value(Some(" on ")));
    }

    #[test]
    fn compatibility_tool_usage_mentions_developer_quarantine() {
        assert!(
            crate::external_shadow_adapter_smoke_usage().contains("developer compatibility tool")
        );
        assert!(crate::external_shadow_adapter_smoke_usage()
            .contains(crate::SKEIN_ENABLE_COMPATIBILITY_TOOLS_ENV));
        assert!(
            crate::nowledge_cypher_migration_gate_usage().contains("developer compatibility tool")
        );
        assert!(crate::nowledge_cypher_migration_gate_usage()
            .contains(crate::SKEIN_ENABLE_COMPATIBILITY_TOOLS_ENV));
    }

    fn test_storage_recovery_report(graph_commit_epoch: u64) -> StorageRecoveryReport {
        StorageRecoveryReport {
            durable: true,
            recovery_mode: RecoveryMode::Strict,
            max_wal_replay_entries: Some(32),
            max_wal_replay_bytes: Some(4096),
            max_wal_record_bytes: Some(1024),
            checkpoint_epoch: Some(1),
            checkpoint_commit_epoch: Some(graph_commit_epoch),
            wal_present: true,
            wal_replay_start_lsn: Some(1),
            next_lsn_after_replay: Some(1),
            replayed_wal_entries: 0,
            torn_tail_ignored: false,
            torn_tail_reason: None,
            recovered_commit_epoch: graph_commit_epoch,
            ..StorageRecoveryReport::default()
        }
    }

    fn unique_main_test_dir(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein-{name}-{nanos}"))
    }

    fn unique_json_file(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein-{name}-{nanos}.json"))
    }

    fn adapter_smoke_report(
        shadow_checks: Vec<CompatibilityShadowCheckReport>,
    ) -> CompatibilityShadowReport {
        let primary_checks = shadow_checks
            .iter()
            .map(|check| CompatibilityCheckReport {
                name: check.name.clone(),
            })
            .collect();
        CompatibilityShadowReport {
            fixture: "external-shadow-adapter-smoke".to_string(),
            shadow_engine: "legacy-wrapper".to_string(),
            primary_checks,
            shadow_checks,
        }
    }

    fn adapter_smoke_shadow_check(
        name: &str,
        status: CompatibilityShadowStatus,
        primary_only_reason: Option<&str>,
    ) -> CompatibilityShadowCheckReport {
        CompatibilityShadowCheckReport {
            name: name.to_string(),
            status,
            primary_only_reason: primary_only_reason.map(str::to_string),
        }
    }
}
