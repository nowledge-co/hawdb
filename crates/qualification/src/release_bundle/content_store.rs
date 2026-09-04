use super::{require_bool, require_empty_array, validate_common_artifact, validate_exact_binding};
use crate::evidence_digest::hash_bytes;
use crate::{
    latency_percentiles, nowledge_content_store_schema_identity, nowledge_content_store_sql_corpus,
    ContentStoreSqlCorpus, ContentStoreSqlStatementKind, CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
    CONTENT_STORE_SHARED_HOST_8_GIB_BYTES, CONTENT_STORE_SHARED_HOST_MAX_CAPACITY_BYTES,
    MAX_PRODUCTION_COMMIT_P95_REGRESSION_PER_MILLION,
    PRODUCTION_CONTENT_STORE_MUTATION_QUALIFICATION_PROTOCOL,
    PRODUCTION_CONTENT_STORE_STORAGE_QUALIFICATION_PROTOCOL,
    PRODUCTION_CONTENT_STORE_WRITER_MATRIX,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use skein::ProductionQualificationIdentity;
use std::collections::{BTreeMap, BTreeSet};

fn validate_read_storage(
    artifact: &Value,
    expected: &ProductionQualificationIdentity,
) -> Vec<String> {
    let mut blockers = Vec::new();
    validate_common_artifact(
        artifact,
        PRODUCTION_CONTENT_STORE_STORAGE_QUALIFICATION_PROTOCOL,
        "representative_production_relational_replica",
        &mut blockers,
    );
    validate_exact_binding(artifact, "/evidence_binding", expected, &mut blockers);
    let corpus = validate_frozen_contract(artifact, &mut blockers);
    validate_profile(artifact, false, &mut blockers);

    let measurement_runs = unsigned(artifact, "/measurement_runs").unwrap_or_default();
    if !(2..=1024).contains(&measurement_runs) {
        blockers.push("content_store_read_measurement_count_invalid".to_string());
    }
    let Some(contracts) = artifact
        .pointer("/read_contracts")
        .and_then(Value::as_array)
    else {
        blockers.push("content_store_read_contracts_missing".to_string());
        return deduplicate(blockers);
    };
    if contracts.is_empty() {
        blockers.push("content_store_read_contracts_missing".to_string());
    }
    let mut contracts_by_name = BTreeMap::new();
    for contract in contracts {
        let Some(name) = string(contract, "/case_name") else {
            blockers.push("content_store_read_contract_identity_invalid".to_string());
            continue;
        };
        if name.is_empty() || contracts_by_name.insert(name, contract).is_some() {
            blockers.push("content_store_read_contract_duplicate".to_string());
        }
        if string(contract, "/statement_name").is_none()
            || corpus.as_ref().is_none_or(|corpus| {
                !valid_statement_contract(
                    contract,
                    corpus,
                    StatementRole::Read,
                    READ_STATEMENT_DIGEST_DOMAIN,
                )
            })
            || !valid_prefixed_sha256(string(contract, "/parameter_sha256").unwrap_or_default())
            || !valid_sha256(string(contract, "/expected_output_sha256").unwrap_or_default())
        {
            blockers.push("content_store_read_contract_digest_invalid".to_string());
        }
        let expected_rows = unsigned(contract, "/expected_output_rows");
        let max_rows = unsigned(contract, "/max_output_rows");
        if expected_rows.is_none()
            || max_rows.is_none()
            || expected_rows > max_rows
            || unsigned(contract, "/max_output_payload_bytes").unwrap_or_default() == 0
            || unsigned(contract, "/max_intermediate_rows").unwrap_or_default() == 0
            || unsigned(contract, "/max_physical_pages_per_run").unwrap_or_default() == 0
            || unsigned(contract, "/max_physical_bytes_per_run").unwrap_or_default() == 0
        {
            blockers.push("content_store_read_contract_budget_invalid".to_string());
        }
        if corpus.as_ref().is_none_or(|corpus| {
            let Some(statement_name) = string(contract, "/statement_name") else {
                return true;
            };
            let Some(statement) = corpus.statement(statement_name) else {
                return true;
            };
            unsigned(contract, "/max_output_rows") != Some(statement.max_rows as u64)
                || unsigned(contract, "/max_output_payload_bytes")
                    != Some(statement.max_payload_bytes as u64)
        }) {
            blockers.push("content_store_read_frozen_statement_budget_mismatch".to_string());
        }
    }
    let result_budget =
        unsigned(artifact, "/runtime_governor/result_budget_bytes").unwrap_or_default();
    if result_budget == 0
        || contracts.iter().any(|contract| {
            unsigned(contract, "/max_output_payload_bytes").unwrap_or(u64::MAX) > result_budget
        })
    {
        blockers.push("content_store_read_result_budget_invalid".to_string());
    }

    let initial = artifact.pointer("/initial_residency");
    let final_residency = artifact.pointer("/final_residency");
    for residency in [initial, final_residency].into_iter().flatten() {
        validate_read_residency(residency, expected, &mut blockers);
    }
    match (initial, final_residency) {
        (Some(initial), Some(final_residency)) => {
            if !same_read_storage_identity(initial, final_residency) {
                blockers.push("content_store_read_storage_identity_changed".to_string());
            }
        }
        _ => blockers.push("content_store_read_residency_missing".to_string()),
    }

    let Some(opens) = artifact.pointer("/opens").and_then(Value::as_array) else {
        blockers.push("content_store_read_opens_missing".to_string());
        return deduplicate(blockers);
    };
    let open_payload_cache_limits = artifact.pointer("/open_payload_cache_limits");
    let expected_open_cache_capacity =
        initial.and_then(|residency| unsigned(residency, "/segment_cache_capacity_bytes"));
    let mut open_names = BTreeSet::new();
    for open in opens {
        let Some(name) = string(open, "/case_name") else {
            blockers.push("content_store_read_open_identity_invalid".to_string());
            continue;
        };
        if !contracts_by_name.contains_key(name) || !open_names.insert(name) {
            blockers.push("content_store_read_open_identity_invalid".to_string());
        }
        if unsigned(open, "/recovered_commit_epoch") != Some(expected.canonical_graph_commit_epoch)
        {
            blockers.push("content_store_read_open_epoch_mismatch".to_string());
        }
        validate_open_timings(
            open,
            "/latency_micros",
            "content_store_read_open_timing_invalid",
            &mut blockers,
        );
        validate_bounded_open_payload_cache(
            open,
            open_payload_cache_limits,
            expected_open_cache_capacity,
            &mut blockers,
        );
    }
    if opens.len() != contracts_by_name.len() || open_names.len() != contracts_by_name.len() {
        blockers.push("content_store_read_open_count_mismatch".to_string());
    }

    let Some(runs) = artifact.pointer("/runs").and_then(Value::as_array) else {
        blockers.push("content_store_read_runs_missing".to_string());
        return deduplicate(blockers);
    };
    if runs.len() as u64 != measurement_runs.saturating_mul(contracts_by_name.len() as u64) {
        blockers.push("content_store_read_run_count_mismatch".to_string());
    }
    let mut run_keys = BTreeSet::new();
    let mut cold_cases = BTreeSet::new();
    let mut warm_cases = BTreeSet::new();
    let mut authoritative_index_observed = false;
    for run in runs {
        let Some(case_name) = string(run, "/case_name") else {
            blockers.push("content_store_read_run_identity_invalid".to_string());
            continue;
        };
        let Some(contract) = contracts_by_name.get(case_name).copied() else {
            blockers.push("content_store_read_run_identity_invalid".to_string());
            continue;
        };
        let run_index = unsigned(run, "/run").unwrap_or(u64::MAX);
        if run_index >= measurement_runs || !run_keys.insert((case_name, run_index)) {
            blockers.push("content_store_read_run_identity_invalid".to_string());
        }
        let expected_phase = if run_index == 0 { "cold" } else { "warm" };
        if string(run, "/phase") != Some(expected_phase) {
            blockers.push("content_store_read_phase_invalid".to_string());
        }
        validate_read_run(
            run,
            contract,
            initial.unwrap_or(&Value::Null),
            artifact,
            &mut blockers,
        );
        let cache_misses = unsigned(run, "/read/cache/misses").unwrap_or_default();
        let cache_hits = unsigned(run, "/read/cache/hits").unwrap_or_default();
        if run_index == 0 && cache_misses > 0 {
            cold_cases.insert(case_name);
        }
        if run_index > 0 && cache_hits > 0 {
            warm_cases.insert(case_name);
        }
        authoritative_index_observed |=
            string(run, "/read/execution/index_runtime_path") == Some("authoritative");
    }
    if cold_cases.len() != contracts_by_name.len() {
        blockers.push("content_store_read_cold_cache_miss_missing".to_string());
    }
    if warm_cases.len() != contracts_by_name.len() {
        blockers.push("content_store_read_warm_cache_hit_missing".to_string());
    }
    if !authoritative_index_observed {
        blockers.push("content_store_read_authoritative_index_missing".to_string());
    }
    validate_read_governor(artifact, runs.len() as u64, &mut blockers);
    validate_process(
        ProcessValidation {
            process: artifact.pointer("/lifecycle_process"),
            limits: artifact.pointer("/resource_limits"),
            total_limit: "max_total_page_faults_per_run",
            minor_limit: "max_minor_page_faults_per_run",
            major_limit: "max_major_page_faults_per_run",
            check_faults: false,
            prefix: "content_store_read_lifecycle",
        },
        &mut blockers,
    );
    deduplicate(blockers)
}

pub(super) fn validate_production_read_storage(
    artifact: &Value,
    expected: &ProductionQualificationIdentity,
) -> Vec<String> {
    let mut blockers = validate_read_storage(artifact, expected);
    if string(artifact, "/resource_profile_kind") == Some("capability512_mib") {
        blockers.push("content_store_production_read_uses_512_mib_capability".to_string());
    }
    deduplicate(blockers)
}

pub(super) fn validate_512_mib_read_storage(
    artifact: &Value,
    expected: &ProductionQualificationIdentity,
) -> Vec<String> {
    let mut blockers = validate_read_storage(artifact, expected);
    if string(artifact, "/resource_profile_kind") != Some("capability512_mib") {
        blockers.push("content_store_512_mib_read_profile_missing".to_string());
    }
    deduplicate(blockers)
}

pub(super) fn validate_mutation_matrix(
    artifact: &Value,
    expected: &ProductionQualificationIdentity,
) -> Vec<String> {
    let mut blockers = Vec::new();
    validate_common_artifact(
        artifact,
        PRODUCTION_CONTENT_STORE_MUTATION_QUALIFICATION_PROTOCOL,
        "representative_production_relational_mutation_replicas",
        &mut blockers,
    );
    validate_exact_binding(artifact, "/evidence_binding", expected, &mut blockers);
    let corpus = validate_frozen_contract(artifact, &mut blockers);
    validate_profile(artifact, true, &mut blockers);

    let regression_limit =
        unsigned(artifact, "/max_commit_p95_regression_per_million").unwrap_or(u64::MAX);
    if regression_limit > u64::from(MAX_PRODUCTION_COMMIT_P95_REGRESSION_PER_MILLION) {
        blockers.push("content_store_mutation_regression_limit_invalid".to_string());
    }
    if string(artifact, "/latency_reference/source_revision")
        .is_none_or(|revision| revision.trim().is_empty())
        || unsigned(artifact, "/latency_reference/generated_at_unix_seconds").unwrap_or_default()
            == 0
        || string(artifact, "/latency_reference/configuration_digest")
            != Some(expected.configuration_digest.as_str())
        || string(artifact, "/latency_reference/dataset_fingerprint")
            != Some(expected.dataset_fingerprint.as_str())
    {
        blockers.push("content_store_mutation_latency_reference_invalid".to_string());
    }

    let Some(cases) = artifact.pointer("/cases").and_then(Value::as_array) else {
        blockers.push("content_store_mutation_cases_missing".to_string());
        return deduplicate(blockers);
    };
    let mut writer_counts = BTreeSet::new();
    let mut expected_sequence = None;
    for case in cases {
        let writer_count = unsigned(case, "/writer_count").unwrap_or_default();
        if !writer_counts.insert(writer_count) {
            blockers.push("content_store_mutation_writer_count_duplicate".to_string());
        }
        validate_mutation_case(
            case,
            artifact,
            expected,
            regression_limit,
            corpus.as_ref(),
            &mut blockers,
        );
        match (expected_sequence.as_ref(), mutation_sequence(case)) {
            (None, Some(sequence)) => expected_sequence = Some(sequence),
            (Some(expected), Some(sequence)) if expected == &sequence => {}
            _ => blockers.push("content_store_mutation_case_sequence_mismatch".to_string()),
        }
    }
    let required = PRODUCTION_CONTENT_STORE_WRITER_MATRIX
        .into_iter()
        .map(|count| count as u64)
        .collect::<BTreeSet<_>>();
    if writer_counts != required || cases.len() != PRODUCTION_CONTENT_STORE_WRITER_MATRIX.len() {
        blockers.push("content_store_mutation_writer_matrix_incomplete".to_string());
    }
    deduplicate(blockers)
}

pub(super) fn validate_frozen_contract(
    artifact: &Value,
    blockers: &mut Vec<String>,
) -> Option<ContentStoreSqlCorpus> {
    let corpus = nowledge_content_store_sql_corpus().ok();
    let corpus_identity = corpus
        .as_ref()
        .map(|corpus| serde_json::to_value(corpus.identity()).expect("corpus is serializable"));
    if artifact.pointer("/corpus") != corpus_identity.as_ref() {
        blockers.push("content_store_corpus_identity_mismatch".to_string());
    }
    let schema = serde_json::to_value(nowledge_content_store_schema_identity())
        .expect("schema identity is serializable");
    if artifact.pointer("/schema") != Some(&schema) {
        blockers.push("content_store_schema_identity_mismatch".to_string());
    }
    corpus
}

fn validate_profile(artifact: &Value, mutation: bool, blockers: &mut Vec<String>) {
    let prefix = if mutation {
        "content_store_mutation"
    } else {
        "content_store_read"
    };
    let configured = unsigned(artifact, "/configured_available_memory_bytes").unwrap_or_default();
    let steady =
        unsigned(artifact, "/resource_limits/max_steady_resident_bytes").unwrap_or_default();
    let peak = unsigned(artifact, "/resource_limits/max_peak_resident_bytes").unwrap_or_default();
    if configured == 0 || steady == 0 || peak == 0 || steady > peak || peak > configured {
        blockers.push(format!("{prefix}_memory_limits_invalid"));
    }
    match string(artifact, "/resource_profile_kind") {
        Some("capability512_mib") => {
            if configured != CONTENT_STORE_512_MIB_CAPABILITY_BYTES {
                blockers.push(format!("{prefix}_512_mib_profile_invalid"));
            }
        }
        Some("shared_host8_gib") => {
            if configured != CONTENT_STORE_SHARED_HOST_8_GIB_BYTES
                || peak > CONTENT_STORE_SHARED_HOST_MAX_CAPACITY_BYTES
            {
                blockers.push(format!("{prefix}_shared_host_profile_invalid"));
            }
        }
        Some("configured_workload") => {}
        _ => blockers.push(format!("{prefix}_profile_kind_invalid")),
    }
}

fn validate_read_residency(
    residency: &Value,
    expected: &ProductionQualificationIdentity,
    blockers: &mut Vec<String>,
) {
    for (pointer, code) in [
        ("/row_serving", "content_store_read_rows_not_serving"),
        ("/index_serving", "content_store_read_indexes_not_serving"),
        (
            "/row_index_epoch_aligned",
            "content_store_read_row_index_epoch_mismatch",
        ),
    ] {
        require_bool(residency, pointer, true, code, blockers);
    }
    let epoch = unsigned(residency, "/database_commit_epoch");
    if epoch != Some(expected.canonical_graph_commit_epoch)
        || unsigned(residency, "/row_visible_commit_epoch") != epoch
        || unsigned(residency, "/index_visible_commit_epoch") != epoch
    {
        blockers.push("content_store_read_residency_epoch_mismatch".to_string());
    }
    if unsigned(residency, "/row_base_generation") != unsigned(residency, "/index_base_generation")
        || unsigned(residency, "/row_base_commit_epoch")
            != unsigned(residency, "/index_base_commit_epoch")
        || unsigned(residency, "/row_visible_commit_epoch")
            != unsigned(residency, "/index_visible_commit_epoch")
    {
        blockers.push("content_store_read_residency_alignment_invalid".to_string());
    }
    let cache = unsigned(residency, "/segment_cache_capacity_bytes").unwrap_or_default();
    let row_bytes = unsigned(residency, "/row_canonical_artifact_bytes").unwrap_or_default();
    let index_bytes = unsigned(residency, "/index_canonical_artifact_bytes").unwrap_or_default();
    if cache == 0
        || row_bytes <= cache
        || index_bytes <= cache
        || boolean(residency, "/row_artifact_exceeds_cache") != Some(row_bytes > cache)
        || boolean(residency, "/index_artifact_exceeds_cache") != Some(index_bytes > cache)
    {
        blockers.push("content_store_read_artifact_cache_relation_invalid".to_string());
    }
    if unsigned(residency, "/segment_cache_resident_bytes").unwrap_or(u64::MAX) > cache
        || unsigned(residency, "/segment_cache_pinned_bytes") != Some(0)
    {
        blockers.push("content_store_read_cache_state_invalid".to_string());
    }
}

fn same_read_storage_identity(left: &Value, right: &Value) -> bool {
    [
        "/database_commit_epoch",
        "/row_base_generation",
        "/row_recovery_delta_generation",
        "/row_base_commit_epoch",
        "/row_visible_commit_epoch",
        "/index_base_generation",
        "/index_recovery_delta_generation",
        "/index_base_commit_epoch",
        "/index_visible_commit_epoch",
    ]
    .into_iter()
    .all(|pointer| left.pointer(pointer) == right.pointer(pointer))
}

fn validate_read_run(
    run: &Value,
    contract: &Value,
    residency: &Value,
    artifact: &Value,
    blockers: &mut Vec<String>,
) {
    for (run_pointer, contract_pointer) in [
        ("/statement_name", "/statement_name"),
        ("/statement_sha256", "/statement_sha256"),
        ("/parameter_sha256", "/parameter_sha256"),
        ("/expected_output_sha256", "/expected_output_sha256"),
    ] {
        if run.pointer(run_pointer) != contract.pointer(contract_pointer) {
            blockers.push("content_store_read_run_contract_mismatch".to_string());
        }
    }
    if run.pointer("/read/statement_name") != contract.pointer("/statement_name")
        || run.pointer("/read/output_rows") != contract.pointer("/expected_output_rows")
        || run.pointer("/read/output_sha256") != contract.pointer("/expected_output_sha256")
        || run.pointer("/read/max_rows") != contract.pointer("/max_output_rows")
        || run.pointer("/read/max_payload_bytes") != contract.pointer("/max_output_payload_bytes")
    {
        blockers.push("content_store_read_output_contract_mismatch".to_string());
    }
    let expected_read_phase = match string(run, "/phase") {
        Some("cold") => "production_cold",
        Some("warm") => "production_warm",
        _ => "invalid",
    };
    if string(run, "/read/phase") != Some(expected_read_phase) {
        blockers.push("content_store_read_nested_phase_mismatch".to_string());
    }
    if unsigned(run, "/read/output_payload_bytes").unwrap_or(u64::MAX)
        > unsigned(contract, "/max_output_payload_bytes").unwrap_or_default()
        || unsigned(run, "/read/execution/intermediate_rows").unwrap_or(u64::MAX)
            > unsigned(contract, "/max_intermediate_rows").unwrap_or_default()
        || sum(
            unsigned(run, "/read/execution/physical_pages"),
            unsigned(run, "/read/execution/index_physical_pages"),
        ) > unsigned(contract, "/max_physical_pages_per_run").unwrap_or_default()
        || sum(
            unsigned(run, "/read/execution/physical_bytes"),
            unsigned(run, "/read/execution/index_physical_bytes"),
        ) > unsigned(contract, "/max_physical_bytes_per_run").unwrap_or_default()
    {
        blockers.push("content_store_read_run_budget_exceeded".to_string());
    }
    if unsigned(run, "/read/execution/base_generation")
        != unsigned(residency, "/row_base_generation")
        || unsigned(run, "/read/execution/base_commit_epoch")
            != unsigned(residency, "/row_base_commit_epoch")
        || unsigned(run, "/read/execution/visible_commit_epoch")
            != unsigned(residency, "/row_visible_commit_epoch")
    {
        blockers.push("content_store_read_generation_identity_mismatch".to_string());
    }
    if unsigned(run, "/read/execution/cache_admission_rejections") != Some(0)
        || unsigned(run, "/read/execution/index_cache_admission_rejections") != Some(0)
        || unsigned(run, "/read/cache/admission_rejections") != Some(0)
        || unsigned(run, "/read/cache/pinned_bytes_after") != Some(0)
        || unsigned(run, "/read/cache/resident_bytes_after").unwrap_or(u64::MAX)
            > unsigned(residency, "/segment_cache_capacity_bytes").unwrap_or_default()
    {
        blockers.push("content_store_read_cache_budget_invalid".to_string());
    }
    let hydration_limit = unsigned(artifact, "/max_relational_hydration_bytes").unwrap_or_default();
    if hydration_limit == 0
        || unsigned(run, "/read/execution/hydrated_compressed_bytes").unwrap_or(u64::MAX)
            > hydration_limit
        || unsigned(run, "/read/execution/hydrated_decompressed_bytes").unwrap_or(u64::MAX)
            > hydration_limit
    {
        blockers.push("content_store_read_hydration_budget_exceeded".to_string());
    }
    validate_process(
        ProcessValidation {
            process: run.pointer("/process"),
            limits: artifact.pointer("/resource_limits"),
            total_limit: "max_total_page_faults_per_run",
            minor_limit: "max_minor_page_faults_per_run",
            major_limit: "max_major_page_faults_per_run",
            check_faults: true,
            prefix: "content_store_read_run",
        },
        blockers,
    );
}

fn validate_read_governor(artifact: &Value, expected_runs: u64, blockers: &mut Vec<String>) {
    let governor = artifact
        .pointer("/runtime_governor")
        .unwrap_or(&Value::Null);
    if unsigned(governor, "/admissions_delta") != Some(expected_runs)
        || unsigned(governor, "/completions_delta") != Some(expected_runs)
        || unsigned(governor, "/admission_waits_delta") != Some(0)
        || unsigned(governor, "/admission_rejections_delta") != Some(0)
    {
        blockers.push("content_store_read_governor_accounting_invalid".to_string());
    }
    for pointer in [
        "/final_active_foreground_tasks",
        "/final_active_background_tasks",
        "/final_active_blocking_tasks",
        "/final_active_cpu_slots",
        "/final_active_foreground_io_slots",
        "/final_active_background_io_slots",
        "/final_admitted_memory_bytes",
    ] {
        if unsigned(governor, pointer) != Some(0) {
            blockers.push("content_store_read_governor_permit_leak".to_string());
        }
    }
    if boolean(governor, "/final_overcommitted") != Some(false) {
        blockers.push("content_store_read_governor_permit_leak".to_string());
    }
    let capacity = unsigned(governor, "/memory_capacity_bytes").unwrap_or_default();
    let budget = unsigned(governor, "/memory_budget_bytes").unwrap_or_default();
    let result_budget = unsigned(governor, "/result_budget_bytes").unwrap_or_default();
    if capacity == 0
        || budget == 0
        || result_budget == 0
        || budget > capacity
        || result_budget > budget
    {
        blockers.push("content_store_read_governor_memory_invalid".to_string());
    }
    match string(artifact, "/resource_profile_kind") {
        Some("capability512_mib") => {
            if unsigned(governor, "/configured_memory_ceiling_bytes")
                != Some(CONTENT_STORE_512_MIB_CAPABILITY_BYTES)
                || capacity > CONTENT_STORE_512_MIB_CAPABILITY_BYTES
            {
                blockers.push("content_store_read_512_mib_governor_invalid".to_string());
            }
        }
        Some("shared_host8_gib")
            if unsigned(governor, "/effective_memory_limit_bytes")
                != Some(CONTENT_STORE_SHARED_HOST_8_GIB_BYTES)
                || !governor
                    .pointer("/configured_memory_ceiling_bytes")
                    .is_some_and(Value::is_null)
                || capacity > CONTENT_STORE_SHARED_HOST_MAX_CAPACITY_BYTES
                || budget > CONTENT_STORE_SHARED_HOST_MAX_CAPACITY_BYTES =>
        {
            blockers.push("content_store_read_shared_host_governor_invalid".to_string());
        }
        _ => {}
    }
}

fn validate_mutation_case(
    case: &Value,
    artifact: &Value,
    expected: &ProductionQualificationIdentity,
    regression_limit: u64,
    corpus: Option<&ContentStoreSqlCorpus>,
    blockers: &mut Vec<String>,
) {
    require_empty_array(
        case,
        "/blocker_codes",
        "content_store_mutation_case_reported_blockers",
        blockers,
    );
    let writer_count = unsigned(case, "/writer_count").unwrap_or_default();
    let Some(runs) = case.pointer("/runs").and_then(Value::as_array) else {
        blockers.push("content_store_mutation_runs_missing".to_string());
        return;
    };
    if unsigned(case, "/operation_count") != Some(runs.len() as u64) || writer_count == 0 {
        blockers.push("content_store_mutation_operation_count_mismatch".to_string());
    }
    let mut run_keys = BTreeSet::new();
    let mut per_writer: BTreeMap<u64, Vec<(u64, &str, &str, &str)>> = BTreeMap::new();
    let mut insert_statement = Vec::new();
    let mut update_statement = Vec::new();
    let mut insert_commit = Vec::new();
    let mut update_commit = Vec::new();
    let mut overall_commit = Vec::new();
    for run in runs {
        let writer = unsigned(run, "/writer_index").unwrap_or(u64::MAX);
        let operation = unsigned(run, "/operation_index").unwrap_or(u64::MAX);
        let kind = string(run, "/kind").unwrap_or("invalid");
        let statement_name = string(run, "/statement_name").unwrap_or("invalid");
        let statement_sha256 = string(run, "/statement_sha256").unwrap_or("invalid");
        if writer >= writer_count || !run_keys.insert((writer, operation)) {
            blockers.push("content_store_mutation_run_identity_invalid".to_string());
        }
        if !matches!(kind, "insert" | "update")
            || corpus.is_none_or(|corpus| {
                !valid_statement_contract(
                    run,
                    corpus,
                    if kind == "insert" {
                        StatementRole::Insert
                    } else {
                        StatementRole::Update
                    },
                    MUTATION_STATEMENT_DIGEST_DOMAIN,
                )
            })
            || !valid_prefixed_sha256(string(run, "/parameter_sha256").unwrap_or_default())
        {
            blockers.push("content_store_mutation_run_contract_invalid".to_string());
        }
        per_writer.entry(writer).or_default().push((
            operation,
            kind,
            statement_name,
            statement_sha256,
        ));
        let statement_latency = unsigned(run, "/statement_latency_micros").unwrap_or_default();
        let commit_latency = unsigned(run, "/commit_latency_micros").unwrap_or_default();
        overall_commit.push(commit_latency);
        match kind {
            "insert" => {
                insert_statement.push(statement_latency);
                insert_commit.push(commit_latency);
            }
            "update" => {
                update_statement.push(statement_latency);
                update_commit.push(commit_latency);
            }
            _ => {}
        }
    }
    if per_writer.len() as u64 != writer_count {
        blockers.push("content_store_mutation_writer_evidence_incomplete".to_string());
    }
    let expected_operations_per_writer = (runs.len() as u64)
        .checked_div(writer_count)
        .unwrap_or_default();
    let mut expected_worker_sequence = None;
    for operations in per_writer.values_mut() {
        operations.sort_unstable_by_key(|(operation, _, _, _)| *operation);
        let indices = operations
            .iter()
            .map(|(operation, _, _, _)| *operation)
            .collect::<Vec<_>>();
        let kinds = operations
            .iter()
            .map(|(_, kind, _, _)| *kind)
            .collect::<BTreeSet<_>>();
        if operations.len() as u64 != expected_operations_per_writer
            || indices != (0..expected_operations_per_writer).collect::<Vec<_>>()
            || !kinds.contains("insert")
            || !kinds.contains("update")
        {
            blockers.push("content_store_mutation_worker_sequence_invalid".to_string());
        }
        let sequence = operations
            .iter()
            .map(|(operation, kind, statement, digest)| (*operation, *kind, *statement, *digest))
            .collect::<Vec<_>>();
        match expected_worker_sequence.as_ref() {
            None => expected_worker_sequence = Some(sequence),
            Some(expected) if expected == &sequence => {}
            Some(_) => blockers.push("content_store_mutation_worker_sequence_mismatch".to_string()),
        }
    }
    for (pointer, samples) in [
        ("/insert_statement_latency", &insert_statement),
        ("/update_statement_latency", &update_statement),
        ("/insert_commit_latency", &insert_commit),
        ("/update_commit_latency", &update_commit),
        ("/overall_commit_latency", &overall_commit),
    ] {
        let computed = serde_json::to_value(latency_percentiles(samples))
            .expect("latency percentiles are serializable");
        if case.pointer(pointer) != Some(&computed) {
            blockers.push("content_store_mutation_latency_summary_mismatch".to_string());
        }
    }

    let initial_epoch = unsigned(case, "/initial_commit_epoch").unwrap_or_default();
    let committed_epoch = unsigned(case, "/committed_epoch").unwrap_or_default();
    if initial_epoch != expected.canonical_graph_commit_epoch
        || committed_epoch != initial_epoch.saturating_add(runs.len() as u64)
    {
        blockers.push("content_store_mutation_commit_epoch_mismatch".to_string());
    }
    for pointer in [
        "/initial_storage",
        "/dirty_storage",
        "/recovered_storage",
        "/final_storage",
    ] {
        validate_mutation_storage(
            case.pointer(pointer),
            if pointer == "/initial_storage" {
                initial_epoch
            } else {
                committed_epoch
            },
            &mut *blockers,
        );
    }
    let initial = case.pointer("/initial_storage").unwrap_or(&Value::Null);
    let dirty = case.pointer("/dirty_storage").unwrap_or(&Value::Null);
    let recovered = case.pointer("/recovered_storage").unwrap_or(&Value::Null);
    let final_storage = case.pointer("/final_storage").unwrap_or(&Value::Null);
    let cache = unsigned(initial, "/cache_capacity_bytes").unwrap_or_default();
    if cache == 0
        || unsigned(initial, "/row_canonical_artifact_bytes").unwrap_or_default() <= cache
        || unsigned(initial, "/index_canonical_artifact_bytes").unwrap_or_default() <= cache
    {
        blockers.push("content_store_mutation_artifact_cache_relation_invalid".to_string());
    }
    for storage in [initial, dirty, recovered, final_storage] {
        if unsigned(storage, "/cache_pinned_bytes") != Some(0) {
            blockers.push("content_store_mutation_cache_pin_leak".to_string());
        }
    }
    if unsigned(dirty, "/wal_bytes").unwrap_or_default()
        <= unsigned(initial, "/wal_bytes").unwrap_or_default()
        || unsigned(dirty, "/row_live_entries").unwrap_or_default() == 0
        || unsigned(dirty, "/index_live_entries").unwrap_or_default() == 0
    {
        blockers.push("content_store_mutation_live_wal_evidence_missing".to_string());
    }

    let wal = case.pointer("/wal_group").unwrap_or(&Value::Null);
    if string(wal, "/activation") != Some("evidence_validated")
        || unsigned(wal, "/submitted_commits") != Some(runs.len() as u64)
        || unsigned(wal, "/completed_commits") != Some(runs.len() as u64)
        || unsigned(wal, "/group_count").unwrap_or_default() == 0
    {
        blockers.push("content_store_mutation_group_commit_accounting_invalid".to_string());
    }
    let replay = case.pointer("/wal_replay_open").unwrap_or(&Value::Null);
    if unsigned(replay, "/replayed_wal_entries").unwrap_or_default() == 0
        || unsigned(replay, "/replayed_wal_bytes").unwrap_or_default() == 0
        || unsigned(replay, "/recovered_commit_epoch") != Some(committed_epoch)
        || boolean(replay, "/torn_tail_ignored") != Some(false)
        || boolean(replay, "/torn_tail_repaired") != Some(false)
    {
        blockers.push("content_store_mutation_wal_replay_invalid".to_string());
    }
    validate_open_timings(
        replay,
        "/total_open_latency_micros",
        "content_store_mutation_wal_open_timing_invalid",
        blockers,
    );
    if unsigned(recovered, "/row_recovery_delta_entries").unwrap_or_default() == 0
        || unsigned(recovered, "/index_recovery_delta_entries").unwrap_or_default() == 0
    {
        blockers.push("content_store_mutation_recovery_delta_missing".to_string());
    }
    let manifest = case.pointer("/manifest_only_open").unwrap_or(&Value::Null);
    if unsigned(manifest, "/replayed_wal_entries") != Some(0)
        || unsigned(manifest, "/replayed_wal_bytes") != Some(0)
        || unsigned(manifest, "/recovered_commit_epoch") != Some(committed_epoch)
        || unsigned(manifest, "/checkpoint_commit_epoch") != Some(committed_epoch)
    {
        blockers.push("content_store_mutation_manifest_open_invalid".to_string());
    }
    validate_open_timings(
        manifest,
        "/total_open_latency_micros",
        "content_store_mutation_manifest_open_timing_invalid",
        blockers,
    );
    if unsigned(final_storage, "/row_recovery_delta_entries") != Some(0)
        || unsigned(final_storage, "/index_recovery_delta_entries") != Some(0)
        || unsigned(final_storage, "/row_live_entries") != Some(0)
        || unsigned(final_storage, "/index_live_entries") != Some(0)
    {
        blockers.push("content_store_mutation_checkpoint_fold_invalid".to_string());
    }
    validate_verification(case, corpus, blockers);

    let overall_p95 = unsigned(case, "/overall_commit_latency/p95_micros").unwrap_or(u64::MAX);
    let reference_p95 = unsigned(case, "/reference_commit_p95_micros").unwrap_or_default();
    let max_p95 = unsigned(case, "/max_commit_p95_micros").unwrap_or_default();
    let computed_regression = regression_per_million(overall_p95, reference_p95);
    if reference_p95 == 0
        || max_p95 == 0
        || overall_p95 > max_p95
        || unsigned(case, "/commit_p95_regression_per_million") != Some(computed_regression)
        || computed_regression > regression_limit
    {
        blockers.push("content_store_mutation_commit_latency_budget_invalid".to_string());
    }
    validate_process(
        ProcessValidation {
            process: case.pointer("/process"),
            limits: artifact.pointer("/resource_limits"),
            total_limit: "max_total_page_faults_per_case",
            minor_limit: "max_minor_page_faults_per_case",
            major_limit: "max_major_page_faults_per_case",
            check_faults: true,
            prefix: "content_store_mutation_case",
        },
        blockers,
    );
}

fn validate_mutation_storage(
    storage: Option<&Value>,
    expected_epoch: u64,
    blockers: &mut Vec<String>,
) {
    let Some(storage) = storage else {
        blockers.push("content_store_mutation_storage_evidence_missing".to_string());
        return;
    };
    let cache = unsigned(storage, "/cache_capacity_bytes").unwrap_or_default();
    if unsigned(storage, "/commit_epoch") != Some(expected_epoch)
        || unsigned(storage, "/row_visible_commit_epoch") != Some(expected_epoch)
        || unsigned(storage, "/index_visible_commit_epoch") != Some(expected_epoch)
        || cache == 0
        || unsigned(storage, "/cache_resident_bytes").unwrap_or(u64::MAX) > cache
    {
        blockers.push("content_store_mutation_storage_view_invalid".to_string());
    }
}

fn mutation_sequence(case: &Value) -> Option<Vec<(u64, String, String)>> {
    let runs = case.pointer("/runs")?.as_array()?;
    let mut sequence = runs
        .iter()
        .filter(|run| unsigned(run, "/writer_index") == Some(0))
        .map(|run| {
            Some((
                unsigned(run, "/operation_index")?,
                string(run, "/kind")?.to_string(),
                string(run, "/statement_sha256")?.to_string(),
            ))
        })
        .collect::<Option<Vec<_>>>()?;
    sequence.sort_unstable_by_key(|(operation, _, _)| *operation);
    (!sequence.is_empty()).then_some(sequence)
}

fn validate_open_timings(
    evidence: &Value,
    external_total_pointer: &str,
    blocker: &str,
    blockers: &mut Vec<String>,
) {
    let Some(timings) = evidence.pointer("/open_timings") else {
        blockers.push(blocker.to_string());
        return;
    };
    let phase_sum = [
        "/durable_manifest_open_micros",
        "/checkpoint_root_open_micros",
        "/wal_replay_micros",
        "/post_replay_open_micros",
    ]
    .into_iter()
    .try_fold(0u64, |sum, pointer| {
        unsigned(timings, pointer).map(|value| sum.saturating_add(value))
    });
    let accounted = unsigned(timings, "/accounted_micros");
    let unaccounted = unsigned(timings, "/unaccounted_micros");
    let internal_total = unsigned(timings, "/total_open_micros");
    let external_total = unsigned(evidence, external_total_pointer);
    let valid = phase_sum.is_some()
        && phase_sum == accounted
        && accounted
            .zip(unaccounted)
            .is_some_and(|(accounted, unaccounted)| {
                accounted.saturating_add(unaccounted) == internal_total.unwrap_or(u64::MAX)
            })
        && boolean(timings, "/consistent") == Some(true)
        && internal_total
            .zip(external_total)
            .is_some_and(|(internal, external)| internal <= external);
    if !valid {
        blockers.push(blocker.to_string());
    }
}

fn validate_bounded_open_payload_cache(
    evidence: &Value,
    limits: Option<&Value>,
    expected_capacity: Option<u64>,
    blockers: &mut Vec<String>,
) {
    let (Some(cache), Some(limits)) = (evidence.pointer("/payload_cache"), limits) else {
        blockers.push("content_store_read_open_payload_cache_invalid".to_string());
        return;
    };
    let capacity = unsigned(cache, "/capacity_bytes");
    let resident = unsigned(cache, "/resident_bytes");
    let hits = unsigned(cache, "/hit_count");
    let misses = unsigned(cache, "/miss_count");
    let max_requests = unsigned(limits, "/max_requests");
    let max_resident_bytes = unsigned(limits, "/max_resident_bytes");
    let bounded = capacity.is_some_and(|capacity| capacity > 0)
        && capacity == expected_capacity
        && max_requests.is_some_and(|requests| requests > 0)
        && max_resident_bytes
            .zip(capacity)
            .is_some_and(|(limit, capacity)| limit > 0 && limit <= capacity)
        && resident
            .zip(max_resident_bytes)
            .is_some_and(|(resident, limit)| resident <= limit)
        && hits
            .zip(misses)
            .zip(max_requests)
            .is_some_and(|((hits, misses), limit)| hits.saturating_add(misses) <= limit)
        && [
            "/pinned_bytes",
            "/eviction_count",
            "/admission_rejection_count",
            "/digest_mismatch_count",
        ]
        .into_iter()
        .all(|pointer| unsigned(cache, pointer) == Some(0))
        && boolean(cache, "/within_limits") == Some(true);
    if !bounded {
        blockers.push("content_store_read_open_payload_cache_invalid".to_string());
    }
}

fn validate_verification(
    case: &Value,
    corpus: Option<&ContentStoreSqlCorpus>,
    blockers: &mut Vec<String>,
) {
    let Some(contracts) = case
        .pointer("/verification_contracts")
        .and_then(Value::as_array)
    else {
        blockers.push("content_store_mutation_verification_contracts_missing".to_string());
        return;
    };
    let replay = case
        .pointer("/replay_verification")
        .and_then(Value::as_array);
    let checkpoint = case
        .pointer("/checkpoint_verification")
        .and_then(Value::as_array);
    let (Some(replay), Some(checkpoint)) = (replay, checkpoint) else {
        blockers.push("content_store_mutation_verification_missing".to_string());
        return;
    };
    if replay != checkpoint || replay.len() != contracts.len() {
        blockers.push("content_store_mutation_verification_count_or_parity_mismatch".to_string());
    }
    let mut names = BTreeSet::new();
    for (contract, observed) in contracts.iter().zip(replay) {
        let name = string(contract, "/case_name").unwrap_or_default();
        if name.is_empty()
            || !names.insert(name)
            || corpus.is_none_or(|corpus| {
                !valid_statement_contract(
                    contract,
                    corpus,
                    StatementRole::Read,
                    MUTATION_STATEMENT_DIGEST_DOMAIN,
                )
            })
            || !valid_prefixed_sha256(string(contract, "/parameter_sha256").unwrap_or_default())
            || !valid_sha256(string(contract, "/expected_output_sha256").unwrap_or_default())
        {
            blockers.push("content_store_mutation_verification_contract_invalid".to_string());
        }
        for (observed_pointer, contract_pointer) in [
            ("/case_name", "/case_name"),
            ("/statement_name", "/statement_name"),
            ("/statement_sha256", "/statement_sha256"),
            ("/parameter_sha256", "/parameter_sha256"),
            ("/output_rows", "/expected_output_rows"),
            ("/output_sha256", "/expected_output_sha256"),
        ] {
            if observed.pointer(observed_pointer) != contract.pointer(contract_pointer) {
                blockers.push("content_store_mutation_verification_mismatch".to_string());
            }
        }
    }
}

struct ProcessValidation<'a> {
    process: Option<&'a Value>,
    limits: Option<&'a Value>,
    total_limit: &'a str,
    minor_limit: &'a str,
    major_limit: &'a str,
    check_faults: bool,
    prefix: &'a str,
}

fn validate_process(inputs: ProcessValidation<'_>, blockers: &mut Vec<String>) {
    let ProcessValidation {
        process,
        limits,
        total_limit,
        minor_limit,
        major_limit,
        check_faults,
        prefix,
    } = inputs;
    let (Some(process), Some(limits)) = (process, limits) else {
        blockers.push(format!("{prefix}_resource_evidence_missing"));
        return;
    };
    if boolean(process, "/resident_memory_supported") != Some(true) {
        blockers.push(format!("{prefix}_resident_memory_unavailable"));
    }
    if unsigned(process, "/steady_resident_bytes").unwrap_or(u64::MAX)
        > unsigned(limits, "/max_steady_resident_bytes").unwrap_or_default()
    {
        blockers.push(format!("{prefix}_steady_resident_budget_exceeded"));
    }
    if unsigned(process, "/peak_resident_bytes").unwrap_or(u64::MAX)
        > unsigned(limits, "/max_peak_resident_bytes").unwrap_or_default()
    {
        blockers.push(format!("{prefix}_peak_resident_budget_exceeded"));
    }
    if !check_faults {
        return;
    }
    for (kind, support_pointer, observed_pointer, limit_field) in [
        (
            "total",
            "/total_page_faults_supported",
            "/total_page_faults",
            total_limit,
        ),
        (
            "minor",
            "/split_page_faults_supported",
            "/minor_page_faults",
            minor_limit,
        ),
        (
            "major",
            "/split_page_faults_supported",
            "/major_page_faults",
            major_limit,
        ),
    ] {
        let limit_pointer = format!("/{limit_field}");
        let Some(limit) = unsigned(limits, &limit_pointer) else {
            continue;
        };
        if boolean(process, support_pointer) != Some(true)
            || unsigned(process, observed_pointer).is_none()
        {
            blockers.push(format!("{prefix}_{kind}_page_faults_unavailable"));
        } else if unsigned(process, observed_pointer).is_some_and(|observed| observed > limit) {
            blockers.push(format!("{prefix}_{kind}_page_fault_budget_exceeded"));
        }
    }
}

pub(super) const READ_STATEMENT_DIGEST_DOMAIN: &[u8] =
    b"skein-production-content-store-statement-v1";
const MUTATION_STATEMENT_DIGEST_DOMAIN: &[u8] =
    b"skein-production-content-store-mutation-statement-v1";

#[derive(Clone, Copy)]
pub(super) enum StatementRole {
    Read,
    Insert,
    Update,
}

pub(super) fn valid_statement_contract(
    evidence: &Value,
    corpus: &ContentStoreSqlCorpus,
    role: StatementRole,
    digest_domain: &[u8],
) -> bool {
    let Some(statement_name) = string(evidence, "/statement_name") else {
        return false;
    };
    let Some(statement) = corpus.statement(statement_name) else {
        return false;
    };
    if string(evidence, "/statement_sha256")
        != Some(statement_digest(digest_domain, &statement.sql).as_str())
    {
        return false;
    }
    match role {
        StatementRole::Read => statement.kind == ContentStoreSqlStatementKind::Read,
        StatementRole::Insert | StatementRole::Update => {
            if statement.kind != ContentStoreSqlStatementKind::Mutation {
                return false;
            }
            let Ok(lowered) = skein::sql::parse_postgres_sql(&statement.sql) else {
                return false;
            };
            matches!(
                (role, lowered),
                (StatementRole::Insert, skein::sql::SqlStatement::Insert(_))
                    | (StatementRole::Update, skein::sql::SqlStatement::Update(_))
            )
        }
    }
}

fn statement_digest(domain: &[u8], sql: &str) -> String {
    let mut hasher = Sha256::new();
    hash_bytes(&mut hasher, domain);
    hash_bytes(&mut hasher, sql.as_bytes());
    format!("sha256:{:x}", hasher.finalize())
}

fn regression_per_million(observed: u64, reference: u64) -> u64 {
    if observed <= reference || reference == 0 {
        0
    } else {
        observed
            .saturating_sub(reference)
            .saturating_mul(1_000_000)
            .checked_div(reference)
            .unwrap_or(u64::MAX)
    }
}

pub(super) fn string<'a>(value: &'a Value, pointer: &str) -> Option<&'a str> {
    value.pointer(pointer).and_then(Value::as_str)
}

pub(super) fn unsigned(value: &Value, pointer: &str) -> Option<u64> {
    value.pointer(pointer).and_then(Value::as_u64)
}

pub(super) fn boolean(value: &Value, pointer: &str) -> Option<bool> {
    value.pointer(pointer).and_then(Value::as_bool)
}

fn sum(left: Option<u64>, right: Option<u64>) -> u64 {
    left.unwrap_or(u64::MAX)
        .saturating_add(right.unwrap_or(u64::MAX))
}

pub(super) fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(super) fn valid_prefixed_sha256(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(valid_sha256)
}

pub(super) fn deduplicate(mut blockers: Vec<String>) -> Vec<String> {
    blockers.sort();
    blockers.dedup();
    blockers
}
