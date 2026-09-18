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

use super::content_store::{
    boolean, deduplicate, string, unsigned, valid_prefixed_sha256, valid_sha256,
    valid_statement_contract, validate_frozen_contract, StatementRole,
    READ_STATEMENT_DIGEST_DOMAIN,
};
use super::{validate_common_artifact, validate_exact_binding};
use crate::{
    CONTENT_STORE_512_MIB_CAPABILITY_BYTES, CONTENT_STORE_SHARED_HOST_8_GIB_BYTES,
    CONTENT_STORE_SHARED_HOST_MAX_CAPACITY_BYTES,
    PRODUCTION_CONTENT_STORE_OVERFLOW_COMPACTION_QUALIFICATION_PROTOCOL,
};
use hawdb::ProductionQualificationIdentity;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

const EVIDENCE_KIND: &str = "representative_production_relational_overflow_compaction";

#[derive(Clone, Copy)]
enum RequiredProfile {
    Capability512Mib,
    SharedHost8Gib,
}

pub(super) fn validate_512_mib_overflow_compaction(
    artifact: &Value,
    expected: &ProductionQualificationIdentity,
) -> Vec<String> {
    validate(artifact, expected, RequiredProfile::Capability512Mib)
}

pub(super) fn validate_shared_host_overflow_compaction(
    artifact: &Value,
    expected: &ProductionQualificationIdentity,
) -> Vec<String> {
    validate(artifact, expected, RequiredProfile::SharedHost8Gib)
}

fn validate(
    artifact: &Value,
    expected: &ProductionQualificationIdentity,
    required_profile: RequiredProfile,
) -> Vec<String> {
    let mut blockers = Vec::new();
    validate_common_artifact(
        artifact,
        PRODUCTION_CONTENT_STORE_OVERFLOW_COMPACTION_QUALIFICATION_PROTOCOL,
        EVIDENCE_KIND,
        &mut blockers,
    );
    validate_exact_binding(artifact, "/evidence_binding", expected, &mut blockers);
    let corpus = validate_frozen_contract(artifact, &mut blockers);
    let contracts = validate_contracts(artifact, corpus.as_ref(), &mut blockers);
    validate_profile(artifact, required_profile, &mut blockers);

    let initial = artifact.pointer("/initial_residency");
    let compacted = artifact.pointer("/compacted_residency");
    let final_residency = artifact.pointer("/final_residency");
    let reopened = artifact.pointer("/reopened_residency");
    match (initial, compacted, final_residency, reopened) {
        (Some(initial), Some(compacted), Some(final_residency), Some(reopened)) => {
            validate_residency(
                initial,
                expected.canonical_graph_commit_epoch,
                &mut blockers,
            );
            validate_initial_residency(initial, &mut blockers);
            validate_residency(
                compacted,
                expected.canonical_graph_commit_epoch,
                &mut blockers,
            );
            validate_compacted_residency(compacted, &mut blockers);
            let cleanup_epoch = expected.canonical_graph_commit_epoch.saturating_add(1);
            validate_residency(final_residency, cleanup_epoch, &mut blockers);
            validate_residency(reopened, cleanup_epoch, &mut blockers);
            validate_residency_transitions(
                artifact,
                initial,
                compacted,
                final_residency,
                reopened,
                &mut blockers,
            );
            validate_reads(
                artifact,
                "/before_reads",
                "production_cold",
                initial,
                &contracts,
                &mut blockers,
            );
            validate_reads(
                artifact,
                "/compacted_reads",
                "production_warm",
                compacted,
                &contracts,
                &mut blockers,
            );
            validate_reads(
                artifact,
                "/reopened_reads",
                "wal_recovery",
                reopened,
                &contracts,
                &mut blockers,
            );
        }
        _ => blockers.push("overflow_compaction_residency_missing".to_string()),
    }

    validate_compaction(artifact, &mut blockers);
    validate_artifacts(artifact, compacted, &mut blockers);
    validate_process(artifact, &mut blockers);
    validate_governor(artifact, required_profile, &contracts, &mut blockers);
    validate_reclamation_and_scrub(artifact, final_residency, &mut blockers);
    deduplicate(blockers)
}

fn validate_contracts<'a>(
    artifact: &'a Value,
    corpus: Option<&crate::ContentStoreSqlCorpus>,
    blockers: &mut Vec<String>,
) -> BTreeMap<&'a str, &'a Value> {
    let Some(contracts) = artifact
        .pointer("/read_contracts")
        .and_then(Value::as_array)
    else {
        blockers.push("overflow_compaction_read_contracts_missing".to_string());
        return BTreeMap::new();
    };
    if contracts.is_empty() {
        blockers.push("overflow_compaction_read_contracts_missing".to_string());
    }
    let mut by_name = BTreeMap::new();
    for contract in contracts {
        let Some(case_name) = string(contract, "/case_name") else {
            blockers.push("overflow_compaction_read_contract_invalid".to_string());
            continue;
        };
        if case_name.is_empty() || by_name.insert(case_name, contract).is_some() {
            blockers.push("overflow_compaction_read_contract_duplicate".to_string());
        }
        if corpus.is_none_or(|corpus| {
            !valid_statement_contract(
                contract,
                corpus,
                StatementRole::Read,
                READ_STATEMENT_DIGEST_DOMAIN,
            )
        }) || !valid_prefixed_sha256(string(contract, "/parameter_sha256").unwrap_or_default())
            || !valid_sha256(string(contract, "/expected_output_sha256").unwrap_or_default())
        {
            blockers.push("overflow_compaction_read_contract_digest_invalid".to_string());
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
            blockers.push("overflow_compaction_read_contract_budget_invalid".to_string());
        }
        if corpus.is_none_or(|corpus| {
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
            blockers.push("overflow_compaction_frozen_statement_budget_mismatch".to_string());
        }
    }
    by_name
}

fn validate_profile(artifact: &Value, required: RequiredProfile, blockers: &mut Vec<String>) {
    let configured = unsigned(artifact, "/configured_available_memory_bytes").unwrap_or_default();
    let steady =
        unsigned(artifact, "/limits/process/max_steady_resident_bytes").unwrap_or_default();
    let peak = unsigned(artifact, "/limits/process/max_peak_resident_bytes").unwrap_or_default();
    if steady == 0 || peak == 0 || steady > peak || peak > configured {
        blockers.push("overflow_compaction_memory_limits_invalid".to_string());
    }
    match required {
        RequiredProfile::Capability512Mib => {
            if string(artifact, "/resource_profile_kind") != Some("capability512_mib")
                || configured != CONTENT_STORE_512_MIB_CAPABILITY_BYTES
                || peak > CONTENT_STORE_512_MIB_CAPABILITY_BYTES
            {
                blockers.push("overflow_compaction_512_mib_profile_invalid".to_string());
            }
        }
        RequiredProfile::SharedHost8Gib => {
            if string(artifact, "/resource_profile_kind") != Some("shared_host8_gib")
                || configured != CONTENT_STORE_SHARED_HOST_8_GIB_BYTES
                || peak > CONTENT_STORE_SHARED_HOST_MAX_CAPACITY_BYTES
            {
                blockers.push("overflow_compaction_shared_host_profile_invalid".to_string());
            }
        }
    }
}

fn validate_residency(residency: &Value, epoch: u64, blockers: &mut Vec<String>) {
    if boolean(residency, "/row_serving") != Some(true)
        || boolean(residency, "/index_serving") != Some(true)
        || boolean(residency, "/row_index_epoch_aligned") != Some(true)
        || boolean(residency, "/row_materialized_rows_resident") != Some(false)
        || boolean(residency, "/row_checkpoint_state_metadata_only") != Some(true)
    {
        blockers.push("overflow_compaction_residency_state_invalid".to_string());
    }
    if unsigned(residency, "/database_commit_epoch") != Some(epoch)
        || unsigned(residency, "/row_visible_commit_epoch") != Some(epoch)
        || unsigned(residency, "/index_visible_commit_epoch") != Some(epoch)
        || unsigned(residency, "/row_base_generation")
            != unsigned(residency, "/index_base_generation")
        || unsigned(residency, "/row_base_commit_epoch")
            != unsigned(residency, "/index_base_commit_epoch")
    {
        blockers.push("overflow_compaction_residency_identity_invalid".to_string());
    }
    let cache = unsigned(residency, "/segment_cache_capacity_bytes").unwrap_or_default();
    if cache == 0
        || unsigned(residency, "/row_overflow_extent_count").unwrap_or_default() == 0
        || unsigned(residency, "/row_overflow_descriptor_artifact_bytes").unwrap_or_default() == 0
        || unsigned(residency, "/segment_cache_resident_bytes").unwrap_or(u64::MAX) > cache
        || unsigned(residency, "/segment_cache_pinned_bytes") != Some(0)
    {
        blockers.push("overflow_compaction_residency_cache_invalid".to_string());
    }
}

fn validate_initial_residency(residency: &Value, blockers: &mut Vec<String>) {
    let cache = unsigned(residency, "/segment_cache_capacity_bytes").unwrap_or_default();
    let row_bytes = unsigned(residency, "/row_canonical_artifact_bytes").unwrap_or_default();
    if row_bytes <= cache
        || boolean(residency, "/row_artifact_exceeds_cache") != Some(true)
        || unsigned(residency, "/row_overflow_extent_artifact_bytes").unwrap_or_default() == 0
    {
        blockers.push("overflow_compaction_initial_artifact_invalid".to_string());
    }
}

fn validate_compacted_residency(residency: &Value, blockers: &mut Vec<String>) {
    if unsigned(residency, "/row_overflow_extent_artifact_bytes").unwrap_or_default() == 0 {
        blockers.push("overflow_compaction_published_artifact_missing".to_string());
    }
}

fn validate_residency_transitions(
    artifact: &Value,
    initial: &Value,
    compacted: &Value,
    final_residency: &Value,
    reopened: &Value,
    blockers: &mut Vec<String>,
) {
    if unsigned(artifact, "/compaction/source_commit_epoch")
        != unsigned(initial, "/database_commit_epoch")
        || unsigned(artifact, "/compaction/published_generation")
            != unsigned(compacted, "/row_base_generation")
        || unsigned(final_residency, "/row_base_generation")
            != unsigned(compacted, "/row_base_generation")
                .map(|generation| generation.saturating_add(1))
        || !same_storage_identity(final_residency, reopened)
    {
        blockers.push("overflow_compaction_published_identity_mismatch".to_string());
    }
}

fn same_storage_identity(left: &Value, right: &Value) -> bool {
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

fn validate_reads(
    artifact: &Value,
    pointer: &str,
    phase: &str,
    residency: &Value,
    contracts: &BTreeMap<&str, &Value>,
    blockers: &mut Vec<String>,
) {
    let Some(reads) = artifact.pointer(pointer).and_then(Value::as_array) else {
        blockers.push("overflow_compaction_verification_reads_missing".to_string());
        return;
    };
    let mut observed = BTreeSet::new();
    let mut authoritative_index_observed = false;
    for evidence in reads {
        let Some(case_name) = string(evidence, "/case_name") else {
            blockers.push("overflow_compaction_verification_identity_invalid".to_string());
            continue;
        };
        let Some(contract) = contracts.get(case_name).copied() else {
            blockers.push("overflow_compaction_verification_identity_invalid".to_string());
            continue;
        };
        if !observed.insert(case_name) {
            blockers.push("overflow_compaction_verification_identity_invalid".to_string());
        }
        for (observed_pointer, contract_pointer) in [
            ("/statement_sha256", "/statement_sha256"),
            ("/parameter_sha256", "/parameter_sha256"),
            ("/expected_output_sha256", "/expected_output_sha256"),
            ("/read/statement_name", "/statement_name"),
            ("/read/output_rows", "/expected_output_rows"),
            ("/read/output_sha256", "/expected_output_sha256"),
            ("/read/max_rows", "/max_output_rows"),
            ("/read/max_payload_bytes", "/max_output_payload_bytes"),
        ] {
            if evidence.pointer(observed_pointer) != contract.pointer(contract_pointer) {
                blockers.push("overflow_compaction_verification_contract_mismatch".to_string());
            }
        }
        if string(evidence, "/read/phase") != Some(phase) {
            blockers.push("overflow_compaction_verification_phase_invalid".to_string());
        }
        if unsigned(evidence, "/read/output_payload_bytes").unwrap_or(u64::MAX)
            > unsigned(contract, "/max_output_payload_bytes").unwrap_or_default()
            || unsigned(evidence, "/read/execution/intermediate_rows").unwrap_or(u64::MAX)
                > unsigned(contract, "/max_intermediate_rows").unwrap_or_default()
            || io_sum(
                evidence,
                "/read/execution/physical_pages",
                "/read/execution/index_physical_pages",
            ) > unsigned(contract, "/max_physical_pages_per_run").unwrap_or_default()
            || io_sum(
                evidence,
                "/read/execution/physical_bytes",
                "/read/execution/index_physical_bytes",
            ) > unsigned(contract, "/max_physical_bytes_per_run").unwrap_or_default()
        {
            blockers.push("overflow_compaction_verification_budget_exceeded".to_string());
        }
        if unsigned(evidence, "/read/execution/base_generation")
            != unsigned(residency, "/row_base_generation")
            || unsigned(evidence, "/read/execution/base_commit_epoch")
                != unsigned(residency, "/row_base_commit_epoch")
            || unsigned(evidence, "/read/execution/visible_commit_epoch")
                != unsigned(residency, "/row_visible_commit_epoch")
        {
            blockers.push("overflow_compaction_verification_generation_mismatch".to_string());
        }
        if unsigned(evidence, "/read/execution/cache_admission_rejections") != Some(0)
            || unsigned(evidence, "/read/execution/index_cache_admission_rejections") != Some(0)
            || unsigned(evidence, "/read/cache/admission_rejections") != Some(0)
            || unsigned(evidence, "/read/cache/pinned_bytes_after") != Some(0)
            || unsigned(evidence, "/read/cache/resident_bytes_after").unwrap_or(u64::MAX)
                > unsigned(residency, "/segment_cache_capacity_bytes").unwrap_or_default()
            || string(evidence, "/read/execution/row_runtime_path") != Some("snapshot_rows")
        {
            blockers.push("overflow_compaction_verification_cache_invalid".to_string());
        }
        authoritative_index_observed |=
            string(evidence, "/read/execution/index_runtime_path") == Some("authoritative");
    }
    if observed.len() != contracts.len() || reads.len() != contracts.len() {
        blockers.push("overflow_compaction_verification_count_mismatch".to_string());
    }
    if !authoritative_index_observed {
        blockers.push("overflow_compaction_authoritative_index_missing".to_string());
    }
}

fn validate_compaction(artifact: &Value, blockers: &mut Vec<String>) {
    let policy = artifact
        .pointer("/compaction_policy")
        .unwrap_or(&Value::Null);
    let compaction = artifact.pointer("/compaction").unwrap_or(&Value::Null);
    for pointer in [
        "/max_scan_rows",
        "/max_scan_pages",
        "/max_scan_bytes",
        "/max_overlay_entries",
        "/max_overlay_bytes",
        "/max_rewrite_bytes",
        "/max_sort_memory_bytes",
        "/max_spill_bytes",
        "/max_spill_runs",
        "/max_reference_occurrences",
        "/admission_bytes",
    ] {
        if unsigned(policy, pointer).unwrap_or_default() == 0 {
            blockers.push("overflow_compaction_policy_invalid".to_string());
        }
    }
    for (observed, limit) in [
        ("/rows_scanned", "/max_scan_rows"),
        ("/pages_read", "/max_scan_pages"),
        ("/row_bytes_read", "/max_scan_bytes"),
        ("/overlay_entries", "/max_overlay_entries"),
        ("/overlay_bytes", "/max_overlay_bytes"),
        ("/reference_occurrences", "/max_reference_occurrences"),
        ("/spill_run_count", "/max_spill_runs"),
        ("/spill_bytes", "/max_spill_bytes"),
        ("/peak_sort_memory_bytes", "/max_sort_memory_bytes"),
    ] {
        if unsigned(compaction, observed).unwrap_or(u64::MAX)
            > unsigned(policy, limit).unwrap_or_default()
        {
            blockers.push("overflow_compaction_internal_budget_invalid".to_string());
        }
    }
    if unsigned(compaction, "/hydrated_values") != Some(0)
        || unsigned(compaction, "/reclaimable_base_extent_count").unwrap_or_default()
            < unsigned(artifact, "/limits/min_reclaimable_base_extent_count").unwrap_or(u64::MAX)
        || unsigned(compaction, "/reused_extent_count") != Some(0)
        || unsigned(compaction, "/new_extent_count")
            != unsigned(compaction, "/published_extent_count")
        || unsigned(compaction, "/copied_base_extent_count")
            .unwrap_or(u64::MAX)
            .saturating_add(unsigned(compaction, "/introduced_extent_count").unwrap_or(u64::MAX))
            != unsigned(compaction, "/new_extent_count").unwrap_or_default()
        || unsigned(compaction, "/admitted_memory_bytes") != unsigned(policy, "/admission_bytes")
    {
        blockers.push("overflow_compaction_rewrite_shape_invalid".to_string());
    }
    if unsigned(compaction, "/tables_scanned").unwrap_or_default() == 0
        || unsigned(compaction, "/previous_extent_count").unwrap_or_default() == 0
        || unsigned(compaction, "/published_extent_count").unwrap_or_default() == 0
        || unsigned(compaction, "/reclaimable_base_extent_count").unwrap_or(u64::MAX)
            > unsigned(compaction, "/previous_extent_count").unwrap_or_default()
    {
        blockers.push("overflow_compaction_inventory_invalid".to_string());
    }
    let governor_budget =
        unsigned(artifact, "/runtime_governor/memory_budget_bytes").unwrap_or_default();
    if unsigned(policy, "/admission_bytes").unwrap_or(u64::MAX) > governor_budget {
        blockers.push("overflow_compaction_admission_budget_invalid".to_string());
    }
}

fn validate_artifacts(artifact: &Value, compacted: Option<&Value>, blockers: &mut Vec<String>) {
    let evidence = artifact.pointer("/artifacts").unwrap_or(&Value::Null);
    let limits = artifact.pointer("/limits").unwrap_or(&Value::Null);
    for pointer in [
        "/files_before",
        "/files_after_compaction",
        "/files_after_cleanup",
    ] {
        if unsigned(evidence, pointer).unwrap_or_default() == 0 {
            blockers.push("overflow_compaction_artifact_inventory_invalid".to_string());
        }
    }
    let new_bytes = unsigned(evidence, "/new_generation_artifact_bytes").unwrap_or(u64::MAX);
    let live_bytes =
        unsigned(evidence, "/published_live_overflow_extent_bytes").unwrap_or_default();
    let reported_write_amplification =
        unsigned(evidence, "/new_artifact_write_amplification_per_million").unwrap_or(u64::MAX);
    let recomputed = ratio_per_million(new_bytes, live_bytes);
    if new_bytes > unsigned(limits, "/max_new_generation_artifact_bytes").unwrap_or_default()
        || live_bytes == 0
        || live_bytes
            > unsigned(artifact, "/compaction_policy/max_rewrite_bytes").unwrap_or_default()
        || compacted.is_none_or(|residency| {
            unsigned(residency, "/row_overflow_extent_artifact_bytes") != Some(live_bytes)
        })
        || reported_write_amplification != recomputed
        || reported_write_amplification
            > unsigned(limits, "/max_new_artifact_write_amplification_per_million")
                .unwrap_or_default()
    {
        blockers.push("overflow_compaction_artifact_budget_invalid".to_string());
    }
    if unsigned(evidence, "/physically_removed_extent_files").unwrap_or_default() == 0
        || unsigned(evidence, "/physically_removed_extent_bytes").unwrap_or_default()
            < unsigned(limits, "/min_physically_removed_extent_bytes").unwrap_or(u64::MAX)
    {
        blockers.push("overflow_compaction_physical_reclamation_invalid".to_string());
    }
}

fn validate_process(artifact: &Value, blockers: &mut Vec<String>) {
    let process = artifact
        .pointer("/compaction_process")
        .unwrap_or(&Value::Null);
    let limits = artifact.pointer("/limits/process").unwrap_or(&Value::Null);
    if boolean(process, "/resident_memory_supported") != Some(true)
        || unsigned(process, "/steady_resident_bytes").unwrap_or(u64::MAX)
            > unsigned(limits, "/max_steady_resident_bytes").unwrap_or_default()
        || unsigned(process, "/peak_resident_bytes").unwrap_or(u64::MAX)
            > unsigned(limits, "/max_peak_resident_bytes").unwrap_or_default()
    {
        blockers.push("overflow_compaction_process_memory_invalid".to_string());
    }
    for (support, observed, limit) in [
        (
            "/total_page_faults_supported",
            "/total_page_faults",
            "/max_total_page_faults_per_run",
        ),
        (
            "/split_page_faults_supported",
            "/minor_page_faults",
            "/max_minor_page_faults_per_run",
        ),
        (
            "/split_page_faults_supported",
            "/major_page_faults",
            "/max_major_page_faults_per_run",
        ),
    ] {
        let Some(limit_value) = unsigned(limits, limit) else {
            continue;
        };
        if boolean(process, support) != Some(true)
            || unsigned(process, observed).is_none_or(|value| value > limit_value)
        {
            blockers.push("overflow_compaction_page_fault_evidence_invalid".to_string());
        }
    }
    if unsigned(artifact, "/compaction_elapsed_micros").unwrap_or(u64::MAX)
        > unsigned(artifact, "/limits/max_compaction_elapsed_micros").unwrap_or_default()
        || unsigned(artifact, "/cleanup_elapsed_micros").unwrap_or(u64::MAX)
            > unsigned(artifact, "/limits/max_cleanup_elapsed_micros").unwrap_or_default()
    {
        blockers.push("overflow_compaction_elapsed_budget_invalid".to_string());
    }
}

fn validate_governor(
    artifact: &Value,
    profile: RequiredProfile,
    contracts: &BTreeMap<&str, &Value>,
    blockers: &mut Vec<String>,
) {
    let governor = artifact
        .pointer("/runtime_governor")
        .unwrap_or(&Value::Null);
    let expected_admissions = u64::try_from(contracts.len())
        .unwrap_or(u64::MAX)
        .saturating_mul(3)
        .saturating_add(1);
    if unsigned(governor, "/admissions_delta") != Some(expected_admissions)
        || unsigned(governor, "/completions_delta") != Some(expected_admissions)
        || unsigned(governor, "/admission_rejections_delta") != Some(0)
        || boolean(governor, "/final_overcommitted") != Some(false)
    {
        blockers.push("overflow_compaction_governor_accounting_invalid".to_string());
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
            blockers.push("overflow_compaction_governor_permit_leak".to_string());
        }
    }
    let capacity = unsigned(governor, "/memory_capacity_bytes").unwrap_or_default();
    let budget = unsigned(governor, "/memory_budget_bytes").unwrap_or_default();
    let result_budget = unsigned(governor, "/result_budget_bytes").unwrap_or_default();
    if capacity == 0
        || budget == 0
        || result_budget == 0
        || budget > capacity
        || result_budget > budget
        || contracts.values().any(|contract| {
            unsigned(contract, "/max_output_payload_bytes").unwrap_or(u64::MAX) > result_budget
        })
    {
        blockers.push("overflow_compaction_governor_memory_invalid".to_string());
    }
    match profile {
        RequiredProfile::Capability512Mib => {
            if unsigned(governor, "/configured_memory_ceiling_bytes")
                != Some(CONTENT_STORE_512_MIB_CAPABILITY_BYTES)
                || capacity > CONTENT_STORE_512_MIB_CAPABILITY_BYTES
                || budget > CONTENT_STORE_512_MIB_CAPABILITY_BYTES
            {
                blockers.push("overflow_compaction_512_mib_governor_invalid".to_string());
            }
        }
        RequiredProfile::SharedHost8Gib => {
            if unsigned(governor, "/effective_memory_limit_bytes")
                != Some(CONTENT_STORE_SHARED_HOST_8_GIB_BYTES)
                || !governor
                    .pointer("/configured_memory_ceiling_bytes")
                    .is_some_and(Value::is_null)
                || capacity > CONTENT_STORE_SHARED_HOST_MAX_CAPACITY_BYTES
                || budget > CONTENT_STORE_SHARED_HOST_MAX_CAPACITY_BYTES
            {
                blockers.push("overflow_compaction_shared_host_governor_invalid".to_string());
            }
        }
    }
}

fn validate_reclamation_and_scrub(
    artifact: &Value,
    final_residency: Option<&Value>,
    blockers: &mut Vec<String>,
) {
    let reclamation = artifact.pointer("/reclamation").unwrap_or(&Value::Null);
    if boolean(reclamation, "/durable") != Some(true)
        || !reclamation
            .pointer("/oldest_reader_commit_epoch")
            .is_some_and(Value::is_null)
        || unsigned(reclamation, "/safe_reclaim_commit_epoch").unwrap_or_default()
            < unsigned(artifact, "/initial_residency/database_commit_epoch").unwrap_or(u64::MAX)
    {
        blockers.push("overflow_compaction_reclamation_watermark_invalid".to_string());
    }
    let scrub = artifact.pointer("/scrub").unwrap_or(&Value::Null);
    if final_residency.is_none_or(|residency| {
        unsigned(scrub, "/generation") != unsigned(residency, "/row_base_generation")
    }) || unsigned(scrub, "/checked_file_count").unwrap_or_default() == 0
        || unsigned(scrub, "/checked_bytes").unwrap_or_default() == 0
    {
        blockers.push("overflow_compaction_scrub_evidence_invalid".to_string());
    }
}

fn io_sum(value: &Value, left: &str, right: &str) -> u64 {
    unsigned(value, left)
        .unwrap_or(u64::MAX)
        .saturating_add(unsigned(value, right).unwrap_or(u64::MAX))
}

fn ratio_per_million(numerator: u64, denominator: u64) -> u64 {
    u64::try_from(u128::from(numerator).saturating_mul(1_000_000) / u128::from(denominator.max(1)))
        .unwrap_or(u64::MAX)
}
