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
use std::sync::atomic::{AtomicU64, Ordering};

static TEST_ID: AtomicU64 = AtomicU64::new(0);

#[test]
fn initial_content_store_tables_are_qualified_through_canonical_row_pages() {
    let id = TEST_ID.fetch_add(1, Ordering::SeqCst);
    let path = std::env::temp_dir().join(format!(
        "hawdb-content-store-row-page-qualification-{}-{id}",
        std::process::id()
    ));
    let mut config =
        ContentStoreInitialRowPageQualificationConfig::synthetic(&path, "test-source-revision");
    config.base_message_count = 4;
    config.message_payload_bytes = 16 * 1024;
    config.base_chunk_count = 5;
    config.chunk_payload_bytes = 12 * 1024;

    let report = run_content_store_initial_row_page_qualification(config)
        .expect("initial Content Store row pages should qualify");

    assert!(report.ready);
    assert_eq!(report.qualified_tables, QUALIFIED_TABLES);
    assert_eq!(report.corpus.partial_caller_count, 0);
    assert_eq!(report.final_message_count, 7);
    assert_eq!(report.base_chunk_count, 5);
    assert_eq!(report.final_chunk_count, 2);
    assert_eq!(report.cold_checkpoint_reads.len(), 9);
    assert_eq!(report.warm_checkpoint_reads.len(), 9);
    assert!(report.wal_replayed_entries > 0);
    assert!(report
        .wal_recovery_read
        .execution
        .delta_generation
        .is_some());
    assert_eq!(
        report.wal_recovery_read.execution.visible_commit_epoch,
        report.wal_content_commit_epoch
    );
    assert!(report
        .wal_recovery_chunk_read
        .execution
        .delta_generation
        .is_some());
    assert!(report
        .wal_recovery_anchor_read
        .execution
        .delta_generation
        .is_some());
    assert_eq!(
        report
            .wal_recovery_chunk_read
            .execution
            .visible_commit_epoch,
        report.wal_content_commit_epoch
    );
    assert_eq!(
        report
            .wal_recovery_anchor_read
            .execution
            .visible_commit_epoch,
        report.wal_content_commit_epoch
    );
    assert!(report.live_overlay_read.execution.overlay_entries > 0);
    assert_eq!(
        report.live_overlay_read.execution.visible_commit_epoch,
        report.live_content_commit_epoch
    );
    assert!(report.live_overlay_chunk_read.execution.overlay_entries > 0);
    assert!(report.live_overlay_anchor_read.execution.overlay_entries > 0);
    assert_eq!(
        report
            .live_overlay_chunk_read
            .execution
            .visible_commit_epoch,
        report.live_content_commit_epoch
    );
    assert_eq!(
        report
            .live_overlay_anchor_read
            .execution
            .visible_commit_epoch,
        report.live_content_commit_epoch
    );
    assert_eq!(report.source_chunk_replacement.initial_chunk_count, 7);
    assert!(report.source_chunk_replacement.stale_suffix_removed);
    assert!(report.source_chunk_replacement.chunk_fields_preserved);
    assert!(report.source_chunk_replacement.duplicate_order_rejected);
    assert!(report.source_chunk_replacement.rejected_statement_atomic);
    assert_eq!(
        report
            .source_chunk_replacement
            .shorter_replacement
            .replacement_chunk_count,
        2
    );
    assert_eq!(
        report
            .source_chunk_replacement
            .shorter_replacement
            .summary_item_count,
        2
    );
    assert!(
        report
            .source_chunk_replacement
            .shorter_replacement
            .summary_size_bytes
            > 0
    );
    assert_eq!(
        report
            .source_chunk_replacement
            .shorter_replacement
            .live_read
            .execution
            .visible_commit_epoch,
        report
            .source_chunk_replacement
            .shorter_replacement
            .committed_epoch
    );
    assert_eq!(
        report
            .source_chunk_replacement
            .shorter_replacement
            .live_read
            .output_sha256,
        report
            .source_chunk_replacement
            .shorter_replacement
            .reopened_read
            .output_sha256
    );
    assert_eq!(
        report
            .source_chunk_replacement
            .empty_replacement
            .replacement_chunk_count,
        0
    );
    assert_eq!(
        report
            .source_chunk_replacement
            .empty_replacement
            .summary_item_count,
        0
    );
    assert_eq!(
        report
            .source_chunk_replacement
            .empty_replacement
            .summary_size_bytes,
        0
    );
    assert_eq!(
        report
            .source_chunk_replacement
            .empty_replacement
            .live_read
            .output_sha256,
        report
            .source_chunk_replacement
            .empty_replacement
            .reopened_read
            .output_sha256
    );
    assert!(
        report
            .source_chunk_replacement
            .empty_replacement
            .checkpoint_generation
            > report
                .source_chunk_replacement
                .shorter_replacement
                .checkpoint_generation
    );
    assert_eq!(report.source_chunk_replacement.final_chunk_count, 0);
    assert_eq!(report.source_ownership_move.previous_space_id, "default");
    assert_eq!(report.source_ownership_move.target_space_id, "work");
    assert_eq!(report.source_ownership_move.chunk_count, 2);
    assert_eq!(report.source_ownership_move.missing_owner_count, 0);
    assert!(report.source_ownership_move.missing_owner_epoch_unchanged);
    assert!(report.source_ownership_move.payload_fields_preserved);
    assert_eq!(
        report.source_ownership_move.payload_sha256_before,
        report.source_ownership_move.payload_sha256_after_live
    );
    assert_eq!(
        report.source_ownership_move.payload_sha256_before,
        report.source_ownership_move.payload_sha256_after_reopen
    );
    assert_eq!(
        report
            .source_ownership_move
            .live_read
            .execution
            .visible_commit_epoch,
        report.source_ownership_move.committed_epoch
    );
    assert!(
        report
            .source_ownership_move
            .live_read
            .execution
            .overlay_entries
            > 0
    );
    assert_eq!(
        report.source_ownership_move.live_read.output_sha256,
        report.source_ownership_move.reopened_read.output_sha256
    );
    assert!(
        report.source_ownership_move.checkpoint_generation
            > report
                .source_chunk_replacement
                .empty_replacement
                .checkpoint_generation
    );
    assert_eq!(report.thread_ownership_move.requested_moves, 3);
    assert_eq!(report.thread_ownership_move.documents_updated, 2);
    assert_eq!(report.thread_ownership_move.messages_updated, 2);
    assert!(report.thread_ownership_move.document_owner_uses_storage_id);
    assert!(report.thread_ownership_move.stale_guard_preserved);
    assert!(report.thread_ownership_move.graph_relational_agreement);
    assert!(report.thread_ownership_move.payload_fields_preserved);
    assert_eq!(report.thread_ownership_move.live_reads.len(), 3);
    assert_eq!(report.thread_ownership_move.reopened_reads.len(), 3);
    assert!(report
        .thread_ownership_move
        .live_reads
        .iter()
        .all(|case| case.read.execution.visible_commit_epoch
            == report.thread_ownership_move.committed_epoch));
    assert!(report
        .thread_ownership_move
        .live_reads
        .iter()
        .all(|case| case.read.execution.overlay_entries > 0));
    assert!(report
        .thread_ownership_move
        .live_reads
        .iter()
        .zip(&report.thread_ownership_move.reopened_reads)
        .all(|(live, reopened)| live.case_name == reopened.case_name
            && live.expected_space_id == reopened.expected_space_id
            && live.read.output_sha256 == reopened.read.output_sha256));
    assert_eq!(
        report
            .thread_ownership_move
            .live_reads
            .iter()
            .map(|case| (case.case_name.as_str(), case.expected_space_id.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("matched_default", "work"),
            ("matched_archive", "work"),
            ("stale_preview", "stale-current"),
        ]
    );
    assert_eq!(
        report.thread_ownership_move.payload_sha256_before,
        report.thread_ownership_move.payload_sha256_after_live
    );
    assert_eq!(
        report.thread_ownership_move.payload_sha256_before,
        report.thread_ownership_move.payload_sha256_after_reopen
    );
    assert_eq!(report.space_merge_ownership.requested_threads, 3);
    assert_eq!(report.space_merge_ownership.requested_sources, 1);
    assert_eq!(report.space_merge_ownership.documents_updated, 3);
    assert_eq!(report.space_merge_ownership.messages_updated, 2);
    assert!(report.space_merge_ownership.document_owner_uses_storage_id);
    assert!(report.space_merge_ownership.stale_guard_preserved);
    assert!(report.space_merge_ownership.graph_relational_agreement);
    assert!(report.space_merge_ownership.payload_fields_preserved);
    assert_eq!(report.space_merge_ownership.live_reads.len(), 4);
    assert_eq!(report.space_merge_ownership.reopened_reads.len(), 4);
    assert!(report
        .space_merge_ownership
        .live_reads
        .iter()
        .all(|case| case.read.execution.visible_commit_epoch
            == report.space_merge_ownership.committed_epoch));
    assert!(report
        .space_merge_ownership
        .live_reads
        .iter()
        .filter(|case| case.case_name != "thread_stale_preview")
        .all(|case| case.read.execution.overlay_entries > 0));
    assert!(report
        .space_merge_ownership
        .live_reads
        .iter()
        .zip(&report.space_merge_ownership.reopened_reads)
        .all(|(live, reopened)| live.case_name == reopened.case_name
            && live.expected_space_id == reopened.expected_space_id
            && live.read.output_sha256 == reopened.read.output_sha256));
    assert_eq!(
        report.space_merge_ownership.payload_sha256_before,
        report.space_merge_ownership.payload_sha256_after_live
    );
    assert_eq!(
        report.space_merge_ownership.payload_sha256_before,
        report.space_merge_ownership.payload_sha256_after_reopen
    );
    assert_eq!(
        report
            .space_merge_ownership
            .live_reads
            .iter()
            .map(|case| (case.case_name.as_str(), case.expected_space_id.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("thread_matched_default", "merged"),
            ("thread_matched_archive", "merged"),
            ("thread_stale_preview", "stale-current"),
            ("source", "merged"),
        ]
    );
    assert_eq!(report.thread_message_upsert.message_count, 2);
    assert_ne!(
        report.thread_message_upsert.thread_id,
        report.thread_message_upsert.thread_storage_id
    );
    assert!(report.thread_message_upsert.document_owner_uses_storage_id);
    assert!(report.thread_message_upsert.message_public_id_preserved);
    assert!(
        report
            .thread_message_upsert
            .document_created_at_preserved_on_conflict
    );
    assert!(
        report
            .thread_message_upsert
            .message_created_at_preserved_on_conflict
    );
    assert!(report.thread_message_upsert.rejected_statement_atomic);
    assert!(report.thread_message_upsert.graph_relational_agreement);
    assert_eq!(
        report
            .thread_message_upsert
            .live_read
            .execution
            .visible_commit_epoch,
        report.thread_message_upsert.committed_epoch
    );
    assert!(
        report
            .thread_message_upsert
            .live_read
            .execution
            .overlay_entries
            > 0
    );
    assert_eq!(
        report.thread_message_upsert.live_read.output_sha256,
        report.thread_message_upsert.reopened_read.output_sha256
    );
    assert_eq!(report.thread_message_reconcile.initial_message_count, 2);
    assert_eq!(report.thread_message_reconcile.final_message_count, 3);
    assert_ne!(
        report.thread_message_reconcile.thread_id,
        report.thread_message_reconcile.thread_storage_id
    );
    assert!(report.thread_message_reconcile.duplicate_mapping_rejected);
    assert!(report.thread_message_reconcile.incomplete_mapping_rejected);
    assert!(report.thread_message_reconcile.unknown_mapping_rejected);
    assert!(
        report
            .thread_message_reconcile
            .invalid_mapping_epoch_unchanged
    );
    assert!(report.thread_message_reconcile.explicit_anchor_reordered);
    assert!(report.thread_message_reconcile.legacy_anchor_reordered);
    assert!(
        report
            .thread_message_reconcile
            .occurrence_identity_preserved
    );
    assert_eq!(report.thread_message_reconcile.summary_item_count, 3);
    assert!(report.thread_message_reconcile.summary_size_bytes > 0);
    assert_eq!(
        report
            .thread_message_reconcile
            .preserved_message_payload_sha256_before,
        report
            .thread_message_reconcile
            .preserved_message_payload_sha256_after_live
    );
    assert_eq!(
        report
            .thread_message_reconcile
            .preserved_message_payload_sha256_before,
        report
            .thread_message_reconcile
            .preserved_message_payload_sha256_after_reopen
    );
    assert_eq!(
        report
            .thread_message_reconcile
            .preserved_anchor_payload_sha256_before,
        report
            .thread_message_reconcile
            .preserved_anchor_payload_sha256_after_live
    );
    assert_eq!(
        report
            .thread_message_reconcile
            .preserved_anchor_payload_sha256_before,
        report
            .thread_message_reconcile
            .preserved_anchor_payload_sha256_after_reopen
    );
    assert_eq!(
        report
            .thread_message_reconcile
            .live_read
            .execution
            .visible_commit_epoch,
        report.thread_message_reconcile.committed_epoch
    );
    assert!(
        report
            .thread_message_reconcile
            .live_read
            .execution
            .overlay_entries
            > 0
    );
    assert_eq!(
        report.thread_message_reconcile.live_read.output_sha256,
        report.thread_message_reconcile.reopened_read.output_sha256
    );
    assert_eq!(
        report.thread_message_reconcile.live_anchor_sha256,
        report.thread_message_reconcile.reopened_anchor_sha256
    );
    assert!(
        report.thread_message_reconcile.checkpoint_generation
            > report.thread_message_reconcile.seed_checkpoint_generation
    );
    assert_eq!(report.thread_tail_delete.thread_id, "qualified-tail-thread");
    assert_eq!(
        report.thread_tail_delete.thread_storage_id,
        "qualified-tail-storage"
    );
    assert_eq!(report.thread_tail_delete.start_index, 2);
    assert_eq!(report.thread_tail_delete.initial_message_count, 4);
    assert_eq!(report.thread_tail_delete.retained_message_count, 2);
    assert_eq!(report.thread_tail_delete.deleted_candidate_sha256.len(), 64);
    assert_eq!(
        report.thread_tail_delete.deleted_message_ids,
        [
            "qualified-tail-message-c".to_string(),
            "qualified-tail-message-d".to_string(),
        ]
    );
    assert_eq!(
        report.thread_tail_delete.deleted_occurrence_ids,
        [
            "qualified-tail-content-message-c".to_string(),
            "qualified-tail-content-message-d".to_string(),
        ]
    );
    assert!(report.thread_tail_delete.negative_start_clamped);
    assert!(report.thread_tail_delete.empty_tail_noop);
    assert!(report.thread_tail_delete.empty_tail_epoch_unchanged);
    assert!(report.thread_tail_delete.rollback_preserved_state);
    assert!(report.thread_tail_delete.graph_relational_agreement);
    assert!(report.thread_tail_delete.exact_summary);
    assert!(report.thread_tail_delete.retained_payload_identity);
    assert_eq!(report.thread_tail_delete.summary_item_count, 2);
    assert!(report.thread_tail_delete.summary_size_bytes > 0);
    assert_eq!(
        report.thread_tail_delete.committed_epoch,
        report.thread_tail_delete.seed_commit_epoch + 1
    );
    assert_eq!(
        report
            .thread_tail_delete
            .retained_message_payload_sha256_before,
        report
            .thread_tail_delete
            .retained_message_payload_sha256_after_live
    );
    assert_eq!(
        report
            .thread_tail_delete
            .retained_message_payload_sha256_before,
        report
            .thread_tail_delete
            .retained_message_payload_sha256_after_reopen
    );
    assert_eq!(
        report
            .thread_tail_delete
            .retained_anchor_payload_sha256_before,
        report
            .thread_tail_delete
            .retained_anchor_payload_sha256_after_live
    );
    assert_eq!(
        report
            .thread_tail_delete
            .retained_anchor_payload_sha256_before,
        report
            .thread_tail_delete
            .retained_anchor_payload_sha256_after_reopen
    );
    assert_eq!(
        report
            .thread_tail_delete
            .deleted_tombstone_read
            .execution
            .visible_commit_epoch,
        report.thread_tail_delete.committed_epoch
    );
    assert!(
        report
            .thread_tail_delete
            .deleted_tombstone_read
            .execution
            .overlay_entries
            > 0
    );
    assert_eq!(
        report.thread_tail_delete.deleted_tombstone_read.output_rows,
        0
    );
    assert_eq!(
        report.thread_tail_delete.live_read.output_sha256,
        report.thread_tail_delete.reopened_read.output_sha256
    );
    assert_eq!(
        report.thread_tail_delete.live_anchor_sha256,
        report.thread_tail_delete.reopened_anchor_sha256
    );
    assert!(
        report.thread_tail_delete.checkpoint_generation
            > report.thread_tail_delete.seed_checkpoint_generation
    );
    assert_eq!(report.thread_delete.thread_id, "qualified-delete-thread");
    assert_eq!(
        report.thread_delete.thread_storage_id,
        "qualified-delete-storage"
    );
    assert_eq!(
        report.thread_delete.requested_thread_id,
        "qualified-delete-alias"
    );
    assert_eq!(
        report.thread_delete.discovered_document_ids,
        [
            "qualified-delete-doc-legacy".to_string(),
            "qualified-delete-doc-owned".to_string(),
        ]
    );
    assert_eq!(report.thread_delete.discovered_document_sha256.len(), 64);
    assert!(report.thread_delete.empty_owned_document_discovered);
    assert_eq!(report.thread_delete.graph_deleted_message_count, 2);
    assert_eq!(report.thread_delete.relational_deleted_message_count, 3);
    assert_eq!(report.thread_delete.deleted_anchor_count, 2);
    assert_eq!(report.thread_delete.deleted_identity_count, 2);
    assert!(report.thread_delete.missing_thread_noop);
    assert!(report.thread_delete.rollback_preserved_state);
    assert!(report.thread_delete.workspace_read_your_own_writes);
    assert!(report.thread_delete.graph_relational_absent);
    assert!(report.thread_delete.unrelated_payload_identity);
    assert!(report.thread_delete.second_delete_noop);
    assert_eq!(
        report.thread_delete.committed_epoch,
        report.thread_delete.seed_commit_epoch + 1
    );
    assert_eq!(
        report.thread_delete.unrelated_state_sha256_before,
        report.thread_delete.unrelated_state_sha256_after_live
    );
    assert_eq!(
        report.thread_delete.unrelated_state_sha256_before,
        report.thread_delete.unrelated_state_sha256_after_reopen
    );
    assert_eq!(
        report
            .thread_delete
            .deleted_tombstone_read
            .execution
            .visible_commit_epoch,
        report.thread_delete.committed_epoch
    );
    assert_eq!(report.thread_delete.deleted_tombstone_read.output_rows, 0);
    assert!(
        report
            .thread_delete
            .deleted_tombstone_read
            .execution
            .overlay_entries
            > 0
    );
    assert_eq!(
        report.thread_delete.live_count_sha256,
        report.thread_delete.reopened_count_sha256
    );
    assert!(
        report.thread_delete.checkpoint_generation
            > report.thread_delete.seed_checkpoint_generation
    );
    assert_eq!(
        report.multi_statement_transaction.index_runtime_path,
        "transaction_workspace"
    );
    assert_eq!(
        report.multi_statement_transaction.row_runtime_path,
        "snapshot_rows"
    );
    assert!(report.multi_statement_transaction.rejected_statement_atomic);
    assert_eq!(
        report
            .multi_statement_transaction
            .canonical_fallback_lookups,
        0
    );
    assert!(
        report
            .multi_statement_transaction
            .transaction_workspace_lookups
            > 0
    );
    assert_eq!(
        report.multi_statement_transaction.page_output_rows,
        report.final_message_count
    );
    assert_eq!(report.multi_statement_transaction.summary_item_count, 7);
    assert!(report.multi_statement_transaction.summary_size_bytes > 0);
    assert!(
        report.multi_statement_transaction.committed_epoch
            > report.live_overlay_read.execution.visible_commit_epoch
    );
    assert!(report.isolation.cancellation_non_poisoning);
    assert_eq!(report.isolation.cancellation_pinned_bytes_after, 0);
    assert!(report.isolation.waiter_timed_out);
    assert!(report.isolation.waiter_aborted);
    assert!(report.isolation.owner_rollback_preserved_row);
    assert_eq!(
        report.isolation.commit_epoch_before,
        report.isolation.commit_epoch_after
    );
    assert_eq!(
        report.isolation.content_message_id,
        report
            .multi_statement_transaction
            .inserted_content_message_id
    );
    assert!(report.corruption.scrub_rejected);
    assert!(report.corruption.damaged_handle_poisoned);
    assert!(report.corruption.post_failure_sql_rejected);
    assert!(report.corruption.source_preserved);
    assert!(report.corruption.artifact_bytes > 0);
    assert!(report.corruption.bit_flip_offset < report.corruption.artifact_bytes);
    assert_eq!(
        report.resources.profile_kind,
        ContentStoreResourceProfileKind::Capability512Mib
    );
    assert_eq!(
        report.resources.configured_available_memory_bytes,
        CONTENT_STORE_512_MIB_CAPABILITY_BYTES
    );
    assert_eq!(report.resources.read_samples, 16);
    assert_eq!(
        report.resources.segment_cache_capacity_bytes,
        report.segment_cache_capacity_bytes
    );
    assert_eq!(
        report.resources.max_relational_index_read_bytes,
        16 * 1024 * 1024
    );
    assert_eq!(
        report.resources.max_relational_hydration_bytes,
        64 * 1024 * 1024
    );
    assert_eq!(report.resources.max_read_result_rows, Some(100_000));
    assert_eq!(
        report.resources.max_read_result_payload_bytes,
        Some(128 * 1024 * 1024)
    );
    assert!(report.resources.execution_batch_rows > 0);
    assert!(report.resources.execution_batch_payload_bytes > 0);
    assert!(report.resources.blocking_operator_bytes > 0);
    assert_eq!(report.resources.read_latency.sample_count, 16);
    assert_eq!(report.resources.output_rows, report.final_message_count);
    assert!(report.resources.logical_mutation_bytes > 0);
    assert!(report.resources.wal_append_bytes > 0);
    assert!(report.resources.new_generation_artifact_bytes > 0);
    assert!(report.resources.durable_write_bytes_lower_bound > 0);
    assert!(report.resources.process.resident_memory_supported);
    assert!(report.resources.process.total_page_faults_supported);
    assert!(report.resources.observed_peak_within_configured_profile);
    assert!(report
        .cold_checkpoint_reads
        .iter()
        .chain(&report.warm_checkpoint_reads)
        .all(|read| read.execution.row_runtime_path == "snapshot_rows"));
    assert!(report
        .cold_checkpoint_reads
        .iter()
        .map(|read| read.statement_name.as_str())
        .eq([
            "thread_owned_document_ids",
            "thread_messages_page",
            "thread_message_summary",
            "source_chunks_page",
            "source_chunks_by_source",
            "source_chunk_count_by_source",
            "source_document_payload_summary",
            "thread_covered_message_count",
            "content_status_anchor_count",
        ]));
    assert!(report
        .cold_checkpoint_reads
        .iter()
        .chain(&report.warm_checkpoint_reads)
        .all(|read| matches!(
            read.execution.index_runtime_path.as_str(),
            "authoritative" | "none"
        )));
    assert!(!report
        .json()
        .to_string()
        .contains(path.to_string_lossy().as_ref()));

    std::fs::remove_dir_all(path).expect("remove qualification database");
}

#[test]
fn qualification_rejects_an_existing_database_path() {
    let id = TEST_ID.fetch_add(1, Ordering::SeqCst);
    let path = std::env::temp_dir().join(format!(
        "hawdb-content-store-row-page-existing-{}-{id}",
        std::process::id()
    ));
    std::fs::create_dir_all(&path).expect("create existing qualification path");

    let error = run_content_store_initial_row_page_qualification(
        ContentStoreInitialRowPageQualificationConfig::synthetic(&path, "test-revision"),
    )
    .expect_err("existing qualification path must be rejected");

    assert!(error.to_string().contains("requires a new database path"));
    std::fs::remove_dir_all(path).expect("remove qualification path");
}

#[test]
fn capability_512_mib_is_evidence_identity_not_a_universal_limit() {
    let id = TEST_ID.fetch_add(1, Ordering::SeqCst);
    let path = std::env::temp_dir().join(format!(
        "hawdb-content-store-row-page-memory-profile-{}-{id}",
        std::process::id()
    ));
    let mut low_memory =
        ContentStoreInitialRowPageQualificationConfig::synthetic(&path, "test-revision");
    low_memory.configured_available_memory_bytes = 256 * 1024 * 1024;
    let low_memory_error = run_content_store_initial_row_page_qualification(low_memory)
        .expect_err("the 512 MiB capability profile must retain its exact identity");
    assert!(low_memory_error
        .to_string()
        .contains("512 MiB capability profile must declare"));

    let mut configured =
        ContentStoreInitialRowPageQualificationConfig::synthetic(&path, "test-revision");
    configured.resource_profile_kind = ContentStoreResourceProfileKind::ConfiguredWorkload;
    configured.configured_available_memory_bytes = 2 * 1024 * 1024 * 1024;
    assert!(configured
        .validate(&nowledge_content_store_sql_corpus().unwrap())
        .is_ok());

    let mut shared_host =
        ContentStoreInitialRowPageQualificationConfig::synthetic(&path, "test-revision");
    shared_host.resource_profile_kind = ContentStoreResourceProfileKind::SharedHost8Gib;
    shared_host.configured_available_memory_bytes = 9 * 1024 * 1024 * 1024;
    let shared_host_error = shared_host
        .validate(&nowledge_content_store_sql_corpus().unwrap())
        .expect_err("the shared-host profile cannot exceed the host profile limit");
    assert!(shared_host_error
        .to_string()
        .contains("shared-host 8 GiB profile"));
    shared_host.configured_available_memory_bytes = 3 * 1024 * 1024 * 1024;
    assert!(shared_host
        .validate(&nowledge_content_store_sql_corpus().unwrap())
        .is_ok());
}
