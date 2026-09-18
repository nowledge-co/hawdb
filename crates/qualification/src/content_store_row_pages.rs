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

mod corruption;
mod evidence;
mod extended_tables;
mod fixture;
mod isolation;
mod production;
mod production_mutation;
mod production_overflow_compaction;
mod resource;
mod source_ownership;
mod source_replacement;
mod space_merge_ownership;
#[cfg(test)]
mod tests;
mod thread_delete;
mod thread_fixture;
mod thread_ownership;
mod thread_reconcile;
mod thread_tail_delete;
mod thread_upsert;
mod transaction;

use crate::{
    nowledge_content_store_schema_identity, nowledge_content_store_sql_corpus,
    ContentStoreSchemaIdentity, ContentStoreSqlCorpus, ContentStoreSqlCorpusIdentity,
    CONTENT_STORE_SHARED_HOST_8_GIB_BYTES,
};
use corruption::qualify_content_store_corruption;
use evidence::{execute_qualified_read, execute_read_set, require_matching_results};
use extended_tables::{
    append_runtime_content, read_pair, require_anchor_occurrence_identity, require_runtime_counts,
    require_source_graph_chunk_count, runtime_extended_read_specs,
};
use fixture::{
    bootstrap_checkpoint, corpus_statement, database_config, initial_read_specs,
    thread_page_parameters, QUALIFIED_TABLES,
};
use hawdb::{
    Database, DurabilityPolicy, HawDBError, RelationalIndexMode, Result, StorageOpenTimings,
};
use isolation::qualify_content_store_isolation;
pub use production::*;
pub use production_mutation::*;
pub use production_overflow_compaction::*;
use resource::{qualify_content_store_resources, ContentStoreResourceProbeConfig};
use serde::Serialize;
use source_ownership::qualify_source_ownership_move;
use source_replacement::qualify_source_chunk_replacement;
use space_merge_ownership::qualify_space_merge_ownership;
use std::path::PathBuf;
use thread_delete::qualify_thread_delete;
use thread_ownership::qualify_thread_ownership_moves;
use thread_reconcile::qualify_thread_message_reconcile;
use thread_tail_delete::qualify_thread_tail_delete;
use thread_upsert::qualify_thread_message_upsert;
use transaction::qualify_multi_statement_transaction;

pub const CONTENT_STORE_INITIAL_ROW_PAGE_QUALIFICATION_PROTOCOL: &str =
    "hawdb-content-store-initial-row-page-qualification-v1";
pub const CONTENT_STORE_512_MIB_CAPABILITY_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentStoreResourceProfileKind {
    Capability512Mib,
    SharedHost8Gib,
    ConfiguredWorkload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ContentStoreOpenTimingEvidence {
    pub durable_manifest_open_micros: u64,
    pub checkpoint_root_open_micros: u64,
    pub wal_replay_micros: u64,
    pub post_replay_open_micros: u64,
    pub accounted_micros: u64,
    pub unaccounted_micros: u64,
    pub total_open_micros: u64,
    pub consistent: bool,
}

impl From<StorageOpenTimings> for ContentStoreOpenTimingEvidence {
    fn from(timings: StorageOpenTimings) -> Self {
        Self {
            durable_manifest_open_micros: timings.durable_manifest_open_micros,
            checkpoint_root_open_micros: timings.checkpoint_root_open_micros,
            wal_replay_micros: timings.wal_replay_micros,
            post_replay_open_micros: timings.post_replay_open_micros,
            accounted_micros: timings.accounted_micros(),
            unaccounted_micros: timings.unaccounted_micros(),
            total_open_micros: timings.total_open_micros,
            consistent: timings.is_consistent(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentStoreInitialRowPageQualificationConfig {
    pub database_path: PathBuf,
    pub source_revision: String,
    pub base_message_count: usize,
    pub message_payload_bytes: usize,
    pub base_chunk_count: usize,
    pub chunk_payload_bytes: usize,
    pub segment_cache_capacity_bytes: u64,
    pub resource_profile_kind: ContentStoreResourceProfileKind,
    pub configured_available_memory_bytes: u64,
    pub resource_read_samples: usize,
}

impl ContentStoreInitialRowPageQualificationConfig {
    pub fn synthetic(
        database_path: impl Into<PathBuf>,
        source_revision: impl Into<String>,
    ) -> Self {
        Self {
            database_path: database_path.into(),
            source_revision: source_revision.into(),
            base_message_count: 8,
            message_payload_bytes: 8 * 1024,
            base_chunk_count: 8,
            chunk_payload_bytes: 8 * 1024,
            segment_cache_capacity_bytes: 512 * 1024,
            resource_profile_kind: ContentStoreResourceProfileKind::Capability512Mib,
            configured_available_memory_bytes: CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
            resource_read_samples: 16,
        }
    }

    fn validate(&self, corpus: &ContentStoreSqlCorpus) -> Result<()> {
        if self.database_path.exists() {
            return Err(HawDBError::Semantic(
                "content-store row-page qualification requires a new database path".to_string(),
            ));
        }
        if self.source_revision.trim().is_empty() {
            return Err(HawDBError::Semantic(
                "content-store row-page qualification source revision must not be empty"
                    .to_string(),
            ));
        }
        if self.base_message_count == 0 || self.message_payload_bytes == 0 {
            return Err(HawDBError::Semantic(
                "content-store row-page qualification message count and payload must be non-zero"
                    .to_string(),
            ));
        }
        if self.base_chunk_count == 0 || self.chunk_payload_bytes == 0 {
            return Err(HawDBError::Semantic(
                "content-store row-page qualification chunk count and payload must be non-zero"
                    .to_string(),
            ));
        }
        if self.segment_cache_capacity_bytes == 0 {
            return Err(HawDBError::Semantic(
                "content-store row-page qualification cache capacity must be non-zero".to_string(),
            ));
        }
        if self.configured_available_memory_bytes == 0 {
            return Err(HawDBError::Semantic(
                "content-store row-page qualification configured memory must be non-zero"
                    .to_string(),
            ));
        }
        if self.resource_profile_kind == ContentStoreResourceProfileKind::Capability512Mib
            && self.configured_available_memory_bytes != CONTENT_STORE_512_MIB_CAPABILITY_BYTES
        {
            return Err(HawDBError::Semantic(format!(
                "content-store 512 MiB capability profile must declare {CONTENT_STORE_512_MIB_CAPABILITY_BYTES} available bytes"
            )));
        }
        if self.resource_profile_kind == ContentStoreResourceProfileKind::SharedHost8Gib
            && self.configured_available_memory_bytes > CONTENT_STORE_SHARED_HOST_8_GIB_BYTES
        {
            return Err(HawDBError::Semantic(format!(
                "content-store shared-host 8 GiB profile cannot declare more than {CONTENT_STORE_SHARED_HOST_8_GIB_BYTES} available bytes"
            )));
        }
        if self.segment_cache_capacity_bytes > self.configured_available_memory_bytes {
            return Err(HawDBError::Semantic(format!(
                "content-store row-page qualification cache capacity {} exceeds configured available memory {}",
                self.segment_cache_capacity_bytes, self.configured_available_memory_bytes
            )));
        }
        if self.resource_read_samples == 0 || self.resource_read_samples > 1024 {
            return Err(HawDBError::Semantic(
                "content-store row-page qualification resource read samples must be between 1 and 1024"
                    .to_string(),
            ));
        }
        let page = corpus_statement(corpus, "thread_messages_page")?;
        let final_message_count = self.base_message_count.saturating_add(3);
        if final_message_count > page.max_rows {
            return Err(HawDBError::Semantic(format!(
                "content-store row-page qualification needs {final_message_count} rows but thread_messages_page admits {}",
                page.max_rows
            )));
        }
        let minimum_payload = self
            .message_payload_bytes
            .checked_mul(final_message_count)
            .ok_or_else(|| {
                HawDBError::Semantic(
                    "content-store row-page qualification payload size overflow".to_string(),
                )
            })?;
        if minimum_payload > page.max_payload_bytes {
            return Err(HawDBError::Semantic(format!(
                "content-store row-page qualification message payloads need at least {minimum_payload} bytes but thread_messages_page admits {}",
                page.max_payload_bytes
            )));
        }
        let chunks = corpus_statement(corpus, "source_chunks_by_source")?;
        let final_chunk_count = self.base_chunk_count.saturating_add(2);
        if final_chunk_count > chunks.max_rows {
            return Err(HawDBError::Semantic(format!(
                "content-store row-page qualification needs {final_chunk_count} chunks but source_chunks_by_source admits {}",
                chunks.max_rows
            )));
        }
        let minimum_chunk_payload = self
            .chunk_payload_bytes
            .checked_mul(final_chunk_count)
            .ok_or_else(|| {
                HawDBError::Semantic(
                    "content-store row-page qualification chunk payload size overflow".to_string(),
                )
            })?;
        if minimum_chunk_payload > chunks.max_payload_bytes {
            return Err(HawDBError::Semantic(format!(
                "content-store row-page qualification chunk payloads need at least {minimum_chunk_payload} bytes but source_chunks_by_source admits {}",
                chunks.max_payload_bytes
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentStoreRowPageReadPhase {
    ColdCheckpoint,
    WarmCheckpoint,
    WalRecovery,
    LiveOverlay,
    ProductionCold,
    ProductionWarm,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreRowPageCacheDelta {
    pub hits: u64,
    pub misses: u64,
    pub insertions: u64,
    pub evictions: u64,
    pub admission_rejections: u64,
    pub resident_bytes_after: u64,
    pub pinned_bytes_after: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreRowPageExecutionEvidence {
    pub index_runtime_path: String,
    pub row_runtime_path: String,
    pub base_generation: u64,
    pub delta_generation: Option<u64>,
    pub base_commit_epoch: u64,
    pub visible_commit_epoch: u64,
    pub root_set_digest: String,
    pub logical_pages: u64,
    pub logical_bytes: u64,
    pub physical_pages: u64,
    pub physical_bytes: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_admission_rejections: u64,
    pub index_logical_pages: u64,
    pub index_logical_bytes: u64,
    pub index_physical_pages: u64,
    pub index_physical_bytes: u64,
    pub index_cache_hits: u64,
    pub index_cache_misses: u64,
    pub index_cache_admission_rejections: u64,
    pub overlay_entries: u64,
    pub overlay_bytes: u64,
    pub rows_visited: u64,
    pub intermediate_rows: u64,
    pub hydrated_rows: u64,
    pub hydrated_compressed_bytes: u64,
    pub hydrated_decompressed_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreRowPageReadReport {
    pub statement_name: String,
    pub phase: ContentStoreRowPageReadPhase,
    pub max_rows: usize,
    pub max_payload_bytes: usize,
    pub output_rows: usize,
    pub output_payload_bytes: usize,
    pub output_sha256: String,
    pub cache: ContentStoreRowPageCacheDelta,
    pub execution: ContentStoreRowPageExecutionEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreInitialRowPageQualificationReport {
    pub protocol: String,
    pub source_revision: String,
    pub corpus: ContentStoreSqlCorpusIdentity,
    pub schema: ContentStoreSchemaIdentity,
    pub qualified_tables: Vec<String>,
    pub base_message_count: usize,
    pub final_message_count: usize,
    pub message_payload_bytes: usize,
    pub base_chunk_count: usize,
    pub final_chunk_count: usize,
    pub chunk_payload_bytes: usize,
    pub segment_cache_capacity_bytes: u64,
    pub checkpoint_generation: u64,
    pub checkpoint_commit_epoch: u64,
    pub cold_checkpoint_reads: Vec<ContentStoreRowPageReadReport>,
    pub warm_checkpoint_reads: Vec<ContentStoreRowPageReadReport>,
    pub wal_replayed_entries: usize,
    pub wal_replayed_bytes: u64,
    pub wal_content_commit_epoch: u64,
    pub wal_recovery_read: ContentStoreRowPageReadReport,
    pub wal_recovery_chunk_read: ContentStoreRowPageReadReport,
    pub wal_recovery_anchor_read: ContentStoreRowPageReadReport,
    pub live_overlay_read: ContentStoreRowPageReadReport,
    pub live_content_commit_epoch: u64,
    pub live_overlay_chunk_read: ContentStoreRowPageReadReport,
    pub live_overlay_anchor_read: ContentStoreRowPageReadReport,
    pub source_chunk_replacement: ContentStoreSourceReplacementQualificationReport,
    pub source_ownership_move: ContentStoreSourceOwnershipMoveQualificationReport,
    pub thread_ownership_move: ContentStoreThreadOwnershipMoveQualificationReport,
    pub space_merge_ownership: ContentStoreSpaceMergeOwnershipQualificationReport,
    pub thread_message_upsert: ContentStoreThreadUpsertQualificationReport,
    pub thread_message_reconcile: ContentStoreThreadReconcileQualificationReport,
    pub thread_tail_delete: ContentStoreThreadTailDeleteQualificationReport,
    pub thread_delete: ContentStoreThreadDeleteQualificationReport,
    pub multi_statement_transaction: ContentStoreTransactionQualificationReport,
    pub resources: ContentStoreResourceEvidence,
    pub corruption: ContentStoreCorruptionQualificationReport,
    pub isolation: ContentStoreIsolationQualificationReport,
    pub ready: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreTransactionQualificationReport {
    pub inserted_content_message_id: String,
    pub page_output_rows: usize,
    pub page_output_sha256: String,
    pub summary_item_count: i64,
    pub summary_size_bytes: i64,
    pub index_runtime_path: String,
    pub row_runtime_path: String,
    pub transaction_workspace_lookups: u64,
    pub canonical_fallback_lookups: u64,
    pub rejected_statement_atomic: bool,
    pub committed_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreSourceReplacementQualificationReport {
    pub initial_chunk_count: usize,
    pub stale_suffix_removed: bool,
    pub chunk_fields_preserved: bool,
    pub duplicate_order_rejected: bool,
    pub rejected_statement_atomic: bool,
    pub shorter_replacement: ContentStoreSourceReplacementPhaseReport,
    pub empty_replacement: ContentStoreSourceReplacementPhaseReport,
    pub final_chunk_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreSourceReplacementPhaseReport {
    pub replacement_chunk_count: usize,
    pub committed_epoch: u64,
    pub summary_item_count: i64,
    pub summary_size_bytes: i64,
    pub live_read: ContentStoreRowPageReadReport,
    pub checkpoint_generation: u64,
    pub reopened_read: ContentStoreRowPageReadReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreSourceOwnershipMoveQualificationReport {
    pub previous_space_id: String,
    pub target_space_id: String,
    pub chunk_count: usize,
    pub missing_owner_count: i64,
    pub missing_owner_epoch_unchanged: bool,
    pub seed_commit_epoch: u64,
    pub committed_epoch: u64,
    pub payload_sha256_before: String,
    pub payload_sha256_after_live: String,
    pub payload_sha256_after_reopen: String,
    pub payload_fields_preserved: bool,
    pub live_read: ContentStoreRowPageReadReport,
    pub checkpoint_generation: u64,
    pub reopened_read: ContentStoreRowPageReadReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreThreadOwnershipMoveQualificationReport {
    pub requested_moves: usize,
    pub documents_updated: usize,
    pub messages_updated: usize,
    pub document_owner_uses_storage_id: bool,
    pub stale_guard_preserved: bool,
    pub graph_relational_agreement: bool,
    pub payload_fields_preserved: bool,
    pub seed_commit_epoch: u64,
    pub committed_epoch: u64,
    pub payload_sha256_before: String,
    pub payload_sha256_after_live: String,
    pub payload_sha256_after_reopen: String,
    pub live_reads: Vec<ContentStoreThreadOwnershipReadReport>,
    pub checkpoint_generation: u64,
    pub reopened_reads: Vec<ContentStoreThreadOwnershipReadReport>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreThreadOwnershipReadReport {
    pub case_name: String,
    pub expected_space_id: String,
    pub read: ContentStoreRowPageReadReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreSpaceMergeOwnershipQualificationReport {
    pub requested_threads: usize,
    pub requested_sources: usize,
    pub documents_updated: usize,
    pub messages_updated: usize,
    pub document_owner_uses_storage_id: bool,
    pub stale_guard_preserved: bool,
    pub graph_relational_agreement: bool,
    pub payload_fields_preserved: bool,
    pub base_epoch: u64,
    pub committed_epoch: u64,
    pub payload_sha256_before: String,
    pub payload_sha256_after_live: String,
    pub payload_sha256_after_reopen: String,
    pub live_reads: Vec<ContentStoreSpaceMergeReadReport>,
    pub checkpoint_generation: u64,
    pub reopened_reads: Vec<ContentStoreSpaceMergeReadReport>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreSpaceMergeReadReport {
    pub case_name: String,
    pub expected_space_id: String,
    pub read: ContentStoreRowPageReadReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreThreadUpsertQualificationReport {
    pub thread_id: String,
    pub thread_storage_id: String,
    pub content_document_id: String,
    pub message_count: usize,
    pub summary_size_bytes: i64,
    pub document_owner_uses_storage_id: bool,
    pub message_public_id_preserved: bool,
    pub document_created_at_preserved_on_conflict: bool,
    pub message_created_at_preserved_on_conflict: bool,
    pub rejected_statement_atomic: bool,
    pub graph_relational_agreement: bool,
    pub base_epoch: u64,
    pub committed_epoch: u64,
    pub live_read: ContentStoreRowPageReadReport,
    pub checkpoint_generation: u64,
    pub reopened_read: ContentStoreRowPageReadReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreThreadReconcileQualificationReport {
    pub thread_id: String,
    pub thread_storage_id: String,
    pub initial_message_count: usize,
    pub final_message_count: usize,
    pub duplicate_mapping_rejected: bool,
    pub incomplete_mapping_rejected: bool,
    pub unknown_mapping_rejected: bool,
    pub invalid_mapping_epoch_unchanged: bool,
    pub explicit_anchor_reordered: bool,
    pub legacy_anchor_reordered: bool,
    pub occurrence_identity_preserved: bool,
    pub summary_item_count: i64,
    pub summary_size_bytes: i64,
    pub seed_commit_epoch: u64,
    pub seed_checkpoint_generation: u64,
    pub committed_epoch: u64,
    pub preserved_message_payload_sha256_before: String,
    pub preserved_message_payload_sha256_after_live: String,
    pub preserved_message_payload_sha256_after_reopen: String,
    pub preserved_anchor_payload_sha256_before: String,
    pub preserved_anchor_payload_sha256_after_live: String,
    pub preserved_anchor_payload_sha256_after_reopen: String,
    pub live_read: ContentStoreRowPageReadReport,
    pub live_anchor_sha256: String,
    pub checkpoint_generation: u64,
    pub reopened_read: ContentStoreRowPageReadReport,
    pub reopened_anchor_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreThreadTailDeleteQualificationReport {
    pub thread_id: String,
    pub thread_storage_id: String,
    pub start_index: i64,
    pub initial_message_count: usize,
    pub retained_message_count: usize,
    pub deleted_message_ids: Vec<String>,
    pub deleted_occurrence_ids: Vec<String>,
    pub deleted_candidate_sha256: String,
    pub negative_start_clamped: bool,
    pub empty_tail_noop: bool,
    pub empty_tail_epoch_unchanged: bool,
    pub rollback_preserved_state: bool,
    pub graph_relational_agreement: bool,
    pub exact_summary: bool,
    pub retained_payload_identity: bool,
    pub summary_item_count: i64,
    pub summary_size_bytes: i64,
    pub seed_commit_epoch: u64,
    pub seed_checkpoint_generation: u64,
    pub committed_epoch: u64,
    pub retained_message_payload_sha256_before: String,
    pub retained_message_payload_sha256_after_live: String,
    pub retained_message_payload_sha256_after_reopen: String,
    pub retained_anchor_payload_sha256_before: String,
    pub retained_anchor_payload_sha256_after_live: String,
    pub retained_anchor_payload_sha256_after_reopen: String,
    pub deleted_tombstone_read: ContentStoreRowPageReadReport,
    pub live_read: ContentStoreRowPageReadReport,
    pub live_anchor_sha256: String,
    pub checkpoint_generation: u64,
    pub reopened_read: ContentStoreRowPageReadReport,
    pub reopened_anchor_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreThreadDeleteQualificationReport {
    pub thread_id: String,
    pub thread_storage_id: String,
    pub requested_thread_id: String,
    pub discovered_document_ids: Vec<String>,
    pub discovered_document_sha256: String,
    pub empty_owned_document_discovered: bool,
    pub graph_deleted_message_count: i64,
    pub relational_deleted_message_count: i64,
    pub deleted_anchor_count: i64,
    pub deleted_identity_count: i64,
    pub missing_thread_noop: bool,
    pub rollback_preserved_state: bool,
    pub workspace_read_your_own_writes: bool,
    pub graph_relational_absent: bool,
    pub unrelated_payload_identity: bool,
    pub second_delete_noop: bool,
    pub seed_commit_epoch: u64,
    pub seed_checkpoint_generation: u64,
    pub committed_epoch: u64,
    pub unrelated_state_sha256_before: String,
    pub unrelated_state_sha256_after_live: String,
    pub unrelated_state_sha256_after_reopen: String,
    pub deleted_tombstone_read: ContentStoreRowPageReadReport,
    pub live_count_sha256: String,
    pub checkpoint_generation: u64,
    pub reopened_count_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreIsolationQualificationReport {
    pub content_message_id: String,
    pub point_max_rows: usize,
    pub point_max_payload_bytes: usize,
    pub row_sha256: String,
    pub cancellation_non_poisoning: bool,
    pub cancellation_pinned_bytes_after: u64,
    pub waiter_lock_timeout_micros: u64,
    pub waiter_timed_out: bool,
    pub waiter_aborted: bool,
    pub owner_rollback_preserved_row: bool,
    pub commit_epoch_before: u64,
    pub commit_epoch_after: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreCorruptionQualificationReport {
    pub artifact_name: String,
    pub artifact_generation: u64,
    pub artifact_bytes: u64,
    pub bit_flip_offset: u64,
    pub scrub_rejected: bool,
    pub damaged_handle_poisoned: bool,
    pub post_failure_sql_rejected: bool,
    pub source_preserved: bool,
    pub source_row_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreResourceEvidence {
    pub profile_kind: ContentStoreResourceProfileKind,
    pub configured_available_memory_bytes: u64,
    pub segment_cache_capacity_bytes: u64,
    pub max_relational_index_read_bytes: usize,
    pub max_relational_hydration_bytes: usize,
    pub max_read_result_rows: Option<usize>,
    pub max_read_result_payload_bytes: Option<usize>,
    pub execution_batch_rows: usize,
    pub execution_batch_payload_bytes: usize,
    pub blocking_operator_bytes: usize,
    pub max_wal_replay_bytes: Option<u64>,
    pub max_out_of_core_delta_bytes: Option<u64>,
    pub read_samples: usize,
    pub read_latency: crate::LatencyPercentiles,
    pub output_rows: usize,
    pub output_payload_bytes: usize,
    pub output_sha256: String,
    pub mutation_latency_micros: u64,
    pub checkpoint_latency_micros: u64,
    pub probe_latency_micros: u64,
    pub logical_mutation_bytes: u64,
    pub wal_append_bytes: u64,
    pub new_generation_artifact_bytes: u64,
    pub durable_write_bytes_lower_bound: u64,
    pub durable_write_amplification_lower_bound_per_million: u64,
    pub write_measurement_scope: String,
    pub process: ContentStoreProcessResourceEvidence,
    pub runtime_memory: ContentStoreRuntimeMemoryEvidence,
    pub observed_peak_within_configured_profile: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreProcessResourceEvidence {
    pub resident_memory_supported: bool,
    pub total_page_faults_supported: bool,
    pub split_page_faults_supported: bool,
    pub start_resident_bytes: u64,
    pub steady_resident_bytes: u64,
    pub peak_resident_bytes: u64,
    pub steady_resident_growth_bytes: u64,
    pub lifetime_peak_resident_growth_bytes: u64,
    pub total_page_faults: Option<u64>,
    pub minor_page_faults: Option<u64>,
    pub major_page_faults: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreRuntimeMemoryEvidence {
    pub host_total_bytes: Option<u64>,
    pub host_available_bytes: Option<u64>,
    pub cgroup_limit_bytes: Option<u64>,
    pub cgroup_high_bytes: Option<u64>,
    pub cgroup_current_bytes: Option<u64>,
    pub effective_limit_bytes: Option<u64>,
    pub effective_available_bytes: Option<u64>,
    pub pressure: String,
}

impl ContentStoreInitialRowPageQualificationReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("content-store row-page report is serializable")
    }
}

/// Qualifies the first relational Content Store tables through the public SQL
/// path. The caller owns the new database directory and may retain it as an
/// evidence artifact after this function returns.
pub fn run_content_store_initial_row_page_qualification(
    config: ContentStoreInitialRowPageQualificationConfig,
) -> Result<ContentStoreInitialRowPageQualificationReport> {
    let corpus = nowledge_content_store_sql_corpus()?;
    config.validate(&corpus)?;

    let checkpoint = bootstrap_checkpoint(&config, &corpus)?;
    let authoritative_config = database_config(&config, RelationalIndexMode::Authoritative);
    let mut database = Database::open_with_durability_and_config(
        &config.database_path,
        DurabilityPolicy::SyncOnEveryWrite,
        authoritative_config.clone(),
    )?;

    let read_specs =
        initial_read_specs(&corpus, config.base_message_count, config.base_chunk_count)?;
    let cold_checkpoint_reads = execute_read_set(
        &mut database,
        &read_specs,
        ContentStoreRowPageReadPhase::ColdCheckpoint,
    )?;
    let warm_checkpoint_reads = execute_read_set(
        &mut database,
        &read_specs,
        ContentStoreRowPageReadPhase::WarmCheckpoint,
    )?;
    require_matching_results(&cold_checkpoint_reads, &warm_checkpoint_reads)?;
    require_source_graph_chunk_count(&mut database, config.base_chunk_count)?;
    require_runtime_counts(
        &mut database,
        &corpus,
        config.base_chunk_count,
        config.base_message_count,
    )?;
    require_anchor_occurrence_identity(&mut database, config.base_message_count, 0)?;

    let wal_content_commit_epoch = append_runtime_content(
        &mut database,
        &corpus,
        config.base_message_count,
        config.message_payload_bytes,
        config.base_chunk_count,
        config.chunk_payload_bytes,
        "wal",
    )?;
    drop(database);

    let mut database = Database::open_with_durability_and_config(
        &config.database_path,
        DurabilityPolicy::SyncOnEveryWrite,
        authoritative_config.clone(),
    )?;
    let recovery = database.storage_recovery_report();
    if recovery.replayed_wal_entries == 0 {
        return Err(HawDBError::Execution(
            "content-store row-page qualification did not replay the post-checkpoint WAL mutation"
                .to_string(),
        ));
    }
    let page = corpus_statement(&corpus, "thread_messages_page")?;
    let wal_recovery_read = execute_qualified_read(
        &mut database,
        page,
        thread_page_parameters(config.base_message_count + 1),
        ContentStoreRowPageReadPhase::WalRecovery,
        config.base_message_count + 1,
    )?;
    if wal_recovery_read.execution.delta_generation.is_none() {
        return Err(HawDBError::Execution(
            "content-store row-page qualification WAL read did not use a recovery delta"
                .to_string(),
        ));
    }
    if wal_recovery_read.execution.visible_commit_epoch != wal_content_commit_epoch {
        return Err(HawDBError::Execution(format!(
            "content-store WAL message read observed epoch {}, expected graph-plus-relational epoch {wal_content_commit_epoch}",
            wal_recovery_read.execution.visible_commit_epoch
        )));
    }
    let wal_chunk_count = config.base_chunk_count + 1;
    require_source_graph_chunk_count(&mut database, wal_chunk_count)?;
    require_runtime_counts(
        &mut database,
        &corpus,
        wal_chunk_count,
        config.base_message_count + 1,
    )?;
    require_anchor_occurrence_identity(&mut database, config.base_message_count + 1, 1)?;
    let (wal_recovery_chunk_read, wal_recovery_anchor_read) = read_pair(
        &mut database,
        runtime_extended_read_specs(&corpus, wal_chunk_count)?,
        ContentStoreRowPageReadPhase::WalRecovery,
    )?;
    if wal_recovery_chunk_read.execution.delta_generation.is_none()
        || wal_recovery_anchor_read
            .execution
            .delta_generation
            .is_none()
    {
        return Err(HawDBError::Execution(
            "content-store extended WAL reads did not use the recovery delta".to_string(),
        ));
    }
    for read in [&wal_recovery_chunk_read, &wal_recovery_anchor_read] {
        if read.execution.visible_commit_epoch != wal_content_commit_epoch {
            return Err(HawDBError::Execution(format!(
                "content-store WAL statement {} observed epoch {}, expected graph-plus-relational epoch {wal_content_commit_epoch}",
                read.statement_name, read.execution.visible_commit_epoch
            )));
        }
    }

    let live_content_commit_epoch = append_runtime_content(
        &mut database,
        &corpus,
        config.base_message_count + 1,
        config.message_payload_bytes,
        config.base_chunk_count + 1,
        config.chunk_payload_bytes,
        "live",
    )?;
    let live_overlay_read = execute_qualified_read(
        &mut database,
        page,
        thread_page_parameters(config.base_message_count + 2),
        ContentStoreRowPageReadPhase::LiveOverlay,
        config.base_message_count + 2,
    )?;
    if live_overlay_read.execution.overlay_entries == 0 {
        return Err(HawDBError::Execution(
            "content-store row-page qualification live read did not use the row overlay"
                .to_string(),
        ));
    }
    if live_overlay_read.execution.visible_commit_epoch != live_content_commit_epoch {
        return Err(HawDBError::Execution(format!(
            "content-store live message read observed epoch {}, expected graph-plus-relational epoch {live_content_commit_epoch}",
            live_overlay_read.execution.visible_commit_epoch
        )));
    }
    let live_chunk_count = config.base_chunk_count + 2;
    require_runtime_counts(
        &mut database,
        &corpus,
        live_chunk_count,
        config.base_message_count + 2,
    )?;
    require_anchor_occurrence_identity(&mut database, config.base_message_count + 2, 2)?;
    let (live_overlay_chunk_read, live_overlay_anchor_read) = read_pair(
        &mut database,
        runtime_extended_read_specs(&corpus, live_chunk_count)?,
        ContentStoreRowPageReadPhase::LiveOverlay,
    )?;
    if live_overlay_chunk_read.execution.overlay_entries == 0
        || live_overlay_anchor_read.execution.overlay_entries == 0
    {
        return Err(HawDBError::Execution(
            "content-store extended live reads did not use the row overlay".to_string(),
        ));
    }
    for read in [&live_overlay_chunk_read, &live_overlay_anchor_read] {
        if read.execution.visible_commit_epoch != live_content_commit_epoch {
            return Err(HawDBError::Execution(format!(
                "content-store live statement {} observed epoch {}, expected graph-plus-relational epoch {live_content_commit_epoch}",
                read.statement_name, read.execution.visible_commit_epoch
            )));
        }
    }

    let (replacement_database, source_chunk_replacement) = qualify_source_chunk_replacement(
        database,
        &config.database_path,
        &authoritative_config,
        &corpus,
        live_chunk_count,
        config.chunk_payload_bytes,
    )?;
    database = replacement_database;

    let (ownership_database, source_ownership_move) = qualify_source_ownership_move(
        database,
        &config.database_path,
        &authoritative_config,
        &corpus,
        config.chunk_payload_bytes,
    )?;
    database = ownership_database;

    let multi_statement_transaction = qualify_multi_statement_transaction(
        &mut database,
        &corpus,
        config.base_message_count + 2,
        config.message_payload_bytes,
    )?;
    let resources = qualify_content_store_resources(
        &mut database,
        &corpus,
        ContentStoreResourceProbeConfig {
            profile_kind: config.resource_profile_kind,
            configured_available_memory_bytes: config.configured_available_memory_bytes,
            read_samples: config.resource_read_samples,
            database_path: &config.database_path,
            database_config: &authoritative_config,
            message_position: config.base_message_count + 2,
            message_payload_bytes: config.message_payload_bytes,
        },
    )?;
    let corruption = qualify_content_store_corruption(
        &mut database,
        &config.database_path,
        &authoritative_config,
        &multi_statement_transaction.inserted_content_message_id,
    )?;
    let (ownership_database, thread_ownership_move) = qualify_thread_ownership_moves(
        database,
        &config.database_path,
        &authoritative_config,
        &corpus,
    )?;
    database = ownership_database;
    let (space_merge_database, space_merge_ownership) = qualify_space_merge_ownership(
        database,
        &config.database_path,
        &authoritative_config,
        &corpus,
    )?;
    database = space_merge_database;
    let (thread_upsert_database, thread_message_upsert) = qualify_thread_message_upsert(
        database,
        &config.database_path,
        &authoritative_config,
        &corpus,
    )?;
    database = thread_upsert_database;
    let (thread_reconcile_database, thread_message_reconcile) = qualify_thread_message_reconcile(
        database,
        &config.database_path,
        &authoritative_config,
        &corpus,
    )?;
    database = thread_reconcile_database;
    let (thread_tail_delete_database, thread_tail_delete) = qualify_thread_tail_delete(
        database,
        &config.database_path,
        &authoritative_config,
        &corpus,
    )?;
    database = thread_tail_delete_database;
    let (thread_delete_database, thread_delete) = qualify_thread_delete(
        database,
        &config.database_path,
        &authoritative_config,
        &corpus,
    )?;
    database = thread_delete_database;
    let isolation = qualify_content_store_isolation(
        database,
        &corpus,
        config.base_message_count + 2,
        config.message_payload_bytes,
    )?;

    Ok(ContentStoreInitialRowPageQualificationReport {
        protocol: CONTENT_STORE_INITIAL_ROW_PAGE_QUALIFICATION_PROTOCOL.to_string(),
        source_revision: config.source_revision,
        corpus: corpus.identity(),
        schema: nowledge_content_store_schema_identity(),
        qualified_tables: QUALIFIED_TABLES
            .iter()
            .map(|table| (*table).to_string())
            .collect(),
        base_message_count: config.base_message_count,
        final_message_count: config.base_message_count + 3,
        message_payload_bytes: config.message_payload_bytes,
        base_chunk_count: config.base_chunk_count,
        final_chunk_count: source_ownership_move.chunk_count,
        chunk_payload_bytes: config.chunk_payload_bytes,
        segment_cache_capacity_bytes: config.segment_cache_capacity_bytes,
        checkpoint_generation: checkpoint.generation,
        checkpoint_commit_epoch: checkpoint.commit_epoch,
        cold_checkpoint_reads,
        warm_checkpoint_reads,
        wal_replayed_entries: recovery.replayed_wal_entries,
        wal_replayed_bytes: recovery.replayed_wal_bytes,
        wal_content_commit_epoch,
        wal_recovery_read,
        wal_recovery_chunk_read,
        wal_recovery_anchor_read,
        live_overlay_read,
        live_content_commit_epoch,
        live_overlay_chunk_read,
        live_overlay_anchor_read,
        source_chunk_replacement,
        source_ownership_move,
        thread_ownership_move,
        space_merge_ownership,
        thread_message_upsert,
        thread_message_reconcile,
        thread_tail_delete,
        thread_delete,
        multi_statement_transaction,
        resources,
        corruption,
        isolation,
        ready: true,
    })
}
