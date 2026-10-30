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

//! Database and session configuration; runtime ownership stays in the facade.

use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseConfig {
    pub read_only: bool,
    /// An existing, already durable ancestor whose own name and ancestry are
    /// guaranteed by the host. Writable open synchronizes every directory from
    /// the project through this anchor, inclusive. The host must keep that
    /// namespace stable while the project is open; existence is not proof.
    /// `None` retains the default walk through the existing filesystem mount.
    /// On Windows this includes the volume root and requires writable directory
    /// handles at every level. Ordinary user-profile storage should select an
    /// explicit host-guaranteed anchor; access denial never supplies one.
    /// Concurrent opens of one project must use the same canonical anchor.
    pub namespace_durability_anchor: Option<PathBuf>,
    /// Maximum monotonic age of uncheckpointed data in an admitted persistent
    /// writer. The background-maintenance capability controls worker creation.
    pub automatic_checkpoint_max_age: std::time::Duration,
    /// Bounded pending-child recovery during a writable project open.
    pub branch_create_recovery_limits: BranchCreateRecoveryLimits,
    /// Optional host-wide read row ceiling. `Some(0)` and `None` (the default) mean unlimited.
    /// Session settings may lower a positive ceiling, but cannot raise it.
    pub max_read_result_rows: Option<usize>,
    pub max_read_result_payload_bytes: Option<usize>,
    pub execution_memory: executor::ExecutionMemoryConfig,
    pub mutation_limits: hawdb_storage::mutation::MutationLimits,
    /// Maximum memo groups available to graph and relational optimizer search.
    pub max_optimizer_groups: Option<usize>,
    /// Maximum memo expressions available to relational join enumeration.
    pub max_relational_join_expressions: Option<usize>,
    pub recovery_mode: RecoveryMode,
    pub max_wal_replay_entries: Option<usize>,
    pub max_wal_replay_bytes: Option<u64>,
    /// Maximum sealed WAL generations recovered for one branch admission.
    /// Checkpoint compaction resets the chain; default is 256.
    pub max_branch_sealed_wal_intervals: usize,
    /// Shared capacity target for all engine-owned project file descriptors.
    /// Defaults to 1024 without pre-opening files. Native Unix acquisition limits
    /// effective admission to the OS soft limit minus 64 host-owned handles.
    /// Process limits remain under host control; idle immutable files use LRU.
    /// Independent contexts for one canonical project must request the same value.
    pub max_open_files: usize,
    /// Reject foreground descriptor admission when `max_open_files` cannot be
    /// met by evicting idle handles. Defaults to false. Internal control I/O,
    /// recovery and maintenance admission always use a non-strict policy.
    pub strict_capacity_limit: bool,
    pub max_wal_quarantine_bytes: u64,
    pub max_wal_record_bytes: Option<usize>,
    pub max_wal_batch_operations: Option<usize>,
    pub max_checkpoint_encoded_bytes: Option<u64>,
    pub max_checkpoint_decoded_bytes: Option<u64>,
    pub segment_cache_capacity_bytes: u64,
    /// Aggregate encoded graph-manifest bytes allowed during database open.
    /// Payload pages remain governed separately by the segment cache.
    pub max_graph_manifest_open_bytes: u64,
    /// Maximum physical relational-index file bytes fetched by one query.
    /// Cache hits consume the separately derived logical traversal budget only.
    pub max_relational_index_read_bytes: NonZeroUsize,
    pub max_relational_hydration_bytes: NonZeroUsize,
    pub storage_residency_mode: hawdb_storage::config::StorageResidencyMode,
    pub auto_materialize_checkpoint_bytes: u64,
    pub max_out_of_core_delta_bytes: Option<u64>,
    /// Derived columnar shadow double-write: every checkpoint also
    /// publishes a column-group catalog under `column-groups/`, and recovery
    /// validates it. Off by default; with the flag off checkpoints are
    /// byte-for-byte unchanged and no shadow directory exists. Reads are
    /// never served from the shadow.
    pub graph_columnar_shadow_checkpoint: bool,
    /// Persistent relational-index publication and read activation mode.
    pub relational_index_mode: hawdb_storage::config::RelationalIndexMode,
    /// Enables the metadata-only monotonic INSERT fast path for RowPage tables.
    /// Disabled by default until workload qualification explicitly activates it.
    pub relational_monotonic_append_fast_path: bool,
    pub max_search_projection_change_log_entries: Option<usize>,
    pub max_search_projection_change_log_bytes: Option<usize>,
    pub search_projection_relational_change_limits:
        hawdb_storage::relational::RelationalPrimaryKeyChangeCaptureLimits,
    /// Maximum entries per graph physical-plan, parameter-binding-template,
    /// or relational SQL-template cache.
    pub max_plan_cache_entries: Option<usize>,
    /// Requested-heap budget for the shared logical parameter-binding cache,
    /// including its keys and table capacity. Defaults to 16 MiB; zero disables
    /// admission. In-flight plans, physical-plan caches, allocator rounding and
    /// fixed cache metadata are outside this retention bound.
    pub max_parameter_binding_cache_bytes: usize,
    /// Commit-epoch lag tolerated before the inline query path recomputes
    /// optimizer statistics. `None` or `Some(0)` refreshes on every committed
    /// data change (freshest plans). Larger values amortize statistics
    /// recomputation over write-heavy workloads; stale statistics only affect
    /// plan choice, never correctness.
    pub optimizer_statistics_inline_commit_lag: Option<u64>,
    pub slow_query_log_capacity: usize,
    pub slow_query_log_threshold_micros: u128,
    pub statement_summary_capacity: usize,
    /// Background-work policy used for this database's full runtime lifetime
    /// by its shared local QoS scheduler.
    pub local_qos_policy: LocalQosPolicy,
    pub runtime_capabilities: hawdb_core::RuntimeCapabilities,
    pub compressed_vector_search_mode: CompressedVectorSearchMode,
    pub adaptive_vector_backend_policy: hawdb_optimizer::AdaptiveVectorBackendPolicy,
}

/// Default host row-count ceiling: no ceiling. Byte budgets remain active.
pub const DEFAULT_MAX_READ_RESULT_ROWS: Option<usize> = None;
pub const DEFAULT_MAX_READ_RESULT_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

/// Per-session result policy. Independent sessions start with no row-count limit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SessionConfig {
    /// Maximum result rows; zero means no session row-count limit.
    /// Payload, execution-memory and explicit host ceilings remain enforced.
    /// A positive ceiling at or above usize::MAX imposes no additional bound:
    /// no in-process result can exceed platform row capacity. On wasm32 this
    /// includes values such as 5,000,000,000; positive host ceilings still apply.
    pub max_result_rows: u64,
}

impl SessionConfig {
    pub(super) fn row_limit(self) -> Option<usize> {
        // No representable result can exceed usize::MAX rows. Avoid asking
        // the execution detector to add one to that effective infinity.
        (self.max_result_rows != 0)
            .then(|| usize::try_from(self.max_result_rows).unwrap_or(usize::MAX))
            .filter(|rows| *rows != usize::MAX)
    }
}

impl DatabaseConfig {
    /// Effective host row-count ceiling. Both zero and `None` disable it.
    pub fn read_result_row_limit(&self) -> Option<usize> {
        self.max_read_result_rows.filter(|rows| *rows != 0)
    }

    pub(super) fn wal_replay_config(&self) -> WalReplayConfig {
        WalReplayConfig {
            max_open_files: self.max_open_files,
            strict_capacity_limit: self.strict_capacity_limit,
            recovery_mode: self.recovery_mode,
            max_entries: self.max_wal_replay_entries,
            max_bytes: self.max_wal_replay_bytes,
            max_branch_sealed_wal_intervals: self.max_branch_sealed_wal_intervals,
            max_quarantine_bytes: self.max_wal_quarantine_bytes,
            max_record_bytes: self.max_wal_record_bytes,
            max_batch_operations: self.max_wal_batch_operations,
            max_checkpoint_encoded_bytes: self.max_checkpoint_encoded_bytes,
            max_checkpoint_decoded_bytes: self.max_checkpoint_decoded_bytes,
            segment_cache_capacity_bytes: self.segment_cache_capacity_bytes,
            max_graph_manifest_open_bytes: self.max_graph_manifest_open_bytes,
            residency_mode: self.storage_residency_mode,
            auto_materialize_checkpoint_bytes: self.auto_materialize_checkpoint_bytes,
            max_out_of_core_delta_bytes: self.max_out_of_core_delta_bytes,
            graph_columnar_shadow_checkpoint: self.graph_columnar_shadow_checkpoint,
            relational_index_mode: self.relational_index_mode,
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            read_only: false,
            namespace_durability_anchor: None,
            automatic_checkpoint_max_age: std::time::Duration::from_secs(60),
            branch_create_recovery_limits: BranchCreateRecoveryLimits::default(),
            max_read_result_rows: DEFAULT_MAX_READ_RESULT_ROWS,
            max_read_result_payload_bytes: Some(DEFAULT_MAX_READ_RESULT_PAYLOAD_BYTES),
            execution_memory: executor::ExecutionMemoryConfig::default(),
            mutation_limits: hawdb_storage::mutation::MutationLimits::default(),
            max_optimizer_groups: None,
            max_relational_join_expressions: None,
            recovery_mode: RecoveryMode::default(),
            max_wal_replay_entries: Some(hawdb_storage::config::DEFAULT_MAX_WAL_REPLAY_ENTRIES),
            max_wal_replay_bytes: Some(hawdb_storage::config::DEFAULT_MAX_WAL_REPLAY_BYTES),
            max_branch_sealed_wal_intervals:
                hawdb_storage::config::DEFAULT_MAX_BRANCH_SEALED_WAL_INTERVALS,
            max_open_files: hawdb_storage::file_descriptors::DEFAULT_MAX_OPEN_FILES,
            strict_capacity_limit: false,
            max_wal_quarantine_bytes: hawdb_storage::config::DEFAULT_MAX_WAL_QUARANTINE_BYTES,
            max_wal_record_bytes: Some(hawdb_storage::config::DEFAULT_MAX_WAL_RECORD_BYTES),
            max_wal_batch_operations: Some(hawdb_storage::config::DEFAULT_MAX_WAL_BATCH_OPERATIONS),
            max_checkpoint_encoded_bytes: Some(
                hawdb_storage::config::DEFAULT_MAX_CHECKPOINT_ENCODED_BYTES,
            ),
            max_checkpoint_decoded_bytes: Some(
                hawdb_storage::config::DEFAULT_MAX_CHECKPOINT_DECODED_BYTES,
            ),
            segment_cache_capacity_bytes:
                hawdb_storage::config::DEFAULT_SEGMENT_CACHE_CAPACITY_BYTES,
            max_graph_manifest_open_bytes:
                hawdb_storage::config::DEFAULT_MAX_GRAPH_MANIFEST_OPEN_BYTES,
            max_relational_index_read_bytes: NonZeroUsize::new(
                hawdb_storage::relational::DEFAULT_RELATIONAL_INDEX_READ_BYTES,
            )
            .expect("default relational index read byte budget is non-zero"),
            max_relational_hydration_bytes: NonZeroUsize::new(
                hawdb_storage::relational::DEFAULT_MAX_RELATIONAL_HYDRATION_BYTES,
            )
            .expect("default relational hydration byte budget is non-zero"),
            storage_residency_mode: hawdb_storage::config::StorageResidencyMode::Auto,
            auto_materialize_checkpoint_bytes:
                hawdb_storage::config::DEFAULT_AUTO_MATERIALIZE_CHECKPOINT_BYTES,
            max_out_of_core_delta_bytes: Some(
                hawdb_storage::config::DEFAULT_MAX_OUT_OF_CORE_DELTA_BYTES,
            ),
            graph_columnar_shadow_checkpoint: false,
            relational_index_mode: hawdb_storage::config::RelationalIndexMode::default(),
            relational_monotonic_append_fast_path: false,
            max_search_projection_change_log_entries: Some(
                DEFAULT_SEARCH_PROJECTION_CHANGE_LOG_MAX_ENTRIES,
            ),
            max_search_projection_change_log_bytes: Some(
                DEFAULT_SEARCH_PROJECTION_CHANGE_LOG_MAX_BYTES,
            ),
            search_projection_relational_change_limits: Default::default(),
            max_plan_cache_entries: Some(DEFAULT_PLAN_CACHE_MAX_ENTRIES),
            max_parameter_binding_cache_bytes: 16 * 1024 * 1024,
            optimizer_statistics_inline_commit_lag: None,
            slow_query_log_capacity: system_sql::DEFAULT_SLOW_QUERY_LOG_CAPACITY,
            slow_query_log_threshold_micros: system_sql::DEFAULT_SLOW_QUERY_LOG_THRESHOLD_MICROS,
            statement_summary_capacity: system_sql::DEFAULT_STATEMENT_SUMMARY_CAPACITY,
            local_qos_policy: LocalQosPolicy::default(),
            runtime_capabilities: crate::compiled_runtime_capabilities(),
            compressed_vector_search_mode: CompressedVectorSearchMode::Disabled,
            adaptive_vector_backend_policy: hawdb_optimizer::AdaptiveVectorBackendPolicy::default(),
        }
    }
}

pub(super) fn restrictive_query_limit(
    configured: Option<usize>,
    requested: Option<usize>,
) -> Option<usize> {
    match (configured, requested) {
        (Some(configured), Some(requested)) => Some(configured.min(requested)),
        (Some(configured), None) => Some(configured),
        (None, Some(requested)) => Some(requested),
        (None, None) => None,
    }
}

pub(super) fn relational_query_limits_with_payload(
    config: &DatabaseConfig,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
) -> crate::relational_sql::RelationalQueryLimits {
    let max_output_rows =
        restrictive_query_limit(config.read_result_row_limit(), max_rows).unwrap_or(usize::MAX);
    let max_output_payload_bytes =
        restrictive_query_limit(config.max_read_result_payload_bytes, max_payload_bytes)
            .unwrap_or(DEFAULT_MAX_READ_RESULT_PAYLOAD_BYTES);
    let max_intermediate_rows = config.read_result_row_limit().unwrap_or(usize::MAX);
    // A blocking relational query first reads the bounded scan projection and
    // then re-reads at most `max_output_rows` selected locators with the final
    // output projection. Budget both phases explicitly so a scan that exactly
    // reaches its intermediate-row limit can still produce an admitted result.
    let max_row_read_rows = max_intermediate_rows.saturating_add(max_output_rows).max(1);
    let max_scan_pages = max_intermediate_rows
        .div_ceil(hawdb_storage::relational::DEFAULT_RELATIONAL_ROW_PAGE_ROWS)
        .max(1);
    let max_row_read_pages = max_scan_pages.saturating_add(max_output_rows).max(1);
    let max_row_read_bytes = max_row_read_pages
        .saturating_mul(hawdb_storage::relational::DEFAULT_RELATIONAL_ROW_PAGE_BYTES)
        .max(1);
    // Logical index traversal includes cache hits and must scale with the
    // statement's admitted intermediate rows. Physical index reads retain a
    // separate fixed I/O ceiling so a warm cache cannot disable query bounds.
    let max_index_read_pages = max_intermediate_rows
        .saturating_mul(
            hawdb_storage::relational::DEFAULT_RELATIONAL_INDEX_READ_TREE_HEIGHT as usize,
        )
        .max(1);
    let max_index_read_bytes = max_index_read_pages
        .saturating_mul(hawdb_storage::index_page::DEFAULT_IMMUTABLE_INDEX_PAGE_BYTES)
        .max(1);
    crate::relational_sql::RelationalQueryLimits {
        max_output_rows,
        max_output_payload_bytes,
        max_intermediate_rows,
        max_candidate_work: max_intermediate_rows,
        hydration: hawdb_storage::relational::RelationalHydrationBudget {
            max_rows: max_row_read_rows,
            max_compressed_bytes: config.max_relational_hydration_bytes.get(),
            max_decompressed_bytes: config.max_relational_hydration_bytes.get(),
            max_memory_bytes: config.max_relational_hydration_bytes.get(),
            ..hawdb_storage::relational::RelationalHydrationBudget::default()
        },
        index_read: hawdb_storage::relational::RelationalIndexReadLimits {
            max_pages: NonZeroUsize::new(max_index_read_pages)
                .expect("relational index query page budget is non-zero"),
            max_rows: NonZeroUsize::new(max_intermediate_rows.max(1))
                .expect("relational index query row budget is non-zero"),
            max_bytes: NonZeroUsize::new(max_index_read_bytes)
                .expect("relational index query byte budget is non-zero"),
            max_file_bytes: config.max_relational_index_read_bytes.get(),
            ..hawdb_storage::relational::RelationalIndexReadLimits::default()
        },
        row_read: hawdb_storage::relational::RelationalRowPageSnapshotReadLimits {
            demand: hawdb_storage::relational::RelationalRowPageDemandReadLimits {
                max_pages: NonZeroUsize::new(max_row_read_pages)
                    .expect("relational row query page budget is non-zero"),
                max_rows: NonZeroUsize::new(max_row_read_rows)
                    .expect("relational row query row budget is non-zero"),
                max_bytes: NonZeroUsize::new(max_row_read_bytes)
                    .expect("relational row query byte budget is non-zero"),
                ..hawdb_storage::relational::RelationalRowPageDemandReadLimits::default()
            },
            ..hawdb_storage::relational::RelationalRowPageSnapshotReadLimits::default()
        },
    }
}

pub(super) fn relational_join_enumeration_config_from_database_config(
    config: &DatabaseConfig,
) -> hawdb_optimizer::RelationalJoinEnumerationConfig {
    let defaults = hawdb_optimizer::RelationalJoinEnumerationConfig::default();
    hawdb_optimizer::RelationalJoinEnumerationConfig {
        max_groups: optimizer_config_from_database_config(config).max_groups,
        max_expressions: config
            .max_relational_join_expressions
            .unwrap_or(defaults.max_expressions),
    }
}

pub(super) fn relational_query_resource_context<'a>(
    config: &'a DatabaseConfig,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
    task_context: Option<&'a hawdb_core::RuntimeTaskContext>,
) -> crate::relational_sql::RelationalQueryResourceContext<'a> {
    crate::relational_sql::RelationalQueryResourceContext::new(
        relational_join_enumeration_config_from_database_config(config),
        relational_query_limits_with_payload(config, max_rows, max_payload_bytes),
        &config.execution_memory,
        task_context,
    )
}

pub(super) fn relational_plan_template_cache_from_database_config(
    config: &DatabaseConfig,
) -> Arc<crate::relational_sql::RelationalPlanTemplateCache> {
    Arc::new(crate::relational_sql::RelationalPlanTemplateCache::new(
        config.max_plan_cache_entries,
    ))
}

pub(super) fn relational_index_read_mode<
    'a,
    R: hawdb_relational::index_runtime::RelationalIndexStoreReader,
>(
    config: &DatabaseConfig,
    store: &'a R,
) -> hawdb_relational::index_runtime::RelationalIndexReadMode<'a, R> {
    match config.relational_index_mode {
        hawdb_storage::config::RelationalIndexMode::Materialized => {
            hawdb_relational::index_runtime::RelationalIndexReadMode::Materialized
        }
        hawdb_storage::config::RelationalIndexMode::Shadow => {
            hawdb_relational::index_runtime::RelationalIndexReadMode::Shadow(store)
        }
        hawdb_storage::config::RelationalIndexMode::DemandPaged => {
            hawdb_relational::index_runtime::RelationalIndexReadMode::DemandPaged(store)
        }
        hawdb_storage::config::RelationalIndexMode::Authoritative => {
            hawdb_relational::index_runtime::RelationalIndexReadMode::Authoritative(store)
        }
    }
}

pub(super) fn optimizer_config_from_database_config(config: &DatabaseConfig) -> OptimizerConfig {
    let mut optimizer = OptimizerConfig::default();
    if let Some(max_groups) = config.max_optimizer_groups {
        optimizer.max_groups = max_groups;
    }
    optimizer
}

pub(super) fn optimizer_from_database_config(config: &DatabaseConfig) -> CascadesOptimizer {
    CascadesOptimizer::with_context(
        OptimizerContext::from_config(optimizer_config_from_database_config(config))
            .with_resource_hints(ResourceHints {
                priority: ResourceHints::default().priority,
                max_memory_bytes: Some(
                    u64::try_from(config.execution_memory.blocking_operator_bytes.get())
                        .unwrap_or(u64::MAX),
                ),
                max_parallelism: executor::MAX_MORSEL_PARALLELISM,
            }),
    )
}
