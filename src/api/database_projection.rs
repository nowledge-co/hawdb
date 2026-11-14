// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Database projection implementation behind the embedded facade.

use super::*;

impl Database {
    /// Returns the durable, owner-scoped projection generation catalog.
    ///
    /// Candidate construction and publication stay on the embedded library
    /// path. In-memory databases do not expose a durable generation catalog.
    pub fn projection_generation_store(
        &self,
    ) -> Result<hawdb_storage::projection_generation::ProjectionGenerationStore> {
        self.runtime.get()?.store.projection_generation_store()
    }

    /// Encodes a row for a relational projection table using the durable
    /// PostgreSQL schema registered in this database.
    pub fn encode_projection_relational_row(
        &self,
        table: &str,
        row: hawdb_storage::relational::RelationalRow,
    ) -> Result<hawdb_storage::projection_generation::ProjectionGenerationMember> {
        let runtime = self.runtime.get()?;
        let schema = runtime
            .store
            .relational_state()
            .table_schema(table)
            .ok_or_else(|| {
                HawDBError::Semantic(format!(
                    "projection relational table {table} has no durable PostgreSQL schema"
                ))
            })?;
        hawdb_storage::projection_generation::encode_projection_relational_member(schema, row)
            .map_err(|error| match error {
                hawdb_storage::projection_generation::ProjectionGenerationError::Corruption(
                    message,
                ) => HawDBError::StorageIntegrity(message),
                error => HawDBError::Execution(error.to_string()),
            })
    }

    pub(crate) fn search_projection_changefeed_status(
        &self,
    ) -> Result<hawdb_storage::projection::SearchProjectionChangefeedStatus> {
        Ok({
            self.runtime
                .get()?
                .store
                .search_projection_changefeed_status()
        })
    }

    pub fn search_projection_changefeed_readiness(
        &self,
        search_index: &SearchIndex,
        require_restart_recoverable: bool,
        max_operations: Option<usize>,
    ) -> Result<hawdb_storage::projection::SearchProjectionChangefeedReadiness> {
        Ok({
            let freshness = search_index.projection_freshness();
            self.runtime
                .get()?
                .store
                .search_projection_changefeed_status()
                .readiness_after(
                    freshness.source_graph_commit_epoch,
                    freshness.durable_source_graph_commit_epoch,
                    require_restart_recoverable,
                    max_operations,
                )
        })
    }

    pub fn rebuild_search_projection(
        &self,
        search_index: &mut SearchIndex,
        options: SearchRebuildOptions,
    ) -> Result<SearchRebuildSummary> {
        search_index.rebuild_from_graph(
            &self.runtime.get()?.catalog,
            &self.runtime.get()?.store,
            options,
        )
    }

    pub fn search_projection_rebuild_background_work_plan(
        &self,
        search_index: &SearchIndex,
        hint: BackgroundWorkHint,
    ) -> Result<Option<BackgroundWorkPlan>> {
        Ok(search_index.rebuild_background_work_plan(&self.runtime.get()?.store, hint))
    }

    pub fn rebuild_background_search_projection(
        &self,
        search_index: &mut SearchIndex,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        options: SearchRebuildOptions,
    ) -> Result<SearchDerivedArtifactReport> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        search_index.rebuild_background_derived_artifacts(
            policy,
            state,
            &self.runtime.get()?.catalog,
            &self.runtime.get()?.store,
            options,
        )
    }

    pub fn rebuild_scheduled_background_search_projection(
        &self,
        search_index: &mut SearchIndex,
        options: SearchRebuildOptions,
    ) -> Result<SearchDerivedArtifactReport> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        let scheduler = self.local_qos_scheduler_for_work();
        search_index.rebuild_scheduled_background_derived_artifacts(
            &scheduler,
            &self.runtime.get()?.catalog,
            &self.runtime.get()?.store,
            options,
        )
    }

    pub fn repair_search_projection_metadata(
        &self,
        search_index: &mut SearchIndex,
        options: MetadataRepairOptions,
    ) -> Result<MetadataRepairSummary> {
        search_index.repair_metadata_from_graph(
            &self.runtime.get()?.catalog,
            &self.runtime.get()?.store,
            options,
        )
    }

    pub fn search_projection_metadata_repair_background_work_plan(
        &self,
        search_index: &SearchIndex,
        hint: BackgroundWorkHint,
    ) -> Result<Option<BackgroundWorkPlan>> {
        Ok(search_index.metadata_repair_background_work_plan(&self.runtime.get()?.store, hint))
    }

    pub fn repair_background_search_projection_metadata(
        &self,
        search_index: &mut SearchIndex,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        options: MetadataRepairOptions,
        estimated_operations: usize,
    ) -> Result<MetadataRepairSummary> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        search_index.repair_background_metadata_from_graph(
            policy,
            state,
            &self.runtime.get()?.catalog,
            &self.runtime.get()?.store,
            options,
            estimated_operations,
        )
    }

    pub fn repair_scheduled_background_search_projection_metadata(
        &self,
        search_index: &mut SearchIndex,
        options: MetadataRepairOptions,
        estimated_operations: usize,
    ) -> Result<MetadataRepairSummary> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        let scheduler = self.local_qos_scheduler_for_work();
        search_index.repair_scheduled_background_metadata_from_graph(
            &scheduler,
            &self.runtime.get()?.catalog,
            &self.runtime.get()?.store,
            options,
            estimated_operations,
        )
    }

    pub fn search_projection_delta_background_work_plan(
        &self,
        delta: &SearchProjectionDelta,
        hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        delta.background_work_plan(hint)
    }

    pub fn search_projection_graph_delta_background_work_plan(
        &self,
        request: &SearchProjectionGraphDeltaRequest,
        hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        request.background_work_plan(hint)
    }

    pub fn search_projection_graph_delta_freshness_background_work_plan(
        &self,
        search_index: &SearchIndex,
        request: &SearchProjectionGraphDeltaRequest,
        mut hint: BackgroundWorkHint,
    ) -> Result<Option<BackgroundWorkPlan>> {
        Ok({
            if hint.recent_delta_operations == 0 {
                hint.recent_delta_operations = request.operation_count();
            }
            if hint.source_graph_commit_lag == 0 {
                hint.source_graph_commit_lag = search_projection_commit_lag(
                    search_index,
                    self.runtime
                        .get()?
                        .store
                        .search_projection_changefeed_status()
                        .required_projection_commit_epoch(),
                );
            }
            if request.operation_count() == 0
                && hint.source_graph_commit_lag > 0
                && request.complete_through_graph_commit_epoch.is_some()
            {
                if hint.recent_delta_operations == 0 {
                    hint.recent_delta_operations = 1;
                }
                return Ok(Some(BackgroundWorkPlan::background(
                    WorkClass::Projection,
                    1,
                    hint,
                )));
            }
            request.background_work_plan(hint)
        })
    }

    pub fn search_projection_freshness_lag_background_work_plan(
        &self,
        search_index: &SearchIndex,
        mut hint: BackgroundWorkHint,
    ) -> Result<Option<BackgroundWorkPlan>> {
        Ok({
            let source_graph_commit_lag = search_projection_commit_lag(
                search_index,
                self.runtime
                    .get()?
                    .store
                    .search_projection_changefeed_status()
                    .required_projection_commit_epoch(),
            );
            if source_graph_commit_lag == 0 {
                return Ok(None);
            }
            let operation_count = usize::try_from(source_graph_commit_lag).unwrap_or(usize::MAX);
            if hint.recent_delta_operations == 0 {
                hint.recent_delta_operations = operation_count;
            }
            if hint.source_graph_commit_lag == 0 {
                hint.source_graph_commit_lag = source_graph_commit_lag;
            }
            Some(BackgroundWorkPlan::background(
                WorkClass::Projection,
                operation_count,
                hint,
            ))
        })
    }

    pub fn build_search_projection_graph_delta_request_after(
        &self,
        source_graph_commit_epoch: u64,
        max_operations: Option<usize>,
    ) -> Result<Option<SearchProjectionGraphDeltaRequest>> {
        let Some(batch) = self.build_search_projection_change_batch_after(
            source_graph_commit_epoch,
            max_operations,
        )?
        else {
            return Ok(None);
        };
        if batch.has_relational_changes() {
            return Err(HawDBError::Storage(format!(
                "search projection commits through epoch {} contain relational primary-key changes; use the unified search projection changefeed and publish one combined projection delta",
                batch.complete_through_commit_epoch().unwrap_or(source_graph_commit_epoch)
            )));
        }
        Ok(Some(batch.into_graph_delta()))
    }

    /// Proves that no graph commit in `(source_commit_epoch,
    /// target_commit_epoch]` changed anything the search projection indexes,
    /// using only this database's own captured search change log.
    ///
    /// `target_commit_epoch` must be the current graph commit epoch. The
    /// whole check holds this branch runtime's read guard
    /// (`BranchRuntimeCell::get`, an `RwLock` read on the admitted runtime):
    /// every graph commit needs that lock for writing, so the epoch and the
    /// node state read below are exactly the state at the target, and no
    /// commit can land until the check returns. The epoch is re-read at the
    /// end as a defensive check.
    ///
    /// The range qualifies only when the change log covers all of it and every
    /// commit in it has an empty projection capture:
    /// - no search document deletion;
    /// - no relational change HawDB cannot attribute to tables (snapshot
    ///   replacement, multiple transactions, missing capture);
    /// - no relational primary-key change outside `non_projection_tables`,
    ///   the host's open-time declaration (changes to declared tables are
    ///   counted in the result);
    /// - no captured node that exists at the target whose label maps to a
    ///   search projection kind.
    ///
    /// Labels are fixed at node creation. A captured node that no longer exists
    /// was deleted inside the range (the target is the current epoch): if it
    /// was projected its deletion recorded the document deletion, and if it
    /// was created inside the range it never reached the projection.
    ///
    /// Refusals are [`HawDBError::SearchProjectionEpochAdvanceRefused`]:
    /// `RangeUnavailable` when the target is not the current commit epoch,
    /// `ChangeLogGap` when the retained log starts after the source epoch, and
    /// `ProjectionChanged` naming the first offending commit.
    pub fn verify_search_projection_epoch_advance(
        &self,
        source_commit_epoch: u64,
        target_commit_epoch: u64,
        non_projection_tables: &BTreeSet<String>,
    ) -> Result<SearchProjectionEpochAdvanceVerification> {
        use hawdb_core::SearchProjectionEpochAdvanceRefusal as Refusal;
        let refused = |reason, message: String| HawDBError::SearchProjectionEpochAdvanceRefused {
            reason,
            source_graph_commit_epoch: Some(source_commit_epoch),
            target_graph_commit_epoch: target_commit_epoch,
            message,
        };
        let not_current = |current_epoch: u64| {
            refused(
                Refusal::RangeUnavailable,
                format!(
                    "target epoch {target_commit_epoch} is not the graph commit epoch {current_epoch}; an advance only reaches the current epoch"
                ),
            )
        };
        let runtime = self.runtime.get()?;
        let current_epoch = runtime.store.commit_epoch();
        if target_commit_epoch != current_epoch {
            return Err(not_current(current_epoch));
        }
        let mut verification = SearchProjectionEpochAdvanceVerification::default();
        if target_commit_epoch <= source_commit_epoch {
            return Ok(verification);
        }
        let resume_floor = runtime.store.search_projection_change_log_start_epoch();
        if source_commit_epoch < resume_floor {
            return Err(refused(
                Refusal::ChangeLogGap,
                format!(
                    "the search change log starts at commit epoch {resume_floor}, after source epoch {source_commit_epoch}; commits in the range were not captured or were trimmed"
                ),
            ));
        }
        for change in runtime
            .store
            .search_projection_changes_after(source_commit_epoch)
        {
            if change.commit_epoch > target_commit_epoch {
                break;
            }
            verification.checked_change_log_entries =
                verification.checked_change_log_entries.saturating_add(1);
            let epoch = change.commit_epoch;
            let changed = |what: String| {
                refused(
                    Refusal::ProjectionChanged,
                    format!("graph commit {epoch} {what}"),
                )
            };
            match &change.relational_primary_key_changes {
                hawdb_storage::relational::RelationalPrimaryKeyChangeCapture::RequiresRebuild {
                    reason,
                } => {
                    return Err(changed(format!(
                        "has a relational change that cannot be attributed to tables ({reason:?})"
                    )));
                }
                hawdb_storage::relational::RelationalPrimaryKeyChangeCapture::Captured {
                    tables,
                    ..
                } => {
                    for table in tables.iter().filter(|table| !table.primary_keys.is_empty()) {
                        if !non_projection_tables.contains(&table.table) {
                            return Err(changed(format!(
                                "changed rows of relational table {}, which is not declared a non-projection table",
                                table.table
                            )));
                        }
                        verification.allowed_relational_change_count = verification
                            .allowed_relational_change_count
                            .saturating_add(table.primary_keys.len());
                    }
                }
            }
            if let Some(document) = change.delete_document_ids.first() {
                return Err(changed(format!("deleted search document {document}")));
            }
            for node_id in &change.upsert_node_ids {
                let Some(node) = runtime.store.node_owned(NodeId(*node_id))? else {
                    continue;
                };
                if let Some(document) = hawdb_storage::projection::projection_document_id_for_node(
                    &runtime.catalog,
                    &node,
                ) {
                    return Err(changed(format!(
                        "changed node {node_id}, projected as search document {document}"
                    )));
                }
            }
        }
        let final_epoch = runtime.store.commit_epoch();
        if final_epoch != target_commit_epoch {
            return Err(not_current(final_epoch));
        }
        Ok(verification)
    }

    pub fn build_search_projection_change_batch_after(
        &self,
        source_commit_epoch: u64,
        max_operations: Option<usize>,
    ) -> Result<Option<SearchProjectionChangeBatch>> {
        let runtime = self.runtime.get()?;
        let current_epoch = runtime.store.commit_epoch();
        if source_commit_epoch >= current_epoch {
            return Ok(None);
        }
        let change_log_start_epoch = runtime.store.search_projection_change_log_start_epoch();
        if source_commit_epoch < change_log_start_epoch {
            return Err(HawDBError::Storage(format!(
                "search projection change log starts at commit epoch {change_log_start_epoch}; requested source commit epoch {source_commit_epoch}; full search projection rebuild required (EmbeddedStoreHandle::rebuild_out_of_core_search_projection rebuilds an incremental projection from the graph into a new directory)"
            )));
        }

        let mut upsert_node_ids = BTreeSet::new();
        let mut delete_document_ids = BTreeSet::new();
        let mut relational_primary_keys =
            BTreeMap::<String, BTreeSet<hawdb_storage::relational::RelationalKey>>::new();
        let mut complete_through_commit_epoch = source_commit_epoch;
        let mut truncated_by_budget = false;
        for change in runtime
            .store
            .search_projection_changes_after(source_commit_epoch)
        {
            let tables = match &change.relational_primary_key_changes {
                hawdb_storage::relational::RelationalPrimaryKeyChangeCapture::Captured {
                    tables,
                    ..
                } => tables,
                hawdb_storage::relational::RelationalPrimaryKeyChangeCapture::RequiresRebuild {
                    reason,
                } => {
                    return Err(HawDBError::Storage(format!(
                        "search projection relational change at commit epoch {} requires a full rebuild: {reason:?}",
                        change.commit_epoch
                    )));
                }
            };
            // The raw native feed retains deleted keys for fixed projectors.
            // This compatibility batch promises live upsert candidates and
            // separately retains every legacy document tombstone.
            let mut additional_live_nodes = 0usize;
            for node_id in &change.upsert_node_ids {
                if !upsert_node_ids.contains(node_id)
                    && runtime.store.contains_node(NodeId(*node_id))?
                {
                    additional_live_nodes += 1;
                }
            }
            let additional_operation_count = additional_live_nodes
                .saturating_add(
                    change
                        .delete_document_ids
                        .iter()
                        .filter(|document_id| !delete_document_ids.contains(*document_id))
                        .count(),
                )
                .saturating_add(
                    tables
                        .iter()
                        .map(|table| {
                            let existing = relational_primary_keys.get(&table.table);
                            table
                                .primary_keys
                                .iter()
                                .filter(|key| existing.is_none_or(|keys| !keys.contains(*key)))
                                .count()
                        })
                        .sum::<usize>(),
                );
            let next_operation_count = upsert_node_ids
                .len()
                .saturating_add(delete_document_ids.len())
                .saturating_add(
                    relational_primary_keys
                        .values()
                        .map(BTreeSet::len)
                        .sum::<usize>(),
                )
                .saturating_add(additional_operation_count);
            if let Some(limit) = max_operations
                && next_operation_count > limit
            {
                if complete_through_commit_epoch == source_commit_epoch {
                    return Err(HawDBError::Storage(format!(
                        "search projection change at commit epoch {} requires {next_operation_count} operations, exceeding configured per-batch limit {limit}",
                        change.commit_epoch
                    )));
                }
                truncated_by_budget = true;
                break;
            }
            for node_id in change.upsert_node_ids {
                if runtime.store.contains_node(NodeId(node_id))? {
                    upsert_node_ids.insert(node_id);
                }
            }
            delete_document_ids.extend(change.delete_document_ids);
            for table in tables {
                relational_primary_keys
                    .entry(table.table.clone())
                    .or_default()
                    .extend(table.primary_keys.iter().cloned());
            }
            complete_through_commit_epoch = change.commit_epoch;
        }
        if !truncated_by_budget {
            complete_through_commit_epoch = current_epoch;
        }

        Ok(Some(SearchProjectionChangeBatch::new(
            SearchProjectionGraphDeltaRequest {
                upsert_node_ids: upsert_node_ids.into_iter().collect(),
                delete_document_ids: delete_document_ids.into_iter().collect(),
                max_operations,
                complete_through_graph_commit_epoch: Some(complete_through_commit_epoch),
            },
            relational_primary_keys
                .into_iter()
                .map(|(table, primary_keys)| {
                    hawdb_storage::relational::RelationalTablePrimaryKeyChanges {
                        table,
                        primary_keys: primary_keys.into_iter().collect(),
                    }
                })
                .collect(),
        )))
    }

    pub fn build_search_projection_graph_delta_request_from_freshness(
        &self,
        search_index: &SearchIndex,
        max_operations: Option<usize>,
    ) -> Result<Option<SearchProjectionGraphDeltaRequest>> {
        self.build_search_projection_graph_delta_request_after(
            search_index
                .projection_freshness()
                .source_graph_commit_epoch
                .unwrap_or(0),
            max_operations,
        )
    }

    pub fn build_search_projection_change_batch_from_freshness(
        &self,
        search_index: &SearchIndex,
        max_operations: Option<usize>,
    ) -> Result<Option<SearchProjectionChangeBatch>> {
        self.build_search_projection_change_batch_after(
            search_index
                .projection_freshness()
                .source_graph_commit_epoch
                .unwrap_or(0),
            max_operations,
        )
    }

    /// Hydrate graph changes from the current source view. A declared complete-through
    /// watermark must equal that view's commit epoch; a changefeed prefix does not
    /// provide historical row contents. Requests without a watermark do not claim
    /// complete coverage and preserve their existing ad hoc behavior.
    pub fn build_search_projection_graph_delta(
        &self,
        request: &SearchProjectionGraphDeltaRequest,
    ) -> Result<SearchProjectionDelta> {
        let result = search_projection_graph_delta_for(
            &self.runtime.get()?.catalog,
            &self.runtime.get()?.store,
            request,
        );
        self.runtime.get()?.store.poison_on_storage_error(&result);
        result
    }

    pub fn apply_search_projection_delta(
        &self,
        search_index: &mut SearchIndex,
        delta: SearchProjectionDelta,
    ) -> Result<SearchProjectionDeltaReport> {
        search_index.apply_projection_delta(delta)
    }

    pub fn apply_search_projection_graph_delta(
        &self,
        search_index: &mut SearchIndex,
        request: SearchProjectionGraphDeltaRequest,
    ) -> Result<SearchProjectionDeltaReport> {
        let delta = self.build_search_projection_graph_delta(&request)?;
        search_index.apply_projection_delta(delta)
    }

    pub fn apply_search_projection_change_batch(
        &self,
        search_index: &mut SearchIndex,
        batch: SearchProjectionChangeBatch,
        relational: SearchProjectionRelationalDelta,
    ) -> Result<SearchProjectionDeltaReport> {
        let delta = self.build_search_projection_change_delta(batch, relational)?;
        search_index.apply_projection_delta(delta)
    }

    /// Builds the complete graph-and-host delta without publishing its watermark.
    /// Both full-residency and incremental persistence must pass this same
    /// relational coverage and source-epoch proof before applying a batch.
    pub fn build_search_projection_change_delta(
        &self,
        batch: SearchProjectionChangeBatch,
        relational: SearchProjectionRelationalDelta,
    ) -> Result<SearchProjectionDelta> {
        let expected_primary_key_count = batch
            .relational_primary_key_changes()
            .iter()
            .map(|table| table.primary_keys.len())
            .fold(0usize, usize::saturating_add);
        if relational.processed_primary_key_count != expected_primary_key_count {
            return Err(HawDBError::Storage(format!(
                "search projection relational delta processed {} primary keys, expected {expected_primary_key_count}; projection watermark was not published",
                relational.processed_primary_key_count
            )));
        }
        if relational.delta.source_graph_commit_epoch.is_some() {
            return Err(HawDBError::Storage(
                "search projection relational delta must not publish its own source epoch"
                    .to_string(),
            ));
        }
        let complete_through_commit_epoch = batch.complete_through_commit_epoch();
        let max_operations = batch.graph_delta().max_operations;
        let mut graph = self.build_search_projection_graph_delta(batch.graph_delta())?;
        let SearchProjectionDelta {
            upserts,
            deletes,
            max_operations: _,
            source_graph_commit_epoch: _,
        } = relational.delta;
        graph.upserts.extend(upserts);
        graph.deletes.extend(deletes);
        graph.max_operations = max_operations;
        graph.source_graph_commit_epoch = complete_through_commit_epoch;
        Ok(graph)
    }

    pub fn apply_background_search_projection_delta(
        &self,
        search_index: &mut SearchIndex,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        delta: SearchProjectionDelta,
    ) -> Result<SearchProjectionDeltaReport> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        search_index.apply_background_projection_delta(policy, state, delta)
    }

    pub fn apply_background_search_projection_graph_delta(
        &self,
        search_index: &mut SearchIndex,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        request: SearchProjectionGraphDeltaRequest,
    ) -> Result<SearchProjectionDeltaReport> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        match policy.admit(state, &request.background_work_request()) {
            QosAdmission::Admit => self.apply_search_projection_graph_delta(search_index, request),
            QosAdmission::Defer { reason, .. } => Err(HawDBError::Storage(format!(
                "background search projection graph delta deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(HawDBError::Storage(format!(
                "background search projection graph delta rejected: {reason}"
            ))),
        }
    }

    pub fn apply_scheduled_background_search_projection_delta(
        &self,
        search_index: &mut SearchIndex,
        delta: SearchProjectionDelta,
    ) -> Result<SearchProjectionDeltaReport> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        let scheduler = self.local_qos_scheduler_for_work();
        search_index.apply_scheduled_background_projection_delta(&scheduler, delta)
    }

    pub fn apply_scheduled_background_search_projection_graph_delta(
        &self,
        search_index: &mut SearchIndex,
        request: SearchProjectionGraphDeltaRequest,
    ) -> Result<SearchProjectionDeltaReport> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        let scheduler = self.local_qos_scheduler_for_work();
        let permit = match scheduler.try_start(request.background_work_request()) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(HawDBError::Storage(format!(
                    "background search projection graph delta deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(HawDBError::Storage(format!(
                    "background search projection graph delta rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

        let result = self.apply_search_projection_graph_delta(search_index, request);
        permit.finish_with_outcome(result.is_ok());
        result
    }
}

/// What [`Database::verify_search_projection_epoch_advance`] inspected.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SearchProjectionEpochAdvanceVerification {
    /// Retained search change-log entries in the advanced range.
    pub checked_change_log_entries: usize,
    /// Relational primary-key changes in the range allowed only because their
    /// tables are declared non-projection tables at open.
    pub allowed_relational_change_count: usize,
}
