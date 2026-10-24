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

    pub fn build_search_projection_change_batch_after(
        &self,
        source_commit_epoch: u64,
        max_operations: Option<usize>,
    ) -> Result<Option<SearchProjectionChangeBatch>> {
        let current_epoch = self.runtime.get()?.store.commit_epoch();
        if source_commit_epoch >= current_epoch {
            return Ok(None);
        }
        let change_log_start_epoch = self
            .runtime
            .get()?
            .store
            .search_projection_change_log_start_epoch();
        if source_commit_epoch < change_log_start_epoch {
            return Err(HawDBError::Storage(format!(
                "search projection change log starts at commit epoch {change_log_start_epoch}; requested source commit epoch {source_commit_epoch}; full search projection rebuild required"
            )));
        }

        let mut upsert_node_ids = BTreeSet::new();
        let mut delete_document_ids = BTreeSet::new();
        let mut relational_primary_keys =
            BTreeMap::<String, BTreeSet<hawdb_storage::relational::RelationalKey>>::new();
        let mut complete_through_commit_epoch = source_commit_epoch;
        let mut truncated_by_budget = false;
        for change in self
            .runtime
            .get()?
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
            let additional_operation_count = change
                .upsert_node_ids
                .iter()
                .filter(|node_id| !upsert_node_ids.contains(*node_id))
                .count()
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
            upsert_node_ids.extend(change.upsert_node_ids);
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
