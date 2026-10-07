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
use crate::search::{
    SearchLexicalSourcePolicy, SearchOutOfCoreGenerationBuildOptions,
    SearchOutOfCoreGenerationWriter,
};
use std::num::NonZeroU64;

/// Explicit development maintenance for an already bootstrapped projection.
/// This mode does not certify production serving or authorize storage cutover.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemIncrementalSearchMaintenanceOptions {
    pub reader_config: SearchOutOfCoreConfig,
    pub build_options: SearchOutOfCoreGenerationBuildOptions,
    pub working_memory_bytes: NonZeroU64,
}

impl Default for NowledgeMemIncrementalSearchMaintenanceOptions {
    fn default() -> Self {
        Self {
            reader_config: SearchOutOfCoreConfig::default(),
            build_options: SearchOutOfCoreGenerationBuildOptions::default(),
            working_memory_bytes: NonZeroU64::new(256 * 1024 * 1024).unwrap(),
        }
    }
}

impl NowledgeMemIncrementalSearchMaintenanceOptions {
    pub(super) fn validate(&self) -> Result<()> {
        if self.build_options.source_graph_commit_epoch.is_some()
            || self
                .build_options
                .import_source_graph_commit_epoch
                .is_some()
        {
            return Err(HawDBError::Semantic(
                "incremental maintenance epochs come from the active projection and unified changefeed".into(),
            ));
        }
        Ok(())
    }
}

impl NowledgeMemOpenOptions {
    /// Selects explicit development maintenance, retaining the configured paths.
    /// Opening requires a complete existing generation; it never recreates an
    /// absent or damaged search projection as an empty index.
    pub fn with_incremental_search_maintenance(
        mut self,
        options: NowledgeMemIncrementalSearchMaintenanceOptions,
    ) -> Self {
        self.search_projection_open_mode =
            NowledgeMemSearchProjectionOpenMode::IncrementalMaintenance(Box::new(options));
        self
    }
}

impl NowledgeMemOutOfCoreSearchProjection {
    pub(super) fn open_incremental(
        path: &Path,
        options: NowledgeMemIncrementalSearchMaintenanceOptions,
    ) -> Result<Self> {
        options.validate()?;
        let reader = SearchOutOfCoreReader::open_with_source_policy(
            path,
            options.reader_config.clone(),
            options.build_options.analyzer_lexicon.clone(),
            SearchLexicalSourcePolicy::new(
                options.build_options.lexical_max_document_source_bytes,
            )?
            .with_max_document_tokens(options.build_options.lexical_max_document_tokens),
        )?;
        Ok(Self {
            reader,
            maintenance: Some((path.to_path_buf(), options)),
        })
    }

    fn refresh_reader(&mut self) -> Result<crate::search::SearchOutOfCoreRefreshReport> {
        self.maintenance.as_ref().ok_or_else(|| {
            HawDBError::Semantic("qualified out-of-core search is not a maintenance target".into())
        })?;
        self.reader.refresh()
    }
}

impl NowledgeMemEmbeddedStoreHandle {
    /// Attempts one bounded compaction on the host's background worker.
    /// Large artifact reads run without holding the store's write lock. Queries
    /// keep their pinned generation, and a concurrent publisher makes the
    /// compaction fail its generation check instead of replacing newer data.
    pub fn compact_search_projection_segments_with_context(
        &self,
        policy: crate::search::SearchOutOfCoreSegmentCompactionPolicy,
        hint: hawdb_qos::BackgroundWorkHint,
        task: RuntimeTaskContext,
    ) -> Result<crate::search::ScheduledSearchOutOfCoreSegmentCompactionReport> {
        let permit = self.admit_search_projection_maintenance(0)?;
        let (path, options, scheduler) = {
            let store = self.read_store()?;
            store.graph.check_runtime_context(&task)?;
            let (path, options) = store
                .out_of_core_search_projection
                .as_ref()
                .and_then(|projection| projection.maintenance.as_ref())
                .ok_or_else(|| {
                    HawDBError::Semantic(
                        "segment compaction requires incremental search maintenance".into(),
                    )
                })?;
            (
                path.clone(),
                options.clone(),
                store.graph.database().local_qos_scheduler(),
            )
        };
        let projection =
            NowledgeMemOutOfCoreSearchProjection::open_incremental(&path, options.clone())?;
        let task = permit.bind_task_context(task).with_memory_reservation(
            hawdb_core::RuntimeMemoryReservation::new(options.working_memory_bytes.get(), 0),
        );
        let result =
            SearchOutOfCoreGenerationWriter::compact_scheduled_background_segments_with_context(
                &projection.reader,
                &scheduler,
                policy,
                hint,
                options.build_options,
                task,
            );
        // Publication may have completed even if its response was lost. Refresh
        // on execution errors too; never infer rollback from an error alone.
        let mut store = self.write_store()?;
        store.refresh_incremental_search_reader()?;
        result
    }

    pub(super) fn admit_search_projection_maintenance(
        &self,
        input_bytes: usize,
    ) -> Result<RuntimePermit> {
        let store = self.read_store()?;
        let configured = store.incremental_search_memory_bytes();
        let working = u64::try_from(
            store
                .graph
                .database()
                .config()
                .execution_memory
                .blocking_operator_bytes
                .get(),
        )
        .unwrap_or(u64::MAX)
        .max(configured);
        store.graph.try_admit_runtime(
            RuntimeWorkRequest::background_maintenance(
                working.saturating_add(u64::try_from(input_bytes).unwrap_or(u64::MAX)),
            )
            .with_io_slots(1)
            .with_blocking(true),
        )
    }
}

impl NowledgeMemEmbeddedStore {
    fn refresh_incremental_search_reader(&mut self) -> Result<()> {
        let projection = self.out_of_core_search_projection.as_mut().ok_or_else(|| {
            HawDBError::Semantic("incremental search maintenance is not configured".into())
        })?;
        match projection.refresh_reader() {
            Ok(_) => {}
            Err(error) => {
                // Keep published evidence on disk, but do not serve a stale
                // generation after failing to attach the durable head.
                self.out_of_core_search_projection = None;
                return Err(error);
            }
        }
        Ok(())
    }

    fn incremental_search_memory_bytes(&self) -> u64 {
        self.out_of_core_search_projection
            .as_ref()
            .and_then(|projection| projection.maintenance.as_ref())
            .map_or(0, |(_, options)| options.working_memory_bytes.get())
    }

    pub(super) fn incremental_search_enabled(&self) -> bool {
        self.incremental_search_memory_bytes() != 0
    }

    pub(super) fn apply_incremental_search_delta(
        &mut self,
        delta: SearchProjectionDelta,
        task: RuntimeTaskContext,
    ) -> Result<SearchProjectionDeltaReport> {
        // A previous response may have been lost after publication. Re-read the
        // durable head before retrying instead of assuming that it rolled back.
        self.refresh_incremental_search_reader()?;
        let projection = self.out_of_core_search_projection.as_mut().ok_or_else(|| {
            HawDBError::Semantic("incremental search maintenance is not configured".into())
        })?;
        let (_, options) = projection.maintenance.as_ref().ok_or_else(|| {
            HawDBError::Semantic("qualified search cannot publish maintenance deltas".into())
        })?;
        let task = task.with_memory_reservation(hawdb_core::RuntimeMemoryReservation::new(
            options.working_memory_bytes.get(),
            0,
        ));
        let result = SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
            &projection.reader,
            delta,
            options.build_options.clone(),
            task,
        )?
        .finish();
        self.refresh_incremental_search_reader()?;
        let (report, _, _) = result?;
        Ok(report)
    }

    pub(super) fn catch_up_incremental_search_with_batch_hydrator<F>(
        &mut self,
        max_changes: usize,
        max_projection: usize,
        max_batches: usize,
        mut hydrator: F,
        task: RuntimeTaskContext,
    ) -> Result<SearchProjectionCatchUpReport>
    where
        F: FnMut(
            &mut DatabaseReadTransaction,
            &SearchProjectionChangeBatch,
        ) -> Result<SearchProjectionRelationalDelta>,
    {
        if max_changes == 0 || max_projection == 0 || max_batches == 0 {
            return Err(HawDBError::Semantic(
                "incremental catch-up requires nonzero change, projection and batch limits".into(),
            ));
        }
        self.refresh_incremental_search_reader()?;
        let projection = self.out_of_core_search_projection.as_ref().ok_or_else(|| {
            HawDBError::Semantic("incremental search maintenance is not configured".into())
        })?;
        let start = projection.freshness();
        let graph_epoch = self.graph.database().commit_epoch()?;
        let mut batch_count = 0usize;
        let mut operation_count = 0usize;
        while batch_count < max_batches {
            self.graph.check_runtime_context(&task)?;
            let cursor = self
                .search_projection_freshness()
                .and_then(|freshness| freshness.durable_source_graph_commit_epoch)
                .unwrap_or(0);
            let Some(mut batch) = self
                .graph
                .database()
                .build_search_projection_change_batch_after(cursor, Some(max_changes))?
            else {
                break;
            };
            let count = batch.operation_count();
            let mut snapshot = self.graph.database().begin_read_transaction()?;
            let relational = hydrator(&mut snapshot, &batch)?;
            batch.graph_delta_mut().max_operations = Some(max_projection);
            let delta = self
                .graph
                .database()
                .build_search_projection_change_delta(batch, relational)?;
            drop(snapshot);
            self.apply_incremental_search_delta(delta, task.clone())?;
            batch_count = batch_count.saturating_add(1);
            operation_count = operation_count.saturating_add(count);
        }
        let end = self.search_projection_freshness().ok_or_else(|| {
            HawDBError::Storage("incremental search reader detached during catch-up".into())
        })?;
        Ok(crate::search::search_projection_catch_up_report(
            graph_epoch,
            start,
            end,
            batch_count,
            operation_count,
        ))
    }

    pub(super) fn admit_incremental_search_maintenance(
        &self,
        input_bytes: usize,
    ) -> Result<RuntimePermit> {
        self.graph.try_admit_runtime(
            RuntimeWorkRequest::background_maintenance(
                self.incremental_search_memory_bytes()
                    .saturating_add(u64::try_from(input_bytes).unwrap_or(u64::MAX)),
            )
            .with_io_slots(1)
            .with_blocking(true),
        )
    }
}
