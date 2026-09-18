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

use super::Database;
use crate::search::{
    run_scheduled_search_projection_catch_up, run_search_projection_catch_up,
    validate_search_projection_catch_up_request, SearchIndex,
};
use crate::{
    DatabaseReadTransaction, HawDBError, Result, SearchProjectionChangeBatch,
    SearchProjectionRelationalDelta,
};
pub use hawdb_search::{
    ScheduledSearchProjectionCatchUpReport, SearchProjectionCatchUpReport,
    SearchProjectionCatchUpStopReason,
};

impl Database {
    pub fn catch_up_search_projection(
        &self,
        search_index: &mut SearchIndex,
        max_operations_per_batch: usize,
        max_batches: usize,
    ) -> Result<SearchProjectionCatchUpReport> {
        self.catch_up_search_projection_with_batch_applier(
            search_index,
            max_operations_per_batch,
            max_batches,
            |database, search_index, max_operations| {
                let Some(request) = database
                    .build_search_projection_graph_delta_request_from_freshness(
                        search_index,
                        Some(max_operations),
                    )?
                else {
                    return Ok(None);
                };
                let report = database.apply_search_projection_graph_delta(search_index, request)?;
                Ok(Some(report.operation_count))
            },
        )
    }

    /// Catches a persistent search projection up through whole unified
    /// graph-and-relational changefeed commits.
    ///
    /// The hydrator receives the exact selected batch and one pinned database
    /// read transaction. It must account for every relational primary key in
    /// the batch. HawDB validates that count before applying either the graph
    /// or relational delta, then checkpoints the combined projection before
    /// selecting another batch. Hydration errors and incomplete accounting do
    /// not advance the projection watermark.
    pub fn catch_up_search_projection_with_relational<F>(
        &self,
        search_index: &mut SearchIndex,
        max_operations_per_batch: usize,
        max_batches: usize,
        mut relational_hydrator: F,
    ) -> Result<SearchProjectionCatchUpReport>
    where
        F: FnMut(
            &mut DatabaseReadTransaction,
            &SearchProjectionChangeBatch,
        ) -> Result<SearchProjectionRelationalDelta>,
    {
        self.catch_up_search_projection_with_batch_hydrator(
            search_index,
            max_operations_per_batch,
            max_operations_per_batch,
            max_batches,
            |snapshot, batch| {
                if batch.has_relational_changes() {
                    relational_hydrator(snapshot, batch)
                } else {
                    Ok(SearchProjectionRelationalDelta::default())
                }
            },
        )
    }

    /// Catches a persistent search projection up through whole unified
    /// graph-and-relational changefeed commits while allowing the host to
    /// derive additional projection work from every selected batch.
    ///
    /// Unlike [`Self::catch_up_search_projection_with_relational`], the
    /// hydrator runs for graph-only batches. This supports bounded host-owned
    /// dependency fan-out without moving application projection semantics into
    /// HawDB. The hydrator must still account for every relational primary key
    /// in the batch. `max_change_operations_per_batch` bounds changefeed
    /// selection, while `max_projection_operations_per_batch` independently
    /// bounds the combined graph and host-derived projection delta. Hydration
    /// errors, output-budget overflow, and incomplete accounting do not advance
    /// the projection watermark.
    pub fn catch_up_search_projection_with_batch_hydrator<F>(
        &self,
        search_index: &mut SearchIndex,
        max_change_operations_per_batch: usize,
        max_projection_operations_per_batch: usize,
        max_batches: usize,
        mut batch_hydrator: F,
    ) -> Result<SearchProjectionCatchUpReport>
    where
        F: FnMut(
            &mut DatabaseReadTransaction,
            &SearchProjectionChangeBatch,
        ) -> Result<SearchProjectionRelationalDelta>,
    {
        validate_search_projection_catch_up_request(
            search_index,
            max_change_operations_per_batch,
            max_batches,
        )?;
        if max_projection_operations_per_batch == 0 {
            return Err(HawDBError::Semantic(
                "search projection catch-up max_projection_operations_per_batch must be greater than zero"
                    .to_string(),
            ));
        }
        self.catch_up_search_projection_with_batch_applier(
            search_index,
            max_change_operations_per_batch,
            max_batches,
            |database, search_index, max_operations| {
                let Some(mut batch) = database
                    .build_search_projection_change_batch_from_freshness(
                        search_index,
                        Some(max_operations),
                    )?
                else {
                    return Ok(None);
                };
                let operation_count = batch.operation_count();
                let mut snapshot = database.begin_read_transaction();
                let hydrated = batch_hydrator(&mut snapshot, &batch)?;
                batch.graph_delta_mut().max_operations = Some(max_projection_operations_per_batch);
                database.apply_search_projection_change_batch(search_index, batch, hydrated)?;
                Ok(Some(operation_count))
            },
        )
    }

    fn catch_up_search_projection_with_batch_applier<F>(
        &self,
        search_index: &mut SearchIndex,
        max_operations_per_batch: usize,
        max_batches: usize,
        mut apply_next_batch: F,
    ) -> Result<SearchProjectionCatchUpReport>
    where
        F: FnMut(&Database, &mut SearchIndex, usize) -> Result<Option<usize>>,
    {
        run_search_projection_catch_up(
            search_index,
            self.store.commit_epoch(),
            max_operations_per_batch,
            max_batches,
            |search_index, max_operations| apply_next_batch(self, search_index, max_operations),
        )
    }

    pub fn catch_up_search_projection_scheduled(
        &self,
        search_index: &mut SearchIndex,
        max_operations_per_batch: usize,
        max_batches: usize,
    ) -> Result<ScheduledSearchProjectionCatchUpReport> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        let scheduler = self.local_qos_scheduler_for_work();
        run_scheduled_search_projection_catch_up(
            search_index,
            self.store.commit_epoch(),
            &scheduler,
            max_operations_per_batch,
            max_batches,
            |search_index, max_operations| {
                let Some(request) = self
                    .build_search_projection_graph_delta_request_from_freshness(
                        search_index,
                        Some(max_operations),
                    )?
                else {
                    return Ok(None);
                };
                let work_request = request.background_work_request();
                Ok(Some((request, work_request)))
            },
            |search_index, request| {
                self.apply_search_projection_graph_delta(search_index, request)
                    .map(|report| report.operation_count)
            },
        )
    }
}
