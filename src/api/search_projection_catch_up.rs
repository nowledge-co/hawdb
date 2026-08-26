use super::Database;
use crate::qos::{
    LocalQosPermit, LocalQosScheduler, QosAdmission, QosAdmissionCode, WorkClass, WorkRequest,
};
use crate::search::{SearchIndex, SearchProjectionFreshness};
use crate::{
    DatabaseReadTransaction, Result, SearchProjectionChangeBatch, SearchProjectionRelationalDelta,
    SkeinError,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchProjectionCatchUpReport {
    pub graph_commit_epoch: u64,
    pub start_applied_epoch: Option<u64>,
    pub start_durable_epoch: Option<u64>,
    pub end_applied_epoch: Option<u64>,
    pub end_durable_epoch: Option<u64>,
    pub applied_batch_count: usize,
    pub applied_operation_count: usize,
    pub complete: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchProjectionCatchUpStopReason {
    CaughtUp,
    BatchBudgetExhausted,
    Deferred(QosAdmissionCode),
    Rejected(QosAdmissionCode),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledSearchProjectionCatchUpReport {
    pub catch_up: SearchProjectionCatchUpReport,
    pub stop_reason: SearchProjectionCatchUpStopReason,
}

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
    /// the batch. Skein validates that count before applying either the graph
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
    /// Skein. The hydrator must still account for every relational primary key
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
        validate_catch_up_request(search_index, max_change_operations_per_batch, max_batches)?;
        if max_projection_operations_per_batch == 0 {
            return Err(SkeinError::Semantic(
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
                batch.graph_delta.max_operations = Some(max_projection_operations_per_batch);
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
        validate_catch_up_request(search_index, max_operations_per_batch, max_batches)?;
        let start = search_index.projection_freshness();
        if start.has_uncheckpointed_changes {
            search_index.checkpoint()?;
        }
        let graph_commit_epoch = self.store.commit_epoch();
        let mut applied_batch_count = 0usize;
        let mut applied_operation_count = 0usize;
        while applied_batch_count < max_batches {
            let Some(operation_count) =
                apply_next_batch(self, search_index, max_operations_per_batch)?
            else {
                break;
            };
            search_index.checkpoint()?;
            applied_batch_count = applied_batch_count.saturating_add(1);
            applied_operation_count = applied_operation_count.saturating_add(operation_count);
        }

        Ok(catch_up_report(
            graph_commit_epoch,
            start,
            search_index.projection_freshness(),
            applied_batch_count,
            applied_operation_count,
        ))
    }

    pub fn catch_up_search_projection_with_scheduler(
        &self,
        search_index: &mut SearchIndex,
        scheduler: &mut LocalQosScheduler,
        max_operations_per_batch: usize,
        max_batches: usize,
    ) -> Result<ScheduledSearchProjectionCatchUpReport> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        validate_catch_up_request(search_index, max_operations_per_batch, max_batches)?;
        self.configure_qos_scheduler_telemetry(scheduler);

        let start = search_index.projection_freshness();
        let graph_commit_epoch = self.store.commit_epoch();
        let mut applied_batch_count = 0usize;
        let mut applied_operation_count = 0usize;

        if start.has_uncheckpointed_changes {
            let permit = match start_background_work(
                scheduler,
                WorkRequest::background(WorkClass::Projection, max_operations_per_batch),
            ) {
                Ok(permit) => permit,
                Err(stop_reason) => {
                    return Ok(scheduled_report(
                        graph_commit_epoch,
                        start,
                        search_index.projection_freshness(),
                        applied_batch_count,
                        applied_operation_count,
                        stop_reason,
                    ));
                }
            };
            let checkpoint = search_index.checkpoint();
            scheduler.finish_with_outcome(permit, checkpoint.is_ok());
            checkpoint?;
        }

        while applied_batch_count < max_batches {
            let Some(request) = self.build_search_projection_graph_delta_request_from_freshness(
                search_index,
                Some(max_operations_per_batch),
            )?
            else {
                return Ok(scheduled_report(
                    graph_commit_epoch,
                    start,
                    search_index.projection_freshness(),
                    applied_batch_count,
                    applied_operation_count,
                    SearchProjectionCatchUpStopReason::CaughtUp,
                ));
            };
            let permit = match start_background_work(scheduler, request.background_work_request()) {
                Ok(permit) => permit,
                Err(stop_reason) => {
                    return Ok(scheduled_report(
                        graph_commit_epoch,
                        start,
                        search_index.projection_freshness(),
                        applied_batch_count,
                        applied_operation_count,
                        stop_reason,
                    ));
                }
            };
            let result = self
                .apply_search_projection_graph_delta(search_index, request)
                .and_then(|report| {
                    search_index.checkpoint()?;
                    Ok(report)
                });
            scheduler.finish_with_outcome(permit, result.is_ok());
            let report = result?;
            applied_batch_count = applied_batch_count.saturating_add(1);
            applied_operation_count =
                applied_operation_count.saturating_add(report.operation_count);
        }

        let end = search_index.projection_freshness();
        let stop_reason = if durable_epoch(&end) == graph_commit_epoch {
            SearchProjectionCatchUpStopReason::CaughtUp
        } else {
            SearchProjectionCatchUpStopReason::BatchBudgetExhausted
        };
        Ok(scheduled_report(
            graph_commit_epoch,
            start,
            end,
            applied_batch_count,
            applied_operation_count,
            stop_reason,
        ))
    }
}

fn validate_catch_up_request(
    search_index: &SearchIndex,
    max_operations_per_batch: usize,
    max_batches: usize,
) -> Result<()> {
    if !search_index.is_persistent() {
        return Err(SkeinError::Storage(
            "durable search projection catch-up requires a persistent search index".to_string(),
        ));
    }
    if max_operations_per_batch == 0 {
        return Err(SkeinError::Semantic(
            "search projection catch-up max_operations_per_batch must be greater than zero"
                .to_string(),
        ));
    }
    if max_batches == 0 {
        return Err(SkeinError::Semantic(
            "search projection catch-up max_batches must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

fn start_background_work(
    scheduler: &mut LocalQosScheduler,
    request: WorkRequest,
) -> std::result::Result<LocalQosPermit, SearchProjectionCatchUpStopReason> {
    scheduler
        .try_start(request)
        .map_err(|admission| match admission {
            QosAdmission::Defer { code, .. } => SearchProjectionCatchUpStopReason::Deferred(code),
            QosAdmission::Reject { code, .. } => SearchProjectionCatchUpStopReason::Rejected(code),
            QosAdmission::Admit => unreachable!("admitted work returns a permit"),
        })
}

fn scheduled_report(
    graph_commit_epoch: u64,
    start: SearchProjectionFreshness,
    end: SearchProjectionFreshness,
    applied_batch_count: usize,
    applied_operation_count: usize,
    stop_reason: SearchProjectionCatchUpStopReason,
) -> ScheduledSearchProjectionCatchUpReport {
    ScheduledSearchProjectionCatchUpReport {
        catch_up: catch_up_report(
            graph_commit_epoch,
            start,
            end,
            applied_batch_count,
            applied_operation_count,
        ),
        stop_reason,
    }
}

fn catch_up_report(
    graph_commit_epoch: u64,
    start: SearchProjectionFreshness,
    end: SearchProjectionFreshness,
    applied_batch_count: usize,
    applied_operation_count: usize,
) -> SearchProjectionCatchUpReport {
    SearchProjectionCatchUpReport {
        graph_commit_epoch,
        start_applied_epoch: start.source_graph_commit_epoch,
        start_durable_epoch: start.durable_source_graph_commit_epoch,
        end_applied_epoch: end.source_graph_commit_epoch,
        end_durable_epoch: end.durable_source_graph_commit_epoch,
        applied_batch_count,
        applied_operation_count,
        complete: durable_epoch(&end) == graph_commit_epoch,
    }
}

fn durable_epoch(freshness: &SearchProjectionFreshness) -> u64 {
    freshness.durable_source_graph_commit_epoch.unwrap_or(0)
}
