use crate::{SearchIndex, SearchProjectionFreshness};
use skein_core::{Result, SkeinError};
use skein_qos::{
    LocalQosPermit, LocalQosScheduler, QosAdmission, QosAdmissionCode, WorkClass, WorkRequest,
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

#[doc(hidden)]
pub fn validate_search_projection_catch_up_request(
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

#[doc(hidden)]
pub fn start_search_projection_background_work(
    scheduler: &LocalQosScheduler,
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

#[doc(hidden)]
pub fn scheduled_search_projection_catch_up_report(
    graph_commit_epoch: u64,
    start: SearchProjectionFreshness,
    end: SearchProjectionFreshness,
    applied_batch_count: usize,
    applied_operation_count: usize,
    stop_reason: SearchProjectionCatchUpStopReason,
) -> ScheduledSearchProjectionCatchUpReport {
    ScheduledSearchProjectionCatchUpReport {
        catch_up: search_projection_catch_up_report(
            graph_commit_epoch,
            start,
            end,
            applied_batch_count,
            applied_operation_count,
        ),
        stop_reason,
    }
}

#[doc(hidden)]
pub fn search_projection_catch_up_report(
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
        complete: search_projection_durable_epoch(&end) == graph_commit_epoch,
    }
}

#[doc(hidden)]
pub fn search_projection_durable_epoch(freshness: &SearchProjectionFreshness) -> u64 {
    freshness.durable_source_graph_commit_epoch.unwrap_or(0)
}

/// Runs the storage-independent portion of durable search projection catch-up.
///
/// The host owns changefeed selection and application because those operations
/// cross the embedded graph store boundary. The search crate owns checkpoint
/// ordering and progress accounting so every host uses the same durable
/// watermark semantics.
#[doc(hidden)]
pub fn run_search_projection_catch_up<ApplyNextBatch>(
    search_index: &mut SearchIndex,
    graph_commit_epoch: u64,
    max_operations_per_batch: usize,
    max_batches: usize,
    mut apply_next_batch: ApplyNextBatch,
) -> Result<SearchProjectionCatchUpReport>
where
    ApplyNextBatch: FnMut(&mut SearchIndex, usize) -> Result<Option<usize>>,
{
    validate_search_projection_catch_up_request(
        search_index,
        max_operations_per_batch,
        max_batches,
    )?;
    let start = search_index.projection_freshness();
    if start.has_uncheckpointed_changes {
        search_index.checkpoint()?;
    }

    let mut applied_batch_count = 0usize;
    let mut applied_operation_count = 0usize;
    while applied_batch_count < max_batches {
        let Some(operation_count) = apply_next_batch(search_index, max_operations_per_batch)?
        else {
            break;
        };
        search_index.checkpoint()?;
        applied_batch_count = applied_batch_count.saturating_add(1);
        applied_operation_count = applied_operation_count.saturating_add(operation_count);
    }

    Ok(search_projection_catch_up_report(
        graph_commit_epoch,
        start,
        search_index.projection_freshness(),
        applied_batch_count,
        applied_operation_count,
    ))
}

/// Runs scheduled durable search projection catch-up around host-supplied
/// changefeed selection and delta application.
#[doc(hidden)]
pub fn run_scheduled_search_projection_catch_up<Batch, SelectNextBatch, ApplyBatch>(
    search_index: &mut SearchIndex,
    graph_commit_epoch: u64,
    scheduler: &LocalQosScheduler,
    max_operations_per_batch: usize,
    max_batches: usize,
    mut select_next_batch: SelectNextBatch,
    mut apply_batch: ApplyBatch,
) -> Result<ScheduledSearchProjectionCatchUpReport>
where
    SelectNextBatch: FnMut(&SearchIndex, usize) -> Result<Option<(Batch, WorkRequest)>>,
    ApplyBatch: FnMut(&mut SearchIndex, Batch) -> Result<usize>,
{
    validate_search_projection_catch_up_request(
        search_index,
        max_operations_per_batch,
        max_batches,
    )?;
    let start = search_index.projection_freshness();
    let mut applied_batch_count = 0usize;
    let mut applied_operation_count = 0usize;

    if start.has_uncheckpointed_changes {
        let permit = match start_search_projection_background_work(
            scheduler,
            WorkRequest::background(WorkClass::Projection, max_operations_per_batch),
        ) {
            Ok(permit) => permit,
            Err(stop_reason) => {
                return Ok(scheduled_search_projection_catch_up_report(
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
        permit.finish_with_outcome(checkpoint.is_ok());
        checkpoint?;
    }

    while applied_batch_count < max_batches {
        let Some((batch, work_request)) =
            select_next_batch(search_index, max_operations_per_batch)?
        else {
            return Ok(scheduled_search_projection_catch_up_report(
                graph_commit_epoch,
                start,
                search_index.projection_freshness(),
                applied_batch_count,
                applied_operation_count,
                SearchProjectionCatchUpStopReason::CaughtUp,
            ));
        };
        let permit = match start_search_projection_background_work(scheduler, work_request) {
            Ok(permit) => permit,
            Err(stop_reason) => {
                return Ok(scheduled_search_projection_catch_up_report(
                    graph_commit_epoch,
                    start,
                    search_index.projection_freshness(),
                    applied_batch_count,
                    applied_operation_count,
                    stop_reason,
                ));
            }
        };
        let result = apply_batch(search_index, batch).and_then(|operation_count| {
            search_index.checkpoint()?;
            Ok(operation_count)
        });
        permit.finish_with_outcome(result.is_ok());
        let operation_count = result?;
        applied_batch_count = applied_batch_count.saturating_add(1);
        applied_operation_count = applied_operation_count.saturating_add(operation_count);
    }

    let end = search_index.projection_freshness();
    let stop_reason = if search_projection_durable_epoch(&end) == graph_commit_epoch {
        SearchProjectionCatchUpStopReason::CaughtUp
    } else {
        SearchProjectionCatchUpStopReason::BatchBudgetExhausted
    };
    Ok(scheduled_search_projection_catch_up_report(
        graph_commit_epoch,
        start,
        end,
        applied_batch_count,
        applied_operation_count,
        stop_reason,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn freshness(applied: Option<u64>, durable: Option<u64>) -> SearchProjectionFreshness {
        SearchProjectionFreshness {
            document_count: 0,
            import_source_graph_commit_epoch: None,
            source_graph_commit_epoch: applied,
            durable_source_graph_commit_epoch: durable,
            has_uncheckpointed_changes: false,
            full_reindex_needed: false,
            full_reindex_reasons: Vec::new(),
            metadata_repair_needed: false,
            metadata_repair_reasons: Vec::new(),
            embedding_model: None,
            embedding_version: None,
            embedding_dimension: None,
        }
    }

    #[test]
    fn report_tracks_checkpointed_progress_and_completion() {
        let report = search_projection_catch_up_report(
            8,
            freshness(Some(3), Some(2)),
            freshness(Some(8), Some(8)),
            2,
            5,
        );

        assert_eq!(report.start_applied_epoch, Some(3));
        assert_eq!(report.start_durable_epoch, Some(2));
        assert_eq!(report.end_applied_epoch, Some(8));
        assert_eq!(report.end_durable_epoch, Some(8));
        assert_eq!(report.applied_batch_count, 2);
        assert_eq!(report.applied_operation_count, 5);
        assert!(report.complete);
    }

    #[test]
    fn scheduled_report_preserves_budget_stop_without_claiming_completion() {
        let scheduled = scheduled_search_projection_catch_up_report(
            8,
            freshness(Some(3), Some(3)),
            freshness(Some(5), Some(5)),
            1,
            2,
            SearchProjectionCatchUpStopReason::BatchBudgetExhausted,
        );

        assert_eq!(
            scheduled.stop_reason,
            SearchProjectionCatchUpStopReason::BatchBudgetExhausted
        );
        assert!(!scheduled.catch_up.complete);
        assert_eq!(search_projection_durable_epoch(&freshness(None, None)), 0);
    }

    #[test]
    fn scheduled_report_preserves_admission_outcomes() {
        let scheduled = scheduled_search_projection_catch_up_report(
            8,
            freshness(Some(3), Some(3)),
            freshness(Some(3), Some(3)),
            0,
            0,
            SearchProjectionCatchUpStopReason::Deferred(
                QosAdmissionCode::TotalBackgroundLimitExceeded,
            ),
        );

        assert_eq!(
            scheduled.stop_reason,
            SearchProjectionCatchUpStopReason::Deferred(
                QosAdmissionCode::TotalBackgroundLimitExceeded,
            )
        );
        assert!(!scheduled.catch_up.complete);
    }
}
