use crate::error::{Result, SkeinError};
use crate::store::{
    GraphStore, RelationalIndexProbeStatistics, RelationalIndexReadLimits,
    RelationalIndexReadViewBackendReport, RelationalIndexReadViewReport,
    RelationalTransactionIndexView,
};
use skein_storage::{RelationalIndexShadowError, RelationalKey, RelationalState};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

#[derive(Debug, Clone, Copy)]
pub(crate) enum RelationalIndexReadMode<'a> {
    Materialized,
    Shadow(&'a GraphStore),
    DemandPaged(&'a GraphStore),
    Authoritative(&'a GraphStore),
    TransactionWorkspace,
    AuthoritativeTransaction(&'a RelationalTransactionIndexView),
}

impl RelationalIndexReadMode<'_> {
    pub(crate) fn probe_statistics(
        self,
        table: &str,
        index: &str,
        prefix_len: usize,
    ) -> Option<RelationalIndexProbeStatistics> {
        match self {
            Self::Materialized | Self::TransactionWorkspace => None,
            Self::Shadow(store) | Self::DemandPaged(store) | Self::Authoritative(store) => {
                store.relational_index_probe_statistics(table, index, prefix_len)
            }
            Self::AuthoritativeTransaction(view) => {
                view.fresh_probe_statistics(table, index, prefix_len)
            }
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RelationalIndexExecutionEvidence {
    pub table: String,
    pub index: String,
    pub lookups: usize,
    pub demand_paged_lookups: usize,
    pub authoritative_lookups: usize,
    pub transaction_workspace_lookups: usize,
    pub canonical_fallback_lookups: usize,
    pub fallback_reasons: BTreeSet<&'static str>,
    pub base_generation: Option<u64>,
    pub delta_generation: Option<u64>,
    pub base_commit_epoch: Option<u64>,
    pub visible_commit_epoch: Option<u64>,
    pub root_set_digest: Option<String>,
    pub logical_pages: usize,
    pub logical_bytes: usize,
    pub file_pages: usize,
    pub file_bytes: usize,
    pub cache_hits: usize,
    pub cache_misses: usize,
    pub cache_admission_rejections: usize,
    pub delta_entries_visited: usize,
    pub live_batches_visited: usize,
    pub live_entries_visited: usize,
    pub live_entries_matched: usize,
    pub live_bytes_visited: usize,
    pub rows_visited: usize,
    pub range_lookups: usize,
    pub exclusive_seek_lookups: usize,
    pub backward_lookups: usize,
    pub early_stop_lookups: usize,
}

impl RelationalIndexExecutionEvidence {
    pub(crate) fn runtime_path(&self) -> &'static str {
        match (
            self.demand_paged_lookups != 0,
            self.authoritative_lookups != 0,
            self.transaction_workspace_lookups != 0,
            self.canonical_fallback_lookups != 0,
        ) {
            (true, false, false, false) => "demand_paged",
            (false, true, false, false) => "authoritative",
            (false, false, true, false) => "transaction_workspace",
            (false, false, false, true) => "canonical_fallback",
            (false, false, false, false) => "not_executed",
            _ => "mixed",
        }
    }
}

pub(crate) struct RelationalIndexRuntime<'a> {
    mode: RelationalIndexReadMode<'a>,
    limits: RelationalIndexReadLimits,
    state: RefCell<RelationalIndexRuntimeState>,
}

#[derive(Default)]
struct RelationalIndexRuntimeState {
    logical_pages: usize,
    logical_bytes: usize,
    rows_visited: usize,
    file_bytes: usize,
    evidence: BTreeMap<(String, String), RelationalIndexExecutionEvidence>,
}

struct RelationalIndexProbe<'input> {
    table: &'input str,
    index: &'input str,
    selector: RelationalIndexProbeSelector<'input>,
}

#[derive(Clone, Copy)]
enum RelationalIndexProbeSelector<'input> {
    Prefix(&'input RelationalKey),
    Range(&'input skein_storage::RelationalIndexRangeScan),
}

impl<'a> RelationalIndexRuntime<'a> {
    pub(crate) fn new(
        mode: RelationalIndexReadMode<'a>,
        limits: RelationalIndexReadLimits,
    ) -> Self {
        Self {
            mode,
            limits,
            state: RefCell::new(RelationalIndexRuntimeState::default()),
        }
    }

    pub(crate) fn evidence(&self) -> Vec<RelationalIndexExecutionEvidence> {
        self.state.borrow().evidence.values().cloned().collect()
    }

    pub(crate) fn visit_prefix(
        &self,
        state: &RelationalState,
        table: &str,
        index: &str,
        prefix: &RelationalKey,
        mut visit: impl FnMut(&RelationalKey) -> Result<bool>,
    ) -> Result<bool> {
        self.visit_prefix_entries(state, table, index, prefix, |_, primary_key| {
            visit(primary_key)
        })
    }

    pub(crate) fn visit_prefix_entries(
        &self,
        state: &RelationalState,
        table: &str,
        index: &str,
        prefix: &RelationalKey,
        mut visit: impl FnMut(&RelationalKey, &RelationalKey) -> Result<bool>,
    ) -> Result<bool> {
        if matches!(
            self.mode,
            RelationalIndexReadMode::Materialized | RelationalIndexReadMode::Shadow(_)
        ) {
            return visit_materialized_prefix_entries(state, table, index, prefix, &mut visit);
        }
        let fallback = |visit: &mut dyn FnMut(&RelationalKey, &RelationalKey) -> Result<bool>| {
            visit_materialized_prefix_entries(state, table, index, prefix, visit)
        };
        self.visit_demand_or_fallback(
            RelationalIndexProbe {
                table,
                index,
                selector: RelationalIndexProbeSelector::Prefix(prefix),
            },
            &mut visit,
            fallback,
        )
    }

    pub(crate) fn visit_range_entries(
        &self,
        state: &RelationalState,
        table: &str,
        index: &str,
        scan: &skein_storage::RelationalIndexRangeScan,
        mut visit: impl FnMut(&RelationalKey, &RelationalKey) -> Result<bool>,
    ) -> Result<bool> {
        if matches!(
            self.mode,
            RelationalIndexReadMode::Materialized | RelationalIndexReadMode::Shadow(_)
        ) {
            return visit_materialized_range_entries(state, table, index, scan, &mut visit);
        }
        let fallback = |visit: &mut dyn FnMut(&RelationalKey, &RelationalKey) -> Result<bool>| {
            visit_materialized_range_entries(state, table, index, scan, visit)
        };
        self.visit_demand_or_fallback(
            RelationalIndexProbe {
                table,
                index,
                selector: RelationalIndexProbeSelector::Range(scan),
            },
            &mut visit,
            fallback,
        )
    }

    fn visit_demand_or_fallback<'input>(
        &self,
        probe: RelationalIndexProbe<'input>,
        visit: &mut dyn FnMut(&RelationalKey, &RelationalKey) -> Result<bool>,
        fallback: impl FnOnce(
            &mut dyn FnMut(&RelationalKey, &RelationalKey) -> Result<bool>,
        ) -> Result<bool>,
    ) -> Result<bool> {
        let RelationalIndexProbe {
            table,
            index,
            selector,
        } = probe;
        enum PersistentTarget<'a> {
            Store(&'a GraphStore),
            Transaction(&'a RelationalTransactionIndexView),
        }

        let (target, authoritative) = match self.mode {
            RelationalIndexReadMode::Materialized | RelationalIndexReadMode::Shadow(_) => {
                unreachable!("handled by the caller")
            }
            RelationalIndexReadMode::TransactionWorkspace => {
                self.record_fallback(table, index, "transaction_workspace")?;
                return fallback(visit);
            }
            RelationalIndexReadMode::DemandPaged(store) => (PersistentTarget::Store(store), false),
            RelationalIndexReadMode::Authoritative(store) => (PersistentTarget::Store(store), true),
            RelationalIndexReadMode::AuthoritativeTransaction(view) => {
                (PersistentTarget::Transaction(view), true)
            }
        };
        let Some(remaining) = self.remaining_limits() else {
            if authoritative {
                return Err(SkeinError::Execution(format!(
                    "authoritative relational index budget is exhausted before reading {table}.{index}"
                )));
            }
            self.record_fallback(table, index, "query_index_budget_exhausted")?;
            return fallback(visit);
        };
        let mut callback_error = None;
        let mut keep_going = true;
        let mut produced_provisional_rows = false;
        let mut visit_locator = |index_key: &RelationalKey, locator: &RelationalKey| {
            produced_provisional_rows = true;
            match visit(index_key, locator) {
                Ok(continue_scan) => {
                    keep_going = continue_scan;
                    continue_scan
                }
                Err(error) => {
                    callback_error = Some(error);
                    false
                }
            }
        };
        let attempt = match target {
            PersistentTarget::Store(store) => match selector {
                RelationalIndexProbeSelector::Prefix(prefix) => store
                    .visit_relational_index_read_view_prefix_entries(
                        table,
                        index,
                        prefix,
                        remaining,
                        &mut visit_locator,
                    ),
                RelationalIndexProbeSelector::Range(scan) => store
                    .visit_relational_index_read_view_range_entries(
                        table,
                        index,
                        scan,
                        remaining,
                        &mut visit_locator,
                    ),
            },
            PersistentTarget::Transaction(view) => Some(match selector {
                RelationalIndexProbeSelector::Prefix(prefix) => {
                    view.visit_prefix_entries(table, index, prefix, remaining, &mut visit_locator)
                }
                RelationalIndexProbeSelector::Range(scan) => {
                    view.visit_range_entries(table, index, scan, remaining, &mut visit_locator)
                }
            }),
        };
        if let Some(error) = callback_error {
            return Err(error);
        }
        match attempt {
            Some(Ok(report)) => {
                self.record_success(table, index, &report, selector)?;
                Ok(keep_going)
            }
            Some(Err(RelationalIndexShadowError::Admission(_))) => {
                if produced_provisional_rows {
                    return Err(SkeinError::Execution(format!(
                        "relational index read for {table}.{index} exhausted admission after producing provisional row locators"
                    )));
                }
                if authoritative {
                    return Err(SkeinError::Execution(format!(
                        "authoritative relational index read for {table}.{index} was rejected by admission"
                    )));
                }
                self.record_fallback(table, index, "admission_rejected")?;
                fallback(visit)
            }
            Some(Err(RelationalIndexShadowError::MissingIndex { .. })) => {
                if produced_provisional_rows {
                    return Err(SkeinError::StorageIntegrity(format!(
                        "relational index {table}.{index} disappeared after producing provisional row locators"
                    )));
                }
                if authoritative {
                    return Err(SkeinError::StorageIntegrity(format!(
                        "authoritative relational index {table}.{index} is missing"
                    )));
                }
                self.record_fallback(table, index, "missing_index")?;
                fallback(visit)
            }
            Some(Err(error @ RelationalIndexShadowError::Corrupt(_))) => {
                Err(SkeinError::StorageIntegrity(format!(
                    "relational index read failed closed for {table}.{index}: {error}"
                )))
            }
            Some(Err(error @ RelationalIndexShadowError::Durability(_)))
            | Some(Err(error @ RelationalIndexShadowError::StaleGeneration { .. })) => {
                Err(SkeinError::StorageIntegrity(format!(
                    "relational index identity failed closed for {table}.{index}: {error}"
                )))
            }
            None => {
                if authoritative {
                    return Err(SkeinError::StorageIntegrity(format!(
                        "authoritative relational index view is unavailable for {table}.{index}"
                    )));
                }
                self.record_fallback(table, index, "read_view_unavailable")?;
                fallback(visit)
            }
        }
    }

    fn remaining_limits(&self) -> Option<RelationalIndexReadLimits> {
        let state = self.state.borrow();
        Some(RelationalIndexReadLimits {
            max_pages: NonZeroUsize::new(
                self.limits
                    .max_pages
                    .get()
                    .checked_sub(state.logical_pages)?,
            )?,
            max_rows: NonZeroUsize::new(
                self.limits.max_rows.get().checked_sub(state.rows_visited)?,
            )?,
            max_bytes: NonZeroUsize::new(
                self.limits
                    .max_bytes
                    .get()
                    .checked_sub(state.logical_bytes)?,
            )?,
            max_file_bytes: self.limits.max_file_bytes.checked_sub(state.file_bytes)?,
            max_tree_height: self.limits.max_tree_height,
        })
    }

    fn evidence_mut<'state>(
        state: &'state mut RelationalIndexRuntimeState,
        table: &str,
        index: &str,
    ) -> &'state mut RelationalIndexExecutionEvidence {
        state
            .evidence
            .entry((table.to_string(), index.to_string()))
            .or_insert_with(|| RelationalIndexExecutionEvidence {
                table: table.to_string(),
                index: index.to_string(),
                ..RelationalIndexExecutionEvidence::default()
            })
    }

    fn record_fallback(&self, table: &str, index: &str, reason: &'static str) -> Result<()> {
        let mut state = self.state.borrow_mut();
        let evidence = Self::evidence_mut(&mut state, table, index);
        evidence.lookups = checked_add(evidence.lookups, 1, "index lookup count")?;
        evidence.canonical_fallback_lookups = checked_add(
            evidence.canonical_fallback_lookups,
            1,
            "canonical fallback count",
        )?;
        evidence.fallback_reasons.insert(reason);
        Ok(())
    }

    fn record_success(
        &self,
        table: &str,
        index: &str,
        report: &RelationalIndexReadViewReport,
        selector: RelationalIndexProbeSelector<'_>,
    ) -> Result<()> {
        let metrics = IndexReadMetrics::from_report(report)?;
        let mut state = self.state.borrow_mut();
        let logical_pages = checked_add(
            state.logical_pages,
            metrics.logical_pages,
            "query index page count",
        )?;
        let logical_bytes = checked_add(
            state.logical_bytes,
            metrics.logical_bytes,
            "query index byte count",
        )?;
        let rows_visited = checked_add(
            state.rows_visited,
            report.rows_visited,
            "query index row count",
        )?;
        let file_bytes = checked_add(
            state.file_bytes,
            metrics.file_bytes,
            "query index file byte count",
        )?;
        if logical_pages > self.limits.max_pages.get()
            || logical_bytes > self.limits.max_bytes.get()
            || rows_visited > self.limits.max_rows.get()
            || file_bytes > self.limits.max_file_bytes
        {
            return Err(SkeinError::Execution(format!(
                "relational index reads exceed the statement budget logical_pages={}/{}, logical_bytes={}/{}, rows={}/{}, file_bytes={}/{}",
                logical_pages,
                self.limits.max_pages,
                logical_bytes,
                self.limits.max_bytes,
                rows_visited,
                self.limits.max_rows,
                file_bytes,
                self.limits.max_file_bytes,
            )));
        }
        state.logical_pages = logical_pages;
        state.logical_bytes = logical_bytes;
        state.rows_visited = rows_visited;
        state.file_bytes = file_bytes;
        let evidence = Self::evidence_mut(&mut state, table, index);
        ensure_identity(evidence, report)?;
        evidence.lookups = checked_add(evidence.lookups, 1, "index lookup count")?;
        match self.mode {
            RelationalIndexReadMode::DemandPaged(_) => {
                evidence.demand_paged_lookups = checked_add(
                    evidence.demand_paged_lookups,
                    1,
                    "demand-paged lookup count",
                )?;
            }
            RelationalIndexReadMode::Authoritative(_) => {
                evidence.authoritative_lookups = checked_add(
                    evidence.authoritative_lookups,
                    1,
                    "authoritative lookup count",
                )?;
            }
            RelationalIndexReadMode::AuthoritativeTransaction(_) => {
                evidence.transaction_workspace_lookups = checked_add(
                    evidence.transaction_workspace_lookups,
                    1,
                    "transaction workspace lookup count",
                )?;
            }
            RelationalIndexReadMode::Materialized
            | RelationalIndexReadMode::Shadow(_)
            | RelationalIndexReadMode::TransactionWorkspace => {
                unreachable!("materialized paths cannot record a persistent-index success")
            }
        }
        metrics.accumulate(evidence)?;
        evidence.live_batches_visited = checked_add(
            evidence.live_batches_visited,
            report.live_batches_visited,
            "live batch count",
        )?;
        evidence.live_entries_visited = checked_add(
            evidence.live_entries_visited,
            report.live_entries_visited,
            "live entry count",
        )?;
        evidence.live_entries_matched = checked_add(
            evidence.live_entries_matched,
            report.live_entries_matched,
            "matched live entry count",
        )?;
        evidence.live_bytes_visited = checked_add(
            evidence.live_bytes_visited,
            report.live_bytes_visited,
            "live byte count",
        )?;
        evidence.rows_visited = checked_add(
            evidence.rows_visited,
            report.rows_visited,
            "index result row count",
        )?;
        if let RelationalIndexProbeSelector::Range(scan) = selector {
            evidence.range_lookups = checked_add(evidence.range_lookups, 1, "range lookup count")?;
            if scan.exclusive_bound.is_some() {
                evidence.exclusive_seek_lookups =
                    checked_add(evidence.exclusive_seek_lookups, 1, "exclusive seek count")?;
            }
            if scan.direction == skein_storage::RelationalIndexScanDirection::Backward {
                evidence.backward_lookups =
                    checked_add(evidence.backward_lookups, 1, "backward lookup count")?;
            }
        }
        if report.stopped_early {
            evidence.early_stop_lookups =
                checked_add(evidence.early_stop_lookups, 1, "early-stop lookup count")?;
        }
        Ok(())
    }
}

fn visit_materialized_prefix_entries<'state>(
    state: &'state RelationalState,
    table: &str,
    index: &str,
    prefix: &RelationalKey,
    visit: &mut dyn FnMut(&RelationalKey, &RelationalKey) -> Result<bool>,
) -> Result<bool> {
    let mut error = None;
    let mut keep_going = true;
    state
        .visit_index_prefix_entries(table, index, prefix, |index_key, primary_key| {
            match visit(index_key, primary_key) {
                Ok(continue_scan) => {
                    keep_going = continue_scan;
                    continue_scan
                }
                Err(candidate_error) => {
                    error = Some(candidate_error);
                    false
                }
            }
        })
        .ok_or_else(|| {
            SkeinError::Execution(format!(
                "relational index {index} on table {table} is not materialized"
            ))
        })?;
    match error {
        Some(error) => Err(error),
        None => Ok(keep_going),
    }
}

fn visit_materialized_range_entries<'state>(
    state: &'state RelationalState,
    table: &str,
    index: &str,
    scan: &skein_storage::RelationalIndexRangeScan,
    visit: &mut dyn FnMut(&RelationalKey, &RelationalKey) -> Result<bool>,
) -> Result<bool> {
    let mut error = None;
    let mut keep_going = true;
    state
        .visit_index_range_entries(table, index, scan, |index_key, primary_key| {
            match visit(index_key, primary_key) {
                Ok(continue_scan) => {
                    keep_going = continue_scan;
                    continue_scan
                }
                Err(candidate_error) => {
                    error = Some(candidate_error);
                    false
                }
            }
        })
        .ok_or_else(|| {
            SkeinError::Execution(format!(
                "relational index {index} on table {table} is not materialized"
            ))
        })?;
    match error {
        Some(error) => Err(error),
        None => Ok(keep_going),
    }
}

fn ensure_identity(
    evidence: &mut RelationalIndexExecutionEvidence,
    report: &RelationalIndexReadViewReport,
) -> Result<()> {
    let observed = (
        Some(report.base_generation),
        report.delta_generation,
        Some(report.base_commit_epoch),
        Some(report.visible_commit_epoch),
        Some(report.root_set_digest.as_str()),
    );
    let expected = (
        evidence.base_generation,
        evidence.delta_generation,
        evidence.base_commit_epoch,
        evidence.visible_commit_epoch,
        evidence.root_set_digest.as_deref(),
    );
    if (evidence.demand_paged_lookups != 0
        || evidence.authoritative_lookups != 0
        || evidence.transaction_workspace_lookups != 0)
        && expected != observed
    {
        return Err(SkeinError::StorageIntegrity(
            "relational index view identity changed within one SQL statement".to_string(),
        ));
    }
    evidence.base_generation = observed.0;
    evidence.delta_generation = observed.1;
    evidence.base_commit_epoch = observed.2;
    evidence.visible_commit_epoch = observed.3;
    evidence.root_set_digest = observed.4.map(str::to_string);
    Ok(())
}

#[derive(Debug, Clone, Copy, Default)]
struct IndexReadMetrics {
    logical_pages: usize,
    logical_bytes: usize,
    file_pages: usize,
    file_bytes: usize,
    cache_hits: usize,
    cache_misses: usize,
    cache_admission_rejections: usize,
    delta_entries_visited: usize,
}

impl IndexReadMetrics {
    fn from_report(report: &RelationalIndexReadViewReport) -> Result<Self> {
        let mut metrics = match &report.backend {
            RelationalIndexReadViewBackendReport::Base(base) => Self {
                logical_pages: base.pages_read,
                logical_bytes: base.bytes_read,
                file_pages: base.file_pages_read,
                file_bytes: base.file_bytes_read,
                cache_hits: base.cache_hits,
                cache_misses: base.cache_misses,
                cache_admission_rejections: base.cache_admission_rejections,
                delta_entries_visited: 0,
            },
            RelationalIndexReadViewBackendReport::Recovered(recovered) => Self {
                logical_pages: checked_add(
                    recovered.base.pages_read,
                    recovered.delta_pages_read,
                    "recovered logical page count",
                )?,
                logical_bytes: checked_add(
                    recovered.base.bytes_read,
                    recovered.delta_bytes_read,
                    "recovered logical byte count",
                )?,
                file_pages: checked_add(
                    recovered.base.file_pages_read,
                    recovered.delta_file_pages_read,
                    "recovered file page count",
                )?,
                file_bytes: checked_add(
                    recovered.base.file_bytes_read,
                    recovered.delta_file_bytes_read,
                    "recovered file byte count",
                )?,
                cache_hits: checked_add(
                    recovered.base.cache_hits,
                    recovered.delta_cache_hits,
                    "recovered cache hit count",
                )?,
                cache_misses: checked_add(
                    recovered.base.cache_misses,
                    recovered.delta_cache_misses,
                    "recovered cache miss count",
                )?,
                cache_admission_rejections: checked_add(
                    recovered.base.cache_admission_rejections,
                    recovered.delta_cache_admission_rejections,
                    "recovered cache rejection count",
                )?,
                delta_entries_visited: recovered.delta_entries_visited,
            },
        };
        metrics.logical_bytes = checked_add(
            metrics.logical_bytes,
            report.live_bytes_visited,
            "logical bytes including live changes",
        )?;
        Ok(metrics)
    }

    fn accumulate(self, evidence: &mut RelationalIndexExecutionEvidence) -> Result<()> {
        evidence.logical_pages = checked_add(
            evidence.logical_pages,
            self.logical_pages,
            "logical page count",
        )?;
        evidence.logical_bytes = checked_add(
            evidence.logical_bytes,
            self.logical_bytes,
            "logical byte count",
        )?;
        evidence.file_pages =
            checked_add(evidence.file_pages, self.file_pages, "physical page count")?;
        evidence.file_bytes =
            checked_add(evidence.file_bytes, self.file_bytes, "physical byte count")?;
        evidence.cache_hits = checked_add(evidence.cache_hits, self.cache_hits, "cache hit count")?;
        evidence.cache_misses =
            checked_add(evidence.cache_misses, self.cache_misses, "cache miss count")?;
        evidence.cache_admission_rejections = checked_add(
            evidence.cache_admission_rejections,
            self.cache_admission_rejections,
            "cache admission rejection count",
        )?;
        evidence.delta_entries_visited = checked_add(
            evidence.delta_entries_visited,
            self.delta_entries_visited,
            "delta entry count",
        )?;
        Ok(())
    }
}

fn checked_add(left: usize, right: usize, counter: &str) -> Result<usize> {
    left.checked_add(right)
        .ok_or_else(|| SkeinError::StorageIntegrity(format!("relational {counter} overflow")))
}
