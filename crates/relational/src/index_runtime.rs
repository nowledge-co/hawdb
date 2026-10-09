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

//! Query-level relational index execution over facade-selected storage views.

#[cfg(test)]
mod tests;

use hawdb_core::{HawDBError, Result};
use hawdb_storage::relational::{
    RelationalIndexReadLimits, RelationalIndexShadowError, RelationalKey, RelationalState,
};
use hawdb_storage::relational_index_view::{
    RelationalIndexProbeStatistics, RelationalIndexReadContext, RelationalIndexReadTarget,
    RelationalIndexReadViewBackendReport, RelationalIndexReadViewReport,
    RelationalTransactionIndexView,
};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

/// Internal static-dispatch seam to the facade-selected committed index view.
/// A missing view returns `None` without invoking the visitor. Implementations
/// preserve reader errors and stop when the visitor returns false; query-level
/// fallback, admission, and evidence remain owned by this runtime.
pub trait RelationalIndexStoreReader {
    /// True only when every context read admits each operation before I/O and
    /// callbacks. Legacy/report-only providers retain strict positive preflight.
    fn supports_relational_index_operation_admission(&self) -> bool {
        false
    }

    /// Optional metadata-count capability of the facade-selected view.
    /// Existing readers decline until they can retain provenance and accounting.
    fn relational_index_exact_posting_count(
        &self,
        _table: &str,
        _index: &str,
        _key: &RelationalKey,
        _limits: RelationalIndexReadLimits,
    ) -> Option<std::result::Result<(u64, RelationalIndexReadViewReport), RelationalIndexShadowError>>
    {
        None
    }

    /// Metadata providers without shared operation admission decline before I/O.
    fn relational_index_exact_posting_count_with_context(
        &self,
        _table: &str,
        _index: &str,
        _key: &RelationalKey,
        _limits: RelationalIndexReadLimits,
        _context: &RelationalIndexReadContext,
    ) -> Option<std::result::Result<(u64, RelationalIndexReadViewReport), RelationalIndexShadowError>>
    {
        None
    }

    /// Providers without operation admission decline before any I/O or callback.
    fn visit_relational_index_read_view_prefix_entries_with_context(
        &self,
        _table: &str,
        _index: &str,
        _prefix: &RelationalKey,
        _limits: RelationalIndexReadLimits,
        _visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
        _context: &RelationalIndexReadContext,
    ) -> Option<std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError>>
    {
        None
    }

    /// Providers without operation admission decline before any I/O or callback.
    fn visit_relational_index_read_view_prefix_entries_many_with_context(
        &self,
        _table: &str,
        _index: &str,
        _prefixes: &[RelationalKey],
        _limits: RelationalIndexReadLimits,
        _visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
        _context: &RelationalIndexReadContext,
    ) -> Option<std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError>>
    {
        None
    }

    /// Providers without operation admission decline before any I/O or callback.
    fn visit_relational_index_read_view_range_entries_with_context(
        &self,
        _table: &str,
        _index: &str,
        _scan: &hawdb_storage::relational::RelationalIndexRangeScan,
        _limits: RelationalIndexReadLimits,
        _visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
        _context: &RelationalIndexReadContext,
    ) -> Option<std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError>>
    {
        None
    }

    fn relational_index_probe_statistics(
        &self,
        table: &str,
        index: &str,
        prefix_len: usize,
    ) -> Option<RelationalIndexProbeStatistics>;

    fn visit_relational_index_read_view_prefix_entries(
        &self,
        table: &str,
        index: &str,
        prefix: &RelationalKey,
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Option<std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError>>;

    fn visit_relational_index_read_view_prefix_entries_many(
        &self,
        table: &str,
        index: &str,
        prefixes: &[RelationalKey],
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Option<std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError>>;

    fn visit_relational_index_read_view_range_entries(
        &self,
        table: &str,
        index: &str,
        scan: &hawdb_storage::relational::RelationalIndexRangeScan,
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Option<std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError>>;
}

#[derive(Debug)]
pub enum RelationalIndexReadMode<'a, R = crate::RelationalMaterializedReader> {
    Materialized,
    Shadow(&'a R),
    DemandPaged(&'a R),
    Authoritative(&'a R),
    TransactionWorkspace,
    AuthoritativeTransaction(&'a RelationalTransactionIndexView),
}

impl<R> Copy for RelationalIndexReadMode<'_, R> {}

impl<R> Clone for RelationalIndexReadMode<'_, R> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<R: RelationalIndexStoreReader> RelationalIndexReadMode<'_, R> {
    pub fn probe_statistics(
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
pub struct RelationalIndexExecutionEvidence {
    pub table: String,
    pub index: String,
    /// All reported index reads, including planning metadata counts.
    pub lookups: usize,
    /// Planning count attempts, including refusals and zero-I/O cache reuse.
    pub metadata_count_lookups: usize,
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
    pub delta_pages_skipped: usize,
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
    pub fn runtime_path(&self) -> &'static str {
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

pub struct RelationalIndexRuntime<'a, R = crate::RelationalMaterializedReader> {
    mode: RelationalIndexReadMode<'a, R>,
    limits: RelationalIndexReadLimits,
    context: RelationalIndexRuntimeContext,
}

/// Statement ownership spans preparation and execution without retaining a
/// generic provider or resetting its cumulative allowance at the phase boundary.
pub(crate) struct RelationalIndexRuntimeContext {
    read_context: RelationalIndexReadContext,
    state: RefCell<RelationalIndexRuntimeState>,
    task: hawdb_core::RuntimeTaskContext,
}

impl RelationalIndexRuntimeContext {
    pub(crate) fn new(
        limits: RelationalIndexReadLimits,
        task: hawdb_core::RuntimeTaskContext,
    ) -> Self {
        Self {
            read_context: RelationalIndexReadContext::with_task(limits, task.clone()),
            state: RefCell::new(RelationalIndexRuntimeState::default()),
            task,
        }
    }

    pub(crate) fn evidence(&self) -> Vec<RelationalIndexExecutionEvidence> {
        self.state.borrow().evidence.values().cloned().collect()
    }
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
    Range(&'input hawdb_storage::relational::RelationalIndexRangeScan),
}

#[derive(Clone, Copy)]
enum RelationalIndexReadPurpose<'input> {
    MetadataCount,
    ExecutionProbe(RelationalIndexProbeSelector<'input>),
}

impl<'a, R: RelationalIndexStoreReader> RelationalIndexRuntime<'a, R> {
    pub fn new(mode: RelationalIndexReadMode<'a, R>, limits: RelationalIndexReadLimits) -> Self {
        Self::with_context(
            mode,
            limits,
            RelationalIndexRuntimeContext::new(limits, Default::default()),
        )
    }

    pub(crate) fn with_context(
        mode: RelationalIndexReadMode<'a, R>,
        limits: RelationalIndexReadLimits,
        context: RelationalIndexRuntimeContext,
    ) -> Self {
        Self {
            mode,
            limits,
            context,
        }
    }

    pub(crate) fn into_context(self) -> RelationalIndexRuntimeContext {
        self.context
    }

    pub fn evidence(&self) -> Vec<RelationalIndexExecutionEvidence> {
        self.context.evidence()
    }

    pub(crate) fn exact_posting_count(
        &self,
        table: &str,
        index: &str,
        key: &RelationalKey,
    ) -> Result<Option<u64>> {
        hawdb_executor::pipeline::runtime_checkpoint(Some(&self.context.task))?;
        let result = match self.mode {
            RelationalIndexReadMode::DemandPaged(store)
            | RelationalIndexReadMode::Authoritative(store) => store
                .relational_index_exact_posting_count_with_context(
                    table,
                    index,
                    key,
                    self.limits,
                    &self.context.read_context,
                ),
            RelationalIndexReadMode::AuthoritativeTransaction(view) => {
                self.context.read_context.count_exact_postings(
                    RelationalIndexReadTarget::Transaction(view),
                    table,
                    index,
                    key,
                    self.limits,
                )
            }
            RelationalIndexReadMode::Materialized
            | RelationalIndexReadMode::Shadow(_)
            | RelationalIndexReadMode::TransactionWorkspace => None,
        };
        hawdb_executor::pipeline::runtime_checkpoint(Some(&self.context.task))?;
        match result {
            Some(Ok((count, report))) => {
                self.record_admitted_success(
                    table,
                    index,
                    &report,
                    RelationalIndexReadPurpose::MetadataCount,
                )?;
                Ok(Some(count))
            }
            Some(Err(RelationalIndexShadowError::FileDescriptors(error))) => {
                Err(HawDBError::FileDescriptors(error))
            }
            Some(Err(RelationalIndexShadowError::Admission(_)))
                if matches!(self.mode, RelationalIndexReadMode::DemandPaged(_)) =>
            {
                self.record_metadata_fallback(table, index, "metadata_count_admission_rejected")?;
                Ok(None)
            }
            Some(Err(RelationalIndexShadowError::MissingIndex { .. }))
                if matches!(self.mode, RelationalIndexReadMode::DemandPaged(_)) =>
            {
                self.record_metadata_fallback(table, index, "metadata_count_missing_index")?;
                Ok(None)
            }
            Some(Err(error @ RelationalIndexShadowError::Admission(_))) => {
                Err(HawDBError::Execution(error.to_string()))
            }
            Some(Err(error)) => Err(HawDBError::StorageIntegrity(format!(
                "relational index metadata count failed closed for {table}.{index}: {error}"
            ))),
            None => Ok(None),
        }
    }

    pub fn visit_prefix(
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

    pub fn visit_prefix_entries(
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

    /// Probes one persistent index view for a deduplicated batch of equally
    /// wide prefixes. The callback receives the requested prefix first so the
    /// caller can retain duplicate outer-row semantics outside the index read.
    pub fn visit_prefix_entries_many(
        &self,
        state: &RelationalState,
        table: &str,
        index: &str,
        prefixes: &[RelationalKey],
        mut visit: impl FnMut(&RelationalKey, &RelationalKey, &RelationalKey) -> Result<bool>,
    ) -> Result<bool> {
        let prefixes = prefixes.iter().cloned().collect::<BTreeSet<_>>();
        if prefixes.is_empty() {
            return Ok(true);
        }
        let prefixes = prefixes.into_iter().collect::<Vec<_>>();
        let prefix_width = prefixes[0].0.len();
        if prefixes.iter().any(|prefix| prefix.0.len() != prefix_width) {
            return Err(HawDBError::Execution(
                "batch index prefixes must have one common key width".to_string(),
            ));
        }
        let requested_prefix = |index_key: &RelationalKey| {
            prefixes
                .iter()
                .find(|prefix| index_key.0.starts_with(&prefix.0))
                .ok_or_else(|| {
                    HawDBError::StorageIntegrity(
                        "batch index reader emitted a key outside every requested prefix"
                            .to_string(),
                    )
                })
        };

        if matches!(
            self.mode,
            RelationalIndexReadMode::Materialized
                | RelationalIndexReadMode::Shadow(_)
                | RelationalIndexReadMode::TransactionWorkspace
        ) {
            return visit_materialized_prefix_entries_many(
                state, table, index, &prefixes, &mut visit,
            );
        }
        enum PersistentTarget<'a, R> {
            Store(&'a R),
            Transaction(&'a RelationalTransactionIndexView),
        }
        let (target, authoritative) = match self.mode {
            RelationalIndexReadMode::DemandPaged(store) => (PersistentTarget::Store(store), false),
            RelationalIndexReadMode::Authoritative(store) => (PersistentTarget::Store(store), true),
            RelationalIndexReadMode::AuthoritativeTransaction(view) => {
                (PersistentTarget::Transaction(view), true)
            }
            RelationalIndexReadMode::Materialized
            | RelationalIndexReadMode::Shadow(_)
            | RelationalIndexReadMode::TransactionWorkspace => {
                unreachable!("handled by the batch prefix fallback")
            }
        };
        let Some(remaining) = self.remaining_limits() else {
            if authoritative {
                return Err(HawDBError::Execution(format!(
                    "authoritative relational index budget is exhausted before reading {table}.{index}"
                )));
            }
            self.record_fallback(table, index, "query_index_budget_exhausted")?;
            return visit_materialized_prefix_entries_many(
                state, table, index, &prefixes, &mut visit,
            );
        };

        let mut callback_error = None;
        let mut keep_going = true;
        let mut produced_provisional_rows = false;
        let mut visit_locator = |index_key: &RelationalKey, locator: &RelationalKey| {
            produced_provisional_rows = true;
            let continue_scan =
                requested_prefix(index_key).and_then(|prefix| visit(prefix, index_key, locator));
            match continue_scan {
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
            PersistentTarget::Store(store) => store
                .visit_relational_index_read_view_prefix_entries_many_with_context(
                    table,
                    index,
                    &prefixes,
                    remaining,
                    &mut visit_locator,
                    &self.context.read_context,
                ),
            PersistentTarget::Transaction(view) => {
                Some(self.context.read_context.visit_prefix_entries_many(
                    RelationalIndexReadTarget::Transaction(view),
                    table,
                    index,
                    &prefixes,
                    remaining,
                    &mut visit_locator,
                ))
            }
        };
        if let Some(error) = callback_error {
            return Err(error);
        }
        match attempt {
            Some(Err(RelationalIndexShadowError::FileDescriptors(error))) => {
                Err(HawDBError::FileDescriptors(error))
            }
            Some(Ok(report)) => {
                self.record_admitted_success(
                    table,
                    index,
                    &report,
                    RelationalIndexReadPurpose::ExecutionProbe(
                        RelationalIndexProbeSelector::Prefix(&prefixes[0]),
                    ),
                )?;
                Ok(keep_going)
            }
            Some(Err(RelationalIndexShadowError::Admission(_))) => {
                if produced_provisional_rows {
                    return Err(HawDBError::Execution(format!(
                        "relational batch index read for {table}.{index} exhausted admission after producing provisional row locators"
                    )));
                }
                if authoritative {
                    return Err(HawDBError::Execution(format!(
                        "authoritative relational batch index read for {table}.{index} was rejected by admission"
                    )));
                }
                self.record_fallback(table, index, "admission_rejected")?;
                visit_materialized_prefix_entries_many(state, table, index, &prefixes, &mut visit)
            }
            Some(Err(RelationalIndexShadowError::MissingIndex { .. })) => {
                if produced_provisional_rows {
                    return Err(HawDBError::StorageIntegrity(format!(
                        "relational batch index {table}.{index} disappeared after producing provisional row locators"
                    )));
                }
                if authoritative {
                    return Err(HawDBError::StorageIntegrity(format!(
                        "authoritative relational index {table}.{index} is missing"
                    )));
                }
                self.record_fallback(table, index, "missing_index")?;
                visit_materialized_prefix_entries_many(state, table, index, &prefixes, &mut visit)
            }
            Some(Err(error @ RelationalIndexShadowError::Corrupt(_))) => {
                Err(HawDBError::StorageIntegrity(format!(
                    "relational batch index read failed closed for {table}.{index}: {error}"
                )))
            }
            Some(Err(error @ RelationalIndexShadowError::Durability(_)))
            | Some(Err(error @ RelationalIndexShadowError::StaleGeneration { .. })) => {
                Err(HawDBError::StorageIntegrity(format!(
                    "relational batch index identity failed closed for {table}.{index}: {error}"
                )))
            }
            None => {
                if authoritative {
                    return Err(HawDBError::StorageIntegrity(format!(
                        "authoritative relational index view is unavailable for {table}.{index}"
                    )));
                }
                self.record_fallback(table, index, "read_view_unavailable")?;
                visit_materialized_prefix_entries_many(state, table, index, &prefixes, &mut visit)
            }
        }
    }

    pub fn visit_range_entries(
        &self,
        state: &RelationalState,
        table: &str,
        index: &str,
        scan: &hawdb_storage::relational::RelationalIndexRangeScan,
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
        enum PersistentTarget<'a, R> {
            Store(&'a R),
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
                return Err(HawDBError::Execution(format!(
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
                    .visit_relational_index_read_view_prefix_entries_with_context(
                        table,
                        index,
                        prefix,
                        remaining,
                        &mut visit_locator,
                        &self.context.read_context,
                    ),
                RelationalIndexProbeSelector::Range(scan) => store
                    .visit_relational_index_read_view_range_entries_with_context(
                        table,
                        index,
                        scan,
                        remaining,
                        &mut visit_locator,
                        &self.context.read_context,
                    ),
            },
            PersistentTarget::Transaction(view) => Some(match selector {
                RelationalIndexProbeSelector::Prefix(prefix) => {
                    self.context.read_context.visit_prefix_entries(
                        RelationalIndexReadTarget::Transaction(view),
                        table,
                        index,
                        prefix,
                        remaining,
                        &mut visit_locator,
                    )
                }
                RelationalIndexProbeSelector::Range(scan) => {
                    self.context.read_context.visit_range_entries(
                        RelationalIndexReadTarget::Transaction(view),
                        table,
                        index,
                        scan,
                        remaining,
                        &mut visit_locator,
                    )
                }
            }),
        };
        if let Some(error) = callback_error {
            return Err(error);
        }
        match attempt {
            Some(Err(RelationalIndexShadowError::FileDescriptors(error))) => {
                Err(HawDBError::FileDescriptors(error))
            }
            Some(Ok(report)) => {
                self.record_admitted_success(
                    table,
                    index,
                    &report,
                    RelationalIndexReadPurpose::ExecutionProbe(selector),
                )?;
                Ok(keep_going)
            }
            Some(Err(RelationalIndexShadowError::Admission(_))) => {
                if produced_provisional_rows {
                    return Err(HawDBError::Execution(format!(
                        "relational index read for {table}.{index} exhausted admission after producing provisional row locators"
                    )));
                }
                if authoritative {
                    return Err(HawDBError::Execution(format!(
                        "authoritative relational index read for {table}.{index} was rejected by admission"
                    )));
                }
                self.record_fallback(table, index, "admission_rejected")?;
                fallback(visit)
            }
            Some(Err(RelationalIndexShadowError::MissingIndex { .. })) => {
                if produced_provisional_rows {
                    return Err(HawDBError::StorageIntegrity(format!(
                        "relational index {table}.{index} disappeared after producing provisional row locators"
                    )));
                }
                if authoritative {
                    return Err(HawDBError::StorageIntegrity(format!(
                        "authoritative relational index {table}.{index} is missing"
                    )));
                }
                self.record_fallback(table, index, "missing_index")?;
                fallback(visit)
            }
            Some(Err(error @ RelationalIndexShadowError::Corrupt(_))) => {
                Err(HawDBError::StorageIntegrity(format!(
                    "relational index read failed closed for {table}.{index}: {error}"
                )))
            }
            Some(Err(error @ RelationalIndexShadowError::Durability(_)))
            | Some(Err(error @ RelationalIndexShadowError::StaleGeneration { .. })) => {
                Err(HawDBError::StorageIntegrity(format!(
                    "relational index identity failed closed for {table}.{index}: {error}"
                )))
            }
            None => {
                if authoritative {
                    return Err(HawDBError::StorageIntegrity(format!(
                        "authoritative relational index view is unavailable for {table}.{index}"
                    )));
                }
                self.record_fallback(table, index, "read_view_unavailable")?;
                fallback(visit)
            }
        }
    }

    fn remaining_limits(&self) -> Option<RelationalIndexReadLimits> {
        let native = match self.mode {
            RelationalIndexReadMode::DemandPaged(store)
            | RelationalIndexReadMode::Authoritative(store) => {
                store.supports_relational_index_operation_admission()
            }
            RelationalIndexReadMode::AuthoritativeTransaction(_) => true,
            _ => false,
        };
        if native {
            self.context.read_context.native_operation_limits().ok()
        } else {
            self.context.read_context.remaining_limits().ok()
        }
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
        let mut state = self.context.state.borrow_mut();
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

    fn record_metadata_fallback(
        &self,
        table: &str,
        index: &str,
        reason: &'static str,
    ) -> Result<()> {
        let mut state = self.context.state.borrow_mut();
        let evidence = Self::evidence_mut(&mut state, table, index);
        evidence.lookups = checked_add(evidence.lookups, 1, "index lookup count")?;
        evidence.metadata_count_lookups =
            checked_add(evidence.metadata_count_lookups, 1, "index metadata count")?;
        // Declining a planning count is not an executed canonical fallback.
        evidence.fallback_reasons.insert(reason);
        Ok(())
    }

    #[cfg(test)]
    fn record_success(
        &self,
        table: &str,
        index: &str,
        report: &RelationalIndexReadViewReport,
        selector: RelationalIndexProbeSelector<'_>,
    ) -> Result<()> {
        self.context
            .read_context
            .admit_reported_usage(report)
            .map_err(|error| HawDBError::Execution(error.to_string()))?;
        self.record_admitted_success(
            table,
            index,
            report,
            RelationalIndexReadPurpose::ExecutionProbe(selector),
        )
    }

    fn record_admitted_success(
        &self,
        table: &str,
        index: &str,
        report: &RelationalIndexReadViewReport,
        purpose: RelationalIndexReadPurpose<'_>,
    ) -> Result<()> {
        let metrics = IndexReadMetrics::from_report(report)?;
        let mut state = self.context.state.borrow_mut();
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
            return Err(HawDBError::Execution(format!(
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
        match purpose {
            RelationalIndexReadPurpose::MetadataCount => {
                evidence.metadata_count_lookups =
                    checked_add(evidence.metadata_count_lookups, 1, "index metadata count")?;
            }
            RelationalIndexReadPurpose::ExecutionProbe(_) => match self.mode {
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
            },
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
        if let RelationalIndexReadPurpose::ExecutionProbe(RelationalIndexProbeSelector::Range(
            scan,
        )) = purpose
        {
            evidence.range_lookups = checked_add(evidence.range_lookups, 1, "range lookup count")?;
            if scan.exclusive_bound.is_some() {
                evidence.exclusive_seek_lookups =
                    checked_add(evidence.exclusive_seek_lookups, 1, "exclusive seek count")?;
            }
            if scan.direction == hawdb_storage::relational::RelationalIndexScanDirection::Backward {
                evidence.backward_lookups =
                    checked_add(evidence.backward_lookups, 1, "backward lookup count")?;
            }
        }
        if matches!(purpose, RelationalIndexReadPurpose::ExecutionProbe(_)) && report.stopped_early
        {
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
            HawDBError::Execution(format!(
                "relational index {index} on table {table} is not materialized"
            ))
        })?;
    match error {
        Some(error) => Err(error),
        None => Ok(keep_going),
    }
}

fn visit_materialized_prefix_entries_many(
    state: &RelationalState,
    table: &str,
    index: &str,
    prefixes: &[RelationalKey],
    visit: &mut dyn FnMut(&RelationalKey, &RelationalKey, &RelationalKey) -> Result<bool>,
) -> Result<bool> {
    for prefix in prefixes {
        if !visit_materialized_prefix_entries(
            state,
            table,
            index,
            prefix,
            &mut |index_key, locator| visit(prefix, index_key, locator),
        )? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn visit_materialized_range_entries<'state>(
    state: &'state RelationalState,
    table: &str,
    index: &str,
    scan: &hawdb_storage::relational::RelationalIndexRangeScan,
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
            HawDBError::Execution(format!(
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
    // Planning metadata binds an identity before any execution probe. Fallback
    // reports leave it unbound; lookup counters cannot determine the binding.
    if evidence.base_generation.is_some() && expected != observed {
        return Err(HawDBError::StorageIntegrity(
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
    delta_pages_skipped: usize,
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
                delta_pages_skipped: 0,
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
                delta_pages_skipped: recovered.delta_pages_skipped,
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
        evidence.delta_pages_skipped = checked_add(
            evidence.delta_pages_skipped,
            self.delta_pages_skipped,
            "delta skipped-page count",
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
        .ok_or_else(|| HawDBError::StorageIntegrity(format!("relational {counter} overflow")))
}
