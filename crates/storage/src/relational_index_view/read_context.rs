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

use super::authoritative::AuthoritativeReadLedger;
use super::transaction::{intersect_read_limits, relational_read_error};
use super::{
    IndexReadAdmission, IndexReadCharge, RelationalIndexRangeScan, RelationalIndexReadBackend,
    RelationalIndexReadLimits, RelationalIndexReadView, RelationalIndexReadViewIdentity,
    RelationalIndexReadViewReport, RelationalIndexShadowError, RelationalKey,
    RelationalTransactionIndexView,
};
use crate::relational::{IndexReadObserver, IndexReadPreflightError, RelationalIndexShadowReader};
use hawdb_core::RuntimeTaskContext;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::Arc;

/// A caller-owned cumulative allowance shared by all reads in one statement.
/// Operation charges precede I/O and visitor reentry; reports remain per-read.
#[derive(Debug)]
pub struct RelationalIndexReadContext {
    ledger: AuthoritativeReadLedger,
    task: RuntimeTaskContext,
    limits: RelationalIndexReadLimits,
    exact_counts: RefCell<ExactPostingCounts>,
}

#[derive(Default)]
struct ExactPostingCounts {
    reader: Option<Arc<RelationalIndexShadowReader>>,
    payload_bytes: usize,
    values: BTreeMap<(String, String, RelationalKey), (RelationalIndexReadViewIdentity, u64)>,
}

impl std::fmt::Debug for ExactPostingCounts {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExactPostingCounts")
            .field("reader_bound", &self.reader.is_some())
            .field("payload_bytes", &self.payload_bytes)
            .field("values", &self.values)
            .finish()
    }
}

impl ExactPostingCounts {
    fn select_reader(&mut self, reader: &Arc<RelationalIndexShadowReader>) {
        if !self
            .reader
            .as_ref()
            .is_some_and(|bound| Arc::ptr_eq(bound, reader))
        {
            // Retain one source to prevent pointer reuse; equal metadata does
            // not identify postings in independently opened databases.
            self.reader = Some(reader.clone());
            self.values.clear();
            self.payload_bytes = 0;
        }
    }
}

#[derive(Clone, Copy)]
pub enum RelationalIndexReadTarget<'a> {
    View(&'a RelationalIndexReadView),
    Transaction(&'a RelationalTransactionIndexView),
}

impl RelationalIndexReadContext {
    pub fn new(limits: RelationalIndexReadLimits) -> Self {
        Self::with_task(limits, RuntimeTaskContext::default())
    }

    pub fn with_task(limits: RelationalIndexReadLimits, task: RuntimeTaskContext) -> Self {
        Self {
            ledger: AuthoritativeReadLedger::new(limits),
            task,
            limits,
            exact_counts: RefCell::new(ExactPostingCounts::default()),
        }
    }

    /// Count metadata and row probes share this statement's allowance and task.
    /// Only successful eligible counts are cached. Reuse rechecks eligibility,
    /// known poison, the retained reader, identity and ledger health; it performs
    /// no page traversal. Switching readers drops the previous source's cache.
    /// Cache entries and retained key payload are bounded by the page and byte
    /// limits. A declined capability creates neither a read attempt nor a cache.
    pub fn count_exact_postings(
        &self,
        target: RelationalIndexReadTarget<'_>,
        table: &str,
        index: &str,
        key: &RelationalKey,
        limits: RelationalIndexReadLimits,
    ) -> Option<Result<(u64, RelationalIndexReadViewReport), RelationalIndexShadowError>> {
        let view = match target {
            RelationalIndexReadTarget::View(view) => view
                .exact_posting_count_reader(table, index, key)
                .map(|reader| reader.map(|_| view)),
            RelationalIndexReadTarget::Transaction(view) => {
                view.exact_posting_count_view(table, index, key)
            }
        }?;
        Some((|| {
            let view = view?;
            self.checkpoint()?;
            self.ledger.check_health().map_err(relational_read_error)?;
            let RelationalIndexReadBackend::Base(reader) = &view.backend else {
                unreachable!("eligible metadata counts use an immutable checkpoint reader")
            };
            self.exact_counts.borrow_mut().select_reader(reader);
            // Bound the owned cache key before allocating or cloning it.
            let payload = table.len().checked_add(index.len()).and_then(|bytes| {
                key.0.iter().try_fold(bytes, |bytes, value| {
                    bytes.checked_add(value.estimated_payload_bytes())
                })
            });
            let cache_key = payload
                .filter(|bytes| *bytes <= self.limits.max_bytes.get())
                .map(|_| (table.to_string(), index.to_string(), key.clone()));
            if let Some(cache_key) = &cache_key
                && let Some((identity, count)) = self.exact_counts.borrow().values.get(cache_key)
                && *identity == view.identity()
            {
                return Ok((*count, view.exact_posting_count_report(Default::default())));
            }
            let result = self.read_value(limits, |limits, admission| match target {
                RelationalIndexReadTarget::View(view) => view
                    .count_exact_postings_with_reader(table, index, key, |reader| {
                        reader.count_exact_postings_admitted(table, index, key, limits, admission)
                    })
                    .expect("immutable eligible checkpoint count remains available"),
                RelationalIndexReadTarget::Transaction(view) => view
                    .count_exact_postings_admitted(table, index, key, limits, admission)
                    .expect("immutable eligible transaction count remains available"),
            })?;
            if let (Some(cache_key), Some(payload)) = (cache_key, payload) {
                let mut cache = self.exact_counts.borrow_mut();
                if let Some(cached) = cache.values.get_mut(&cache_key) {
                    *cached = (view.identity(), result.0);
                } else if cache.values.len() < self.limits.max_pages.get()
                    && cache.payload_bytes.saturating_add(payload) <= self.limits.max_bytes.get()
                {
                    cache.payload_bytes += payload;
                    cache.values.insert(cache_key, (view.identity(), result.0));
                }
            }
            Ok(result)
        })())
    }

    fn checkpoint(&self) -> Result<(), RelationalIndexShadowError> {
        self.task.checkpoint().map_err(|reason| {
            RelationalIndexShadowError::Admission(format!("runtime task stopped: {reason}"))
        })
    }

    pub fn remaining_limits(
        &self,
    ) -> Result<RelationalIndexReadLimits, RelationalIndexShadowError> {
        self.ledger
            .remaining_limits()
            .map_err(relational_read_error)
    }

    /// Positive transport limits for providers that admit every native action.
    /// Zero page/byte allowance uses one for API compatibility. The bounded
    /// configured row window permits tombstone filtering; the unchanged ledger
    /// still refuses every output row or other charge beyond its true cap.
    pub fn native_operation_limits(
        &self,
    ) -> Result<RelationalIndexReadLimits, RelationalIndexShadowError> {
        self.ledger
            .native_operation_limits()
            .map_err(relational_read_error)
    }

    /// Admit known logical usage without reading storage. Persistent reads must
    /// use the operation methods below so nested callbacks cannot reuse usage.
    #[doc(hidden)]
    pub fn admit_reported_usage(
        &self,
        report: &RelationalIndexReadViewReport,
    ) -> Result<(), RelationalIndexShadowError> {
        self.ledger
            .begin_read()
            .map_err(relational_read_error)?
            .record(report)
            .map_err(relational_read_error)
    }

    pub fn visit_prefix_entries(
        &self,
        target: RelationalIndexReadTarget<'_>,
        table: &str,
        index: &str,
        prefix: &RelationalKey,
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Result<RelationalIndexReadViewReport, RelationalIndexShadowError> {
        self.visit_range_entries(
            target,
            table,
            index,
            &RelationalIndexRangeScan {
                prefix: prefix.clone(),
                exclusive_bound: None,
                direction: super::RelationalIndexScanDirection::Forward,
            },
            limits,
            visit,
        )
    }

    pub fn visit_range_entries(
        &self,
        target: RelationalIndexReadTarget<'_>,
        table: &str,
        index: &str,
        scan: &RelationalIndexRangeScan,
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Result<RelationalIndexReadViewReport, RelationalIndexShadowError> {
        self.read(limits, |limits, admission| match target {
            RelationalIndexReadTarget::View(view) => {
                view.visit_range_entries_admitted(table, index, scan, limits, visit, admission)
            }
            RelationalIndexReadTarget::Transaction(view) => {
                view.visit_range_entries_admitted(table, index, scan, limits, visit, admission)
            }
        })
    }

    pub fn visit_prefix_entries_many(
        &self,
        target: RelationalIndexReadTarget<'_>,
        table: &str,
        index: &str,
        prefixes: &[RelationalKey],
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Result<RelationalIndexReadViewReport, RelationalIndexShadowError> {
        self.read(limits, |limits, admission| match target {
            RelationalIndexReadTarget::View(view) => view.visit_prefix_entries_many_admitted(
                table, index, prefixes, limits, visit, admission,
            ),
            RelationalIndexReadTarget::Transaction(view) => view
                .visit_prefix_entries_many_admitted(
                    table, index, prefixes, limits, visit, admission,
                ),
        })
    }

    fn read(
        &self,
        limits: RelationalIndexReadLimits,
        read: impl FnOnce(
            RelationalIndexReadLimits,
            IndexReadAdmission<'_>,
        ) -> Result<RelationalIndexReadViewReport, RelationalIndexShadowError>,
    ) -> Result<RelationalIndexReadViewReport, RelationalIndexShadowError> {
        self.read_value(limits, |limits, admission| {
            read(limits, admission).map(|report| ((), report))
        })
        .map(|(_, report)| report)
    }

    fn read_value<T>(
        &self,
        limits: RelationalIndexReadLimits,
        read: impl FnOnce(
            RelationalIndexReadLimits,
            IndexReadAdmission<'_>,
        )
            -> Result<(T, RelationalIndexReadViewReport), RelationalIndexShadowError>,
    ) -> Result<(T, RelationalIndexReadViewReport), RelationalIndexShadowError> {
        self.checkpoint()?;
        let attempt = self
            .ledger
            .begin_native_read()
            .map_err(relational_read_error)?;
        let limits = intersect_read_limits(limits, attempt.limits());
        let observer = TaskIndexReadAdmission {
            context: self,
            inner: IndexReadAdmission::new(&attempt),
        };
        let result = read(limits, IndexReadAdmission::new(&observer));
        attempt.finish(result)
    }
}

struct TaskIndexReadAdmission<'a> {
    context: &'a RelationalIndexReadContext,
    inner: IndexReadAdmission<'a>,
}

impl IndexReadObserver for TaskIndexReadAdmission<'_> {
    fn check_charge(&self, charge: IndexReadCharge) -> Result<(), IndexReadPreflightError> {
        self.context
            .checkpoint()
            .map_err(IndexReadPreflightError::Failed)?;
        self.inner.check_charge(charge)
    }

    fn set_budget_refusal(&self, refused: bool) {
        self.inner.set_budget_refusal(refused);
    }

    fn charge(&self, charge: IndexReadCharge) -> Result<(), RelationalIndexShadowError> {
        self.context.checkpoint()?;
        self.inner.charge(charge)
    }

    fn file_budget(&self, requested: usize) -> Result<usize, RelationalIndexShadowError> {
        self.context.checkpoint()?;
        self.inner.file_budget(requested)
    }
}

pub(super) struct PairedIndexReadAdmission<'a> {
    pub local: IndexReadAdmission<'a>,
    pub outer: IndexReadAdmission<'a>,
}

impl IndexReadObserver for PairedIndexReadAdmission<'_> {
    fn check_charge(&self, charge: IndexReadCharge) -> Result<(), IndexReadPreflightError> {
        self.local.check_charge(charge)?;
        self.outer.check_charge(charge)
    }

    fn set_budget_refusal(&self, refused: bool) {
        self.local.set_budget_refusal(refused);
        self.outer.set_budget_refusal(refused);
    }

    fn charge(&self, charge: IndexReadCharge) -> Result<(), RelationalIndexShadowError> {
        // Both owners must accept before either receives work; there is no
        // payload, callback or ledger mutation between these checks.
        IndexReadAdmission::new(self).preflight_charge(charge)?;
        self.local.charge(charge)?;
        self.outer.charge(charge)
    }

    fn file_budget(&self, requested: usize) -> Result<usize, RelationalIndexShadowError> {
        self.outer.file_budget(self.local.file_budget(requested)?)
    }
}
