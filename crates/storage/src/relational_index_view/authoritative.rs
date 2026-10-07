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
use crate::relational::{IndexReadObserver, RelationalConstraintIndex, RelationalError};
use std::cell::RefCell;

#[derive(Debug, Default, Clone, Copy)]
struct AuthoritativeReadUsage {
    closed_after_failed_read: bool,
    logical_pages: usize,
    logical_bytes: usize,
    rows: usize,
    file_bytes: usize,
}

#[derive(Debug)]
pub(super) struct AuthoritativeReadLedger {
    limits: RelationalIndexReadLimits,
    usage: RefCell<AuthoritativeReadUsage>,
}

impl AuthoritativeReadLedger {
    pub(super) fn new(limits: RelationalIndexReadLimits) -> Self {
        Self {
            limits,
            usage: RefCell::new(AuthoritativeReadUsage::default()),
        }
    }

    pub(super) fn remaining_limits(&self) -> Result<RelationalIndexReadLimits, RelationalError> {
        let usage = self.usage.borrow();
        if usage.closed_after_failed_read {
            return Err(RelationalError::Admission(
                "authoritative relational index read ledger is closed after a failed read"
                    .to_string(),
            ));
        }
        let max_pages = self
            .limits
            .max_pages
            .get()
            .checked_sub(usage.logical_pages)
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| {
                RelationalError::Admission(
                    "authoritative relational index page budget is exhausted".to_string(),
                )
            })?;
        let max_rows = self
            .limits
            .max_rows
            .get()
            .checked_sub(usage.rows)
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| {
                RelationalError::Admission(
                    "authoritative relational index row budget is exhausted".to_string(),
                )
            })?;
        let max_bytes = self
            .limits
            .max_bytes
            .get()
            .checked_sub(usage.logical_bytes)
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| {
                RelationalError::Admission(
                    "authoritative relational index byte budget is exhausted".to_string(),
                )
            })?;
        Ok(RelationalIndexReadLimits {
            max_pages,
            max_rows,
            max_bytes,
            max_file_bytes: self
                .limits
                .max_file_bytes
                .checked_sub(usage.file_bytes)
                .ok_or_else(|| {
                    RelationalError::Admission(
                        "authoritative relational index file-byte accounting overflow".to_string(),
                    )
                })?,
            max_tree_height: self.limits.max_tree_height,
        })
    }

    pub(super) fn begin_read(&self) -> Result<AuthoritativeReadAttempt<'_>, RelationalError> {
        Ok(AuthoritativeReadAttempt {
            ledger: self,
            limits: self.remaining_limits()?,
            recorded: false,
            charged: RefCell::new(AuthoritativeReadUsage::default()),
        })
    }

    #[cfg(test)]
    pub(super) fn record(
        &self,
        report: &RelationalIndexReadViewReport,
    ) -> Result<(), RelationalError> {
        self.begin_read()?.record(report)
    }

    fn charge_usage(&self, delta: AuthoritativeReadUsage) -> Result<(), RelationalError> {
        let mut usage = self.usage.borrow_mut();
        if usage.closed_after_failed_read {
            return Err(RelationalError::Admission(
                "authoritative relational index read ledger is closed after a failed read"
                    .to_string(),
            ));
        }
        let next = usage.add(delta)?;
        if next.logical_pages > self.limits.max_pages.get()
            || next.logical_bytes > self.limits.max_bytes.get()
            || next.rows > self.limits.max_rows.get()
            || next.file_bytes > self.limits.max_file_bytes
        {
            return Err(RelationalError::Admission(
                "authoritative relational index reader exceeded its transaction budget".to_string(),
            ));
        }
        *usage = next;
        Ok(())
    }
}

impl AuthoritativeReadUsage {
    fn from_report(report: &RelationalIndexReadViewReport) -> Result<Self, RelationalError> {
        let (backend_pages, backend_bytes, file_bytes) = match &report.backend {
            RelationalIndexReadViewBackendReport::Base(report) => {
                (report.pages_read, report.bytes_read, report.file_bytes_read)
            }
            RelationalIndexReadViewBackendReport::Recovered(report) => {
                let pages = report
                    .base
                    .pages_read
                    .checked_add(report.delta_pages_read)
                    .ok_or_else(|| {
                        RelationalError::Admission(
                            "authoritative index page accounting overflow".to_string(),
                        )
                    })?;
                let bytes = report
                    .base
                    .bytes_read
                    .checked_add(report.delta_bytes_read)
                    .ok_or_else(|| {
                        RelationalError::Admission(
                            "authoritative index byte accounting overflow".to_string(),
                        )
                    })?;
                let file_bytes = report
                    .base
                    .file_bytes_read
                    .checked_add(report.delta_file_bytes_read)
                    .ok_or_else(|| {
                        RelationalError::Admission(
                            "authoritative index file-byte accounting overflow".to_string(),
                        )
                    })?;
                (pages, bytes, file_bytes)
            }
        };
        let logical_bytes = backend_bytes
            .checked_add(report.live_bytes_visited)
            .ok_or_else(|| {
                RelationalError::Admission(
                    "authoritative index live-byte accounting overflow".to_string(),
                )
            })?;
        Ok(Self {
            logical_pages: backend_pages,
            logical_bytes,
            file_bytes,
            rows: report.rows_visited,
            closed_after_failed_read: false,
        })
    }

    fn add(self, other: Self) -> Result<Self, RelationalError> {
        let checked = |a: usize, b: usize| {
            a.checked_add(b).ok_or_else(|| {
                RelationalError::Admission("authoritative index accounting overflow".to_string())
            })
        };
        Ok(Self {
            logical_pages: checked(self.logical_pages, other.logical_pages)?,
            logical_bytes: checked(self.logical_bytes, other.logical_bytes)?,
            rows: checked(self.rows, other.rows)?,
            file_bytes: checked(self.file_bytes, other.file_bytes)?,
            closed_after_failed_read: self.closed_after_failed_read,
        })
    }

    fn remaining_report(self, charged: Self) -> Result<Self, RelationalError> {
        let checked = |a: usize, b: usize| {
            a.checked_sub(b).ok_or_else(|| {
                RelationalError::Admission(
                    "authoritative index report disagrees with admitted operations".to_string(),
                )
            })
        };
        Ok(Self {
            logical_pages: checked(self.logical_pages, charged.logical_pages)?,
            logical_bytes: checked(self.logical_bytes, charged.logical_bytes)?,
            rows: checked(self.rows, charged.rows)?,
            file_bytes: checked(self.file_bytes, charged.file_bytes)?,
            closed_after_failed_read: false,
        })
    }
}

/// An admitted read records its complete report or closes this ledger. Typed
/// descriptor refusal retains operation charges and health for a later retry.
/// Other errors and unwinds cannot discard partial work and reuse allowance.
pub(super) struct AuthoritativeReadAttempt<'a> {
    ledger: &'a AuthoritativeReadLedger,
    limits: RelationalIndexReadLimits,
    recorded: bool,
    charged: RefCell<AuthoritativeReadUsage>,
}

impl AuthoritativeReadAttempt<'_> {
    pub(super) fn limits(&self) -> RelationalIndexReadLimits {
        self.limits
    }

    /// Settle every admitted metadata, visitor and constraint read consistently.
    /// Descriptor acquisition cannot discard already charged work; all other
    /// failures leave Drop to close the ledger conservatively.
    pub(super) fn finish<T>(
        self,
        result: Result<(T, RelationalIndexReadViewReport), RelationalIndexShadowError>,
    ) -> Result<(T, RelationalIndexReadViewReport), RelationalIndexShadowError> {
        let (value, report) = match result {
            Err(error @ RelationalIndexShadowError::FileDescriptors(_)) => {
                self.finish_descriptor_rejection();
                return Err(error);
            }
            result => result?,
        };
        self.record(&report)
            .map_err(super::transaction::relational_read_error)?;
        Ok((value, report))
    }

    pub(super) fn record(
        mut self,
        report: &RelationalIndexReadViewReport,
    ) -> Result<(), RelationalError> {
        let remaining = AuthoritativeReadUsage::from_report(report)?
            .remaining_report(*self.charged.borrow())?;
        self.ledger.charge_usage(remaining)?;
        self.recorded = true;
        Ok(())
    }

    /// Operation observers have already charged all completed work. A typed
    /// descriptor-acquisition rejection consumes no partial file payload, so
    /// the owning statement or transaction may retry without refunding charges.
    pub(super) fn finish_descriptor_rejection(mut self) {
        self.recorded = true;
    }
}

impl IndexReadObserver for AuthoritativeReadAttempt<'_> {
    fn charge(&self, charge: IndexReadCharge) -> Result<(), RelationalIndexShadowError> {
        let mut delta = AuthoritativeReadUsage::default();
        match charge {
            IndexReadCharge::Page(bytes) => {
                delta.logical_pages = 1;
                delta.logical_bytes = bytes;
            }
            IndexReadCharge::FileBytes(bytes) => delta.file_bytes = bytes,
            IndexReadCharge::LiveBytes(bytes) => delta.logical_bytes = bytes,
            IndexReadCharge::Row => delta.rows = 1,
        }
        self.ledger
            .charge_usage(delta)
            .map_err(super::transaction::relational_read_error)?;
        let mut charged = self.charged.borrow_mut();
        *charged = charged
            .add(delta)
            .map_err(super::transaction::relational_read_error)?;
        Ok(())
    }

    fn file_budget(&self, requested: usize) -> Result<usize, RelationalIndexShadowError> {
        let usage = self.ledger.usage.borrow();
        if usage.closed_after_failed_read {
            return Err(super::admission(
                "authoritative relational index read ledger is closed after a failed read",
            ));
        }
        let remaining = self
            .ledger
            .limits
            .max_file_bytes
            .checked_sub(usage.file_bytes)
            .ok_or_else(|| super::admission("authoritative index file-byte accounting overflow"))?;
        Ok(requested.min(remaining))
    }
}

impl Drop for AuthoritativeReadAttempt<'_> {
    fn drop(&mut self) {
        if !self.recorded {
            self.ledger.usage.borrow_mut().closed_after_failed_read = true;
        }
    }
}

/// Transaction-scoped persistent constraint reader.
///
/// The owned `Arc` pins one immutable visibility epoch. A single ledger spans
/// every primary, unique, UPSERT, and foreign-key probe in the transaction so
/// a sequence of individually small lookups cannot bypass admission.
pub struct AuthoritativeRelationalConstraintIndex {
    view: Arc<RelationalIndexReadView>,
    ledger: AuthoritativeReadLedger,
}

impl AuthoritativeRelationalConstraintIndex {
    pub fn new(view: Arc<RelationalIndexReadView>, limits: RelationalIndexReadLimits) -> Self {
        Self {
            view,
            ledger: AuthoritativeReadLedger::new(limits),
        }
    }
}

impl RelationalConstraintIndex for AuthoritativeRelationalConstraintIndex {
    fn visit_exact_primary_keys(
        &self,
        table: &str,
        index: &str,
        key: &RelationalKey,
        visit: &mut dyn FnMut(&RelationalKey) -> bool,
    ) -> Result<(), RelationalError> {
        let attempt = self.ledger.begin_read()?;
        let result = self
            .view
            .visit_exact_postings_admitted(
                table,
                index,
                key,
                attempt.limits(),
                visit,
                IndexReadAdmission::new(&attempt),
            )
            .map(|report| ((), report));
        attempt
            .finish(result)
            .map(|_| ())
            .map_err(map_constraint_read_error)
    }
}

pub(super) fn map_constraint_read_error(error: RelationalIndexShadowError) -> RelationalError {
    match error {
            RelationalIndexShadowError::FileDescriptors(error) => RelationalError::FileDescriptors(error),
        RelationalIndexShadowError::Admission(message) => RelationalError::Admission(message),
        RelationalIndexShadowError::Durability(message) => RelationalError::Durability(message),
        RelationalIndexShadowError::Corrupt(message) => RelationalError::Corruption(message),
        RelationalIndexShadowError::MissingIndex { table, index } => RelationalError::Corruption(
            format!("authoritative relational index {table}.{index} is missing"),
        ),
        RelationalIndexShadowError::StaleGeneration {
            expected_previous,
            actual_previous,
        } => RelationalError::Corruption(format!(
            "authoritative relational index generation changed: expected {expected_previous:?}, got {actual_previous:?}"
        )),
    }
}
