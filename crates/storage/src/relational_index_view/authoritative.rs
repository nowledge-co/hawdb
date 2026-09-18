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
use crate::{RelationalConstraintIndex, RelationalError};
use std::cell::RefCell;

#[derive(Debug, Default)]
struct AuthoritativeReadUsage {
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

    pub(super) fn record(
        &self,
        report: &RelationalIndexReadViewReport,
    ) -> Result<(), RelationalError> {
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
        let mut usage = self.usage.borrow_mut();
        usage.logical_pages = usage
            .logical_pages
            .checked_add(backend_pages)
            .ok_or_else(|| {
                RelationalError::Admission(
                    "authoritative index page accounting overflow".to_string(),
                )
            })?;
        usage.logical_bytes = usage
            .logical_bytes
            .checked_add(logical_bytes)
            .ok_or_else(|| {
                RelationalError::Admission(
                    "authoritative index byte accounting overflow".to_string(),
                )
            })?;
        usage.rows = usage.rows.checked_add(report.rows_visited).ok_or_else(|| {
            RelationalError::Admission("authoritative index row accounting overflow".to_string())
        })?;
        usage.file_bytes = usage.file_bytes.checked_add(file_bytes).ok_or_else(|| {
            RelationalError::Admission(
                "authoritative index file-byte accounting overflow".to_string(),
            )
        })?;
        if usage.logical_pages > self.limits.max_pages.get()
            || usage.logical_bytes > self.limits.max_bytes.get()
            || usage.rows > self.limits.max_rows.get()
            || usage.file_bytes > self.limits.max_file_bytes
        {
            return Err(RelationalError::Admission(
                "authoritative relational index reader exceeded its transaction budget".to_string(),
            ));
        }
        Ok(())
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
        let report = self
            .view
            .visit_exact_postings(table, index, key, self.ledger.remaining_limits()?, visit)
            .map_err(map_constraint_read_error)?;
        self.ledger.record(&report)
    }
}

pub(super) fn map_constraint_read_error(error: RelationalIndexShadowError) -> RelationalError {
    match error {
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
