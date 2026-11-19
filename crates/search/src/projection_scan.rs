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

//! Exhaustive projection reads with finite logical output and scratch bounds.

use crate::{HawDBError, Result, RuntimeTaskContext, SearchDocument, SearchIndex};
use hawdb_core::ReadBudgetResource;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchProjectionScanBudget {
    pub max_rows: usize,
    pub max_payload_bytes: usize,
    /// Independent cap on one encoded out-of-core record, including hidden rows.
    pub max_encoded_record_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchProjectionScanReport {
    pub document_count: usize,
    /// UTF-8 strings, metadata keys/values and four bytes per embedding value.
    pub payload_bytes: usize,
    /// The actual retained out-of-core publication; resident reads return None.
    pub artifact_generation: Option<u64>,
}

impl SearchProjectionScanBudget {
    pub(crate) fn validate(self) -> Result<()> {
        if self.max_rows == 0 || self.max_payload_bytes == 0 || self.max_encoded_record_bytes == 0 {
            return Err(HawDBError::Execution(
                "projection scan budgets must be positive".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn check(self, rows: usize, bytes: usize) -> Result<()> {
        if rows > self.max_rows {
            return Err(HawDBError::read_budget_exceeded(
                ReadBudgetResource::Rows,
                self.max_rows,
                "complete projection exceeds row budget",
            ));
        }
        if bytes > self.max_payload_bytes {
            return Err(HawDBError::read_budget_exceeded(
                ReadBudgetResource::PayloadBytes,
                self.max_payload_bytes,
                "complete projection exceeds payload budget",
            ));
        }
        Ok(())
    }
}

pub(crate) fn checkpoint(task: &RuntimeTaskContext) -> Result<()> {
    task.checkpoint().map_err(HawDBError::RuntimeTaskStopped)
}

pub(crate) fn add(left: usize, right: usize) -> Result<usize> {
    left.checked_add(right)
        .ok_or_else(|| HawDBError::Execution("projection scan size overflow".into()))
}

pub(crate) fn payload_bytes(document: &SearchDocument) -> Result<usize> {
    let mut bytes = add(
        add(document.id.len(), document.title.len())?,
        document.content.len(),
    )?;
    for (key, value) in &document.metadata {
        bytes = add(bytes, add(key.len(), value.len())?)?;
    }
    add(
        bytes,
        document.embedding.as_ref().map_or(Ok(0), |values| {
            values
                .len()
                .checked_mul(size_of::<f32>())
                .ok_or_else(|| HawDBError::Execution("projection vector size overflow".into()))
        })?,
    )
}

impl SearchIndex {
    /// Visits every document, with no candidate/filter limit. Order is unspecified.
    /// Only successful return establishes exhaustion. The visitor borrows each
    /// document; any copies it retains need separate host-owned accounting.
    pub fn visit_projection_documents(
        &self,
        budget: SearchProjectionScanBudget,
        task: &RuntimeTaskContext,
        visitor: &mut dyn FnMut(&SearchDocument) -> Result<()>,
    ) -> Result<SearchProjectionScanReport> {
        budget.validate()?;
        checkpoint(task)?;
        budget.check(self.documents.len(), 0)?;
        let mut bytes = 0;
        for document in self.documents.values() {
            checkpoint(task)?;
            bytes = add(bytes, payload_bytes(document)?)?;
            budget.check(self.documents.len(), bytes)?;
        }
        for document in self.documents.values() {
            checkpoint(task)?;
            visitor(document)?;
        }
        checkpoint(task)?;
        Ok(SearchProjectionScanReport {
            document_count: self.documents.len(),
            payload_bytes: bytes,
            artifact_generation: None,
        })
    }
}

#[cfg(test)]
mod tests;
