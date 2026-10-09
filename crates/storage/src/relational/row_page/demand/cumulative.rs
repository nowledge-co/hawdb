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

//! Statement admission, independent of individual traversal lifetimes.

use super::{RelationalRowPageDemandReadError, RelationalRowPageDemandReadReport};
use crate::relational::row_page::snapshot::RelationalRowPageSnapshotReadLimits;
use std::sync::Mutex;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RelationalRowPageCumulativeReadReport {
    pub demand: RelationalRowPageDemandReadReport,
    pub overlay_entries: usize,
    /// Sum of each operation's admitted peak, including active traversals.
    pub overlay_resident_bytes: usize,
    /// Includes admitted rows that subsequently failed hydration/resolution,
    /// and rows from a statement's other backend.
    pub admitted_rows: usize,
}

/// Remaining statement admission, including active and failed work and any
/// previously tightened caps. Zero denotes exhaustion, not a missing budget.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RelationalRowPageCumulativeReadRemaining {
    pub pages: usize,
    pub bytes: usize,
    pub rows: usize,
    pub overlay_entries: usize,
    pub overlay_resident_bytes: usize,
}

const RESOURCES: [&str; 5] = ["page", "byte", "row", "overlay-entry", "overlay-byte"];

#[derive(Default)]
pub(crate) struct CumulativeReadBudget {
    state: Mutex<Option<CumulativeReadState>>,
}

struct CumulativeReadState {
    limits: [usize; 5],
    used: [usize; 5],
    report: RelationalRowPageCumulativeReadReport,
}

impl CumulativeReadState {
    fn admit(&mut self, work: [usize; 5]) -> Result<(), RelationalRowPageDemandReadError> {
        let mut next = self.used;
        for (i, amount) in work.into_iter().enumerate() {
            next[i] = next[i].checked_add(amount).ok_or_else(|| {
                RelationalRowPageDemandReadError::Admission(format!(
                    "cumulative relational {} counter overflow",
                    RESOURCES[i]
                ))
            })?;
            if next[i] > self.limits[i] {
                return Err(RelationalRowPageDemandReadError::Admission(format!(
                    "cumulative relational {} read needs {}, exceeding limit {}",
                    RESOURCES[i], next[i], self.limits[i]
                )));
            }
        }
        self.used = next;
        self.report.admitted_rows = next[2];
        Ok(())
    }
}

impl CumulativeReadBudget {
    /// First attachment begins the statement. Later attachment can only
    /// tighten its limits: neither usage nor evidence can be reset.
    pub(crate) fn restrict(
        &self,
        limits: RelationalRowPageSnapshotReadLimits,
    ) -> Result<(), RelationalRowPageDemandReadError> {
        let caps = [
            limits.demand.max_pages.get(),
            limits.demand.max_bytes.get(),
            limits.demand.max_rows.get(),
            limits.max_overlay_entries.get(),
            limits.max_overlay_bytes.get(),
        ];
        let mut guard = self.state.lock().unwrap_or_else(|error| error.into_inner());
        match guard.as_mut() {
            Some(state) => {
                let mut next = state.limits;
                for (i, cap) in caps.into_iter().enumerate() {
                    next[i] = next[i].min(cap);
                    if state.used[i] > next[i] {
                        return Err(RelationalRowPageDemandReadError::Admission(format!(
                            "cumulative relational {} limit {} is below already admitted {}",
                            RESOURCES[i], next[i], state.used[i]
                        )));
                    }
                }
                state.limits = next;
            }
            None => {
                *guard = Some(CumulativeReadState {
                    limits: caps,
                    used: [0; 5],
                    report: Default::default(),
                })
            }
        }
        Ok(())
    }

    pub(crate) fn report(&self) -> Option<RelationalRowPageCumulativeReadReport> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .map(|state| state.report)
    }

    pub(crate) fn remaining(&self) -> Option<RelationalRowPageCumulativeReadRemaining> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .map(|state| RelationalRowPageCumulativeReadRemaining {
                pages: state.limits[0] - state.used[0],
                bytes: state.limits[1] - state.used[1],
                rows: state.limits[2] - state.used[2],
                overlay_entries: state.limits[3] - state.used[3],
                overlay_resident_bytes: state.limits[4] - state.used[4],
            })
    }

    pub(crate) fn admit_page(&self, bytes: usize) -> Result<(), RelationalRowPageDemandReadError> {
        self.update(|state| {
            state.admit([1, bytes, 0, 0, 0])?;
            // Logical attempted work remains charged on an I/O failure;
            // physical/cache evidence is recorded only after the actual read.
            state.report.demand.pages_read += 1;
            state.report.demand.bytes_read += bytes;
            Ok(())
        })
    }

    pub(crate) fn admit_row(&self) -> Result<(), RelationalRowPageDemandReadError> {
        self.update(|state| state.admit([0, 0, 1, 0, 0]))
    }

    pub(crate) fn admit_overlay(
        &self,
        entries: usize,
        bytes: usize,
    ) -> Result<(), RelationalRowPageDemandReadError> {
        if entries == 0 && bytes == 0 {
            return Ok(());
        }
        self.update(|state| {
            state.admit([0, 0, 0, entries, bytes])?;
            state.report.overlay_entries += entries;
            state.report.overlay_resident_bytes += bytes;
            Ok(())
        })
    }

    /// Mixed-source statement work is charged here but reported by its own
    /// runtime, so it is not counted twice as snapshot work.
    pub(crate) fn admit_external(
        &self,
        pages: usize,
        bytes: usize,
        rows: usize,
    ) -> Result<(), RelationalRowPageDemandReadError> {
        self.update(|state| state.admit([pages, bytes, rows, 0, 0]))
    }

    pub(crate) fn record(
        &self,
        delta: RelationalRowPageDemandReadReport,
    ) -> Result<(), RelationalRowPageDemandReadError> {
        self.update(|state| {
            let mut next = state.report.demand;
            macro_rules! add {
                ($field:ident) => {
                    next.$field = next.$field.checked_add(delta.$field).ok_or_else(|| {
                        RelationalRowPageDemandReadError::Admission(format!(
                            "cumulative relational {} counter overflow",
                            stringify!($field)
                        ))
                    })?;
                };
            }
            add!(descriptor_reads);
            add!(file_pages_read);
            add!(file_bytes_read);
            add!(cache_hits);
            add!(cache_misses);
            add!(cache_admission_rejections);
            add!(rows_decoded);
            add!(rows_emitted);
            add!(borrowed_rows_emitted);
            add!(owned_rows_emitted);
            state.report.demand = next;
            Ok(())
        })
    }

    fn update(
        &self,
        operation: impl FnOnce(&mut CumulativeReadState) -> Result<(), RelationalRowPageDemandReadError>,
    ) -> Result<(), RelationalRowPageDemandReadError> {
        let mut guard = self.state.lock().unwrap_or_else(|error| error.into_inner());
        match guard.as_mut() {
            Some(state) => operation(state),
            None => Ok(()),
        }
    }
}
