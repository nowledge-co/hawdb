// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Snapshot-local, bounded statistics probes. Estimates never establish query
//! emptiness or another semantic proof. Posting preparation is part of the budget.

use crate::store::AdjacencyDirection;
use crate::NodeId;
use hawdb_core::time::{Duration, Instant};
use hawdb_core::{HawDBError, LabelId, RelTypeId, RuntimeTaskContext, Value};
use std::cell::{Cell, RefCell};

/// Independent ceilings for one query's complete sampling attempt, including
/// both candidate orientations. Physical reads are admitted before I/O; cached
/// data still consumes preparation/probe work. Time limits are cooperative.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JoinSamplingLimits {
    pub max_samples: usize,
    pub max_probes: u64,
    pub max_prepared_entries: u64,
    pub max_read_bytes: u64,
    pub max_read_operations: u64,
    pub max_retained_bytes: usize,
    pub max_duration: Duration,
    pub seed: u64,
}

impl Default for JoinSamplingLimits {
    fn default() -> Self {
        Self {
            max_samples: 32,
            max_probes: 1_024,
            max_prepared_entries: 16_384,
            max_read_bytes: 8 * 1024 * 1024,
            max_read_operations: 256,
            max_retained_bytes: 4 * 1024 * 1024,
            max_duration: Duration::from_millis(20),
            seed: 0x6a09_e667_f3bc_c909,
        }
    }
}

hawdb_core::impl_retained_heap_for_copy!(JoinSamplingLimits);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinSamplingNode {
    pub label: LabelId,
    pub property: String,
    pub value: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JoinSamplingHop {
    pub rel_type: RelTypeId,
    pub direction: AdjacencyDirection,
}

/// A directed chain of one or two fixed hops. Every node domain is one indexed
/// equality. Two-hop relationship types must differ, preserving relationship
/// isomorphism without adding a sampling-only execution semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinSamplingRequest {
    pub nodes: Vec<JoinSamplingNode>,
    pub hops: Vec<JoinSamplingHop>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinSamplingRefusal {
    Unsupported,
    ChangedBase,
    ProbeBudget,
    PreparationBudget,
    ReadByteBudget,
    ReadOperationBudget,
    MemoryBudget,
    IoAdmission,
    Deadline,
    EmptySample,
    Arithmetic,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct JoinSamplingWork {
    pub elapsed_nanos: u64,
    pub probes: u64,
    pub prepared_entries: u64,
    pub read_bytes: u64,
    pub read_operations: u64,
    /// Cumulative admitted capacity, a conservative bound on retained scratch.
    /// Storage's separately bounded immutable page cache is not charged here.
    pub retained_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JoinHopSample {
    pub candidate_rows: u64,
    pub output_rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinPathSample {
    pub seed_rows: u64,
    pub hops: Vec<JoinHopSample>,
    /// Estimated visited rows, including candidates rejected by node filters.
    pub work_rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinSamplingEstimates {
    pub forward: JoinPathSample,
    pub reverse: JoinPathSample,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinSamplingReport {
    pub snapshot_epoch: u64,
    pub estimates: Option<JoinSamplingEstimates>,
    pub refusal: Option<JoinSamplingRefusal>,
    pub work: JoinSamplingWork,
}

#[derive(Debug)]
pub(crate) enum SamplingError {
    Refused(JoinSamplingRefusal),
    Storage(HawDBError),
}

impl From<HawDBError> for SamplingError {
    fn from(error: HawDBError) -> Self {
        Self::Storage(error)
    }
}

pub(crate) type SamplingResult<T> = Result<T, SamplingError>;

/// A synchronous read scope. All handles borrow the same immutable store and
/// context; no sample or partially prepared posting escapes to another snapshot.
pub struct JoinSamplingContext<'a> {
    pub(crate) limits: JoinSamplingLimits,
    work: Cell<JoinSamplingWork>,
    started: Instant,
    task: Option<&'a RuntimeTaskContext>,
    bridge_error: RefCell<Option<SamplingError>>,
    io_permit: RefCell<Option<Box<dyn hawdb_core::RuntimeIoWavePermit>>>,
}

impl<'a> JoinSamplingContext<'a> {
    pub fn new(limits: JoinSamplingLimits, task: Option<&'a RuntimeTaskContext>) -> Self {
        let mut limits = limits;
        if let Some(task) = task {
            let ceiling =
                task.memory_reservation()
                    .map_or(limits.max_retained_bytes as u64, |reservation| {
                        reservation
                            .memory_bytes()
                            .min(limits.max_retained_bytes as u64)
                    });
            limits.max_retained_bytes = task.working_memory_target(ceiling) as usize;
        }
        Self {
            limits,
            work: Cell::new(JoinSamplingWork::default()),
            started: Instant::now(),
            task,
            bridge_error: RefCell::new(None),
            io_permit: RefCell::new(None),
        }
    }

    pub fn work(&self) -> JoinSamplingWork {
        JoinSamplingWork {
            elapsed_nanos: self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            ..self.work.get()
        }
    }

    pub(crate) fn checkpoint(&self) -> SamplingResult<()> {
        if let Some(task) = self.task {
            task.check_work_limit()?;
            task.checkpoint().map_err(|reason| {
                SamplingError::Storage(HawDBError::Execution(format!(
                    "runtime task stopped: {reason}"
                )))
            })?;
        }
        if self.started.elapsed() >= self.limits.max_duration {
            return Err(SamplingError::Refused(JoinSamplingRefusal::Deadline));
        }
        Ok(())
    }

    pub(crate) fn probe(&self) -> SamplingResult<()> {
        self.checkpoint()?;
        let mut work = self.work.get();
        work.probes = bounded_add(
            work.probes,
            1,
            self.limits.max_probes,
            JoinSamplingRefusal::ProbeBudget,
        )?;
        if let Some(task) = self.task {
            task.charge_work(1)?;
        }
        self.work.set(work);
        Ok(())
    }

    pub(crate) fn prepare(&self, entries: usize) -> SamplingResult<()> {
        self.checkpoint()?;
        let mut work = self.work.get();
        work.prepared_entries = bounded_add(
            work.prepared_entries,
            entries as u64,
            self.limits.max_prepared_entries,
            JoinSamplingRefusal::PreparationBudget,
        )?;
        if let Some(task) = self.task {
            task.charge_work(entries)?;
        }
        self.work.set(work);
        Ok(())
    }

    pub(crate) fn read(&self, bytes: u64) -> SamplingResult<()> {
        self.checkpoint()?;
        let mut work = self.work.get();
        let operations = bounded_add(
            work.read_operations,
            1,
            self.limits.max_read_operations,
            JoinSamplingRefusal::ReadOperationBudget,
        )?;
        let total = bounded_add(
            work.read_bytes,
            bytes,
            self.limits.max_read_bytes,
            JoinSamplingRefusal::ReadByteBudget,
        )?;
        let needs_io_permit = self.io_permit.borrow().is_none();
        if let Some(task) = self.task
            && needs_io_permit
        {
            match task.try_acquire_io_wave(std::num::NonZeroUsize::MIN) {
                Ok(hawdb_core::RuntimeIoWaveTryAcquire::Acquired(permit)) => {
                    *self.io_permit.borrow_mut() = permit;
                }
                Ok(hawdb_core::RuntimeIoWaveTryAcquire::Pending)
                | Err(hawdb_core::RuntimeIoWaveError::ReservationExceeded { .. }) => {
                    return Err(SamplingError::Refused(JoinSamplingRefusal::IoAdmission));
                }
                Err(hawdb_core::RuntimeIoWaveError::Stopped(reason)) => {
                    return Err(SamplingError::Storage(HawDBError::Execution(format!(
                        "join sampling I/O stopped: {reason}"
                    ))));
                }
            }
        }
        work.read_operations = operations;
        work.read_bytes = total;
        self.work.set(work);
        Ok(())
    }

    pub(crate) fn memory(&self, bytes: usize) -> SamplingResult<()> {
        self.checkpoint()?;
        let mut work = self.work.get();
        work.retained_bytes = work
            .retained_bytes
            .checked_add(bytes)
            .filter(|&total| total <= self.limits.max_retained_bytes)
            .ok_or(SamplingError::Refused(JoinSamplingRefusal::MemoryBudget))?;
        self.work.set(work);
        Ok(())
    }

    pub(crate) fn reserve_runtime_memory(
        &self,
    ) -> SamplingResult<Option<Box<dyn hawdb_core::RuntimeMemoryPermit>>> {
        self.checkpoint()?;
        self.task.map_or(Ok(None), |task| {
            task.reserve_working_memory(self.limits.max_retained_bytes as u64)
                .map_err(|error| match error {
                    hawdb_core::RuntimeMemoryError::Stopped(reason) => SamplingError::Storage(
                        HawDBError::Execution(format!("join sampling memory stopped: {reason}")),
                    ),
                    _ => SamplingError::Refused(JoinSamplingRefusal::MemoryBudget),
                })
        })
    }

    pub(crate) fn reserve<T>(&self, values: &mut Vec<T>, additional: usize) -> SamplingResult<()> {
        let required = values
            .len()
            .checked_add(additional)
            .ok_or(SamplingError::Refused(JoinSamplingRefusal::Arithmetic))?;
        if required > values.capacity() {
            let capacity = required
                .checked_next_power_of_two()
                .ok_or(SamplingError::Refused(JoinSamplingRefusal::MemoryBudget))?;
            let bytes = (capacity - values.capacity())
                .checked_mul(std::mem::size_of::<T>())
                .ok_or(SamplingError::Refused(JoinSamplingRefusal::MemoryBudget))?;
            self.memory(bytes)?;
            values
                .try_reserve_exact(capacity - values.len())
                .map_err(|_| SamplingError::Refused(JoinSamplingRefusal::MemoryBudget))?;
        }
        Ok(())
    }

    pub(crate) fn bridge_read(
        &self,
        bytes: u64,
    ) -> Result<(), crate::graph_descriptor_tree::GraphDescriptorTreeError> {
        let admission = (|| {
            let scratch = usize::try_from(bytes)
                .ok()
                .and_then(|bytes| bytes.checked_mul(2))
                .ok_or(SamplingError::Refused(JoinSamplingRefusal::MemoryBudget))?;
            self.memory(scratch)?;
            self.read(bytes)
        })();
        self.bridge(admission)
    }

    pub(crate) fn bridge_visit(
        &self,
        _bytes: u64,
    ) -> Result<(), crate::graph_descriptor_tree::GraphDescriptorTreeError> {
        let admission = (|| {
            self.prepare(1)?;
            // Sparse cycle tracking and traversal state, including cached pages.
            self.memory(128)
        })();
        self.bridge(admission)
    }

    fn bridge(
        &self,
        admission: SamplingResult<()>,
    ) -> Result<(), crate::graph_descriptor_tree::GraphDescriptorTreeError> {
        admission.map_err(|error| {
            *self.bridge_error.borrow_mut() = Some(error);
            crate::graph_descriptor_tree::GraphDescriptorTreeError::Admission(
                "join sampling read admission refused".into(),
            )
        })
    }

    pub(crate) fn descriptor_error(
        &self,
        error: crate::graph_descriptor_tree::GraphDescriptorTreeError,
    ) -> SamplingError {
        self.bridge_error.borrow_mut().take().unwrap_or_else(|| {
            if matches!(
                &error,
                crate::graph_descriptor_tree::GraphDescriptorTreeError::Admission(_)
            ) {
                SamplingError::Refused(JoinSamplingRefusal::PreparationBudget)
            } else {
                SamplingError::Storage(HawDBError::StorageIntegrity(error.to_string()))
            }
        })
    }
}

fn bounded_add(
    value: u64,
    additional: u64,
    limit: u64,
    reason: JoinSamplingRefusal,
) -> SamplingResult<u64> {
    value
        .checked_add(additional)
        .filter(|&total| total <= limit)
        .ok_or(SamplingError::Refused(reason))
}

pub(crate) fn sampled_rows(rows: f64) -> SamplingResult<u64> {
    if !rows.is_finite() || rows < 0.0 || rows >= u64::MAX as f64 {
        return Err(SamplingError::Refused(JoinSamplingRefusal::Arithmetic));
    }
    Ok((rows.ceil() as u64).max(1))
}

/// Uniform positions without replacement, rather than a fixed quota per source
/// tuple. Rejection before modulo avoids favoring a remainder of the domain.
pub(crate) fn sample_positions(
    population: u64,
    limit: usize,
    random: &mut u64,
    context: &JoinSamplingContext<'_>,
) -> SamplingResult<Vec<u64>> {
    let count = population.min(limit as u64);
    let mut selected = Vec::new();
    context.reserve(&mut selected, count as usize)?;
    for position in population - count..population {
        let bound = position
            .checked_add(1)
            .ok_or(SamplingError::Refused(JoinSamplingRefusal::Arithmetic))?;
        let threshold = bound.wrapping_neg() % bound;
        let draw = loop {
            context.probe()?;
            *random = random.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut value = *random;
            value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            value ^= value >> 31;
            if value >= threshold {
                break value % bound;
            }
        };
        selected.push(if selected.contains(&draw) {
            position
        } else {
            draw
        });
    }
    selected.sort_unstable();
    Ok(selected)
}

pub(crate) struct NodeSamplingPosting {
    pub(crate) ids: Vec<NodeId>,
}

impl NodeSamplingPosting {
    pub(crate) fn contains(&self, id: NodeId) -> bool {
        self.ids.binary_search(&id).is_ok()
    }
}
