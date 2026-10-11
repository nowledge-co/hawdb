// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Shared admission for retained result buffers and independently owned views.
//!
//! These reservations own charges, not query payload. A producer must reserve
//! before allocating and keep the reservation until its payload is destroyed.

use super::*;
use retained_memory::reserve_retained_memory;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeRetainedResultResource {
    Bytes,
    Handles,
    QueryBytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeRetainedResultError {
    /// Retryable exhaustion is backpressure; an indivisible request larger
    /// than the complete allowance is a working-unit-too-large error.
    Capacity {
        resource: RuntimeRetainedResultResource,
        requested: u64,
        available: u64,
        retryable: bool,
    },
    Admission(RuntimeAdmissionError),
    SizeOverflow,
}

impl RuntimeRetainedResultError {
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Capacity { retryable, .. } => *retryable,
            Self::Admission(error) => error.is_retryable(),
            Self::SizeOverflow => false,
        }
    }
}

impl Display for RuntimeRetainedResultError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Capacity {
                resource,
                requested,
                available,
                retryable,
            } => write!(
                formatter,
                "retained result {} ({resource:?}): requested {requested}, available {available}",
                if *retryable {
                    "backpressure"
                } else {
                    "working unit too large"
                },
            ),
            Self::Admission(error) => Display::fmt(error, formatter),
            Self::SizeOverflow => formatter.write_str("retained result size overflow"),
        }
    }
}

impl Error for RuntimeRetainedResultError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Admission(error) => Some(error),
            _ => None,
        }
    }
}

impl From<RuntimeAdmissionError> for RuntimeRetainedResultError {
    fn from(error: RuntimeAdmissionError) -> Self {
        Self::Admission(error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RuntimeRetainedResultSnapshot {
    pub budget_bytes: u64,
    pub handle_limit: usize,
    pub retained_bytes: u64,
    pub peak_retained_bytes: u64,
    /// One owner may cover multiple buffers whose capacities were summed.
    pub buffer_owners: usize,
    pub view_handles: usize,
    pub peak_view_handles: usize,
    pub backpressure_events: u64,
}

/// One admitted view of a shared immutable result allocation owner.
///
/// Deliberately not `Clone`: every extra view must use [`Self::try_retain`].
/// Capacities remain charged once even after the active permit is dropped.
/// This token does not keep a query, database, CPU slot or I/O slot alive.
#[derive(Debug)]
pub struct RuntimeRetainedResult {
    owner: Arc<ResultBufferOwner>,
    handle: ResultCharge,
}

#[derive(Debug)]
struct ResultBufferOwner {
    charge: ResultCharge,
    capacity_bytes: u64,
    priority: RuntimeWorkPriority,
}

#[derive(Debug, Clone, Copy)]
enum ChargeKind {
    BufferOwner,
    ViewHandle,
}

#[derive(Debug)]
struct ResultCharge {
    memory: RuntimeRetainedMemory,
    kind: ChargeKind,
}

impl RuntimeGovernor {
    /// Aggregate retained-result state shared by every clone of this governor.
    pub fn retained_result_snapshot(&self) -> RuntimeRetainedResultSnapshot {
        let state = mutex_lock(&self.inner.state);
        RuntimeRetainedResultSnapshot {
            budget_bytes: state.limits.result_budget_bytes,
            handle_limit: self.inner.config.retained_result_handle_limit.get(),
            ..state.retained_results
        }
    }
}

impl RuntimePermit {
    /// Admit a new immutable buffer owner and its first independently owned view.
    ///
    /// `capacity_bytes` includes all retained payload/validity/selection and
    /// producer-owner metadata. `view_metadata_bytes` includes caller-owned
    /// descriptors and foreign wrapper overhead. Native token/Arc layout costs
    /// are added automatically. No payload may be allocated before success.
    /// Charges overlap this permit conservatively; closing the permit cannot
    /// revoke an already admitted immutable result.
    pub fn reserve_retained_result(
        &self,
        capacity_bytes: u64,
        view_metadata_bytes: u64,
    ) -> Result<RuntimeRetainedResult, RuntimeRetainedResultError> {
        let owner_bytes = capacity_bytes
            .checked_add(RuntimeRetainedResult::owner_overhead_bytes())
            .ok_or(RuntimeRetainedResultError::SizeOverflow)?;
        let handle_bytes = view_metadata_bytes
            .checked_add(RuntimeRetainedResult::handle_overhead_bytes())
            .ok_or(RuntimeRetainedResultError::SizeOverflow)?;
        let requested = owner_bytes
            .checked_add(handle_bytes)
            .ok_or(RuntimeRetainedResultError::SizeOverflow)?;
        if requested > self.request.result_bytes {
            return Err(RuntimeRetainedResultError::Capacity {
                resource: RuntimeRetainedResultResource::QueryBytes,
                requested,
                available: self.request.result_bytes,
                retryable: false,
            });
        }
        let (owner, handle) = reserve_charges(
            &self.governor,
            self.request.priority,
            Some(owner_bytes),
            handle_bytes,
        )?;
        Ok(RuntimeRetainedResult {
            owner: Arc::new(ResultBufferOwner {
                charge: owner.expect("new buffer owner was admitted"),
                capacity_bytes,
                priority: self.request.priority,
            }),
            handle,
        })
    }
}

impl RuntimeRetainedResult {
    /// Native Arc owner/header and alignment padding; excludes caller buffers.
    pub const fn owner_overhead_bytes() -> u64 {
        (std::mem::size_of::<ResultBufferOwner>()
            + 2 * std::mem::size_of::<usize>()
            + std::mem::align_of::<ResultBufferOwner>()) as u64
    }

    /// Native token layout; caller descriptor/wrapper costs are additional.
    pub const fn handle_overhead_bytes() -> u64 {
        std::mem::size_of::<Self>() as u64
    }

    pub fn capacity_bytes(&self) -> u64 {
        self.owner.capacity_bytes
    }

    pub fn handle_bytes(&self) -> u64 {
        self.handle.memory.bytes()
    }

    /// Reserve an extra descriptor/view without duplicating the payload charge.
    /// May be called after the originating permit/cursor/database has closed.
    /// Failure neither revokes the original view nor waits for its release.
    pub fn try_retain(&self, view_metadata_bytes: u64) -> Result<Self, RuntimeRetainedResultError> {
        let handle_bytes = view_metadata_bytes
            .checked_add(Self::handle_overhead_bytes())
            .ok_or(RuntimeRetainedResultError::SizeOverflow)?;
        let (_, handle) = reserve_charges(
            &self.owner.charge.memory.governor,
            self.owner.priority,
            None,
            handle_bytes,
        )?;
        Ok(Self {
            owner: Arc::clone(&self.owner),
            handle,
        })
    }
}

fn check_capacity(
    governor: &RuntimeGovernorInner,
    state: &mut RuntimeGovernorState,
    bytes: u64,
) -> Result<(), RuntimeRetainedResultError> {
    let limit = state.limits.result_budget_bytes;
    if bytes > limit {
        return Err(RuntimeRetainedResultError::Capacity {
            resource: RuntimeRetainedResultResource::Bytes,
            requested: bytes,
            available: limit,
            retryable: false,
        });
    }
    let available = limit.saturating_sub(state.retained_results.retained_bytes);
    let handles = governor.config.retained_result_handle_limit.get();
    let error = if bytes > available {
        Some(RuntimeRetainedResultError::Capacity {
            resource: RuntimeRetainedResultResource::Bytes,
            requested: bytes,
            available,
            retryable: true,
        })
    } else if state.retained_results.view_handles >= handles {
        Some(RuntimeRetainedResultError::Capacity {
            resource: RuntimeRetainedResultResource::Handles,
            requested: 1,
            available: 0,
            retryable: true,
        })
    } else {
        None
    };
    if let Some(error) = error {
        state.retained_results.backpressure_events =
            state.retained_results.backpressure_events.saturating_add(1);
        Err(error)
    } else {
        Ok(())
    }
}

fn reserve_charges(
    governor: &Arc<RuntimeGovernorInner>,
    priority: RuntimeWorkPriority,
    owner_bytes: Option<u64>,
    handle_bytes: u64,
) -> Result<(Option<ResultCharge>, ResultCharge), RuntimeRetainedResultError> {
    let bytes = owner_bytes
        .unwrap_or(0)
        .checked_add(handle_bytes)
        .ok_or(RuntimeRetainedResultError::SizeOverflow)?;
    {
        let mut state = mutex_lock(&governor.state);
        check_capacity(governor, &mut state, bytes)?;
    }
    // Reserve process and governor memory before allocating even owner metadata.
    // A concurrent reservation can consume the result allowance while these
    // calls run, so recheck it under the final lock. All failed paths drop their
    // temporary memory charges outside the lock, including policy notifications.
    let owner_memory = owner_bytes
        .map(|bytes| reserve_retained_memory(governor, priority, bytes))
        .transpose()?;
    let handle_memory = reserve_retained_memory(governor, priority, handle_bytes)?;
    {
        let mut state = mutex_lock(&governor.state);
        check_capacity(governor, &mut state, bytes)?;
        let results = &mut state.retained_results;
        results.retained_bytes += bytes;
        results.peak_retained_bytes = results.peak_retained_bytes.max(results.retained_bytes);
        results.buffer_owners += usize::from(owner_bytes.is_some());
        results.view_handles += 1;
        results.peak_view_handles = results.peak_view_handles.max(results.view_handles);
    }
    Ok((
        owner_memory.map(|memory| ResultCharge {
            memory,
            kind: ChargeKind::BufferOwner,
        }),
        ResultCharge {
            memory: handle_memory,
            kind: ChargeKind::ViewHandle,
        },
    ))
}

impl Drop for ResultCharge {
    fn drop(&mut self) {
        {
            let mut state = mutex_lock(&self.memory.governor.state);
            let results = &mut state.retained_results;
            results.retained_bytes -= self.memory.bytes();
            match self.kind {
                ChargeKind::BufferOwner => results.buffer_owners -= 1,
                ChargeKind::ViewHandle => results.view_handles -= 1,
            }
        }
        // `memory` drops after this callback, without the result-state lock.
    }
}

#[cfg(test)]
mod tests;
