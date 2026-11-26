// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Caller-selected runtime admission policy carried by a task context.

use crate::time::{Duration, Instant};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Scheduling class of one unit of governed runtime work.
///
/// Background work uses its own task pool and I/O depth and yields under
/// critical memory pressure. CPU slots, total memory and blocking-task slots
/// are shared by both classes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum RuntimeWorkPriority {
    #[default]
    Foreground,
    Background,
}

impl RuntimeWorkPriority {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Foreground => "foreground",
            Self::Background => "background",
        }
    }

    /// The lower of two priorities: Background wins.
    pub const fn lowered_by(self, other: Self) -> Self {
        match (self, other) {
            (Self::Foreground, Self::Foreground) => Self::Foreground,
            _ => Self::Background,
        }
    }
}

/// What a synchronous admission does when capacity is short.
///
/// The default refuses at once with a typed, retryable admission refusal.
/// Waiting joins the runtime governor's priority-ordered admission queue and
/// never extends the task's own deadline or ignores its cancellation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RuntimeAdmissionWait {
    /// Refuse immediately when capacity is short.
    #[default]
    Refuse,
    /// Wait in the admission queue until this instant, then refuse.
    Until(Instant),
    /// Wait until admitted, cancelled, or the task deadline passes.
    ///
    /// A nested unbounded wait made while the same thread holds another
    /// permit (inside a transaction or read-snapshot callback) can wait for
    /// itself: on a one-CPU governor without a task deadline it never
    /// returns. Prefer [`Self::Until`] there.
    Unbounded,
}

/// Caller-held record of how long a task's runtime admissions queued.
///
/// Attach a clone with `RuntimeTaskContext::with_admission_trace` and read it
/// after the call returns. Each admission made for the task (an embedded
/// query admits planning and execution separately) adds its time spent queued
/// for capacity; an immediate admission adds zero. Clones share one record.
#[derive(Debug, Clone, Default)]
pub struct RuntimeAdmissionTrace {
    inner: Arc<RuntimeAdmissionTraceState>,
}

#[derive(Debug, Default)]
struct RuntimeAdmissionTraceState {
    admissions: AtomicU64,
    queued_micros: AtomicU64,
}

impl RuntimeAdmissionTrace {
    pub fn new() -> Self {
        Self::default()
    }

    /// Admissions granted so far.
    pub fn admissions(&self) -> u64 {
        self.inner.admissions.load(Ordering::Acquire)
    }

    /// Total time the granted admissions spent queued (microsecond precision).
    pub fn queued_for(&self) -> Duration {
        Duration::from_micros(self.inner.queued_micros.load(Ordering::Acquire))
    }

    pub(super) fn record(&self, queued_for: Duration) {
        let micros = u64::try_from(queued_for.as_micros()).unwrap_or(u64::MAX);
        // u64 microseconds outlast any process; wrapping is unreachable.
        self.inner.queued_micros.fetch_add(micros, Ordering::AcqRel);
        self.inner.admissions.fetch_add(1, Ordering::AcqRel);
    }
}
