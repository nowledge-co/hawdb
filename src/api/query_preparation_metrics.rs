// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Process-wide qualification observations of the shared Cypher preparation path.

use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static PARSE_CALLS: AtomicU64 = AtomicU64::new(0);
static PARSE_NANOS: AtomicU64 = AtomicU64::new(0);
static PLAN_CALLS: AtomicU64 = AtomicU64::new(0);
static PLAN_NANOS: AtomicU64 = AtomicU64::new(0);
static SAMPLING_CALLS: AtomicU64 = AtomicU64::new(0);
static SAMPLING_NANOS: AtomicU64 = AtomicU64::new(0);

/// Completed shared-runtime parses and plan preparation attempts, including
/// errors. Preparation includes cache lookup/binding and optimization on misses;
/// its time includes waits inside that function. Counters are process-wide and
/// snapshots are not atomic across fields. SQL and standalone parser users are
/// outside this observation boundary. This API exists only with `qualification`.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct QueryPreparationMetricsSnapshot {
    pub cypher_parse_calls: u64,
    pub cypher_parse_nanos: u64,
    pub cypher_plan_calls: u64,
    pub cypher_plan_nanos: u64,
    /// Returned sampling reports, including budget/unsupported refusals.
    pub join_sampling_calls: u64,
    /// Sampling duration is a subset of `cypher_plan_nanos`, not additive.
    pub join_sampling_nanos: u64,
}

/// Observe preparation across all handles in this process. Take window snapshots
/// while workers are quiescent; do not attribute concurrent process work to one
/// query or database. Counters saturate and are never reset.
pub fn query_preparation_metrics() -> QueryPreparationMetricsSnapshot {
    QueryPreparationMetricsSnapshot {
        cypher_parse_calls: PARSE_CALLS.load(Ordering::Relaxed),
        cypher_parse_nanos: PARSE_NANOS.load(Ordering::Relaxed),
        cypher_plan_calls: PLAN_CALLS.load(Ordering::Relaxed),
        cypher_plan_nanos: PLAN_NANOS.load(Ordering::Relaxed),
        join_sampling_calls: SAMPLING_CALLS.load(Ordering::Relaxed),
        join_sampling_nanos: SAMPLING_NANOS.load(Ordering::Relaxed),
    }
}

fn add(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
        Some(old.saturating_add(value))
    });
}

pub(super) fn record_parse(nanos: u64) {
    add(&PARSE_NANOS, nanos);
    add(&PARSE_CALLS, 1);
}

pub(super) fn record_join_sampling(nanos: u64) {
    add(&SAMPLING_NANOS, nanos);
    add(&SAMPLING_CALLS, 1);
}

pub(super) struct PlanPreparationTiming(Instant);

impl PlanPreparationTiming {
    pub(super) fn start() -> Self {
        Self(Instant::now())
    }
}

impl Drop for PlanPreparationTiming {
    fn drop(&mut self) {
        add(
            &PLAN_NANOS,
            self.0.elapsed().as_nanos().min(u64::MAX as u128) as u64,
        );
        add(&PLAN_CALLS, 1);
    }
}
