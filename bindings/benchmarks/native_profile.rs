// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Developer-only conversion timings plus Rust allocation observations.
#[path = "native_alloc.rs"]
mod allocation;

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct BoundaryProfileSnapshot {
    pub allocation_calls: u64,
    pub allocated_bytes: u64,
    pub deallocation_calls: u64,
    pub deallocated_bytes: u64,
    pub reallocation_calls: u64,
    pub live_requested_bytes: u64,
    pub process_peak_requested_bytes: u64,
    pub parameter_conversion_ns: u64,
    pub engine_call_ns: u64,
    pub result_conversion_ns: u64,
}

static PARAMETERS_NS: AtomicU64 = AtomicU64::new(0);
static ENGINE_NS: AtomicU64 = AtomicU64::new(0);
static RESULTS_NS: AtomicU64 = AtomicU64::new(0);

pub fn snapshot() -> BoundaryProfileSnapshot {
    let native = allocation::snapshot();
    BoundaryProfileSnapshot {
        allocation_calls: native.allocation_calls,
        allocated_bytes: native.allocated_bytes,
        deallocation_calls: native.deallocation_calls,
        deallocated_bytes: native.deallocated_bytes,
        reallocation_calls: native.reallocation_calls,
        live_requested_bytes: native.live_requested_bytes,
        process_peak_requested_bytes: native.process_peak_requested_bytes,
        parameter_conversion_ns: PARAMETERS_NS.load(Ordering::Relaxed),
        engine_call_ns: ENGINE_NS.load(Ordering::Relaxed),
        result_conversion_ns: RESULTS_NS.load(Ordering::Relaxed),
    }
}

pub enum Phase {
    Parameters,
    Engine,
    Results,
}

pub struct PhaseTimer {
    phase: Phase,
    started: Instant,
}

impl PhaseTimer {
    pub fn start(phase: Phase) -> Self {
        Self {
            phase,
            started: Instant::now(),
        }
    }
}

impl Drop for PhaseTimer {
    fn drop(&mut self) {
        let elapsed = u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let counter = match self.phase {
            Phase::Parameters => &PARAMETERS_NS,
            Phase::Engine => &ENGINE_NS,
            Phase::Results => &RESULTS_NS,
        };
        counter.fetch_add(elapsed, Ordering::Relaxed);
    }
}
