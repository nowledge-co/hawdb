// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Instrumented lower bound. Compare allocations with foreign profiling builds;
//! use the ordinary host_boundary binary for uninstrumented release latency.

#[path = "../bindings/benchmarks/native_alloc.rs"]
mod native_profile;
#[path = "host_boundary/workload.rs"]
mod workload;

use native_profile::BoundaryAllocationSnapshot;
use std::sync::Mutex;

static BEFORE: Mutex<Option<BoundaryAllocationSnapshot>> = Mutex::new(None);

fn begin() {
    *BEFORE.lock().expect("profile lock") = Some(native_profile::snapshot());
}

fn finish() -> serde_json::Value {
    let after = native_profile::snapshot();
    let before = BEFORE
        .lock()
        .expect("profile lock")
        .take()
        .expect("profile start");
    serde_json::json!({
        "allocation_calls": after.allocation_calls - before.allocation_calls,
        "allocated_bytes": after.allocated_bytes - before.allocated_bytes,
        "deallocation_calls": after.deallocation_calls - before.deallocation_calls,
        "deallocated_bytes": after.deallocated_bytes - before.deallocated_bytes,
        "reallocation_calls": after.reallocation_calls - before.reallocation_calls,
        "live_requested_bytes_before": before.live_requested_bytes,
        "live_requested_bytes_after": after.live_requested_bytes,
        "process_peak_requested_bytes": after.process_peak_requested_bytes,
        "scope": "Rust global allocator requests; foreign heaps/rounding/non-Rust workspace excluded",
    })
}

fn main() {
    workload::main_with_observer(Some(&workload::Observer { begin, finish }));
}
