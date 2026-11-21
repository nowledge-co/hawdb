// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! A `memory_pressure` refusal on an `EmbeddedStoreHandle` whose governor was
//! detected from the host (not pinned) must clear once the host recovers,
//! without the caller knowing to call `refresh_runtime_resources` (#1879).
//!
//! These tests drive the real host detector, so they need a test host that is
//! not itself under critical memory pressure. A host that is critical at open
//! or still critical when the retries run out skips the assertion instead of
//! failing. The QoS crate's `runtime::host_refresh_tests` cover recovery and
//! onset deterministically with a scripted host.

use super::*;
use crate::qos::{RuntimeMemoryPressure, RuntimeMemorySnapshot, RuntimeResourceSnapshot};
use std::time::{Duration, Instant};

fn is_memory_pressure(
    result: &crate::Result<crate::search::SearchProjectionEpochAdvanceReport>,
) -> bool {
    matches!(
        result,
        Err(HawDBError::RuntimeAdmissionRefused(refusal)) if refusal.code == "memory_pressure"
    )
}

fn host_is_critical() -> bool {
    RuntimeResourceSnapshot::detect().memory.pressure == RuntimeMemoryPressure::Critical
}

/// Opens the handle exactly as `EmbeddedStoreHandle::open_with_options` does
/// (host-detected, unpinned governor), then records a critical host sample
/// through the same `update_resources` step `refresh_from_host` uses.
fn handle_with_stale_critical_sample(
    name: &str,
) -> Option<(std::path::PathBuf, EmbeddedStoreHandle)> {
    let root = unique_embedded_store_test_dir(name);
    let options =
        EmbeddedStoreOpenOptions::graph_only(root.join("graph"), GraphMode::WritableCutover);
    let (handle, _) = EmbeddedStoreHandle::open_with_options(options).unwrap();
    let healthy = handle.runtime_governor_snapshot().unwrap();
    assert!(!healthy.resources_pinned);
    if healthy.resources.memory.pressure == RuntimeMemoryPressure::Critical {
        eprintln!("skipping: the test host itself is under critical memory pressure");
        drop(handle);
        std::fs::remove_dir_all(root).unwrap();
        return None;
    }
    let critical = RuntimeResourceSnapshot::from_parts(
        healthy.resources.cpu,
        RuntimeMemorySnapshot::from_limits(
            healthy.resources.memory.host_total_bytes,
            Some(0),
            None,
            None,
            None,
        ),
    );
    assert_eq!(critical.memory.pressure, RuntimeMemoryPressure::Critical);
    let governor = handle
        .read_store()
        .unwrap()
        .graph
        .runtime_governor()
        .clone();
    assert!(governor.update_resources(critical));
    Some((root, handle))
}

#[test]
fn stale_critical_memory_pressure_clears_after_host_recovers_without_explicit_refresh() {
    let Some((root, handle)) = handle_with_stale_critical_sample("stale_memory_pressure") else {
        return;
    };
    // The host is healthy now. Retry the same Background call the way the
    // documented contract says ("retry it later unchanged"), for longer than
    // the Tokio adapter's default 1 s resource refresh interval.
    let started = Instant::now();
    let mut attempts = 0;
    let mut last = handle.advance_search_projection_source_epoch(1);
    while is_memory_pressure(&last) && started.elapsed() < Duration::from_secs(3) {
        attempts += 1;
        std::thread::sleep(Duration::from_millis(250));
        last = handle.advance_search_projection_source_epoch(1);
    }
    let pressure = handle
        .runtime_governor_snapshot()
        .unwrap()
        .resources
        .memory
        .pressure;
    let still_refused = is_memory_pressure(&last);
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
    if still_refused && host_is_critical() {
        eprintln!("skipping: the test host came under critical memory pressure");
        return;
    }
    assert!(
        !still_refused,
        "host memory is healthy but {attempts} retries over {:?} were all refused with \
         memory_pressure; governor still reports {pressure:?}: {last:?}",
        started.elapsed()
    );
}

#[test]
fn explicit_refresh_clears_stale_critical_memory_pressure() {
    let Some((root, handle)) = handle_with_stale_critical_sample("stale_memory_pressure_refresh")
    else {
        return;
    };
    assert!(handle.refresh_runtime_resources().unwrap());
    let result = handle.advance_search_projection_source_epoch(1);
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
    if is_memory_pressure(&result) && host_is_critical() {
        eprintln!("skipping: the test host came under critical memory pressure");
        return;
    }
    assert!(!is_memory_pressure(&result), "{result:?}");
}
