// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::qos::{
    IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
    RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkPriority, RuntimeWorkRequest,
};
use crate::{RuntimeIoWavePermit, RuntimeTaskContext};
use std::cell::Cell;
use std::num::NonZeroUsize;

fn governor() -> RuntimeGovernor {
    RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(2 << 30),
            result_budget_bytes: 64 << 20,
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::new(8).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(Some(8 << 30), Some(8 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    )
}

fn physical_wave(
    governor: &RuntimeGovernor,
    priority: RuntimeWorkPriority,
    slots: usize,
) -> (RuntimeTaskContext, Box<dyn RuntimeIoWavePermit>) {
    let permit = governor
        .try_admit(RuntimeWorkRequest::io(priority, 0, 0).with_io_wave_slots(slots))
        .unwrap();
    let context = permit.bind_task_context(RuntimeTaskContext::default());
    let wave = context
        .acquire_io_wave(NonZeroUsize::new(slots).unwrap())
        .unwrap()
        .unwrap();
    drop(permit);
    (context, wave)
}

fn fixture(
    name: &str,
    incremental: bool,
    governor: &RuntimeGovernor,
) -> (std::path::PathBuf, NowledgeMemEmbeddedStoreHandle) {
    let (root, original, options) = incremental_maintenance_fixture(name, Default::default());
    drop(original);
    let options = if incremental {
        options
    } else {
        NowledgeMemOpenOptions::with_search_projection(
            root.join("graph"),
            root.join("search"),
            NowledgeMemGraphMode::WritableCutover,
        )
    };
    let (handle, _) = NowledgeMemEmbeddedStoreHandle::open_with_options_and_runtime_governor(
        options,
        governor.clone(),
    )
    .unwrap();
    handle
        .query_with_report("CREATE (:Memory {id: 'priority-current', title: 'Current source'})")
        .unwrap();
    (root, handle)
}

#[test]
fn explicit_foreground_catch_up_progresses_while_background_physical_io_is_full() {
    for incremental in [false, true] {
        let governor = governor();
        let (_context, wave) = physical_wave(&governor, RuntimeWorkPriority::Background, 1);
        // Observe our task refund before any automatic-maintenance owner exists.
        assert_eq!(governor.snapshot().active_background_tasks, 0);
        let (root, handle) = fixture("catch_up_background_pressure", incremental, &governor);
        let before = governor.snapshot();
        assert_eq!(before.active_background_io_slots, 1);
        assert_eq!(before.active_foreground_io_slots, 0);
        let called = Cell::new(false);
        let refused = handle
            .catch_up_search_projection_with_batch_hydrator(4, 4, 4, |_, _| {
                called.set(true);
                Ok(Default::default())
            })
            .unwrap_err();
        assert!(refused
            .to_string()
            .contains("runtime admission io_saturated"));
        assert!(
            !called.get(),
            "background admission refuses before hydration"
        );
        let report = handle
            .catch_up_search_projection_with_priority_and_batch_hydrator(
                RuntimeWorkPriority::Foreground,
                4,
                4,
                4,
                |_, _| Ok(Default::default()),
            )
            .unwrap();
        assert!(report.complete);
        assert_eq!(report.end_durable_epoch, Some(report.graph_commit_epoch));
        let documents = handle
            .search_projection_documents(&["memory:priority-current".to_string()], 1)
            .unwrap();
        assert_eq!(documents.len(), 1);
        assert_eq!(documents[0].id, "memory:priority-current");
        assert_eq!(governor.snapshot().active_background_io_slots, 1);
        drop(wave);
        drop(handle);
        let after = governor.snapshot();
        assert_eq!(after.active_foreground_io_slots, 0);
        assert_eq!(after.active_background_io_slots, 0);
        assert_eq!(after.admissions, after.completions);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn explicit_foreground_catch_up_refuses_real_foreground_pressure_then_retries() {
    for incremental in [false, true] {
        let governor = governor();
        let (root, handle) = fixture("catch_up_foreground_pressure", incremental, &governor);
        let (_context, wave) = physical_wave(&governor, RuntimeWorkPriority::Foreground, 2);
        let called = Cell::new(false);
        let refused = handle
            .catch_up_search_projection_with_priority_and_batch_hydrator(
                RuntimeWorkPriority::Foreground,
                4,
                4,
                4,
                |_, _| {
                    called.set(true);
                    Ok(Default::default())
                },
            )
            .unwrap_err();
        assert!(refused
            .to_string()
            .contains("runtime admission io_saturated"));
        assert!(!called.get(), "foreground refusal precedes hydration");
        let after = governor.snapshot();
        assert_eq!(after.active_foreground_io_slots, 2);
        assert_eq!(after.active_foreground_tasks, 0);
        drop(wave);
        let report = handle
            .catch_up_search_projection_with_priority_and_batch_hydrator(
                RuntimeWorkPriority::Foreground,
                4,
                4,
                4,
                |_, _| Ok(Default::default()),
            )
            .unwrap();
        assert!(report.complete);
        assert_eq!(report.end_durable_epoch, Some(report.graph_commit_epoch));
        assert_eq!(
            handle
                .search_projection_documents(&["memory:priority-current".to_string()], 1)
                .unwrap()
                .len(),
            1,
        );
        drop(handle);
        assert_eq!(governor.snapshot().active_foreground_io_slots, 0);
        std::fs::remove_dir_all(root).unwrap();
    }
}
