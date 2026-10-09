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

use super::*;
use crate::background::CheckpointWorkProbe;
use crate::graph_descriptor_tree::{demand, GraphDescriptorTreeRootReader};
use hawdb_core::{
    RuntimeIoWaveController, RuntimeIoWaveError, RuntimeIoWavePermit, RuntimeMemoryError,
    RuntimeTaskContext,
};
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
};
use std::num::NonZeroUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

const CEILING: u64 = 64 * 1024;

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "hawdb-descriptor-buffer-memory-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn paths(&self) -> GraphDescriptorTreePaths {
        GraphDescriptorTreePaths::new(
            self.0.join("tree-pages.hawdb"),
            self.0.join("tree-root.hawdb"),
        )
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn governor(ceiling: u64) -> RuntimeGovernor {
    RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(ceiling),
            background_task_limit: Some(NonZeroUsize::MIN),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    )
}

fn used(task: &RuntimeTaskContext, ceiling: u64) -> u64 {
    match task.reserve_working_memory(ceiling) {
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes, ..
        }) => ceiling - available_bytes,
        other => panic!("concrete retained-memory probe failed: {other:?}"),
    }
}

fn config() -> GraphDescriptorTreeBuildConfig {
    GraphDescriptorTreeBuildConfig {
        page_limits: crate::graph_descriptor_page::GraphDescriptorPageLimits {
            max_page_bytes: NonZeroUsize::new(512).unwrap(),
            max_entries: NonZeroUsize::new(4).unwrap(),
            max_key_bytes: NonZeroUsize::new(64).unwrap(),
            max_value_bytes: NonZeroUsize::new(128).unwrap(),
        },
        ..GraphDescriptorTreeBuildConfig::default()
    }
}

fn builder(
    directory: &Directory,
    work: CheckpointWorkContext,
) -> Result<GraphDescriptorTreeBuilder, GraphDescriptorTreeError> {
    GraphDescriptorTreeBuilder::create_checkpoint(
        directory.paths(),
        GraphDescriptorKind::CanonicalSegment,
        47,
        47,
        19,
        config(),
        work,
    )
}

#[test]
fn checkpoint_units_descriptor_tree_memory_related_each_fixed_buffer_denial_cleans_and_retries() {
    for ceiling in [1, TREE_IO_BUFFER_BYTES as u64 + 1024] {
        let denied = Directory::new();
        let governor = governor(ceiling);
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
            .unwrap();
        let task = permit.bind_task_context(RuntimeTaskContext::default());
        let work = CheckpointWorkContext::new(task.clone());
        let result = builder(&denied, work.clone());
        assert!(matches!(
            result,
            Err(GraphDescriptorTreeError::Work(CheckpointWorkError::Memory(
                RuntimeMemoryError::ReservationExceeded { .. }
            )))
        ));
        assert_eq!(used(&task, ceiling), 0);
        assert_eq!(governor.snapshot().active_background_io_slots, 0);
        assert_eq!(fs::read_dir(&denied.0).unwrap().count(), 0);
        // A denied private workspace is abandoned; a fresh sufficiently
        // admitted workspace must produce the complete ordinary tree.
        drop(work);
        drop(task);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        let retry = Directory::new();
        let governor = self::governor(CEILING);
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
            .unwrap();
        let task = permit.bind_task_context(RuntimeTaskContext::default());
        let mut actual = builder(&retry, CheckpointWorkContext::new(task.clone())).unwrap();
        actual.push(17u64.to_be_bytes().to_vec(), vec![19]).unwrap();
        actual.finish().unwrap().publish().unwrap();
        assert_eq!(used(&task, CEILING), 0);
        assert_complete(&retry, 1);
    }
}

#[test]
fn checkpoint_units_descriptor_tree_memory_related_buffers_survive_execution_owner_close() {
    let directory = Directory::new();
    let governor = governor(CEILING);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let actual = builder(&directory, work.clone()).unwrap();
    assert!(used(&task, CEILING) >= (2 * TREE_IO_BUFFER_BYTES) as u64);
    drop(work);
    drop(permit);
    assert!(matches!(
        task.reserve_working_memory(1),
        Err(RuntimeMemoryError::Closed)
    ));
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, CEILING);
    drop(actual);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(fs::read_dir(&directory.0).unwrap().count(), 0);
    drop(task);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

fn build_all(
    directory: &Directory,
    work: CheckpointWorkContext,
    count: u64,
) -> Result<(), GraphDescriptorTreeError> {
    let mut actual = builder(directory, work)?;
    for index in 0..count {
        actual.push(index.to_be_bytes().to_vec(), vec![index as u8; 48])?;
    }
    actual.finish()?.publish()?;
    Ok(())
}

fn assert_complete(directory: &Directory, count: u64) {
    let root = GraphDescriptorTreeRootReader::open(directory.paths(), config()).unwrap();
    let reader = demand::GraphDescriptorTreeDemandReader::open(
        root,
        config(),
        Arc::new(crate::cache::SegmentCache::new(0)),
        crate::cache::StoreId(47),
    )
    .unwrap();
    let mut seen = 0u64;
    reader
        .deep_visit(|key, value| {
            if count == 1 {
                assert_eq!(key, 17u64.to_be_bytes());
                assert_eq!(value, [19]);
            } else {
                assert_eq!(key, seen.to_be_bytes());
                assert_eq!(value, vec![seen as u8; 48]);
            }
            seen += 1;
            Ok(demand::GraphDescriptorTreeScanControl::Continue)
        })
        .unwrap();
    assert_eq!(seen, count);
}

#[test]
fn checkpoint_units_descriptor_tree_memory_related_complete_interior_bytes_and_reopen() {
    let ordinary = Directory::new();
    let mut reference = GraphDescriptorTreeBuilder::create(
        ordinary.paths(),
        GraphDescriptorKind::CanonicalSegment,
        47,
        47,
        19,
        config(),
    )
    .unwrap();
    for index in 0..2000u64 {
        reference
            .push(index.to_be_bytes().to_vec(), vec![index as u8; 48])
            .unwrap();
    }
    reference.finish().unwrap().publish().unwrap();
    assert_complete(&ordinary, 2000);
    let directory = Directory::new();
    let governor = governor(CEILING);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    build_all(&directory, CheckpointWorkContext::new(task.clone()), 2000).unwrap();
    assert_eq!(used(&task, CEILING), 0);
    assert_complete(&directory, 2000);
    assert_eq!(
        fs::read(ordinary.paths().page_artifact).unwrap(),
        fs::read(directory.paths().page_artifact).unwrap()
    );
    assert_eq!(
        fs::read(ordinary.paths().root_manifest).unwrap(),
        fs::read(directory.paths().root_manifest).unwrap()
    );
}

#[derive(Debug)]
struct ObservedIo {
    admitted: RuntimeTaskContext,
    probe: Arc<CheckpointWorkProbe>,
}

#[derive(Debug)]
struct Wave {
    _admitted: Box<dyn RuntimeIoWavePermit>,
    _observed: Box<dyn RuntimeIoWavePermit>,
}

impl RuntimeIoWaveController for ObservedIo {
    fn acquire(
        &self,
        slots: NonZeroUsize,
        task: &RuntimeTaskContext,
    ) -> Result<Box<dyn RuntimeIoWavePermit>, RuntimeIoWaveError> {
        assert_eq!(slots, NonZeroUsize::MIN);
        let admitted = self.admitted.acquire_io_wave(slots)?.unwrap();
        let observed = self.probe.acquire(slots, task)?;
        Ok(Box::new(Wave {
            _admitted: admitted,
            _observed: observed,
        }))
    }

    fn try_acquire(
        &self,
        slots: NonZeroUsize,
        task: &RuntimeTaskContext,
    ) -> Result<Option<Box<dyn RuntimeIoWavePermit>>, RuntimeIoWaveError> {
        self.acquire(slots, task).map(Some)
    }
}

#[test]
fn checkpoint_units_descriptor_tree_memory_related_each_constructor_cpu_and_real_io_cut_retries() {
    let governor = governor(CEILING);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(4),
        ..Default::default()
    });
    let baseline = Arc::new(CheckpointWorkProbe::default());
    scheduler.set_telemetry_sink(Some(baseline.clone()));
    let complete = Directory::new();
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(scheduler.clone());
    let actual = builder(&complete, work).unwrap();
    let cpu_count = baseline.completed.load(Ordering::SeqCst);
    drop(actual);
    assert!(cpu_count > 0);
    assert_eq!(used(&task, CEILING), 0);
    for (cpu_cut, io_cut) in (1..=cpu_count)
        .map(|cut| (cut, 0))
        .chain(std::iter::once((0, 1)))
    {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(cpu_cut, Ordering::SeqCst);
        probe.cancel_on_io_wave.store(io_cut, Ordering::SeqCst);
        scheduler.set_telemetry_sink(Some(probe.clone()));
        let observed = permit.bind_task_context(RuntimeTaskContext::without_deadline(
            probe.cancellation.clone(),
        ));
        let work =
            CheckpointWorkContext::new(observed.with_io_wave_controller(Arc::new(ObservedIo {
                admitted: task.clone(),
                probe: probe.clone(),
            })))
            .with_scheduler(scheduler.clone());
        let denied = Directory::new();
        let result = builder(&denied, work);
        // A completion callback may cancel just after the last constructor
        // unit. The next cooperative boundary must observe that cancellation.
        let result = result.and_then(|actual| actual.finish());
        if io_cut == 0 {
            assert!(matches!(
                result,
                Err(GraphDescriptorTreeError::Work(
                    CheckpointWorkError::Stopped(hawdb_core::RuntimeCancellationReason::Cancelled)
                ))
            ));
        } else {
            assert!(matches!(
                result,
                Err(GraphDescriptorTreeError::Work(CheckpointWorkError::Io(
                    RuntimeIoWaveError::Stopped(hawdb_core::RuntimeCancellationReason::Cancelled)
                )))
            ));
        }
        assert_eq!(used(&task, CEILING), 0);
        assert_eq!(governor.snapshot().active_background_io_slots, 0);
        probe.assert_released(&scheduler);
        assert_eq!(fs::read_dir(&denied.0).unwrap().count(), 0);
        scheduler.set_telemetry_sink(None);
        let retry = Directory::new();
        build_all(&retry, CheckpointWorkContext::new(task.clone()), 17).unwrap();
        assert_complete(&retry, 17);
        assert_eq!(used(&task, CEILING), 0);
    }
}
