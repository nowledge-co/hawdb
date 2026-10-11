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
use crate::background::{CheckpointWorkContext, CheckpointWorkError, CheckpointWorkProbe};
use hawdb_core::{
    RuntimeIoWaveController, RuntimeIoWaveError, RuntimeIoWavePermit, RuntimeIoWaveTryAcquire,
    RuntimeMemoryError, RuntimeTaskContext,
};
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeWorkRequest,
};

const CEILING: u64 = 4 * 1024 * 1024;

#[derive(Debug)]
struct ObservedIo {
    governed: RuntimeTaskContext,
    probe: Arc<CheckpointWorkProbe>,
}

#[derive(Debug)]
struct ObservedWave {
    _governed: Option<Box<dyn RuntimeIoWavePermit>>,
    _observed: Box<dyn RuntimeIoWavePermit>,
}

impl RuntimeIoWaveController for ObservedIo {
    fn acquire(
        &self,
        slots: NonZeroUsize,
        task: &RuntimeTaskContext,
    ) -> Result<Box<dyn RuntimeIoWavePermit>, RuntimeIoWaveError> {
        let observed = self.probe.acquire(slots, task)?;
        let governed = self.governed.acquire_io_wave(slots)?;
        Ok(Box::new(ObservedWave {
            _governed: governed,
            _observed: observed,
        }))
    }

    fn try_acquire(
        &self,
        slots: NonZeroUsize,
        task: &RuntimeTaskContext,
    ) -> Result<Option<Box<dyn RuntimeIoWavePermit>>, RuntimeIoWaveError> {
        let Some(observed) = self.probe.try_acquire(slots, task)? else {
            return Ok(None);
        };
        match self.governed.try_acquire_io_wave(slots)? {
            RuntimeIoWaveTryAcquire::Pending => Ok(None),
            RuntimeIoWaveTryAcquire::Acquired(governed) => Ok(Some(Box::new(ObservedWave {
                _governed: governed,
                _observed: observed,
            }))),
        }
    }
}

fn observe(task: RuntimeTaskContext, probe: &Arc<CheckpointWorkProbe>) -> RuntimeTaskContext {
    // Binding installs the real governor controller. Observe it after binding,
    // preserving its actual reservation and permit rather than replacing it.
    let observer = Arc::new(ObservedIo {
        governed: task.clone(),
        probe: probe.clone(),
    });
    task.with_io_wave_controller(observer)
}

fn governor() -> RuntimeGovernor {
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(CEILING),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    governor
}

fn available(task: &RuntimeTaskContext) -> u64 {
    match task.reserve_working_memory(CEILING) {
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes, ..
        }) => available_bytes,
        _ => panic!("exact-ceiling probe must expose actual working-memory remainder"),
    }
}

fn lookup(
    reader: &GraphDescriptorTreeDemandReader,
    work: &CheckpointWorkContext,
    limits: GraphDescriptorTreeReadLimits,
) -> Result<(Option<u64>, GraphDescriptorTreeReadReport), GraphDescriptorTreeError> {
    let mut key = [0; 16];
    key[..8].copy_from_slice(&7u64.to_be_bytes());
    key[8..].copy_from_slice(&50u64.to_be_bytes());
    let mut selected = None;
    let (report, _) = reader.checkpoint_scan_from(&key, limits, work, |key, value| {
        assert_eq!(key.len(), 16);
        assert_eq!(u64::from_be_bytes(key[..8].try_into().unwrap()), 7);
        selected = Some(u64::from_le_bytes(value.try_into().unwrap()));
        Ok(GraphDescriptorTreeScanControl::Stop)
    })?;
    Ok((selected, report))
}

#[test]
fn checkpoint_units_metadata_demand_denial_retries_same_reservation_without_using_serving_cache() {
    let directory = TestDirectory::new("checkpoint-denial");
    build(directory.path());
    let reader = open_reader(directory.path(), 4 * 1024 * 1024);
    assert!(reader.root.height > 1);
    assert_eq!(scan_from(&reader, 7, 50, 1).unwrap().0, vec![(7, 50)]);
    let serving_bytes = reader.cache.snapshot().resident_bytes;
    assert!(serving_bytes > 0);
    let physical = fs::read(&reader.page_artifact).unwrap();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let idle = available(&task);
    let held = task.reserve_working_memory(idle - 256).unwrap().unwrap();
    let before = available(&task);
    let work = CheckpointWorkContext::new(task.clone());
    assert!(matches!(
        lookup(&reader, &work, limits()),
        Err(GraphDescriptorTreeError::Work(CheckpointWorkError::Memory(
            _
        )))
    ));
    assert_eq!(available(&task), before);
    assert!(!reader.is_poisoned());
    assert_eq!(reader.cache.snapshot().resident_bytes, serving_bytes);
    drop(held);
    let (result, report) = lookup(&reader, &work, limits()).unwrap();
    assert_eq!(result, Some(50));
    assert_eq!(report.pages_visited, u64::from(reader.root.height) + 1);
    assert_eq!(report.maximum_depth, reader.root.height);
    assert_eq!(report.cache_hits, 0);
    assert_eq!(report.cache_misses, 0);
    assert!(report.storage_bytes_read > 0);
    assert_eq!(available(&task), idle);
    assert_eq!(reader.cache.snapshot().resident_bytes, serving_bytes);
    assert_eq!(fs::read(&reader.page_artifact).unwrap(), physical);
    assert_eq!(governor.snapshot().admissions, 1);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
}

#[test]
fn checkpoint_units_metadata_demand_cancels_every_actual_unit_and_io_wave_then_retries() {
    let directory = TestDirectory::new("checkpoint-cancel");
    build(directory.path());
    let reader = open_reader(directory.path(), 4 * 1024 * 1024);
    let physical = fs::read(&reader.page_artifact).unwrap();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let idle = available(&task);
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    });
    let probe = Arc::new(CheckpointWorkProbe::default());
    scheduler.set_telemetry_sink(Some(probe.clone()));
    let bound = observe(
        permit.bind_task_context(RuntimeTaskContext::without_deadline(
            probe.cancellation.clone(),
        )),
        &probe,
    );
    let work = CheckpointWorkContext::new(bound).with_scheduler(scheduler.clone());
    let (_, report) = lookup(&reader, &work, limits()).unwrap();
    let units = probe.completed.load(Ordering::SeqCst);
    let waves = probe.io_waves.load(Ordering::SeqCst);
    assert!(units > 20 && waves >= report.pages_visited as usize);
    assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
    probe.assert_released(&scheduler);
    drop(work);
    assert_eq!(available(&task), idle);
    for io in [false, true] {
        for stop in 1..=if io { waves } else { units } {
            // Measure each cut from the same cold captured-reader state. A
            // warmed native handle skips the physical open I/O wave.
            let reader = open_reader(directory.path(), 4 * 1024 * 1024);
            let probe = Arc::new(CheckpointWorkProbe::default());
            if io {
                probe.cancel_on_io_wave.store(stop, Ordering::SeqCst);
            } else {
                probe.cancel_after.store(stop, Ordering::SeqCst);
            }
            scheduler.set_telemetry_sink(Some(probe.clone()));
            let bound = observe(
                permit.bind_task_context(RuntimeTaskContext::without_deadline(
                    probe.cancellation.clone(),
                )),
                &probe,
            );
            let work = CheckpointWorkContext::new(bound).with_scheduler(scheduler.clone());
            assert!(
                matches!(
                    lookup(&reader, &work, limits()),
                    Err(GraphDescriptorTreeError::Work(_))
                ),
                "cut={stop}, io={io}"
            );
            probe.assert_released(&scheduler);
            assert_eq!(available(&task), idle);
            assert!(!reader.is_poisoned());
            drop(work);
            scheduler.set_telemetry_sink(None);
            let retry = CheckpointWorkContext::new(task.clone()).with_scheduler(scheduler.clone());
            assert_eq!(lookup(&reader, &retry, limits()).unwrap().0, Some(50));
            assert_eq!(available(&task), idle);
        }
    }
    assert_eq!(fs::read(&reader.page_artifact).unwrap(), physical);
    assert_eq!(governor.snapshot().admissions, 1);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
}

#[test]
fn checkpoint_units_metadata_demand_limit_denial_is_recoverable_and_damage_poisons_warm_source() {
    let directory = TestDirectory::new("checkpoint-damage");
    build(directory.path());
    let reader = open_reader(directory.path(), 4 * 1024 * 1024);
    scan_from(&reader, 7, 50, 1).unwrap();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let idle = available(&task);
    let work = CheckpointWorkContext::new(task.clone());
    let mut bounded = limits();
    bounded.max_pages = NonZeroU64::MIN;
    assert!(matches!(
        lookup(&reader, &work, bounded),
        Err(GraphDescriptorTreeError::Admission(_))
    ));
    assert!(!reader.is_poisoned());
    assert_eq!(available(&task), idle);
    assert_eq!(lookup(&reader, &work, limits()).unwrap().0, Some(50));
    let mut bytes = fs::read(&reader.page_artifact).unwrap();
    let root = reader.root.root.as_ref().unwrap();
    bytes[root.offset as usize + 84] ^= 0x80;
    fs::write(&reader.page_artifact, bytes).unwrap();
    assert!(matches!(
        lookup(&reader, &work, limits()),
        Err(GraphDescriptorTreeError::Page(
            GraphDescriptorPageError::Corrupt(_)
        )) | Err(GraphDescriptorTreeError::Io(_))
            | Err(GraphDescriptorTreeError::Corrupt(_))
    ));
    assert!(reader.is_poisoned());
    assert_eq!(available(&task), idle);
    assert!(lookup(&reader, &work, limits()).is_err());
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
}
