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
use crate::canonical::checkpoint_flush_memory_tests::Fixture;
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

const CEILING: u64 = 32 * 1024 * 1024;

fn governor() -> RuntimeGovernor {
    RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(CEILING),
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

fn local() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    })
}

fn used(task: &RuntimeTaskContext) -> u64 {
    match task.reserve_working_memory(CEILING) {
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes, ..
        }) => CEILING - available_bytes,
        other => panic!("full-ceiling concrete lease probe failed: {other:?}"),
    }
}

fn admitted_bloom(
    keys: &[u64],
    work: &CheckpointWorkContext,
) -> Result<(CanonicalEndpointBloom, CheckpointAllocationOwner), CanonicalSegmentError> {
    work.classify(|work| {
        let context = CheckpointDecodeContext {
            work: work.clone(),
            memory: RefCell::new(CheckpointAllocationOwner::default()),
        };
        let bloom = bloom_admitted(keys, &context)?;
        Ok((bloom, context.memory.into_inner()))
    })
    .map_err(|error| match error {
        CheckpointOperationError::Work(error) => CanonicalSegmentError::Work(error),
        CheckpointOperationError::Operation(error) => error,
    })
}

#[test]
fn checkpoint_units_canonical_descriptor_memory_related_bloom_wire_and_zeroing_units() {
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(CEILING))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let local = local();
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    for count in [0usize, 1, 255, 256, 257, 4097, 65535, 65536, 65537, 1048576] {
        let keys: Vec<_> = (0..count as u64)
            .map(|id| id.wrapping_mul(0x9e37_79b9_7f4a_7c15))
            .collect();
        let expected = CanonicalEndpointBloom::from_keys(&keys);
        let probe = Arc::new(CheckpointWorkProbe::default());
        local.set_telemetry_sink(Some(probe.clone()));
        let (actual, memory) = admitted_bloom(&keys, &work).unwrap();
        assert_eq!(actual, expected);
        for key in &keys {
            assert!(actual.might_contain(*key));
        }
        let word_count = actual.words.len();
        // classifier, capacity admission, bounded zeroing, box transfer,
        // and each actual 256-key insertion unit are independently observed.
        assert_eq!(
            probe.completed.load(Ordering::SeqCst),
            3 + word_count.div_ceil(8192) + count.div_ceil(256)
        );
        assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
        assert_eq!(probe.io_waves.load(Ordering::SeqCst), 0);
        probe.assert_released(&local);
        assert!(used(&task) >= (word_count * 8) as u64);
        drop(actual);
        drop(memory);
        assert_eq!(used(&task), 0);
    }
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_canonical_descriptor_memory_related_bloom_every_cpu_cut_releases_and_retries() {
    let keys: Vec<_> = (0..4097).map(|id| id * 17).collect();
    let expected = CanonicalEndpointBloom::from_keys(&keys);
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(CEILING))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let local = local();
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    let baseline = Arc::new(CheckpointWorkProbe::default());
    local.set_telemetry_sink(Some(baseline.clone()));
    let (actual, memory) = admitted_bloom(&keys, &work).unwrap();
    assert_eq!(actual, expected);
    drop(actual);
    drop(memory);
    let total = baseline.completed.load(Ordering::SeqCst);
    assert!(total > 16);
    baseline.assert_released(&local);
    for cut in 1..=total {
        let child = task.child();
        let probe = Arc::new(CheckpointWorkProbe {
            cancellation: child.cancellation().clone(),
            ..Default::default()
        });
        probe.cancel_after.store(cut, Ordering::SeqCst);
        local.set_telemetry_sink(Some(probe.clone()));
        let stopped = CheckpointWorkContext::new(child).with_scheduler(local.clone());
        assert!(matches!(
            admitted_bloom(&keys, &stopped),
            Err(CanonicalSegmentError::Work(CheckpointWorkError::Stopped(_)))
        ));
        assert_eq!(probe.completed.load(Ordering::SeqCst), cut);
        assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
        probe.assert_released(&local);
        drop(stopped);
        assert_eq!(used(&task), 0);
        local.set_telemetry_sink(None);
        let (actual, memory) = admitted_bloom(&keys, &work).unwrap();
        assert_eq!(actual, expected);
        drop(actual);
        drop(memory);
        assert_eq!(used(&task), 0);
    }
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[derive(Debug)]
struct ObservedIo {
    admitted: RuntimeTaskContext,
    probe: Arc<CheckpointWorkProbe>,
}
#[derive(Debug)]
struct ObservedIoLease {
    _actual: Box<dyn RuntimeIoWavePermit>,
    _probe: Box<dyn RuntimeIoWavePermit>,
}
impl RuntimeIoWaveController for ObservedIo {
    fn try_acquire(
        &self,
        slots: NonZeroUsize,
        task: &RuntimeTaskContext,
    ) -> Result<Option<Box<dyn RuntimeIoWavePermit>>, RuntimeIoWaveError> {
        task.checkpoint().map_err(RuntimeIoWaveError::Stopped)?;
        let hawdb_core::RuntimeIoWaveTryAcquire::Acquired(Some(actual)) =
            self.admitted.try_acquire_io_wave(slots)?
        else {
            return Ok(None);
        };
        let probe = self.probe.acquire(slots, task)?;
        Ok(Some(Box::new(ObservedIoLease {
            _actual: actual,
            _probe: probe,
        })))
    }
    fn acquire(
        &self,
        slots: NonZeroUsize,
        task: &RuntimeTaskContext,
    ) -> Result<Box<dyn RuntimeIoWavePermit>, RuntimeIoWaveError> {
        task.checkpoint().map_err(RuntimeIoWaveError::Stopped)?;
        let actual = self
            .admitted
            .acquire_io_wave(slots)?
            .expect("real admitted wave");
        let probe = self.probe.acquire(slots, task)?;
        Ok(Box::new(ObservedIoLease {
            _actual: actual,
            _probe: probe,
        }))
    }
}
fn observed_context(
    task: &RuntimeTaskContext,
    local: &LocalQosScheduler,
    probe: Arc<CheckpointWorkProbe>,
) -> CheckpointWorkContext {
    local.set_telemetry_sink(Some(probe.clone()));
    CheckpointWorkContext::new(task.clone().with_io_wave_controller(Arc::new(ObservedIo {
        admitted: task.clone(),
        probe,
    })))
    .with_scheduler(local.clone())
}

fn attempt(
    fixture: &Fixture,
    task: &RuntimeTaskContext,
    work: &CheckpointWorkContext,
    name: &str,
) -> (
    Result<checkpoint_writer::descriptor::Descriptor, CanonicalSegmentError>,
    hawdb_integrity::IntegrityDigest,
    PathBuf,
) {
    // Source preparation is separately admitted, outside the flush probe.
    let plain = CheckpointWorkContext::new(task.clone());
    let mut source = checkpoint_writer::accumulator::Accumulator::new(
        fixture.kind,
        ManifestGeneration(31),
        5,
        CanonicalSegmentConfig::default(),
        plain.clone(),
        true,
    );
    match fixture.kind {
        CanonicalSegmentKind::Nodes => {
            let payload = encode_node_with_property_spills(&fixture.node, None, None).unwrap();
            source.push(17, &payload, None).unwrap();
            source
                .add_node_properties(&fixture.node, Some(&plain))
                .unwrap();
        }
        CanonicalSegmentKind::Relationships => {
            let payload = encode_relationship(&fixture.relationship).unwrap();
            source.push(17, &payload, Some((23, 29))).unwrap();
        }
    }
    let path = fixture.directory.join(name);
    let mut file = File::create(&path).unwrap();
    file.write_all(&fixture.expected[..24]).unwrap();
    let mut digest = IntegrityHasher::new();
    digest.update(&fixture.expected[..24]);
    let result = source.flush_with_work_context(&mut file, &mut digest, 24, Some(work));
    drop(file);
    (result, digest.finish(), path)
}
fn initial_digest(fixture: &Fixture) -> hawdb_integrity::IntegrityDigest {
    let mut digest = IntegrityHasher::new();
    digest.update(&fixture.expected[..24]);
    digest.finish()
}

#[test]
fn checkpoint_units_canonical_descriptor_memory_related_owned_flush_every_cpu_and_real_io_cut_retries(
) {
    for kind in [
        CanonicalSegmentKind::Nodes,
        CanonicalSegmentKind::Relationships,
    ] {
        let fixture = Fixture::new(kind);
        let governor = governor();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
            .unwrap();
        let task = permit.bind_task_context(RuntimeTaskContext::default());
        let local = local();
        let baseline = Arc::new(CheckpointWorkProbe::default());
        let work = observed_context(&task, &local, baseline.clone());
        let (result, digest, path) = attempt(&fixture, &task, &work, "owned-baseline.hawdb");
        let descriptor = result.unwrap();
        fixture.assert_result(&path, &descriptor, digest);
        assert!(used(&task) > 0);
        drop(descriptor);
        assert_eq!(used(&task), 0);
        let cpu = baseline.completed.load(Ordering::SeqCst);
        let io = baseline.io_waves.load(Ordering::SeqCst);
        assert_eq!(
            io,
            1 + (fixture.expected.len() - 24 - 29).div_ceil(64 * 1024)
        );
        assert!(cpu > 2 * io + 9);
        assert_eq!(baseline.peak_units.load(Ordering::SeqCst), 1);
        baseline.assert_released(&local);
        drop(work);
        for is_io in [false, true] {
            for cut in 1..=if is_io { io } else { cpu } {
                let child = task.child();
                let probe = Arc::new(CheckpointWorkProbe {
                    cancellation: child.cancellation().clone(),
                    ..Default::default()
                });
                if is_io {
                    probe.cancel_on_io_wave.store(cut, Ordering::SeqCst);
                } else {
                    probe.cancel_after.store(cut, Ordering::SeqCst);
                }
                let stopped = observed_context(&child, &local, probe.clone());
                let (result, digest, path) = attempt(
                    &fixture,
                    &child,
                    &stopped,
                    &format!("owned-cut-{is_io}-{cut}.hawdb"),
                );
                assert!(matches!(
                    result,
                    Err(CanonicalSegmentError::Work(
                        CheckpointWorkError::Stopped(_)
                            | CheckpointWorkError::Io(RuntimeIoWaveError::Stopped(_))
                    ))
                ));
                assert_eq!(digest, initial_digest(&fixture));
                assert!(fixture.expected.starts_with(&std::fs::read(path).unwrap()));
                assert_eq!(
                    if is_io {
                        probe.io_waves.load(Ordering::SeqCst)
                    } else {
                        probe.completed.load(Ordering::SeqCst)
                    },
                    cut
                );
                assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
                probe.assert_released(&local);
                drop(stopped);
                drop(child);
                assert_eq!(used(&task), 0);
                assert_eq!(governor.snapshot().active_background_io_slots, 0);
                local.set_telemetry_sink(None);
                let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
                let (result, digest, path) = attempt(
                    &fixture,
                    &task,
                    &work,
                    &format!("owned-retry-{is_io}-{cut}.hawdb"),
                );
                let descriptor = result.unwrap();
                fixture.assert_result(&path, &descriptor, digest);
                drop(descriptor);
                drop(work);
                assert_eq!(used(&task), 0);
            }
        }
        drop(task);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn checkpoint_units_canonical_descriptor_memory_related_owned_flush_zero_wave_denial_and_retry() {
    let fixture = Fixture::new(CanonicalSegmentKind::Relationships);
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(CEILING))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local());
    let (result, digest, path) = attempt(&fixture, &task, &work, "owned-wave-denied.hawdb");
    assert!(matches!(
        result,
        Err(CanonicalSegmentError::Work(CheckpointWorkError::Io(
            RuntimeIoWaveError::ReservationExceeded { .. }
        )))
    ));
    assert_eq!(digest, initial_digest(&fixture));
    assert_eq!(std::fs::read(path).unwrap(), fixture.expected[..24]);
    assert_eq!(used(&task), 0);
    drop(work);
    drop(task);
    drop(permit);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local());
    let (result, digest, path) = attempt(&fixture, &task, &work, "owned-wave-retry.hawdb");
    let descriptor = result.unwrap();
    fixture.assert_result(&path, &descriptor, digest);
    drop(descriptor);
    assert_eq!(used(&task), 0);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
}
