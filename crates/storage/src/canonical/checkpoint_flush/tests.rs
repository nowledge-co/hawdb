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
fn local() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    })
}
fn assert_no_memory(task: &RuntimeTaskContext) {
    match task.reserve_working_memory(1) {
        Ok(lease) => drop(lease),
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes: 1, ..
        }) => {}
        other => panic!("unexpected retained flush scratch {other:?}"),
    }
}
fn attempt(
    fixture: &Fixture,
    work: &CheckpointWorkContext,
    name: &str,
) -> (
    Result<CanonicalSegmentDescriptor, CanonicalSegmentError>,
    hawdb_integrity::IntegrityDigest,
    PathBuf,
) {
    // Source records predate this flush-scratch observation. Their ownership,
    // and the result's Bloom allocation, remain separate ledger obligations.
    let source = fixture.accumulator();
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
// Observe every actual I/O boundary while still acquiring the real governor's
// one-wave reservation. The probe alone is not an admission oracle.
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
            .expect("admitted context has a real I/O controller");
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
    let context = task.clone().with_io_wave_controller(Arc::new(ObservedIo {
        admitted: task.clone(),
        probe,
    }));
    CheckpointWorkContext::new(context).with_scheduler(local.clone())
}

#[test]
fn checkpoint_units_canonical_flush_memory_related_every_cpu_and_io_cut_retries_complete_bytes() {
    for kind in [
        CanonicalSegmentKind::Nodes,
        CanonicalSegmentKind::Relationships,
    ] {
        let fixture = Fixture::new(kind);
        let governor = governor(1);
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(1).with_io_wave_slots(1))
            .unwrap();
        let task = permit.bind_task_context(RuntimeTaskContext::default());
        let local = local();
        let baseline = Arc::new(CheckpointWorkProbe::default());
        let work = observed_context(&task, &local, baseline.clone());
        let (result, digest, path) = attempt(&fixture, &work, "baseline.hawdb");
        fixture.assert_result(&path, &result.unwrap(), digest);
        let cpu = baseline.completed.load(Ordering::SeqCst);
        let io = baseline.io_waves.load(Ordering::SeqCst);
        let blocks = 1 + (fixture.expected.len() - 24 - 29).div_ceil(64 * 1024);
        assert_eq!(io, blocks);
        assert!(cpu >= 2 * blocks + 3);
        assert_eq!(baseline.peak_units.load(Ordering::SeqCst), 1);
        baseline.assert_released(&local);
        assert_no_memory(&task);
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
                let work = observed_context(&child, &local, probe.clone());
                let (result, digest, path) =
                    attempt(&fixture, &work, &format!("cut-{is_io}-{cut}.hawdb"));
                assert!(
                    matches!(
                        result,
                        Err(CanonicalSegmentError::Work(
                            CheckpointWorkError::Stopped(_)
                                | CheckpointWorkError::Io(RuntimeIoWaveError::Stopped(_))
                        ))
                    ),
                    "cut={cut}, io={is_io}, result={result:?}"
                );
                assert_eq!(
                    digest,
                    initial_digest(&fixture),
                    "failed flush cannot advance caller digest"
                );
                let partial = std::fs::read(&path).unwrap();
                assert!(fixture.expected.starts_with(&partial));
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
                assert_no_memory(&task);
                assert_eq!(governor.snapshot().active_background_io_slots, 0);
            }
        }
        local.set_telemetry_sink(None);
        let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
        let (result, digest, path) = attempt(&fixture, &work, "retry.hawdb");
        fixture.assert_result(&path, &result.unwrap(), digest);
        assert_no_memory(&task);
        drop(work);
        drop(task);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        assert_eq!(governor.snapshot().active_background_io_slots, 0);
    }
}
#[test]
fn checkpoint_units_canonical_flush_memory_related_real_wave_denial_leaves_header_and_retry() {
    let fixture = Fixture::new(CanonicalSegmentKind::Relationships);
    let governor = governor(1);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local());
    let (result, digest, path) = attempt(&fixture, &work, "denied.hawdb");
    assert!(matches!(
        result,
        Err(CanonicalSegmentError::Work(CheckpointWorkError::Io(_)))
    ));
    assert_eq!(std::fs::read(&path).unwrap(), fixture.expected[..24]);
    assert_eq!(digest, initial_digest(&fixture));
    assert_no_memory(&task);
    // The denial is an actual zero-slot reservation error, not a blocking
    // wait for a wave held by this same thread. Retry re-admits one I/O slot;
    // every-cut coverage above separately retries the same one-wave grant.
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(1).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local());
    let (result, digest, path) = attempt(&fixture, &work, "retry.hawdb");
    fixture.assert_result(&path, &result.unwrap(), digest);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
#[test]
fn checkpoint_units_canonical_flush_memory_related_size_error_precedes_bytes_and_preserves_digest()
{
    let fixture = Fixture::new(CanonicalSegmentKind::Nodes);
    let governor = governor(1);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(1).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let local = local();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = observed_context(&task, &local, probe.clone());
    let mut source = fixture.accumulator();
    source.config.target_segment_bytes = NonZeroU64::MIN;
    source.config.max_record_bytes = NonZeroU64::MIN;
    let path = fixture.directory.join("invalid.hawdb");
    let mut file = File::create(&path).unwrap();
    file.write_all(&fixture.expected[..24]).unwrap();
    let mut digest = IntegrityHasher::new();
    digest.update(&fixture.expected[..24]);
    let observation = crate::test_allocator::AllocationObservation::start();
    let result = source.flush_with_work_context(&mut file, &mut digest, 24, Some(&work));
    let allocations = observation.finish();
    drop(file);
    assert!(
        matches!(result,Err(CanonicalSegmentError::SegmentTooLarge {segment_bytes,max_bytes}) if segment_bytes==fixture.expected.len() as u64-24 && max_bytes==30)
    );
    assert_eq!(allocations, 0);
    assert_eq!(probe.completed.load(Ordering::SeqCst), 0);
    assert_eq!(probe.io_waves.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read(&path).unwrap(), fixture.expected[..24]);
    assert_eq!(digest.finish(), initial_digest(&fixture));
    probe.assert_released(&local);
    assert_no_memory(&task);
    let (result, digest, path) = attempt(&fixture, &work, "retry.hawdb");
    fixture.assert_result(&path, &result.unwrap(), digest);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
#[test]
fn checkpoint_units_canonical_flush_memory_related_bloom_insertions_match_reference_and_every_cut()
{
    // CPU-boundary evidence only. Returned Bloom memory ownership is not
    // admitted yet and is explicitly outside this flush-scratch correction.
    let keys = (0..8193)
        .map(|i| (i as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15))
        .collect::<Vec<_>>();
    let expected = CanonicalEndpointBloom::from_keys(&keys);
    let governor = governor(1 << 20);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(1 << 20))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let local = local();
    let baseline = Arc::new(CheckpointWorkProbe::default());
    local.set_telemetry_sink(Some(baseline.clone()));
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    assert_eq!(bloom(&keys, Some(&work)).unwrap(), expected);
    let total = baseline.completed.load(Ordering::SeqCst);
    assert_eq!(total, 1 + keys.len().div_ceil(256));
    assert_eq!(baseline.peak_units.load(Ordering::SeqCst), 1);
    baseline.assert_released(&local);
    drop(work);
    for cut in 1..=total {
        let child = task.child();
        let probe = Arc::new(CheckpointWorkProbe {
            cancellation: child.cancellation().clone(),
            ..Default::default()
        });
        probe.cancel_after.store(cut, Ordering::SeqCst);
        local.set_telemetry_sink(Some(probe.clone()));
        let work = CheckpointWorkContext::new(child).with_scheduler(local.clone());
        assert!(matches!(
            bloom(&keys, Some(&work)),
            Err(CanonicalSegmentError::Work(CheckpointWorkError::Stopped(_)))
        ));
        assert_eq!(probe.completed.load(Ordering::SeqCst), cut);
        assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
        probe.assert_released(&local);
    }
    local.set_telemetry_sink(None);
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    assert_eq!(bloom(&keys, Some(&work)).unwrap(), expected);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
