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
use crate::file_descriptors::ProjectFileDescriptors;
use crate::immutable_files::ImmutableFileBinding;
use crate::immutable_object::{ImmutableObjectStore, ObjectKind, ObjectReference};
use std::sync::atomic::{AtomicU64, Ordering};

const CEILING: u64 = 4 * 1024 * 1024;
const CHUNK: usize = 64 * 1024;

struct ImmutableFixture {
    root: PathBuf,
    project: ProjectFileDescriptors,
    reader: FileSegmentRangeReader,
    range: SegmentReadRange,
    bytes: Vec<u8>,
}

impl ImmutableFixture {
    fn new(length: usize) -> Self {
        static ORDINAL: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "hawdb-checkpoint-immutable-handle-{}-{}",
            std::process::id(),
            ORDINAL.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        let project = ProjectFileDescriptors::acquire_existing(&root, 8).unwrap();
        let mut objects = ImmutableObjectStore::open(root.join("immutable")).unwrap();
        let bytes = (0..length)
            .map(|ordinal| (ordinal.wrapping_mul(79) % 251) as u8)
            .collect::<Vec<_>>();
        let reference = ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, &bytes);
        objects.publish(reference, &bytes).unwrap();
        let path = root.join("captured.page");
        project
            .immutable_handles
            .mount(
                &path,
                ImmutableFileBinding {
                    reference,
                    object_path: objects.object_path(reference),
                },
                &crate::file_descriptors::context_for_path(&path).unwrap(),
            )
            .unwrap();
        let mut reader = FileSegmentRangeReader::new();
        reader.register(7, path);
        assert!(reader.artifacts[&7]
            .registration()
            .unwrap()
            .binding
            .is_some());
        assert_eq!(project.metrics().cached_handles, 0);
        let range = SegmentReadRange::new(7, 11, length as u64 - 7, NonZeroU64::new(7).unwrap())
            .with_content_digest(content_digest(&bytes[length - 7..]));
        Self {
            root,
            project,
            reader,
            range,
            bytes,
        }
    }

    fn expected(&self) -> &[u8] {
        &self.bytes[self.bytes.len() - 7..]
    }
}

impl Drop for ImmutableFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[test]
fn checkpoint_units_immutable_handle_cold_identity_validation_is_bounded_even_for_tiny_ranges() {
    let fixture = ImmutableFixture::new(32 * CHUNK + 7);
    let governor = governor(CEILING);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
        .unwrap();
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    local.set_telemetry_sink(Some(probe.clone()));
    let task = permit.bind_task_context(RuntimeTaskContext::without_deadline(
        probe.cancellation.clone(),
    ));
    let work = CheckpointWorkContext::new(task).with_scheduler(local.clone());
    let output = fixture
        .reader
        .checkpoint_range_with_work_context(&fixture.range, &work)
        .unwrap();
    assert_eq!(&*output, fixture.expected());
    let completed = probe.completed.load(Ordering::SeqCst);
    assert!(
        completed >= fixture.bytes.len().div_ceil(CHUNK),
        "a seven-byte read validated the entire {}-byte immutable object in only {completed} units",
        fixture.bytes.len()
    );
    assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
    probe.assert_released(&local);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(fixture.project.metrics().cached_handles, 1);
}

#[test]
fn checkpoint_units_immutable_handle_cold_identity_scratch_requires_memory_admission() {
    let fixture = ImmutableFixture::new(32 * CHUNK + 7);
    let governor = governor(4096);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(4096).with_io_wave_slots(1))
        .unwrap();
    let local = scheduler();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task).with_scheduler(local.clone());
    assert!(matches!(
        fixture
            .reader
            .checkpoint_range_with_work_context(&fixture.range, &work),
        Err(CheckpointRangeReadError::Work(CheckpointWorkError::Memory(
            _
        )))
    ));
    assert_eq!(fixture.project.metrics().cached_handles, 0);
    assert_eq!(fixture.project.metrics().open, 0);
    assert_eq!(local.state().running_background_operations, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

use crate::file_descriptors::DescriptorCache;
use hawdb_core::{
    RuntimeIoWaveController, RuntimeIoWaveError, RuntimeIoWavePermit, RuntimeIoWaveTryAcquire,
};

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

fn observed(task: RuntimeTaskContext, probe: &Arc<CheckpointWorkProbe>) -> RuntimeTaskContext {
    let controller = Arc::new(ObservedIo {
        governed: task.clone(),
        probe: probe.clone(),
    });
    task.with_io_wave_controller(controller)
}

fn available(task: &RuntimeTaskContext) -> u64 {
    match task.reserve_working_memory(CEILING) {
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes, ..
        }) => available_bytes,
        Ok(Some(lease)) => {
            drop(lease);
            CEILING
        }
        _ => panic!("the probe must retain the actual governor memory controller"),
    }
}

impl ImmutableFixture {
    fn binding(&self) -> &ImmutableFileBinding {
        self.reader.artifacts[&7]
            .registration()
            .unwrap()
            .binding
            .as_ref()
            .unwrap()
    }

    fn assert_source_unchanged(&self) {
        assert_eq!(
            std::fs::read(&self.binding().object_path).unwrap(),
            self.bytes
        );
        assert_eq!(
            ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, &self.bytes),
            self.binding().reference
        );
    }

    fn evict(&self) {
        self.project.immutable_handles.evict_idle(usize::MAX);
        assert_eq!(self.project.metrics().open, 0);
        assert_eq!(self.project.metrics().reserved, 0);
    }
}

#[test]
fn checkpoint_units_immutable_handle_cancels_every_actual_cpu_and_io_wave_then_retries() {
    let fixture = ImmutableFixture::new(2 * CHUNK + 7);
    let governor = governor(CEILING);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
        .unwrap();
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    local.set_telemetry_sink(Some(probe.clone()));
    let task = observed(
        permit.bind_task_context(RuntimeTaskContext::without_deadline(
            probe.cancellation.clone(),
        )),
        &probe,
    );
    let idle = available(&task);
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    let output = fixture
        .reader
        .checkpoint_range_with_work_context(&fixture.range, &work)
        .unwrap();
    assert_eq!(&*output, fixture.expected());
    let units = probe.completed.load(Ordering::SeqCst);
    let waves = probe.io_waves.load(Ordering::SeqCst);
    assert!(units >= 2 * fixture.bytes.len().div_ceil(CHUNK));
    assert!(waves >= fixture.bytes.len().div_ceil(CHUNK) + 3);
    drop(output);
    assert_eq!(available(&task), idle);
    probe.assert_released(&local);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    fixture.evict();
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);

    for (cpu, limit) in (1..=units)
        .map(|limit| (true, limit))
        .chain((1..=waves).map(|limit| (false, limit)))
    {
        let governor = super::governor(CEILING);
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
            .unwrap();
        let local = scheduler();
        let probe = Arc::new(CheckpointWorkProbe::default());
        if cpu {
            probe.cancel_after.store(limit, Ordering::SeqCst);
        } else {
            probe.cancel_on_io_wave.store(limit, Ordering::SeqCst);
        }
        local.set_telemetry_sink(Some(probe.clone()));
        let task = observed(
            permit.bind_task_context(RuntimeTaskContext::without_deadline(
                probe.cancellation.clone(),
            )),
            &probe,
        );
        let accounting = permit.bind_task_context(RuntimeTaskContext::default());
        let idle = available(&accounting);
        let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
        let result = fixture
            .reader
            .checkpoint_range_with_work_context(&fixture.range, &work);
        assert!(
            matches!(
                result,
                Err(CheckpointRangeReadError::Work(
                    CheckpointWorkError::Stopped(_)
                )) | Err(CheckpointRangeReadError::Work(CheckpointWorkError::Io(
                    RuntimeIoWaveError::Stopped(_)
                )))
            ),
            "actual cpu={cpu} boundary={limit} did not stop: {result:?}"
        );
        assert_eq!(available(&accounting), idle);
        probe.assert_released(&local);
        assert_eq!(governor.snapshot().active_background_io_slots, 0);
        fixture.assert_source_unchanged();
        drop(work);
        drop(task);
        local.set_telemetry_sink(None);
        let retry =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()))
                .with_scheduler(local.clone());
        let output = fixture
            .reader
            .checkpoint_range_with_work_context(&fixture.range, &retry)
            .unwrap();
        assert_eq!(&*output, fixture.expected());
        assert_eq!(governor.snapshot().admissions, 1);
        drop(output);
        drop(retry);
        drop(accounting);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        fixture.evict();
    }
}

#[test]
fn checkpoint_units_immutable_handle_memory_denial_retries_the_same_governor_reservation() {
    let fixture = ImmutableFixture::new(2 * CHUNK + 7);
    let governor = governor(CEILING);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let idle = available(&task);
    // The governor also admits its concrete lease allocation. Measure that
    // ownership through the same controller rather than assuming zero overhead.
    let probe_lease = task.reserve_working_memory(1).unwrap().unwrap();
    let lease_overhead = idle - available(&task) - 1;
    drop(probe_lease);
    assert_eq!(available(&task), idle);
    let held = task
        .reserve_working_memory(idle - 4096 - lease_overhead)
        .unwrap()
        .unwrap();
    let before = available(&task);
    assert_eq!(before, 4096);
    let local = scheduler();
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    assert!(matches!(
        fixture
            .reader
            .checkpoint_range_with_work_context(&fixture.range, &work),
        Err(CheckpointRangeReadError::Work(CheckpointWorkError::Memory(
            _
        )))
    ));
    assert_eq!(available(&task), before);
    assert_eq!(fixture.project.metrics().open, 0);
    assert_eq!(local.state().running_background_operations, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    fixture.assert_source_unchanged();
    drop(held);
    let output = fixture
        .reader
        .checkpoint_range_with_work_context(&fixture.range, &work)
        .unwrap();
    assert_eq!(&*output, fixture.expected());
    assert_eq!(governor.snapshot().admissions, 1);
    drop(output);
    assert_eq!(available(&task), idle);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    fixture.evict();
}

#[test]
fn checkpoint_units_immutable_handle_descriptor_denial_is_typed_and_retries_after_release() {
    use hawdb_core::error::FileDescriptorError;
    let fixture = ImmutableFixture::new(2 * CHUNK + 7);
    let governor = governor(CEILING);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let idle = available(&task);
    let local = scheduler();
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    let held = fixture.project.reserve(8).unwrap();
    assert_eq!(fixture.project.metrics().reserved, 8);
    let result = fixture
        .reader
        .checkpoint_range_with_work_context(&fixture.range, &work);
    assert!(
        matches!(
            result,
            Err(CheckpointRangeReadError::Work(
                CheckpointWorkError::FileDescriptors(FileDescriptorError::BudgetExceeded {
                    requested: 1,
                    available: 0,
                    limit: 8
                })
            ))
        ),
        "FD denial must be a recoverable work rejection: {result:?}"
    );
    assert_eq!(available(&task), idle);
    assert_eq!(fixture.project.metrics().open, 0);
    assert_eq!(fixture.project.metrics().reserved, 8);
    assert_eq!(local.state().running_background_operations, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(held);
    let output = fixture
        .reader
        .checkpoint_range_with_work_context(&fixture.range, &work)
        .unwrap();
    assert_eq!(&*output, fixture.expected());
    fixture.assert_source_unchanged();
    drop(output);
    assert_eq!(available(&task), idle);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    fixture.evict();
}

#[test]
fn checkpoint_units_immutable_handle_cold_validation_rejects_corruption_outside_requested_range() {
    use std::io::Write;
    let fixture = ImmutableFixture::new(2 * CHUNK + 7);
    let mut damaged = std::fs::OpenOptions::new()
        .write(true)
        .open(&fixture.binding().object_path)
        .unwrap();
    damaged
        .write_all(&[fixture.bytes[0].wrapping_add(1)])
        .unwrap();
    drop(damaged);
    assert_eq!(
        &std::fs::read(&fixture.binding().object_path).unwrap()[fixture.bytes.len() - 7..],
        fixture.expected()
    );
    let governor = governor(CEILING);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let idle = available(&task);
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(scheduler());
    assert!(matches!(
        fixture.reader.checkpoint_range_with_work_context(&fixture.range, &work),
        Err(CheckpointRangeReadError::Read(SegmentReadError::Io { source, .. }))
            if source.kind() == std::io::ErrorKind::InvalidData
                && source.to_string().contains("identity digest mismatch")
    ));
    assert_eq!(available(&task), idle);
    assert_eq!(fixture.project.metrics().open, 0);
    assert_eq!(fixture.project.metrics().cached_handles, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_immutable_handle_warm_read_keeps_captured_identity_after_alias_replacement() {
    use crate::scan::SegmentRangeReader;
    let fixture = ImmutableFixture::new(2 * CHUNK + 7);
    let ordinary = fixture.reader.read_range(&fixture.range).unwrap();
    assert_eq!(&*ordinary, fixture.expected());
    assert_eq!(fixture.project.metrics().cached_handles, 1);
    let replacement = fixture.root.join("replacement.page");
    std::fs::write(&replacement, vec![42u8; fixture.bytes.len()]).unwrap();
    std::fs::rename(replacement, fixture.root.join("captured.page")).unwrap();
    let governor = governor(4096);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(4096).with_io_wave_slots(1))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()))
        .with_scheduler(scheduler());
    let output = fixture
        .reader
        .checkpoint_range_with_work_context(&fixture.range, &work)
        .unwrap();
    assert_eq!(&*output, fixture.expected());
    fixture.assert_source_unchanged();
    drop(output);
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    fixture.evict();
}
