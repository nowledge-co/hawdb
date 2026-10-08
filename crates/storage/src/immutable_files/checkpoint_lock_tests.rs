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
use crate::background::{CheckpointWorkContext, CheckpointWorkProbe};
use crate::cache::content_digest;
use crate::file_descriptors::ProjectFileDescriptors;
use crate::immutable_object::{ImmutableObjectStore, ObjectKind};
use crate::scan::{FileSegmentRangeReader, SegmentRangeReader, SegmentReadRange};
use hawdb_core::{
    RuntimeIoWaveController, RuntimeIoWaveError, RuntimeIoWavePermit, RuntimeIoWaveTryAcquire,
    RuntimeTaskContext,
};
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
};
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::mpsc;
use std::time::Duration;

const CEILING: u64 = 4 * 1024 * 1024;

struct Fixture {
    root: PathBuf,
    project: ProjectFileDescriptors,
    background: FileSegmentRangeReader,
    foreground: FileSegmentRangeReader,
    background_range: SegmentReadRange,
    foreground_range: SegmentReadRange,
    background_binding: ImmutableFileBinding,
    background_bytes: Vec<u8>,
    foreground_bytes: Vec<u8>,
}

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "hawdb-checkpoint-opening-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        std::fs::create_dir(&root).unwrap();
        let project = ProjectFileDescriptors::acquire_existing(&root, 8).unwrap();
        let mut objects = ImmutableObjectStore::open(root.join("immutable")).unwrap();
        let background_bytes = (0..32 * 64 * 1024 + 7)
            .map(|index| (index * 79 % 251) as u8)
            .collect::<Vec<_>>();
        let foreground_bytes = (0..128).map(|index| (index * 11) as u8).collect::<Vec<_>>();
        let mut mount = |name: &str, artifact: u64, bytes: &[u8]| {
            let reference = ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, bytes);
            objects.publish(reference, bytes).unwrap();
            let binding = ImmutableFileBinding {
                reference,
                object_path: objects.object_path(reference),
            };
            let path = root.join(name);
            project
                .immutable_handles
                .mount(&path, binding.clone(), &project.io_context())
                .unwrap();
            assert_eq!(
                project
                    .immutable_handles
                    .binding(&path)
                    .unwrap()
                    .unwrap()
                    .reference,
                reference
            );
            let mut reader = FileSegmentRangeReader::new();
            reader.register(artifact, path);
            let range = SegmentReadRange::new(
                artifact,
                artifact + 100,
                bytes.len() as u64 - 7,
                NonZeroU64::new(7).unwrap(),
            )
            .with_content_digest(content_digest(&bytes[bytes.len() - 7..]));
            (reader, range, binding)
        };
        let (background, background_range, background_binding) =
            mount("background.page", 7, &background_bytes);
        let (foreground, foreground_range, _) = mount("foreground.page", 9, &foreground_bytes);
        assert_eq!(project.metrics().open, 0);
        assert_eq!(project.metrics().cached_handles, 0);
        Self {
            root,
            project,
            background,
            foreground,
            background_range,
            foreground_range,
            background_binding,
            background_bytes,
            foreground_bytes,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[derive(Debug)]
struct PausedIo {
    governed: RuntimeTaskContext,
    probe: Arc<CheckpointWorkProbe>,
    entered: mpsc::Sender<()>,
    resume: Mutex<mpsc::Receiver<()>>,
}

#[derive(Debug)]
struct PausedWave {
    _governed: Option<Box<dyn RuntimeIoWavePermit>>,
    _observed: Box<dyn RuntimeIoWavePermit>,
}

impl PausedIo {
    fn pause(&self) {
        if self.probe.io_waves.load(Ordering::SeqCst) == 2 {
            self.entered.send(()).unwrap();
            self.resume
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(30))
                .expect("the owner must release the paused native read");
        }
    }
}

impl RuntimeIoWaveController for PausedIo {
    fn acquire(
        &self,
        slots: NonZeroUsize,
        task: &RuntimeTaskContext,
    ) -> Result<Box<dyn RuntimeIoWavePermit>, RuntimeIoWaveError> {
        let observed = self.probe.acquire(slots, task)?;
        let governed = self.governed.acquire_io_wave(slots)?;
        self.pause();
        Ok(Box::new(PausedWave {
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
            RuntimeIoWaveTryAcquire::Acquired(governed) => {
                self.pause();
                Ok(Some(Box::new(PausedWave {
                    _governed: governed,
                    _observed: observed,
                })))
            }
        }
    }
}

fn paused_action<R: Send>(fixture: &Fixture, action: impl FnOnce() -> R + Send) -> Option<R> {
    let governor = RuntimeGovernor::new(
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
    );
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
        .unwrap();
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..LocalQosPolicy::default()
    });
    let probe = Arc::new(CheckpointWorkProbe::default());
    scheduler.set_telemetry_sink(Some(probe.clone()));
    let (entered, paused) = mpsc::channel();
    let (resume, release) = mpsc::channel();
    let task = permit.bind_task_context(RuntimeTaskContext::without_deadline(
        probe.cancellation.clone(),
    ));
    let controller = Arc::new(PausedIo {
        governed: task.clone(),
        probe: probe.clone(),
        entered,
        resume: Mutex::new(release),
    });
    let work = CheckpointWorkContext::new(task.with_io_wave_controller(controller))
        .with_scheduler(scheduler.clone());
    let result = std::thread::scope(|scope| {
        let background = scope.spawn(|| {
            fixture
                .background
                .checkpoint_range_with_work_context(&fixture.background_range, &work)
                .map(|bytes| bytes.to_vec())
        });
        paused.recv_timeout(Duration::from_secs(30)).unwrap();
        assert_eq!(governor.snapshot().active_background_io_slots, 1);
        assert_eq!(fixture.project.metrics().open, 1);
        assert_eq!(fixture.project.metrics().cached_handles, 1);
        assert!(fixture
            .project
            .immutable_handles
            .handles
            .lock()
            .unwrap()
            .is_empty());
        let (finished, completed) = mpsc::channel();
        let concurrent = scope.spawn(move || assert!(finished.send(action()).is_ok()));
        let result = completed.recv_timeout(Duration::from_secs(5)).ok();
        // Always unblock both threads before checking whether the action stalled.
        resume.send(()).unwrap();
        assert_eq!(
            background.join().unwrap().unwrap(),
            fixture.background_bytes[fixture.background_bytes.len() - 7..]
        );
        concurrent.join().unwrap();
        result
    });
    probe.assert_released(&scheduler);
    assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(
        std::fs::read(&fixture.background_binding.object_path).unwrap(),
        fixture.background_bytes
    );
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    result
}

#[test]
fn checkpoint_units_immutable_opening_foreground_read_finishes_while_background_hash_is_paused() {
    let fixture = Fixture::new();
    let result = paused_action(&fixture, || {
        fixture
            .foreground
            .read_range(&fixture.foreground_range)
            .map(|bytes| bytes.to_vec())
    });
    let result = result.expect("foreground read waited for the paused background validation");
    assert_eq!(
        result.unwrap(),
        fixture.foreground_bytes[fixture.foreground_bytes.len() - 7..]
    );
    assert_eq!(fixture.project.metrics().cached_handles, 2);
    assert_eq!(fixture.project.immutable_handles.evict_idle(usize::MAX), 2);
    assert_eq!(fixture.project.metrics().open, 0);
    assert_eq!(fixture.project.metrics().reserved, 0);
}

#[test]
fn checkpoint_units_immutable_opening_retirement_defers_without_waiting_for_background_hash() {
    let fixture = Fixture::new();
    let result = paused_action(&fixture, || {
        fixture
            .project
            .immutable_handles
            .retire_unreachable(fixture.background_binding.reference)
    });
    let result = result.expect("retirement waited for the paused background validation");
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    fixture
        .project
        .immutable_handles
        .retire_unreachable(fixture.background_binding.reference)
        .unwrap();
    assert_eq!(fixture.project.metrics().open, 0);
    assert_eq!(fixture.project.metrics().reserved, 0);
}

#[test]
fn checkpoint_units_immutable_opening_reuses_a_foreground_published_handle_of_the_same_object() {
    let fixture = Fixture::new();
    let result = paused_action(&fixture, || {
        fixture
            .project
            .immutable_handles
            .get(&fixture.background_binding, &fixture.project.io_context())
    });
    let foreground = result
        .expect("same-object foreground validation waited for the paused checkpoint")
        .unwrap();
    let published = fixture
        .project
        .immutable_handles
        .handles
        .lock()
        .unwrap()
        .get(&fixture.background_binding.reference)
        .unwrap()
        .clone();
    assert!(Arc::ptr_eq(&foreground, &published));
    assert_eq!(fixture.project.metrics().open, 1);
    assert_eq!(fixture.project.metrics().cached_handles, 1);
    assert_eq!(
        fixture
            .project
            .immutable_handles
            .retire_unreachable(fixture.background_binding.reference)
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    drop(published);
    drop(foreground);
    fixture
        .project
        .immutable_handles
        .retire_unreachable(fixture.background_binding.reference)
        .unwrap();
    assert_eq!(fixture.project.metrics().open, 0);
    assert_eq!(fixture.project.metrics().reserved, 0);
}

#[test]
fn checkpoint_units_immutable_opening_object_sweep_retains_inflight_source_and_retries_after_close()
{
    use crate::immutable_object::ImmutableObjectError;
    let fixture = Fixture::new();
    let result = paused_action(&fixture, || {
        ImmutableObjectStore::open(fixture.root.join("immutable"))
            .unwrap()
            .reclaim_unreachable(&[fixture.background_binding.reference], &[])
    });
    let error = result
        .expect("object sweep waited for the paused checkpoint validation")
        .unwrap_err();
    assert!(
        matches!(error, ImmutableObjectError::Io { ref source, .. } if source.kind() == io::ErrorKind::WouldBlock),
        "{error:?}"
    );
    let report = ImmutableObjectStore::open(fixture.root.join("immutable"))
        .unwrap()
        .reclaim_unreachable(&[fixture.background_binding.reference], &[])
        .unwrap();
    assert_eq!(report.reclaimed_objects, 1);
    assert_eq!(
        report.reclaimed_bytes,
        fixture.background_bytes.len() as u64
    );
    assert!(!fixture.background_binding.object_path.exists());
    assert_eq!(fixture.project.metrics().open, 0);
    assert_eq!(fixture.project.metrics().reserved, 0);
}
