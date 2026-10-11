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
        Ok(lease) => {
            drop(lease);
            0
        }
        Err(error) => panic!("unexpected working memory error: {error}"),
    }
}
struct Fixture {
    reader: CanonicalSegmentReader,
    nodes: Vec<NodeRecord>,
    relationships: Vec<RelRecord>,
    directory: PathBuf,
    path: PathBuf,
}
impl Fixture {
    fn new(n: usize, payload: usize) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "hawdb-checkpoint-scan-related-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("canonical.hawdb");
        let properties = |i: usize| {
            BTreeMap::from([
                ("id".into(), Value::Int(i as i64)),
                ("binary".into(), Value::Binary(vec![0xb7; payload])),
                ("text".into(), Value::String("界".repeat(payload / 3))),
                (
                    "nested".into(),
                    Value::List(vec![
                        Value::Null,
                        Value::Map(BTreeMap::from([("flag".into(), Value::Bool(true))])),
                    ]),
                ),
            ])
        };
        let nodes = (0..n)
            .map(|i| NodeRecord {
                id: NodeId(i as u64),
                labels: BTreeSet::from([LabelId(7)]),
                properties: properties(i),
            })
            .collect::<Vec<_>>();
        let relationships = (0..n)
            .map(|i| RelRecord {
                id: RelId(i as u64),
                source: NodeId(i as u64),
                target: NodeId(((i + 1) % n) as u64),
                rel_type: RelTypeId(11),
                properties: properties(i),
            })
            .collect::<Vec<_>>();
        Self::from_records(directory, path, nodes, relationships)
    }
    fn from_records(
        directory: PathBuf,
        path: PathBuf,
        nodes: Vec<NodeRecord>,
        relationships: Vec<RelRecord>,
    ) -> Self {
        let manifest = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .write(&path, ManifestGeneration(17), &nodes, &relationships)
            .unwrap();
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest,
            Arc::new(SegmentCache::new(8 * 1024 * 1024)),
            StoreId(17),
            NonZeroU64::new(32 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        Self {
            reader,
            nodes,
            relationships,
            directory,
            path,
        }
    }
    fn check_reader(
        &self,
        reader: &CanonicalSegmentReader,
        work: &CheckpointWorkContext,
    ) -> Result<(), CanonicalSegmentError> {
        let mut count = 0;
        for node in CheckpointCanonicalIterator::<NodeRecord>::new(reader, work) {
            assert_eq!(*node?, self.nodes[count]);
            count += 1;
        }
        assert_eq!(count, self.nodes.len());
        let mut count = 0;
        for rel in CheckpointCanonicalIterator::<RelRecord>::new(reader, work) {
            assert_eq!(*rel?, self.relationships[count]);
            count += 1;
        }
        assert_eq!(count, self.relationships.len());
        Ok(())
    }
    fn check(&self, work: &CheckpointWorkContext) -> Result<(), CanonicalSegmentError> {
        self.check_reader(&self.reader, work)
    }
    fn fresh_reader(&self) -> CanonicalSegmentReader {
        CanonicalSegmentReader::open(
            &self.path,
            self.reader.manifest.as_ref().clone(),
            Arc::new(SegmentCache::new(8 * 1024 * 1024)),
            StoreId(17),
            NonZeroU64::new(32 * 1024 * 1024).unwrap(),
        )
        .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.directory).unwrap();
    }
}

#[test]
fn checkpoint_units_scan_memory_related_node_and_relationship_owners_survive_iterator_and_execution(
) {
    let fixture = Fixture::new(3, 257 * 1024 + 3);
    let ceiling = 32 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let mut nodes = CheckpointCanonicalIterator::<NodeRecord>::new(&fixture.reader, &work);
    let node = nodes.next().unwrap().unwrap();
    drop(nodes);
    let mut relationships = CheckpointCanonicalIterator::<RelRecord>::new(&fixture.reader, &work);
    let relationship = relationships.next().unwrap().unwrap();
    drop(relationships);
    let selected_heap = 2 * (257 * 1024 + 3) + 2 * (257 * 1024 / 3 * 3);
    assert!(
        used(&task, ceiling) >= selected_heap,
        "selected records retain actual wide byte/string capacity"
    );
    assert!(
        used(&task, ceiling) < selected_heap + 96 * 1024,
        "other decoded rows, input and outer arrays must be released"
    );
    assert_eq!(*node, fixture.nodes[0]);
    assert_eq!(*relationship, fixture.relationships[0]);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    assert_eq!(*node, fixture.nodes[0]);
    assert_eq!(*relationship, fixture.relationships[0]);
    drop(node);
    assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    drop(relationship);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_scan_memory_related_outer_record_array_has_independent_capacity_admission() {
    let directory = std::env::temp_dir().join(format!(
        "hawdb-checkpoint-scan-array-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("canonical.hawdb");
    let n = 8193;
    let nodes = (0..n)
        .map(|i| NodeRecord {
            id: NodeId(i as u64),
            labels: BTreeSet::new(),
            properties: BTreeMap::new(),
        })
        .collect();
    let fixture = Fixture::from_records(directory, path, nodes, Vec::new());
    let ceiling = 32 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let mut scan = CheckpointCanonicalIterator::<NodeRecord>::new(&fixture.reader, &work);
    let first = scan.next().unwrap().unwrap();
    assert_eq!(*first, fixture.nodes[0]);
    let lower = n * std::mem::size_of::<CheckpointRecord<NodeRecord>>();
    assert!(lower > 512 * 1024);
    assert!(
        used(&task, ceiling) >= lower as u64,
        "the outer array must be charged independently of empty row payloads"
    );
    assert!(
        used(&task, ceiling) < (2 * lower + 96 * 1024) as u64,
        "segment input and descriptor inventory are temporary"
    );
    drop(scan);
    drop(first);
    assert_eq!(used(&task, ceiling), 0);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_scan_memory_related_work_denial_preserves_physical_source_and_retries_same_task(
) {
    let fixture = Fixture::new(3, 257 * 1024 + 3);
    let before = std::fs::read(&fixture.path).unwrap();
    let ceiling = 32 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let held = task.reserve_working_memory(ceiling - 64).unwrap();
    let before_used = used(&task, ceiling);
    let error = fixture.check(&work).unwrap_err();
    assert!(matches!(
        error,
        CanonicalSegmentError::Work(CheckpointWorkError::Memory(
            RuntimeMemoryError::ReservationExceeded { .. }
        ))
    ));
    assert!(!fixture.reader.is_poisoned());
    assert_eq!(used(&task, ceiling), before_used);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(std::fs::read(&fixture.path).unwrap(), before);
    drop(held);
    fixture.check(&work).unwrap();
    assert_eq!(used(&task, ceiling), 0);
    assert!(!fixture.reader.is_poisoned());
    assert_eq!(std::fs::read(&fixture.path).unwrap(), before);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
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
fn checkpoint_units_scan_memory_related_every_actual_cpu_and_io_cut_releases_before_full_retry() {
    let fixture = Fixture::new(3, 23);
    let before = std::fs::read(&fixture.path).unwrap();
    let ceiling = 8 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let local = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    });
    let baseline = Arc::new(CheckpointWorkProbe::default());
    fixture
        .check(&observed_context(&task, &local, baseline.clone()))
        .unwrap();
    let cpu = baseline.completed.load(Ordering::SeqCst);
    let io = baseline.io_waves.load(Ordering::SeqCst);
    assert!(cpu > 100);
    assert!(io >= 4);
    assert_eq!(baseline.peak_units.load(Ordering::SeqCst), 1);
    baseline.assert_released(&local);
    assert_eq!(used(&task, ceiling), 0);
    for is_io in [false, true] {
        for cut in 1..=if is_io { io } else { cpu } {
            let attempt = task.child();
            let probe = Arc::new(CheckpointWorkProbe {
                cancellation: attempt.cancellation().clone(),
                ..CheckpointWorkProbe::default()
            });
            if is_io {
                probe.cancel_on_io_wave.store(cut, Ordering::SeqCst);
            } else {
                probe.cancel_after.store(cut, Ordering::SeqCst);
            }
            // Each cut includes the same cold captured-handle verification
            // boundaries measured by the baseline, even after earlier retries.
            let reader = fixture.fresh_reader();
            let error = match fixture.check_reader(&reader, &observed_context(&attempt, &local, probe.clone())) {
                Err(error) => error,
                Ok(()) => panic!("expected cancellation cut={cut}, io={is_io}, cpu_total={cpu}, io_total={io}, actual_cpu={}, actual_io={}", probe.completed.load(Ordering::SeqCst), probe.io_waves.load(Ordering::SeqCst)),
            };
            assert!(
                matches!(
                    error,
                    CanonicalSegmentError::Work(
                        CheckpointWorkError::Stopped(_)
                            | CheckpointWorkError::Io(RuntimeIoWaveError::Stopped(_))
                    )
                ),
                "cut={cut}, io={is_io}, error={error}"
            );
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
            assert_eq!(used(&task, ceiling), 0, "cut={cut}, io={is_io}");
            assert_eq!(governor.snapshot().active_background_io_slots, 0);
            assert!(!fixture.reader.is_poisoned());
            assert!(!reader.is_poisoned());
        }
    }
    local.set_telemetry_sink(None);
    fixture
        .check(&CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone()))
        .unwrap();
    assert_eq!(used(&task, ceiling), 0);
    assert_eq!(std::fs::read(&fixture.path).unwrap(), before);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_scan_memory_related_overlay_replacement_tombstones_keep_full_record_parity() {
    use crate::graph_overlay::CheckpointOverlayIterator;
    let fixture = Fixture::new(3, 23);
    let mut delta = BTreeMap::new();
    let mut replacement = fixture.nodes[1].clone();
    replacement
        .properties
        .insert("revision".into(), Value::Int(2));
    delta.insert(replacement.id, replacement.clone());
    let mut inserted = fixture.nodes[0].clone();
    inserted.id = NodeId(99);
    delta.insert(inserted.id, inserted.clone());
    let tombstones = BTreeSet::from([fixture.nodes[0].id]);
    let work = CheckpointWorkContext::default();
    let mut scan =
        CheckpointOverlayIterator::new(Some(&fixture.reader), delta.values(), &tombstones, &work)
            .checkpoint_steps();
    assert!(scan.next().unwrap().unwrap().is_none());
    for expected in [replacement, fixture.nodes[2].clone(), inserted] {
        let record = scan.next().unwrap().unwrap().unwrap();
        assert_eq!(*record, expected);
    }
    assert!(scan.next().is_none());
    let mut delta = BTreeMap::new();
    let mut replacement = fixture.relationships[1].clone();
    replacement.source = NodeId(99);
    delta.insert(replacement.id, replacement.clone());
    let tombstones = BTreeSet::from([fixture.relationships[0].id]);
    let actual =
        CheckpointOverlayIterator::new(Some(&fixture.reader), delta.values(), &tombstones, &work)
            .collect::<hawdb_core::Result<Vec<_>>>()
            .unwrap();
    assert_eq!(actual, vec![replacement, fixture.relationships[2].clone()]);
}

#[test]
fn checkpoint_units_scan_memory_related_physical_corruption_precedes_replacement_and_tombstones() {
    use crate::graph_overlay::CheckpointOverlayIterator;
    let fixture = Fixture::new(3, 23);
    // Warm serving caches before corrupting the captured artifact. Checkpoint
    // scans must verify physical bytes rather than reuse the serving cache.
    assert_eq!(
        fixture
            .reader
            .get_node(fixture.nodes[0].id)
            .unwrap()
            .unwrap(),
        fixture.nodes[0]
    );
    let delta = fixture
        .nodes
        .iter()
        .cloned()
        .map(|n| (n.id, n))
        .collect::<BTreeMap<_, _>>();
    let tombstones = fixture.nodes.iter().map(|n| n.id).collect::<BTreeSet<_>>();
    let mut bytes = std::fs::read(&fixture.path).unwrap();
    bytes[ARTIFACT_HEADER.len() + 8 + 48] ^= 0x31;
    std::fs::write(&fixture.path, bytes).unwrap();
    let mut scan = CheckpointOverlayIterator::new(
        Some(&fixture.reader),
        delta.values(),
        &tombstones,
        &CheckpointWorkContext::default(),
    )
    .checkpoint_steps();
    assert!(matches!(
        scan.next().unwrap(),
        Err(hawdb_core::HawDBError::StorageIntegrity(_))
    ));
    assert!(fixture.reader.is_poisoned());
}

#[test]
fn checkpoint_units_scan_memory_related_segment_denial_precedes_any_unadmitted_large_input() {
    let fixture = Fixture::new(3, 257 * 1024 + 3);
    let ceiling = 128 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let observation = crate::test_allocator::AllocationObservation::start();
    let error = fixture.check(&work).unwrap_err();
    let allocations = observation.finish();
    assert!(matches!(
        error,
        CanonicalSegmentError::Work(CheckpointWorkError::Memory(
            RuntimeMemoryError::ReservationExceeded { .. }
        ))
    ));
    assert_eq!(
        allocations, 0,
        "physical segment admission must precede its large input allocation"
    );
    assert!(!fixture.reader.is_poisoned());
    assert_eq!(used(&task, ceiling), 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
