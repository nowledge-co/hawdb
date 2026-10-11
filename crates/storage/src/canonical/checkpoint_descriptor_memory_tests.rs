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
use crate::background::CheckpointOperationError;
use hawdb_core::{RuntimeMemoryError, RuntimeTaskContext};
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
};
use std::num::NonZeroUsize;

const ROWS: u64 = 65537;
const CEILING: u64 = 32 * 1024 * 1024;

struct Fixture {
    root: PathBuf,
    payload: Vec<u8>,
    expected: Vec<u8>,
    descriptor: CanonicalSegmentDescriptor,
}

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "hawdb-canonical-descriptor-memory-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        std::fs::create_dir(&root).unwrap();
        let rel = RelRecord {
            id: RelId(1),
            source: NodeId(17),
            target: NodeId(29),
            rel_type: RelTypeId(11),
            properties: BTreeMap::new(),
        };
        let payload = encode_relationship(&rel).unwrap();
        let mut source = SegmentAccumulator::new(
            CanonicalSegmentKind::Relationships,
            ManifestGeneration(47),
            1,
            CanonicalSegmentConfig::default(),
        );
        for id in 1..=ROWS {
            source.push(id, &payload, Some((17, 29))).unwrap();
        }
        let path = root.join("ordinary.hawdb");
        let mut file = File::create(&path).unwrap();
        let mut digest = IntegrityHasher::new();
        write_hashed(&mut file, &mut digest, ARTIFACT_HEADER).unwrap();
        write_hashed(&mut file, &mut digest, &47u64.to_le_bytes()).unwrap();
        let descriptor = source.flush(&mut file, &mut digest, 24).unwrap();
        file.sync_all().unwrap();
        drop(file);
        let expected = std::fs::read(&path).unwrap();
        let mut count = 0;
        decode_segment_records(
            &expected[24..],
            ManifestGeneration(47),
            &descriptor,
            |id, bytes| {
                let actual = decode_relationship(id, bytes)?;
                assert_eq!(actual.id, RelId(count + 1));
                assert_eq!(actual.source, rel.source);
                assert_eq!(actual.target, rel.target);
                assert_eq!(actual.rel_type, rel.rel_type);
                assert_eq!(actual.properties, rel.properties);
                count += 1;
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(count, ROWS);
        assert!(descriptor.source_endpoint_bloom.words.len() * 8 > 64 * 1024);
        assert!(descriptor.target_endpoint_bloom.words.len() * 8 > 64 * 1024);
        Self {
            root,
            payload,
            expected,
            descriptor,
        }
    }

    fn accumulator(
        &self,
        work: &CheckpointWorkContext,
    ) -> checkpoint_writer::accumulator::Accumulator {
        let mut source = checkpoint_writer::accumulator::Accumulator::new(
            CanonicalSegmentKind::Relationships,
            ManifestGeneration(47),
            1,
            CanonicalSegmentConfig::default(),
            work.clone(),
            true,
        );
        for id in 1..=ROWS {
            source.push(id, &self.payload, Some((17, 29))).unwrap();
        }
        source
    }

    fn file(&self, name: &str) -> (File, IntegrityHasher) {
        let mut file = File::create(self.root.join(name)).unwrap();
        let mut digest = IntegrityHasher::new();
        write_hashed(&mut file, &mut digest, ARTIFACT_HEADER).unwrap();
        write_hashed(&mut file, &mut digest, &47u64.to_le_bytes()).unwrap();
        (file, digest)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.root).unwrap();
    }
}

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

fn used(task: &RuntimeTaskContext) -> u64 {
    match task.reserve_working_memory(CEILING) {
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes, ..
        }) => CEILING - available_bytes,
        other => panic!("full-ceiling concrete lease probe failed: {other:?}"),
    }
}

fn local() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    })
}

#[test]
fn checkpoint_units_canonical_descriptor_memory_flush_denies_bloom_before_large_allocation() {
    // The complete ordinary wire/full-read reference and admitted source
    // accumulation precede observation. Denial must include the source/Bloom
    // overlap, even though source data will be destroyed after the flush.
    let fixture = Fixture::new();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local());
    let charge_probe = task.reserve_working_memory(1).unwrap();
    let permit_charge = used(&task) - 1;
    drop(charge_probe);
    let source = fixture.accumulator(&work);
    let retained = used(&task);
    let blocker = task
        .reserve_working_memory(CEILING - retained - permit_charge)
        .unwrap();
    let (mut file, mut digest) = fixture.file("denied.hawdb");
    let original_digest = digest.clone().finish();
    let observation = crate::test_allocator::AllocationObservation::start();
    let result = work
        .classify(|work| source.flush_with_work_context(&mut file, &mut digest, 24, Some(work)));
    let allocations = observation.finish();
    assert!(
        matches!(
            &result,
            Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
                RuntimeMemoryError::ReservationExceeded { .. }
            )))
        ),
        "actual admitted segment flush must deny Bloom capacity before allocating; observed large allocations={allocations}"
    );
    assert_eq!(allocations, 0);
    assert_eq!(digest.finish(), original_digest);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(result);
    drop(file);
    drop(blocker);
    assert_eq!(used(&task), 0);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_canonical_descriptor_memory_returned_blooms_retain_admission_after_source_and_worker_close(
) {
    let fixture = Fixture::new();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local());
    let source = fixture.accumulator(&work);
    let (mut file, mut digest) = fixture.file("retained.hawdb");
    let descriptor = source
        .flush_with_work_context(&mut file, &mut digest, 24, Some(&work))
        .unwrap();
    file.sync_all().unwrap();
    drop(file);
    assert_eq!(
        std::fs::read(fixture.root.join("retained.hawdb")).unwrap(),
        fixture.expected
    );
    assert_eq!(
        descriptor.encode_descriptor_tree_value().unwrap(),
        fixture.descriptor.encode_descriptor_tree_value().unwrap()
    );
    let bloom_bytes = 8
        * (descriptor.source_endpoint_bloom.words.len()
            + descriptor.target_endpoint_bloom.words.len()
            + descriptor.node_property_bloom.words.len());
    assert!(used(&task) >= bloom_bytes as u64, "returned Bloom bytes must retain their own admission after the consuming flush destroys segment input");
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, CEILING);
    assert!(descriptor.source_endpoint_bloom.might_contain(17));
    assert!(descriptor.target_endpoint_bloom.might_contain(29));
    drop(descriptor);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
