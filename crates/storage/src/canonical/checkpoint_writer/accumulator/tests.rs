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
use hawdb_core::{RuntimeMemoryError, RuntimeTaskContext};
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

fn local() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    })
}

fn used(task: &RuntimeTaskContext, ceiling: u64) -> u64 {
    match task.reserve_working_memory(ceiling) {
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes, ..
        }) => ceiling - available_bytes,
        other => panic!("full-ceiling concrete lease probe failed: {other:?}"),
    }
}

fn accumulator(work: CheckpointWorkContext, kind: CanonicalSegmentKind) -> Accumulator {
    Accumulator::new(
        kind,
        ManifestGeneration(47),
        1,
        CanonicalSegmentConfig::default(),
        work,
        true,
    )
}

fn node() -> NodeRecord {
    NodeRecord {
        id: NodeId(17),
        labels: BTreeSet::from([LabelId(7), LabelId(9)]),
        properties: BTreeMap::from([
            ("number".into(), Value::Int(19)),
            (
                "nested".into(),
                Value::List(vec![Value::Null, Value::Bool(true)]),
            ),
        ]),
    }
}

fn fill(
    work: &CheckpointWorkContext,
    payload: &[u8],
) -> Result<Accumulator, CanonicalSegmentError> {
    let mut output = accumulator(work.clone(), CanonicalSegmentKind::Relationships);
    for id in 1..=17 {
        output.push(id, payload, Some((id * 3, u64::MAX - id)))?;
    }
    output.add_node_properties(&node(), Some(work))?;
    Ok(output)
}

fn compare(actual: &SegmentAccumulator, expected: &SegmentAccumulator) {
    assert_eq!(actual.records, expected.records);
    assert_eq!(actual.record_count, expected.record_count);
    assert_eq!(actual.min_record_id, expected.min_record_id);
    assert_eq!(actual.max_record_id, expected.max_record_id);
    assert_eq!(actual.source_endpoint_keys, expected.source_endpoint_keys);
    assert_eq!(actual.target_endpoint_keys, expected.target_endpoint_keys);
    assert_eq!(actual.node_property_keys, expected.node_property_keys);
}

fn reference(payload: &[u8]) -> SegmentAccumulator {
    let mut output = SegmentAccumulator::new(
        CanonicalSegmentKind::Relationships,
        ManifestGeneration(47),
        1,
        CanonicalSegmentConfig::default(),
    );
    for id in 1..=17 {
        output
            .push(id, payload, Some((id * 3, u64::MAX - id)))
            .unwrap();
    }
    output.add_node_properties(&node(), None).unwrap();
    output
}

#[test]
fn checkpoint_units_canonical_accumulator_memory_related_wire_arrays_and_retained_lifetime() {
    let ceiling = 32 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let local = local();
    let probe = Arc::new(CheckpointWorkProbe::default());
    local.set_telemetry_sink(Some(probe.clone()));
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    for count in [0, 1, 65535, 65536, 65537, 257 * 1024 + 3] {
        let payload = vec![0x9f; count];
        let expected = reference(&payload);
        let actual = fill(&work, &payload).unwrap();
        compare(&actual, &expected);
        let capacity_bytes = actual.records.capacity()
            + 8 * (actual.source_endpoint_keys.capacity()
                + actual.target_endpoint_keys.capacity()
                + actual.node_property_keys.capacity());
        assert!(used(&task, ceiling) >= capacity_bytes as u64);
        assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
        assert_eq!(probe.io_waves.load(Ordering::SeqCst), 0);
        probe.assert_released(&local);
        drop(actual);
        assert_eq!(used(&task, ceiling), 0);
    }
    let actual = fill(&work, &[0x9f; 19]).unwrap();
    let retained = used(&task, ceiling);
    assert!(retained > actual.records.capacity() as u64);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    drop(actual);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_canonical_accumulator_memory_related_every_cpu_cut_and_complete_retry() {
    let ceiling = 4 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let local = local();
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    let payload = vec![0x9f; 65537];
    let expected = reference(&payload);
    let baseline = Arc::new(CheckpointWorkProbe::default());
    local.set_telemetry_sink(Some(baseline.clone()));
    let actual = fill(&work, &payload).unwrap();
    compare(&actual, &expected);
    drop(actual);
    let total = baseline.completed.load(Ordering::SeqCst);
    assert!(total > 64);
    assert_eq!(baseline.peak_units.load(Ordering::SeqCst), 1);
    baseline.assert_released(&local);
    assert_eq!(used(&task, ceiling), 0);
    for cut in 1..=total {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(cut, Ordering::SeqCst);
        local.set_telemetry_sink(Some(probe.clone()));
        let stopped = CheckpointWorkContext::new(permit.bind_task_context(
            RuntimeTaskContext::without_deadline(probe.cancellation.clone()),
        ))
        .with_scheduler(local.clone());
        assert!(matches!(
            stopped.classify(|work| fill(work, &payload)),
            Err(CheckpointOperationError::Work(
                CheckpointWorkError::Stopped(_)
            ))
        ));
        assert_eq!(probe.completed.load(Ordering::SeqCst), cut);
        assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
        probe.assert_released(&local);
        drop(stopped);
        assert_eq!(used(&task, ceiling), 0);
        local.set_telemetry_sink(None);
        let actual = fill(&work, &payload).unwrap();
        compare(&actual, &expected);
        drop(actual);
        assert_eq!(used(&task, ceiling), 0);
    }
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_canonical_accumulator_memory_related_replacement_overlap_denial_and_retry() {
    let ceiling = 64 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let local = local();
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    let mut actual = accumulator(work.clone(), CanonicalSegmentKind::Nodes);
    actual.push(1, &[0x9f; 8], None).unwrap();
    assert_eq!(actual.records.capacity(), 20);
    let initial = used(&task, ceiling);
    let next_charge = initial + 20;
    let blocker = task
        .reserve_working_memory(ceiling - initial - next_charge + 1)
        .unwrap();
    let before = actual.records.clone();
    assert!(matches!(
        actual.push(2, &[0x9f; 8], None),
        Err(CanonicalSegmentError::Work(CheckpointWorkError::Memory(
            RuntimeMemoryError::ReservationExceeded { .. }
        )))
    ));
    assert_eq!(actual.records, before);
    assert_eq!(actual.record_count, 1);
    drop(blocker);
    assert_eq!(used(&task, ceiling), initial);
    actual.push(2, &[0x9f; 8], None).unwrap();
    assert_eq!(actual.records.capacity(), 40);
    assert_eq!(actual.record_count, 2);
    assert_eq!(actual.records.len(), 40);
    drop(actual);
    assert_eq!(used(&task, ceiling), 0);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_canonical_accumulator_memory_related_validation_before_capacity_mutation() {
    let ceiling = 64 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let mut actual = accumulator(work.clone(), CanonicalSegmentKind::Nodes);
    actual.push(17, &[0x9f; 8], None).unwrap();
    let before = actual.records.clone();
    let retained = used(&task, ceiling);
    assert!(matches!(
        actual.push(16, &[0x9f; 8], None),
        Err(CanonicalSegmentError::Corrupt(_))
    ));
    actual.segment.config.max_record_bytes = NonZeroU64::new(19).unwrap();
    assert!(matches!(
        actual.push(18, &[0x9f; 8], None),
        Err(CanonicalSegmentError::RecordTooLarge { .. })
    ));
    assert_eq!(actual.records, before);
    assert_eq!(used(&task, ceiling), retained);
    actual.segment.config.max_record_bytes = NonZeroU64::new(20).unwrap();
    actual.segment.record_count = u32::MAX;
    assert!(matches!(
        actual.push(18, &[0x9f; 8], None),
        Err(CanonicalSegmentError::Corrupt(_))
    ));
    assert_eq!(actual.records, before);
    assert_eq!(used(&task, ceiling), retained);
    drop(actual);
    assert_eq!(used(&task, ceiling), 0);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_canonical_accumulator_memory_related_each_lookup_array_denies_before_allocation(
) {
    let ceiling = 64 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local());
    let charge_probe = task.reserve_working_memory(1).unwrap();
    let permit_charge = used(&task, ceiling) - 1;
    drop(charge_probe);
    for class in 0..3 {
        let mut actual = accumulator(work.clone(), CanonicalSegmentKind::Relationships);
        actual.push(1, &[0x9f; 100], None).unwrap();
        actual.push(2, &[], None).unwrap();
        if class == 1 {
            // Leave one admitted source slot available, so denial exercises
            // the target array rather than short-circuiting at the source.
            actual
                .with_memory(|segment, context| {
                    context
                        .push(&mut segment.source_endpoint_keys, 0)
                        .map_err(source)
                })
                .unwrap();
            actual.segment.source_endpoint_keys.clear();
        }
        let retained = used(&task, ceiling);
        let blocker = task
            .reserve_working_memory(ceiling - retained - permit_charge)
            .unwrap();
        let result = if class == 2 {
            actual.add_node_properties(&node(), Some(&work))
        } else {
            actual.push(3, &[], Some((7, 9)))
        };
        assert!(matches!(
            result,
            Err(CanonicalSegmentError::Work(CheckpointWorkError::Memory(
                RuntimeMemoryError::ReservationExceeded { .. }
            )))
        ));
        assert_eq!(actual.target_endpoint_keys.capacity(), 0);
        assert_eq!(actual.node_property_keys.capacity(), 0);
        if class == 0 {
            assert_eq!(actual.source_endpoint_keys.capacity(), 0);
        }
        // A failed multi-array push abandons its workspace; retry from the
        // full immutable input, since bytes may precede endpoint admission.
        drop(actual);
        drop(blocker);
        assert_eq!(used(&task, ceiling), 0);
        let retry = fill(&work, &[0x9f; 100]).unwrap();
        compare(&retry, &reference(&[0x9f; 100]));
        drop(retry);
        assert_eq!(used(&task, ceiling), 0);
    }
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_canonical_accumulator_memory_related_payload_and_replacement_copy_units_are_bounded(
) {
    let ceiling = 4 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let local = local();
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    let mut small_push = 0;
    let mut small_copy = 0;
    for count in [1usize, 65535, 65536, 65537, 257 * 1024 + 3] {
        let probe = Arc::new(CheckpointWorkProbe::default());
        local.set_telemetry_sink(Some(probe.clone()));
        let payload = vec![0x9f; count];
        let mut actual = accumulator(work.clone(), CanonicalSegmentKind::Nodes);
        actual.push(1, &payload, None).unwrap();
        let push_units = probe.completed.load(Ordering::SeqCst);
        actual.push(2, &[], None).unwrap();
        let copy_units = probe.completed.load(Ordering::SeqCst) - push_units;
        if count == 1 {
            small_push = push_units;
            small_copy = copy_units;
        }
        assert!(push_units >= small_push + count.div_ceil(64 * 1024) - 1);
        assert!(copy_units >= small_copy + (count + 12).div_ceil(64 * 1024) - 1);
        let mut expected = SegmentAccumulator::new(
            CanonicalSegmentKind::Nodes,
            ManifestGeneration(47),
            1,
            CanonicalSegmentConfig::default(),
        );
        expected.push(1, &payload, None).unwrap();
        expected.push(2, &[], None).unwrap();
        compare(&actual, &expected);
        assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
        probe.assert_released(&local);
        drop(actual);
        assert_eq!(used(&task, ceiling), 0);
    }
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_canonical_accumulator_memory_related_full_writer_arrays_files_and_complete_readback(
) {
    use crate::property_spill::{PersistentPropertySpillDescriptorTree, PropertySpillConfig};

    let root = std::env::temp_dir().join(format!(
        "hawdb-accumulator-full-writer-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    std::fs::create_dir(&root).unwrap();
    let nodes: Vec<_> = (1..=257)
        .map(|id| NodeRecord {
            id: NodeId(id),
            labels: BTreeSet::from([LabelId(7), LabelId(9)]),
            properties: BTreeMap::from([("number".into(), Value::Int(id as i64))]),
        })
        .collect();
    let relationships: Vec<_> = (1..=257)
        .map(|id| RelRecord {
            id: RelId(id),
            source: NodeId(id),
            target: NodeId(258 - id),
            rel_type: RelTypeId(3),
            properties: BTreeMap::from([("number".into(), Value::Int(id as i64))]),
        })
        .collect();
    let spill_config = PropertySpillConfig {
        spill_threshold_bytes: NonZeroU64::MIN,
        target_block_bytes: NonZeroU64::new(256).unwrap(),
        ..Default::default()
    };
    let make_tree = |directory: &Path| {
        PersistentPropertySpillDescriptorTree::new(
            GraphDescriptorTreePaths::new(
                directory.join("spill.pages.hawdb"),
                directory.join("spill.root.hawdb"),
            ),
            GraphDescriptorTreeBuildConfig::default(),
        )
    };
    let config = CanonicalSegmentConfig {
        target_segment_bytes: NonZeroU64::new(4096).unwrap(),
        ..Default::default()
    };
    let ceiling = 4 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let mut results = Vec::new();
    for name in ["ordinary", "admitted"] {
        let directory = root.join(name);
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("canonical.hawdb");
        let spill = directory.join("properties.hawdb");
        let options = PropertySpillWriteOptions {
            artifact_path: &spill,
            source_commit_epoch: 47,
            config: spill_config,
            descriptor_tree: make_tree(&directory),
        };
        let writer = CanonicalSegmentWriter::new(config).with_work_context(work.clone());
        let (manifest, output) = if name == "ordinary" {
            writer
                .write_borrowed_steps_owned(
                    &path,
                    ManifestGeneration(47),
                    nodes.iter().map(|node| Ok(Some(node))),
                    relationships.iter().map(|rel| Ok(Some(rel))),
                    options,
                )
                .unwrap()
        } else {
            writer
                .write_checkpoint_steps(
                    &path,
                    ManifestGeneration(47),
                    nodes.iter().map(|node| Ok(Some(node))),
                    relationships.iter().map(|rel| Ok(Some(rel))),
                    options,
                )
                .unwrap()
        };
        assert_eq!(output.manifest.value_count, 514);
        assert_eq!(manifest.manifest.node_count, 257);
        assert_eq!(manifest.manifest.relationship_count, 257);
        let cache = Arc::new(SegmentCache::new(0));
        let spills = PropertySpillReader::open(
            &spill,
            output.manifest.clone(),
            make_tree(&directory),
            cache.clone(),
            StoreId(47),
            NonZeroU64::new(32 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        let reader = CanonicalSegmentReader::open_with_property_spills(
            &path,
            manifest.manifest.clone(),
            cache,
            StoreId(47),
            NonZeroU64::new(32 * 1024 * 1024).unwrap(),
            spills,
        )
        .unwrap();
        assert_eq!(
            reader
                .node_records()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            nodes
        );
        assert_eq!(
            reader
                .relationship_records()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            relationships
        );
        assert_eq!(governor.snapshot().active_background_io_slots, 0);
        results.push((
            std::fs::read(&path).unwrap(),
            std::fs::read(&spill).unwrap(),
            manifest.manifest.encode().unwrap(),
            output.manifest.encode().unwrap(),
        ));
        drop(reader);
        drop(manifest);
        assert_eq!(used(&task, ceiling), 0);
    }
    assert_eq!(results[0], results[1]);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    std::fs::remove_dir_all(root).unwrap();
}
