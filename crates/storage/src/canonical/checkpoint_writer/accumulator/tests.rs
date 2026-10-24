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
    assert_eq!(actual.node_label_keys, expected.node_label_keys);
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
                + actual.node_property_keys.capacity()
                + actual.node_label_keys.capacity());
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

#[test]
fn retained_flush_keeps_records_and_leases_after_prewrite_cancellation() {
    let ceiling = 128 * 1024;
    let resources = governor(ceiling);
    let permit = resources
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let local = local();
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    let payload = vec![42; 1024];
    let mut actual = fill(&work, &payload).unwrap();
    let expected = reference(&payload);
    let retained = used(&task, ceiling);
    assert!(retained > 0);
    let root = std::env::temp_dir().join(format!(
        "hawdb-retained-canonical-flush-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("actual.hawdb");
    let mut file = File::create(&path).unwrap();
    file.write_all(b"prefix").unwrap();
    let mut digest = IntegrityHasher::new();
    digest.update(b"prefix");
    let covered_digest = digest.clone().finish();
    let probe = Arc::new(CheckpointWorkProbe::default());
    probe.cancel_after.store(1, Ordering::SeqCst);
    local.set_telemetry_sink(Some(probe.clone()));
    let stopped = CheckpointWorkContext::new(permit.bind_task_context(
        RuntimeTaskContext::without_deadline(probe.cancellation.clone()),
    ))
    .with_scheduler(local.clone());
    assert!(matches!(
        actual.flush_retained_with_work_context(&mut file, &mut digest, 6, &stopped),
        Err(CanonicalSegmentError::Work(CheckpointWorkError::Stopped(_)))
    ));
    probe.assert_released(&local);
    compare(&actual, &expected);
    assert_eq!(used(&task, ceiling), retained);
    assert_eq!(digest.clone().finish(), covered_digest);
    assert_eq!(std::fs::read(&path).unwrap(), b"prefix");
    local.set_telemetry_sink(None);
    let descriptor = actual
        .flush_retained_with_work_context(&mut file, &mut digest, 6, &work)
        .unwrap();
    let reference_path = root.join("reference.hawdb");
    let mut reference_file = File::create(&reference_path).unwrap();
    reference_file.write_all(b"prefix").unwrap();
    let mut reference_digest = IntegrityHasher::new();
    reference_digest.update(b"prefix");
    let reference_descriptor = expected
        .flush(&mut reference_file, &mut reference_digest, 6)
        .unwrap();
    assert_eq!(
        descriptor.encode_descriptor_tree_value().unwrap(),
        reference_descriptor.encode_descriptor_tree_value().unwrap()
    );
    assert_eq!(digest.finish(), reference_digest.finish());
    assert_eq!(
        std::fs::read(&path).unwrap(),
        std::fs::read(&reference_path).unwrap()
    );
    drop(descriptor);
    assert_eq!(used(&task, ceiling), retained);
    drop(actual);
    assert_eq!(used(&task, ceiling), 0);
    drop(file);
    drop(reference_file);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn retained_flush_keeps_complete_blocks_at_every_actual_io_cut() {
    let ceiling = 2 * 1024 * 1024;
    let resources = governor(ceiling);
    let permit = resources
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let local = local();
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    let payload = vec![37; 16 * 1024];
    let root = std::env::temp_dir().join(format!(
        "hawdb-retained-flush-io-cuts-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    std::fs::create_dir(&root).unwrap();
    let reference_path = root.join("reference.hawdb");
    let mut reference_file = File::create(&reference_path).unwrap();
    reference_file.write_all(b"prefix").unwrap();
    let mut reference_digest = IntegrityHasher::new();
    reference_digest.update(b"prefix");
    let baseline = Arc::new(CheckpointWorkProbe::default());
    let baseline_work =
        CheckpointWorkContext::new(task.clone().with_io_wave_controller(baseline.clone()))
            .with_scheduler(local.clone());
    let mut input = fill(&work, &payload).unwrap();
    let reference_descriptor = input
        .flush_retained_with_work_context(
            &mut reference_file,
            &mut reference_digest,
            6,
            &baseline_work,
        )
        .unwrap();
    let descriptor_bytes = reference_descriptor.encode_descriptor_tree_value().unwrap();
    let digest = reference_digest.finish();
    let io_cuts = baseline.io_waves.load(Ordering::SeqCst);
    let bytes = std::fs::read(&reference_path).unwrap();
    assert!(io_cuts >= 6, "exercise several physical payload blocks");
    drop(input);
    drop(reference_descriptor);
    assert_eq!(used(&task, ceiling), 0);
    let mut partial_cuts = 0;
    for cut in 1..=io_cuts {
        let path = root.join(format!("cut-{cut}.hawdb"));
        let mut file = File::create(&path).unwrap();
        file.write_all(b"prefix").unwrap();
        let mut covered = IntegrityHasher::new();
        covered.update(b"prefix");
        let covered_digest = covered.clone().finish();
        let mut input = fill(&work, &payload).unwrap();
        let retained = used(&task, ceiling);
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_on_io_wave.store(cut, Ordering::SeqCst);
        local.set_telemetry_sink(Some(probe.clone()));
        let stopped = CheckpointWorkContext::new(
            permit
                .bind_task_context(RuntimeTaskContext::without_deadline(
                    probe.cancellation.clone(),
                ))
                .with_io_wave_controller(probe.clone()),
        )
        .with_scheduler(local.clone());
        assert!(matches!(
            input.flush_retained_with_work_context(&mut file, &mut covered, 6, &stopped),
            Err(CanonicalSegmentError::Work(_))
        ));
        assert!(probe.cancellation.is_cancelled());
        probe.assert_released(&local);
        assert_eq!(covered.clone().finish(), covered_digest);
        assert_eq!(used(&task, ceiling), retained);
        let partial = std::fs::read(&path).unwrap();
        assert!(bytes.starts_with(&partial));
        if partial.len() > 6 {
            partial_cuts += 1;
        }
        local.set_telemetry_sink(None);
        if input.pending_flush.is_some() {
            assert!(matches!(
                input.push(99, &payload, None),
                Err(CanonicalSegmentError::Corrupt(_))
            ));
            assert!(matches!(
                input.add_node_properties(&node(), Some(&work)),
                Err(CanonicalSegmentError::Corrupt(_))
            ));
            assert!(matches!(
                input.flush_retained_with_work_context(&mut file, &mut covered, 7, &work),
                Err(CanonicalSegmentError::Corrupt(_))
            ));
            assert_eq!(std::fs::read(&path).unwrap(), partial);
        }
        let actual = input
            .flush_retained_with_work_context(&mut file, &mut covered, 6, &work)
            .unwrap();
        assert_eq!(
            actual.encode_descriptor_tree_value().unwrap(),
            descriptor_bytes
        );
        assert_eq!(covered.finish(), digest);
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        assert!(input.pending_flush.is_none());
        drop(actual);
        assert_eq!(used(&task, ceiling), retained);
        drop(input);
        assert_eq!(used(&task, ceiling), 0);
        eprintln!(
            "retained-flush actual_io_cut={cut}/{io_cuts} partial_bytes={}",
            partial.len()
        );
    }
    assert!(partial_cuts >= 4);
    drop(reference_file);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn retained_flush_completes_with_two_units_per_attempt() {
    let ceiling = 2 * 1024 * 1024;
    let resources = governor(ceiling);
    let permit = resources
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let local = local();
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    let payload = vec![31; 16 * 1024];
    let mut input = fill(&work, &payload).unwrap();
    let mut expected = reference(&payload);
    // Exercise more than two insertion chunks in the property Bloom while
    // keeping the same work quantum and working-memory ceiling.
    for _ in 0..128 {
        input.add_node_properties(&node(), Some(&work)).unwrap();
        expected.add_node_properties(&node(), None).unwrap();
    }
    let retained = used(&task, ceiling);
    let blocks = 1 + input.records.len().div_ceil(64 * 1024);
    let root = std::env::temp_dir().join(format!(
        "hawdb-retained-crc-progress-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("actual.hawdb");
    let mut file = File::create(&path).unwrap();
    file.write_all(b"prefix").unwrap();
    let mut digest = IntegrityHasher::new();
    digest.update(b"prefix");
    let initial = digest.clone().finish();
    let mut completed = 0;
    for attempt in 1..=blocks {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(2, Ordering::SeqCst);
        local.set_telemetry_sink(Some(probe.clone()));
        let stopped = CheckpointWorkContext::new(permit.bind_task_context(
            RuntimeTaskContext::without_deadline(probe.cancellation.clone()),
        ))
        .with_scheduler(local.clone());
        assert!(matches!(
            input.flush_retained_with_work_context(&mut file, &mut digest, 6, &stopped),
            Err(CanonicalSegmentError::Work(CheckpointWorkError::Stopped(_)))
        ));
        probe.assert_released(&local);
        assert_eq!(probe.completed.load(Ordering::SeqCst), 2);
        assert!(input.flush_progress.completed_blocks > completed);
        completed = input.flush_progress.completed_blocks;
        assert_eq!(used(&task, ceiling), retained);
        assert_eq!(digest.clone().finish(), initial);
        assert_eq!(std::fs::read(&path).unwrap(), b"prefix");
        eprintln!("retained-crc attempt={attempt} completed_blocks={completed}/{blocks}");
        if completed == blocks {
            break;
        }
    }
    assert_eq!(completed, blocks);
    let mut written = 0;
    for attempt in 1..=blocks {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(2, Ordering::SeqCst);
        local.set_telemetry_sink(Some(probe.clone()));
        let stopped = CheckpointWorkContext::new(permit.bind_task_context(
            RuntimeTaskContext::without_deadline(probe.cancellation.clone()),
        ))
        .with_scheduler(local.clone());
        assert!(matches!(
            input.flush_retained_with_work_context(&mut file, &mut digest, 6, &stopped),
            Err(CanonicalSegmentError::Work(CheckpointWorkError::Stopped(_)))
        ));
        probe.assert_released(&local);
        assert_eq!(probe.completed.load(Ordering::SeqCst), 2);
        assert!(input.flush_progress.written_blocks > written);
        written = input.flush_progress.written_blocks;
        assert!(!input.flush_progress.dirty_tail);
        assert_eq!(
            file.metadata().unwrap().len(),
            6 + input.flush_progress.written_bytes
        );
        assert_eq!(digest.clone().finish(), initial);
        assert_eq!(used(&task, ceiling), retained);
        eprintln!("retained-write attempt={attempt} completed_blocks={written}/{blocks}");
        if written == blocks {
            break;
        }
    }
    assert_eq!(written, blocks);
    local.set_telemetry_sink(None);
    let complete_file = std::fs::read(&path).unwrap();
    let mut previous = 0;
    let mut finished = None;
    for attempt in 1..=32 {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(2, Ordering::SeqCst);
        local.set_telemetry_sink(Some(probe.clone()));
        let stopped = CheckpointWorkContext::new(permit.bind_task_context(
            RuntimeTaskContext::without_deadline(probe.cancellation.clone()),
        ))
        .with_scheduler(local.clone());
        let result = input.flush_retained_with_work_context(&mut file, &mut digest, 6, &stopped);
        probe.assert_released(&local);
        assert_eq!(std::fs::read(&path).unwrap(), complete_file);
        match result {
            Ok(descriptor) => {
                finished = Some(descriptor);
                break;
            }
            Err(CanonicalSegmentError::Work(CheckpointWorkError::Stopped(_))) => {
                assert_eq!(probe.completed.load(Ordering::SeqCst), 2);
                let progress = input.flush_progress.bloom_progress_for_test();
                assert!(progress > previous);
                previous = progress;
                assert_eq!(digest.clone().finish(), initial);
                assert!(used(&task, ceiling) >= retained);
                eprintln!("retained-bloom attempt={attempt} progress={progress}");
            }
            Err(error) => panic!("uncontrolled bounded flush failure: {error:?}"),
        }
    }
    local.set_telemetry_sink(None);
    let actual = finished.expect("all flush stages must finish with the same two-unit quantum");
    let reference_path = root.join("reference.hawdb");
    let mut reference_file = File::create(&reference_path).unwrap();
    reference_file.write_all(b"prefix").unwrap();
    let mut reference_digest = IntegrityHasher::new();
    reference_digest.update(b"prefix");
    let reference_descriptor = expected
        .flush(&mut reference_file, &mut reference_digest, 6)
        .unwrap();
    assert_eq!(
        actual.encode_descriptor_tree_value().unwrap(),
        reference_descriptor.encode_descriptor_tree_value().unwrap()
    );
    assert_eq!(digest.finish(), reference_digest.finish());
    assert_eq!(
        std::fs::read(&path).unwrap(),
        std::fs::read(&reference_path).unwrap()
    );
    assert_eq!(input.flush_progress.completed_blocks, 0);
    drop(actual);
    assert_eq!(used(&task, ceiling), retained);
    drop(input);
    assert_eq!(used(&task, ceiling), 0);
    drop(file);
    drop(reference_file);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn retained_flush_partial_block_error_preserves_prior_complete_blocks() {
    let ceiling = 2 * 1024 * 1024;
    let resources = governor(ceiling);
    let permit = resources
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let local = local();
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    let payload = vec![73; 16 * 1024];
    let mut input = fill(&work, &payload).unwrap();
    let retained = used(&task, ceiling);
    let expected = reference(&payload);
    let root = std::env::temp_dir().join(format!(
        "hawdb-private-block-error-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("actual.hawdb");
    let mut file = File::create(&path).unwrap();
    file.write_all(b"prefix").unwrap();
    let mut digest = IntegrityHasher::new();
    digest.update(b"prefix");
    let initial = digest.clone().finish();
    // Header completes; the next physical write persists17payloadbytes and
    // then returns an IO error through the ordinary private-file error path.
    input.flush_progress.partial_write_probe = Some((1, 17));
    assert!(matches!(
        input.flush_retained_with_work_context(&mut file, &mut digest, 6, &work),
        Err(CanonicalSegmentError::Io(_))
    ));
    assert!(input.flush_progress.dirty_tail);
    assert_eq!(input.flush_progress.written_blocks, 1);
    let complete_bytes = input.flush_progress.written_bytes;
    assert_eq!(file.metadata().unwrap().len(), 6 + complete_bytes + 17);
    assert_eq!(digest.clone().finish(), initial);
    assert_eq!(used(&task, ceiling), retained);
    assert_eq!(local.state().running_background_operations, 0);
    eprintln!("retained-partial-block complete_bytes={complete_bytes} incomplete_bytes=17");
    let actual = input
        .flush_retained_with_work_context(&mut file, &mut digest, 6, &work)
        .unwrap();
    let reference_path = root.join("reference.hawdb");
    let mut reference_file = File::create(&reference_path).unwrap();
    reference_file.write_all(b"prefix").unwrap();
    let mut reference_digest = IntegrityHasher::new();
    reference_digest.update(b"prefix");
    let reference_descriptor = expected
        .flush(&mut reference_file, &mut reference_digest, 6)
        .unwrap();
    assert_eq!(
        actual.encode_descriptor_tree_value().unwrap(),
        reference_descriptor.encode_descriptor_tree_value().unwrap()
    );
    assert_eq!(digest.finish(), reference_digest.finish());
    assert_eq!(
        std::fs::read(&path).unwrap(),
        std::fs::read(&reference_path).unwrap()
    );
    assert!(!input.flush_progress.dirty_tail);
    assert!(input.pending_flush.is_none());
    drop(actual);
    assert_eq!(used(&task, ceiling), retained);
    drop(input);
    assert_eq!(used(&task, ceiling), 0);
    drop(file);
    drop(reference_file);
    std::fs::remove_dir_all(root).unwrap();
}

fn bounded_record_append(kind: CanonicalSegmentKind, endpoints: Option<(u64, u64)>) {
    let ceiling = 2 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let local = local();
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    let old_payload = vec![0x31; 128 * 1024];
    let payload = vec![0x97; 256 * 1024];
    let mut actual = accumulator(work.clone(), kind);
    actual
        .push(1, &old_payload, endpoints.map(|_| (3, 7)))
        .unwrap();
    let mut expected = SegmentAccumulator::new(
        kind,
        ManifestGeneration(47),
        1,
        CanonicalSegmentConfig::default(),
    );
    expected
        .push(1, &old_payload, endpoints.map(|_| (3, 7)))
        .unwrap();
    expected.push(2, &payload, endpoints).unwrap();
    let mut finished = false;
    let mut attempts = 0;
    for attempt in 1..=32 {
        attempts = attempt;
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(2, Ordering::SeqCst);
        local.set_telemetry_sink(Some(probe.clone()));
        let fresh = CheckpointWorkContext::new(permit.bind_task_context(
            RuntimeTaskContext::without_deadline(probe.cancellation.clone()),
        ))
        .with_scheduler(local.clone());
        match actual.push_retained_with_work_context(2, &payload, endpoints, &fresh) {
            Ok(()) => {
                finished = true;
                break;
            }
            Err(CanonicalSegmentError::Work(CheckpointWorkError::Stopped(_))) => {}
            other => panic!("unexpected record resume result: {other:?}"),
        }
        probe.assert_released(&local);
        assert_eq!(probe.completed.load(Ordering::SeqCst), 2);
        assert!(actual.pending_push.is_some());
        assert!(matches!(
            actual.push_retained_with_work_context(3, &payload, endpoints, &work),
            Err(CanonicalSegmentError::Corrupt(_))
        ));
    }
    assert!(
        finished,
        "record append restarted instead of making bounded progress"
    );
    assert!(attempts >= 8);
    compare(&actual, &expected);
    local.set_telemetry_sink(None);
    drop(actual);
    assert_eq!(used(&task, ceiling), 0);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn retained_record_append_retries_every_cut_without_duplicate_records_or_endpoints() {
    let ceiling = 2 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let local = local();
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    let prefix = vec![0x31; 16 * 1024];
    let payload = vec![0x97; 128 * 1024];
    let mut expected = SegmentAccumulator::new(
        CanonicalSegmentKind::Relationships,
        ManifestGeneration(47),
        1,
        CanonicalSegmentConfig::default(),
    );
    expected.push(1, &prefix, Some((3, 7))).unwrap();
    expected.push(2, &payload, Some((11, 13))).unwrap();
    let mut baseline_output = accumulator(work.clone(), CanonicalSegmentKind::Relationships);
    baseline_output.push(1, &prefix, Some((3, 7))).unwrap();
    let baseline = Arc::new(CheckpointWorkProbe::default());
    local.set_telemetry_sink(Some(baseline.clone()));
    baseline_output.push(2, &payload, Some((11, 13))).unwrap();
    let total = baseline.completed.load(Ordering::SeqCst);
    assert!(total > 10);
    drop(baseline_output);
    local.set_telemetry_sink(None);
    for cut in 1..=total {
        let mut actual = accumulator(work.clone(), CanonicalSegmentKind::Relationships);
        actual.push(1, &prefix, Some((3, 7))).unwrap();
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(cut, Ordering::SeqCst);
        local.set_telemetry_sink(Some(probe.clone()));
        let stopped = CheckpointWorkContext::new(permit.bind_task_context(
            RuntimeTaskContext::without_deadline(probe.cancellation.clone()),
        ))
        .with_scheduler(local.clone());
        assert!(
            matches!(
                actual.push_retained_with_work_context(2, &payload, Some((11, 13)), &stopped),
                Err(CanonicalSegmentError::Work(CheckpointWorkError::Stopped(_)))
            ),
            "cut={cut}"
        );
        probe.assert_released(&local);
        local.set_telemetry_sink(None);
        actual
            .push_retained_with_work_context(2, &payload, Some((11, 13)), &work)
            .unwrap();
        compare(&actual, &expected);
        assert!(actual.pending_push.is_none());
        drop(actual);
        assert_eq!(used(&task, ceiling), 0, "cut={cut}");
    }
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn retained_record_append_completes_capacity_copy_and_payload_with_two_units_per_attempt() {
    bounded_record_append(CanonicalSegmentKind::Nodes, None);
}

#[test]
fn retained_relationship_append_scans_inventory_and_copies_payload_with_two_units_per_attempt() {
    bounded_record_append(CanonicalSegmentKind::Relationships, Some((11, 13)));
}

#[test]
fn retained_node_property_batch_completes_with_fixed_budget_without_duplicate_keys() {
    let ceiling = 2 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let local = local();
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    let node = NodeRecord {
        id: NodeId(17),
        labels: BTreeSet::from([LabelId(7), LabelId(9)]),
        properties: (0..64)
            .map(|id| (format!("property-{id:04}-界"), Value::Int(id)))
            .collect(),
    };
    let mut expected = SegmentAccumulator::new(
        CanonicalSegmentKind::Nodes,
        ManifestGeneration(47),
        1,
        CanonicalSegmentConfig::default(),
    );
    expected.add_node_properties(&node, None).unwrap();
    let mut actual = accumulator(work.clone(), CanonicalSegmentKind::Nodes);
    let mut finished = false;
    for _ in 0..128 {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(24, Ordering::SeqCst);
        local.set_telemetry_sink(Some(probe.clone()));
        let fresh = CheckpointWorkContext::new(permit.bind_task_context(
            RuntimeTaskContext::without_deadline(probe.cancellation.clone()),
        ))
        .with_scheduler(local.clone());
        actual.work = fresh;
        match actual.add_node_properties(&node, None) {
            Ok(()) => {
                finished = true;
                break;
            }
            Err(CanonicalSegmentError::Work(CheckpointWorkError::Stopped(_))) => {}
            other => panic!("unexpected property resume result: {other:?}"),
        }
        probe.assert_released(&local);
        assert_eq!(probe.completed.load(Ordering::SeqCst), 24);
    }
    local.set_telemetry_sink(None);
    assert!(
        finished,
        "node property batch restarted instead of retaining progress"
    );
    compare(&actual, &expected);
    drop(actual);
    assert_eq!(used(&task, ceiling), 0);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn retained_node_property_batch_retries_every_cut_without_repeating_completed_keys() {
    let ceiling = 2 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let local = local();
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    let node = node();
    let mut expected = SegmentAccumulator::new(
        CanonicalSegmentKind::Nodes,
        ManifestGeneration(47),
        1,
        CanonicalSegmentConfig::default(),
    );
    expected.add_node_properties(&node, None).unwrap();
    expected.add_node_properties(&node, None).unwrap();
    let mut baseline_output = accumulator(work.clone(), CanonicalSegmentKind::Nodes);
    baseline_output.add_node_properties(&node, None).unwrap();
    let baseline = Arc::new(CheckpointWorkProbe::default());
    local.set_telemetry_sink(Some(baseline.clone()));
    baseline_output.add_node_properties(&node, None).unwrap();
    let total = baseline.completed.load(Ordering::SeqCst);
    assert!(total > 16);
    drop(baseline_output);
    local.set_telemetry_sink(None);
    for cut in 1..=total {
        let mut actual = accumulator(work.clone(), CanonicalSegmentKind::Nodes);
        actual.add_node_properties(&node, None).unwrap();
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(cut, Ordering::SeqCst);
        local.set_telemetry_sink(Some(probe.clone()));
        let stopped = CheckpointWorkContext::new(permit.bind_task_context(
            RuntimeTaskContext::without_deadline(probe.cancellation.clone()),
        ))
        .with_scheduler(local.clone());
        assert!(
            matches!(
                actual.add_node_properties(&node, Some(&stopped)),
                Err(CanonicalSegmentError::Work(CheckpointWorkError::Stopped(_)))
            ),
            "cut={cut}"
        );
        probe.assert_released(&local);
        local.set_telemetry_sink(None);
        if actual.pending_properties.is_some() {
            let empty = NodeRecord {
                id: node.id,
                labels: node.labels.clone(),
                properties: BTreeMap::new(),
            };
            assert!(matches!(
                actual.add_node_properties(&empty, Some(&work)),
                Err(CanonicalSegmentError::Corrupt(_))
            ));
        }
        actual.add_node_properties(&node, Some(&work)).unwrap();
        compare(&actual, &expected);
        assert!(actual.pending_properties.is_none());
        drop(actual);
        assert_eq!(used(&task, ceiling), 0, "cut={cut}");
    }
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
