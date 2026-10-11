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
use crate::property_spill::{PersistentPropertySpillDescriptorTree, PropertySpillConfig};
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

fn local() -> hawdb_qos::LocalQosScheduler {
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
        other => panic!("full-ceiling probe must include the concrete lease charge: {other:?}"),
    }
}

fn tree(directory: &Path) -> PersistentPropertySpillDescriptorTree {
    PersistentPropertySpillDescriptorTree::new(
        GraphDescriptorTreePaths::new(
            directory.join("spill-descriptors.pages.hawdb"),
            directory.join("spill-descriptors.root.hawdb"),
        ),
        GraphDescriptorTreeBuildConfig::default(),
    )
}

fn source() -> NodeRecord {
    NodeRecord {
        id: NodeId(17),
        labels: (0..4097).map(LabelId).collect(),
        properties: BTreeMap::from([
            ("binary".into(), Value::Binary(vec![0x9f; 257 * 1024 + 3])),
            ("text".into(), Value::String("界".repeat(87_723) + "🦀\0")),
        ]),
    }
}

fn reference(node: &NodeRecord) -> (Vec<u8>, Vec<String>) {
    let mut dictionary = PropertyKeyDictionary::default();
    let bytes = encode_node_with_property_spills(node, None, Some(&mut dictionary)).unwrap();
    (bytes, dictionary.into_keys().values)
}

fn attempt(
    work: &CheckpointWorkContext,
    node: &NodeRecord,
) -> Result<(CheckpointBytes, Keys), CheckpointOperationError<CanonicalSegmentError>> {
    work.classify(|work| {
        let mut dictionary = PropertyKeyDictionary::with_work_context(work.clone());
        let bytes = super::node(
            node,
            None,
            &mut dictionary,
            CanonicalSegmentConfig::default(),
        )?;
        Ok((bytes, dictionary.into_keys()))
    })
}

fn failure<T>(error: CheckpointOperationError<CanonicalSegmentError>) -> T {
    match error {
        CheckpointOperationError::Work(error) => panic!("unexpected work failure: {error:?}"),
        CheckpointOperationError::Operation(error) => panic!("unexpected codec failure: {error}"),
    }
}

#[test]
fn checkpoint_units_canonical_record_memory_related_complete_wire_and_buffer_lifetime() {
    let ceiling = 32 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let local = local();
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    let mut values = vec![
        Value::Null,
        Value::Bool(false),
        Value::Bool(true),
        Value::Int(i64::MIN),
        Value::Int(i64::MAX),
        Value::Float(-0.0),
        Value::Float(f64::from_bits(0x7ff8_0123_4567_89ab)),
        Value::Uuid(hawdb_core::Uuid::from_bytes([0x9f; 16])),
        Value::List(Vec::new()),
        Value::Map(BTreeMap::new()),
    ];
    for count in [0, 1, 65535, 65536, 65537, 257 * 1024 + 3] {
        values.push(Value::Binary(vec![0x9f; count]));
        values.push(Value::String("界".repeat(count / 3) + "🦀\0"));
    }
    values.push(Value::Map(BTreeMap::from([(
        "nested".into(),
        Value::List(vec![Value::Null, Value::Int(19)]),
    )])));
    for value in values {
        let node = NodeRecord {
            id: NodeId(17),
            labels: BTreeSet::from([LabelId(7)]),
            properties: BTreeMap::from([("payload".into(), value)]),
        };
        let (expected, names) = reference(&node);
        let (actual, keys) = attempt(&work, &node).unwrap_or_else(failure);
        assert_eq!(&*actual, expected);
        assert_eq!(keys.values, names);
        assert!(used(&task, ceiling) >= actual.len() as u64);
        drop(actual);
        drop(keys);
        assert_eq!(used(&task, ceiling), 0);
    }
    for count in [0, 1, 1023, 1024, 1025, 4097, 65_793] {
        let probe = Arc::new(CheckpointWorkProbe::default());
        local.set_telemetry_sink(Some(probe.clone()));
        let node = NodeRecord {
            id: NodeId(17),
            labels: (0..count).map(LabelId).collect(),
            properties: BTreeMap::new(),
        };
        let (expected, names) = reference(&node);
        let (actual, keys) = attempt(&work, &node).unwrap_or_else(failure);
        assert_eq!(&*actual, expected);
        assert_eq!(keys.values, names);
        assert!(
            probe.completed.load(Ordering::SeqCst)
                >= 2 * usize::try_from(count).unwrap().div_ceil(1024)
        );
        assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
        assert_eq!(probe.io_waves.load(Ordering::SeqCst), 0);
        probe.assert_released(&local);
        drop(actual);
        drop(keys);
        assert_eq!(used(&task, ceiling), 0);
    }
    local.set_telemetry_sink(None);
    let node = source();
    let rel = RelRecord {
        id: RelId(23),
        source: NodeId(u64::MAX),
        target: NodeId(0),
        rel_type: RelTypeId(u32::MAX),
        properties: node.properties.clone(),
    };
    let mut ordinary = PropertyKeyDictionary::default();
    let expected =
        encode_relationship_with_property_spills(&rel, None, Some(&mut ordinary)).unwrap();
    let mut dictionary = PropertyKeyDictionary::with_work_context(work.clone());
    let actual = super::relationship(
        &rel,
        None,
        &mut dictionary,
        CanonicalSegmentConfig::default(),
    )
    .unwrap();
    assert_eq!(&*actual, expected);
    assert_eq!(dictionary.into_keys().values, ordinary.into_keys().values);
    drop(actual);
    assert_eq!(used(&task, ceiling), 0);
    let (output, keys) = attempt(&work, &node).unwrap_or_else(failure);
    drop(keys);
    assert!(used(&task, ceiling) >= output.len() as u64);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    drop(output);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_canonical_record_memory_related_every_cpu_cut_releases_and_fully_retries() {
    let node = source();
    let (expected, names) = reference(&node);
    let ceiling = 8 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let local = local();
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    let baseline = Arc::new(CheckpointWorkProbe::default());
    local.set_telemetry_sink(Some(baseline.clone()));
    let (bytes, keys) = attempt(&work, &node).unwrap_or_else(failure);
    assert_eq!(&*bytes, expected);
    assert_eq!(keys.values, names);
    drop(bytes);
    drop(keys);
    let total = baseline.completed.load(Ordering::SeqCst);
    assert!(total > 32);
    assert_eq!(baseline.peak_units.load(Ordering::SeqCst), 1);
    assert_eq!(baseline.io_waves.load(Ordering::SeqCst), 0);
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
            attempt(&stopped, &node),
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
        let (bytes, keys) = attempt(&work, &node).unwrap_or_else(failure);
        assert_eq!(&*bytes, expected);
        assert_eq!(keys.values, names);
        drop(bytes);
        drop(keys);
        assert_eq!(used(&task, ceiling), 0);
        assert_eq!(reference(&node), (expected.clone(), names.clone()));
    }
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_canonical_record_memory_related_size_and_depth_errors_preserve_cleanup() {
    let node = source();
    let (expected, _) = reference(&node);
    let ceiling = 8 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local());
    let max = expected.len() as u64 + 11;
    let config = CanonicalSegmentConfig {
        max_record_bytes: NonZeroU64::new(max).unwrap(),
        ..Default::default()
    };
    let mut dictionary = PropertyKeyDictionary::with_work_context(work.clone());
    assert!(
        matches!(super::node(&node, None, &mut dictionary, config), Err(CanonicalSegmentError::RecordTooLarge { record_bytes, max_bytes }) if record_bytes == expected.len() as u64 + 12 && max_bytes == max)
    );
    drop(dictionary);
    assert_eq!(used(&task, ceiling), 0);
    let mut invalid = Value::Null;
    for _ in 0..MAX_VALUE_DEPTH + 2 {
        invalid = Value::List(vec![invalid]);
    }
    let node = NodeRecord {
        id: NodeId(17),
        labels: BTreeSet::new(),
        properties: BTreeMap::from([("invalid".into(), invalid)]),
    };
    let mut ordinary = PropertyKeyDictionary::default();
    let expected = encode_node_with_property_spills(&node, None, Some(&mut ordinary))
        .unwrap_err()
        .to_string();
    let error = match attempt(&work, &node) {
        Err(CheckpointOperationError::Operation(error)) => error,
        _ => panic!("invalid depth must remain an ordinary codec diagnostic"),
    };
    assert_eq!(error.to_string(), expected);
    assert_eq!(used(&task, ceiling), 0);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_canonical_record_memory_related_full_borrowed_writer_matches_ordinary_files_and_reads(
) {
    let directory = std::env::temp_dir().join(format!(
        "hawdb-record-wire-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    std::fs::create_dir(&directory).unwrap();
    let nodes = [
        source(),
        NodeRecord {
            id: NodeId(19),
            labels: BTreeSet::from([LabelId(7)]),
            properties: BTreeMap::from([(
                "inline".into(),
                Value::List((0..16).map(Value::Int).collect()),
            )]),
        },
    ];
    let rel = RelRecord {
        id: RelId(23),
        source: NodeId(17),
        target: NodeId(19),
        rel_type: RelTypeId(7),
        properties: nodes[0].properties.clone(),
    };
    let ordinary = directory.join("ordinary");
    let controlled = directory.join("controlled");
    std::fs::create_dir(&ordinary).unwrap();
    std::fs::create_dir(&controlled).unwrap();
    let ordinary_path = ordinary.join("canonical.hawdb");
    let ordinary_spill = ordinary.join("properties.hawdb");
    let _ = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
        .write_fallible_with_property_spills(
            &ordinary_path,
            ManifestGeneration(47),
            nodes.iter().cloned().map(Ok::<_, CanonicalSegmentError>),
            [Ok(rel.clone())],
            PropertySpillWriteOptions {
                artifact_path: &ordinary_spill,
                source_commit_epoch: 47,
                config: PropertySpillConfig::default(),
                descriptor_tree: tree(&ordinary),
            },
        )
        .unwrap();
    let ceiling = 16 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let path = controlled.join("canonical.hawdb");
    let spill = controlled.join("properties.hawdb");
    // Whole-writer descriptor/spill construction still has separate work-unit
    // obligations. The direct record codec tests above enforce one local unit.
    let (manifest, output) = work
        .classify(|work| {
            CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
                .with_work_context(work.clone())
                .write_checkpoint_steps(
                    &path,
                    ManifestGeneration(47),
                    nodes.iter().map(|node| Ok(Some(node))),
                    [Ok(Some(&rel))],
                    PropertySpillWriteOptions {
                        artifact_path: &spill,
                        source_commit_epoch: 47,
                        config: PropertySpillConfig::default(),
                        descriptor_tree: tree(&controlled),
                    },
                )
        })
        .unwrap_or_else(failure);
    assert_eq!(
        std::fs::read(&path).unwrap(),
        std::fs::read(&ordinary_path).unwrap()
    );
    assert_eq!(
        std::fs::read(&spill).unwrap(),
        std::fs::read(&ordinary_spill).unwrap()
    );
    let cache = Arc::new(SegmentCache::new(0));
    let spills = PropertySpillReader::open(
        &spill,
        output.manifest,
        tree(&controlled),
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
    for node in &nodes {
        assert_eq!(reader.get_node(node.id).unwrap().unwrap(), *node);
    }
    assert_eq!(reader.get_relationship(rel.id).unwrap().unwrap(), rel);
    drop(reader);
    drop(manifest);
    assert_eq!(used(&task, ceiling), 0);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    std::fs::remove_dir_all(directory).unwrap();
}
