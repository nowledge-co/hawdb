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

#[test]
fn checkpoint_units_canonical_writer_memory_related_manifest_retains_keys_after_worker_closes() {
    let ceiling = 16 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let directory = std::env::temp_dir().join(format!(
        "hawdb-checkpoint-key-lifetime-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("canonical.hawdb");
    let spill = directory.join("properties.hawdb");
    let node = NodeRecord {
        id: NodeId(17),
        labels: BTreeSet::from([LabelId(7)]),
        properties: (0..256)
            .map(|id| (format!("{id:04}-{}", "界".repeat(1364)), Value::Int(id)))
            .collect(),
    };
    let (manifest, spill_output) = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
        .with_work_context(work.clone())
        .write_checkpoint_steps(
            &path,
            ManifestGeneration(47),
            [Ok(Some(&node))],
            std::iter::empty::<Result<Option<&RelRecord>, CanonicalSegmentError>>(),
            PropertySpillWriteOptions {
                artifact_path: &spill,
                source_commit_epoch: 47,
                config: PropertySpillConfig::default(),
                descriptor_tree: tree(&directory),
            },
        )
        .unwrap();
    assert_eq!(
        manifest.manifest.property_keys,
        node.properties.keys().cloned().collect::<Vec<_>>()
    );
    let backing = manifest
        .manifest
        .property_keys
        .iter()
        .map(String::capacity)
        .sum::<usize>()
        + manifest.manifest.property_keys.capacity() * std::mem::size_of::<String>();
    let retained = used(&task, ceiling);
    assert!(retained >= backing as u64);
    // The fixed-width lookup tree is construction scratch. Keeping its four
    // node bounds per digest would exceed this independent retained-key bound.
    assert!(retained < backing as u64 + 256 * 1024);
    let ordinary = manifest.manifest.encode().unwrap();
    let encoded = manifest.encode_with_work_context(&work).unwrap();
    assert_eq!(encoded.as_bytes(), ordinary.as_bytes());
    drop(encoded);
    assert_eq!(used(&task, ceiling), retained);
    let cache = Arc::new(SegmentCache::new(0));
    let spills = PropertySpillReader::open(
        &spill,
        spill_output.manifest,
        tree(&directory),
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
    assert_eq!(reader.get_node(node.id).unwrap().unwrap(), node);
    drop(reader);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    drop(manifest);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn checkpoint_units_canonical_writer_memory_related_value_codec_every_cut_and_lifetime() {
    let ceiling = 32 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let local = local();
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
    let value = Value::Map(BTreeMap::from([
        ("binary".into(), Value::Binary(vec![0x9f; 257 * 1024 + 3])),
        (
            "nested".into(),
            Value::List(vec![Value::String("界".repeat(87_723)), Value::Null]),
        ),
    ]));
    values.push(value.clone());
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    for value in &values {
        let mut expected = Vec::new();
        crate::canonical::encode_value(value, &mut expected, 1).unwrap();
        let actual = encode_value(value, &work).unwrap();
        assert_eq!(&*actual, expected);
        assert!(used(&task, ceiling) >= expected.len() as u64);
        drop(actual);
        assert_eq!(used(&task, ceiling), 0);
    }
    let mut invalid = Value::Null;
    for _ in 0..MAX_VALUE_DEPTH + 2 {
        invalid = Value::List(vec![invalid]);
    }
    let mut ordinary = Vec::new();
    let expected_error = crate::canonical::encode_value(&invalid, &mut ordinary, 1)
        .unwrap_err()
        .to_string();
    assert_eq!(
        encode_value(&invalid, &work).err().unwrap().to_string(),
        expected_error
    );
    assert_eq!(used(&task, ceiling), 0);
    let mut expected = Vec::new();
    crate::canonical::encode_value(&value, &mut expected, 1).unwrap();
    let baseline = Arc::new(CheckpointWorkProbe::default());
    local.set_telemetry_sink(Some(baseline.clone()));
    let actual = encode_value(&value, &work).unwrap();
    assert_eq!(&*actual, expected);
    let total = baseline.completed.load(Ordering::SeqCst);
    assert!(total > 32);
    assert_eq!(baseline.peak_units.load(Ordering::SeqCst), 1);
    baseline.assert_released(&local);
    drop(actual);
    for cut in 1..=total {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(cut, Ordering::SeqCst);
        local.set_telemetry_sink(Some(probe.clone()));
        let stopped = CheckpointWorkContext::new(permit.bind_task_context(
            RuntimeTaskContext::without_deadline(probe.cancellation.clone()),
        ))
        .with_scheduler(local.clone());
        assert!(matches!(
            encode_value(&value, &stopped),
            Err(CanonicalSegmentError::Work(CheckpointWorkError::Stopped(_)))
        ));
        assert_eq!(probe.completed.load(Ordering::SeqCst), cut);
        probe.assert_released(&local);
        drop(stopped);
        assert_eq!(used(&task, ceiling), 0);
        local.set_telemetry_sink(None);
        let retry = encode_value(&value, &work).unwrap();
        assert_eq!(&*retry, expected);
        drop(retry);
        assert_eq!(used(&task, ceiling), 0);
    }
    assert_eq!(baseline.io_waves.load(Ordering::SeqCst), 0);
    let output = encode_value(&value, &work).unwrap();
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    drop(output);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

fn collision(dictionary: &mut Dictionary, key: &str) -> Result<u32, CanonicalSegmentError> {
    // Force a digest collision at the private lookup seam, without a production
    // mode or hash override. Admission and comparison use the real implementation.
    let work = dictionary.work.clone();
    work.classify(|work| {
        let keys = CheckpointDecodeContext {
            work: work.clone(),
            memory: RefCell::new(std::mem::take(&mut dictionary.key_memory)),
        };
        let index = CheckpointDecodeContext {
            work: work.clone(),
            memory: RefCell::new(std::mem::take(&mut dictionary.index_memory)),
        };
        let result = dictionary.intern_digest(key, [0; 32], &keys, &index);
        dictionary.key_memory = keys.memory.into_inner();
        dictionary.index_memory = index.memory.into_inner();
        result
    })
    .map_err(|error| match error {
        CheckpointOperationError::Work(error) => CanonicalSegmentError::Work(error),
        CheckpointOperationError::Operation(error) => error,
    })
}

fn colliding_sequence(
    work: &CheckpointWorkContext,
    a: &str,
    b: &str,
) -> Result<Keys, CanonicalSegmentError> {
    let mut dictionary = Dictionary::new(work.clone());
    assert_eq!(collision(&mut dictionary, a)?, 0);
    assert_eq!(collision(&mut dictionary, b)?, 1);
    assert_eq!(collision(&mut dictionary, a)?, 0);
    Ok(dictionary.into_keys())
}

#[test]
fn checkpoint_units_canonical_writer_memory_related_first_seen_growth_and_collision_cuts() {
    let ceiling = 64 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let local = local();
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    for count in [0, 1, 31, 32, 33, 1023, 1024, 1025, 4097, 8193] {
        let mut reference = BTreeMap::new();
        let mut ordered = Vec::new();
        let mut dictionary = Dictionary::new(work.clone());
        for id in (0..count).rev().chain(0..count) {
            let key = format!("key-{id:05}-界🦀\0");
            let expected = *reference.entry(key.clone()).or_insert_with(|| {
                let id = ordered.len() as u32;
                ordered.push(key.clone());
                id
            });
            assert_eq!(dictionary.intern(&key).unwrap(), expected);
        }
        let keys = dictionary.into_keys();
        assert_eq!(keys.values, ordered);
        let backing = keys.values.iter().map(String::capacity).sum::<usize>()
            + keys.values.capacity() * std::mem::size_of::<String>();
        let retained = used(&task, ceiling);
        assert!(retained >= backing as u64);
        assert!(retained <= backing as u64 + 256 * (count as u64 + 1));
        drop(keys);
        assert_eq!(used(&task, ceiling), 0);
    }
    let a = "界".repeat(87_723) + "A";
    let b = "界".repeat(87_723) + "B";
    let baseline = Arc::new(CheckpointWorkProbe::default());
    local.set_telemetry_sink(Some(baseline.clone()));
    let keys = colliding_sequence(&work, &a, &b).unwrap();
    assert_eq!(keys.values, [a.clone(), b.clone()]);
    drop(keys);
    let total = baseline.completed.load(Ordering::SeqCst);
    assert!(total > 32);
    assert_eq!(baseline.peak_units.load(Ordering::SeqCst), 1);
    baseline.assert_released(&local);
    for cut in 1..=total {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(cut, Ordering::SeqCst);
        local.set_telemetry_sink(Some(probe.clone()));
        let stopped = CheckpointWorkContext::new(permit.bind_task_context(
            RuntimeTaskContext::without_deadline(probe.cancellation.clone()),
        ))
        .with_scheduler(local.clone());
        assert!(matches!(
            colliding_sequence(&stopped, &a, &b),
            Err(CanonicalSegmentError::Work(CheckpointWorkError::Stopped(_)))
        ));
        assert_eq!(probe.completed.load(Ordering::SeqCst), cut);
        probe.assert_released(&local);
        drop(stopped);
        assert_eq!(used(&task, ceiling), 0);
        local.set_telemetry_sink(None);
        let keys = colliding_sequence(&work, &a, &b).unwrap();
        assert_eq!(keys.values, [a.clone(), b.clone()]);
        drop(keys);
    }
    assert_eq!(used(&task, ceiling), 0);
    assert_eq!(baseline.io_waves.load(Ordering::SeqCst), 0);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_canonical_writer_memory_related_growth_denial_keeps_old_capacity_and_retries() {
    {
        let ceiling = 1024;
        let governor = governor(ceiling);
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let task = permit.bind_task_context(RuntimeTaskContext::default());
        let mut dictionary = Dictionary::new(CheckpointWorkContext::new(task.clone()));
        assert!(matches!(
            dictionary.intern("x"),
            Err(CanonicalSegmentError::Work(CheckpointWorkError::Memory(
                RuntimeMemoryError::ReservationExceeded { .. }
            )))
        ));
        // The key and one key-vector slot fit; the first lookup node does not.
        assert_eq!(dictionary.keys, ["x"]);
        assert!(dictionary.ids.is_empty());
        drop(dictionary);
        assert_eq!(used(&task, ceiling), 0);
        drop(task);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
    let ceiling = 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let local = local();
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local);
    let source = (0..33).map(|id| format!("{id:02}")).collect::<Vec<_>>();
    let mut dictionary = Dictionary::new(work.clone());
    for (id, key) in source[..32].iter().enumerate() {
        assert_eq!(dictionary.intern(key).unwrap(), id as u32);
    }
    assert_eq!(dictionary.keys.capacity(), 32);
    let address = dictionary.keys.as_ptr();
    let held = task
        .reserve_working_memory(ceiling - used(&task, ceiling) - 1500)
        .unwrap();
    assert!(matches!(
        dictionary.intern(&source[32]),
        Err(CanonicalSegmentError::Work(CheckpointWorkError::Memory(
            RuntimeMemoryError::ReservationExceeded { .. }
        )))
    ));
    assert_eq!(dictionary.keys.as_ptr(), address);
    assert_eq!(dictionary.keys.capacity(), 32);
    assert_eq!(dictionary.keys, source[..32]);
    // Checkpoint cancellation aborts this private workspace. Retry reconstructs
    // it from borrowed source, rather than promising a reusable partial writer.
    drop(dictionary);
    drop(held);
    assert_eq!(used(&task, ceiling), 0);
    let mut retry = Dictionary::new(work.clone());
    for (id, key) in source.iter().enumerate() {
        assert_eq!(retry.intern(key).unwrap(), id as u32);
    }
    let keys = retry.into_keys();
    assert_eq!(keys.values, source);
    drop(keys);
    assert_eq!(used(&task, ceiling), 0);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_canonical_writer_memory_related_pending_spills_retain_until_flush_or_abort() {
    let ceiling = 16 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let directory = std::env::temp_dir().join(format!(
        "hawdb-checkpoint-value-pending-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    std::fs::create_dir(&directory).unwrap();
    let value = Value::Binary(vec![0x9f; 257 * 1024 + 3]);
    let mut expected = Vec::new();
    crate::canonical::encode_value(&value, &mut expected, 1).unwrap();
    let cancelled = Arc::new(CheckpointWorkProbe::default());
    let stopped = CheckpointWorkContext::new(permit.bind_task_context(
        RuntimeTaskContext::without_deadline(cancelled.cancellation.clone()),
    ));
    let abandoned = directory.join("abandoned.hawdb");
    let mut writer = PropertySpillWriter::create_with_work_context(
        &abandoned,
        ManifestGeneration(47),
        47,
        PropertySpillConfig::default(),
        tree(&directory),
        stopped.clone(),
    )
    .unwrap();
    let base = used(&task, ceiling);
    assert_eq!(
        writer
            .push_checkpoint(encode_value(&value, &stopped).unwrap())
            .unwrap(),
        0
    );
    assert!(used(&task, ceiling) >= base + expected.len() as u64);
    cancelled.cancellation.cancel();
    assert!(matches!(
        writer.finish(),
        Err(crate::property_spill::PropertySpillError::Work(
            CheckpointWorkError::Stopped(_)
        ))
    ));
    drop(stopped);
    assert_eq!(used(&task, ceiling), 0);
    let attempt = directory.join("retry");
    std::fs::create_dir(&attempt).unwrap();
    let temporary = attempt.join("temporary.hawdb");
    let path = attempt.join("properties.hawdb");
    let mut writer = PropertySpillWriter::create_with_work_context(
        &temporary,
        ManifestGeneration(47),
        47,
        PropertySpillConfig::default(),
        tree(&attempt),
        work.clone(),
    )
    .unwrap();
    let base = used(&task, ceiling);
    for id in 0..2 {
        assert_eq!(
            writer
                .push_checkpoint(encode_value(&value, &work).unwrap())
                .unwrap(),
            id
        );
        assert!(used(&task, ceiling) >= base + (id + 1) * expected.len() as u64);
    }
    let prepared = writer.finish().unwrap();
    // Both value buffers were destroyed by the completed physical flush. The
    // prepared descriptor artifact has a separate, much smaller inventory.
    assert!(used(&task, ceiling) < expected.len() as u64);
    let output = prepared.publish(&path).unwrap();
    assert_eq!(used(&task, ceiling), 0);
    let reader = PropertySpillReader::open(
        &path,
        output.manifest,
        tree(&attempt),
        Arc::new(SegmentCache::new(0)),
        StoreId(47),
        NonZeroU64::new(32 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    for id in 0..2 {
        assert_eq!(&*reader.get(id).unwrap().unwrap(), expected);
    }
    drop(reader);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    std::fs::remove_dir_all(directory).unwrap();
}
