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
use crate::property_spill::{PersistentPropertySpillDescriptorTree, PropertySpillConfig};
use hawdb_core::{RuntimeMemoryError, RuntimeTaskContext};
use hawdb_qos::{
    IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
    RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
};
use std::num::NonZeroUsize;

struct Fixture {
    root: PathBuf,
    node: NodeRecord,
}

fn tree(directory: &Path) -> PersistentPropertySpillDescriptorTree {
    PersistentPropertySpillDescriptorTree::new(
        GraphDescriptorTreePaths::new(
            directory.join("spill.pages.hawdb"),
            directory.join("spill.root.hawdb"),
        ),
        GraphDescriptorTreeBuildConfig::default(),
    )
}

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "hawdb-descriptor-tree-memory-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        std::fs::create_dir(&root).unwrap();
        let node = NodeRecord {
            id: NodeId(17),
            labels: BTreeSet::from([LabelId(7)]),
            properties: BTreeMap::from([("number".into(), Value::Int(19))]),
        };
        let directory = root.join("ordinary");
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("canonical.hawdb");
        let spill = directory.join("properties.hawdb");
        let (manifest, output) = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .write_fallible_with_property_spills(
                &path,
                ManifestGeneration(47),
                std::iter::once(Ok::<_, CanonicalSegmentError>(node.clone())),
                std::iter::empty::<Result<RelRecord, CanonicalSegmentError>>(),
                PropertySpillWriteOptions {
                    artifact_path: &spill,
                    source_commit_epoch: 47,
                    config: PropertySpillConfig::default(),
                    descriptor_tree: tree(&directory),
                },
            )
            .unwrap();
        let cache = Arc::new(SegmentCache::new(0));
        let spills = PropertySpillReader::open(
            &spill,
            output.manifest,
            tree(&directory),
            cache.clone(),
            StoreId(47),
            NonZeroU64::new(32 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        let reader = CanonicalSegmentReader::open_with_property_spills(
            &path,
            manifest,
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
            vec![node.clone()]
        );
        drop(reader);
        Self { root, node }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.root).unwrap();
    }
}

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
        other => panic!("full-ceiling concrete lease probe failed: {other:?}"),
    }
}

#[test]
fn checkpoint_units_descriptor_tree_memory_private_constructor_denies_before_reading_source() {
    // Complete ordinary artifact and all-row readback predate the attempt.
    // This reservation cannot admit either fixed 8 KiB tree I/O buffer.
    // A private writer must deny that retained scratch before fetching input.
    let fixture = Fixture::new();
    let governor = governor(1);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(1).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let directory = fixture.root.join("denied");
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("canonical.hawdb");
    let spill = directory.join("properties.hawdb");
    let mut calls = 0;
    let result = work.classify(|work| {
        CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .with_work_context(work.clone())
            .write_checkpoint_steps(
                &path,
                ManifestGeneration(47),
                std::iter::once(&fixture.node).map(|node| {
                    calls += 1;
                    Ok(Some(node))
                }),
                std::iter::empty::<Result<Option<&RelRecord>, CanonicalSegmentError>>(),
                PropertySpillWriteOptions {
                    artifact_path: &spill,
                    source_commit_epoch: 47,
                    config: PropertySpillConfig::default(),
                    descriptor_tree: tree(&directory),
                },
            )
    });
    assert!(matches!(
        &result,
        Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
            RuntimeMemoryError::ReservationExceeded { .. }
        )))
    ));
    assert_eq!(
        calls, 0,
        "private descriptor-tree scratch must be admitted before any source row is fetched"
    );
    assert!(!path.exists());
    assert!(!spill.exists());
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(result);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_descriptor_tree_memory_private_input_observes_retained_tree_io_buffers() {
    let fixture = Fixture::new();
    let ceiling = 64 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let directory = fixture.root.join("retained");
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("canonical.hawdb");
    let spill = directory.join("properties.hawdb");
    let mut observed = None;
    let result = work.classify(|work| {
        CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .with_work_context(work.clone())
            .write_checkpoint_steps(
                &path,
                ManifestGeneration(47),
                std::iter::once(&fixture.node).map(|node| {
                    observed = Some(used(&task, ceiling));
                    Ok(Some(node))
                }),
                std::iter::empty::<Result<Option<&RelRecord>, CanonicalSegmentError>>(),
                PropertySpillWriteOptions {
                    artifact_path: &spill,
                    source_commit_epoch: 47,
                    config: PropertySpillConfig::default(),
                    descriptor_tree: tree(&directory),
                },
            )
    });
    assert!(result.is_ok());
    assert!(observed.unwrap() >= 4 * 8192, "both canonical/spill descriptor writers must retain their fixed page/run buffers through the source callback");
    let (manifest, output) = match result {
        Ok(value) => value,
        Err(_) => unreachable!("checked success"),
    };
    let cache = Arc::new(SegmentCache::new(0));
    let spills = PropertySpillReader::open(
        &spill,
        output.manifest,
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
    assert_eq!(
        reader
            .node_records()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![fixture.node.clone()]
    );
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(reader);
    drop(manifest);
    assert_eq!(used(&task, ceiling), 0);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
