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

fn tree(directory: &Path) -> PersistentPropertySpillDescriptorTree {
    PersistentPropertySpillDescriptorTree::new(
        GraphDescriptorTreePaths::new(
            directory.join("spill-descriptors.pages.hawdb"),
            directory.join("spill-descriptors.root.hawdb"),
        ),
        GraphDescriptorTreeBuildConfig::default(),
    )
}

struct Fixture {
    root: PathBuf,
    node: NodeRecord,
}

impl Fixture {
    fn new(labels: BTreeSet<LabelId>, properties: BTreeMap<String, Value>) -> Self {
        let root = std::env::temp_dir().join(format!(
            "hawdb-canonical-record-memory-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        std::fs::create_dir(&root).unwrap();
        let node = NodeRecord {
            id: NodeId(17),
            labels,
            properties,
        };
        let directory = root.join("reference");
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("canonical.hawdb");
        let spill = directory.join("properties.hawdb");
        let (manifest, output) = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .write_fallible_with_property_spills(
                &path,
                ManifestGeneration(47),
                [Ok(node.clone())],
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
        assert_eq!(reader.get_node(node.id).unwrap().unwrap(), node);
        Self { root, node }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.root).unwrap();
    }
}

fn assert_record_capacity_is_admitted(fixture: &Fixture) {
    // Borrowed source and complete ordinary artifact/readback predate the
    // observation. Individual inline values fit this reservation; the combined
    // record capacity, or the label-only record, needs more than 64 KiB.
    let ceiling = 64 * 1024;
    let governor = RuntimeGovernor::new(
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
    );
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let directory = fixture.root.join("attempt");
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("canonical.hawdb");
    let spill = directory.join("properties.hawdb");
    let options = PropertySpillWriteOptions {
        artifact_path: &spill,
        source_commit_epoch: 47,
        config: PropertySpillConfig::default(),
        descriptor_tree: tree(&directory),
    };
    let observation = crate::test_allocator::AllocationObservation::start();
    let result = work.classify(|work| {
        CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .with_work_context(work.clone())
            .write_checkpoint_steps(
                &path,
                ManifestGeneration(47),
                [Ok(Some(&fixture.node))],
                std::iter::empty::<Result<Option<&RelRecord>, CanonicalSegmentError>>(),
                options,
            )
    });
    let allocations = observation.finish();
    assert!(
        matches!(
            &result,
            Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
                RuntimeMemoryError::ReservationExceeded { .. }
            )))
        ),
        "borrowed checkpoint writer must deny complete record capacity before large allocation; observed large allocations={allocations}"
    );
    assert_eq!(allocations, 0);
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
fn checkpoint_units_canonical_record_memory_wide_node_labels_are_admitted_before_encoding() {
    let fixture = Fixture::new((0..65_793).map(LabelId).collect(), BTreeMap::new());
    assert_record_capacity_is_admitted(&fixture);
}

#[test]
fn checkpoint_units_canonical_record_memory_combined_inline_node_is_admitted_before_encoding() {
    let fixture = Fixture::new(
        BTreeSet::from([LabelId(7)]),
        (0..8)
            .map(|id| (format!("key-{id}"), Value::Binary(vec![0x9f; 37 * 1024])))
            .collect(),
    );
    assert_record_capacity_is_admitted(&fixture);
}
