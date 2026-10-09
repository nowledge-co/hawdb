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
    nodes: Vec<NodeRecord>,
    config: PropertySpillConfig,
}

impl Fixture {
    fn new(nodes: Vec<NodeRecord>, config: PropertySpillConfig) -> Self {
        let root = std::env::temp_dir().join(format!(
            "hawdb-canonical-accumulator-memory-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        std::fs::create_dir(&root).unwrap();
        let directory = root.join("reference");
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("canonical.hawdb");
        let spill = directory.join("properties.hawdb");
        let (manifest, output) = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .write_fallible_with_property_spills(
                &path,
                ManifestGeneration(47),
                nodes.iter().cloned().map(Ok::<_, CanonicalSegmentError>),
                std::iter::empty::<Result<RelRecord, CanonicalSegmentError>>(),
                PropertySpillWriteOptions {
                    artifact_path: &spill,
                    source_commit_epoch: 47,
                    config,
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
            nodes
        );
        Self {
            root,
            nodes,
            config,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.root).unwrap();
    }
}

fn assert_accumulator_capacity_is_admitted(fixture: &Fixture) {
    // Borrowed rows and complete ordinary artifact/readback predate observation.
    // Each record is small. Retained segment and spill-pending array capacity
    // must be admitted independently before it grows past this reservation.
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
        config: fixture.config,
        descriptor_tree: tree(&directory),
    };
    let observation = crate::test_allocator::AllocationObservation::start();
    let result = work.classify(|work| {
        CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .with_work_context(work.clone())
            .write_checkpoint_steps(
                &path,
                ManifestGeneration(47),
                fixture.nodes.iter().map(|node| Ok(Some(node))),
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
        "borrowed checkpoint writer must deny retained segment/pending-array capacity before large allocation; observed large allocations={allocations}"
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
fn checkpoint_units_canonical_accumulator_memory_many_empty_records_admit_segment_capacity() {
    let nodes = (1..=4097)
        .map(|id| NodeRecord {
            id: NodeId(id),
            labels: BTreeSet::new(),
            properties: BTreeMap::new(),
        })
        .collect();
    let fixture = Fixture::new(nodes, PropertySpillConfig::default());
    assert_accumulator_capacity_is_admitted(&fixture);
}

#[test]
fn checkpoint_units_canonical_accumulator_memory_many_small_spills_admit_pending_capacity() {
    let nodes = (1..=2049)
        .map(|id| NodeRecord {
            id: NodeId(id),
            labels: BTreeSet::from([LabelId(7)]),
            properties: BTreeMap::from([("payload".into(), Value::Int(19))]),
        })
        .collect();
    let fixture = Fixture::new(
        nodes,
        PropertySpillConfig {
            spill_threshold_bytes: NonZeroU64::MIN,
            ..Default::default()
        },
    );
    assert_accumulator_capacity_is_admitted(&fixture);
}
