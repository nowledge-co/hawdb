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

use super::super::*;
use super::BufferedWriter;
use crate::file_descriptors::ProjectFileDescriptors;
use crate::graph_descriptor_tree::{demand, GraphDescriptorTreeRootReader};
use crate::power_loss::{
    image::ImageLimits, IoEvent, ObservationBoundary, ObservationPoint, PowerLossModel,
};
use hawdb_core::RuntimeTaskContext;
use hawdb_qos::{
    IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
    RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
};
use std::num::NonZeroUsize;
use std::sync::Arc;

const CEILING: u64 = 64 * 1024;

struct Fixture {
    root: PathBuf,
    reference: PathBuf,
    model: PowerLossModel,
}

fn paths(root: &Path) -> GraphDescriptorTreePaths {
    GraphDescriptorTreePaths::new(
        root.join("descriptor-pages.hawdb"),
        root.join("descriptor-root.hawdb"),
    )
}

impl Fixture {
    fn new() -> Self {
        let id = hawdb_core::generate_uuidv7().unwrap();
        let root = std::env::temp_dir().join(format!("hawdb-descriptor-abort-{id}"));
        let reference = std::env::temp_dir().join(format!("hawdb-descriptor-abort-reference-{id}"));
        fs::create_dir(&reference).unwrap();
        let mut ordinary = GraphDescriptorTreeBuilder::create(
            paths(&reference),
            GraphDescriptorKind::CanonicalSegment,
            47,
            47,
            19,
            GraphDescriptorTreeBuildConfig::default(),
        )
        .unwrap();
        ordinary
            .push(17u64.to_be_bytes().to_vec(), 19u64.to_le_bytes().to_vec())
            .unwrap();
        ordinary.finish().unwrap().publish().unwrap();
        let ordinary = GraphDescriptorTreeRootReader::open(
            paths(&reference),
            GraphDescriptorTreeBuildConfig::default(),
        )
        .unwrap();
        let ordinary = demand::GraphDescriptorTreeDemandReader::open(
            ordinary,
            GraphDescriptorTreeBuildConfig::default(),
            Arc::new(crate::cache::SegmentCache::new(0)),
            crate::cache::StoreId(47),
        )
        .unwrap();
        let mut count = 0;
        ordinary
            .deep_visit(|key, value| {
                assert_eq!(key, 17u64.to_be_bytes());
                assert_eq!(value, 19u64.to_le_bytes());
                count += 1;
                Ok(demand::GraphDescriptorTreeScanControl::Continue)
            })
            .unwrap();
        assert_eq!(count, 1);
        drop(ordinary);
        let project = ProjectFileDescriptors::acquire(&root, 32).unwrap();
        let model = PowerLossModel::attach(&project, ImageLimits::default()).unwrap();
        Self {
            root,
            reference,
            model,
        }
    }

    fn observe_write(&self, path: &Path) {
        self.model
            .observe(ObservationPoint {
                event: IoEvent::Write,
                relative_path: path.strip_prefix(&self.root).unwrap().to_path_buf(),
                boundary: ObservationBoundary::After,
                skip_matches: 0,
                include_descendants: false,
                keep_last: false,
            })
            .unwrap();
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.root).unwrap();
        std::fs::remove_dir_all(&self.reference).unwrap();
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

fn builder(fixture: &Fixture, task: RuntimeTaskContext) -> GraphDescriptorTreeBuilder {
    GraphDescriptorTreeBuilder::create_checkpoint(
        paths(&fixture.root),
        GraphDescriptorKind::CanonicalSegment,
        47,
        47,
        19,
        GraphDescriptorTreeBuildConfig::default(),
        CheckpointWorkContext::new(task),
    )
    .unwrap()
}

#[test]
fn checkpoint_units_descriptor_abort_io_related_ordinary_drop_preserves_buffered_write() {
    let fixture = Fixture::new();
    let path = fixture.root.join("ordinary-buffered.data");
    let mut actual = BufferedWriter::new(8192, File::create(&path).unwrap(), false);
    actual.write_all(b"ordinary buffered bytes").unwrap();
    fixture.observe_write(&path);
    drop(actual);
    assert!(fixture.model.take_observation().unwrap().is_some());
    assert_eq!(fs::read(&path).unwrap(), b"ordinary buffered bytes");
    assert_eq!(fixture.model.project().metrics().open, 0);
}

#[test]
fn checkpoint_units_descriptor_abort_io_related_complete_private_publish_flushes_and_reopens() {
    let fixture = Fixture::new();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
        .unwrap();
    let mut actual = builder(
        &fixture,
        permit.bind_task_context(RuntimeTaskContext::default()),
    );
    actual
        .push(17u64.to_be_bytes().to_vec(), 19u64.to_le_bytes().to_vec())
        .unwrap();
    fixture.observe_write(&paths(&fixture.root).page_tmp());
    let output = actual.finish().unwrap().publish().unwrap();
    assert!(fixture.model.take_observation().unwrap().is_some());
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, CEILING);
    let reader = GraphDescriptorTreeRootReader::open_bound(
        paths(&fixture.root),
        output.generation_artifacts(),
        GraphDescriptorTreeBuildConfig::default(),
    )
    .unwrap();
    let reader = demand::GraphDescriptorTreeDemandReader::open(
        reader,
        GraphDescriptorTreeBuildConfig::default(),
        Arc::new(crate::cache::SegmentCache::new(0)),
        crate::cache::StoreId(47),
    )
    .unwrap();
    let mut count = 0;
    reader
        .deep_visit(|key, value| {
            assert_eq!(key, 17u64.to_be_bytes());
            assert_eq!(value, 19u64.to_le_bytes());
            count += 1;
            Ok(demand::GraphDescriptorTreeScanControl::Continue)
        })
        .unwrap();
    assert_eq!(count, 1);
    drop(reader);
    assert_eq!(
        fs::read(paths(&fixture.root).page_artifact).unwrap(),
        fs::read(paths(&fixture.reference).page_artifact).unwrap()
    );
    assert_eq!(
        fs::read(paths(&fixture.root).root_manifest).unwrap(),
        fs::read(paths(&fixture.reference).root_manifest).unwrap()
    );
    drop(output);
    drop(permit);
    assert_eq!(fixture.model.project().metrics().open, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_descriptor_abort_io_related_failed_flush_is_not_retried_during_drop() {
    let fixture = Fixture::new();
    let path = fixture.root.join("readonly.data");
    drop(File::create(&path).unwrap());
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let mut actual = BufferedWriter::new(8192, File::open(&path).unwrap(), true);
    actual.write_all(b"unacknowledged bytes").unwrap();
    {
        let wave = task.acquire_io_wave(NonZeroUsize::MIN).unwrap().unwrap();
        assert!(actual.flush().is_err());
        drop(wave);
    }
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    fixture
        .model
        .observe(ObservationPoint {
            event: IoEvent::Write,
            relative_path: path.strip_prefix(&fixture.root).unwrap().to_path_buf(),
            boundary: ObservationBoundary::Before,
            skip_matches: 0,
            include_descendants: false,
            keep_last: false,
        })
        .unwrap();
    drop(task);
    drop(permit);
    drop(actual);
    assert!(
        fixture.model.take_observation().unwrap().is_none(),
        "a private failed flush must not retry a native write from drop after admission closes"
    );
    assert!(fs::read(&path).unwrap().is_empty());
    assert_eq!(fixture.model.project().metrics().open, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
