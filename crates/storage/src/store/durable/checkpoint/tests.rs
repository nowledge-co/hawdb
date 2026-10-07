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
use crate::file_descriptors::ProjectFileDescriptors;
use crate::schema::{Catalog, LabelId};
use crate::store::{encode_projected_graph_artifacts, GraphStore};
use crate::{NodeId, NodeRecord, Value};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[test]
fn checkpoint_units_projected_graph_publication_cancels_at_every_io_and_retries() {
    use crate::background::CheckpointWorkProbe;
    use crate::projection::{ProjectedGraphArtifactData, ProjectedGraphDefinition};
    use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};

    let scheduler = || {
        LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: Some(1),
            max_total_background_operations: Some(4),
            ..LocalQosPolicy::default()
        })
    };
    let data = ProjectedGraphArtifactData {
        nodes: (0..4096).map(NodeId).collect(),
        csr_offsets: (0..=4096).collect(),
        csr_targets: (0..4096).collect(),
        csc_offsets: (0..=4096).collect(),
        csc_sources: (0..4096).collect(),
    };
    let definition = ProjectedGraphDefinition {
        node_labels: vec!["Memory".into()],
        rel_types: vec!["LINKS".into()],
    };
    let body = crate::projection::artifact::encode_projected_graph_artifacts(
        41,
        43,
        [("graph", &definition, data.clone())],
    );
    let baseline = Fixture::new();
    let baseline_path = baseline.staging.join("controlled_projection.hawdb");
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    baseline
        .durable()
        .write_projected_graph_artifacts_to_with_work_context(
            &baseline_path,
            &body,
            &probe.context(local.clone()),
        )
        .unwrap();
    let waves = probe.io_waves.load(Ordering::SeqCst);
    assert!(waves >= 4);
    probe.assert_released(&local);
    let expected = std::fs::read(&baseline_path).unwrap();
    for wave in 1..=waves {
        let fixture = Fixture::new();
        let path = fixture.staging.join("controlled_projection.hawdb");
        let temporary = path.with_extension("hawdb.tmp");
        if wave == 1 {
            std::fs::write(&temporary, b"previous writer evidence").unwrap();
        }
        let local = scheduler();
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_on_io_wave.store(wave, Ordering::SeqCst);
        let error = fixture
            .durable()
            .write_projected_graph_artifacts_to_with_work_context(
                &path,
                &body,
                &probe.context(local.clone()),
            )
            .unwrap_err();
        assert_eq!(
            error,
            hawdb_core::HawDBError::Storage(
                "checkpoint build I/O stopped: runtime I/O wave stopped: cancelled".into(),
            )
        );
        assert_eq!(probe.io_waves.load(Ordering::SeqCst), wave);
        probe.assert_released(&local);
        if wave == 1 {
            assert_eq!(
                std::fs::read(&temporary).unwrap(),
                b"previous writer evidence"
            );
            let conflict = Arc::new(CheckpointWorkProbe::default());
            assert!(fixture
                .durable()
                .write_projected_graph_artifacts_to_with_work_context(
                    &path,
                    &body,
                    &conflict.context(local.clone()),
                )
                .is_err());
            conflict.assert_released(&local);
            assert_eq!(
                std::fs::read(&temporary).unwrap(),
                b"previous writer evidence"
            );
            // This fixture owns the evidence and removes it before a clean retry.
            std::fs::remove_file(&temporary).unwrap();
        } else {
            assert!(!temporary.exists());
        }
        assert!(!path.exists());
        assert_eq!(read_sidecars(&fixture.root), fixture.old);
        assert_eq!(
            std::fs::read(fixture.durable().manifest_path()).unwrap(),
            fixture.manifest
        );
        let retry = Arc::new(CheckpointWorkProbe::default());
        fixture
            .durable()
            .write_projected_graph_artifacts_to_with_work_context(
                &path,
                &body,
                &retry.context(local.clone()),
            )
            .unwrap();
        retry.assert_released(&local);
        assert!(!temporary.exists());
        assert_eq!(std::fs::read(&path).unwrap(), expected);
        let text = crate::store::read_durable_text(&path, "projection retry").unwrap();
        let (decoded, _) =
            crate::projection::artifact::split_projected_graph_artifact_checksum(&text).unwrap();
        let (epoch, mut recovered) =
            crate::projection::artifact::decode_projected_graph_artifacts(decoded).unwrap();
        assert_eq!(epoch, 43);
        assert_eq!(recovered.remove("graph").unwrap().data, data);
        assert!(recovered.is_empty());
        assert_eq!(fixture.project.metrics().reserved, 0);
        assert!(fixture.project.metrics().high_water <= 8);
    }
}

thread_local! {
    static AFTER_FIRST_PUBLICATION: RefCell<Option<Box<dyn FnOnce()>>> =
        const { RefCell::new(None) };
}

pub(super) fn after_first_publication() {
    if let Some(callback) = AFTER_FIRST_PUBLICATION.with_borrow_mut(Option::take) {
        callback();
    }
}

struct Fixture {
    store: Option<GraphStore>,
    project: ProjectFileDescriptors,
    root: PathBuf,
    staging: PathBuf,
    source_scan_publication: source_scan::SourceScanPublication,
    old: Vec<Vec<u8>>,
    new: Vec<Vec<u8>>,
    manifest: Vec<u8>,
}

impl Fixture {
    fn new() -> Self {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "hawdb-sidecar-admission-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let project = ProjectFileDescriptors::acquire(&root, 8).unwrap();
        let mut catalog = Catalog::default();
        let store = GraphStore::open_with_durability_and_replay_config(
            &root,
            &mut catalog,
            crate::config::DurabilityPolicy::default(),
            crate::config::WalReplayConfig {
                max_open_files: 8,
                ..Default::default()
            },
        )
        .unwrap();
        let durable = store.durable.as_ref().unwrap();
        durable
            .write_projected_graph_artifacts(&encode_projected_graph_artifacts(&catalog, &store, 1))
            .unwrap();
        write_source_scan(&root, 1);
        let old = read_sidecars(&root);
        let manifest = std::fs::read(durable.manifest_path()).unwrap();
        let staging = durable.prepare_checkpoint_staging(7).unwrap();
        durable
            .write_projected_graph_artifacts_to(
                &staging.join(PROJECTED_GRAPHS_FILE),
                &encode_projected_graph_artifacts(&catalog, &store, 2),
            )
            .unwrap();
        let source_scan_publication = write_source_scan(&staging, 2);
        let new = read_sidecars(&staging);
        assert!(old.iter().zip(&new).all(|(old, new)| old != new));
        Self {
            store: Some(store),
            project,
            root,
            staging,
            source_scan_publication,
            old,
            new,
            manifest,
        }
    }

    fn durable(&self) -> &DurableStore {
        self.store.as_ref().unwrap().durable.as_ref().unwrap()
    }

    fn publish(&self) -> Result<()> {
        self.durable().publish_checkpoint_sidecars(
            &self.staging,
            true,
            Some(self.source_scan_publication),
        )
    }

    fn assert_published(&self) {
        assert_eq!(read_sidecars(&self.root), self.new);
        assert!(!self.staging.exists());
        assert_eq!(
            std::fs::read(self.durable().manifest_path()).unwrap(),
            self.manifest
        );
        assert!(source_scan::load(
            &self.root,
            self.source_scan_publication.graph_epoch(),
            self.source_scan_publication.descriptor_checksum(),
        )
        .unwrap()
        .is_some());
        assert_eq!(self.project.metrics().reserved, 0);
        assert!(self.project.metrics().high_water <= 8);
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        drop(self.store.take());
        assert_eq!(self.project.metrics().open, 0);
        std::fs::remove_dir_all(&self.root).unwrap();
    }
}

fn write_source_scan(root: &Path, epoch: u64) -> source_scan::SourceScanPublication {
    let node = NodeRecord {
        id: NodeId(epoch),
        labels: BTreeSet::from([LabelId(1)]),
        properties: BTreeMap::from([("id".into(), Value::String(format!("source-{epoch}")))]),
    };
    let mut projection = source_scan::build(epoch, Some(LabelId(1)), std::iter::once(&node));
    source_scan::write(root, &mut projection).unwrap()
}

fn read_sidecars(root: &Path) -> Vec<Vec<u8>> {
    [
        PROJECTED_GRAPHS_FILE,
        source_scan::SOURCE_SCAN_PAYLOAD_FILE,
        source_scan::SOURCE_SCAN_DESCRIPTOR_FILE,
    ]
    .map(|file| std::fs::read(root.join(file)).unwrap())
    .into()
}

#[test]
fn checkpoint_sidecar_admission_rejects_before_replacement_and_retries() {
    let fixture = Fixture::new();
    let baseline = fixture.project.metrics().open;
    let held_count = 8 - baseline - (SIDECAR_PUBLICATION_DESCRIPTORS - 1);
    let held = (0..held_count)
        .map(|_| File::open(fixture.durable().manifest_path()).unwrap())
        .collect::<Vec<_>>();
    let error = fixture.publish().unwrap_err();
    assert_eq!(
        error,
        HawDBError::FileDescriptors(hawdb_core::error::FileDescriptorError::BudgetExceeded {
            requested: SIDECAR_PUBLICATION_DESCRIPTORS,
            available: SIDECAR_PUBLICATION_DESCRIPTORS - 1,
            limit: 8,
        })
    );
    assert_eq!(read_sidecars(&fixture.root), fixture.old);
    assert_eq!(read_sidecars(&fixture.staging), fixture.new);
    assert_eq!(
        std::fs::read(fixture.durable().manifest_path()).unwrap(),
        fixture.manifest
    );
    assert_eq!(fixture.project.metrics().reserved, 0);
    assert_eq!(fixture.project.metrics().open, baseline + held_count);
    drop(held);
    fixture.publish().unwrap();
    fixture.assert_published();
    assert_eq!(fixture.project.metrics().open, baseline);
}

#[test]
fn checkpoint_sidecar_reservation_survives_competing_owner_after_first_replacement() {
    let fixture = Fixture::new();
    let baseline = fixture.project.metrics().open;
    let held = Arc::new(Mutex::new(Vec::new()));
    let owner = held.clone();
    let project = fixture.project.clone();
    let path = fixture.durable().manifest_path().to_path_buf();
    AFTER_FIRST_PUBLICATION.with_borrow_mut(|callback| {
        assert!(callback.is_none());
        *callback = Some(Box::new(move || {
            // A separate thread has no inherited quota. Occupy every slot that
            // remains available to an independent owner after the first rename.
            let files = std::thread::spawn(move || {
                let metrics = project.metrics();
                let files = (0..metrics.limit - metrics.open - metrics.reserved)
                    .map(|_| File::open(&path).unwrap())
                    .collect::<Vec<_>>();
                let metrics = project.metrics();
                assert_eq!(metrics.open + metrics.reserved, 8);
                files
            })
            .join()
            .unwrap();
            *owner.lock().unwrap() = files;
        }));
    });
    let result = fixture.publish();
    assert!(
        result.is_ok(),
        "sidecar publication failed: {result:?}; projected replaced={}, source payload retained={}",
        std::fs::read(fixture.root.join(PROJECTED_GRAPHS_FILE)).unwrap() == fixture.new[0],
        std::fs::read(fixture.root.join(source_scan::SOURCE_SCAN_PAYLOAD_FILE)).unwrap()
            == fixture.old[1],
    );
    fixture.assert_published();
    assert_eq!(
        fixture.project.metrics().open,
        baseline + held.lock().unwrap().len()
    );
    held.lock().unwrap().clear();
    assert_eq!(fixture.project.metrics().open, baseline);
}

#[test]
fn checkpoint_units_metadata_publication_cancels_every_io_preserves_evidence_and_retries() {
    use crate::background::CheckpointWorkProbe;
    use hawdb_core::{BasicGraphStatistics, GraphStatistics};
    use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
    let scheduler = || {
        LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: Some(1),
            max_total_background_operations: Some(4),
            ..LocalQosPolicy::default()
        })
    };
    let mut random = 31u64;
    let fingerprint = (0..131073)
        .map(|_| {
            random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
            char::from(b'!' + ((random >> 32) % 90) as u8)
        })
        .collect::<String>();
    let mut catalog = Catalog::default();
    let label = catalog.get_or_create_label("Memory");
    let statistics = crate::statistics::graph_statistics_from_basic(
        BasicGraphStatistics {
            computed_at_commit_epoch: 41,
            node_count: 31,
            label_counts: BTreeMap::from([(label, 31)]),
            ..BasicGraphStatistics::default()
        },
        false,
    );
    let projected = BTreeMap::new();
    let changes = Vec::new();
    let image = || CheckpointImage {
        catalog: &catalog,
        commit_epoch: 41,
        next_node_id: 31,
        next_rel_id: 0,
        search_projection_change_log_start_epoch: 41,
        search_projection_graph_changes: &changes,
        statistics: &statistics,
        projected_graphs: &projected,
        initial_import_source_fingerprint: Some(&fingerprint),
        search_projection_database_identity: None,
        relational_checkpoint: None,
    };
    let ordinary = crate::checkpoint::encode_checkpoint_body(&image(), 17).unwrap();
    let expected_text = format!(
        "{ordinary}checksum\t{}\n",
        crate::store::checksum_bytes(ordinary.as_bytes())
    );
    let baseline = Fixture::new();
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let metadata = baseline
        .durable()
        .write_checkpoint(image(), 17, changes.iter(), &probe.context(local.clone()))
        .unwrap();
    let path = baseline.root.join(checkpoint_generation_file(17));
    let expected = std::fs::read(&path).unwrap();
    assert_eq!(metadata, DurableArtifactMetadata::for_bytes(&expected));
    assert_eq!(
        crate::store::read_durable_text(&path, "controlled checkpoint").unwrap(),
        expected_text
    );
    let waves = probe.io_waves.load(Ordering::SeqCst);
    let units = probe.completed.load(Ordering::SeqCst);
    assert!(waves >= 5);
    probe.assert_released(&local);
    for wave in 1..=waves {
        let fixture = Fixture::new();
        let path = fixture.root.join(checkpoint_generation_file(17));
        let temporary = path.with_extension("hawdb.tmp");
        if wave == 1 {
            std::fs::write(&temporary, b"previous checkpoint evidence").unwrap();
        }
        let local = scheduler();
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_on_io_wave.store(wave, Ordering::SeqCst);
        let error = fixture
            .durable()
            .write_checkpoint(image(), 17, changes.iter(), &probe.context(local.clone()))
            .unwrap_err();
        assert_eq!(
            error,
            hawdb_core::HawDBError::Storage(
                "checkpoint build I/O stopped: runtime I/O wave stopped: cancelled".into()
            )
        );
        assert_eq!(probe.io_waves.load(Ordering::SeqCst), wave);
        probe.assert_released(&local);
        assert_eq!(
            std::fs::read(fixture.durable().manifest_path()).unwrap(),
            fixture.manifest
        );
        assert_eq!(read_sidecars(&fixture.root), fixture.old);
        assert!(!path.exists());
        if wave == 1 {
            assert_eq!(
                std::fs::read(&temporary).unwrap(),
                b"previous checkpoint evidence"
            );
            let conflict = Arc::new(CheckpointWorkProbe::default());
            assert!(fixture
                .durable()
                .write_checkpoint(
                    image(),
                    17,
                    changes.iter(),
                    &conflict.context(local.clone())
                )
                .is_err());
            conflict.assert_released(&local);
            assert_eq!(
                std::fs::read(&temporary).unwrap(),
                b"previous checkpoint evidence"
            );
            std::fs::remove_file(&temporary).unwrap();
        } else {
            assert!(!temporary.exists());
        }
        let retry = Arc::new(CheckpointWorkProbe::default());
        let actual = fixture
            .durable()
            .write_checkpoint(image(), 17, changes.iter(), &retry.context(local.clone()))
            .unwrap();
        retry.assert_released(&local);
        assert!(!temporary.exists());
        assert_eq!(actual, metadata);
        assert_eq!(std::fs::read(&path).unwrap(), expected);
        assert_eq!(
            crate::store::read_durable_text(&path, "checkpoint retry").unwrap(),
            expected_text
        );
        let mut recovered_catalog = Catalog::default();
        let mut decoded = crate::checkpoint::DecodedCheckpoint::default();
        crate::checkpoint::parse_checkpoint(&ordinary, &mut recovered_catalog, &mut decoded)
            .unwrap();
        assert_eq!(
            decoded.initial_import_source_fingerprint.as_deref(),
            Some(fingerprint.as_str())
        );
        assert_eq!(
            decoded.checkpoint_statistics,
            GraphStatistics {
                advanced_statistics_complete: false,
                ..statistics.clone()
            }
        );
        assert_eq!(fixture.project.metrics().reserved, 0);
    }
    // Cancellation after the complete rename is a lost response, not rollback.
    let fixture = Fixture::new();
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    probe.cancel_after.store(units, Ordering::SeqCst);
    let error = fixture
        .durable()
        .write_checkpoint(image(), 17, changes.iter(), &probe.context(local.clone()))
        .unwrap_err();
    assert_eq!(
        error,
        hawdb_core::HawDBError::Storage("checkpoint build stopped: cancelled".into())
    );
    probe.assert_released(&local);
    let path = fixture.root.join(checkpoint_generation_file(17));
    assert_eq!(std::fs::read(&path).unwrap(), expected);
    assert_eq!(
        std::fs::read(fixture.durable().manifest_path()).unwrap(),
        fixture.manifest
    );
    let retry = Arc::new(CheckpointWorkProbe::default());
    assert_eq!(
        fixture
            .durable()
            .write_checkpoint(image(), 17, changes.iter(), &retry.context(local.clone()))
            .unwrap(),
        metadata
    );
    retry.assert_released(&local);
    assert_eq!(std::fs::read(&path).unwrap(), expected);
}
