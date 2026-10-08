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
use crate::column_group::manifest::PublishedColumnGroupCatalog;

struct Fixture {
    directory: PathBuf,
    store: GraphStore,
    catalog: Catalog,
    expected: Vec<NodeRecord>,
}

impl Fixture {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "hawdb-checkpoint-captured-shadow-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &directory,
            &mut catalog,
            DurabilityPolicy::default(),
            WalReplayConfig {
                residency_mode: StorageResidencyMode::OutOfCore,
                graph_columnar_shadow_checkpoint: true,
                max_out_of_core_delta_bytes: Some(32 * 1024 * 1024),
                ..WalReplayConfig::default()
            },
        )
        .unwrap();
        store.columnar_shadow.buffer_budget_bytes = 32;
        let mut expected = Vec::new();
        for index in 0..1057 {
            let id = store
                .create_node(
                    &mut catalog,
                    "Source",
                    BTreeMap::from([(
                        "payload".into(),
                        Value::String(format!("{index:04}-{}", "x".repeat(512))),
                    )]),
                )
                .unwrap();
            expected.push(store.node_owned(id).unwrap().unwrap());
        }
        store.checkpoint(&catalog).unwrap();
        assert!(store.is_out_of_core());
        assert_eq!(store.commit_epoch(), 1057);
        let report = store.columnar_shadow_checkpoint_report().unwrap();
        assert_eq!(
            report.status,
            crate::column_group::shadow::ColumnarShadowCheckpointStatus::Published
        );
        assert_eq!(report.table_count, 1);
        assert_eq!(report.oversized_row_group_count, 1057);
        assert_eq!(published(&store).directories()[0].groups().len(), 1057);
        Self {
            directory,
            store,
            catalog,
            expected,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

fn published(store: &GraphStore) -> &PublishedColumnGroupCatalog {
    store.columnar_shadow.catalog.as_ref().unwrap()
}

/// Distinct live vector entries and filename payloads, deduplicated by address.
/// Slice lengths provide a physical lower bound; spare capacity is excluded.
fn physical_metadata_bytes<'a>(
    catalogs: impl IntoIterator<Item = &'a PublishedColumnGroupCatalog>,
) -> usize {
    let mut allocations = BTreeMap::new();
    for catalog in catalogs {
        let tables = catalog.manifest().tables();
        assert_eq!(tables.len(), 1);
        allocations.insert(tables.as_ptr() as usize, std::mem::size_of_val(tables));
        for table in tables {
            allocations.insert(table.file_name().as_ptr() as usize, table.file_name().len());
        }
        let directories = catalog.directories();
        assert_eq!(directories.len(), 1);
        allocations.insert(
            directories.as_ptr() as usize,
            std::mem::size_of_val(directories),
        );
        for directory in directories {
            let groups = directory.groups();
            assert_eq!(groups.len(), 1057);
            allocations.insert(groups.as_ptr() as usize, std::mem::size_of_val(groups));
            for group in groups {
                assert_eq!(group.row_count(), 1);
                allocations.insert(
                    group.group_file().as_ptr() as usize,
                    group.group_file().len(),
                );
            }
        }
    }
    allocations.values().sum()
}

fn complete_rows(store: &GraphStore) -> Vec<NodeRecord> {
    store
        .node_records_owned()
        .collect::<Result<Vec<_>>>()
        .unwrap()
}

#[test]
fn checkpoint_units_capture_shadow_does_not_multiply_existing_directory_metadata() {
    let fixture = Fixture::new();
    let before = physical_metadata_bytes([published(&fixture.store)]);
    assert!(before > 200 * 1024);
    let context = fixture
        .store
        .durable
        .as_ref()
        .unwrap()
        .file_descriptor_context();
    let fds = context.state.metrics();
    let sources = (0..8)
        .map(|_| fixture.store.checkpoint_source())
        .collect::<Vec<_>>();
    let actual = physical_metadata_bytes(
        std::iter::once(published(&fixture.store)).chain(sources.iter().map(published)),
    );
    assert_eq!(
        actual,
        before,
        "eight real captures added {} unadmitted shadow metadata bytes",
        actual - before
    );
    for source in &sources {
        assert_eq!(
            source.checkpoint_source_identity(),
            fixture.store.checkpoint_source_identity()
        );
        assert_eq!(published(source), published(&fixture.store));
        assert_eq!(complete_rows(source), fixture.expected);
    }
    let after = context.state.metrics();
    assert_eq!(after.open, fds.open);
    assert_eq!(after.reserved, fds.reserved);
    assert_eq!(fixture.catalog.labels().count(), 1);
}

#[test]
fn checkpoint_units_capture_shadow_public_catalog_clones_retain_one_directory_allocation() {
    let fixture = Fixture::new();
    let catalog = published(&fixture.store);
    let before = physical_metadata_bytes([catalog]);
    assert!(before > 200 * 1024);
    let readers = (0..8).map(|_| catalog.clone()).collect::<Vec<_>>();
    let actual = physical_metadata_bytes(std::iter::once(catalog).chain(readers.iter()));
    assert_eq!(
        actual,
        before,
        "public shadow catalog clones added {} live metadata bytes",
        actual - before
    );
    for reader in &readers {
        assert_eq!(reader, catalog);
        assert_eq!(reader.manifest().source_commit_epoch(), 1057);
        assert_eq!(reader.manifest().generation().0, 1);
    }
}

#[path = "checkpoint_capture_shadow_related_tests.rs"]
mod related_tests;
