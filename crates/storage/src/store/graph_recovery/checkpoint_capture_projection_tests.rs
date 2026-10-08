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
use crate::property_projection::{
    PersistentPropertyProjectionDefinition, PersistentPropertyProjectionManifest,
};

struct Fixture {
    directory: PathBuf,
    store: GraphStore,
    catalog: Catalog,
    expected: NodeRecord,
}

impl Fixture {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "hawdb-checkpoint-captured-projection-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &directory,
            &mut catalog,
            DurabilityPolicy::default(),
            WalReplayConfig {
                residency_mode: StorageResidencyMode::OutOfCore,
                max_out_of_core_delta_bytes: Some(32 * 1024 * 1024),
                ..WalReplayConfig::default()
            },
        )
        .unwrap();
        let properties: BTreeMap<String, Value> = (0..1057)
            .map(|i| {
                (
                    format!("k{i:04}-{}", "x".repeat(1024)),
                    Value::Int(i as i64),
                )
            })
            .collect();
        let label = catalog.get_or_create_label("Source");
        for property in properties.keys() {
            catalog.get_or_create_property_index(label, property);
        }
        let id = store
            .create_node(&mut catalog, "Source", properties)
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        assert!(store.is_out_of_core());
        let expected = store.node_owned(id).unwrap().unwrap();
        assert_eq!(expected.properties.len(), 1057);
        assert_eq!(store.commit_epoch(), 1);
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

fn manifests(store: &GraphStore) -> [&PersistentPropertyProjectionManifest; 2] {
    [
        store
            .persistent_property_projection
            .as_ref()
            .unwrap()
            .manifest(),
        store
            .durable
            .as_ref()
            .unwrap()
            .persistent_property_projection
            .as_ref()
            .unwrap()
            .manifest(),
    ]
}

/// Inventory actual live key/vector allocations by address and capacity. This
/// does not depend on the reader's clone representation or a logical estimate.
fn physical_definition_bytes<'a>(
    dictionaries: impl IntoIterator<Item = &'a PersistentPropertyProjectionManifest>,
) -> usize {
    let mut allocations = BTreeMap::new();
    for manifest in dictionaries {
        let keys = &manifest.definitions;
        assert_eq!(keys.len(), 1057);
        let entries =
            keys.capacity() * std::mem::size_of::<PersistentPropertyProjectionDefinition>();
        if let Some(previous) = allocations.insert(keys.as_ptr() as usize, entries) {
            assert_eq!(previous, entries);
        }
        for definition in keys {
            assert!(definition.complete);
            let key = &definition.property;
            assert!(!key.is_empty());
            if let Some(previous) = allocations.insert(key.as_ptr() as usize, key.capacity()) {
                assert_eq!(previous, key.capacity());
            }
        }
    }
    allocations.values().sum()
}

#[test]
fn checkpoint_units_capture_projection_does_not_multiply_the_existing_definition_working_set() {
    let fixture = Fixture::new();
    let before = physical_definition_bytes(manifests(&fixture.store));
    assert!(before > 1024 * 1024);
    let original_bytes = manifests(&fixture.store)[0].encode().unwrap();
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
    let actual = physical_definition_bytes(
        manifests(&fixture.store)
            .into_iter()
            .chain(sources.iter().flat_map(manifests)),
    );
    assert_eq!(
        actual,
        before,
        "eight real captures added {} unadmitted projection definition bytes",
        actual - before
    );
    for source in &sources {
        assert_eq!(
            source.checkpoint_source_identity(),
            fixture.store.checkpoint_source_identity()
        );
        assert_eq!(
            source.node_owned(fixture.expected.id).unwrap(),
            Some(fixture.expected.clone())
        );
        for manifest in manifests(source) {
            assert_eq!(manifest.encode().unwrap(), original_bytes);
        }
    }
    let after = context.state.metrics();
    assert_eq!(after.open, fds.open);
    assert_eq!(after.reserved, fds.reserved);
    assert_eq!(fixture.catalog.property_indexes().count(), 1057);
}

#[test]
fn checkpoint_units_capture_projection_public_reader_clones_retain_one_definition_allocation() {
    let fixture = Fixture::new();
    let reader = fixture
        .store
        .persistent_property_projection
        .as_ref()
        .unwrap();
    let before = physical_definition_bytes([reader.manifest()]);
    assert!(before > 1024 * 1024);
    let readers = (0..8).map(|_| reader.clone()).collect::<Vec<_>>();
    let actual = physical_definition_bytes(
        std::iter::once(reader.manifest()).chain(readers.iter().map(|reader| reader.manifest())),
    );
    assert_eq!(
        actual,
        before,
        "public projection reader clones added {} live definition bytes",
        actual - before
    );
    for reader in &readers {
        assert_eq!(
            reader.manifest(),
            fixture
                .store
                .persistent_property_projection_manifest()
                .unwrap()
        );
        assert_eq!(
            reader.path(),
            fixture
                .store
                .persistent_property_projection
                .as_ref()
                .unwrap()
                .path()
        );
    }
}

#[path = "checkpoint_capture_projection_related_tests.rs"]
mod related_tests;
