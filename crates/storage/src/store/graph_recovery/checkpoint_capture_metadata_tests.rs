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
use crate::canonical::CanonicalSegmentManifest;

struct Fixture {
    directory: PathBuf,
    store: GraphStore,
    catalog: Catalog,
    expected: NodeRecord,
}

impl Fixture {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "hawdb-checkpoint-captured-metadata-{}",
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
        let properties = (0..1057)
            .map(|i| {
                (
                    format!("k{i:04}-{}", "x".repeat(1024)),
                    Value::Int(i as i64),
                )
            })
            .collect();
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

fn manifests(store: &GraphStore) -> [&CanonicalSegmentManifest; 2] {
    [
        store.canonical_base.as_ref().unwrap().manifest(),
        store
            .durable
            .as_ref()
            .unwrap()
            .canonical_segments
            .as_ref()
            .unwrap()
            .manifest(),
    ]
}

/// Inventory actual live key/vector allocations by address and capacity. This
/// does not depend on the reader's clone representation or a logical estimate.
fn physical_dictionary_bytes<'a>(
    dictionaries: impl IntoIterator<Item = &'a CanonicalSegmentManifest>,
) -> usize {
    let mut allocations = BTreeMap::new();
    for manifest in dictionaries {
        let keys = &manifest.property_keys;
        assert_eq!(keys.len(), 1057);
        let entries = keys.capacity() * std::mem::size_of::<String>();
        if let Some(previous) = allocations.insert(keys.as_ptr() as usize, entries) {
            assert_eq!(previous, entries);
        }
        for key in keys {
            assert!(!key.is_empty());
            if let Some(previous) = allocations.insert(key.as_ptr() as usize, key.capacity()) {
                assert_eq!(previous, key.capacity());
            }
        }
    }
    allocations.values().sum()
}

#[test]
fn checkpoint_units_capture_metadata_does_not_multiply_the_existing_dictionary_working_set() {
    let fixture = Fixture::new();
    let before = physical_dictionary_bytes(manifests(&fixture.store));
    assert!(before > 1024 * 1024);
    let fds = fixture
        .store
        .durable
        .as_ref()
        .unwrap()
        .file_descriptor_metrics();
    let sources = (0..8)
        .map(|_| fixture.store.checkpoint_source())
        .collect::<Vec<_>>();
    let actual = physical_dictionary_bytes(
        manifests(&fixture.store)
            .into_iter()
            .chain(sources.iter().flat_map(manifests)),
    );
    assert_eq!(
        actual,
        before,
        "eight real captures added {} unadmitted dictionary bytes",
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
    }
    let after = fixture
        .store
        .durable
        .as_ref()
        .unwrap()
        .file_descriptor_metrics();
    assert_eq!(after.open, fds.open);
    assert_eq!(after.reserved, fds.reserved);
    assert_eq!(fixture.catalog.labels().count(), 1);
}

#[test]
fn checkpoint_units_capture_metadata_public_reader_clones_retain_one_dictionary_allocation() {
    let fixture = Fixture::new();
    let reader = fixture.store.canonical_base.as_ref().unwrap();
    let before = physical_dictionary_bytes([reader.manifest()]);
    assert!(before > 1024 * 1024);
    let copies = (0..8).map(|_| reader.clone()).collect::<Vec<_>>();
    let actual = physical_dictionary_bytes(
        [reader.manifest()]
            .into_iter()
            .chain(copies.iter().map(|reader| reader.manifest())),
    );
    assert_eq!(
        actual,
        before,
        "public reader clones added {} dictionary bytes",
        actual - before
    );
    for reader in &copies {
        assert_eq!(
            reader.manifest(),
            fixture.store.canonical_base.as_ref().unwrap().manifest()
        );
    }
}

#[path = "checkpoint_capture_metadata_related_tests.rs"]
mod related;
