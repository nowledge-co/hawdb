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

#[test]
fn checkpoint_units_capture_metadata_retains_old_complete_rows_after_writer_mutation_and_close() {
    let mut fixture = Fixture::new();
    let source = fixture.store.checkpoint_source();
    let captured_identity = source.checkpoint_source_identity();
    let context = fixture
        .store
        .durable
        .as_ref()
        .unwrap()
        .file_descriptor_context();
    let key = fixture.expected.properties.keys().next().unwrap().clone();
    let updated_ids = fixture
        .store
        .set_node_property(&mut fixture.catalog, "Source", None, &key, Value::Int(-9))
        .unwrap();
    assert_eq!(updated_ids, vec![fixture.expected.id]);
    let added = fixture
        .store
        .create_node(
            &mut fixture.catalog,
            "Source",
            BTreeMap::from([(key.clone(), Value::Int(88))]),
        )
        .unwrap();
    let mut latest = fixture.expected.clone();
    latest.properties.insert(key, Value::Int(-9));
    let added_row = fixture.store.node_owned(added).unwrap().unwrap();
    assert_eq!(fixture.store.commit_epoch(), 3);
    assert_eq!(
        fixture.store.node_owned(latest.id).unwrap(),
        Some(latest.clone())
    );
    assert_eq!(source.checkpoint_source_identity(), captured_identity);
    assert_eq!(source.node_owned(added).unwrap(), None);
    assert_eq!(
        source.node_owned(fixture.expected.id).unwrap(),
        Some(fixture.expected.clone())
    );

    drop(std::mem::take(&mut fixture.store));
    assert_eq!(source.checkpoint_source_identity(), captured_identity);
    assert_eq!(source.node_owned(added).unwrap(), None);
    assert_eq!(
        source.node_owned(fixture.expected.id).unwrap(),
        Some(fixture.expected.clone())
    );
    assert_eq!(context.state.metrics().reserved, 0);
    drop(source);
    assert_eq!(context.state.metrics().open, 0);
    assert_eq!(context.state.metrics().reserved, 0);

    let mut reopened_catalog = Catalog::default();
    let reopened = GraphStore::open_with_durability_and_replay_config(
        &fixture.directory,
        &mut reopened_catalog,
        DurabilityPolicy::default(),
        WalReplayConfig {
            residency_mode: StorageResidencyMode::OutOfCore,
            max_out_of_core_delta_bytes: Some(32 * 1024 * 1024),
            ..WalReplayConfig::default()
        },
    )
    .unwrap();
    assert!(reopened.is_out_of_core());
    assert_eq!(reopened.commit_epoch(), 3);
    assert_eq!(reopened.node_owned(latest.id).unwrap(), Some(latest));
    assert_eq!(reopened.node_owned(added).unwrap(), Some(added_row));
    assert_eq!(reopened.node_count_for_label(None), 2);
    drop(reopened);
    assert_eq!(context.state.metrics().open, 0);
    assert_eq!(context.state.metrics().reserved, 0);
}

#[test]
fn checkpoint_units_capture_metadata_reader_outlives_original_store_and_releases_native_handles() {
    let mut fixture = Fixture::new();
    let reader = fixture.store.canonical_base.as_ref().unwrap().clone();
    let expected_dictionary_bytes = physical_dictionary_bytes([reader.manifest()]);
    let context = fixture
        .store
        .durable
        .as_ref()
        .unwrap()
        .file_descriptor_context();
    drop(std::mem::take(&mut fixture.store));
    assert_eq!(
        physical_dictionary_bytes([reader.manifest()]),
        expected_dictionary_bytes
    );
    assert_eq!(
        reader.get_node(fixture.expected.id).unwrap(),
        Some(fixture.expected.clone())
    );
    assert!(context.state.metrics().open > 0);
    assert_eq!(context.state.metrics().reserved, 0);
    drop(reader);
    assert_eq!(context.state.metrics().open, 0);
    assert_eq!(context.state.metrics().reserved, 0);
}
