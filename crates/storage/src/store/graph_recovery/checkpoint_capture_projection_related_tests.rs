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
use crate::property_projection::PersistentPropertyProjectionReader;

fn verify_complete_index(reader: &PersistentPropertyProjectionReader, expected: &NodeRecord) {
    assert_eq!(reader.manifest().definitions.len(), 1057);
    let label = *expected.labels.first().unwrap();
    for (property, value) in &expected.properties {
        let mut ids = Vec::new();
        let (_, control) = reader
            .scan_equality_candidates(label, property, value, |id| {
                ids.push(id);
                Ok(CanonicalScanControl::Continue)
            })
            .unwrap();
        assert_eq!(control, CanonicalScanControl::Continue);
        assert_eq!(ids, [expected.id], "{property}");
        let mut absent = Vec::new();
        reader
            .scan_equality_candidates(label, property, &Value::Int(-1), |id| {
                absent.push(id);
                Ok(CanonicalScanControl::Continue)
            })
            .unwrap();
        assert!(absent.is_empty(), "{property}");
    }
}

fn equality_rows(
    store: &GraphStore,
    label: LabelId,
    property: &str,
    value: Value,
) -> Vec<NodeRecord> {
    let mut rows = Vec::new();
    store
        .visit_nodes_by_property_owned(label, property, &[value], |node| {
            rows.push(node);
            GraphScanControl::Continue
        })
        .unwrap();
    rows.sort_by_key(|row| row.id);
    rows
}

#[test]
fn checkpoint_units_capture_projection_retains_all_old_index_results_after_writer_changes_and_close(
) {
    let mut fixture = Fixture::new();
    let source = fixture.store.checkpoint_source();
    let identity = source.checkpoint_source_identity();
    let context = fixture
        .store
        .durable
        .as_ref()
        .unwrap()
        .file_descriptor_context();
    let label = fixture.catalog.label_id("Source").unwrap();
    let property = fixture.expected.properties.keys().next().unwrap().clone();
    let original = fixture.expected.properties[&property].clone();
    assert_eq!(
        fixture
            .store
            .set_node_property(
                &mut fixture.catalog,
                "Source",
                None,
                &property,
                Value::Int(-9)
            )
            .unwrap(),
        [fixture.expected.id]
    );
    let added = fixture
        .store
        .create_node(
            &mut fixture.catalog,
            "Source",
            BTreeMap::from([(property.clone(), Value::Int(88))]),
        )
        .unwrap();
    let mut latest = fixture.expected.clone();
    latest.properties.insert(property.clone(), Value::Int(-9));
    let added_row = fixture.store.node_owned(added).unwrap().unwrap();
    assert_eq!(fixture.store.commit_epoch(), 3);
    assert_eq!(source.checkpoint_source_identity(), identity);
    assert_eq!(source.node_owned(added).unwrap(), None);
    assert_eq!(
        source.node_owned(fixture.expected.id).unwrap(),
        Some(fixture.expected.clone())
    );
    drop(std::mem::take(&mut fixture.store));
    assert_eq!(source.checkpoint_source_identity(), identity);
    assert_eq!(source.node_owned(added).unwrap(), None);
    verify_complete_index(
        source.persistent_property_projection.as_ref().unwrap(),
        &fixture.expected,
    );
    assert_eq!(
        equality_rows(&source, label, &property, original.clone()),
        [fixture.expected.clone()]
    );
    assert!(equality_rows(&source, label, &property, Value::Int(-9)).is_empty());
    assert_eq!(
        source.node_owned(fixture.expected.id).unwrap(),
        Some(fixture.expected.clone())
    );
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
    assert_eq!(reopened_catalog.property_indexes().count(), 1057);
    assert_eq!(
        reopened.node_owned(latest.id).unwrap(),
        Some(latest.clone())
    );
    assert_eq!(reopened.node_owned(added).unwrap(), Some(added_row.clone()));
    assert_eq!(reopened.node_count_for_label(None), 2);
    assert!(equality_rows(&reopened, label, &property, original).is_empty());
    assert_eq!(
        equality_rows(&reopened, label, &property, Value::Int(-9)),
        [latest]
    );
    assert_eq!(
        equality_rows(&reopened, label, &property, Value::Int(88)),
        [added_row]
    );
    drop(reopened);
    assert_eq!(context.state.metrics().open, 0);
    assert_eq!(context.state.metrics().reserved, 0);
}

#[test]
fn checkpoint_units_capture_projection_public_reader_survives_writer_and_releases_all_native_handles(
) {
    let mut fixture = Fixture::new();
    let reader = fixture
        .store
        .persistent_property_projection
        .as_ref()
        .unwrap()
        .clone();
    let original = reader.manifest().encode().unwrap();
    let before = physical_definition_bytes([reader.manifest()]);
    let context = fixture
        .store
        .durable
        .as_ref()
        .unwrap()
        .file_descriptor_context();
    drop(std::mem::take(&mut fixture.store));
    assert_eq!(physical_definition_bytes([reader.manifest()]), before);
    assert_eq!(reader.manifest().encode().unwrap(), original);
    verify_complete_index(&reader, &fixture.expected);
    assert!(!reader.is_poisoned());
    drop(reader);
    assert_eq!(context.state.metrics().reserved, 0);
    assert_eq!(context.state.metrics().open, 0);
}
