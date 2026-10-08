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
use crate::column_group::group::ColumnGroupReader;
use crate::column_group::shadow::{encode_label_set, COLUMN_GROUP_SHADOW_DIR};

fn verify_complete_catalog(
    root: &Path,
    catalog: &PublishedColumnGroupCatalog,
    expected: &[NodeRecord],
) {
    catalog.scrub_artifacts(root).unwrap();
    let expected = expected
        .iter()
        .map(|record| (record.id.0, record))
        .collect::<BTreeMap<_, _>>();
    let mut seen = BTreeSet::new();
    for directory in catalog.directories() {
        for descriptor in directory.groups() {
            assert_eq!(descriptor.row_count(), 1);
            let reader = ColumnGroupReader::open_path(&root.join(descriptor.group_file())).unwrap();
            let ids = reader.read_ids().unwrap();
            assert_eq!(ids.len(), 1);
            assert!(seen.insert(ids[0]));
            let record = expected[&ids[0]];
            let mut payloads = 0;
            let mut label_sets = 0;
            for column in &reader.directory().columns {
                let values = reader.read_column(column.property_id, None).unwrap();
                assert_eq!(values.len(), 1);
                match &values[0] {
                    Value::String(_) => {
                        assert_eq!(&values[0], &record.properties["payload"]);
                        payloads += 1;
                    }
                    Value::Binary(bytes) => {
                        assert_eq!(*bytes, encode_label_set(&record.labels));
                        label_sets += 1;
                    }
                    other => panic!("unexpected shadow column {other:?}"),
                }
            }
            assert_eq!(payloads, 1);
            assert_eq!(label_sets, 1);
        }
    }
    assert_eq!(seen, expected.keys().copied().collect());
}

#[test]
fn checkpoint_units_capture_shadow_source_and_public_catalog_survive_writer_close_and_reopen() {
    let mut fixture = Fixture::new();
    let source = fixture.store.checkpoint_source();
    let public = published(&fixture.store).clone();
    let identity = source.checkpoint_source_identity();
    let before = physical_metadata_bytes([&public]);
    let context = fixture
        .store
        .durable
        .as_ref()
        .unwrap()
        .file_descriptor_context();
    let original = fixture.expected.clone();
    assert_eq!(
        fixture
            .store
            .set_node_property(
                &mut fixture.catalog,
                "Source",
                None,
                "payload",
                Value::String("updated".into()),
            )
            .unwrap(),
        original.iter().map(|record| record.id).collect::<Vec<_>>()
    );
    let added = fixture
        .store
        .create_node(
            &mut fixture.catalog,
            "Source",
            BTreeMap::from([("payload".into(), Value::String("added".into()))]),
        )
        .unwrap();
    let latest = complete_rows(&fixture.store);
    assert_eq!(latest.len(), 1058);
    assert_eq!(fixture.store.commit_epoch(), 1059);
    assert_eq!(source.checkpoint_source_identity(), identity);
    assert_eq!(complete_rows(&source), original);
    assert_eq!(source.node_owned(added).unwrap(), None);
    drop(std::mem::take(&mut fixture.store));
    assert_eq!(source.checkpoint_source_identity(), identity);
    assert_eq!(complete_rows(&source), original);
    assert_eq!(
        physical_metadata_bytes([&public, published(&source)]),
        before
    );
    assert_eq!(&public, published(&source));
    drop(source);
    assert_eq!(context.state.metrics().open, 0);
    assert_eq!(context.state.metrics().reserved, 0);
    verify_complete_catalog(
        &fixture.directory.join(COLUMN_GROUP_SHADOW_DIR),
        &public,
        &original,
    );
    assert_eq!(physical_metadata_bytes([&public]), before);
    assert_eq!(context.state.metrics().open, 0);
    assert_eq!(context.state.metrics().reserved, 0);

    let mut reopened_catalog = Catalog::default();
    let reopened = GraphStore::open_with_durability_and_replay_config(
        &fixture.directory,
        &mut reopened_catalog,
        DurabilityPolicy::default(),
        WalReplayConfig {
            residency_mode: StorageResidencyMode::OutOfCore,
            graph_columnar_shadow_checkpoint: true,
            max_out_of_core_delta_bytes: Some(32 * 1024 * 1024),
            ..WalReplayConfig::default()
        },
    )
    .unwrap();
    assert!(reopened.is_out_of_core());
    assert_eq!(reopened.commit_epoch(), 1059);
    assert_eq!(complete_rows(&reopened), latest);
    assert!(reopened.columnar_shadow_recovery_status().validated);
    assert_eq!(published(&reopened), &public);
    assert_eq!(published(&reopened).manifest().source_commit_epoch(), 1057);
    assert!(!reopened.columnar_shadow.dirty.is_empty());
    drop(reopened);
    drop(public);
    assert_eq!(context.state.metrics().open, 0);
    assert_eq!(context.state.metrics().reserved, 0);
}
