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
use crate::config::{DurabilityPolicy, RelationalIndexMode, WalReplayConfig};
use crate::relational::{
    RelationalColumnSchema, RelationalHydrationBudget, RelationalInsertMode, RelationalKey,
    RelationalRow, RelationalRowPageSnapshotReadLimits, RelationalRowPageSnapshotReader,
    RelationalScalarType, RelationalTableSchema, RelationalTransaction, RelationalValue,
    RelationalWrite,
};

fn insert(keys: &[&str]) -> RelationalWrite {
    RelationalWrite::Insert {
        table: "documents".into(),
        rows: keys
            .iter()
            .map(|key| RelationalRow::new(vec![RelationalValue::Text((*key).into())]))
            .collect(),
        mode: RelationalInsertMode::Error,
    }
}

fn assert_row(reader: &RelationalRowPageSnapshotReader, key: &str, present: bool) {
    let (row, report) = reader
        .point_projected(
            "documents",
            &RelationalKey(vec![RelationalValue::Text(key.into())]),
            &[0],
            RelationalRowPageSnapshotReadLimits::default(),
            &mut RelationalHydrationBudget::default(),
            &RuntimeTaskContext::default(),
        )
        .unwrap();
    assert_eq!(
        report.identity.visible_commit_epoch,
        reader.identity().visible_commit_epoch
    );
    assert_eq!(row.is_some(), present);
    if let Some(row) = row {
        assert_eq!(row.fields.len(), 1);
        assert_eq!(row.fields[0].value, RelationalValue::Text(key.into()));
    }
}

#[test]
fn relational_suffix_after_unchanged_prefix_seals_and_keeps_all_later_prefixes_bound() {
    for mode in [
        RelationalIndexMode::Shadow,
        RelationalIndexMode::Authoritative,
    ] {
        for policy in [
            DurabilityPolicy::SyncOnEveryWrite,
            DurabilityPolicy::SyncOnCheckpoint,
        ] {
            let root = std::env::temp_dir().join(format!(
                "hawdb-relational-prefix-transition-{}",
                hawdb_core::generate_uuidv7().unwrap()
            ));
            let mut catalog = Catalog::default();
            let replay = WalReplayConfig {
                relational_index_mode: mode,
                ..WalReplayConfig::default()
            };
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &root,
                &mut catalog,
                policy,
                WalReplayConfig {
                    relational_index_mode: RelationalIndexMode::Shadow,
                    ..replay
                },
            )
            .unwrap();
            store
                .commit_relational_transaction(
                    &mut catalog,
                    RelationalTransaction {
                        writes: vec![
                            RelationalWrite::CreateTable(RelationalTableSchema {
                                name: "documents".into(),
                                columns: vec![RelationalColumnSchema {
                                    name: "id".into(),
                                    scalar_type: RelationalScalarType::Text,
                                    nullable: false,
                                    default: None,
                                }],
                                primary_key: vec!["id".into()],
                                unique_constraints: Vec::new(),
                                foreign_keys: Vec::new(),
                                indexes: Vec::new(),
                            }),
                            insert(&["kept"]),
                        ],
                    },
                )
                .unwrap();
            store.checkpoint(&catalog).unwrap();
            drop(store);
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &root,
                &mut catalog,
                policy,
                replay,
            )
            .unwrap();
            store
                .create_node(&mut catalog, "Memory", BTreeMap::new())
                .unwrap();
            let pinned = store.checkpoint_source();
            let mut candidate = pinned
                .prepare_checkpoint_candidate(&catalog)
                .unwrap()
                .unwrap();
            store
                .create_node(&mut catalog, "Memory", BTreeMap::new())
                .unwrap();
            candidate.catch_up(&store).unwrap();
            candidate.finish_catch_up().unwrap();
            assert!(candidate.recovery_selectors.iter().all(Option::is_none));
            let old_reader = candidate
                .store
                .as_ref()
                .unwrap()
                .open_relational_row_snapshot_reader()
                .unwrap()
                .unwrap();
            let old_epoch = old_reader.identity().visible_commit_epoch;
            assert_row(&old_reader, "kept", true);
            assert_row(&old_reader, "new-a", false);
            // Both inserts are one complete SQL/WAL transaction, rather than
            // independent host writes or an artificial flag assignment.
            store
                .commit_relational_transaction(
                    &mut catalog,
                    RelationalTransaction {
                        writes: vec![insert(&["new-a"]), insert(&["new-b"])],
                    },
                )
                .unwrap();
            candidate.catch_up(&store).unwrap();
            candidate.finish_catch_up().unwrap();
            assert!(
                candidate.recovery_selectors.iter().all(Option::is_some),
                "the first relational suffix must prepare both recovery selectors"
            );
            // A later graph-only suffix must retain relational recovery and
            // bind its new complete source fence, including the earlier SQL.
            store
                .create_node(&mut catalog, "Memory", BTreeMap::new())
                .unwrap();
            candidate.catch_up(&store).unwrap();
            candidate.finish_catch_up().unwrap();
            assert!(
                candidate.recovery_selectors.iter().all(Option::is_some),
                "a later unchanged suffix must not forget earlier relational replay"
            );
            let next = candidate.store.as_ref().unwrap();
            next.validate_authoritative_relational_index_open().unwrap();
            let current_reader = next.open_relational_row_snapshot_reader().unwrap().unwrap();
            assert_eq!(
                current_reader.identity().visible_commit_epoch,
                store.commit_epoch()
            );
            for key in ["kept", "new-a", "new-b"] {
                assert_row(&current_reader, key, true);
            }
            assert_eq!(old_reader.identity().visible_commit_epoch, old_epoch);
            assert_row(&old_reader, "kept", true);
            for key in ["new-a", "new-b"] {
                assert_row(&old_reader, key, false);
            }
            let expected = store
                .node_records_owned()
                .collect::<crate::Result<Vec<_>>>()
                .unwrap();
            store
                .publish_checkpoint_candidate(&mut candidate, None, &Default::default())
                .unwrap();
            assert_eq!(
                store
                    .node_records_owned()
                    .collect::<crate::Result<Vec<_>>>()
                    .unwrap(),
                expected
            );
            for key in ["kept", "new-a", "new-b"] {
                assert_row(&current_reader, key, true);
            }
            for key in ["new-a", "new-b"] {
                assert_row(&old_reader, key, false);
            }
            drop(current_reader);
            drop(old_reader);
            drop(candidate);
            drop(pinned);
            drop(store);
            let reopened = GraphStore::open_with_durability_and_replay_config(
                &root,
                &mut catalog,
                policy,
                replay,
            )
            .unwrap();
            assert_eq!(
                reopened
                    .node_records_owned()
                    .collect::<crate::Result<Vec<_>>>()
                    .unwrap(),
                expected
            );
            assert_eq!(reopened.relational_state().total_row_count(), 3);
            reopened
                .validate_authoritative_relational_index_open()
                .unwrap();
            let recovered_reader = reopened
                .open_relational_row_snapshot_reader()
                .unwrap()
                .unwrap();
            for key in ["kept", "new-a", "new-b"] {
                assert_row(&recovered_reader, key, true);
            }
            drop(recovered_reader);
            drop(reopened);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}
