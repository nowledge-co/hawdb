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
    RelationalColumnSchema, RelationalInsertMode, RelationalKey, RelationalRow,
    RelationalScalarType, RelationalTableSchema, RelationalTransaction, RelationalValue,
    RelationalWrite,
};

fn bootstrap_rows(store: &mut GraphStore, catalog: &mut Catalog) {
    store
        .commit_relational_transaction(
            catalog,
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
                    RelationalWrite::Insert {
                        table: "documents".into(),
                        rows: vec![RelationalRow::new(vec![RelationalValue::Text(
                            "kept".into(),
                        )])],
                        mode: RelationalInsertMode::Error,
                    },
                ],
            },
        )
        .unwrap();
}

fn unchanged_prefix(existing_rows: bool) {
    for mode in [
        RelationalIndexMode::Shadow,
        RelationalIndexMode::Authoritative,
    ] {
        for policy in [
            DurabilityPolicy::SyncOnEveryWrite,
            DurabilityPolicy::SyncOnCheckpoint,
        ] {
            let root = std::env::temp_dir().join(format!(
                "hawdb-unchanged-relational-prefix-{}",
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
            if existing_rows {
                bootstrap_rows(&mut store, &mut catalog);
            }
            store
                .create_node(&mut catalog, "Memory", BTreeMap::new())
                .unwrap();
            // Authoritative mode requires a previously selected canonical
            // index binding. Bootstrap through the supported Shadow path and
            // reopen before capturing a new graph-only checkpoint debt.
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
                .set_node_property(&mut catalog, "Memory", None, "base", Value::Int(1))
                .unwrap();
            let pinned = store.checkpoint_source();
            let pinned_epoch = pinned.commit_epoch();
            let mut candidate = pinned
                .prepare_checkpoint_candidate(&catalog)
                .unwrap()
                .unwrap();
            for id in 0..3 {
                store
                    .create_node(
                        &mut catalog,
                        "Memory",
                        BTreeMap::from([("id".into(), Value::Int(id))]),
                    )
                    .unwrap();
                let source_identity = store.checkpoint_source_identity();
                candidate.catch_up(&store).unwrap();
                candidate.finish_catch_up().unwrap();
                assert!(
                    candidate.recovery_selectors.iter().all(Option::is_none),
                    "an unchanged relational prefix must not rewrite or synchronize empty recovery selectors"
                );
                assert_eq!(store.checkpoint_source_identity(), source_identity);
                assert_eq!(pinned.commit_epoch(), pinned_epoch);
                assert_eq!(pinned.scan_nodes(None).count(), 1);
                let next = candidate.store.as_ref().unwrap();
                next.validate_authoritative_relational_index_open().unwrap();
                let reader = next.open_relational_row_snapshot_reader().unwrap().unwrap();
                assert_eq!(reader.identity().visible_commit_epoch, store.commit_epoch());
                assert_eq!(
                    next.relational_row_pages
                        .current_read_view(next.commit_epoch())
                        .unwrap()
                        .live_batch_count(),
                    0
                );
                assert_eq!(
                    next.relational_row_pages
                        .current_read_view(next.commit_epoch())
                        .unwrap()
                        .live_entry_count(),
                    0
                );
                if existing_rows {
                    assert_eq!(next.relational_state().total_row_count(), 1);
                    assert_eq!(
                        next.relational_state().row(
                            "documents",
                            &RelationalKey(vec![RelationalValue::Text("kept".into())])
                        ),
                        Some(&RelationalRow::new(vec![RelationalValue::Text(
                            "kept".into()
                        )]))
                    );
                }
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
            assert_eq!(
                reopened.relational_state().total_row_count(),
                usize::from(existing_rows)
            );
            reopened
                .validate_authoritative_relational_index_open()
                .unwrap();
            drop(reopened);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}

#[test]
fn graph_only_prefix_keeps_empty_relational_roots_without_private_seals() {
    unchanged_prefix(false);
}

#[test]
fn graph_only_prefix_preserves_existing_relational_rows_without_private_seals() {
    unchanged_prefix(true);
}
