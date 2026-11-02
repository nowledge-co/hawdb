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

//! The resident row byte total is maintained incrementally
//! (nowledge-co/hawdb#1519). Every test compares it with the full O(rows)
//! fold after each kind of row mutation.

use super::*;
use crate::background::CheckpointDecodeContext;

fn assert_pages_exact(pages: &RelationalRowPages) {
    assert_eq!(pages.len(), pages.iter().count());
    assert_eq!(pages.entry_bytes(), pages.recomputed_entry_bytes());
}

fn assert_state_exact(state: &RelationalState) {
    for segment in state.segments.values() {
        assert_pages_exact(&segment.rows);
    }
    assert_eq!(
        state.estimated_materialized_row_bytes(),
        state.recomputed_materialized_row_bytes()
    );
}

fn key(id: i64) -> RelationalKey {
    RelationalKey(vec![RelationalValue::BigInt(id)])
}

fn row(id: i64, payload_bytes: usize) -> RelationalRow {
    RelationalRow::new(vec![
        RelationalValue::BigInt(id),
        RelationalValue::Text("p".repeat(payload_bytes)),
    ])
}

fn unmetered_context() -> CheckpointDecodeContext {
    CheckpointDecodeContext {
        work: Default::default(),
        memory: Default::default(),
    }
}

#[test]
fn row_pages_maintain_exact_entry_bytes_across_every_mutation() {
    let mut pages = RelationalRowPages::default();
    assert_eq!(pages.entry_bytes(), 0);

    // Insert into an empty map, then grow past the entry-count split.
    for id in 0..600 {
        assert!(pages.insert(key(id), row(id, (id % 17) as usize)).is_none());
        assert_pages_exact(&pages);
    }
    assert!(pages.page_count() > 1);

    // Replacement keeps the stored key and swaps only the row bytes.
    assert!(pages.insert(key(7), row(7, 4096)).is_some());
    assert_pages_exact(&pages);
    assert!(pages.insert(key(7), row(7, 0)).is_some());
    assert_pages_exact(&pages);

    // A row above the byte target splits its page by size.
    assert!(pages
        .insert(key(300), row(300, RELATIONAL_ROW_PAGE_TARGET_BYTES + 1))
        .is_some());
    assert_pages_exact(&pages);

    // Removal, including a missing key and emptying a whole page.
    assert!(pages.remove(&key(10_000)).is_none());
    assert_pages_exact(&pages);
    let before = pages.page_count();
    for id in 0..300 {
        assert!(pages.remove(&key(id)).is_some());
        assert_pages_exact(&pages);
    }
    assert!(pages.page_count() < before);

    // A COW clone keeps its own total while the source stays unchanged.
    let snapshot = pages.clone();
    let snapshot_bytes = snapshot.entry_bytes();
    pages.insert(key(5_000), row(5_000, 99));
    pages.remove(&key(301));
    assert_pages_exact(&pages);
    assert_pages_exact(&snapshot);
    assert_eq!(snapshot.entry_bytes(), snapshot_bytes);

    // Checkpoint-allocated insert, replacement and page copy.
    let context = unmetered_context();
    assert!(pages
        .insert_with_allocations(key(6_000), row(6_000, 5), Some(&context))
        .unwrap()
        .is_none());
    assert_pages_exact(&pages);
    assert!(pages
        .insert_with_allocations(key(6_000), row(6_000, 500), Some(&context))
        .unwrap()
        .is_some());
    assert_pages_exact(&pages);
    for id in 7_000..7_300 {
        pages
            .insert_with_allocations(key(id), row(id, 3), Some(&context))
            .unwrap();
    }
    assert_pages_exact(&pages);
    let shared = pages.clone();
    pages
        .prepare_checkpoint_copy_for_key(&key(7_100), &context.work)
        .unwrap();
    assert_pages_exact(&pages);
    assert_eq!(pages.entry_bytes(), shared.entry_bytes());

    // Schema rewrites change every row.
    let fill = RelationalValue::Text("default".into());
    pages.append_value_to_all(&fill);
    assert_pages_exact(&pages);
    pages
        .append_value_to_all_with_allocations(&RelationalValue::BigInt(1), &context)
        .unwrap();
    assert_pages_exact(&pages);
    assert_pages_exact(&shared);

    // Bulk construction.
    let map = pages
        .iter()
        .map(|(key, row)| (key.clone(), row.clone()))
        .collect::<BTreeMap<_, _>>();
    let rebuilt = RelationalRowPages::from_map(map);
    assert_pages_exact(&rebuilt);
    assert_eq!(rebuilt.entry_bytes(), pages.entry_bytes());
}

#[test]
fn row_pages_entry_bytes_survive_a_deterministic_mutation_mix() {
    let mut pages = RelationalRowPages::default();
    let mut model = BTreeMap::new();
    let mut seed = 0x9e37_79b9_7f4a_7c15u64;
    for _ in 0..3_000 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let id = (seed % 700) as i64;
        let payload = ((seed >> 20) % 2_000) as usize;
        if seed.is_multiple_of(3) {
            assert_eq!(pages.remove(&key(id)), model.remove(&key(id)));
        } else {
            assert_eq!(
                pages.insert(key(id), row(id, payload)),
                model.insert(key(id), row(id, payload))
            );
        }
        assert_pages_exact(&pages);
    }
    assert_eq!(
        pages.entry_bytes(),
        RelationalRowPages::from_map(model).entry_bytes()
    );
}

#[test]
fn sparse_hydration_appends_maintain_entry_bytes() {
    let context = unmetered_context();
    let mut pages = RelationalRowPages::default();
    for id in 0..700 {
        assert!(pages
            .push_sorted_with_allocations(key(id), row(id, (id % 5) as usize), &context, 8)
            .unwrap()
            .is_none());
    }
    assert_pages_exact(&pages);
    // A repeated key is replaced (the caller reports it as corruption).
    assert!(pages
        .push_sorted_with_allocations(key(699), row(699, 4_000), &context, 8)
        .unwrap()
        .is_some());
    assert_pages_exact(&pages);
}

fn bigint_column(name: &str) -> RelationalColumnSchema {
    RelationalColumnSchema {
        name: name.to_string(),
        scalar_type: RelationalScalarType::BigInt,
        nullable: false,
        default: None,
    }
}

fn create_tables() -> RelationalTransaction {
    RelationalTransaction {
        writes: vec![
            create_upsert_table().writes.remove(0),
            // No primary key: rows use hidden row handles.
            RelationalWrite::CreateTable(RelationalTableSchema {
                materialized_view: None,
                name: "events".to_string(),
                columns: vec![bigint_column("at"), text_column("body", true)],
                primary_key: Vec::new(),
                unique_constraints: Vec::new(),
                foreign_keys: Vec::new(),
                indexes: Vec::new(),
            }),
        ],
    }
}

fn stage(state: &RelationalState, writes: Vec<RelationalWrite>) -> RelationalState {
    let next = state
        .stage_transaction(
            RelationalTransaction { writes },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("stage fixture transaction");
    assert_state_exact(&next);
    next
}

fn compare(column: &str, value: RelationalValue) -> RelationalPredicate {
    RelationalPredicate::Compare {
        column: column.to_string(),
        op: RelationalComparisonOp::Eq,
        value,
    }
}

#[test]
fn relational_state_row_bytes_track_transactions_rollback_and_recovery() {
    let empty = RelationalState::default();
    assert_state_exact(&empty);
    let base = stage(&empty, create_tables().writes);
    let base = stage(
        &base,
        vec![
            RelationalWrite::Insert {
                table: "documents".to_string(),
                rows: (0..400)
                    .map(|id| {
                        upsert_row(
                            &format!("id-{id:04}"),
                            &format!("owner-{id:04}"),
                            &"x".repeat(id % 23),
                        )
                    })
                    .collect(),
                mode: RelationalInsertMode::Error,
            },
            RelationalWrite::Insert {
                table: "events".to_string(),
                rows: (0..300)
                    .map(|at| {
                        RelationalRow::new(vec![
                            RelationalValue::BigInt(at),
                            RelationalValue::Text("e".repeat(at as usize % 11)),
                        ])
                    })
                    .collect(),
                mode: RelationalInsertMode::Error,
            },
        ],
    );
    let base_bytes = base.estimated_materialized_row_bytes();
    assert!(base_bytes > 0);

    let updated = stage(
        &base,
        vec![RelationalWrite::UpdateWhere {
            table: "documents".to_string(),
            assignments: vec![RelationalUpdateAssignment {
                column: "payload".to_string(),
                value: RelationalUpdateValue::Value(RelationalValue::Text("y".repeat(1_000))),
            }],
            predicate: compare("id", RelationalValue::Text("id-0007".into())),
        }],
    );
    assert!(updated.estimated_materialized_row_bytes() > base_bytes);
    // Staging is copy-on-write: the source state is untouched.
    assert_state_exact(&base);
    assert_eq!(base.estimated_materialized_row_bytes(), base_bytes);

    // Primary-key change moves a row to a new key.
    let rekeyed = stage(
        &updated,
        vec![RelationalWrite::UpdateWhere {
            table: "documents".to_string(),
            assignments: vec![RelationalUpdateAssignment {
                column: "id".to_string(),
                value: RelationalUpdateValue::Value(RelationalValue::Text(
                    "id-moved-to-a-longer-key".into(),
                )),
            }],
            predicate: compare("id", RelationalValue::Text("id-0008".into())),
        }],
    );

    let upserted = stage(
        &rekeyed,
        vec![
            RelationalWrite::Upsert {
                table: "documents".to_string(),
                rows: vec![
                    upsert_row("id-0009", "owner-0009", "short"),
                    upsert_row("id-9999", "owner-9999", "new"),
                ],
                conflict_columns: vec!["id".to_string()],
                action: RelationalConflictAction::Update(vec![RelationalUpsertAssignment {
                    column: "payload".to_string(),
                    value: RelationalUpsertValue::ExcludedColumn("payload".to_string()),
                }]),
            },
            RelationalWrite::Upsert {
                table: "documents".to_string(),
                rows: vec![upsert_row("id-0010", "owner-0010", "ignored")],
                conflict_columns: vec!["id".to_string()],
                action: RelationalConflictAction::DoNothing,
            },
        ],
    );

    let deleted = stage(
        &upserted,
        vec![
            RelationalWrite::DeleteByPrimaryKey {
                table: "documents".to_string(),
                keys: (20..120)
                    .map(|id| RelationalKey(vec![RelationalValue::Text(format!("id-{id:04}"))]))
                    .collect(),
            },
            RelationalWrite::DeleteWhere {
                table: "events".to_string(),
                predicate: compare("body", RelationalValue::Text(String::new())),
            },
            RelationalWrite::UpdateWhere {
                table: "events".to_string(),
                assignments: vec![RelationalUpdateAssignment {
                    column: "body".to_string(),
                    value: RelationalUpdateValue::Value(RelationalValue::Null),
                }],
                predicate: compare("at", RelationalValue::BigInt(5)),
            },
        ],
    );

    let altered = stage(
        &deleted,
        vec![
            RelationalWrite::AddColumn {
                table: "documents".to_string(),
                column: RelationalColumnSchema {
                    name: "note".to_string(),
                    scalar_type: RelationalScalarType::Text,
                    nullable: true,
                    default: Some(RelationalColumnDefault::Literal(RelationalValue::Text(
                        "note".into(),
                    ))),
                },
            },
            RelationalWrite::CreateIndex {
                table: "events".to_string(),
                index: RelationalIndexSchema {
                    name: "events_at".to_string(),
                    columns: vec!["at".to_string()],
                    unique: false,
                },
            },
        ],
    );

    // A rejected transaction leaves its source state, and total, unchanged.
    let altered_bytes = altered.estimated_materialized_row_bytes();
    assert!(altered
        .stage_transaction(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::Insert {
                        table: "documents".to_string(),
                        rows: vec![upsert_row("id-fresh", "owner-fresh", "z")],
                        mode: RelationalInsertMode::Error,
                    },
                    RelationalWrite::Insert {
                        table: "documents".to_string(),
                        rows: vec![upsert_row("id-0001", "owner-0001", "duplicate")],
                        mode: RelationalInsertMode::Error,
                    },
                ],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .is_err());
    assert_state_exact(&altered);
    assert_eq!(altered.estimated_materialized_row_bytes(), altered_bytes);

    // Checkpoint recovery rebuilds the same total from decoded rows.
    let encoded = encode_relational_checkpoint(3, &altered).expect("encode checkpoint");
    let recovered = decode_relational_checkpoint(&encoded, RelationalDecodeLimits::checkpoint())
        .expect("decode checkpoint")
        .state;
    assert_state_exact(&recovered);
    assert_eq!(recovered.estimated_materialized_row_bytes(), altered_bytes);

    // Detaching rows records the maintained total for checkpoint sizing.
    let mut detached = recovered.clone();
    detached.omit_materialized_rows();
    assert_eq!(detached.estimated_materialized_row_bytes(), 0);
    assert_eq!(
        detached.estimated_checkpoint_bytes(),
        recovered.estimated_checkpoint_bytes()
    );
    assert_state_exact(&recovered);
}

#[test]
fn checkpoint_size_estimate_does_not_walk_resident_rows() {
    let state = stage(&RelationalState::default(), create_tables().writes);
    let state = stage(
        &state,
        vec![RelationalWrite::Insert {
            table: "events".to_string(),
            rows: (0..5_000)
                .map(|at| {
                    RelationalRow::new(vec![
                        RelationalValue::BigInt(at),
                        RelationalValue::Text("body".into()),
                    ])
                })
                .collect(),
            mode: RelationalInsertMode::Error,
        }],
    );
    let before = row_entry_byte_probe::visits();
    let estimated = state.estimated_checkpoint_bytes();
    let materialized = state.estimated_materialized_row_bytes();
    assert_eq!(row_entry_byte_probe::visits(), before);
    assert_eq!(materialized, state.recomputed_materialized_row_bytes());
    assert!(estimated >= materialized);
    // Positive control: a real row walk is visible to the probe.
    let _ = state.estimated_checkpoint_admission_bytes_with_visit(&mut || {
        Ok::<_, std::convert::Infallible>(())
    });
    assert!(row_entry_byte_probe::visits() >= before + 5_000);
}
