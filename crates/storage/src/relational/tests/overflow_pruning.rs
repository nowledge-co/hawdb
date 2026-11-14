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

//! Overflow-segment pruning after a transaction must cost the rows the
//! transaction changed, not the database (#1439), and stay exact.

use super::*;

fn always_overflow() -> RelationalOverflowConfig {
    RelationalOverflowConfig {
        threshold_bytes: 1,
        ..RelationalOverflowConfig::default()
    }
}

fn stage(state: &RelationalState, writes: Vec<RelationalWrite>) -> RelationalState {
    state
        .stage_transaction(
            RelationalTransaction { writes },
            RelationalMutationLimits::default(),
            always_overflow(),
        )
        .expect("stage relational transaction")
}

/// Rows examined for overflow reachability while staging `writes`.
fn stage_counted(state: &RelationalState, writes: Vec<RelationalWrite>) -> (RelationalState, u64) {
    let before = overflow::relational_overflow_reachability_rows_visited();
    let next = stage(state, writes);
    let visited = overflow::relational_overflow_reachability_rows_visited() - before;
    (next, visited)
}

fn message(id: usize, payload: &str) -> RelationalRow {
    RelationalRow::new(vec![
        RelationalValue::Text(format!("message-{id:06}")),
        RelationalValue::Text(payload.to_string()),
    ])
}

fn message_key(id: usize) -> RelationalKey {
    RelationalKey(vec![RelationalValue::Text(format!("message-{id:06}"))])
}

fn insert_messages(rows: Vec<RelationalRow>) -> RelationalWrite {
    RelationalWrite::Insert {
        table: "messages".to_string(),
        rows,
        mode: RelationalInsertMode::Error,
    }
}

fn delete_messages(ids: &[usize]) -> RelationalWrite {
    RelationalWrite::DeleteByPrimaryKey {
        table: "messages".to_string(),
        keys: ids.iter().copied().map(message_key).collect(),
    }
}

fn set_payload(id: usize, payload: &str) -> RelationalWrite {
    RelationalWrite::UpdateWhere {
        table: "messages".to_string(),
        assignments: vec![RelationalUpdateAssignment {
            column: "payload".to_string(),
            value: RelationalUpdateValue::Value(RelationalValue::Text(payload.to_string())),
        }],
        predicate: RelationalPredicate::Compare {
            column: "id".to_string(),
            op: RelationalComparisonOp::Eq,
            value: RelationalValue::Text(format!("message-{id:06}")),
        },
    }
}

fn assert_checkpoint_accepts(state: &RelationalState) {
    // Checkpoint encode/decode rejects unreachable or missing segments.
    let mut checkpoint = std::io::Cursor::new(Vec::new());
    encode_relational_checkpoint_to_writer(
        &mut checkpoint,
        1,
        state,
        RelationalDecodeLimits::checkpoint().max_record_bytes,
    )
    .expect("exact overflow segments encode");
    let decoded =
        decode_relational_checkpoint(checkpoint.get_ref(), RelationalDecodeLimits::checkpoint())
            .expect("exact overflow segments decode");
    assert_eq!(
        decoded.state.overflow_segment_count(),
        state.overflow_segment_count()
    );
}

#[test]
fn overflow_pruning_work_per_insert_commit_does_not_grow_with_table_size() {
    const BATCH: usize = 50;
    const BATCHES: usize = 40;
    let mut state = stage(&RelationalState::default(), create_payload_table().writes);
    let mut per_commit = Vec::with_capacity(BATCHES);
    for batch in 0..BATCHES {
        let rows = (batch * BATCH..(batch + 1) * BATCH)
            .map(|id| message(id, &format!("payload-{id}")))
            .collect();
        let (next, visited) = stage_counted(&state, vec![insert_messages(rows)]);
        state = next;
        per_commit.push(visited);
    }
    assert_eq!(state.overflow_segment_count(), BATCH * BATCHES);
    // Each insert commit examines its own rows only: a full reachability pass
    // over the table would examine every row committed so far.
    assert!(
        per_commit
            .iter()
            .all(|visited| *visited <= 2 * BATCH as u64),
        "per-commit overflow pruning work grew with the table: {per_commit:?}"
    );
    assert_eq!(per_commit.first(), per_commit.last());

    // An update that keeps the overflow value examines only the changed row
    // (old and new image).
    let (next, visited) = stage_counted(&state, vec![set_payload(7, "payload-7")]);
    assert_eq!(visited, 2, "update examined {visited} rows");
    assert_eq!(next.overflow_segment_count(), BATCH * BATCHES);
    // Releasing a value still decides reachability over the rows, since an
    // unchanged row may share the content-addressed segment.
    let next = stage(&next, vec![set_payload(8, "replaced-8")]);
    assert_eq!(next.overflow_segment_count(), BATCH * BATCHES);
    assert_checkpoint_accepts(&next);
}

#[test]
fn overflow_pruning_keeps_shared_segments_until_their_last_reference() {
    let state = stage(&RelationalState::default(), create_payload_table().writes);
    let state = stage(
        &state,
        vec![insert_messages(vec![
            message(1, "shared"),
            message(2, "shared"),
            message(3, "solo"),
            message(4, "moved"),
        ])],
    );
    assert_eq!(state.overflow_segment_count(), 3);

    // One of two references goes: the shared segment stays.
    let state = stage(&state, vec![delete_messages(&[1])]);
    assert_eq!(state.overflow_segment_count(), 3);
    assert_checkpoint_accepts(&state);

    // The last reference goes: the segment is pruned.
    let state = stage(&state, vec![delete_messages(&[2])]);
    assert_eq!(state.overflow_segment_count(), 2);

    // A replaced value is pruned and its replacement kept.
    let state = stage(&state, vec![set_payload(3, "solo-v2")]);
    assert_eq!(state.overflow_segment_count(), 2);
    let mut budget = RelationalHydrationBudget::default();
    let row = state
        .hydrate_row("messages", &message_key(3), &mut budget)
        .expect("hydrate updated row")
        .expect("updated row");
    assert_eq!(
        row.values()[1],
        RelationalValue::Text("solo-v2".to_string())
    );

    // A value released by one changed row and taken by another in the same
    // transaction is kept; a value inserted and deleted in it is not kept.
    let state = stage(
        &state,
        vec![
            delete_messages(&[4]),
            insert_messages(vec![message(5, "moved"), message(6, "transient")]),
            delete_messages(&[6]),
        ],
    );
    assert_eq!(state.overflow_segment_count(), 2);
    let row = state
        .hydrate_row("messages", &message_key(5), &mut budget)
        .expect("hydrate moved row")
        .expect("moved row");
    assert_eq!(row.values()[1], RelationalValue::Text("moved".to_string()));
    assert_checkpoint_accepts(&state);

    let state = stage(&state, vec![delete_messages(&[3, 5])]);
    assert_eq!(state.overflow_segment_count(), 0);
    assert_checkpoint_accepts(&state);
}

#[test]
fn overflow_pruning_drops_values_of_rows_an_upsert_did_not_store() {
    let state = stage(&RelationalState::default(), create_upsert_table().writes);
    let state = stage(
        &state,
        vec![RelationalWrite::Insert {
            table: "documents".to_string(),
            rows: vec![upsert_row("id-1", "owner-1", "kept")],
            mode: RelationalInsertMode::Error,
        }],
    );
    assert_eq!(state.overflow_segment_count(), 1);
    let (state, _) = stage_counted(
        &state,
        vec![RelationalWrite::Upsert {
            table: "documents".to_string(),
            rows: vec![upsert_row("id-2", "owner-1", "never stored")],
            conflict_columns: vec!["owner".to_string()],
            action: RelationalConflictAction::DoNothing,
        }],
    );
    assert_eq!(state.overflow_segment_count(), 1);
    assert_checkpoint_accepts(&state);
}

#[test]
fn overflow_pruning_after_whole_table_changes_stays_exact() {
    let state = stage(&RelationalState::default(), create_payload_table().writes);
    let state = stage(
        &state,
        vec![insert_messages(vec![message(1, "one"), message(2, "two")])],
    );
    let state = stage(
        &state,
        vec![RelationalWrite::AddColumn {
            table: "messages".to_string(),
            column: RelationalColumnSchema {
                default: Some(RelationalColumnDefault::Literal(RelationalValue::Text(
                    "filled".to_string(),
                ))),
                ..text_column("note", true)
            },
        }],
    );
    assert_eq!(state.overflow_segment_count(), 3);
    assert_checkpoint_accepts(&state);
}
