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
use crate::append_table::{
    AppendLiveBatch, AppendMutationLimits, AppendOrderMode, AppendTableSchema, AppendTransaction,
    AppendWrite,
};
use crate::background::CheckpointWorkProbe;
use crate::relational::{
    RelationalColumnSchema, RelationalKey, RelationalRow, RelationalScalarType, RelationalValue,
};
use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::Arc;

fn scheduler() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(4),
        ..LocalQosPolicy::default()
    })
}

fn schema(table: &str) -> AppendTableSchema {
    AppendTableSchema {
        name: table.into(),
        columns: [
            ("stream", RelationalScalarType::Text),
            ("sequence", RelationalScalarType::BigInt),
            ("payload", RelationalScalarType::Bytea),
        ]
        .into_iter()
        .map(|(name, scalar_type)| RelationalColumnSchema {
            name: name.into(),
            scalar_type,
            nullable: false,
            default: None,
        })
        .collect(),
        partition_key: vec!["stream".into()],
        order_key: vec!["sequence".into()],
        order_mode: AppendOrderMode::CallerProvided,
    }
}

fn row(sequence: i64) -> AppendTableRow {
    let table = if sequence % 2 == 0 { "a" } else { "z" };
    let partition = format!("s{}🦀", sequence % 7);
    AppendTableRow {
        table: table.into(),
        partition_key: RelationalKey(vec![RelationalValue::Text(partition.clone())]),
        order_key: RelationalKey(vec![RelationalValue::BigInt(sequence)]),
        row: RelationalRow::new(vec![
            RelationalValue::Text(partition),
            RelationalValue::BigInt(sequence),
            RelationalValue::Bytea(vec![sequence as u8; 17]),
        ]),
    }
}

fn live_state(count: i64) -> AppendState {
    let mut state = AppendState::default()
        .stage_transaction(
            &AppendTransaction {
                writes: ["a", "z"]
                    .into_iter()
                    .map(|table| AppendWrite::CreateTable {
                        schema: schema(table),
                    })
                    .collect(),
            },
            AppendMutationLimits::default(),
        )
        .unwrap();
    for start in (0..count).step_by(1024) {
        let end = (start + 1024).min(count);
        let writes = ["z", "a"]
            .into_iter()
            .map(|table| AppendWrite::Append {
                table: table.into(),
                rows: (start..end)
                    .map(row)
                    .filter(|row| row.table == table)
                    .map(|row| row.row)
                    .collect(),
            })
            .collect();
        state = state
            .stage_transaction(
                &AppendTransaction { writes },
                AppendMutationLimits::default(),
            )
            .unwrap();
    }
    state
}

fn assert_stopped<T: std::fmt::Debug>(result: Result<T, AppendTableError>) {
    assert!(
        matches!(&result, Err(AppendTableError::Admission(message)) if message.contains("checkpoint build stopped")),
        "{result:?}"
    );
}

#[test]
fn checkpoint_units_append_capture_preserves_every_table_partition_and_row() {
    let state = live_state(5127);
    let expected = ["a", "z"]
        .into_iter()
        .flat_map(|table| {
            (0..7).flat_map(move |partition| {
                (0..5127).map(row).filter(move |row| {
                    row.table == table
                        && row.partition_key
                            == RelationalKey(vec![RelationalValue::Text(format!("s{partition}🦀"))])
                })
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(expected.len(), 5127);
    assert_eq!(state.checkpoint_rows(5127).unwrap(), expected);
    let scheduler = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let actual = state
        .checkpoint_rows_with_work_context(5127, &probe.context(scheduler.clone()))
        .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(probe.peak_units.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(probe.io_waves.load(AtomicOrdering::SeqCst), 0);
    probe.assert_released(&scheduler);
}

#[test]
fn checkpoint_units_append_capture_sort_and_merge_cancel_then_retry() {
    let state = live_state(6145);
    let expected = state.checkpoint_rows(6145).unwrap();
    let scheduler = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    probe.cancel_after.store(17, AtomicOrdering::SeqCst);
    assert_stopped(
        state.checkpoint_rows_with_work_context(6145, &probe.context(scheduler.clone())),
    );
    assert_eq!(probe.completed.load(AtomicOrdering::SeqCst), 17);
    probe.assert_released(&scheduler);
    for limit in [2, 32] {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(limit, AtomicOrdering::SeqCst);
        // Seven sorting runs, seven admitted heads and one output allocation
        // precede the merge; 32 cancels after 17 actual merged records.
        assert_stopped(sort_rows_with_work_context(
            (0..6145).rev().map(row).collect(),
            &probe.context(scheduler.clone()),
        ));
        assert_eq!(probe.completed.load(AtomicOrdering::SeqCst), limit);
        assert_eq!(probe.peak_units.load(AtomicOrdering::SeqCst), 1);
        probe.assert_released(&scheduler);
    }
    assert_eq!(state.checkpoint_rows(6145).unwrap(), expected);
    let retry = Arc::new(CheckpointWorkProbe::default());
    assert_eq!(
        state
            .checkpoint_rows_with_work_context(6145, &retry.context(scheduler.clone()))
            .unwrap(),
        expected
    );
    retry.assert_released(&scheduler);
}

#[test]
fn checkpoint_units_append_capture_preserves_admission_and_cross_run_corruption_errors() {
    let state = live_state(4097);
    let scheduler = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    assert_eq!(
        state
            .checkpoint_rows_with_work_context(4096, &probe.context(scheduler.clone()))
            .map(|rows| rows.to_vec()),
        state.checkpoint_rows(4096)
    );
    assert_eq!(probe.completed.load(AtomicOrdering::SeqCst), 0);
    probe.assert_released(&scheduler);
    let mut rows = (0..4097).map(row).collect::<Vec<_>>();
    rows[4096] = rows[0].clone();
    let corrupt = AppendState {
        live_head: Some(Arc::new(AppendLiveBatch {
            rows: rows.into(),
            watermarks: Arc::new(BTreeMap::new()),
            previous: None,
            payload_bytes: 0,
        })),
        live_rows: 4097,
        ..AppendState::default()
    };
    let probe = Arc::new(CheckpointWorkProbe::default());
    let expected = corrupt.checkpoint_rows(4097).unwrap_err();
    assert!(matches!(expected, AppendTableError::Corruption(_)));
    assert_eq!(
        corrupt
            .checkpoint_rows_with_work_context(4097, &probe.context(scheduler.clone()))
            .unwrap_err(),
        expected
    );
    probe.assert_released(&scheduler);
    let corrupt = AppendState {
        live_rows: 4096,
        ..corrupt
    };
    assert_eq!(
        corrupt
            .checkpoint_rows_with_work_context(4097, &Default::default())
            .map(|rows| rows.to_vec()),
        corrupt.checkpoint_rows(4097)
    );
}

#[path = "memory_tests.rs"]
mod memory_tests;
