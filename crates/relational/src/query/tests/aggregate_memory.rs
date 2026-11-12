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

//! A whole-table `COUNT`/`SUM` accumulates while it scans (#1679). Raising the
//! intermediate-row ceiling admits more input rows, not more query memory.

use super::*;

const SMALL_ROWS: i64 = 10_000;
const LARGE_ROWS: i64 = 200_000;
const INSERT_BATCH: i64 = 5_000;

fn content(id: i64) -> String {
    "c".repeat(8 + (id % 41) as usize)
}

fn content_state(rows: i64) -> RelationalState {
    let mut state = RelationalState::default();
    let create = compile_relational_statement_sql(
        "CREATE TABLE content_rows (id BIGINT PRIMARY KEY, content TEXT NOT NULL)",
        &[],
        &state,
    )
    .unwrap();
    state = state
        .stage_transaction(create, Default::default(), Default::default())
        .unwrap();
    for start in (0..rows).step_by(INSERT_BATCH as usize) {
        let values = (start..(start + INSERT_BATCH).min(rows))
            .map(|id| format!("({id}, '{}')", content(id)))
            .collect::<Vec<_>>()
            .join(", ");
        let insert = compile_relational_statement_sql(
            &format!("INSERT INTO content_rows (id, content) VALUES {values}"),
            &[],
            &state,
        )
        .unwrap();
        state = state
            .stage_transaction(insert, Default::default(), Default::default())
            .unwrap();
    }
    state
}

/// One output row; the scan bound is independent of the output bound. Row
/// reads are budgeted from the intermediate rows, as the facade derives them.
fn limits(max_intermediate_rows: usize) -> RelationalQueryLimits {
    let rows = NonZeroUsize::new(max_intermediate_rows.saturating_add(1)).unwrap();
    let defaults = batched_index_join_limits();
    RelationalQueryLimits {
        max_output_rows: 1,
        max_intermediate_rows,
        max_candidate_work: max_intermediate_rows,
        hydration: RelationalHydrationBudget {
            max_rows: rows.get(),
            ..defaults.hydration
        },
        row_read: hawdb_storage::relational::RelationalRowPageSnapshotReadLimits {
            demand: hawdb_storage::relational::RelationalRowPageDemandReadLimits {
                max_rows: rows,
                ..defaults.row_read.demand
            },
            max_overlay_entries: rows,
            ..defaults.row_read
        },
        ..defaults
    }
}

struct Measured {
    rows: Vec<Row>,
    intermediate_rows: usize,
    peak_bytes: usize,
}

fn run(state: &RelationalState, sql: &str, limits: RelationalQueryLimits) -> Result<Measured> {
    let SqlStatement::Select(select) = hawdb_sql::prepare_postgres_sql(sql)?.statement else {
        panic!("expected SELECT statement");
    };
    let read_modes = RelationalQueryReadModes::new(
        RelationalIndexReadMode::<crate::RelationalMaterializedReader>::Materialized,
        RelationalRowReadMode::<crate::RelationalMaterializedReader>::CanonicalMemory,
    );
    let prepared = prepare_relational_select(
        select,
        &[],
        state,
        read_modes,
        limits,
        RelationalJoinPlanningContext::default(),
        RelationalSqlStageTimings::default(),
    )?;
    let memory = hawdb_executor::ExecutionMemoryConfig::default();
    let execution = prepared.execution.admit(
        state,
        read_modes,
        RelationalQueryResourceContext::new(
            RelationalJoinEnumerationConfig::default(),
            limits,
            &memory,
            None,
        ),
    )?;
    let ledger = execution.memory_ledger.clone();
    let output = execute_select(&prepared, &[], execution)?;
    let snapshot = ledger.snapshot();
    assert_eq!(snapshot.used_bytes, 0, "{sql}: released all query memory");
    Ok(Measured {
        rows: output.rows.into_rows(),
        intermediate_rows: output.intermediate_rows,
        peak_bytes: snapshot.peak_bytes,
    })
}

#[test]
fn whole_table_count_and_sum_peak_memory_is_independent_of_row_count() {
    // The plain statement uses the columnar aggregate; HAVING selects the
    // row-at-a-time aggregate. Both must accumulate without retaining rows.
    let queries = [
        "SELECT COUNT(*) AS row_count, SUM(OCTET_LENGTH(content)) AS content_bytes FROM content_rows",
        "SELECT COUNT(*) AS row_count, SUM(OCTET_LENGTH(content)) AS content_bytes FROM content_rows HAVING COUNT(*) > 0",
    ];
    let small = content_state(SMALL_ROWS);
    let large = content_state(LARGE_ROWS);
    for sql in queries {
        let mut peaks = Vec::new();
        for (state, rows) in [(&small, SMALL_ROWS), (&large, LARGE_ROWS)] {
            let row_count = usize::try_from(rows).unwrap();
            let expected_bytes = (0..rows).map(|id| content(id).len() as i64).sum::<i64>();
            let measured = run(state, sql, limits(row_count)).unwrap();
            assert_eq!(
                measured.rows,
                vec![Row::from([
                    ("row_count".into(), Value::Int(rows)),
                    ("content_bytes".into(), Value::Int(expected_bytes)),
                ])],
                "{sql}"
            );
            assert_eq!(measured.intermediate_rows, row_count, "{sql}");
            peaks.push(measured.peak_bytes);
            // The same statement is refused one row below the table size.
            let error = run(state, sql, limits(row_count - 1)).err().unwrap();
            assert!(
                error.to_string().contains("max_intermediate_rows"),
                "{sql}: {error}"
            );
        }
        let (small_peak, large_peak) = (peaks[0], peaks[1]);
        // Twenty times the input rows must not grow the ledger peak beyond a
        // small constant (an aggregate state or batch rounding difference).
        assert!(
            large_peak <= small_peak.saturating_add(1024),
            "{sql}: peak grew from {small_peak} bytes ({SMALL_ROWS} rows) to {large_peak} bytes ({LARGE_ROWS} rows)"
        );
    }
}
