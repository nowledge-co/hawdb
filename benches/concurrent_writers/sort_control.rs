// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Bounded read-only controls for single-column sort-key preparation.

use hawdb::{Database, Value};
use serde_json::json;
use std::collections::BTreeMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::Instant;

const UNSORTED: &str = "MATCH (n:SortControl) RETURN n.id AS id, n.sort_value AS sort_value";
const SORTED: &str =
    "MATCH (n:SortControl) RETURN n.id AS id, n.sort_value AS sort_value ORDER BY sort_value";
const REPEATS: usize = 8;

// Keep the identical fixture usable with the original admission baseline,
// whose begin methods returned the transaction directly.
trait CheckedTransaction<'a> {
    fn checked(self) -> hawdb::DatabaseTransaction<'a>;
}

impl<'a> CheckedTransaction<'a> for hawdb::DatabaseTransaction<'a> {
    fn checked(self) -> hawdb::DatabaseTransaction<'a> {
        self
    }
}

impl<'a> CheckedTransaction<'a> for hawdb::Result<hawdb::DatabaseTransaction<'a>> {
    fn checked(self) -> hawdb::DatabaseTransaction<'a> {
        self.unwrap()
    }
}

trait CheckedReadTransaction {
    fn checked(self) -> hawdb::DatabaseReadTransaction;
}

impl CheckedReadTransaction for hawdb::DatabaseReadTransaction {
    fn checked(self) -> hawdb::DatabaseReadTransaction {
        self
    }
}

impl CheckedReadTransaction for hawdb::Result<hawdb::DatabaseReadTransaction> {
    fn checked(self) -> hawdb::DatabaseReadTransaction {
        self.unwrap()
    }
}

fn key(kind: &str, rows: usize, id: usize) -> Value {
    let rank = u32::try_from(rows - id).unwrap();
    match kind {
        "integer" => Value::Int(i64::from(rank)),
        "float" => Value::Float(f64::from(rank) / 2.0),
        "string" => Value::String(format!("{rank:08}-{}", "x".repeat(247))),
        "null" => Value::Null,
        "mixed" if id.is_multiple_of(2) => Value::Int(i64::from(rank)),
        "mixed" => Value::Float(f64::from(rank) / 2.0),
        _ => unreachable!("control kind is fixed by the protocol"),
    }
}

pub(super) fn run() -> Result<(), &'static str> {
    if super::SMOKE {
        return Err("sort controls require the global release profile");
    }
    for rows in [256, 4096] {
        for kind in ["integer", "float", "string", "null", "mixed"] {
            let mut database = Database::new();
            let setup = Instant::now();
            let mut transaction = database.begin_transaction().checked();
            for id in 0..rows {
                transaction
                    .query_with_params(
                        "CREATE (:SortControl {id: $id, sort_value: $sort_value})",
                        &BTreeMap::from([
                            ("id".into(), Value::Int(i64::try_from(id).unwrap())),
                            ("sort_value".into(), key(kind, rows, id)),
                        ]),
                    )
                    .unwrap();
            }
            transaction.commit().unwrap();
            let setup_ns = super::nanos(setup);
            let plan = database.explain_query(SORTED).unwrap().physical_plan;
            let physical_plan = format!("{plan:?}");
            assert!(physical_plan.contains("SortExec"), "{physical_plan}");
            assert!(
                physical_plan.contains("Column(\"sort_value\")"),
                "{physical_plan}"
            );
            let mut snapshot = database.begin_read_transaction().checked();
            let mut expected = snapshot
                .query(UNSORTED)
                .unwrap()
                .rows
                .iter()
                .map(|row| {
                    BTreeMap::from([
                        ("id".to_owned(), row["id"].clone()),
                        ("sort_value".to_owned(), row["sort_value"].clone()),
                    ])
                })
                .collect::<Vec<_>>();
            assert_eq!(expected.len(), rows);
            let mut identities = Vec::with_capacity(rows);
            for row in &expected {
                let Value::Int(id) = &row["id"] else {
                    panic!("control identity changed type");
                };
                let id = usize::try_from(*id).unwrap();
                assert!(id < rows);
                assert_eq!(row["sort_value"], key(kind, rows, id));
                identities.push(id);
            }
            identities.sort_unstable();
            assert_eq!(identities, (0..rows).collect::<Vec<_>>());
            // Dataset identity is independent of unspecified source ordering.
            let mut identity_rows = expected.iter().collect::<Vec<_>>();
            identity_rows.sort_by(|left, right| left["id"].cmp(&right["id"]));
            let mut checksum = DefaultHasher::new();
            identity_rows.hash(&mut checksum);
            drop(identity_rows);
            // The bounded canonical input is the stable reference for ties.
            expected.sort_by(|left, right| left["sort_value"].cmp(&right["sort_value"]));
            let mut samples = Vec::with_capacity(REPEATS);
            let mut warm_up_ns = 0;
            for repetition in 0..=REPEATS {
                let started = Instant::now();
                let output = snapshot.query(SORTED).unwrap();
                let elapsed = super::nanos(started);
                assert_eq!(output.rows, expected);
                if repetition == 0 {
                    warm_up_ns = elapsed;
                } else {
                    samples.push(elapsed);
                }
            }
            println!(
                "{}",
                json!({
                    "schema": "sort-key-preparation-control-v1",
                    "scope": "resident read-only pinned snapshot; bounded canonical reference",
                    "kind": kind, "rows": rows, "iterations": REPEATS,
                    "physical_plan": physical_plan, "setup_ns": setup_ns,
                    "warm_up_ns": warm_up_ns, "latency": super::latency(&samples),
                    "typed_rows_exact": true,
                    "checksum_scope": "canonical dataset sorted by explicit id; result order checked separately",
                "checksum": format!("{:016x}", checksum.finish()),
                })
            );
        }
    }
    Ok(())
}
