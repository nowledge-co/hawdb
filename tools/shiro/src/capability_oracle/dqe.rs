// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Differential Query Execution over a generated nullable two-column predicate.

use super::*;
use std::cmp::Ordering;

#[derive(Clone, Copy)]
struct Key(Option<i64>, Option<i64>);

impl Key {
    fn compare(self, other: Self) -> Option<Ordering> {
        let first = self.0?.cmp(&other.0?);
        if first == Ordering::Equal {
            Some(self.1?.cmp(&other.1?))
        } else {
            Some(first)
        }
    }
}

#[derive(Clone, Copy)]
enum Predicate {
    Less,
    AtMost,
    Greater,
    AtLeast,
}

impl Predicate {
    fn from_variant(variant: u64) -> Self {
        [Self::Less, Self::AtMost, Self::Greater, Self::AtLeast][variant as usize % 4]
    }

    fn sql(self) -> &'static str {
        match self {
            Self::Less => "<",
            Self::AtMost => "<=",
            Self::Greater => ">",
            Self::AtLeast => ">=",
        }
    }

    fn matches(self, order: Ordering) -> bool {
        match self {
            Self::Less => order.is_lt(),
            Self::AtMost => order.is_le(),
            Self::Greater => order.is_gt(),
            Self::AtLeast => order.is_ge(),
        }
    }
}

fn rows(ids: impl IntoIterator<Item = i64>) -> Vec<Row> {
    ids.into_iter()
        .map(|id| row(&[("id", Value::Int(id))]))
        .collect()
}

fn run_with_index(case: &Case, run: &mut Run, indexed: bool) -> Result<()> {
    let mut db = Database::new();
    let profile = if indexed { "indexed" } else { "unindexed" };
    let phase = |name| format!("dqe_{profile}_{name}");
    run.sql(
        &mut db,
        &phase("schema"),
        "CREATE TABLE items (id BIGINT PRIMARY KEY, grp BIGINT, score BIGINT, marker BIGINT NOT NULL)",
        &[],
    )?;
    if indexed {
        run.sql(
            &mut db,
            &phase("index"),
            "CREATE INDEX by_group_score ON items (grp, score)",
            &[],
        )?;
    }

    let bound = Key(
        (!case.variant.is_multiple_of(11)).then_some((case.variant % 5) as i64 - 2),
        (!case.variant.is_multiple_of(13)).then_some((case.variant % 7) as i64 - 3),
    );
    let reversed = case.variant & 4 != 0;
    let predicate = Predicate::from_variant(case.variant);
    let mut matching = Vec::new();
    let mut remaining = Vec::new();
    for (offset, value) in case.values.iter().copied().enumerate() {
        let id = offset as i64;
        let key = Key(
            (value % 7 != 0).then_some(value.rem_euclid(5) - 2),
            (value % 3 != 0).then_some(value),
        );
        run.sql(
            &mut db,
            &phase("insert"),
            "INSERT INTO items VALUES ($1, $2, $3, 0)",
            &[
                Value::Int(id),
                key.0.map_or(Value::Null, Value::Int),
                key.1.map_or(Value::Null, Value::Int),
            ],
        )?;
        let order = if reversed {
            bound.compare(key)
        } else {
            key.compare(bound)
        };
        if order.is_some_and(|order| predicate.matches(order)) {
            matching.push(id);
        } else {
            remaining.push(id);
        }
    }
    let (left, right) = if reversed {
        ("($1, $2)", "(grp, score)")
    } else {
        ("(grp, score)", "($1, $2)")
    };
    let where_clause = format!("{left} {} {right}", predicate.sql());
    let parameters = [
        bound.0.map_or(Value::Null, Value::Int),
        bound.1.map_or(Value::Null, Value::Int),
    ];
    let json_parameters = || json!(parameters.iter().map(typed_value_json).collect::<Vec<_>>());
    let select = format!("SELECT id FROM items WHERE {where_clause}");
    let selected = run.sql(&mut db, &phase("select"), &select, &parameters)?;
    run.equal(
        &phase("select_model"),
        &selected,
        &rows(matching.iter().copied()),
        false,
    )?;
    let unchanged = |run: &mut Run, db: &mut Database, name| -> Result<()> {
        let actual = run.sql(
            db,
            &phase(name),
            "SELECT id FROM items WHERE marker = 0",
            &[],
        )?;
        run.equal(
            &phase(name),
            &actual,
            &rows(0..case.values.len() as i64),
            false,
        )
    };

    let mut tx = db
        .begin_transaction()
        .map_err(|e| error(&phase("begin_update"), e))?;
    let update = format!("UPDATE items SET marker = 1 WHERE {where_clause} RETURNING id");
    let updated = run.output(
        &phase("update"),
        "sql",
        &update,
        json_parameters(),
        tx.query_sql_with_params(&update, &parameters),
    )?;
    run.equal(&phase("update_ids"), &updated, &selected, false)?;
    let marked = "SELECT id FROM items WHERE marker = 1";
    let marked_rows = run.output(
        &phase("marked"),
        "sql",
        marked,
        json!([]),
        tx.query_sql(marked),
    )?;
    run.equal(&phase("marked"), &marked_rows, &selected, false)?;
    tx.rollback();
    unchanged(run, &mut db, "update_rollback")?;

    let mut tx = db
        .begin_transaction()
        .map_err(|e| error(&phase("begin_delete"), e))?;
    let delete = format!("DELETE FROM items WHERE {where_clause} RETURNING id");
    let deleted = run.output(
        &phase("delete"),
        "sql",
        &delete,
        json_parameters(),
        tx.query_sql_with_params(&delete, &parameters),
    )?;
    run.equal(&phase("delete_ids"), &deleted, &selected, false)?;
    let survivors = "SELECT id FROM items";
    let observed = run.output(
        &phase("survivors"),
        "sql",
        survivors,
        json!([]),
        tx.query_sql(survivors),
    )?;
    run.equal(&phase("survivors"), &observed, &rows(remaining), false)?;
    tx.rollback();
    unchanged(run, &mut db, "delete_rollback")
}

pub(super) fn check(case: &Case, run: &mut Run) -> Result<()> {
    run_with_index(case, run, false)?;
    run_with_index(case, run, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn differential_query_execution_checks_mutations_and_sql_three_valued_order() {
        assert_eq!(
            Key(Some(2), None).compare(Key(Some(1), None)),
            Some(Ordering::Greater)
        );
        assert_eq!(Key(Some(1), None).compare(Key(Some(1), Some(0))), None);
        assert_eq!(Key(None, Some(2)).compare(Key(Some(1), Some(0))), None);
        for variant in 0..=14 {
            let case = Case {
                seed: 7,
                family: 4,
                variant,
                values: vec![-7, -3, -2, -1, 0, 1, 2, 3, 3, 7],
            };
            let mut run = Run::default();
            check(&case, &mut run).unwrap();
            for profile in ["indexed", "unindexed"] {
                assert!(run
                    .verified
                    .contains(&format!("dqe_{profile}_delete_rollback")));
            }
        }
    }
}
