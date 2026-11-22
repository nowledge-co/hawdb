// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Constant folding of uncorrelated membership queries on one pinned snapshot.

use super::*;

fn membership(left: Option<i64>, right: &[Option<i64>]) -> Option<bool> {
    if right.is_empty() {
        return Some(false);
    }
    let left = left?;
    if right.contains(&Some(left)) {
        Some(true)
    } else if right.contains(&None) {
        None
    } else {
        Some(false)
    }
}

pub(super) fn check(case: &Case, run: &mut Run) -> Result<()> {
    let mut db = Database::new();
    for table in ["left_rows", "right_rows"] {
        run.sql(
            &mut db,
            "codd_schema",
            &format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY, k BIGINT)"),
            &[],
        )?;
    }
    let inputs = case
        .values
        .iter()
        .map(|n| (n.rem_euclid(5) != 0).then_some(n.rem_euclid(4)))
        .collect::<Vec<_>>();
    let rhs = inputs
        .iter()
        .skip((case.variant % 3) as usize)
        .copied()
        .collect::<Vec<_>>();
    for (table, values) in [
        ("left_rows", inputs.as_slice()),
        ("right_rows", rhs.as_slice()),
    ] {
        for (id, key) in values.iter().copied().enumerate() {
            run.sql(
                &mut db,
                "codd_insert",
                &format!("INSERT INTO {table} VALUES ($1, $2)"),
                &[Value::Int(id as i64), key.map_or(Value::Null, Value::Int)],
            )?;
        }
    }
    let read = db
        .begin_read_transaction()
        .map_err(|e| error("codd_snapshot", e))?;
    let query = |run: &mut Run, phase: &str, sql: &str, parameters: &[Value]| {
        run.output(
            phase,
            "sql",
            sql,
            json!(parameters.iter().map(typed_value_json).collect::<Vec<_>>()),
            read.query_sql_with_params(sql, parameters),
        )
    };
    const SUBQUERY: &str = "SELECT k FROM right_rows WHERE id < $1";
    let cutoff = case.variant as usize % (rhs.len() + 1);
    for count in BTreeSet::from([0, cutoff, rhs.len()]) {
        let bound = [Value::Int(count as i64)];
        let materialized = query(
            run,
            "codd_materialized",
            &format!("{SUBQUERY} ORDER BY id"),
            &bound,
        )?;
        let expected = rhs[..count]
            .iter()
            .map(|key| row(&[("k", key.map_or(Value::Null, Value::Int))]))
            .collect::<Vec<_>>();
        run.equal("codd_materialized", &materialized, &expected, true)?;
        let folded_arguments = materialized
            .iter()
            .map(|row| row["k"].clone())
            .collect::<Vec<_>>();
        for (operator, negated) in [("IN", false), ("NOT IN", true)] {
            let phase = if negated { "codd_not_in" } else { "codd_in" };
            let original = format!("SELECT id FROM left_rows WHERE k {operator} ({SUBQUERY})");
            let actual = query(run, phase, &original, &bound)?;
            let predicate = if folded_arguments.is_empty() {
                if negated {
                    "1 = 1".to_string()
                } else {
                    "1 = 0".to_string()
                }
            } else {
                let placeholders = (1..=folded_arguments.len())
                    .map(|i| format!("${i}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("k {operator} ({placeholders})")
            };
            let folded_sql = format!("SELECT id FROM left_rows WHERE {predicate}");
            let folded = query(
                run,
                &format!("{phase}_folded"),
                &folded_sql,
                &folded_arguments,
            )?;
            run.equal(phase, &actual, &folded, false)?;
            let expected = inputs
                .iter()
                .copied()
                .enumerate()
                .filter(|(_, left)| {
                    membership(*left, &rhs[..count]).is_some_and(|found| found != negated)
                })
                .map(|(id, _)| row(&[("id", Value::Int(id as i64))]))
                .collect::<Vec<_>>();
            run.equal(&format!("{phase}_model"), &folded, &expected, false)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coddtest_checks_duplicates_null_and_empty_in_one_snapshot() {
        assert_eq!(membership(None, &[]), Some(false));
        assert_eq!(membership(Some(4), &[Some(1), None]), None);
        assert_eq!(membership(Some(1), &[None, Some(1), Some(1)]), Some(true));
        assert_eq!(membership(Some(4), &[Some(1), Some(1)]), Some(false));
        for values in [vec![], vec![0, 1, 1, 2, 3, 4, 5], vec![0, 0], vec![1, 2, 3]] {
            for variant in [0, 1, 5, 7] {
                let case = Case {
                    seed: 42,
                    family: 4,
                    variant,
                    values: values.clone(),
                };
                let mut run = Run::default();
                check(&case, &mut run).unwrap();
                assert!(run.verified.contains("codd_in_model"));
                assert!(run.verified.contains("codd_not_in_model"));
            }
        }
    }
}
