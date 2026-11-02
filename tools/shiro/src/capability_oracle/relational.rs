// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb::{DatabaseConfig, RelationalIndexMode};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct Fixture {
    path: Option<PathBuf>,
    config: DatabaseConfig,
    passed: bool,
}
impl Fixture {
    fn new(case: &Case, run: &mut Run) -> Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let profile = case.variant % 3;
        let path = if profile == 0 {
            None
        } else {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| error("clock", e))?
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "shiro-capabilities-{}-{nonce}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&path).map_err(|e| error("fixture", e))?;
            Some(path)
        };
        run.trace.push(json!({"storage": (["memory", "shadow", "demand_paged"][profile as usize]), "path": path}));
        Ok(Self {
            path,
            config: DatabaseConfig {
                relational_index_mode: if profile == 2 {
                    RelationalIndexMode::DemandPaged
                } else {
                    RelationalIndexMode::Shadow
                },
                automatic_checkpoint_max_age: Duration::from_secs(3600),
                ..DatabaseConfig::default()
            },
            passed: false,
        })
    }
    fn open(&self) -> Result<Database> {
        match &self.path {
            Some(path) => {
                Database::open_with_config(path, self.config.clone()).map_err(|e| error("open", e))
            }
            None => Ok(Database::new()),
        }
    }
    fn recover(
        &self,
        mut db: Database,
        run: &mut Run,
        sql: &str,
        expected: &[Row],
    ) -> Result<Database> {
        if self.path.is_none() {
            return Ok(db);
        }
        // Expected rows come from the model, not another recovered relation.
        let epoch = db
            .commit_epoch()
            .map_err(|e| error("before_close_epoch", e))?;
        for checkpoint in [false, true] {
            if checkpoint {
                db.checkpoint().map_err(|e| error("checkpoint", e))?;
            }
            drop(db);
            db = self.open()?;
            let phase = if checkpoint {
                "checkpoint_reopen"
            } else {
                "wal_reopen"
            };
            let recovery = db.storage_recovery_report().map_err(|e| error(phase, e))?;
            let recovered_epoch = db.commit_epoch().map_err(|e| error(phase, e))?;
            run.trace.push(json!({"phase":phase, "expected_epoch":epoch, "recovered_epoch":recovered_epoch, "recovery":format!("{recovery:?}")}));
            if !recovery.durable
                || recovered_epoch != epoch
                || recovery.recovered_commit_epoch != epoch
                || (checkpoint
                    && (recovery.checkpoint_commit_epoch != Some(epoch)
                        || recovery.replayed_wal_entries != 0))
                || (!checkpoint && recovery.replayed_wal_entries == 0)
            {
                return Err(error(&format!("{phase}:boundary"), format!("{recovery:?}")));
            }
            let actual = run.sql(&mut db, phase, sql, &[])?;
            run.equal(phase, &actual, expected, false)?;
        }
        Ok(db)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if self.passed
            && let Some(path) = &self.path
        {
            let _ = std::fs::remove_dir_all(path);
        }
    }
}

// Parsing only constructs typed values. No production Decimal arithmetic or
// rescaling participates in the independent fixed-point reference calculation.
fn decimal_value(coefficient: i128, scale: u32) -> Value {
    let magnitude = coefficient.unsigned_abs();
    let divisor = 10_u128.pow(scale);
    let sign = if coefficient < 0 { "-" } else { "" };
    let text = format!(
        "{sign}{}.{:0width$}",
        magnitude / divisor,
        magnitude % divisor,
        width = scale as usize
    );
    Value::Decimal(text.parse().expect("bounded model decimal"))
}
fn cents(mills: i128) -> i128 {
    mills / 10
        + if mills.abs() % 10 >= 5 {
            mills.signum()
        } else {
            0
        }
}
fn money_rows(values: &[i128]) -> Vec<Row> {
    values
        .iter()
        .enumerate()
        .map(|(id, amount)| {
            row(&[
                ("id", Value::Int(id as i64)),
                ("amount", decimal_value(*amount, 2)),
            ])
        })
        .collect()
}

pub(super) fn decimal(case: &Case, run: &mut Run) -> Result<()> {
    let mut fixture = Fixture::new(case, run)?;
    let mut db = fixture.open()?;
    run.sql(
        &mut db,
        "schema",
        "CREATE TABLE money (id BIGINT PRIMARY KEY, amount DECIMAL(38,2) DEFAULT 1.235)",
        &[],
    )?;
    run.sql(
        &mut db,
        "index",
        "CREATE INDEX by_amount ON money (amount)",
        &[],
    )?;
    let mut inputs = vec![
        9_007_199_254_740_993_015_i128,
        -9_007_199_254_740_993_015,
        5,
        -5,
        0,
    ];
    inputs.extend(
        case.values
            .iter()
            .map(|v| i128::from(*v) * 10 + i128::from(case.variant % 10)),
    );
    for (id, value) in inputs.iter().enumerate() {
        run.sql(
            &mut db,
            "insert",
            "INSERT INTO money VALUES ($1, $2)",
            &[Value::Int(id as i64), decimal_value(*value, 3)],
        )?;
    }
    let mut model = inputs.into_iter().map(cents).collect::<Vec<_>>();
    run.sql(
        &mut db,
        "default",
        "INSERT INTO money (id) VALUES ($1)",
        &[Value::Int(model.len() as i64)],
    )?;
    model.push(124);
    const READ: &str = "SELECT id, amount FROM money ORDER BY id";
    let before = money_rows(&model);
    let actual = run.sql(&mut db, "coercion", READ, &[])?;
    run.equal("coercion", &actual, &before, true)?;
    // The same cached statement receives different exact values and scales.
    for bound in [model[0], -1, 0, 124] {
        let actual = run.sql(
            &mut db,
            "range_rebind",
            "SELECT id, amount FROM money WHERE amount >= $1 ORDER BY id",
            &[decimal_value(bound * 100, 4)],
        )?;
        let expected = before
            .iter()
            .zip(&model)
            .filter(|(_, value)| **value >= bound)
            .map(|(row, _)| row.clone())
            .collect::<Vec<_>>();
        run.equal("range_rebind", &actual, &expected, true)?;
    }
    let pinned = db
        .begin_read_transaction()
        .map_err(|e| error("snapshot", e))?;
    let multiplier = i128::from(case.variant % 5) - 2;
    let delta = if case.variant.is_multiple_of(2) {
        5
    } else {
        -5
    };
    run.sql(
        &mut db,
        "arithmetic",
        "UPDATE money SET amount = amount * $1 + $2 WHERE id >= 0",
        &[Value::Int(multiplier as i64), decimal_value(delta, 3)],
    )?;
    for value in &mut model {
        *value = cents(*value * multiplier * 10 + delta);
    }
    let expected = money_rows(&model);
    let actual = run.sql(&mut db, "arithmetic_result", READ, &[])?;
    run.equal("arithmetic_result", &actual, &expected, true)?;
    let old = run.output("pinned", "sql", READ, json!([]), pinned.query_sql(READ))?;
    run.equal("pinned", &old, &before, true)?;
    drop(pinned);
    let mut tx = db.begin_transaction().map_err(|e| error("begin", e))?;
    let sql = "UPDATE money SET amount = amount + 1 WHERE id >= 0";
    run.output("rollback_update", "sql", sql, json!([]), tx.query_sql(sql))?;
    tx.rollback();
    let actual = run.sql(&mut db, "rollback", READ, &[])?;
    run.equal("rollback", &actual, &expected, true)?;
    run.refusal(
        &mut db,
        "zero_divisor",
        "UPDATE money SET amount = amount / (id - $1) WHERE id >= 0",
        &[Value::Int((model.len() - 1) as i64)],
        "division by zero",
    )?;
    run.refusal(
        &mut db,
        "precision_overflow",
        "UPDATE money SET amount = CAST(amount + 1000 AS DECIMAL(2,0)) WHERE id >= 0",
        &[],
        "decimal result exceeds 38 coefficient digits or scale 38",
    )?;
    let actual = run.sql(&mut db, "refusal_atomicity", READ, &[])?;
    run.equal("refusal_atomicity", &actual, &expected, true)?;
    let actual = run.sql(
        &mut db,
        "division",
        "SELECT CAST(($1 + 0.2) * 10 / 3 AS DECIMAL(20,4)) AS amount",
        &[decimal_value(1, 1)],
    )?;
    run.equal(
        "division",
        &actual,
        &[row(&[("amount", decimal_value(10000, 4))])],
        true,
    )?;
    let actual = run.sql(
        &mut db,
        "null_cast",
        "SELECT CAST($1 AS DECIMAL(20,2)) AS amount",
        &[Value::Null],
    )?;
    run.equal(
        "null_cast",
        &actual,
        &[row(&[("amount", Value::Null)])],
        true,
    )?;
    run.refusal(
        &mut db,
        "mixed_float",
        "SELECT $1 + 0.1 AS amount",
        &[Value::Float(1.0)],
        "require an explicit CAST",
    )?;
    let actual = run.sql(
        &mut db,
        "explicit_float_cast",
        "SELECT CAST(CAST($1 AS DECIMAL(8,2)) + 0.1 AS DECIMAL(8,2)) AS amount",
        &[Value::Float(1.0)],
    )?;
    run.equal(
        "explicit_float_cast",
        &actual,
        &[row(&[("amount", decimal_value(110, 2))])],
        true,
    )?;
    db = fixture.recover(db, run, READ, &expected)?;
    drop(db);
    fixture.passed = true;
    Ok(())
}

fn bag_rows(values: &[(i64, i64)]) -> Vec<Row> {
    values
        .iter()
        .map(|(bucket, score)| {
            row(&[
                ("bucket", Value::Int(*bucket)),
                ("score", Value::Int(*score)),
                ("note", Value::Null),
            ])
        })
        .collect()
}

pub(super) fn keyless(case: &Case, run: &mut Run) -> Result<()> {
    let mut fixture = Fixture::new(case, run)?;
    let mut db = fixture.open()?;
    run.sql(
        &mut db,
        "schema",
        "CREATE TABLE events (bucket BIGINT, score BIGINT DEFAULT 7, note TEXT)",
        &[],
    )?;
    let mut model = Vec::new();
    for value in &case.values {
        // Repeated rows are intentional: a map keyed by visible columns is wrong.
        let pair = (value.rem_euclid(3), *value);
        for _ in 0..2 {
            run.sql(
                &mut db,
                "insert",
                "INSERT INTO events VALUES ($1, $2, NULL)",
                &[Value::Int(pair.0), Value::Int(pair.1)],
            )?;
            model.push(pair);
        }
    }
    run.sql(
        &mut db,
        "index_backfill",
        "CREATE INDEX by_bucket_score ON events (bucket, score)",
        &[],
    )?;
    const READ: &str = "SELECT bucket, score, note FROM events";
    let before = bag_rows(&model);
    let actual = run.sql(&mut db, "initial_bag", READ, &[])?;
    run.equal("initial_bag", &actual, &before, false)?;
    let pinned = db
        .begin_read_transaction()
        .map_err(|e| error("snapshot", e))?;
    let bucket = (case.variant % 3) as i64;
    run.sql(
        &mut db,
        "default",
        "INSERT INTO events (bucket) VALUES ($1)",
        &[Value::Int(bucket)],
    )?;
    model.push((bucket, 7));
    run.sql(
        &mut db,
        "update",
        "UPDATE events AS e SET score = score + 2 WHERE e.bucket = $1",
        &[Value::Int(bucket)],
    )?;
    for (b, score) in &mut model {
        if *b == bucket {
            *score += 2;
        }
    }
    run.sql(
        &mut db,
        "delete",
        "DELETE FROM events AS e WHERE e.score < $1",
        &[Value::Int(-10)],
    )?;
    model.retain(|(_, score)| *score >= -10);
    let expected = bag_rows(&model);
    let actual = run.sql(&mut db, "committed_bag", READ, &[])?;
    run.equal("committed_bag", &actual, &expected, false)?;
    let old = run.output("pinned_bag", "sql", READ, json!([]), pinned.query_sql(READ))?;
    run.equal("pinned_bag", &old, &before, false)?;
    drop(pinned);
    for bound in [-10, 0, 9, 100] {
        let actual = run.sql(
            &mut db,
            "bag_range",
            "SELECT bucket, score, note FROM events WHERE bucket = $1 AND score >= $2",
            &[Value::Int(bucket), Value::Int(bound)],
        )?;
        let expected = bag_rows(
            &model
                .iter()
                .copied()
                .filter(|(b, score)| *b == bucket && *score >= bound)
                .collect::<Vec<_>>(),
        );
        run.equal("bag_range", &actual, &expected, false)?;
    }
    let mut tx = db.begin_transaction().map_err(|e| error("begin", e))?;
    let sql = "DELETE FROM events WHERE bucket >= 0";
    run.output("rollback_delete", "sql", sql, json!([]), tx.query_sql(sql))?;
    tx.rollback();
    let actual = run.sql(&mut db, "rollback_bag", READ, &[])?;
    run.equal("rollback_bag", &actual, &expected, false)?;
    db = fixture.recover(db, run, READ, &expected)?;
    drop(db);
    fixture.passed = true;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fixed_point_rounding_is_symmetric_and_keeps_large_values_exact() {
        assert_eq!(
            [14, 15, 16, -14, -15, -16].map(cents),
            [1, 2, 2, -1, -2, -2]
        );
        assert_eq!(cents(9_007_199_254_740_993_015), 900_719_925_474_099_302);
    }
}
