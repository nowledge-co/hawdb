// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use crate::case::{Case, Mode, Operation, Rows};
use crate::oracle::Oracle;
use hawdb::{
    ConcurrentDatabase, ConcurrentDatabaseTransaction, ConcurrentTransactionOptions, Database,
    HawDBError, QueryOutput, Value,
};
use serde::{Deserialize, Serialize};

const READ: &str = "SELECT id, value FROM records ORDER BY id";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Failure {
    pub kind: String,
    pub step: usize,
    pub detail: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Observation {
    pub trace: Vec<serde_json::Value>,
    pub failure: Option<Failure>,
}

impl Observation {
    fn check(
        &mut self,
        condition: bool,
        kind: &str,
        step: usize,
        detail: String,
    ) -> Result<(), Failure> {
        if condition {
            Ok(())
        } else {
            Err(Failure {
                kind: kind.into(),
                step,
                detail,
            })
        }
    }
}

fn rows(output: QueryOutput) -> Result<Rows, String> {
    let mut result = Rows::new();
    for row in output.rows {
        let (Some(Value::Int(id)), Some(Value::Int(value))) = (row.get("id"), row.get("value"))
        else {
            return Err(format!("unexpected typed row: {row:?}"));
        };
        if row.len() != 2 || result.insert(*id, *value).is_some() {
            return Err("unexpected column or duplicate primary key in result".into());
        }
    }
    Ok(result)
}

fn execute(tx: &mut ConcurrentDatabaseTransaction, op: &Operation) -> hawdb::Result<QueryOutput> {
    let (sql, parameters) = op.sql();
    tx.query_sql_with_params(
        sql,
        &parameters.into_iter().map(Value::Int).collect::<Vec<_>>(),
    )
}

fn failure(step: usize, detail: impl ToString) -> Failure {
    Failure {
        kind: "engine_error".into(),
        step,
        detail: detail.to_string(),
    }
}

fn check_visible(
    db: &ConcurrentDatabase,
    expected: &Rows,
    step: usize,
    observation: &mut Observation,
) -> Result<(), Failure> {
    let actual =
        rows(db.query_sql(READ).map_err(|e| failure(step, e))?).map_err(|e| failure(step, e))?;
    observation
        .trace
        .push(serde_json::json!({"step": step, "visible": actual, "expected_visible": expected}));
    observation.check(
        &actual == expected,
        "visible_rows",
        step,
        format!("expected {expected:?}, got {actual:?}"),
    )
}

fn run(case: &Case, observation: &mut Observation) -> Result<(), Failure> {
    let mut database = Database::new();
    database
        .query_sql("CREATE TABLE records (id BIGINT PRIMARY KEY, value BIGINT NOT NULL)")
        .map_err(|e| failure(0, e))?;
    for (&id, &value) in &case.initial {
        database
            .query_sql_with_params(
                "INSERT INTO records (id, value) VALUES ($1, $2)",
                &[Value::Int(id), Value::Int(value)],
            )
            .map_err(|e| failure(0, e))?;
    }
    let db = database.into_concurrent();
    let options = ConcurrentTransactionOptions::optimistic();
    // Both snapshots precede every scheduled statement and commit.
    let mut transactions = [
        Some(db.begin_transaction(options).map_err(|e| failure(0, e))?),
        Some(db.begin_transaction(options).map_err(|e| failure(0, e))?),
    ];
    let pinned = db.begin_read_transaction().map_err(|e| failure(0, e))?;
    let mut oracle = Oracle::new(case);
    let mut positions = [0; 2];
    for (step, &tx) in case.schedule.iter().enumerate() {
        observation
            .trace
            .push(serde_json::json!({"step": step, "tx": tx}));
        let program = &case.transactions[tx];
        if let Some(op) = program.operations.get(positions[tx]) {
            let active = transactions[tx]
                .as_mut()
                .ok_or_else(|| failure(step, "transaction already finished"))?;
            let output = execute(active, op).map_err(|e| failure(step, e))?;
            oracle.execute(tx, op).map_err(|e| failure(step, e))?;
            if matches!(op, Operation::Read) {
                let actual = rows(output).map_err(|e| failure(step, e))?;
                observation
                    .trace
                    .push(serde_json::json!({"step": step, "tx": tx, "read": actual, "expected_read": oracle.private[tx]}));
                observation.check(
                    actual == oracle.private[tx],
                    "snapshot_rows",
                    step,
                    format!("expected {:?}, got {actual:?}", oracle.private[tx]),
                )?;
            } else {
                observation
                    .trace
                    .push(serde_json::json!({"step": step, "tx": tx, "operation": op}));
            }
        } else {
            let active = transactions[tx]
                .take()
                .ok_or_else(|| failure(step, "duplicate terminal"))?;
            if program.rollback {
                active.rollback();
                observation
                    .trace
                    .push(serde_json::json!({"step": step, "tx": tx, "terminal": "rollback", "expected_terminal": "rollback"}));
            } else {
                let expected_conflict = oracle.conflicts(tx);
                let result = active.commit();
                let outcome = match &result {
                    Ok(_) => "committed",
                    Err(HawDBError::TransactionConflict { .. }) => "conflict",
                    Err(_) => "error",
                };
                observation.trace.push(serde_json::json!({"step": step, "tx": tx, "terminal": outcome, "expected_terminal": if expected_conflict { "conflict" } else { "committed" }, "error": result.as_ref().err().map(ToString::to_string)}));
                let actual_conflict = matches!(result, Err(HawDBError::TransactionConflict { .. }));
                if let Err(error) = &result
                    && !actual_conflict
                {
                    return Err(failure(step, error));
                }
                observation.check(
                    actual_conflict == expected_conflict,
                    "commit_outcome",
                    step,
                    format!("expected conflict={expected_conflict}, got {outcome}"),
                )?;
                if !expected_conflict {
                    oracle.commit(tx);
                }
            }
        }
        positions[tx] += 1;
        check_visible(&db, &oracle.visible, step, observation)?;
        let original = rows(pinned.query_sql(READ).map_err(|e| failure(step, e))?)
            .map_err(|e| failure(step, e))?;
        observation.check(
            original == case.initial,
            "pinned_rows",
            step,
            format!("expected {:?}, got {original:?}", case.initial),
        )?;
    }
    Ok(())
}

pub fn observe(case: &Case) -> Result<Observation, String> {
    case.validate()?;
    let mut observation = Observation {
        trace: Vec::new(),
        failure: None,
    };
    let execute = |observation: &mut Observation| match case.mode {
        Mode::Optimistic => run(case, observation),
        Mode::Pessimistic => crate::pessimistic::run(case, observation),
    };
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| execute(&mut observation))) {
        Ok(Ok(())) => {}
        Ok(Err(error)) => observation.failure = Some(error),
        Err(payload) => {
            let detail = payload
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| payload.downcast_ref::<&str>().copied())
                .unwrap_or("non-string panic");
            observation.failure = Some(Failure {
                kind: "panic".into(),
                step: observation
                    .trace
                    .last()
                    .and_then(|event| event["step"].as_u64())
                    .unwrap_or(0) as usize,
                detail: detail.into(),
            });
        }
    }
    Ok(observation)
}
