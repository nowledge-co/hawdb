// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Bounded lock-footprint models. Zero-timeout probes distinguish conflict from
//! success without relying on thread timing; wait/deadlock scheduling remains
//! the responsibility of the existing pessimistic campaign.
mod model;

use crate::engine::{Failure, Observation};
use crate::{options, revision, write_json, ToolResult};
use hawdb::{
    ConcurrentDatabaseTransaction, ConcurrentTransactionOptions, Database, HawDBError, QueryOutput,
    TransactionRetryReason, Value,
};
use model::{Action, Case, Record, Selection, COLUMNS, FAMILIES, PROTOCOL, READ};
use serde_json::{json, Value as Json};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

type Result<T> = std::result::Result<T, Failure>;
const OPTIONS: ConcurrentTransactionOptions =
    ConcurrentTransactionOptions::pessimistic(Duration::ZERO);

fn failure(phase: &str, observation: &Observation, detail: impl ToString) -> Failure {
    Failure {
        kind: phase.into(),
        step: observation.trace.len(),
        detail: detail.to_string(),
    }
}
fn records(output: QueryOutput) -> std::result::Result<Vec<Record>, String> {
    output
        .rows
        .into_iter()
        .map(|row| {
            let integer = |name| match row.get(name) {
                Some(Value::Int(v)) => Ok(*v),
                _ => Err(format!("unexpected integer {name}: {row:?}")),
            };
            if row.len() != 6 {
                return Err(format!("unexpected columns: {row:?}"));
            }
            let tag = match row.get("tag") {
                Some(Value::Null) => None,
                Some(Value::Int(v)) => Some(*v),
                _ => return Err(format!("unexpected tag: {row:?}")),
            };
            Ok(Record {
                w: integer("w")?,
                k: integer("k")?,
                tail: integer("tail")?,
                band: integer("band")?,
                tag,
                value: integer("value")?,
            })
        })
        .collect()
}
fn compare(
    observation: &mut Observation,
    phase: &str,
    output: hawdb::Result<QueryOutput>,
    expected: &[Record],
) -> Result<()> {
    let mut actual =
        records(output.map_err(|e| failure(&format!("{phase}:engine"), observation, e))?)
            .map_err(|e| failure(&format!("{phase}:type"), observation, e))?;
    let mut expected = expected.to_vec();
    actual.sort();
    expected.sort();
    observation
        .trace
        .push(json!({"phase":phase, "expected": expected, "actual": actual}));
    if actual == expected {
        Ok(())
    } else {
        Err(failure(
            &format!("{phase}:rows"),
            observation,
            "typed bags differ",
        ))
    }
}
fn execute(
    tx: &mut ConcurrentDatabaseTransaction,
    observation: &mut Observation,
    phase: &str,
    sql: &str,
    params: &[Value],
) -> hawdb::Result<QueryOutput> {
    observation.trace.push(json!({"phase": phase, "sql": sql, "parameters": params.iter().map(|v| match v { Value::Int(v) => json!({"type":"int","value":v}), Value::Null => json!({"type":"null"}), _ => unreachable!("bounded integer inputs") }).collect::<Vec<_>>() }));
    tx.query_sql_with_params(sql, params)
}
fn select(
    tx: &mut ConcurrentDatabaseTransaction,
    observation: &mut Observation,
    phase: &str,
    selection: &Selection,
    rows: &[Record],
) -> Result<()> {
    let (sql, params) = selection.sql();
    let output = execute(tx, observation, phase, &sql, &params);
    compare(observation, phase, output, &selection.rows(rows))
}
fn action(
    tx: &mut ConcurrentDatabaseTransaction,
    observation: &mut Observation,
    phase: &str,
    action: &Action,
    rows: &[Record],
) -> Result<()> {
    let (sql, params) = action.sql();
    let output = execute(tx, observation, phase, &sql, &params);
    if let Action::Read { selection } = action {
        compare(observation, phase, output, &selection.rows(rows))
    } else {
        output
            .map(|_| ())
            .map_err(|e| failure(&format!("{phase}:engine"), observation, e))
    }
}
fn reject(
    observation: &mut Observation,
    phase: &str,
    output: hawdb::Result<QueryOutput>,
    retry: bool,
) -> Result<()> {
    let accepted = match &output {
        Err(HawDBError::TransactionRetryRequired(error)) if retry => {
            matches!(error.reason, TransactionRetryReason::SnapshotChanged { .. })
        }
        Err(HawDBError::Execution(error)) if !retry => {
            error.contains("transaction lock wait timed out")
        }
        _ => false,
    };
    observation.trace.push(json!({"phase":phase, "expected":if retry {"snapshot_changed"} else {"lock_timeout"}, "actual":format!("{output:?}")}));
    if accepted {
        Ok(())
    } else {
        Err(failure(
            &format!("{phase}:outcome"),
            observation,
            format!("unexpected result {output:?}"),
        ))
    }
}

fn run(case: &Case, observation: &mut Observation) -> Result<()> {
    let mut setup = Database::new();
    let primary = if case.keyless() {
        ""
    } else {
        ", PRIMARY KEY (w, k, tail)"
    };
    let nullable = if case.nullable_suffix {
        ""
    } else {
        " NOT NULL"
    };
    let ddl = format!("CREATE TABLE records (w BIGINT NOT NULL, k BIGINT NOT NULL, tail BIGINT NOT NULL, band BIGINT NOT NULL, tag BIGINT{nullable}, value BIGINT{primary})");
    observation.trace.push(json!({"phase":"setup", "ddl":ddl, "index":"CREATE INDEX by_band ON records (w, band, tag)", "initial":case.initial}));
    setup
        .query_sql(&ddl)
        .map_err(|e| failure("schema", observation, e))?;
    setup
        .query_sql("CREATE INDEX by_band ON records (w, band, tag)")
        .map_err(|e| failure("index", observation, e))?;
    for row in &case.initial {
        setup
            .query_sql_with_params(
                "INSERT INTO records VALUES ($1, $2, $3, $4, $5, $6)",
                &row.values(),
            )
            .map_err(|e| failure("insert", observation, e))?;
    }
    let db = setup.into_concurrent();
    let pinned = db
        .begin_read_transaction()
        .map_err(|e| failure("snapshot", observation, e))?;
    let mut holder = db
        .begin_transaction(OPTIONS)
        .map_err(|e| failure("begin", observation, e))?;
    let mut expected = case.initial.clone();
    if case.stale() {
        let output = execute(&mut holder, observation, "pin_snapshot", READ, &[]);
        compare(observation, "pin_snapshot", output, &expected)?;
        let mut writer = db
            .begin_transaction(OPTIONS)
            .map_err(|e| failure("writer_begin", observation, e))?;
        action(
            &mut writer,
            observation,
            "writer",
            &case.contender,
            &expected,
        )?;
        writer
            .commit()
            .map_err(|e| failure("writer_commit", observation, e))?;
        case.contender.apply(&mut expected);
        if case.family == "escalation_stale" {
            // These keys are disjoint from the changed range. The 65th request
            // upgrades the existing footprint, but must not bless a later read.
            for offset in 0..65 {
                let sql = format!(
                    "SELECT {COLUMNS} FROM records WHERE w = $1 AND k = $2 AND tail = 1 FOR UPDATE"
                );
                let output = execute(
                    &mut holder,
                    observation,
                    "escalate",
                    &sql,
                    &[Value::Int(case.holder.tenant + 10), Value::Int(offset)],
                );
                compare(observation, "escalate", output, &[])?;
            }
        }
        let (sql, params) = case.holder.sql();
        let output = execute(&mut holder, observation, "stale_request", &sql, &params);
        if case.expected_conflict() {
            reject(observation, "stale_request", output, true)?;
        } else {
            compare(
                observation,
                "stale_request",
                output,
                &case.holder.rows(&case.initial),
            )?;
        }
        holder.rollback();
    } else {
        if case.reverse() {
            action(
                &mut holder,
                observation,
                "holder_write",
                &case.contender,
                &expected,
            )?;
        } else {
            select(
                &mut holder,
                observation,
                "holder_read",
                &case.holder,
                &expected,
            )?;
        }
        let mut contender = db
            .begin_transaction(OPTIONS)
            .map_err(|e| failure("contender_begin", observation, e))?;
        let operation = if case.reverse() {
            Action::Read {
                selection: case.holder.clone(),
            }
        } else {
            case.contender.clone()
        };
        if case.conflicts() {
            let (sql, params) = operation.sql();
            let output = execute(&mut contender, observation, "conflict_probe", &sql, &params);
            reject(observation, "conflict_probe", output, false)?;
        } else {
            action(
                &mut contender,
                observation,
                "disjoint_probe",
                &operation,
                &expected,
            )?;
        }
        contender.rollback();
        if case.reverse() && !case.rollback {
            holder
                .commit()
                .map_err(|e| failure("holder_commit", observation, e))?;
            case.contender.apply(&mut expected);
        } else {
            holder.rollback();
        }
        compare(observation, "after_holder", db.query_sql(READ), &expected)?;
        // A fresh transaction must succeed after release, regardless of whether
        // the probe timed out. Check commit/rollback against the same model.
        let mut released = db
            .begin_transaction(OPTIONS)
            .map_err(|e| failure("released_begin", observation, e))?;
        action(
            &mut released,
            observation,
            "released",
            &operation,
            &expected,
        )?;
        if case.rollback {
            released.rollback();
        } else {
            released
                .commit()
                .map_err(|e| failure("released_commit", observation, e))?;
            operation.apply(&mut expected);
        }
    }
    compare(observation, "pinned", pinned.query_sql(READ), &case.initial)?;
    compare(observation, "visible", db.query_sql(READ), &expected)
}

fn observe(case: &Case) -> Observation {
    let mut observation = Observation {
        trace: Vec::new(),
        failure: None,
    };
    observation.failure = run(case, &mut observation).err();
    observation
}
fn read(path: &std::path::Path) -> ToolResult<Case> {
    if std::fs::metadata(path)?.len() > 16 * 1024 * 1024 {
        return Err("range replay exceeds 16 MiB".into());
    }
    let value: Json = serde_json::from_slice(&std::fs::read(path)?)?;
    let case: Case = serde_json::from_value(value.get("case").cloned().unwrap_or(value))?;
    case.validate()?;
    Ok(case)
}
fn artifact(case: &Case, observation: &Observation) -> Json {
    json!({"protocol":PROTOCOL, "source_revision":revision(), "case":case, "observation":observation,
        "expected_conflict":case.expected_conflict(), "expected_table_guard":case.table_guard(), "wait_scheduling_qualification":false})
}

pub fn cli(args: &[String]) -> ToolResult<bool> {
    if args
        .iter()
        .any(|a| matches!(a.as_str(), "--mode" | "--wait-probe-ms"))
    {
        return Err(
            "ranges uses zero-timeout lock probes; mode/wait options belong to the legacy campaign"
                .into(),
        );
    }
    let options = options(args)?;
    if options.command == "generate" {
        println!(
            "{}",
            serde_json::to_string_pretty(&model::generate(options.seed, options.index))?
        );
        return Ok(false);
    }
    std::fs::create_dir_all(&options.directory)?;
    let directory = options.directory.join(format!(
        "ranges-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    ));
    std::fs::create_dir(&directory)?;
    let started = Instant::now();
    let mut completed = 0;
    let mut failed = false;
    let mut coverage = std::collections::BTreeMap::<String, [u64; 3]>::new();
    if options.command == "run" {
        for index in 0..options.cases {
            if completed > 0
                && options
                    .seconds
                    .is_some_and(|s| started.elapsed() >= Duration::from_secs(s))
            {
                break;
            }
            let case = model::generate(options.seed, index);
            case.validate()?;
            let observation = observe(&case);
            completed += 1;
            let counts = coverage.entry(case.family.clone()).or_default();
            counts[0] += 1;
            if observation.failure.is_none() {
                counts[1 + usize::from(case.expected_conflict())] += 1;
            } else {
                write_json(
                    &directory.join("failure.json"),
                    &artifact(&case, &observation),
                )?;
                failed = true;
            }
            if failed || options.explain {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&artifact(&case, &observation))?
                );
            }
            if failed {
                break;
            }
        }
    } else {
        let mut case = read(options.input.as_deref().ok_or("missing input")?)?;
        let mut attempts = 0;
        let mut original_signature = None;
        if options.command == "shrink" {
            let original = observe(&case);
            let signature = original
                .failure
                .ok_or("case passes; nothing to shrink")?
                .kind;
            original_signature = Some(signature.clone());
            (case, attempts) =
                model::reduce_with(case, options.attempts, &signature, |candidate| {
                    observe(candidate).failure.map(|f| f.kind)
                });
        }
        let observation = observe(&case);
        if original_signature.is_some()
            && observation.failure.as_ref().map(|failure| &failure.kind)
                != original_signature.as_ref()
        {
            return Err("reduced case did not reproduce the original failure signature".into());
        }
        failed = observation.failure.is_some();
        completed = 1;
        let mut report = artifact(&case, &observation);
        report["reduction_attempts"] = json!(attempts);
        write_json(
            &directory.join(format!("{}.json", options.command)),
            &report,
        )?;
        if options.explain || options.print {
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
    }
    let summary = json!({"protocol":PROTOCOL, "success":!failed, "completed":completed, "source_revision":revision(), "families":FAMILIES,
        "coverage_columns":["executed", "verified_disjoint", "verified_conflict"], "coverage":coverage,
        "wait_scheduling_qualification":false, "artifacts":directory});
    write_json(&directory.join("summary.json"), &summary)?;
    if options.print || failed {
        println!("{}", serde_json::to_string_pretty(&summary)?);
    }
    Ok(failed)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn generated_range_models_match_runtime_and_preserve_bags() {
        for seed in [1, 7, 42] {
            for index in 0..10 {
                let case = model::generate(seed, index);
                case.validate().unwrap();
                let observation = observe(&case);
                assert!(
                    observation.failure.is_none(),
                    "{}",
                    serde_json::to_string_pretty(&artifact(&case, &observation)).unwrap()
                );
            }
        }
    }
    #[test]
    fn timeout_probe_does_not_accept_unrelated_errors_or_success() {
        let mut observation = Observation {
            trace: vec![],
            failure: None,
        };
        assert!(reject(
            &mut observation,
            "probe",
            Err(HawDBError::Semantic(
                "transaction lock wait timed out".into()
            )),
            false
        )
        .is_err());
        assert!(reject(
            &mut observation,
            "probe",
            Err(HawDBError::Execution("unexpected error".into())),
            false
        )
        .is_err());
        assert!(reject(
            &mut observation,
            "probe",
            Err(HawDBError::Execution(
                "transaction lock wait timed out".into()
            )),
            true
        )
        .is_err());
    }
}
