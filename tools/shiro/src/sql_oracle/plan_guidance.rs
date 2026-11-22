// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Schedule bounded fixture mutations using verified physical access patterns.

use super::*;
use crate::{emit_fuzz_report, FuzzError, DEFAULT_FUZZ_LOG_DIRECTORY};
use std::collections::BTreeSet;
use std::io;
use std::path::PathBuf;

const USAGE: &str = "Usage: shiro qpg [--seed N] [--cases 1..64] [--log-directory PATH]";
const ROW_COUNT: i64 = 32;
const STALL_LIMIT: usize = 2;
const FIXED_INTERVAL: usize = 8;
const SCHEMA: &str = "CREATE TABLE probes (id BIGINT PRIMARY KEY, k BIGINT, body TEXT NOT NULL)";

#[derive(Clone, Copy)]
enum Profile {
    Scan,
    Secondary,
    Covering,
    Skewed,
}

impl Profile {
    const ALL: [Self; 4] = [Self::Scan, Self::Secondary, Self::Covering, Self::Skewed];

    fn name(self) -> &'static str {
        match self {
            Self::Scan => "scan",
            Self::Secondary => "secondary",
            Self::Covering => "covering",
            Self::Skewed => "skewed",
        }
    }

    fn index(self) -> Option<&'static str> {
        match self {
            Self::Scan => None,
            Self::Secondary | Self::Skewed => Some("CREATE INDEX by_key ON probes (k)"),
            Self::Covering => Some("CREATE INDEX by_key ON probes (k) INCLUDE (id, body)"),
        }
    }

    fn key(self, id: i64) -> Option<i64> {
        match self {
            Self::Skewed if id % 7 == 0 => None,
            Self::Skewed if id < ROW_COUNT / 2 => Some(0),
            _ => Some(id % 12),
        }
    }
}

fn check(
    profile: Profile,
    query: &SqlQueryInvocation,
    keys: (i64, i64),
    setup: &mut Vec<SqlMutation>,
) -> Result<(Vec<String>, usize), (String, String)> {
    let mut db = Database::new();
    setup.push(SqlMutation {
        sql: SCHEMA.into(),
        parameters: vec![],
        reducible: false,
    });
    if let Some(ddl) = profile.index() {
        setup.push(SqlMutation {
            sql: ddl.into(),
            parameters: vec![],
            reducible: false,
        });
    }
    let mut expected = Vec::new();
    for id in 0..ROW_COUNT {
        let key = profile.key(id);
        let body = Value::String(format!("row-{id}"));
        setup.push(SqlMutation {
            sql: "INSERT INTO probes VALUES ($1, $2, $3)".into(),
            parameters: vec![
                Value::Int(id),
                key.map_or(Value::Null, Value::Int),
                body.clone(),
            ],
            reducible: true,
        });
        if key.is_some_and(|k| k >= keys.0 && k < keys.1) {
            expected.push(hawdb::executor::Row::from([
                ("id".into(), Value::Int(id)),
                ("body".into(), body),
            ]));
        }
    }
    for mutation in setup {
        db.query_sql_with_params(&mutation.sql, &mutation.parameters)
            .map_err(|error| {
                (
                    format!("fixture:{}", error_class(&error)),
                    error.to_string(),
                )
            })?;
    }
    let snapshot = db.begin_read_transaction().map_err(|error| {
        (
            format!("snapshot:{}", error_class(&error)),
            error.to_string(),
        )
    })?;
    let execution = snapshot
        .query_sql_with_params_options_profiled(
            &query.sql,
            &query.parameters,
            QueryStreamOptions::default(),
        )
        .map_err(|error| {
            (
                format!("execute:{}", error_class(&error)),
                error.to_string(),
            )
        })?;
    let actual = execution.output.rows.into_rows();
    compare_rows(&actual, &expected, query.result_semantics).map_err(|reason| {
        (
            "result_mismatch".into(),
            format!(
                "{reason}; expected {}; actual {}",
                json!(expected.iter().map(row_json).collect::<Vec<_>>()),
                json!(actual.iter().map(row_json).collect::<Vec<_>>())
            ),
        )
    })?;
    let operators = execution.profile.operator_cardinality_profiles;
    if operators.is_empty()
        || operators
            .iter()
            .any(|op| op.actual_rows.is_none() || !op.fully_consumed)
    {
        return Err((
            "executed_evidence_missing".into(),
            "expected fully consumed executed access operators".into(),
        ));
    }
    let signature = operators
        .iter()
        .map(|op| {
            format!(
                "{}(covering={},row_fetch={})",
                op.operator.as_str(),
                op.access_path.covering,
                op.access_path.requires_row_fetch,
            )
        })
        .collect();
    Ok((signature, actual.len()))
}

fn schedule(seed: u64, cases: usize, guided: bool) -> JsonValue {
    let mut profile_index = 0;
    let mut stalled = 0;
    let mut signatures = BTreeSet::new();
    let mut reports = Vec::with_capacity(cases);
    for index in 0..cases {
        if guided {
            if stalled == STALL_LIMIT {
                profile_index += 1;
                stalled = 0;
            }
        } else {
            profile_index = index / FIXED_INTERVAL;
        }
        let profile = Profile::ALL[profile_index % Profile::ALL.len()];
        let lower = seed.wrapping_add((index as u64).wrapping_mul(7)) % 10;
        let upper = lower + 1 + (index as u64 % 3);
        let query = SqlQueryInvocation {
            sql: "SELECT id, body FROM probes WHERE k >= $1 AND k < $2".into(),
            parameters: vec![Value::Int(lower as i64), Value::Int(upper as i64)],
            result_semantics: ResultSemantics::Bag,
        };
        let mut setup = Vec::new();
        let mut report = json!({"index": index, "fixture": profile.name(), "query": query.json()});
        match check(profile, &query, (lower as i64, upper as i64), &mut setup) {
            Ok((signature, count)) => {
                let novel = signatures.insert(signature.clone());
                stalled = if novel { 0 } else { stalled + 1 };
                report["success"] = json!(true);
                report["plan_signature"] = json!(signature);
                report["novel"] = json!(novel);
                report["row_count"] = json!(count);
            }
            Err((signature, reason)) => {
                stalled += 1;
                report["success"] = json!(false);
                report["failure"] = json!({ "signature": signature, "reason": reason,
                    "setup": setup.iter().map(SqlMutation::json).collect::<Vec<_>>() });
            }
        }
        reports.push(report);
    }
    json!({
        "successful_queries": reports.iter().filter(|report| report["success"] == true).count(),
        "unique_executed_plans": signatures.len(),
        "plans": signatures,
        "cases": reports,
    })
}

fn campaign(seed: u64, cases: usize) -> JsonValue {
    let guided = schedule(seed, cases, true);
    let fixed = schedule(seed, cases, false);
    json!({
        "protocol": "shiro-sql-qpg-v1", "seed": seed, "queries_per_schedule": cases,
        "success": guided["successful_queries"] == cases && fixed["successful_queries"] == cases,
        "guided": guided, "fixed": fixed,
    })
}

pub fn query_plan_guidance_cli(mut args: impl Iterator<Item = String>) -> Result<bool, FuzzError> {
    let mut seed = 7;
    let mut cases = 12;
    let mut log_directory = PathBuf::from(DEFAULT_FUZZ_LOG_DIRECTORY);
    let mut seen = BTreeSet::new();
    while let Some(argument) = args.next() {
        if argument == "--help" && seen.is_empty() && args.next().is_none() {
            println!("{USAGE}");
            return Ok(true);
        }
        let value = args
            .next()
            .filter(|_| seen.insert(argument.clone()))
            .ok_or_else(|| FuzzError::new(USAGE))?;
        match argument.as_str() {
            "--seed" => seed = value.parse().map_err(|_| FuzzError::new("invalid seed"))?,
            "--cases" => {
                cases = value
                    .parse()
                    .map_err(|_| FuzzError::new("invalid case count"))?
            }
            "--log-directory" => log_directory = PathBuf::from(value),
            _ => return Err(FuzzError::new(USAGE)),
        }
    }
    if !(1..=64).contains(&cases) {
        return Err(FuzzError::new(USAGE));
    }
    let report = campaign(seed, cases);
    let success = report["success"] == true;
    emit_fuzz_report(
        &log_directory,
        "shiro-qpg",
        &format!("seed-{seed}-cases-{cases}"),
        &report,
        success,
        true,
        &mut io::stdout().lock(),
    )
    .map_err(FuzzError::new)?;
    Ok(success)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_guidance_reacts_to_executed_plan_novelty_under_equal_budgets() {
        for seed in [7, 42] {
            let report = campaign(seed, 8);
            assert_eq!(report["success"], true, "{report:#}");
            assert_eq!(report["guided"]["successful_queries"], 8);
            assert_eq!(report["fixed"]["successful_queries"], 8);
            assert_eq!(report["guided"]["cases"][3]["fixture"], "secondary");
            assert_eq!(report["fixed"]["cases"][3]["fixture"], "scan");
            assert!(report["guided"]["unique_executed_plans"].as_u64().unwrap() > 1);
            for i in 0..8 {
                assert_eq!(
                    report["guided"]["cases"][i]["query"],
                    report["fixed"]["cases"][i]["query"]
                );
            }
            assert_eq!(report, campaign(seed, 8));
        }
    }
}
