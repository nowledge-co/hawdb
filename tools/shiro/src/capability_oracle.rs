// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Small independent models for recently added language and storage capabilities.
//! Inputs, rather than generated SQL, are reduced: every candidate is rebuilt in
//! a fresh database and must reproduce the same phase and failure class.

mod cypher;
mod relational;

use crate::{compare_rows, error_class, row_json, typed_value_json, ResultSemantics};
use hawdb::executor::Row;
use hawdb::{Database, QueryOutput, Value};
use serde_json::{json, Value as Json};
use std::collections::{BTreeMap, BTreeSet};

pub const PROTOCOL: &str = "shiro-capabilities-v1";
pub const FAMILIES: &[&str] = &[
    "decimal_arithmetic",
    "cypher_pipeline",
    "cypher_binding_set",
    "temporal_case",
    "keyless_bag",
];

#[derive(Clone, Debug, PartialEq, Eq)]
struct Case {
    seed: u64,
    family: usize,
    variant: u64,
    values: Vec<i64>,
}

impl Case {
    fn generate(seed: u64, index: usize) -> Self {
        let mut state = seed ^ (index as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
        let mut next = || {
            state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^ (z >> 31)
        };
        let variant = next() % 256;
        let count = next() % 13;
        let values = (0..count).map(|_| (next() % 41) as i64 - 20).collect();
        Self {
            seed,
            family: index % FAMILIES.len(),
            variant,
            values,
        }
    }

    fn json(&self) -> Json {
        json!({"protocol": PROTOCOL, "seed": self.seed, "family": FAMILIES[self.family],
            "variant": self.variant, "values": self.values})
    }

    fn read(value: &Json) -> std::result::Result<Self, String> {
        let value = value.get("case").unwrap_or(value);
        if value["protocol"] != PROTOCOL {
            return Err("unsupported capability replay protocol".into());
        }
        if value.as_object().is_none_or(|object| {
            object.keys().any(|key| {
                !["protocol", "seed", "family", "variant", "values"].contains(&key.as_str())
            })
        }) {
            return Err("unknown capability case field".into());
        }
        let family = FAMILIES
            .iter()
            .position(|name| value["family"] == *name)
            .ok_or("unknown capability family")?;
        let seed = value["seed"].as_u64().ok_or("missing seed")?;
        let variant = value["variant"]
            .as_u64()
            .filter(|v| *v < 256)
            .ok_or("invalid variant")?;
        let values = value["values"]
            .as_array()
            .filter(|v| v.len() <= 16)
            .ok_or("values must contain at most 16 bounded integers")?
            .iter()
            .map(|v| {
                v.as_i64()
                    .filter(|v| (-1_000_000..=1_000_000).contains(v))
                    .ok_or_else(|| "value outside model bounds".to_string())
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(Self {
            seed,
            family,
            variant,
            values,
        })
    }
}

#[derive(Debug)]
struct Failure {
    signature: String,
    evidence: Json,
}
type Result<T> = std::result::Result<T, Failure>;

#[derive(Default)]
struct Run {
    trace: Vec<Json>,
    verified: BTreeSet<String>,
}

impl Run {
    fn output(
        &mut self,
        phase: &str,
        language: &str,
        query: &str,
        params: Json,
        output: hawdb::Result<QueryOutput>,
    ) -> Result<Vec<Row>> {
        self.trace.push(json!({"phase": phase, "language": language,
            "query": query, "parameters": params}));
        match output {
            Ok(output) => {
                let rows = output.rows.into_rows();
                self.trace.last_mut().unwrap()["rows"] =
                    json!(rows.iter().map(row_json).collect::<Vec<_>>());
                Ok(rows)
            }
            Err(error) => Err(Failure {
                signature: format!("{phase}:{}", error_class(&error)),
                evidence: json!({"error": error.to_string()}),
            }),
        }
    }

    fn sql(
        &mut self,
        db: &mut Database,
        phase: &str,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<Row>> {
        self.output(
            phase,
            "sql",
            sql,
            json!(params.iter().map(typed_value_json).collect::<Vec<_>>()),
            db.query_sql_with_params(sql, params),
        )
    }

    fn cypher(
        &mut self,
        db: &mut Database,
        phase: &str,
        query: &str,
        params: &[(&str, Value)],
    ) -> Result<Vec<Row>> {
        let params: BTreeMap<_, _> = params
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        self.output(
            phase,
            "cypher",
            query,
            crate::parameters_json(&params),
            db.query_with_params(query, &params),
        )
    }

    fn cypher_refusal(
        &mut self,
        db: &mut Database,
        phase: &str,
        query: &str,
        params: &[(&str, Value)],
        expected_class: &str,
        message: &str,
    ) -> Result<()> {
        let params: BTreeMap<_, _> = params
            .iter()
            .map(|(key, value)| (key.to_string(), value.clone()))
            .collect();
        let output = db.query_with_params(query, &params);
        self.trace.push(json!({
            "phase": phase,
            "language": "cypher",
            "query": query,
            "parameters": crate::parameters_json(&params),
            "expected_error_class": expected_class,
            "expected_error_contains": message,
            "actual": format!("{output:?}"),
        }));
        match output {
            Err(error)
                if crate::error_class(&error) == expected_class
                    && error.to_string().contains(message) =>
            {
                self.verified.insert(phase.into());
                Ok(())
            }
            _ => Err(Failure {
                signature: format!("{phase}:refusal"),
                evidence: json!({"actual": format!("{output:?}")}),
            }),
        }
    }

    fn equal(
        &mut self,
        phase: &str,
        actual: &[Row],
        expected: &[Row],
        ordered: bool,
    ) -> Result<()> {
        self.trace.push(
            json!({"phase": phase, "expected": expected.iter().map(row_json).collect::<Vec<_>>(),
            "actual": actual.iter().map(row_json).collect::<Vec<_>>(), "ordered": ordered}),
        );
        compare_rows(
            actual,
            expected,
            if ordered {
                ResultSemantics::Ordered
            } else {
                ResultSemantics::Bag
            },
        )
        .map_err(|detail| Failure {
            signature: format!("{phase}:rows"),
            evidence: json!({"detail": detail}),
        })?;
        self.verified.insert(phase.to_string());
        Ok(())
    }

    fn refusal(
        &mut self,
        db: &mut Database,
        phase: &str,
        sql: &str,
        params: &[Value],
        message: &str,
    ) -> Result<()> {
        let output = db.query_sql_with_params(sql, params);
        self.trace
            .push(json!({"phase": phase, "language": "sql", "query": sql,
            "parameters": params.iter().map(typed_value_json).collect::<Vec<_>>(),
            "expected_error_contains": message, "actual": format!("{output:?}")}));
        match output {
            Err(error)
                if (matches!(
                    &error,
                    hawdb::HawDBError::Execution(_) | hawdb::HawDBError::Semantic(_)
                ) || matches!(&error, hawdb::HawDBError::Storage(detail) if detail.starts_with("relational constraint violation: execution error: ")))
                    && error.to_string().contains(message) =>
            {
                self.verified.insert(phase.into());
                Ok(())
            }
            _ => Err(Failure {
                signature: format!("{phase}:refusal"),
                evidence: json!({"actual": format!("{output:?}")}),
            }),
        }
    }
}

fn row(values: &[(&str, Value)]) -> Row {
    values
        .iter()
        .map(|(key, value)| (key.to_string(), value.clone()))
        .collect()
}
fn text(value: impl Into<String>) -> Value {
    Value::String(value.into())
}
fn error(phase: &str, err: impl std::fmt::Display) -> Failure {
    Failure {
        signature: phase.into(),
        evidence: json!({"error": err.to_string()}),
    }
}

fn run(case: &Case) -> (Run, Option<Failure>) {
    let mut run = Run::default();
    let result = match case.family {
        0 => relational::decimal(case, &mut run),
        1 => cypher::pipeline(case, &mut run),
        2 => cypher::binding_set(case, &mut run),
        3 => cypher::temporal(case, &mut run),
        4 => relational::keyless(case, &mut run),
        _ => unreachable!("validated family"),
    };
    (run, result.err())
}

fn reduce_with(
    mut case: Case,
    signature: &str,
    limit: usize,
    mut observe: impl FnMut(&Case) -> Option<String>,
) -> (Case, usize) {
    let mut attempts = 0;
    let mut position = 0;
    while position < case.values.len() && attempts < limit {
        let mut candidate = case.clone();
        candidate.values.remove(position);
        attempts += 1;
        if observe(&candidate).as_deref() == Some(signature) {
            case = candidate;
        } else {
            position += 1;
        }
    }
    for position in 0..case.values.len() {
        if attempts == limit {
            break;
        }
        if case.values[position] == 0 {
            continue;
        }
        let mut candidate = case.clone();
        candidate.values[position] = 0;
        attempts += 1;
        if observe(&candidate).as_deref() == Some(signature) {
            case = candidate;
        }
    }
    (case, attempts)
}

fn report(case: &Case) -> Json {
    let (run, failure) = run(case);
    json!({"protocol": PROTOCOL, "family": FAMILIES[case.family], "case": case.json(),
        "success": failure.is_none(), "verified": run.verified, "trace": run.trace,
        "failure": failure.map(|f| json!({"signature": f.signature, "evidence": f.evidence}))})
}

pub(crate) fn evaluate(seed: u64, index: usize) -> Json {
    report(&Case::generate(seed, index))
}

pub(crate) fn coverage(cases: &[Json]) -> Json {
    let mut families = BTreeMap::new();
    for family in FAMILIES {
        let selected = cases
            .iter()
            .filter(|case| case["capabilities"]["family"] == *family)
            .collect::<Vec<_>>();
        families.insert(*family, json!({"executed": selected.len(),
            "verified": selected.iter().filter(|case| case["capabilities"]["success"] == true).count()}));
    }
    json!({"protocol": PROTOCOL, "families": families,
        "complete": FAMILIES.iter().all(|family| cases.iter().any(|case|
            case["capabilities"]["family"] == *family && case["capabilities"]["success"] == true)),
        "power_loss_qualification": false})
}

/// Editable JSON replay is intentionally separate from legacy seed/index replay.
pub fn capabilities_cli(args: impl Iterator<Item = String>) -> std::result::Result<bool, String> {
    let args = args.collect::<Vec<_>>();
    let usage =
        "shiro capabilities generate --seed N --index N | replay FILE | shrink FILE [--attempts N]";
    let Some(command) = args.first() else {
        return Err(usage.into());
    };
    let mut seed = 1;
    let mut index = 0;
    let mut attempts = 32;
    let mut offset = if matches!(command.as_str(), "replay" | "shrink") {
        2
    } else {
        1
    };
    let mut seen = BTreeSet::new();
    while offset < args.len() {
        let flag = &args[offset];
        if !seen.insert(flag) {
            return Err("duplicate option".into());
        }
        let value = args.get(offset + 1).ok_or(usage)?;
        match (command.as_str(), flag.as_str()) {
            ("generate", "--seed") => seed = value.parse().map_err(|_| "invalid seed")?,
            ("generate", "--index") => index = value.parse().map_err(|_| "invalid index")?,
            ("shrink", "--attempts") => attempts = value.parse().map_err(|_| "invalid attempts")?,
            _ => return Err(usage.into()),
        }
        offset += 2;
    }
    if !(1..=256).contains(&attempts) {
        return Err("attempts must be 1..256".into());
    }
    let case = if command == "generate" {
        Case::generate(seed, index)
    } else {
        let path = args.get(1).ok_or(usage)?;
        if std::fs::metadata(path).map_err(|e| e.to_string())?.len() > 1024 * 1024 {
            return Err("replay exceeds 1 MiB".into());
        }
        let value = serde_json::from_slice(&std::fs::read(path).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
        Case::read(&value)?
    };
    let output = match command.as_str() {
        "generate" => case.json(),
        "replay" => report(&case),
        "shrink" => {
            let (_, failure) = run(&case);
            let failure = failure.ok_or("case passes; nothing to shrink")?;
            let (reduced, used) = reduce_with(case, &failure.signature, attempts, |candidate| {
                run(candidate).1.map(|f| f.signature)
            });
            let mut output = report(&reduced);
            if output["failure"]["signature"] != failure.signature {
                return Err("reduced case did not reproduce the original failure signature".into());
            }
            output["reduction_attempts"] = json!(used);
            output
        }
        _ => return Err(usage.into()),
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&output).map_err(|e| e.to_string())?
    );
    Ok(output["success"] != false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_models_cover_all_families_and_storage_profiles() {
        for seed in [1, 7, 42, 9007199254740993] {
            for index in 0..15 {
                let case = Case::generate(seed, index);
                assert_eq!(Case::read(&case.json()).unwrap(), case);
                let report = report(&case);
                assert_eq!(report["success"], true, "{report:#}");
            }
        }
    }

    #[test]
    fn temporal_candidate_models_preserve_clock_inputs_and_replay() {
        for variant in 0..8 {
            let case = Case {
                seed: 42,
                family: 3,
                variant,
                values: vec![-2, -1, 0, 0, 2, 3, 3],
            };
            let output = report(&case);
            assert_eq!(output["success"], true, "{output:#}");
            assert!(output["verified"]
                .as_array()
                .unwrap()
                .iter()
                .any(|phase| phase == "clock_bounded_order"));
            assert_eq!(report(&Case::read(&case.json()).unwrap()), output);
        }
    }

    #[test]
    fn scalar_regex_capability_model_covers_matches_and_typed_refusals() {
        let case = Case::generate(7, 1);
        assert_eq!(FAMILIES[case.family], "cypher_pipeline");
        let report = report(&case);
        assert_eq!(report["success"], true, "{report:#}");
        let verified = report["verified"].as_array().unwrap();
        for phase in [
            "scalar_regex_parameter",
            "scalar_regex_literal",
            "scalar_regex_invalid_pattern",
            "scalar_regex_non_string_pattern",
            "scalar_regex_non_string_left",
        ] {
            assert!(verified.iter().any(|value| value == phase), "{phase}");
        }
    }

    #[test]
    fn model_comparison_rejects_missing_duplicates_nulls_and_decimal_scale() {
        let value = row(&[("x", Value::Int(1))]);
        let mut run = Run::default();
        assert!(run
            .equal(
                "bag",
                &[value.clone(), value.clone()],
                std::slice::from_ref(&value),
                false
            )
            .is_err());
        assert!(run
            .equal("null", &[row(&[("x", Value::Null)])], &[value], false)
            .is_err());
        assert!(run
            .equal(
                "scale",
                &[row(&[("x", Value::Decimal("1.00".parse().unwrap()))])],
                &[row(&[("x", Value::Decimal("1.0".parse().unwrap()))])],
                false
            )
            .is_err());
        assert!(run.verified.is_empty());
    }

    #[test]
    fn reducer_preserves_failure_signature_and_replay_inputs() {
        let mut case = Case::generate(1, 0);
        case.values = vec![1, 7, 3];
        let (reduced, _) = reduce_with(case, "expected", 32, |case| {
            Some(
                if case.values.contains(&7) {
                    "expected"
                } else {
                    "different"
                }
                .into(),
            )
        });
        assert_eq!(reduced.values, vec![7]);
        assert_eq!(Case::read(&reduced.json()).unwrap(), reduced);
        assert!(Case::read(&json!({"protocol":"shiro-capabilities-v0"})).is_err());
    }

    #[test]
    fn coverage_requires_successfully_executed_families() {
        let cases = FAMILIES
            .iter()
            .map(|family| json!({"capabilities": {"family": family, "success": false}}))
            .collect::<Vec<_>>();
        assert_eq!(coverage(&cases)["complete"], false);
    }
}
