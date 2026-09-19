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

use crate::corpus_support::{clock_nanos, normalize_clock_slots, parameters};
use serde_json::Value;
use std::collections::BTreeMap;

const CASES: &str = include_str!("../../cypher/fixtures/migration_corpus_v1.jsonl");
const MANIFEST: &str = include_str!("../../cypher/fixtures/migration_manifest_v1.json");

#[test]
fn migration_corpus_preserves_bindings_and_logical_plans() {
    let manifest: Value = serde_json::from_str(MANIFEST).unwrap();
    let mut outcomes = BTreeMap::new();
    let mut clock_goldens = 0;
    for line in CASES.lines() {
        let case: Value = serde_json::from_str(line).unwrap();
        let id = case["id"].as_str().unwrap();
        let query = case["query"].as_str().unwrap();
        let kind = case["plan"]["kind"].as_str().unwrap();
        *outcomes.entry(kind.to_owned()).or_insert(0usize) += 1;
        let statement = hawdb_cypher::parse(query);
        if kind == "parse_rejected" {
            assert!(statement.is_err(), "{id}: expected parser rejection");
            continue;
        }
        let statement = statement.unwrap_or_else(|error| panic!("{id}: {error}"));
        let before = clock_nanos();
        let planned = crate::plan_with_params(&statement, &parameters(&case["parameters"]));
        let after = clock_nanos();
        match kind {
            "golden" => {
                let mut plan = planned.unwrap_or_else(|error| panic!("{id}: {error}"));
                normalize_clock_slots(
                    &mut plan,
                    &case["clock_slots"],
                    before.min(after)..=before.max(after),
                );
                clock_goldens += usize::from(!case["clock_slots"].as_array().unwrap().is_empty());
                assert_eq!(
                    format!("{plan:?}"),
                    case["plan"]["text"],
                    "{id}: logical plan changed"
                );
            }
            "missing_parameters" | "binding_rejected" | "session_control" => {
                let error = planned.expect_err(id).to_string();
                assert_eq!(error, case["plan"]["text"], "{id}: binding outcome changed");
            }
            _ => panic!("{id}: unknown plan outcome {kind}"),
        }
    }
    assert_eq!(
        serde_json::to_value(outcomes).unwrap(),
        manifest["planner_outcomes"]
    );
    assert_eq!(clock_goldens, 11);
    assert_eq!(
        clock_goldens,
        manifest["clock_golden_count"].as_u64().unwrap() as usize
    );
}

#[test]
fn clock_normalization_preserves_literal_values_in_the_same_time_window() {
    let mut plan = crate::LogicalPlan::CreateNode {
        label: "ClockProbe".to_owned(),
        properties: BTreeMap::from([
            ("generated".to_owned(), hawdb_core::Value::Int(100)),
            ("literal".to_owned(), hawdb_core::Value::Int(100)),
        ]),
    };
    normalize_clock_slots(
        &mut plan,
        &serde_json::json!([
            {"kind": "create", "property": "generated"}
        ]),
        90..=110,
    );
    let crate::LogicalPlan::CreateNode { properties, .. } = plan else {
        unreachable!()
    };
    assert_eq!(properties["generated"], hawdb_core::Value::Int(0));
    assert_eq!(properties["literal"], hawdb_core::Value::Int(100));
}

#[test]
fn ordered_read_pipeline_preserves_corpus_binding_outcomes() {
    use hawdb_cypher::ClauseKind;
    let mut eligible = BTreeMap::new();
    let mut failures = Vec::new();
    for line in CASES.lines() {
        let case: Value = serde_json::from_str(line).unwrap();
        let kind = case["plan"]["kind"].as_str().unwrap();
        if !matches!(kind, "golden" | "missing_parameters") {
            continue;
        }
        let query = case["query"].as_str().unwrap();
        let Ok(pipeline) = hawdb_cypher::parse_pipeline(query) else {
            continue;
        };
        if !pipeline.clauses.iter().all(|clause| match &clause.kind {
            ClauseKind::Match { patterns, .. } => {
                patterns.iter().all(|pattern| pattern.variable.is_none())
            }
            ClauseKind::With(_) | ClauseKind::Return(_) => true,
            _ => false,
        }) {
            continue;
        }
        *eligible.entry(kind.to_owned()).or_insert(0) += 1;
        let actual = crate::plan_pipeline_query(query, &parameters(&case["parameters"]));
        match (kind, actual) {
            ("golden", Ok(_)) => {}
            ("missing_parameters", Err(error)) if error.to_string() == case["plan"]["text"] => {}
            (_, actual) => failures.push(format!(
                "{}: {actual:?}; expected {}; {query}",
                case["id"], case["plan"]["text"]
            )),
        }
    }
    eprintln!(
        "ordered read bindings: {eligible:?} eligible; {} failures",
        failures.len()
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    assert_eq!(eligible["golden"], 245);
    assert_eq!(eligible["missing_parameters"], 710);
}

#[test]
fn ordered_mutation_pipeline_preserves_frozen_plans_and_errors() {
    use hawdb_cypher::ClauseKind;
    let mut covered = BTreeMap::new();
    let mut failures = Vec::new();
    for line in CASES.lines() {
        let case: Value = serde_json::from_str(line).unwrap();
        let query = case["query"].as_str().unwrap();
        let Ok(pipeline) = hawdb_cypher::parse_pipeline(query) else {
            continue;
        };
        if !pipeline.clauses.iter().any(|clause| {
            matches!(
                clause.kind,
                ClauseKind::Create(_)
                    | ClauseKind::Merge { .. }
                    | ClauseKind::Set(_)
                    | ClauseKind::Delete { .. }
            )
        }) {
            continue;
        }
        let kind = case["plan"]["kind"].as_str().unwrap();
        if !matches!(kind, "golden" | "missing_parameters" | "binding_rejected") {
            continue;
        }
        *covered.entry(kind.to_string()).or_insert(0) += 1;
        let before = clock_nanos();
        let actual = crate::plan_pipeline_query(query, &parameters(&case["parameters"]));
        let after = clock_nanos();
        let text = match actual {
            Ok(mut plan) => {
                normalize_clock_slots(
                    &mut plan,
                    &case["clock_slots"],
                    before.min(after)..=before.max(after),
                );
                format!("{plan:?}")
            }
            Err(error) => error.to_string(),
        };
        if text != case["plan"]["text"] {
            failures.push(format!(
                "{}: {text}; expected {}; {query}",
                case["id"], case["plan"]["text"]
            ));
        }
    }
    eprintln!(
        "ordered mutation corpus: {covered:?}; {} failures",
        failures.len()
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    assert_eq!(
        covered,
        BTreeMap::from([
            ("golden".to_string(), 114),
            ("missing_parameters".to_string(), 240)
        ])
    );
}

#[test]
fn ordered_procedure_and_shortest_path_plans_preserve_frozen_outcomes() {
    use hawdb_cypher::{ClauseKind, PathSearch};
    let mut covered = BTreeMap::new();
    for line in CASES.lines() {
        let case: Value = serde_json::from_str(line).unwrap();
        let query = case["query"].as_str().unwrap();
        let Ok(pipeline) = hawdb_cypher::parse_pipeline(query) else {
            continue;
        };
        if !pipeline.clauses.iter().any(|clause| match &clause.kind {
            ClauseKind::Call { .. } => true,
            ClauseKind::Match { patterns, .. } => patterns.iter().any(|pattern| {
                pattern
                    .steps
                    .iter()
                    .any(|step| step.relationship.search == PathSearch::AllShortest)
            }),
            _ => false,
        }) {
            continue;
        }
        let kind = case["plan"]["kind"].as_str().unwrap();
        *covered.entry(kind.to_string()).or_insert(0) += 1;
        let actual = crate::plan_pipeline_query(query, &parameters(&case["parameters"]));
        let text = match actual {
            Ok(plan) => format!("{plan:?}"),
            Err(error) => error.to_string(),
        };
        assert_eq!(text, case["plan"]["text"], "{}: {query}", case["id"]);
    }
    assert_eq!(
        covered,
        BTreeMap::from([
            ("golden".to_string(), 11),
            ("missing_parameters".to_string(), 2),
            ("binding_rejected".to_string(), 2),
        ])
    );
}

#[test]
fn complete_ordered_query_corpus_preserves_binding_outcomes() {
    let mut covered = BTreeMap::new();
    let mut failures = Vec::new();
    for line in CASES.lines() {
        let case: Value = serde_json::from_str(line).unwrap();
        let query = case["query"].as_str().unwrap();
        if hawdb_cypher::parse_pipeline(query).is_err() {
            continue;
        }
        let kind = case["plan"]["kind"].as_str().unwrap();
        if !matches!(kind, "golden" | "missing_parameters" | "binding_rejected") {
            continue;
        }
        *covered.entry(kind.to_string()).or_insert(0) += 1;
        let actual = crate::plan_pipeline_query(query, &parameters(&case["parameters"]));
        match actual {
            Ok(_) if kind == "golden" => {}
            Err(error) if kind != "golden" && error.to_string() == case["plan"]["text"] => {}
            actual => failures.push(format!(
                "{}: {query}; {actual:?}; expected {}",
                case["id"], case["plan"]["text"]
            )),
        }
    }
    eprintln!("complete ordered corpus: {covered:?}");
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    assert_eq!(
        covered,
        BTreeMap::from([
            ("golden".to_string(), 371),
            ("missing_parameters".to_string(), 954),
            ("binding_rejected".to_string(), 2),
        ])
    );
}

#[test]
fn normalized_pipeline_frozen_plan_coverage() {
    let mut exact = 0;
    let mut differences = Vec::new();
    for line in CASES.lines() {
        let case: Value = serde_json::from_str(line).unwrap();
        let query = case["query"].as_str().unwrap();
        if case["plan"]["kind"] != "golden" || hawdb_cypher::parse_pipeline(query).is_err() {
            continue;
        }
        let before = clock_nanos();
        let mut actual =
            crate::plan_normalized_pipeline_query(query, &parameters(&case["parameters"])).unwrap();
        let after = clock_nanos();
        normalize_clock_slots(
            &mut actual,
            &case["clock_slots"],
            before.min(after)..=before.max(after),
        );
        if format!("{actual:?}") == case["plan"]["text"] {
            exact += 1;
        } else {
            differences.push(serde_json::json!({"id":case["id"], "query":query, "expected":case["plan"]["text"], "actual":format!("{actual:?}")}));
        }
    }
    eprintln!(
        "normalized pipeline exact plans: {exact}; remaining: {}",
        differences.len()
    );
    eprintln!(
        "remaining case ids: {:?}",
        differences
            .iter()
            .map(|case| &case["id"])
            .collect::<Vec<_>>()
    );
    assert_eq!(exact, 368);
    assert_eq!(differences.len(), 3);
}
