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
const DEFAULT_PIPELINE: &str =
    include_str!("../../cypher/fixtures/migration_default_pipeline_v1.json");
const MANIFEST: &str = include_str!("../../cypher/fixtures/migration_manifest_v1.json");

// The immutable pre-migration corpus predates the optional lookup policy slot.
// Expect its explicit None default without rewriting that historical corpus;
// a populated policy or any other plan change still fails the exact comparison.
fn frozen_plan_text(case: &Value) -> String {
    let baseline = case["plan"]["text"].as_str().unwrap();
    let mut parts = baseline.split("NodeColumnLookup {");
    let mut expected = parts.next().unwrap().to_owned();
    for part in parts {
        expected.push_str("NodeColumnLookup {");
        let (fields, input) = part.split_once(", input:").unwrap();
        expected.push_str(fields);
        expected.push_str(", node_visibility_predicate: None, input:");
        expected.push_str(input);
    }
    expected
}

#[test]
fn migration_corpus_preserves_bindings_and_logical_plans() {
    let manifest: Value = serde_json::from_str(MANIFEST).unwrap();
    let migration: Value = serde_json::from_str(DEFAULT_PIPELINE).unwrap();
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
            if let Some(change) = migration["parser_stage_changes"].get(id) {
                let statement = statement.unwrap_or_else(|error| panic!("{id}: {error}"));
                let error = crate::plan_with_params(&statement, &parameters(&case["parameters"]))
                    .expect_err(id);
                assert_eq!(
                    error.to_string(),
                    change["empty_parameters_error"],
                    "{id}: rejection stage changed"
                );
            } else {
                assert!(statement.is_err(), "{id}: expected parser rejection");
            }
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
                    migration["logical_plan_representations"]
                        .get(id)
                        .map(|text| text.as_str().unwrap().to_owned())
                        .unwrap_or_else(|| frozen_plan_text(&case)),
                    "{id}: logical plan changed"
                );
            }
            "missing_parameters" | "binding_rejected" | "session_control" => {
                let error = planned.expect_err(id).to_string();
                assert_eq!(
                    error,
                    frozen_plan_text(&case),
                    "{id}: binding outcome changed"
                );
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
            ("missing_parameters", Err(error)) if error.to_string() == frozen_plan_text(&case) => {}
            (_, actual) => failures.push(format!(
                "{}: {actual:?}; expected {}; {query}",
                case["id"],
                frozen_plan_text(&case)
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
        if text != frozen_plan_text(&case) {
            failures.push(format!(
                "{}: {text}; expected {}; {query}",
                case["id"],
                frozen_plan_text(&case)
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
    let migration: Value = serde_json::from_str(DEFAULT_PIPELINE).unwrap();
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
        let expected = migration["logical_plan_representations"]
            .get(case["id"].as_str().unwrap())
            .map(|plan| plan.as_str().unwrap().to_owned())
            .unwrap_or_else(|| frozen_plan_text(&case));
        assert_eq!(text, expected, "{}: {query}", case["id"]);
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
            Err(error) if kind != "golden" && error.to_string() == frozen_plan_text(&case) => {}
            actual => failures.push(format!(
                "{}: {query}; {actual:?}; expected {}",
                case["id"],
                frozen_plan_text(&case)
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
        if format!("{actual:?}") == frozen_plan_text(&case) {
            exact += 1;
        } else {
            differences.push(serde_json::json!({"id":case["id"], "query":query, "expected":frozen_plan_text(&case), "actual":format!("{actual:?}")}));
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
    assert_eq!(
        exact,
        361,
        "{}",
        serde_json::to_string_pretty(&differences).unwrap()
    );
    let migration: Value = serde_json::from_str(DEFAULT_PIPELINE).unwrap();
    assert_eq!(
        migration["logical_plan_representations"]
            .as_object()
            .unwrap()
            .len(),
        differences.len()
    );
    for difference in &differences {
        assert_eq!(
            difference["actual"],
            migration["logical_plan_representations"][difference["id"].as_str().unwrap()]
        );
    }
    // Every difference is explicitly qualified in the migration fixture.
    // The two probe differences are not permission to restore the incorrect
    // group-key or pre-lookup RETURN window (#757).
    assert_eq!(
        differences
            .iter()
            .map(|case| case["id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [
            "mem-0370",
            "mem-0374",
            "owner-0057",
            "owner-0058",
            "owner-0059",
            "owner-0060",
            "owner-0061",
            "probe-0008",
            "probe-0040",
            "probe-0042",
        ]
    );
}

#[test]
fn default_pipeline_stage_qualification() {
    let migration: Value = serde_json::from_str(DEFAULT_PIPELINE).unwrap();
    let changes = migration["parser_stage_changes"].as_object().unwrap();
    let mut qualified = std::collections::BTreeSet::new();
    for line in CASES.lines() {
        let case: Value = serde_json::from_str(line).unwrap();
        let id = case["id"].as_str().unwrap();
        let Some(change) = changes.get(id) else {
            continue;
        };
        assert_eq!(case["parse"], change["prior_parse"]);
        let statement = hawdb_cypher::parse(case["query"].as_str().unwrap()).unwrap();
        assert!(matches!(statement, hawdb_cypher::Statement::Pipeline(_)));
        let empty = crate::plan_with_params(&statement, &BTreeMap::new()).expect_err(id);
        assert_eq!(empty.to_string(), change["empty_parameters_error"]);
        let supplied = BTreeMap::from([
            (
                "memory_id".to_string(),
                hawdb_core::Value::String("source".to_string()),
            ),
            (
                "older_id".to_string(),
                hawdb_core::Value::String("older".to_string()),
            ),
            (
                "newer_id".to_string(),
                hawdb_core::Value::String("newer".to_string()),
            ),
        ]);
        let planned = crate::plan_with_params(&statement, &supplied);
        match change["supplied_parameters"].as_str().unwrap() {
            "accepted" => assert!(
                matches!(planned, Ok(crate::LogicalPlan::Aggregate { .. })),
                "{id}: {planned:?}"
            ),
            "rejected" => assert_eq!(
                planned.expect_err(id).to_string(),
                change["empty_parameters_error"]
            ),
            stage => panic!("unknown stage {stage}"),
        }
        qualified.insert(id.to_string());
    }
    assert_eq!(
        qualified,
        std::collections::BTreeSet::from(["mem-0344".to_string(), "mem-0361".to_string()])
    );
}
