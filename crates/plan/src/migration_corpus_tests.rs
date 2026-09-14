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
        let statement = skein_cypher::parse(query);
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
            ("generated".to_owned(), skein_core::Value::Int(100)),
            ("literal".to_owned(), skein_core::Value::Int(100)),
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
    assert_eq!(properties["generated"], skein_core::Value::Int(0));
    assert_eq!(properties["literal"], skein_core::Value::Int(100));
}
