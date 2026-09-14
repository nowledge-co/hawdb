use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

const CASES: &str = include_str!("../../fixtures/migration_corpus_v1.jsonl");
const MANIFEST: &str = include_str!("../../fixtures/migration_manifest_v1.json");

#[test]
fn migration_corpus_preserves_parser_outcomes_and_source_inventory() {
    let manifest: Value = serde_json::from_str(MANIFEST).unwrap();
    let mut ids = BTreeSet::new();
    let mut origins = BTreeMap::new();
    let mut outcomes = BTreeMap::new();
    let mut mem_sources = BTreeSet::new();
    for line in CASES.lines() {
        let case: Value = serde_json::from_str(line).unwrap();
        let id = case["id"].as_str().unwrap();
        assert!(ids.insert(id.to_owned()), "duplicate case: {id}");
        let source = case["source"].as_str().unwrap();
        let origin = case["origin"].as_str().unwrap();
        *origins.entry(origin.to_owned()).or_insert(0usize) += 1;
        let query = case["query"].as_str().unwrap();
        let parsed = crate::parse(query);
        let outcome = if parsed.is_ok() {
            "accepted"
        } else {
            "rejected"
        };
        assert_eq!(outcome, case["parse"], "{id} at {source}: {parsed:?}");
        *outcomes.entry(outcome).or_insert(0usize) += 1;
        if id.starts_with("mem-") {
            let (path, line) = source.rsplit_once(':').unwrap();
            assert!(line.parse::<usize>().unwrap() > 0);
            assert!(manifest["mem_source_sha256"][path].is_string());
            mem_sources.insert(path.to_owned());
            // Normalized inventory text is only a locator. Parsing and planning
            // use the decoded literal so whitespace inside values is retained.
            let normalized = query.split_whitespace().collect::<Vec<_>>().join(" ");
            assert_eq!(
                normalized.trim_end_matches(';').trim(),
                case["normalized_query"]
            );
        }
    }
    assert_eq!(ids.len(), 1427);
    assert_eq!(ids.len(), manifest["total"].as_u64().unwrap() as usize);
    assert_eq!(serde_json::to_value(origins).unwrap(), manifest["origins"]);
    assert_eq!(
        serde_json::to_value(outcomes).unwrap(),
        manifest["parser_outcomes"]
    );
    assert_eq!(mem_sources.len(), 94);
    assert_eq!(
        mem_sources.len(),
        manifest["mem_source_sha256"].as_object().unwrap().len()
    );
    for cases in manifest["prior_statement_shapes"]
        .as_object()
        .unwrap()
        .values()
    {
        for case in cases.as_array().unwrap() {
            assert!(ids.contains(case.as_str().unwrap()));
        }
    }
}
