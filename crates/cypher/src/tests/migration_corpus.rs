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

use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

const CASES: &str = include_str!("../../fixtures/migration_corpus_v1.jsonl");
const DEFAULT_PIPELINE: &str = include_str!("../../fixtures/migration_default_pipeline_v1.json");
const MANIFEST: &str = include_str!("../../fixtures/migration_manifest_v1.json");

#[test]
fn migration_corpus_preserves_parser_outcomes_and_source_inventory() {
    let manifest: Value = serde_json::from_str(MANIFEST).unwrap();
    let migration: Value = serde_json::from_str(DEFAULT_PIPELINE).unwrap();
    let stage_changes = migration["parser_stage_changes"].as_object().unwrap();
    let mut requalified = BTreeSet::new();
    let mut current_outcomes = BTreeMap::new();
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
        if let Some(change) = stage_changes.get(id) {
            assert_eq!(case["parse"], change["prior_parse"], "{id}");
            assert_eq!(outcome, change["parse"], "{id} at {source}: {parsed:?}");
            assert!(matches!(parsed, Ok(crate::Statement::Pipeline(_))), "{id}");
            requalified.insert(id.to_string());
        } else {
            assert_eq!(outcome, case["parse"], "{id} at {source}: {parsed:?}");
        }
        *outcomes
            .entry(case["parse"].as_str().unwrap().to_owned())
            .or_insert(0usize) += 1;
        *current_outcomes.entry(outcome).or_insert(0usize) += 1;
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
    assert_eq!(
        requalified,
        BTreeSet::from(["mem-0344".to_string(), "mem-0361".to_string()])
    );
    assert_eq!(stage_changes.len(), requalified.len());
    assert_eq!(
        current_outcomes["accepted"],
        outcomes["accepted"] + requalified.len()
    );
    assert_eq!(
        current_outcomes["rejected"] + requalified.len(),
        outcomes["rejected"]
    );
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
