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

use super::*;
use hawdb_integrity::IntegrityHasher;
use serde_json::{json, Value as Json};

// Captured on unchanged main cc36de143a276696c99a6ddba897003dbe9f33e5.
// These bind the complete ordered input/output corpus, not just ready flags.
const SMOKE_DIGEST: &str = "b147b0f76e9b5ee75710b2ec61b2d1c9897bd944f1e441f5659a7d064c51a58d";
const CAMPAIGN_DIGEST: &str = "1ef7fde9e212b76a43f4ec097558befceb78d648bb9e422589dcb8265fbf4aa8";

fn next(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e3779b97f4a7c15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

fn record(hasher: &mut IntegrityHasher, value: &Json) {
    let bytes = serde_json::to_vec(value).unwrap();
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(&bytes);
}

fn evaluate(hasher: &mut IntegrityHasher, primary: &Json, probe: &Json) -> Json {
    let before = probe.clone();
    let evidence = nowledge_search_projection_evidence_json(probe);
    let shadow = nowledge_search_projection_shadow_evidence_json(primary, probe);
    let typed = NowledgeSearchProjectionEvidenceReport::from_probe(probe);
    assert_eq!(typed.json(), evidence);
    assert_eq!(probe, &before);
    assert_eq!(
        typed,
        NowledgeSearchProjectionEvidenceReport::from_evidence(evidence.clone())
    );
    record(hasher, probe);
    record(hasher, &evidence);
    record(hasher, &shadow);
    record(hasher, &json!(format!("{typed:?}")));
    evidence
}

fn model_case(base: &Json, mask: u64, index: usize) -> (Json, Vec<String>) {
    let mut probe = base.clone();
    let bad = |bit: u32| (mask & (1_u64 << bit)) != 0_u64;
    let mut present = [true; 6];
    let mut fts = [true; 6];
    let mut vector = [true; 6];
    if bad(0) {
        present[index] = false;
    }
    if bad(1) {
        fts[(index + 1) % 6] = false;
    }
    if bad(2) {
        vector[(index + 2) % 6] = false;
    }
    let tables = probe["tables"].as_array_mut().unwrap();
    for (i, table) in tables.iter_mut().enumerate() {
        table["fts_ready"] = json!(fts[i]);
        table["vector_ready"] = json!(vector[i]);
    }
    if bad(0) {
        tables.remove(index);
    }
    probe["derived_projection"] = json!(!bad(3));
    if bad(4) {
        probe["document_identity"]["checksum"] = Json::Null;
    }
    if bad(5) {
        probe["embedding_manifest"]["active_dimension"] = json!(1025);
    }
    probe["fail_soft"]["no_500_on_leg_failure"] = json!(!bad(6));
    probe["lifecycle"]["rebuild_marker_ready"] = json!(!bad(7));
    probe["lifecycle"]["metadata_repair_marker_ready"] = json!(!bad(8));
    if bad(9) {
        probe["incremental_update"]["source_graph_commit_epoch"] = Json::Null;
    }
    probe["predicate_pushdown"]["range_ready"] = json!(!bad(10));
    probe["predicate_pushdown"]["persisted_segment_descriptor_ready"] = json!(!bad(11));
    if bad(12) {
        let summaries = probe["predicate_pushdown"]["segment_descriptor_field_summaries"]
            .as_array_mut()
            .unwrap();
        let importance = summaries
            .iter_mut()
            .find(|value| value["field"] == "importance")
            .unwrap();
        importance["numeric_range_summary_used"] = json!(false);
    }
    probe["production_filter_pruning"]["ready"] = json!(!bad(13));
    if bad(14) {
        for sample in probe["production_filter_pruning"]["samples"]
            .as_array_mut()
            .unwrap()
        {
            sample["explain_analyze"]["payload_read_avoidance"] = json!(false);
        }
    }
    probe["compressed_vector_projection"]["ready"] = json!(!bad(15));
    if bad(16) {
        probe["blocker_codes"] = json!(["generated_blocker", "generated_blocker", 17]);
    }
    if bad(17) {
        probe["protocol"] = json!("hawdb-nowledge-search-projection-probe");
    }
    if bad(18) {
        let identity = probe
            .as_object_mut()
            .unwrap()
            .remove("document_identity")
            .unwrap();
        probe["projection_identity"] = identity;
    }

    // Derive gates from generated facts, without reading any report or using
    // the production field lists, fallback helpers, or readiness functions.
    let is_hawdb = base["engine"] == "hawdb" || bad(17);
    let all_tables = present.iter().all(|value| *value);
    let fts_ready = (0..6).all(|i| present[i] && fts[i]);
    let vector_ready = [0, 2, 3, 4, 5].iter().all(|&i| present[i] && vector[i]);
    let source_ready = present[5] && fts[5] && vector[5];
    let gates = [
        (bad(3), "not_derived_projection"),
        (!all_tables, "missing_required_search_tables"),
        (!fts_ready, "fts_not_ready"),
        (!vector_ready, "vector_not_ready"),
        (bad(4), "document_identity_not_ready"),
        (bad(5), "embedding_identity_not_ready"),
        (bad(6), "fail_soft_not_ready"),
        (bad(7), "rebuild_marker_not_ready"),
        (bad(8), "metadata_repair_marker_not_ready"),
        (bad(9), "incremental_update_not_ready"),
        (!source_ready, "source_chunks_index_not_ready"),
        (bad(10), "predicate_pushdown_not_ready"),
        (
            is_hawdb && (bad(11) || bad(12)),
            "hawdb_predicate_pushdown_descriptor_not_ready",
        ),
        (
            is_hawdb && (bad(13) || bad(14)),
            "hawdb_production_filter_pruning_not_ready",
        ),
        (
            is_hawdb && vector_ready && bad(15),
            "compressed_vector_projection_not_ready",
        ),
        (bad(16), "generated_blocker"),
    ];
    let mut blockers: Vec<_> = gates
        .into_iter()
        .filter(|(failed, _)| *failed)
        .map(|(_, code)| code.to_string())
        .collect();
    blockers.sort();
    (probe, blockers)
}

fn paths(value: &Json, prefix: &str, output: &mut Vec<String>) {
    match value {
        Json::Object(fields) => {
            for (name, value) in fields {
                let path = format!("{prefix}/{name}");
                output.push(path.clone());
                paths(value, &path, output);
            }
        }
        Json::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                let path = format!("{prefix}/{index}");
                output.push(path.clone());
                paths(value, &path, output);
            }
        }
        _ => {}
    }
}

fn malformed_case(base: &Json, state: &mut u64) -> Json {
    let mut probe = base.clone();
    for _ in 0..4 {
        let mut candidates = Vec::new();
        paths(&probe, "", &mut candidates);
        if candidates.is_empty() {
            break;
        }
        let path = &candidates[(next(state) % candidates.len() as u64) as usize];
        let value = match next(state) % 9 {
            0 => Json::Null,
            1 => json!(false),
            2 => json!(true),
            3 => json!(u64::MAX),
            4 => json!(-1),
            5 => json!("not-a-boolean-or-count"),
            6 => json!([]),
            7 => json!({}),
            _ => json!(0),
        };
        *probe.pointer_mut(path).unwrap() = value;
    }
    probe
}

fn count_case(state: &mut u64, case: usize) -> (Json, bool) {
    let a = next(state) % 100 + 1;
    let b = next(state) % 100 + 1;
    let (segments, scanned, pruned) = match case % 8 {
        0 => (a + b, a, b),
        1 => (u64::MAX, u64::MAX - 1, 1),
        2 => (0, u64::MAX, 1),
        3 => (a + b + 1, a, b),
        4 => (a, a, 0),
        5 => (b, 0, b),
        6 => (u64::MAX, u64::MAX, 1),
        _ => (a + b, a, b),
    };
    let explain_matches = case % 8 != 7;
    let sample = json!({
        "field": "kind", "operation": "eq", "operation_family": "equality",
        "ready": true, "capability_ready": true, "persisted_segment_descriptor_used": true,
        "segment_count": segments, "scanned_segment_count": scanned, "pruned_segment_count": pruned,
        "segment_pruning_candidate_document_count": 6,
        "segment_scanned_document_count": 2, "segment_pruned_document_count": 4,
        "explain_analyze": {
            "ready": true, "operator": "search_projection_segment_scan",
            "segment_count": if explain_matches { segments } else { segments + 1 },
            "scanned_segment_count": scanned, "pruned_segment_count": pruned,
            "candidate_document_count": 6, "scanned_document_count": 2, "pruned_document_count": 4,
            "payload_read_avoidance": true
        }
    });
    let expected = scanned > 0
        && pruned > 0
        && segments > 0
        && u128::from(scanned) + u128::from(pruned) == u128::from(segments)
        && explain_matches;
    (sample, expected)
}

fn campaign(seeds: u64, expected_digest: &str) {
    let contract = nowledge_search_projection_probe_contract_json();
    let primary = &contract["example_primary_probe"];
    let bases = [primary, &contract["example_hawdb_probe"]];
    let mut hasher = IntegrityHasher::new();
    record(&mut hasher, &contract);
    let mut cases = 0;
    for seed in 0..seeds {
        let mut state = seed;
        for base in bases {
            for case in 0..24 {
                let mask = if case == 0 {
                    0
                } else if case <= 19 {
                    1 << (case - 1)
                } else {
                    next(&mut state)
                };
                let (probe, blockers) = model_case(base, mask, (next(&mut state) % 6) as usize);
                let evidence = evaluate(&mut hasher, primary, &probe);
                assert_eq!(
                    evidence["blocker_codes"],
                    json!(blockers),
                    "seed={seed} case={case}"
                );
                assert_eq!(
                    evidence["ready"],
                    blockers.is_empty(),
                    "seed={seed} case={case}"
                );
                cases += 1;
            }
            for _ in 0..8 {
                let probe = malformed_case(base, &mut state);
                evaluate(&mut hasher, primary, &probe);
                cases += 1;
            }
        }
        for case in 0..16 {
            let (sample, expected) = count_case(&mut state, case);
            let actual = production_filter_pruning_sample_ready(&sample);
            assert_eq!(actual, expected, "seed={seed} count_case={case}");
            record(&mut hasher, &sample);
            record(&mut hasher, &json!(actual));
            cases += 1;
        }
        for value in [
            Json::Null,
            json!(false),
            json!(42),
            json!("ready"),
            json!([]),
        ] {
            let typed = NowledgeSearchProjectionEvidenceReport::from_evidence(value.clone());
            assert!(!typed.ready);
            assert_eq!(typed.json(), value);
            record(&mut hasher, &json!(format!("{typed:?}")));
            cases += 1;
        }
    }
    let digest = hasher.finish().sha256.to_string();
    eprintln!("search-projection-evidence-v1 seeds={seeds} cases={cases} sha256={digest}");
    assert_eq!(digest, expected_digest, "baseline report corpus changed");
}

#[test]
fn search_projection_evidence_differential_smoke() {
    campaign(2, SMOKE_DIGEST);
}

#[test]
#[ignore = "explicit local search projection evidence campaign"]
fn search_projection_evidence_differential_campaign() {
    campaign(128, CAMPAIGN_DIGEST);
}
