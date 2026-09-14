use super::super::{
    nowledge_replacement_summary_json, nowledge_replacement_summary_json_with_options,
    NowledgeReplacementSummaryOptions,
};
use serde_json::{json, Value};

const NESTED_EVIDENCE_FIELDS: &[&str] = &[
    "search_projection_evidence",
    "search_projection_shadow_evidence",
    "search_candidate_shadow_evidence",
    "search_route_ownership",
    "active_search_route_ownership",
    "active_search_route_readiness",
    "bounded_read_evidence",
    "graph_route_readiness",
    "query_runtime_preflight",
    "workload_fixture_evidence",
    "source_mutation_dual_write_readiness",
];

#[test]
fn replacement_summary_preserves_complete_pre_migration_json() {
    for source in [include_str!("ready.json"), include_str!("blocked.json")] {
        let fixture: Value = serde_json::from_str(source).unwrap();
        assert_eq!(
            serde_json::to_vec(&nowledge_replacement_summary_json(&fixture["bundle"])).unwrap(),
            serde_json::to_vec(&fixture["summary"]).unwrap(),
            "fixture={}",
            fixture["name"],
        );
    }
}

#[test]
fn replacement_summary_differential_smoke() {
    campaign(4);
}

#[test]
#[ignore = "explicit local generated replacement-readiness campaign"]
fn replacement_summary_differential_campaign() {
    campaign(128);
}

fn campaign(seeds: usize) {
    let fixture: Value = serde_json::from_str(include_str!("ready.json")).unwrap();
    let mutations = rejected_mutations();
    let mut summaries = 0;
    let mut rejected = 0;
    for seed in 0..seeds {
        let mut bundle = fixture["bundle"].clone();
        let mut rng = Generator(seed as u64 + 1);
        let families = bundle["replacement_readiness_by_query_family"]
            .as_array_mut()
            .unwrap();
        let rotation = seed % families.len();
        families.rotate_left(rotation);
        let count = 1 + rng.below(1024);
        for field in [
            "primary_check_count",
            "shadow_check_count",
            "matched_check_count",
        ] {
            bundle["dual_engine_evidence"][field] = json!(count);
        }
        for field in NESTED_EVIDENCE_FIELDS {
            if rng.below(2) == 0 {
                let value = bundle.as_object_mut().unwrap().remove(*field).unwrap();
                bundle["cutover_evidence"][*field] = value;
            }
        }
        if rng.below(2) == 0 {
            let value = bundle
                .as_object_mut()
                .unwrap()
                .remove("dual_engine_evidence")
                .unwrap();
            bundle["cutover"]["dual_engine_evidence"] = value;
        }
        let mut blockers = Vec::new();
        for section in ["inventory_gate", "cutover", "migration_gate"] {
            let values = (0..6)
                .map(|_| format!("reason_{}", rng.below(7)))
                .collect::<Vec<_>>();
            blockers.extend(values.iter().cloned());
            bundle[section]["blockers"] = json!(values);
            // Non-string diagnostics must not become accidental blocker text.
            bundle[section]["blockers"].as_array_mut().unwrap().extend([
                Value::Null,
                json!(true),
                json!(17),
            ]);
        }
        blockers.sort();
        blockers.dedup();
        let baseline = nowledge_replacement_summary_json(&bundle);
        assert_eq!(baseline["production_cutover_ready"], true, "seed={seed}");
        assert_eq!(baseline["blocking_categories"], json!([]));
        assert_eq!(baseline["blockers"], json!(blockers));
        summaries += check_options(&bundle, &baseline, seed);

        for (case, (pointer, bad, category)) in mutations.iter().enumerate() {
            for invalid in [bad.clone(), Value::Null, json!("invalid-field-type")] {
                let mut input = bundle.clone();
                replace_effective(&mut input, pointer, invalid);
                let summary = nowledge_replacement_summary_json(&input);
                assert_rejected(&summary, category, seed, case);
                assert_eq!(summary["blockers"], json!(blockers));
                summaries += check_options(&input, &summary, seed + case);
                rejected += 1;
            }
        }

        // Combine independent broken gates without trusting the input's ready
        // flags. Every selected contradiction must survive the combined report.
        for case in 0..8 {
            let mut input = bundle.clone();
            let mut categories = Vec::new();
            for (index, (pointer, bad, category)) in mutations.iter().enumerate() {
                if index == case || rng.below(4) == 0 {
                    replace_effective(&mut input, pointer, bad.clone());
                    categories.push(*category);
                }
            }
            let summary = nowledge_replacement_summary_json(&input);
            for category in categories {
                assert_rejected(&summary, category, seed, case);
            }
            summaries += check_options(&input, &summary, seed + case);
            rejected += 1;
        }
    }
    eprintln!(
        "replacement-summary-differential-v1 seeds={seeds} rejected_cases={rejected} option_comparisons={summaries}"
    );
}

fn check_options(bundle: &Value, full: &Value, seed: usize) -> usize {
    let options = [
        NowledgeReplacementSummaryOptions::default(),
        NowledgeReplacementSummaryOptions {
            include_family_details: false,
            max_family_items: Some(usize::MAX),
            include_blocker_details: false,
            max_blockers: Some(usize::MAX),
        },
        NowledgeReplacementSummaryOptions {
            include_family_details: true,
            max_family_items: Some(seed % 7),
            include_blocker_details: true,
            max_blockers: Some(seed % 9),
        },
        NowledgeReplacementSummaryOptions {
            include_family_details: true,
            max_family_items: Some(usize::MAX),
            include_blocker_details: true,
            max_blockers: Some(usize::MAX),
        },
    ];
    for option in options {
        // The oracle changes presentation only, independently of the reducer's
        // detail helpers; every readiness field and diagnostic order is exact.
        let mut expected = full.clone();
        for (items, summary, visible, limit) in [
            (
                "replacement_readiness_by_query_family",
                "replacement_readiness_family_summary",
                option.include_family_details,
                option.max_family_items,
            ),
            (
                "blockers",
                "blocker_summary",
                option.include_blocker_details,
                option.max_blockers,
            ),
        ] {
            let original = full[items].as_array().unwrap();
            let kept = if visible {
                limit.unwrap_or(original.len()).min(original.len())
            } else {
                0
            };
            expected[items] = json!(&original[..kept]);
            expected[summary]["omitted_count"] = json!(original.len() - kept);
        }
        assert_eq!(
            nowledge_replacement_summary_json_with_options(bundle, option),
            expected,
            "seed={seed} options={option:?}",
        );
    }
    options.len()
}

fn assert_rejected(summary: &Value, category: &str, seed: usize, case: usize) {
    assert_eq!(
        summary["production_cutover_ready"], false,
        "seed={seed} case={case} category={category}"
    );
    assert_eq!(summary["production_replacement_per_million"], 0);
    let categories = summary["blocking_categories"].as_array().unwrap();
    assert!(
        categories.contains(&json!(category)),
        "seed={seed} case={case} expected={category} actual={categories:?}"
    );
    let names = categories
        .iter()
        .map(|value| value.as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(names.windows(2).all(|pair| pair[0] < pair[1]));
}

fn replace_effective(bundle: &mut Value, pointer: &str, value: Value) {
    let effective = if bundle.pointer(pointer).is_some() {
        pointer.to_string()
    } else if pointer.starts_with("/dual_engine_evidence/") {
        format!("/cutover{pointer}")
    } else {
        format!("/cutover_evidence{pointer}")
    };
    *bundle
        .pointer_mut(&effective)
        .expect("mutation must reach an existing field") = value;
}

fn rejected_mutations() -> Vec<(&'static str, Value, &'static str)> {
    vec![
        ("/migration_gate/decision", json!("blocked"), "migration_gate"),
        ("/cutover/decision", json!("blocked"), "shadow_parity"),
        ("/cutover_evidence/eligible", json!(false), "cutover_evidence"),
        ("/shadow_ready/wrapper_identity", json!("different-wrapper"), "shadow_parity"),
        ("/previous_wrapper_contract_evidence/ready", json!(false), "previous_wrapper_contract"),
        ("/full_contract_checked", json!(false), "previous_wrapper_contract"),
        ("/dual_engine_evidence/matched_check_count", json!(0), "dual_engine_evidence"),
        ("/search_projection_evidence/fts_ready", json!(false), "search_projection_evidence"),
        ("/search_projection_shadow_evidence/document_count_parity", json!(false), "search_projection_shadow_evidence"),
        ("/search_candidate_shadow_evidence/primary_only_candidate_count", json!(1), "search_candidate_shadow_evidence"),
        ("/search_route_ownership/lancedb_route_count", json!(1), "search_route_ownership"),
        ("/active_search_route_ownership/skein_route_count", json!(0), "active_search_route_ownership"),
        ("/active_search_route_readiness/lancedb_handle_required_route_count", json!(1), "active_search_route_readiness"),
        ("/bounded_read_evidence/payload_budget_exceeded", json!(true), "bounded_read_evidence"),
        ("/graph_route_readiness/protocol", json!("wrong-protocol"), "graph_route_readiness"),
        ("/query_runtime_preflight/failed_probe_count", json!(1), "query_runtime_preflight"),
        ("/workload_fixture_evidence/failed_query_count", json!(1), "workload_fixture_evidence"),
        ("/source_mutation_dual_write_readiness/ready_family_count", json!(0), "source_mutation_dual_write_readiness"),
        ("/cutover_evidence/storage_recovery_torn_tail_clean", json!(false), "storage_recovery"),
        ("/cutover_evidence/background_maintenance_executable_search_projection_graph_delta_count", Value::Null, "background_maintenance"),
        ("/replacement_readiness_by_query_family", json!([]), "query_family_readiness"),
        ("/replacement_readiness_per_million", json!(0), "query_family_readiness"),
    ]
}

#[test]
fn top_level_invalid_evidence_never_falls_back_to_nested_ready_evidence() {
    let fixture: Value = serde_json::from_str(include_str!("ready.json")).unwrap();
    let ready = &fixture["bundle"];
    let baseline = nowledge_replacement_summary_json(ready);
    for (parent, field) in NESTED_EVIDENCE_FIELDS
        .iter()
        .map(|field| ("cutover_evidence", *field))
        .chain([("cutover", "dual_engine_evidence")])
    {
        let mut nested = ready.clone();
        let value = nested.as_object_mut().unwrap().remove(field).unwrap();
        nested[parent][field] = value;
        assert_eq!(
            nowledge_replacement_summary_json(&nested),
            baseline,
            "field={field}"
        );
        for invalid in [Value::Null, json!({}), json!(false)] {
            let mut input = nested.clone();
            input[field] = invalid;
            let summary = nowledge_replacement_summary_json(&input);
            assert_eq!(summary["production_cutover_ready"], false, "field={field}");
        }
    }

    let mut nested_contract = ready.clone();
    for field in [
        "required_contract_ready",
        "full_contract_checked",
        "full_contract_ready",
        "selected_checks",
        "check_count",
    ] {
        nested_contract["contract_evidence"][field] = ready[field].clone();
    }
    assert_eq!(
        nowledge_replacement_summary_json(&nested_contract),
        baseline
    );
    for invalid in [Value::Null, json!({}), json!(false)] {
        nested_contract["contract_evidence"] = invalid;
        assert_eq!(
            nowledge_replacement_summary_json(&nested_contract)["production_cutover_ready"],
            false,
        );
    }
}

struct Generator(u64);

impl Generator {
    fn below(&mut self, bound: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % bound as u64) as usize
    }
}
