use super::{nowledge_mem_integration_bundle_json, IntegrationBundleInputs};
use crate::graph_summary::{
    nowledge_graph_route_readiness_summary, nowledge_graph_route_readiness_summary_from_bundle,
};
use serde_json::{json, Value};

const GROUPS: &[&str] = &[
    "bounded_read_evidence",
    "graph_route_readiness",
    "query_runtime_preflight",
    "search_route_ownership",
    "active_search_route_ownership",
    "active_search_route_readiness",
];

fn ready() -> Value {
    serde_json::from_str(include_str!("ready.json")).unwrap()
}

pub(crate) fn inputs(value: &serde_json::Value) -> IntegrationBundleInputs {
    IntegrationBundleInputs {
        require_ready: false,
        submodule_path: Some("/redacted/vendor/skein".to_string()),
        submodule_commit: Some("abc1234".to_string()),
        legacy_data_retained: true,
        legacy_data_deleted: false,
        coexistence_mode: Some("shadow".to_string()),
        content_store_present: true,
        content_store_engine: Some("sqlite".to_string()),
        content_store_messages_available: true,
        content_store_source_chunks_available: true,
        previous_wrapper_preflight: value.get("previous_wrapper_preflight").cloned(),
        replacement_summary: value.get("replacement_summary").cloned(),
        bounded_read_evidence: value.get("bounded_read_evidence").cloned(),
        graph_route_readiness: value.get("graph_route_readiness").cloned(),
        route_ownership: value.get("route_ownership").cloned(),
        search_route_ownership: value.get("search_route_ownership").cloned(),
        active_search_route_ownership: value.get("active_search_route_ownership").cloned(),
        active_search_route_readiness: value.get("active_search_route_readiness").cloned(),
        query_runtime_preflight: value.get("query_runtime_preflight").cloned(),
        search_candidate_shadow_evidence: value.get("search_candidate_shadow_evidence").cloned(),
        library_readiness: value.get("library_readiness").cloned(),
        cutover_controls: value.get("cutover_controls").cloned(),
        operations_readiness: value.get("operations_readiness").cloned(),
        blackbox_manifest: value.get("blackbox_manifest").cloned(),
    }
}

fn patch(value: &mut Value, changes: &Value) {
    for change in changes.as_array().unwrap() {
        let path = change[0].as_str().unwrap();
        if let Some(replacement) = change.get(1) {
            *value
                .pointer_mut(path)
                .unwrap_or_else(|| panic!("missing fixture path: {path}")) = replacement.clone();
        } else {
            let (parent, key) = path.rsplit_once('/').unwrap();
            assert!(value
                .pointer_mut(parent)
                .unwrap()
                .as_object_mut()
                .unwrap()
                .remove(key)
                .is_some());
        }
    }
}

fn cases() -> Vec<Value> {
    include_str!("cases.jsonl")
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn check(selected: &[&Value], label: &str) {
    let mut expected = ready();
    let mut expected_summary: Value =
        serde_json::from_str(include_str!("../graph_summary/ready.json")).unwrap();
    for case in selected {
        // Input reports are passed through verbatim; only derived fields need an oracle delta.
        patch(&mut expected, &case["changes"]);
        patch(&mut expected, &case["expected"]);
        patch(&mut expected_summary, &case["summary"]);
    }
    let before = expected.clone();
    let actual = nowledge_mem_integration_bundle_json(inputs(&expected)).unwrap();
    assert_eq!(actual, expected, "{label}: {selected:?}");
    let graph = &actual["graph_route_readiness"];
    let direct = nowledge_graph_route_readiness_summary(graph);
    assert_eq!(direct.json(), expected_summary, "{label}: graph summary");
    assert_eq!(
        nowledge_graph_route_readiness_summary_from_bundle(&actual),
        direct
    );
    assert_eq!(
        nowledge_graph_route_readiness_summary_from_bundle(
            &json!({"cutover_evidence": {"graph_route_readiness": graph}})
        ),
        direct
    );
    assert_eq!(expected, before, "{label}: caller evidence mutated");
}

#[test]
fn frozen_complete_bundle_and_summary_contract() {
    check(&[], "baseline");
}

#[test]
fn pre_migration_boundary_corpus() {
    let corpus = cases();
    assert_eq!(corpus.len(), 1266);
    for case in &corpus {
        check(&[case], case["name"].as_str().unwrap());
    }
}

#[test]
fn integration_bundle_differential_smoke() {
    campaign(2, 8, false);
}

#[test]
#[ignore = "explicit local cross-report combination campaign"]
fn integration_bundle_differential_campaign() {
    campaign(32, 128, true);
}

fn campaign(seeds: u64, cases_per_seed: usize, include_corpus: bool) {
    let corpus = cases();
    let groups = GROUPS
        .iter()
        .map(|group| {
            corpus
                .iter()
                .filter(|case| case["group"] == *group)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut visited = std::collections::BTreeSet::new();
    for seed in 0..seeds {
        let mut state = seed + 1;
        for case in 0..cases_per_seed {
            let selected = groups
                .iter()
                .map(|group| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    let chosen = group[(state % group.len() as u64) as usize];
                    visited.insert(chosen["name"].as_str().unwrap());
                    chosen
                })
                .collect::<Vec<_>>();
            check(&selected, &format!("seed={seed} case={case}"));
        }
    }
    // Every frozen case is exercised separately, including combinations not drawn by a seed.
    if include_corpus {
        for case in &corpus {
            check(&[case], case["name"].as_str().unwrap());
        }
    }
    eprintln!("integration-bundle-differential-v1 seeds={seeds} combined_cases={} corpus_cases={} combined_corpus_entries={}", seeds as usize * cases_per_seed, if include_corpus { corpus.len() } else { 0 }, visited.len());
}

#[test]
fn required_inputs_preserve_admission_errors_and_order() {
    let err = nowledge_mem_integration_bundle_json(IntegrationBundleInputs::default()).unwrap_err();
    assert!(
        matches!(err, skein_core::SkeinError::Semantic(ref message) if message == "--submodule-path is required")
    );
    let complete = inputs(&ready());
    type RemoveInput = fn(&mut IntegrationBundleInputs);
    let missing: &[(&str, RemoveInput)] = &[
        ("--previous-wrapper-preflight-json", |input| {
            input.previous_wrapper_preflight = None
        }),
        ("--replacement-summary-json", |input| {
            input.replacement_summary = None
        }),
        ("--bounded-read-evidence-json", |input| {
            input.bounded_read_evidence = None
        }),
        ("--graph-route-readiness-json", |input| {
            input.graph_route_readiness = None
        }),
        ("--route-ownership-json", |input| {
            input.route_ownership = None
        }),
        ("--search-route-ownership-json", |input| {
            input.search_route_ownership = None
        }),
        ("--active-search-route-ownership-json", |input| {
            input.active_search_route_ownership = None
        }),
        ("--active-search-route-readiness-json", |input| {
            input.active_search_route_readiness = None
        }),
        ("--query-runtime-preflight-json", |input| {
            input.query_runtime_preflight = None
        }),
        ("--search-candidate-shadow-evidence-json", |input| {
            input.search_candidate_shadow_evidence = None
        }),
        ("--library-readiness-json", |input| {
            input.library_readiness = None
        }),
        ("--cutover-controls-json", |input| {
            input.cutover_controls = None
        }),
        ("--operations-readiness-json", |input| {
            input.operations_readiness = None
        }),
        ("--blackbox-manifest-json", |input| {
            input.blackbox_manifest = None
        }),
    ];
    for &(flag, remove) in missing {
        let mut input = complete.clone();
        remove(&mut input);
        let err = nowledge_mem_integration_bundle_json(input).unwrap_err();
        assert!(
            matches!(err, skein_core::SkeinError::Semantic(ref message) if message == &format!("{flag} is required")),
            "{flag}: {err:?}"
        );
    }
    for path in [None, Some("".to_string()), Some(" \t ".to_string())] {
        let mut input = complete.clone();
        input.submodule_path = path;
        assert!(
            matches!(nowledge_mem_integration_bundle_json(input), Err(skein_core::SkeinError::Semantic(message)) if message == "--submodule-path is required")
        );
    }
    let mut input = complete;
    input.coexistence_mode = Some("active".to_string());
    input.content_store_engine = None;
    assert!(
        matches!(nowledge_mem_integration_bundle_json(input), Err(skein_core::SkeinError::Semantic(message)) if message == "--coexistence-mode must be shadow or side_by_side")
    );
}

#[test]
fn require_ready_remains_adapter_policy_and_blocker_order_is_stable() {
    for bits in 0..64 {
        let mut input = inputs(&ready());
        input.require_ready = bits & 1 != 0;
        input.legacy_data_retained = bits & 2 != 0;
        input.legacy_data_deleted = bits & 4 != 0;
        input.content_store_present = bits & 8 != 0;
        input.content_store_messages_available = bits & 16 != 0;
        input.content_store_source_chunks_available = bits & 32 != 0;
        let expected_coexistence = [
            (!input.legacy_data_retained, "legacy_data_not_retained"),
            (input.legacy_data_deleted, "legacy_data_deleted"),
        ]
        .into_iter()
        .filter_map(|(blocked, code)| blocked.then_some(code))
        .collect::<Vec<_>>();
        let expected_content = [
            (!input.content_store_present, "content_store_missing"),
            (
                !input.content_store_messages_available,
                "content_store_messages_missing",
            ),
            (
                !input.content_store_source_chunks_available,
                "content_store_source_chunks_missing",
            ),
        ]
        .into_iter()
        .filter_map(|(blocked, code)| blocked.then_some(code))
        .collect::<Vec<_>>();
        let mut other = input.clone();
        other.require_ready = !input.require_ready;
        let bundle = nowledge_mem_integration_bundle_json(input).unwrap();
        assert_eq!(bundle, nowledge_mem_integration_bundle_json(other).unwrap());
        assert_eq!(
            bundle["coexistence"]["blocker_codes"],
            json!(expected_coexistence)
        );
        assert_eq!(
            bundle["content_store"]["blocker_codes"],
            json!(expected_content)
        );
    }
}

#[test]
fn path_redaction_and_nonempty_strings_are_not_normalized() {
    for (path, label) in [
        ("/secret/vendor/skein", "skein"),
        ("/", "<redacted>"),
        ("/secret/ ", "<redacted>"),
        ("relative", "relative"),
    ] {
        let mut input = inputs(&ready());
        input.submodule_path = Some(path.to_string());
        input.submodule_commit = Some(" commit ".to_string());
        input.content_store_engine = Some(" sqlite ".to_string());
        let result = nowledge_mem_integration_bundle_json(input).unwrap();
        assert_eq!(result["submodule"]["path"], label);
        assert_eq!(result["submodule"]["commit"], " commit ");
        assert_eq!(result["content_store"]["engine"], " sqlite ");
    }
}
