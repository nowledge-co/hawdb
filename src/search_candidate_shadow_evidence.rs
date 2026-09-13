use crate::{nowledge_mem_search_candidate_shadow_evidence_json, Result, SkeinError};
use std::path::Path;

pub use skein_search::candidate_evidence::parse_search_candidate_shadow_probe;

#[cfg(test)]
#[path = "../crates/search/src/candidate_evidence/tests/probe_fixtures.rs"]
mod probe_fixtures;

pub fn nowledge_search_candidate_shadow_evidence_usage() -> String {
    "nowledge-search-candidate-shadow-evidence requires [--require-ready] <candidate-shadow-probe-json>".to_string()
}

pub fn run_nowledge_search_candidate_shadow_evidence(
    args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let mut require_ready = false;
    let mut probe_path = None;
    for arg in args {
        match arg.as_str() {
            "--require-ready" => {
                require_ready = true;
            }
            value if value.starts_with("--") => {
                return Err(SkeinError::Semantic(
                    nowledge_search_candidate_shadow_evidence_usage(),
                ));
            }
            path => {
                if probe_path.replace(path.to_string()).is_some() {
                    return Err(SkeinError::Semantic(
                        nowledge_search_candidate_shadow_evidence_usage(),
                    ));
                }
            }
        }
    }
    let Some(probe_path) = probe_path else {
        return Err(SkeinError::Semantic(
            nowledge_search_candidate_shadow_evidence_usage(),
        ));
    };
    let probe = read_json_file(Path::new(&probe_path))?;
    let accumulator = parse_search_candidate_shadow_probe(&probe)?;
    Ok((
        nowledge_mem_search_candidate_shadow_evidence_json(&accumulator.evidence()),
        require_ready,
    ))
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let content = std::fs::read_to_string(path).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to read search candidate shadow probe JSON: {}",
            error.kind()
        ))
    })?;
    serde_json::from_str(&content).map_err(|_| {
        SkeinError::Semantic(
            "failed to parse search candidate shadow probe JSON: invalid_json".to_string(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::run_nowledge_search_candidate_shadow_evidence;
    use crate::NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn search_candidate_shadow_evidence_command_accepts_ready_probe() {
        let path = unique_test_file("search_candidate_shadow_probe");
        std::fs::write(&path, ready_probe().to_string()).unwrap();

        let (evidence, require_ready) = run_nowledge_search_candidate_shadow_evidence(
            ["--require-ready", path.to_str().unwrap()]
                .into_iter()
                .map(str::to_string),
        )
        .unwrap();

        assert!(require_ready);
        assert_eq!(
            evidence["protocol"],
            "skein-nowledge-search-candidate-shadow-evidence"
        );
        assert_eq!(
            evidence["route"],
            "/search-index/skein-shadow/candidate-evidence"
        );
        assert_eq!(evidence["ready"], true);
        assert_eq!(evidence["request_count"], 2);
        assert_eq!(evidence["primary_candidate_count"], 3);
        assert_eq!(evidence["shadow_candidate_count"], 3);
        assert_eq!(evidence["matched_candidate_count"], 3);
        assert_eq!(evidence["primary_only_candidate_count"], 0);
        assert_eq!(evidence["text_retriever_ready"], true);
        assert_eq!(evidence["vector_retriever_ready"], true);
        assert_eq!(evidence["retriever_leg_candidate_counts"]["text"], 3);
        assert_eq!(evidence["retriever_leg_candidate_counts"]["vector"], 3);
        assert_eq!(evidence["fts_top_k_overlap_ready"], true);
        assert_eq!(evidence["vector_top_k_overlap_ready"], true);
        assert_eq!(evidence["candidate_identity"]["ready"], true);
        assert_eq!(evidence["filter_pushdown"]["ready"], true);
        assert_eq!(
            evidence["filter_pushdown"]["field_summary_count"],
            NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len()
        );
        assert_eq!(evidence["blocker_codes"], serde_json::json!([]));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn search_candidate_shadow_probe_parse_errors_are_redacted_by_default() {
        let path = unique_test_file("search_candidate_secret_probe_path_do_not_emit");
        std::fs::write(
            &path,
            "{ \"primary_candidate_ids\": [\"secret-candidate-do-not-emit\"], \"unterminated\": ",
        )
        .unwrap();

        let error = super::read_json_file(&path).unwrap_err().to_string();

        assert_eq!(
            error,
            "semantic error: failed to parse search candidate shadow probe JSON: invalid_json"
        );
        assert!(!error.contains("search_candidate_secret_probe_path_do_not_emit"));
        assert!(!error.contains("secret-candidate-do-not-emit"));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn search_candidate_shadow_evidence_command_fails_closed_for_mismatch() {
        let path = unique_test_file("search_candidate_shadow_probe_mismatch");
        let mut probe = ready_probe();
        probe["requests"][0]["shadow_candidate_ids"] = serde_json::json!(["mem_1"]);
        std::fs::write(&path, probe.to_string()).unwrap();

        let (evidence, _) = run_nowledge_search_candidate_shadow_evidence(
            [path.to_str().unwrap()].into_iter().map(str::to_string),
        )
        .unwrap();

        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["candidate_identity"]["ready"], false);
        assert!(evidence["primary_only_candidate_count"].as_u64().unwrap() > 0);
        std::fs::remove_file(path).unwrap();
    }

    use super::probe_fixtures::ready_probe;

    fn unique_test_file(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein_{name}_{}_{nanos}.json", std::process::id()))
    }
}
