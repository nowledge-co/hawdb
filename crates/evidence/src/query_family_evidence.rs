use crate::inventory::{
    replacement_readiness_family_evidence_health_from_bundle,
    ReplacementReadinessFamilyEvidenceHealth, REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES,
};
use skein_core::{Result, SkeinError};
use std::path::Path;

pub fn nowledge_query_family_evidence_usage() -> String {
    "nowledge-query-family-evidence requires [--require-ready] <query-family-json>".to_string()
}

pub fn run_nowledge_query_family_evidence(
    mut args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let mut require_ready = false;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--require-ready" => {
                require_ready = true;
            }
            path => {
                if args.next().is_some() {
                    return Err(SkeinError::Semantic(nowledge_query_family_evidence_usage()));
                }
                let input = read_json_file(Path::new(path))?;
                return Ok((nowledge_query_family_evidence_json(&input)?, require_ready));
            }
        }
    }
    Err(SkeinError::Semantic(nowledge_query_family_evidence_usage()))
}

pub fn nowledge_query_family_evidence_json(input: &serde_json::Value) -> Result<serde_json::Value> {
    let families = query_family_array(input)?;
    let bundle = serde_json::json!({
        "replacement_readiness_by_query_family": families,
    });
    let health = replacement_readiness_family_evidence_health_from_bundle(&bundle);
    let blocker_codes = query_family_blocker_codes(&health);
    Ok(serde_json::json!({
        "protocol": "skein-nowledge-query-family-evidence-v1",
        "present": health.present,
        "ready": health.ready,
        "min_replacement_readiness_per_million": health.min_replacement_readiness_per_million,
        "invalid_family_count": health.invalid_family_count,
        "blocked_query_families": health.blocked_query_families,
        "required_query_families": REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES,
        "missing_required_query_families": health.missing_required_query_families,
        "blocker_codes": blocker_codes,
        "blockers": health.blockers,
        "replacement_readiness_by_query_family": families,
    }))
}

fn query_family_array(input: &serde_json::Value) -> Result<&Vec<serde_json::Value>> {
    if let Some(array) = input.as_array() {
        return Ok(array);
    }
    input
        .get("replacement_readiness_by_query_family")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            SkeinError::Semantic(
                "query family evidence requires a family array or replacement_readiness_by_query_family array".to_string(),
            )
        })
}

fn query_family_blocker_codes(
    health: &ReplacementReadinessFamilyEvidenceHealth,
) -> Vec<&'static str> {
    let mut blockers = Vec::new();
    if health.invalid_family_count > 0 {
        blockers.push("invalid_family_entries");
    }
    if !health.blocked_query_families.is_empty() {
        blockers.push("blocked_query_families");
    }
    if !health.missing_required_query_families.is_empty() {
        blockers.push("missing_required_query_families");
    }
    blockers
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let content = std::fs::read_to_string(path).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to read query family JSON: {}",
            error.kind()
        ))
    })?;
    serde_json::from_str(&content).map_err(|_| {
        SkeinError::Semantic("failed to parse query family JSON: invalid_json".to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::{run_nowledge_query_family_evidence, REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES};
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn query_family_evidence_command_accepts_ready_families() {
        let path = unique_test_file("query_family_ready");
        std::fs::write(&path, ready_families().to_string()).unwrap();

        let (evidence, require_ready) = run_nowledge_query_family_evidence(
            ["--require-ready", path.to_str().unwrap()]
                .into_iter()
                .map(str::to_string),
        )
        .unwrap();

        assert!(require_ready);
        assert_eq!(
            evidence["protocol"],
            "skein-nowledge-query-family-evidence-v1"
        );
        assert_eq!(evidence["ready"], true);
        assert_eq!(evidence["min_replacement_readiness_per_million"], 1_000_000);
        assert_eq!(evidence["blocker_codes"], serde_json::json!([]));
        assert_eq!(
            evidence["missing_required_query_families"],
            serde_json::json!([])
        );
        assert_eq!(
            evidence["replacement_readiness_by_query_family"]
                .as_array()
                .unwrap()
                .len(),
            REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES.len()
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn query_family_parse_errors_are_redacted_by_default() {
        let path = unique_test_file("query_family_secret_path_do_not_emit");
        std::fs::write(
            &path,
            "{ \"query_family\": \"secret-family-do-not-emit\", \"unterminated\": ",
        )
        .unwrap();

        let error = super::read_json_file(&path).unwrap_err().to_string();

        assert_eq!(
            error,
            "semantic error: failed to parse query family JSON: invalid_json"
        );
        assert!(!error.contains("query_family_secret_path_do_not_emit"));
        assert!(!error.contains("secret-family-do-not-emit"));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn query_family_evidence_command_fails_closed_for_missing_required_families() {
        let path = unique_test_file("query_family_missing_required");
        let families = serde_json::json!([
            {
                "query_family": "read",
                "replacement_readiness_per_million": 1000000
            },
            {
                "query_family": "mutation",
                "replacement_readiness_per_million": 1000000
            }
        ]);
        std::fs::write(&path, families.to_string()).unwrap();

        let (evidence, _) = run_nowledge_query_family_evidence(
            [path.to_str().unwrap()].into_iter().map(str::to_string),
        )
        .unwrap();

        assert_eq!(evidence["ready"], false);
        assert_eq!(
            evidence["blocker_codes"],
            serde_json::json!(["missing_required_query_families"])
        );
        assert_eq!(
            evidence["missing_required_query_families"],
            serde_json::json!([
                "memory_lookup",
                "graph_traversal",
                "projected_graph",
                "label_stats_read",
                "search_projection"
            ])
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn query_family_evidence_command_fails_closed_for_blocked_family() {
        let path = unique_test_file("query_family_blocked");
        let families = serde_json::json!([
            {
                "query_family": "graph_traversal",
                "replacement_readiness_per_million": 500000
            },
            {
                "query_family": "memory_lookup",
                "replacement_readiness_per_million": 1000000
            },
            {
                "query_family": "projected_graph",
                "replacement_readiness_per_million": 1000000
            },
            {
                "query_family": "label_stats_read",
                "replacement_readiness_per_million": 1000000
            },
            {
                "query_family": "search_projection",
                "replacement_readiness_per_million": 1000000
            }
        ]);
        std::fs::write(&path, families.to_string()).unwrap();

        let (evidence, _) = run_nowledge_query_family_evidence(
            [path.to_str().unwrap()].into_iter().map(str::to_string),
        )
        .unwrap();

        assert_eq!(evidence["ready"], false);
        assert_eq!(
            evidence["blocked_query_families"],
            serde_json::json!(["graph_traversal"])
        );
        assert_eq!(
            evidence["blocker_codes"],
            serde_json::json!(["blocked_query_families"])
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn query_family_evidence_command_rejects_missing_family_array() {
        let path = unique_test_file("query_family_missing");
        std::fs::write(&path, serde_json::json!({}).to_string()).unwrap();

        let error = run_nowledge_query_family_evidence(
            [path.to_str().unwrap()].into_iter().map(str::to_string),
        )
        .unwrap_err();

        assert!(error.to_string().contains("query family evidence requires"));
        std::fs::remove_file(path).unwrap();
    }

    fn ready_families() -> serde_json::Value {
        serde_json::json!({
            "replacement_readiness_by_query_family": [
                {
                    "query_family": "memory_lookup",
                    "replacement_readiness_per_million": 1000000
                },
                {
                    "query_family": "graph_traversal",
                    "replacement_readiness_per_million": 1000000
                },
                {
                    "query_family": "projected_graph",
                    "replacement_readiness_per_million": 1000000
                },
                {
                    "query_family": "label_stats_read",
                    "replacement_readiness_per_million": 1000000
                },
                {
                    "query_family": "search_projection",
                    "replacement_readiness_per_million": 1000000
                }
            ]
        })
    }

    fn unique_test_file(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein_{name}_{}_{nanos}.json", std::process::id()))
    }
}
