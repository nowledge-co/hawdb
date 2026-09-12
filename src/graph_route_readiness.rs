//! Compatibility facade and developer file/CLI adapters for graph route readiness.

use crate::{Result, SkeinError};
use std::path::Path;

pub use skein_readiness::graph_route::{
    nowledge_graph_route_readiness_json, NMEM_GRAPH_ROUTE_EVIDENCE_PROTOCOL,
    NMEM_GRAPH_ROUTE_READINESS_PROTOCOL,
};

pub fn nowledge_graph_route_readiness_usage() -> String {
    "nowledge-graph-route-readiness requires [--require-ready] <route-evidence-json>".to_string()
}

pub fn run_nowledge_graph_route_readiness(
    args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let mut require_ready = false;
    let mut evidence_path = None;
    for arg in args {
        match arg.as_str() {
            "--require-ready" => {
                require_ready = true;
            }
            value if value.starts_with("--") => {
                return Err(SkeinError::Semantic(nowledge_graph_route_readiness_usage()));
            }
            path => {
                if evidence_path.replace(path.to_string()).is_some() {
                    return Err(SkeinError::Semantic(nowledge_graph_route_readiness_usage()));
                }
            }
        }
    }
    let Some(evidence_path) = evidence_path else {
        return Err(SkeinError::Semantic(nowledge_graph_route_readiness_usage()));
    };
    Ok((
        nowledge_graph_route_readiness_json(&read_json_file(Path::new(&evidence_path))?)?,
        require_ready,
    ))
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let raw = std::fs::read_to_string(path).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to read graph route readiness evidence: {}",
            error.kind()
        ))
    })?;
    serde_json::from_str(&raw).map_err(|_| {
        SkeinError::Semantic(
            "failed to parse graph route readiness evidence: invalid_json".to_string(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn facade_preserves_owner_reports_errors_and_protocol_paths() {
        for evidence in [
            serde_json::json!([]),
            serde_json::json!({"routes": []}),
            serde_json::json!({"routes": [{"route": " "}]}),
            serde_json::Value::Null,
        ] {
            let owner =
                skein_readiness::graph_route::nowledge_graph_route_readiness_json(&evidence)
                    .map_err(|error| error.to_string());
            assert_eq!(
                nowledge_graph_route_readiness_json(&evidence).map_err(|error| error.to_string()),
                owner
            );
            assert_eq!(
                crate::nowledge_graph_route_readiness_json(&evidence)
                    .map_err(|error| error.to_string()),
                owner
            );
        }
        assert_eq!(
            NMEM_GRAPH_ROUTE_READINESS_PROTOCOL,
            "nmem-graph-route-readiness-v1"
        );
        assert_eq!(
            NMEM_GRAPH_ROUTE_EVIDENCE_PROTOCOL,
            "nmem-graph-route-evidence-v1"
        );
        assert_eq!(
            crate::NMEM_GRAPH_ROUTE_READINESS_PROTOCOL,
            NMEM_GRAPH_ROUTE_READINESS_PROTOCOL
        );
        assert_eq!(
            crate::NMEM_GRAPH_ROUTE_EVIDENCE_PROTOCOL,
            NMEM_GRAPH_ROUTE_EVIDENCE_PROTOCOL
        );
        assert_eq!(
            crate::NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL,
            "skein-nowledge-mem-query-report-v1"
        );
        assert_eq!(
            crate::nowledge_mem::NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL,
            skein_evidence::inventory::NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL
        );
    }

    #[test]
    fn file_adapter_preserves_require_ready_and_owner_report() {
        let path = unique_test_file("graph_route_readiness_adapter");
        let evidence = serde_json::json!({"routes": []});
        std::fs::write(&path, serde_json::to_vec(&evidence).unwrap()).unwrap();
        for require_ready in [false, true] {
            let mut args = vec![path.to_str().unwrap().to_string()];
            if require_ready {
                args.push("--require-ready".to_string());
            }
            let (report, required) = run_nowledge_graph_route_readiness(args.into_iter()).unwrap();
            assert_eq!(required, require_ready);
            assert_eq!(
                report,
                nowledge_graph_route_readiness_json(&evidence).unwrap()
            );
            assert_eq!(report["route_primary_ready"], false);
        }
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn file_adapter_preserves_argument_errors_and_read_error_redaction() {
        for args in [
            vec![],
            vec!["--unknown"],
            vec!["first", "second"],
            vec!["--require-ready"],
        ] {
            assert!(matches!(
                run_nowledge_graph_route_readiness(args.into_iter().map(str::to_string)),
                Err(SkeinError::Semantic(message)) if message == nowledge_graph_route_readiness_usage()
            ));
        }
        let path = unique_test_file("private_graph_route_evidence");
        let error =
            run_nowledge_graph_route_readiness([path.to_str().unwrap().to_string()].into_iter())
                .unwrap_err();
        assert!(matches!(error, SkeinError::Execution(_)));
        let message = error.to_string();
        assert!(message.contains("failed to read graph route readiness evidence"));
        assert!(!message.contains("private_graph_route_evidence"));
        assert!(!message.contains(path.to_str().unwrap()));
    }

    #[test]
    fn route_readiness_parse_error_redacts_json_details() {
        let evidence_path = unique_test_file("secret_graph_route_readiness");
        std::fs::write(&evidence_path, "{\"secret_route_id\":").unwrap();

        let error = run_nowledge_graph_route_readiness(
            [evidence_path.to_str().unwrap().to_string()].into_iter(),
        )
        .unwrap_err();
        let message = error.to_string();

        assert_eq!(
            message,
            "semantic error: failed to parse graph route readiness evidence: invalid_json"
        );
        assert!(!message.contains(evidence_path.to_str().unwrap()));
        assert!(!message.contains("secret_route_id"));
    }

    fn unique_test_file(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein_{name}_{}_{nanos}.json", std::process::id()))
    }
}
