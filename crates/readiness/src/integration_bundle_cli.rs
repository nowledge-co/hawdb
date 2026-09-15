//! File-backed input adapter for integration-bundle preflight evidence.
//! The bundle assembly remains usable directly through typed Rust APIs.

use crate::integration_bundle::{nowledge_mem_integration_bundle_json, IntegrationBundleInputs};
use skein_core::{Result, SkeinError};
use std::path::Path;

pub fn nowledge_mem_integration_bundle_usage() -> String {
    "nowledge-mem-integration-bundle requires [--require-ready] --submodule-path <path> --submodule-commit <commit> --legacy-data-retained --coexistence-mode shadow|side_by_side --content-store-present --content-store-engine sqlite --content-store-messages-available --content-store-source-chunks-available --previous-wrapper-preflight-json <path> --replacement-summary-json <path> --bounded-read-evidence-json <path> --graph-route-readiness-json <path> --route-ownership-json <path> --search-route-ownership-json <path> --active-search-route-ownership-json <path> --active-search-route-readiness-json <path> --query-runtime-preflight-json <path> --search-candidate-shadow-evidence-json <path> --library-readiness-json <path> --cutover-controls-json <path> --operations-readiness-json <path> --blackbox-manifest-json <path>"
        .to_string()
}

pub fn run_nowledge_mem_integration_bundle(
    mut args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let mut inputs = IntegrationBundleInputs::default();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--require-ready" => {
                inputs.require_ready = true;
            }
            "--submodule-path" => {
                inputs.submodule_path = Some(next_arg(&mut args)?);
            }
            "--submodule-commit" => {
                inputs.submodule_commit = Some(next_arg(&mut args)?);
            }
            "--legacy-data-retained" => {
                inputs.legacy_data_retained = true;
            }
            "--legacy-data-deleted" => {
                inputs.legacy_data_deleted = true;
            }
            "--coexistence-mode" => {
                inputs.coexistence_mode = Some(next_arg(&mut args)?);
            }
            "--content-store-present" => {
                inputs.content_store_present = true;
            }
            "--content-store-engine" => {
                inputs.content_store_engine = Some(next_arg(&mut args)?);
            }
            "--content-store-messages-available" => {
                inputs.content_store_messages_available = true;
            }
            "--content-store-source-chunks-available" => {
                inputs.content_store_source_chunks_available = true;
            }
            "--previous-wrapper-preflight-json" => {
                inputs.previous_wrapper_preflight = Some(read_json_arg(&mut args)?);
            }
            "--replacement-summary-json" => {
                inputs.replacement_summary = Some(read_json_arg(&mut args)?);
            }
            "--bounded-read-evidence-json" => {
                inputs.bounded_read_evidence = Some(read_json_arg(&mut args)?);
            }
            "--graph-route-readiness-json" => {
                inputs.graph_route_readiness = Some(read_json_arg(&mut args)?);
            }
            "--route-ownership-json" => {
                inputs.route_ownership = Some(read_json_arg(&mut args)?);
            }
            "--search-route-ownership-json" => {
                inputs.search_route_ownership = Some(read_json_arg(&mut args)?);
            }
            "--active-search-route-ownership-json" => {
                inputs.active_search_route_ownership = Some(read_json_arg(&mut args)?);
            }
            "--active-search-route-readiness-json" => {
                inputs.active_search_route_readiness = Some(read_json_arg(&mut args)?);
            }
            "--query-runtime-preflight-json" => {
                inputs.query_runtime_preflight = Some(read_json_arg(&mut args)?);
            }
            "--search-candidate-shadow-evidence-json" => {
                inputs.search_candidate_shadow_evidence = Some(read_json_arg(&mut args)?);
            }
            "--library-readiness-json" => {
                inputs.library_readiness = Some(read_json_arg(&mut args)?);
            }
            "--cutover-controls-json" => {
                inputs.cutover_controls = Some(read_json_arg(&mut args)?);
            }
            "--operations-readiness-json" => {
                inputs.operations_readiness = Some(read_json_arg(&mut args)?);
            }
            "--blackbox-manifest-json" => {
                inputs.blackbox_manifest = Some(read_json_arg(&mut args)?);
            }
            _ => {
                return Err(SkeinError::Semantic(nowledge_mem_integration_bundle_usage()));
            }
        }
    }

    let require_ready = inputs.require_ready;
    Ok((nowledge_mem_integration_bundle_json(inputs)?, require_ready))
}

fn read_json_arg(args: &mut impl Iterator<Item = String>) -> Result<serde_json::Value> {
    let path = next_arg(args)?;
    read_json_file(Path::new(&path))
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let raw = std::fs::read_to_string(path).map_err(|_| {
        SkeinError::Execution(
            "failed to read Nowledge Mem integration bundle input: io_error".to_string(),
        )
    })?;
    serde_json::from_str(&raw).map_err(|_| {
        SkeinError::Semantic(
            "failed to parse Nowledge Mem integration bundle input: invalid_json".to_string(),
        )
    })
}

fn next_arg(args: &mut impl Iterator<Item = String>) -> Result<String> {
    args.next()
        .ok_or_else(|| SkeinError::Semantic(nowledge_mem_integration_bundle_usage()))
}

#[cfg(test)]
mod tests {
    use super::{
        nowledge_mem_integration_bundle_usage, read_json_file, run_nowledge_mem_integration_bundle,
    };
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn bundle_input_read_errors_are_redacted_by_default() {
        let secret_path =
            unique_test_path("bundle-input-secret-path-do-not-emit").join("missing-secret.json");

        let error = read_json_file(&secret_path).unwrap_err().to_string();

        assert_eq!(
            error,
            "execution error: failed to read Nowledge Mem integration bundle input: io_error"
        );
        assert!(!error.contains("bundle-input-secret-path-do-not-emit"));
        assert!(!error.contains("missing-secret"));
    }

    #[test]
    fn bundle_input_parse_errors_are_redacted_by_default() {
        let root = unique_test_path("bundle-input-parse-redaction");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("secret-bundle-input-path-do-not-emit.json");
        std::fs::write(
            &path,
            "{ \"secret\": \"bundle-input-parse-secret-do-not-emit\", \"unterminated\": ",
        )
        .unwrap();

        let error = read_json_file(&path).unwrap_err().to_string();

        assert_eq!(
            error,
            "semantic error: failed to parse Nowledge Mem integration bundle input: invalid_json"
        );
        assert!(!error.contains("secret-bundle-input-path-do-not-emit"));
        assert!(!error.contains("bundle-input-parse-secret-do-not-emit"));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cli_rejects_unknown_flags_with_the_stable_usage_contract() {
        let error = run_nowledge_mem_integration_bundle(["--unknown".to_string()].into_iter())
            .unwrap_err()
            .to_string();

        assert_eq!(
            error,
            format!(
                "semantic error: {}",
                nowledge_mem_integration_bundle_usage()
            )
        );
    }

    #[test]
    fn cli_maps_complete_file_backed_evidence_to_the_owner_bundle() {
        let root = unique_test_path("integration-bundle-cli");
        std::fs::create_dir_all(&root).unwrap();
        let expected: serde_json::Value =
            serde_json::from_str(include_str!("integration_bundle/ready.json")).unwrap();
        let args = vec![
            "--require-ready".to_string(),
            "--submodule-path".to_string(),
            "/redacted/vendor/skein".to_string(),
            "--submodule-commit".to_string(),
            "abc1234".to_string(),
            "--legacy-data-retained".to_string(),
            "--coexistence-mode".to_string(),
            "shadow".to_string(),
            "--content-store-present".to_string(),
            "--content-store-engine".to_string(),
            "sqlite".to_string(),
            "--content-store-messages-available".to_string(),
            "--content-store-source-chunks-available".to_string(),
            "--previous-wrapper-preflight-json".to_string(),
            write_json(
                &root,
                "previous-wrapper-preflight",
                &expected["previous_wrapper_preflight"],
            ),
            "--replacement-summary-json".to_string(),
            write_json(
                &root,
                "replacement-summary",
                &expected["replacement_summary"],
            ),
            "--bounded-read-evidence-json".to_string(),
            write_json(
                &root,
                "bounded-read-evidence",
                &expected["bounded_read_evidence"],
            ),
            "--graph-route-readiness-json".to_string(),
            write_json(
                &root,
                "graph-route-readiness",
                &expected["graph_route_readiness"],
            ),
            "--route-ownership-json".to_string(),
            write_json(&root, "route-ownership", &expected["route_ownership"]),
            "--search-route-ownership-json".to_string(),
            write_json(
                &root,
                "search-route-ownership",
                &expected["search_route_ownership"],
            ),
            "--active-search-route-ownership-json".to_string(),
            write_json(
                &root,
                "active-search-route-ownership",
                &expected["active_search_route_ownership"],
            ),
            "--active-search-route-readiness-json".to_string(),
            write_json(
                &root,
                "active-search-route-readiness",
                &expected["active_search_route_readiness"],
            ),
            "--query-runtime-preflight-json".to_string(),
            write_json(
                &root,
                "query-runtime-preflight",
                &expected["query_runtime_preflight"],
            ),
            "--search-candidate-shadow-evidence-json".to_string(),
            write_json(
                &root,
                "search-candidate-shadow-evidence",
                &expected["search_candidate_shadow_evidence"],
            ),
            "--library-readiness-json".to_string(),
            write_json(&root, "library-readiness", &expected["library_readiness"]),
            "--cutover-controls-json".to_string(),
            write_json(&root, "cutover-controls", &expected["cutover_controls"]),
            "--operations-readiness-json".to_string(),
            write_json(
                &root,
                "operations-readiness",
                &expected["operations_readiness"],
            ),
            "--blackbox-manifest-json".to_string(),
            write_json(&root, "blackbox-manifest", &expected["blackbox_manifest"]),
        ];

        let (actual, require_ready) =
            run_nowledge_mem_integration_bundle(args.into_iter()).unwrap();

        assert!(require_ready);
        assert_eq!(actual, expected);
        std::fs::remove_dir_all(root).unwrap();
    }

    fn unique_test_path(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein-readiness-{label}-{nonce}"))
    }

    fn write_json(root: &Path, name: &str, value: &serde_json::Value) -> String {
        let path = root.join(format!("{name}.json"));
        std::fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
        path.to_string_lossy().into_owned()
    }
}
