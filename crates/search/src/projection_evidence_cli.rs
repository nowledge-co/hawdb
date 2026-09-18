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

//! Developer-facing search projection evidence and probe adapters.
//!
//! The typed evidence reducer lives in `projection_evidence`; this module only
//! maps CLI and file inputs to the search owner APIs.
use crate::{SearchIndex, SearchProjectionProbeOptions};
use hawdb_core::{HawDBError, Result};
use std::path::Path;

pub use crate::projection_evidence::{
    nowledge_search_projection_evidence_json, nowledge_search_projection_probe_contract_json,
    nowledge_search_projection_shadow_evidence_json, NowledgeSearchProjectionEvidenceReport,
};

pub fn nowledge_search_projection_evidence_usage() -> String {
    "nowledge-search-projection-evidence requires [--require-ready] <search-projection-probe-json>"
        .to_string()
}

pub fn hawdb_search_projection_probe_usage() -> String {
    "hawdb-search-projection-probe requires [--active-model <model>] [--active-dimension <dimension>] <search-index-dir>"
        .to_string()
}

pub fn nowledge_search_projection_shadow_evidence_usage() -> String {
    "nowledge-search-projection-shadow-evidence requires [--require-ready] --primary-probe-json <path> --shadow-probe-json <path>"
        .to_string()
}

pub fn nowledge_search_projection_probe_contract_usage() -> String {
    "nowledge-search-projection-probe-contract requires no arguments".to_string()
}

pub fn run_nowledge_search_projection_evidence(
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
                    return Err(HawDBError::Semantic(
                        nowledge_search_projection_evidence_usage(),
                    ));
                }
                let probe = read_json_file(Path::new(path))?;
                return Ok((
                    nowledge_search_projection_evidence_json(&probe),
                    require_ready,
                ));
            }
        }
    }
    Err(HawDBError::Semantic(
        nowledge_search_projection_evidence_usage(),
    ))
}

pub fn run_hawdb_search_projection_probe(
    mut args: impl Iterator<Item = String>,
) -> Result<serde_json::Value> {
    let mut options = SearchProjectionProbeOptions::default();
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--active-model" => {
                options.active_embedding_model =
                    Some(args.next().ok_or_else(|| {
                        HawDBError::Semantic(hawdb_search_projection_probe_usage())
                    })?);
            }
            "--active-dimension" => {
                let raw_dimension = args
                    .next()
                    .ok_or_else(|| HawDBError::Semantic(hawdb_search_projection_probe_usage()))?;
                options.active_embedding_dimension =
                    Some(parse_positive_usize("--active-dimension", &raw_dimension)?);
            }
            path => {
                if args.next().is_some() {
                    return Err(HawDBError::Semantic(hawdb_search_projection_probe_usage()));
                }
                let index = SearchIndex::open(path)?;
                return Ok(index.nowledge_search_projection_probe_json(options));
            }
        }
    }
    Err(HawDBError::Semantic(hawdb_search_projection_probe_usage()))
}

pub fn run_nowledge_search_projection_shadow_evidence(
    mut args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let mut require_ready = false;
    let mut primary_probe = None;
    let mut shadow_probe = None;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--require-ready" => {
                require_ready = true;
            }
            "--primary-probe-json" => {
                let path = args.next().ok_or_else(|| {
                    HawDBError::Semantic(nowledge_search_projection_shadow_evidence_usage())
                })?;
                primary_probe = Some(read_json_file(Path::new(&path))?);
            }
            "--shadow-probe-json" => {
                let path = args.next().ok_or_else(|| {
                    HawDBError::Semantic(nowledge_search_projection_shadow_evidence_usage())
                })?;
                shadow_probe = Some(read_json_file(Path::new(&path))?);
            }
            _ => {
                return Err(HawDBError::Semantic(
                    nowledge_search_projection_shadow_evidence_usage(),
                ));
            }
        }
    }
    let primary_probe = primary_probe
        .ok_or_else(|| HawDBError::Semantic(nowledge_search_projection_shadow_evidence_usage()))?;
    let shadow_probe = shadow_probe
        .ok_or_else(|| HawDBError::Semantic(nowledge_search_projection_shadow_evidence_usage()))?;
    Ok((
        nowledge_search_projection_shadow_evidence_json(&primary_probe, &shadow_probe),
        require_ready,
    ))
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let content = std::fs::read_to_string(path).map_err(|_| {
        HawDBError::Execution(
            "failed to read search projection evidence JSON: io_error".to_string(),
        )
    })?;
    serde_json::from_str(&content).map_err(|_| {
        HawDBError::Semantic(
            "failed to parse search projection evidence JSON: invalid_json".to_string(),
        )
    })
}

fn parse_positive_usize(flag: &str, value: &str) -> Result<usize> {
    let parsed = value.parse::<usize>().map_err(|error| {
        HawDBError::Semantic(format!("invalid {flag} value '{value}': {error}"))
    })?;
    if parsed == 0 {
        return Err(HawDBError::Semantic(format!(
            "invalid {flag} value '{value}': expected a positive integer"
        )));
    }
    Ok(parsed)
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        SearchEmbeddingManifest, SearchIndex, SearchProjectionDelta, SearchProjectionKind,
        SearchProjectionRow,
    };
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn commands_reject_incomplete_options() {
        let evidence_error = super::run_nowledge_search_projection_evidence(
            ["--require-ready".to_string()].into_iter(),
        )
        .unwrap_err()
        .to_string();
        assert_eq!(
            evidence_error,
            format!(
                "semantic error: {}",
                super::nowledge_search_projection_evidence_usage()
            )
        );

        let probe_error = super::run_hawdb_search_projection_probe(
            ["--active-dimension".to_string()].into_iter(),
        )
        .unwrap_err()
        .to_string();
        assert_eq!(
            probe_error,
            format!(
                "semantic error: {}",
                super::hawdb_search_projection_probe_usage()
            )
        );

        let shadow_error = super::run_nowledge_search_projection_shadow_evidence(
            ["--primary-probe-json".to_string()].into_iter(),
        )
        .unwrap_err()
        .to_string();
        assert_eq!(
            shadow_error,
            format!(
                "semantic error: {}",
                super::nowledge_search_projection_shadow_evidence_usage()
            )
        );
    }

    #[test]
    fn search_projection_evidence_owner_type_round_trips_contract_probe() {
        let contract = super::nowledge_search_projection_probe_contract_json();
        let probe = &contract["example_hawdb_probe"];
        let report = super::NowledgeSearchProjectionEvidenceReport::from_probe(probe);
        assert!(report.ready);
        assert_eq!(
            report.json(),
            nowledge_search_projection_evidence_json(probe)
        );
    }

    #[test]
    fn search_projection_evidence_read_errors_are_redacted_by_default() {
        let secret_path =
            unique_test_dir("search_projection_secret_path_do_not_emit").join("missing-probe.json");

        let error = super::read_json_file(&secret_path).unwrap_err().to_string();

        assert_eq!(
            error,
            "execution error: failed to read search projection evidence JSON: io_error"
        );
        assert!(!error.contains("search_projection_secret_path_do_not_emit"));
        assert!(!error.contains("missing-probe"));
    }

    #[test]
    fn search_projection_evidence_parse_errors_are_redacted_by_default() {
        let root = unique_test_dir("search_projection_parse_redaction");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("secret-projection-path-do-not-emit.json");
        std::fs::write(
            &path,
            "{ \"external_id\": \"secret-projection-doc-do-not-emit\", \"unterminated\": ",
        )
        .unwrap();

        let error = super::read_json_file(&path).unwrap_err().to_string();

        assert_eq!(
            error,
            "semantic error: failed to parse search projection evidence JSON: invalid_json"
        );
        assert!(!error.contains("secret-projection-path-do-not-emit"));
        assert!(!error.contains("secret-projection-doc-do-not-emit"));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn hawdb_probe_output_feeds_search_projection_evidence() {
        let path = unique_test_dir("search_projection_probe_command");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            index
                .apply_embedding_manifest(SearchEmbeddingManifest {
                    model: "bge-m3".to_string(),
                    version: None,
                    dimension: 8,
                })
                .unwrap();
            index
                .apply_projection_delta(SearchProjectionDelta {
                    upserts: nowledge_probe_rows(),
                    deletes: Vec::new(),
                    max_operations: None,
                    source_graph_commit_epoch: Some(11),
                })
                .unwrap();
            index.checkpoint().unwrap();
        }

        let probe = SearchIndex::open(&path)
            .unwrap()
            .nowledge_search_projection_probe_json(crate::SearchProjectionProbeOptions {
                active_embedding_model: Some("bge-m3".to_string()),
                active_embedding_dimension: Some(8),
            });
        let evidence = nowledge_search_projection_evidence_json(&probe);
        // The fixture requires a compressed vector projection. Profiles without
        // that backend must retain the complete probe and report its blocker.
        let vector_ready = cfg!(feature = "vector-search");

        assert_eq!(probe["protocol"], "hawdb-nowledge-search-projection-probe");
        assert_eq!(
            evidence["ready"], vector_ready,
            "probe={probe:#}\nevidence={evidence:#}"
        );
        assert_eq!(evidence["covered_table_count"], 6);
        assert_eq!(evidence["source_chunk_ready"], true);
        assert_eq!(evidence["predicate_pushdown_ready"], true);
        assert_eq!(evidence["hawdb_predicate_pushdown_ready"], true);
        assert_eq!(
            evidence["predicate_pushdown"]["segment_descriptor_scan_filter_fields_ready"],
            true
        );
        assert_eq!(evidence["compressed_vector_projection_required"], true);
        assert_eq!(evidence["compressed_vector_projection_ready"], vector_ready);
        if !vector_ready {
            assert_eq!(
                evidence["blocker_codes"],
                serde_json::json!(["compressed_vector_projection_not_ready"])
            );
            assert_eq!(
                evidence["compressed_vector_projection"]["blocker_codes"],
                serde_json::json!(["vector_search_feature_disabled"])
            );
        }
        assert_eq!(
            probe["predicate_pushdown"]["supported_ops"],
            serde_json::json!(["eq", "in", "not_in", "gt", "gte", "lt", "lte"])
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    fn nowledge_probe_rows() -> Vec<SearchProjectionRow> {
        let mut rows = vec![
            nowledge_probe_row(SearchProjectionKind::Memory, "mem_1"),
            nowledge_probe_row_without_embedding(SearchProjectionKind::Message, "msg_1"),
            nowledge_probe_row(SearchProjectionKind::Community, "community_1"),
            nowledge_probe_row(SearchProjectionKind::Entity, "entity_1"),
            nowledge_probe_row(SearchProjectionKind::Source, "source_1"),
            nowledge_probe_row(SearchProjectionKind::SourceChunk, "chunk_1"),
        ];
        rows.extend((0..124).map(|ordinal| {
            nowledge_probe_row(
                SearchProjectionKind::Memory,
                &format!("mem_filler_{ordinal:03}"),
            )
        }));
        rows
    }

    fn nowledge_probe_row(kind: SearchProjectionKind, external_id: &str) -> SearchProjectionRow {
        SearchProjectionRow {
            kind,
            external_id: external_id.to_string(),
            title: format!("{external_id} title"),
            body: format!("{external_id} body"),
            embedding: Some(vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            source_id: Some("source_1".to_string()),
            metadata: BTreeMap::from([
                ("space_id".to_string(), "default".to_string()),
                ("unit_type".to_string(), "fact".to_string()),
                ("lifecycle_state".to_string(), "active".to_string()),
                ("temporal_context".to_string(), "current".to_string()),
                ("importance".to_string(), "0.8".to_string()),
                ("confidence".to_string(), "0.9".to_string()),
                ("created_at".to_string(), "11".to_string()),
                ("updated_at".to_string(), "12".to_string()),
                ("event_start".to_string(), "10".to_string()),
                ("event_end".to_string(), "20".to_string()),
                ("is_latest".to_string(), "true".to_string()),
            ]),
        }
    }

    fn nowledge_probe_row_without_embedding(
        kind: SearchProjectionKind,
        external_id: &str,
    ) -> SearchProjectionRow {
        SearchProjectionRow {
            embedding: None,
            ..nowledge_probe_row(kind, external_id)
        }
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("hawdb_{name}_{}_{nanos}", std::process::id()))
    }
}
