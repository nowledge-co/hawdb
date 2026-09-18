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

//! Developer-only publication for validated portable bootstrap artifacts.
//!
//! Publication is a file-protocol transition guarded by a validated staging
//! catalog and, when requested, a matching import lifecycle marker.

use crate::{
    hawdb_lightning_import_state_marker, read_hawdb_lightning_staging_artifact_json,
    sync_bootstrap_directory, verify_hawdb_lightning_staging_catalog, write_bootstrap_atomic_file,
};
use hawdb_core::{HawDBError, Result};
use hawdb_integrity::checksum_u64;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HawDBLightningPublishOptions {
    pub require_state_marker: bool,
    pub fencing_token: Option<String>,
    pub expected_database_epoch: Option<u64>,
}

pub fn publish_hawdb_lightning_staging_catalog(
    staging_dir: impl AsRef<Path>,
    publish_dir: impl AsRef<Path>,
) -> Result<serde_json::Value> {
    publish_hawdb_lightning_staging_catalog_with_options(
        staging_dir,
        publish_dir,
        HawDBLightningPublishOptions::default(),
    )
}

pub fn publish_hawdb_lightning_staging_catalog_with_options(
    staging_dir: impl AsRef<Path>,
    publish_dir: impl AsRef<Path>,
    options: HawDBLightningPublishOptions,
) -> Result<serde_json::Value> {
    let staging_dir = staging_dir.as_ref();
    let publish_dir = publish_dir.as_ref();
    let verification = verify_hawdb_lightning_staging_catalog(staging_dir)?;
    if verification
        .get("validation_gate")
        .and_then(|gate| gate.get("decision"))
        .and_then(serde_json::Value::as_str)
        != Some("ready")
    {
        return Err(HawDBError::Execution(
            "HawDB Lightning staging verification is not ready".to_string(),
        ));
    }

    let catalog_path = staging_dir.join("hawdb_lightning_staging_catalog.json");
    let catalog_bytes = fs::read(&catalog_path)?;
    let catalog_checksum = checksum_u64(&catalog_bytes);
    let catalog = serde_json::from_slice::<serde_json::Value>(&catalog_bytes)
        .map_err(|_| HawDBError::Execution("invalid JSON file: invalid_json".to_string()))?;
    let manifest = read_hawdb_lightning_staging_artifact_json(&catalog, staging_dir, "manifest")?;
    let publish_preflight = hawdb_lightning_publish_preflight(staging_dir, &manifest, &options)?;
    let pointer = serde_json::json!({
        "protocol": "hawdb-lightning-published-manifest",
        "protocol_version": 1,
        "state": "PUBLISHED",
        "database_commit_epoch": manifest["database_commit_epoch"].clone(),
        "graph_commit_epoch": manifest["graph_commit_epoch"].clone(),
        "logical_checksum": manifest["logical_checksum"].clone(),
        "schema_checksum": manifest["schema_checksum"].clone(),
        "graph_stream_checksum": manifest["graph_stream_checksum"].clone(),
        "graph_stream_byte_len": manifest["graph_stream_byte_len"].clone(),
        "relational_stream_checksum": manifest["relational_stream_checksum"].clone(),
        "relational_stream_byte_len": manifest["relational_stream_byte_len"].clone(),
        "relational_table_count": manifest["relational_table_count"].clone(),
        "relational_row_count": manifest["relational_row_count"].clone(),
        "node_count": manifest["node_count"].clone(),
        "relationship_count": manifest["relationship_count"].clone(),
        "staging_catalog": {
            "path": "hawdb_lightning_staging_catalog.json",
            "checksum": catalog_checksum,
            "byte_len": catalog_bytes.len(),
        },
    });

    fs::create_dir_all(publish_dir)?;
    let pointer_path = publish_dir.join("hawdb_lightning_published_manifest.json");
    if pointer_path.exists() {
        let existing = read_json_file(&pointer_path)?;
        if same_published_manifest_identity(&existing, &pointer) {
            let mut report = pointer;
            if let Some(object) = report.as_object_mut() {
                object.insert(
                    "publish_gate".to_string(),
                    serde_json::json!({
                        "decision": "idempotent",
                        "preflight": publish_preflight,
                        "errors": [],
                    }),
                );
            }
            return Ok(report);
        }
        return Err(HawDBError::Execution(
            "published HawDB Lightning manifest already points to a different snapshot".to_string(),
        ));
    }

    let mut report = pointer;
    if let Some(object) = report.as_object_mut() {
        object.insert(
            "publish_gate".to_string(),
            serde_json::json!({
                "decision": "published",
                "preflight": publish_preflight,
                "errors": [],
            }),
        );
    }
    let pointer_bytes = serde_json::to_vec_pretty(&report).unwrap();
    write_bootstrap_atomic_file(
        publish_dir,
        "hawdb_lightning_published_manifest.json",
        &pointer_bytes,
    )?;
    sync_bootstrap_directory(publish_dir)?;
    Ok(report)
}

fn hawdb_lightning_publish_preflight(
    staging_dir: &Path,
    manifest: &serde_json::Value,
    options: &HawDBLightningPublishOptions,
) -> Result<serde_json::Value> {
    let mut errors = Vec::new();
    let mut state_errors = Vec::new();
    let state_marker =
        hawdb_lightning_import_state_marker(staging_dir, &mut errors, &mut state_errors);
    let marker_present = state_marker
        .get("present")
        .and_then(serde_json::Value::as_bool)
        == Some(true);
    let marker_state = state_marker
        .get("import_state")
        .and_then(serde_json::Value::as_str);
    if options.require_state_marker && !marker_present {
        record_error(
            &mut errors,
            &mut state_errors,
            "publish requires HawDB Lightning import state marker",
        );
    }
    if marker_present && marker_state != Some("VALIDATING") {
        record_error(
            &mut errors,
            &mut state_errors,
            format!(
                "publish requires VALIDATING import state marker, found {}",
                marker_state.unwrap_or("missing")
            ),
        );
    }

    let manifest_epoch = manifest
        .get("database_commit_epoch")
        .and_then(serde_json::Value::as_u64);
    let expected_database_epoch_matches = options
        .expected_database_epoch
        .is_none_or(|expected| manifest_epoch == Some(expected));
    if !expected_database_epoch_matches {
        record_error(
            &mut errors,
            &mut state_errors,
            format!(
                "expected database epoch {:?} did not match staged manifest epoch {:?}",
                options.expected_database_epoch, manifest_epoch
            ),
        );
    }

    let marker_fencing_token = state_marker
        .get("idempotency_key")
        .and_then(|key| key.get("fencing_token"))
        .and_then(serde_json::Value::as_str);
    let fencing_token_matches = options
        .fencing_token
        .as_deref()
        .is_none_or(|expected| marker_fencing_token == Some(expected));
    if !fencing_token_matches {
        record_error(
            &mut errors,
            &mut state_errors,
            "publish fencing token did not match import state marker",
        );
    }

    if !errors.is_empty() {
        return Err(HawDBError::Execution(format!(
            "HawDB Lightning publish preflight blocked: {}",
            errors.join("; ")
        )));
    }

    Ok(serde_json::json!({
        "decision": "ready",
        "require_state_marker": options.require_state_marker,
        "expected_database_epoch": options.expected_database_epoch,
        "manifest_database_epoch": manifest_epoch,
        "expected_database_epoch_matches": expected_database_epoch_matches,
        "fencing_token_required": options.fencing_token.is_some(),
        "fencing_token_matches": fencing_token_matches,
        "state_marker": state_marker,
        "state_errors": state_errors.len(),
        "state_error_messages": state_errors,
        "errors": errors,
    }))
}

fn same_published_manifest_identity(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    [
        "database_commit_epoch",
        "graph_commit_epoch",
        "logical_checksum",
        "schema_checksum",
        "graph_stream_checksum",
        "graph_stream_byte_len",
        "relational_stream_checksum",
        "relational_stream_byte_len",
        "relational_table_count",
        "relational_row_count",
        "node_count",
        "relationship_count",
    ]
    .iter()
    .all(|key| left.get(*key) == right.get(*key))
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let bytes = fs::read(path)?;
    serde_json::from_slice(&bytes)
        .map_err(|_| HawDBError::Execution("invalid JSON file: invalid_json".to_string()))
}

fn record_error(errors: &mut Vec<String>, group: &mut Vec<String>, message: impl Into<String>) {
    let message = message.into();
    errors.push(message.clone());
    group.push(message);
}
