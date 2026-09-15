//! Developer-only verification for portable bootstrap staging artifacts.
//!
//! This module validates only staged files and typed bootstrap contracts. It
//! never opens an embedded database or lets a staged artifact relax a host
//! resource limit.

use crate::{
    skein_lightning_artifact_summary, skein_lightning_graph_stream_validation_json,
    skein_lightning_relational_stream_validation_json, validate_skein_lightning_graph_stream,
    validate_skein_lightning_relational_stream, SKEIN_LIGHTNING_BOOTSTRAP_PROTOCOL_VERSION,
    SKEIN_LIGHTNING_RELATIONAL_STREAM_FORMAT_VERSION,
    SKEIN_LIGHTNING_STAGING_CATALOG_PROTOCOL_VERSION,
};
use skein_core::{Result, SkeinError};
use skein_integrity::checksum_u64;
use std::fs;
use std::path::Path;

pub fn verify_skein_lightning_staging_catalog(
    staging_dir: impl AsRef<Path>,
) -> Result<serde_json::Value> {
    let staging_dir = staging_dir.as_ref();
    let catalog_path = staging_dir.join("skein_lightning_staging_catalog.json");
    let catalog = read_json_file(&catalog_path)?;
    let mut errors = Vec::new();
    let mut artifact_errors = Vec::new();
    let mut manifest_errors = Vec::new();
    let mut graph_stream_errors = Vec::new();
    let mut relational_stream_errors = Vec::new();
    let mut bundle_errors = Vec::new();
    let mut catalog_errors = Vec::new();
    let mut artifact_reports = Vec::new();
    let mut manifest = None;
    let mut graph_stream = None;
    let mut relational_stream = None;
    let mut bundle = None;

    let catalog_protocol_matches = catalog.get("protocol").and_then(serde_json::Value::as_str)
        == Some("skein-lightning-staging-catalog");
    if !catalog_protocol_matches {
        record_error(
            &mut errors,
            &mut catalog_errors,
            "staging catalog protocol mismatch",
        );
    }
    let catalog_protocol_version_matches = catalog
        .get("protocol_version")
        .and_then(serde_json::Value::as_u64)
        == Some(SKEIN_LIGHTNING_STAGING_CATALOG_PROTOCOL_VERSION);
    if !catalog_protocol_version_matches {
        record_error(
            &mut errors,
            &mut catalog_errors,
            "staging catalog protocol version mismatch",
        );
    }

    let artifacts = catalog
        .get("artifacts")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_else(|| {
            record_error(
                &mut errors,
                &mut catalog_errors,
                "staging catalog missing artifacts array",
            );
            Vec::new()
        });
    for artifact in &artifacts {
        let kind = artifact
            .get("kind")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let Some(path) = artifact.get("path").and_then(serde_json::Value::as_str) else {
            record_error(
                &mut errors,
                &mut artifact_errors,
                format!("staging artifact {kind} missing path"),
            );
            continue;
        };
        if path.contains('/') || path.contains('\\') {
            record_error(
                &mut errors,
                &mut artifact_errors,
                format!("staging artifact {kind} uses non-local path {path}"),
            );
            continue;
        }
        let artifact_path = staging_dir.join(path);
        let expected_byte_len = artifact.get("byte_len").and_then(serde_json::Value::as_u64);
        let expected_checksum = artifact.get("checksum").and_then(serde_json::Value::as_u64);
        match fs::read(&artifact_path) {
            Ok(bytes) => {
                let actual_byte_len = bytes.len() as u64;
                let actual_checksum = checksum_u64(&bytes);
                let byte_len_matches = expected_byte_len == Some(actual_byte_len);
                let checksum_matches = expected_checksum == Some(actual_checksum);
                if !byte_len_matches {
                    record_error(
                        &mut errors,
                        &mut artifact_errors,
                        format!("staging artifact {kind} byte length mismatch"),
                    );
                }
                if !checksum_matches {
                    record_error(
                        &mut errors,
                        &mut artifact_errors,
                        format!("staging artifact {kind} checksum mismatch"),
                    );
                }
                match kind {
                    "manifest" => match serde_json::from_slice::<serde_json::Value>(&bytes) {
                        Ok(value) => manifest = Some(value),
                        Err(error) => record_error(
                            &mut errors,
                            &mut manifest_errors,
                            format!("invalid manifest artifact JSON: {error}"),
                        ),
                    },
                    "graph_stream" => match String::from_utf8(bytes.clone()) {
                        Ok(value) => graph_stream = Some(value),
                        Err(error) => record_error(
                            &mut errors,
                            &mut graph_stream_errors,
                            format!("invalid GraphStream UTF-8: {error}"),
                        ),
                    },
                    "relational_stream" => relational_stream = Some(bytes),
                    "bundle" => match serde_json::from_slice::<serde_json::Value>(&bytes) {
                        Ok(value) => bundle = Some(value),
                        Err(error) => record_error(
                            &mut errors,
                            &mut bundle_errors,
                            format!("invalid bundle artifact JSON: {error}"),
                        ),
                    },
                    _ => record_error(
                        &mut errors,
                        &mut artifact_errors,
                        format!("unknown staging artifact kind {kind}"),
                    ),
                }
                artifact_reports.push(serde_json::json!({
                    "kind": kind,
                    "path": path,
                    "expected_byte_len": expected_byte_len,
                    "actual_byte_len": actual_byte_len,
                    "byte_len_matches": byte_len_matches,
                    "expected_checksum": expected_checksum,
                    "actual_checksum": actual_checksum,
                    "checksum_matches": checksum_matches,
                }));
            }
            Err(error) => {
                record_error(
                    &mut errors,
                    &mut artifact_errors,
                    format!("missing staging artifact {kind} at {path}: {error}"),
                );
                artifact_reports.push(serde_json::json!({
                    "kind": kind,
                    "path": path,
                    "expected_byte_len": expected_byte_len,
                    "actual_byte_len": serde_json::Value::Null,
                    "byte_len_matches": false,
                    "expected_checksum": expected_checksum,
                    "actual_checksum": serde_json::Value::Null,
                    "checksum_matches": false,
                }));
            }
        }
    }

    let manifest_protocol_version_matches = manifest
        .as_ref()
        .and_then(|manifest| manifest.get("protocol_version"))
        .and_then(serde_json::Value::as_u64)
        == Some(SKEIN_LIGHTNING_BOOTSTRAP_PROTOCOL_VERSION);
    if !manifest_protocol_version_matches {
        record_error(
            &mut errors,
            &mut manifest_errors,
            "manifest protocol version mismatch",
        );
    }

    let graph_stream_validation = graph_stream
        .as_ref()
        .map(|encoded| validate_skein_lightning_graph_stream(encoded, None));
    let graph_stream_validation_json = graph_stream_validation
        .as_ref()
        .map(skein_lightning_graph_stream_validation_json);
    let relational_stream_validation = relational_stream
        .as_ref()
        .map(|encoded| validate_skein_lightning_relational_stream(encoded, None));
    let mut relational_stream_validation_json = relational_stream_validation
        .as_ref()
        .map(skein_lightning_relational_stream_validation_json);
    if let (Some(validation), Some(manifest)) = (
        relational_stream_validation_json.as_mut(),
        manifest.as_ref(),
    ) {
        validation["expected_stream_checksum"] = manifest["relational_stream_checksum"].clone();
    }
    let manifest_matches_graph_stream = match (&manifest, &graph_stream, &graph_stream_validation) {
        (Some(manifest), Some(graph_stream), Some(validation)) => {
            let matches = manifest
                .get("graph_stream_checksum")
                .and_then(serde_json::Value::as_u64)
                == validation.expected_stream_checksum
                && manifest
                    .get("graph_stream_byte_len")
                    .and_then(serde_json::Value::as_u64)
                    == Some(graph_stream.len() as u64)
                && manifest
                    .get("graph_commit_epoch")
                    .and_then(serde_json::Value::as_u64)
                    == validation.graph_commit_epoch
                && manifest
                    .get("logical_checksum")
                    .and_then(serde_json::Value::as_u64)
                    == validation.logical_checksum
                && manifest
                    .get("node_count")
                    .and_then(serde_json::Value::as_u64)
                    == Some(validation.node_count as u64)
                && manifest
                    .get("relationship_count")
                    .and_then(serde_json::Value::as_u64)
                    == Some(validation.relationship_count as u64);
            if !matches {
                record_error(
                    &mut errors,
                    &mut manifest_errors,
                    "manifest does not match GraphStream artifact",
                );
            }
            matches
        }
        _ => {
            record_error(
                &mut errors,
                &mut manifest_errors,
                "manifest or GraphStream artifact missing",
            );
            false
        }
    };
    let manifest_matches_relational_stream =
        match (&manifest, &relational_stream, &relational_stream_validation) {
            (Some(manifest), Some(relational_stream), Some(validation)) => {
                let matches = manifest
                    .get("relational_stream_format_version")
                    .and_then(serde_json::Value::as_u64)
                    == Some(SKEIN_LIGHTNING_RELATIONAL_STREAM_FORMAT_VERSION)
                    && manifest
                        .get("relational_stream_checksum")
                        .and_then(serde_json::Value::as_u64)
                        == Some(validation.actual_stream_checksum)
                    && manifest
                        .get("relational_stream_byte_len")
                        .and_then(serde_json::Value::as_u64)
                        == Some(relational_stream.len() as u64)
                    && manifest
                        .get("database_commit_epoch")
                        .and_then(serde_json::Value::as_u64)
                        == validation.database_commit_epoch
                    && manifest
                        .get("relational_table_count")
                        .and_then(serde_json::Value::as_u64)
                        == Some(validation.table_count as u64)
                    && manifest
                        .get("relational_row_count")
                        .and_then(serde_json::Value::as_u64)
                        == Some(validation.row_count as u64)
                    && manifest
                        .get("relational_overflow_segment_count")
                        .and_then(serde_json::Value::as_u64)
                        == Some(validation.overflow_segment_count as u64);
                if !matches {
                    record_error(
                        &mut errors,
                        &mut manifest_errors,
                        "manifest does not match relational stream artifact",
                    );
                }
                matches
            }
            _ => {
                record_error(
                    &mut errors,
                    &mut manifest_errors,
                    "manifest or relational stream artifact missing",
                );
                false
            }
        };
    let bundle_matches_artifacts = match (
        &bundle,
        &manifest,
        &graph_stream_validation_json,
        &relational_stream_validation_json,
    ) {
        (Some(bundle), Some(manifest), Some(graph_validation), Some(relational_validation)) => {
            let matches = bundle.get("manifest") == Some(manifest)
                && bundle.get("graph_stream_validation") == Some(graph_validation)
                && bundle.get("relational_stream_validation") == Some(relational_validation)
                && bundle.get("export_gate") == catalog.get("export_gate");
            if !matches {
                record_error(
                    &mut errors,
                    &mut bundle_errors,
                    "bundle does not match staged manifest, stream validations, or catalog gate",
                );
            }
            matches
        }
        _ => {
            record_error(&mut errors, &mut bundle_errors, "bundle artifact missing");
            false
        }
    };
    let storage_recovery_evidence = match (&bundle, &manifest) {
        (Some(bundle), Some(manifest)) => verify_bundle_storage_recovery_evidence(
            bundle,
            manifest,
            &mut errors,
            &mut bundle_errors,
        ),
        _ => StorageRecoveryEvidenceVerification::default(),
    };
    let artifact_integrity = artifact_reports.iter().all(|report| {
        report
            .get("byte_len_matches")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
            && report
                .get("checksum_matches")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
    });
    let artifact_summary = skein_lightning_artifact_summary(&artifact_reports, "actual_byte_len");
    let catalog_state_ready = catalog
        .get("stage_state")
        .and_then(serde_json::Value::as_str)
        == Some("READY")
        && catalog
            .get("export_gate")
            .and_then(|gate| gate.get("decision"))
            .and_then(serde_json::Value::as_str)
            == Some("ready");
    if !catalog_state_ready {
        record_error(
            &mut errors,
            &mut catalog_errors,
            "staging catalog is not READY",
        );
    }
    let graph_stream_valid = graph_stream_validation
        .as_ref()
        .is_some_and(|validation| validation.is_valid);
    if !graph_stream_valid {
        record_error(
            &mut errors,
            &mut graph_stream_errors,
            "GraphStream validation failed",
        );
    }
    let relational_stream_valid = relational_stream_validation
        .as_ref()
        .is_some_and(|validation| validation.is_valid);
    if !relational_stream_valid {
        record_error(
            &mut errors,
            &mut relational_stream_errors,
            "relational stream validation failed",
        );
    }
    let decision = if errors.is_empty()
        && artifact_integrity
        && catalog_protocol_matches
        && catalog_protocol_version_matches
        && manifest_protocol_version_matches
        && manifest_matches_graph_stream
        && manifest_matches_relational_stream
        && bundle_matches_artifacts
        && storage_recovery_evidence.valid
        && catalog_state_ready
        && graph_stream_valid
        && relational_stream_valid
    {
        "ready"
    } else {
        "blocked"
    };
    Ok(serde_json::json!({
        "protocol": "skein-lightning-staging-verification",
        "protocol_version": SKEIN_LIGHTNING_STAGING_CATALOG_PROTOCOL_VERSION,
        "catalog_path": "skein_lightning_staging_catalog.json",
        "artifact_integrity": artifact_integrity,
        "catalog_protocol_matches": catalog_protocol_matches,
        "catalog_protocol_version_matches": catalog_protocol_version_matches,
        "manifest_protocol_version_matches": manifest_protocol_version_matches,
        "manifest_matches_graph_stream": manifest_matches_graph_stream,
        "manifest_matches_relational_stream": manifest_matches_relational_stream,
        "bundle_matches_artifacts": bundle_matches_artifacts,
        "storage_recovery_evidence": {
            "present": storage_recovery_evidence.present,
            "valid": storage_recovery_evidence.valid,
            "protocol_matches": storage_recovery_evidence.protocol_matches,
            "storage_version_present": storage_recovery_evidence.storage_version_present,
            "recovered_commit_epoch_matches_manifest": storage_recovery_evidence.recovered_commit_epoch_matches_manifest,
        },
        "catalog_state_ready": catalog_state_ready,
        "graph_stream_validation": graph_stream_validation_json,
        "relational_stream_validation": relational_stream_validation_json,
        "artifact_summary": artifact_summary,
        "artifacts": artifact_reports,
        "validation_gate": {
            "decision": decision,
            "artifact_errors": artifact_errors.len(),
            "manifest_errors": manifest_errors.len(),
            "graph_stream_errors": graph_stream_errors.len(),
            "relational_stream_errors": relational_stream_errors.len(),
            "bundle_errors": bundle_errors.len(),
            "catalog_errors": catalog_errors.len(),
            "artifact_error_messages": artifact_errors,
            "manifest_error_messages": manifest_errors,
            "graph_stream_error_messages": graph_stream_errors,
            "relational_stream_error_messages": relational_stream_errors,
            "bundle_error_messages": bundle_errors,
            "catalog_error_messages": catalog_errors,
            "errors": errors,
        },
    }))
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let bytes = fs::read(path)?;
    serde_json::from_slice(&bytes)
        .map_err(|_| SkeinError::Execution("invalid JSON file: invalid_json".to_string()))
}

fn record_error(errors: &mut Vec<String>, group: &mut Vec<String>, message: impl Into<String>) {
    let message = message.into();
    errors.push(message.clone());
    group.push(message);
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct StorageRecoveryEvidenceVerification {
    present: bool,
    valid: bool,
    protocol_matches: bool,
    storage_version_present: bool,
    recovered_commit_epoch_matches_manifest: bool,
}

fn verify_bundle_storage_recovery_evidence(
    bundle: &serde_json::Value,
    manifest: &serde_json::Value,
    errors: &mut Vec<String>,
    bundle_errors: &mut Vec<String>,
) -> StorageRecoveryEvidenceVerification {
    let Some(storage_recovery) = bundle.get("storage_recovery") else {
        return StorageRecoveryEvidenceVerification {
            present: false,
            valid: true,
            protocol_matches: false,
            storage_version_present: false,
            recovered_commit_epoch_matches_manifest: false,
        };
    };
    let protocol_matches = storage_recovery
        .get("protocol")
        .and_then(serde_json::Value::as_str)
        == Some("skein-storage-recovery-report");
    if !protocol_matches {
        record_error(
            errors,
            bundle_errors,
            "bundle storage_recovery protocol mismatch",
        );
    }
    let storage_version_present = storage_recovery
        .get("storage_version")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|version| !version.is_empty());
    if !storage_version_present {
        record_error(
            errors,
            bundle_errors,
            "bundle storage_recovery missing storage_version",
        );
    }
    let recovered_commit_epoch_matches_manifest = storage_recovery
        .get("recovered_commit_epoch")
        .and_then(serde_json::Value::as_u64)
        == manifest
            .get("database_commit_epoch")
            .and_then(serde_json::Value::as_u64);
    if !recovered_commit_epoch_matches_manifest {
        record_error(
            errors,
            bundle_errors,
            "bundle storage_recovery recovered commit epoch does not match manifest database epoch",
        );
    }
    StorageRecoveryEvidenceVerification {
        present: true,
        valid: protocol_matches
            && storage_version_present
            && recovered_commit_epoch_matches_manifest,
        protocol_matches,
        storage_version_present,
        recovered_commit_epoch_matches_manifest,
    }
}

pub fn verify_skein_lightning_published_manifest(
    staging_dir: impl AsRef<Path>,
    publish_dir: impl AsRef<Path>,
) -> Result<serde_json::Value> {
    let staging_dir = staging_dir.as_ref();
    let publish_dir = publish_dir.as_ref();
    let published_path = publish_dir.join("skein_lightning_published_manifest.json");
    let published = read_json_file(&published_path)?;
    let staging_verification = verify_skein_lightning_staging_catalog(staging_dir)?;
    let catalog_path = staging_dir.join("skein_lightning_staging_catalog.json");
    let catalog_bytes = fs::read(&catalog_path)?;
    let actual_catalog_checksum = checksum_u64(&catalog_bytes);
    let actual_catalog_byte_len = catalog_bytes.len() as u64;
    let expected_catalog_checksum = published
        .get("staging_catalog")
        .and_then(|catalog| catalog.get("checksum"))
        .and_then(serde_json::Value::as_u64);
    let expected_catalog_byte_len = published
        .get("staging_catalog")
        .and_then(|catalog| catalog.get("byte_len"))
        .and_then(serde_json::Value::as_u64);
    let catalog_checksum_matches = expected_catalog_checksum == Some(actual_catalog_checksum);
    let catalog_byte_len_matches = expected_catalog_byte_len == Some(actual_catalog_byte_len);
    let pointer_state_published =
        published.get("state").and_then(serde_json::Value::as_str) == Some("PUBLISHED");
    let staging_ready = staging_verification
        .get("validation_gate")
        .and_then(|gate| gate.get("decision"))
        .and_then(serde_json::Value::as_str)
        == Some("ready");
    let storage_recovery_evidence = staging_verification
        .get("storage_recovery_evidence")
        .cloned()
        .unwrap_or_else(default_storage_recovery_evidence);
    let catalog = serde_json::from_slice::<serde_json::Value>(&catalog_bytes)
        .map_err(|_| SkeinError::Execution("invalid JSON file: invalid_json".to_string()))?;
    let manifest = read_staging_artifact_json(&catalog, staging_dir, "manifest")?;
    let pointer_matches_manifest = published.get("database_commit_epoch")
        == manifest.get("database_commit_epoch")
        && published.get("graph_commit_epoch") == manifest.get("graph_commit_epoch")
        && published.get("logical_checksum") == manifest.get("logical_checksum")
        && published.get("schema_checksum") == manifest.get("schema_checksum")
        && published.get("graph_stream_checksum") == manifest.get("graph_stream_checksum")
        && published.get("graph_stream_byte_len") == manifest.get("graph_stream_byte_len")
        && published.get("relational_stream_checksum")
            == manifest.get("relational_stream_checksum")
        && published.get("relational_stream_byte_len")
            == manifest.get("relational_stream_byte_len")
        && published.get("relational_table_count") == manifest.get("relational_table_count")
        && published.get("relational_row_count") == manifest.get("relational_row_count")
        && published.get("node_count") == manifest.get("node_count")
        && published.get("relationship_count") == manifest.get("relationship_count");
    let mut errors = Vec::new();
    let mut pointer_errors = Vec::new();
    let mut catalog_errors = Vec::new();
    let mut staging_errors = Vec::new();
    if !pointer_state_published {
        record_error(
            &mut errors,
            &mut pointer_errors,
            "published pointer is not PUBLISHED",
        );
    }
    if !catalog_checksum_matches {
        record_error(
            &mut errors,
            &mut catalog_errors,
            "published pointer staging catalog checksum mismatch",
        );
    }
    if !catalog_byte_len_matches {
        record_error(
            &mut errors,
            &mut catalog_errors,
            "published pointer staging catalog byte length mismatch",
        );
    }
    if !staging_ready {
        record_error(
            &mut errors,
            &mut staging_errors,
            "published staging catalog is not ready",
        );
    }
    if !pointer_matches_manifest {
        record_error(
            &mut errors,
            &mut pointer_errors,
            "published pointer does not match staged manifest",
        );
    }
    let decision = if errors.is_empty() {
        "ready"
    } else {
        "blocked"
    };
    Ok(serde_json::json!({
        "protocol": "skein-lightning-published-verification",
        "protocol_version": 1,
        "pointer_state_published": pointer_state_published,
        "catalog_checksum_matches": catalog_checksum_matches,
        "catalog_byte_len_matches": catalog_byte_len_matches,
        "staging_ready": staging_ready,
        "pointer_matches_manifest": pointer_matches_manifest,
        "storage_recovery_evidence": storage_recovery_evidence,
        "published_manifest": published,
        "staging_verification": staging_verification,
        "validation_gate": {
            "decision": decision,
            "pointer_errors": pointer_errors.len(),
            "catalog_errors": catalog_errors.len(),
            "staging_errors": staging_errors.len(),
            "pointer_error_messages": pointer_errors,
            "catalog_error_messages": catalog_errors,
            "staging_error_messages": staging_errors,
            "errors": errors,
        },
    }))
}

pub fn read_skein_lightning_staging_artifact_json(
    catalog: &serde_json::Value,
    staging_dir: &Path,
    kind: &str,
) -> Result<serde_json::Value> {
    read_staging_artifact_json(catalog, staging_dir, kind)
}

fn read_staging_artifact_json(
    catalog: &serde_json::Value,
    staging_dir: &Path,
    kind: &str,
) -> Result<serde_json::Value> {
    let artifacts = catalog
        .get("artifacts")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            SkeinError::Execution("staging catalog missing artifacts array".to_string())
        })?;
    let artifact = artifacts
        .iter()
        .find(|artifact| artifact.get("kind").and_then(serde_json::Value::as_str) == Some(kind))
        .ok_or_else(|| SkeinError::Execution(format!("staging catalog missing {kind} artifact")))?;
    let path = artifact
        .get("path")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| SkeinError::Execution(format!("staging {kind} artifact missing path")))?;
    if path.contains('/') || path.contains('\\') {
        return Err(SkeinError::Execution(format!(
            "staging {kind} artifact uses non-local path {path}"
        )));
    }
    read_json_file(&staging_dir.join(path))
}

fn default_storage_recovery_evidence() -> serde_json::Value {
    serde_json::json!({
        "present": false,
        "valid": true,
        "protocol_matches": false,
        "storage_version_present": false,
        "recovered_commit_epoch_matches_manifest": false,
    })
}

#[cfg(test)]
mod tests {
    use super::verify_skein_lightning_staging_catalog;
    use crate::{
        stage_skein_lightning_bootstrap_export, CanonicalGraphSnapshotExport,
        SkeinLightningBootstrapExport, SkeinLightningRelationalStream,
    };
    use skein_storage::RelationalState;

    #[test]
    fn verifies_a_staged_export_through_the_bootstrap_owner() {
        let snapshot = CanonicalGraphSnapshotExport::from_rows(7, Vec::new(), Vec::new());
        let relational_stream =
            SkeinLightningRelationalStream::from_state(7, &RelationalState::default()).unwrap();
        let manifest = snapshot.skein_lightning_bootstrap_manifest(&relational_stream);
        let graph_stream = snapshot.skein_lightning_graph_stream();
        let export = SkeinLightningBootstrapExport {
            snapshot,
            manifest,
            graph_stream,
            relational_stream,
        };
        let staging_dir = std::env::temp_dir().join(format!(
            "skein-bootstrap-staging-verification-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&staging_dir);
        stage_skein_lightning_bootstrap_export(&export, &staging_dir).unwrap();

        let report = verify_skein_lightning_staging_catalog(&staging_dir).unwrap();

        assert_eq!(report["validation_gate"]["decision"], "ready");
        assert_eq!(report["artifact_integrity"], true);
        assert_eq!(report["manifest_matches_graph_stream"], true);
        assert_eq!(report["manifest_matches_relational_stream"], true);
        assert_eq!(report["bundle_matches_artifacts"], true);
        std::fs::remove_dir_all(staging_dir).unwrap();
    }
}
