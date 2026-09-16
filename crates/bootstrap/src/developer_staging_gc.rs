//! Developer-only retention reporting for portable bootstrap staging artifacts.
//!
//! The report is file-protocol based and fails closed when a published pointer
//! cannot be verified against its staged catalog.

use crate::{skein_lightning_artifact_summary, verify_skein_lightning_published_manifest};
use skein_core::{Result, SkeinError};
use skein_integrity::checksum_u64;
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

pub fn skein_lightning_gc_staging_report(
    staging_dir: impl AsRef<Path>,
    publish_dir: impl AsRef<Path>,
) -> Result<serde_json::Value> {
    let staging_dir = staging_dir.as_ref();
    let publish_dir = publish_dir.as_ref();
    let catalog_path = staging_dir.join("skein_lightning_staging_catalog.json");
    let catalog_bytes = fs::read(&catalog_path)?;
    let catalog = serde_json::from_slice::<serde_json::Value>(&catalog_bytes)
        .map_err(|_| SkeinError::Execution("invalid JSON file: invalid_json".to_string()))?;
    let candidates = skein_lightning_staging_gc_candidates(&catalog, &catalog_bytes)?;
    let published_path = publish_dir.join("skein_lightning_published_manifest.json");
    let mut errors = Vec::new();
    let mut published_pointer_errors = Vec::new();
    let mut pinned_paths = BTreeSet::new();
    let pointer_state = if published_path.exists() {
        let verification = verify_skein_lightning_published_manifest(staging_dir, publish_dir)?;
        if verification
            .get("validation_gate")
            .and_then(|gate| gate.get("decision"))
            .and_then(serde_json::Value::as_str)
            == Some("ready")
        {
            pinned_paths = candidates
                .iter()
                .filter_map(|candidate| {
                    candidate
                        .get("path")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string)
                })
                .collect();
            "verified"
        } else {
            record_error(
                &mut errors,
                &mut published_pointer_errors,
                "published pointer verification failed; refusing to mark staging artifacts deletable",
            );
            if let Some(verification_errors) = verification
                .get("validation_gate")
                .and_then(|gate| gate.get("errors"))
                .and_then(serde_json::Value::as_array)
            {
                for error in verification_errors
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                {
                    record_error(&mut errors, &mut published_pointer_errors, error);
                }
            }
            "verification_failed"
        }
    } else {
        "missing"
    };

    let fail_closed = pointer_state == "verification_failed";
    let candidate_reports = candidates
        .into_iter()
        .map(|candidate| {
            let path = candidate
                .get("path")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            let pinned_by_published_pointer = pinned_paths.contains(path);
            let deletable = !fail_closed && !pinned_by_published_pointer;
            let reason = if pinned_by_published_pointer {
                "published_pointer"
            } else if fail_closed {
                "published_pointer_unverified"
            } else {
                "not_pinned"
            };
            serde_json::json!({
                "kind": candidate["kind"].clone(),
                "path": candidate["path"].clone(),
                "byte_len": candidate["byte_len"].clone(),
                "checksum": candidate["checksum"].clone(),
                "pinned_by_published_pointer": pinned_by_published_pointer,
                "deletable": deletable,
                "reason": reason,
            })
        })
        .collect::<Vec<_>>();
    let deletable_count = candidate_reports
        .iter()
        .filter(|candidate| {
            candidate
                .get("deletable")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
        })
        .count();
    let pinned_count = candidate_reports
        .iter()
        .filter(|candidate| {
            candidate
                .get("pinned_by_published_pointer")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
        })
        .count();
    let total_bytes = skein_lightning_sum_artifact_bytes(&candidate_reports, |_| true);
    let deletable_bytes = skein_lightning_sum_artifact_bytes(&candidate_reports, |candidate| {
        candidate
            .get("deletable")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
    });
    let pinned_bytes = skein_lightning_sum_artifact_bytes(&candidate_reports, |candidate| {
        candidate
            .get("pinned_by_published_pointer")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
    });
    let artifact_summary = skein_lightning_artifact_summary(&candidate_reports, "byte_len");
    let decision = if errors.is_empty() {
        "ready"
    } else {
        "blocked"
    };
    Ok(serde_json::json!({
        "protocol": "skein-lightning-staging-gc-report",
        "protocol_version": 1,
        "published_pointer_state": pointer_state,
        "candidate_count": candidate_reports.len(),
        "pinned_count": pinned_count,
        "deletable_count": deletable_count,
        "total_bytes": total_bytes,
        "pinned_bytes": pinned_bytes,
        "deletable_bytes": deletable_bytes,
        "artifact_summary": artifact_summary,
        "candidates": candidate_reports,
        "gc_gate": {
            "decision": decision,
            "published_pointer_errors": published_pointer_errors.len(),
            "published_pointer_error_messages": published_pointer_errors,
            "errors": errors,
        },
    }))
}

fn skein_lightning_staging_gc_candidates(
    catalog: &serde_json::Value,
    catalog_bytes: &[u8],
) -> Result<Vec<serde_json::Value>> {
    let mut candidates = Vec::new();
    candidates.push(serde_json::json!({
        "kind": "staging_catalog",
        "path": "skein_lightning_staging_catalog.json",
        "byte_len": catalog_bytes.len(),
        "checksum": checksum_u64(catalog_bytes),
    }));
    let artifacts = catalog
        .get("artifacts")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            SkeinError::Execution("staging catalog missing artifacts array".to_string())
        })?;
    for artifact in artifacts {
        let kind = artifact
            .get("kind")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| SkeinError::Execution("staging artifact missing kind".to_string()))?;
        let path = artifact
            .get("path")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| SkeinError::Execution("staging artifact missing path".to_string()))?;
        if path.contains('/') || path.contains('\\') {
            return Err(SkeinError::Execution(format!(
                "staging artifact {kind} uses non-local path {path}"
            )));
        }
        candidates.push(serde_json::json!({
            "kind": kind,
            "path": path,
            "byte_len": artifact.get("byte_len").cloned().unwrap_or(serde_json::Value::Null),
            "checksum": artifact.get("checksum").cloned().unwrap_or(serde_json::Value::Null),
        }));
    }
    Ok(candidates)
}

fn skein_lightning_sum_artifact_bytes(
    artifacts: &[serde_json::Value],
    predicate: impl Fn(&serde_json::Value) -> bool,
) -> u64 {
    artifacts
        .iter()
        .filter(|artifact| predicate(artifact))
        .filter_map(|artifact| artifact.get("byte_len").and_then(serde_json::Value::as_u64))
        .sum()
}

fn record_error(errors: &mut Vec<String>, group: &mut Vec<String>, message: impl Into<String>) {
    let message = message.into();
    errors.push(message.clone());
    group.push(message);
}
