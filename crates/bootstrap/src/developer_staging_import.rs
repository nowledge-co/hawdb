//! Developer-only import lifecycle reporting for portable bootstrap artifacts.
//!
//! The report reads staging and publication state, validates checkpoints, and
//! preserves fail-closed retention decisions without opening a host database.

use crate::{
    skein_lightning_gc_staging_report, verify_skein_lightning_published_manifest,
    verify_skein_lightning_staging_catalog,
};
use skein_core::{Result, SkeinError};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

pub fn skein_lightning_import_status(
    staging_dir: impl AsRef<Path>,
    publish_dir: impl AsRef<Path>,
) -> Result<serde_json::Value> {
    let staging_dir = staging_dir.as_ref();
    let publish_dir = publish_dir.as_ref();
    let catalog_path = staging_dir.join("skein_lightning_staging_catalog.json");
    let published_path = publish_dir.join("skein_lightning_published_manifest.json");
    let staging_catalog_present = catalog_path.exists();
    let published_pointer_present = published_path.exists();
    let mut errors = Vec::new();
    let mut presence_errors = Vec::new();
    let mut staging_errors = Vec::new();
    let mut published_errors = Vec::new();
    let mut resource_errors = Vec::new();
    let mut state_errors = Vec::new();
    let mut checkpoint_errors = Vec::new();
    let mut staging_verification = None;
    let mut published_verification = None;
    let state_marker =
        skein_lightning_import_state_marker(staging_dir, &mut errors, &mut state_errors);
    let checkpoint_log =
        skein_lightning_import_checkpoint_log(staging_dir, &mut errors, &mut checkpoint_errors);
    let artifact_state = if !staging_catalog_present && published_pointer_present {
        record_error(
            &mut errors,
            &mut presence_errors,
            "published pointer exists without a matching staging catalog; refusing to treat import as created",
        );
        "QUARANTINED"
    } else if !staging_catalog_present {
        "CREATED"
    } else {
        let staging_report = verify_skein_lightning_staging_catalog(staging_dir)?;
        let staging_ready = gate_decision(&staging_report, "validation_gate") == Some("ready");
        if !staging_ready {
            for error in gate_errors(&staging_report, "validation_gate") {
                record_error(&mut errors, &mut staging_errors, error);
            }
        }
        staging_verification = Some(staging_report);
        if !staging_ready {
            "QUARANTINED"
        } else if published_pointer_present {
            let published_report =
                verify_skein_lightning_published_manifest(staging_dir, publish_dir)?;
            let published_ready =
                gate_decision(&published_report, "validation_gate") == Some("ready");
            if !published_ready {
                for error in gate_errors(&published_report, "validation_gate") {
                    record_error(&mut errors, &mut published_errors, error);
                }
            }
            published_verification = Some(published_report);
            if published_ready {
                "PUBLISHED"
            } else {
                "QUARANTINED"
            }
        } else {
            "READY"
        }
    };
    let import_state = skein_lightning_effective_import_state(
        artifact_state,
        state_marker
            .get("import_state")
            .and_then(serde_json::Value::as_str),
    );
    let resume_action = skein_lightning_import_resume_action(import_state);
    let resource_retention = skein_lightning_import_resource_retention(
        import_state,
        staging_catalog_present,
        staging_dir,
        publish_dir,
        &mut errors,
        &mut resource_errors,
    );
    let storage_recovery_evidence = skein_lightning_import_storage_recovery_evidence(
        staging_verification.as_ref(),
        published_verification.as_ref(),
    );
    let decision = if import_state == "QUARANTINED"
        || !resource_errors.is_empty()
        || !state_errors.is_empty()
        || !checkpoint_errors.is_empty()
    {
        "blocked"
    } else {
        "ready"
    };
    Ok(serde_json::json!({
        "protocol": "skein-lightning-import-status",
        "protocol_version": 1,
        "import_state": import_state,
        "artifact_state": artifact_state,
        "state_marker": state_marker,
        "checkpoint_log": checkpoint_log,
        "resume_action": resume_action,
        "resource_retention": resource_retention,
        "staging_catalog_present": staging_catalog_present,
        "published_pointer_present": published_pointer_present,
        "storage_recovery_evidence": storage_recovery_evidence,
        "staging_verification": staging_verification,
        "published_verification": published_verification,
        "status_gate": {
            "decision": decision,
            "presence_errors": presence_errors.len(),
            "staging_errors": staging_errors.len(),
            "published_errors": published_errors.len(),
            "resource_errors": resource_errors.len(),
            "state_errors": state_errors.len(),
            "checkpoint_errors": checkpoint_errors.len(),
            "presence_error_messages": presence_errors,
            "staging_error_messages": staging_errors,
            "published_error_messages": published_errors,
            "resource_error_messages": resource_errors,
            "state_error_messages": state_errors,
            "checkpoint_error_messages": checkpoint_errors,
            "errors": errors,
        },
    }))
}

fn skein_lightning_import_storage_recovery_evidence(
    staging_verification: Option<&serde_json::Value>,
    published_verification: Option<&serde_json::Value>,
) -> serde_json::Value {
    published_verification
        .and_then(|verification| verification.get("storage_recovery_evidence"))
        .or_else(|| {
            staging_verification
                .and_then(|verification| verification.get("storage_recovery_evidence"))
        })
        .cloned()
        .unwrap_or_else(|| {
            serde_json::json!({
                "present": false,
                "valid": true,
                "protocol_matches": false,
                "storage_version_present": false,
                "recovered_commit_epoch_matches_manifest": false,
            })
        })
}

fn skein_lightning_import_checkpoint_log(
    staging_dir: &Path,
    errors: &mut Vec<String>,
    checkpoint_errors: &mut Vec<String>,
) -> serde_json::Value {
    let checkpoint_path = staging_dir.join("skein_lightning_import_checkpoints.jsonl");
    if !checkpoint_path.exists() {
        return serde_json::json!({
            "present": false,
            "path": "skein_lightning_import_checkpoints.jsonl",
            "entry_count": 0,
            "idempotency_key_count": 0,
            "idempotency_conflicts": 0,
            "idempotency_conflict_messages": [],
            "last_checkpoint": serde_json::Value::Null,
            "failed_checkpoints": [],
            "checkpoint_summary": {
                "stage_counts": {},
                "status_counts": {},
                "failure_rule_counts": {},
                "failure_partition_counts": {},
            },
            "resume_summary": {
                "last_stage": serde_json::Value::Null,
                "last_source_range": serde_json::Value::Null,
                "last_object_digest": serde_json::Value::Null,
                "last_partition": serde_json::Value::Null,
                "failed_source_ranges": [],
                "failed_object_digests": [],
                "failed_partitions": [],
                "failed_validation_rules": [],
            },
            "checkpoint_gate": {
                "decision": "ready",
                "errors": [],
            },
        });
    }

    let content = match fs::read_to_string(&checkpoint_path) {
        Ok(content) => content,
        Err(error) => {
            record_error(
                errors,
                checkpoint_errors,
                format!("checkpoint log could not be read: {error}"),
            );
            return skein_lightning_import_checkpoint_blocked_json(checkpoint_errors);
        }
    };

    let mut entries = Vec::new();
    let mut failed = Vec::new();
    let mut failed_source_ranges = BTreeSet::new();
    let mut failed_object_digests = BTreeSet::new();
    let mut failed_partitions = BTreeSet::new();
    let mut failed_validation_rules = BTreeSet::new();
    let mut idempotency_fingerprints = BTreeMap::new();
    let mut idempotency_conflicts = Vec::new();
    let mut stage_counts = BTreeMap::new();
    let mut status_counts = BTreeMap::new();
    let mut failure_rule_counts = BTreeMap::new();
    let mut failure_partition_counts = BTreeMap::new();
    for (line_index, line) in content.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let entry = match serde_json::from_str::<serde_json::Value>(trimmed) {
            Ok(entry) => entry,
            Err(error) => {
                record_error(
                    errors,
                    checkpoint_errors,
                    format!(
                        "checkpoint log line {} is invalid JSON: {error}",
                        line_index + 1
                    ),
                );
                continue;
            }
        };
        skein_lightning_validate_checkpoint_entry(
            &entry,
            line_index + 1,
            errors,
            checkpoint_errors,
        );
        increment_string_field(&entry, "stage", &mut stage_counts);
        increment_string_field(&entry, "status", &mut status_counts);
        if let Some((key, fingerprint)) = skein_lightning_checkpoint_idempotency_fingerprint(&entry)
        {
            if let Some(previous) = idempotency_fingerprints.get(&key) {
                if previous != &fingerprint {
                    let message = format!(
                        "checkpoint log line {} reuses idempotency key for conflicting checkpoint coordinates",
                        line_index + 1
                    );
                    record_error(errors, checkpoint_errors, message.clone());
                    idempotency_conflicts.push(message);
                }
            } else {
                idempotency_fingerprints.insert(key, fingerprint);
            }
        }
        if entry.get("status").and_then(serde_json::Value::as_str) == Some("failed") {
            collect_string_field(&entry, "source_range", &mut failed_source_ranges);
            collect_string_field(&entry, "object_digest", &mut failed_object_digests);
            collect_string_field(&entry, "partition", &mut failed_partitions);
            collect_string_field(&entry, "validation_rule", &mut failed_validation_rules);
            increment_string_field(&entry, "partition", &mut failure_partition_counts);
            increment_string_field(&entry, "validation_rule", &mut failure_rule_counts);
            failed.push(entry.clone());
        }
        entries.push(entry);
    }

    let last_checkpoint = entries.last().cloned().unwrap_or(serde_json::Value::Null);
    let decision = if checkpoint_errors.is_empty() {
        "ready"
    } else {
        "blocked"
    };
    serde_json::json!({
        "present": true,
        "path": "skein_lightning_import_checkpoints.jsonl",
        "entry_count": entries.len(),
        "idempotency_key_count": idempotency_fingerprints.len(),
        "idempotency_conflicts": idempotency_conflicts.len(),
        "idempotency_conflict_messages": idempotency_conflicts,
        "last_checkpoint": last_checkpoint,
        "failed_checkpoints": failed,
        "checkpoint_summary": {
            "stage_counts": stage_counts,
            "status_counts": status_counts,
            "failure_rule_counts": failure_rule_counts,
            "failure_partition_counts": failure_partition_counts,
        },
        "resume_summary": {
            "last_stage": last_checkpoint.get("stage").cloned().unwrap_or(serde_json::Value::Null),
            "last_source_range": last_checkpoint.get("source_range").cloned().unwrap_or(serde_json::Value::Null),
            "last_object_digest": last_checkpoint.get("object_digest").cloned().unwrap_or(serde_json::Value::Null),
            "last_partition": last_checkpoint.get("partition").cloned().unwrap_or(serde_json::Value::Null),
            "failed_source_ranges": failed_source_ranges.into_iter().collect::<Vec<_>>(),
            "failed_object_digests": failed_object_digests.into_iter().collect::<Vec<_>>(),
            "failed_partitions": failed_partitions.into_iter().collect::<Vec<_>>(),
            "failed_validation_rules": failed_validation_rules.into_iter().collect::<Vec<_>>(),
        },
        "checkpoint_gate": {
            "decision": decision,
            "errors": checkpoint_errors,
        },
    })
}

fn skein_lightning_checkpoint_idempotency_fingerprint(
    entry: &serde_json::Value,
) -> Option<(String, serde_json::Value)> {
    let import_id = marker_string_field(entry, "import_id")?;
    let task_id = marker_string_field(entry, "task_id")?;
    let fencing_token = marker_string_field(entry, "fencing_token")?;
    let object_digest = marker_string_field(entry, "object_digest")?;
    let key = serde_json::json!({
        "import_id": import_id,
        "task_id": task_id,
        "fencing_token": fencing_token,
        "object_digest": object_digest,
    })
    .to_string();
    let fingerprint = serde_json::json!({
        "source_range": entry.get("source_range").cloned().unwrap_or(serde_json::Value::Null),
        "partition": entry.get("partition").cloned().unwrap_or(serde_json::Value::Null),
        "manifest_digest": entry.get("manifest_digest").cloned().unwrap_or(serde_json::Value::Null),
    });
    Some((key, fingerprint))
}

fn skein_lightning_import_checkpoint_blocked_json(
    checkpoint_errors: &[String],
) -> serde_json::Value {
    serde_json::json!({
        "present": true,
        "path": "skein_lightning_import_checkpoints.jsonl",
        "entry_count": 0,
        "idempotency_key_count": 0,
        "idempotency_conflicts": 0,
        "idempotency_conflict_messages": [],
        "last_checkpoint": serde_json::Value::Null,
        "failed_checkpoints": [],
        "checkpoint_summary": {
            "stage_counts": {},
            "status_counts": {},
            "failure_rule_counts": {},
            "failure_partition_counts": {},
        },
        "resume_summary": {
            "last_stage": serde_json::Value::Null,
            "last_source_range": serde_json::Value::Null,
            "last_object_digest": serde_json::Value::Null,
            "last_partition": serde_json::Value::Null,
            "failed_source_ranges": [],
            "failed_object_digests": [],
            "failed_partitions": [],
            "failed_validation_rules": [],
        },
        "checkpoint_gate": {
            "decision": "blocked",
            "errors": checkpoint_errors,
        },
    })
}

fn skein_lightning_validate_checkpoint_entry(
    entry: &serde_json::Value,
    line_number: usize,
    errors: &mut Vec<String>,
    checkpoint_errors: &mut Vec<String>,
) {
    let stage = marker_string_field(entry, "stage");
    if stage.is_none() {
        record_error(
            errors,
            checkpoint_errors,
            format!("checkpoint log line {line_number} missing stage"),
        );
    }
    let status = marker_string_field(entry, "status");
    if !matches!(status, Some("completed" | "failed")) {
        record_error(
            errors,
            checkpoint_errors,
            format!("checkpoint log line {line_number} has unsupported status"),
        );
    }
    if status == Some("failed") {
        for field in [
            "source_range",
            "object_digest",
            "partition",
            "validation_rule",
        ] {
            if marker_string_field(entry, field).is_none() {
                record_error(
                    errors,
                    checkpoint_errors,
                    format!("checkpoint log line {line_number} failed entry missing {field}"),
                );
            }
        }
    }
    if matches!(
        stage,
        Some("object_uploaded" | "object_verified" | "merge_range_committed")
    ) {
        for field in ["import_id", "task_id", "fencing_token", "object_digest"] {
            if marker_string_field(entry, field).is_none() {
                record_error(
                    errors,
                    checkpoint_errors,
                    format!("checkpoint log line {line_number} missing idempotency field {field}"),
                );
            }
        }
    }
}

fn collect_string_field(entry: &serde_json::Value, field: &str, output: &mut BTreeSet<String>) {
    if let Some(value) = marker_string_field(entry, field) {
        output.insert(value.to_string());
    }
}

fn increment_string_field(
    entry: &serde_json::Value,
    field: &str,
    output: &mut BTreeMap<String, usize>,
) {
    if let Some(value) = marker_string_field(entry, field) {
        *output.entry(value.to_string()).or_insert(0) += 1;
    }
}

pub fn skein_lightning_import_state_marker(
    staging_dir: &Path,
    errors: &mut Vec<String>,
    state_errors: &mut Vec<String>,
) -> serde_json::Value {
    let state_path = staging_dir.join("skein_lightning_import_state.json");
    if !state_path.exists() {
        return serde_json::json!({
            "present": false,
            "path": "skein_lightning_import_state.json",
            "import_state": serde_json::Value::Null,
            "idempotency_ready": false,
            "idempotency_key": serde_json::Value::Null,
            "raw": serde_json::Value::Null,
        });
    }

    let marker = match read_json_file(&state_path) {
        Ok(marker) => marker,
        Err(error) => {
            record_error(
                errors,
                state_errors,
                format!("import state marker could not be read: {error}"),
            );
            return serde_json::json!({
                "present": true,
                "path": "skein_lightning_import_state.json",
                "import_state": "QUARANTINED",
                "idempotency_ready": false,
                "idempotency_key": serde_json::Value::Null,
                "raw": serde_json::Value::Null,
            });
        }
    };
    let protocol_valid = marker.get("protocol").and_then(serde_json::Value::as_str)
        == Some("skein-lightning-import-state");
    let version_valid = marker
        .get("protocol_version")
        .and_then(serde_json::Value::as_u64)
        == Some(1);
    let import_state = marker
        .get("import_state")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("QUARANTINED");

    if !protocol_valid {
        record_error(
            errors,
            state_errors,
            "import state marker protocol mismatch",
        );
    }
    if !version_valid {
        record_error(
            errors,
            state_errors,
            "import state marker protocol version mismatch",
        );
    }
    if !skein_lightning_import_marker_state_allowed(import_state) {
        record_error(
            errors,
            state_errors,
            format!("import state marker uses unsupported state {import_state}"),
        );
    }
    let idempotency_key =
        skein_lightning_import_marker_idempotency_key(&marker, import_state, errors, state_errors);

    serde_json::json!({
        "present": true,
        "path": "skein_lightning_import_state.json",
        "import_state": if state_errors.is_empty() { import_state } else { "QUARANTINED" },
        "idempotency_ready": state_errors.is_empty() && idempotency_key.is_some(),
        "idempotency_key": idempotency_key,
        "raw": marker,
    })
}

fn skein_lightning_import_marker_state_allowed(import_state: &str) -> bool {
    matches!(
        import_state,
        "EXPORTING" | "UPLOADING" | "MERGING" | "VALIDATING" | "FAILED" | "CANCELED"
    )
}

fn skein_lightning_import_marker_idempotency_key(
    marker: &serde_json::Value,
    import_state: &str,
    errors: &mut Vec<String>,
    state_errors: &mut Vec<String>,
) -> Option<serde_json::Value> {
    let import_id = marker_string_field(marker, "import_id");
    let task_id = marker_string_field(marker, "task_id");
    let fencing_token = marker_string_field(marker, "fencing_token");
    let object_digest = marker_string_field(marker, "object_digest");
    if skein_lightning_import_marker_state_is_active(import_state) {
        for missing in [
            ("import_id", import_id),
            ("task_id", task_id),
            ("fencing_token", fencing_token),
            ("object_digest", object_digest),
        ]
        .into_iter()
        .filter_map(|(field, value)| value.is_none().then_some(field))
        {
            record_error(
                errors,
                state_errors,
                format!("active import state marker missing idempotency field {missing}"),
            );
        }
    }

    Some(serde_json::json!({
        "import_id": import_id?,
        "task_id": task_id?,
        "fencing_token": fencing_token?,
        "object_digest": object_digest?,
    }))
}

fn skein_lightning_import_marker_state_is_active(import_state: &str) -> bool {
    matches!(
        import_state,
        "EXPORTING" | "UPLOADING" | "MERGING" | "VALIDATING"
    )
}

fn marker_string_field<'a>(marker: &'a serde_json::Value, field: &str) -> Option<&'a str> {
    marker
        .get(field)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
}

fn skein_lightning_effective_import_state<'a>(
    artifact_state: &'a str,
    marker_state: Option<&'a str>,
) -> &'a str {
    if artifact_state == "PUBLISHED" || artifact_state == "QUARANTINED" {
        return artifact_state;
    }
    marker_state.unwrap_or(artifact_state)
}

fn skein_lightning_import_resource_retention(
    import_state: &str,
    staging_catalog_present: bool,
    staging_dir: &Path,
    publish_dir: &Path,
    errors: &mut Vec<String>,
    resource_errors: &mut Vec<String>,
) -> serde_json::Value {
    if !staging_catalog_present {
        return serde_json::json!({
            "action": "none",
            "safe_to_collect": false,
            "protected_count": 0,
            "deletable_count": 0,
            "reason": "staging catalog is missing",
            "gc_report": serde_json::Value::Null,
        });
    }

    let gc_report = match skein_lightning_gc_staging_report(staging_dir, publish_dir) {
        Ok(report) => report,
        Err(error) => {
            record_error(
                errors,
                resource_errors,
                format!("resource retention report failed: {error}"),
            );
            return serde_json::json!({
                "action": "hold_for_inspection",
                "safe_to_collect": false,
                "protected_count": 0,
                "deletable_count": 0,
                "reason": "resource retention could not verify staging artifacts",
                "gc_report": serde_json::Value::Null,
            });
        }
    };
    let candidate_count = gc_report
        .get("candidate_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let pinned_count = gc_report
        .get("pinned_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let gc_deletable_count = gc_report
        .get("deletable_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let gc_ready = gate_decision(&gc_report, "gc_gate") == Some("ready");

    match import_state {
        "READY" => serde_json::json!({
            "action": "retain_for_publish",
            "safe_to_collect": false,
            "protected_count": candidate_count,
            "deletable_count": 0,
            "gc_deletable_count": gc_deletable_count,
            "reason": "staging artifacts are required for publishing",
            "gc_report": gc_report,
        }),
        "PUBLISHED" => serde_json::json!({
            "action": "follow_gc_report",
            "safe_to_collect": gc_ready && gc_deletable_count > 0,
            "protected_count": pinned_count,
            "deletable_count": if gc_ready { gc_deletable_count } else { 0 },
            "gc_deletable_count": gc_deletable_count,
            "reason": "published pointer verification controls staging retention",
            "gc_report": gc_report,
        }),
        "EXPORTING" | "UPLOADING" | "MERGING" | "VALIDATING" => serde_json::json!({
            "action": "retain_for_active_import",
            "safe_to_collect": false,
            "protected_count": candidate_count,
            "deletable_count": 0,
            "gc_deletable_count": gc_deletable_count,
            "reason": "import state marker reports active import work",
            "gc_report": gc_report,
        }),
        "FAILED" | "CANCELED" => serde_json::json!({
            "action": "hold_for_inspection",
            "safe_to_collect": false,
            "protected_count": candidate_count,
            "deletable_count": 0,
            "gc_deletable_count": gc_deletable_count,
            "reason": "import state marker reports terminal import work",
            "gc_report": gc_report,
        }),
        "QUARANTINED" => serde_json::json!({
            "action": "hold_for_inspection",
            "safe_to_collect": false,
            "protected_count": candidate_count,
            "deletable_count": 0,
            "gc_deletable_count": gc_deletable_count,
            "reason": "status gate has blocking errors",
            "gc_report": gc_report,
        }),
        _ => serde_json::json!({
            "action": "hold_for_inspection",
            "safe_to_collect": false,
            "protected_count": candidate_count,
            "deletable_count": 0,
            "gc_deletable_count": gc_deletable_count,
            "reason": "unknown import state",
            "gc_report": gc_report,
        }),
    }
}

fn skein_lightning_import_resume_action(import_state: &str) -> serde_json::Value {
    match import_state {
        "CREATED" => serde_json::json!({
            "operation": "stage_bootstrap",
            "safe_to_retry": true,
            "terminal": false,
            "reason": "staging catalog is missing",
        }),
        "READY" => serde_json::json!({
            "operation": "publish_staging",
            "safe_to_retry": true,
            "terminal": false,
            "reason": "staging catalog verified but no published pointer exists",
        }),
        "EXPORTING" => serde_json::json!({
            "operation": "continue_export",
            "safe_to_retry": true,
            "terminal": false,
            "reason": "import state marker reports export in progress",
        }),
        "UPLOADING" => serde_json::json!({
            "operation": "continue_upload",
            "safe_to_retry": true,
            "terminal": false,
            "reason": "import state marker reports upload in progress",
        }),
        "MERGING" => serde_json::json!({
            "operation": "continue_merge",
            "safe_to_retry": true,
            "terminal": false,
            "reason": "import state marker reports merge in progress",
        }),
        "VALIDATING" => serde_json::json!({
            "operation": "continue_validation",
            "safe_to_retry": true,
            "terminal": false,
            "reason": "import state marker reports validation in progress",
        }),
        "PUBLISHED" => serde_json::json!({
            "operation": "none",
            "safe_to_retry": false,
            "terminal": true,
            "reason": "published pointer verified",
        }),
        "FAILED" => serde_json::json!({
            "operation": "inspect_errors",
            "safe_to_retry": false,
            "terminal": true,
            "reason": "import state marker reports failed import",
        }),
        "CANCELED" => serde_json::json!({
            "operation": "none",
            "safe_to_retry": false,
            "terminal": true,
            "reason": "import state marker reports canceled import",
        }),
        "QUARANTINED" => serde_json::json!({
            "operation": "inspect_errors",
            "safe_to_retry": false,
            "terminal": true,
            "reason": "status gate has blocking errors",
        }),
        _ => serde_json::json!({
            "operation": "inspect_errors",
            "safe_to_retry": false,
            "terminal": true,
            "reason": "unknown import state",
        }),
    }
}

fn gate_decision<'a>(report: &'a serde_json::Value, gate: &str) -> Option<&'a str> {
    report
        .get(gate)
        .and_then(|gate| gate.get("decision"))
        .and_then(serde_json::Value::as_str)
}

fn gate_errors(report: &serde_json::Value, gate: &str) -> Vec<String> {
    report
        .get(gate)
        .and_then(|gate| gate.get("errors"))
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(str::to_string)
        .collect()
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

#[cfg(test)]
mod tests {
    use super::skein_lightning_import_status;

    #[test]
    fn reports_created_before_any_staging_artifact_exists() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "skein-bootstrap-import-status-{}_{}",
            std::process::id(),
            nonce
        ));
        let staging_dir = root.join("staging");
        let publish_dir = root.join("published");
        std::fs::create_dir_all(&staging_dir).unwrap();

        let report = skein_lightning_import_status(&staging_dir, &publish_dir).unwrap();

        assert_eq!(report["import_state"], "CREATED");
        assert_eq!(report["artifact_state"], "CREATED");
        assert_eq!(report["status_gate"]["decision"], "ready");
        std::fs::remove_dir_all(root).unwrap();
    }
}
