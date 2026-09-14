use crate::inventory::storage_recovery_evidence_health;
use skein_core::{Result, SkeinError};
use std::path::Path;

pub fn nowledge_storage_recovery_evidence_usage() -> String {
    "nowledge-storage-recovery-evidence requires [--require-ready] [--optional] <storage-recovery-report-json>".to_string()
}

pub fn run_nowledge_storage_recovery_evidence(
    mut args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let mut require_ready = false;
    let mut required = true;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--require-ready" => {
                require_ready = true;
            }
            "--optional" => {
                required = false;
            }
            path => {
                if args.next().is_some() {
                    return Err(SkeinError::Semantic(
                        nowledge_storage_recovery_evidence_usage(),
                    ));
                }
                let report = read_json_file(Path::new(path))?;
                return Ok((
                    nowledge_storage_recovery_evidence_json(&report, required),
                    require_ready,
                ));
            }
        }
    }
    Err(SkeinError::Semantic(
        nowledge_storage_recovery_evidence_usage(),
    ))
}

pub fn nowledge_storage_recovery_evidence_json(
    report: &serde_json::Value,
    required: bool,
) -> serde_json::Value {
    let health = storage_recovery_evidence_health(Some(report), required);
    serde_json::json!({
        "protocol": "skein-nowledge-storage-recovery-evidence-v1",
        "required": health.required,
        "present": health.present,
        "ready": health.ready,
        "protocol_matches": health.protocol_matches,
        "durable_recovery_observed": health.durable_recovery_observed,
        "checkpoint_boundary_present": health.checkpoint_boundary_present,
        "wal_replay_bounded": health.wal_replay_bounded,
        "replay_boundary_consistent": health.replay_boundary_consistent,
        "torn_tail_clean": health.torn_tail_clean,
        "blocker_codes": health.blocker_codes,
        "blockers": health.blockers,
        "storage_recovery_required": health.required,
        "storage_recovery_present": health.present,
        "storage_recovery_ready": health.ready,
        "storage_recovery_protocol_matches": health.protocol_matches,
        "storage_recovery_durable": health.durable_recovery_observed,
        "storage_recovery_durable_recovery_observed": health.durable_recovery_observed,
        "storage_recovery_checkpoint_boundary_present": health.checkpoint_boundary_present,
        "storage_recovery_wal_replay_bounded": health.wal_replay_bounded,
        "storage_recovery_replay_boundary_consistent": health.replay_boundary_consistent,
        "storage_recovery_torn_tail_clean": health.torn_tail_clean,
        "storage_recovery_blocker_codes": health.blocker_codes,
        "storage_recovery_blockers": health.blockers,
    })
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let content = std::fs::read_to_string(path).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to read storage recovery report JSON: {}",
            error.kind()
        ))
    })?;
    serde_json::from_str(&content).map_err(|_| {
        SkeinError::Semantic(
            "failed to parse storage recovery report JSON: invalid_json".to_string(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::run_nowledge_storage_recovery_evidence;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn storage_recovery_evidence_command_accepts_ready_report() {
        let path = unique_test_file("storage_recovery_ready");
        std::fs::write(&path, ready_report().to_string()).unwrap();

        let (evidence, require_ready) = run_nowledge_storage_recovery_evidence(
            ["--require-ready", path.to_str().unwrap()]
                .into_iter()
                .map(str::to_string),
        )
        .unwrap();

        assert!(require_ready);
        assert_eq!(
            evidence["protocol"],
            "skein-nowledge-storage-recovery-evidence-v1"
        );
        assert_eq!(evidence["ready"], true);
        assert_eq!(evidence["storage_recovery_required"], true);
        assert_eq!(evidence["storage_recovery_ready"], true);
        assert_eq!(evidence["storage_recovery_protocol_matches"], true);
        assert_eq!(evidence["storage_recovery_durable"], true);
        assert_eq!(
            evidence["storage_recovery_checkpoint_boundary_present"],
            true
        );
        assert_eq!(evidence["storage_recovery_wal_replay_bounded"], true);
        assert_eq!(
            evidence["storage_recovery_replay_boundary_consistent"],
            true
        );
        assert_eq!(
            evidence["storage_recovery_blocker_codes"],
            serde_json::json!([])
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn storage_recovery_parse_errors_are_redacted_by_default() {
        let path = unique_test_file("storage_recovery_secret_path_do_not_emit");
        std::fs::write(
            &path,
            "{ \"wal_path\": \"secret-wal-path-do-not-emit\", \"unterminated\": ",
        )
        .unwrap();

        let error = super::read_json_file(&path).unwrap_err().to_string();

        assert_eq!(
            error,
            "semantic error: failed to parse storage recovery report JSON: invalid_json"
        );
        assert!(!error.contains("storage_recovery_secret_path_do_not_emit"));
        assert!(!error.contains("secret-wal-path-do-not-emit"));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn storage_recovery_evidence_command_fails_closed_for_incomplete_report() {
        let path = unique_test_file("storage_recovery_incomplete");
        let mut report = ready_report();
        report["readiness"]["wal_replay_bounded"] = serde_json::json!(false);
        report["readiness"]["torn_tail_clean"] = serde_json::json!(false);
        std::fs::write(&path, report.to_string()).unwrap();

        let (evidence, _) = run_nowledge_storage_recovery_evidence(
            [path.to_str().unwrap()].into_iter().map(str::to_string),
        )
        .unwrap();

        assert_eq!(evidence["ready"], false);
        assert_eq!(
            evidence["storage_recovery_blocker_codes"],
            serde_json::json!(["wal_replay_unbounded", "torn_tail_observed"])
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn storage_recovery_evidence_command_recomputes_contradictory_raw_fields() {
        let path = unique_test_file("storage_recovery_contradictory");
        let mut report = ready_report();
        report["checkpoint_commit_epoch"] = serde_json::json!(null);
        report["replayed_wal_entries"] = serde_json::json!(2048);
        report["max_wal_replay_entries"] = serde_json::json!(1024);
        report["next_lsn_after_replay"] = serde_json::json!(42);
        report["recovered_commit_epoch"] = serde_json::json!(700);
        report["torn_tail_ignored"] = serde_json::json!(true);
        report["torn_tail_reason"] = serde_json::json!("partial wal entry");
        std::fs::write(&path, report.to_string()).unwrap();

        let (evidence, _) = run_nowledge_storage_recovery_evidence(
            [path.to_str().unwrap()].into_iter().map(str::to_string),
        )
        .unwrap();

        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["storage_recovery_ready"], false);
        assert_eq!(evidence["checkpoint_boundary_present"], false);
        assert_eq!(evidence["wal_replay_bounded"], false);
        assert_eq!(evidence["replay_boundary_consistent"], false);
        assert_eq!(evidence["torn_tail_clean"], false);
        assert_eq!(
            evidence["storage_recovery_blocker_codes"],
            serde_json::json!([
                "checkpoint_boundary_missing",
                "wal_replay_unbounded",
                "replay_boundary_inconsistent",
                "torn_tail_observed"
            ])
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn storage_recovery_evidence_command_rejects_inconsistent_replay_boundary() {
        let path = unique_test_file("storage_recovery_replay_boundary");
        let mut report = ready_report();
        report["next_lsn_after_replay"] = serde_json::json!(12);
        report["recovered_commit_epoch"] = serde_json::json!(9);
        std::fs::write(&path, report.to_string()).unwrap();

        let (evidence, _) = run_nowledge_storage_recovery_evidence(
            [path.to_str().unwrap()].into_iter().map(str::to_string),
        )
        .unwrap();

        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["storage_recovery_ready"], false);
        assert_eq!(evidence["storage_recovery_wal_replay_bounded"], true);
        assert_eq!(
            evidence["storage_recovery_replay_boundary_consistent"],
            false
        );
        assert_eq!(
            evidence["storage_recovery_blocker_codes"],
            serde_json::json!(["replay_boundary_inconsistent"])
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn storage_recovery_evidence_optional_missing_file_still_reports_read_error() {
        let result = run_nowledge_storage_recovery_evidence(
            ["--optional", "/path/that/does/not/exist.json"]
                .into_iter()
                .map(str::to_string),
        );

        let err = result.unwrap_err().to_string();
        assert!(err.contains("failed to read storage recovery report JSON"));
        assert!(!err.contains("/path/that/does/not/exist.json"));
    }

    fn ready_report() -> serde_json::Value {
        serde_json::json!({
            "protocol": "skein-storage-recovery-report",
            "storage_version": "skein-storage-v1",
            "durable": true,
            "recovery_mode": "snapshot_and_wal",
            "max_wal_replay_entries": 1024,
            "checkpoint_epoch": 7,
            "checkpoint_commit_epoch": 7,
            "wal_present": true,
            "wal_replay_start_lsn": 8,
            "next_lsn_after_replay": 9,
            "replayed_wal_entries": 1,
            "torn_tail_ignored": false,
            "torn_tail_reason": null,
            "recovered_commit_epoch": 8,
            "readiness": {
                "durable_recovery_observed": true,
                "checkpoint_boundary_present": true,
                "wal_replay_bounded": true,
                "torn_tail_clean": true
            }
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
