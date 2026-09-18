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

//! Host-neutral Nowledge storage-recovery lifecycle protocol.

use hawdb_storage::{RecoveryMode, StorageOpenTimings, StorageRecoveryReport};

pub const NOWLEDGE_MEM_STORAGE_LIFECYCLE_DECISION_PROTOCOL: &str =
    "hawdb-nowledge-mem-storage-lifecycle-decision-v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemStorageRecoveryReport {
    pub protocol: String,
    pub present: bool,
    pub ready: bool,
    pub open_timings: StorageOpenTimings,
    pub durable: bool,
    pub recovery_mode: RecoveryMode,
    pub checkpoint_epoch: Option<u64>,
    pub checkpoint_commit_epoch: Option<u64>,
    pub wal_present: bool,
    pub wal_generation: Option<u64>,
    pub wal_replay_start_lsn: Option<u64>,
    pub next_lsn_after_replay: Option<u64>,
    pub replayed_wal_entries: usize,
    pub replayed_wal_bytes: u64,
    pub max_wal_replay_entries: Option<usize>,
    pub max_wal_replay_bytes: Option<u64>,
    pub max_wal_record_bytes: Option<usize>,
    pub torn_tail_ignored: bool,
    pub torn_tail_repaired: bool,
    pub discarded_wal_tail_bytes: u64,
    pub torn_tail_reason: Option<String>,
    pub recovered_commit_epoch: u64,
    pub durable_recovery_observed: bool,
    pub checkpoint_boundary_present: bool,
    pub wal_replay_bounded: bool,
    pub replay_boundary_consistent: bool,
    pub torn_tail_clean: bool,
    pub open_timing_consistent: bool,
    pub blocker_codes: Vec<String>,
}

impl NowledgeMemStorageRecoveryReport {
    pub fn from_storage_report(report: &StorageRecoveryReport) -> Self {
        let durable_recovery_observed = report.durable;
        let checkpoint_boundary_present =
            report.checkpoint_epoch.is_some() && report.checkpoint_commit_epoch.is_some();
        let wal_replay_bounded = report
            .max_wal_replay_entries
            .is_some_and(|limit| report.replayed_wal_entries <= limit)
            && report
                .max_wal_replay_bytes
                .is_some_and(|limit| report.replayed_wal_bytes <= limit)
            && report.max_wal_record_bytes.is_some();
        let replay_boundary_consistent = storage_recovery_replay_boundary_consistent(report);
        let torn_tail_clean = (!report.torn_tail_ignored && report.torn_tail_reason.is_none())
            || report.torn_tail_repaired;
        let open_timing_consistent = report.open_timings.is_consistent();
        let mut blocker_codes = Vec::new();
        if !durable_recovery_observed {
            blocker_codes.push("durable_recovery_not_observed".to_string());
        }
        if !checkpoint_boundary_present {
            blocker_codes.push("checkpoint_boundary_missing".to_string());
        }
        if !wal_replay_bounded {
            blocker_codes.push("wal_replay_unbounded".to_string());
        }
        if !replay_boundary_consistent {
            blocker_codes.push("replay_boundary_inconsistent".to_string());
        }
        if !torn_tail_clean {
            blocker_codes.push("torn_tail_observed".to_string());
        }
        if !open_timing_consistent {
            blocker_codes.push("storage_open_timing_inconsistent".to_string());
        }

        Self {
            protocol: "hawdb-storage-recovery-report".to_string(),
            present: true,
            ready: blocker_codes.is_empty(),
            open_timings: report.open_timings,
            durable: report.durable,
            recovery_mode: report.recovery_mode,
            checkpoint_epoch: report.checkpoint_epoch,
            checkpoint_commit_epoch: report.checkpoint_commit_epoch,
            wal_present: report.wal_present,
            wal_generation: report.wal_generation,
            wal_replay_start_lsn: report.wal_replay_start_lsn,
            next_lsn_after_replay: report.next_lsn_after_replay,
            replayed_wal_entries: report.replayed_wal_entries,
            replayed_wal_bytes: report.replayed_wal_bytes,
            max_wal_replay_entries: report.max_wal_replay_entries,
            max_wal_replay_bytes: report.max_wal_replay_bytes,
            max_wal_record_bytes: report.max_wal_record_bytes,
            torn_tail_ignored: report.torn_tail_ignored,
            torn_tail_repaired: report.torn_tail_repaired,
            discarded_wal_tail_bytes: report.discarded_wal_tail_bytes,
            torn_tail_reason: report.torn_tail_reason.clone(),
            recovered_commit_epoch: report.recovered_commit_epoch,
            durable_recovery_observed,
            checkpoint_boundary_present,
            wal_replay_bounded,
            replay_boundary_consistent,
            torn_tail_clean,
            open_timing_consistent,
            blocker_codes,
        }
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "present": self.present,
            "ready": self.ready,
            "open_timings": {
                "durable_manifest_open_micros": self.open_timings.durable_manifest_open_micros,
                "checkpoint_root_open_micros": self.open_timings.checkpoint_root_open_micros,
                "wal_replay_micros": self.open_timings.wal_replay_micros,
                "post_replay_open_micros": self.open_timings.post_replay_open_micros,
                "accounted_micros": self.open_timings.accounted_micros(),
                "unaccounted_micros": self.open_timings.unaccounted_micros(),
                "total_open_micros": self.open_timings.total_open_micros,
            },
            "durable": self.durable,
            "recovery_mode": recovery_mode_name(self.recovery_mode),
            "checkpoint_epoch": self.checkpoint_epoch,
            "checkpoint_commit_epoch": self.checkpoint_commit_epoch,
            "wal_present": self.wal_present,
            "wal_generation": self.wal_generation,
            "wal_replay_start_lsn": self.wal_replay_start_lsn,
            "next_lsn_after_replay": self.next_lsn_after_replay,
            "replayed_wal_entries": self.replayed_wal_entries,
            "replayed_wal_bytes": self.replayed_wal_bytes,
            "max_wal_replay_entries": self.max_wal_replay_entries,
            "max_wal_replay_bytes": self.max_wal_replay_bytes,
            "max_wal_record_bytes": self.max_wal_record_bytes,
            "torn_tail_ignored": self.torn_tail_ignored,
            "torn_tail_repaired": self.torn_tail_repaired,
            "discarded_wal_tail_bytes": self.discarded_wal_tail_bytes,
            "torn_tail_reason": self.torn_tail_reason,
            "recovered_commit_epoch": self.recovered_commit_epoch,
            "readiness": {
                "durable_recovery_observed": self.durable_recovery_observed,
                "checkpoint_boundary_present": self.checkpoint_boundary_present,
                "wal_replay_bounded": self.wal_replay_bounded,
                "replay_boundary_consistent": self.replay_boundary_consistent,
                "torn_tail_clean": self.torn_tail_clean,
                "open_timing_consistent": self.open_timing_consistent,
            },
            "blocker_codes": self.blocker_codes,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NowledgeMemStorageLifecycleActionKind {
    Ready,
    RunCheckpoint,
    RepairWalTail,
    Quarantine,
    OpenReadOnlyInspect,
}

impl NowledgeMemStorageLifecycleActionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::RunCheckpoint => "run_checkpoint",
            Self::RepairWalTail => "repair_wal_tail",
            Self::Quarantine => "quarantine",
            Self::OpenReadOnlyInspect => "open_read_only_inspect",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemStorageLifecycleDecision {
    pub protocol: String,
    pub action: NowledgeMemStorageLifecycleActionKind,
    pub ready_for_mem_lifecycle: bool,
    pub storage_recovery_ready: bool,
    pub checkpoint_required: bool,
    pub repair_required: bool,
    pub quarantine_required: bool,
    pub read_only_inspection_required: bool,
    pub blocker_codes: Vec<String>,
    pub recovery: NowledgeMemStorageRecoveryReport,
}

impl NowledgeMemStorageLifecycleDecision {
    pub fn from_storage_recovery(recovery: NowledgeMemStorageRecoveryReport) -> Self {
        let mut blocker_codes = recovery.blocker_codes.clone();
        let action = if recovery.ready {
            NowledgeMemStorageLifecycleActionKind::Ready
        } else if !recovery.durable_recovery_observed {
            push_unique_blocker(&mut blocker_codes, "storage_not_durable");
            NowledgeMemStorageLifecycleActionKind::OpenReadOnlyInspect
        } else if !recovery.torn_tail_clean {
            push_unique_blocker(&mut blocker_codes, "wal_tail_repair_required");
            NowledgeMemStorageLifecycleActionKind::RepairWalTail
        } else if !recovery.checkpoint_boundary_present {
            push_unique_blocker(&mut blocker_codes, "checkpoint_required");
            NowledgeMemStorageLifecycleActionKind::RunCheckpoint
        } else if !recovery.replay_boundary_consistent || !recovery.wal_replay_bounded {
            push_unique_blocker(&mut blocker_codes, "storage_recovery_quarantine_required");
            NowledgeMemStorageLifecycleActionKind::Quarantine
        } else {
            push_unique_blocker(&mut blocker_codes, "storage_recovery_unknown_blocker");
            NowledgeMemStorageLifecycleActionKind::Quarantine
        };

        Self {
            protocol: NOWLEDGE_MEM_STORAGE_LIFECYCLE_DECISION_PROTOCOL.to_string(),
            ready_for_mem_lifecycle: action == NowledgeMemStorageLifecycleActionKind::Ready,
            storage_recovery_ready: recovery.ready,
            checkpoint_required: action == NowledgeMemStorageLifecycleActionKind::RunCheckpoint,
            repair_required: action == NowledgeMemStorageLifecycleActionKind::RepairWalTail,
            quarantine_required: action == NowledgeMemStorageLifecycleActionKind::Quarantine,
            read_only_inspection_required: action
                == NowledgeMemStorageLifecycleActionKind::OpenReadOnlyInspect,
            action,
            blocker_codes,
            recovery,
        }
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "action": self.action.as_str(),
            "ready_for_mem_lifecycle": self.ready_for_mem_lifecycle,
            "storage_recovery_ready": self.storage_recovery_ready,
            "checkpoint_required": self.checkpoint_required,
            "repair_required": self.repair_required,
            "quarantine_required": self.quarantine_required,
            "read_only_inspection_required": self.read_only_inspection_required,
            "blocker_codes": self.blocker_codes,
            "recovery": self.recovery.json(),
        })
    }
}

fn push_unique_blocker(blocker_codes: &mut Vec<String>, code: &str) {
    if !blocker_codes.iter().any(|existing| existing == code) {
        blocker_codes.push(code.to_string());
    }
}

fn storage_recovery_replay_boundary_consistent(report: &StorageRecoveryReport) -> bool {
    let Some(checkpoint_commit_epoch) = report.checkpoint_commit_epoch else {
        return false;
    };
    let Some(wal_replay_start_lsn) = report.wal_replay_start_lsn else {
        return false;
    };
    let Some(next_lsn_after_replay) = report.next_lsn_after_replay else {
        return false;
    };
    let Ok(replayed_wal_entries) = u64::try_from(report.replayed_wal_entries) else {
        return false;
    };
    checkpoint_commit_epoch <= report.recovered_commit_epoch
        && wal_replay_start_lsn.checked_add(replayed_wal_entries) == Some(next_lsn_after_replay)
        && checkpoint_commit_epoch.checked_add(replayed_wal_entries)
            == Some(report.recovered_commit_epoch)
}

fn recovery_mode_name(mode: RecoveryMode) -> &'static str {
    match mode {
        RecoveryMode::Strict => "strict",
        RecoveryMode::AutoRepairTornTail => "auto_repair_torn_tail",
        RecoveryMode::DoctorRepairTornTail => "doctor_repair_torn_tail",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clean_storage_recovery() -> StorageRecoveryReport {
        StorageRecoveryReport {
            durable: true,
            recovery_mode: RecoveryMode::Strict,
            max_wal_replay_entries: Some(16),
            max_wal_replay_bytes: Some(4096),
            max_wal_record_bytes: Some(1024),
            checkpoint_epoch: Some(3),
            checkpoint_commit_epoch: Some(11),
            wal_present: true,
            wal_replay_start_lsn: Some(4),
            next_lsn_after_replay: Some(7),
            replayed_wal_entries: 3,
            recovered_commit_epoch: 14,
            ..StorageRecoveryReport::default()
        }
    }

    #[test]
    fn clean_recovery_is_ready_and_preserves_protocol_fields() {
        let report =
            NowledgeMemStorageRecoveryReport::from_storage_report(&clean_storage_recovery());

        assert!(report.ready);
        assert!(report.replay_boundary_consistent);
        assert_eq!(report.json()["recovery_mode"], "strict");
    }

    #[test]
    fn torn_tail_requires_repair_before_lifecycle_is_ready() {
        let mut raw = clean_storage_recovery();
        raw.torn_tail_ignored = true;
        raw.torn_tail_reason = Some("partial wal entry".to_string());

        let decision = NowledgeMemStorageLifecycleDecision::from_storage_recovery(
            NowledgeMemStorageRecoveryReport::from_storage_report(&raw),
        );

        assert_eq!(
            decision.action,
            NowledgeMemStorageLifecycleActionKind::RepairWalTail
        );
        assert!(decision.repair_required);
        assert!(!decision.ready_for_mem_lifecycle);
    }
}
