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

use crate::production::{ProductionEvidenceBinding, ProductionQualificationIdentity};
use std::collections::BTreeSet;

pub const STORAGE_CRASH_RECOVERY_EVIDENCE_PROTOCOL: &str =
    "hawdb-storage-crash-recovery-evidence-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum StorageCrashPoint {
    BeforeWalAppend,
    AfterWalAppend,
    AfterWalSync,
    DuringCheckpointPublication,
    AfterManifestPublication,
}

impl StorageCrashPoint {
    pub const ALL: [Self; 5] = [
        Self::BeforeWalAppend,
        Self::AfterWalAppend,
        Self::AfterWalSync,
        Self::DuringCheckpointPublication,
        Self::AfterManifestPublication,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BeforeWalAppend => "before_wal_append",
            Self::AfterWalAppend => "after_wal_append",
            Self::AfterWalSync => "after_wal_sync",
            Self::DuringCheckpointPublication => "during_checkpoint_publication",
            Self::AfterManifestPublication => "after_manifest_publication",
        }
    }

    const fn requires_committed_batch(self) -> bool {
        matches!(
            self,
            Self::AfterWalSync | Self::DuringCheckpointPublication | Self::AfterManifestPublication
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageCrashCaseEvidence {
    pub point: StorageCrashPoint,
    pub repetition: u32,
    pub process_terminated: bool,
    pub recovered_batch_present: bool,
    pub whole_batch_recovered: bool,
    pub commit_epoch: u64,
    pub recovered_commit_epoch: u64,
    pub replay_lsn_present: bool,
    pub relationship_endpoints_valid: bool,
    pub projection_watermark_valid: bool,
    pub artifact_generation_valid: bool,
}

impl StorageCrashCaseEvidence {
    fn ready(&self) -> bool {
        self.process_terminated
            && self.whole_batch_recovered
            && self.commit_epoch == self.recovered_commit_epoch
            && self.replay_lsn_present
            && self.relationship_endpoints_valid
            && self.projection_watermark_valid
            && self.artifact_generation_valid
            && (self.point != StorageCrashPoint::BeforeWalAppend || !self.recovered_batch_present)
            && (!self.point.requires_committed_batch() || self.recovered_batch_present)
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "point": self.point.as_str(),
            "repetition": self.repetition,
            "process_terminated": self.process_terminated,
            "recovered_batch_present": self.recovered_batch_present,
            "whole_batch_recovered": self.whole_batch_recovered,
            "commit_epoch": self.commit_epoch,
            "recovered_commit_epoch": self.recovered_commit_epoch,
            "replay_lsn_present": self.replay_lsn_present,
            "relationship_endpoints_valid": self.relationship_endpoints_valid,
            "projection_watermark_valid": self.projection_watermark_valid,
            "artifact_generation_valid": self.artifact_generation_valid,
            "ready": self.ready(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageCrashRecoveryEvidence {
    pub evidence_binding: ProductionEvidenceBinding,
    pub expected_identity: ProductionQualificationIdentity,
    pub required_repetitions: u32,
    pub cases: Vec<StorageCrashCaseEvidence>,
    pub blocker_codes: Vec<String>,
    pub ready: bool,
}

impl StorageCrashRecoveryEvidence {
    pub fn evaluate(
        evidence_binding: ProductionEvidenceBinding,
        expected_identity: ProductionQualificationIdentity,
        required_repetitions: u32,
        cases: Vec<StorageCrashCaseEvidence>,
    ) -> Self {
        let mut report = Self {
            evidence_binding,
            expected_identity,
            required_repetitions,
            cases,
            blocker_codes: Vec::new(),
            ready: false,
        };
        report.blocker_codes = report.recompute_blocker_codes();
        report.ready = report.blocker_codes.is_empty();
        report
    }

    pub fn json(&self) -> serde_json::Value {
        let blocker_codes = self.recompute_blocker_codes();
        serde_json::json!({
            "protocol": STORAGE_CRASH_RECOVERY_EVIDENCE_PROTOCOL,
            "protocol_version": 1,
            "evidence_binding": self.evidence_binding.json(),
            "expected_identity": self.expected_identity.json(),
            "required_repetitions": self.required_repetitions,
            "case_count": self.cases.len(),
            "cases": self.cases.iter().map(StorageCrashCaseEvidence::json).collect::<Vec<_>>(),
            "blocker_codes": blocker_codes,
            "ready": blocker_codes.is_empty(),
        })
    }

    fn recompute_blocker_codes(&self) -> Vec<String> {
        let mut blockers = self
            .evidence_binding
            .blocker_codes_for(&self.expected_identity);
        if self.required_repetitions == 0 {
            blockers.push("required_repetitions_missing".to_string());
        }
        let mut observed_cases = BTreeSet::new();
        for case in &self.cases {
            if case.repetition >= self.required_repetitions {
                blockers.push(format!("{}_repetition_out_of_range", case.point.as_str()));
            }
            if !observed_cases.insert((case.point, case.repetition)) {
                blockers.push(format!("{}_repetition_duplicate", case.point.as_str()));
            }
            if !case.ready() {
                blockers.push(format!("{}_case_failed", case.point.as_str()));
            }
        }
        for point in StorageCrashPoint::ALL {
            for repetition in 0..self.required_repetitions {
                if !observed_cases.contains(&(point, repetition)) {
                    blockers.push(format!("{}_coverage_incomplete", point.as_str()));
                    break;
                }
            }
        }
        blockers.sort();
        blockers.dedup();
        blockers
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incomplete_crash_matrix_fails_closed() {
        let identity = ProductionQualificationIdentity {
            source_revision: "revision".to_string(),
            rust_toolchain: "toolchain".to_string(),
            target_os: "linux".to_string(),
            target_arch: "x86_64".to_string(),
            enabled_features: Vec::new(),
            durable_format_version: 1,
            schema_version: 1,
            configuration_digest: "config".to_string(),
            deployment_profile: "crash-test".to_string(),
            dataset_fingerprint: "crash-matrix-v1".to_string(),
            canonical_graph_commit_epoch: 1,
            policy_version: crate::PRODUCTION_QUALIFICATION_POLICY_VERSION,
        };
        let report = StorageCrashRecoveryEvidence::evaluate(
            ProductionEvidenceBinding {
                identity: identity.clone(),
                generated_at_unix_seconds: 1,
            },
            identity,
            2,
            Vec::new(),
        );

        assert!(!report.ready);
        assert_eq!(report.blocker_codes.len(), StorageCrashPoint::ALL.len());
    }

    #[test]
    fn duplicate_repetition_cannot_satisfy_crash_matrix_coverage() {
        let identity = ProductionQualificationIdentity {
            source_revision: "revision".to_string(),
            rust_toolchain: "toolchain".to_string(),
            target_os: "linux".to_string(),
            target_arch: "x86_64".to_string(),
            enabled_features: Vec::new(),
            durable_format_version: 1,
            schema_version: 1,
            configuration_digest: "config".to_string(),
            deployment_profile: "crash-test".to_string(),
            dataset_fingerprint: "crash-matrix-v1".to_string(),
            canonical_graph_commit_epoch: 1,
            policy_version: crate::PRODUCTION_QUALIFICATION_POLICY_VERSION,
        };
        let case = StorageCrashCaseEvidence {
            point: StorageCrashPoint::BeforeWalAppend,
            repetition: 0,
            process_terminated: true,
            recovered_batch_present: false,
            whole_batch_recovered: true,
            commit_epoch: 1,
            recovered_commit_epoch: 1,
            replay_lsn_present: true,
            relationship_endpoints_valid: true,
            projection_watermark_valid: true,
            artifact_generation_valid: true,
        };
        let report = StorageCrashRecoveryEvidence::evaluate(
            ProductionEvidenceBinding {
                identity: identity.clone(),
                generated_at_unix_seconds: 1,
            },
            identity,
            2,
            vec![case.clone(), case],
        );

        assert!(!report.ready);
        assert!(report
            .blocker_codes
            .contains(&"before_wal_append_repetition_duplicate".to_string()));
        assert!(report
            .blocker_codes
            .contains(&"before_wal_append_coverage_incomplete".to_string()));
    }
}
