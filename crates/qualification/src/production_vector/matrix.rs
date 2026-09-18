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

use super::ProductionVectorQualificationReport;
use hawdb::ProductionQualificationIdentity;
use std::collections::BTreeSet;

pub const PRODUCTION_VECTOR_QUALIFICATION_MATRIX_PROTOCOL: &str =
    "hawdb-production-vector-qualification-matrix-v1";
const REQUIRED_TARGETS: [(&str, &str); 4] = [
    ("linux", "aarch64"),
    ("linux", "x86_64"),
    ("macos", "aarch64"),
    ("windows", "x86_64"),
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionVectorMatrixExpectation {
    pub source_revision: String,
    pub rust_toolchain: String,
    pub enabled_features: Vec<String>,
    pub durable_format_version: u64,
    pub schema_version: u64,
    pub configuration_digest: String,
    pub deployment_profile: String,
    pub dataset_fingerprint: String,
    pub canonical_graph_commit_epoch: u64,
    pub policy_version: u64,
}

impl ProductionVectorMatrixExpectation {
    pub fn from_identity(identity: &ProductionQualificationIdentity) -> Self {
        Self {
            source_revision: identity.source_revision.clone(),
            rust_toolchain: identity.rust_toolchain.clone(),
            enabled_features: identity.enabled_features.clone(),
            durable_format_version: identity.durable_format_version,
            schema_version: identity.schema_version,
            configuration_digest: identity.configuration_digest.clone(),
            deployment_profile: identity.deployment_profile.clone(),
            dataset_fingerprint: identity.dataset_fingerprint.clone(),
            canonical_graph_commit_epoch: identity.canonical_graph_commit_epoch,
            policy_version: identity.policy_version,
        }
    }

    fn matches(&self, identity: &ProductionQualificationIdentity) -> bool {
        self == &Self::from_identity(identity)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionVectorQualificationMatrixReport {
    pub protocol: String,
    pub ready: bool,
    pub blocker_codes: Vec<String>,
    pub expectation: ProductionVectorMatrixExpectation,
    pub reports: Vec<ProductionVectorQualificationReport>,
}

impl ProductionVectorQualificationMatrixReport {
    pub fn evaluate(
        reports: Vec<ProductionVectorQualificationReport>,
        expectation: ProductionVectorMatrixExpectation,
    ) -> Self {
        let mut blocker_codes = Vec::new();
        let mut targets = BTreeSet::new();
        let mut projection_identity = None;
        for report in &reports {
            let identity = &report.expected_identity;
            if !targets.insert((identity.target_os.clone(), identity.target_arch.clone())) {
                blocker_codes.push("duplicate_target_evidence".to_string());
            }
            if !report.ready || !report.recompute_blocker_codes().is_empty() {
                blocker_codes.push("target_not_ready".to_string());
            }
            if !expectation.matches(identity) {
                blocker_codes.push("release_identity_mismatch".to_string());
            }
            if !report
                .query_evidence
                .iter()
                .all(|evidence| evidence.auto_scalar_candidate_parity)
            {
                blocker_codes.push("scalar_reference_parity_failed".to_string());
            }
            match &projection_identity {
                Some(expected) if expected != &report.projection_identity => {
                    blocker_codes.push("projection_identity_mismatch".to_string());
                }
                None => projection_identity = Some(report.projection_identity.clone()),
                Some(_) => {}
            }
        }
        for (target_os, target_arch) in REQUIRED_TARGETS {
            if !targets.contains(&(target_os.to_string(), target_arch.to_string())) {
                blocker_codes.push(format!("missing_target_{target_os}_{target_arch}"));
            }
        }
        blocker_codes.sort();
        blocker_codes.dedup();
        Self {
            protocol: PRODUCTION_VECTOR_QUALIFICATION_MATRIX_PROTOCOL.to_string(),
            ready: blocker_codes.is_empty(),
            blocker_codes,
            expectation,
            reports,
        }
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "ready": self.ready,
            "blocker_codes": self.blocker_codes,
            "expectation": {
                "source_revision": self.expectation.source_revision,
                "rust_toolchain": self.expectation.rust_toolchain,
                "enabled_features": self.expectation.enabled_features,
                "durable_format_version": self.expectation.durable_format_version,
                "schema_version": self.expectation.schema_version,
                "configuration_digest": self.expectation.configuration_digest,
                "deployment_profile": self.expectation.deployment_profile,
                "dataset_fingerprint": self.expectation.dataset_fingerprint,
                "canonical_graph_commit_epoch": self.expectation.canonical_graph_commit_epoch,
                "policy_version": self.expectation.policy_version,
            },
            "reports": self.reports.iter().map(ProductionVectorQualificationReport::json).collect::<Vec<_>>(),
        })
    }
}
