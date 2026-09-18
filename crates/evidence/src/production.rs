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

use hawdb_core::{HawDBError, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const PRODUCTION_QUALIFICATION_POLICY_VERSION: u64 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductionQualificationIdentity {
    pub source_revision: String,
    pub rust_toolchain: String,
    pub target_os: String,
    pub target_arch: String,
    pub enabled_features: Vec<String>,
    pub durable_format_version: u64,
    pub schema_version: u64,
    pub configuration_digest: String,
    pub deployment_profile: String,
    pub dataset_fingerprint: String,
    pub canonical_graph_commit_epoch: u64,
    pub policy_version: u64,
}

impl ProductionQualificationIdentity {
    pub fn validate(&self) -> Result<()> {
        let blockers = self.blocker_codes();
        if blockers.is_empty() {
            Ok(())
        } else {
            Err(HawDBError::Semantic(format!(
                "invalid production qualification identity: {}",
                blockers.join(",")
            )))
        }
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "source_revision": self.source_revision,
            "rust_toolchain": self.rust_toolchain,
            "target_os": self.target_os,
            "target_arch": self.target_arch,
            "enabled_features": self.enabled_features,
            "durable_format_version": self.durable_format_version,
            "schema_version": self.schema_version,
            "configuration_digest": self.configuration_digest,
            "deployment_profile": self.deployment_profile,
            "dataset_fingerprint": self.dataset_fingerprint,
            "canonical_graph_commit_epoch": self.canonical_graph_commit_epoch,
            "policy_version": self.policy_version,
        })
    }

    pub(crate) fn blocker_codes(&self) -> Vec<String> {
        let mut blockers = Vec::new();
        for (name, value) in [
            ("source_revision", self.source_revision.as_str()),
            ("rust_toolchain", self.rust_toolchain.as_str()),
            ("target_os", self.target_os.as_str()),
            ("target_arch", self.target_arch.as_str()),
            ("configuration_digest", self.configuration_digest.as_str()),
            ("deployment_profile", self.deployment_profile.as_str()),
            ("dataset_fingerprint", self.dataset_fingerprint.as_str()),
        ] {
            if value.trim().is_empty() {
                blockers.push(format!("{name}_missing"));
            }
        }
        if !matches!(self.target_os.as_str(), "linux" | "macos" | "windows") {
            blockers.push("target_os_unsupported".to_string());
        }
        if self.durable_format_version == 0 {
            blockers.push("durable_format_version_missing".to_string());
        }
        if self.schema_version == 0 {
            blockers.push("schema_version_missing".to_string());
        }
        if self.policy_version != PRODUCTION_QUALIFICATION_POLICY_VERSION {
            blockers.push("policy_version_mismatch".to_string());
        }
        let features = self.enabled_features.iter().collect::<BTreeSet<_>>();
        if features.len() != self.enabled_features.len()
            || self
                .enabled_features
                .windows(2)
                .any(|pair| pair[0] > pair[1])
            || self
                .enabled_features
                .iter()
                .any(|feature| feature.trim().is_empty())
        {
            blockers.push("enabled_features_not_canonical".to_string());
        }
        blockers
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductionEvidenceBinding {
    pub identity: ProductionQualificationIdentity,
    pub generated_at_unix_seconds: u64,
}

impl ProductionEvidenceBinding {
    pub fn validate_for(&self, expected: &ProductionQualificationIdentity) -> Result<()> {
        let blockers = self.blocker_codes_for(expected);
        if blockers.is_empty() {
            Ok(())
        } else {
            Err(HawDBError::Semantic(format!(
                "production evidence identity mismatch: {}",
                blockers.join(",")
            )))
        }
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "identity": self.identity.json(),
            "generated_at_unix_seconds": self.generated_at_unix_seconds,
        })
    }

    pub(crate) fn blocker_codes_for(
        &self,
        expected: &ProductionQualificationIdentity,
    ) -> Vec<String> {
        let mut blockers = self
            .identity
            .blocker_codes()
            .into_iter()
            .map(|code| format!("evidence_{code}"))
            .collect::<Vec<_>>();
        blockers.extend(
            expected
                .blocker_codes()
                .into_iter()
                .map(|code| format!("expected_{code}")),
        );
        if self.generated_at_unix_seconds == 0 {
            blockers.push("evidence_generation_time_missing".to_string());
        }
        if self.identity != *expected {
            blockers.push("evidence_identity_mismatch".to_string());
        }
        blockers
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(revision: &str) -> ProductionQualificationIdentity {
        ProductionQualificationIdentity {
            source_revision: revision.to_string(),
            rust_toolchain: "rustc 1.90.0".to_string(),
            target_os: "linux".to_string(),
            target_arch: "x86_64".to_string(),
            enabled_features: vec!["full-text-search".to_string(), "vector-search".to_string()],
            durable_format_version: 1,
            schema_version: 1,
            configuration_digest: "config-01".to_string(),
            deployment_profile: "production-replica".to_string(),
            dataset_fingerprint: "dataset-01".to_string(),
            canonical_graph_commit_epoch: 42,
            policy_version: PRODUCTION_QUALIFICATION_POLICY_VERSION,
        }
    }

    #[test]
    fn evidence_binding_rejects_stale_revision() {
        let binding = ProductionEvidenceBinding {
            identity: identity("revision-a"),
            generated_at_unix_seconds: 1,
        };

        binding.validate_for(&identity("revision-a")).unwrap();
        let error = binding.validate_for(&identity("revision-b")).unwrap_err();
        assert!(error.to_string().contains("evidence_identity_mismatch"));
    }

    #[test]
    fn identity_requires_canonical_feature_order() {
        let mut identity = identity("revision-a");
        identity.enabled_features.reverse();

        let error = identity.validate().unwrap_err();
        assert!(error.to_string().contains("enabled_features_not_canonical"));
    }
}
