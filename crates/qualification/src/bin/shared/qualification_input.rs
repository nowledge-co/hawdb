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

use hawdb::{ProductionEvidenceBinding, ProductionQualificationIdentity};
use serde::Deserialize;
use std::fs::File;
use std::io::Read;
use std::path::Path;

const MAX_PLAN_BYTES: u64 = 32 * 1024 * 1024;

pub(crate) fn read_bounded_json<T: serde::de::DeserializeOwned>(
    path: &Path,
    kind: &str,
) -> Result<T, String> {
    let file = File::open(path).map_err(|error| format!("failed to open {kind}: {error}"))?;
    let mut bytes = Vec::new();
    file.take(MAX_PLAN_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|error| format!("failed to read {kind}: {error}"))?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_PLAN_BYTES {
        return Err(format!("{kind} exceeds {MAX_PLAN_BYTES} bytes"));
    }
    serde_json::from_slice(&bytes).map_err(|error| format!("invalid {kind}: {error}"))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EvidenceBindingInput {
    identity: ProductionIdentityInput,
    generated_at_unix_seconds: u64,
}

impl From<EvidenceBindingInput> for ProductionEvidenceBinding {
    fn from(input: EvidenceBindingInput) -> Self {
        Self {
            identity: input.identity.into(),
            generated_at_unix_seconds: input.generated_at_unix_seconds,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProductionIdentityInput {
    source_revision: String,
    rust_toolchain: String,
    target_os: String,
    target_arch: String,
    enabled_features: Vec<String>,
    durable_format_version: u64,
    schema_version: u64,
    configuration_digest: String,
    deployment_profile: String,
    dataset_fingerprint: String,
    canonical_graph_commit_epoch: u64,
    policy_version: u64,
}

impl From<ProductionIdentityInput> for ProductionQualificationIdentity {
    fn from(input: ProductionIdentityInput) -> Self {
        Self {
            source_revision: input.source_revision,
            rust_toolchain: input.rust_toolchain,
            target_os: input.target_os,
            target_arch: input.target_arch,
            enabled_features: input.enabled_features,
            durable_format_version: input.durable_format_version,
            schema_version: input.schema_version,
            configuration_digest: input.configuration_digest,
            deployment_profile: input.deployment_profile,
            dataset_fingerprint: input.dataset_fingerprint,
            canonical_graph_commit_epoch: input.canonical_graph_commit_epoch,
            policy_version: input.policy_version,
        }
    }
}
