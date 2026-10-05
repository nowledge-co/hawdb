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

//! Serializable contracts for derived-artifact repair.
//!
//! Repair execution belongs to the embedded store, while these data contracts
//! describe durable audit records and the resource envelope shared by planning,
//! reopening, and repair application.

use crate::{
    canonical_adjacency::CanonicalAdjacencyConfig,
    config::{
        DEFAULT_MAX_GRAPH_MANIFEST_OPEN_BYTES, DEFAULT_MAX_WAL_REPLAY_BYTES,
        DEFAULT_MAX_WAL_REPLAY_ENTRIES,
    },
    property_projection::PersistentPropertyProjectionConfig,
};
use hawdb_core::{HawDBError, Result};
use serde::{Deserialize, Serialize};
use std::num::{NonZeroU64, NonZeroUsize};

pub const DERIVED_ARTIFACT_REPAIR_PROTOCOL: &str = "hawdb-derived-artifact-repair-v2";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DerivedArtifactKind {
    CanonicalAdjacency,
    PersistentPropertyProjection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DerivedArtifactHealthState {
    Healthy,
    RepairRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivedArtifactHealth {
    pub kind: DerivedArtifactKind,
    pub state: DerivedArtifactHealthState,
    pub reason_code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivedArtifactHealthReport {
    pub protocol: String,
    pub checkpoint_generation: u64,
    pub checkpoint_commit_epoch: u64,
    pub canonical_source_validated: bool,
    pub source_node_count: u64,
    pub source_relationship_count: u64,
    pub source_logical_bytes: u64,
    pub artifacts: Vec<DerivedArtifactHealth>,
    pub repair_required: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivedArtifactRebuildOptions {
    pub max_source_records: u64,
    pub max_source_logical_bytes: u64,
    pub max_temporary_bytes: u64,
    pub build_memory_bytes: u64,
    pub max_spill_runs: usize,
    pub max_generated_property_entries: u64,
    pub segment_cache_capacity_bytes: u64,
    pub max_graph_manifest_open_bytes: u64,
    pub max_wal_replay_bytes: u64,
    pub max_wal_replay_entries: usize,
}

impl Default for DerivedArtifactRebuildOptions {
    fn default() -> Self {
        Self {
            max_source_records: 100_000_000,
            max_source_logical_bytes: 1024 * 1024 * 1024 * 1024,
            max_temporary_bytes: 1024 * 1024 * 1024 * 1024,
            build_memory_bytes: 64 * 1024 * 1024,
            max_spill_runs: 4_096,
            max_generated_property_entries: 100_000_000,
            segment_cache_capacity_bytes: 64 * 1024 * 1024,
            max_graph_manifest_open_bytes: DEFAULT_MAX_GRAPH_MANIFEST_OPEN_BYTES,
            max_wal_replay_bytes: DEFAULT_MAX_WAL_REPLAY_BYTES,
            max_wal_replay_entries: DEFAULT_MAX_WAL_REPLAY_ENTRIES,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivedArtifactBranchSource {
    #[serde(with = "uuid_identity")]
    pub project_id: hawdb_core::Uuid,
    #[serde(with = "uuid_identity")]
    pub branch_id: hawdb_core::Uuid,
    pub metadata_revision: u64,
    pub head_len: u64,
    pub head_crc32c: u64,
    pub head_sha256: String,
}

mod uuid_identity {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        value: &hawdb_core::Uuid,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.collect_str(value)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<hawdb_core::Uuid, D::Error> {
        let text = String::deserialize(deserializer)?;
        let value = text
            .parse::<hawdb_core::Uuid>()
            .map_err(serde::de::Error::custom)?;
        if value.is_nil() || value.to_string() != text {
            return Err(serde::de::Error::custom(
                "repair UUID is not canonical and non-nil",
            ));
        }
        Ok(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivedArtifactRepairPlan {
    pub protocol: String,
    pub plan_id: String,
    pub branch: Option<DerivedArtifactBranchSource>,
    pub source_generation: u64,
    pub target_generation: u64,
    pub source_commit_epoch: u64,
    pub manifest_len: u64,
    pub manifest_crc32c: u64,
    pub manifest_sha256: String,
    pub wal_len: u64,
    pub wal_crc32c: u64,
    pub wal_sha256: String,
    pub source_node_count: u64,
    pub source_relationship_count: u64,
    pub source_logical_bytes: u64,
    pub estimated_temporary_bytes: u64,
    pub targets: Vec<DerivedArtifactKind>,
    pub options: DerivedArtifactRebuildOptions,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivedArtifactRepairReport {
    pub protocol: String,
    pub plan_id: String,
    pub source_generation: u64,
    pub published_generation: u64,
    pub source_commit_epoch: u64,
    pub targets: Vec<DerivedArtifactKind>,
    pub quarantined_files: Vec<String>,
    pub repair_record_file: String,
    pub resumed_interrupted_repair: bool,
}

// Internal ownership seams; the embedded facade does not re-export these helpers.
#[doc(hidden)]
pub fn validate_options(options: DerivedArtifactRebuildOptions) -> Result<()> {
    if options.max_source_records == 0
        || options.max_source_logical_bytes == 0
        || options.max_temporary_bytes == 0
        || options.build_memory_bytes == 0
        || options.max_spill_runs == 0
        || options.max_generated_property_entries == 0
        || options.segment_cache_capacity_bytes == 0
        || options.max_graph_manifest_open_bytes == 0
        || options.max_wal_replay_bytes == 0
        || options.max_wal_replay_entries == 0
    {
        return Err(HawDBError::Storage(
            "derived artifact rebuild limits must all be non-zero".to_string(),
        ));
    }
    Ok(())
}

#[doc(hidden)]
pub fn validate_plan(plan: &DerivedArtifactRepairPlan) -> Result<()> {
    validate_options(plan.options)?;
    if plan.protocol != DERIVED_ARTIFACT_REPAIR_PROTOCOL
        || plan.plan_id != plan_identity(plan)
        || plan.targets.is_empty()
        || plan.targets.windows(2).any(|pair| pair[0] >= pair[1])
        || plan.manifest_len == 0
        || plan
            .manifest_sha256
            .parse::<hawdb_integrity::Sha256Digest>()
            .is_err()
        || plan
            .wal_sha256
            .parse::<hawdb_integrity::Sha256Digest>()
            .is_err()
        || match &plan.branch {
            None => plan.source_generation.checked_add(1) != Some(plan.target_generation),
            Some(branch) => {
                plan.target_generation <= plan.source_generation
                    || branch.project_id.is_nil()
                    || branch.branch_id.is_nil()
                    || branch.project_id == branch.branch_id
                    || branch.metadata_revision == 0
                    || branch.head_len == 0
                    || branch
                        .head_sha256
                        .parse::<hawdb_integrity::Sha256Digest>()
                        .is_err()
            }
        }
    {
        return Err(HawDBError::Storage(
            "derived artifact repair plan identity is invalid".to_string(),
        ));
    }
    Ok(())
}

#[doc(hidden)]
pub fn plan_identity(plan: &DerivedArtifactRepairPlan) -> String {
    let encoded = serde_json::to_vec(&(
        &plan.protocol,
        &plan.branch,
        (
            plan.source_generation,
            plan.target_generation,
            plan.source_commit_epoch,
            plan.manifest_len,
            plan.manifest_crc32c,
            &plan.manifest_sha256,
            plan.wal_len,
            plan.wal_crc32c,
            &plan.wal_sha256,
            plan.source_node_count,
            plan.source_relationship_count,
            plan.source_logical_bytes,
            plan.estimated_temporary_bytes,
            &plan.targets,
            plan.options,
        ),
    ))
    .expect("derived repair plan identity fields are serializable");
    hawdb_integrity::integrity_digest(&encoded)
        .sha256
        .to_string()
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DerivedArtifactBuildConfig {
    pub adjacency: CanonicalAdjacencyConfig,
    pub property_projection: PersistentPropertyProjectionConfig,
}

pub fn build_config(options: DerivedArtifactRebuildOptions) -> Result<DerivedArtifactBuildConfig> {
    let memory = NonZeroU64::new(options.build_memory_bytes)
        .ok_or_else(|| HawDBError::Storage("derived repair memory limit is zero".to_string()))?;
    let spill = NonZeroU64::new(options.max_temporary_bytes)
        .ok_or_else(|| HawDBError::Storage("derived repair spill limit is zero".to_string()))?;
    let spill_runs = NonZeroUsize::new(options.max_spill_runs)
        .ok_or_else(|| HawDBError::Storage("derived repair spill run limit is zero".to_string()))?;
    let generated = NonZeroU64::new(options.max_generated_property_entries).ok_or_else(|| {
        HawDBError::Storage("derived repair generated entry limit is zero".to_string())
    })?;
    let adjacency = CanonicalAdjacencyConfig {
        memory_budget_bytes: memory,
        max_spill_bytes: spill,
        max_spill_runs: spill_runs,
        ..CanonicalAdjacencyConfig::default()
    };
    let property_projection = PersistentPropertyProjectionConfig {
        memory_budget_bytes: memory,
        max_spill_bytes: spill,
        max_spill_runs: spill_runs,
        max_generated_entries: generated,
        ..PersistentPropertyProjectionConfig::default()
    };
    Ok(DerivedArtifactBuildConfig {
        adjacency,
        property_projection,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_options_preserve_nonzero_resource_limits() {
        let options = DerivedArtifactRebuildOptions::default();
        validate_options(options).unwrap();
        assert_eq!(options.max_source_records, 100_000_000);
        assert_eq!(options.max_wal_replay_bytes, DEFAULT_MAX_WAL_REPLAY_BYTES);
        assert_eq!(
            options.max_graph_manifest_open_bytes,
            DEFAULT_MAX_GRAPH_MANIFEST_OPEN_BYTES
        );
    }

    fn repair_plan() -> DerivedArtifactRepairPlan {
        let mut plan = DerivedArtifactRepairPlan {
            protocol: DERIVED_ARTIFACT_REPAIR_PROTOCOL.to_string(),
            plan_id: String::new(),
            branch: None,
            source_generation: 4,
            target_generation: 5,
            source_commit_epoch: 7,
            manifest_len: 11,
            manifest_crc32c: 13,
            manifest_sha256: hawdb_integrity::integrity_digest(b"manifest")
                .sha256
                .to_string(),
            wal_len: 17,
            wal_crc32c: 19,
            wal_sha256: hawdb_integrity::integrity_digest(b"wal").sha256.to_string(),
            source_node_count: 23,
            source_relationship_count: 29,
            source_logical_bytes: 31,
            estimated_temporary_bytes: 37,
            targets: vec![DerivedArtifactKind::CanonicalAdjacency],
            options: DerivedArtifactRebuildOptions::default(),
        };
        plan.plan_id = plan_identity(&plan);
        plan
    }

    fn branch_repair_plan() -> DerivedArtifactRepairPlan {
        let mut plan = repair_plan();
        plan.branch = Some(DerivedArtifactBranchSource {
            project_id: "12345678-1234-1234-1234-123456789abc".parse().unwrap(),
            branch_id: "22345678-1234-1234-1234-123456789abc".parse().unwrap(),
            metadata_revision: 3,
            head_len: 197,
            head_crc32c: 43,
            head_sha256: hawdb_integrity::integrity_digest(b"branch head")
                .sha256
                .to_string(),
        });
        plan.target_generation = 11;
        plan.plan_id = plan_identity(&plan);
        plan
    }

    #[test]
    fn plan_identity_detects_contract_mutation() {
        let mut plan = repair_plan();
        validate_plan(&plan).unwrap();

        plan.estimated_temporary_bytes = 41;
        assert!(validate_plan(&plan).is_err());
    }

    #[test]
    fn branch_plan_round_trips_sparse_generations_and_uuid_scope() {
        let plan = branch_repair_plan();
        validate_plan(&plan).unwrap();
        let encoded = serde_json::to_vec(&plan).unwrap();
        let decoded: DerivedArtifactRepairPlan = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, plan);
        validate_plan(&decoded).unwrap();
        let mut legacy = plan;
        legacy.branch = None;
        legacy.plan_id = plan_identity(&legacy);
        assert!(validate_plan(&legacy).is_err());
    }

    #[test]
    fn branch_plan_rejects_scope_and_head_tampering() {
        let original = branch_repair_plan();
        for field in [
            "project_id",
            "branch_id",
            "metadata_revision",
            "head_len",
            "head_crc32c",
            "head_sha256",
        ] {
            let mut value = serde_json::to_value(&original).unwrap();
            value["branch"][field] = match field {
                "project_id" | "branch_id" => {
                    serde_json::json!("32345678-1234-1234-1234-123456789abc")
                }
                "head_sha256" => {
                    serde_json::json!(hawdb_integrity::integrity_digest(b"changed head")
                        .sha256
                        .to_string())
                }
                _ => serde_json::json!(47),
            };
            let changed: DerivedArtifactRepairPlan = serde_json::from_value(value).unwrap();
            assert!(validate_plan(&changed).is_err(), "{field}");
        }
    }

    #[test]
    fn branch_plan_rejects_noncanonical_uuid_and_invalid_generation() {
        let original = branch_repair_plan();
        for text in [
            "00000000-0000-0000-0000-000000000000",
            "22345678-1234-1234-1234-123456789ABC",
            "not-a-uuid",
        ] {
            let mut value = serde_json::to_value(&original).unwrap();
            value["branch"]["branch_id"] = serde_json::json!(text);
            assert!(
                serde_json::from_value::<DerivedArtifactRepairPlan>(value).is_err(),
                "{text}"
            );
        }
        for target in [
            0,
            original.source_generation - 1,
            original.source_generation,
        ] {
            let mut plan = original.clone();
            plan.target_generation = target;
            plan.plan_id = plan_identity(&plan);
            assert!(validate_plan(&plan).is_err());
        }
        let mut plan = repair_plan();
        plan.source_generation = u64::MAX;
        plan.target_generation = u64::MAX;
        plan.plan_id = plan_identity(&plan);
        assert!(validate_plan(&plan).is_err());
    }
}
