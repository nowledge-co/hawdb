//! Serializable contracts for derived-artifact repair.
//!
//! Repair execution belongs to the embedded store, while these data contracts
//! describe durable audit records and the resource envelope shared by planning,
//! reopening, and repair application.

use crate::{
    DEFAULT_MAX_GRAPH_MANIFEST_OPEN_BYTES, DEFAULT_MAX_WAL_REPLAY_BYTES,
    DEFAULT_MAX_WAL_REPLAY_ENTRIES,
};
use serde::{Deserialize, Serialize};
use skein_core::{Result, SkeinError};

pub const DERIVED_ARTIFACT_REPAIR_PROTOCOL: &str = "skein-derived-artifact-repair-v1";

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
pub struct DerivedArtifactRepairPlan {
    pub protocol: String,
    pub plan_id: String,
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
        return Err(SkeinError::Storage(
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
        || plan.target_generation != plan.source_generation.saturating_add(1)
    {
        return Err(SkeinError::Storage(
            "derived artifact repair plan identity is invalid".to_string(),
        ));
    }
    Ok(())
}

#[doc(hidden)]
pub fn plan_identity(plan: &DerivedArtifactRepairPlan) -> String {
    let encoded = serde_json::to_vec(&(
        &plan.protocol,
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
    ))
    .expect("derived repair plan identity fields are serializable");
    skein_integrity::integrity_digest(&encoded)
        .sha256
        .to_string()
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

    #[test]
    fn plan_identity_detects_contract_mutation() {
        let mut plan = DerivedArtifactRepairPlan {
            protocol: DERIVED_ARTIFACT_REPAIR_PROTOCOL.to_string(),
            plan_id: String::new(),
            source_generation: 4,
            target_generation: 5,
            source_commit_epoch: 7,
            manifest_len: 11,
            manifest_crc32c: 13,
            manifest_sha256: "manifest".to_string(),
            wal_len: 17,
            wal_crc32c: 19,
            wal_sha256: "wal".to_string(),
            source_node_count: 23,
            source_relationship_count: 29,
            source_logical_bytes: 31,
            estimated_temporary_bytes: 37,
            targets: vec![DerivedArtifactKind::CanonicalAdjacency],
            options: DerivedArtifactRebuildOptions::default(),
        };
        plan.plan_id = plan_identity(&plan);
        validate_plan(&plan).unwrap();

        plan.estimated_temporary_bytes = 41;
        assert!(validate_plan(&plan).is_err());
    }
}
