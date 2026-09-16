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

impl DerivedArtifactRebuildOptions {
    pub fn validate(self) -> Result<()> {
        if self.max_source_records == 0
            || self.max_source_logical_bytes == 0
            || self.max_temporary_bytes == 0
            || self.build_memory_bytes == 0
            || self.max_spill_runs == 0
            || self.max_generated_property_entries == 0
            || self.segment_cache_capacity_bytes == 0
            || self.max_graph_manifest_open_bytes == 0
            || self.max_wal_replay_bytes == 0
            || self.max_wal_replay_entries == 0
        {
            return Err(SkeinError::Storage(
                "derived artifact rebuild limits must all be non-zero".to_string(),
            ));
        }
        Ok(())
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

impl DerivedArtifactRepairPlan {
    pub fn refresh_identity(&mut self) {
        self.plan_id = self.identity();
    }

    pub fn validate(&self) -> Result<()> {
        self.options.validate()?;
        if self.protocol != DERIVED_ARTIFACT_REPAIR_PROTOCOL
            || self.plan_id != self.identity()
            || self.targets.is_empty()
            || self.target_generation != self.source_generation.saturating_add(1)
        {
            return Err(SkeinError::Storage(
                "derived artifact repair plan identity is invalid".to_string(),
            ));
        }
        Ok(())
    }

    fn identity(&self) -> String {
        let encoded = serde_json::to_vec(&(
            &self.protocol,
            self.source_generation,
            self.target_generation,
            self.source_commit_epoch,
            self.manifest_len,
            self.manifest_crc32c,
            &self.manifest_sha256,
            self.wal_len,
            self.wal_crc32c,
            &self.wal_sha256,
            self.source_node_count,
            self.source_relationship_count,
            self.source_logical_bytes,
            self.estimated_temporary_bytes,
            &self.targets,
            self.options,
        ))
        .expect("derived repair plan identity fields are serializable");
        skein_integrity::integrity_digest(&encoded)
            .sha256
            .to_string()
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_options_preserve_nonzero_resource_limits() {
        let options = DerivedArtifactRebuildOptions::default();
        options.validate().unwrap();
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
        plan.refresh_identity();
        plan.validate().unwrap();

        plan.estimated_temporary_bytes = 41;
        assert!(plan.validate().is_err());
    }
}
