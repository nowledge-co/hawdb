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

//! Canonical graph snapshot and initial-import protocol contracts.
//!
//! This crate owns the portable bootstrap stream format, its validation, and
//! import-readiness decisions. The embedded facade remains responsible for
//! extracting a live graph snapshot from its `GraphStore`.

use hawdb_core::{HawDBError, Result, Value};
use hawdb_search::{SearchProjectionDelta, SearchProjectionFreshness, SearchProjectionKind};
use hawdb_storage::{
    decode_relational_checkpoint, encode_relational_checkpoint, RelationalDecodeLimits,
    RelationalState, StoreStableIdMapping,
};
use std::collections::{BTreeMap, BTreeSet};

#[doc(hidden)]
pub mod developer_json;
#[doc(hidden)]
pub mod developer_staging;
#[doc(hidden)]
pub mod developer_staging_gc;
#[doc(hidden)]
pub mod developer_staging_import;
#[doc(hidden)]
pub mod developer_staging_publish;
#[doc(hidden)]
pub mod developer_staging_verification;

#[doc(hidden)]
pub use developer_json::{
    endpoint_violations_json, hawdb_lightning_bootstrap_bundle_json,
    hawdb_lightning_bootstrap_bundle_json_with_optional_storage_recovery,
    hawdb_lightning_bootstrap_manifest_json, hawdb_lightning_graph_stream_validation_json,
    hawdb_lightning_relational_stream_validation_json, stable_identity_audit_json,
};
#[doc(hidden)]
pub use developer_staging::{
    hawdb_lightning_artifact_summary, stage_hawdb_lightning_bootstrap_export,
    stage_hawdb_lightning_bootstrap_export_with_optional_storage_recovery,
    sync_bootstrap_directory, write_bootstrap_atomic_file,
    HAWDB_LIGHTNING_STAGING_CATALOG_PROTOCOL_VERSION,
};
#[doc(hidden)]
pub use developer_staging_gc::hawdb_lightning_gc_staging_report;
#[doc(hidden)]
pub use developer_staging_import::{
    hawdb_lightning_import_state_marker, hawdb_lightning_import_status,
};
#[doc(hidden)]
pub use developer_staging_publish::{
    publish_hawdb_lightning_staging_catalog, publish_hawdb_lightning_staging_catalog_with_options,
    HawDBLightningPublishOptions,
};
#[doc(hidden)]
pub use developer_staging_verification::{
    read_hawdb_lightning_staging_artifact_json, verify_hawdb_lightning_published_manifest,
    verify_hawdb_lightning_staging_catalog,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalGraphSnapshotExport {
    pub graph_commit_epoch: u64,
    pub logical_checksum: u64,
    pub stable_identity: CanonicalSnapshotIdentityAudit,
    pub nodes: Vec<CanonicalSnapshotNode>,
    pub relationships: Vec<CanonicalSnapshotRelationship>,
}

impl CanonicalGraphSnapshotExport {
    /// Build a canonical snapshot at a host-owned source watermark.
    ///
    /// Importers use this at the boundary where a foreign graph has already
    /// been read consistently. Keeping checksum and stable-identity derivation
    /// here prevents each host integration from reimplementing that contract.
    pub fn from_rows(
        graph_commit_epoch: u64,
        nodes: Vec<CanonicalSnapshotNode>,
        relationships: Vec<CanonicalSnapshotRelationship>,
    ) -> Self {
        let stable_identity = canonical_snapshot_identity_audit(&nodes, &relationships);
        let logical_checksum = canonical_graph_snapshot_checksum(&nodes, &relationships);
        Self {
            graph_commit_epoch,
            logical_checksum,
            stable_identity,
            nodes,
            relationships,
        }
    }

    pub fn with_stable_id_mapping(&self, mapping: &CanonicalStableIdMapping) -> Self {
        let mut export = self.clone();
        for node in &mut export.nodes {
            if let Some(stable_id) = mapping.node_stable_ids.get(&node.node_id) {
                node.stable_id = Some(stable_id.clone());
            }
        }
        for relationship in &mut export.relationships {
            if let Some(stable_id) = mapping
                .relationship_stable_ids
                .get(&relationship.relationship_id)
            {
                relationship.stable_id = Some(stable_id.clone());
            }
        }
        export.stable_identity =
            canonical_snapshot_identity_audit(&export.nodes, &export.relationships);
        export.logical_checksum =
            canonical_graph_snapshot_checksum(&export.nodes, &export.relationships);
        export
    }

    pub fn hawdb_lightning_bootstrap_manifest(
        &self,
        relational_stream: &HawDBLightningRelationalStream,
    ) -> HawDBLightningBootstrapManifest {
        let validation = self.validate();
        let graph_stream_body = encode_hawdb_lightning_graph_stream_body(self);
        let graph_stream_checksum = checksum_bytes(graph_stream_body.as_bytes());
        let graph_stream_byte_len =
            graph_stream_body.len() + format!("checksum\t{graph_stream_checksum}\n").len();
        let relational_validation = relational_stream.validate();
        let mut manifest = HawDBLightningBootstrapManifest {
            protocol_version: HAWDB_LIGHTNING_BOOTSTRAP_PROTOCOL_VERSION,
            database_commit_epoch: self.graph_commit_epoch,
            graph_commit_epoch: self.graph_commit_epoch,
            logical_checksum: self.logical_checksum,
            graph_stream_checksum,
            graph_stream_byte_len,
            relational_stream_format_version: relational_stream.format_version,
            relational_stream_checksum: relational_stream.stream_checksum,
            relational_stream_byte_len: relational_stream.byte_len,
            relational_table_count: relational_stream.table_count,
            relational_row_count: relational_stream.row_count,
            relational_overflow_segment_count: relational_stream.overflow_segment_count,
            schema_checksum: canonical_graph_snapshot_schema_checksum(
                &self.nodes,
                &self.relationships,
            ),
            node_count: self.nodes.len(),
            relationship_count: self.relationships.len(),
            label_count: self
                .nodes
                .iter()
                .flat_map(|node| node.labels.iter().cloned())
                .collect::<BTreeSet<_>>()
                .len(),
            relationship_type_count: self
                .relationships
                .iter()
                .map(|relationship| relationship.rel_type.clone())
                .collect::<BTreeSet<_>>()
                .len(),
            node_property_count: self.nodes.iter().map(|node| node.properties.len()).sum(),
            relationship_property_count: self
                .relationships
                .iter()
                .map(|relationship| relationship.properties.len())
                .sum(),
            validation,
            relational_validation,
        };
        manifest.relational_validation = relational_stream.validate_against_manifest(&manifest);
        manifest
    }

    pub fn hawdb_lightning_graph_stream(&self) -> HawDBLightningGraphStream {
        let body = encode_hawdb_lightning_graph_stream_body(self);
        let stream_checksum = checksum_bytes(body.as_bytes());
        let encoded = format!("{body}checksum\t{stream_checksum}\n");
        HawDBLightningGraphStream {
            format_version: HAWDB_LIGHTNING_GRAPH_STREAM_FORMAT_VERSION,
            graph_commit_epoch: self.graph_commit_epoch,
            logical_checksum: self.logical_checksum,
            stream_checksum,
            byte_len: encoded.len(),
            node_count: self.nodes.len(),
            relationship_count: self.relationships.len(),
            encoded,
        }
    }

    pub fn validate(&self) -> CanonicalGraphSnapshotValidation {
        let expected_logical_checksum =
            canonical_graph_snapshot_checksum(&self.nodes, &self.relationships);
        let expected_stable_identity =
            canonical_snapshot_identity_audit(&self.nodes, &self.relationships);
        let duplicate_node_ids = duplicate_u64s(self.nodes.iter().map(|node| node.node_id));
        let duplicate_relationship_ids = duplicate_u64s(
            self.relationships
                .iter()
                .map(|relationship| relationship.relationship_id),
        );
        let node_ids = self
            .nodes
            .iter()
            .map(|node| node.node_id)
            .collect::<BTreeSet<_>>();
        let missing_sources = self
            .relationships
            .iter()
            .filter(|relationship| !node_ids.contains(&relationship.source_node_id))
            .map(|relationship| CanonicalSnapshotEndpointViolation {
                relationship_id: relationship.relationship_id,
                missing_node_id: relationship.source_node_id,
            })
            .collect::<Vec<_>>();
        let missing_targets = self
            .relationships
            .iter()
            .filter(|relationship| !node_ids.contains(&relationship.target_node_id))
            .map(|relationship| CanonicalSnapshotEndpointViolation {
                relationship_id: relationship.relationship_id,
                missing_node_id: relationship.target_node_id,
            })
            .collect::<Vec<_>>();
        let checksum_matches = self.logical_checksum == expected_logical_checksum;
        let stable_identity_matches = self.stable_identity == expected_stable_identity;
        let stable_identity_ready = !expected_stable_identity.requires_stable_id_mapping;
        let is_valid = checksum_matches
            && stable_identity_matches
            && duplicate_node_ids.is_empty()
            && duplicate_relationship_ids.is_empty()
            && missing_sources.is_empty()
            && missing_targets.is_empty();
        let is_import_ready = is_valid && stable_identity_ready;
        CanonicalGraphSnapshotValidation {
            is_valid,
            is_import_ready,
            checksum_matches,
            expected_logical_checksum,
            stable_identity_matches,
            stable_identity_ready,
            expected_stable_identity,
            duplicate_node_ids,
            duplicate_relationship_ids,
            missing_sources,
            missing_targets,
        }
    }
}

impl HawDBLightningGraphStream {
    pub fn validate_against_manifest(
        &self,
        manifest: &HawDBLightningBootstrapManifest,
    ) -> HawDBLightningGraphStreamValidation {
        validate_hawdb_lightning_graph_stream(&self.encoded, Some(manifest))
    }
}

impl HawDBLightningRelationalStream {
    pub fn from_state(database_commit_epoch: u64, state: &RelationalState) -> Result<Self> {
        state
            .require_materialized_rows("HawDB Lightning relational export")
            .map_err(|error| HawDBError::Storage(error.to_string()))?;
        let encoded = encode_relational_checkpoint(database_commit_epoch, state)
            .map_err(|error| HawDBError::Storage(error.to_string()))?;
        let table_count = state.table_schemas().count();
        let row_count = state
            .table_schemas()
            .map(|schema| state.row_count(&schema.name))
            .sum();
        Ok(Self {
            format_version: HAWDB_LIGHTNING_RELATIONAL_STREAM_FORMAT_VERSION,
            database_commit_epoch,
            stream_checksum: checksum_bytes(&encoded),
            byte_len: encoded.len(),
            table_count,
            row_count,
            overflow_segment_count: state.overflow_segment_count(),
            encoded,
        })
    }

    pub fn validate(&self) -> HawDBLightningRelationalStreamValidation {
        validate_hawdb_lightning_relational_stream(&self.encoded, None)
    }

    pub fn validate_against_manifest(
        &self,
        manifest: &HawDBLightningBootstrapManifest,
    ) -> HawDBLightningRelationalStreamValidation {
        validate_hawdb_lightning_relational_stream(&self.encoded, Some(manifest))
    }
}

pub const HAWDB_LIGHTNING_BOOTSTRAP_PROTOCOL_VERSION: u64 = 1;
pub const HAWDB_LIGHTNING_GRAPH_STREAM_FORMAT_VERSION: u64 = 1;
pub const HAWDB_LIGHTNING_RELATIONAL_STREAM_FORMAT_VERSION: u64 = 1;
pub const HAWDB_LIGHTNING_INITIAL_IMPORT_DURABLE_STATE_PROTOCOL: &str =
    "hawdb-lightning-initial-import-durable-state-v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningBootstrapExport {
    pub snapshot: CanonicalGraphSnapshotExport,
    pub manifest: HawDBLightningBootstrapManifest,
    pub graph_stream: HawDBLightningGraphStream,
    pub relational_stream: HawDBLightningRelationalStream,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningBootstrapManifest {
    pub protocol_version: u64,
    pub database_commit_epoch: u64,
    pub graph_commit_epoch: u64,
    pub logical_checksum: u64,
    pub graph_stream_checksum: u64,
    pub graph_stream_byte_len: usize,
    pub relational_stream_format_version: u64,
    pub relational_stream_checksum: u64,
    pub relational_stream_byte_len: usize,
    pub relational_table_count: usize,
    pub relational_row_count: usize,
    pub relational_overflow_segment_count: usize,
    pub schema_checksum: u64,
    pub node_count: usize,
    pub relationship_count: usize,
    pub label_count: usize,
    pub relationship_type_count: usize,
    pub node_property_count: usize,
    pub relationship_property_count: usize,
    pub validation: CanonicalGraphSnapshotValidation,
    pub relational_validation: HawDBLightningRelationalStreamValidation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningGraphStream {
    pub format_version: u64,
    pub graph_commit_epoch: u64,
    pub logical_checksum: u64,
    pub stream_checksum: u64,
    pub byte_len: usize,
    pub node_count: usize,
    pub relationship_count: usize,
    pub encoded: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningRelationalStream {
    pub format_version: u64,
    pub database_commit_epoch: u64,
    pub stream_checksum: u64,
    pub byte_len: usize,
    pub table_count: usize,
    pub row_count: usize,
    pub overflow_segment_count: usize,
    pub encoded: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningRelationalStreamValidation {
    pub is_valid: bool,
    pub checksum_matches: bool,
    pub format_version_matches: bool,
    pub epoch_matches: bool,
    pub count_matches: bool,
    pub manifest_matches: bool,
    pub expected_stream_checksum: Option<u64>,
    pub actual_stream_checksum: u64,
    pub database_commit_epoch: Option<u64>,
    pub table_count: usize,
    pub row_count: usize,
    pub overflow_segment_count: usize,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningGraphStreamValidation {
    pub is_valid: bool,
    pub checksum_matches: bool,
    pub format_version_matches: bool,
    pub count_matches: bool,
    pub endpoint_integrity: bool,
    pub manifest_matches: bool,
    pub expected_stream_checksum: Option<u64>,
    pub actual_stream_checksum: u64,
    pub format_version: Option<u64>,
    pub graph_commit_epoch: Option<u64>,
    pub logical_checksum: Option<u64>,
    pub node_count: usize,
    pub relationship_count: usize,
    pub duplicate_node_ids: Vec<u64>,
    pub duplicate_relationship_ids: Vec<u64>,
    pub missing_sources: Vec<CanonicalSnapshotEndpointViolation>,
    pub missing_targets: Vec<CanonicalSnapshotEndpointViolation>,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningInitialImportReadiness {
    pub ready: bool,
    pub manifest_import_ready: bool,
    pub projection_present: bool,
    pub graph_import_caught_up: bool,
    pub projection_watermark_caught_up: bool,
    pub projection_checkpointed: bool,
    pub manifest_graph_commit_epoch: u64,
    pub target_graph_commit_epoch: u64,
    pub projection_source_graph_commit_epoch: Option<u64>,
    pub projection_durable_source_graph_commit_epoch: Option<u64>,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningInitialImportCheckpoint {
    pub protocol_version: u64,
    pub import_id: String,
    pub task_id: String,
    pub fencing_token: String,
    pub object_digest: String,
    pub schema_checksum: u64,
    pub graph_stream_checksum: u64,
    pub graph_stream_byte_len: usize,
    pub relational_stream_checksum: u64,
    pub relational_stream_byte_len: usize,
    pub manifest_database_commit_epoch: u64,
    pub manifest_graph_commit_epoch: u64,
    pub applied_graph_commit_epoch: u64,
    pub applied_search_projection_commit_epoch: Option<u64>,
    pub durable_search_projection_commit_epoch: Option<u64>,
    pub completed_batches: u64,
    pub total_batches: u64,
    pub document_identity_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningInitialImportIdempotencyKey {
    pub import_id: String,
    pub task_id: String,
    pub fencing_token: String,
    pub object_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningInitialImportCheckpointReadiness {
    pub ready: bool,
    pub idempotency_key_present: bool,
    pub idempotency_key: Option<HawDBLightningInitialImportIdempotencyKey>,
    pub checkpoint_matches_manifest: bool,
    pub graph_checkpoint_caught_up: bool,
    pub search_projection_applied_caught_up: bool,
    pub search_projection_durable_caught_up: bool,
    pub batches_complete: bool,
    pub document_identities_present: bool,
    pub manifest_graph_commit_epoch: u64,
    pub applied_graph_commit_epoch: u64,
    pub applied_search_projection_commit_epoch: Option<u64>,
    pub durable_search_projection_commit_epoch: Option<u64>,
    pub completed_batches: u64,
    pub total_batches: u64,
    pub document_identity_count: usize,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningInitialImportCheckpointProgress {
    pub applied_graph_commit_epoch: u64,
    pub applied_search_projection_commit_epoch: Option<u64>,
    pub durable_search_projection_commit_epoch: Option<u64>,
    pub completed_batches: u64,
    pub total_batches: u64,
    pub document_identity_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningInitialImportCheckpointProgressReport {
    pub accepted: bool,
    pub checkpoint: HawDBLightningInitialImportCheckpoint,
    pub readiness: HawDBLightningInitialImportCheckpointReadiness,
    pub resume_action: HawDBLightningInitialImportResumeAction,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningInitialImportDocumentIdentity {
    pub kind: SearchProjectionKind,
    pub document_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningInitialImportDocumentIdentityKindReport {
    pub kind: SearchProjectionKind,
    pub document_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningInitialImportDocumentIdentityCoverage {
    pub ready: bool,
    pub document_identity_count: usize,
    pub unique_document_identity_count: usize,
    pub expected_kinds: Vec<SearchProjectionKind>,
    pub observed_kinds: Vec<SearchProjectionKind>,
    pub kind_reports: Vec<HawDBLightningInitialImportDocumentIdentityKindReport>,
    pub missing_kinds: Vec<SearchProjectionKind>,
    pub duplicate_document_ids: Vec<String>,
    pub empty_document_id_count: usize,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HawDBLightningInitialImportResumeActionKind {
    Start,
    Resume,
    ReadyForCutover,
    Quarantine,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningInitialImportResumeAction {
    pub kind: HawDBLightningInitialImportResumeActionKind,
    pub next_batch: Option<u64>,
    pub idempotency_key: Option<HawDBLightningInitialImportIdempotencyKey>,
    pub completed_batches: u64,
    pub total_batches: u64,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningInitialImportPlan {
    pub ready_for_database_import: bool,
    pub ready_for_graph_import: bool,
    pub ready_for_cutover: bool,
    pub graph_stream_validation: HawDBLightningGraphStreamValidation,
    pub relational_stream_validation: HawDBLightningRelationalStreamValidation,
    pub decoded_snapshot_import_ready: bool,
    pub decoded_graph_commit_epoch: Option<u64>,
    pub decoded_node_count: Option<usize>,
    pub decoded_relationship_count: Option<usize>,
    pub decoded_relational_table_count: Option<usize>,
    pub decoded_relational_row_count: Option<usize>,
    pub target_readiness: HawDBLightningInitialImportReadiness,
    pub checkpoint_readiness: Option<HawDBLightningInitialImportCheckpointReadiness>,
    pub document_identity_coverage: Option<HawDBLightningInitialImportDocumentIdentityCoverage>,
    pub resume_action: HawDBLightningInitialImportResumeAction,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningInitialImportApplyReport {
    pub applied: bool,
    pub ready_for_cutover: bool,
    pub database_commit_epoch: u64,
    pub node_count: usize,
    pub relationship_count: usize,
    pub relational_table_count: usize,
    pub relational_row_count: usize,
    pub plan: HawDBLightningInitialImportPlan,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningInitialImportSearchProjectionBatchReport {
    pub ready: bool,
    pub checkpoint_present: bool,
    pub checkpoint_matches_manifest: bool,
    pub checkpoint_idempotency_key_present: bool,
    pub total_batches_match_checkpoint: bool,
    pub source_graph_commit_epoch_matches: bool,
    pub batch_position_valid: bool,
    pub operation_limit_ok: bool,
    pub empty_batch: bool,
    pub delete_count: usize,
    pub document_identity_coverage: HawDBLightningInitialImportDocumentIdentityCoverage,
    pub source_graph_commit_epoch: Option<u64>,
    pub batch_index: u64,
    pub total_batches: u64,
    pub operation_count: usize,
    pub checkpoint_progress: Option<HawDBLightningInitialImportCheckpointProgress>,
    pub checkpoint_progress_accepted: bool,
    pub checkpoint_progress_readiness: Option<HawDBLightningInitialImportCheckpointReadiness>,
    pub checkpoint_resume_action: Option<HawDBLightningInitialImportResumeAction>,
    pub checkpoint_progress_blocker_codes: Vec<String>,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningInitialImportSourceBundleReadiness {
    pub ready: bool,
    pub database_source_import_ready: bool,
    pub checkpoint_present: bool,
    pub projection_batch_count: usize,
    pub ready_projection_batch_count: usize,
    pub total_batches: u64,
    pub source_fingerprint: HawDBLightningInitialImportSourceFingerprint,
    pub document_identity_coverage: HawDBLightningInitialImportDocumentIdentityCoverage,
    pub batch_reports: Vec<HawDBLightningInitialImportSearchProjectionBatchReport>,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningInitialImportSourceFingerprint {
    pub protocol_version: u64,
    pub database_commit_epoch: u64,
    pub graph_commit_epoch: u64,
    pub logical_checksum: u64,
    pub graph_stream_checksum: u64,
    pub graph_stream_byte_len: usize,
    pub relational_stream_checksum: u64,
    pub relational_stream_byte_len: usize,
    pub schema_checksum: u64,
    pub node_count: usize,
    pub relationship_count: usize,
    pub relational_table_count: usize,
    pub relational_row_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningInitialImportDurableState {
    pub source_fingerprint: HawDBLightningInitialImportSourceFingerprint,
    pub checkpoint: HawDBLightningInitialImportCheckpoint,
    pub document_identities: Vec<HawDBLightningInitialImportDocumentIdentity>,
    pub document_identity_coverage: HawDBLightningInitialImportDocumentIdentityCoverage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningInitialImportDurableStateReport {
    pub persistable: bool,
    pub ready_for_cutover: bool,
    pub state: Option<HawDBLightningInitialImportDurableState>,
    pub checkpoint_readiness: HawDBLightningInitialImportCheckpointReadiness,
    pub resume_action: HawDBLightningInitialImportResumeAction,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningInitialImportDurableBatchAdvanceReport {
    pub ready: bool,
    pub idempotent_replay: bool,
    pub batch_report: HawDBLightningInitialImportSearchProjectionBatchReport,
    pub durable_state_report: HawDBLightningInitialImportDurableStateReport,
    pub blocker_codes: Vec<String>,
}

/// Result of accepting one bounded projection page during initial import.
///
/// Unlike [`HawDBLightningInitialImportDurableBatchAdvanceReport`], this
/// report does not require six-kind document coverage before every page. That
/// coverage remains mandatory for `ready_for_cutover`, while `accepted`
/// permits a host to persist bounded progress without buffering a complete
/// LanceDB projection in memory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningInitialImportStreamingBatchAdvanceReport {
    pub accepted: bool,
    pub idempotent_replay: bool,
    pub completed: bool,
    pub ready_for_cutover: bool,
    pub durable_state_report: HawDBLightningInitialImportDurableStateReport,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningInitialImportSessionReport {
    pub ready_for_database_import: bool,
    pub ready_for_cutover: bool,
    pub durable_state_present: bool,
    pub durable_state_source_matches_manifest: bool,
    pub plan: HawDBLightningInitialImportPlan,
    pub durable_state_report: Option<HawDBLightningInitialImportDurableStateReport>,
    pub next_action: HawDBLightningInitialImportResumeAction,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningInitialImportCutoverCatchUpReport {
    pub ready: bool,
    pub session_ready_for_cutover: bool,
    pub durable_state_present: bool,
    pub live_projection_present: bool,
    pub import_graph_commit_epoch: Option<u64>,
    pub import_durable_search_projection_commit_epoch: Option<u64>,
    pub live_graph_commit_epoch: u64,
    pub live_search_projection_commit_epoch: Option<u64>,
    pub live_durable_search_projection_commit_epoch: Option<u64>,
    pub graph_watermark_caught_up: bool,
    pub search_projection_watermark_caught_up: bool,
    pub live_projection_checkpointed: bool,
    pub live_projection_healthy: bool,
    pub cutover_watermark: Option<u64>,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningInitialImportSessionBundleReadiness {
    pub ready: bool,
    pub resumable: bool,
    pub ready_for_cutover: bool,
    pub source_bundle_ready: bool,
    pub session_ready_for_database_import: bool,
    pub session_ready_for_cutover: bool,
    pub durable_state_present: bool,
    pub durable_state_source_matches_manifest: bool,
    pub catch_up_required: bool,
    pub catch_up_present: bool,
    pub catch_up_ready: bool,
    pub cutover_watermark: Option<u64>,
    pub next_action: HawDBLightningInitialImportResumeAction,
    pub blocker_codes: Vec<String>,
}

/// Immutable source material and projection watermarks used to evaluate an
/// initial-import startup or recovery attempt.
///
/// Keeping these values together prevents callers from accidentally mixing a
/// graph stream, relational stream, manifest, and projection evidence from
/// different bootstrap exports.
#[derive(Debug, Clone, Copy)]
pub struct HawDBLightningInitialImportReadinessInputs<'a> {
    pub encoded_graph_stream: &'a str,
    pub encoded_relational_stream: &'a [u8],
    pub manifest: &'a HawDBLightningBootstrapManifest,
    pub projection_batches: &'a [SearchProjectionDelta],
    pub target_projection_freshness: Option<&'a SearchProjectionFreshness>,
    pub live_projection_freshness: Option<&'a SearchProjectionFreshness>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningInitialImportStartupReadinessReport {
    pub ready: bool,
    pub source_bundle: HawDBLightningInitialImportSourceBundleReadiness,
    pub session: HawDBLightningInitialImportSessionReport,
    pub cutover_catch_up: Option<HawDBLightningInitialImportCutoverCatchUpReport>,
    pub readiness: HawDBLightningInitialImportSessionBundleReadiness,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningInitialImportDurableStateCodecReport {
    pub ready: bool,
    pub protocol: String,
    pub source_fingerprint_matches_manifest: bool,
    pub state: Option<HawDBLightningInitialImportDurableState>,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HawDBLightningInitialImportRecoveryReadinessReport {
    pub ready: bool,
    pub durable_state_payload_present: bool,
    pub durable_state_codec: Option<HawDBLightningInitialImportDurableStateCodecReport>,
    pub startup: HawDBLightningInitialImportStartupReadinessReport,
    pub next_action: HawDBLightningInitialImportResumeAction,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalGraphSnapshotValidation {
    pub is_valid: bool,
    pub is_import_ready: bool,
    pub checksum_matches: bool,
    pub expected_logical_checksum: u64,
    pub stable_identity_matches: bool,
    pub stable_identity_ready: bool,
    pub expected_stable_identity: CanonicalSnapshotIdentityAudit,
    pub duplicate_node_ids: Vec<u64>,
    pub duplicate_relationship_ids: Vec<u64>,
    pub missing_sources: Vec<CanonicalSnapshotEndpointViolation>,
    pub missing_targets: Vec<CanonicalSnapshotEndpointViolation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CanonicalStableIdMapping {
    pub node_stable_ids: BTreeMap<u64, Value>,
    pub relationship_stable_ids: BTreeMap<u64, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalSnapshotEndpointViolation {
    pub relationship_id: u64,
    pub missing_node_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalSnapshotIdentityAudit {
    pub requires_stable_id_mapping: bool,
    pub nodes_without_stable_id: Vec<u64>,
    pub relationships_without_stable_id: Vec<u64>,
    pub duplicate_node_stable_ids: Vec<Value>,
    pub duplicate_relationship_stable_ids: Vec<Value>,
}

impl From<StoreStableIdMapping> for CanonicalStableIdMapping {
    fn from(mapping: StoreStableIdMapping) -> Self {
        Self {
            node_stable_ids: mapping
                .node_stable_ids
                .into_iter()
                .map(|(id, stable_id)| (id.0, stable_id))
                .collect(),
            relationship_stable_ids: mapping
                .relationship_stable_ids
                .into_iter()
                .map(|(id, stable_id)| (id.0, stable_id))
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalSnapshotNode {
    pub node_id: u64,
    pub stable_id: Option<Value>,
    pub labels: Vec<String>,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalSnapshotRelationship {
    pub relationship_id: u64,
    pub stable_id: Option<Value>,
    pub source_node_id: u64,
    pub target_node_id: u64,
    pub rel_type: String,
    pub properties: BTreeMap<String, Value>,
}

fn canonical_snapshot_identity_audit(
    nodes: &[CanonicalSnapshotNode],
    relationships: &[CanonicalSnapshotRelationship],
) -> CanonicalSnapshotIdentityAudit {
    let nodes_without_stable_id = nodes
        .iter()
        .filter(|node| node.stable_id.is_none())
        .map(|node| node.node_id)
        .collect::<Vec<_>>();
    let relationships_without_stable_id = relationships
        .iter()
        .filter(|relationship| relationship.stable_id.is_none())
        .map(|relationship| relationship.relationship_id)
        .collect::<Vec<_>>();
    let duplicate_node_stable_ids =
        duplicate_stable_ids(nodes.iter().filter_map(|node| node.stable_id.as_ref()));
    let duplicate_relationship_stable_ids = duplicate_stable_ids(
        relationships
            .iter()
            .filter_map(|relationship| relationship.stable_id.as_ref()),
    );
    let requires_stable_id_mapping = !nodes_without_stable_id.is_empty()
        || !relationships_without_stable_id.is_empty()
        || !duplicate_node_stable_ids.is_empty()
        || !duplicate_relationship_stable_ids.is_empty();
    CanonicalSnapshotIdentityAudit {
        requires_stable_id_mapping,
        nodes_without_stable_id,
        relationships_without_stable_id,
        duplicate_node_stable_ids,
        duplicate_relationship_stable_ids,
    }
}

fn duplicate_stable_ids<'a>(values: impl Iterator<Item = &'a Value>) -> Vec<Value> {
    let mut counts = BTreeMap::<Value, usize>::new();
    for value in values {
        *counts.entry(value.clone()).or_default() += 1;
    }
    counts
        .into_iter()
        .filter_map(|(value, count)| (count > 1).then_some(value))
        .collect()
}

fn duplicate_u64s(values: impl Iterator<Item = u64>) -> Vec<u64> {
    let mut counts = BTreeMap::<u64, usize>::new();
    for value in values {
        *counts.entry(value).or_default() += 1;
    }
    counts
        .into_iter()
        .filter_map(|(value, count)| (count > 1).then_some(value))
        .collect()
}

fn canonical_graph_snapshot_checksum(
    nodes: &[CanonicalSnapshotNode],
    relationships: &[CanonicalSnapshotRelationship],
) -> u64 {
    let mut body = String::new();
    body.push_str("HAWDB_CANONICAL_GRAPH_SNAPSHOT_V1\n");
    body.push_str(&format!("node_count\t{}\n", nodes.len()));
    for node in nodes {
        body.push_str(&format!("node\t{}\n", node.node_id));
        append_optional_canonical_value(&mut body, "stable_id", node.stable_id.as_ref());
        for label in &node.labels {
            append_canonical_string(&mut body, "label", label);
        }
        append_canonical_properties(&mut body, &node.properties);
    }
    body.push_str(&format!("relationship_count\t{}\n", relationships.len()));
    for relationship in relationships {
        body.push_str(&format!(
            "rel\t{}\t{}\t{}\n",
            relationship.relationship_id, relationship.source_node_id, relationship.target_node_id
        ));
        append_optional_canonical_value(&mut body, "stable_id", relationship.stable_id.as_ref());
        append_canonical_string(&mut body, "type", &relationship.rel_type);
        append_canonical_properties(&mut body, &relationship.properties);
    }
    checksum_bytes(body.as_bytes())
}

fn canonical_graph_snapshot_schema_checksum(
    nodes: &[CanonicalSnapshotNode],
    relationships: &[CanonicalSnapshotRelationship],
) -> u64 {
    let labels = nodes
        .iter()
        .flat_map(|node| node.labels.iter())
        .cloned()
        .collect::<BTreeSet<_>>();
    let relationship_types = relationships
        .iter()
        .map(|relationship| relationship.rel_type.clone())
        .collect::<BTreeSet<_>>();
    let node_properties = nodes
        .iter()
        .flat_map(|node| node.properties.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let relationship_properties = relationships
        .iter()
        .flat_map(|relationship| relationship.properties.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut body = String::new();
    body.push_str("HAWDB_LIGHTNING_BOOTSTRAP_SCHEMA_V1\n");
    body.push_str(&format!("label_count\t{}\n", labels.len()));
    for label in labels {
        append_canonical_string(&mut body, "label", &label);
    }
    body.push_str(&format!(
        "relationship_type_count\t{}\n",
        relationship_types.len()
    ));
    for relationship_type in relationship_types {
        append_canonical_string(&mut body, "relationship_type", &relationship_type);
    }
    body.push_str(&format!(
        "node_property_key_count\t{}\n",
        node_properties.len()
    ));
    for property in node_properties {
        append_canonical_string(&mut body, "node_property", &property);
    }
    body.push_str(&format!(
        "relationship_property_key_count\t{}\n",
        relationship_properties.len()
    ));
    for property in relationship_properties {
        append_canonical_string(&mut body, "relationship_property", &property);
    }
    checksum_bytes(body.as_bytes())
}

fn encode_hawdb_lightning_graph_stream_body(snapshot: &CanonicalGraphSnapshotExport) -> String {
    let node_stable_keys = snapshot
        .nodes
        .iter()
        .map(|node| (node.node_id, canonical_stable_key(node.stable_id.as_ref())))
        .collect::<BTreeMap<_, _>>();
    let mut nodes = snapshot.nodes.iter().collect::<Vec<_>>();
    nodes.sort_by_key(|node| {
        (
            node.labels.clone(),
            canonical_stable_key(node.stable_id.as_ref()),
            node.node_id,
        )
    });
    let mut relationships = snapshot.relationships.iter().collect::<Vec<_>>();
    relationships.sort_by_key(|relationship| {
        (
            relationship.rel_type.clone(),
            node_stable_keys
                .get(&relationship.source_node_id)
                .cloned()
                .unwrap_or_default(),
            node_stable_keys
                .get(&relationship.target_node_id)
                .cloned()
                .unwrap_or_default(),
            canonical_stable_key(relationship.stable_id.as_ref()),
            relationship.relationship_id,
        )
    });

    let mut body = String::new();
    body.push_str("HAWDB_LIGHTNING_GRAPH_STREAM_V1\n");
    body.push_str(&format!(
        "format_version\t{}\n",
        HAWDB_LIGHTNING_GRAPH_STREAM_FORMAT_VERSION
    ));
    body.push_str(&format!(
        "graph_commit_epoch\t{}\n",
        snapshot.graph_commit_epoch
    ));
    body.push_str(&format!(
        "logical_checksum\t{}\n",
        snapshot.logical_checksum
    ));
    body.push_str(&format!("node_count\t{}\n", nodes.len()));
    for node in nodes {
        body.push_str(&format!("node\t{}\n", node.node_id));
        append_optional_canonical_value(&mut body, "stable_id", node.stable_id.as_ref());
        body.push_str(&format!("label_count\t{}\n", node.labels.len()));
        for label in &node.labels {
            append_canonical_string(&mut body, "label", label);
        }
        append_canonical_properties(&mut body, &node.properties);
    }
    body.push_str(&format!("relationship_count\t{}\n", relationships.len()));
    for relationship in relationships {
        body.push_str(&format!(
            "relationship\t{}\t{}\t{}\n",
            relationship.relationship_id, relationship.source_node_id, relationship.target_node_id
        ));
        append_optional_canonical_value(&mut body, "stable_id", relationship.stable_id.as_ref());
        append_canonical_string(&mut body, "relationship_type", &relationship.rel_type);
        append_canonical_properties(&mut body, &relationship.properties);
    }
    body
}

fn canonical_stable_key(value: Option<&Value>) -> String {
    let mut key = String::new();
    append_optional_canonical_value(&mut key, "stable_id", value);
    key
}

pub fn validate_hawdb_lightning_graph_stream(
    encoded: &str,
    manifest: Option<&HawDBLightningBootstrapManifest>,
) -> HawDBLightningGraphStreamValidation {
    let (body, expected_stream_checksum, mut errors) = split_graph_stream_checksum(encoded);
    let actual_stream_checksum = checksum_bytes(body.as_bytes());
    let checksum_matches = expected_stream_checksum == Some(actual_stream_checksum);
    if !checksum_matches {
        errors.push("graph stream checksum mismatch".to_string());
    }

    let mut parsed = parse_hawdb_lightning_graph_stream_body(body, &mut errors);

    let duplicate_node_ids = duplicate_u64s(parsed.node_ids.iter().copied());
    let duplicate_relationship_ids = duplicate_u64s(parsed.relationship_ids.iter().copied());
    let node_id_set = parsed.node_ids.iter().copied().collect::<BTreeSet<_>>();
    let missing_sources = parsed
        .relationships
        .iter()
        .filter(|(_, source, _)| !node_id_set.contains(source))
        .map(
            |(relationship_id, source, _)| CanonicalSnapshotEndpointViolation {
                relationship_id: *relationship_id,
                missing_node_id: *source,
            },
        )
        .collect::<Vec<_>>();
    let missing_targets = parsed
        .relationships
        .iter()
        .filter(|(_, _, target)| !node_id_set.contains(target))
        .map(
            |(relationship_id, _, target)| CanonicalSnapshotEndpointViolation {
                relationship_id: *relationship_id,
                missing_node_id: *target,
            },
        )
        .collect::<Vec<_>>();
    let format_version_matches =
        parsed.format_version == Some(HAWDB_LIGHTNING_GRAPH_STREAM_FORMAT_VERSION);
    let count_matches = parsed.declared_node_count == Some(parsed.node_ids.len() as u64)
        && parsed.declared_relationship_count == Some(parsed.relationship_ids.len() as u64);
    let endpoint_integrity = duplicate_node_ids.is_empty()
        && duplicate_relationship_ids.is_empty()
        && missing_sources.is_empty()
        && missing_targets.is_empty();
    let manifest_matches = manifest.is_none_or(|manifest| {
        parsed.graph_commit_epoch == Some(manifest.graph_commit_epoch)
            && parsed.logical_checksum == Some(manifest.logical_checksum)
            && expected_stream_checksum == Some(manifest.graph_stream_checksum)
            && encoded.len() == manifest.graph_stream_byte_len
            && parsed.declared_node_count == Some(manifest.node_count as u64)
            && parsed.declared_relationship_count == Some(manifest.relationship_count as u64)
    });
    if !format_version_matches {
        errors.push("graph stream format version mismatch".to_string());
    }
    if !count_matches {
        errors.push("graph stream count mismatch".to_string());
    }
    if !endpoint_integrity {
        errors.push("graph stream endpoint integrity failed".to_string());
    }
    if !manifest_matches {
        errors.push("graph stream manifest mismatch".to_string());
    }
    let is_valid = checksum_matches
        && format_version_matches
        && count_matches
        && endpoint_integrity
        && manifest_matches
        && errors.is_empty();

    HawDBLightningGraphStreamValidation {
        is_valid,
        checksum_matches,
        format_version_matches,
        count_matches,
        endpoint_integrity,
        manifest_matches,
        expected_stream_checksum,
        actual_stream_checksum,
        format_version: parsed.format_version.take(),
        graph_commit_epoch: parsed.graph_commit_epoch.take(),
        logical_checksum: parsed.logical_checksum.take(),
        node_count: parsed.node_ids.len(),
        relationship_count: parsed.relationship_ids.len(),
        duplicate_node_ids,
        duplicate_relationship_ids,
        missing_sources,
        missing_targets,
        errors,
    }
}

pub fn validate_hawdb_lightning_relational_stream(
    encoded: &[u8],
    manifest: Option<&HawDBLightningBootstrapManifest>,
) -> HawDBLightningRelationalStreamValidation {
    let actual_stream_checksum = checksum_bytes(encoded);
    let expected_stream_checksum = manifest.map(|value| value.relational_stream_checksum);
    let checksum_matches =
        expected_stream_checksum.is_none_or(|value| value == actual_stream_checksum);
    let mut errors = Vec::new();
    if !checksum_matches {
        errors.push("relational stream checksum mismatch".to_string());
    }

    let decoded = decode_relational_checkpoint(encoded, RelationalDecodeLimits::checkpoint());
    let (database_commit_epoch, table_count, row_count, overflow_segment_count) = match decoded {
        Ok(checkpoint) => {
            let table_count = checkpoint.state.table_schemas().count();
            let row_count = checkpoint
                .state
                .table_schemas()
                .map(|schema| checkpoint.state.row_count(&schema.name))
                .sum();
            (
                Some(checkpoint.epoch),
                table_count,
                row_count,
                checkpoint.state.overflow_segment_count(),
            )
        }
        Err(error) => {
            errors.push(format!("relational stream decode failed: {error}"));
            (None, 0, 0, 0)
        }
    };
    let format_version_matches = manifest.is_none_or(|value| {
        value.relational_stream_format_version == HAWDB_LIGHTNING_RELATIONAL_STREAM_FORMAT_VERSION
    });
    let epoch_matches = manifest.is_none_or(|value| {
        database_commit_epoch == Some(value.database_commit_epoch)
            && value.graph_commit_epoch == value.database_commit_epoch
    });
    let count_matches = manifest.is_none_or(|value| {
        table_count == value.relational_table_count
            && row_count == value.relational_row_count
            && overflow_segment_count == value.relational_overflow_segment_count
    });
    let manifest_matches = manifest.is_none_or(|value| {
        encoded.len() == value.relational_stream_byte_len
            && checksum_matches
            && format_version_matches
            && epoch_matches
            && count_matches
    });
    if !format_version_matches {
        errors.push("relational stream format version mismatch".to_string());
    }
    if !epoch_matches {
        errors.push("relational stream database epoch mismatch".to_string());
    }
    if !count_matches {
        errors.push("relational stream count mismatch".to_string());
    }
    if !manifest_matches {
        errors.push("relational stream manifest mismatch".to_string());
    }
    let is_valid = checksum_matches
        && format_version_matches
        && epoch_matches
        && count_matches
        && manifest_matches
        && errors.is_empty();
    HawDBLightningRelationalStreamValidation {
        is_valid,
        checksum_matches,
        format_version_matches,
        epoch_matches,
        count_matches,
        manifest_matches,
        expected_stream_checksum,
        actual_stream_checksum,
        database_commit_epoch,
        table_count,
        row_count,
        overflow_segment_count,
        errors,
    }
}

pub fn parse_hawdb_lightning_graph_stream_export(
    encoded: &str,
    manifest: Option<&HawDBLightningBootstrapManifest>,
) -> Result<CanonicalGraphSnapshotExport> {
    let validation = validate_hawdb_lightning_graph_stream(encoded, manifest);
    if !validation.is_valid {
        return Err(HawDBError::Storage(format!(
            "HawDB Lightning graph stream is not import ready: {}",
            validation.errors.join("; ")
        )));
    }
    let (body, _, mut errors) = split_graph_stream_checksum(encoded);
    let parsed = parse_hawdb_lightning_graph_stream_body(body, &mut errors);
    if !errors.is_empty() {
        return Err(HawDBError::Storage(format!(
            "HawDB Lightning graph stream parse failed: {}",
            errors.join("; ")
        )));
    }
    let stable_identity =
        canonical_snapshot_identity_audit(&parsed.nodes, &parsed.snapshot_relationships);
    let logical_checksum =
        canonical_graph_snapshot_checksum(&parsed.nodes, &parsed.snapshot_relationships);
    let export = CanonicalGraphSnapshotExport {
        graph_commit_epoch: parsed.graph_commit_epoch.unwrap_or(0),
        logical_checksum,
        stable_identity,
        nodes: parsed.nodes,
        relationships: parsed.snapshot_relationships,
    };
    let snapshot_validation = export.validate();
    if !snapshot_validation.is_import_ready {
        return Err(HawDBError::Storage(
            "HawDB Lightning graph stream decoded to a snapshot that is not import ready"
                .to_string(),
        ));
    }
    Ok(export)
}

pub fn hawdb_lightning_initial_import_plan(
    encoded_graph_stream: &str,
    encoded_relational_stream: &[u8],
    manifest: &HawDBLightningBootstrapManifest,
    target_graph_commit_epoch: u64,
    projection_freshness: Option<&SearchProjectionFreshness>,
    checkpoint: Option<&HawDBLightningInitialImportCheckpoint>,
) -> HawDBLightningInitialImportPlan {
    hawdb_lightning_initial_import_plan_with_document_identities(
        encoded_graph_stream,
        encoded_relational_stream,
        manifest,
        target_graph_commit_epoch,
        projection_freshness,
        checkpoint,
        None,
    )
}

pub fn hawdb_lightning_initial_import_plan_with_document_identities(
    encoded_graph_stream: &str,
    encoded_relational_stream: &[u8],
    manifest: &HawDBLightningBootstrapManifest,
    target_graph_commit_epoch: u64,
    projection_freshness: Option<&SearchProjectionFreshness>,
    checkpoint: Option<&HawDBLightningInitialImportCheckpoint>,
    document_identities: Option<&[HawDBLightningInitialImportDocumentIdentity]>,
) -> HawDBLightningInitialImportPlan {
    let graph_stream_validation =
        validate_hawdb_lightning_graph_stream(encoded_graph_stream, Some(manifest));
    let relational_stream_validation =
        validate_hawdb_lightning_relational_stream(encoded_relational_stream, Some(manifest));
    let decoded_snapshot = if graph_stream_validation.is_valid {
        parse_hawdb_lightning_graph_stream_export(encoded_graph_stream, Some(manifest)).ok()
    } else {
        None
    };
    let decoded_snapshot_import_ready = decoded_snapshot
        .as_ref()
        .is_some_and(|snapshot| snapshot.validate().is_import_ready);
    let decoded_graph_commit_epoch = decoded_snapshot
        .as_ref()
        .map(|snapshot| snapshot.graph_commit_epoch);
    let decoded_node_count = decoded_snapshot
        .as_ref()
        .map(|snapshot| snapshot.nodes.len());
    let decoded_relationship_count = decoded_snapshot
        .as_ref()
        .map(|snapshot| snapshot.relationships.len());
    let decoded_relational_table_count = relational_stream_validation
        .is_valid
        .then_some(relational_stream_validation.table_count);
    let decoded_relational_row_count = relational_stream_validation
        .is_valid
        .then_some(relational_stream_validation.row_count);
    let target_readiness = hawdb_lightning_initial_import_readiness(
        manifest,
        target_graph_commit_epoch,
        projection_freshness,
    );
    let checkpoint_readiness = checkpoint.map(|checkpoint| {
        hawdb_lightning_initial_import_checkpoint_readiness(manifest, checkpoint)
    });
    let document_identity_coverage =
        document_identities.map(hawdb_lightning_initial_import_document_identity_coverage);
    let resume_action = hawdb_lightning_initial_import_resume_action(manifest, checkpoint);
    let ready_for_graph_import = graph_stream_validation.is_valid && decoded_snapshot_import_ready;
    let ready_for_database_import = ready_for_graph_import && relational_stream_validation.is_valid;
    let document_identities_ready = document_identity_coverage
        .as_ref()
        .map(|coverage| coverage.ready)
        .unwrap_or(true);
    let ready_for_cutover = ready_for_database_import
        && target_readiness.ready
        && checkpoint_readiness
            .as_ref()
            .map(|readiness| readiness.ready)
            .unwrap_or(false)
        && document_identities_ready
        && resume_action.kind == HawDBLightningInitialImportResumeActionKind::ReadyForCutover;
    let mut blocker_codes = BTreeSet::new();
    if !graph_stream_validation.is_valid {
        blocker_codes.insert("hawdb_lightning_graph_stream_invalid".to_string());
    }
    if graph_stream_validation.is_valid && !decoded_snapshot_import_ready {
        blocker_codes.insert("hawdb_lightning_graph_stream_decode_not_import_ready".to_string());
    }
    if !relational_stream_validation.is_valid {
        blocker_codes.insert("hawdb_lightning_relational_stream_invalid".to_string());
    }
    if !target_readiness.ready {
        blocker_codes.extend(target_readiness.blocker_codes.iter().cloned());
    }
    match &checkpoint_readiness {
        Some(readiness) => {
            if !readiness.ready {
                blocker_codes.extend(readiness.blocker_codes.iter().cloned());
            }
        }
        None => {
            blocker_codes.insert("initial_import_checkpoint_missing".to_string());
        }
    }
    if let Some(coverage) = &document_identity_coverage
        && !coverage.ready
    {
        blocker_codes.extend(coverage.blocker_codes.iter().cloned());
    }
    if ready_for_database_import && !ready_for_cutover {
        match resume_action.kind {
            HawDBLightningInitialImportResumeActionKind::Start => {
                blocker_codes.insert("initial_import_not_started".to_string());
            }
            HawDBLightningInitialImportResumeActionKind::Resume => {
                blocker_codes.insert("initial_import_checkpoint_incomplete".to_string());
            }
            HawDBLightningInitialImportResumeActionKind::Quarantine => {
                blocker_codes.insert("initial_import_checkpoint_quarantined".to_string());
            }
            HawDBLightningInitialImportResumeActionKind::ReadyForCutover => {}
        }
    }
    HawDBLightningInitialImportPlan {
        ready_for_database_import,
        ready_for_graph_import,
        ready_for_cutover,
        graph_stream_validation,
        relational_stream_validation,
        decoded_snapshot_import_ready,
        decoded_graph_commit_epoch,
        decoded_node_count,
        decoded_relationship_count,
        decoded_relational_table_count,
        decoded_relational_row_count,
        target_readiness,
        checkpoint_readiness,
        document_identity_coverage,
        resume_action,
        blocker_codes: blocker_codes.into_iter().collect(),
    }
}

pub fn hawdb_lightning_initial_import_readiness(
    manifest: &HawDBLightningBootstrapManifest,
    target_graph_commit_epoch: u64,
    projection_freshness: Option<&SearchProjectionFreshness>,
) -> HawDBLightningInitialImportReadiness {
    let manifest_epoch_matches = manifest.database_commit_epoch == manifest.graph_commit_epoch;
    let manifest_import_ready = manifest.validation.is_import_ready
        && manifest.relational_validation.is_valid
        && manifest_epoch_matches;
    let graph_import_caught_up = target_graph_commit_epoch >= manifest.graph_commit_epoch;
    let projection_present = projection_freshness.is_some();
    let projection_source_graph_commit_epoch =
        projection_freshness.and_then(|freshness| freshness.source_graph_commit_epoch);
    let projection_durable_source_graph_commit_epoch =
        projection_freshness.and_then(|freshness| freshness.durable_source_graph_commit_epoch);
    let imported_projection_matches_manifest = projection_freshness.is_some_and(|freshness| {
        freshness.import_source_graph_commit_epoch == Some(manifest.graph_commit_epoch)
    });
    let projection_watermark_caught_up = if imported_projection_matches_manifest {
        // An imported projection's cursor belongs to the newly created local
        // WAL. Its presence is sufficient for source-import readiness; live
        // local catch-up is evaluated by the cutover report below.
        projection_durable_source_graph_commit_epoch.is_some()
    } else {
        projection_durable_source_graph_commit_epoch
            .is_some_and(|epoch| epoch >= target_graph_commit_epoch)
    };
    let projection_checkpointed = projection_freshness
        .map(|freshness| !freshness.has_uncheckpointed_changes)
        .unwrap_or(false);
    let projection_healthy = projection_freshness
        .map(|freshness| !freshness.full_reindex_needed && !freshness.metadata_repair_needed)
        .unwrap_or(false);
    let mut blocker_codes = BTreeSet::new();
    if !manifest_import_ready {
        blocker_codes.insert("hawdb_lightning_manifest_not_import_ready".to_string());
    }
    if !manifest_epoch_matches {
        blocker_codes.insert("hawdb_lightning_manifest_epoch_mismatch".to_string());
    }
    if !graph_import_caught_up {
        blocker_codes.insert("graph_import_watermark_behind_manifest".to_string());
    }
    if !projection_present {
        blocker_codes.insert("search_projection_missing".to_string());
    }
    if projection_present && !projection_watermark_caught_up {
        blocker_codes.insert("search_projection_watermark_behind_graph".to_string());
    }
    if projection_present && !projection_checkpointed {
        blocker_codes.insert("search_projection_not_checkpointed".to_string());
    }
    if projection_present && !projection_healthy {
        blocker_codes.insert("search_projection_repair_required".to_string());
    }
    let blocker_codes = blocker_codes.into_iter().collect::<Vec<_>>();
    HawDBLightningInitialImportReadiness {
        ready: blocker_codes.is_empty(),
        manifest_import_ready,
        projection_present,
        graph_import_caught_up,
        projection_watermark_caught_up,
        projection_checkpointed,
        manifest_graph_commit_epoch: manifest.graph_commit_epoch,
        target_graph_commit_epoch,
        projection_source_graph_commit_epoch,
        projection_durable_source_graph_commit_epoch,
        blocker_codes,
    }
}

pub fn hawdb_lightning_initial_import_checkpoint_readiness(
    manifest: &HawDBLightningBootstrapManifest,
    checkpoint: &HawDBLightningInitialImportCheckpoint,
) -> HawDBLightningInitialImportCheckpointReadiness {
    let idempotency_key = hawdb_lightning_initial_import_idempotency_key(checkpoint);
    let idempotency_key_present = idempotency_key.is_some();
    let checkpoint_matches_manifest = checkpoint.protocol_version == 1
        && checkpoint.schema_checksum == manifest.schema_checksum
        && checkpoint.graph_stream_checksum == manifest.graph_stream_checksum
        && checkpoint.graph_stream_byte_len == manifest.graph_stream_byte_len
        && checkpoint.relational_stream_checksum == manifest.relational_stream_checksum
        && checkpoint.relational_stream_byte_len == manifest.relational_stream_byte_len
        && checkpoint.manifest_database_commit_epoch == manifest.database_commit_epoch
        && checkpoint.manifest_graph_commit_epoch == manifest.graph_commit_epoch;
    let graph_checkpoint_caught_up =
        checkpoint.applied_graph_commit_epoch >= manifest.graph_commit_epoch;
    let search_projection_applied_caught_up = checkpoint
        .applied_search_projection_commit_epoch
        .is_some_and(|epoch| epoch >= checkpoint.applied_graph_commit_epoch);
    let search_projection_durable_caught_up = checkpoint
        .durable_search_projection_commit_epoch
        .is_some_and(|epoch| epoch >= checkpoint.applied_graph_commit_epoch);
    let batches_complete =
        checkpoint.total_batches > 0 && checkpoint.completed_batches == checkpoint.total_batches;
    let document_identities_present = checkpoint.document_identity_count > 0;
    let mut blocker_codes = BTreeSet::new();
    if !idempotency_key_present {
        blocker_codes.insert("initial_import_checkpoint_idempotency_key_missing".to_string());
    }
    if !checkpoint_matches_manifest {
        blocker_codes.insert("initial_import_checkpoint_manifest_mismatch".to_string());
    }
    if !graph_checkpoint_caught_up {
        blocker_codes.insert("initial_import_graph_checkpoint_behind_manifest".to_string());
    }
    if !search_projection_applied_caught_up {
        blocker_codes.insert("initial_import_search_projection_apply_behind_graph".to_string());
    }
    if !search_projection_durable_caught_up {
        blocker_codes
            .insert("initial_import_search_projection_checkpoint_behind_graph".to_string());
    }
    if !batches_complete {
        blocker_codes.insert("initial_import_batches_incomplete".to_string());
    }
    if !document_identities_present {
        blocker_codes.insert("initial_import_document_identities_missing".to_string());
    }
    let blocker_codes = blocker_codes.into_iter().collect::<Vec<_>>();
    HawDBLightningInitialImportCheckpointReadiness {
        ready: blocker_codes.is_empty(),
        idempotency_key_present,
        idempotency_key,
        checkpoint_matches_manifest,
        graph_checkpoint_caught_up,
        search_projection_applied_caught_up,
        search_projection_durable_caught_up,
        batches_complete,
        document_identities_present,
        manifest_graph_commit_epoch: manifest.graph_commit_epoch,
        applied_graph_commit_epoch: checkpoint.applied_graph_commit_epoch,
        applied_search_projection_commit_epoch: checkpoint.applied_search_projection_commit_epoch,
        durable_search_projection_commit_epoch: checkpoint.durable_search_projection_commit_epoch,
        completed_batches: checkpoint.completed_batches,
        total_batches: checkpoint.total_batches,
        document_identity_count: checkpoint.document_identity_count,
        blocker_codes,
    }
}

pub fn hawdb_lightning_initial_import_advance_checkpoint(
    manifest: &HawDBLightningBootstrapManifest,
    checkpoint: &HawDBLightningInitialImportCheckpoint,
    progress: HawDBLightningInitialImportCheckpointProgress,
) -> HawDBLightningInitialImportCheckpointProgressReport {
    let previous_readiness =
        hawdb_lightning_initial_import_checkpoint_readiness(manifest, checkpoint);
    let mut blocker_codes = BTreeSet::new();
    if !previous_readiness.idempotency_key_present {
        blocker_codes.insert("initial_import_checkpoint_idempotency_key_missing".to_string());
    }
    if !previous_readiness.checkpoint_matches_manifest {
        blocker_codes.insert("initial_import_checkpoint_manifest_mismatch".to_string());
    }
    if progress.applied_graph_commit_epoch < checkpoint.applied_graph_commit_epoch {
        blocker_codes.insert("initial_import_graph_checkpoint_regressed".to_string());
    }
    if optional_epoch_regressed(
        checkpoint.applied_search_projection_commit_epoch,
        progress.applied_search_projection_commit_epoch,
    ) {
        blocker_codes.insert("initial_import_search_projection_apply_regressed".to_string());
    }
    if optional_epoch_regressed(
        checkpoint.durable_search_projection_commit_epoch,
        progress.durable_search_projection_commit_epoch,
    ) {
        blocker_codes.insert("initial_import_search_projection_checkpoint_regressed".to_string());
    }
    if progress.completed_batches < checkpoint.completed_batches {
        blocker_codes.insert("initial_import_completed_batches_regressed".to_string());
    }
    if progress.total_batches < checkpoint.total_batches {
        blocker_codes.insert("initial_import_total_batches_regressed".to_string());
    }
    if progress.completed_batches > progress.total_batches {
        blocker_codes.insert("initial_import_completed_batches_exceed_total".to_string());
    }
    if progress.document_identity_count < checkpoint.document_identity_count {
        blocker_codes.insert("initial_import_document_identities_regressed".to_string());
    }

    if !blocker_codes.is_empty() {
        let blocker_codes = blocker_codes.into_iter().collect::<Vec<_>>();
        return HawDBLightningInitialImportCheckpointProgressReport {
            accepted: false,
            checkpoint: checkpoint.clone(),
            readiness: previous_readiness,
            resume_action: hawdb_lightning_initial_import_resume_action(manifest, Some(checkpoint)),
            blocker_codes,
        };
    }

    let advanced = HawDBLightningInitialImportCheckpoint {
        applied_graph_commit_epoch: progress.applied_graph_commit_epoch,
        applied_search_projection_commit_epoch: progress.applied_search_projection_commit_epoch,
        durable_search_projection_commit_epoch: progress.durable_search_projection_commit_epoch,
        completed_batches: progress.completed_batches,
        total_batches: progress.total_batches,
        document_identity_count: progress.document_identity_count,
        ..checkpoint.clone()
    };
    let readiness = hawdb_lightning_initial_import_checkpoint_readiness(manifest, &advanced);
    let resume_action = hawdb_lightning_initial_import_resume_action(manifest, Some(&advanced));
    HawDBLightningInitialImportCheckpointProgressReport {
        accepted: true,
        checkpoint: advanced,
        readiness,
        resume_action,
        blocker_codes: Vec::new(),
    }
}

pub fn hawdb_lightning_initial_import_search_projection_batch_report(
    manifest: &HawDBLightningBootstrapManifest,
    checkpoint: Option<&HawDBLightningInitialImportCheckpoint>,
    delta: &SearchProjectionDelta,
    batch_index: u64,
    total_batches: u64,
) -> HawDBLightningInitialImportSearchProjectionBatchReport {
    let document_identities = search_projection_delta_document_identities(delta);
    hawdb_lightning_initial_import_search_projection_batch_report_with_document_identities(
        manifest,
        checkpoint,
        delta,
        batch_index,
        total_batches,
        &document_identities,
    )
}

pub fn hawdb_lightning_initial_import_source_bundle_readiness(
    manifest: &HawDBLightningBootstrapManifest,
    checkpoint: Option<&HawDBLightningInitialImportCheckpoint>,
    projection_batches: &[SearchProjectionDelta],
) -> HawDBLightningInitialImportSourceBundleReadiness {
    let source_fingerprint = hawdb_lightning_initial_import_source_fingerprint(manifest);
    let total_batches = projection_batches.len() as u64;
    let document_identities = projection_batches
        .iter()
        .flat_map(search_projection_delta_document_identities)
        .collect::<Vec<_>>();
    let document_identity_coverage =
        hawdb_lightning_initial_import_document_identity_coverage(&document_identities);
    let batch_reports = projection_batches
        .iter()
        .enumerate()
        .map(|(batch_index, delta)| {
            hawdb_lightning_initial_import_search_projection_batch_report_with_document_identities(
                manifest,
                checkpoint,
                delta,
                batch_index as u64,
                total_batches,
                &document_identities,
            )
        })
        .collect::<Vec<_>>();
    let ready_projection_batch_count = batch_reports.iter().filter(|report| report.ready).count();
    let database_source_import_ready = manifest.validation.is_import_ready
        && manifest.relational_validation.is_valid
        && manifest.database_commit_epoch == manifest.graph_commit_epoch;
    let checkpoint_present = checkpoint.is_some();
    let mut blocker_codes = BTreeSet::new();
    if !database_source_import_ready {
        blocker_codes.insert("initial_import_source_bundle_database_not_import_ready".to_string());
    }
    if !checkpoint_present {
        blocker_codes.insert("initial_import_source_bundle_checkpoint_missing".to_string());
    }
    if projection_batches.is_empty() {
        blocker_codes.insert("initial_import_source_bundle_projection_batches_missing".to_string());
    }
    if !document_identity_coverage.ready {
        blocker_codes.extend(document_identity_coverage.blocker_codes.iter().cloned());
    }
    for report in &batch_reports {
        blocker_codes.extend(report.blocker_codes.iter().cloned());
        blocker_codes.extend(report.checkpoint_progress_blocker_codes.iter().cloned());
    }
    let blocker_codes = blocker_codes.into_iter().collect::<Vec<_>>();
    let ready = blocker_codes.is_empty()
        && database_source_import_ready
        && checkpoint_present
        && !projection_batches.is_empty()
        && ready_projection_batch_count == projection_batches.len();

    HawDBLightningInitialImportSourceBundleReadiness {
        ready,
        database_source_import_ready,
        checkpoint_present,
        projection_batch_count: projection_batches.len(),
        ready_projection_batch_count,
        total_batches,
        source_fingerprint,
        document_identity_coverage,
        batch_reports,
        blocker_codes,
    }
}

pub fn hawdb_lightning_initial_import_search_projection_batch_report_with_document_identities(
    manifest: &HawDBLightningBootstrapManifest,
    checkpoint: Option<&HawDBLightningInitialImportCheckpoint>,
    delta: &SearchProjectionDelta,
    batch_index: u64,
    total_batches: u64,
    document_identities: &[HawDBLightningInitialImportDocumentIdentity],
) -> HawDBLightningInitialImportSearchProjectionBatchReport {
    let source_graph_commit_epoch_matches =
        delta.source_graph_commit_epoch == Some(manifest.graph_commit_epoch);
    let batch_position_valid = total_batches > 0 && batch_index < total_batches;
    let operation_count = delta.operation_count();
    let operation_limit_ok = delta
        .max_operations
        .is_none_or(|limit| operation_count <= limit);
    let checkpoint_readiness = checkpoint.map(|checkpoint| {
        hawdb_lightning_initial_import_checkpoint_readiness(manifest, checkpoint)
    });
    let checkpoint_present = checkpoint.is_some();
    let checkpoint_matches_manifest = checkpoint_readiness
        .as_ref()
        .is_some_and(|readiness| readiness.checkpoint_matches_manifest);
    let checkpoint_idempotency_key_present = checkpoint_readiness
        .as_ref()
        .is_some_and(|readiness| readiness.idempotency_key_present);
    let total_batches_match_checkpoint = checkpoint
        .map(|checkpoint| checkpoint.total_batches == total_batches)
        .unwrap_or(false);
    let empty_batch = operation_count == 0;
    let delete_count = delta.deletes.len();
    let document_identity_coverage =
        hawdb_lightning_initial_import_document_identity_coverage(document_identities);
    let mut blocker_codes = BTreeSet::new();
    if !checkpoint_present {
        blocker_codes
            .insert("initial_import_search_projection_batch_checkpoint_missing".to_string());
    }
    if checkpoint_present && !checkpoint_matches_manifest {
        blocker_codes
            .insert("initial_import_search_projection_batch_checkpoint_mismatch".to_string());
    }
    if checkpoint_present && !checkpoint_idempotency_key_present {
        blocker_codes.insert(
            "initial_import_search_projection_batch_checkpoint_idempotency_missing".to_string(),
        );
    }
    if checkpoint_present && !total_batches_match_checkpoint {
        blocker_codes.insert("initial_import_search_projection_batch_total_mismatch".to_string());
    }
    if !source_graph_commit_epoch_matches {
        blocker_codes.insert("initial_import_search_projection_batch_epoch_mismatch".to_string());
    }
    if !batch_position_valid {
        blocker_codes.insert("initial_import_search_projection_batch_position_invalid".to_string());
    }
    if !operation_limit_ok {
        blocker_codes.insert("initial_import_search_projection_batch_limit_exceeded".to_string());
    }
    if empty_batch {
        blocker_codes.insert("initial_import_search_projection_batch_empty".to_string());
    }
    if delete_count > 0 {
        blocker_codes.insert("initial_import_search_projection_batch_has_deletes".to_string());
    }
    if !document_identity_coverage.ready {
        blocker_codes.extend(document_identity_coverage.blocker_codes.iter().cloned());
    }
    let blocker_codes = blocker_codes.into_iter().collect::<Vec<_>>();
    let ready = blocker_codes.is_empty();
    let checkpoint_progress = if ready {
        checkpoint.map(|checkpoint| {
            let completed_batches = checkpoint
                .completed_batches
                .max(batch_index.saturating_add(1));
            HawDBLightningInitialImportCheckpointProgress {
                applied_graph_commit_epoch: checkpoint
                    .applied_graph_commit_epoch
                    .max(manifest.graph_commit_epoch),
                applied_search_projection_commit_epoch: Some(manifest.graph_commit_epoch),
                durable_search_projection_commit_epoch: checkpoint
                    .durable_search_projection_commit_epoch,
                completed_batches,
                total_batches: checkpoint.total_batches.max(total_batches),
                document_identity_count: checkpoint
                    .document_identity_count
                    .max(document_identities.len()),
            }
        })
    } else {
        None
    };
    let checkpoint_progress_report = checkpoint_progress.as_ref().and_then(|progress| {
        checkpoint.map(|checkpoint| {
            hawdb_lightning_initial_import_advance_checkpoint(
                manifest,
                checkpoint,
                progress.clone(),
            )
        })
    });
    let checkpoint_progress_accepted = checkpoint_progress_report
        .as_ref()
        .is_some_and(|report| report.accepted);
    let checkpoint_progress_readiness = checkpoint_progress_report
        .as_ref()
        .map(|report| report.readiness.clone());
    let checkpoint_resume_action = checkpoint_progress_report
        .as_ref()
        .map(|report| report.resume_action.clone());
    let checkpoint_progress_blocker_codes = checkpoint_progress_report
        .map(|report| report.blocker_codes)
        .unwrap_or_default();
    HawDBLightningInitialImportSearchProjectionBatchReport {
        ready,
        checkpoint_present,
        checkpoint_matches_manifest,
        checkpoint_idempotency_key_present,
        total_batches_match_checkpoint,
        source_graph_commit_epoch_matches,
        batch_position_valid,
        operation_limit_ok,
        empty_batch,
        delete_count,
        document_identity_coverage,
        source_graph_commit_epoch: delta.source_graph_commit_epoch,
        batch_index,
        total_batches,
        operation_count,
        checkpoint_progress,
        checkpoint_progress_accepted,
        checkpoint_progress_readiness,
        checkpoint_resume_action,
        checkpoint_progress_blocker_codes,
        blocker_codes,
    }
}

pub fn hawdb_lightning_initial_import_durable_state_report(
    manifest: &HawDBLightningBootstrapManifest,
    checkpoint: &HawDBLightningInitialImportCheckpoint,
    document_identities: &[HawDBLightningInitialImportDocumentIdentity],
) -> HawDBLightningInitialImportDurableStateReport {
    let checkpoint_readiness =
        hawdb_lightning_initial_import_checkpoint_readiness(manifest, checkpoint);
    let resume_action = hawdb_lightning_initial_import_resume_action(manifest, Some(checkpoint));
    let document_identity_coverage =
        hawdb_lightning_initial_import_document_identity_coverage(document_identities);
    let mut blocker_codes = BTreeSet::new();
    if !checkpoint_readiness.idempotency_key_present {
        blocker_codes.insert("initial_import_durable_state_idempotency_missing".to_string());
    }
    if !checkpoint_readiness.checkpoint_matches_manifest {
        blocker_codes.insert("initial_import_durable_state_manifest_mismatch".to_string());
    }
    if checkpoint.total_batches == 0 {
        blocker_codes.insert("initial_import_durable_state_total_batches_missing".to_string());
    }
    if checkpoint.completed_batches > checkpoint.total_batches {
        blocker_codes
            .insert("initial_import_durable_state_completed_batches_exceed_total".to_string());
    }
    if checkpoint.document_identity_count > document_identities.len() {
        blocker_codes
            .insert("initial_import_durable_state_document_identity_regressed".to_string());
    }
    let persistable = blocker_codes.is_empty();
    if !document_identity_coverage.ready {
        blocker_codes.extend(document_identity_coverage.blocker_codes.iter().cloned());
    }
    let ready_for_cutover =
        persistable && checkpoint_readiness.ready && document_identity_coverage.ready;
    let state = persistable.then(|| HawDBLightningInitialImportDurableState {
        source_fingerprint: hawdb_lightning_initial_import_source_fingerprint(manifest),
        checkpoint: checkpoint.clone(),
        document_identities: document_identities.to_vec(),
        document_identity_coverage: document_identity_coverage.clone(),
    });
    HawDBLightningInitialImportDurableStateReport {
        persistable,
        ready_for_cutover,
        state,
        checkpoint_readiness,
        resume_action,
        blocker_codes: blocker_codes.into_iter().collect(),
    }
}

fn hawdb_lightning_initial_import_durable_state_json(
    state: &HawDBLightningInitialImportDurableState,
) -> serde_json::Value {
    serde_json::json!({
        "protocol": HAWDB_LIGHTNING_INITIAL_IMPORT_DURABLE_STATE_PROTOCOL,
        "source_fingerprint": hawdb_lightning_initial_import_source_fingerprint_json(&state.source_fingerprint),
        "checkpoint": hawdb_lightning_initial_import_checkpoint_json(&state.checkpoint),
        "document_identities": state
            .document_identities
            .iter()
            .map(hawdb_lightning_initial_import_document_identity_json)
            .collect::<Vec<_>>(),
        "document_identity_coverage": hawdb_lightning_initial_import_document_identity_coverage_json(&state.document_identity_coverage),
    })
}

pub fn hawdb_lightning_initial_import_encode_durable_state(
    state: &HawDBLightningInitialImportDurableState,
) -> Result<String> {
    serde_json::to_string(&hawdb_lightning_initial_import_durable_state_json(state)).map_err(|_| {
        HawDBError::Execution(
            "initial import durable state serialization failed: invalid_json".to_string(),
        )
    })
}

fn hawdb_lightning_initial_import_decode_durable_state_value(
    manifest: &HawDBLightningBootstrapManifest,
    value: &serde_json::Value,
) -> Result<HawDBLightningInitialImportDurableStateCodecReport> {
    let mut blocker_codes = BTreeSet::new();
    let protocol = required_json_string(value, "protocol")?.to_string();
    if protocol != HAWDB_LIGHTNING_INITIAL_IMPORT_DURABLE_STATE_PROTOCOL {
        blocker_codes.insert("initial_import_durable_state_codec_protocol_mismatch".to_string());
    }
    let source_fingerprint = parse_hawdb_lightning_initial_import_source_fingerprint(
        required_json_object(value, "source_fingerprint")?,
    )?;
    let source_fingerprint_matches_manifest =
        source_fingerprint == hawdb_lightning_initial_import_source_fingerprint(manifest);
    if !source_fingerprint_matches_manifest {
        blocker_codes.insert("initial_import_durable_state_codec_source_mismatch".to_string());
    }
    let checkpoint = parse_hawdb_lightning_initial_import_checkpoint(required_json_object(
        value,
        "checkpoint",
    )?)?;
    let document_identities = parse_hawdb_lightning_initial_import_document_identities(
        required_json_array(value, "document_identities")?,
    )?;
    let state_report = hawdb_lightning_initial_import_durable_state_report(
        manifest,
        &checkpoint,
        &document_identities,
    );
    if !state_report.persistable {
        blocker_codes.extend(state_report.blocker_codes.iter().cloned());
    }
    let ready = blocker_codes.is_empty();
    let state = (ready && source_fingerprint_matches_manifest)
        .then_some(state_report.state)
        .flatten();
    Ok(HawDBLightningInitialImportDurableStateCodecReport {
        ready,
        protocol,
        source_fingerprint_matches_manifest,
        state,
        blocker_codes: blocker_codes.into_iter().collect(),
    })
}

pub fn hawdb_lightning_initial_import_decode_durable_state(
    manifest: &HawDBLightningBootstrapManifest,
    raw: &str,
) -> Result<HawDBLightningInitialImportDurableStateCodecReport> {
    let value = serde_json::from_str::<serde_json::Value>(raw).map_err(|_| {
        HawDBError::Semantic("initial import durable state parse failed: invalid_json".to_string())
    })?;
    hawdb_lightning_initial_import_decode_durable_state_value(manifest, &value)
}

pub fn hawdb_lightning_initial_import_advance_durable_state_with_search_projection_batch(
    manifest: &HawDBLightningBootstrapManifest,
    state: &HawDBLightningInitialImportDurableState,
    delta: &SearchProjectionDelta,
    batch_index: u64,
    total_batches: u64,
) -> HawDBLightningInitialImportDurableBatchAdvanceReport {
    let document_identities = merge_initial_import_document_identities(
        &state.document_identities,
        &search_projection_delta_document_identities(delta),
    );
    let batch_report =
        hawdb_lightning_initial_import_search_projection_batch_report_with_document_identities(
            manifest,
            Some(&state.checkpoint),
            delta,
            batch_index,
            total_batches,
            &document_identities,
        );
    let idempotent_replay = batch_index < state.checkpoint.completed_batches;
    let durable_state_report = if let Some(progress) = batch_report.checkpoint_progress.as_ref() {
        let progress_report = hawdb_lightning_initial_import_advance_checkpoint(
            manifest,
            &state.checkpoint,
            progress.clone(),
        );
        if progress_report.accepted {
            hawdb_lightning_initial_import_durable_state_report(
                manifest,
                &progress_report.checkpoint,
                &document_identities,
            )
        } else {
            hawdb_lightning_initial_import_durable_state_report(
                manifest,
                &state.checkpoint,
                &state.document_identities,
            )
        }
    } else {
        hawdb_lightning_initial_import_durable_state_report(
            manifest,
            &state.checkpoint,
            &state.document_identities,
        )
    };
    let mut blocker_codes = BTreeSet::new();
    if !batch_report.ready {
        blocker_codes.extend(batch_report.blocker_codes.iter().cloned());
    }
    if !batch_report.checkpoint_progress_accepted {
        blocker_codes.extend(
            batch_report
                .checkpoint_progress_blocker_codes
                .iter()
                .cloned(),
        );
        if batch_report.checkpoint_progress.is_none() {
            blocker_codes
                .insert("initial_import_durable_batch_checkpoint_progress_missing".to_string());
        }
    }
    if !durable_state_report.persistable {
        blocker_codes.extend(durable_state_report.blocker_codes.iter().cloned());
    }
    let blocker_codes = blocker_codes.into_iter().collect::<Vec<_>>();
    HawDBLightningInitialImportDurableBatchAdvanceReport {
        ready: blocker_codes.is_empty(),
        idempotent_replay,
        batch_report,
        durable_state_report,
        blocker_codes,
    }
}

/// Advances a durable initial-import checkpoint from one bounded projection
/// page. It deliberately separates page admission from final six-kind
/// coverage: a page can be durably accepted before every projection kind has
/// been scanned, but read cutover remains blocked until the accumulated state
/// satisfies the normal coverage and checkpoint checks.
pub fn hawdb_lightning_initial_import_advance_durable_state_streaming(
    manifest: &HawDBLightningBootstrapManifest,
    state: &HawDBLightningInitialImportDurableState,
    delta: &SearchProjectionDelta,
    batch_index: u64,
    total_batches: u64,
) -> HawDBLightningInitialImportStreamingBatchAdvanceReport {
    let checkpoint_readiness =
        hawdb_lightning_initial_import_checkpoint_readiness(manifest, &state.checkpoint);
    let mut blocker_codes = BTreeSet::new();
    if !checkpoint_readiness.idempotency_key_present {
        blocker_codes.insert("initial_import_streaming_batch_idempotency_missing".to_string());
    }
    if !checkpoint_readiness.checkpoint_matches_manifest {
        blocker_codes.insert("initial_import_streaming_batch_checkpoint_mismatch".to_string());
    }
    if state.checkpoint.total_batches != total_batches {
        blocker_codes.insert("initial_import_streaming_batch_total_mismatch".to_string());
    }
    if delta.source_graph_commit_epoch != Some(manifest.graph_commit_epoch) {
        blocker_codes.insert("initial_import_streaming_batch_epoch_mismatch".to_string());
    }
    if total_batches == 0 || batch_index >= total_batches {
        blocker_codes.insert("initial_import_streaming_batch_position_invalid".to_string());
    }
    if delta.operation_count() == 0 {
        blocker_codes.insert("initial_import_streaming_batch_empty".to_string());
    }
    if !delta.deletes.is_empty() {
        blocker_codes.insert("initial_import_streaming_batch_has_deletes".to_string());
    }
    if delta
        .max_operations
        .is_some_and(|limit| delta.operation_count() > limit)
    {
        blocker_codes.insert("initial_import_streaming_batch_limit_exceeded".to_string());
    }

    let idempotent_replay = batch_index < state.checkpoint.completed_batches;
    if !blocker_codes.is_empty() {
        let durable_state_report = hawdb_lightning_initial_import_durable_state_report(
            manifest,
            &state.checkpoint,
            &state.document_identities,
        );
        return HawDBLightningInitialImportStreamingBatchAdvanceReport {
            accepted: false,
            idempotent_replay,
            completed: state.checkpoint.completed_batches == state.checkpoint.total_batches,
            ready_for_cutover: durable_state_report.ready_for_cutover,
            durable_state_report,
            blocker_codes: blocker_codes.into_iter().collect(),
        };
    }

    let document_identities = merge_initial_import_document_identities(
        &state.document_identities,
        &search_projection_delta_document_identities(delta),
    );
    let progress = HawDBLightningInitialImportCheckpointProgress {
        applied_graph_commit_epoch: state
            .checkpoint
            .applied_graph_commit_epoch
            .max(manifest.graph_commit_epoch),
        applied_search_projection_commit_epoch: Some(manifest.graph_commit_epoch),
        durable_search_projection_commit_epoch: Some(manifest.graph_commit_epoch),
        completed_batches: state
            .checkpoint
            .completed_batches
            .max(batch_index.saturating_add(1)),
        total_batches,
        document_identity_count: state
            .checkpoint
            .document_identity_count
            .max(document_identities.len()),
    };
    let progress_report =
        hawdb_lightning_initial_import_advance_checkpoint(manifest, &state.checkpoint, progress);
    if !progress_report.accepted {
        return HawDBLightningInitialImportStreamingBatchAdvanceReport {
            accepted: false,
            idempotent_replay,
            completed: state.checkpoint.completed_batches == state.checkpoint.total_batches,
            ready_for_cutover: false,
            durable_state_report: hawdb_lightning_initial_import_durable_state_report(
                manifest,
                &state.checkpoint,
                &state.document_identities,
            ),
            blocker_codes: progress_report.blocker_codes,
        };
    }
    let durable_state_report = hawdb_lightning_initial_import_durable_state_report(
        manifest,
        &progress_report.checkpoint,
        &document_identities,
    );
    let completed = progress_report.checkpoint.completed_batches == total_batches;
    HawDBLightningInitialImportStreamingBatchAdvanceReport {
        accepted: durable_state_report.persistable,
        idempotent_replay,
        completed,
        ready_for_cutover: completed && durable_state_report.ready_for_cutover,
        durable_state_report,
        blocker_codes: Vec::new(),
    }
}

pub fn hawdb_lightning_initial_import_session_report(
    encoded_graph_stream: &str,
    encoded_relational_stream: &[u8],
    manifest: &HawDBLightningBootstrapManifest,
    target_graph_commit_epoch: u64,
    projection_freshness: Option<&SearchProjectionFreshness>,
    durable_state: Option<&HawDBLightningInitialImportDurableState>,
) -> HawDBLightningInitialImportSessionReport {
    let checkpoint = durable_state.map(|state| &state.checkpoint);
    let document_identities = durable_state.map(|state| state.document_identities.as_slice());
    let plan = hawdb_lightning_initial_import_plan_with_document_identities(
        encoded_graph_stream,
        encoded_relational_stream,
        manifest,
        target_graph_commit_epoch,
        projection_freshness,
        checkpoint,
        document_identities,
    );
    let durable_state_report = durable_state.map(|state| {
        hawdb_lightning_initial_import_durable_state_report(
            manifest,
            &state.checkpoint,
            &state.document_identities,
        )
    });
    let durable_state_source_matches_manifest = durable_state
        .map(|state| {
            state.source_fingerprint == hawdb_lightning_initial_import_source_fingerprint(manifest)
        })
        .unwrap_or(true);
    let mut blocker_codes = BTreeSet::new();
    blocker_codes.extend(plan.blocker_codes.iter().cloned());
    if !durable_state_source_matches_manifest {
        blocker_codes.insert("initial_import_durable_state_source_mismatch".to_string());
    }
    if let Some(report) = &durable_state_report
        && !report.persistable
    {
        blocker_codes.extend(report.blocker_codes.iter().cloned());
    }

    let next_action = if !durable_state_source_matches_manifest {
        HawDBLightningInitialImportResumeAction {
            kind: HawDBLightningInitialImportResumeActionKind::Quarantine,
            next_batch: None,
            idempotency_key: None,
            completed_batches: durable_state
                .map(|state| state.checkpoint.completed_batches)
                .unwrap_or(0),
            total_batches: durable_state
                .map(|state| state.checkpoint.total_batches)
                .unwrap_or(0),
            blocker_codes: vec!["initial_import_durable_state_source_mismatch".to_string()],
        }
    } else {
        plan.resume_action.clone()
    };
    let ready_for_database_import =
        plan.ready_for_database_import && durable_state_source_matches_manifest;
    let ready_for_cutover = plan.ready_for_cutover
        && durable_state_source_matches_manifest
        && durable_state_report
            .as_ref()
            .is_some_and(|report| report.ready_for_cutover);

    HawDBLightningInitialImportSessionReport {
        ready_for_database_import,
        ready_for_cutover,
        durable_state_present: durable_state.is_some(),
        durable_state_source_matches_manifest,
        plan,
        durable_state_report,
        next_action,
        blocker_codes: blocker_codes.into_iter().collect(),
    }
}

pub fn hawdb_lightning_initial_import_cutover_catch_up_report(
    session: &HawDBLightningInitialImportSessionReport,
    live_graph_commit_epoch: u64,
    live_projection_freshness: Option<&SearchProjectionFreshness>,
) -> HawDBLightningInitialImportCutoverCatchUpReport {
    let durable_state = session
        .durable_state_report
        .as_ref()
        .and_then(|report| report.state.as_ref());
    let import_graph_commit_epoch =
        durable_state.map(|state| state.checkpoint.applied_graph_commit_epoch);
    let import_durable_search_projection_commit_epoch =
        durable_state.and_then(|state| state.checkpoint.durable_search_projection_commit_epoch);
    let live_projection_present = live_projection_freshness.is_some();
    let import_projection_provenance_matches = live_projection_freshness
        .zip(import_graph_commit_epoch)
        .is_some_and(|(freshness, import_epoch)| {
            freshness.import_source_graph_commit_epoch == Some(import_epoch)
        });
    let live_search_projection_commit_epoch =
        live_projection_freshness.and_then(|freshness| freshness.source_graph_commit_epoch);
    let live_durable_search_projection_commit_epoch =
        live_projection_freshness.and_then(|freshness| freshness.durable_source_graph_commit_epoch);
    let live_projection_checkpointed = live_projection_freshness
        .map(|freshness| !freshness.has_uncheckpointed_changes)
        .unwrap_or(false);
    let live_projection_healthy = live_projection_freshness
        .map(|freshness| !freshness.full_reindex_needed && !freshness.metadata_repair_needed)
        .unwrap_or(false);
    // `import_*` epochs name the frozen legacy source. `live_*` epochs name
    // the post-import local WAL. They are separate domains and must not be
    // compared numerically. The source is checked by immutable provenance;
    // live catch-up is checked only against the local durable cursor.
    let graph_watermark_caught_up = session.ready_for_cutover
        && durable_state.is_some()
        && import_projection_provenance_matches;
    let search_projection_watermark_caught_up = live_durable_search_projection_commit_epoch
        .is_some_and(|live_epoch| live_epoch >= live_graph_commit_epoch);
    let cutover_watermark = if graph_watermark_caught_up
        && search_projection_watermark_caught_up
        && live_projection_checkpointed
        && live_projection_healthy
    {
        Some(live_graph_commit_epoch)
    } else {
        None
    };

    let mut blocker_codes = BTreeSet::new();
    if !session.ready_for_cutover {
        blocker_codes.insert("initial_import_session_not_ready_for_cutover".to_string());
    }
    if durable_state.is_none() {
        blocker_codes.insert("initial_import_durable_state_missing".to_string());
    }
    if !live_projection_present {
        blocker_codes.insert("initial_import_live_projection_missing".to_string());
    }
    if !graph_watermark_caught_up {
        blocker_codes.insert("initial_import_live_graph_watermark_not_caught_up".to_string());
    }
    if live_projection_present && !import_projection_provenance_matches {
        blocker_codes.insert("initial_import_projection_provenance_mismatch".to_string());
    }
    if !search_projection_watermark_caught_up {
        blocker_codes
            .insert("initial_import_live_search_projection_watermark_not_caught_up".to_string());
    }
    if live_projection_present && !live_projection_checkpointed {
        blocker_codes.insert("initial_import_live_projection_not_checkpointed".to_string());
    }
    if live_projection_present && !live_projection_healthy {
        blocker_codes.insert("initial_import_live_projection_repair_required".to_string());
    }

    HawDBLightningInitialImportCutoverCatchUpReport {
        ready: blocker_codes.is_empty(),
        session_ready_for_cutover: session.ready_for_cutover,
        durable_state_present: durable_state.is_some(),
        live_projection_present,
        import_graph_commit_epoch,
        import_durable_search_projection_commit_epoch,
        live_graph_commit_epoch,
        live_search_projection_commit_epoch,
        live_durable_search_projection_commit_epoch,
        graph_watermark_caught_up,
        search_projection_watermark_caught_up,
        live_projection_checkpointed,
        live_projection_healthy,
        cutover_watermark,
        blocker_codes: blocker_codes.into_iter().collect(),
    }
}

pub fn hawdb_lightning_initial_import_session_bundle_readiness(
    source_bundle: &HawDBLightningInitialImportSourceBundleReadiness,
    session: &HawDBLightningInitialImportSessionReport,
    catch_up: Option<&HawDBLightningInitialImportCutoverCatchUpReport>,
) -> HawDBLightningInitialImportSessionBundleReadiness {
    let catch_up_required = session.ready_for_cutover;
    let catch_up_present = catch_up.is_some();
    let catch_up_ready = catch_up.is_some_and(|report| report.ready);
    let cutover_watermark = catch_up.and_then(|report| report.cutover_watermark);
    let resumable = source_bundle.ready
        && session.ready_for_database_import
        && session.durable_state_present
        && session.durable_state_source_matches_manifest
        && session.next_action.kind != HawDBLightningInitialImportResumeActionKind::Quarantine;
    let ready_for_cutover = resumable && session.ready_for_cutover && catch_up_ready;
    let mut blocker_codes = BTreeSet::new();
    if !source_bundle.ready {
        blocker_codes.extend(source_bundle.blocker_codes.iter().cloned());
        blocker_codes.insert("initial_import_session_bundle_source_not_ready".to_string());
    }
    if !session.ready_for_database_import {
        blocker_codes.insert("initial_import_session_bundle_database_import_not_ready".to_string());
    }
    if !session.durable_state_present {
        blocker_codes.insert("initial_import_session_bundle_durable_state_missing".to_string());
    }
    if !session.durable_state_source_matches_manifest {
        blocker_codes.insert("initial_import_session_bundle_source_mismatch".to_string());
    }
    if session.next_action.kind == HawDBLightningInitialImportResumeActionKind::Quarantine {
        blocker_codes.insert("initial_import_session_bundle_quarantine_required".to_string());
    }
    blocker_codes.extend(session.blocker_codes.iter().cloned());
    if catch_up_required {
        if !catch_up_present {
            blocker_codes
                .insert("initial_import_session_bundle_cutover_catch_up_missing".to_string());
        } else if !catch_up_ready {
            blocker_codes
                .insert("initial_import_session_bundle_cutover_catch_up_not_ready".to_string());
        }
    }
    if let Some(report) = catch_up
        && !report.ready
    {
        blocker_codes.extend(report.blocker_codes.iter().cloned());
    }
    HawDBLightningInitialImportSessionBundleReadiness {
        ready: blocker_codes.is_empty(),
        resumable,
        ready_for_cutover,
        source_bundle_ready: source_bundle.ready,
        session_ready_for_database_import: session.ready_for_database_import,
        session_ready_for_cutover: session.ready_for_cutover,
        durable_state_present: session.durable_state_present,
        durable_state_source_matches_manifest: session.durable_state_source_matches_manifest,
        catch_up_required,
        catch_up_present,
        catch_up_ready,
        cutover_watermark,
        next_action: session.next_action.clone(),
        blocker_codes: blocker_codes.into_iter().collect(),
    }
}

pub fn hawdb_lightning_initial_import_startup_readiness(
    inputs: HawDBLightningInitialImportReadinessInputs<'_>,
    target_graph_commit_epoch: u64,
    durable_state: Option<&HawDBLightningInitialImportDurableState>,
) -> HawDBLightningInitialImportStartupReadinessReport {
    let checkpoint = durable_state.map(|state| &state.checkpoint);
    let source_bundle = hawdb_lightning_initial_import_source_bundle_readiness(
        inputs.manifest,
        checkpoint,
        inputs.projection_batches,
    );
    let session = hawdb_lightning_initial_import_session_report(
        inputs.encoded_graph_stream,
        inputs.encoded_relational_stream,
        inputs.manifest,
        target_graph_commit_epoch,
        inputs.target_projection_freshness,
        durable_state,
    );
    let cutover_catch_up = session.ready_for_cutover.then(|| {
        hawdb_lightning_initial_import_cutover_catch_up_report(
            &session,
            target_graph_commit_epoch,
            inputs.live_projection_freshness,
        )
    });
    let readiness = hawdb_lightning_initial_import_session_bundle_readiness(
        &source_bundle,
        &session,
        cutover_catch_up.as_ref(),
    );
    HawDBLightningInitialImportStartupReadinessReport {
        ready: readiness.ready,
        blocker_codes: readiness.blocker_codes.clone(),
        source_bundle,
        session,
        cutover_catch_up,
        readiness,
    }
}

pub fn hawdb_lightning_initial_import_recovery_readiness(
    inputs: HawDBLightningInitialImportReadinessInputs<'_>,
    target_graph_commit_epoch: u64,
    durable_state_payload: Option<&str>,
) -> HawDBLightningInitialImportRecoveryReadinessReport {
    let durable_state_payload_present = durable_state_payload.is_some();
    let mut decode_blocker_codes = BTreeSet::new();
    let durable_state_codec = durable_state_payload.and_then(|payload| {
        match hawdb_lightning_initial_import_decode_durable_state(inputs.manifest, payload) {
            Ok(report) => {
                if !report.ready {
                    decode_blocker_codes.extend(report.blocker_codes.iter().cloned());
                }
                Some(report)
            }
            Err(_) => {
                decode_blocker_codes
                    .insert("initial_import_durable_state_codec_decode_failed".to_string());
                None
            }
        }
    });
    let durable_state = durable_state_codec
        .as_ref()
        .filter(|report| report.ready)
        .and_then(|report| report.state.as_ref());
    let startup = hawdb_lightning_initial_import_startup_readiness(
        inputs,
        target_graph_commit_epoch,
        durable_state,
    );
    let invalid_payload = durable_state_payload_present && durable_state.is_none();
    let next_action = if invalid_payload {
        HawDBLightningInitialImportResumeAction {
            kind: HawDBLightningInitialImportResumeActionKind::Quarantine,
            next_batch: None,
            idempotency_key: None,
            completed_batches: 0,
            total_batches: 0,
            blocker_codes: decode_blocker_codes.iter().cloned().collect(),
        }
    } else {
        startup.readiness.next_action.clone()
    };
    let mut blocker_codes = BTreeSet::new();
    blocker_codes.extend(startup.blocker_codes.iter().cloned());
    blocker_codes.extend(decode_blocker_codes);
    if invalid_payload {
        blocker_codes
            .insert("initial_import_recovery_durable_state_quarantine_required".to_string());
    }
    HawDBLightningInitialImportRecoveryReadinessReport {
        ready: !invalid_payload && startup.ready,
        durable_state_payload_present,
        durable_state_codec,
        startup,
        next_action,
        blocker_codes: blocker_codes.into_iter().collect(),
    }
}

fn hawdb_lightning_initial_import_source_fingerprint_json(
    fingerprint: &HawDBLightningInitialImportSourceFingerprint,
) -> serde_json::Value {
    serde_json::json!({
        "protocol_version": fingerprint.protocol_version,
        "database_commit_epoch": fingerprint.database_commit_epoch,
        "graph_commit_epoch": fingerprint.graph_commit_epoch,
        "logical_checksum": fingerprint.logical_checksum,
        "graph_stream_checksum": fingerprint.graph_stream_checksum,
        "graph_stream_byte_len": fingerprint.graph_stream_byte_len,
        "relational_stream_checksum": fingerprint.relational_stream_checksum,
        "relational_stream_byte_len": fingerprint.relational_stream_byte_len,
        "schema_checksum": fingerprint.schema_checksum,
        "node_count": fingerprint.node_count,
        "relationship_count": fingerprint.relationship_count,
        "relational_table_count": fingerprint.relational_table_count,
        "relational_row_count": fingerprint.relational_row_count,
    })
}

fn hawdb_lightning_initial_import_checkpoint_json(
    checkpoint: &HawDBLightningInitialImportCheckpoint,
) -> serde_json::Value {
    serde_json::json!({
        "protocol_version": checkpoint.protocol_version,
        "import_id": checkpoint.import_id,
        "task_id": checkpoint.task_id,
        "fencing_token": checkpoint.fencing_token,
        "object_digest": checkpoint.object_digest,
        "schema_checksum": checkpoint.schema_checksum,
        "graph_stream_checksum": checkpoint.graph_stream_checksum,
        "graph_stream_byte_len": checkpoint.graph_stream_byte_len,
        "relational_stream_checksum": checkpoint.relational_stream_checksum,
        "relational_stream_byte_len": checkpoint.relational_stream_byte_len,
        "manifest_database_commit_epoch": checkpoint.manifest_database_commit_epoch,
        "manifest_graph_commit_epoch": checkpoint.manifest_graph_commit_epoch,
        "applied_graph_commit_epoch": checkpoint.applied_graph_commit_epoch,
        "applied_search_projection_commit_epoch": checkpoint.applied_search_projection_commit_epoch,
        "durable_search_projection_commit_epoch": checkpoint.durable_search_projection_commit_epoch,
        "completed_batches": checkpoint.completed_batches,
        "total_batches": checkpoint.total_batches,
        "document_identity_count": checkpoint.document_identity_count,
    })
}

fn hawdb_lightning_initial_import_document_identity_json(
    identity: &HawDBLightningInitialImportDocumentIdentity,
) -> serde_json::Value {
    serde_json::json!({
        "kind": identity.kind.as_str(),
        "document_id": identity.document_id,
    })
}

fn hawdb_lightning_initial_import_document_identity_coverage_json(
    coverage: &HawDBLightningInitialImportDocumentIdentityCoverage,
) -> serde_json::Value {
    serde_json::json!({
        "ready": coverage.ready,
        "document_identity_count": coverage.document_identity_count,
        "unique_document_identity_count": coverage.unique_document_identity_count,
        "expected_kinds": coverage
            .expected_kinds
            .iter()
            .map(|kind| kind.as_str())
            .collect::<Vec<_>>(),
        "observed_kinds": coverage
            .observed_kinds
            .iter()
            .map(|kind| kind.as_str())
            .collect::<Vec<_>>(),
        "kind_reports": coverage
            .kind_reports
            .iter()
            .map(|report| {
                serde_json::json!({
                    "kind": report.kind.as_str(),
                    "document_count": report.document_count,
                })
            })
            .collect::<Vec<_>>(),
        "missing_kinds": coverage
            .missing_kinds
            .iter()
            .map(|kind| kind.as_str())
            .collect::<Vec<_>>(),
        "empty_document_id_count": coverage.empty_document_id_count,
        "blocker_codes": coverage.blocker_codes,
    })
}

fn parse_hawdb_lightning_initial_import_source_fingerprint(
    value: &serde_json::Value,
) -> Result<HawDBLightningInitialImportSourceFingerprint> {
    Ok(HawDBLightningInitialImportSourceFingerprint {
        protocol_version: required_json_u64(value, "protocol_version")?,
        database_commit_epoch: required_json_u64(value, "database_commit_epoch")?,
        graph_commit_epoch: required_json_u64(value, "graph_commit_epoch")?,
        logical_checksum: required_json_u64(value, "logical_checksum")?,
        graph_stream_checksum: required_json_u64(value, "graph_stream_checksum")?,
        graph_stream_byte_len: required_json_usize(value, "graph_stream_byte_len")?,
        relational_stream_checksum: required_json_u64(value, "relational_stream_checksum")?,
        relational_stream_byte_len: required_json_usize(value, "relational_stream_byte_len")?,
        schema_checksum: required_json_u64(value, "schema_checksum")?,
        node_count: required_json_usize(value, "node_count")?,
        relationship_count: required_json_usize(value, "relationship_count")?,
        relational_table_count: required_json_usize(value, "relational_table_count")?,
        relational_row_count: required_json_usize(value, "relational_row_count")?,
    })
}

fn parse_hawdb_lightning_initial_import_checkpoint(
    value: &serde_json::Value,
) -> Result<HawDBLightningInitialImportCheckpoint> {
    Ok(HawDBLightningInitialImportCheckpoint {
        protocol_version: required_json_u64(value, "protocol_version")?,
        import_id: required_json_string(value, "import_id")?.to_string(),
        task_id: required_json_string(value, "task_id")?.to_string(),
        fencing_token: required_json_string(value, "fencing_token")?.to_string(),
        object_digest: required_json_string(value, "object_digest")?.to_string(),
        schema_checksum: required_json_u64(value, "schema_checksum")?,
        graph_stream_checksum: required_json_u64(value, "graph_stream_checksum")?,
        graph_stream_byte_len: required_json_usize(value, "graph_stream_byte_len")?,
        relational_stream_checksum: required_json_u64(value, "relational_stream_checksum")?,
        relational_stream_byte_len: required_json_usize(value, "relational_stream_byte_len")?,
        manifest_database_commit_epoch: required_json_u64(value, "manifest_database_commit_epoch")?,
        manifest_graph_commit_epoch: required_json_u64(value, "manifest_graph_commit_epoch")?,
        applied_graph_commit_epoch: required_json_u64(value, "applied_graph_commit_epoch")?,
        applied_search_projection_commit_epoch: optional_json_u64(
            value,
            "applied_search_projection_commit_epoch",
        )?,
        durable_search_projection_commit_epoch: optional_json_u64(
            value,
            "durable_search_projection_commit_epoch",
        )?,
        completed_batches: required_json_u64(value, "completed_batches")?,
        total_batches: required_json_u64(value, "total_batches")?,
        document_identity_count: required_json_usize(value, "document_identity_count")?,
    })
}

fn parse_hawdb_lightning_initial_import_document_identities(
    items: &[serde_json::Value],
) -> Result<Vec<HawDBLightningInitialImportDocumentIdentity>> {
    items
        .iter()
        .map(|value| {
            Ok(HawDBLightningInitialImportDocumentIdentity {
                kind: parse_search_projection_kind(required_json_string(value, "kind")?)?,
                document_id: required_json_string(value, "document_id")?.to_string(),
            })
        })
        .collect()
}

fn parse_search_projection_kind(raw: &str) -> Result<SearchProjectionKind> {
    match raw {
        "memory" => Ok(SearchProjectionKind::Memory),
        "message" => Ok(SearchProjectionKind::Message),
        "entity" => Ok(SearchProjectionKind::Entity),
        "source" => Ok(SearchProjectionKind::Source),
        "source_chunk" => Ok(SearchProjectionKind::SourceChunk),
        "community" => Ok(SearchProjectionKind::Community),
        _ => Err(HawDBError::Semantic(
            "initial import durable state field kind is invalid".to_string(),
        )),
    }
}

fn required_json_object<'a>(
    value: &'a serde_json::Value,
    field: &str,
) -> Result<&'a serde_json::Value> {
    let item = value
        .get(field)
        .ok_or_else(|| durable_state_codec_invalid_field(field, "object"))?;
    if item.is_object() {
        Ok(item)
    } else {
        Err(durable_state_codec_invalid_field(field, "object"))
    }
}

fn required_json_array<'a>(
    value: &'a serde_json::Value,
    field: &str,
) -> Result<&'a [serde_json::Value]> {
    value
        .get(field)
        .and_then(serde_json::Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| durable_state_codec_invalid_field(field, "array"))
}

fn required_json_string<'a>(value: &'a serde_json::Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| durable_state_codec_invalid_field(field, "string"))
}

fn required_json_u64(value: &serde_json::Value, field: &str) -> Result<u64> {
    value
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| durable_state_codec_invalid_field(field, "u64"))
}

fn required_json_usize(value: &serde_json::Value, field: &str) -> Result<usize> {
    required_json_u64(value, field)?
        .try_into()
        .map_err(|_| durable_state_codec_invalid_field(field, "usize"))
}

fn optional_json_u64(value: &serde_json::Value, field: &str) -> Result<Option<u64>> {
    match value.get(field) {
        Some(serde_json::Value::Null) | None => Ok(None),
        Some(item) => item
            .as_u64()
            .map(Some)
            .ok_or_else(|| durable_state_codec_invalid_field(field, "optional_u64")),
    }
}

fn durable_state_codec_invalid_field(field: &str, expected: &str) -> HawDBError {
    HawDBError::Semantic(format!(
        "initial import durable state field {field} is invalid: expected_{expected}"
    ))
}

pub fn hawdb_lightning_initial_import_source_fingerprint(
    manifest: &HawDBLightningBootstrapManifest,
) -> HawDBLightningInitialImportSourceFingerprint {
    HawDBLightningInitialImportSourceFingerprint {
        protocol_version: manifest.protocol_version,
        database_commit_epoch: manifest.database_commit_epoch,
        graph_commit_epoch: manifest.graph_commit_epoch,
        logical_checksum: manifest.logical_checksum,
        graph_stream_checksum: manifest.graph_stream_checksum,
        graph_stream_byte_len: manifest.graph_stream_byte_len,
        relational_stream_checksum: manifest.relational_stream_checksum,
        relational_stream_byte_len: manifest.relational_stream_byte_len,
        schema_checksum: manifest.schema_checksum,
        node_count: manifest.node_count,
        relationship_count: manifest.relationship_count,
        relational_table_count: manifest.relational_table_count,
        relational_row_count: manifest.relational_row_count,
    }
}

fn merge_initial_import_document_identities(
    existing: &[HawDBLightningInitialImportDocumentIdentity],
    incoming: &[HawDBLightningInitialImportDocumentIdentity],
) -> Vec<HawDBLightningInitialImportDocumentIdentity> {
    let mut identities = Vec::with_capacity(existing.len().saturating_add(incoming.len()));
    let mut seen = BTreeSet::new();
    for identity in existing.iter().chain(incoming.iter()) {
        if seen.insert((identity.kind, identity.document_id.clone())) {
            identities.push(identity.clone());
        }
    }
    identities
}

fn search_projection_delta_document_identities(
    delta: &SearchProjectionDelta,
) -> Vec<HawDBLightningInitialImportDocumentIdentity> {
    delta
        .upserts
        .iter()
        .map(|row| HawDBLightningInitialImportDocumentIdentity {
            kind: row.kind,
            document_id: format!("{}:{}", row.kind.as_str(), row.external_id),
        })
        .collect()
}

pub fn hawdb_lightning_initial_import_document_identity_coverage(
    identities: &[HawDBLightningInitialImportDocumentIdentity],
) -> HawDBLightningInitialImportDocumentIdentityCoverage {
    let expected_kinds = hawdb_lightning_initial_import_required_search_projection_kinds();
    let mut document_ids = BTreeMap::<String, usize>::new();
    let mut kind_counts = BTreeMap::<SearchProjectionKind, usize>::new();
    let mut empty_document_id_count = 0;
    for identity in identities {
        if identity.document_id.is_empty() {
            empty_document_id_count += 1;
        } else {
            *document_ids
                .entry(identity.document_id.clone())
                .or_default() += 1;
        }
        *kind_counts.entry(identity.kind).or_default() += 1;
    }
    let observed_kinds = kind_counts.keys().copied().collect::<Vec<_>>();
    let kind_reports = kind_counts
        .iter()
        .map(
            |(kind, document_count)| HawDBLightningInitialImportDocumentIdentityKindReport {
                kind: *kind,
                document_count: *document_count,
            },
        )
        .collect::<Vec<_>>();
    let missing_kinds = expected_kinds
        .iter()
        .copied()
        .filter(|kind| !kind_counts.contains_key(kind))
        .collect::<Vec<_>>();
    let duplicate_document_ids = document_ids
        .iter()
        .filter(|(_, count)| **count > 1)
        .map(|(document_id, _)| document_id.clone())
        .collect::<Vec<_>>();
    let mut blocker_codes = BTreeSet::new();
    if !missing_kinds.is_empty() {
        blocker_codes.insert("initial_import_document_identity_kind_missing".to_string());
    }
    if empty_document_id_count > 0 {
        blocker_codes.insert("initial_import_document_identity_empty".to_string());
    }
    if !duplicate_document_ids.is_empty() {
        blocker_codes.insert("initial_import_document_identity_duplicate".to_string());
    }
    let blocker_codes = blocker_codes.into_iter().collect::<Vec<_>>();
    HawDBLightningInitialImportDocumentIdentityCoverage {
        ready: blocker_codes.is_empty(),
        document_identity_count: identities.len(),
        unique_document_identity_count: document_ids.len(),
        expected_kinds,
        observed_kinds,
        kind_reports,
        missing_kinds,
        duplicate_document_ids,
        empty_document_id_count,
        blocker_codes,
    }
}

pub fn hawdb_lightning_initial_import_resume_action(
    manifest: &HawDBLightningBootstrapManifest,
    checkpoint: Option<&HawDBLightningInitialImportCheckpoint>,
) -> HawDBLightningInitialImportResumeAction {
    let Some(checkpoint) = checkpoint else {
        return HawDBLightningInitialImportResumeAction {
            kind: HawDBLightningInitialImportResumeActionKind::Start,
            next_batch: Some(0),
            idempotency_key: None,
            completed_batches: 0,
            total_batches: 0,
            blocker_codes: Vec::new(),
        };
    };
    let readiness = hawdb_lightning_initial_import_checkpoint_readiness(manifest, checkpoint);
    let hard_mismatch = readiness.blocker_codes.iter().any(|code| {
        code == "initial_import_checkpoint_manifest_mismatch"
            || code == "initial_import_checkpoint_idempotency_key_missing"
    });
    let kind = if hard_mismatch {
        HawDBLightningInitialImportResumeActionKind::Quarantine
    } else if readiness.ready {
        HawDBLightningInitialImportResumeActionKind::ReadyForCutover
    } else {
        HawDBLightningInitialImportResumeActionKind::Resume
    };
    let next_batch = match kind {
        HawDBLightningInitialImportResumeActionKind::Start => Some(0),
        HawDBLightningInitialImportResumeActionKind::Resume => {
            Some(checkpoint.completed_batches.min(checkpoint.total_batches))
        }
        HawDBLightningInitialImportResumeActionKind::ReadyForCutover
        | HawDBLightningInitialImportResumeActionKind::Quarantine => None,
    };
    HawDBLightningInitialImportResumeAction {
        kind,
        next_batch,
        idempotency_key: readiness.idempotency_key,
        completed_batches: checkpoint.completed_batches,
        total_batches: checkpoint.total_batches,
        blocker_codes: readiness.blocker_codes,
    }
}

fn hawdb_lightning_initial_import_idempotency_key(
    checkpoint: &HawDBLightningInitialImportCheckpoint,
) -> Option<HawDBLightningInitialImportIdempotencyKey> {
    if checkpoint.import_id.is_empty()
        || checkpoint.task_id.is_empty()
        || checkpoint.fencing_token.is_empty()
        || checkpoint.object_digest.is_empty()
    {
        return None;
    }
    Some(HawDBLightningInitialImportIdempotencyKey {
        import_id: checkpoint.import_id.clone(),
        task_id: checkpoint.task_id.clone(),
        fencing_token: checkpoint.fencing_token.clone(),
        object_digest: checkpoint.object_digest.clone(),
    })
}

fn optional_epoch_regressed(previous: Option<u64>, next: Option<u64>) -> bool {
    match (previous, next) {
        (Some(_), None) => true,
        (Some(previous), Some(next)) => next < previous,
        (None, _) => false,
    }
}

fn hawdb_lightning_initial_import_required_search_projection_kinds() -> Vec<SearchProjectionKind> {
    vec![
        SearchProjectionKind::Memory,
        SearchProjectionKind::Message,
        SearchProjectionKind::Entity,
        SearchProjectionKind::Source,
        SearchProjectionKind::SourceChunk,
        SearchProjectionKind::Community,
    ]
}

#[derive(Debug, Default)]
struct ParsedHawDBLightningGraphStream {
    format_version: Option<u64>,
    graph_commit_epoch: Option<u64>,
    logical_checksum: Option<u64>,
    declared_node_count: Option<u64>,
    declared_relationship_count: Option<u64>,
    node_ids: Vec<u64>,
    relationship_ids: Vec<u64>,
    relationships: Vec<(u64, u64, u64)>,
    nodes: Vec<CanonicalSnapshotNode>,
    snapshot_relationships: Vec<CanonicalSnapshotRelationship>,
}

fn parse_hawdb_lightning_graph_stream_body(
    body: &str,
    errors: &mut Vec<String>,
) -> ParsedHawDBLightningGraphStream {
    let mut cursor = GraphStreamCursor::new(body);
    let mut parsed = ParsedHawDBLightningGraphStream::default();

    match cursor.read_line() {
        Some("HAWDB_LIGHTNING_GRAPH_STREAM_V1") => {}
        Some(line) => {
            errors.push(format!("invalid graph stream header: {line}"));
            return parsed;
        }
        None => {
            errors.push("missing graph stream header".to_string());
            return parsed;
        }
    }

    parsed.format_version =
        cursor.read_tagged_u64("format_version", "graph stream format version", errors);
    parsed.graph_commit_epoch =
        cursor.read_tagged_u64("graph_commit_epoch", "graph stream commit epoch", errors);
    parsed.logical_checksum =
        cursor.read_tagged_u64("logical_checksum", "graph stream logical checksum", errors);
    parsed.declared_node_count =
        cursor.read_tagged_u64("node_count", "graph stream node count", errors);

    let node_count = parsed.declared_node_count.unwrap_or(0);
    for _ in 0..node_count {
        let node_id = cursor.read_node_id(errors);
        if let Some(node_id) = node_id {
            parsed.node_ids.push(node_id);
        }
        let stable_id = cursor.read_optional_canonical_value("stable_id", errors);
        let label_count = cursor
            .read_tagged_u64("label_count", "graph stream label count", errors)
            .unwrap_or(0);
        let mut labels = Vec::new();
        for _ in 0..label_count {
            if let Some(label) = cursor.read_canonical_string_line("label", errors) {
                labels.push(label);
            }
        }
        let properties = read_graph_stream_properties(&mut cursor, errors);
        if let Some(node_id) = node_id {
            parsed.nodes.push(CanonicalSnapshotNode {
                node_id,
                stable_id,
                labels,
                properties,
            });
        }
    }

    parsed.declared_relationship_count = cursor.read_tagged_u64(
        "relationship_count",
        "graph stream relationship count",
        errors,
    );
    let relationship_count = parsed.declared_relationship_count.unwrap_or(0);
    for _ in 0..relationship_count {
        let relationship = cursor.read_relationship(errors);
        if let Some((relationship_id, source, target)) = relationship {
            parsed.relationship_ids.push(relationship_id);
            parsed.relationships.push((relationship_id, source, target));
        }
        let stable_id = cursor.read_optional_canonical_value("stable_id", errors);
        let rel_type = cursor
            .read_canonical_string_line("relationship_type", errors)
            .unwrap_or_default();
        let properties = read_graph_stream_properties(&mut cursor, errors);
        if let Some((relationship_id, source, target)) = relationship {
            parsed
                .snapshot_relationships
                .push(CanonicalSnapshotRelationship {
                    relationship_id,
                    stable_id,
                    source_node_id: source,
                    target_node_id: target,
                    rel_type,
                    properties,
                });
        }
    }

    if !cursor.is_finished() {
        let remaining = cursor.remaining_preview();
        errors.push(format!("trailing graph stream data: {remaining}"));
    }

    parsed.relationships = parsed
        .snapshot_relationships
        .iter()
        .map(|relationship| {
            (
                relationship.relationship_id,
                relationship.source_node_id,
                relationship.target_node_id,
            )
        })
        .collect();
    parsed
}

fn read_graph_stream_properties(
    cursor: &mut GraphStreamCursor<'_>,
    errors: &mut Vec<String>,
) -> BTreeMap<String, Value> {
    let property_count = cursor
        .read_tagged_u64("property_count", "graph stream property count", errors)
        .unwrap_or(0);
    let mut properties = BTreeMap::new();
    for _ in 0..property_count {
        let property = cursor.read_canonical_string_line("property", errors);
        let value = cursor.read_canonical_value(errors);
        cursor.expect_byte(b'\n', "graph stream property value terminator", errors);
        if let (Some(property), Some(value)) = (property, value) {
            properties.insert(property, value);
        }
    }
    properties
}

struct GraphStreamCursor<'a> {
    input: &'a str,
    offset: usize,
}

impl<'a> GraphStreamCursor<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, offset: 0 }
    }

    fn is_finished(&self) -> bool {
        self.offset >= self.input.len()
    }

    fn remaining_preview(&self) -> String {
        self.input[self.offset..]
            .chars()
            .take(64)
            .collect::<String>()
            .replace('\n', "\\n")
    }

    fn read_line(&mut self) -> Option<&'a str> {
        if self.is_finished() {
            return None;
        }
        let remaining = &self.input[self.offset..];
        if let Some(line_len) = remaining.find('\n') {
            let start = self.offset;
            let end = start + line_len;
            self.offset = end + 1;
            Some(&self.input[start..end])
        } else {
            let start = self.offset;
            self.offset = self.input.len();
            Some(&self.input[start..])
        }
    }

    fn read_tagged_u64(&mut self, tag: &str, name: &str, errors: &mut Vec<String>) -> Option<u64> {
        let Some(line) = self.read_line() else {
            errors.push(format!("missing {name}"));
            return None;
        };
        let Some(raw) = line
            .strip_prefix(tag)
            .and_then(|line| line.strip_prefix('\t'))
        else {
            errors.push(format!("expected {tag} line, found {line}"));
            return None;
        };
        parse_api_u64(raw, name, errors)
    }

    fn read_node_id(&mut self, errors: &mut Vec<String>) -> Option<u64> {
        self.read_tagged_u64("node", "graph stream node id", errors)
    }

    fn read_relationship(&mut self, errors: &mut Vec<String>) -> Option<(u64, u64, u64)> {
        let Some(line) = self.read_line() else {
            errors.push("missing graph stream relationship".to_string());
            return None;
        };
        let fields = line.split('\t').collect::<Vec<_>>();
        let ["relationship", raw_id, raw_source, raw_target] = fields.as_slice() else {
            errors.push(format!("invalid graph stream relationship line: {line}"));
            return None;
        };
        let id = parse_api_u64(raw_id, "graph stream relationship id", errors);
        let source = parse_api_u64(raw_source, "graph stream relationship source", errors);
        let target = parse_api_u64(raw_target, "graph stream relationship target", errors);
        match (id, source, target) {
            (Some(id), Some(source), Some(target)) => Some((id, source, target)),
            _ => None,
        }
    }

    fn read_optional_canonical_value(
        &mut self,
        prefix: &str,
        errors: &mut Vec<String>,
    ) -> Option<Value> {
        if !self.expect_str(prefix, errors) {
            return None;
        }
        if !self.expect_byte(b'\t', "graph stream optional value separator", errors) {
            return None;
        }
        let value = if self.remaining().starts_with("missing") {
            self.offset += "missing".len();
            None
        } else {
            self.read_canonical_value(errors)
        };
        self.expect_byte(b'\n', "graph stream optional value terminator", errors);
        value
    }

    fn read_canonical_string_line(
        &mut self,
        prefix: &str,
        errors: &mut Vec<String>,
    ) -> Option<String> {
        if !self.expect_str(prefix, errors) {
            return None;
        }
        if !self.expect_byte(b'\t', "graph stream canonical string separator", errors) {
            return None;
        }
        let value = self.read_length_prefixed_string("graph stream canonical string", errors);
        self.expect_byte(b'\n', "graph stream canonical string terminator", errors);
        value
    }

    fn read_canonical_value(&mut self, errors: &mut Vec<String>) -> Option<Value> {
        if self.remaining().starts_with("null") {
            self.offset += "null".len();
            Some(Value::Null)
        } else if self.remaining().starts_with("bool:true") {
            self.offset += "bool:true".len();
            Some(Value::Bool(true))
        } else if self.remaining().starts_with("bool:false") {
            self.offset += "bool:false".len();
            Some(Value::Bool(false))
        } else if self.remaining().starts_with("int:") {
            self.offset += "int:".len();
            self.read_scalar_token()
                .and_then(|raw| match raw.parse::<i64>() {
                    Ok(value) => Some(Value::Int(value)),
                    Err(_) => {
                        errors.push(format!("invalid graph stream int value: {raw}"));
                        None
                    }
                })
        } else if self.remaining().starts_with("float:") {
            self.offset += "float:".len();
            self.read_scalar_token()
                .and_then(|raw| match u64::from_str_radix(raw, 16) {
                    Ok(bits) => Some(Value::Float(f64::from_bits(bits))),
                    Err(_) => {
                        errors.push(format!("invalid graph stream float value: {raw}"));
                        None
                    }
                })
        } else if self.remaining().starts_with("string:") {
            self.offset += "string:".len();
            self.read_length_prefixed_string("graph stream string value", errors)
                .map(Value::String)
        } else if self.remaining().starts_with("list:") {
            self.offset += "list:".len();
            let count = self.parse_decimal("graph stream list item count", errors);
            if !self.expect_byte(b':', "graph stream list count separator", errors)
                || !self.expect_byte(b'[', "graph stream list opener", errors)
            {
                return None;
            }
            let mut values = Vec::new();
            for _ in 0..count.unwrap_or(0) {
                if let Some(value) = self.read_canonical_value(errors) {
                    values.push(value);
                }
                self.expect_byte(b';', "graph stream list item terminator", errors);
            }
            self.expect_byte(b']', "graph stream list closer", errors);
            Some(Value::List(values))
        } else if self.remaining().starts_with("map:") {
            self.offset += "map:".len();
            let count = self.parse_decimal("graph stream map item count", errors);
            if !self.expect_byte(b':', "graph stream map count separator", errors)
                || !self.expect_byte(b'{', "graph stream map opener", errors)
            {
                return None;
            }
            let mut values = BTreeMap::new();
            for _ in 0..count.unwrap_or(0) {
                let key = self.read_length_prefixed_string("graph stream map key", errors);
                if !self.expect_byte(b'=', "graph stream map key separator", errors) {
                    return None;
                }
                let value = self.read_canonical_value(errors);
                self.expect_byte(b';', "graph stream map item terminator", errors);
                if let (Some(key), Some(value)) = (key, value) {
                    values.insert(key, value);
                }
            }
            self.expect_byte(b'}', "graph stream map closer", errors);
            Some(Value::Map(values))
        } else {
            errors.push(format!(
                "invalid graph stream canonical value: {}",
                self.remaining_preview()
            ));
            None
        }
    }

    fn read_length_prefixed_string(
        &mut self,
        name: &str,
        errors: &mut Vec<String>,
    ) -> Option<String> {
        let len = self.parse_decimal(name, errors)?;
        if !self.expect_byte(b':', "graph stream length separator", errors) {
            return None;
        }
        let end = self.offset.saturating_add(len as usize);
        if end > self.input.len() {
            errors.push(format!("{name} exceeds graph stream length"));
            self.offset = self.input.len();
            return None;
        }
        if !self.input.is_char_boundary(end) {
            errors.push(format!("{name} ends inside a UTF-8 codepoint"));
            self.offset = self.input.len();
            return None;
        }
        let value = self.input[self.offset..end].to_string();
        self.offset = end;
        Some(value)
    }

    fn read_scalar_token(&mut self) -> Option<&'a str> {
        let start = self.offset;
        while let Some(byte) = self.current_byte() {
            if matches!(byte, b';' | b'\n' | b']' | b'}') {
                break;
            }
            self.offset += 1;
        }
        (start != self.offset).then_some(&self.input[start..self.offset])
    }

    fn parse_decimal(&mut self, name: &str, errors: &mut Vec<String>) -> Option<u64> {
        let start = self.offset;
        while let Some(byte) = self.current_byte() {
            if !byte.is_ascii_digit() {
                break;
            }
            self.offset += 1;
        }
        if start == self.offset {
            errors.push(format!("missing {name}"));
            return None;
        }
        match self.input[start..self.offset].parse::<u64>() {
            Ok(value) => Some(value),
            Err(_) => {
                errors.push(format!(
                    "invalid {name}: {}",
                    &self.input[start..self.offset]
                ));
                None
            }
        }
    }

    fn expect_str(&mut self, expected: &str, errors: &mut Vec<String>) -> bool {
        if self.remaining().starts_with(expected) {
            self.offset += expected.len();
            true
        } else {
            errors.push(format!(
                "expected {expected}, found {}",
                self.remaining_preview()
            ));
            false
        }
    }

    fn expect_byte(&mut self, expected: u8, name: &str, errors: &mut Vec<String>) -> bool {
        if self.current_byte() == Some(expected) {
            self.offset += 1;
            true
        } else {
            errors.push(format!("expected {name}"));
            false
        }
    }

    fn current_byte(&self) -> Option<u8> {
        self.input.as_bytes().get(self.offset).copied()
    }

    fn remaining(&self) -> &'a str {
        &self.input[self.offset..]
    }
}

fn split_graph_stream_checksum(encoded: &str) -> (&str, Option<u64>, Vec<String>) {
    let Some((body, footer)) = encoded.rsplit_once("checksum\t") else {
        return (
            encoded,
            None,
            vec!["graph stream missing checksum footer".to_string()],
        );
    };
    let raw = footer.trim();
    match raw.parse::<u64>() {
        Ok(checksum) => (body, Some(checksum), Vec::new()),
        Err(_) => (
            body,
            None,
            vec![format!("invalid graph stream checksum: {raw}")],
        ),
    }
}

fn parse_api_u64(input: &str, name: &str, errors: &mut Vec<String>) -> Option<u64> {
    match input.parse::<u64>() {
        Ok(value) => Some(value),
        Err(_) => {
            errors.push(format!("invalid {name}: {input}"));
            None
        }
    }
}

fn append_canonical_properties(body: &mut String, properties: &BTreeMap<String, Value>) {
    body.push_str(&format!("property_count\t{}\n", properties.len()));
    for (property, value) in properties {
        append_canonical_string(body, "property", property);
        append_canonical_value(body, value);
        body.push('\n');
    }
}

fn append_canonical_string(body: &mut String, prefix: &str, value: &str) {
    body.push_str(prefix);
    body.push('\t');
    body.push_str(&value.len().to_string());
    body.push(':');
    body.push_str(value);
    body.push('\n');
}

fn append_optional_canonical_value(body: &mut String, prefix: &str, value: Option<&Value>) {
    body.push_str(prefix);
    body.push('\t');
    match value {
        Some(value) => append_canonical_value(body, value),
        None => body.push_str("missing"),
    }
    body.push('\n');
}

fn append_canonical_value(body: &mut String, value: &Value) {
    match value {
        Value::Null => body.push_str("null"),
        Value::Bool(value) => body.push_str(if *value { "bool:true" } else { "bool:false" }),
        Value::Int(value) => body.push_str(&format!("int:{value}")),
        Value::Float(value) => body.push_str(&format!("float:{:016x}", value.to_bits())),
        Value::String(value) => {
            body.push_str("string:");
            body.push_str(&value.len().to_string());
            body.push(':');
            body.push_str(value);
        }
        Value::Binary(value) => {
            body.push_str("binary:");
            body.push_str(&value.len().to_string());
            body.push(':');
            append_hex(body, value);
        }
        Value::Uuid(value) => body.push_str(&format!("uuid:{value}")),
        Value::List(values) => {
            body.push_str(&format!("list:{}:[", values.len()));
            for value in values {
                append_canonical_value(body, value);
                body.push(';');
            }
            body.push(']');
        }
        Value::Map(values) => {
            body.push_str(&format!("map:{}:{{", values.len()));
            for (key, value) in values {
                body.push_str(&key.len().to_string());
                body.push(':');
                body.push_str(key);
                body.push('=');
                append_canonical_value(body, value);
                body.push(';');
            }
            body.push('}');
        }
    }
}

fn append_hex(output: &mut String, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        output.push(HEX[usize::from(byte >> 4)] as char);
        output.push(HEX[usize::from(byte & 0x0f)] as char);
    }
}

fn checksum_bytes(bytes: &[u8]) -> u64 {
    hawdb_integrity::checksum_u64(bytes)
}
