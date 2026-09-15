use super::super::*;

#[rustfmt::skip]
#[cfg(test)]
mod legacy_business_types {
use super::super::*;
use super::*;

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgePageRankScoreUpdate {
    pub label: String,
    pub external_id: String,
    pub score: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgePageRankScoreBatchRequest {
    pub updates: Vec<KnowledgePageRankScoreUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePageRankScoreBatchRow {
    pub label: String,
    pub external_id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub updated: bool,
    pub duplicate: bool,
    pub non_writable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePageRankScoreBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgePageRankScoreBatchRow>,
    pub matched_count: usize,
    pub missing_count: usize,
    pub duplicate_count: usize,
    pub non_writable_count: usize,
    pub updated_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePageRankClearRequest {
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePageRankClearRow {
    pub label: String,
    pub external_id: Option<String>,
    pub node_id: u64,
    pub cleared: bool,
    pub non_writable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePageRankClearOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgePageRankClearRow>,
    pub candidate_count: usize,
    pub cleared_count: usize,
    pub non_writable_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityAssignmentClearRequest {
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityAssignmentClearRow {
    pub labels: Vec<String>,
    pub external_id: Option<String>,
    pub node_id: u64,
    pub cleared: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityAssignmentClearOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeCommunityAssignmentClearRow>,
    pub candidate_count: usize,
    pub cleared_count: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeCommunityMembershipCreate {
    pub entity_id: String,
    pub community_id: String,
    pub strength: f64,
    pub created_at: Value,
    pub properties: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeCommunityMembershipCreateBatchRequest {
    pub memberships: Vec<KnowledgeCommunityMembershipCreate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityMembershipCreateBatchRow {
    pub entity_id: String,
    pub community_id: String,
    pub entity_node_id: Option<u64>,
    pub community_node_id: Option<u64>,
    pub matched: bool,
    pub non_writable: bool,
    pub created: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityMembershipCreateBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeCommunityMembershipCreateBatchRow>,
    pub matched_count: usize,
    pub missing_endpoint_count: usize,
    pub non_writable_count: usize,
    pub created_relationship_count: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeCommunityCreate {
    pub id: String,
    pub community_id: i64,
    pub name: String,
    pub description: Value,
    pub ai_summary: Value,
    pub member_count: i64,
    pub resolution: f64,
    pub created_at: Value,
    pub updated_at: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunitySummaryUpdate {
    pub id: String,
    pub name: String,
    pub description: Value,
    pub ai_summary: Value,
    pub updated_at: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeCommunityLifecycleBatchRequest {
    pub creates: Vec<KnowledgeCommunityCreate>,
    pub summary_updates: Vec<KnowledgeCommunitySummaryUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityCreateBatchRow {
    pub id: String,
    pub node_id: Option<u64>,
    pub created: bool,
    pub already_exists: bool,
    pub duplicate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunitySummaryUpdateBatchRow {
    pub id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub updated: bool,
    pub missing: bool,
    pub duplicate: bool,
    pub non_writable: bool,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityLifecycleBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub create_rows: Vec<KnowledgeCommunityCreateBatchRow>,
    pub summary_update_rows: Vec<KnowledgeCommunitySummaryUpdateBatchRow>,
    pub created_count: usize,
    pub already_exists_count: usize,
    pub duplicate_count: usize,
    pub updated_count: usize,
    pub missing_count: usize,
    pub non_writable_count: usize,
    pub created_node_count: usize,
    pub updated_property_count: usize,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeCommunityListOrder {
    MemberCountDesc,
    SummaryPresenceThenMemberCountDesc,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityListRequest {
    pub require_summary: bool,
    pub require_non_negative_community_id: bool,
    pub order: KnowledgeCommunityListOrder,
    pub limit: usize,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KnowledgeCommunityLookupKey {
    Id(String),
    CommunityId(i64),
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityRequest {
    pub key: KnowledgeCommunityLookupKey,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityRow {
    pub id: Option<String>,
    pub node_id: u64,
    pub community_id: Option<i64>,
    pub name: Option<String>,
    pub description: Option<Value>,
    pub ai_summary: Option<Value>,
    pub member_count: Option<i64>,
    pub updated_at: Option<Value>,
    pub has_summary: bool,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityListOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgeCommunityRow>,
    pub matched_count: usize,
    pub returned_count: usize,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityOutput {
    pub graph_commit_epoch: u64,
    pub row: Option<KnowledgeCommunityRow>,
    pub found: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityCleanupRequest {
    pub detach: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityCleanupRow {
    pub id: Option<String>,
    pub node_id: u64,
    pub deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityCleanupOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeCommunityCleanupRow>,
    pub candidate_count: usize,
    pub deleted_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeGraphMetaStamp {
    pub meta_id: String,
    pub assignments: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeGraphMetaStampBatchRequest {
    pub stamps: Vec<KnowledgeGraphMetaStamp>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeGraphMetaStampBatchRow {
    pub meta_id: String,
    pub node_id: Option<u64>,
    pub created: bool,
    pub updated: bool,
    pub duplicate: bool,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeGraphMetaStampBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeGraphMetaStampBatchRow>,
    pub created_count: usize,
    pub updated_count: usize,
    pub duplicate_count: usize,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeGraphMetaRequest {
    pub meta_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeGraphMetaDeleteOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSchemaMigrationApply {
    pub migration_id: String,
    pub applied_at: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSchemaMigrationApplyBatchRequest {
    pub migrations: Vec<KnowledgeSchemaMigrationApply>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSchemaMigrationApplyBatchRow {
    pub migration_id: String,
    pub node_id: Option<u64>,
    pub created: bool,
    pub already_applied: bool,
    pub duplicate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSchemaMigrationApplyBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeSchemaMigrationApplyBatchRow>,
    pub created_count: usize,
    pub already_applied_count: usize,
    pub duplicate_count: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum KnowledgeAugmentationJobLifecycleTransition {
    Create {
        job_type: String,
        parameters: Value,
        created_at: Value,
    },
    MarkRunning {
        started_at: Value,
    },
    UpdateProgress {
        progress: f64,
        message: String,
    },
    MarkCompleted {
        result: Value,
        completed_at: Value,
    },
    MarkFailed {
        error_message: String,
        completed_at: Value,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeAugmentationJobLifecycleUpdate {
    pub job_id: String,
    pub transition: KnowledgeAugmentationJobLifecycleTransition,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeAugmentationJobLifecycleBatchRequest {
    pub updates: Vec<KnowledgeAugmentationJobLifecycleUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeAugmentationJobLifecycleBatchRow {
    pub job_id: String,
    pub node_id: Option<u64>,
    pub created: bool,
    pub updated: bool,
    pub missing: bool,
    pub already_exists: bool,
    pub status_mismatch: bool,
    pub duplicate: bool,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeAugmentationJobLifecycleBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeAugmentationJobLifecycleBatchRow>,
    pub created_count: usize,
    pub updated_count: usize,
    pub missing_count: usize,
    pub already_exists_count: usize,
    pub status_mismatch_count: usize,
    pub duplicate_count: usize,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeAugmentationJobInterruptRequest {
    pub error_message: String,
    pub completed_at: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeAugmentationJobInterruptRow {
    pub job_id: Option<String>,
    pub node_id: u64,
    pub previous_status: String,
    pub interrupted: bool,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeAugmentationJobInterruptOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeAugmentationJobInterruptRow>,
    pub candidate_count: usize,
    pub interrupted_count: usize,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityDeleteRequest {
    pub entity: KnowledgeEntityRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedEntityDeleteRequest {
    pub delete: KnowledgeEntityDeleteRequest,
    pub metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityDeleteOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub filtered_out: bool,
    pub deleted_node_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityDeleteBatchRequest {
    pub label: String,
    pub external_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedEntityDeleteBatchRequest {
    pub delete: KnowledgeEntityDeleteBatchRequest,
    pub metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityDeleteBatchRow {
    pub external_id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub filtered_out: bool,
    pub non_writable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityDeleteBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeEntityDeleteBatchRow>,
    pub matched_count: usize,
    pub missing_count: usize,
    pub filtered_out_count: usize,
    pub non_writable_count: usize,
    pub deleted_node_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipCreateRequest {
    pub source: KnowledgeEntityRequest,
    pub target: KnowledgeEntityRequest,
    pub relationship_type: String,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedRelationshipCreateRequest {
    pub create: KnowledgeRelationshipCreateRequest,
    pub source_metadata_filters: BTreeMap<String, String>,
    pub target_metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipCreateOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub source_node_id: Option<u64>,
    pub target_node_id: Option<u64>,
    pub matched: bool,
    pub source_filtered_out: bool,
    pub target_filtered_out: bool,
    pub created_relationship_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipCreateBatchRequest {
    pub creates: Vec<KnowledgeRelationshipCreateRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedRelationshipCreateBatchRequest {
    pub creates: Vec<KnowledgeRelationshipCreateRequest>,
    pub source_metadata_filters: BTreeMap<String, String>,
    pub target_metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipCreateBatchRow {
    pub source: KnowledgeEntityRequest,
    pub target: KnowledgeEntityRequest,
    pub relationship_type: String,
    pub source_node_id: Option<u64>,
    pub target_node_id: Option<u64>,
    pub matched: bool,
    pub source_filtered_out: bool,
    pub target_filtered_out: bool,
    pub non_writable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipCreateBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeRelationshipCreateBatchRow>,
    pub matched_count: usize,
    pub missing_endpoint_count: usize,
    pub source_filtered_out_count: usize,
    pub target_filtered_out_count: usize,
    pub non_writable_count: usize,
    pub created_relationship_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipUpsertRequest {
    pub source: KnowledgeEntityRequest,
    pub target: KnowledgeEntityRequest,
    pub relationship_type: String,
    pub create_properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedRelationshipUpsertRequest {
    pub upsert: KnowledgeRelationshipUpsertRequest,
    pub source_metadata_filters: BTreeMap<String, String>,
    pub target_metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipUpsertOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub source_node_id: Option<u64>,
    pub target_node_id: Option<u64>,
    pub relationship_id: Option<u64>,
    pub matched: bool,
    pub created: bool,
    pub already_exists: bool,
    pub source_filtered_out: bool,
    pub target_filtered_out: bool,
    pub non_writable: bool,
    pub created_relationship_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipUpsertBatchRequest {
    pub upserts: Vec<KnowledgeRelationshipUpsertRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedRelationshipUpsertBatchRequest {
    pub upserts: Vec<KnowledgeRelationshipUpsertRequest>,
    pub source_metadata_filters: BTreeMap<String, String>,
    pub target_metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipUpsertBatchRow {
    pub source: KnowledgeEntityRequest,
    pub target: KnowledgeEntityRequest,
    pub relationship_type: String,
    pub source_node_id: Option<u64>,
    pub target_node_id: Option<u64>,
    pub relationship_id: Option<u64>,
    pub matched: bool,
    pub created: bool,
    pub already_exists: bool,
    pub source_filtered_out: bool,
    pub target_filtered_out: bool,
    pub non_writable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipUpsertBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeRelationshipUpsertBatchRow>,
    pub matched_count: usize,
    pub created_count: usize,
    pub already_exists_count: usize,
    pub missing_endpoint_count: usize,
    pub source_filtered_out_count: usize,
    pub target_filtered_out_count: usize,
    pub non_writable_count: usize,
    pub created_relationship_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipDeleteRequest {
    pub source: KnowledgeEntityRequest,
    pub target: KnowledgeEntityRequest,
    pub relationship_type: String,
    pub relationship_properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedRelationshipDeleteRequest {
    pub delete: KnowledgeRelationshipDeleteRequest,
    pub source_metadata_filters: BTreeMap<String, String>,
    pub target_metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipDeleteOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub source_node_id: Option<u64>,
    pub target_node_id: Option<u64>,
    pub matched: bool,
    pub source_filtered_out: bool,
    pub target_filtered_out: bool,
    pub deleted_relationship_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipUpdateRequest {
    pub source: KnowledgeEntityRequest,
    pub target: KnowledgeEntityRequest,
    pub relationship_type: String,
    pub relationship_properties: BTreeMap<String, Value>,
    pub assignments: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedRelationshipUpdateRequest {
    pub update: KnowledgeRelationshipUpdateRequest,
    pub source_metadata_filters: BTreeMap<String, String>,
    pub target_metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipUpdateOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub source_node_id: Option<u64>,
    pub target_node_id: Option<u64>,
    pub matched: bool,
    pub source_filtered_out: bool,
    pub target_filtered_out: bool,
    pub updated_relationship_count: usize,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipUpdateBatchRequest {
    pub updates: Vec<KnowledgeRelationshipUpdateRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedRelationshipUpdateBatchRequest {
    pub updates: Vec<KnowledgeRelationshipUpdateRequest>,
    pub source_metadata_filters: BTreeMap<String, String>,
    pub target_metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipUpdateBatchRow {
    pub source: KnowledgeEntityRequest,
    pub target: KnowledgeEntityRequest,
    pub relationship_type: String,
    pub source_node_id: Option<u64>,
    pub target_node_id: Option<u64>,
    pub matched: bool,
    pub source_filtered_out: bool,
    pub target_filtered_out: bool,
    pub non_writable: bool,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipUpdateBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeRelationshipUpdateBatchRow>,
    pub matched_count: usize,
    pub missing_endpoint_count: usize,
    pub source_filtered_out_count: usize,
    pub target_filtered_out_count: usize,
    pub non_writable_count: usize,
    pub updated_relationship_count: usize,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipDeleteBatchRequest {
    pub deletes: Vec<KnowledgeRelationshipDeleteRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedRelationshipDeleteBatchRequest {
    pub deletes: Vec<KnowledgeRelationshipDeleteRequest>,
    pub source_metadata_filters: BTreeMap<String, String>,
    pub target_metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipDeleteBatchRow {
    pub source: KnowledgeEntityRequest,
    pub target: KnowledgeEntityRequest,
    pub relationship_type: String,
    pub source_node_id: Option<u64>,
    pub target_node_id: Option<u64>,
    pub matched: bool,
    pub source_filtered_out: bool,
    pub target_filtered_out: bool,
    pub non_writable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipDeleteBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeRelationshipDeleteBatchRow>,
    pub matched_count: usize,
    pub missing_endpoint_count: usize,
    pub source_filtered_out_count: usize,
    pub target_filtered_out_count: usize,
    pub non_writable_count: usize,
    pub deleted_relationship_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSourceReferenceRelationshipCleanupRequest {
    pub source_reference: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSourceReferenceRelationshipCleanupRow {
    pub relationship_id: u64,
    pub source_node_id: u64,
    pub target_node_id: u64,
    pub source_external_id: Option<String>,
    pub target_external_id: Option<String>,
    pub deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSourceReferenceRelationshipCleanupOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeSourceReferenceRelationshipCleanupRow>,
    pub candidate_count: usize,
    pub deleted_relationship_count: usize,
}

}

#[cfg(test)]
pub use legacy_business_types::*;

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeEntity {
    pub node_id: u64,
    pub labels: Vec<String>,
    pub external_id: Option<String>,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeGraphPathDirection {
    Outgoing,
    Incoming,
}

impl KnowledgeGraphPathDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Outgoing => "outgoing",
            Self::Incoming => "incoming",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeGraphContextPath {
    pub seed_hit_id: String,
    pub hop: usize,
    pub direction: KnowledgeGraphPathDirection,
    pub relationship_id: u64,
    pub relationship_type: String,
    pub relationship_properties: BTreeMap<String, Value>,
    pub source_node_id: u64,
    pub source_labels: Vec<String>,
    pub source_external_id: Option<String>,
    pub target_node_id: u64,
    pub target_labels: Vec<String>,
    pub target_external_id: Option<String>,
}

pub(crate) struct QueryExecutionTrace {
    pub(crate) statement: cypher::Statement,
    pub(crate) optimizer_trace: Option<OptimizerTrace>,
    pub(crate) plan_cache_lookup: Option<PlanCacheLookup>,
    pub(crate) execution_profile: Option<executor::ReadExecutionProfile>,
}

impl QueryExecutionTrace {
    pub(in crate::api) fn uncached(statement: cypher::Statement) -> Self {
        Self {
            statement,
            optimizer_trace: None,
            plan_cache_lookup: None,
            execution_profile: None,
        }
    }
}
