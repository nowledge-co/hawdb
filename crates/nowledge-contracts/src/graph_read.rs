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

//! Test-only graph-read request and response models.

//! The root facade re-exports these only from its cfg(test) API surface.

use crate::{
    KnowledgeEntity, KnowledgeFanoutReasonCode, KnowledgeFanoutReasonDetail,
    KnowledgeGraphContextPath,
};
use hawdb_core::Value;
use hawdb_search::{SearchCandidateSetReport, SearchRetrieverCandidateSetReport};
use std::collections::BTreeMap;
use std::str::FromStr;

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeNeighborDirection {
    Outgoing,
    Incoming,
    Both,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeNeighborsRequest {
    pub label: String,
    pub external_id: String,
    pub relationship_type: Option<String>,
    #[doc(hidden)]
    pub direction: KnowledgeNeighborDirection,
    pub limit: usize,
    pub max_hops: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedNeighborsRequest {
    pub navigation: KnowledgeNeighborsRequest,
    pub metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeNeighborsOutput {
    pub graph_commit_epoch: u64,
    pub seed_node_id: Option<u64>,
    pub paths: Vec<KnowledgeGraphContextPath>,
    pub fanout_reason_codes: Vec<KnowledgeFanoutReasonCode>,
    pub fanout_reason_details: Vec<KnowledgeFanoutReasonDetail>,
    pub fanout_reasons: Vec<String>,
    pub diagnostics: KnowledgeTraversalDiagnostics,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipsRequest {
    pub seeds: Vec<KnowledgeEntityRequest>,
    pub relationship_type: Option<String>,
    #[doc(hidden)]
    pub direction: KnowledgeNeighborDirection,
    pub limit_per_seed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedRelationshipsRequest {
    pub relationships: KnowledgeRelationshipsRequest,
    pub metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipGroup {
    pub seed: KnowledgeEntityRequest,
    pub seed_node_id: Option<u64>,
    pub filtered_out: bool,
    pub relationships: Vec<KnowledgeGraphContextPath>,
    pub fanout_reason_codes: Vec<KnowledgeFanoutReasonCode>,
    pub fanout_reason_details: Vec<KnowledgeFanoutReasonDetail>,
    pub fanout_reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipsOutput {
    pub graph_commit_epoch: u64,
    pub groups: Vec<KnowledgeRelationshipGroup>,
    pub relationship_type_found: bool,
    pub found_seed_count: usize,
    pub missing_seed_count: usize,
    pub filtered_out_seed_count: usize,
    pub relationship_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeInducedEdgeListRequest {
    pub external_ids: Vec<String>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeInducedEdgeRow {
    pub source_id: Option<String>,
    pub source_node_id: u64,
    pub target_id: Option<String>,
    pub target_node_id: u64,
    pub relationship_id: u64,
    pub relationship_type: String,
    pub strength: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeInducedEdgeListOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgeInducedEdgeRow>,
    pub matched_node_count: usize,
    pub missing_external_ids: Vec<String>,
    pub matched_count: usize,
    pub returned_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePathRequest {
    pub source_label: String,
    pub source_external_id: String,
    pub target_label: String,
    pub target_external_id: String,
    pub relationship_type: Option<String>,
    #[doc(hidden)]
    pub direction: KnowledgeNeighborDirection,
    pub max_hops: usize,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedPathRequest {
    pub navigation: KnowledgePathRequest,
    pub source_metadata_filters: BTreeMap<String, String>,
    pub target_metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePathOutput {
    pub graph_commit_epoch: u64,
    pub source_node_id: Option<u64>,
    pub target_node_id: Option<u64>,
    pub paths: Vec<KnowledgeGraphPath>,
    pub fanout_reason_codes: Vec<KnowledgeFanoutReasonCode>,
    pub fanout_reason_details: Vec<KnowledgeFanoutReasonDetail>,
    pub fanout_reasons: Vec<String>,
    pub diagnostics: KnowledgeTraversalDiagnostics,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeGraphPath {
    pub segments: Vec<KnowledgeGraphContextPath>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSubgraphRequest {
    pub label: String,
    pub external_id: String,
    pub relationship_type: Option<String>,
    #[doc(hidden)]
    pub direction: KnowledgeNeighborDirection,
    pub max_hops: usize,
    pub node_limit: usize,
    pub relationship_limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedSubgraphRequest {
    pub navigation: KnowledgeSubgraphRequest,
    pub metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeSubgraphOutput {
    pub graph_commit_epoch: u64,
    pub seed_node_id: Option<u64>,
    pub nodes: Vec<KnowledgeEntity>,
    pub relationships: Vec<KnowledgeGraphContextPath>,
    pub fanout_reason_codes: Vec<KnowledgeFanoutReasonCode>,
    pub fanout_reason_details: Vec<KnowledgeFanoutReasonDetail>,
    pub fanout_reasons: Vec<String>,
    pub diagnostics: KnowledgeTraversalDiagnostics,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeTraversalDiagnostics {
    pub seed_found: bool,
    pub target_found: Option<bool>,
    pub input_candidate_set: SearchCandidateSetReport,
    pub candidate_set: SearchRetrieverCandidateSetReport,
    pub path_count: usize,
    pub node_count: usize,
    pub relationship_count: usize,
    pub fanout_reason_count: usize,
    pub fanout_reason_codes: Vec<KnowledgeFanoutReasonCode>,
    pub fanout_reason_details: Vec<KnowledgeFanoutReasonDetail>,
    pub fanout_reasons: Vec<String>,
    pub fallback_reason_codes: Vec<KnowledgeTraversalFallbackReasonCode>,
    pub fallback_reasons: Vec<String>,
    pub max_hops: usize,
    pub path_limit: Option<usize>,
    pub node_limit: Option<usize>,
    pub relationship_limit: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeTraversalFallbackReasonCode {
    SeedNotFound,
    TargetNotFound,
    MaxHopsZero,
    PathLimitZero,
    NodeLimitZero,
    RelationshipLimitZero,
    RelationshipTypeNotFound,
    QueryRuntimeFailed,
}

impl KnowledgeTraversalFallbackReasonCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SeedNotFound => "seed_not_found",
            Self::TargetNotFound => "target_not_found",
            Self::MaxHopsZero => "max_hops_zero",
            Self::PathLimitZero => "path_limit_zero",
            Self::NodeLimitZero => "node_limit_zero",
            Self::RelationshipLimitZero => "relationship_limit_zero",
            Self::RelationshipTypeNotFound => "relationship_type_not_found",
            Self::QueryRuntimeFailed => "query_runtime_failed",
        }
    }
}

impl FromStr for KnowledgeTraversalFallbackReasonCode {
    type Err = &'static str;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "seed_not_found" => Ok(Self::SeedNotFound),
            "target_not_found" => Ok(Self::TargetNotFound),
            "max_hops_zero" => Ok(Self::MaxHopsZero),
            "path_limit_zero" => Ok(Self::PathLimitZero),
            "node_limit_zero" => Ok(Self::NodeLimitZero),
            "relationship_limit_zero" => Ok(Self::RelationshipLimitZero),
            "relationship_type_not_found" => Ok(Self::RelationshipTypeNotFound),
            "query_runtime_failed" => Ok(Self::QueryRuntimeFailed),
            _ => Err("unknown knowledge traversal fallback reason code"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityRequest {
    pub label: String,
    pub external_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedEntityRequest {
    pub entity: KnowledgeEntityRequest,
    pub metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityBatchRequest {
    pub entities: Vec<KnowledgeEntityRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedEntityBatchRequest {
    pub entities: Vec<KnowledgeEntityRequest>,
    pub metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeEntityOutput {
    pub graph_commit_epoch: u64,
    pub entity: Option<KnowledgeEntity>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityDetailsRequest {
    pub label: String,
    pub external_id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeEntityDetailsOutput {
    pub graph_commit_epoch: u64,
    pub entity: Option<KnowledgeEntity>,
    pub neighbor_count: u64,
    pub relationship_count: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeEntityBatchOutput {
    pub graph_commit_epoch: u64,
    pub entities: Vec<Option<KnowledgeEntity>>,
    pub found_count: usize,
    pub missing_count: usize,
    pub filtered_out_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityEntityVisibilityRequest {
    pub community_ids: Vec<Value>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityEntityVisibilityRow {
    pub community_id: Value,
    pub entity_id: Option<String>,
    pub entity_node_id: u64,
    pub entity_name: Option<String>,
    pub entity_type: Option<String>,
    pub memory_id: Option<String>,
    pub memory_node_id: Option<u64>,
    pub memory_metadata: Option<Value>,
    pub memory_is_latest: bool,
    pub memory_lifecycle_state: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityEntityVisibilityOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgeCommunityEntityVisibilityRow>,
    pub matched_entity_count: usize,
    pub matched_row_count: usize,
    pub returned_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeCommunityMemorySource {
    MentionedEntities,
    DirectMemoryCommunity,
    Both,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeCommunityMemoryCrystalFilter {
    Any,
    FalseOnly,
    NullOrFalse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeCommunityMemoryListOrder {
    CommunityBreadthImportanceCreatedAt,
    EntityCountImportancePagerank,
    CommunityImportanceCreatedAt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityMemoryListRequest {
    pub community_ids: Vec<Value>,
    pub source: KnowledgeCommunityMemorySource,
    pub crystal_filter: KnowledgeCommunityMemoryCrystalFilter,
    pub unit_types: Vec<String>,
    pub order: KnowledgeCommunityMemoryListOrder,
    pub limit: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum KnowledgeCommunityMemoryRowSource {
    MentionedEntities,
    DirectMemoryCommunity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityMemoryRow {
    pub community_id: Value,
    pub source: KnowledgeCommunityMemoryRowSource,
    pub memory_id: Option<String>,
    pub memory_node_id: u64,
    pub title: Option<String>,
    pub title_or_empty: String,
    pub content: Option<String>,
    pub content_or_empty: String,
    pub unit_type: Option<String>,
    pub metadata: Option<Value>,
    pub is_latest: bool,
    pub lifecycle_state: Option<String>,
    pub importance: Option<Value>,
    pub created_at: Option<Value>,
    pub is_crystal: Option<bool>,
    pub pagerank_score: Option<Value>,
    pub mention_breadth: usize,
    pub entity_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityMemoryListOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgeCommunityMemoryRow>,
    pub matched_row_count: usize,
    pub returned_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeCrystalListOrder {
    ExternalIdAsc,
    ImportanceDescCreatedAtDesc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCrystalListRequest {
    pub key_match: Option<String>,
    pub after_id: Option<String>,
    pub limit: usize,
    pub order: KnowledgeCrystalListOrder,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCrystalRow {
    pub memory_id: Option<String>,
    pub node_id: u64,
    pub crystal_title: Option<String>,
    pub title: Option<String>,
    pub display_title: String,
    pub content: Option<String>,
    pub importance: Option<Value>,
    pub unit_type: Option<String>,
    pub created_at: Option<Value>,
    pub updated_at: Option<Value>,
    pub metadata: Option<Value>,
    pub is_latest: Option<bool>,
    pub is_crystal: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCrystalListOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgeCrystalRow>,
    pub matched_count: usize,
    pub returned_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCrystalSourceMergeRequest {
    pub crystal_memory_id: String,
    pub source_memory_id: String,
    pub weight: Value,
    pub created_at: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCrystalSourceMergeOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub crystal_memory_id: String,
    pub source_memory_id: String,
    pub crystal_node_id: Option<u64>,
    pub source_node_id: Option<u64>,
    pub relationship_id: Option<u64>,
    pub matched: bool,
    pub created: bool,
    pub already_exists: bool,
    pub missing_endpoint: bool,
    pub non_writable: bool,
    pub created_relationship_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KnowledgeCrystalCommunityScope {
    CommunityIds(Vec<Value>),
    NonNullCommunity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeCrystalCommunityListOrder {
    CommunityIdAscCrystalIdAsc,
    HitsDescImportanceDesc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCrystalCommunityListRequest {
    pub scope: KnowledgeCrystalCommunityScope,
    pub limit: usize,
    pub order: KnowledgeCrystalCommunityListOrder,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCrystalCommunityRow {
    pub crystal_memory_id: Option<String>,
    pub crystal_node_id: u64,
    pub community_id: Value,
    pub hit_count: usize,
    pub source_memory_count: usize,
    pub crystal_title: Option<String>,
    pub title: Option<String>,
    pub display_title: String,
    pub content: Option<String>,
    pub importance: Option<Value>,
    pub metadata: Option<Value>,
    pub is_latest: Option<bool>,
    pub lifecycle_state: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCrystalCommunityListOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgeCrystalCommunityRow>,
    pub matched_path_count: usize,
    pub matched_pair_count: usize,
    pub returned_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCrystalSourceVisibilityRequest {
    pub community_ids: Vec<Value>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCrystalSourceVisibilityRow {
    pub crystal_memory_id: Option<String>,
    pub crystal_node_id: u64,
    pub source_memory_id: Option<String>,
    pub source_node_id: u64,
    pub entity_id: Option<String>,
    pub entity_node_id: u64,
    pub community_id: Value,
    pub crystal_title: Option<String>,
    pub title: Option<String>,
    pub display_title: String,
    pub content: Option<String>,
    pub importance: Option<Value>,
    pub crystal_metadata: Option<Value>,
    pub crystal_is_latest: bool,
    pub crystal_lifecycle_state: Option<String>,
    pub source_metadata: Option<Value>,
    pub source_is_latest: bool,
    pub source_lifecycle_state: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCrystalSourceVisibilityOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgeCrystalSourceVisibilityRow>,
    pub matched_path_count: usize,
    pub returned_count: usize,
}

#[cfg(test)]
mod tests {
    use super::KnowledgeTraversalFallbackReasonCode;

    #[test]
    fn fallback_reason_codes_parse_only_known_values() {
        assert_eq!(
            "max_hops_zero".parse::<KnowledgeTraversalFallbackReasonCode>(),
            Ok(KnowledgeTraversalFallbackReasonCode::MaxHopsZero)
        );
        assert!("not_a_reason"
            .parse::<KnowledgeTraversalFallbackReasonCode>()
            .is_err());
    }
}
