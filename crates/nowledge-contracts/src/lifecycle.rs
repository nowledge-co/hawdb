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

//! Test-only lifecycle mutation request and response models.

//! The root facade re-exports these only from its cfg(test) API surface.

use hawdb_core::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillUsageStatsUpdate {
    pub skill_id: String,
    pub use_count: i64,
    pub success_rate: Option<Value>,
    pub last_activity_at: Value,
    pub updated_at: Value,
    pub metadata: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillUsageStatsBatchRequest {
    pub updates: Vec<KnowledgeSkillUsageStatsUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillUsageStatsBatchRow {
    pub skill_id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub updated: bool,
    pub duplicate: bool,
    pub non_writable: bool,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillUsageStatsBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeSkillUsageStatsBatchRow>,
    pub matched_count: usize,
    pub missing_count: usize,
    pub duplicate_count: usize,
    pub non_writable_count: usize,
    pub updated_count: usize,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillMetadataUpdate {
    pub skill_id: String,
    pub metadata: Value,
    pub updated_at: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillMetadataBatchRequest {
    pub updates: Vec<KnowledgeSkillMetadataUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillMetadataBatchRow {
    pub skill_id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub updated: bool,
    pub duplicate: bool,
    pub non_writable: bool,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillMetadataBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeSkillMetadataBatchRow>,
    pub matched_count: usize,
    pub missing_count: usize,
    pub duplicate_count: usize,
    pub non_writable_count: usize,
    pub updated_count: usize,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillSourceMergeRequest {
    pub skill_id: String,
    pub memory_id: String,
    pub occasion_key: String,
    pub created_at: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillSourceMergeOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub skill_id: String,
    pub memory_id: String,
    pub skill_node_id: Option<u64>,
    pub memory_node_id: Option<u64>,
    pub relationship_id: Option<u64>,
    pub matched: bool,
    pub created: bool,
    pub already_exists: bool,
    pub missing_endpoint: bool,
    pub non_writable: bool,
    pub created_relationship_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillLifecycleUpdate {
    pub skill_id: String,
    pub stage: Option<String>,
    pub rejected_at: Option<Value>,
    pub rationale: Option<Value>,
    pub version: Option<Value>,
    pub title: Option<Value>,
    pub name: Option<Value>,
    pub description: Option<Value>,
    pub triggers: Option<Value>,
    pub tools: Option<Value>,
    pub bundle_path: Option<Value>,
    pub content_hash: Option<Value>,
    pub write_origin: Option<String>,
    pub metadata: Option<Value>,
    pub updated_at: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillLifecycleBatchRequest {
    pub updates: Vec<KnowledgeSkillLifecycleUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillLifecycleBatchRow {
    pub skill_id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub updated: bool,
    pub duplicate: bool,
    pub non_writable: bool,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillLifecycleBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeSkillLifecycleBatchRow>,
    pub matched_count: usize,
    pub missing_count: usize,
    pub duplicate_count: usize,
    pub non_writable_count: usize,
    pub updated_count: usize,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillDeleteBatchRequest {
    pub skill_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillDeleteBatchRow {
    pub skill_id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub non_writable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillDeleteBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeSkillDeleteBatchRow>,
    pub matched_count: usize,
    pub missing_count: usize,
    pub non_writable_count: usize,
    pub deleted_node_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadMetadataUpdate {
    pub thread_id: String,
    pub metadata: Value,
    pub updated_at: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadMetadataBatchRequest {
    pub updates: Vec<KnowledgeThreadMetadataUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadMetadataBatchRow {
    pub thread_id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub updated: bool,
    pub duplicate: bool,
    pub non_writable: bool,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadMetadataBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeThreadMetadataBatchRow>,
    pub matched_count: usize,
    pub missing_count: usize,
    pub duplicate_count: usize,
    pub non_writable_count: usize,
    pub updated_count: usize,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadMessageCountUpdate {
    pub thread_id: String,
    pub message_count: i64,
    pub updated_at: Option<Value>,
    pub preserve_newer_existing_updated_at: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadMessageCountBatchRequest {
    pub updates: Vec<KnowledgeThreadMessageCountUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadMessageCountBatchRow {
    pub thread_id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub updated: bool,
    pub duplicate: bool,
    pub non_writable: bool,
    pub updated_at_changed: bool,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadMessageCountBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeThreadMessageCountBatchRow>,
    pub matched_count: usize,
    pub missing_count: usize,
    pub duplicate_count: usize,
    pub non_writable_count: usize,
    pub updated_count: usize,
    pub updated_at_changed_count: usize,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadDeleteBatchRequest {
    pub thread_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadDeleteBatchRow {
    pub thread_id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub non_writable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadDeleteBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeThreadDeleteBatchRow>,
    pub matched_count: usize,
    pub missing_count: usize,
    pub non_writable_count: usize,
    pub deleted_node_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadIdentityCascadeDeleteKeys {
    pub public_thread_id: String,
    pub input_thread_id: String,
    pub thread_uuid: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadIdentityDeleteRequest {
    pub identity_key: Option<String>,
    pub cascade_keys: Option<KnowledgeThreadIdentityCascadeDeleteKeys>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadIdentityDeleteOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub matched_identity_count: usize,
    pub deleted_identity_count: usize,
    pub deleted_node_ids: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadCompactionLinkRequest {
    pub thread_id: String,
    pub memory_id: String,
    pub compaction_method: String,
    pub created_at: Value,
    pub properties: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadCompactionLinkOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub thread_id: String,
    pub memory_id: String,
    pub thread_node_id: Option<u64>,
    pub memory_node_id: Option<u64>,
    pub matched: bool,
    pub missing_endpoint: bool,
    pub non_writable: bool,
    pub created_relationship_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadMessageDeleteRequest {
    pub thread_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadMessageDeleteOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub thread_id: String,
    pub thread_node_id: Option<u64>,
    pub found_thread: bool,
    pub matched_relationship_count: usize,
    pub deleted_message_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeLabelLifecycleUpdate {
    pub label_id: String,
    pub name: Option<String>,
    pub canonical_name: Option<String>,
    pub metadata: Option<Value>,
    pub updated_at: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeLabelLifecycleBatchRequest {
    pub updates: Vec<KnowledgeLabelLifecycleUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeLabelLifecycleBatchRow {
    pub label_id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub updated: bool,
    pub duplicate: bool,
    pub non_writable: bool,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeLabelLifecycleBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeLabelLifecycleBatchRow>,
    pub matched_count: usize,
    pub missing_count: usize,
    pub duplicate_count: usize,
    pub non_writable_count: usize,
    pub updated_count: usize,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryLabelDeleteRequest {
    pub memory_id: String,
    pub label_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryLabelDeleteOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub memory_id: String,
    pub label_id: Option<String>,
    pub memory_node_id: Option<u64>,
    pub label_node_id: Option<u64>,
    pub found_memory: bool,
    pub found_label: bool,
    pub non_writable: bool,
    pub matched_relationship_count: usize,
    pub deleted_relationship_count: usize,
    pub deleted_relationship_ids: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeLabelMemoryTransferRequest {
    pub source_label_id: String,
    pub target_label_id: String,
    pub created_at: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeLabelMemoryTransferRow {
    pub memory_id: Option<String>,
    pub memory_node_id: u64,
    pub relationship_id: Option<u64>,
    pub created: bool,
    pub already_exists: bool,
    pub non_writable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeLabelMemoryTransferOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub source_label_id: String,
    pub target_label_id: String,
    pub source_label_node_id: Option<u64>,
    pub target_label_node_id: Option<u64>,
    pub found_source_label: bool,
    pub found_target_label: bool,
    pub rows: Vec<KnowledgeLabelMemoryTransferRow>,
    pub matched_memory_count: usize,
    pub created_count: usize,
    pub already_exists_count: usize,
    pub non_writable_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryLabelTransferRequest {
    pub older_memory_id: String,
    pub newer_memory_id: String,
    pub space_id: String,
    pub created_at: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryLabelTransferRow {
    pub label_id: Option<String>,
    pub label_node_id: u64,
    pub relationship_id: Option<u64>,
    pub created: bool,
    pub already_exists: bool,
    pub non_writable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryLabelTransferOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub older_memory_id: String,
    pub newer_memory_id: String,
    pub space_id: String,
    pub older_memory_node_id: Option<u64>,
    pub newer_memory_node_id: Option<u64>,
    pub found_older_memory: bool,
    pub found_newer_memory: bool,
    pub older_space_matches: bool,
    pub newer_space_matches: bool,
    pub rows: Vec<KnowledgeMemoryLabelTransferRow>,
    pub matched_label_count: usize,
    pub created_count: usize,
    pub already_exists_count: usize,
    pub non_writable_count: usize,
    pub duplicate_source_edge_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityLabelListRequest {
    pub entity_label: String,
    pub external_ids: Vec<String>,
    pub limit_per_entity: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityLabelRow {
    pub label_id: Option<String>,
    pub node_id: u64,
    pub name: Option<String>,
    pub canonical_name: Option<String>,
    pub color: Option<Value>,
    pub description: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityLabelGroup {
    pub external_id: String,
    pub node_id: Option<u64>,
    pub found: bool,
    pub labels: Vec<KnowledgeEntityLabelRow>,
    pub returned_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityLabelListOutput {
    pub graph_commit_epoch: u64,
    pub groups: Vec<KnowledgeEntityLabelGroup>,
    pub found_entity_count: usize,
    pub missing_entity_count: usize,
    pub label_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityLabelProjectedListRequest {
    pub list: KnowledgeEntityLabelListRequest,
    pub label_property_names: Vec<String>,
    pub relationship_property_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityLabelProjectedRow {
    pub label_id: Option<String>,
    pub label_node_id: u64,
    pub relationship_id: u64,
    pub label_properties: BTreeMap<String, Value>,
    pub relationship_properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityLabelProjectedGroup {
    pub external_id: String,
    pub node_id: Option<u64>,
    pub found: bool,
    pub labels: Vec<KnowledgeEntityLabelProjectedRow>,
    pub returned_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityLabelProjectedListOutput {
    pub graph_commit_epoch: u64,
    pub groups: Vec<KnowledgeEntityLabelProjectedGroup>,
    pub found_entity_count: usize,
    pub missing_entity_count: usize,
    pub label_count: usize,
}
