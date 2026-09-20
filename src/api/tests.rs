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

use super::{
    nowledge_deep_search_graph_seed_limit, validate_hawdb_lightning_graph_stream,
    BackgroundMaintenanceKind, BackgroundMaintenanceOptions, CanonicalStableIdMapping, Database,
    DatabaseConfig, DatabaseReadTransaction, DerivedArtifactJobStatus,
    ExternalContentArtifactJobCompletion, ExternalContentArtifactRuntimeManifest,
    KnowledgeAugmentationJobInterruptRequest, KnowledgeAugmentationJobLifecycleBatchRequest,
    KnowledgeAugmentationJobLifecycleTransition, KnowledgeAugmentationJobLifecycleUpdate,
    KnowledgeCandidateScoringPolicy, KnowledgeCandidateSource,
    KnowledgeCommunityAssignmentClearRequest, KnowledgeCommunityCleanupRequest,
    KnowledgeCommunityCreate, KnowledgeCommunityEntityVisibilityRequest,
    KnowledgeCommunityLifecycleBatchRequest, KnowledgeCommunityListOrder,
    KnowledgeCommunityListRequest, KnowledgeCommunityLookupKey, KnowledgeCommunityMembershipCreate,
    KnowledgeCommunityMembershipCreateBatchRequest, KnowledgeCommunityMemoryCrystalFilter,
    KnowledgeCommunityMemoryListOrder, KnowledgeCommunityMemoryListRequest,
    KnowledgeCommunityMemoryRowSource, KnowledgeCommunityMemorySource, KnowledgeCommunityRequest,
    KnowledgeCommunitySummaryUpdate, KnowledgeCrystalCommunityListOrder,
    KnowledgeCrystalCommunityListRequest, KnowledgeCrystalCommunityScope,
    KnowledgeCrystalListOrder, KnowledgeCrystalListRequest, KnowledgeCrystalSourceMergeRequest,
    KnowledgeCrystalSourceVisibilityRequest, KnowledgeEntityBatchRequest,
    KnowledgeEntityCreateBatchRequest, KnowledgeEntityCreateRequest,
    KnowledgeEntityDeleteBatchRequest, KnowledgeEntityDeleteRequest,
    KnowledgeEntityLabelListRequest, KnowledgeEntityLabelProjectedListRequest,
    KnowledgeEntityRequest, KnowledgeEntityUpsertBatchRequest, KnowledgeEntityUpsertRequest,
    KnowledgeFallbackReasonCode, KnowledgeFanoutReasonCode, KnowledgeGraphMetaRequest,
    KnowledgeGraphMetaStamp, KnowledgeGraphMetaStampBatchRequest, KnowledgeGraphPathDirection,
    KnowledgeInducedEdgeListRequest, KnowledgeLabelLifecycleBatchRequest,
    KnowledgeLabelLifecycleUpdate, KnowledgeLabelMemoryTransferRequest,
    KnowledgeMemoryAccessBatchRequest, KnowledgeMemoryAccessTouch,
    KnowledgeMemoryContentBatchRequest, KnowledgeMemoryContentUpdate,
    KnowledgeMemoryDecayRefreshBatchRequest, KnowledgeMemoryDecayRefreshUpdate,
    KnowledgeMemoryDedupReviewedBatchRequest, KnowledgeMemoryEvolvesCreate,
    KnowledgeMemoryEvolvesCreateBatchRequest, KnowledgeMemoryLabelDeleteRequest,
    KnowledgeMemoryLabelTransferRequest, KnowledgeMemoryLatestBatchRequest,
    KnowledgeMemoryLatestUpdate, KnowledgeMemoryLifecycleBatchRequest,
    KnowledgeMemoryLifecycleUpdate, KnowledgeMemoryMetadataBatchRequest,
    KnowledgeMemoryMetadataUpdate, KnowledgeNeighborDirection, KnowledgeNeighborsRequest,
    KnowledgeNormalizedSpaceMoveBatchRequest, KnowledgePageRankClearRequest,
    KnowledgePageRankScoreBatchRequest, KnowledgePageRankScoreUpdate, KnowledgePathRequest,
    KnowledgePropertyBatchRequest, KnowledgePropertyUpdateBatchRequest,
    KnowledgePropertyUpdateRequest, KnowledgeRelationshipCreateBatchRequest,
    KnowledgeRelationshipCreateRequest, KnowledgeRelationshipDeleteBatchRequest,
    KnowledgeRelationshipDeleteRequest, KnowledgeRelationshipUpdateBatchRequest,
    KnowledgeRelationshipUpdateRequest, KnowledgeRelationshipUpsertBatchRequest,
    KnowledgeRelationshipUpsertRequest, KnowledgeRelationshipsRequest,
    KnowledgeRetrievalEmptyReasonCode, KnowledgeRetrievalRequest, KnowledgeRetrievalStage,
    KnowledgeSchemaMigrationApply, KnowledgeSchemaMigrationApplyBatchRequest,
    KnowledgeScopedEntityBatchRequest, KnowledgeScopedEntityDeleteBatchRequest,
    KnowledgeScopedEntityDeleteRequest, KnowledgeScopedEntityRequest,
    KnowledgeScopedNeighborsRequest, KnowledgeScopedPathRequest,
    KnowledgeScopedPropertyBatchRequest, KnowledgeScopedPropertyUpdateBatchRequest,
    KnowledgeScopedPropertyUpdateRequest, KnowledgeScopedRelationshipCreateBatchRequest,
    KnowledgeScopedRelationshipCreateRequest, KnowledgeScopedRelationshipDeleteBatchRequest,
    KnowledgeScopedRelationshipDeleteRequest, KnowledgeScopedRelationshipUpdateBatchRequest,
    KnowledgeScopedRelationshipUpdateRequest, KnowledgeScopedRelationshipsRequest,
    KnowledgeScopedSubgraphRequest, KnowledgeSkillDeleteBatchRequest,
    KnowledgeSkillLifecycleBatchRequest, KnowledgeSkillLifecycleUpdate,
    KnowledgeSkillMetadataBatchRequest, KnowledgeSkillMetadataUpdate,
    KnowledgeSkillSourceMergeRequest, KnowledgeSkillUsageStatsBatchRequest,
    KnowledgeSkillUsageStatsUpdate, KnowledgeSourceDeleteBatchRequest,
    KnowledgeSourceLabelAssignment, KnowledgeSourceLabelAssignmentBatchRequest,
    KnowledgeSourceLabelDelete, KnowledgeSourceLabelDeleteBatchRequest,
    KnowledgeSourceLifecycleBatchRequest, KnowledgeSourceLifecycleUpdate,
    KnowledgeSourceMemoryCountAdjustment, KnowledgeSourceMemoryCountBatchRequest,
    KnowledgeSourceMetadataBatchRequest, KnowledgeSourceMetadataUpdate,
    KnowledgeSourceParsedCreate, KnowledgeSourceParsedCreateBatchRequest,
    KnowledgeSourceParsedMetadataBatchRequest, KnowledgeSourceParsedMetadataUpdate,
    KnowledgeSourceReferenceRelationshipCleanupRequest, KnowledgeSourceRevisionCreate,
    KnowledgeSourceRevisionCreateBatchRequest, KnowledgeSubgraphRequest,
    KnowledgeThreadCompactionLinkRequest, KnowledgeThreadDeleteBatchRequest,
    KnowledgeThreadIdentityCascadeDeleteKeys, KnowledgeThreadIdentityDeleteRequest,
    KnowledgeThreadMessageCountBatchRequest, KnowledgeThreadMessageCountUpdate,
    KnowledgeThreadMessageDeleteRequest, KnowledgeThreadMetadataBatchRequest,
    KnowledgeThreadMetadataUpdate, KnowledgeTraversalFallbackReasonCode,
    KnowledgeTruncationReasonCode, NowledgeGraphAdapter, NowledgeGraphStatement,
    PlanCacheBypassReason, PlanCacheLookup, QueryOutput, QueryStreamOptions, RecoveryMode,
    SearchProjectionGraphDeltaRequest, HAWDB_LIGHTNING_BOOTSTRAP_PROTOCOL_VERSION,
    NOWLEDGE_DEEP_SEARCH_FILTERED_RANK_WINDOW, NOWLEDGE_DEEP_SEARCH_GRAPH_CONTEXT_MAX_HOPS,
    NOWLEDGE_DEEP_SEARCH_MIN_GRAPH_SEED_LIMIT, NOWLEDGE_DEEP_SEARCH_MIN_RANK_WINDOW,
};
use crate::optimizer::PlanCost;
use crate::qos::{
    BackgroundWorkHint, BackgroundWorkReasonCode, LocalQosPolicy, LocalQosState, QosAdmission,
    WorkClass, WorkPriority, WorkRequest,
};
use crate::schema::{
    ConstraintKind, ConstraintSubject, IndexKind, PropertyType, SchemaObjectState, TableKind,
};
use crate::search::{
    MetadataRepairOptions, SearchDocument, SearchFallbackReasonCode, SearchFusionWeights,
    SearchIndex, SearchMode, SearchProjectionDelta, SearchProjectionKind, SearchProjectionRow,
    SearchRebuildOptions, SearchTruncationReasonCode,
};
use crate::store::{DatabaseDoctor, NodeId, WalDoctorOptions, DENSE_ADJACENCY_DEGREE_THRESHOLD};
use crate::NowledgeMemStorageRecoveryReport;
use crate::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Cursor, Write};
use std::num::NonZeroUsize;

mod aggregates;
mod augmentation_jobs_interrupt;
mod augmentation_jobs_lifecycle;
mod augmentation_jobs_reads;
mod background_maintenance;
mod canonical_snapshot;
mod community_assignments;
mod community_cleanup;
mod community_lifecycle;
mod community_memberships;
mod community_reads;
mod concurrent_transactions;
mod delete_mutations;
mod expression_functions;
mod external_content_artifacts;
mod graph_hash_join;
mod graph_meta;
mod graph_rag_schema_guidance;
mod knowledge_community_entity_visibility;
mod knowledge_community_memories;
mod knowledge_context_memory_preview;
mod knowledge_entity_batch_deletes;
mod knowledge_entity_delete_guard;
mod knowledge_entity_deletes;
mod knowledge_entity_mention_counts;
mod knowledge_entity_reads;
mod knowledge_label_canonical_reads;
mod knowledge_label_memory_distribution;
mod knowledge_label_regex_memory_connections;
mod knowledge_label_usage;
mod knowledge_memory_cleanup_fingerprints;
mod knowledge_memory_crystal_synthesis_counts;
mod knowledge_memory_decay_detail;
mod knowledge_memory_entities;
mod knowledge_memory_evolves_latest;
mod knowledge_memory_evolves_neighbors;
mod knowledge_memory_evolves_projected_successors;
mod knowledge_memory_evolves_relation_counts;
mod knowledge_memory_lists;
mod knowledge_memory_metadata_related;
mod knowledge_memory_prefix_ownership;
mod knowledge_memory_title_contents;
mod knowledge_navigation;
mod knowledge_property_batch_mutations;
mod knowledge_reason_codes;
mod knowledge_related_entity_names;
mod knowledge_relationship_batch_creates;
mod knowledge_relationship_batch_deletes;
mod knowledge_relationship_create_upserts;
mod knowledge_relationship_deletes;
mod knowledge_relationship_updates;
mod knowledge_retrieval;
mod knowledge_retrieval_candidates;
mod knowledge_retrieval_diagnostics;
mod knowledge_retrieval_fallbacks;
mod knowledge_retrieval_filters;
mod knowledge_retrieval_graph_context;
mod knowledge_retrieval_identity;
mod knowledge_retrieval_pipeline;
mod knowledge_retrieval_ranking;
mod merge_nodes;
mod merge_relationships;
mod mutation_guards;
mod mutation_persistence;
mod nowledge_graph_adapter;
mod optional_match;
mod pagerank;
mod predicates;
mod projected_graph_artifacts;
mod projected_graph_execution;
mod projection_generations;
mod query_execution;
mod query_first_support;
mod query_observability;
mod read_transactions;
mod relational_access_cost;
mod relational_cross_join;
mod relational_explain_output;
mod relational_from_scopes;
mod relational_having;
mod relational_ordering;
mod relational_projection_expressions;
mod relational_update_delete_outcomes;
mod relationship_patterns;
mod relationship_property_mutations;
mod runtime_capabilities;
mod schema_indexes;
mod schema_migrations;
mod search_projection_delta_facade;
mod search_projection_graph_delta;
mod search_projection_rebuild_facade;
mod set_mutations;
mod skill_reads;
mod source_reads;
mod source_reference;
mod statistics;
mod storage_recovery;
mod synthesized_source_reads;
mod system_introspection;
mod system_schema_registry;
mod system_variables;
mod thread_compaction_reads;
mod thread_distillation_reads;
mod thread_message_reads;
mod thread_metadata_reads;
mod transaction_compaction;
mod transaction_control;
mod transaction_merge;

use query_first_support::QueryFirstTestExt;

#[test]
fn runs_create_match_return_demo() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Runtime strategy'})")
        .unwrap();

    let output = db
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Graph foundations".to_string()))
    );
}

#[test]
fn database_config_applies_execution_memory_spill_to_read_queries() {
    let execution_memory = crate::executor::ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(2048).unwrap(),
        ..crate::executor::ExecutionMemoryConfig::default()
    };
    let mut db = Database::new_with_config(DatabaseConfig {
        execution_memory,
        ..DatabaseConfig::default()
    });
    for value in 0..10 {
        db.query(&format!(
            "CREATE (:Memory {{title: '{value}-{}'}})",
            "x".repeat(96)
        ))
        .unwrap();
    }

    let output = db
        .query("MATCH (m:Memory) RETURN DISTINCT m.title AS title")
        .unwrap();
    assert_eq!(output.rows.len(), 10);
}

fn search_projection_row(external_id: &str, title: &str, body: &str) -> SearchProjectionRow {
    SearchProjectionRow {
        kind: SearchProjectionKind::Memory,
        external_id: external_id.to_string(),
        title: title.to_string(),
        body: body.to_string(),
        embedding: None,
        source_id: None,
        metadata: BTreeMap::new(),
    }
}

#[test]
fn database_session_runs_transaction_control_statements() {
    let mut db = Database::new();
    {
        let mut session = db.session();
        assert!(session.query("BEGIN TRANSACTION").unwrap().rows.is_empty());
        assert!(session
            .query("CREATE (:Memory {id: 1, title: 'Committed'})")
            .unwrap()
            .rows
            .is_empty());
        let commit = session.query("COMMIT;").unwrap();
        assert_eq!(commit.rows.len(), 1);
    }

    let output = db
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Committed".to_string()))
    );
}

#[test]
fn database_session_rolls_back_buffered_transaction() {
    let mut db = Database::new();
    {
        let mut session = db.session();
        session.query("BEGIN TRANSACTION").unwrap();
        session
            .query("CREATE (:Memory {id: 1, title: 'Rolled back'})")
            .unwrap();
        assert!(session.query("ROLLBACK").unwrap().rows.is_empty());
    }

    let output = db
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();
    assert!(output.rows.is_empty());
}

#[test]
fn database_session_reads_own_writes_inside_transaction() {
    let mut db = Database::new();
    {
        let mut session = db.session();
        session.query("BEGIN TRANSACTION").unwrap();
        session
            .query("CREATE (:Memory {id: 1, state: 'staged'})")
            .unwrap();
        let staged = session
            .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.state AS state")
            .unwrap();
        assert_eq!(staged.rows.len(), 1);
        assert_eq!(
            staged.rows[0].get("state"),
            Some(&Value::String("staged".to_string()))
        );
        session.query("COMMIT").unwrap();
    }

    let committed = db
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.state AS state")
        .unwrap();
    assert_eq!(committed.rows.len(), 1);
}

#[test]
fn public_unwind_mutation_batches_direct_and_transactional_bootstrap_rows() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("hawdb-public-unwind-{nonce}"));
    let query = "UNWIND $rows AS row MERGE (entity:Entity {id: row.id}) ON CREATE SET entity.name = row.name";
    let rows = |entries: &[(&str, &str)]| {
        Value::List(
            entries
                .iter()
                .map(|(id, name)| {
                    Value::Map(BTreeMap::from([
                        ("id".to_string(), Value::String((*id).to_string())),
                        ("name".to_string(), Value::String((*name).to_string())),
                    ]))
                })
                .collect(),
        )
    };
    let mut db = Database::open(&path).unwrap();
    db.query_with_params(
        query,
        &BTreeMap::from([(
            "rows".to_string(),
            rows(&[("one", "First"), ("two", "Second")]),
        )]),
    )
    .unwrap();

    let mut transaction = db.begin_transaction();
    let error = transaction
        .query_with_params(
            query,
            &BTreeMap::from([(
                "rows".to_string(),
                Value::List(vec![
                    Value::Map(BTreeMap::from([
                        ("id".to_string(), Value::String("three".to_string())),
                        ("name".to_string(), Value::String("Third".to_string())),
                    ])),
                    Value::Int(4),
                ]),
            )]),
        )
        .unwrap_err();
    assert!(error.to_string().contains("must be a map"), "{error}");
    assert_eq!(
        transaction
            .query("MATCH (entity:Entity) RETURN entity.id AS id ORDER BY id")
            .unwrap()
            .rows
            .len(),
        2
    );

    transaction
        .query_with_params(
            query,
            &BTreeMap::from([(
                "rows".to_string(),
                rows(&[("three", "Third"), ("four", "Fourth")]),
            )]),
        )
        .unwrap();
    assert_eq!(
        transaction
            .query("MATCH (entity:Entity) RETURN entity.id AS id ORDER BY id")
            .unwrap()
            .rows
            .len(),
        4
    );
    transaction.commit().unwrap();

    drop(db);
    let mut db = Database::open(&path).unwrap();

    let output = db
        .query("MATCH (entity:Entity) RETURN entity.id AS id, entity.name AS name ORDER BY id")
        .unwrap();
    assert_eq!(output.rows.len(), 4);
    assert_eq!(
        output.rows[2].get("name"),
        Some(&Value::String("Third".to_string()))
    );
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn returns_relationship_endpoint_properties() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'left'})-[:RELATES_TO]->(:Entity {id: 'right'})")
        .unwrap();

    let output = db
        .query("MATCH (e1:Entity)-[r:RELATES_TO]->(e2:Entity) RETURN e1.id, e2.id")
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("e1.id"),
        Some(&Value::String("left".to_string()))
    );
    assert_eq!(
        output.rows[0].get("e2.id"),
        Some(&Value::String("right".to_string()))
    );
}

#[test]
fn reads_crystals_for_wiki_and_okf_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'crystal-alpha', is_crystal: true, crystal_title: 'Alpha Crystal', title: 'Alpha Title', content: 'Alpha content', importance: 0.8, unit_type: 'fact', created_at: 10, updated_at: 20, metadata: '{\"a\":1}', is_latest: true})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'crystal-beta', is_crystal: true, title: 'Beta Title', content: 'Beta content', importance: 0.9, unit_type: 'decision', created_at: 5, updated_at: 25, metadata: '{\"b\":1}', is_latest: false})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'archive-crystal', is_crystal: true, crystal_title: 'Archive Crystal', content: 'Archive content', importance: 0.1, unit_type: 'note', created_at: 30})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'crystal-non', is_crystal: false, crystal_title: 'Non Crystal', importance: 5.0, created_at: 99})")
        .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();

    let wiki_detail = db
        .query_crystals_via_cypher(&KnowledgeCrystalListRequest {
            key_match: Some("crystal-a".to_string()),
            after_id: None,
            limit: 1,
            order: KnowledgeCrystalListOrder::ExternalIdAsc,
        })
        .unwrap();
    assert_eq!(wiki_detail.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);
    assert_eq!(wiki_detail.matched_count, 1);
    assert_eq!(wiki_detail.returned_count, 1);
    assert_eq!(
        wiki_detail.rows[0].memory_id.as_deref(),
        Some("crystal-alpha")
    );
    assert_eq!(
        wiki_detail.rows[0].crystal_title.as_deref(),
        Some("Alpha Crystal")
    );
    assert_eq!(wiki_detail.rows[0].title.as_deref(), Some("Alpha Title"));
    assert_eq!(wiki_detail.rows[0].display_title, "Alpha Crystal");
    assert_eq!(
        wiki_detail.rows[0].content.as_deref(),
        Some("Alpha content")
    );
    assert_eq!(wiki_detail.rows[0].importance, Some(Value::Float(0.8)));
    assert_eq!(wiki_detail.rows[0].unit_type.as_deref(), Some("fact"));
    assert_eq!(wiki_detail.rows[0].created_at, Some(Value::Int(10)));

    let page = db
        .query_crystals_via_cypher(&KnowledgeCrystalListRequest {
            key_match: None,
            after_id: Some("crystal-alpha".to_string()),
            limit: 10,
            order: KnowledgeCrystalListOrder::ExternalIdAsc,
        })
        .unwrap();
    assert_eq!(page.matched_count, 1);
    assert_eq!(page.returned_count, 1);
    assert_eq!(page.rows[0].memory_id.as_deref(), Some("crystal-beta"));
    assert_eq!(page.rows[0].display_title, "Beta Title");
    assert_eq!(page.rows[0].is_crystal, Some(true));
    assert_eq!(page.rows[0].is_latest, Some(false));
    assert_eq!(
        page.rows[0].metadata,
        Some(Value::String("{\"b\":1}".to_string()))
    );
}

#[test]
fn crystal_read_rejects_invalid_filters() {
    let db = Database::new();

    let empty_key_error = db
        .query_crystals_via_cypher(&KnowledgeCrystalListRequest {
            key_match: Some(String::new()),
            after_id: None,
            limit: 1,
            order: KnowledgeCrystalListOrder::ExternalIdAsc,
        })
        .unwrap_err();
    assert!(empty_key_error.to_string().contains("non-empty key match"));

    let empty_after_error = db
        .query_crystals_via_cypher(&KnowledgeCrystalListRequest {
            key_match: None,
            after_id: Some(String::new()),
            limit: 1,
            order: KnowledgeCrystalListOrder::ExternalIdAsc,
        })
        .unwrap_err();
    assert!(empty_after_error.to_string().contains("non-empty after id"));

    let mixed_filter_error = db
        .query_crystals_via_cypher(&KnowledgeCrystalListRequest {
            key_match: Some("crystal".to_string()),
            after_id: Some("crystal-alpha".to_string()),
            limit: 1,
            order: KnowledgeCrystalListOrder::ExternalIdAsc,
        })
        .unwrap_err();
    assert!(mixed_filter_error
        .to_string()
        .contains("key_match or after_id"));
}

#[test]
fn crystal_reads_use_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'crystal-cache-alpha', is_crystal: true, crystal_title: 'Alpha', importance: 0.9, created_at: 20})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'crystal-cache-beta', is_crystal: true, crystal_title: 'Beta', importance: 0.7, created_at: 30})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'crystal-cache-skip', is_crystal: false, crystal_title: 'Skip', importance: 9.0})")
        .unwrap();
    let request = KnowledgeCrystalListRequest {
        key_match: Some("crystal-cache".to_string()),
        after_id: None,
        limit: 1,
        order: KnowledgeCrystalListOrder::ImportanceDescCreatedAtDesc,
    };

    let first = db.query_crystals_via_cypher(&request).unwrap();
    let second = db.query_crystals_via_cypher(&request).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.matched_count, 2);
    assert_eq!(first.returned_count, 1);
    assert_eq!(
        first.rows[0].memory_id.as_deref(),
        Some("crystal-cache-alpha")
    );
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 1);
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hits, 1);
}

#[test]
fn merges_crystal_source_for_mcp_create_crystal_shape() {
    let path = unique_test_dir("crystal_source_merge_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'crystal-source-1', is_crystal: true, crystal_title: 'Crystal Source'})")
            .unwrap();
        db.query(
            "CREATE (:Memory {id: 'source-memory-1', is_crystal: false, title: 'Source Memory'})",
        )
        .unwrap();

        let output = db
            .merge_knowledge_crystal_source(&KnowledgeCrystalSourceMergeRequest {
                crystal_memory_id: "crystal-source-1".to_string(),
                source_memory_id: "source-memory-1".to_string(),
                weight: Value::Float(0.75),
                created_at: Value::Int(100),
            })
            .unwrap();
        assert_eq!(output.crystal_memory_id, "crystal-source-1");
        assert_eq!(output.source_memory_id, "source-memory-1");
        assert!(output.matched);
        assert!(output.created);
        assert!(!output.already_exists);
        assert!(!output.missing_endpoint);
        assert!(!output.non_writable);
        assert!(output.crystal_node_id.is_some());
        assert!(output.source_node_id.is_some());
        assert!(output.relationship_id.is_some());
        assert_eq!(output.created_relationship_count, 1);
        assert!(output.graph_commit_epoch_after > output.graph_commit_epoch_before);

        let count = db
            .query("MATCH (:Memory {id: 'crystal-source-1'})-[r:SYNTHESIZED_FROM]->(:Memory {id: 'source-memory-1'}) RETURN count(r) AS total")
            .unwrap();
        assert_eq!(count.rows[0].get("total"), Some(&Value::Int(1)));
        let properties = db
            .query("MATCH (:Memory {id: 'crystal-source-1'})-[r:SYNTHESIZED_FROM]->(:Memory {id: 'source-memory-1'}) RETURN r.weight AS weight, r.occasion_key AS occasion_key, r.created_at AS created_at")
            .unwrap();
        assert_eq!(properties.rows[0].get("weight"), Some(&Value::Float(0.75)));
        assert_eq!(
            properties.rows[0].get("occasion_key"),
            Some(&Value::String(String::new()))
        );
        assert_eq!(properties.rows[0].get("created_at"), Some(&Value::Int(100)));

        let commit_epoch_after_create = db.store.commit_epoch();
        let second = db
            .merge_knowledge_crystal_source(&KnowledgeCrystalSourceMergeRequest {
                crystal_memory_id: "crystal-source-1".to_string(),
                source_memory_id: "source-memory-1".to_string(),
                weight: Value::Float(0.25),
                created_at: Value::Int(200),
            })
            .unwrap();
        assert!(second.matched);
        assert!(!second.created);
        assert!(second.already_exists);
        assert_eq!(second.relationship_id, output.relationship_id);
        assert_eq!(second.created_relationship_count, 0);
        assert_eq!(second.graph_commit_epoch_after, commit_epoch_after_create);
        assert_eq!(db.store.commit_epoch(), commit_epoch_after_create);

        let unchanged = db
            .query("MATCH (:Memory {id: 'crystal-source-1'})-[r:SYNTHESIZED_FROM]->(:Memory {id: 'source-memory-1'}) RETURN r.weight AS weight, r.occasion_key AS occasion_key, r.created_at AS created_at")
            .unwrap();
        assert_eq!(unchanged.rows[0].get("weight"), Some(&Value::Float(0.75)));
        assert_eq!(
            unchanged.rows[0].get("occasion_key"),
            Some(&Value::String(String::new()))
        );
        assert_eq!(unchanged.rows[0].get("created_at"), Some(&Value::Int(100)));
    }
    {
        let mut db = Database::open(&path).unwrap();
        let count = db
            .query("MATCH (:Memory {id: 'crystal-source-1'})-[r:SYNTHESIZED_FROM]->(:Memory {id: 'source-memory-1'}) RETURN count(r) AS total")
            .unwrap();
        assert_eq!(count.rows[0].get("total"), Some(&Value::Int(1)));
        let properties = db
            .query("MATCH (:Memory {id: 'crystal-source-1'})-[r:SYNTHESIZED_FROM]->(:Memory {id: 'source-memory-1'}) RETURN r.weight AS weight, r.occasion_key AS occasion_key, r.created_at AS created_at")
            .unwrap();
        assert_eq!(properties.rows[0].get("weight"), Some(&Value::Float(0.75)));
        assert_eq!(
            properties.rows[0].get("occasion_key"),
            Some(&Value::String(String::new()))
        );
        assert_eq!(properties.rows[0].get("created_at"), Some(&Value::Int(100)));
    }

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn crystal_source_merge_reports_missing_endpoint_and_rejects_invalid_inputs() {
    let mut db = Database::new();

    let empty_crystal = db
        .merge_knowledge_crystal_source(&KnowledgeCrystalSourceMergeRequest {
            crystal_memory_id: String::new(),
            source_memory_id: "source-memory-1".to_string(),
            weight: Value::Float(1.0),
            created_at: Value::Int(100),
        })
        .unwrap_err();
    assert!(empty_crystal
        .to_string()
        .contains("non-empty crystal memory id"));

    let empty_source = db
        .merge_knowledge_crystal_source(&KnowledgeCrystalSourceMergeRequest {
            crystal_memory_id: "crystal-source-1".to_string(),
            source_memory_id: String::new(),
            weight: Value::Float(1.0),
            created_at: Value::Int(100),
        })
        .unwrap_err();
    assert!(empty_source
        .to_string()
        .contains("non-empty source memory id"));

    let invalid_weight = db
        .merge_knowledge_crystal_source(&KnowledgeCrystalSourceMergeRequest {
            crystal_memory_id: "crystal-source-1".to_string(),
            source_memory_id: "source-memory-1".to_string(),
            weight: Value::String("heavy".to_string()),
            created_at: Value::Int(100),
        })
        .unwrap_err();
    assert!(invalid_weight.to_string().contains("numeric finite weight"));

    db.query("CREATE (:Memory {id: 'crystal-source-1', is_crystal: true})")
        .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();
    let missing = db
        .merge_knowledge_crystal_source(&KnowledgeCrystalSourceMergeRequest {
            crystal_memory_id: "crystal-source-1".to_string(),
            source_memory_id: "missing-source-memory".to_string(),
            weight: Value::Int(1),
            created_at: Value::Int(100),
        })
        .unwrap();
    assert!(!missing.matched);
    assert!(!missing.created);
    assert!(!missing.already_exists);
    assert!(missing.missing_endpoint);
    assert!(!missing.non_writable);
    assert!(missing.crystal_node_id.is_some());
    assert_eq!(missing.source_node_id, None);
    assert_eq!(missing.relationship_id, None);
    assert_eq!(missing.created_relationship_count, 0);
    assert_eq!(missing.graph_commit_epoch_before, graph_commit_epoch);
    assert_eq!(missing.graph_commit_epoch_after, graph_commit_epoch);
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);
}

#[test]
fn reads_crystal_communities_for_topic_ranking_and_okf_mapping() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'crystal-alpha', is_crystal: true, crystal_title: 'Alpha Crystal', title: 'Alpha Title', content: 'Alpha content', importance: 0.8, metadata: '{\"c\":1}', is_latest: true, lifecycle_state: 'active'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'crystal-beta', is_crystal: true, title: 'Beta Title', content: 'Beta content', importance: 0.9, is_latest: false})")
        .unwrap();
    db.query(
        "CREATE (:Memory {id: 'plain-memory', is_crystal: false, title: 'Plain', importance: 9.0})",
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 'source-one', metadata: '{\"s\":1}', is_latest: true, lifecycle_state: 'ready'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'source-two', metadata: '{\"s\":2}', is_latest: false, lifecycle_state: 'archived'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'source-three'})").unwrap();
    db.query("CREATE (:Entity {id: 'entity-one', name: 'One', community_id: 7})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity-two', name: 'Two', community_id: 7})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity-three', name: 'Three', community_id: 8})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity-null', name: 'Null Entity'})")
        .unwrap();
    db.query(
        "MATCH (c:Memory {id: 'crystal-alpha'}), (s:Memory {id: 'source-one'}) CREATE (c)-[:SYNTHESIZED_FROM]->(s)",
    )
    .unwrap();
    db.query(
        "MATCH (c:Memory {id: 'crystal-alpha'}), (s:Memory {id: 'source-two'}) CREATE (c)-[:SYNTHESIZED_FROM]->(s)",
    )
    .unwrap();
    db.query(
        "MATCH (c:Memory {id: 'crystal-beta'}), (s:Memory {id: 'source-three'}) CREATE (c)-[:SYNTHESIZED_FROM]->(s)",
    )
    .unwrap();
    db.query(
        "MATCH (c:Memory {id: 'plain-memory'}), (s:Memory {id: 'source-one'}) CREATE (c)-[:SYNTHESIZED_FROM]->(s)",
    )
    .unwrap();
    db.query(
        "MATCH (s:Memory {id: 'source-one'}), (e:Entity {id: 'entity-one'}) CREATE (s)-[:MENTIONS]->(e)",
    )
    .unwrap();
    db.query(
        "MATCH (s:Memory {id: 'source-one'}), (e:Entity {id: 'entity-two'}) CREATE (s)-[:MENTIONS]->(e)",
    )
    .unwrap();
    db.query(
        "MATCH (s:Memory {id: 'source-two'}), (e:Entity {id: 'entity-one'}) CREATE (s)-[:MENTIONS]->(e)",
    )
    .unwrap();
    db.query(
        "MATCH (s:Memory {id: 'source-two'}), (e:Entity {id: 'entity-three'}) CREATE (s)-[:MENTIONS]->(e)",
    )
    .unwrap();
    db.query(
        "MATCH (s:Memory {id: 'source-two'}), (e:Entity {id: 'entity-null'}) CREATE (s)-[:MENTIONS]->(e)",
    )
    .unwrap();
    db.query(
        "MATCH (s:Memory {id: 'source-three'}), (e:Entity {id: 'entity-three'}) CREATE (s)-[:MENTIONS]->(e)",
    )
    .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();

    let topic = db
        .query_crystal_communities_via_cypher(&KnowledgeCrystalCommunityListRequest {
            scope: KnowledgeCrystalCommunityScope::CommunityIds(vec![Value::Int(7)]),
            limit: 10,
            order: KnowledgeCrystalCommunityListOrder::HitsDescImportanceDesc,
        })
        .unwrap();
    assert_eq!(topic.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);
    assert_eq!(topic.matched_path_count, 3);
    assert_eq!(topic.matched_pair_count, 1);
    assert_eq!(topic.returned_count, 1);
    assert_eq!(
        topic.rows[0].crystal_memory_id.as_deref(),
        Some("crystal-alpha")
    );
    assert_eq!(topic.rows[0].community_id, Value::Int(7));
    assert_eq!(topic.rows[0].hit_count, 3);
    assert_eq!(topic.rows[0].source_memory_count, 2);
    assert_eq!(
        topic.rows[0].crystal_title.as_deref(),
        Some("Alpha Crystal")
    );
    assert_eq!(topic.rows[0].display_title, "Alpha Crystal");
    assert_eq!(topic.rows[0].content.as_deref(), Some("Alpha content"));
    assert_eq!(topic.rows[0].importance, Some(Value::Float(0.8)));
    assert_eq!(
        topic.rows[0].metadata,
        Some(Value::String("{\"c\":1}".to_string()))
    );
    assert_eq!(topic.rows[0].is_latest, Some(true));
    assert_eq!(topic.rows[0].lifecycle_state.as_deref(), Some("active"));
}

#[test]
fn crystal_community_read_rejects_invalid_scope() {
    let db = Database::new();

    let empty_ids_error = db
        .query_crystal_communities_via_cypher(&KnowledgeCrystalCommunityListRequest {
            scope: KnowledgeCrystalCommunityScope::CommunityIds(Vec::new()),
            limit: 10,
            order: KnowledgeCrystalCommunityListOrder::HitsDescImportanceDesc,
        })
        .unwrap_err();
    assert!(empty_ids_error
        .to_string()
        .contains("non-empty community ids"));

    let null_id_error = db
        .query_crystal_communities_via_cypher(&KnowledgeCrystalCommunityListRequest {
            scope: KnowledgeCrystalCommunityScope::CommunityIds(vec![Value::Null]),
            limit: 10,
            order: KnowledgeCrystalCommunityListOrder::HitsDescImportanceDesc,
        })
        .unwrap_err();
    assert!(null_id_error.to_string().contains("non-null community ids"));
}

#[test]
fn crystal_community_reads_use_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'community-cache-crystal', is_crystal: true, importance: 0.8})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'community-cache-source-one'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'community-cache-source-two'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'community-cache-entity-one', community_id: 13})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'community-cache-entity-two', community_id: 13})")
        .unwrap();
    db.query("MATCH (c:Memory {id: 'community-cache-crystal'}), (s:Memory {id: 'community-cache-source-one'}) CREATE (c)-[:SYNTHESIZED_FROM]->(s)")
        .unwrap();
    db.query("MATCH (c:Memory {id: 'community-cache-crystal'}), (s:Memory {id: 'community-cache-source-two'}) CREATE (c)-[:SYNTHESIZED_FROM]->(s)")
        .unwrap();
    db.query("MATCH (s:Memory {id: 'community-cache-source-one'}), (e:Entity {id: 'community-cache-entity-one'}) CREATE (s)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (s:Memory {id: 'community-cache-source-one'}), (e:Entity {id: 'community-cache-entity-two'}) CREATE (s)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (s:Memory {id: 'community-cache-source-two'}), (e:Entity {id: 'community-cache-entity-one'}) CREATE (s)-[:MENTIONS]->(e)")
        .unwrap();
    let request = KnowledgeCrystalCommunityListRequest {
        scope: KnowledgeCrystalCommunityScope::CommunityIds(vec![Value::Int(13)]),
        limit: 0,
        order: KnowledgeCrystalCommunityListOrder::HitsDescImportanceDesc,
    };

    let first = db.query_crystal_communities_via_cypher(&request).unwrap();
    let second = db.query_crystal_communities_via_cypher(&request).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.matched_path_count, 3);
    assert_eq!(first.matched_pair_count, 1);
    assert_eq!(first.returned_count, 1);
    assert_eq!(first.rows[0].hit_count, 3);
    assert_eq!(first.rows[0].source_memory_count, 2);
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 1);
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hits, 1);
}

#[test]
fn reads_crystal_source_visibility_for_wiki_community_rows() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'crystal-alpha', is_crystal: true, crystal_title: 'Alpha Crystal', title: 'Alpha Title', content: 'Alpha content', importance: 0.8, metadata: '{\"c\":1}', is_latest: false, lifecycle_state: 'active'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'crystal-beta', is_crystal: true, title: 'Beta Title', content: 'Beta content', importance: 0.9})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'plain-memory', is_crystal: false, title: 'Plain', importance: 9.0, metadata: '{\"plain\":1}'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'source-one', metadata: '{\"s\":1}', is_latest: true, lifecycle_state: 'ready'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'source-two', metadata: '{\"s\":2}', is_latest: false, lifecycle_state: 'archived'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'source-three'})").unwrap();
    db.query("CREATE (:Entity {id: 'entity-one', name: 'One', community_id: 7})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity-two', name: 'Two', community_id: 7})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity-three', name: 'Three', community_id: 8})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity-null', name: 'Null Entity'})")
        .unwrap();
    db.query(
        "MATCH (c:Memory {id: 'crystal-alpha'}), (s:Memory {id: 'source-one'}) CREATE (c)-[:SYNTHESIZED_FROM]->(s)",
    )
    .unwrap();
    db.query(
        "MATCH (c:Memory {id: 'crystal-alpha'}), (s:Memory {id: 'source-two'}) CREATE (c)-[:SYNTHESIZED_FROM]->(s)",
    )
    .unwrap();
    db.query(
        "MATCH (c:Memory {id: 'crystal-beta'}), (s:Memory {id: 'source-three'}) CREATE (c)-[:SYNTHESIZED_FROM]->(s)",
    )
    .unwrap();
    db.query(
        "MATCH (c:Memory {id: 'plain-memory'}), (s:Memory {id: 'source-one'}) CREATE (c)-[:SYNTHESIZED_FROM]->(s)",
    )
    .unwrap();
    db.query(
        "MATCH (s:Memory {id: 'source-one'}), (e:Entity {id: 'entity-one'}) CREATE (s)-[:MENTIONS]->(e)",
    )
    .unwrap();
    db.query(
        "MATCH (s:Memory {id: 'source-one'}), (e:Entity {id: 'entity-two'}) CREATE (s)-[:MENTIONS]->(e)",
    )
    .unwrap();
    db.query(
        "MATCH (s:Memory {id: 'source-two'}), (e:Entity {id: 'entity-one'}) CREATE (s)-[:MENTIONS]->(e)",
    )
    .unwrap();
    db.query(
        "MATCH (s:Memory {id: 'source-two'}), (e:Entity {id: 'entity-three'}) CREATE (s)-[:MENTIONS]->(e)",
    )
    .unwrap();
    db.query(
        "MATCH (s:Memory {id: 'source-two'}), (e:Entity {id: 'entity-null'}) CREATE (s)-[:MENTIONS]->(e)",
    )
    .unwrap();
    db.query(
        "MATCH (s:Memory {id: 'source-three'}), (e:Entity {id: 'entity-three'}) CREATE (s)-[:MENTIONS]->(e)",
    )
    .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();

    let visibility = db
        .query_crystal_source_visibility_via_cypher(&KnowledgeCrystalSourceVisibilityRequest {
            community_ids: vec![Value::Int(7), Value::Int(8)],
            limit: 0,
        })
        .unwrap();
    assert_eq!(visibility.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);
    assert_eq!(visibility.matched_path_count, 5);
    assert_eq!(visibility.returned_count, 5);
    assert_eq!(
        visibility.rows[0].crystal_memory_id.as_deref(),
        Some("crystal-alpha")
    );
    assert_eq!(
        visibility.rows[0].source_memory_id.as_deref(),
        Some("source-one")
    );
    assert_eq!(visibility.rows[0].entity_id.as_deref(), Some("entity-one"));
    assert_eq!(visibility.rows[0].community_id, Value::Int(7));
    assert_eq!(
        visibility.rows[0].crystal_title.as_deref(),
        Some("Alpha Crystal")
    );
    assert_eq!(visibility.rows[0].display_title, "Alpha Crystal");
    assert_eq!(visibility.rows[0].content.as_deref(), Some("Alpha content"));
    assert_eq!(visibility.rows[0].importance, Some(Value::Float(0.8)));
    assert_eq!(
        visibility.rows[0].crystal_metadata,
        Some(Value::String("{\"c\":1}".to_string()))
    );
    assert!(!visibility.rows[0].crystal_is_latest);
    assert_eq!(
        visibility.rows[0].crystal_lifecycle_state.as_deref(),
        Some("active")
    );
    assert_eq!(
        visibility.rows[0].source_metadata,
        Some(Value::String("{\"s\":1}".to_string()))
    );
    assert!(visibility.rows[0].source_is_latest);
    assert_eq!(
        visibility.rows[0].source_lifecycle_state.as_deref(),
        Some("ready")
    );
    assert_eq!(
        visibility.rows[2].source_memory_id.as_deref(),
        Some("source-two")
    );
    assert!(!visibility.rows[2].source_is_latest);
    assert_eq!(
        visibility.rows[4].crystal_memory_id.as_deref(),
        Some("crystal-beta")
    );
    assert_eq!(visibility.rows[4].display_title, "Beta Title");
    assert!(visibility.rows[4].crystal_is_latest);
    assert!(visibility.rows[4].source_is_latest);
}

#[test]
fn crystal_source_visibility_rejects_invalid_scope() {
    let db = Database::new();

    let empty_ids_error = db
        .query_crystal_source_visibility_via_cypher(&KnowledgeCrystalSourceVisibilityRequest {
            community_ids: Vec::new(),
            limit: 0,
        })
        .unwrap_err();
    assert!(empty_ids_error
        .to_string()
        .contains("non-empty community ids"));

    let null_id_error = db
        .query_crystal_source_visibility_via_cypher(&KnowledgeCrystalSourceVisibilityRequest {
            community_ids: vec![Value::Null],
            limit: 0,
        })
        .unwrap_err();
    assert!(null_id_error.to_string().contains("non-null community ids"));
}

#[test]
fn crystal_source_visibility_uses_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query(
        "CREATE (:Memory {id: 'visibility-cache-crystal', is_crystal: true, title: 'Cached'})",
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 'visibility-cache-source'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'visibility-cache-entity', community_id: 11})")
        .unwrap();
    db.query("MATCH (c:Memory {id: 'visibility-cache-crystal'}), (s:Memory {id: 'visibility-cache-source'}) CREATE (c)-[:SYNTHESIZED_FROM]->(s)")
        .unwrap();
    db.query("MATCH (s:Memory {id: 'visibility-cache-source'}), (e:Entity {id: 'visibility-cache-entity'}) CREATE (s)-[:MENTIONS]->(e)")
        .unwrap();
    let request = KnowledgeCrystalSourceVisibilityRequest {
        community_ids: vec![Value::Int(11)],
        limit: 0,
    };

    let first = db
        .query_crystal_source_visibility_via_cypher(&request)
        .unwrap();
    let second = db
        .query_crystal_source_visibility_via_cypher(&request)
        .unwrap();

    assert_eq!(first, second);
    assert_eq!(first.matched_path_count, 1);
    assert_eq!(first.returned_count, 1);
    assert_eq!(
        first.rows[0].crystal_memory_id.as_deref(),
        Some("visibility-cache-crystal")
    );
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 1);
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hits, 1);
}

#[test]
fn scoped_knowledge_entity_batch_reports_filtered_and_missing_items() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'memory_1', title: 'First', source_id: 'thread_1', space_id: ''})",
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', title: 'Second', source_id: 'thread_2', space_id: 'default'})")
        .unwrap();

    let output = db
        .query_scoped_entity_batch_via_cypher(&KnowledgeScopedEntityBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_2".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "missing".to_string(),
                },
            ],
            metadata_filters: BTreeMap::from([
                ("source_id".to_string(), "thread_1".to_string()),
                ("space_id".to_string(), "default".to_string()),
            ]),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch, 2);
    assert_eq!(output.entities.len(), 3);
    assert_eq!(output.found_count, 1);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.filtered_out_count, 1);
    assert_eq!(
        output.entities[0]
            .as_ref()
            .and_then(|entity| entity.external_id.as_deref()),
        Some("memory_1")
    );
    assert!(output.entities[1].is_none());
    assert!(output.entities[2].is_none());
}

#[test]
fn scoped_knowledge_entity_batch_uses_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'scoped_cache_memory_1', source_id: 'thread_1', space_id: ''})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'scoped_cache_memory_2', source_id: 'thread_2', space_id: 'default'})")
        .unwrap();
    let request = KnowledgeScopedEntityBatchRequest {
        entities: vec![
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "scoped_cache_memory_1".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "scoped_cache_memory_2".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "scoped_cache_missing".to_string(),
            },
        ],
        metadata_filters: BTreeMap::from([
            ("source_id".to_string(), "thread_1".to_string()),
            ("space_id".to_string(), "default".to_string()),
        ]),
    };

    let first = db.query_scoped_entity_batch_via_cypher(&request).unwrap();
    let second = db.query_scoped_entity_batch_via_cypher(&request).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.found_count, 1);
    assert_eq!(first.filtered_out_count, 1);
    assert_eq!(first.missing_count, 1);
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 1);
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hits, 1);
}

#[test]
fn creates_knowledge_entity_through_typed_api() {
    let mut db = Database::new();

    let output = db
        .create_knowledge_entity(&KnowledgeEntityCreateRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
            properties: BTreeMap::from([
                ("title".to_string(), Value::String("First".to_string())),
                (
                    "source_id".to_string(),
                    Value::String("thread_1".to_string()),
                ),
            ]),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 0);
    assert_eq!(output.graph_commit_epoch_after, 1);
    assert_eq!(output.node_id, Some(0));
    assert!(output.created);
    assert!(!output.already_exists);
    assert_eq!(output.created_node_count, 1);
    let entity = db
        .query_entity_via_cypher(&KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        })
        .unwrap()
        .entity
        .expect("expected created memory");
    assert_eq!(
        entity.properties.get("title"),
        Some(&Value::String("First".to_string()))
    );
    assert_eq!(
        entity.properties.get("id"),
        Some(&Value::String("memory_1".to_string()))
    );
}

#[test]
fn create_knowledge_entity_reports_existing_identity_without_writing() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'Old'})")
        .unwrap();

    let output = db
        .create_knowledge_entity(&KnowledgeEntityCreateRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
            properties: BTreeMap::from([("title".to_string(), Value::String("New".to_string()))]),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 1);
    assert_eq!(output.node_id, Some(0));
    assert!(!output.created);
    assert!(output.already_exists);
    assert_eq!(output.created_node_count, 0);
    let entity = db
        .query_entity_via_cypher(&KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        })
        .unwrap()
        .entity
        .expect("expected existing memory");
    assert_eq!(
        entity.properties.get("title"),
        Some(&Value::String("Old".to_string()))
    );
}

#[test]
fn creates_knowledge_entity_batch_through_typed_api() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'existing', title: 'Existing'})")
        .unwrap();

    let output = db
        .create_knowledge_entity_batch(&KnowledgeEntityCreateBatchRequest {
            creates: vec![
                KnowledgeEntityCreateRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                    properties: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("First".to_string()),
                    )]),
                },
                KnowledgeEntityCreateRequest {
                    label: "Memory".to_string(),
                    external_id: "existing".to_string(),
                    properties: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("Ignored".to_string()),
                    )]),
                },
                KnowledgeEntityCreateRequest {
                    label: "Entity".to_string(),
                    external_id: "entity_1".to_string(),
                    properties: BTreeMap::from([(
                        "name".to_string(),
                        Value::String("HawDB".to_string()),
                    )]),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.rows.len(), 3);
    assert_eq!(output.created_count, 2);
    assert_eq!(output.already_exists_count, 1);
    assert_eq!(output.created_node_count, 2);
    assert!(output.rows[0].created);
    assert!(output.rows[0].node_id.is_some());
    assert!(output.rows[1].already_exists);
    assert_eq!(output.rows[1].node_id, Some(0));
    assert!(output.rows[2].created);
    let created = db
        .query_entity_batch_via_cypher(&KnowledgeEntityBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Entity".to_string(),
                    external_id: "entity_1".to_string(),
                },
            ],
        })
        .unwrap();
    assert_eq!(created.found_count, 2);
}

#[test]
fn create_knowledge_entity_batch_deduplicates_pending_identity() {
    let mut db = Database::new();

    let output = db
        .create_knowledge_entity_batch(&KnowledgeEntityCreateBatchRequest {
            creates: vec![
                KnowledgeEntityCreateRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                    properties: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("First".to_string()),
                    )]),
                },
                KnowledgeEntityCreateRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                    properties: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("Duplicate".to_string()),
                    )]),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 0);
    assert_eq!(output.graph_commit_epoch_after, 1);
    assert_eq!(output.created_count, 1);
    assert_eq!(output.already_exists_count, 1);
    assert!(output.rows[0].created);
    assert!(output.rows[1].already_exists);
    assert_eq!(output.rows[1].node_id, None);
    let entities = db
        .query_entity_batch_via_cypher(&KnowledgeEntityBatchRequest {
            entities: vec![KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            }],
        })
        .unwrap();
    assert_eq!(entities.found_count, 1);
}

#[test]
fn create_knowledge_entity_rejects_invalid_identifiers_and_id_mismatch() {
    let mut db = Database::new();
    let invalid_property = db
        .create_knowledge_entity(&KnowledgeEntityCreateRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
            properties: BTreeMap::from([("bad-name".to_string(), Value::Int(1))]),
        })
        .unwrap_err();
    assert!(invalid_property.to_string().contains("property identifier"));
    let id_mismatch = db
        .create_knowledge_entity(&KnowledgeEntityCreateRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
            properties: BTreeMap::from([(
                "id".to_string(),
                Value::String("different".to_string()),
            )]),
        })
        .unwrap_err();
    assert!(id_mismatch
        .to_string()
        .contains("does not match external id"));
    assert_eq!(db.store.commit_epoch(), 0);
}

#[test]
fn read_only_database_rejects_typed_knowledge_entity_create() {
    let path = unique_test_dir("read_only_typed_knowledge_entity_create");
    {
        let _db = Database::open(&path).unwrap();
    }
    {
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                read_only: true,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        let error = db
            .create_knowledge_entity(&KnowledgeEntityCreateRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
                properties: BTreeMap::new(),
            })
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_knowledge_entity_batch_create_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_knowledge_entity_batch_create_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        let batch_count_before_create = read_test_wal(&path)
            .unwrap_or_default()
            .matches("\tbatch\t")
            .count();
        db.create_knowledge_entity_batch(&KnowledgeEntityCreateBatchRequest {
            creates: vec![
                KnowledgeEntityCreateRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                    properties: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("First".to_string()),
                    )]),
                },
                KnowledgeEntityCreateRequest {
                    label: "Entity".to_string(),
                    external_id: "entity_1".to_string(),
                    properties: BTreeMap::from([(
                        "name".to_string(),
                        Value::String("HawDB".to_string()),
                    )]),
                },
            ],
        })
        .unwrap();
        let batch_count_after_create = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_create, batch_count_before_create + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("create_node"));
    {
        let db = Database::open(&path).unwrap();
        let output = db
            .query_entity_batch_via_cypher(&KnowledgeEntityBatchRequest {
                entities: vec![
                    KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    KnowledgeEntityRequest {
                        label: "Entity".to_string(),
                        external_id: "entity_1".to_string(),
                    },
                ],
            })
            .unwrap();
        assert_eq!(output.found_count, 2);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn upserts_knowledge_entity_through_typed_api() {
    let mut db = Database::new();

    let created = db
        .upsert_knowledge_entity(&KnowledgeEntityUpsertRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
            create_properties: BTreeMap::from([(
                "title".to_string(),
                Value::String("First".to_string()),
            )]),
            update_properties: BTreeMap::from([(
                "updated_at".to_string(),
                Value::String("ignored-on-create".to_string()),
            )]),
        })
        .unwrap();

    assert!(created.created);
    assert!(!created.updated);
    assert!(!created.already_exists);
    assert_eq!(created.node_id, Some(0));
    assert_eq!(created.created_node_count, 1);
    assert_eq!(created.updated_property_count, 0);
    let existing = db
        .upsert_knowledge_entity(&KnowledgeEntityUpsertRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
            create_properties: BTreeMap::from([(
                "title".to_string(),
                Value::String("Should Not Replace".to_string()),
            )]),
            update_properties: BTreeMap::from([
                ("id".to_string(), Value::String("memory_1".to_string())),
                (
                    "updated_at".to_string(),
                    Value::String("2026-07-19".to_string()),
                ),
            ]),
        })
        .unwrap();

    assert!(!existing.created);
    assert!(existing.updated);
    assert!(existing.already_exists);
    assert!(!existing.non_writable);
    assert_eq!(existing.node_id, Some(0));
    assert_eq!(existing.updated_property_count, 1);
    let entity = db
        .query_entity_via_cypher(&KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        })
        .unwrap()
        .entity
        .expect("expected upserted memory");
    assert_eq!(
        entity.properties.get("title"),
        Some(&Value::String("First".to_string()))
    );
    assert_eq!(
        entity.properties.get("updated_at"),
        Some(&Value::String("2026-07-19".to_string()))
    );
}

#[test]
fn knowledge_entity_upsert_rejects_id_mismatch_before_writing() {
    let mut db = Database::new();

    let error = db
        .upsert_knowledge_entity(&KnowledgeEntityUpsertRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
            create_properties: BTreeMap::new(),
            update_properties: BTreeMap::from([(
                "id".to_string(),
                Value::String("different".to_string()),
            )]),
        })
        .unwrap_err();

    assert!(error.to_string().contains("does not match external id"));
    assert_eq!(db.store.commit_epoch(), 0);
}

#[test]
fn knowledge_entity_upsert_does_not_write_projected_idless_identity() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {name: 'HawDB', description: 'old'})")
        .unwrap();

    let output = db
        .upsert_knowledge_entity(&KnowledgeEntityUpsertRequest {
            label: "Entity".to_string(),
            external_id: "0".to_string(),
            create_properties: BTreeMap::from([(
                "description".to_string(),
                Value::String("create".to_string()),
            )]),
            update_properties: BTreeMap::from([(
                "description".to_string(),
                Value::String("new".to_string()),
            )]),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 1);
    assert_eq!(output.node_id, Some(0));
    assert!(output.already_exists);
    assert!(output.non_writable);
    assert!(!output.updated);
    let entity = db
        .query_entity_via_cypher(&KnowledgeEntityRequest {
            label: "Entity".to_string(),
            external_id: "0".to_string(),
        })
        .unwrap()
        .entity
        .expect("expected projected entity");
    assert_eq!(
        entity.properties.get("description"),
        Some(&Value::String("old".to_string()))
    );
}

#[test]
fn upserts_knowledge_entity_batch_through_typed_api() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'existing', title: 'Old'})")
        .unwrap();

    let output = db
        .upsert_knowledge_entity_batch(&KnowledgeEntityUpsertBatchRequest {
            upserts: vec![
                KnowledgeEntityUpsertRequest {
                    label: "Memory".to_string(),
                    external_id: "created".to_string(),
                    create_properties: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("Created".to_string()),
                    )]),
                    update_properties: BTreeMap::new(),
                },
                KnowledgeEntityUpsertRequest {
                    label: "Memory".to_string(),
                    external_id: "existing".to_string(),
                    create_properties: BTreeMap::new(),
                    update_properties: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("Updated".to_string()),
                    )]),
                },
                KnowledgeEntityUpsertRequest {
                    label: "Memory".to_string(),
                    external_id: "created".to_string(),
                    create_properties: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("Duplicate".to_string()),
                    )]),
                    update_properties: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("Duplicate Update".to_string()),
                    )]),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.created_count, 1);
    assert_eq!(output.updated_count, 1);
    assert_eq!(output.already_exists_count, 2);
    assert_eq!(output.created_node_count, 1);
    assert_eq!(output.updated_property_count, 1);
    assert!(output.rows[0].created);
    assert!(output.rows[0].node_id.is_some());
    assert!(output.rows[1].updated);
    assert_eq!(output.rows[1].node_id, Some(0));
    assert!(output.rows[2].already_exists);
    assert_eq!(output.rows[2].node_id, None);

    let rows = db
        .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "created".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "existing".to_string(),
                },
            ],
            property_names: vec!["title".to_string()],
        })
        .unwrap();
    assert_eq!(
        rows.rows[0].properties.get("title"),
        Some(&Some(Value::String("Created".to_string())))
    );
    assert_eq!(
        rows.rows[1].properties.get("title"),
        Some(&Some(Value::String("Updated".to_string())))
    );
}

#[test]
fn read_only_database_rejects_typed_knowledge_entity_upsert() {
    let path = unique_test_dir("read_only_typed_knowledge_entity_upsert");
    {
        let _db = Database::open(&path).unwrap();
    }
    {
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                read_only: true,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        let error = db
            .upsert_knowledge_entity(&KnowledgeEntityUpsertRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
                create_properties: BTreeMap::new(),
                update_properties: BTreeMap::new(),
            })
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_knowledge_entity_batch_upsert_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_knowledge_entity_batch_upsert_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'existing', title: 'Old'})")
            .unwrap();
        let batch_count_before_upsert = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.upsert_knowledge_entity_batch(&KnowledgeEntityUpsertBatchRequest {
            upserts: vec![
                KnowledgeEntityUpsertRequest {
                    label: "Memory".to_string(),
                    external_id: "created".to_string(),
                    create_properties: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("Created".to_string()),
                    )]),
                    update_properties: BTreeMap::new(),
                },
                KnowledgeEntityUpsertRequest {
                    label: "Memory".to_string(),
                    external_id: "existing".to_string(),
                    create_properties: BTreeMap::new(),
                    update_properties: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("Updated".to_string()),
                    )]),
                },
            ],
        })
        .unwrap();
        let batch_count_after_upsert = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_upsert, batch_count_before_upsert + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("create_node"));
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db
            .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
                entities: vec![
                    KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "created".to_string(),
                    },
                    KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "existing".to_string(),
                    },
                ],
                property_names: vec!["title".to_string()],
            })
            .unwrap();
        assert_eq!(rows.found_count, 2);
        assert_eq!(
            rows.rows[0].properties.get("title"),
            Some(&Some(Value::String("Created".to_string())))
        );
        assert_eq!(
            rows.rows[1].properties.get("title"),
            Some(&Some(Value::String("Updated".to_string())))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn retrieves_knowledge_property_batch_without_hydrating_full_entities() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'First', source_id: 'thread_1', metadata: 'large metadata'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', title: 'Second', source_id: 'thread_2'})")
        .unwrap();

    let output = db
        .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_2".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "missing".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
            ],
            property_names: vec![
                "title".to_string(),
                "source_id".to_string(),
                "title".to_string(),
                "metadata".to_string(),
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch, 2);
    assert_eq!(
        output.property_names,
        vec![
            "title".to_string(),
            "source_id".to_string(),
            "metadata".to_string()
        ]
    );
    assert_eq!(output.rows.len(), 3);
    assert_eq!(output.found_count, 2);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.filtered_out_count, 0);
    assert_eq!(
        output.rows[0].properties.get("title"),
        Some(&Some(Value::String("Second".to_string())))
    );
    assert_eq!(output.rows[0].properties.get("metadata"), Some(&None));
    assert!(output.rows[1].node_id.is_none());
    assert_eq!(output.rows[1].properties.get("title"), Some(&None));
    assert_eq!(
        output.rows[2].properties.get("metadata"),
        Some(&Some(Value::String("large metadata".to_string())))
    );
}

#[test]
fn knowledge_property_batch_uses_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'property_cache_1', title: 'First', source_id: 'thread_1'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'property_cache_2', title: 'Second', source_id: 'thread_2'})")
        .unwrap();
    let request = KnowledgePropertyBatchRequest {
        entities: vec![
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "property_cache_2".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "property_cache_missing".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "property_cache_1".to_string(),
            },
        ],
        property_names: vec!["title".to_string(), "source_id".to_string()],
    };

    let first = db.query_property_batch_via_cypher(&request).unwrap();
    let second = db.query_property_batch_via_cypher(&request).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.found_count, 2);
    assert_eq!(first.missing_count, 1);
    assert_eq!(
        first.rows[0].properties.get("title"),
        Some(&Some(Value::String("Second".to_string())))
    );
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 1);
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hits, 1);
}

#[test]
fn scoped_knowledge_property_batch_reports_filtered_rows() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'memory_1', title: 'First', source_id: 'thread_1', space_id: ''})",
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', title: 'Second', source_id: 'thread_2', space_id: 'default'})")
        .unwrap();

    let output = db
        .query_scoped_property_batch_via_cypher(&KnowledgeScopedPropertyBatchRequest {
            projection: KnowledgePropertyBatchRequest {
                entities: vec![
                    KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                ],
                property_names: vec!["title".to_string(), "source_id".to_string()],
            },
            metadata_filters: BTreeMap::from([
                ("source_id".to_string(), "thread_1".to_string()),
                ("space_id".to_string(), "default".to_string()),
            ]),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch, 2);
    assert_eq!(output.found_count, 1);
    assert_eq!(output.missing_count, 0);
    assert_eq!(output.filtered_out_count, 1);
    assert!(!output.rows[0].filtered_out);
    assert_eq!(
        output.rows[0].properties.get("title"),
        Some(&Some(Value::String("First".to_string())))
    );
    assert!(output.rows[1].filtered_out);
    assert_eq!(output.rows[1].properties.get("title"), Some(&None));
}

#[test]
fn scoped_knowledge_property_batch_uses_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'scoped_property_cache_1', title: 'First', source_id: 'thread_1', space_id: ''})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'scoped_property_cache_2', title: 'Second', source_id: 'thread_2', space_id: 'default'})")
        .unwrap();
    let request = KnowledgeScopedPropertyBatchRequest {
        projection: KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "scoped_property_cache_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "scoped_property_cache_2".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "scoped_property_cache_missing".to_string(),
                },
            ],
            property_names: vec!["title".to_string(), "source_id".to_string()],
        },
        metadata_filters: BTreeMap::from([
            ("source_id".to_string(), "thread_1".to_string()),
            ("space_id".to_string(), "default".to_string()),
        ]),
    };

    let first = db.query_scoped_property_batch_via_cypher(&request).unwrap();
    let second = db.query_scoped_property_batch_via_cypher(&request).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.found_count, 1);
    assert_eq!(first.filtered_out_count, 1);
    assert_eq!(first.missing_count, 1);
    assert_eq!(
        first.rows[0].properties.get("title"),
        Some(&Some(Value::String("First".to_string())))
    );
    assert!(first.rows[1].filtered_out);
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 1);
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hits, 1);
}

#[test]
fn updates_knowledge_properties_through_typed_api() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'Old', review_status: 'pending'})")
        .unwrap();

    let output = db
        .update_knowledge_properties(&KnowledgePropertyUpdateRequest {
            entity: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            assignments: BTreeMap::from([
                ("title".to_string(), Value::String("New".to_string())),
                (
                    "review_status".to_string(),
                    Value::String("approved".to_string()),
                ),
            ]),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.node_id, Some(0));
    assert!(output.matched);
    assert!(!output.filtered_out);
    assert_eq!(output.updated_property_count, 2);
    let row = db
        .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
            entities: vec![KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            }],
            property_names: vec!["title".to_string(), "review_status".to_string()],
        })
        .unwrap();
    assert_eq!(
        row.rows[0].properties.get("title"),
        Some(&Some(Value::String("New".to_string())))
    );
    assert_eq!(
        row.rows[0].properties.get("review_status"),
        Some(&Some(Value::String("approved".to_string())))
    );
}

#[test]
fn scoped_knowledge_property_update_does_not_write_filtered_seed() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'memory_1', title: 'Old', source_id: 'thread_1', space_id: ''})",
    )
    .unwrap();

    let output = db
        .update_scoped_knowledge_properties(&KnowledgeScopedPropertyUpdateRequest {
            update: KnowledgePropertyUpdateRequest {
                entity: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                assignments: BTreeMap::from([(
                    "title".to_string(),
                    Value::String("New".to_string()),
                )]),
            },
            metadata_filters: BTreeMap::from([
                ("source_id".to_string(), "thread_2".to_string()),
                ("space_id".to_string(), "default".to_string()),
            ]),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 1);
    assert_eq!(output.node_id, Some(0));
    assert!(!output.matched);
    assert!(output.filtered_out);
    assert_eq!(output.updated_property_count, 0);
    let row = db
        .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
            entities: vec![KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            }],
            property_names: vec!["title".to_string()],
        })
        .unwrap();
    assert_eq!(
        row.rows[0].properties.get("title"),
        Some(&Some(Value::String("Old".to_string())))
    );
}

#[test]
fn knowledge_property_update_rejects_invalid_identifiers() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'Old'})")
        .unwrap();

    let error = db
        .update_knowledge_properties(&KnowledgePropertyUpdateRequest {
            entity: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            assignments: BTreeMap::from([(
                "bad-name".to_string(),
                Value::String("New".to_string()),
            )]),
        })
        .unwrap_err();

    assert!(error.to_string().contains("property identifier"));
    assert_eq!(db.store.commit_epoch(), 1);
}

#[test]
fn read_only_database_rejects_typed_knowledge_property_update() {
    let path = unique_test_dir("read_only_typed_knowledge_property_update");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1', title: 'Old'})")
            .unwrap();
    }
    {
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                read_only: true,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        let error = db
            .update_knowledge_properties(&KnowledgePropertyUpdateRequest {
                entity: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                assignments: BTreeMap::from([(
                    "title".to_string(),
                    Value::String("New".to_string()),
                )]),
            })
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_knowledge_property_update_persists_and_replays_from_wal() {
    let path = unique_test_dir("typed_knowledge_property_update_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1', title: 'Old'})")
            .unwrap();
        db.update_knowledge_properties(&KnowledgePropertyUpdateRequest {
            entity: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            assignments: BTreeMap::from([("title".to_string(), Value::String("New".to_string()))]),
        })
        .unwrap();
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let output = db
            .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
                entities: vec![KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                }],
                property_names: vec!["title".to_string()],
            })
            .unwrap();
        assert_eq!(
            output.rows[0].properties.get("title"),
            Some(&Some(Value::String("New".to_string())))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn updates_knowledge_properties_batch_through_typed_api() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'Old 1', review_status: 'pending'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', title: 'Old 2', review_status: 'pending'})")
        .unwrap();

    let output = db
        .update_knowledge_properties_batch(&KnowledgePropertyUpdateBatchRequest {
            updates: vec![
                KnowledgePropertyUpdateRequest {
                    entity: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    assignments: BTreeMap::from([
                        ("title".to_string(), Value::String("New 1".to_string())),
                        (
                            "review_status".to_string(),
                            Value::String("approved".to_string()),
                        ),
                    ]),
                },
                KnowledgePropertyUpdateRequest {
                    entity: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                    assignments: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("New 2".to_string()),
                    )]),
                },
                KnowledgePropertyUpdateRequest {
                    entity: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "missing".to_string(),
                    },
                    assignments: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("Ignored".to_string()),
                    )]),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 3);
    assert_eq!(output.rows.len(), 3);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.filtered_out_count, 0);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.updated_property_count, 3);
    assert_eq!(output.rows[0].updated_property_count, 2);
    assert_eq!(output.rows[1].updated_property_count, 1);
    assert_eq!(output.rows[2].node_id, None);

    let row = db
        .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_2".to_string(),
                },
            ],
            property_names: vec!["title".to_string(), "review_status".to_string()],
        })
        .unwrap();
    assert_eq!(
        row.rows[0].properties.get("title"),
        Some(&Some(Value::String("New 1".to_string())))
    );
    assert_eq!(
        row.rows[0].properties.get("review_status"),
        Some(&Some(Value::String("approved".to_string())))
    );
    assert_eq!(
        row.rows[1].properties.get("title"),
        Some(&Some(Value::String("New 2".to_string())))
    );
}

#[test]
fn moves_thread_normalized_space_batch_by_identity_property() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'storage-1', thread_id: 'thread-1', space_id: ''})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'storage-2', thread_id: 'thread-2', space_id: 'default'})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'storage-3', thread_id: 'thread-3', space_id: 'team'})")
        .unwrap();

    let output = db
        .move_knowledge_normalized_space_batch(&KnowledgeNormalizedSpaceMoveBatchRequest {
            label: "Thread".to_string(),
            identity_property: "thread_id".to_string(),
            external_ids: vec![
                "thread-1".to_string(),
                "thread-2".to_string(),
                "thread-3".to_string(),
                "thread-2".to_string(),
                "missing".to_string(),
            ],
            source_space_id: Some("default".to_string()),
            target_space_id: "team".to_string(),
            updated_at: Some(Value::String("2026-07-19".to_string())),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 3);
    assert_eq!(output.graph_commit_epoch_after, 4);
    assert_eq!(output.matched_count, 4);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.source_mismatch_count, 1);
    assert_eq!(output.already_in_target_count, 0);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.moved_count, 2);
    assert_eq!(
        output.moved_external_ids,
        vec!["thread-1".to_string(), "thread-2".to_string()]
    );
    assert!(output.rows[0].moved);
    assert!(output.rows[1].moved);
    assert!(output.rows[2].source_mismatch);
    assert!(output.rows[3].duplicate);
    assert!(!output.rows[4].matched);

    let rows = db
        .query("MATCH (t:Thread) RETURN t.thread_id, t.space_id, t.updated_at ORDER BY t.thread_id")
        .unwrap();
    assert_eq!(
        rows.rows[0].get("t.space_id"),
        Some(&Value::String("team".to_string()))
    );
    assert_eq!(
        rows.rows[0].get("t.updated_at"),
        Some(&Value::String("2026-07-19".to_string()))
    );
    assert_eq!(
        rows.rows[1].get("t.space_id"),
        Some(&Value::String("team".to_string()))
    );
    assert_eq!(
        rows.rows[2].get("t.space_id"),
        Some(&Value::String("team".to_string()))
    );
}

#[test]
fn knowledge_normalized_space_move_batch_rejects_invalid_identifiers() {
    let mut db = Database::new();
    let error = db
        .move_knowledge_normalized_space_batch(&KnowledgeNormalizedSpaceMoveBatchRequest {
            label: "Thread".to_string(),
            identity_property: "thread-id".to_string(),
            external_ids: vec!["thread-1".to_string()],
            source_space_id: None,
            target_space_id: "team".to_string(),
            updated_at: None,
        })
        .unwrap_err();

    assert!(error.to_string().contains("identity property identifier"));
    assert_eq!(db.store.commit_epoch(), 0);
}

#[test]
fn typed_knowledge_normalized_space_move_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_knowledge_normalized_space_move_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1', space_id: ''})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'memory_2', space_id: 'default'})")
            .unwrap();
        let batch_count_before_move = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.move_knowledge_normalized_space_batch(&KnowledgeNormalizedSpaceMoveBatchRequest {
            label: "Memory".to_string(),
            identity_property: "id".to_string(),
            external_ids: vec!["memory_1".to_string(), "memory_2".to_string()],
            source_space_id: Some("default".to_string()),
            target_space_id: "archive".to_string(),
            updated_at: None,
        })
        .unwrap();
        let batch_count_after_move = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_move, batch_count_before_move + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db
            .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
                entities: vec![
                    KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                ],
                property_names: vec!["space_id".to_string()],
            })
            .unwrap();
        assert_eq!(
            rows.rows[0].properties.get("space_id"),
            Some(&Some(Value::String("archive".to_string())))
        );
        assert_eq!(
            rows.rows[1].properties.get("space_id"),
            Some(&Some(Value::String("archive".to_string())))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn touches_memory_access_batch_with_incremental_counters() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', access_count: 2, clicks: 1, total_dwell_time_ms: 100, last_accessed_at: 'old', last_clicked_at: 'old'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2'})").unwrap();

    let output = db
        .touch_knowledge_memory_access_batch(&KnowledgeMemoryAccessBatchRequest {
            touches: vec![
                KnowledgeMemoryAccessTouch {
                    memory_id: "memory_1".to_string(),
                    accessed_at: Value::String("2026-07-19T10:00:00Z".to_string()),
                    click_dwell_time_ms: None,
                },
                KnowledgeMemoryAccessTouch {
                    memory_id: "memory_1".to_string(),
                    accessed_at: Value::String("2026-07-19T10:01:00Z".to_string()),
                    click_dwell_time_ms: Some(50),
                },
                KnowledgeMemoryAccessTouch {
                    memory_id: "memory_2".to_string(),
                    accessed_at: Value::String("2026-07-19T10:02:00Z".to_string()),
                    click_dwell_time_ms: Some(25),
                },
                KnowledgeMemoryAccessTouch {
                    memory_id: "missing".to_string(),
                    accessed_at: Value::String("2026-07-19T10:03:00Z".to_string()),
                    click_dwell_time_ms: None,
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 3);
    assert_eq!(output.rows.len(), 4);
    assert_eq!(output.matched_count, 3);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.touched_count, 3);
    assert_eq!(output.click_touch_count, 2);
    assert!(output.rows[0].touched);
    assert!(!output.rows[0].clicked);
    assert!(output.rows[1].clicked);
    assert!(output.rows[2].clicked);
    assert!(!output.rows[3].matched);

    let rows = db
        .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_2".to_string(),
                },
            ],
            property_names: vec![
                "access_count".to_string(),
                "clicks".to_string(),
                "total_dwell_time_ms".to_string(),
                "last_accessed_at".to_string(),
                "last_clicked_at".to_string(),
            ],
        })
        .unwrap();
    assert_eq!(
        rows.rows[0].properties.get("access_count"),
        Some(&Some(Value::Int(4)))
    );
    assert_eq!(
        rows.rows[0].properties.get("clicks"),
        Some(&Some(Value::Int(2)))
    );
    assert_eq!(
        rows.rows[0].properties.get("total_dwell_time_ms"),
        Some(&Some(Value::Int(150)))
    );
    assert_eq!(
        rows.rows[0].properties.get("last_accessed_at"),
        Some(&Some(Value::String("2026-07-19T10:01:00Z".to_string())))
    );
    assert_eq!(
        rows.rows[0].properties.get("last_clicked_at"),
        Some(&Some(Value::String("2026-07-19T10:01:00Z".to_string())))
    );
    assert_eq!(
        rows.rows[1].properties.get("access_count"),
        Some(&Some(Value::Int(1)))
    );
    assert_eq!(
        rows.rows[1].properties.get("clicks"),
        Some(&Some(Value::Int(1)))
    );
    assert_eq!(
        rows.rows[1].properties.get("total_dwell_time_ms"),
        Some(&Some(Value::Int(25)))
    );
}

#[test]
fn knowledge_memory_access_batch_rejects_negative_dwell_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .touch_knowledge_memory_access_batch(&KnowledgeMemoryAccessBatchRequest {
            touches: vec![KnowledgeMemoryAccessTouch {
                memory_id: "memory_1".to_string(),
                accessed_at: Value::String("2026-07-19T10:00:00Z".to_string()),
                click_dwell_time_ms: Some(-1),
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-negative dwell time"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_knowledge_memory_access_batch_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_knowledge_memory_access_batch_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
        db.query("CREATE (:Memory {id: 'memory_2', access_count: 4, clicks: 2, total_dwell_time_ms: 10})")
            .unwrap();
        let batch_count_before_touch = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.touch_knowledge_memory_access_batch(&KnowledgeMemoryAccessBatchRequest {
            touches: vec![
                KnowledgeMemoryAccessTouch {
                    memory_id: "memory_1".to_string(),
                    accessed_at: Value::String("2026-07-19T11:00:00Z".to_string()),
                    click_dwell_time_ms: None,
                },
                KnowledgeMemoryAccessTouch {
                    memory_id: "memory_2".to_string(),
                    accessed_at: Value::String("2026-07-19T11:01:00Z".to_string()),
                    click_dwell_time_ms: Some(90),
                },
            ],
        })
        .unwrap();
        let batch_count_after_touch = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_touch, batch_count_before_touch + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db
            .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
                entities: vec![
                    KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                ],
                property_names: vec![
                    "access_count".to_string(),
                    "clicks".to_string(),
                    "total_dwell_time_ms".to_string(),
                ],
            })
            .unwrap();
        assert_eq!(
            rows.rows[0].properties.get("access_count"),
            Some(&Some(Value::Int(1)))
        );
        assert_eq!(
            rows.rows[1].properties.get("access_count"),
            Some(&Some(Value::Int(5)))
        );
        assert_eq!(
            rows.rows[1].properties.get("clicks"),
            Some(&Some(Value::Int(3)))
        );
        assert_eq!(
            rows.rows[1].properties.get("total_dwell_time_ms"),
            Some(&Some(Value::Int(100)))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

fn memory_content_update(memory_id: &str, title: &str) -> KnowledgeMemoryContentUpdate {
    KnowledgeMemoryContentUpdate {
        memory_id: memory_id.to_string(),
        content: format!("{title} content"),
        title: title.to_string(),
        semantic_field: format!("{title} semantic field"),
        importance: Value::Float(0.7),
        confidence: Value::Float(0.8),
        unit_type: "fact".to_string(),
        source: "codex".to_string(),
        source_range: Value::String("1..3".to_string()),
        space_id: "default".to_string(),
        updated_at: Value::Int(1700000000),
        reindex_needed: true,
        review_status: "reviewed".to_string(),
        extraction_method: "manual".to_string(),
    }
}

#[test]
fn updates_memory_content_batch_for_nowledge_full_update_shape() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'memory_1', title: 'Old', content: 'old', reindex_needed: false})",
    )
    .unwrap();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();
    let idless = db
        .query("MATCH (m:Memory) WHERE m.title = 'Idless memory' RETURN id(m) AS id")
        .unwrap();
    let idless_memory_id = match idless.rows[0].get("id").unwrap() {
        Value::Int(id) => id.to_string(),
        other => panic!("expected projected id int, got {other:?}"),
    };

    let output = db
        .update_knowledge_memory_content_batch(&KnowledgeMemoryContentBatchRequest {
            updates: vec![
                memory_content_update("memory_1", "Updated Memory"),
                memory_content_update("missing", "Missing Memory"),
                memory_content_update(&idless_memory_id, "Idless Memory"),
                memory_content_update("memory_1", "Duplicate Memory"),
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 3);
    assert_eq!(output.rows.len(), 4);
    assert_eq!(output.matched_count, 1);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.non_writable_count, 1);
    assert_eq!(output.updated_count, 1);
    assert_eq!(output.updated_property_count, 13);
    assert!(output.rows[0].updated);
    assert!(!output.rows[1].matched);
    assert!(output.rows[2].non_writable);
    assert!(output.rows[3].duplicate);

    let rows = db
        .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
            entities: vec![KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            }],
            property_names: vec![
                "content".to_string(),
                "title".to_string(),
                "semantic_field".to_string(),
                "importance".to_string(),
                "confidence".to_string(),
                "unit_type".to_string(),
                "source".to_string(),
                "source_range".to_string(),
                "space_id".to_string(),
                "updated_at".to_string(),
                "reindex_needed".to_string(),
                "review_status".to_string(),
                "extraction_method".to_string(),
            ],
        })
        .unwrap();
    let properties = &rows.rows[0].properties;
    assert_eq!(
        properties.get("content"),
        Some(&Some(Value::String("Updated Memory content".to_string())))
    );
    assert_eq!(
        properties.get("title"),
        Some(&Some(Value::String("Updated Memory".to_string())))
    );
    assert_eq!(
        properties.get("semantic_field"),
        Some(&Some(Value::String(
            "Updated Memory semantic field".to_string()
        )))
    );
    assert_eq!(properties.get("importance"), Some(&Some(Value::Float(0.7))));
    assert_eq!(properties.get("confidence"), Some(&Some(Value::Float(0.8))));
    assert_eq!(
        properties.get("unit_type"),
        Some(&Some(Value::String("fact".to_string())))
    );
    assert_eq!(
        properties.get("source"),
        Some(&Some(Value::String("codex".to_string())))
    );
    assert_eq!(
        properties.get("source_range"),
        Some(&Some(Value::String("1..3".to_string())))
    );
    assert_eq!(
        properties.get("space_id"),
        Some(&Some(Value::String("default".to_string())))
    );
    assert_eq!(
        properties.get("updated_at"),
        Some(&Some(Value::Int(1700000000)))
    );
    assert_eq!(
        properties.get("reindex_needed"),
        Some(&Some(Value::Bool(true)))
    );
    assert_eq!(
        properties.get("review_status"),
        Some(&Some(Value::String("reviewed".to_string())))
    );
    assert_eq!(
        properties.get("extraction_method"),
        Some(&Some(Value::String("manual".to_string())))
    );
}

#[test]
fn updates_memory_metadata_batch_for_nowledge_replace_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', metadata: '{}', updated_at: 1})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', metadata: '{}', updated_at: 2})")
        .unwrap();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();
    let idless = db
        .query("MATCH (m:Memory) WHERE m.title = 'Idless memory' RETURN id(m) AS id")
        .unwrap();
    let idless_memory_id = match idless.rows[0].get("id").unwrap() {
        Value::Int(id) => id.to_string(),
        other => panic!("expected projected id int, got {other:?}"),
    };
    let graph_commit_epoch_before = db.store.commit_epoch();

    let output = db
        .update_knowledge_memory_metadata_batch(&KnowledgeMemoryMetadataBatchRequest {
            updates: vec![
                KnowledgeMemoryMetadataUpdate {
                    memory_id: "memory_1".to_string(),
                    metadata: Value::String("{\"replace\":true}".to_string()),
                    updated_at: None,
                },
                KnowledgeMemoryMetadataUpdate {
                    memory_id: "memory_2".to_string(),
                    metadata: Value::String("{\"replace\":\"with-ts\"}".to_string()),
                    updated_at: Some(Value::Int(100)),
                },
                KnowledgeMemoryMetadataUpdate {
                    memory_id: "missing".to_string(),
                    metadata: Value::String("{}".to_string()),
                    updated_at: None,
                },
                KnowledgeMemoryMetadataUpdate {
                    memory_id: idless_memory_id,
                    metadata: Value::String("{}".to_string()),
                    updated_at: None,
                },
                KnowledgeMemoryMetadataUpdate {
                    memory_id: "memory_1".to_string(),
                    metadata: Value::String("{\"duplicate\":true}".to_string()),
                    updated_at: Some(Value::Int(200)),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, graph_commit_epoch_before);
    assert_eq!(
        output.graph_commit_epoch_after,
        graph_commit_epoch_before + 1
    );
    assert_eq!(output.rows.len(), 5);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.non_writable_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.updated_count, 2);
    assert_eq!(output.updated_property_count, 3);
    assert_eq!(output.rows[0].updated_property_count, 1);
    assert_eq!(output.rows[1].updated_property_count, 2);
    assert!(!output.rows[2].matched);
    assert!(output.rows[3].non_writable);
    assert!(output.rows[4].duplicate);

    let rows = db
        .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_2".to_string(),
                },
            ],
            property_names: vec!["metadata".to_string(), "updated_at".to_string()],
        })
        .unwrap();
    assert_eq!(
        rows.rows[0].properties.get("metadata"),
        Some(&Some(Value::String("{\"replace\":true}".to_string())))
    );
    assert_eq!(
        rows.rows[0].properties.get("updated_at"),
        Some(&Some(Value::Int(1)))
    );
    assert_eq!(
        rows.rows[1].properties.get("metadata"),
        Some(&Some(Value::String(
            "{\"replace\":\"with-ts\"}".to_string()
        )))
    );
    assert_eq!(
        rows.rows[1].properties.get("updated_at"),
        Some(&Some(Value::Int(100)))
    );
}

#[test]
fn memory_metadata_update_rejects_empty_id_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', metadata: '{}'})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .update_knowledge_memory_metadata_batch(&KnowledgeMemoryMetadataBatchRequest {
            updates: vec![KnowledgeMemoryMetadataUpdate {
                memory_id: String::new(),
                metadata: Value::String("{}".to_string()),
                updated_at: None,
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-empty memory id"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_memory_metadata_update_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_memory_metadata_update_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1', metadata: '{}', updated_at: 1})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'memory_2', metadata: '{}', updated_at: 2})")
            .unwrap();
        let batch_count_before_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.update_knowledge_memory_metadata_batch(&KnowledgeMemoryMetadataBatchRequest {
            updates: vec![
                KnowledgeMemoryMetadataUpdate {
                    memory_id: "memory_1".to_string(),
                    metadata: Value::String("{\"one\":true}".to_string()),
                    updated_at: None,
                },
                KnowledgeMemoryMetadataUpdate {
                    memory_id: "memory_2".to_string(),
                    metadata: Value::String("{\"two\":true}".to_string()),
                    updated_at: Some(Value::Int(20)),
                },
            ],
        })
        .unwrap();
        let batch_count_after_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db
            .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
                entities: vec![
                    KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                ],
                property_names: vec!["metadata".to_string(), "updated_at".to_string()],
            })
            .unwrap();
        assert_eq!(
            rows.rows[0].properties.get("metadata"),
            Some(&Some(Value::String("{\"one\":true}".to_string())))
        );
        assert_eq!(
            rows.rows[0].properties.get("updated_at"),
            Some(&Some(Value::Int(1)))
        );
        assert_eq!(
            rows.rows[1].properties.get("metadata"),
            Some(&Some(Value::String("{\"two\":true}".to_string())))
        );
        assert_eq!(
            rows.rows[1].properties.get("updated_at"),
            Some(&Some(Value::Int(20)))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn memory_content_update_rejects_invalid_rows_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let empty_id = db
        .update_knowledge_memory_content_batch(&KnowledgeMemoryContentBatchRequest {
            updates: vec![memory_content_update("", "Empty")],
        })
        .unwrap_err();
    assert!(empty_id.to_string().contains("non-empty memory id"));

    let mut invalid_importance = memory_content_update("memory_1", "Invalid Importance");
    invalid_importance.importance = Value::String("high".to_string());
    let importance_error = db
        .update_knowledge_memory_content_batch(&KnowledgeMemoryContentBatchRequest {
            updates: vec![invalid_importance],
        })
        .unwrap_err();
    assert!(importance_error
        .to_string()
        .contains("numeric finite importance"));

    let mut empty_unit_type = memory_content_update("memory_1", "Empty Unit Type");
    empty_unit_type.unit_type.clear();
    let unit_type_error = db
        .update_knowledge_memory_content_batch(&KnowledgeMemoryContentBatchRequest {
            updates: vec![empty_unit_type],
        })
        .unwrap_err();
    assert!(unit_type_error.to_string().contains("non-empty unit type"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_memory_content_update_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_memory_content_update_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1', title: 'Old 1'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'memory_2', title: 'Old 2'})")
            .unwrap();
        let batch_count_before_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.update_knowledge_memory_content_batch(&KnowledgeMemoryContentBatchRequest {
            updates: vec![
                memory_content_update("memory_1", "Updated One"),
                memory_content_update("memory_2", "Updated Two"),
            ],
        })
        .unwrap();
        let batch_count_after_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db
            .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
                entities: vec![
                    KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                ],
                property_names: vec!["title".to_string(), "reindex_needed".to_string()],
            })
            .unwrap();
        assert_eq!(
            rows.rows[0].properties.get("title"),
            Some(&Some(Value::String("Updated One".to_string())))
        );
        assert_eq!(
            rows.rows[1].properties.get("title"),
            Some(&Some(Value::String("Updated Two".to_string())))
        );
        assert_eq!(
            rows.rows[0].properties.get("reindex_needed"),
            Some(&Some(Value::Bool(true)))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn updates_memory_dedup_reviewed_batch_for_scheduler_shape() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'One'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', title: 'Two'})")
        .unwrap();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();
    let idless = db
        .query("MATCH (m:Memory) WHERE m.title = 'Idless memory' RETURN id(m) AS id")
        .unwrap();
    let idless_memory_id = match idless.rows[0].get("id").unwrap() {
        Value::Int(id) => id.to_string(),
        other => panic!("expected projected id int, got {other:?}"),
    };

    let output = db
        .update_knowledge_memory_dedup_reviewed_batch(&KnowledgeMemoryDedupReviewedBatchRequest {
            memory_ids: vec![
                "memory_1".to_string(),
                "missing".to_string(),
                idless_memory_id,
                "memory_2".to_string(),
                "memory_1".to_string(),
            ],
            reviewed_at: Value::Int(1800000000),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 3);
    assert_eq!(output.graph_commit_epoch_after, 4);
    assert_eq!(output.rows.len(), 5);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.non_writable_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.updated_count, 2);
    assert!(output.rows[0].updated);
    assert!(!output.rows[1].matched);
    assert!(output.rows[2].non_writable);
    assert!(output.rows[3].updated);
    assert!(output.rows[4].duplicate);

    let rows = db
        .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_2".to_string(),
                },
            ],
            property_names: vec!["dedup_reviewed_at".to_string()],
        })
        .unwrap();
    assert_eq!(
        rows.rows[0].properties.get("dedup_reviewed_at"),
        Some(&Some(Value::Int(1800000000)))
    );
    assert_eq!(
        rows.rows[1].properties.get("dedup_reviewed_at"),
        Some(&Some(Value::Int(1800000000)))
    );
}

#[test]
fn memory_dedup_reviewed_rejects_empty_ids_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .update_knowledge_memory_dedup_reviewed_batch(&KnowledgeMemoryDedupReviewedBatchRequest {
            memory_ids: vec!["memory_1".to_string(), String::new()],
            reviewed_at: Value::Int(1800000000),
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-empty memory ids"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_memory_dedup_reviewed_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_memory_dedup_reviewed_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
        db.query("CREATE (:Memory {id: 'memory_2'})").unwrap();
        let batch_count_before_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.update_knowledge_memory_dedup_reviewed_batch(
            &KnowledgeMemoryDedupReviewedBatchRequest {
                memory_ids: vec!["memory_1".to_string(), "memory_2".to_string()],
                reviewed_at: Value::String("2026-07-19T18:00:00Z".to_string()),
            },
        )
        .unwrap();
        let batch_count_after_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db
            .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
                entities: vec![
                    KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                ],
                property_names: vec!["dedup_reviewed_at".to_string()],
            })
            .unwrap();
        assert_eq!(
            rows.rows[0].properties.get("dedup_reviewed_at"),
            Some(&Some(Value::String("2026-07-19T18:00:00Z".to_string())))
        );
        assert_eq!(
            rows.rows[1].properties.get("dedup_reviewed_at"),
            Some(&Some(Value::String("2026-07-19T18:00:00Z".to_string())))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn updates_memory_decay_refresh_batch_for_scheduler_shapes() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'decay-refresh-memory-1', decay_score_cached: 1.0, confidence: 0.1})",
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 'decay-refresh-memory-2', decay_score_cached: 1.0})")
        .unwrap();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();
    let idless = db
        .query("MATCH (m:Memory) WHERE m.title = 'Idless memory' RETURN id(m) AS id")
        .unwrap();
    let idless_memory_id = match idless.rows[0].get("id").unwrap() {
        Value::Int(id) => id.to_string(),
        other => panic!("expected projected id int, got {other:?}"),
    };
    let graph_commit_epoch_before = db.store.commit_epoch();

    let output = db
        .update_knowledge_memory_decay_refresh_batch(&KnowledgeMemoryDecayRefreshBatchRequest {
            updates: vec![
                KnowledgeMemoryDecayRefreshUpdate {
                    memory_id: "decay-refresh-memory-1".to_string(),
                    decay_score_cached: Value::Float(0.42),
                    confidence: Some(Value::Float(0.77)),
                },
                KnowledgeMemoryDecayRefreshUpdate {
                    memory_id: "decay-refresh-memory-2".to_string(),
                    decay_score_cached: Value::Float(0.31),
                    confidence: None,
                },
                KnowledgeMemoryDecayRefreshUpdate {
                    memory_id: "missing".to_string(),
                    decay_score_cached: Value::Float(0.5),
                    confidence: None,
                },
                KnowledgeMemoryDecayRefreshUpdate {
                    memory_id: idless_memory_id,
                    decay_score_cached: Value::Float(0.6),
                    confidence: Some(Value::Float(0.6)),
                },
                KnowledgeMemoryDecayRefreshUpdate {
                    memory_id: "decay-refresh-memory-1".to_string(),
                    decay_score_cached: Value::Float(0.9),
                    confidence: None,
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, graph_commit_epoch_before);
    assert_eq!(
        output.graph_commit_epoch_after,
        graph_commit_epoch_before + 1
    );
    assert_eq!(output.rows.len(), 5);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.non_writable_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.updated_count, 2);
    assert_eq!(output.updated_property_count, 3);
    assert_eq!(output.rows[0].updated_property_count, 2);
    assert_eq!(output.rows[1].updated_property_count, 1);
    assert!(!output.rows[2].matched);
    assert!(output.rows[3].non_writable);
    assert!(output.rows[4].duplicate);

    let rows = db
        .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "decay-refresh-memory-1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "decay-refresh-memory-2".to_string(),
                },
            ],
            property_names: vec!["decay_score_cached".to_string(), "confidence".to_string()],
        })
        .unwrap();
    assert_eq!(
        rows.rows[0].properties.get("decay_score_cached"),
        Some(&Some(Value::Float(0.42)))
    );
    assert_eq!(
        rows.rows[0].properties.get("confidence"),
        Some(&Some(Value::Float(0.77)))
    );
    assert_eq!(
        rows.rows[1].properties.get("decay_score_cached"),
        Some(&Some(Value::Float(0.31)))
    );
    assert_eq!(rows.rows[1].properties.get("confidence"), Some(&None));
}

#[test]
fn memory_decay_refresh_rejects_invalid_rows_before_wal() {
    let path = unique_test_dir("memory_decay_refresh_rejects_invalid_rows_before_wal");
    let mut db = Database::open(&path).unwrap();
    db.query("CREATE (:Memory {id: 'decay-refresh-memory-1', decay_score_cached: 1.0})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();
    let wal_before = read_test_wal(&path).unwrap();

    let empty_id = db
        .update_knowledge_memory_decay_refresh_batch(&KnowledgeMemoryDecayRefreshBatchRequest {
            updates: vec![KnowledgeMemoryDecayRefreshUpdate {
                memory_id: String::new(),
                decay_score_cached: Value::Float(0.42),
                confidence: None,
            }],
        })
        .unwrap_err();
    assert!(empty_id.to_string().contains("non-empty memory id"));

    let invalid_decay = db
        .update_knowledge_memory_decay_refresh_batch(&KnowledgeMemoryDecayRefreshBatchRequest {
            updates: vec![KnowledgeMemoryDecayRefreshUpdate {
                memory_id: "decay-refresh-memory-1".to_string(),
                decay_score_cached: Value::String("stale".to_string()),
                confidence: None,
            }],
        })
        .unwrap_err();
    assert!(invalid_decay
        .to_string()
        .contains("numeric finite decay score"));

    let invalid_confidence = db
        .update_knowledge_memory_decay_refresh_batch(&KnowledgeMemoryDecayRefreshBatchRequest {
            updates: vec![KnowledgeMemoryDecayRefreshUpdate {
                memory_id: "decay-refresh-memory-1".to_string(),
                decay_score_cached: Value::Float(0.42),
                confidence: Some(Value::String("high".to_string())),
            }],
        })
        .unwrap_err();
    assert!(invalid_confidence
        .to_string()
        .contains("numeric finite confidence"));

    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
    assert_eq!(read_test_wal(&path).unwrap(), wal_before);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_memory_decay_refresh_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_memory_decay_refresh_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'decay-refresh-memory-1', decay_score_cached: 1.0, confidence: 0.1})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'decay-refresh-memory-2', decay_score_cached: 1.0})")
            .unwrap();
        let batch_count_before_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.update_knowledge_memory_decay_refresh_batch(&KnowledgeMemoryDecayRefreshBatchRequest {
            updates: vec![
                KnowledgeMemoryDecayRefreshUpdate {
                    memory_id: "decay-refresh-memory-1".to_string(),
                    decay_score_cached: Value::Float(0.42),
                    confidence: Some(Value::Float(0.77)),
                },
                KnowledgeMemoryDecayRefreshUpdate {
                    memory_id: "decay-refresh-memory-2".to_string(),
                    decay_score_cached: Value::Float(0.31),
                    confidence: None,
                },
            ],
        })
        .unwrap();
        let batch_count_after_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db
            .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
                entities: vec![
                    KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "decay-refresh-memory-1".to_string(),
                    },
                    KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "decay-refresh-memory-2".to_string(),
                    },
                ],
                property_names: vec!["decay_score_cached".to_string(), "confidence".to_string()],
            })
            .unwrap();
        assert_eq!(
            rows.rows[0].properties.get("decay_score_cached"),
            Some(&Some(Value::Float(0.42)))
        );
        assert_eq!(
            rows.rows[0].properties.get("confidence"),
            Some(&Some(Value::Float(0.77)))
        );
        assert_eq!(
            rows.rows[1].properties.get("decay_score_cached"),
            Some(&Some(Value::Float(0.31)))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn adjusts_source_memory_count_batch_with_floor_decrements() {
    let mut db = Database::new();
    db.query("CREATE (:Source {id: 'source_1', memory_count: 1})")
        .unwrap();
    db.query("CREATE (:Source {id: 'source_2'})").unwrap();
    db.query("CREATE (:Source {id: 'bad', memory_count: 'many'})")
        .unwrap();

    let output = db
        .adjust_knowledge_source_memory_count_batch(&KnowledgeSourceMemoryCountBatchRequest {
            adjustments: vec![
                KnowledgeSourceMemoryCountAdjustment {
                    source_id: "source_1".to_string(),
                    delta: 1,
                },
                KnowledgeSourceMemoryCountAdjustment {
                    source_id: "source_1".to_string(),
                    delta: -1,
                },
                KnowledgeSourceMemoryCountAdjustment {
                    source_id: "source_1".to_string(),
                    delta: -1,
                },
                KnowledgeSourceMemoryCountAdjustment {
                    source_id: "source_1".to_string(),
                    delta: -1,
                },
                KnowledgeSourceMemoryCountAdjustment {
                    source_id: "source_2".to_string(),
                    delta: -1,
                },
                KnowledgeSourceMemoryCountAdjustment {
                    source_id: "missing".to_string(),
                    delta: 1,
                },
                KnowledgeSourceMemoryCountAdjustment {
                    source_id: "bad".to_string(),
                    delta: 1,
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 3);
    assert_eq!(output.graph_commit_epoch_after, 4);
    assert_eq!(output.rows.len(), 7);
    assert_eq!(output.matched_count, 5);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.invalid_current_count_count, 1);
    assert_eq!(output.adjusted_count, 5);
    assert_eq!(output.rows[0].old_count, Some(1));
    assert_eq!(output.rows[0].new_count, Some(2));
    assert_eq!(output.rows[1].old_count, Some(2));
    assert_eq!(output.rows[1].new_count, Some(1));
    assert_eq!(output.rows[2].old_count, Some(1));
    assert_eq!(output.rows[2].new_count, Some(0));
    assert_eq!(output.rows[3].old_count, Some(0));
    assert_eq!(output.rows[3].new_count, Some(0));
    assert_eq!(output.rows[4].old_count, Some(0));
    assert_eq!(output.rows[4].new_count, Some(0));
    assert!(!output.rows[5].matched);
    assert!(output.rows[6].invalid_current_count);

    let rows = db
        .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Source".to_string(),
                    external_id: "source_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Source".to_string(),
                    external_id: "source_2".to_string(),
                },
            ],
            property_names: vec!["memory_count".to_string()],
        })
        .unwrap();
    assert_eq!(
        rows.rows[0].properties.get("memory_count"),
        Some(&Some(Value::Int(0)))
    );
    assert_eq!(
        rows.rows[1].properties.get("memory_count"),
        Some(&Some(Value::Int(0)))
    );
}

#[test]
fn source_memory_count_batch_rejects_zero_delta_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Source {id: 'source_1', memory_count: 1})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .adjust_knowledge_source_memory_count_batch(&KnowledgeSourceMemoryCountBatchRequest {
            adjustments: vec![KnowledgeSourceMemoryCountAdjustment {
                source_id: "source_1".to_string(),
                delta: 0,
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-zero delta"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_source_memory_count_batch_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_source_memory_count_batch_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Source {id: 'source_1', memory_count: 0})")
            .unwrap();
        db.query("CREATE (:Source {id: 'source_2', memory_count: 2})")
            .unwrap();
        let batch_count_before_adjust = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.adjust_knowledge_source_memory_count_batch(&KnowledgeSourceMemoryCountBatchRequest {
            adjustments: vec![
                KnowledgeSourceMemoryCountAdjustment {
                    source_id: "source_1".to_string(),
                    delta: 1,
                },
                KnowledgeSourceMemoryCountAdjustment {
                    source_id: "source_2".to_string(),
                    delta: -1,
                },
            ],
        })
        .unwrap();
        let batch_count_after_adjust = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_adjust, batch_count_before_adjust + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db
            .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
                entities: vec![
                    KnowledgeEntityRequest {
                        label: "Source".to_string(),
                        external_id: "source_1".to_string(),
                    },
                    KnowledgeEntityRequest {
                        label: "Source".to_string(),
                        external_id: "source_2".to_string(),
                    },
                ],
                property_names: vec!["memory_count".to_string()],
            })
            .unwrap();
        assert_eq!(
            rows.rows[0].properties.get("memory_count"),
            Some(&Some(Value::Int(1)))
        );
        assert_eq!(
            rows.rows[1].properties.get("memory_count"),
            Some(&Some(Value::Int(1)))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn updates_source_lifecycle_batch_for_nowledge_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:Source {id: 'source_1', lifecycle_state: 'extracted', updated_at: 1})")
        .unwrap();
    db.query(
        "CREATE (:Source {id: 'source_2', lifecycle_state: 'parsed', chunk_count: 0, updated_at: 1})",
    )
    .unwrap();
    db.query("CREATE (:Source {id: 'source_3', lifecycle_state: 'parsed', updated_at: 1})")
        .unwrap();

    let output = db
        .update_knowledge_source_lifecycle_batch(&KnowledgeSourceLifecycleBatchRequest {
            updates: vec![
                KnowledgeSourceLifecycleUpdate {
                    source_id: "source_1".to_string(),
                    current_lifecycle_state: Some("extracted".to_string()),
                    lifecycle_state: "indexed".to_string(),
                    chunk_count: None,
                    updated_at: Value::String("2026-07-19T12:00:00Z".to_string()),
                },
                KnowledgeSourceLifecycleUpdate {
                    source_id: "source_2".to_string(),
                    current_lifecycle_state: None,
                    lifecycle_state: "indexed".to_string(),
                    chunk_count: Some(8),
                    updated_at: Value::String("2026-07-19T12:01:00Z".to_string()),
                },
                KnowledgeSourceLifecycleUpdate {
                    source_id: "source_3".to_string(),
                    current_lifecycle_state: Some("extracted".to_string()),
                    lifecycle_state: "indexed".to_string(),
                    chunk_count: None,
                    updated_at: Value::String("2026-07-19T12:02:00Z".to_string()),
                },
                KnowledgeSourceLifecycleUpdate {
                    source_id: "source_2".to_string(),
                    current_lifecycle_state: None,
                    lifecycle_state: "archived".to_string(),
                    chunk_count: None,
                    updated_at: Value::String("2026-07-19T12:03:00Z".to_string()),
                },
                KnowledgeSourceLifecycleUpdate {
                    source_id: "missing".to_string(),
                    current_lifecycle_state: None,
                    lifecycle_state: "indexed".to_string(),
                    chunk_count: None,
                    updated_at: Value::String("2026-07-19T12:04:00Z".to_string()),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 3);
    assert_eq!(output.graph_commit_epoch_after, 4);
    assert_eq!(output.rows.len(), 5);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.filtered_out_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.updated_count, 2);
    assert_eq!(output.updated_property_count, 5);
    assert_eq!(output.rows[0].updated_property_count, 2);
    assert_eq!(output.rows[1].updated_property_count, 3);
    assert!(output.rows[2].filtered_out);
    assert!(output.rows[3].duplicate);
    assert!(!output.rows[4].matched);

    let rows = db
        .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Source".to_string(),
                    external_id: "source_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Source".to_string(),
                    external_id: "source_2".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Source".to_string(),
                    external_id: "source_3".to_string(),
                },
            ],
            property_names: vec![
                "lifecycle_state".to_string(),
                "chunk_count".to_string(),
                "updated_at".to_string(),
            ],
        })
        .unwrap();
    assert_eq!(
        rows.rows[0].properties.get("lifecycle_state"),
        Some(&Some(Value::String("indexed".to_string())))
    );
    assert_eq!(
        rows.rows[0].properties.get("updated_at"),
        Some(&Some(Value::String("2026-07-19T12:00:00Z".to_string())))
    );
    assert_eq!(
        rows.rows[1].properties.get("lifecycle_state"),
        Some(&Some(Value::String("indexed".to_string())))
    );
    assert_eq!(
        rows.rows[1].properties.get("chunk_count"),
        Some(&Some(Value::Int(8)))
    );
    assert_eq!(
        rows.rows[1].properties.get("updated_at"),
        Some(&Some(Value::String("2026-07-19T12:01:00Z".to_string())))
    );
    assert_eq!(
        rows.rows[2].properties.get("lifecycle_state"),
        Some(&Some(Value::String("parsed".to_string())))
    );
}

#[test]
fn source_lifecycle_batch_rejects_invalid_rows_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Source {id: 'source_1', lifecycle_state: 'parsed'})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .update_knowledge_source_lifecycle_batch(&KnowledgeSourceLifecycleBatchRequest {
            updates: vec![KnowledgeSourceLifecycleUpdate {
                source_id: "source_1".to_string(),
                current_lifecycle_state: None,
                lifecycle_state: "indexed".to_string(),
                chunk_count: Some(-1),
                updated_at: Value::String("2026-07-19T12:00:00Z".to_string()),
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-negative chunk count"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_source_lifecycle_batch_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_source_lifecycle_batch_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Source {id: 'source_1', lifecycle_state: 'extracted', chunk_count: 0})")
            .unwrap();
        db.query("CREATE (:Source {id: 'source_2', lifecycle_state: 'parsed', chunk_count: 0})")
            .unwrap();
        let batch_count_before_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.update_knowledge_source_lifecycle_batch(&KnowledgeSourceLifecycleBatchRequest {
            updates: vec![
                KnowledgeSourceLifecycleUpdate {
                    source_id: "source_1".to_string(),
                    current_lifecycle_state: Some("extracted".to_string()),
                    lifecycle_state: "indexed".to_string(),
                    chunk_count: None,
                    updated_at: Value::String("2026-07-19T12:00:00Z".to_string()),
                },
                KnowledgeSourceLifecycleUpdate {
                    source_id: "source_2".to_string(),
                    current_lifecycle_state: None,
                    lifecycle_state: "indexed".to_string(),
                    chunk_count: Some(3),
                    updated_at: Value::String("2026-07-19T12:01:00Z".to_string()),
                },
            ],
        })
        .unwrap();
        let batch_count_after_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db
            .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
                entities: vec![
                    KnowledgeEntityRequest {
                        label: "Source".to_string(),
                        external_id: "source_1".to_string(),
                    },
                    KnowledgeEntityRequest {
                        label: "Source".to_string(),
                        external_id: "source_2".to_string(),
                    },
                ],
                property_names: vec!["lifecycle_state".to_string(), "chunk_count".to_string()],
            })
            .unwrap();
        assert_eq!(
            rows.rows[0].properties.get("lifecycle_state"),
            Some(&Some(Value::String("indexed".to_string())))
        );
        assert_eq!(
            rows.rows[1].properties.get("lifecycle_state"),
            Some(&Some(Value::String("indexed".to_string())))
        );
        assert_eq!(
            rows.rows[1].properties.get("chunk_count"),
            Some(&Some(Value::Int(3)))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn updates_source_metadata_batch_for_nowledge_auto_ocr_shape() {
    let mut db = Database::new();
    db.query("CREATE (:Source {id: 'source_1', metadata: '{}', updated_at: 1})")
        .unwrap();
    db.query("CREATE (:Source {id: 'source_2', metadata: '{}', updated_at: 1})")
        .unwrap();
    db.query("CREATE (:Source {original_name: 'Idless Source'})")
        .unwrap();
    let idless = db
        .query("MATCH (s:Source) WHERE s.original_name = 'Idless Source' RETURN id(s) AS id")
        .unwrap();
    let idless_source_id = match idless.rows[0].get("id").unwrap() {
        Value::Int(id) => id.to_string(),
        other => panic!("expected projected id int, got {other:?}"),
    };

    let output = db
        .update_knowledge_source_metadata_batch(&KnowledgeSourceMetadataBatchRequest {
            updates: vec![
                KnowledgeSourceMetadataUpdate {
                    source_id: "source_1".to_string(),
                    metadata: Value::String("{\"ocr\":\"ready\"}".to_string()),
                    updated_at: Value::String("2026-07-19T12:00:00Z".to_string()),
                },
                KnowledgeSourceMetadataUpdate {
                    source_id: "missing".to_string(),
                    metadata: Value::String("{\"missing\":true}".to_string()),
                    updated_at: Value::String("2026-07-19T12:01:00Z".to_string()),
                },
                KnowledgeSourceMetadataUpdate {
                    source_id: idless_source_id,
                    metadata: Value::String("{\"idless\":true}".to_string()),
                    updated_at: Value::String("2026-07-19T12:02:00Z".to_string()),
                },
                KnowledgeSourceMetadataUpdate {
                    source_id: "source_2".to_string(),
                    metadata: Value::String("{\"ocr\":\"queued\"}".to_string()),
                    updated_at: Value::String("2026-07-19T12:03:00Z".to_string()),
                },
                KnowledgeSourceMetadataUpdate {
                    source_id: "source_1".to_string(),
                    metadata: Value::String("{\"duplicate\":true}".to_string()),
                    updated_at: Value::String("2026-07-19T12:04:00Z".to_string()),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 3);
    assert_eq!(output.graph_commit_epoch_after, 4);
    assert_eq!(output.rows.len(), 5);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.non_writable_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.updated_count, 2);
    assert_eq!(output.updated_property_count, 4);
    assert!(output.rows[0].updated);
    assert!(!output.rows[1].matched);
    assert!(output.rows[2].non_writable);
    assert!(output.rows[4].duplicate);

    let rows = db
        .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Source".to_string(),
                    external_id: "source_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Source".to_string(),
                    external_id: "source_2".to_string(),
                },
            ],
            property_names: vec!["metadata".to_string(), "updated_at".to_string()],
        })
        .unwrap();
    assert_eq!(
        rows.rows[0].properties.get("metadata"),
        Some(&Some(Value::String("{\"ocr\":\"ready\"}".to_string())))
    );
    assert_eq!(
        rows.rows[0].properties.get("updated_at"),
        Some(&Some(Value::String("2026-07-19T12:00:00Z".to_string())))
    );
    assert_eq!(
        rows.rows[1].properties.get("metadata"),
        Some(&Some(Value::String("{\"ocr\":\"queued\"}".to_string())))
    );
    assert_eq!(
        rows.rows[1].properties.get("updated_at"),
        Some(&Some(Value::String("2026-07-19T12:03:00Z".to_string())))
    );
}

#[test]
fn source_metadata_batch_rejects_empty_source_id_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Source {id: 'source_1', metadata: '{}', updated_at: 1})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .update_knowledge_source_metadata_batch(&KnowledgeSourceMetadataBatchRequest {
            updates: vec![KnowledgeSourceMetadataUpdate {
                source_id: String::new(),
                metadata: Value::String("{}".to_string()),
                updated_at: Value::String("2026-07-19T12:00:00Z".to_string()),
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-empty source id"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_source_metadata_batch_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_source_metadata_batch_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Source {id: 'source_1', metadata: '{}', updated_at: 1})")
            .unwrap();
        db.query("CREATE (:Source {id: 'source_2', metadata: '{}', updated_at: 1})")
            .unwrap();
        let batch_count_before_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.update_knowledge_source_metadata_batch(&KnowledgeSourceMetadataBatchRequest {
            updates: vec![
                KnowledgeSourceMetadataUpdate {
                    source_id: "source_1".to_string(),
                    metadata: Value::String("{\"ocr\":\"ready\"}".to_string()),
                    updated_at: Value::String("2026-07-19T12:00:00Z".to_string()),
                },
                KnowledgeSourceMetadataUpdate {
                    source_id: "source_2".to_string(),
                    metadata: Value::String("{\"ocr\":\"queued\"}".to_string()),
                    updated_at: Value::String("2026-07-19T12:01:00Z".to_string()),
                },
            ],
        })
        .unwrap();
        let batch_count_after_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db
            .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
                entities: vec![
                    KnowledgeEntityRequest {
                        label: "Source".to_string(),
                        external_id: "source_1".to_string(),
                    },
                    KnowledgeEntityRequest {
                        label: "Source".to_string(),
                        external_id: "source_2".to_string(),
                    },
                ],
                property_names: vec!["metadata".to_string(), "updated_at".to_string()],
            })
            .unwrap();
        assert_eq!(
            rows.rows[0].properties.get("metadata"),
            Some(&Some(Value::String("{\"ocr\":\"ready\"}".to_string())))
        );
        assert_eq!(
            rows.rows[1].properties.get("updated_at"),
            Some(&Some(Value::String("2026-07-19T12:01:00Z".to_string())))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn deletes_sources_for_nowledge_detach_delete_shape() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})-[:SOURCED_FROM]->(:Source {id: 'source_1'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2'})-[:SOURCED_FROM]->(:Source {id: 'source_2'})")
        .unwrap();
    db.query("CREATE (:Source {id: 'source_3'})").unwrap();
    db.query("CREATE (:Source {original_name: 'Idless Source'})")
        .unwrap();
    let idless = db
        .query("MATCH (s:Source) WHERE s.original_name = 'Idless Source' RETURN id(s) AS id")
        .unwrap();
    let idless_source_id = match idless.rows[0].get("id").unwrap() {
        Value::Int(id) => id.to_string(),
        other => panic!("expected projected id int, got {other:?}"),
    };

    let output = db
        .delete_knowledge_sources(&KnowledgeSourceDeleteBatchRequest {
            source_ids: vec![
                "source_1".to_string(),
                "missing".to_string(),
                idless_source_id,
                "source_2".to_string(),
                "source_1".to_string(),
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 4);
    assert_eq!(output.graph_commit_epoch_after, 5);
    assert_eq!(output.rows.len(), 5);
    assert_eq!(output.matched_count, 3);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.non_writable_count, 1);
    assert_eq!(output.deleted_node_count, 2);
    assert!(output.rows[0].matched);
    assert!(!output.rows[1].matched);
    assert!(output.rows[2].non_writable);
    assert!(output.rows[3].matched);
    assert!(output.rows[4].matched);

    let sources = db
        .query(
            "MATCH (s:Source) WHERE s.id IS NOT NULL \
             RETURN s.id AS source_id ORDER BY source_id ASC",
        )
        .unwrap();
    assert_eq!(sources.rows.len(), 1);
    assert_eq!(
        sources.rows[0].get("source_id"),
        Some(&Value::String("source_3".to_string()))
    );
    assert_eq!(
        db.query("MATCH (m:Memory)-[r:SOURCED_FROM]->(s:Source) RETURN count(r) AS relationships")
            .unwrap()
            .rows[0]
            .get("relationships"),
        Some(&Value::Int(0))
    );
    assert_eq!(
        db.query("MATCH (m:Memory) RETURN count(m) AS memories")
            .unwrap()
            .rows[0]
            .get("memories"),
        Some(&Value::Int(2))
    );
}

#[test]
fn source_delete_rejects_empty_source_id_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Source {id: 'source_1'})").unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .delete_knowledge_sources(&KnowledgeSourceDeleteBatchRequest {
            source_ids: vec![String::new()],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-empty source id"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_source_delete_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_source_delete_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})-[:SOURCED_FROM]->(:Source {id: 'source_1'})")
            .unwrap();
        db.query("CREATE (:Source {id: 'source_2'})").unwrap();
        let batch_count_before_delete = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.delete_knowledge_sources(&KnowledgeSourceDeleteBatchRequest {
            source_ids: vec!["source_1".to_string(), "source_2".to_string()],
        })
        .unwrap();
        let batch_count_after_delete = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_delete, batch_count_before_delete + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("delete_node"));
    {
        let db = Database::open(&path).unwrap();
        let sources = db
            .query_read_only_with_params_bounded(
                "MATCH (s:Source) WHERE s.id IN $source_ids RETURN count(s) AS total",
                &BTreeMap::from([(
                    "source_ids".to_string(),
                    Value::List(vec![
                        Value::String("source_1".to_string()),
                        Value::String("source_2".to_string()),
                    ]),
                )]),
                Some(1),
            )
            .unwrap();
        assert_eq!(sources.rows[0].get("total"), Some(&Value::Int(0)));
        assert!(db
            .query_entity_via_cypher(&KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            })
            .unwrap()
            .entity
            .is_some());
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn assigns_source_labels_for_nowledge_merge_shape() {
    let mut db = Database::new();
    db.query("CREATE (:Source {id: 'source_1', original_name: 'First'})")
        .unwrap();
    db.query("CREATE (:Source {id: 'source_2', original_name: 'Second'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'label_1', name: 'First Label'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'label_2', name: 'Second Label'})")
        .unwrap();
    db.query("CREATE (:Source {original_name: 'Idless Source'})")
        .unwrap();
    db.query("MATCH (s:Source {id: 'source_2'}), (l:Label {id: 'label_2'}) CREATE (s)-[:HAS_LABEL {assigned_by: 'existing'}]->(l)")
        .unwrap();
    let idless = db
        .query("MATCH (s:Source) WHERE s.original_name = 'Idless Source' RETURN id(s) AS id")
        .unwrap();
    let idless_source_id = match idless.rows[0].get("id").unwrap() {
        Value::Int(id) => id.to_string(),
        other => panic!("expected projected id int, got {other:?}"),
    };

    let output = db
        .assign_knowledge_source_labels_batch(&KnowledgeSourceLabelAssignmentBatchRequest {
            assignments: vec![
                KnowledgeSourceLabelAssignment {
                    source_id: "source_1".to_string(),
                    label_id: "label_1".to_string(),
                    assigned_by: "system".to_string(),
                    created_at: Value::Int(987),
                    properties: Value::String("{\"scope\":\"source\"}".to_string()),
                },
                KnowledgeSourceLabelAssignment {
                    source_id: "source_2".to_string(),
                    label_id: "label_2".to_string(),
                    assigned_by: "system".to_string(),
                    created_at: Value::Int(988),
                    properties: Value::String("{\"scope\":\"existing\"}".to_string()),
                },
                KnowledgeSourceLabelAssignment {
                    source_id: "missing".to_string(),
                    label_id: "label_1".to_string(),
                    assigned_by: "system".to_string(),
                    created_at: Value::Int(989),
                    properties: Value::String("{}".to_string()),
                },
                KnowledgeSourceLabelAssignment {
                    source_id: idless_source_id,
                    label_id: "label_1".to_string(),
                    assigned_by: "system".to_string(),
                    created_at: Value::Int(990),
                    properties: Value::String("{}".to_string()),
                },
                KnowledgeSourceLabelAssignment {
                    source_id: "source_1".to_string(),
                    label_id: "label_1".to_string(),
                    assigned_by: "duplicate".to_string(),
                    created_at: Value::Int(991),
                    properties: Value::String("{\"duplicate\":true}".to_string()),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 6);
    assert_eq!(output.graph_commit_epoch_after, 7);
    assert_eq!(output.rows.len(), 5);
    assert_eq!(output.matched_count, 3);
    assert_eq!(output.missing_endpoint_count, 1);
    assert_eq!(output.non_writable_count, 1);
    assert_eq!(output.created_count, 1);
    assert_eq!(output.already_exists_count, 2);
    assert_eq!(output.created_relationship_count, 1);
    assert!(output.rows[0].created);
    assert!(output.rows[1].already_exists);
    assert!(!output.rows[2].matched);
    assert!(output.rows[3].non_writable);
    assert!(output.rows[4].already_exists);

    let rels = db
        .query("MATCH (:Source)-[r:HAS_LABEL]->(:Label) RETURN count(r) AS total")
        .unwrap();
    assert_eq!(rels.rows[0].get("total"), Some(&Value::Int(2)));
    let created = db
        .query("MATCH (:Source {id: 'source_1'})-[r:HAS_LABEL]->(:Label {id: 'label_1'}) RETURN r.assigned_by AS assigned_by, r.created_at AS created_at, r.properties AS properties")
        .unwrap();
    assert_eq!(
        created.rows[0].get("assigned_by"),
        Some(&Value::String("system".to_string()))
    );
    assert_eq!(created.rows[0].get("created_at"), Some(&Value::Int(987)));
    assert_eq!(
        created.rows[0].get("properties"),
        Some(&Value::String("{\"scope\":\"source\"}".to_string()))
    );
    let existing = db
        .query("MATCH (:Source {id: 'source_2'})-[r:HAS_LABEL]->(:Label {id: 'label_2'}) RETURN r.assigned_by AS assigned_by, r.properties AS properties")
        .unwrap();
    assert_eq!(
        existing.rows[0].get("assigned_by"),
        Some(&Value::String("existing".to_string()))
    );
    assert_eq!(existing.rows[0].get("properties"), Some(&Value::Null));
}

#[test]
fn source_label_assignment_rejects_empty_fields_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Source {id: 'source_1'})").unwrap();
    db.query("CREATE (:Label {id: 'label_1'})").unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .assign_knowledge_source_labels_batch(&KnowledgeSourceLabelAssignmentBatchRequest {
            assignments: vec![KnowledgeSourceLabelAssignment {
                source_id: "source_1".to_string(),
                label_id: "label_1".to_string(),
                assigned_by: String::new(),
                created_at: Value::Int(1),
                properties: Value::String("{}".to_string()),
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-empty assigned_by"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_source_label_assignment_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_source_label_assignment_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Source {id: 'source_1'})").unwrap();
        db.query("CREATE (:Source {id: 'source_2'})").unwrap();
        db.query("CREATE (:Label {id: 'label_1'})").unwrap();
        db.query("CREATE (:Label {id: 'label_2'})").unwrap();
        let batch_count_before_assign = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.assign_knowledge_source_labels_batch(&KnowledgeSourceLabelAssignmentBatchRequest {
            assignments: vec![
                KnowledgeSourceLabelAssignment {
                    source_id: "source_1".to_string(),
                    label_id: "label_1".to_string(),
                    assigned_by: "system".to_string(),
                    created_at: Value::Int(1),
                    properties: Value::String("{}".to_string()),
                },
                KnowledgeSourceLabelAssignment {
                    source_id: "source_2".to_string(),
                    label_id: "label_2".to_string(),
                    assigned_by: "system".to_string(),
                    created_at: Value::Int(2),
                    properties: Value::String("{\"scope\":\"source\"}".to_string()),
                },
            ],
        })
        .unwrap();
        let batch_count_after_assign = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_assign, batch_count_before_assign + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("create_rel"));
    {
        let mut db = Database::open(&path).unwrap();
        let rels = db
            .query("MATCH (:Source)-[r:HAS_LABEL]->(:Label) RETURN count(r) AS total")
            .unwrap();
        assert_eq!(rels.rows[0].get("total"), Some(&Value::Int(2)));
        let row = db
            .query("MATCH (:Source {id: 'source_2'})-[r:HAS_LABEL]->(:Label {id: 'label_2'}) RETURN r.properties AS properties")
            .unwrap();
        assert_eq!(
            row.rows[0].get("properties"),
            Some(&Value::String("{\"scope\":\"source\"}".to_string()))
        );
    }
    let _ = std::fs::remove_dir_all(path);
}

#[test]
fn deletes_source_labels_for_nowledge_delete_shape() {
    let mut db = Database::new();
    db.query("CREATE (:Source {id: 'source_1'})").unwrap();
    db.query("CREATE (:Source {id: 'source_2'})").unwrap();
    db.query("CREATE (:Label {id: 'label_1'})").unwrap();
    db.query("CREATE (:Label {id: 'label_2'})").unwrap();
    db.query("CREATE (:Source {original_name: 'Idless Source'})")
        .unwrap();
    db.query(
        "MATCH (s:Source {id: 'source_1'}), (l:Label {id: 'label_1'}) CREATE (s)-[:HAS_LABEL]->(l)",
    )
    .unwrap();
    db.query(
        "MATCH (s:Source {id: 'source_1'}), (l:Label {id: 'label_2'}) CREATE (s)-[:HAS_LABEL]->(l)",
    )
    .unwrap();
    db.query(
        "MATCH (s:Source {id: 'source_2'}), (l:Label {id: 'label_1'}) CREATE (s)-[:HAS_LABEL]->(l)",
    )
    .unwrap();
    let idless = db
        .query("MATCH (s:Source) WHERE s.original_name = 'Idless Source' RETURN id(s) AS id")
        .unwrap();
    let idless_source_id = match idless.rows[0].get("id").unwrap() {
        Value::Int(id) => id.to_string(),
        other => panic!("expected projected id int, got {other:?}"),
    };

    let output = db
        .delete_knowledge_source_labels_batch(&KnowledgeSourceLabelDeleteBatchRequest {
            deletes: vec![
                KnowledgeSourceLabelDelete {
                    source_id: "source_1".to_string(),
                    label_id: "label_1".to_string(),
                },
                KnowledgeSourceLabelDelete {
                    source_id: "missing".to_string(),
                    label_id: "label_1".to_string(),
                },
                KnowledgeSourceLabelDelete {
                    source_id: idless_source_id,
                    label_id: "label_1".to_string(),
                },
                KnowledgeSourceLabelDelete {
                    source_id: "source_1".to_string(),
                    label_id: "missing".to_string(),
                },
                KnowledgeSourceLabelDelete {
                    source_id: "source_2".to_string(),
                    label_id: "label_1".to_string(),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 8);
    assert_eq!(output.graph_commit_epoch_after, 9);
    assert_eq!(output.rows.len(), 5);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.missing_endpoint_count, 2);
    assert_eq!(output.non_writable_count, 1);
    assert_eq!(output.deleted_relationship_count, 2);
    assert!(output.rows[0].matched);
    assert!(!output.rows[1].matched);
    assert!(output.rows[2].non_writable);
    assert!(!output.rows[3].matched);
    assert!(output.rows[4].matched);

    let rels = db
        .query("MATCH (:Source)-[r:HAS_LABEL]->(:Label) RETURN count(r) AS total")
        .unwrap();
    assert_eq!(rels.rows[0].get("total"), Some(&Value::Int(1)));
    let remaining = db
        .query("MATCH (:Source {id: 'source_1'})-[r:HAS_LABEL]->(:Label {id: 'label_2'}) RETURN count(r) AS total")
        .unwrap();
    assert_eq!(remaining.rows[0].get("total"), Some(&Value::Int(1)));
}

#[test]
fn source_label_delete_rejects_empty_fields_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Source {id: 'source_1'})").unwrap();
    db.query("CREATE (:Label {id: 'label_1'})").unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .delete_knowledge_source_labels_batch(&KnowledgeSourceLabelDeleteBatchRequest {
            deletes: vec![KnowledgeSourceLabelDelete {
                source_id: String::new(),
                label_id: "label_1".to_string(),
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-empty source id"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_source_label_delete_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_source_label_delete_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Source {id: 'source_1'})").unwrap();
        db.query("CREATE (:Source {id: 'source_2'})").unwrap();
        db.query("CREATE (:Label {id: 'label_1'})").unwrap();
        db.query("CREATE (:Label {id: 'label_2'})").unwrap();
        db.query("MATCH (s:Source {id: 'source_1'}), (l:Label {id: 'label_1'}) CREATE (s)-[:HAS_LABEL]->(l)")
            .unwrap();
        db.query("MATCH (s:Source {id: 'source_2'}), (l:Label {id: 'label_2'}) CREATE (s)-[:HAS_LABEL]->(l)")
            .unwrap();
        let batch_count_before_delete = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.delete_knowledge_source_labels_batch(&KnowledgeSourceLabelDeleteBatchRequest {
            deletes: vec![
                KnowledgeSourceLabelDelete {
                    source_id: "source_1".to_string(),
                    label_id: "label_1".to_string(),
                },
                KnowledgeSourceLabelDelete {
                    source_id: "source_2".to_string(),
                    label_id: "label_2".to_string(),
                },
            ],
        })
        .unwrap();
        let batch_count_after_delete = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_delete, batch_count_before_delete + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("delete_rel"));
    {
        let mut db = Database::open(&path).unwrap();
        let rels = db
            .query("MATCH (:Source)-[r:HAS_LABEL]->(:Label) RETURN count(r) AS total")
            .unwrap();
        assert_eq!(rels.rows[0].get("total"), Some(&Value::Int(0)));
        assert_eq!(
            db.query("MATCH (s:Source) RETURN count(s) AS total")
                .unwrap()
                .rows[0]
                .get("total"),
            Some(&Value::Int(2))
        );
        assert_eq!(
            db.query("MATCH (l:Label) RETURN count(l) AS total")
                .unwrap()
                .rows[0]
                .get("total"),
            Some(&Value::Int(2))
        );
    }
    let _ = std::fs::remove_dir_all(path);
}

fn source_parsed_metadata_update(
    source_id: &str,
    summary: &str,
    sha256: &str,
) -> KnowledgeSourceParsedMetadataUpdate {
    KnowledgeSourceParsedMetadataUpdate {
        source_id: source_id.to_string(),
        parsed_path: None,
        file_path: None,
        original_name: None,
        mime_type: None,
        source_url: None,
        summary: summary.to_string(),
        sha256: sha256.to_string(),
        size_bytes: 101,
        updated_at: Value::String("2026-07-19T12:00:00Z".to_string()),
        metadata: None,
    }
}

#[test]
fn updates_source_parsed_metadata_batch_for_nowledge_parser_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:Source {id: 'source_1', lifecycle_state: 'ingested', summary: 'old'})")
        .unwrap();
    db.query("CREATE (:Source {id: 'source_2', lifecycle_state: 'ingested'})")
        .unwrap();
    db.query("CREATE (:Source {original_name: 'Idless Source'})")
        .unwrap();
    let idless = db
        .query("MATCH (s:Source) WHERE s.original_name = 'Idless Source' RETURN id(s) AS id")
        .unwrap();
    let idless_source_id = match idless.rows[0].get("id").unwrap() {
        Value::Int(id) => id.to_string(),
        other => panic!("expected projected id int, got {other:?}"),
    };

    let mut full = source_parsed_metadata_update("source_1", "HTML summary", "sha-html");
    full.parsed_path = Some("/tmp/html.parsed".to_string());
    full.file_path = Some("".to_string());
    full.original_name = Some("HTML Source".to_string());
    full.mime_type = Some("text/html".to_string());
    full.source_url = Some("https://example.test/source".to_string());
    full.size_bytes = 4096;
    full.updated_at = Value::String("2026-07-19T12:01:00Z".to_string());
    full.metadata = Some(Value::String("{\"parser\":\"html\"}".to_string()));

    let mut minimal = source_parsed_metadata_update("source_2", "Summary only", "sha-summary");
    minimal.size_bytes = 128;
    minimal.updated_at = Value::String("2026-07-19T12:02:00Z".to_string());

    let output = db
        .update_knowledge_source_parsed_metadata_batch(&KnowledgeSourceParsedMetadataBatchRequest {
            updates: vec![
                full,
                source_parsed_metadata_update("missing", "Missing", "sha-missing"),
                source_parsed_metadata_update(&idless_source_id, "Idless", "sha-idless"),
                minimal,
                source_parsed_metadata_update("source_1", "Duplicate", "sha-duplicate"),
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 3);
    assert_eq!(output.graph_commit_epoch_after, 4);
    assert_eq!(output.rows.len(), 5);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.non_writable_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.updated_count, 2);
    assert_eq!(output.updated_property_count, 16);
    assert_eq!(output.rows[0].updated_property_count, 11);
    assert!(!output.rows[1].matched);
    assert!(output.rows[2].non_writable);
    assert_eq!(output.rows[3].updated_property_count, 5);
    assert!(output.rows[4].duplicate);

    let rows = db
        .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Source".to_string(),
                    external_id: "source_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Source".to_string(),
                    external_id: "source_2".to_string(),
                },
            ],
            property_names: vec![
                "lifecycle_state".to_string(),
                "parsed_path".to_string(),
                "file_path".to_string(),
                "original_name".to_string(),
                "mime_type".to_string(),
                "source_url".to_string(),
                "summary".to_string(),
                "sha256".to_string(),
                "size_bytes".to_string(),
                "updated_at".to_string(),
                "metadata".to_string(),
            ],
        })
        .unwrap();
    assert_eq!(
        rows.rows[0].properties.get("lifecycle_state"),
        Some(&Some(Value::String("parsed".to_string())))
    );
    assert_eq!(
        rows.rows[0].properties.get("parsed_path"),
        Some(&Some(Value::String("/tmp/html.parsed".to_string())))
    );
    assert_eq!(
        rows.rows[0].properties.get("file_path"),
        Some(&Some(Value::String("".to_string())))
    );
    assert_eq!(
        rows.rows[0].properties.get("original_name"),
        Some(&Some(Value::String("HTML Source".to_string())))
    );
    assert_eq!(
        rows.rows[0].properties.get("mime_type"),
        Some(&Some(Value::String("text/html".to_string())))
    );
    assert_eq!(
        rows.rows[0].properties.get("source_url"),
        Some(&Some(Value::String(
            "https://example.test/source".to_string()
        )))
    );
    assert_eq!(
        rows.rows[0].properties.get("summary"),
        Some(&Some(Value::String("HTML summary".to_string())))
    );
    assert_eq!(
        rows.rows[0].properties.get("sha256"),
        Some(&Some(Value::String("sha-html".to_string())))
    );
    assert_eq!(
        rows.rows[0].properties.get("size_bytes"),
        Some(&Some(Value::Int(4096)))
    );
    assert_eq!(
        rows.rows[0].properties.get("updated_at"),
        Some(&Some(Value::String("2026-07-19T12:01:00Z".to_string())))
    );
    assert_eq!(
        rows.rows[0].properties.get("metadata"),
        Some(&Some(Value::String("{\"parser\":\"html\"}".to_string())))
    );
    assert_eq!(
        rows.rows[1].properties.get("lifecycle_state"),
        Some(&Some(Value::String("parsed".to_string())))
    );
    assert_eq!(
        rows.rows[1].properties.get("summary"),
        Some(&Some(Value::String("Summary only".to_string())))
    );
    assert_eq!(
        rows.rows[1].properties.get("sha256"),
        Some(&Some(Value::String("sha-summary".to_string())))
    );
    assert_eq!(
        rows.rows[1].properties.get("size_bytes"),
        Some(&Some(Value::Int(128)))
    );
    assert_eq!(rows.rows[1].properties.get("parsed_path"), Some(&None));
}

#[test]
fn source_parsed_metadata_batch_rejects_invalid_rows_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Source {id: 'source_1', lifecycle_state: 'ingested'})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let mut invalid = source_parsed_metadata_update("source_1", "Invalid", "sha-invalid");
    invalid.size_bytes = -1;
    let error = db
        .update_knowledge_source_parsed_metadata_batch(&KnowledgeSourceParsedMetadataBatchRequest {
            updates: vec![invalid],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-negative size bytes"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_source_parsed_metadata_batch_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_source_parsed_metadata_batch_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Source {id: 'source_1', lifecycle_state: 'ingested'})")
            .unwrap();
        db.query("CREATE (:Source {id: 'source_2', lifecycle_state: 'ingested'})")
            .unwrap();
        let batch_count_before_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        let mut first = source_parsed_metadata_update("source_1", "First", "sha-first");
        first.parsed_path = Some("/tmp/first.parsed".to_string());
        let mut second = source_parsed_metadata_update("source_2", "Second", "sha-second");
        second.parsed_path = Some("/tmp/second.parsed".to_string());
        db.update_knowledge_source_parsed_metadata_batch(
            &KnowledgeSourceParsedMetadataBatchRequest {
                updates: vec![first, second],
            },
        )
        .unwrap();
        let batch_count_after_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db
            .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
                entities: vec![
                    KnowledgeEntityRequest {
                        label: "Source".to_string(),
                        external_id: "source_1".to_string(),
                    },
                    KnowledgeEntityRequest {
                        label: "Source".to_string(),
                        external_id: "source_2".to_string(),
                    },
                ],
                property_names: vec![
                    "lifecycle_state".to_string(),
                    "parsed_path".to_string(),
                    "summary".to_string(),
                    "sha256".to_string(),
                ],
            })
            .unwrap();
        assert_eq!(
            rows.rows[0].properties.get("lifecycle_state"),
            Some(&Some(Value::String("parsed".to_string())))
        );
        assert_eq!(
            rows.rows[0].properties.get("parsed_path"),
            Some(&Some(Value::String("/tmp/first.parsed".to_string())))
        );
        assert_eq!(
            rows.rows[0].properties.get("summary"),
            Some(&Some(Value::String("First".to_string())))
        );
        assert_eq!(
            rows.rows[1].properties.get("parsed_path"),
            Some(&Some(Value::String("/tmp/second.parsed".to_string())))
        );
        assert_eq!(
            rows.rows[1].properties.get("sha256"),
            Some(&Some(Value::String("sha-second".to_string())))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

fn parsed_source_create(source_id: &str) -> KnowledgeSourceParsedCreate {
    KnowledgeSourceParsedCreate {
        source_id: source_id.to_string(),
        source_type: "file".to_string(),
        original_name: "Document.md".to_string(),
        mime_type: "text/markdown".to_string(),
        file_path: "/tmp/document.md".to_string(),
        parsed_path: "/tmp/document.parsed".to_string(),
        source_url: String::new(),
        sha256: "sha-document".to_string(),
        size_bytes: 1024,
        version: 1,
        space_id: "default".to_string(),
        section_tree: String::new(),
        summary: "Document summary".to_string(),
        created_at: Value::String("2026-07-19T12:00:00Z".to_string()),
        updated_at: Value::String("2026-07-19T12:01:00Z".to_string()),
        metadata: Value::String("{\"import\":\"manual\"}".to_string()),
    }
}

#[test]
fn creates_source_parsed_batch_for_nowledge_ingest_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:Source {id: 'existing', original_name: 'Existing'})")
        .unwrap();

    let mut markdown = parsed_source_create("markdown-source");
    markdown.version = 3;
    let mut url = parsed_source_create("url-source");
    url.source_type = "url".to_string();
    url.original_name = "URL Source".to_string();
    url.mime_type = "text/html".to_string();
    url.file_path = String::new();
    url.source_url = "https://example.test/source".to_string();
    url.section_tree = "{\"sections\":[]}".to_string();
    url.sha256 = "sha-url".to_string();
    url.size_bytes = 2048;
    let mut pdf = parsed_source_create("pdf-source");
    pdf.original_name = "Source.pdf".to_string();
    pdf.mime_type = "application/pdf".to_string();
    pdf.file_path = "/tmp/source.pdf".to_string();
    pdf.source_url = "file:///tmp/source.pdf".to_string();
    pdf.sha256 = "sha-pdf".to_string();

    let output = db
        .create_knowledge_source_parsed_batch(&KnowledgeSourceParsedCreateBatchRequest {
            creates: vec![
                markdown,
                url,
                pdf,
                parsed_source_create("existing"),
                parsed_source_create("markdown-source"),
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.rows.len(), 5);
    assert_eq!(output.created_count, 3);
    assert_eq!(output.already_exists_count, 2);
    assert_eq!(output.created_node_count, 3);
    assert!(output.rows[0].created);
    assert!(output.rows[1].created);
    assert!(output.rows[2].created);
    assert!(output.rows[3].already_exists);
    assert!(output.rows[4].already_exists);
    assert!(output.rows[0].node_id.is_some());

    let rows = db
        .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Source".to_string(),
                    external_id: "markdown-source".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Source".to_string(),
                    external_id: "url-source".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Source".to_string(),
                    external_id: "pdf-source".to_string(),
                },
            ],
            property_names: vec![
                "source_type".to_string(),
                "original_name".to_string(),
                "mime_type".to_string(),
                "file_path".to_string(),
                "parsed_path".to_string(),
                "source_url".to_string(),
                "sha256".to_string(),
                "size_bytes".to_string(),
                "version".to_string(),
                "space_id".to_string(),
                "lifecycle_state".to_string(),
                "chunk_count".to_string(),
                "memory_count".to_string(),
                "section_tree".to_string(),
                "summary".to_string(),
                "error_message".to_string(),
                "created_at".to_string(),
                "updated_at".to_string(),
                "metadata".to_string(),
            ],
        })
        .unwrap();
    assert_eq!(
        rows.rows[0].properties.get("source_type"),
        Some(&Some(Value::String("file".to_string())))
    );
    assert_eq!(
        rows.rows[0].properties.get("mime_type"),
        Some(&Some(Value::String("text/markdown".to_string())))
    );
    assert_eq!(
        rows.rows[0].properties.get("version"),
        Some(&Some(Value::Int(3)))
    );
    assert_eq!(
        rows.rows[0].properties.get("lifecycle_state"),
        Some(&Some(Value::String("parsed".to_string())))
    );
    assert_eq!(
        rows.rows[0].properties.get("chunk_count"),
        Some(&Some(Value::Int(0)))
    );
    assert_eq!(
        rows.rows[0].properties.get("memory_count"),
        Some(&Some(Value::Int(0)))
    );
    assert_eq!(
        rows.rows[0].properties.get("error_message"),
        Some(&Some(Value::String(String::new())))
    );
    assert_eq!(
        rows.rows[1].properties.get("source_type"),
        Some(&Some(Value::String("url".to_string())))
    );
    assert_eq!(
        rows.rows[1].properties.get("file_path"),
        Some(&Some(Value::String(String::new())))
    );
    assert_eq!(
        rows.rows[1].properties.get("source_url"),
        Some(&Some(Value::String(
            "https://example.test/source".to_string()
        )))
    );
    assert_eq!(
        rows.rows[1].properties.get("section_tree"),
        Some(&Some(Value::String("{\"sections\":[]}".to_string())))
    );
    assert_eq!(
        rows.rows[2].properties.get("mime_type"),
        Some(&Some(Value::String("application/pdf".to_string())))
    );
    assert_eq!(
        rows.rows[2].properties.get("source_url"),
        Some(&Some(Value::String("file:///tmp/source.pdf".to_string())))
    );
}

#[test]
fn source_parsed_create_batch_rejects_invalid_rows_before_wal() {
    let mut db = Database::new();
    let mut invalid = parsed_source_create("invalid-source");
    invalid.version = 0;
    let error = db
        .create_knowledge_source_parsed_batch(&KnowledgeSourceParsedCreateBatchRequest {
            creates: vec![invalid],
        })
        .unwrap_err();

    assert!(error.to_string().contains("positive version"));
    assert_eq!(db.store.commit_epoch(), 0);
}

#[test]
fn typed_source_parsed_create_batch_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_source_parsed_create_batch_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        let batch_count_before_create = read_test_wal(&path)
            .unwrap_or_default()
            .matches("\tbatch\t")
            .count();
        db.create_knowledge_source_parsed_batch(&KnowledgeSourceParsedCreateBatchRequest {
            creates: vec![
                parsed_source_create("source_1"),
                parsed_source_create("source_2"),
            ],
        })
        .unwrap();
        let batch_count_after_create = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_create, batch_count_before_create + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("create_node"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db
            .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
                entities: vec![
                    KnowledgeEntityRequest {
                        label: "Source".to_string(),
                        external_id: "source_1".to_string(),
                    },
                    KnowledgeEntityRequest {
                        label: "Source".to_string(),
                        external_id: "source_2".to_string(),
                    },
                ],
                property_names: vec![
                    "lifecycle_state".to_string(),
                    "sha256".to_string(),
                    "chunk_count".to_string(),
                    "memory_count".to_string(),
                ],
            })
            .unwrap();
        assert_eq!(
            rows.rows[0].properties.get("lifecycle_state"),
            Some(&Some(Value::String("parsed".to_string())))
        );
        assert_eq!(
            rows.rows[0].properties.get("sha256"),
            Some(&Some(Value::String("sha-document".to_string())))
        );
        assert_eq!(
            rows.rows[1].properties.get("chunk_count"),
            Some(&Some(Value::Int(0)))
        );
        assert_eq!(
            rows.rows[1].properties.get("memory_count"),
            Some(&Some(Value::Int(0)))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

const SOURCE_LATEST_BY_NAME_QUERY: &str = "MATCH (s:Source) \
     WHERE s.space_id = $space_id AND s.original_name = $original_name \
     OPTIONAL MATCH (m:Memory)-[r:SOURCED_FROM]->(s) \
     WITH s, count(r) AS sourced_memory_count \
     RETURN s.id AS source_id, id(s) AS node_id, s.original_name AS original_name, \
     COALESCE(s.version, 1) AS version, sourced_memory_count AS sourced_memory_count \
     ORDER BY version DESC, source_id ASC, node_id ASC LIMIT 1";

const SOURCE_LATEST_BY_SHA_QUERY: &str = "MATCH (s:Source) \
     WHERE s.space_id = $space_id AND s.sha256 = $sha256 \
     OPTIONAL MATCH (m:Memory)-[r:SOURCED_FROM]->(s) \
     WITH s, count(r) AS sourced_memory_count \
     RETURN s.id AS source_id, id(s) AS node_id, s.original_name AS original_name, \
     COALESCE(s.version, 1) AS version, sourced_memory_count AS sourced_memory_count \
     ORDER BY version DESC, source_id ASC, node_id ASC LIMIT 1";

#[test]
fn reads_source_latest_version_with_fixed_parameterized_queries() {
    let mut db = Database::new();
    db.query("CREATE (:Source {id: 'source-v1', original_name: 'Doc.md', sha256: 'sha-a', space_id: 'default', version: 1, created_at: 10})")
        .unwrap();
    db.query("CREATE (:Source {id: 'source-v3', original_name: 'Doc.md', sha256: 'sha-b', space_id: 'default', version: 3, created_at: 30})")
        .unwrap();
    db.query("CREATE (:Source {id: 'source-v2-other', original_name: 'Doc.md', sha256: 'sha-a', space_id: 'archive', version: 2, created_at: 20})")
        .unwrap();
    db.query("CREATE (:Source {id: 'source-sha-v4', original_name: 'Other.md', sha256: 'sha-a', space_id: 'default', version: 4, created_at: 40})")
        .unwrap();
    let graph_commit_epoch = db.commit_epoch();
    let mut read = db.begin_read_transaction();
    let by_name_parameters = BTreeMap::from([
        (
            "original_name".to_string(),
            Value::String("Doc.md".to_string()),
        ),
        ("space_id".to_string(), Value::String("default".to_string())),
    ]);
    let by_name = read
        .query_with_params_bounded(SOURCE_LATEST_BY_NAME_QUERY, &by_name_parameters, Some(1))
        .unwrap();
    assert_eq!(read.commit_epoch(), graph_commit_epoch);
    assert_eq!(
        by_name.rows[0].get("source_id"),
        Some(&Value::String("source-v3".to_string()))
    );
    assert_eq!(by_name.rows[0].get("version"), Some(&Value::Int(3)));

    let by_sha_parameters = BTreeMap::from([
        ("sha256".to_string(), Value::String("sha-a".to_string())),
        ("space_id".to_string(), Value::String("default".to_string())),
    ]);
    let by_sha = read
        .query_with_params_bounded(SOURCE_LATEST_BY_SHA_QUERY, &by_sha_parameters, Some(1))
        .unwrap();
    assert_eq!(
        by_sha.rows[0].get("source_id"),
        Some(&Value::String("source-sha-v4".to_string()))
    );
    assert_eq!(by_sha.rows[0].get("version"), Some(&Value::Int(4)));
    assert_eq!(
        by_sha.rows[0].get("original_name"),
        Some(&Value::String("Other.md".to_string()))
    );

    let missing_parameters = BTreeMap::from([
        (
            "original_name".to_string(),
            Value::String("Missing.md".to_string()),
        ),
        ("space_id".to_string(), Value::String("default".to_string())),
    ]);
    let missing = read
        .query_with_params_bounded(SOURCE_LATEST_BY_NAME_QUERY, &missing_parameters, Some(1))
        .unwrap();
    assert!(missing.rows.is_empty());
}

#[test]
fn source_latest_version_values_remain_parameters() {
    let mut db = Database::new();
    db.query("CREATE (:Source {id: 'source-v1', original_name: 'Doc.md', space_id: 'default', version: 1})")
        .unwrap();
    let mut read = db.begin_read_transaction();
    let parameters = BTreeMap::from([
        (
            "original_name".to_string(),
            Value::String("Doc.md') MATCH (n) RETURN n //".to_string()),
        ),
        ("space_id".to_string(), Value::String("default".to_string())),
    ]);
    assert!(read
        .query_with_params_bounded(SOURCE_LATEST_BY_NAME_QUERY, &parameters, Some(1))
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn source_latest_version_query_uses_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Source {id: 'source-v1', original_name: 'Doc.md', space_id: 'default', version: 1})")
        .unwrap();
    db.query("CREATE (:Source {id: 'source-v2', original_name: 'Doc.md', space_id: 'default', version: 2})")
        .unwrap();
    db.query(
        "CREATE (:Source {id: 'source-unversioned', original_name: 'Doc.md', space_id: 'default'})",
    )
    .unwrap();
    let parameters = BTreeMap::from([
        (
            "original_name".to_string(),
            Value::String("Doc.md".to_string()),
        ),
        ("space_id".to_string(), Value::String("default".to_string())),
    ]);
    let first = db
        .query_read_only_with_params_bounded(SOURCE_LATEST_BY_NAME_QUERY, &parameters, Some(1))
        .unwrap();
    let second = db
        .query_read_only_with_params_bounded(SOURCE_LATEST_BY_NAME_QUERY, &parameters, Some(1))
        .unwrap();

    assert_eq!(first, second);
    assert_eq!(
        first.rows[0].get("source_id"),
        Some(&Value::String("source-v2".to_string()))
    );
    assert_eq!(first.rows[0].get("version"), Some(&Value::Int(2)));
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 1);
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hits, 1);

    let summary = db
        .query_sql(
            "SELECT execution_count FROM system.statement_summary \
             WHERE statement_kind = 'match_return' \
             ORDER BY execution_count DESC LIMIT 1",
        )
        .unwrap();
    assert_eq!(summary.rows[0].get("execution_count"), Some(&Value::Int(2)));
}

#[test]
fn creates_source_revision_batch_for_nowledge_revision_edges() {
    let mut db = Database::new();
    db.query("CREATE (:Source {id: 'newer', version: 2})")
        .unwrap();
    db.query("CREATE (:Source {id: 'older', version: 1})")
        .unwrap();
    db.query("CREATE (:Source {original_name: 'Idless Source'})")
        .unwrap();
    let idless = db
        .query("MATCH (s:Source) WHERE s.original_name = 'Idless Source' RETURN id(s) AS id")
        .unwrap();
    let idless_source_id = match idless.rows[0].get("id").unwrap() {
        Value::Int(id) => id.to_string(),
        other => panic!("expected projected id int, got {other:?}"),
    };

    let output = db
        .create_knowledge_source_revision_batch(&KnowledgeSourceRevisionCreateBatchRequest {
            creates: vec![
                KnowledgeSourceRevisionCreate {
                    newer_source_id: "newer".to_string(),
                    older_source_id: "older".to_string(),
                    created_at: Value::String("2026-07-19T12:00:00Z".to_string()),
                },
                KnowledgeSourceRevisionCreate {
                    newer_source_id: "newer".to_string(),
                    older_source_id: "missing".to_string(),
                    created_at: Value::String("2026-07-19T12:01:00Z".to_string()),
                },
                KnowledgeSourceRevisionCreate {
                    newer_source_id: idless_source_id,
                    older_source_id: "older".to_string(),
                    created_at: Value::String("2026-07-19T12:02:00Z".to_string()),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 3);
    assert_eq!(output.graph_commit_epoch_after, 4);
    assert_eq!(output.rows.len(), 3);
    assert_eq!(output.matched_count, 1);
    assert_eq!(output.missing_endpoint_count, 1);
    assert_eq!(output.non_writable_count, 1);
    assert_eq!(output.created_relationship_count, 1);
    assert!(output.rows[0].matched);
    assert!(!output.rows[1].matched);
    assert!(output.rows[2].non_writable);

    let rows = db
        .query("MATCH (newer:Source)-[r:REVISED_AS]->(older:Source) RETURN newer.id, older.id, r.diff_summary, r.sections_changed, r.revision_type, r.detected_by, r.created_at")
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(
        rows.rows[0].get("newer.id"),
        Some(&Value::String("newer".to_string()))
    );
    assert_eq!(
        rows.rows[0].get("older.id"),
        Some(&Value::String("older".to_string()))
    );
    assert_eq!(
        rows.rows[0].get("r.diff_summary"),
        Some(&Value::String(String::new()))
    );
    assert_eq!(
        rows.rows[0].get("r.sections_changed"),
        Some(&Value::String("[]".to_string()))
    );
    assert_eq!(
        rows.rows[0].get("r.revision_type"),
        Some(&Value::String("update".to_string()))
    );
    assert_eq!(
        rows.rows[0].get("r.detected_by"),
        Some(&Value::String("filename_match".to_string()))
    );
    assert_eq!(
        rows.rows[0].get("r.created_at"),
        Some(&Value::String("2026-07-19T12:00:00Z".to_string()))
    );
}

#[test]
fn source_revision_create_batch_rejects_empty_ids_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Source {id: 'newer'})").unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .create_knowledge_source_revision_batch(&KnowledgeSourceRevisionCreateBatchRequest {
            creates: vec![KnowledgeSourceRevisionCreate {
                newer_source_id: "newer".to_string(),
                older_source_id: String::new(),
                created_at: Value::String("2026-07-19T12:00:00Z".to_string()),
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("older source id"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_source_revision_batch_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_source_revision_batch_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Source {id: 'newer-1'})").unwrap();
        db.query("CREATE (:Source {id: 'older-1'})").unwrap();
        db.query("CREATE (:Source {id: 'newer-2'})").unwrap();
        db.query("CREATE (:Source {id: 'older-2'})").unwrap();
        let batch_count_before_create = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.create_knowledge_source_revision_batch(&KnowledgeSourceRevisionCreateBatchRequest {
            creates: vec![
                KnowledgeSourceRevisionCreate {
                    newer_source_id: "newer-1".to_string(),
                    older_source_id: "older-1".to_string(),
                    created_at: Value::String("2026-07-19T12:00:00Z".to_string()),
                },
                KnowledgeSourceRevisionCreate {
                    newer_source_id: "newer-2".to_string(),
                    older_source_id: "older-2".to_string(),
                    created_at: Value::String("2026-07-19T12:01:00Z".to_string()),
                },
            ],
        })
        .unwrap();
        let batch_count_after_create = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_create, batch_count_before_create + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("\tbatch\t"));
    {
        let mut db = Database::open(&path).unwrap();
        let rows = db
            .query("MATCH (newer:Source)-[r:REVISED_AS]->(older:Source) RETURN newer.id, older.id, r.created_at ORDER BY newer.id")
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(
            rows.rows[0].get("newer.id"),
            Some(&Value::String("newer-1".to_string()))
        );
        assert_eq!(
            rows.rows[1].get("older.id"),
            Some(&Value::String("older-2".to_string()))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn updates_memory_lifecycle_batch_for_metadata_state() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', metadata: '{}', is_latest: true, lifecycle_state: 'active', updated_at: 1})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', metadata: '{}', is_latest: true, lifecycle_state: 'active', updated_at: 1})")
        .unwrap();

    let output = db
        .update_knowledge_memory_lifecycle_batch(&KnowledgeMemoryLifecycleBatchRequest {
            updates: vec![
                KnowledgeMemoryLifecycleUpdate {
                    memory_id: "memory_1".to_string(),
                    metadata: Value::String("{\"archived\":true}".to_string()),
                    is_latest: false,
                    lifecycle_state: "archived".to_string(),
                    updated_at: Value::String("2026-07-19T13:00:00Z".to_string()),
                },
                KnowledgeMemoryLifecycleUpdate {
                    memory_id: "memory_2".to_string(),
                    metadata: Value::String("{\"reviewed\":true}".to_string()),
                    is_latest: true,
                    lifecycle_state: "active".to_string(),
                    updated_at: Value::String("2026-07-19T13:01:00Z".to_string()),
                },
                KnowledgeMemoryLifecycleUpdate {
                    memory_id: "memory_2".to_string(),
                    metadata: Value::String("{\"duplicate\":true}".to_string()),
                    is_latest: false,
                    lifecycle_state: "archived".to_string(),
                    updated_at: Value::String("2026-07-19T13:02:00Z".to_string()),
                },
                KnowledgeMemoryLifecycleUpdate {
                    memory_id: "missing".to_string(),
                    metadata: Value::String("{}".to_string()),
                    is_latest: false,
                    lifecycle_state: "archived".to_string(),
                    updated_at: Value::String("2026-07-19T13:03:00Z".to_string()),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 3);
    assert_eq!(output.rows.len(), 4);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.updated_count, 2);
    assert_eq!(output.updated_property_count, 8);
    assert_eq!(output.rows[0].updated_property_count, 4);
    assert_eq!(output.rows[1].updated_property_count, 4);
    assert!(output.rows[2].duplicate);
    assert!(!output.rows[3].matched);

    let rows = db
        .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_2".to_string(),
                },
            ],
            property_names: vec![
                "metadata".to_string(),
                "is_latest".to_string(),
                "lifecycle_state".to_string(),
                "updated_at".to_string(),
            ],
        })
        .unwrap();
    assert_eq!(
        rows.rows[0].properties.get("metadata"),
        Some(&Some(Value::String("{\"archived\":true}".to_string())))
    );
    assert_eq!(
        rows.rows[0].properties.get("is_latest"),
        Some(&Some(Value::Bool(false)))
    );
    assert_eq!(
        rows.rows[0].properties.get("lifecycle_state"),
        Some(&Some(Value::String("archived".to_string())))
    );
    assert_eq!(
        rows.rows[1].properties.get("metadata"),
        Some(&Some(Value::String("{\"reviewed\":true}".to_string())))
    );
    assert_eq!(
        rows.rows[1].properties.get("lifecycle_state"),
        Some(&Some(Value::String("active".to_string())))
    );
}

#[test]
fn memory_lifecycle_batch_rejects_empty_state_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', lifecycle_state: 'active'})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .update_knowledge_memory_lifecycle_batch(&KnowledgeMemoryLifecycleBatchRequest {
            updates: vec![KnowledgeMemoryLifecycleUpdate {
                memory_id: "memory_1".to_string(),
                metadata: Value::String("{}".to_string()),
                is_latest: true,
                lifecycle_state: String::new(),
                updated_at: Value::String("2026-07-19T13:00:00Z".to_string()),
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-empty lifecycle state"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_memory_lifecycle_batch_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_memory_lifecycle_batch_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1', metadata: '{}', is_latest: true, lifecycle_state: 'active'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'memory_2', metadata: '{}', is_latest: true, lifecycle_state: 'active'})")
            .unwrap();
        let batch_count_before_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.update_knowledge_memory_lifecycle_batch(&KnowledgeMemoryLifecycleBatchRequest {
            updates: vec![
                KnowledgeMemoryLifecycleUpdate {
                    memory_id: "memory_1".to_string(),
                    metadata: Value::String("{\"state\":\"archived\"}".to_string()),
                    is_latest: false,
                    lifecycle_state: "archived".to_string(),
                    updated_at: Value::String("2026-07-19T13:00:00Z".to_string()),
                },
                KnowledgeMemoryLifecycleUpdate {
                    memory_id: "memory_2".to_string(),
                    metadata: Value::String("{\"state\":\"active\"}".to_string()),
                    is_latest: true,
                    lifecycle_state: "active".to_string(),
                    updated_at: Value::String("2026-07-19T13:01:00Z".to_string()),
                },
            ],
        })
        .unwrap();
        let batch_count_after_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db
            .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
                entities: vec![
                    KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                ],
                property_names: vec!["metadata".to_string(), "is_latest".to_string()],
            })
            .unwrap();
        assert_eq!(
            rows.rows[0].properties.get("metadata"),
            Some(&Some(Value::String("{\"state\":\"archived\"}".to_string())))
        );
        assert_eq!(
            rows.rows[0].properties.get("is_latest"),
            Some(&Some(Value::Bool(false)))
        );
        assert_eq!(
            rows.rows[1].properties.get("metadata"),
            Some(&Some(Value::String("{\"state\":\"active\"}".to_string())))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn updates_memory_latest_batch_for_nowledge_evolution_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'older', is_latest: true, space_id: 'space_a', metadata: '{}', lifecycle_state: 'active'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'newer', is_latest: false, space_id: 'space_a', metadata: '{}', lifecycle_state: 'active'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'other_space', is_latest: true, space_id: 'space_b'})")
        .unwrap();

    let output = db
        .update_knowledge_memory_latest_batch(&KnowledgeMemoryLatestBatchRequest {
            updates: vec![
                KnowledgeMemoryLatestUpdate {
                    memory_id: "older".to_string(),
                    is_latest: false,
                    space_id_filter: Some("space_a".to_string()),
                },
                KnowledgeMemoryLatestUpdate {
                    memory_id: "newer".to_string(),
                    is_latest: true,
                    space_id_filter: None,
                },
                KnowledgeMemoryLatestUpdate {
                    memory_id: "other_space".to_string(),
                    is_latest: false,
                    space_id_filter: Some("space_a".to_string()),
                },
                KnowledgeMemoryLatestUpdate {
                    memory_id: "older".to_string(),
                    is_latest: true,
                    space_id_filter: None,
                },
                KnowledgeMemoryLatestUpdate {
                    memory_id: "missing".to_string(),
                    is_latest: true,
                    space_id_filter: None,
                },
            ],
        })
        .unwrap();

    assert_eq!(output.matched_count, 2);
    assert_eq!(output.updated_count, 2);
    assert_eq!(output.filtered_out_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.non_writable_count, 0);
    assert!(output.rows[0].updated);
    assert!(output.rows[1].updated);
    assert!(output.rows[2].filtered_out);
    assert!(output.rows[3].duplicate);

    let rows = db
        .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "older".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "newer".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "other_space".to_string(),
                },
            ],
            property_names: vec![
                "is_latest".to_string(),
                "metadata".to_string(),
                "lifecycle_state".to_string(),
            ],
        })
        .unwrap();
    assert_eq!(
        rows.rows[0].properties.get("is_latest"),
        Some(&Some(Value::Bool(false)))
    );
    assert_eq!(
        rows.rows[1].properties.get("is_latest"),
        Some(&Some(Value::Bool(true)))
    );
    assert_eq!(
        rows.rows[2].properties.get("is_latest"),
        Some(&Some(Value::Bool(true)))
    );
    assert_eq!(
        rows.rows[0].properties.get("metadata"),
        Some(&Some(Value::String("{}".to_string())))
    );
    assert_eq!(
        rows.rows[0].properties.get("lifecycle_state"),
        Some(&Some(Value::String("active".to_string())))
    );
}

#[test]
fn memory_latest_batch_rejects_empty_id_before_wal() {
    let path = unique_test_dir("memory_latest_empty_id");
    let mut db = Database::open(&path).unwrap();
    db.query("CREATE (:Memory {id: 'memory_1', is_latest: true})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();
    let wal_before = read_test_wal(&path).unwrap();

    let error = db
        .update_knowledge_memory_latest_batch(&KnowledgeMemoryLatestBatchRequest {
            updates: vec![KnowledgeMemoryLatestUpdate {
                memory_id: String::new(),
                is_latest: false,
                space_id_filter: None,
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-empty memory id"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
    assert_eq!(read_test_wal(&path).unwrap(), wal_before);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_memory_latest_batch_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_memory_latest_batch_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'older', is_latest: true, space_id: 'space_a'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'newer', is_latest: false, space_id: 'space_a'})")
            .unwrap();
        let batch_count_before_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.update_knowledge_memory_latest_batch(&KnowledgeMemoryLatestBatchRequest {
            updates: vec![
                KnowledgeMemoryLatestUpdate {
                    memory_id: "older".to_string(),
                    is_latest: false,
                    space_id_filter: Some("space_a".to_string()),
                },
                KnowledgeMemoryLatestUpdate {
                    memory_id: "newer".to_string(),
                    is_latest: true,
                    space_id_filter: None,
                },
            ],
        })
        .unwrap();
        let batch_count_after_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db
            .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
                entities: vec![
                    KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "older".to_string(),
                    },
                    KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "newer".to_string(),
                    },
                ],
                property_names: vec!["is_latest".to_string()],
            })
            .unwrap();
        assert_eq!(
            rows.rows[0].properties.get("is_latest"),
            Some(&Some(Value::Bool(false)))
        );
        assert_eq!(
            rows.rows[1].properties.get("is_latest"),
            Some(&Some(Value::Bool(true)))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn creates_memory_evolves_batch_for_nowledge_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'older_basic'})").unwrap();
    db.query("CREATE (:Memory {id: 'newer_basic'})").unwrap();
    db.query("CREATE (:Memory {id: 'older_progression'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'newer_progression'})")
        .unwrap();
    db.query("CREATE (:Memory {title: 'Idless Memory'})")
        .unwrap();
    let idless = db
        .query("MATCH (m:Memory) WHERE m.title = 'Idless Memory' RETURN id(m) AS id")
        .unwrap();
    let idless_memory_id = match idless.rows[0].get("id").unwrap() {
        Value::Int(id) => id.to_string(),
        other => panic!("expected projected id int, got {other:?}"),
    };

    let output = db
        .create_knowledge_memory_evolves_batch(&KnowledgeMemoryEvolvesCreateBatchRequest {
            creates: vec![
                KnowledgeMemoryEvolvesCreate {
                    older_memory_id: "older_basic".to_string(),
                    newer_memory_id: "newer_basic".to_string(),
                    content_relation: "confirms".to_string(),
                    created_at: Value::Int(100),
                    is_progression: None,
                    confidence: None,
                    detected_by: None,
                    reviewed: None,
                    reason: None,
                },
                KnowledgeMemoryEvolvesCreate {
                    older_memory_id: "older_progression".to_string(),
                    newer_memory_id: "newer_progression".to_string(),
                    content_relation: "replaces".to_string(),
                    created_at: Value::Int(200),
                    is_progression: Some(true),
                    confidence: Some(Value::Float(0.87)),
                    detected_by: Some("scheduler".to_string()),
                    reviewed: Some(false),
                    reason: Some(Value::String("replacement".to_string())),
                },
                KnowledgeMemoryEvolvesCreate {
                    older_memory_id: "older_basic".to_string(),
                    newer_memory_id: "missing".to_string(),
                    content_relation: "supersedes".to_string(),
                    created_at: Value::Int(300),
                    is_progression: None,
                    confidence: None,
                    detected_by: None,
                    reviewed: None,
                    reason: None,
                },
                KnowledgeMemoryEvolvesCreate {
                    older_memory_id: idless_memory_id,
                    newer_memory_id: "newer_basic".to_string(),
                    content_relation: "projected".to_string(),
                    created_at: Value::Int(400),
                    is_progression: None,
                    confidence: None,
                    detected_by: None,
                    reviewed: None,
                    reason: None,
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 5);
    assert_eq!(output.graph_commit_epoch_after, 6);
    assert_eq!(output.rows.len(), 4);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.missing_endpoint_count, 1);
    assert_eq!(output.non_writable_count, 1);
    assert_eq!(output.created_relationship_count, 2);
    assert!(output.rows[0].matched);
    assert!(output.rows[1].matched);
    assert!(!output.rows[2].matched);
    assert!(output.rows[3].non_writable);

    let basic = db
        .query("MATCH (:Memory {id: 'older_basic'})-[r:EVOLVES]->(:Memory {id: 'newer_basic'}) RETURN r.content_relation AS relation, r.created_at AS created_at")
        .unwrap();
    assert_eq!(basic.rows.len(), 1);
    assert_eq!(
        basic.rows[0].get("relation"),
        Some(&Value::String("confirms".to_string()))
    );
    assert_eq!(basic.rows[0].get("created_at"), Some(&Value::Int(100)));

    let progression = db
        .query("MATCH (:Memory {id: 'older_progression'})-[r:EVOLVES]->(:Memory {id: 'newer_progression'}) RETURN r.content_relation AS relation, r.created_at AS created_at, r.is_progression AS is_progression, r.confidence AS confidence, r.detected_by AS detected_by, r.reviewed AS reviewed, r.reason AS reason")
        .unwrap();
    assert_eq!(progression.rows.len(), 1);
    assert_eq!(
        progression.rows[0].get("relation"),
        Some(&Value::String("replaces".to_string()))
    );
    assert_eq!(
        progression.rows[0].get("created_at"),
        Some(&Value::Int(200))
    );
    assert_eq!(
        progression.rows[0].get("is_progression"),
        Some(&Value::Bool(true))
    );
    assert_eq!(
        progression.rows[0].get("confidence"),
        Some(&Value::Float(0.87))
    );
    assert_eq!(
        progression.rows[0].get("detected_by"),
        Some(&Value::String("scheduler".to_string()))
    );
    assert_eq!(
        progression.rows[0].get("reviewed"),
        Some(&Value::Bool(false))
    );
    assert_eq!(
        progression.rows[0].get("reason"),
        Some(&Value::String("replacement".to_string()))
    );
}

#[test]
fn memory_evolves_create_rejects_empty_fields_before_wal() {
    let path = unique_test_dir("memory_evolves_empty_fields");
    let mut db = Database::open(&path).unwrap();
    db.query("CREATE (:Memory {id: 'older'})").unwrap();
    db.query("CREATE (:Memory {id: 'newer'})").unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();
    let wal_before = read_test_wal(&path).unwrap();

    let error = db
        .create_knowledge_memory_evolves_batch(&KnowledgeMemoryEvolvesCreateBatchRequest {
            creates: vec![KnowledgeMemoryEvolvesCreate {
                older_memory_id: "older".to_string(),
                newer_memory_id: "newer".to_string(),
                content_relation: String::new(),
                created_at: Value::Int(100),
                is_progression: None,
                confidence: None,
                detected_by: None,
                reviewed: None,
                reason: None,
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("content relation"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
    assert_eq!(read_test_wal(&path).unwrap(), wal_before);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn memory_evolves_create_rejects_non_numeric_confidence_before_wal() {
    let path = unique_test_dir("memory_evolves_non_numeric_confidence");
    let mut db = Database::open(&path).unwrap();
    db.query("CREATE (:Memory {id: 'older'})").unwrap();
    db.query("CREATE (:Memory {id: 'newer'})").unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();
    let wal_before = read_test_wal(&path).unwrap();

    let error = db
        .create_knowledge_memory_evolves_batch(&KnowledgeMemoryEvolvesCreateBatchRequest {
            creates: vec![KnowledgeMemoryEvolvesCreate {
                older_memory_id: "older".to_string(),
                newer_memory_id: "newer".to_string(),
                content_relation: "replaces".to_string(),
                created_at: Value::Int(100),
                is_progression: Some(true),
                confidence: Some(Value::String("high".to_string())),
                detected_by: Some("scheduler".to_string()),
                reviewed: None,
                reason: None,
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("numeric finite confidence"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
    assert_eq!(read_test_wal(&path).unwrap(), wal_before);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_memory_evolves_create_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_memory_evolves_create_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'older_1'})").unwrap();
        db.query("CREATE (:Memory {id: 'newer_1'})").unwrap();
        db.query("CREATE (:Memory {id: 'older_2'})").unwrap();
        db.query("CREATE (:Memory {id: 'newer_2'})").unwrap();
        let batch_count_before_create = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.create_knowledge_memory_evolves_batch(&KnowledgeMemoryEvolvesCreateBatchRequest {
            creates: vec![
                KnowledgeMemoryEvolvesCreate {
                    older_memory_id: "older_1".to_string(),
                    newer_memory_id: "newer_1".to_string(),
                    content_relation: "confirms".to_string(),
                    created_at: Value::Int(100),
                    is_progression: None,
                    confidence: None,
                    detected_by: None,
                    reviewed: None,
                    reason: None,
                },
                KnowledgeMemoryEvolvesCreate {
                    older_memory_id: "older_2".to_string(),
                    newer_memory_id: "newer_2".to_string(),
                    content_relation: "replaces".to_string(),
                    created_at: Value::Int(200),
                    is_progression: Some(true),
                    confidence: Some(Value::Float(0.9)),
                    detected_by: Some("scheduler".to_string()),
                    reviewed: Some(false),
                    reason: Some(Value::String("new evidence".to_string())),
                },
            ],
        })
        .unwrap();
        let batch_count_after_create = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_create, batch_count_before_create + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("\tbatch\t"));
    {
        let mut db = Database::open(&path).unwrap();
        let rows = db
            .query("MATCH (older:Memory)-[r:EVOLVES]->(newer:Memory) RETURN older.id, newer.id, r.content_relation, r.created_at, r.confidence ORDER BY older.id")
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(
            rows.rows[0].get("older.id"),
            Some(&Value::String("older_1".to_string()))
        );
        assert_eq!(
            rows.rows[0].get("newer.id"),
            Some(&Value::String("newer_1".to_string()))
        );
        assert_eq!(
            rows.rows[0].get("r.content_relation"),
            Some(&Value::String("confirms".to_string()))
        );
        assert_eq!(rows.rows[0].get("r.created_at"), Some(&Value::Int(100)));
        assert_eq!(
            rows.rows[1].get("older.id"),
            Some(&Value::String("older_2".to_string()))
        );
        assert_eq!(
            rows.rows[1].get("newer.id"),
            Some(&Value::String("newer_2".to_string()))
        );
        assert_eq!(
            rows.rows[1].get("r.content_relation"),
            Some(&Value::String("replaces".to_string()))
        );
        assert_eq!(rows.rows[1].get("r.created_at"), Some(&Value::Int(200)));
        assert_eq!(rows.rows[1].get("r.confidence"), Some(&Value::Float(0.9)));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn updates_skill_usage_stats_batch_for_nowledge_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:Skill {id: 'skill_1', use_count: 1, success_rate: 0.5, metadata: '{}', updated_at: 100})")
        .unwrap();
    db.query("CREATE (:Skill {id: 'skill_2', use_count: 2, metadata: '{}', updated_at: 100})")
        .unwrap();

    let output = db
        .update_knowledge_skill_usage_stats_batch(&KnowledgeSkillUsageStatsBatchRequest {
            updates: vec![
                KnowledgeSkillUsageStatsUpdate {
                    skill_id: "skill_1".to_string(),
                    use_count: 8,
                    success_rate: Some(Value::Float(0.75)),
                    last_activity_at: Value::Int(810),
                    updated_at: Value::Int(820),
                    metadata: Value::String("{\"runs\":8}".to_string()),
                },
                KnowledgeSkillUsageStatsUpdate {
                    skill_id: "skill_2".to_string(),
                    use_count: 5,
                    success_rate: None,
                    last_activity_at: Value::Int(910),
                    updated_at: Value::Int(910),
                    metadata: Value::String("{\"runs\":5}".to_string()),
                },
                KnowledgeSkillUsageStatsUpdate {
                    skill_id: "skill_2".to_string(),
                    use_count: 6,
                    success_rate: None,
                    last_activity_at: Value::Int(920),
                    updated_at: Value::Int(920),
                    metadata: Value::String("{\"duplicate\":true}".to_string()),
                },
                KnowledgeSkillUsageStatsUpdate {
                    skill_id: "missing".to_string(),
                    use_count: 1,
                    success_rate: Some(Value::Float(1.0)),
                    last_activity_at: Value::Int(930),
                    updated_at: Value::Int(930),
                    metadata: Value::String("{}".to_string()),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 3);
    assert_eq!(output.rows.len(), 4);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.updated_count, 2);
    assert_eq!(output.updated_property_count, 9);
    assert_eq!(output.rows[0].updated_property_count, 5);
    assert_eq!(output.rows[1].updated_property_count, 4);
    assert!(output.rows[2].duplicate);
    assert!(!output.rows[3].matched);

    let rows = db
        .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Skill".to_string(),
                    external_id: "skill_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Skill".to_string(),
                    external_id: "skill_2".to_string(),
                },
            ],
            property_names: vec![
                "use_count".to_string(),
                "success_rate".to_string(),
                "last_activity_at".to_string(),
                "updated_at".to_string(),
                "metadata".to_string(),
            ],
        })
        .unwrap();
    assert_eq!(
        rows.rows[0].properties.get("use_count"),
        Some(&Some(Value::Int(8)))
    );
    assert_eq!(
        rows.rows[0].properties.get("success_rate"),
        Some(&Some(Value::Float(0.75)))
    );
    assert_eq!(
        rows.rows[0].properties.get("metadata"),
        Some(&Some(Value::String("{\"runs\":8}".to_string())))
    );
    assert_eq!(
        rows.rows[1].properties.get("use_count"),
        Some(&Some(Value::Int(5)))
    );
    assert_eq!(rows.rows[1].properties.get("success_rate"), Some(&None));
    assert_eq!(
        rows.rows[1].properties.get("last_activity_at"),
        Some(&Some(Value::Int(910)))
    );
    assert_eq!(
        rows.rows[1].properties.get("metadata"),
        Some(&Some(Value::String("{\"runs\":5}".to_string())))
    );
}

#[test]
fn skill_usage_stats_batch_rejects_negative_use_count_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Skill {id: 'skill_1', use_count: 1})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .update_knowledge_skill_usage_stats_batch(&KnowledgeSkillUsageStatsBatchRequest {
            updates: vec![KnowledgeSkillUsageStatsUpdate {
                skill_id: "skill_1".to_string(),
                use_count: -1,
                success_rate: None,
                last_activity_at: Value::Int(1),
                updated_at: Value::Int(1),
                metadata: Value::String("{}".to_string()),
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-negative use count"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_skill_usage_stats_batch_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_skill_usage_stats_batch_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query(
            "CREATE (:Skill {id: 'skill_1', use_count: 1, success_rate: 0.5, metadata: '{}'})",
        )
        .unwrap();
        db.query("CREATE (:Skill {id: 'skill_2', use_count: 1, metadata: '{}'})")
            .unwrap();
        let batch_count_before_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.update_knowledge_skill_usage_stats_batch(&KnowledgeSkillUsageStatsBatchRequest {
            updates: vec![
                KnowledgeSkillUsageStatsUpdate {
                    skill_id: "skill_1".to_string(),
                    use_count: 8,
                    success_rate: Some(Value::Float(0.75)),
                    last_activity_at: Value::Int(810),
                    updated_at: Value::Int(820),
                    metadata: Value::String("{\"runs\":8}".to_string()),
                },
                KnowledgeSkillUsageStatsUpdate {
                    skill_id: "skill_2".to_string(),
                    use_count: 5,
                    success_rate: None,
                    last_activity_at: Value::Int(910),
                    updated_at: Value::Int(910),
                    metadata: Value::String("{\"runs\":5}".to_string()),
                },
            ],
        })
        .unwrap();
        let batch_count_after_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db
            .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
                entities: vec![
                    KnowledgeEntityRequest {
                        label: "Skill".to_string(),
                        external_id: "skill_1".to_string(),
                    },
                    KnowledgeEntityRequest {
                        label: "Skill".to_string(),
                        external_id: "skill_2".to_string(),
                    },
                ],
                property_names: vec![
                    "use_count".to_string(),
                    "success_rate".to_string(),
                    "metadata".to_string(),
                ],
            })
            .unwrap();
        assert_eq!(
            rows.rows[0].properties.get("use_count"),
            Some(&Some(Value::Int(8)))
        );
        assert_eq!(
            rows.rows[0].properties.get("success_rate"),
            Some(&Some(Value::Float(0.75)))
        );
        assert_eq!(
            rows.rows[1].properties.get("use_count"),
            Some(&Some(Value::Int(5)))
        );
        assert_eq!(rows.rows[1].properties.get("success_rate"), Some(&None));
        assert_eq!(
            rows.rows[1].properties.get("metadata"),
            Some(&Some(Value::String("{\"runs\":5}".to_string())))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn updates_skill_metadata_batch_for_nowledge_shape() {
    let mut db = Database::new();
    db.query("CREATE (:Skill {id: 'skill_1', metadata: '{}', updated_at: 1})")
        .unwrap();
    db.query("CREATE (:Skill {id: 'skill_2', metadata: '{}', updated_at: 1})")
        .unwrap();
    db.query("CREATE (:Skill {title: 'Idless Skill', metadata: '{}', updated_at: 1})")
        .unwrap();
    let idless = db
        .query("MATCH (s:Skill) WHERE s.title = 'Idless Skill' RETURN id(s) AS id")
        .unwrap();
    let idless_skill_id = match idless.rows[0].get("id").unwrap() {
        Value::Int(id) => id.to_string(),
        other => panic!("expected projected id int, got {other:?}"),
    };

    let output = db
        .update_knowledge_skill_metadata_batch(&KnowledgeSkillMetadataBatchRequest {
            updates: vec![
                KnowledgeSkillMetadataUpdate {
                    skill_id: "skill_1".to_string(),
                    metadata: Value::String("{\"source\":\"rest\"}".to_string()),
                    updated_at: Value::Int(101),
                },
                KnowledgeSkillMetadataUpdate {
                    skill_id: "skill_2".to_string(),
                    metadata: Value::String("{\"source\":\"mcp\"}".to_string()),
                    updated_at: Value::Int(202),
                },
                KnowledgeSkillMetadataUpdate {
                    skill_id: "missing".to_string(),
                    metadata: Value::String("{\"missing\":true}".to_string()),
                    updated_at: Value::Int(303),
                },
                KnowledgeSkillMetadataUpdate {
                    skill_id: idless_skill_id,
                    metadata: Value::String("{\"idless\":true}".to_string()),
                    updated_at: Value::Int(404),
                },
                KnowledgeSkillMetadataUpdate {
                    skill_id: "skill_1".to_string(),
                    metadata: Value::String("{\"duplicate\":true}".to_string()),
                    updated_at: Value::Int(505),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 3);
    assert_eq!(output.graph_commit_epoch_after, 4);
    assert_eq!(output.rows.len(), 5);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.non_writable_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.updated_count, 2);
    assert_eq!(output.updated_property_count, 4);
    assert_eq!(output.rows[0].updated_property_count, 2);
    assert_eq!(output.rows[1].updated_property_count, 2);
    assert!(!output.rows[2].matched);
    assert!(output.rows[3].non_writable);
    assert!(output.rows[4].duplicate);

    let rows = db
        .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Skill".to_string(),
                    external_id: "skill_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Skill".to_string(),
                    external_id: "skill_2".to_string(),
                },
            ],
            property_names: vec!["metadata".to_string(), "updated_at".to_string()],
        })
        .unwrap();
    assert_eq!(
        rows.rows[0].properties.get("metadata"),
        Some(&Some(Value::String("{\"source\":\"rest\"}".to_string())))
    );
    assert_eq!(
        rows.rows[0].properties.get("updated_at"),
        Some(&Some(Value::Int(101)))
    );
    assert_eq!(
        rows.rows[1].properties.get("metadata"),
        Some(&Some(Value::String("{\"source\":\"mcp\"}".to_string())))
    );
    assert_eq!(
        rows.rows[1].properties.get("updated_at"),
        Some(&Some(Value::Int(202)))
    );
}

#[test]
fn skill_metadata_update_rejects_empty_id_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Skill {id: 'skill_1', metadata: '{}', updated_at: 1})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .update_knowledge_skill_metadata_batch(&KnowledgeSkillMetadataBatchRequest {
            updates: vec![KnowledgeSkillMetadataUpdate {
                skill_id: String::new(),
                metadata: Value::String("{}".to_string()),
                updated_at: Value::Int(2),
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-empty skill id"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_skill_metadata_update_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_skill_metadata_update_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Skill {id: 'skill_1', metadata: '{}', updated_at: 1})")
            .unwrap();
        db.query("CREATE (:Skill {id: 'skill_2', metadata: '{}', updated_at: 1})")
            .unwrap();
        let batch_count_before_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.update_knowledge_skill_metadata_batch(&KnowledgeSkillMetadataBatchRequest {
            updates: vec![
                KnowledgeSkillMetadataUpdate {
                    skill_id: "skill_1".to_string(),
                    metadata: Value::String("{\"source\":\"rest\"}".to_string()),
                    updated_at: Value::Int(101),
                },
                KnowledgeSkillMetadataUpdate {
                    skill_id: "skill_2".to_string(),
                    metadata: Value::String("{\"source\":\"mcp\"}".to_string()),
                    updated_at: Value::Int(202),
                },
            ],
        })
        .unwrap();
        let batch_count_after_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db
            .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
                entities: vec![
                    KnowledgeEntityRequest {
                        label: "Skill".to_string(),
                        external_id: "skill_1".to_string(),
                    },
                    KnowledgeEntityRequest {
                        label: "Skill".to_string(),
                        external_id: "skill_2".to_string(),
                    },
                ],
                property_names: vec!["metadata".to_string(), "updated_at".to_string()],
            })
            .unwrap();
        assert_eq!(
            rows.rows[0].properties.get("metadata"),
            Some(&Some(Value::String("{\"source\":\"rest\"}".to_string())))
        );
        assert_eq!(
            rows.rows[0].properties.get("updated_at"),
            Some(&Some(Value::Int(101)))
        );
        assert_eq!(
            rows.rows[1].properties.get("metadata"),
            Some(&Some(Value::String("{\"source\":\"mcp\"}".to_string())))
        );
        assert_eq!(
            rows.rows[1].properties.get("updated_at"),
            Some(&Some(Value::Int(202)))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

fn skill_lifecycle_update(skill_id: &str) -> KnowledgeSkillLifecycleUpdate {
    KnowledgeSkillLifecycleUpdate {
        skill_id: skill_id.to_string(),
        stage: None,
        rejected_at: None,
        rationale: None,
        version: None,
        title: None,
        name: None,
        description: None,
        triggers: None,
        tools: None,
        bundle_path: None,
        content_hash: None,
        write_origin: None,
        metadata: None,
        updated_at: Value::Int(1),
    }
}

#[test]
fn updates_skill_lifecycle_batch_for_nowledge_shapes() {
    let mut db = Database::new();
    for skill_id in [
        "skill_reject",
        "skill_compiled",
        "skill_promote",
        "skill_draft",
        "skill_content",
    ] {
        db.query(format!("CREATE (:Skill {{id: '{skill_id}', stage: 'candidate', metadata: '{{}}', updated_at: 1}})").as_str())
            .unwrap();
    }

    let output = db
        .update_knowledge_skill_lifecycle_batch(&KnowledgeSkillLifecycleBatchRequest {
            updates: vec![
                KnowledgeSkillLifecycleUpdate {
                    stage: Some("rejected".to_string()),
                    rejected_at: Some(Value::Int(201)),
                    updated_at: Value::Int(202),
                    ..skill_lifecycle_update("skill_reject")
                },
                KnowledgeSkillLifecycleUpdate {
                    version: Some(Value::Int(2)),
                    title: Some(Value::String("compiled-skill".to_string())),
                    name: Some(Value::String("compiled-skill".to_string())),
                    description: Some(Value::String("compiled description".to_string())),
                    content_hash: Some(Value::String("hash-compiled-rest".to_string())),
                    bundle_path: Some(Value::String("/tmp/compiled-rest".to_string())),
                    metadata: Some(Value::String("{\"compiled\":true}".to_string())),
                    updated_at: Value::Int(401),
                    ..skill_lifecycle_update("skill_compiled")
                },
                KnowledgeSkillLifecycleUpdate {
                    stage: Some("promotable".to_string()),
                    rationale: Some(Value::String("ready to promote".to_string())),
                    updated_at: Value::Int(1_700_000_032),
                    ..skill_lifecycle_update("skill_promote")
                },
                KnowledgeSkillLifecycleUpdate {
                    stage: Some("draft".to_string()),
                    name: Some(Value::String("mcp-skill".to_string())),
                    description: Some(Value::String("compiled skill".to_string())),
                    triggers: Some(Value::List(vec![Value::String("compile".to_string())])),
                    tools: Some(Value::List(vec![Value::String("shell".to_string())])),
                    bundle_path: Some(Value::String("/tmp/mcp-skill".to_string())),
                    content_hash: Some(Value::String("hash-write".to_string())),
                    write_origin: Some("compiler".to_string()),
                    updated_at: Value::Int(1_700_000_033),
                    ..skill_lifecycle_update("skill_draft")
                },
                KnowledgeSkillLifecycleUpdate {
                    content_hash: Some(Value::String("hash-updated".to_string())),
                    metadata: Some(Value::String("{\"updated\":true}".to_string())),
                    updated_at: Value::Int(701),
                    ..skill_lifecycle_update("skill_content")
                },
                KnowledgeSkillLifecycleUpdate {
                    stage: Some("active".to_string()),
                    updated_at: Value::Int(800),
                    ..skill_lifecycle_update("skill_draft")
                },
                KnowledgeSkillLifecycleUpdate {
                    stage: Some("active".to_string()),
                    updated_at: Value::Int(900),
                    ..skill_lifecycle_update("missing")
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 5);
    assert_eq!(output.graph_commit_epoch_after, 6);
    assert_eq!(output.rows.len(), 7);
    assert_eq!(output.matched_count, 5);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.updated_count, 5);
    assert_eq!(output.updated_property_count, 26);
    assert_eq!(output.rows[0].updated_property_count, 3);
    assert_eq!(output.rows[1].updated_property_count, 8);
    assert_eq!(output.rows[2].updated_property_count, 3);
    assert_eq!(output.rows[3].updated_property_count, 9);
    assert_eq!(output.rows[4].updated_property_count, 3);
    assert!(output.rows[5].duplicate);
    assert!(!output.rows[6].matched);

    let rows = db
        .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Skill".to_string(),
                    external_id: "skill_reject".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Skill".to_string(),
                    external_id: "skill_compiled".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Skill".to_string(),
                    external_id: "skill_promote".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Skill".to_string(),
                    external_id: "skill_draft".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Skill".to_string(),
                    external_id: "skill_content".to_string(),
                },
            ],
            property_names: vec![
                "stage".to_string(),
                "rejected_at".to_string(),
                "rationale".to_string(),
                "version".to_string(),
                "title".to_string(),
                "name".to_string(),
                "description".to_string(),
                "triggers".to_string(),
                "tools".to_string(),
                "bundle_path".to_string(),
                "content_hash".to_string(),
                "write_origin".to_string(),
                "metadata".to_string(),
                "updated_at".to_string(),
            ],
        })
        .unwrap();
    assert_eq!(
        rows.rows[0].properties.get("stage"),
        Some(&Some(Value::String("rejected".to_string())))
    );
    assert_eq!(
        rows.rows[0].properties.get("rejected_at"),
        Some(&Some(Value::Int(201)))
    );
    assert_eq!(
        rows.rows[1].properties.get("version"),
        Some(&Some(Value::Int(2)))
    );
    assert_eq!(
        rows.rows[1].properties.get("content_hash"),
        Some(&Some(Value::String("hash-compiled-rest".to_string())))
    );
    assert_eq!(
        rows.rows[2].properties.get("stage"),
        Some(&Some(Value::String("promotable".to_string())))
    );
    assert_eq!(
        rows.rows[2].properties.get("rationale"),
        Some(&Some(Value::String("ready to promote".to_string())))
    );
    assert_eq!(
        rows.rows[3].properties.get("stage"),
        Some(&Some(Value::String("draft".to_string())))
    );
    assert_eq!(
        rows.rows[3].properties.get("triggers"),
        Some(&Some(Value::List(vec![Value::String(
            "compile".to_string()
        )])))
    );
    assert_eq!(
        rows.rows[3].properties.get("write_origin"),
        Some(&Some(Value::String("compiler".to_string())))
    );
    assert_eq!(
        rows.rows[4].properties.get("content_hash"),
        Some(&Some(Value::String("hash-updated".to_string())))
    );
    assert_eq!(
        rows.rows[4].properties.get("metadata"),
        Some(&Some(Value::String("{\"updated\":true}".to_string())))
    );
}

#[test]
fn skill_lifecycle_batch_rejects_empty_stage_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Skill {id: 'skill_1', stage: 'candidate'})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .update_knowledge_skill_lifecycle_batch(&KnowledgeSkillLifecycleBatchRequest {
            updates: vec![KnowledgeSkillLifecycleUpdate {
                stage: Some(String::new()),
                updated_at: Value::Int(100),
                ..skill_lifecycle_update("skill_1")
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-empty stage"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_skill_lifecycle_batch_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_skill_lifecycle_batch_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query(
            "CREATE (:Skill {id: 'skill_1', stage: 'candidate', metadata: '{}', updated_at: 1})",
        )
        .unwrap();
        db.query("CREATE (:Skill {id: 'skill_2', stage: 'draft', metadata: '{}', updated_at: 1})")
            .unwrap();
        let batch_count_before_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.update_knowledge_skill_lifecycle_batch(&KnowledgeSkillLifecycleBatchRequest {
            updates: vec![
                KnowledgeSkillLifecycleUpdate {
                    stage: Some("promotable".to_string()),
                    rationale: Some(Value::String("ready".to_string())),
                    updated_at: Value::Int(100),
                    ..skill_lifecycle_update("skill_1")
                },
                KnowledgeSkillLifecycleUpdate {
                    content_hash: Some(Value::String("hash-updated".to_string())),
                    metadata: Some(Value::String("{\"updated\":true}".to_string())),
                    updated_at: Value::Int(200),
                    ..skill_lifecycle_update("skill_2")
                },
            ],
        })
        .unwrap();
        let batch_count_after_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db
            .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
                entities: vec![
                    KnowledgeEntityRequest {
                        label: "Skill".to_string(),
                        external_id: "skill_1".to_string(),
                    },
                    KnowledgeEntityRequest {
                        label: "Skill".to_string(),
                        external_id: "skill_2".to_string(),
                    },
                ],
                property_names: vec![
                    "stage".to_string(),
                    "rationale".to_string(),
                    "content_hash".to_string(),
                    "metadata".to_string(),
                ],
            })
            .unwrap();
        assert_eq!(
            rows.rows[0].properties.get("stage"),
            Some(&Some(Value::String("promotable".to_string())))
        );
        assert_eq!(
            rows.rows[0].properties.get("rationale"),
            Some(&Some(Value::String("ready".to_string())))
        );
        assert_eq!(
            rows.rows[1].properties.get("content_hash"),
            Some(&Some(Value::String("hash-updated".to_string())))
        );
        assert_eq!(
            rows.rows[1].properties.get("metadata"),
            Some(&Some(Value::String("{\"updated\":true}".to_string())))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn deletes_skills_for_nowledge_detach_delete_shape() {
    let mut db = Database::new();
    db.query("CREATE (:Skill {id: 'skill_1'})-[:SYNTHESIZED_FROM]->(:Memory {id: 'memory_1'})")
        .unwrap();
    db.query("CREATE (:Skill {id: 'skill_2'})-[:SYNTHESIZED_FROM]->(:Memory {id: 'memory_2'})")
        .unwrap();
    db.query("CREATE (:Skill {id: 'skill_3'})").unwrap();
    db.query("CREATE (:Skill {title: 'Idless Skill'})").unwrap();
    let idless = db
        .query("MATCH (s:Skill) WHERE s.title = 'Idless Skill' RETURN id(s) AS id")
        .unwrap();
    let idless_skill_id = match idless.rows[0].get("id").unwrap() {
        Value::Int(id) => id.to_string(),
        other => panic!("expected projected id int, got {other:?}"),
    };

    let output = db
        .delete_knowledge_skills(&KnowledgeSkillDeleteBatchRequest {
            skill_ids: vec![
                "skill_1".to_string(),
                "missing".to_string(),
                idless_skill_id,
                "skill_2".to_string(),
                "skill_1".to_string(),
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 4);
    assert_eq!(output.graph_commit_epoch_after, 5);
    assert_eq!(output.rows.len(), 5);
    assert_eq!(output.matched_count, 3);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.non_writable_count, 1);
    assert_eq!(output.deleted_node_count, 2);
    assert!(output.rows[0].matched);
    assert!(!output.rows[1].matched);
    assert!(output.rows[2].non_writable);
    assert!(output.rows[3].matched);
    assert!(output.rows[4].matched);

    let skills = db
        .query(
            "MATCH (s:Skill) WHERE s.id IS NOT NULL \
             RETURN s.id AS skill_id ORDER BY skill_id ASC",
        )
        .unwrap();
    assert_eq!(skills.rows.len(), 1);
    assert_eq!(
        skills.rows[0].get("skill_id"),
        Some(&Value::String("skill_3".to_string()))
    );
    assert_eq!(
        db.query(
            "MATCH (s:Skill)-[r:SYNTHESIZED_FROM]->(m:Memory) RETURN count(r) AS relationships"
        )
        .unwrap()
        .rows[0]
            .get("relationships"),
        Some(&Value::Int(0))
    );
    assert_eq!(
        db.query("MATCH (m:Memory) RETURN count(m) AS memories")
            .unwrap()
            .rows[0]
            .get("memories"),
        Some(&Value::Int(2))
    );
}

#[test]
fn skill_delete_rejects_empty_skill_id_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Skill {id: 'skill_1'})").unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .delete_knowledge_skills(&KnowledgeSkillDeleteBatchRequest {
            skill_ids: vec![String::new()],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-empty skill id"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_skill_delete_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_skill_delete_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Skill {id: 'skill_1'})-[:SYNTHESIZED_FROM]->(:Memory {id: 'memory_1'})")
            .unwrap();
        db.query("CREATE (:Skill {id: 'skill_2'})").unwrap();
        let batch_count_before_delete = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.delete_knowledge_skills(&KnowledgeSkillDeleteBatchRequest {
            skill_ids: vec!["skill_1".to_string(), "skill_2".to_string()],
        })
        .unwrap();
        let batch_count_after_delete = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_delete, batch_count_before_delete + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("delete_node"));
    {
        let db = Database::open(&path).unwrap();
        let skills = db
            .query_read_only_with_params_bounded(
                "MATCH (s:Skill) WHERE s.id IN $skill_ids RETURN count(s) AS total",
                &BTreeMap::from([(
                    "skill_ids".to_string(),
                    Value::List(vec![
                        Value::String("skill_1".to_string()),
                        Value::String("skill_2".to_string()),
                    ]),
                )]),
                Some(1),
            )
            .unwrap();
        assert_eq!(skills.rows[0].get("total"), Some(&Value::Int(0)));
        assert!(db
            .query_entity_via_cypher(&KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            })
            .unwrap()
            .entity
            .is_some());
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn merges_skill_source_for_rest_skills_write_shape() {
    let path = unique_test_dir("skill_source_merge_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Skill {id: 'skill-source-1', stage: 'active'})")
            .unwrap();
        db.query(
            "CREATE (:Memory {id: 'memory-source-1', title: 'Memory Source', created_at: 10})",
        )
        .unwrap();

        let output = db
            .merge_knowledge_skill_source(&KnowledgeSkillSourceMergeRequest {
                skill_id: "skill-source-1".to_string(),
                memory_id: "memory-source-1".to_string(),
                occasion_key: String::new(),
                created_at: Value::Int(100),
            })
            .unwrap();
        assert_eq!(output.skill_id, "skill-source-1");
        assert_eq!(output.memory_id, "memory-source-1");
        assert!(output.matched);
        assert!(output.created);
        assert!(!output.already_exists);
        assert!(!output.missing_endpoint);
        assert!(!output.non_writable);
        assert!(output.skill_node_id.is_some());
        assert!(output.memory_node_id.is_some());
        assert!(output.relationship_id.is_some());
        assert_eq!(output.created_relationship_count, 1);
        assert!(output.graph_commit_epoch_after > output.graph_commit_epoch_before);

        let count = db
            .query("MATCH (:Skill {id: 'skill-source-1'})-[r:SYNTHESIZED_FROM]->(:Memory {id: 'memory-source-1'}) RETURN count(r) AS total")
            .unwrap();
        assert_eq!(count.rows[0].get("total"), Some(&Value::Int(1)));
        let properties = db
            .query("MATCH (:Skill {id: 'skill-source-1'})-[r:SYNTHESIZED_FROM]->(:Memory {id: 'memory-source-1'}) RETURN r.weight AS weight, r.occasion_key AS occasion_key, r.created_at AS created_at")
            .unwrap();
        assert_eq!(properties.rows[0].get("weight"), Some(&Value::Float(1.0)));
        assert_eq!(
            properties.rows[0].get("occasion_key"),
            Some(&Value::String(String::new()))
        );
        assert_eq!(properties.rows[0].get("created_at"), Some(&Value::Int(100)));

        let commit_epoch_after_create = db.store.commit_epoch();
        let second = db
            .merge_knowledge_skill_source(&KnowledgeSkillSourceMergeRequest {
                skill_id: "skill-source-1".to_string(),
                memory_id: "memory-source-1".to_string(),
                occasion_key: "later".to_string(),
                created_at: Value::Int(200),
            })
            .unwrap();
        assert!(second.matched);
        assert!(!second.created);
        assert!(second.already_exists);
        assert_eq!(second.relationship_id, output.relationship_id);
        assert_eq!(second.created_relationship_count, 0);
        assert_eq!(second.graph_commit_epoch_after, commit_epoch_after_create);
        assert_eq!(db.store.commit_epoch(), commit_epoch_after_create);
        let unchanged = db
            .query("MATCH (:Skill {id: 'skill-source-1'})-[r:SYNTHESIZED_FROM]->(:Memory {id: 'memory-source-1'}) RETURN r.weight AS weight, r.occasion_key AS occasion_key, r.created_at AS created_at")
            .unwrap();
        assert_eq!(unchanged.rows[0].get("weight"), Some(&Value::Float(1.0)));
        assert_eq!(
            unchanged.rows[0].get("occasion_key"),
            Some(&Value::String(String::new()))
        );
        assert_eq!(unchanged.rows[0].get("created_at"), Some(&Value::Int(100)));
    }
    {
        let mut db = Database::open(&path).unwrap();
        let count = db
            .query("MATCH (:Skill {id: 'skill-source-1'})-[r:SYNTHESIZED_FROM]->(:Memory {id: 'memory-source-1'}) RETURN count(r) AS total")
            .unwrap();
        assert_eq!(count.rows[0].get("total"), Some(&Value::Int(1)));
        let properties = db
            .query("MATCH (:Skill {id: 'skill-source-1'})-[r:SYNTHESIZED_FROM]->(:Memory {id: 'memory-source-1'}) RETURN r.weight AS weight, r.occasion_key AS occasion_key, r.created_at AS created_at")
            .unwrap();
        assert_eq!(properties.rows[0].get("weight"), Some(&Value::Float(1.0)));
        assert_eq!(
            properties.rows[0].get("occasion_key"),
            Some(&Value::String(String::new()))
        );
        assert_eq!(properties.rows[0].get("created_at"), Some(&Value::Int(100)));
    }

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn skill_source_merge_reports_missing_endpoint_and_rejects_empty_ids() {
    let mut db = Database::new();

    let empty_skill = db
        .merge_knowledge_skill_source(&KnowledgeSkillSourceMergeRequest {
            skill_id: String::new(),
            memory_id: "memory-source-1".to_string(),
            occasion_key: String::new(),
            created_at: Value::Int(100),
        })
        .unwrap_err();
    assert!(empty_skill.to_string().contains("non-empty skill id"));

    let empty_memory = db
        .merge_knowledge_skill_source(&KnowledgeSkillSourceMergeRequest {
            skill_id: "skill-source-1".to_string(),
            memory_id: String::new(),
            occasion_key: String::new(),
            created_at: Value::Int(100),
        })
        .unwrap_err();
    assert!(empty_memory.to_string().contains("non-empty memory id"));

    db.query("CREATE (:Skill {id: 'skill-source-1', stage: 'active'})")
        .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();
    let missing = db
        .merge_knowledge_skill_source(&KnowledgeSkillSourceMergeRequest {
            skill_id: "skill-source-1".to_string(),
            memory_id: "missing-memory".to_string(),
            occasion_key: String::new(),
            created_at: Value::Int(100),
        })
        .unwrap();
    assert!(!missing.matched);
    assert!(!missing.created);
    assert!(!missing.already_exists);
    assert!(missing.missing_endpoint);
    assert!(!missing.non_writable);
    assert!(missing.skill_node_id.is_some());
    assert_eq!(missing.memory_node_id, None);
    assert_eq!(missing.relationship_id, None);
    assert_eq!(missing.created_relationship_count, 0);
    assert_eq!(missing.graph_commit_epoch_before, graph_commit_epoch);
    assert_eq!(missing.graph_commit_epoch_after, graph_commit_epoch);
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);
}

#[test]
fn updates_thread_metadata_batch_for_nowledge_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'thread_1', metadata: '{}', updated_at: 1})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'thread_2', metadata: '{}', updated_at: 1})")
        .unwrap();

    let output = db
        .update_knowledge_thread_metadata_batch(&KnowledgeThreadMetadataBatchRequest {
            updates: vec![
                KnowledgeThreadMetadataUpdate {
                    thread_id: "thread_1".to_string(),
                    metadata: Value::String("{\"summary\":\"ready\"}".to_string()),
                    updated_at: Some(Value::Int(100)),
                },
                KnowledgeThreadMetadataUpdate {
                    thread_id: "thread_2".to_string(),
                    metadata: Value::String("{\"summary\":\"metadata-only\"}".to_string()),
                    updated_at: None,
                },
                KnowledgeThreadMetadataUpdate {
                    thread_id: "thread_2".to_string(),
                    metadata: Value::String("{\"duplicate\":true}".to_string()),
                    updated_at: Some(Value::Int(200)),
                },
                KnowledgeThreadMetadataUpdate {
                    thread_id: "missing".to_string(),
                    metadata: Value::String("{}".to_string()),
                    updated_at: Some(Value::Int(300)),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 3);
    assert_eq!(output.rows.len(), 4);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.updated_count, 2);
    assert_eq!(output.updated_property_count, 3);
    assert_eq!(output.rows[0].updated_property_count, 2);
    assert_eq!(output.rows[1].updated_property_count, 1);
    assert!(output.rows[2].duplicate);
    assert!(!output.rows[3].matched);

    let rows = db
        .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Thread".to_string(),
                    external_id: "thread_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Thread".to_string(),
                    external_id: "thread_2".to_string(),
                },
            ],
            property_names: vec!["metadata".to_string(), "updated_at".to_string()],
        })
        .unwrap();
    assert_eq!(
        rows.rows[0].properties.get("metadata"),
        Some(&Some(Value::String("{\"summary\":\"ready\"}".to_string())))
    );
    assert_eq!(
        rows.rows[0].properties.get("updated_at"),
        Some(&Some(Value::Int(100)))
    );
    assert_eq!(
        rows.rows[1].properties.get("metadata"),
        Some(&Some(Value::String(
            "{\"summary\":\"metadata-only\"}".to_string()
        )))
    );
    assert_eq!(
        rows.rows[1].properties.get("updated_at"),
        Some(&Some(Value::Int(1)))
    );
}

#[test]
fn thread_metadata_batch_rejects_empty_thread_id_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'thread_1', metadata: '{}', updated_at: 1})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .update_knowledge_thread_metadata_batch(&KnowledgeThreadMetadataBatchRequest {
            updates: vec![KnowledgeThreadMetadataUpdate {
                thread_id: String::new(),
                metadata: Value::String("{}".to_string()),
                updated_at: Some(Value::Int(100)),
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-empty thread id"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_thread_metadata_batch_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_thread_metadata_batch_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Thread {id: 'thread_1', metadata: '{}', updated_at: 1})")
            .unwrap();
        db.query("CREATE (:Thread {id: 'thread_2', metadata: '{}', updated_at: 1})")
            .unwrap();
        let batch_count_before_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.update_knowledge_thread_metadata_batch(&KnowledgeThreadMetadataBatchRequest {
            updates: vec![
                KnowledgeThreadMetadataUpdate {
                    thread_id: "thread_1".to_string(),
                    metadata: Value::String("{\"summary\":\"ready\"}".to_string()),
                    updated_at: Some(Value::Int(100)),
                },
                KnowledgeThreadMetadataUpdate {
                    thread_id: "thread_2".to_string(),
                    metadata: Value::String("{\"summary\":\"metadata-only\"}".to_string()),
                    updated_at: None,
                },
            ],
        })
        .unwrap();
        let batch_count_after_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db
            .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
                entities: vec![
                    KnowledgeEntityRequest {
                        label: "Thread".to_string(),
                        external_id: "thread_1".to_string(),
                    },
                    KnowledgeEntityRequest {
                        label: "Thread".to_string(),
                        external_id: "thread_2".to_string(),
                    },
                ],
                property_names: vec!["metadata".to_string(), "updated_at".to_string()],
            })
            .unwrap();
        assert_eq!(
            rows.rows[0].properties.get("metadata"),
            Some(&Some(Value::String("{\"summary\":\"ready\"}".to_string())))
        );
        assert_eq!(
            rows.rows[0].properties.get("updated_at"),
            Some(&Some(Value::Int(100)))
        );
        assert_eq!(
            rows.rows[1].properties.get("metadata"),
            Some(&Some(Value::String(
                "{\"summary\":\"metadata-only\"}".to_string()
            )))
        );
        assert_eq!(
            rows.rows[1].properties.get("updated_at"),
            Some(&Some(Value::Int(1)))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn updates_thread_message_count_batch_with_preserve_newer_timestamp() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'thread_1', message_count: 1, updated_at: 200})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'thread_2', message_count: 1, updated_at: 100})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'thread_3', message_count: 1, updated_at: 10})")
        .unwrap();

    let output = db
        .update_knowledge_thread_message_count_batch(&KnowledgeThreadMessageCountBatchRequest {
            updates: vec![
                KnowledgeThreadMessageCountUpdate {
                    thread_id: "thread_1".to_string(),
                    message_count: 7,
                    updated_at: Some(Value::Int(150)),
                    preserve_newer_existing_updated_at: true,
                },
                KnowledgeThreadMessageCountUpdate {
                    thread_id: "thread_2".to_string(),
                    message_count: 8,
                    updated_at: Some(Value::Int(150)),
                    preserve_newer_existing_updated_at: true,
                },
                KnowledgeThreadMessageCountUpdate {
                    thread_id: "thread_3".to_string(),
                    message_count: 9,
                    updated_at: None,
                    preserve_newer_existing_updated_at: false,
                },
                KnowledgeThreadMessageCountUpdate {
                    thread_id: "thread_2".to_string(),
                    message_count: 10,
                    updated_at: Some(Value::Int(300)),
                    preserve_newer_existing_updated_at: false,
                },
                KnowledgeThreadMessageCountUpdate {
                    thread_id: "missing".to_string(),
                    message_count: 1,
                    updated_at: Some(Value::Int(100)),
                    preserve_newer_existing_updated_at: false,
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 3);
    assert_eq!(output.graph_commit_epoch_after, 4);
    assert_eq!(output.rows.len(), 5);
    assert_eq!(output.matched_count, 3);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.updated_count, 3);
    assert_eq!(output.updated_at_changed_count, 1);
    assert_eq!(output.updated_property_count, 4);
    assert_eq!(output.rows[0].updated_property_count, 1);
    assert!(!output.rows[0].updated_at_changed);
    assert_eq!(output.rows[1].updated_property_count, 2);
    assert!(output.rows[1].updated_at_changed);
    assert_eq!(output.rows[2].updated_property_count, 1);
    assert!(output.rows[3].duplicate);
    assert!(!output.rows[4].matched);

    let rows = db
        .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Thread".to_string(),
                    external_id: "thread_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Thread".to_string(),
                    external_id: "thread_2".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Thread".to_string(),
                    external_id: "thread_3".to_string(),
                },
            ],
            property_names: vec!["message_count".to_string(), "updated_at".to_string()],
        })
        .unwrap();
    assert_eq!(
        rows.rows[0].properties.get("message_count"),
        Some(&Some(Value::Int(7)))
    );
    assert_eq!(
        rows.rows[0].properties.get("updated_at"),
        Some(&Some(Value::Int(200)))
    );
    assert_eq!(
        rows.rows[1].properties.get("message_count"),
        Some(&Some(Value::Int(8)))
    );
    assert_eq!(
        rows.rows[1].properties.get("updated_at"),
        Some(&Some(Value::Int(150)))
    );
    assert_eq!(
        rows.rows[2].properties.get("message_count"),
        Some(&Some(Value::Int(9)))
    );
    assert_eq!(
        rows.rows[2].properties.get("updated_at"),
        Some(&Some(Value::Int(10)))
    );
}

#[test]
fn thread_message_count_batch_rejects_negative_count_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'thread_1', message_count: 1})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .update_knowledge_thread_message_count_batch(&KnowledgeThreadMessageCountBatchRequest {
            updates: vec![KnowledgeThreadMessageCountUpdate {
                thread_id: "thread_1".to_string(),
                message_count: -1,
                updated_at: Some(Value::Int(100)),
                preserve_newer_existing_updated_at: false,
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-negative message count"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_thread_message_count_batch_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_thread_message_count_batch_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Thread {id: 'thread_1', message_count: 1, updated_at: 200})")
            .unwrap();
        db.query("CREATE (:Thread {id: 'thread_2', message_count: 1, updated_at: 100})")
            .unwrap();
        let batch_count_before_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.update_knowledge_thread_message_count_batch(&KnowledgeThreadMessageCountBatchRequest {
            updates: vec![
                KnowledgeThreadMessageCountUpdate {
                    thread_id: "thread_1".to_string(),
                    message_count: 7,
                    updated_at: Some(Value::Int(150)),
                    preserve_newer_existing_updated_at: true,
                },
                KnowledgeThreadMessageCountUpdate {
                    thread_id: "thread_2".to_string(),
                    message_count: 8,
                    updated_at: Some(Value::Int(150)),
                    preserve_newer_existing_updated_at: true,
                },
            ],
        })
        .unwrap();
        let batch_count_after_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db
            .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
                entities: vec![
                    KnowledgeEntityRequest {
                        label: "Thread".to_string(),
                        external_id: "thread_1".to_string(),
                    },
                    KnowledgeEntityRequest {
                        label: "Thread".to_string(),
                        external_id: "thread_2".to_string(),
                    },
                ],
                property_names: vec!["message_count".to_string(), "updated_at".to_string()],
            })
            .unwrap();
        assert_eq!(
            rows.rows[0].properties.get("message_count"),
            Some(&Some(Value::Int(7)))
        );
        assert_eq!(
            rows.rows[0].properties.get("updated_at"),
            Some(&Some(Value::Int(200)))
        );
        assert_eq!(
            rows.rows[1].properties.get("message_count"),
            Some(&Some(Value::Int(8)))
        );
        assert_eq!(
            rows.rows[1].properties.get("updated_at"),
            Some(&Some(Value::Int(150)))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn deletes_threads_for_nowledge_compensation_delete_shape() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'thread_1'})-[:CONTAINS]->(:Message {id: 'message_1'})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'thread_2'})-[:COMPACTS_TO]->(:Memory {id: 'memory_1'})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'thread_3'})").unwrap();
    db.query("CREATE (:Thread {title: 'Idless Thread'})")
        .unwrap();
    let idless = db
        .query("MATCH (t:Thread) WHERE t.title = 'Idless Thread' RETURN id(t) AS id")
        .unwrap();
    let idless_thread_id = match idless.rows[0].get("id").unwrap() {
        Value::Int(id) => id.to_string(),
        other => panic!("expected projected id int, got {other:?}"),
    };

    let output = db
        .delete_knowledge_threads(&KnowledgeThreadDeleteBatchRequest {
            thread_ids: vec![
                "thread_1".to_string(),
                "missing".to_string(),
                idless_thread_id,
                "thread_2".to_string(),
                "thread_1".to_string(),
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 4);
    assert_eq!(output.graph_commit_epoch_after, 5);
    assert_eq!(output.rows.len(), 5);
    assert_eq!(output.matched_count, 3);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.non_writable_count, 1);
    assert_eq!(output.deleted_node_count, 2);
    assert!(output.rows[0].matched);
    assert!(!output.rows[1].matched);
    assert!(output.rows[2].non_writable);
    assert!(output.rows[3].matched);
    assert!(output.rows[4].matched);

    for thread_id in ["thread_1", "thread_2"] {
        assert!(!test_thread_exists(&db, thread_id));
    }
    assert!(test_thread_exists(&db, "thread_3"));
    assert_eq!(
        db.query("MATCH (t:Thread)-[r:CONTAINS]->(m:Message) RETURN count(r) AS relationships")
            .unwrap()
            .rows[0]
            .get("relationships"),
        Some(&Value::Int(0))
    );
    assert_eq!(
        db.query("MATCH (t:Thread)-[r:COMPACTS_TO]->(m:Memory) RETURN count(r) AS relationships")
            .unwrap()
            .rows[0]
            .get("relationships"),
        Some(&Value::Int(0))
    );
    assert_eq!(
        db.query("MATCH (m:Message) RETURN count(m) AS messages")
            .unwrap()
            .rows[0]
            .get("messages"),
        Some(&Value::Int(1))
    );
    assert_eq!(
        db.query("MATCH (m:Memory) RETURN count(m) AS memories")
            .unwrap()
            .rows[0]
            .get("memories"),
        Some(&Value::Int(1))
    );
}

#[test]
fn thread_delete_rejects_empty_thread_id_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'thread_1'})").unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .delete_knowledge_threads(&KnowledgeThreadDeleteBatchRequest {
            thread_ids: vec![String::new()],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-empty thread id"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_thread_delete_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_thread_delete_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Thread {id: 'thread_1'})-[:CONTAINS]->(:Message {id: 'message_1'})")
            .unwrap();
        db.query("CREATE (:Thread {id: 'thread_2'})").unwrap();
        let batch_count_before_delete = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.delete_knowledge_threads(&KnowledgeThreadDeleteBatchRequest {
            thread_ids: vec!["thread_1".to_string(), "thread_2".to_string()],
        })
        .unwrap();
        let batch_count_after_delete = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_delete, batch_count_before_delete + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("delete_node"));
    {
        let db = Database::open(&path).unwrap();
        for thread_id in ["thread_1", "thread_2"] {
            assert!(!test_thread_exists(&db, thread_id));
        }
        assert!(db
            .query_entity_via_cypher(&KnowledgeEntityRequest {
                label: "Message".to_string(),
                external_id: "message_1".to_string(),
            })
            .unwrap()
            .entity
            .is_some());
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn deletes_thread_messages_for_nowledge_cleanup_shape() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'thread_1'})-[:CONTAINS {order_index: 1}]->(:Message {id: 'message_1', order_index: 1})")
        .unwrap();
    db.query("MATCH (t:Thread {id: 'thread_1'}), (m:Message {id: 'message_1'}) CREATE (t)-[:CONTAINS {order_index: 2}]->(m)")
        .unwrap();
    db.query("CREATE (:Message {id: 'message_2', order_index: 3})")
        .unwrap();
    db.query("MATCH (t:Thread {id: 'thread_1'}), (m:Message {id: 'message_2'}) CREATE (t)-[:CONTAINS {order_index: 3}]->(m)")
        .unwrap();
    db.query("CREATE (:Thread {id: 'thread_2'})-[:CONTAINS]->(:Message {id: 'message_other'})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let output = db
        .delete_knowledge_thread_messages(&KnowledgeThreadMessageDeleteRequest {
            thread_id: "thread_1".to_string(),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, graph_commit_epoch_before);
    assert_eq!(
        output.graph_commit_epoch_after,
        graph_commit_epoch_before + 1
    );
    assert!(output.found_thread);
    assert!(output.thread_node_id.is_some());
    assert_eq!(output.matched_relationship_count, 3);
    assert_eq!(output.deleted_message_count, 2);
    assert!(test_thread_exists(&db, "thread_1"));
    assert_eq!(
        db.query("MATCH (m:Message) RETURN m.id AS id ORDER BY id")
            .unwrap()
            .rows
            .iter()
            .map(|row| row.get("id").unwrap().clone())
            .collect::<Vec<_>>(),
        vec![Value::String("message_other".to_string())]
    );
    assert_eq!(test_thread_message_count(&db, "thread_1"), 0);

    let missing = db
        .delete_knowledge_thread_messages(&KnowledgeThreadMessageDeleteRequest {
            thread_id: "missing".to_string(),
        })
        .unwrap();
    assert!(!missing.found_thread);
    assert_eq!(missing.deleted_message_count, 0);
    assert_eq!(
        missing.graph_commit_epoch_after,
        output.graph_commit_epoch_after
    );

    let empty = db
        .delete_knowledge_thread_messages(&KnowledgeThreadMessageDeleteRequest {
            thread_id: "thread_1".to_string(),
        })
        .unwrap();
    assert!(empty.found_thread);
    assert_eq!(empty.matched_relationship_count, 0);
    assert_eq!(empty.deleted_message_count, 0);
    assert_eq!(
        empty.graph_commit_epoch_after,
        output.graph_commit_epoch_after
    );
}

#[test]
fn thread_message_delete_rejects_empty_thread_id_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'thread_1'})-[:CONTAINS]->(:Message {id: 'message_1'})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .delete_knowledge_thread_messages(&KnowledgeThreadMessageDeleteRequest {
            thread_id: String::new(),
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-empty thread id"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_thread_message_delete_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_thread_message_delete_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Thread {id: 'thread_1'})-[:CONTAINS]->(:Message {id: 'message_1'})")
            .unwrap();
        db.query("CREATE (:Message {id: 'message_2'})").unwrap();
        db.query("MATCH (t:Thread {id: 'thread_1'}), (m:Message {id: 'message_2'}) CREATE (t)-[:CONTAINS]->(m)")
            .unwrap();
        let batch_count_before_delete = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.delete_knowledge_thread_messages(&KnowledgeThreadMessageDeleteRequest {
            thread_id: "thread_1".to_string(),
        })
        .unwrap();
        let batch_count_after_delete = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_delete, batch_count_before_delete + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("delete_node"));
    {
        let mut db = Database::open(&path).unwrap();
        assert!(test_thread_exists(&db, "thread_1"));
        assert_eq!(
            db.query("MATCH (m:Message) RETURN count(m) AS messages")
                .unwrap()
                .rows[0]
                .get("messages"),
            Some(&Value::Int(0))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn creates_thread_compaction_link_for_nowledge_distill_shape() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'thread_1', thread_id: 'logical_1'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'Memory One', importance: 1.0})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2'})").unwrap();
    db.query("CREATE (:Thread {title: 'Idless Thread'})")
        .unwrap();
    let idless = db
        .query("MATCH (t:Thread) WHERE t.title = 'Idless Thread' RETURN id(t) AS id")
        .unwrap();
    let idless_thread_id = match idless.rows[0].get("id").unwrap() {
        Value::Int(id) => id.to_string(),
        other => panic!("expected projected id int, got {other:?}"),
    };
    let graph_commit_epoch_before = db.store.commit_epoch();

    let created = db
        .create_knowledge_thread_compaction_link(&KnowledgeThreadCompactionLinkRequest {
            thread_id: "thread_1".to_string(),
            memory_id: "memory_1".to_string(),
            compaction_method: "manual_distillation".to_string(),
            created_at: Value::Int(1_700_000_070),
            properties: Value::String("{\"mode\":\"manual\"}".to_string()),
        })
        .unwrap();
    assert_eq!(created.graph_commit_epoch_before, graph_commit_epoch_before);
    assert_eq!(
        created.graph_commit_epoch_after,
        graph_commit_epoch_before + 1
    );
    assert!(created.matched);
    assert!(!created.missing_endpoint);
    assert!(!created.non_writable);
    assert!(created.thread_node_id.is_some());
    assert!(created.memory_node_id.is_some());
    assert_eq!(created.created_relationship_count, 1);

    let compacted = test_thread_compaction_links(&db, "thread_1");
    assert_eq!(compacted.rows.len(), 1);
    assert_eq!(
        compacted.rows[0].get("memory_id"),
        Some(&Value::String("memory_1".to_string()))
    );
    assert_eq!(
        compacted.rows[0].get("compaction_method"),
        Some(&Value::String("manual_distillation".to_string()))
    );
    assert_eq!(
        compacted.rows[0].get("created_at"),
        Some(&Value::Int(1_700_000_070))
    );
    assert_eq!(
        compacted.rows[0].get("properties"),
        Some(&Value::String("{\"mode\":\"manual\"}".to_string()))
    );

    let missing = db
        .create_knowledge_thread_compaction_link(&KnowledgeThreadCompactionLinkRequest {
            thread_id: "thread_1".to_string(),
            memory_id: "missing".to_string(),
            compaction_method: "manual_distillation".to_string(),
            created_at: Value::Int(1),
            properties: Value::String("{}".to_string()),
        })
        .unwrap();
    assert!(!missing.matched);
    assert!(missing.missing_endpoint);
    assert!(!missing.non_writable);
    assert_eq!(missing.created_relationship_count, 0);
    assert_eq!(
        missing.graph_commit_epoch_after,
        created.graph_commit_epoch_after
    );

    let non_writable = db
        .create_knowledge_thread_compaction_link(&KnowledgeThreadCompactionLinkRequest {
            thread_id: idless_thread_id,
            memory_id: "memory_2".to_string(),
            compaction_method: "manual_distillation".to_string(),
            created_at: Value::Int(2),
            properties: Value::String("{}".to_string()),
        })
        .unwrap();
    assert!(!non_writable.matched);
    assert!(!non_writable.missing_endpoint);
    assert!(non_writable.non_writable);
    assert_eq!(non_writable.created_relationship_count, 0);
    assert_eq!(
        non_writable.graph_commit_epoch_after,
        created.graph_commit_epoch_after
    );
}

#[test]
fn thread_compaction_link_rejects_empty_fields_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'thread_1'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let empty_thread = db
        .create_knowledge_thread_compaction_link(&KnowledgeThreadCompactionLinkRequest {
            thread_id: String::new(),
            memory_id: "memory_1".to_string(),
            compaction_method: "manual_distillation".to_string(),
            created_at: Value::Int(1),
            properties: Value::String("{}".to_string()),
        })
        .unwrap_err();
    assert!(empty_thread.to_string().contains("non-empty thread id"));

    let empty_memory = db
        .create_knowledge_thread_compaction_link(&KnowledgeThreadCompactionLinkRequest {
            thread_id: "thread_1".to_string(),
            memory_id: String::new(),
            compaction_method: "manual_distillation".to_string(),
            created_at: Value::Int(1),
            properties: Value::String("{}".to_string()),
        })
        .unwrap_err();
    assert!(empty_memory.to_string().contains("non-empty memory id"));

    let empty_method = db
        .create_knowledge_thread_compaction_link(&KnowledgeThreadCompactionLinkRequest {
            thread_id: "thread_1".to_string(),
            memory_id: "memory_1".to_string(),
            compaction_method: String::new(),
            created_at: Value::Int(1),
            properties: Value::String("{}".to_string()),
        })
        .unwrap_err();
    assert!(empty_method
        .to_string()
        .contains("non-empty compaction method"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_thread_compaction_link_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_thread_compaction_link_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Thread {id: 'thread_1'})").unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
        let batch_count_before_link = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.create_knowledge_thread_compaction_link(&KnowledgeThreadCompactionLinkRequest {
            thread_id: "thread_1".to_string(),
            memory_id: "memory_1".to_string(),
            compaction_method: "manual_distillation".to_string(),
            created_at: Value::Int(10),
            properties: Value::String("{\"mode\":\"manual\"}".to_string()),
        })
        .unwrap();
        let batch_count_after_link = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_link, batch_count_before_link + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("create_rel"));
    {
        let db = Database::open(&path).unwrap();
        let output = test_thread_compaction_links(&db, "thread_1");
        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("memory_id"),
            Some(&Value::String("memory_1".to_string()))
        );
        assert_eq!(
            output.rows[0].get("compaction_method"),
            Some(&Value::String("manual_distillation".to_string()))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn deletes_thread_identities_for_nowledge_compensation_and_cascade_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:ThreadIdentity {id: 'identity_compensation', thread_node_id: 'thread_a', thread_id: 'logical_a'})")
        .unwrap();
    db.query("CREATE (:ThreadIdentity {id: 'identity_public', thread_node_id: 'thread_other_1', thread_id: 'logical_public'})")
        .unwrap();
    db.query("CREATE (:ThreadIdentity {id: 'identity_input', thread_node_id: 'thread_other_2', thread_id: 'logical_input'})")
        .unwrap();
    db.query("CREATE (:ThreadIdentity {id: 'identity_by_node', thread_node_id: 'thread_uuid', thread_id: 'logical_node'})")
        .unwrap();
    db.query("CREATE (:ThreadIdentity {id: 'identity_duplicate', thread_node_id: 'thread_uuid', thread_id: 'logical_duplicate'})")
        .unwrap();
    db.query("CREATE (:ThreadIdentity {id: 'identity_survivor', thread_node_id: 'survivor_uuid', thread_id: 'logical_survivor'})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let compensation = db
        .delete_knowledge_thread_identities(&KnowledgeThreadIdentityDeleteRequest {
            identity_key: Some("identity_compensation".to_string()),
            cascade_keys: None,
        })
        .unwrap();
    assert_eq!(
        compensation.graph_commit_epoch_before,
        graph_commit_epoch_before
    );
    assert_eq!(
        compensation.graph_commit_epoch_after,
        graph_commit_epoch_before + 1
    );
    assert_eq!(compensation.matched_identity_count, 1);
    assert_eq!(compensation.deleted_identity_count, 1);
    assert_eq!(compensation.deleted_node_ids.len(), 1);

    let cascade = db
        .delete_knowledge_thread_identities(&KnowledgeThreadIdentityDeleteRequest {
            identity_key: None,
            cascade_keys: Some(KnowledgeThreadIdentityCascadeDeleteKeys {
                public_thread_id: "identity_public".to_string(),
                input_thread_id: "identity_input".to_string(),
                thread_uuid: "thread_uuid".to_string(),
            }),
        })
        .unwrap();
    assert_eq!(
        cascade.graph_commit_epoch_before,
        compensation.graph_commit_epoch_after
    );
    assert_eq!(
        cascade.graph_commit_epoch_after,
        compensation.graph_commit_epoch_after + 1
    );
    assert_eq!(cascade.matched_identity_count, 4);
    assert_eq!(cascade.deleted_identity_count, 4);
    assert_eq!(cascade.deleted_node_ids.len(), 4);

    for identity_key in [
        "identity_compensation",
        "identity_public",
        "identity_input",
        "identity_by_node",
        "identity_duplicate",
    ] {
        assert!(!test_thread_identity_exists(&db, identity_key));
    }
    assert!(test_thread_identity_exists(&db, "identity_survivor"));

    let missing = db
        .delete_knowledge_thread_identities(&KnowledgeThreadIdentityDeleteRequest {
            identity_key: Some("missing".to_string()),
            cascade_keys: None,
        })
        .unwrap();
    assert_eq!(missing.matched_identity_count, 0);
    assert_eq!(missing.deleted_identity_count, 0);
    assert_eq!(
        missing.graph_commit_epoch_after,
        cascade.graph_commit_epoch_after
    );
}

#[test]
fn thread_identity_delete_rejects_invalid_modes_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:ThreadIdentity {id: 'identity_1'})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let empty_identity = db
        .delete_knowledge_thread_identities(&KnowledgeThreadIdentityDeleteRequest {
            identity_key: Some(String::new()),
            cascade_keys: None,
        })
        .unwrap_err();
    assert!(empty_identity
        .to_string()
        .contains("non-empty identity key"));

    let empty_cascade = db
        .delete_knowledge_thread_identities(&KnowledgeThreadIdentityDeleteRequest {
            identity_key: None,
            cascade_keys: Some(KnowledgeThreadIdentityCascadeDeleteKeys {
                public_thread_id: "public".to_string(),
                input_thread_id: String::new(),
                thread_uuid: "thread".to_string(),
            }),
        })
        .unwrap_err();
    assert!(empty_cascade.to_string().contains("non-empty cascade keys"));

    let no_mode = db
        .delete_knowledge_thread_identities(&KnowledgeThreadIdentityDeleteRequest {
            identity_key: None,
            cascade_keys: None,
        })
        .unwrap_err();
    assert!(no_mode.to_string().contains("exactly one delete mode"));

    let both_modes = db
        .delete_knowledge_thread_identities(&KnowledgeThreadIdentityDeleteRequest {
            identity_key: Some("identity_1".to_string()),
            cascade_keys: Some(KnowledgeThreadIdentityCascadeDeleteKeys {
                public_thread_id: "public".to_string(),
                input_thread_id: "input".to_string(),
                thread_uuid: "thread".to_string(),
            }),
        })
        .unwrap_err();
    assert!(both_modes.to_string().contains("exactly one delete mode"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_thread_identity_delete_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_thread_identity_delete_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:ThreadIdentity {id: 'identity_public', thread_node_id: 'thread_other', thread_id: 'logical_public'})")
            .unwrap();
        db.query("CREATE (:ThreadIdentity {id: 'identity_by_node', thread_node_id: 'thread_uuid', thread_id: 'logical_node'})")
            .unwrap();
        let batch_count_before_delete = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.delete_knowledge_thread_identities(&KnowledgeThreadIdentityDeleteRequest {
            identity_key: None,
            cascade_keys: Some(KnowledgeThreadIdentityCascadeDeleteKeys {
                public_thread_id: "identity_public".to_string(),
                input_thread_id: "missing_input".to_string(),
                thread_uuid: "thread_uuid".to_string(),
            }),
        })
        .unwrap();
        let batch_count_after_delete = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_delete, batch_count_before_delete + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("delete_node"));
    {
        let db = Database::open(&path).unwrap();
        for identity_key in ["identity_public", "identity_by_node"] {
            assert!(!test_thread_identity_exists(&db, identity_key));
        }
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn updates_label_lifecycle_batch_for_nowledge_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:Label {id: 'label_meta', name: 'Meta', metadata: '{}', updated_at: 1})")
        .unwrap();
    db.query("CREATE (:Label {id: 'label_canonical', name: 'Canonical'})")
        .unwrap();
    db.query(
        "CREATE (:Label {id: 'label_rename', name: 'Old', canonical_name: 'old', updated_at: 1})",
    )
    .unwrap();

    let output = db
        .update_knowledge_label_lifecycle_batch(&KnowledgeLabelLifecycleBatchRequest {
            updates: vec![
                KnowledgeLabelLifecycleUpdate {
                    label_id: "label_meta".to_string(),
                    name: None,
                    canonical_name: None,
                    metadata: Some(Value::String("{\"owner\":\"mem\"}".to_string())),
                    updated_at: Some(Value::Int(100)),
                },
                KnowledgeLabelLifecycleUpdate {
                    label_id: "label_canonical".to_string(),
                    name: None,
                    canonical_name: Some("canonical-label".to_string()),
                    metadata: None,
                    updated_at: None,
                },
                KnowledgeLabelLifecycleUpdate {
                    label_id: "label_rename".to_string(),
                    name: Some("Renamed".to_string()),
                    canonical_name: Some("renamed".to_string()),
                    metadata: None,
                    updated_at: Some(Value::Int(200)),
                },
                KnowledgeLabelLifecycleUpdate {
                    label_id: "label_rename".to_string(),
                    name: Some("Duplicate".to_string()),
                    canonical_name: Some("duplicate".to_string()),
                    metadata: None,
                    updated_at: Some(Value::Int(300)),
                },
                KnowledgeLabelLifecycleUpdate {
                    label_id: "missing".to_string(),
                    name: None,
                    canonical_name: Some("missing".to_string()),
                    metadata: None,
                    updated_at: None,
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 3);
    assert_eq!(output.graph_commit_epoch_after, 4);
    assert_eq!(output.rows.len(), 5);
    assert_eq!(output.matched_count, 3);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.updated_count, 3);
    assert_eq!(output.updated_property_count, 6);
    assert_eq!(output.rows[0].updated_property_count, 2);
    assert_eq!(output.rows[1].updated_property_count, 1);
    assert_eq!(output.rows[2].updated_property_count, 3);
    assert!(output.rows[3].duplicate);
    assert!(!output.rows[4].matched);

    let rows = db
        .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Label".to_string(),
                    external_id: "label_meta".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Label".to_string(),
                    external_id: "label_canonical".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Label".to_string(),
                    external_id: "label_rename".to_string(),
                },
            ],
            property_names: vec![
                "name".to_string(),
                "canonical_name".to_string(),
                "metadata".to_string(),
                "updated_at".to_string(),
            ],
        })
        .unwrap();
    assert_eq!(
        rows.rows[0].properties.get("metadata"),
        Some(&Some(Value::String("{\"owner\":\"mem\"}".to_string())))
    );
    assert_eq!(
        rows.rows[0].properties.get("updated_at"),
        Some(&Some(Value::Int(100)))
    );
    assert_eq!(
        rows.rows[1].properties.get("canonical_name"),
        Some(&Some(Value::String("canonical-label".to_string())))
    );
    assert_eq!(
        rows.rows[2].properties.get("name"),
        Some(&Some(Value::String("Renamed".to_string())))
    );
    assert_eq!(
        rows.rows[2].properties.get("canonical_name"),
        Some(&Some(Value::String("renamed".to_string())))
    );
    assert_eq!(
        rows.rows[2].properties.get("updated_at"),
        Some(&Some(Value::Int(200)))
    );
}

#[test]
fn label_lifecycle_batch_rejects_empty_canonical_name_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Label {id: 'label_1', name: 'Label'})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .update_knowledge_label_lifecycle_batch(&KnowledgeLabelLifecycleBatchRequest {
            updates: vec![KnowledgeLabelLifecycleUpdate {
                label_id: "label_1".to_string(),
                name: None,
                canonical_name: Some(String::new()),
                metadata: None,
                updated_at: None,
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-empty canonical name"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_label_lifecycle_batch_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_label_lifecycle_batch_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Label {id: 'label_meta', name: 'Meta', metadata: '{}', updated_at: 1})")
            .unwrap();
        db.query("CREATE (:Label {id: 'label_rename', name: 'Old', canonical_name: 'old', updated_at: 1})")
            .unwrap();
        let batch_count_before_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.update_knowledge_label_lifecycle_batch(&KnowledgeLabelLifecycleBatchRequest {
            updates: vec![
                KnowledgeLabelLifecycleUpdate {
                    label_id: "label_meta".to_string(),
                    name: None,
                    canonical_name: None,
                    metadata: Some(Value::String("{\"owner\":\"mem\"}".to_string())),
                    updated_at: Some(Value::Int(100)),
                },
                KnowledgeLabelLifecycleUpdate {
                    label_id: "label_rename".to_string(),
                    name: Some("Renamed".to_string()),
                    canonical_name: Some("renamed".to_string()),
                    metadata: None,
                    updated_at: Some(Value::Int(200)),
                },
            ],
        })
        .unwrap();
        let batch_count_after_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db
            .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
                entities: vec![
                    KnowledgeEntityRequest {
                        label: "Label".to_string(),
                        external_id: "label_meta".to_string(),
                    },
                    KnowledgeEntityRequest {
                        label: "Label".to_string(),
                        external_id: "label_rename".to_string(),
                    },
                ],
                property_names: vec![
                    "name".to_string(),
                    "canonical_name".to_string(),
                    "metadata".to_string(),
                    "updated_at".to_string(),
                ],
            })
            .unwrap();
        assert_eq!(
            rows.rows[0].properties.get("metadata"),
            Some(&Some(Value::String("{\"owner\":\"mem\"}".to_string())))
        );
        assert_eq!(
            rows.rows[0].properties.get("updated_at"),
            Some(&Some(Value::Int(100)))
        );
        assert_eq!(
            rows.rows[1].properties.get("name"),
            Some(&Some(Value::String("Renamed".to_string())))
        );
        assert_eq!(
            rows.rows[1].properties.get("canonical_name"),
            Some(&Some(Value::String("renamed".to_string())))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn projects_entity_labels_for_nowledge_growth() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'projected_label_memory_a'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'projected_label_memory_b'})")
        .unwrap();
    db.query("CREATE (:Source {id: 'projected_label_source_a'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'label_beta', name: 'Beta', canonical_name: 'beta', color: '#00f', future_label_field: 'label-b'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'label_zeta', name: 'Zeta', canonical_name: 'zeta', color: '#0f0', future_label_field: 'label-z'})")
        .unwrap();
    db.query(
        "CREATE (:Label {id: 'label_source', name: 'Source', future_label_field: 'label-source'})",
    )
    .unwrap();
    db.query("MATCH (m:Memory {id: 'projected_label_memory_a'}), (l:Label {id: 'label_beta'}) CREATE (m)-[:HAS_LABEL {assigned_by: 'system', weight: 2, future_edge_field: 'edge-b'}]->(l)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'projected_label_memory_a'}), (l:Label {id: 'label_zeta'}) CREATE (m)-[:HAS_LABEL {assigned_by: 'manual', future_edge_field: 'edge-z'}]->(l)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'projected_label_memory_b'}), (l:Label {id: 'label_zeta'}) CREATE (m)-[:HAS_LABEL {assigned_by: 'system', future_edge_field: 'edge-b2'}]->(l)")
        .unwrap();
    db.query("MATCH (s:Source {id: 'projected_label_source_a'}), (l:Label {id: 'label_source'}) CREATE (s)-[:HAS_LABEL {assigned_by: 'source', future_edge_field: 'edge-source'}]->(l)")
        .unwrap();
    db.query("CREATE (:Label {id: 'label_alpha', name: 'Alpha', canonical_name: 'alpha', color: '#f00', future_label_field: 'label-a'})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'projected_label_memory_a'}), (l:Label {id: 'label_alpha'}) CREATE (m)-[:HAS_LABEL {assigned_by: 'later', weight: 9, future_edge_field: 'edge-a'}]->(l)")
        .unwrap();

    let projected_request = KnowledgeEntityLabelProjectedListRequest {
        list: KnowledgeEntityLabelListRequest {
            entity_label: "Memory".to_string(),
            external_ids: vec![
                "projected_label_memory_a".to_string(),
                "missing_projected_label_memory".to_string(),
                "projected_label_memory_b".to_string(),
            ],
            limit_per_entity: 1,
        },
        label_property_names: vec![
            "name".to_string(),
            "future_label_field".to_string(),
            "canonical_name".to_string(),
            "name".to_string(),
        ],
        relationship_property_names: vec![
            "assigned_by".to_string(),
            "future_edge_field".to_string(),
        ],
    };
    let projected = db
        .query_entity_label_projected_list_via_cypher(&projected_request)
        .unwrap();
    assert_eq!(projected.found_entity_count, 2);
    assert_eq!(projected.missing_entity_count, 1);
    assert_eq!(projected.label_count, 2);
    assert!(projected.groups[0].found);
    assert_eq!(projected.groups[0].returned_count, 1);
    assert_eq!(
        projected.groups[0].labels[0].label_id.as_deref(),
        Some("label_alpha")
    );
    assert_eq!(
        projected.groups[0].labels[0]
            .label_properties
            .get("future_label_field"),
        Some(&Value::String("label-a".to_string()))
    );
    assert!(!projected.groups[0].labels[0]
        .label_properties
        .contains_key("color"));
    assert_eq!(
        projected.groups[0].labels[0]
            .relationship_properties
            .get("future_edge_field"),
        Some(&Value::String("edge-a".to_string()))
    );
    assert!(!projected.groups[0].labels[0]
        .relationship_properties
        .contains_key("weight"));
    assert!(!projected.groups[1].found);
    assert!(projected.groups[1].labels.is_empty());
    assert_eq!(
        projected.groups[2].labels[0].label_id.as_deref(),
        Some("label_zeta")
    );

    let stats = db.plan_cache_stats();
    let repeated_projected = db
        .query_entity_label_projected_list_via_cypher(&projected_request)
        .unwrap();
    assert_eq!(repeated_projected, projected);
    let repeated_stats = db.plan_cache_stats();
    assert_eq!(repeated_stats.entries, stats.entries);
    assert_eq!(repeated_stats.misses, stats.misses);
    assert_eq!(repeated_stats.hits, stats.hits + 3);

    let source_projected = db
        .query_entity_label_projected_list_via_cypher(&KnowledgeEntityLabelProjectedListRequest {
            list: KnowledgeEntityLabelListRequest {
                entity_label: "Source".to_string(),
                external_ids: vec!["projected_label_source_a".to_string()],
                limit_per_entity: 0,
            },
            label_property_names: vec!["future_label_field".to_string()],
            relationship_property_names: vec!["assigned_by".to_string()],
        })
        .unwrap();
    assert_eq!(source_projected.found_entity_count, 1);
    assert_eq!(source_projected.label_count, 1);
    assert_eq!(
        source_projected.groups[0].labels[0].label_id.as_deref(),
        Some("label_source")
    );
}

#[test]
fn entity_label_projected_read_rejects_empty_property_names_without_wal() {
    let path = unique_test_dir("entity_label_projected_empty_property_without_wal");
    let mut db = Database::open(&path).unwrap();
    db.query("CREATE (:Memory {id: 'projected_label_wal_memory'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'projected_label_wal_label'})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'projected_label_wal_memory'}), (l:Label {id: 'projected_label_wal_label'}) CREATE (m)-[:HAS_LABEL]->(l)")
        .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();
    let wal_before = read_test_wal(&path).unwrap();

    let label_property_error = db
        .query_entity_label_projected_list_via_cypher(&KnowledgeEntityLabelProjectedListRequest {
            list: KnowledgeEntityLabelListRequest {
                entity_label: "Memory".to_string(),
                external_ids: vec!["projected_label_wal_memory".to_string()],
                limit_per_entity: 10,
            },
            label_property_names: vec![String::new()],
            relationship_property_names: Vec::new(),
        })
        .unwrap_err();
    assert!(label_property_error
        .to_string()
        .contains("non-empty property names"));

    let relationship_property_error = db
        .query_entity_label_projected_list_via_cypher(&KnowledgeEntityLabelProjectedListRequest {
            list: KnowledgeEntityLabelListRequest {
                entity_label: "Memory".to_string(),
                external_ids: vec!["projected_label_wal_memory".to_string()],
                limit_per_entity: 10,
            },
            label_property_names: Vec::new(),
            relationship_property_names: vec![String::new()],
        })
        .unwrap_err();
    assert!(relationship_property_error
        .to_string()
        .contains("non-empty property names"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);
    assert_eq!(read_test_wal(&path).unwrap(), wal_before);
}

#[test]
fn transfers_label_memory_edges_for_nowledge_label_merge_shape() {
    let mut db = Database::new();
    db.query("CREATE (:Label {id: 'source_label'})").unwrap();
    db.query("CREATE (:Label {id: 'target_label'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory_create'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory_existing'})")
        .unwrap();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory_create'}), (l:Label {id: 'source_label'}) CREATE (m)-[:HAS_LABEL]->(l)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory_create'}), (l:Label {id: 'source_label'}) CREATE (m)-[:HAS_LABEL {duplicate: true}]->(l)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory_existing'}), (l:Label {id: 'source_label'}) CREATE (m)-[:HAS_LABEL]->(l)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory_existing'}), (l:Label {id: 'target_label'}) CREATE (m)-[:HAS_LABEL {assigned_by: 'existing'}]->(l)")
        .unwrap();
    let idless = db
        .query("MATCH (m:Memory) WHERE m.title = 'Idless memory' RETURN id(m) AS id")
        .unwrap();
    let idless_memory_node_id = match idless.rows[0].get("id").unwrap() {
        Value::Int(id) => NodeId(*id as u64),
        other => panic!("expected projected id int, got {other:?}"),
    };
    let source_label = db
        .query("MATCH (l:Label {id: 'source_label'}) RETURN id(l) AS id")
        .unwrap();
    let source_label_node_id = match source_label.rows[0].get("id").unwrap() {
        Value::Int(id) => NodeId(*id as u64),
        other => panic!("expected projected id int, got {other:?}"),
    };
    db.store
        .create_relationship(
            &mut db.catalog,
            idless_memory_node_id,
            source_label_node_id,
            "HAS_LABEL",
            BTreeMap::new(),
        )
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let output = db
        .transfer_knowledge_label_memory_edges(&KnowledgeLabelMemoryTransferRequest {
            source_label_id: "source_label".to_string(),
            target_label_id: "target_label".to_string(),
            created_at: Value::Int(100),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, graph_commit_epoch_before);
    assert_eq!(
        output.graph_commit_epoch_after,
        graph_commit_epoch_before + 1
    );
    assert!(output.found_source_label);
    assert!(output.found_target_label);
    assert_eq!(output.matched_memory_count, 3);
    assert_eq!(output.created_count, 1);
    assert_eq!(output.already_exists_count, 1);
    assert_eq!(output.non_writable_count, 1);
    assert!(output
        .rows
        .iter()
        .any(|row| row.memory_id.as_deref() == Some("memory_create") && row.created));
    assert!(output
        .rows
        .iter()
        .any(|row| row.memory_id.as_deref() == Some("memory_existing") && row.already_exists));
    assert!(output.rows.iter().any(|row| row.non_writable));

    let target_edges = db
        .query(
            "MATCH (:Memory)-[r:HAS_LABEL]->(:Label {id: 'target_label'}) RETURN COUNT(r) AS total",
        )
        .unwrap();
    assert_eq!(target_edges.rows[0].get("total"), Some(&Value::Int(2)));
    let created_props = db
        .query("MATCH (:Memory {id: 'memory_create'})-[r:HAS_LABEL]->(:Label {id: 'target_label'}) RETURN r.assigned_by AS assigned_by, r.created_at AS created_at, r.properties AS properties")
        .unwrap();
    assert_eq!(
        created_props.rows[0].get("assigned_by"),
        Some(&Value::String("label_merge".to_string()))
    );
    assert_eq!(
        created_props.rows[0].get("created_at"),
        Some(&Value::Int(100))
    );
    assert_eq!(
        created_props.rows[0].get("properties"),
        Some(&Value::String("{}".to_string()))
    );

    let missing = db
        .transfer_knowledge_label_memory_edges(&KnowledgeLabelMemoryTransferRequest {
            source_label_id: "missing".to_string(),
            target_label_id: "target_label".to_string(),
            created_at: Value::Int(200),
        })
        .unwrap();
    assert!(!missing.found_source_label);
    assert!(missing.found_target_label);
    assert_eq!(missing.matched_memory_count, 0);
    assert_eq!(
        missing.graph_commit_epoch_after,
        output.graph_commit_epoch_after
    );
}

#[test]
fn label_memory_transfer_rejects_empty_ids_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Label {id: 'source_label'})").unwrap();
    db.query("CREATE (:Label {id: 'target_label'})").unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let empty_source = db
        .transfer_knowledge_label_memory_edges(&KnowledgeLabelMemoryTransferRequest {
            source_label_id: String::new(),
            target_label_id: "target_label".to_string(),
            created_at: Value::Int(1),
        })
        .unwrap_err();
    assert!(empty_source
        .to_string()
        .contains("non-empty source label id"));

    let empty_target = db
        .transfer_knowledge_label_memory_edges(&KnowledgeLabelMemoryTransferRequest {
            source_label_id: "source_label".to_string(),
            target_label_id: String::new(),
            created_at: Value::Int(1),
        })
        .unwrap_err();
    assert!(empty_target
        .to_string()
        .contains("non-empty target label id"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_label_memory_transfer_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_label_memory_transfer_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Label {id: 'source_label'})").unwrap();
        db.query("CREATE (:Label {id: 'target_label'})").unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
        db.query("CREATE (:Memory {id: 'memory_2'})").unwrap();
        db.query("MATCH (m:Memory {id: 'memory_1'}), (l:Label {id: 'source_label'}) CREATE (m)-[:HAS_LABEL]->(l)")
            .unwrap();
        db.query("MATCH (m:Memory {id: 'memory_2'}), (l:Label {id: 'source_label'}) CREATE (m)-[:HAS_LABEL]->(l)")
            .unwrap();
        let batch_count_before_transfer =
            read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.transfer_knowledge_label_memory_edges(&KnowledgeLabelMemoryTransferRequest {
            source_label_id: "source_label".to_string(),
            target_label_id: "target_label".to_string(),
            created_at: Value::Int(10),
        })
        .unwrap();
        let batch_count_after_transfer = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_transfer, batch_count_before_transfer + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("create_rel"));
    {
        let mut db = Database::open(&path).unwrap();
        let target_edges = db
            .query("MATCH (:Memory)-[r:HAS_LABEL]->(:Label {id: 'target_label'}) RETURN COUNT(r) AS total")
            .unwrap();
        assert_eq!(target_edges.rows[0].get("total"), Some(&Value::Int(2)));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn transfers_memory_label_edges_for_nowledge_memory_label_carry_over_shape() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'older', space_id: 'default'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'newer', space_id: 'default'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'other_space', space_id: 'other'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'alpha'})").unwrap();
    db.query("CREATE (:Label {id: 'beta'})").unwrap();
    db.query(
        "MATCH (m:Memory {id: 'older'}), (l:Label {id: 'alpha'}) CREATE (m)-[:HAS_LABEL]->(l)",
    )
    .unwrap();
    db.query("MATCH (m:Memory {id: 'older'}), (l:Label {id: 'alpha'}) CREATE (m)-[:HAS_LABEL {duplicate: true}]->(l)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'older'}), (l:Label {id: 'beta'}) CREATE (m)-[:HAS_LABEL]->(l)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'newer'}), (l:Label {id: 'beta'}) CREATE (m)-[:HAS_LABEL {assigned_by: 'existing'}]->(l)")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let output = db
        .transfer_knowledge_memory_label_edges(&KnowledgeMemoryLabelTransferRequest {
            older_memory_id: "older".to_string(),
            newer_memory_id: "newer".to_string(),
            space_id: "default".to_string(),
            created_at: Value::Int(7100),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, graph_commit_epoch_before);
    assert_eq!(
        output.graph_commit_epoch_after,
        graph_commit_epoch_before + 1
    );
    assert!(output.found_older_memory);
    assert!(output.found_newer_memory);
    assert!(output.older_space_matches);
    assert!(output.newer_space_matches);
    assert_eq!(output.matched_label_count, 2);
    assert_eq!(output.created_count, 1);
    assert_eq!(output.already_exists_count, 1);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.duplicate_source_edge_count, 1);
    assert!(output
        .rows
        .iter()
        .any(|row| row.label_id.as_deref() == Some("alpha") && row.created));
    assert!(output
        .rows
        .iter()
        .any(|row| row.label_id.as_deref() == Some("beta") && row.already_exists));

    let alpha_props = db
        .query("MATCH (:Memory {id: 'newer'})-[r:HAS_LABEL]->(:Label {id: 'alpha'}) RETURN count(r) AS total, min(r.assigned_by) AS assigned_by, min(r.created_at) AS created_at, min(r.properties) AS properties")
        .unwrap();
    assert_eq!(alpha_props.rows[0].get("total"), Some(&Value::Int(1)));
    assert_eq!(
        alpha_props.rows[0].get("assigned_by"),
        Some(&Value::String("system".to_string()))
    );
    assert_eq!(
        alpha_props.rows[0].get("created_at"),
        Some(&Value::Int(7100))
    );
    assert_eq!(
        alpha_props.rows[0].get("properties"),
        Some(&Value::String("{}".to_string()))
    );

    let epoch_before_space_mismatch = db.store.commit_epoch();
    let mismatch = db
        .transfer_knowledge_memory_label_edges(&KnowledgeMemoryLabelTransferRequest {
            older_memory_id: "older".to_string(),
            newer_memory_id: "other_space".to_string(),
            space_id: "default".to_string(),
            created_at: Value::Int(7200),
        })
        .unwrap();
    assert!(mismatch.older_space_matches);
    assert!(!mismatch.newer_space_matches);
    assert_eq!(mismatch.matched_label_count, 0);
    assert_eq!(
        mismatch.graph_commit_epoch_after,
        epoch_before_space_mismatch
    );
    assert_eq!(db.store.commit_epoch(), epoch_before_space_mismatch);
}

#[test]
fn memory_label_transfer_rejects_empty_inputs_before_wal() {
    let path = unique_test_dir("memory_label_transfer_empty_inputs");
    let mut db = Database::open(&path).unwrap();
    db.query("CREATE (:Memory {id: 'older', space_id: 'default'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'newer', space_id: 'default'})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();
    let wal_before = read_test_wal(&path).unwrap();

    let empty_older = db
        .transfer_knowledge_memory_label_edges(&KnowledgeMemoryLabelTransferRequest {
            older_memory_id: String::new(),
            newer_memory_id: "newer".to_string(),
            space_id: "default".to_string(),
            created_at: Value::Int(1),
        })
        .unwrap_err();
    assert!(empty_older
        .to_string()
        .contains("non-empty older memory id"));

    let empty_newer = db
        .transfer_knowledge_memory_label_edges(&KnowledgeMemoryLabelTransferRequest {
            older_memory_id: "older".to_string(),
            newer_memory_id: String::new(),
            space_id: "default".to_string(),
            created_at: Value::Int(1),
        })
        .unwrap_err();
    assert!(empty_newer
        .to_string()
        .contains("non-empty newer memory id"));

    let empty_space = db
        .transfer_knowledge_memory_label_edges(&KnowledgeMemoryLabelTransferRequest {
            older_memory_id: "older".to_string(),
            newer_memory_id: "newer".to_string(),
            space_id: String::new(),
            created_at: Value::Int(1),
        })
        .unwrap_err();
    assert!(empty_space.to_string().contains("non-empty space id"));

    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
    assert_eq!(read_test_wal(&path).unwrap(), wal_before);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_memory_label_transfer_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_memory_label_transfer_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'older', space_id: 'default'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'newer', space_id: 'default'})")
            .unwrap();
        db.query("CREATE (:Label {id: 'alpha'})").unwrap();
        db.query("CREATE (:Label {id: 'beta'})").unwrap();
        db.query(
            "MATCH (m:Memory {id: 'older'}), (l:Label {id: 'alpha'}) CREATE (m)-[:HAS_LABEL]->(l)",
        )
        .unwrap();
        db.query(
            "MATCH (m:Memory {id: 'older'}), (l:Label {id: 'beta'}) CREATE (m)-[:HAS_LABEL]->(l)",
        )
        .unwrap();
        let batch_count_before_transfer =
            read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.transfer_knowledge_memory_label_edges(&KnowledgeMemoryLabelTransferRequest {
            older_memory_id: "older".to_string(),
            newer_memory_id: "newer".to_string(),
            space_id: "default".to_string(),
            created_at: Value::Int(7300),
        })
        .unwrap();
        let batch_count_after_transfer = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_transfer, batch_count_before_transfer + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("create_rel"));
    {
        let mut db = Database::open(&path).unwrap();
        let target_edges = db
            .query("MATCH (:Memory {id: 'newer'})-[r:HAS_LABEL]->(:Label) RETURN COUNT(r) AS total")
            .unwrap();
        assert_eq!(target_edges.rows[0].get("total"), Some(&Value::Int(2)));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn deletes_memory_labels_for_nowledge_label_cleanup_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory_2'})").unwrap();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'alpha'})").unwrap();
    db.query("CREATE (:Label {id: 'beta'})").unwrap();
    db.query("CREATE (:Label {id: 'gamma'})").unwrap();
    db.query(
        "MATCH (m:Memory {id: 'memory_1'}), (l:Label {id: 'alpha'}) CREATE (m)-[:HAS_LABEL]->(l)",
    )
    .unwrap();
    db.query(
        "MATCH (m:Memory {id: 'memory_1'}), (l:Label {id: 'beta'}) CREATE (m)-[:HAS_LABEL]->(l)",
    )
    .unwrap();
    db.query(
        "MATCH (m:Memory {id: 'memory_1'}), (l:Label {id: 'gamma'}) CREATE (m)-[:HAS_LABEL]->(l)",
    )
    .unwrap();
    db.query(
        "MATCH (m:Memory {id: 'memory_2'}), (l:Label {id: 'alpha'}) CREATE (m)-[:HAS_LABEL]->(l)",
    )
    .unwrap();
    let idless = db
        .query("MATCH (m:Memory) WHERE m.title = 'Idless memory' RETURN id(m) AS id")
        .unwrap();
    let idless_memory_id = match idless.rows[0].get("id").unwrap() {
        Value::Int(id) => id.to_string(),
        other => panic!("expected projected id int, got {other:?}"),
    };
    let graph_commit_epoch_before = db.store.commit_epoch();

    let exact = db
        .delete_knowledge_memory_labels(&KnowledgeMemoryLabelDeleteRequest {
            memory_id: "memory_1".to_string(),
            label_id: Some("alpha".to_string()),
        })
        .unwrap();
    assert_eq!(exact.graph_commit_epoch_before, graph_commit_epoch_before);
    assert_eq!(
        exact.graph_commit_epoch_after,
        graph_commit_epoch_before + 1
    );
    assert!(exact.found_memory);
    assert!(exact.found_label);
    assert!(!exact.non_writable);
    assert_eq!(exact.matched_relationship_count, 1);
    assert_eq!(exact.deleted_relationship_count, 1);
    assert_eq!(exact.deleted_relationship_ids.len(), 1);

    let all = db
        .delete_knowledge_memory_labels(&KnowledgeMemoryLabelDeleteRequest {
            memory_id: "memory_1".to_string(),
            label_id: None,
        })
        .unwrap();
    assert_eq!(
        all.graph_commit_epoch_before,
        exact.graph_commit_epoch_after
    );
    assert_eq!(
        all.graph_commit_epoch_after,
        exact.graph_commit_epoch_after + 1
    );
    assert!(all.found_memory);
    assert!(all.found_label);
    assert_eq!(all.matched_relationship_count, 2);
    assert_eq!(all.deleted_relationship_count, 2);
    assert_eq!(all.deleted_relationship_ids.len(), 2);

    assert_eq!(
        db.query("MATCH (m:Memory {id: 'memory_1'})-[r:HAS_LABEL]->(:Label) RETURN COUNT(r)")
            .unwrap()
            .rows[0]
            .values()
            .next(),
        Some(&Value::Int(0))
    );
    assert_eq!(
        db.query("MATCH (m:Memory {id: 'memory_2'})-[r:HAS_LABEL]->(:Label) RETURN COUNT(r)")
            .unwrap()
            .rows[0]
            .values()
            .next(),
        Some(&Value::Int(1))
    );

    let missing_label = db
        .delete_knowledge_memory_labels(&KnowledgeMemoryLabelDeleteRequest {
            memory_id: "memory_2".to_string(),
            label_id: Some("missing".to_string()),
        })
        .unwrap();
    assert!(missing_label.found_memory);
    assert!(!missing_label.found_label);
    assert_eq!(missing_label.deleted_relationship_count, 0);
    assert_eq!(
        missing_label.graph_commit_epoch_after,
        all.graph_commit_epoch_after
    );

    let non_writable = db
        .delete_knowledge_memory_labels(&KnowledgeMemoryLabelDeleteRequest {
            memory_id: idless_memory_id,
            label_id: None,
        })
        .unwrap();
    assert!(non_writable.found_memory);
    assert!(non_writable.non_writable);
    assert_eq!(non_writable.deleted_relationship_count, 0);
    assert_eq!(
        non_writable.graph_commit_epoch_after,
        all.graph_commit_epoch_after
    );

    let empty_all = db
        .delete_knowledge_memory_labels(&KnowledgeMemoryLabelDeleteRequest {
            memory_id: "memory_1".to_string(),
            label_id: None,
        })
        .unwrap();
    assert!(empty_all.found_memory);
    assert_eq!(empty_all.matched_relationship_count, 0);
    assert_eq!(
        empty_all.graph_commit_epoch_after,
        all.graph_commit_epoch_after
    );
}

#[test]
fn memory_label_delete_rejects_empty_ids_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    db.query("CREATE (:Label {id: 'label_1'})").unwrap();
    db.query(
        "MATCH (m:Memory {id: 'memory_1'}), (l:Label {id: 'label_1'}) CREATE (m)-[:HAS_LABEL]->(l)",
    )
    .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let empty_memory = db
        .delete_knowledge_memory_labels(&KnowledgeMemoryLabelDeleteRequest {
            memory_id: String::new(),
            label_id: None,
        })
        .unwrap_err();
    assert!(empty_memory.to_string().contains("non-empty memory id"));

    let empty_label = db
        .delete_knowledge_memory_labels(&KnowledgeMemoryLabelDeleteRequest {
            memory_id: "memory_1".to_string(),
            label_id: Some(String::new()),
        })
        .unwrap_err();
    assert!(empty_label.to_string().contains("non-empty label id"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_memory_label_delete_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_memory_label_delete_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
        db.query("CREATE (:Label {id: 'alpha'})").unwrap();
        db.query("CREATE (:Label {id: 'beta'})").unwrap();
        db.query(
            "MATCH (m:Memory {id: 'memory_1'}), (l:Label {id: 'alpha'}) CREATE (m)-[:HAS_LABEL]->(l)",
        )
        .unwrap();
        db.query(
            "MATCH (m:Memory {id: 'memory_1'}), (l:Label {id: 'beta'}) CREATE (m)-[:HAS_LABEL]->(l)",
        )
        .unwrap();
        let batch_count_before_delete = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.delete_knowledge_memory_labels(&KnowledgeMemoryLabelDeleteRequest {
            memory_id: "memory_1".to_string(),
            label_id: None,
        })
        .unwrap();
        let batch_count_after_delete = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_delete, batch_count_before_delete + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("delete_rel"));
    {
        let mut db = Database::open(&path).unwrap();
        assert_eq!(
            db.query("MATCH (m:Memory {id: 'memory_1'})-[r:HAS_LABEL]->(:Label) RETURN COUNT(r)")
                .unwrap()
                .rows[0]
                .values()
                .next(),
            Some(&Value::Int(0))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn reads_entity_labels_for_nowledge_has_label_shapes() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Label {id: 'alpha', name: 'Alpha', canonical_name: 'alpha', color: '#fff', description: 'Alpha label'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'beta', name: 'Beta', canonical_name: 'beta'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'gamma', name: 'Gamma', canonical_name: 'gamma'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory_2'})").unwrap();
    db.query("CREATE (:Source {id: 'source_1'})").unwrap();
    db.query(
        "MATCH (m:Memory {id: 'memory_1'}), (l:Label {id: 'beta'}) CREATE (m)-[:HAS_LABEL]->(l)",
    )
    .unwrap();
    db.query(
        "MATCH (m:Memory {id: 'memory_1'}), (l:Label {id: 'alpha'}) CREATE (m)-[:HAS_LABEL]->(l)",
    )
    .unwrap();
    db.query(
        "MATCH (s:Source {id: 'source_1'}), (l:Label {id: 'gamma'}) CREATE (s)-[:HAS_LABEL]->(l)",
    )
    .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();

    let memories_request = KnowledgeEntityLabelListRequest {
        entity_label: "Memory".to_string(),
        external_ids: vec![
            "memory_1".to_string(),
            "memory_2".to_string(),
            "missing".to_string(),
        ],
        limit_per_entity: 0,
    };
    let memories = db
        .query_entity_labels_via_cypher(&memories_request)
        .unwrap();

    assert_eq!(memories.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);
    assert_eq!(memories.found_entity_count, 2);
    assert_eq!(memories.missing_entity_count, 1);
    assert_eq!(memories.label_count, 2);
    assert_eq!(memories.groups.len(), 3);
    assert!(memories.groups[0].found);
    assert_eq!(memories.groups[0].external_id, "memory_1");
    assert_eq!(memories.groups[0].returned_count, 2);
    assert_eq!(
        memories.groups[0].labels[0].label_id.as_deref(),
        Some("alpha")
    );
    assert_eq!(memories.groups[0].labels[0].name.as_deref(), Some("Alpha"));
    assert_eq!(
        memories.groups[0].labels[0].canonical_name.as_deref(),
        Some("alpha")
    );
    assert_eq!(
        memories.groups[0].labels[0].color,
        Some(Value::String("#fff".to_string()))
    );
    assert_eq!(
        memories.groups[0].labels[0].description,
        Some(Value::String("Alpha label".to_string()))
    );
    assert_eq!(
        memories.groups[0].labels[1].label_id.as_deref(),
        Some("beta")
    );
    assert!(memories.groups[1].found);
    assert_eq!(memories.groups[1].returned_count, 0);
    assert!(!memories.groups[2].found);
    assert_eq!(memories.groups[2].node_id, None);

    let stats = db.plan_cache_stats();
    let repeated_memories = db
        .query_entity_labels_via_cypher(&memories_request)
        .unwrap();
    assert_eq!(repeated_memories, memories);
    let repeated_stats = db.plan_cache_stats();
    assert_eq!(repeated_stats.entries, stats.entries);
    assert_eq!(repeated_stats.misses, stats.misses);
    assert_eq!(repeated_stats.hits, stats.hits + 3);

    let sources = db
        .query_entity_labels_via_cypher(&KnowledgeEntityLabelListRequest {
            entity_label: "Source".to_string(),
            external_ids: vec!["source_1".to_string()],
            limit_per_entity: 1,
        })
        .unwrap();

    assert_eq!(sources.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);
    assert_eq!(sources.found_entity_count, 1);
    assert_eq!(sources.missing_entity_count, 0);
    assert_eq!(sources.label_count, 1);
    assert_eq!(sources.groups[0].returned_count, 1);
    assert_eq!(
        sources.groups[0].labels[0].label_id.as_deref(),
        Some("gamma")
    );
}

#[test]
fn entity_label_read_requests_validate_non_empty_filters() {
    let db = Database::new();
    let entity_label_error = db
        .query_entity_labels_via_cypher(&KnowledgeEntityLabelListRequest {
            entity_label: "Bad Label".to_string(),
            external_ids: vec!["memory_1".to_string()],
            limit_per_entity: 10,
        })
        .unwrap_err();
    assert!(entity_label_error
        .to_string()
        .contains("knowledge entity label"));

    let external_ids_error = db
        .query_entity_labels_via_cypher(&KnowledgeEntityLabelListRequest {
            entity_label: "Memory".to_string(),
            external_ids: Vec::new(),
            limit_per_entity: 10,
        })
        .unwrap_err();
    assert!(external_ids_error
        .to_string()
        .contains("non-empty external ids"));
}

fn test_thread_exists(db: &Database, thread_id: &str) -> bool {
    let parameters = BTreeMap::from([(
        "thread_id".to_string(),
        Value::String(thread_id.to_string()),
    )]);
    !db.query_read_only_with_params_bounded(
        "MATCH (t:Thread {id: $thread_id}) RETURN id(t) AS node_id LIMIT 1",
        &parameters,
        Some(1),
    )
    .unwrap()
    .rows
    .is_empty()
}

fn test_thread_identity_exists(db: &Database, identity_key: &str) -> bool {
    let parameters = BTreeMap::from([(
        "identity_key".to_string(),
        Value::String(identity_key.to_string()),
    )]);
    !db.query_read_only_with_params_bounded(
        "MATCH (ti:ThreadIdentity {id: $identity_key}) RETURN id(ti) AS node_id LIMIT 1",
        &parameters,
        Some(1),
    )
    .unwrap()
    .rows
    .is_empty()
}

fn test_thread_message_count(db: &Database, thread_id: &str) -> usize {
    let parameters = BTreeMap::from([(
        "thread_id".to_string(),
        Value::String(thread_id.to_string()),
    )]);
    db.query_read_only_with_params_bounded(
        "MATCH (t:Thread {id: $thread_id})-[:CONTAINS]->(m:Message) \
         RETURN id(m) AS message_node_id",
        &parameters,
        None,
    )
    .unwrap()
    .rows
    .len()
}

fn test_thread_compaction_links(db: &Database, thread_id: &str) -> QueryOutput {
    let parameters = BTreeMap::from([(
        "thread_id".to_string(),
        Value::String(thread_id.to_string()),
    )]);
    db.query_read_only_with_params_bounded(
        "MATCH (t:Thread {id: $thread_id})-[r:COMPACTS_TO]->(m:Memory) \
         RETURN m.id AS memory_id, r.compaction_method AS compaction_method, \
         r.created_at AS created_at, r.properties AS properties \
         ORDER BY id(r) ASC",
        &parameters,
        None,
    )
    .unwrap()
}

#[test]
fn relational_insert_returning_reports_provisional_and_committed_outcomes() {
    let mut db = Database::new();
    let mut schema = db.begin_transaction();
    schema
        .query_sql(
            "CREATE TABLE raw_turns (\
                raw_turn_id TEXT PRIMARY KEY, \
                org_id TEXT, \
                request_id TEXT, \
                UNIQUE (org_id, request_id)\
            )",
        )
        .expect("stage raw turn table");
    schema.commit().expect("commit raw turn table");

    let mut inserted = db.begin_transaction();
    let provisional = inserted
        .query_sql_with_result(
            "INSERT INTO raw_turns (raw_turn_id, org_id, request_id) \
             VALUES ('turn-1', 'org-1', 'request-1') \
             ON CONFLICT (org_id, request_id) DO NOTHING \
             RETURNING raw_turn_id",
        )
        .expect("stage idempotent raw turn insert");
    let mutation = provisional.mutation.expect("provisional mutation result");
    assert!(mutation.provisional);
    assert_eq!(mutation.affected_rows, 1);
    assert_eq!(mutation.conflict_rows, 0);
    assert_eq!(mutation.rows.len(), 1);
    assert_eq!(
        mutation.rows.first().and_then(|row| row.get("raw_turn_id")),
        Some(&Value::String("turn-1".to_string()))
    );
    let committed = inserted
        .commit_with_result()
        .expect("commit idempotent raw turn insert");
    assert_eq!(committed.mutations.len(), 1);
    assert!(!committed.mutations[0].provisional);
    assert_eq!(committed.mutations[0].affected_rows, 1);
    assert_eq!(committed.output.rows.len(), 1);

    let mut duplicate = db.begin_transaction();
    let provisional = duplicate
        .query_sql_with_result(
            "INSERT INTO raw_turns (raw_turn_id, org_id, request_id) \
             VALUES ('turn-2', 'org-1', 'request-1') \
             ON CONFLICT (org_id, request_id) DO NOTHING \
             RETURNING raw_turn_id",
        )
        .expect("stage duplicate raw turn insert");
    let mutation = provisional.mutation.expect("duplicate mutation result");
    assert_eq!(mutation.affected_rows, 0);
    assert_eq!(mutation.conflict_rows, 1);
    assert!(mutation.rows.is_empty());
    let committed = duplicate
        .commit_with_result()
        .expect("commit duplicate raw turn no-op");
    assert_eq!(committed.mutations[0].affected_rows, 0);
    assert_eq!(committed.mutations[0].conflict_rows, 1);
    assert!(committed.output.rows.is_empty());

    let mut local_duplicate = db.begin_transaction();
    let staged = local_duplicate
        .query_sql_with_result(
            "INSERT INTO raw_turns (raw_turn_id, org_id, request_id) \
             VALUES \
                ('turn-3', 'org-2', 'request-2'), \
                ('turn-4', 'org-2', 'request-2') \
             ON CONFLICT (org_id, request_id) DO NOTHING \
             RETURNING raw_turn_id",
        )
        .expect("resolve a transaction-local duplicate");
    let mutation = staged.mutation.expect("transaction-local mutation result");
    assert_eq!(mutation.affected_rows, 1);
    assert_eq!(mutation.conflict_rows, 1);
    assert_eq!(mutation.rows.len(), 1);
    local_duplicate.rollback();
    assert!(db
        .begin_read_transaction()
        .query_sql("SELECT raw_turn_id FROM raw_turns WHERE org_id = 'org-2'")
        .expect("read after rollback")
        .rows
        .is_empty());

    let mut nullable = db.begin_transaction();
    let staged = nullable
        .query_sql_with_result(
            "INSERT INTO raw_turns (raw_turn_id, org_id, request_id) \
             VALUES \
                ('turn-5', NULL, 'request-3'), \
                ('turn-6', NULL, 'request-3') \
             ON CONFLICT (org_id, request_id) DO NOTHING \
             RETURNING raw_turn_id",
        )
        .expect("NULL unique keys do not conflict");
    let mutation = staged.mutation.expect("nullable mutation result");
    assert_eq!(mutation.affected_rows, 2);
    assert_eq!(mutation.conflict_rows, 0);
    assert_eq!(mutation.rows.len(), 2);
    nullable.commit().expect("commit nullable unique rows");
}

#[test]
fn relational_insert_returning_conflicts_remain_deterministic_after_recovery() {
    let path = unique_test_dir("relational_insert_returning_recovery");
    {
        let mut db = Database::open(&path).expect("open durable database");
        let mut schema = db.begin_transaction();
        schema
            .query_sql(
                "CREATE TABLE raw_turns (\
                    raw_turn_id TEXT PRIMARY KEY, \
                    request_id TEXT UNIQUE NOT NULL\
                )",
            )
            .expect("stage raw turn table");
        schema.commit().expect("commit raw turn table");
        let mut insert = db.begin_transaction();
        insert
            .query_sql(
                "INSERT INTO raw_turns (raw_turn_id, request_id) \
                 VALUES ('turn-1', 'request-1') \
                 ON CONFLICT (request_id) DO NOTHING \
                 RETURNING raw_turn_id",
            )
            .expect("stage durable raw turn");
        insert.commit().expect("commit durable raw turn");
    }
    {
        let mut db = Database::open(&path).expect("recover durable database");
        let mut duplicate = db.begin_transaction();
        let staged = duplicate
            .query_sql_with_result(
                "INSERT INTO raw_turns (raw_turn_id, request_id) \
                 VALUES ('turn-2', 'request-1') \
                 ON CONFLICT (request_id) DO NOTHING \
                 RETURNING raw_turn_id",
            )
            .expect("stage recovered duplicate");
        assert_eq!(staged.mutation.unwrap().conflict_rows, 1);
        let committed = duplicate
            .commit_with_result()
            .expect("commit recovered duplicate no-op");
        assert_eq!(committed.mutations[0].affected_rows, 0);
        assert_eq!(committed.mutations[0].conflict_rows, 1);
    }
    std::fs::remove_dir_all(path).expect("remove durable returning fixture");
}

#[test]
fn relational_insert_returning_conflicts_cover_checkpoint_base_and_live_delta() {
    let path = unique_test_dir("relational_insert_returning_base_and_delta");
    let mut db = Database::open(&path).expect("open durable database");
    db.query_sql(
        "CREATE TABLE raw_turns (\
            raw_turn_id TEXT PRIMARY KEY, \
            request_id TEXT UNIQUE NOT NULL\
        )",
    )
    .expect("create raw turn table");
    db.query_sql(
        "INSERT INTO raw_turns (raw_turn_id, request_id) \
         VALUES ('turn-base', 'request-base')",
    )
    .expect("insert checkpoint base row");
    db.checkpoint().expect("checkpoint base row");
    db.query_sql(
        "INSERT INTO raw_turns (raw_turn_id, request_id) \
         VALUES ('turn-delta', 'request-delta')",
    )
    .expect("insert live delta row");

    let mut duplicate = db.begin_transaction();
    for (raw_turn_id, request_id) in [
        ("turn-base-duplicate", "request-base"),
        ("turn-delta-duplicate", "request-delta"),
    ] {
        let staged = duplicate
            .query_sql_with_result(&format!(
                "INSERT INTO raw_turns (raw_turn_id, request_id) \
                 VALUES ('{raw_turn_id}', '{request_id}') \
                 ON CONFLICT (request_id) DO NOTHING \
                 RETURNING raw_turn_id"
            ))
            .expect("stage conflict no-op");
        let mutation = staged.mutation.expect("provisional mutation result");
        assert_eq!(mutation.affected_rows, 0);
        assert_eq!(mutation.conflict_rows, 1);
        assert!(mutation.rows.is_empty());
    }
    let committed = duplicate
        .commit_with_result()
        .expect("commit base and delta conflict no-ops");
    assert_eq!(committed.mutations.len(), 2);
    assert!(committed
        .mutations
        .iter()
        .all(|mutation| mutation.affected_rows == 0 && mutation.conflict_rows == 1));

    drop(db);
    std::fs::remove_dir_all(path).expect("remove base and delta fixture");
}

#[test]
fn relational_insert_returning_result_budget_fails_before_staging() {
    let mut db = Database::new_with_config(DatabaseConfig {
        mutation_limits: hawdb_storage::MutationLimits {
            max_result_rows: NonZeroUsize::new(1).unwrap(),
            ..hawdb_storage::MutationLimits::default()
        },
        ..DatabaseConfig::default()
    });
    let mut schema = db.begin_transaction();
    schema
        .query_sql("CREATE TABLE messages (id TEXT PRIMARY KEY)")
        .expect("stage messages table");
    schema.commit().expect("commit messages table");

    let mut tx = db.begin_transaction();
    let error = tx
        .query_sql_with_result(
            "INSERT INTO messages (id) VALUES ('message-1'), ('message-2') RETURNING id",
        )
        .expect_err("oversized RETURNING result must fail closed");
    assert!(error.to_string().contains("max_result_rows 1"));
    assert!(tx
        .query_sql("SELECT id FROM messages")
        .expect("failed mutation leaves transaction state unchanged")
        .rows
        .is_empty());
    tx.rollback();
}

#[test]
fn relational_insert_returning_payload_budget_fails_before_staging() {
    let mut db = Database::new_with_config(DatabaseConfig {
        mutation_limits: hawdb_storage::MutationLimits {
            max_result_payload_bytes: NonZeroUsize::new(4).unwrap(),
            ..hawdb_storage::MutationLimits::default()
        },
        ..DatabaseConfig::default()
    });
    db.query_sql("CREATE TABLE messages (id TEXT PRIMARY KEY)")
        .expect("create messages table");

    let mut tx = db.begin_transaction();
    let error = tx
        .query_sql_with_result(
            "INSERT INTO messages (id) VALUES ('message-oversized') RETURNING id",
        )
        .expect_err("oversized RETURNING payload must fail closed");
    assert!(error.to_string().contains("max_result_payload_bytes 4"));
    assert!(tx
        .query_sql("SELECT id FROM messages")
        .expect("failed mutation leaves transaction state unchanged")
        .rows
        .is_empty());
    tx.rollback();
}

#[test]
fn autocommit_insert_returning_exposes_only_inserted_rows() {
    let mut db = Database::new();
    db.query_sql(
        "CREATE TABLE requests (\
            id TEXT PRIMARY KEY, \
            idempotency_key TEXT UNIQUE NOT NULL\
        )",
    )
    .expect("create request table");
    let inserted = db
        .query_sql(
            "INSERT INTO requests (id, idempotency_key) \
             VALUES ('request-1', 'key-1') \
             ON CONFLICT (idempotency_key) DO NOTHING \
             RETURNING id",
        )
        .expect("autocommit inserted request");
    assert_eq!(inserted.rows.len(), 1);
    assert_eq!(
        inserted.rows[0].get("id"),
        Some(&Value::String("request-1".to_string()))
    );
    let conflict = db
        .query_sql(
            "INSERT INTO requests (id, idempotency_key) \
             VALUES ('request-2', 'key-1') \
             ON CONFLICT (idempotency_key) DO NOTHING \
             RETURNING id",
        )
        .expect("autocommit duplicate request");
    assert!(conflict.rows.is_empty());
}

#[test]
fn idempotent_raw_insert_and_dirty_mark_commit_atomically_on_both_paths() {
    let mut db = Database::new();
    let mut schema = db.begin_transaction();
    schema
        .query_sql(
            "CREATE TABLE raw_turns (\
                raw_turn_id TEXT PRIMARY KEY, \
                request_id TEXT UNIQUE NOT NULL\
            )",
        )
        .expect("stage raw turn table");
    schema
        .query_sql(
            "CREATE TABLE dirty_queue (\
                org_id TEXT PRIMARY KEY, \
                dirty BOOLEAN NOT NULL\
            )",
        )
        .expect("stage dirty queue table");
    schema.commit().expect("commit ingestion schema");

    let mut seed = db.begin_transaction();
    seed.query_sql(
        "INSERT INTO raw_turns (raw_turn_id, request_id) \
         VALUES ('turn-1', 'request-1') \
         ON CONFLICT (request_id) DO NOTHING \
         RETURNING raw_turn_id",
    )
    .expect("stage initial raw turn");
    seed.query_sql(
        "INSERT INTO dirty_queue (org_id, dirty) VALUES ('org-1', FALSE) \
         ON CONFLICT (org_id) DO UPDATE SET dirty = EXCLUDED.dirty",
    )
    .expect("stage initial dirty marker");
    seed.commit().expect("commit initial ingestion");

    let mut duplicate = db.begin_transaction();
    let raw = duplicate
        .query_sql_with_result(
            "INSERT INTO raw_turns (raw_turn_id, request_id) \
             VALUES ('turn-2', 'request-1') \
             ON CONFLICT (request_id) DO NOTHING \
             RETURNING raw_turn_id",
        )
        .expect("stage duplicate raw turn");
    assert_eq!(raw.mutation.unwrap().conflict_rows, 1);
    duplicate
        .query_sql(
            "INSERT INTO dirty_queue (org_id, dirty) VALUES ('org-1', TRUE) \
             ON CONFLICT (org_id) DO UPDATE SET dirty = EXCLUDED.dirty",
        )
        .expect("stage dirty marker on duplicate path");
    let committed = duplicate
        .commit_with_result()
        .expect("commit duplicate ingestion atomically");
    assert_eq!(committed.mutations.len(), 2);
    assert_eq!(committed.mutations[0].conflict_rows, 1);
    assert_eq!(committed.mutations[1].affected_rows, 1);
    assert_eq!(committed.mutations[1].conflict_rows, 1);
    assert_eq!(
        db.begin_read_transaction()
            .query_sql("SELECT dirty FROM dirty_queue WHERE org_id = 'org-1'")
            .expect("read committed dirty marker")
            .rows[0]
            .get("dirty"),
        Some(&Value::Bool(true))
    );
}

fn unique_test_dir(name: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("hawdb_{name}_{nanos}"))
}

fn active_wal_path(path: impl AsRef<std::path::Path>) -> std::path::PathBuf {
    active_generation_path(path.as_ref(), "wal_generation", "wal")
}

fn active_checkpoint_path(path: impl AsRef<std::path::Path>) -> std::path::PathBuf {
    active_generation_path(path.as_ref(), "checkpoint_generation", "checkpoint")
}

fn read_test_wal(path: impl AsRef<std::path::Path>) -> std::io::Result<String> {
    crate::store::render_wal_records_for_test(&active_wal_path(path))
}

fn read_test_plan_cache_metric(read: &DatabaseReadTransaction, metric: &str) -> i64 {
    let output = read
        .query_sql("SELECT metric, value FROM system.plan_cache")
        .unwrap();
    output
        .rows
        .iter()
        .find(|row| row.get("metric") == Some(&Value::String(metric.to_string())))
        .and_then(|row| row.get("value"))
        .and_then(|value| match value {
            Value::Int(value) => Some(*value),
            _ => None,
        })
        .unwrap_or_else(|| panic!("missing plan-cache metric {metric}"))
}

fn active_generation_path(
    root: &std::path::Path,
    manifest_field: &str,
    prefix: &str,
) -> std::path::PathBuf {
    let manifest = std::fs::read_to_string(root.join("manifest.hawdb")).unwrap();
    assert!(manifest.contains("HAWDB_MANIFEST_V1\n"));
    let generation = manifest.lines().find_map(|line| {
        let (field, value) = line.split_once('\t')?;
        (field == manifest_field && value != "none").then_some(value)
    });
    root.join(format!(
        "{prefix}.{}.hawdb",
        generation.expect("active generation must exist")
    ))
}

fn read_test_durable_text(path: &std::path::Path) -> std::io::Result<String> {
    let bytes = std::fs::read(path)?;
    if !bytes.starts_with(b"HAWDB_COMPRESSED_V1") {
        return String::from_utf8(bytes)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error));
    }
    let header_end = bytes
        .windows(2)
        .position(|window| window == b"\n\n")
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "missing compressed envelope header terminator",
            )
        })?;
    let payload = &bytes[header_end + 2..];
    let decoded = zstd::stream::decode_all(Cursor::new(payload))?;
    String::from_utf8(decoded)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
}
