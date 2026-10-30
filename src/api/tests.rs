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
    KnowledgeCandidateScoringPolicy, KnowledgeCandidateSource, KnowledgeFallbackReasonCode,
    KnowledgeFanoutReasonCode, KnowledgeGraphPathDirection, KnowledgeRetrievalEmptyReasonCode,
    KnowledgeRetrievalRequest, KnowledgeRetrievalStage, KnowledgeTruncationReasonCode,
    NowledgeGraphAdapter, NowledgeGraphStatement, PlanCacheBypassReason, PlanCacheLookup,
    QueryOutput, QueryStreamOptions, RecoveryMode, SearchProjectionGraphDeltaRequest,
    HAWDB_LIGHTNING_BOOTSTRAP_PROTOCOL_VERSION, NOWLEDGE_DEEP_SEARCH_FILTERED_RANK_WINDOW,
    NOWLEDGE_DEEP_SEARCH_GRAPH_CONTEXT_MAX_HOPS, NOWLEDGE_DEEP_SEARCH_MIN_GRAPH_SEED_LIMIT,
    NOWLEDGE_DEEP_SEARCH_MIN_RANK_WINDOW,
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
use crate::{HawDBError, Result, Value};
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
mod crystal_reads;
mod crystal_source;
mod default_pipeline;
mod delete_mutations;
mod descriptor_capacity;
mod entity_labels;
mod entity_writes;
mod expression_functions;
mod external_content_artifacts;
mod graph_analytics;
mod graph_hash_join;
mod graph_meta;
mod graph_rag_schema_guidance;
mod induced_edge_reads;
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
mod label_lifecycle;
mod materialized_views;
mod memory_lifecycle;
mod merge_nodes;
mod merge_relationships;
mod mutation_guards;
mod mutation_persistence;
mod neighbor_reads;
mod nowledge_graph_adapter;
mod optional_match;
mod ordered_composite;
mod ordered_range;
mod pagerank;
mod predicates;
mod projected_graph_artifacts;
mod projected_graph_execution;
mod projection_generations;
mod query_execution;
mod query_observability;
mod read_transactions;
mod relational_access_cost;
mod relational_cross_join;
mod relational_explain_output;
mod relational_from_scopes;
mod relational_having;
mod relational_ordering;
mod relational_projection_expressions;
mod relational_range;
mod relational_update_delete_outcomes;
mod relationship_patterns;
mod relationship_property_mutations;
mod relationship_reads;
mod result_column_order;
mod runtime_capabilities;
mod schema_indexes;
mod schema_migrations;
mod search_projection_delta_facade;
mod search_projection_graph_delta;
mod search_projection_rebuild_facade;
mod set_mutations;
mod simple_case_dispatch;
mod skill_lifecycle;
mod skill_reads;
mod snapshot_retention;
mod source_labels;
mod source_lifecycle;
mod source_memory_count;
mod source_parsed_create;
mod source_parsed_metadata;
mod source_reads;
mod source_reference;
mod source_revisions;
mod statistics;
mod storage_recovery;
mod synthesized_source_reads;
mod system_introspection;
mod system_schema_registry;
mod system_variables;
mod thread_compaction_reads;
mod thread_distillation_reads;
mod thread_lifecycle;
mod thread_message_reads;
mod thread_metadata_reads;
mod transaction_compaction;
mod transaction_control;
mod transaction_merge;

#[test]
fn database_instance_identity_rejects_poisoned_store() {
    let database = Database::new();
    assert!(database.instance_identity().is_ok());
    let store = &database.runtime.get().unwrap().store;
    store.poison_on_storage_error::<()>(&Err(crate::HawDBError::StorageIntegrity(
        "injected identity admission failure".into(),
    )));
    let expected = store.ensure_usable().unwrap_err();
    assert!(matches!(expected, crate::HawDBError::Storage(_)));
    for _ in 0..2 {
        assert_eq!(database.instance_identity().unwrap_err(), expected);
    }
}

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

    let mut transaction = db.begin_transaction().unwrap();
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
fn unwind_transaction_matches_sequential_merges_and_wal_for_repeated_keys() {
    const SINGLE: &str =
        "MERGE (e:Entity {id: $id}) ON CREATE SET e.name = $name, e.aliases = $aliases";
    const BATCH: &str = "UNWIND $rows AS row MERGE (e:Entity {id: row.id}) ON CREATE SET e.name = row.name, e.aliases = row.aliases";
    const READ: &str =
        "MATCH (e:Entity) RETURN e.id AS id, e.name AS name, e.aliases AS aliases ORDER BY id";

    // Permute duplicate keys and values independently. First creation wins,
    // including NULL and list properties; an existing key must never change.
    for seed in 0..8 {
        let rows = (0..32)
            .map(|index| {
                BTreeMap::from([
                    ("id".to_string(), Value::Int((index * 7 + seed) % 11)),
                    (
                        "name".to_string(),
                        if index % 3 == 0 {
                            Value::Null
                        } else {
                            Value::String(format!("name-{seed}-{index}"))
                        },
                    ),
                    (
                        "aliases".to_string(),
                        Value::List(vec![Value::String(format!("alias-{index}"))]),
                    ),
                ])
            })
            .collect::<Vec<_>>();
        let mut evidence = Vec::new();
        for batched in [false, true] {
            let path = unique_test_dir("unwind_transaction_equivalence");
            let mut db = Database::open(&path).unwrap();
            db.query("CREATE (:Entity {id: 0, name: 'existing', aliases: []})")
                .unwrap();
            let epoch = db.runtime.get().unwrap().store.commit_epoch();
            let prefix = std::fs::read(active_wal_path(&path)).unwrap();
            let mut transaction = db.begin_transaction().unwrap();
            if batched {
                transaction
                    .query_with_params(
                        BATCH,
                        &BTreeMap::from([(
                            "rows".to_string(),
                            Value::List(rows.iter().cloned().map(Value::Map).collect()),
                        )]),
                    )
                    .unwrap();
            } else {
                for row in &rows {
                    transaction.query_with_params(SINGLE, row).unwrap();
                }
            }
            let staged = transaction.query(READ).unwrap();
            assert_eq!(staged.rows.len(), 11);
            assert_eq!(std::fs::read(active_wal_path(&path)).unwrap(), prefix);
            transaction.commit().unwrap();
            assert_eq!(db.runtime.get().unwrap().store.commit_epoch(), epoch + 1);
            let committed_wal = std::fs::read(active_wal_path(&path)).unwrap();
            assert!(committed_wal.len() > prefix.len());
            drop(db);
            let mut reopened = Database::open(&path).unwrap();
            let recovered = reopened.query(READ).unwrap();
            assert_eq!(recovered.rows, staged.rows);
            assert_eq!(
                recovered.rows[0].get("name"),
                Some(&Value::String("existing".to_string()))
            );
            evidence.push((recovered.rows, committed_wal));
            drop(reopened);
            std::fs::remove_dir_all(path).unwrap();
        }
        assert_eq!(evidence[0], evidence[1], "seed {seed}");
    }
}

#[test]
fn unwind_transaction_rejection_preserves_prior_statement_and_wal() {
    let path = unique_test_dir("unwind_statement_savepoint");
    let config = DatabaseConfig {
        mutation_limits: hawdb_storage::mutation::MutationLimits {
            max_operations: std::num::NonZeroUsize::new(2).unwrap(),
            ..hawdb_storage::mutation::MutationLimits::default()
        },
        ..DatabaseConfig::default()
    };
    let mut db = Database::open_with_config(&path, config).unwrap();
    let prefix = std::fs::read(active_wal_path(&path)).unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    transaction.query("CREATE (:Entity {id: 'prior'})").unwrap();
    let query = "UNWIND $rows AS row CREATE (:Entity {id: row.id})";
    let parameters = |ids: &[&str]| {
        BTreeMap::from([(
            "rows".to_string(),
            Value::List(
                ids.iter()
                    .map(|id| {
                        Value::Map(BTreeMap::from([(
                            "id".to_string(),
                            Value::String((*id).to_string()),
                        )]))
                    })
                    .collect(),
            ),
        )])
    };
    let error = transaction
        .query_with_params(query, &parameters(&["a", "b", "c"]))
        .unwrap_err();
    assert!(
        error.to_string().contains("max_mutation_operations"),
        "{error}"
    );
    let read = "MATCH (e:Entity) RETURN e.id AS id ORDER BY id";
    let staged = transaction.query(read).unwrap();
    assert_eq!(staged.rows.len(), 1);
    assert_eq!(
        staged.rows[0].get("id"),
        Some(&Value::String("prior".into()))
    );
    assert_eq!(std::fs::read(active_wal_path(&path)).unwrap(), prefix);
    transaction
        .query_with_params(query, &parameters(&["retry"]))
        .unwrap();
    transaction.commit().unwrap();
    drop(db);
    let mut reopened = Database::open(&path).unwrap();
    let recovered = reopened.query(read).unwrap();
    assert_eq!(recovered.rows.len(), 2);
    assert_eq!(
        recovered.rows[0].get("id"),
        Some(&Value::String("prior".into()))
    );
    assert_eq!(
        recovered.rows[1].get("id"),
        Some(&Value::String("retry".into()))
    );
    drop(reopened);
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

const PROPERTY_TITLE_UPDATE: &str = "MATCH (m:Memory {id: $id}) SET m.title = $title \
    RETURN id(m) AS node_id";
const PROPERTY_PAIR_UPDATE: &str = "MATCH (m:Memory {id: $id}) \
    SET m.title = $title, m.review_status = $status RETURN id(m) AS node_id";
const PROPERTY_SNAPSHOT: &str = "MATCH (m:Memory) \
    RETURN m.id AS id, id(m) AS node_id, m.title AS title, m.review_status AS status \
    ORDER BY m.id LIMIT 5";

fn property_title_parameters(id: &str, title: Option<&str>) -> Result<BTreeMap<String, Value>> {
    // Validate caller input before opening a transaction; fixed statements own
    // property selection and all graph matching.
    let title =
        title.ok_or_else(|| HawDBError::Semantic("at least one assignment required".into()))?;
    Ok(BTreeMap::from([
        ("id".into(), Value::String(id.into())),
        ("title".into(), Value::String(title.into())),
    ]))
}

#[test]
fn updates_knowledge_properties_through_bounded_query() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'Old', review_status: 'pending'})")
        .unwrap();
    let before = db.commit_epoch().unwrap();
    let mut parameters = property_title_parameters("memory_1", Some("New")).unwrap();
    parameters.insert("status".into(), Value::String("approved".into()));
    let output = db
        .query_request(bounded_batch_query(PROPERTY_PAIR_UPDATE, &parameters))
        .unwrap();
    assert_eq!(before, 1);
    assert_eq!(db.commit_epoch().unwrap(), before + 1);
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0]["node_id"], Value::Int(0));
    let output = db
        .query_request(bounded_batch_query(PROPERTY_SNAPSHOT, &BTreeMap::new()))
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0]["title"], Value::String("New".into()));
    assert_eq!(output.rows[0]["status"], Value::String("approved".into()));
}

#[test]
fn scoped_knowledge_property_update_does_not_write_filtered_seed() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'memory_1', title: 'Old', source_id: 'thread_1', space_id: ''})",
    )
    .unwrap();
    let before = db.commit_epoch().unwrap();
    let mut parameters = property_title_parameters("memory_1", Some("New")).unwrap();
    parameters.insert("source".into(), Value::String("thread_2".into()));
    parameters.insert("space".into(), Value::String("default".into()));
    let output = db.query_request(bounded_batch_query(
        "MATCH (m:Memory {id: $id}) WHERE m.source_id = $source \
         AND (m.space_id = $space OR ($space = 'default' AND (m.space_id IS NULL OR m.space_id = ''))) \
         SET m.title = $title RETURN id(m) AS node_id", &parameters)).unwrap();
    assert!(output.rows.is_empty());
    assert_eq!(before, 1);
    assert_eq!(db.commit_epoch().unwrap(), before);
    let output = db
        .query_request(bounded_batch_query(PROPERTY_SNAPSHOT, &BTreeMap::new()))
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0]["node_id"], Value::Int(0));
    assert_eq!(output.rows[0]["title"], Value::String("Old".into()));
}

#[test]
fn knowledge_property_update_rejects_invalid_identifiers() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'Old'})")
        .unwrap();
    let parameters = property_title_parameters("memory_1", Some("New")).unwrap();
    let error = db
        .query_request(bounded_batch_query(
            "MATCH (m:Memory {id: $id}) SET m.bad-name = $title RETURN id(m)",
            &parameters,
        ))
        .unwrap_err();
    assert!(matches!(error, HawDBError::Parse(_)), "{error}");
    assert_eq!(db.commit_epoch().unwrap(), 1);
    let output = db
        .query_request(bounded_batch_query(PROPERTY_SNAPSHOT, &BTreeMap::new()))
        .unwrap();
    assert_eq!(output.rows[0]["title"], Value::String("Old".into()));
}

#[test]
fn read_only_database_rejects_knowledge_property_query_update() {
    let path = unique_test_dir("read_only_knowledge_property_query_update");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1', title: 'Old'})")
            .unwrap();
    }
    let wal_before = read_test_wal(&path).unwrap();
    {
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                read_only: true,
                recovery_mode: crate::RecoveryMode::Strict,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        let parameters = property_title_parameters("memory_1", Some("New")).unwrap();
        let error = db
            .query_request(bounded_batch_query(PROPERTY_TITLE_UPDATE, &parameters))
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
    }
    assert_eq!(read_test_wal(&path).unwrap(), wal_before);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn knowledge_property_query_update_persists_and_replays_from_wal() {
    let path = unique_test_dir("knowledge_property_query_update_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1', title: 'Old'})")
            .unwrap();
        let parameters = property_title_parameters("memory_1", Some("New")).unwrap();
        let output = db
            .query_request(bounded_batch_query(PROPERTY_TITLE_UPDATE, &parameters))
            .unwrap();
        assert_eq!(output.rows.len(), 1);
    }
    assert!(read_test_wal(&path).unwrap().contains("set_node_property"));
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query_request(bounded_batch_query(PROPERTY_SNAPSHOT, &BTreeMap::new()))
            .unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0]["node_id"], Value::Int(0));
        assert_eq!(output.rows[0]["title"], Value::String("New".into()));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn updates_knowledge_properties_batch_through_bounded_queries() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'Old 1', review_status: 'pending'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', title: 'Old 2', review_status: 'pending'})")
        .unwrap();
    let before = db.commit_epoch().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    let mut parameters = property_title_parameters("memory_1", Some("New 1")).unwrap();
    parameters.insert("status".into(), Value::String("approved".into()));
    let first = transaction
        .query_request(bounded_batch_query(PROPERTY_PAIR_UPDATE, &parameters))
        .unwrap();
    assert_eq!(first.rows.len(), 1);
    assert_eq!(first.rows[0]["node_id"], Value::Int(0));
    for (id, title, count) in [("memory_2", "New 2", 1), ("missing", "Ignored", 0)] {
        let parameters = property_title_parameters(id, Some(title)).unwrap();
        let output = transaction
            .query_request(bounded_batch_query(PROPERTY_TITLE_UPDATE, &parameters))
            .unwrap();
        assert_eq!(output.rows.len(), count);
        if count == 1 {
            assert_eq!(output.rows[0]["node_id"], Value::Int(1));
        }
    }
    transaction.commit().unwrap();
    assert_eq!(before, 2);
    assert_eq!(db.commit_epoch().unwrap(), before + 1);
    let output = db
        .query_request(bounded_batch_query(PROPERTY_SNAPSHOT, &BTreeMap::new()))
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.rows[0]["title"], Value::String("New 1".into()));
    assert_eq!(output.rows[0]["status"], Value::String("approved".into()));
    assert_eq!(output.rows[1]["title"], Value::String("New 2".into()));
    assert_eq!(output.rows[1]["status"], Value::String("pending".into()));
}

const THREAD_SPACE_MOVE: &str = "MATCH (t:Thread) WHERE t.thread_id IN $ids \
    AND CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END = $source \
    AND CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END <> $target \
    SET t.space_id = $target, t.updated_at = $updated_at RETURN t.thread_id AS id";
const MEMORY_SPACE_MOVE: &str = "MATCH (m:Memory) WHERE m.id IN $ids \
    AND CASE WHEN m.space_id IS NULL OR m.space_id = '' THEN 'default' ELSE m.space_id END = $source \
    AND CASE WHEN m.space_id IS NULL OR m.space_id = '' THEN 'default' ELSE m.space_id END <> $target \
    SET m.space_id = $target RETURN m.id AS id";

#[test]
fn moves_thread_normalized_space_batch_by_identity_property() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'storage-1', thread_id: 'thread-1', space_id: ''})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'storage-2', thread_id: 'thread-2', space_id: 'default'})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'storage-3', thread_id: 'thread-3', space_id: 'team'})")
        .unwrap();
    let mut parameters =
        batch_id_parameters(&["thread-1", "thread-2", "thread-3", "thread-2", "missing"]).unwrap();
    parameters.insert("source".into(), Value::String("default".into()));
    parameters.insert("target".into(), Value::String("team".into()));
    parameters.insert("updated_at".into(), Value::String("2026-07-19".into()));
    let before = db.commit_epoch().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    let candidates = transaction
        .query_request(bounded_batch_query(
            "MATCH (t:Thread) WHERE t.thread_id IN $ids RETURN t.thread_id AS id ORDER BY id",
            &parameters,
        ))
        .unwrap();
    assert_eq!(candidates.rows.len(), 3);
    let output = transaction
        .query_request(bounded_batch_query(THREAD_SPACE_MOVE, &parameters))
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    let moved = output
        .rows
        .iter()
        .map(|row| row["id"].clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        moved,
        BTreeSet::from([
            Value::String("thread-1".into()),
            Value::String("thread-2".into())
        ])
    );
    transaction.commit().unwrap();
    assert_eq!(before, 3);
    assert_eq!(db.commit_epoch().unwrap(), before + 1);
    let rows = db.query_request(bounded_batch_query(
        "MATCH (t:Thread) RETURN t.id AS storage_id, t.thread_id AS id, t.space_id AS space, t.updated_at AS updated_at ORDER BY id", &BTreeMap::new())).unwrap();
    assert_eq!(rows.rows.len(), 3);
    for (index, row) in rows.rows.iter().enumerate() {
        assert_eq!(
            row["storage_id"],
            Value::String(format!("storage-{}", index + 1))
        );
        assert_eq!(row["id"], Value::String(format!("thread-{}", index + 1)));
        assert_eq!(row["space"], Value::String("team".into()));
    }
    assert_eq!(
        rows.rows[0]["updated_at"],
        Value::String("2026-07-19".into())
    );
    assert_eq!(
        rows.rows[1]["updated_at"],
        Value::String("2026-07-19".into())
    );
    assert_eq!(rows.rows[2]["updated_at"], Value::Null);
    // All selected rows are now outside the source; retrying must be a no-op.
    let epoch = db.commit_epoch().unwrap();
    assert!(db
        .query_request(bounded_batch_query(THREAD_SPACE_MOVE, &parameters))
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(db.commit_epoch().unwrap(), epoch);
}

#[test]
fn knowledge_normalized_space_move_batch_rejects_invalid_identifiers() {
    let mut db = Database::new();
    let parameters = BTreeMap::from([
        ("id".into(), Value::String("thread-1".into())),
        ("target".into(), Value::String("team".into())),
    ]);
    let error = db
        .query_request(bounded_batch_query(
            "MATCH (t:Thread {thread-id: $id}) SET t.space_id = $target",
            &parameters,
        ))
        .unwrap_err();
    assert!(matches!(error, HawDBError::Parse(_)), "{error}");
    assert_eq!(db.commit_epoch().unwrap(), 0);
}

#[test]
fn knowledge_normalized_space_move_query_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("knowledge_normalized_space_move_query_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1', space_id: ''})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'memory_2', space_id: 'default'})")
            .unwrap();
        let batches_before = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        let mut parameters = batch_id_parameters(&["memory_1", "memory_2"]).unwrap();
        parameters.insert("source".into(), Value::String("default".into()));
        parameters.insert("target".into(), Value::String("archive".into()));
        let output = db
            .query_request(bounded_batch_query(MEMORY_SPACE_MOVE, &parameters))
            .unwrap();
        assert_eq!(output.rows.len(), 2);
        assert_eq!(
            read_test_wal(&path).unwrap().matches("\tbatch\t").count(),
            batches_before + 1
        );
    }
    assert!(read_test_wal(&path).unwrap().contains("set_node_property"));
    {
        let mut db = Database::open(&path).unwrap();
        let output = db.query_request(bounded_batch_query(
            "MATCH (m:Memory) RETURN m.id AS id, m.space_id AS space, m.updated_at AS updated_at ORDER BY id", &BTreeMap::new())).unwrap();
        assert_eq!(output.rows.len(), 2);
        for (index, row) in output.rows.iter().enumerate() {
            assert_eq!(row["id"], Value::String(format!("memory_{}", index + 1)));
            assert_eq!(row["space"], Value::String("archive".into()));
            assert_eq!(row["updated_at"], Value::Null);
        }
    }
    std::fs::remove_dir_all(path).unwrap();
}

const MEMORY_ACCESS_TOUCH: &str = "MATCH (m:Memory {id: $id}) \
    SET m.access_count = COALESCE(m.access_count, 0) + 1, m.last_accessed_at = $at \
    RETURN m.access_count AS accesses, m.clicks AS clicks";
const MEMORY_CLICK_TOUCH: &str = "MATCH (m:Memory {id: $id}) \
    SET m.access_count = COALESCE(m.access_count, 0) + 1, m.last_accessed_at = $at, \
    m.clicks = COALESCE(m.clicks, 0) + 1, m.last_clicked_at = $at, \
    m.total_dwell_time_ms = COALESCE(m.total_dwell_time_ms, 0) + $dwell \
    RETURN m.access_count AS accesses, m.clicks AS clicks";
const MEMORY_ACCESS_READ: &str = "MATCH (m:Memory) RETURN m.id AS id, \
    m.access_count AS accesses, m.clicks AS clicks, m.total_dwell_time_ms AS dwell, \
    m.last_accessed_at AS accessed_at, m.last_clicked_at AS clicked_at ORDER BY m.id LIMIT 5";

fn memory_access_parameters(
    id: &str,
    at: &str,
    dwell: Option<i64>,
) -> Result<BTreeMap<String, Value>> {
    if id.is_empty() {
        return Err(HawDBError::Semantic("non-empty memory id required".into()));
    }
    if dwell.is_some_and(|value| value < 0) {
        return Err(HawDBError::Semantic(
            "non-negative dwell time required".into(),
        ));
    }
    let mut parameters = BTreeMap::from([
        ("id".into(), Value::String(id.into())),
        ("at".into(), Value::String(at.into())),
    ]);
    if let Some(dwell) = dwell {
        parameters.insert("dwell".into(), Value::Int(dwell));
    }
    Ok(parameters)
}

#[test]
fn touches_memory_access_batch_with_incremental_counters() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', access_count: 2, clicks: 1, total_dwell_time_ms: 100, last_accessed_at: 'old', last_clicked_at: 'old'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory_2'})").unwrap();
    let before = db.commit_epoch().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    let mut counts = Vec::new();
    for (id, at, dwell, accesses, clicks) in [
        ("memory_1", "2026-07-19T10:00:00Z", None, 3, 1),
        ("memory_1", "2026-07-19T10:01:00Z", Some(50), 4, 2),
        ("memory_2", "2026-07-19T10:02:00Z", Some(25), 1, 1),
        ("missing", "2026-07-19T10:03:00Z", None, 0, 0),
    ] {
        let parameters = memory_access_parameters(id, at, dwell).unwrap();
        let statement = if dwell.is_some() {
            MEMORY_CLICK_TOUCH
        } else {
            MEMORY_ACCESS_TOUCH
        };
        let output = transaction
            .query_request(bounded_batch_query(statement, &parameters))
            .unwrap();
        counts.push(output.rows.len());
        if !output.rows.is_empty() {
            assert_eq!(output.rows[0]["accesses"], Value::Int(accesses));
            assert_eq!(output.rows[0]["clicks"], Value::Int(clicks));
        }
    }
    assert_eq!(counts, [1, 1, 1, 0]);
    transaction.commit().unwrap();
    assert_eq!(before, 2);
    assert_eq!(db.commit_epoch().unwrap(), before + 1);
    let rows = db
        .query_request(bounded_batch_query(MEMORY_ACCESS_READ, &BTreeMap::new()))
        .unwrap();
    assert_eq!(rows.rows.len(), 2);
    assert_eq!(rows.rows[0]["accesses"], Value::Int(4));
    assert_eq!(rows.rows[0]["clicks"], Value::Int(2));
    assert_eq!(rows.rows[0]["dwell"], Value::Int(150));
    assert_eq!(
        rows.rows[0]["accessed_at"],
        Value::String("2026-07-19T10:01:00Z".into())
    );
    assert_eq!(
        rows.rows[0]["clicked_at"],
        Value::String("2026-07-19T10:01:00Z".into())
    );
    assert_eq!(rows.rows[1]["accesses"], Value::Int(1));
    assert_eq!(rows.rows[1]["clicks"], Value::Int(1));
    assert_eq!(rows.rows[1]["dwell"], Value::Int(25));
}

#[test]
fn knowledge_memory_access_batch_rejects_negative_dwell_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    let before = db.commit_epoch().unwrap();
    let error = memory_access_parameters("memory_1", "2026-07-19T10:00:00Z", Some(-1)).unwrap_err();
    assert!(error.to_string().contains("non-negative dwell time"));
    assert!(memory_access_parameters("", "2026-07-19T10:00:00Z", None).is_err());
    assert_eq!(db.commit_epoch().unwrap(), before);
}

#[test]
fn memory_access_queries_persist_as_one_wal_batch_and_replay() {
    let path = unique_test_dir("memory_access_query_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
        db.query("CREATE (:Memory {id: 'memory_2', access_count: 4, clicks: 2, total_dwell_time_ms: 10})").unwrap();
        let batches_before = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        let mut transaction = db.begin_transaction().unwrap();
        for (id, at, dwell) in [
            ("memory_1", "2026-07-19T11:00:00Z", None),
            ("memory_2", "2026-07-19T11:01:00Z", Some(90)),
        ] {
            let parameters = memory_access_parameters(id, at, dwell).unwrap();
            let statement = if dwell.is_some() {
                MEMORY_CLICK_TOUCH
            } else {
                MEMORY_ACCESS_TOUCH
            };
            let output = transaction
                .query_request(bounded_batch_query(statement, &parameters))
                .unwrap();
            assert_eq!(output.rows.len(), 1);
        }
        transaction.commit().unwrap();
        assert_eq!(
            read_test_wal(&path).unwrap().matches("\tbatch\t").count(),
            batches_before + 1
        );
    }
    assert!(read_test_wal(&path).unwrap().contains("set_node_property"));
    {
        let mut db = Database::open(&path).unwrap();
        let rows = db
            .query_request(bounded_batch_query(MEMORY_ACCESS_READ, &BTreeMap::new()))
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(rows.rows[0]["accesses"], Value::Int(1));
        assert_eq!(rows.rows[0]["clicks"], Value::Null);
        assert_eq!(rows.rows[0]["dwell"], Value::Null);
        assert_eq!(rows.rows[1]["accesses"], Value::Int(5));
        assert_eq!(rows.rows[1]["clicks"], Value::Int(3));
        assert_eq!(rows.rows[1]["dwell"], Value::Int(100));
    }
    std::fs::remove_dir_all(path).unwrap();
}

const MEMORY_CONTENT_REPLACE: &str = "MATCH (m:Memory {id: $id}) \
    SET m.content = $content, m.title = $title, m.semantic_field = $semantic_field, \
    m.importance = $importance, m.confidence = $confidence, m.unit_type = $unit_type, \
    m.source = $source, m.source_range = $source_range, m.space_id = $space_id, \
    m.updated_at = $updated_at, m.reindex_needed = $reindex_needed, \
    m.review_status = $review_status, m.extraction_method = $extraction_method \
    RETURN id(m) AS node_id";
const MEMORY_CONTENT_READ: &str = "MATCH (m:Memory {id: $id}) \
    RETURN m.content AS content, m.title AS title, m.semantic_field AS semantic_field, \
    m.importance AS importance, m.confidence AS confidence, m.unit_type AS unit_type, \
    m.source AS source, m.source_range AS source_range, m.space_id AS space_id, \
    m.updated_at AS updated_at, m.reindex_needed AS reindex_needed, \
    m.review_status AS review_status, m.extraction_method AS extraction_method LIMIT 1";

fn memory_content_parameters(id: &str, title: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("id".into(), Value::String(id.into())),
        ("content".into(), Value::String(format!("{title} content"))),
        ("title".into(), Value::String(title.into())),
        (
            "semantic_field".into(),
            Value::String(format!("{title} semantic field")),
        ),
        ("importance".into(), Value::Float(0.7)),
        ("confidence".into(), Value::Float(0.8)),
        ("unit_type".into(), Value::String("fact".into())),
        ("source".into(), Value::String("codex".into())),
        ("source_range".into(), Value::String("1..3".into())),
        ("space_id".into(), Value::String("default".into())),
        ("updated_at".into(), Value::Int(1700000000)),
        ("reindex_needed".into(), Value::Bool(true)),
        ("review_status".into(), Value::String("reviewed".into())),
        ("extraction_method".into(), Value::String("manual".into())),
    ])
}

fn validate_content_parameters(parameters: &BTreeMap<String, Value>) -> Result<()> {
    // This is host request validation only, with no graph access or query dispatch.
    for property in ["id", "unit_type", "extraction_method"] {
        if !matches!(parameters.get(property), Some(Value::String(value)) if !value.is_empty()) {
            return Err(HawDBError::Semantic(format!(
                "non-empty {property} required"
            )));
        }
    }
    for property in ["importance", "confidence"] {
        let valid = match parameters.get(property) {
            Some(Value::Int(_)) => true,
            Some(Value::Float(value)) => value.is_finite(),
            _ => false,
        };
        if !valid {
            return Err(HawDBError::Semantic(format!(
                "numeric finite {property} required"
            )));
        }
    }
    Ok(())
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
    let before = db.commit_epoch().unwrap();
    let updates = [
        memory_content_parameters("memory_1", "Updated Memory"),
        memory_content_parameters("missing", "Missing Memory"),
        memory_content_parameters("1", "Idless Memory"),
        memory_content_parameters("memory_1", "Duplicate Memory"),
    ];
    for parameters in &updates {
        validate_content_parameters(parameters).unwrap();
    }
    let mut transaction = db.begin_transaction().unwrap();
    let mut seen = BTreeSet::new();
    let mut counts = Vec::new();
    let mut duplicates = 0;
    for parameters in updates {
        let Value::String(id) = &parameters["id"] else {
            unreachable!()
        };
        if !seen.insert(id.clone()) {
            duplicates += 1;
            continue;
        }
        let output = transaction
            .query_request(bounded_batch_query(MEMORY_CONTENT_REPLACE, &parameters))
            .unwrap();
        counts.push(output.rows.len());
        if !output.rows.is_empty() {
            assert_eq!(output.rows[0]["node_id"], Value::Int(0));
        }
    }
    assert_eq!(counts, [1, 0, 0]);
    assert_eq!(duplicates, 1);
    transaction.commit().unwrap();
    assert_eq!(before, 2);
    assert_eq!(db.commit_epoch().unwrap(), before + 1);
    let parameters = BTreeMap::from([("id".into(), Value::String("memory_1".into()))]);
    let rows = db
        .query_request(bounded_batch_query(MEMORY_CONTENT_READ, &parameters))
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    let properties = &rows.rows[0];
    assert_eq!(
        properties["content"],
        Value::String("Updated Memory content".into())
    );
    assert_eq!(properties["title"], Value::String("Updated Memory".into()));
    assert_eq!(
        properties["semantic_field"],
        Value::String("Updated Memory semantic field".into())
    );
    assert_eq!(properties["importance"], Value::Float(0.7));
    assert_eq!(properties["confidence"], Value::Float(0.8));
    assert_eq!(properties["unit_type"], Value::String("fact".into()));
    assert_eq!(properties["source"], Value::String("codex".into()));
    assert_eq!(properties["source_range"], Value::String("1..3".into()));
    assert_eq!(properties["space_id"], Value::String("default".into()));
    assert_eq!(properties["updated_at"], Value::Int(1700000000));
    assert_eq!(properties["reindex_needed"], Value::Bool(true));
    assert_eq!(
        properties["review_status"],
        Value::String("reviewed".into())
    );
    assert_eq!(
        properties["extraction_method"],
        Value::String("manual".into())
    );
    let idless = db
        .query_request(bounded_batch_query(
            "MATCH (m:Memory) WHERE m.id IS NULL RETURN id(m) AS node_id, m.title AS title LIMIT 1",
            &BTreeMap::new(),
        ))
        .unwrap();
    assert_eq!(idless.rows.len(), 1);
    assert_eq!(idless.rows[0]["node_id"], Value::Int(1));
    assert_eq!(
        idless.rows[0]["title"],
        Value::String("Idless memory".into())
    );
}

const MEMORY_METADATA_REPLACE: &str = "MATCH (m:Memory) WHERE m.id IN $ids \
    SET m.metadata = $metadata RETURN id(m) AS node_id";
const MEMORY_METADATA_REPLACE_WITH_TIME: &str = "MATCH (m:Memory) WHERE m.id IN $ids \
    SET m.metadata = $metadata, m.updated_at = $updated_at RETURN id(m) AS node_id";
const MEMORY_METADATA_SNAPSHOT: &str = "MATCH (m:Memory) \
    RETURN id(m) AS node_id, m.id AS id, m.metadata AS metadata, m.updated_at AS updated_at \
    ORDER BY id(m) LIMIT 5";

#[test]
fn updates_memory_metadata_batch_for_nowledge_replace_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', metadata: '{}', updated_at: 1})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', metadata: '{}', updated_at: 2})")
        .unwrap();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();
    let before = db.commit_epoch().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    let mut seen = BTreeSet::new();
    let mut counts = Vec::new();
    let mut duplicates = 0;
    for (id, metadata, timestamp) in [
        ("memory_1", "{\"replace\":true}", None),
        ("memory_2", "{\"replace\":\"with-ts\"}", Some(100)),
        ("missing", "{}", None),
        ("2", "{}", None),
        ("memory_1", "{\"duplicate\":true}", Some(200)),
    ] {
        if !seen.insert(id) {
            duplicates += 1;
            continue;
        }
        let mut parameters = batch_id_parameters(&[id]).unwrap();
        parameters.insert("metadata".into(), Value::String(metadata.into()));
        let statement = if let Some(timestamp) = timestamp {
            parameters.insert("updated_at".into(), Value::Int(timestamp));
            MEMORY_METADATA_REPLACE_WITH_TIME
        } else {
            MEMORY_METADATA_REPLACE
        };
        let output = transaction
            .query_request(bounded_batch_query(statement, &parameters))
            .unwrap();
        counts.push(output.rows.len());
    }
    assert_eq!(counts, [1, 1, 0, 0]);
    assert_eq!(duplicates, 1);
    transaction.commit().unwrap();
    assert_eq!(db.commit_epoch().unwrap(), before + 1);
    let rows = db
        .query_request(bounded_batch_query(
            MEMORY_METADATA_SNAPSHOT,
            &BTreeMap::new(),
        ))
        .unwrap();
    assert_eq!(rows.rows.len(), 3);
    assert_eq!(
        rows.rows[0]["metadata"],
        Value::String("{\"replace\":true}".into())
    );
    assert_eq!(rows.rows[0]["updated_at"], Value::Int(1));
    assert_eq!(
        rows.rows[1]["metadata"],
        Value::String("{\"replace\":\"with-ts\"}".into())
    );
    assert_eq!(rows.rows[1]["updated_at"], Value::Int(100));
    assert_eq!(rows.rows[2]["node_id"], Value::Int(2));
    assert_eq!(rows.rows[2]["id"], Value::Null);
    assert_eq!(rows.rows[2]["metadata"], Value::Null);
    assert_eq!(rows.rows[2]["updated_at"], Value::Null);
}

#[test]
fn memory_metadata_update_rejects_empty_id_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', metadata: '{}'})")
        .unwrap();
    let before = db.commit_epoch().unwrap();
    let error = batch_id_parameters(&[""]).unwrap_err();
    assert!(error.to_string().contains("non-empty id"));
    assert_eq!(db.commit_epoch().unwrap(), before);
}

#[test]
fn memory_metadata_queries_persist_as_one_wal_batch_and_replay() {
    let path = unique_test_dir("memory_metadata_query_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1', metadata: '{}', updated_at: 1})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'memory_2', metadata: '{}', updated_at: 2})")
            .unwrap();
        let batches_before = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        let mut transaction = db.begin_transaction().unwrap();
        for (id, metadata, timestamp) in [
            ("memory_1", "{\"one\":true}", None),
            ("memory_2", "{\"two\":true}", Some(20)),
        ] {
            let mut parameters = batch_id_parameters(&[id]).unwrap();
            parameters.insert("metadata".into(), Value::String(metadata.into()));
            let statement = if let Some(timestamp) = timestamp {
                parameters.insert("updated_at".into(), Value::Int(timestamp));
                MEMORY_METADATA_REPLACE_WITH_TIME
            } else {
                MEMORY_METADATA_REPLACE
            };
            let output = transaction
                .query_request(bounded_batch_query(statement, &parameters))
                .unwrap();
            assert_eq!(output.rows.len(), 1);
        }
        transaction.commit().unwrap();
        assert_eq!(
            read_test_wal(&path).unwrap().matches("\tbatch\t").count(),
            batches_before + 1
        );
    }
    assert!(read_test_wal(&path).unwrap().contains("set_node_property"));
    {
        let mut db = Database::open(&path).unwrap();
        let rows = db
            .query_request(bounded_batch_query(
                MEMORY_METADATA_SNAPSHOT,
                &BTreeMap::new(),
            ))
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(
            rows.rows[0]["metadata"],
            Value::String("{\"one\":true}".into())
        );
        assert_eq!(rows.rows[0]["updated_at"], Value::Int(1));
        assert_eq!(
            rows.rows[1]["metadata"],
            Value::String("{\"two\":true}".into())
        );
        assert_eq!(rows.rows[1]["updated_at"], Value::Int(20));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn memory_content_update_rejects_invalid_rows_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    let before = db.commit_epoch().unwrap();
    let error = validate_content_parameters(&memory_content_parameters("", "Empty")).unwrap_err();
    assert!(error.to_string().contains("non-empty id"));
    for (property, value, message) in [
        (
            "importance",
            Value::String("high".into()),
            "numeric finite importance",
        ),
        (
            "importance",
            Value::Float(f64::INFINITY),
            "numeric finite importance",
        ),
        (
            "confidence",
            Value::Float(f64::NAN),
            "numeric finite confidence",
        ),
        (
            "unit_type",
            Value::String(String::new()),
            "non-empty unit_type",
        ),
        (
            "extraction_method",
            Value::String(String::new()),
            "non-empty extraction_method",
        ),
    ] {
        let mut parameters = memory_content_parameters("memory_1", "Invalid");
        parameters.insert(property.into(), value);
        assert!(validate_content_parameters(&parameters)
            .unwrap_err()
            .to_string()
            .contains(message));
    }
    assert_eq!(db.commit_epoch().unwrap(), before);
}

#[test]
fn memory_content_queries_persist_as_one_wal_batch_and_replay() {
    let path = unique_test_dir("memory_content_query_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1', title: 'Old 1'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'memory_2', title: 'Old 2'})")
            .unwrap();
        let batches_before = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        let updates = [
            memory_content_parameters("memory_1", "Updated One"),
            memory_content_parameters("memory_2", "Updated Two"),
        ];
        for parameters in &updates {
            validate_content_parameters(parameters).unwrap();
        }
        let mut transaction = db.begin_transaction().unwrap();
        for parameters in updates {
            let rows = transaction
                .query_request(bounded_batch_query(MEMORY_CONTENT_REPLACE, &parameters))
                .unwrap();
            assert_eq!(rows.rows.len(), 1);
        }
        transaction.commit().unwrap();
        assert_eq!(
            read_test_wal(&path).unwrap().matches("\tbatch\t").count(),
            batches_before + 1
        );
    }
    assert!(read_test_wal(&path).unwrap().contains("set_node_property"));
    {
        let mut db = Database::open(&path).unwrap();
        for (id, title) in [("memory_1", "Updated One"), ("memory_2", "Updated Two")] {
            let parameters = BTreeMap::from([("id".into(), Value::String(id.into()))]);
            let rows = db
                .query_request(bounded_batch_query(MEMORY_CONTENT_READ, &parameters))
                .unwrap();
            assert_eq!(rows.rows.len(), 1);
            assert_eq!(rows.rows[0]["title"], Value::String(title.into()));
            assert_eq!(rows.rows[0]["reindex_needed"], Value::Bool(true));
        }
    }
    std::fs::remove_dir_all(path).unwrap();
}

const MEMORY_DEDUP_REVIEW: &str = "MATCH (m:Memory) WHERE m.id IN $ids \
    SET m.dedup_reviewed_at = $reviewed_at RETURN m.id AS id";
const MEMORY_DEDUP_SNAPSHOT: &str = "MATCH (m:Memory) \
    RETURN id(m) AS node_id, m.id AS id, m.title AS title, m.dedup_reviewed_at AS reviewed_at \
    ORDER BY id(m) LIMIT 5";

#[test]
fn updates_memory_dedup_reviewed_batch_for_scheduler_shape() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'One'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', title: 'Two'})")
        .unwrap();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();
    let before = db.commit_epoch().unwrap();
    let mut parameters =
        batch_id_parameters(&["memory_1", "missing", "2", "memory_2", "memory_1"]).unwrap();
    parameters.insert("reviewed_at".into(), Value::Int(1800000000));
    let mut transaction = db.begin_transaction().unwrap();
    let output = transaction
        .query_request(bounded_batch_query(MEMORY_DEDUP_REVIEW, &parameters))
        .unwrap();
    // IN selects graph rows once even when the caller repeats an id.
    assert_eq!(output.rows.len(), 2);
    for id in ["memory_1", "memory_2"] {
        assert!(output
            .rows
            .iter()
            .any(|row| row["id"] == Value::String(id.into())));
    }
    transaction.commit().unwrap();
    assert_eq!(before, 3);
    assert_eq!(db.commit_epoch().unwrap(), before + 1);
    let rows = db
        .query_request(bounded_batch_query(MEMORY_DEDUP_SNAPSHOT, &BTreeMap::new()))
        .unwrap();
    assert_eq!(rows.rows.len(), 3);
    assert_eq!(rows.rows[0]["reviewed_at"], Value::Int(1800000000));
    assert_eq!(rows.rows[1]["reviewed_at"], Value::Int(1800000000));
    assert_eq!(rows.rows[2]["node_id"], Value::Int(2));
    assert_eq!(rows.rows[2]["id"], Value::Null);
    assert_eq!(rows.rows[2]["title"], Value::String("Idless memory".into()));
    assert_eq!(rows.rows[2]["reviewed_at"], Value::Null);
}

#[test]
fn memory_dedup_reviewed_rejects_empty_ids_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    let before = db.commit_epoch().unwrap();
    let error = batch_id_parameters(&["memory_1", ""]).unwrap_err();
    assert!(error.to_string().contains("non-empty id"));
    assert_eq!(db.commit_epoch().unwrap(), before);
}

#[test]
fn memory_dedup_review_queries_persist_as_one_wal_batch_and_replay() {
    let path = unique_test_dir("memory_dedup_review_query_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
        db.query("CREATE (:Memory {id: 'memory_2'})").unwrap();
        let batches_before = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        let mut parameters = batch_id_parameters(&["memory_1", "memory_2"]).unwrap();
        parameters.insert(
            "reviewed_at".into(),
            Value::String("2026-07-19T18:00:00Z".into()),
        );
        let mut transaction = db.begin_transaction().unwrap();
        let output = transaction
            .query_request(bounded_batch_query(MEMORY_DEDUP_REVIEW, &parameters))
            .unwrap();
        assert_eq!(output.rows.len(), 2);
        transaction.commit().unwrap();
        assert_eq!(
            read_test_wal(&path).unwrap().matches("\tbatch\t").count(),
            batches_before + 1
        );
    }
    assert!(read_test_wal(&path).unwrap().contains("set_node_property"));
    {
        let mut db = Database::open(&path).unwrap();
        let rows = db
            .query_request(bounded_batch_query(MEMORY_DEDUP_SNAPSHOT, &BTreeMap::new()))
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        for row in &rows.rows {
            assert_eq!(
                row["reviewed_at"],
                Value::String("2026-07-19T18:00:00Z".into())
            );
        }
    }
    std::fs::remove_dir_all(path).unwrap();
}

const MEMORY_DECAY_UPDATE: &str = "MATCH (m:Memory {id: $id}) \
    SET m.decay_score_cached = $decay RETURN id(m) AS node_id";
const MEMORY_DECAY_CONFIDENCE_UPDATE: &str = "MATCH (m:Memory {id: $id}) \
    SET m.decay_score_cached = $decay, m.confidence = $confidence RETURN id(m) AS node_id";
const MEMORY_DECAY_READ: &str = "MATCH (m:Memory) \
    RETURN id(m) AS node_id, m.id AS id, m.title AS title, \
    m.decay_score_cached AS decay, m.confidence AS confidence ORDER BY node_id";

fn memory_decay_parameters(
    id: &str,
    decay: Value,
    confidence: Option<Value>,
) -> Result<BTreeMap<String, Value>> {
    if id.is_empty() {
        return Err(HawDBError::Semantic("non-empty memory id required".into()));
    }
    for (name, value) in [
        ("decay score", Some(&decay)),
        ("confidence", confidence.as_ref()),
    ] {
        if let Some(value) = value {
            let valid = match value {
                Value::Int(_) => true,
                Value::Float(number) => number.is_finite(),
                _ => false,
            };
            if !valid {
                return Err(HawDBError::Semantic(format!(
                    "numeric finite {name} required"
                )));
            }
        }
    }
    let mut parameters = BTreeMap::from([
        ("id".into(), Value::String(id.into())),
        ("decay".into(), decay),
    ]);
    if let Some(confidence) = confidence {
        parameters.insert("confidence".into(), confidence);
    }
    Ok(parameters)
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
    let before = db.commit_epoch().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    let mut seen = BTreeSet::new();
    let mut counts = Vec::new();
    let mut duplicates = 0;
    for (id, decay, confidence) in [
        ("decay-refresh-memory-1", 0.42, Some(0.77)),
        ("decay-refresh-memory-2", 0.31, None),
        ("missing", 0.5, None),
        ("2", 0.6, Some(0.6)),
        ("decay-refresh-memory-1", 0.9, None),
    ] {
        let parameters =
            memory_decay_parameters(id, Value::Float(decay), confidence.map(Value::Float)).unwrap();
        // Repeated caller input uses the first assignment; matching remains in Cypher.
        if !seen.insert(id) {
            duplicates += 1;
            continue;
        }
        let query = if confidence.is_some() {
            MEMORY_DECAY_CONFIDENCE_UPDATE
        } else {
            MEMORY_DECAY_UPDATE
        };
        let output = transaction
            .query_request(bounded_batch_query(query, &parameters))
            .unwrap();
        counts.push(output.rows.len());
    }
    assert_eq!(counts, [1, 1, 0, 0]);
    assert_eq!(duplicates, 1);
    transaction.commit().unwrap();
    assert_eq!(before, 3);
    assert_eq!(db.commit_epoch().unwrap(), before + 1);
    let rows = db
        .query_request(bounded_batch_query(MEMORY_DECAY_READ, &BTreeMap::new()))
        .unwrap();
    assert_eq!(rows.rows.len(), 3);
    assert_eq!(
        rows.rows[0]["id"],
        Value::String("decay-refresh-memory-1".into())
    );
    assert_eq!(rows.rows[0]["decay"], Value::Float(0.42));
    assert_eq!(rows.rows[0]["confidence"], Value::Float(0.77));
    assert_eq!(
        rows.rows[1]["id"],
        Value::String("decay-refresh-memory-2".into())
    );
    assert_eq!(rows.rows[1]["decay"], Value::Float(0.31));
    assert_eq!(rows.rows[1]["confidence"], Value::Null);
    assert_eq!(rows.rows[2]["node_id"], Value::Int(2));
    assert_eq!(rows.rows[2]["id"], Value::Null);
    assert_eq!(rows.rows[2]["title"], Value::String("Idless memory".into()));
    assert_eq!(rows.rows[2]["decay"], Value::Null);
    assert_eq!(rows.rows[2]["confidence"], Value::Null);
}

#[test]
fn memory_decay_refresh_rejects_invalid_rows_before_wal() {
    let path = unique_test_dir("memory_decay_refresh_rejects_invalid_rows_before_wal");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'decay-refresh-memory-1', decay_score_cached: 1.0})")
            .unwrap();
        let before = db.commit_epoch().unwrap();
        let wal_before = read_test_wal(&path).unwrap();
        let error = memory_decay_parameters("", Value::Float(0.42), None).unwrap_err();
        assert!(error.to_string().contains("non-empty memory id"));
        for value in [Value::String("stale".into()), Value::Float(f64::NAN)] {
            let error = memory_decay_parameters("decay-refresh-memory-1", value, None).unwrap_err();
            assert!(error.to_string().contains("numeric finite decay score"));
        }
        for value in [Value::String("high".into()), Value::Float(f64::INFINITY)] {
            let error =
                memory_decay_parameters("decay-refresh-memory-1", Value::Float(0.42), Some(value))
                    .unwrap_err();
            assert!(error.to_string().contains("numeric finite confidence"));
        }
        assert_eq!(db.commit_epoch().unwrap(), before);
        assert_eq!(read_test_wal(&path).unwrap(), wal_before);
        let rows = db
            .query_request(bounded_batch_query(MEMORY_DECAY_READ, &BTreeMap::new()))
            .unwrap();
        assert_eq!(rows.rows[0]["decay"], Value::Float(1.0));
        assert_eq!(rows.rows[0]["confidence"], Value::Null);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn memory_decay_refresh_queries_persist_as_one_wal_batch_and_replay() {
    let path = unique_test_dir("memory_decay_refresh_query_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'decay-refresh-memory-1', decay_score_cached: 1.0, confidence: 0.1})").unwrap();
        db.query("CREATE (:Memory {id: 'decay-refresh-memory-2', decay_score_cached: 1.0})")
            .unwrap();
        let batches_before = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        let mut transaction = db.begin_transaction().unwrap();
        for (id, decay, confidence) in [
            ("decay-refresh-memory-1", 0.42, Some(0.77)),
            ("decay-refresh-memory-2", 0.31, None),
        ] {
            let parameters =
                memory_decay_parameters(id, Value::Float(decay), confidence.map(Value::Float))
                    .unwrap();
            let query = if confidence.is_some() {
                MEMORY_DECAY_CONFIDENCE_UPDATE
            } else {
                MEMORY_DECAY_UPDATE
            };
            let output = transaction
                .query_request(bounded_batch_query(query, &parameters))
                .unwrap();
            assert_eq!(output.rows.len(), 1);
        }
        transaction.commit().unwrap();
        assert_eq!(
            read_test_wal(&path).unwrap().matches("\tbatch\t").count(),
            batches_before + 1
        );
    }
    assert!(read_test_wal(&path).unwrap().contains("set_node_property"));
    {
        let mut db = Database::open(&path).unwrap();
        let rows = db
            .query_request(bounded_batch_query(MEMORY_DECAY_READ, &BTreeMap::new()))
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(rows.rows[0]["decay"], Value::Float(0.42));
        assert_eq!(rows.rows[0]["confidence"], Value::Float(0.77));
        assert_eq!(rows.rows[1]["decay"], Value::Float(0.31));
        assert_eq!(rows.rows[1]["confidence"], Value::Null);
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
        .query_request(bounded_batch_query(
            "MATCH (s:Source) WHERE s.original_name = 'Idless Source' RETURN id(s) AS id",
            &BTreeMap::new(),
        ))
        .unwrap();
    let idless_source_id = match idless.rows[0].get("id").unwrap() {
        Value::Int(id) => id.to_string(),
        other => panic!("expected projected id int, got {other:?}"),
    };

    let parameters = batch_id_parameters(&[
        "source_1",
        "missing",
        &idless_source_id,
        "source_2",
        "source_1",
    ])
    .unwrap();
    let before = db.commit_epoch().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    let candidates = transaction
        .query_request(bounded_batch_query(
            "MATCH (n:Source) WHERE n.id IN $ids RETURN n.id AS id ORDER BY id",
            &parameters,
        ))
        .unwrap();
    assert_eq!(candidates.rows.len(), 2);
    assert_eq!(candidates.rows[0]["id"], Value::String("source_1".into()));
    assert_eq!(candidates.rows[1]["id"], Value::String("source_2".into()));
    transaction
        .query_request(bounded_batch_query(
            "MATCH (n:Source) WHERE n.id IN $ids DETACH DELETE n",
            &parameters,
        ))
        .unwrap();
    let after = transaction
        .query_request(bounded_batch_query(
            "MATCH (n:Source) WHERE n.id IN $ids RETURN n.id AS id",
            &parameters,
        ))
        .unwrap();
    assert!(after.rows.is_empty());
    assert_eq!(transaction.commit().unwrap().rows.len(), 2);
    assert_eq!(before, 4);
    assert_eq!(db.commit_epoch().unwrap(), before + 1);
    let retained = db
        .query_request(bounded_batch_query(
            "MATCH (n:Source) WHERE n.id IS NULL RETURN n.original_name AS identity",
            &BTreeMap::new(),
        ))
        .unwrap();
    assert_eq!(retained.rows.len(), 1);
    assert_eq!(
        retained.rows[0]["identity"],
        Value::String("Idless Source".into())
    );

    let sources = db
        .query_request(bounded_batch_query(
            "MATCH (s:Source) WHERE s.id IS NOT NULL \
             RETURN s.id AS source_id ORDER BY source_id ASC",
            &BTreeMap::new(),
        ))
        .unwrap();
    assert_eq!(sources.rows.len(), 1);
    assert_eq!(
        sources.rows[0].get("source_id"),
        Some(&Value::String("source_3".to_string()))
    );
    assert_eq!(
        db.query_request(bounded_batch_query(
            "MATCH (m:Memory)-[r:SOURCED_FROM]->(s:Source) RETURN count(r) AS relationships",
            &BTreeMap::new()
        ))
        .unwrap()
        .rows[0]
            .get("relationships"),
        Some(&Value::Int(0))
    );
    assert_eq!(
        db.query_request(bounded_batch_query(
            "MATCH (m:Memory) RETURN count(m) AS memories",
            &BTreeMap::new()
        ))
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
    let graph_commit_epoch_before = db.runtime.get().unwrap().store.commit_epoch();

    let error = batch_id_parameters(&[""]).unwrap_err();

    assert!(error.to_string().contains("non-empty id"));
    assert_eq!(
        db.runtime.get().unwrap().store.commit_epoch(),
        graph_commit_epoch_before
    );
}

#[test]
fn source_delete_query_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("source_delete_query_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})-[:SOURCED_FROM]->(:Source {id: 'source_1'})")
            .unwrap();
        db.query("CREATE (:Source {id: 'source_2'})").unwrap();
        let batch_count_before_delete = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        let parameters = batch_id_parameters(&["source_1", "source_2"]).unwrap();
        let output = db
            .query_request(bounded_batch_query(
                "MATCH (n:Source) WHERE n.id IN $ids DETACH DELETE n",
                &parameters,
            ))
            .unwrap();
        assert_eq!(output.rows.len(), 2);
        let batch_count_after_delete = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_delete, batch_count_before_delete + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("delete_node"));
    {
        let db = Database::open(&path).unwrap();
        let parameters = batch_id_parameters(&["source_1", "source_2"]).unwrap();
        let sources = db
            .begin_read_transaction()
            .unwrap()
            .query_request(bounded_batch_query(
                "MATCH (n:Source) WHERE n.id IN $ids RETURN count(n) AS total",
                &parameters,
            ))
            .unwrap();
        assert_eq!(sources.rows[0].get("total"), Some(&Value::Int(0)));
        assert_eq!(
            db.begin_read_transaction()
                .unwrap()
                .query_request(bounded_batch_query(
                    "MATCH (n:Memory) WHERE n.id IN $ids RETURN n.id AS id",
                    &batch_id_parameters(&["memory_1"]).unwrap()
                ))
                .unwrap()
                .rows
                .len(),
            1
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
    let graph_commit_epoch = db.commit_epoch().unwrap();
    let mut read = db.begin_read_transaction().unwrap();
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
    let mut read = db.begin_read_transaction().unwrap();
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
    let stats = db.plan_cache_stats().unwrap();
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
fn deletes_skills_for_nowledge_detach_delete_shape() {
    let mut db = Database::new();
    db.query("CREATE (:Skill {id: 'skill_1'})-[:SYNTHESIZED_FROM]->(:Memory {id: 'memory_1'})")
        .unwrap();
    db.query("CREATE (:Skill {id: 'skill_2'})-[:SYNTHESIZED_FROM]->(:Memory {id: 'memory_2'})")
        .unwrap();
    db.query("CREATE (:Skill {id: 'skill_3'})").unwrap();
    db.query("CREATE (:Skill {title: 'Idless Skill'})").unwrap();
    let idless = db
        .query_request(bounded_batch_query(
            "MATCH (s:Skill) WHERE s.title = 'Idless Skill' RETURN id(s) AS id",
            &BTreeMap::new(),
        ))
        .unwrap();
    let idless_skill_id = match idless.rows[0].get("id").unwrap() {
        Value::Int(id) => id.to_string(),
        other => panic!("expected projected id int, got {other:?}"),
    };

    let parameters =
        batch_id_parameters(&["skill_1", "missing", &idless_skill_id, "skill_2", "skill_1"])
            .unwrap();
    let before = db.commit_epoch().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    let candidates = transaction
        .query_request(bounded_batch_query(
            "MATCH (n:Skill) WHERE n.id IN $ids RETURN n.id AS id ORDER BY id",
            &parameters,
        ))
        .unwrap();
    assert_eq!(candidates.rows.len(), 2);
    assert_eq!(candidates.rows[0]["id"], Value::String("skill_1".into()));
    assert_eq!(candidates.rows[1]["id"], Value::String("skill_2".into()));
    transaction
        .query_request(bounded_batch_query(
            "MATCH (n:Skill) WHERE n.id IN $ids DETACH DELETE n",
            &parameters,
        ))
        .unwrap();
    let after = transaction
        .query_request(bounded_batch_query(
            "MATCH (n:Skill) WHERE n.id IN $ids RETURN n.id AS id",
            &parameters,
        ))
        .unwrap();
    assert!(after.rows.is_empty());
    assert_eq!(transaction.commit().unwrap().rows.len(), 2);
    assert_eq!(before, 4);
    assert_eq!(db.commit_epoch().unwrap(), before + 1);
    let retained = db
        .query_request(bounded_batch_query(
            "MATCH (n:Skill) WHERE n.id IS NULL RETURN n.title AS identity",
            &BTreeMap::new(),
        ))
        .unwrap();
    assert_eq!(retained.rows.len(), 1);
    assert_eq!(
        retained.rows[0]["identity"],
        Value::String("Idless Skill".into())
    );

    let skills = db
        .query_request(bounded_batch_query(
            "MATCH (s:Skill) WHERE s.id IS NOT NULL \
             RETURN s.id AS skill_id ORDER BY skill_id ASC",
            &BTreeMap::new(),
        ))
        .unwrap();
    assert_eq!(skills.rows.len(), 1);
    assert_eq!(
        skills.rows[0].get("skill_id"),
        Some(&Value::String("skill_3".to_string()))
    );
    assert_eq!(
        db.query_request(bounded_batch_query(
            "MATCH (s:Skill)-[r:SYNTHESIZED_FROM]->(m:Memory) RETURN count(r) AS relationships",
            &BTreeMap::new()
        ))
        .unwrap()
        .rows[0]
            .get("relationships"),
        Some(&Value::Int(0))
    );
    assert_eq!(
        db.query_request(bounded_batch_query(
            "MATCH (m:Memory) RETURN count(m) AS memories",
            &BTreeMap::new()
        ))
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
    let graph_commit_epoch_before = db.runtime.get().unwrap().store.commit_epoch();

    let error = batch_id_parameters(&[""]).unwrap_err();

    assert!(error.to_string().contains("non-empty id"));
    assert_eq!(
        db.runtime.get().unwrap().store.commit_epoch(),
        graph_commit_epoch_before
    );
}

#[test]
fn skill_delete_query_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("skill_delete_query_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Skill {id: 'skill_1'})-[:SYNTHESIZED_FROM]->(:Memory {id: 'memory_1'})")
            .unwrap();
        db.query("CREATE (:Skill {id: 'skill_2'})").unwrap();
        let batch_count_before_delete = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        let parameters = batch_id_parameters(&["skill_1", "skill_2"]).unwrap();
        let output = db
            .query_request(bounded_batch_query(
                "MATCH (n:Skill) WHERE n.id IN $ids DETACH DELETE n",
                &parameters,
            ))
            .unwrap();
        assert_eq!(output.rows.len(), 2);
        let batch_count_after_delete = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_delete, batch_count_before_delete + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("delete_node"));
    {
        let db = Database::open(&path).unwrap();
        let parameters = batch_id_parameters(&["skill_1", "skill_2"]).unwrap();
        let skills = db
            .begin_read_transaction()
            .unwrap()
            .query_request(bounded_batch_query(
                "MATCH (n:Skill) WHERE n.id IN $ids RETURN count(n) AS total",
                &parameters,
            ))
            .unwrap();
        assert_eq!(skills.rows[0].get("total"), Some(&Value::Int(0)));
        assert_eq!(
            db.begin_read_transaction()
                .unwrap()
                .query_request(bounded_batch_query(
                    "MATCH (n:Memory) WHERE n.id IN $ids RETURN n.id AS id",
                    &batch_id_parameters(&["memory_1"]).unwrap()
                ))
                .unwrap()
                .rows
                .len(),
            1
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
        .query_request(bounded_batch_query(
            "MATCH (t:Thread) WHERE t.title = 'Idless Thread' RETURN id(t) AS id",
            &BTreeMap::new(),
        ))
        .unwrap();
    let idless_thread_id = match idless.rows[0].get("id").unwrap() {
        Value::Int(id) => id.to_string(),
        other => panic!("expected projected id int, got {other:?}"),
    };

    let parameters = batch_id_parameters(&[
        "thread_1",
        "missing",
        &idless_thread_id,
        "thread_2",
        "thread_1",
    ])
    .unwrap();
    let before = db.commit_epoch().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    let candidates = transaction
        .query_request(bounded_batch_query(
            "MATCH (n:Thread) WHERE n.id IN $ids RETURN n.id AS id ORDER BY id",
            &parameters,
        ))
        .unwrap();
    assert_eq!(candidates.rows.len(), 2);
    assert_eq!(candidates.rows[0]["id"], Value::String("thread_1".into()));
    assert_eq!(candidates.rows[1]["id"], Value::String("thread_2".into()));
    transaction
        .query_request(bounded_batch_query(
            "MATCH (n:Thread) WHERE n.id IN $ids DETACH DELETE n",
            &parameters,
        ))
        .unwrap();
    let after = transaction
        .query_request(bounded_batch_query(
            "MATCH (n:Thread) WHERE n.id IN $ids RETURN n.id AS id",
            &parameters,
        ))
        .unwrap();
    assert!(after.rows.is_empty());
    assert_eq!(transaction.commit().unwrap().rows.len(), 2);
    assert_eq!(before, 4);
    assert_eq!(db.commit_epoch().unwrap(), before + 1);
    let retained = db
        .query_request(bounded_batch_query(
            "MATCH (n:Thread) WHERE n.id IS NULL RETURN n.title AS identity",
            &BTreeMap::new(),
        ))
        .unwrap();
    assert_eq!(retained.rows.len(), 1);
    assert_eq!(
        retained.rows[0]["identity"],
        Value::String("Idless Thread".into())
    );

    for thread_id in ["thread_1", "thread_2"] {
        let parameters = batch_id_parameters(&[thread_id]).unwrap();
        assert!(db
            .begin_read_transaction()
            .unwrap()
            .query_request(bounded_batch_query(
                "MATCH (n:Thread) WHERE n.id IN $ids RETURN n.id AS id",
                &parameters,
            ))
            .unwrap()
            .rows
            .is_empty());
    }
    assert_eq!(
        db.query_request(bounded_batch_query(
            "MATCH (n:Thread) WHERE n.id IN $ids RETURN n.id AS id",
            &batch_id_parameters(&["thread_3"]).unwrap(),
        ))
        .unwrap()
        .rows
        .len(),
        1
    );
    assert_eq!(
        db.query_request(bounded_batch_query(
            "MATCH (t:Thread)-[r:CONTAINS]->(m:Message) RETURN count(r) AS relationships",
            &BTreeMap::new()
        ))
        .unwrap()
        .rows[0]
            .get("relationships"),
        Some(&Value::Int(0))
    );
    assert_eq!(
        db.query_request(bounded_batch_query(
            "MATCH (t:Thread)-[r:COMPACTS_TO]->(m:Memory) RETURN count(r) AS relationships",
            &BTreeMap::new()
        ))
        .unwrap()
        .rows[0]
            .get("relationships"),
        Some(&Value::Int(0))
    );
    assert_eq!(
        db.query_request(bounded_batch_query(
            "MATCH (m:Message) RETURN count(m) AS messages",
            &BTreeMap::new()
        ))
        .unwrap()
        .rows[0]
            .get("messages"),
        Some(&Value::Int(1))
    );
    assert_eq!(
        db.query_request(bounded_batch_query(
            "MATCH (m:Memory) RETURN count(m) AS memories",
            &BTreeMap::new()
        ))
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
    let graph_commit_epoch_before = db.runtime.get().unwrap().store.commit_epoch();

    let error = batch_id_parameters(&[""]).unwrap_err();

    assert!(error.to_string().contains("non-empty id"));
    assert_eq!(
        db.runtime.get().unwrap().store.commit_epoch(),
        graph_commit_epoch_before
    );
}

#[test]
fn thread_delete_query_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("thread_delete_query_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Thread {id: 'thread_1'})-[:CONTAINS]->(:Message {id: 'message_1'})")
            .unwrap();
        db.query("CREATE (:Thread {id: 'thread_2'})").unwrap();
        let batch_count_before_delete = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        let parameters = batch_id_parameters(&["thread_1", "thread_2"]).unwrap();
        let output = db
            .query_request(bounded_batch_query(
                "MATCH (n:Thread) WHERE n.id IN $ids DETACH DELETE n",
                &parameters,
            ))
            .unwrap();
        assert_eq!(output.rows.len(), 2);
        let batch_count_after_delete = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_delete, batch_count_before_delete + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("delete_node"));
    {
        let db = Database::open(&path).unwrap();
        for thread_id in ["thread_1", "thread_2"] {
            let parameters = batch_id_parameters(&[thread_id]).unwrap();
            assert!(db
                .begin_read_transaction()
                .unwrap()
                .query_request(bounded_batch_query(
                    "MATCH (n:Thread) WHERE n.id IN $ids RETURN n.id AS id",
                    &parameters,
                ))
                .unwrap()
                .rows
                .is_empty());
        }
        assert_eq!(
            db.begin_read_transaction()
                .unwrap()
                .query_request(bounded_batch_query(
                    "MATCH (n:Message) WHERE n.id IN $ids RETURN n.id AS id",
                    &batch_id_parameters(&["message_1"]).unwrap()
                ))
                .unwrap()
                .rows
                .len(),
            1
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn relational_insert_returning_reports_provisional_and_committed_outcomes() {
    let mut db = Database::new();
    let mut schema = db.begin_transaction().unwrap();
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

    let mut inserted = db.begin_transaction().unwrap();
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

    let mut duplicate = db.begin_transaction().unwrap();
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

    let mut local_duplicate = db.begin_transaction().unwrap();
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
        .unwrap()
        .query_sql("SELECT raw_turn_id FROM raw_turns WHERE org_id = 'org-2'")
        .expect("read after rollback")
        .rows
        .is_empty());

    let mut nullable = db.begin_transaction().unwrap();
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
        let mut schema = db.begin_transaction().unwrap();
        schema
            .query_sql(
                "CREATE TABLE raw_turns (\
                    raw_turn_id TEXT PRIMARY KEY, \
                    request_id TEXT UNIQUE NOT NULL\
                )",
            )
            .expect("stage raw turn table");
        schema.commit().expect("commit raw turn table");
        let mut insert = db.begin_transaction().unwrap();
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
        let mut duplicate = db.begin_transaction().unwrap();
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

    let mut duplicate = db.begin_transaction().unwrap();
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
        mutation_limits: hawdb_storage::mutation::MutationLimits {
            max_result_rows: NonZeroUsize::new(1).unwrap(),
            ..hawdb_storage::mutation::MutationLimits::default()
        },
        ..DatabaseConfig::default()
    });
    let mut schema = db.begin_transaction().unwrap();
    schema
        .query_sql("CREATE TABLE messages (id TEXT PRIMARY KEY)")
        .expect("stage messages table");
    schema.commit().expect("commit messages table");

    let mut tx = db.begin_transaction().unwrap();
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
        mutation_limits: hawdb_storage::mutation::MutationLimits {
            max_result_payload_bytes: NonZeroUsize::new(4).unwrap(),
            ..hawdb_storage::mutation::MutationLimits::default()
        },
        ..DatabaseConfig::default()
    });
    db.query_sql("CREATE TABLE messages (id TEXT PRIMARY KEY)")
        .expect("create messages table");

    let mut tx = db.begin_transaction().unwrap();
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
    let mut schema = db.begin_transaction().unwrap();
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

    let mut seed = db.begin_transaction().unwrap();
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

    let mut duplicate = db.begin_transaction().unwrap();
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
            .unwrap()
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
    let root = path.as_ref();
    if let Some(directory) = test_main_branch_directory(root) {
        let head =
            hawdb_storage::branch_head::read_branch_head(&directory.join("branch.head")).unwrap();
        return directory.join(hawdb_storage::artifact_files::wal_generation_file(
            head.active_wal.generation,
        ));
    }
    active_generation_path(root, "wal_generation", "wal")
}

fn active_checkpoint_path(path: impl AsRef<std::path::Path>) -> std::path::PathBuf {
    active_generation_path(path.as_ref(), "checkpoint_generation", "checkpoint")
}

fn test_main_branch_directory(root: &std::path::Path) -> Option<std::path::PathBuf> {
    match hawdb_storage::branch_project::inspect_project_manifest(root).unwrap() {
        hawdb_storage::branch_project::ProjectManifest::Branch(selector) => Some(
            root.join("branches")
                .join(selector.main_branch_id().as_uuid().to_string()),
        ),
        _ => None,
    }
}

pub(super) fn active_storage_root(root: &std::path::Path) -> std::path::PathBuf {
    test_main_branch_directory(root)
        .map_or_else(|| root.to_path_buf(), |branch| branch.join("data"))
}

fn active_runtime_manifest_path(root: &std::path::Path) -> std::path::PathBuf {
    test_main_branch_directory(root).map_or_else(
        || root.join("manifest.hawdb"),
        |branch| branch.join("runtime/manifest.hawdb"),
    )
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
    let manifest = std::fs::read_to_string(active_runtime_manifest_path(root)).unwrap();
    assert!(manifest.contains("HAWDB_MANIFEST_V1\n"));
    let generation = manifest.lines().find_map(|line| {
        let (field, value) = line.split_once('\t')?;
        (field == manifest_field && value != "none").then_some(value)
    });
    active_storage_root(root).join(format!(
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

#[cfg(all(any(unix, windows), feature = "test-support"))]
mod power_loss;

fn batch_id_parameters(ids: &[&str]) -> Result<BTreeMap<String, Value>> {
    if ids.iter().any(|id| id.is_empty()) {
        return Err(HawDBError::Semantic("non-empty id required".into()));
    }
    Ok(BTreeMap::from([(
        "ids".into(),
        Value::List(ids.iter().map(|id| Value::String((*id).into())).collect()),
    )]))
}

fn bounded_batch_query<'a>(
    query: &'a str,
    parameters: &'a BTreeMap<String, Value>,
) -> crate::QueryRequest<'a> {
    crate::QueryRequest::new(query)
        .with_params(parameters)
        .with_output_limits(QueryStreamOptions {
            max_rows: Some(5),
            max_payload_bytes: Some(4096),
        })
}

fn community_parameters(ids: Vec<Value>) -> Result<BTreeMap<String, Value>> {
    if ids.is_empty() {
        return Err(HawDBError::Semantic(
            "non-empty community ids required".into(),
        ));
    }
    if ids.contains(&Value::Null) {
        return Err(HawDBError::Semantic(
            "non-null community ids required".into(),
        ));
    }
    Ok(BTreeMap::from([
        ("community_ids".into(), Value::List(ids)),
        ("limit".into(), Value::Int(10)),
    ]))
}
