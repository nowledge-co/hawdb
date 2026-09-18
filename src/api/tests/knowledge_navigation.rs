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

use super::*;

#[test]
fn knowledge_entity_returns_none_for_missing_seed() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'entity_1', name: 'HawDB'})")
        .unwrap();

    let output = db
        .query_entity_via_cypher(&KnowledgeEntityRequest {
            label: "Entity".to_string(),
            external_id: "missing".to_string(),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch, 1);
    assert!(output.entity.is_none());
}

#[test]
fn knowledge_entity_uses_projected_identity_for_idless_nodes() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {name: 'Anonymous entity', kind: 'concept'})")
        .unwrap();

    let output = db
        .query_entity_via_cypher(&KnowledgeEntityRequest {
            label: "Entity".to_string(),
            external_id: "0".to_string(),
        })
        .unwrap();

    let entity = output.entity.expect("expected entity");
    assert_eq!(output.graph_commit_epoch, 1);
    assert_eq!(entity.node_id, 0);
    assert_eq!(entity.external_id.as_deref(), Some("0"));
    assert_eq!(
        entity.properties.get("name"),
        Some(&Value::String("Anonymous entity".to_string()))
    );
}

#[test]
fn induced_edge_read_rejects_empty_external_ids() {
    let db = Database::new();

    let empty_list_error = db
        .query_induced_edges_via_cypher(&KnowledgeInducedEdgeListRequest {
            external_ids: Vec::new(),
            limit: 10,
        })
        .unwrap_err();
    assert!(empty_list_error
        .to_string()
        .contains("non-empty external ids"));

    let empty_id_error = db
        .query_induced_edges_via_cypher(&KnowledgeInducedEdgeListRequest {
            external_ids: vec![String::new()],
            limit: 10,
        })
        .unwrap_err();
    assert!(empty_id_error
        .to_string()
        .contains("non-empty external ids"));
}

#[test]
fn knowledge_relationships_fail_soft_for_unknown_relationship_type() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'First'})")
        .unwrap();

    let output = db
        .query_relationships_via_cypher(&KnowledgeRelationshipsRequest {
            seeds: vec![KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            }],
            relationship_type: Some("DOES_NOT_EXIST".to_string()),
            direction: KnowledgeNeighborDirection::Both,
            limit_per_seed: 4,
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch, 1);
    assert!(!output.relationship_type_found);
    assert_eq!(output.groups.len(), 1);
    assert_eq!(output.found_seed_count, 0);
    assert_eq!(output.missing_seed_count, 0);
    assert_eq!(output.filtered_out_seed_count, 0);
    assert_eq!(output.relationship_count, 0);
    assert!(output.groups[0].relationships.is_empty());
}

#[test]
fn typed_knowledge_navigation_uses_projected_identity_for_idless_seed() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {title: 'Anonymous root'})-[:LINKS]->(:Entity {id: 'leaf', name: 'Leaf'})",
    )
    .unwrap();

    let neighbors = db
        .query_neighbors_via_cypher(&KnowledgeNeighborsRequest {
            label: "Memory".to_string(),
            external_id: "0".to_string(),
            relationship_type: Some("LINKS".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            limit: 4,
            max_hops: 1,
        })
        .unwrap();
    assert_eq!(neighbors.seed_node_id, Some(0));
    assert_eq!(neighbors.paths.len(), 1);
    assert_eq!(neighbors.paths[0].source_external_id.as_deref(), Some("0"));
    assert_eq!(
        neighbors.paths[0].target_external_id.as_deref(),
        Some("leaf")
    );

    let paths = db
        .query_paths_via_cypher(&KnowledgePathRequest {
            source_label: "Memory".to_string(),
            source_external_id: "0".to_string(),
            target_label: "Entity".to_string(),
            target_external_id: "leaf".to_string(),
            relationship_type: Some("LINKS".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            max_hops: 1,
            limit: 4,
        })
        .unwrap();
    assert_eq!(paths.source_node_id, Some(0));
    assert_eq!(paths.target_node_id, Some(1));
    assert_eq!(paths.paths.len(), 1);
    assert_eq!(
        paths.paths[0].segments[0].source_external_id.as_deref(),
        Some("0")
    );

    let subgraph = db
        .query_subgraph_via_cypher(&KnowledgeSubgraphRequest {
            label: "Memory".to_string(),
            external_id: "0".to_string(),
            relationship_type: Some("LINKS".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            max_hops: 1,
            node_limit: 4,
            relationship_limit: 4,
        })
        .unwrap();
    assert_eq!(subgraph.seed_node_id, Some(0));
    assert!(subgraph
        .nodes
        .iter()
        .any(|node| node.external_id.as_deref() == Some("0")));
    assert!(subgraph
        .nodes
        .iter()
        .any(|node| node.external_id.as_deref() == Some("leaf")));
}

#[test]
fn retrieves_knowledge_neighbors_without_search_projection() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS]->(:Entity {id: 'mid', name: 'Mid'})",
    )
    .unwrap();
    let leaf = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("leaf".to_string())),
                ("name".to_string(), Value::String("Leaf".to_string())),
            ]),
        )
        .unwrap();
    let mention = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("mention".to_string())),
                ("name".to_string(), Value::String("Mention".to_string())),
            ]),
        )
        .unwrap();
    db.store
        .create_relationship(
            &mut db.catalog,
            NodeId(1),
            leaf,
            "LINKS",
            BTreeMap::from([("weight".to_string(), Value::Int(2))]),
        )
        .unwrap();
    db.store
        .create_relationship(
            &mut db.catalog,
            mention,
            NodeId(0),
            "MENTIONS",
            BTreeMap::new(),
        )
        .unwrap();

    let outgoing = db
        .query_neighbors_via_cypher(&KnowledgeNeighborsRequest {
            label: "Memory".to_string(),
            external_id: "root".to_string(),
            relationship_type: Some("LINKS".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            limit: 8,
            max_hops: 2,
        })
        .unwrap();
    assert_eq!(outgoing.seed_node_id, Some(0));
    assert_eq!(outgoing.graph_commit_epoch, 5);
    assert_eq!(outgoing.paths.len(), 2);
    assert!(outgoing.diagnostics.seed_found);
    assert_eq!(outgoing.diagnostics.target_found, None);
    assert_eq!(outgoing.diagnostics.path_count, 2);
    assert_eq!(outgoing.diagnostics.node_count, 3);
    assert_eq!(outgoing.diagnostics.relationship_count, 2);
    assert_eq!(outgoing.diagnostics.fanout_reason_count, 0);
    assert_eq!(outgoing.diagnostics.max_hops, 2);
    assert_eq!(outgoing.diagnostics.path_limit, Some(8));
    assert_eq!(
        outgoing.diagnostics.input_candidate_set.representation,
        "traversal_seed_node_ids"
    );
    assert_eq!(outgoing.diagnostics.input_candidate_set.cardinality, 1);
    assert_eq!(
        outgoing.diagnostics.candidate_set.id_space,
        "canonical_graph_relationship_id"
    );
    assert_eq!(
        outgoing.diagnostics.candidate_set.representation,
        "neighbor_relationship_ids"
    );
    assert_eq!(outgoing.diagnostics.candidate_set.cardinality, 2);
    assert_eq!(
        outgoing
            .diagnostics
            .candidate_set
            .snapshot_source_graph_commit_epoch,
        Some(outgoing.graph_commit_epoch)
    );
    assert!(outgoing.fanout_reasons.is_empty());
    assert!(outgoing
        .paths
        .iter()
        .all(|path| path.relationship_type == "LINKS"));
    assert!(outgoing
        .paths
        .iter()
        .all(|path| { path.direction == KnowledgeGraphPathDirection::Outgoing }));
    assert!(outgoing.paths.iter().any(|path| path.hop == 2
        && path.source_external_id.as_deref() == Some("mid")
        && path.target_external_id.as_deref() == Some("leaf")
        && path.relationship_properties.get("weight") == Some(&Value::Int(2))));

    let incoming = db
        .query_neighbors_via_cypher(&KnowledgeNeighborsRequest {
            label: "Memory".to_string(),
            external_id: "root".to_string(),
            relationship_type: None,
            direction: KnowledgeNeighborDirection::Incoming,
            limit: 8,
            max_hops: 1,
        })
        .unwrap();
    assert_eq!(incoming.paths.len(), 1);
    assert_eq!(incoming.paths[0].relationship_type, "MENTIONS");
    assert_eq!(
        incoming.paths[0].source_external_id.as_deref(),
        Some("mention")
    );

    let unknown_type = db
        .query_neighbors_via_cypher(&KnowledgeNeighborsRequest {
            label: "Memory".to_string(),
            external_id: "root".to_string(),
            relationship_type: Some("DOES_NOT_EXIST".to_string()),
            direction: KnowledgeNeighborDirection::Both,
            limit: 8,
            max_hops: 2,
        })
        .unwrap();
    assert_eq!(unknown_type.seed_node_id, Some(0));
    assert!(unknown_type.paths.is_empty());
    assert!(unknown_type.diagnostics.seed_found);
    assert_eq!(unknown_type.diagnostics.path_count, 0);
    assert_eq!(unknown_type.diagnostics.node_count, 0);
    assert_eq!(unknown_type.diagnostics.relationship_count, 0);
    assert!(unknown_type.fanout_reasons.is_empty());
    assert_eq!(
        unknown_type.diagnostics.fallback_reasons,
        vec!["relationship type DOES_NOT_EXIST not found".to_string()]
    );
    assert_eq!(
        unknown_type.diagnostics.fallback_reason_codes,
        vec![KnowledgeTraversalFallbackReasonCode::RelationshipTypeNotFound]
    );
}

#[test]
fn scoped_knowledge_neighbors_filters_seed_by_metadata() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query(
        "CREATE (:Memory {id: 'root', title: 'Root', space_id: ''})-[:LINKS]->(:Entity {id: 'leaf', name: 'Leaf'})",
    )
    .unwrap();

    let scoped_request = KnowledgeScopedNeighborsRequest {
        navigation: KnowledgeNeighborsRequest {
            label: "Memory".to_string(),
            external_id: "root".to_string(),
            relationship_type: Some("LINKS".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            limit: 4,
            max_hops: 1,
        },
        metadata_filters: BTreeMap::from([("space_id".to_string(), "default".to_string())]),
    };
    let scoped = db
        .query_scoped_neighbors_via_cypher(&scoped_request)
        .unwrap();

    assert_eq!(scoped.paths.len(), 1);
    assert!(scoped.diagnostics.seed_found);
    assert_eq!(scoped.diagnostics.input_candidate_set.filtered_out_count, 0);
    assert_eq!(
        scoped
            .diagnostics
            .input_candidate_set
            .metadata_filters
            .get("space_id")
            .map(String::as_str),
        Some("default")
    );

    let stats = db.plan_cache_stats();
    let repeated_scoped = db
        .query_scoped_neighbors_via_cypher(&scoped_request)
        .unwrap();
    assert_eq!(repeated_scoped, scoped);
    let repeated_stats = db.plan_cache_stats();
    assert_eq!(repeated_stats.entries, stats.entries);
    assert_eq!(repeated_stats.misses, stats.misses);
    assert!(repeated_stats.hits > stats.hits);

    let filtered = db
        .query_scoped_neighbors_via_cypher(&KnowledgeScopedNeighborsRequest {
            navigation: KnowledgeNeighborsRequest {
                label: "Memory".to_string(),
                external_id: "root".to_string(),
                relationship_type: Some("LINKS".to_string()),
                direction: KnowledgeNeighborDirection::Outgoing,
                limit: 4,
                max_hops: 1,
            },
            metadata_filters: BTreeMap::from([("space_id".to_string(), "team".to_string())]),
        })
        .unwrap();

    assert_eq!(filtered.seed_node_id, Some(0));
    assert!(filtered.paths.is_empty());
    assert!(!filtered.diagnostics.seed_found);
    assert_eq!(
        filtered.diagnostics.input_candidate_set.filtered_out_count,
        1
    );
    assert_eq!(
        filtered
            .diagnostics
            .input_candidate_set
            .metadata_filters
            .get("space_id")
            .map(String::as_str),
        Some("team")
    );
}

#[test]
fn reads_induced_edges_for_nowledge_overview_and_subgraph_shapes() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();
    db.query("CREATE (:Entity {id: 'entity_2'})").unwrap();
    db.query("CREATE (:Source {id: 'source_outside'})").unwrap();
    db.query("MATCH (m:Memory {id: 'memory_1'}), (e:Entity {id: 'entity_1'}) CREATE (m)-[:MENTIONS {confidence: 0.8}]->(e)")
        .unwrap();
    db.query("MATCH (e1:Entity {id: 'entity_1'}), (e2:Entity {id: 'entity_2'}) CREATE (e1)-[:RELATES_TO {strength: 0.9}]->(e2)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory_1'}), (e:Entity {id: 'entity_2'}) CREATE (m)-[:RELATES_TO]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory_1'}), (s:Source {id: 'source_outside'}) CREATE (m)-[:SOURCED_FROM]->(s)")
        .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();

    let output = db
        .query_induced_edges_via_cypher(&KnowledgeInducedEdgeListRequest {
            external_ids: vec![
                "memory_1".to_string(),
                "entity_1".to_string(),
                "entity_2".to_string(),
                "missing".to_string(),
            ],
            limit: 0,
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);
    assert_eq!(output.matched_node_count, 3);
    assert_eq!(output.missing_external_ids, vec!["missing".to_string()]);
    assert_eq!(output.matched_count, 3);
    assert_eq!(output.returned_count, 3);
    assert_eq!(output.rows[0].source_id.as_deref(), Some("entity_1"));
    assert_eq!(output.rows[0].target_id.as_deref(), Some("entity_2"));
    assert_eq!(output.rows[0].relationship_type, "RELATES_TO");
    assert_eq!(output.rows[0].strength, Value::Float(0.9));
    assert_eq!(output.rows[1].source_id.as_deref(), Some("memory_1"));
    assert_eq!(output.rows[1].target_id.as_deref(), Some("entity_1"));
    assert_eq!(output.rows[1].relationship_type, "MENTIONS");
    assert_eq!(output.rows[1].strength, Value::Float(0.8));
    assert_eq!(output.rows[2].source_id.as_deref(), Some("memory_1"));
    assert_eq!(output.rows[2].target_id.as_deref(), Some("entity_2"));
    assert_eq!(output.rows[2].relationship_type, "RELATES_TO");
    assert_eq!(output.rows[2].strength, Value::Float(0.5));

    let limited_request = KnowledgeInducedEdgeListRequest {
        external_ids: vec![
            "memory_1".to_string(),
            "entity_1".to_string(),
            "entity_2".to_string(),
        ],
        limit: 2,
    };
    let limited = db.query_induced_edges_via_cypher(&limited_request).unwrap();
    assert_eq!(limited.matched_count, 3);
    assert_eq!(limited.returned_count, 2);

    let stats = db.plan_cache_stats();
    let repeated_limited = db.query_induced_edges_via_cypher(&limited_request).unwrap();
    assert_eq!(repeated_limited, limited);
    let repeated_stats = db.plan_cache_stats();
    assert_eq!(repeated_stats.entries, stats.entries);
    assert_eq!(repeated_stats.misses, stats.misses);
    assert!(repeated_stats.hits > stats.hits);
}

#[test]
fn retrieves_knowledge_relationships_grouped_by_seed() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query(
        "CREATE (:Memory {id: 'memory_1', title: 'First'})-[:HAS_LABEL {weight: 3}]->(:Label {id: 'label_1', name: 'Database'})",
    )
    .unwrap();
    db.query(
        "CREATE (:Memory {id: 'memory_2', title: 'Second'})-[:HAS_LABEL]->(:Label {id: 'label_2', name: 'Rust'})",
    )
    .unwrap();

    let request = KnowledgeRelationshipsRequest {
        seeds: vec![
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
        relationship_type: Some("HAS_LABEL".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        limit_per_seed: 4,
    };
    let output = db.query_relationships_via_cypher(&request).unwrap();

    assert_eq!(output.graph_commit_epoch, 2);
    assert!(output.relationship_type_found);
    assert_eq!(output.groups.len(), 3);
    assert_eq!(output.found_seed_count, 2);
    assert_eq!(output.missing_seed_count, 1);
    assert_eq!(output.filtered_out_seed_count, 0);
    assert_eq!(output.relationship_count, 2);
    assert_eq!(output.groups[0].seed.external_id, "memory_2");
    assert_eq!(output.groups[0].relationships.len(), 1);
    assert_eq!(
        output.groups[0].relationships[0]
            .target_external_id
            .as_deref(),
        Some("label_2")
    );
    assert_eq!(output.groups[1].seed.external_id, "missing");
    assert!(output.groups[1].relationships.is_empty());
    assert_eq!(output.groups[2].relationships.len(), 1);
    assert_eq!(
        output.groups[2].relationships[0]
            .relationship_properties
            .get("weight"),
        Some(&Value::Int(3))
    );

    let stats = db.plan_cache_stats();
    let repeated_output = db.query_relationships_via_cypher(&request).unwrap();
    assert_eq!(repeated_output, output);
    let repeated_stats = db.plan_cache_stats();
    assert_eq!(repeated_stats.entries, stats.entries);
    assert_eq!(repeated_stats.misses, stats.misses);
    assert!(repeated_stats.hits > stats.hits);
}

#[test]
fn scoped_knowledge_relationships_report_filtered_seeds() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query(
        "CREATE (:Memory {id: 'memory_1', title: 'First', source_id: 'thread_1', space_id: ''})-[:HAS_LABEL]->(:Label {id: 'label_1', name: 'Database'})",
    )
    .unwrap();
    db.query(
        "CREATE (:Memory {id: 'memory_2', title: 'Second', source_id: 'thread_2', space_id: 'default'})-[:HAS_LABEL]->(:Label {id: 'label_2', name: 'Rust'})",
    )
    .unwrap();

    let request = KnowledgeScopedRelationshipsRequest {
        relationships: KnowledgeRelationshipsRequest {
            seeds: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_2".to_string(),
                },
            ],
            relationship_type: Some("HAS_LABEL".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            limit_per_seed: 4,
        },
        metadata_filters: BTreeMap::from([
            ("source_id".to_string(), "thread_1".to_string()),
            ("space_id".to_string(), "default".to_string()),
        ]),
    };
    let output = db.query_scoped_relationships_via_cypher(&request).unwrap();

    assert_eq!(output.graph_commit_epoch, 2);
    assert!(output.relationship_type_found);
    assert_eq!(output.found_seed_count, 1);
    assert_eq!(output.missing_seed_count, 0);
    assert_eq!(output.filtered_out_seed_count, 1);
    assert_eq!(output.relationship_count, 1);
    assert!(!output.groups[0].filtered_out);
    assert_eq!(output.groups[0].relationships.len(), 1);
    assert!(output.groups[1].filtered_out);
    assert!(output.groups[1].relationships.is_empty());

    let stats = db.plan_cache_stats();
    let repeated_output = db.query_scoped_relationships_via_cypher(&request).unwrap();
    assert_eq!(repeated_output, output);
    let repeated_stats = db.plan_cache_stats();
    assert_eq!(repeated_stats.entries, stats.entries);
    assert_eq!(repeated_stats.misses, stats.misses);
    assert!(repeated_stats.hits > stats.hits);
}

#[test]
fn knowledge_neighbors_reports_limit_and_missing_seed() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS]->(:Entity {id: 'left', name: 'Left'})")
            .unwrap();
    let right = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("right".to_string())),
                ("name".to_string(), Value::String("Right".to_string())),
            ]),
        )
        .unwrap();
    db.store
        .create_relationship(&mut db.catalog, NodeId(0), right, "LINKS", BTreeMap::new())
        .unwrap();

    let limited_request = KnowledgeNeighborsRequest {
        label: "Memory".to_string(),
        external_id: "root".to_string(),
        relationship_type: None,
        direction: KnowledgeNeighborDirection::Both,
        limit: 1,
        max_hops: 1,
    };
    let limited = db.query_neighbors_via_cypher(&limited_request).unwrap();
    assert_eq!(limited.paths.len(), 1);
    assert!(limited.diagnostics.seed_found);
    assert_eq!(limited.diagnostics.path_count, 1);
    assert_eq!(limited.diagnostics.node_count, 2);
    assert_eq!(limited.diagnostics.relationship_count, 1);
    assert_eq!(limited.diagnostics.fanout_reason_count, 1);
    assert!(limited.diagnostics.fallback_reasons.is_empty());
    assert_eq!(limited.diagnostics.path_limit, Some(1));
    assert_eq!(limited.fanout_reasons.len(), 1);
    assert!(limited.fanout_reasons[0].contains("knowledge_neighbors limit 1"));
    assert_eq!(
        limited.fanout_reason_codes,
        vec![KnowledgeFanoutReasonCode::PathLimitReached]
    );
    assert_eq!(
        limited.diagnostics.fanout_reason_codes,
        limited.fanout_reason_codes
    );
    assert_eq!(limited.diagnostics.fanout_reasons, limited.fanout_reasons);

    let stats = db.plan_cache_stats();
    let repeated_limited = db.query_neighbors_via_cypher(&limited_request).unwrap();
    assert_eq!(repeated_limited, limited);
    let repeated_stats = db.plan_cache_stats();
    assert_eq!(repeated_stats.entries, stats.entries);
    assert_eq!(repeated_stats.misses, stats.misses);
    assert!(repeated_stats.hits > stats.hits);

    let disabled = db
        .query_neighbors_via_cypher(&KnowledgeNeighborsRequest {
            label: "Memory".to_string(),
            external_id: "root".to_string(),
            relationship_type: None,
            direction: KnowledgeNeighborDirection::Both,
            limit: 0,
            max_hops: 1,
        })
        .unwrap();
    assert!(disabled.paths.is_empty());
    assert_eq!(disabled.diagnostics.fanout_reason_count, 1);
    assert_eq!(
        disabled.diagnostics.fallback_reasons,
        vec!["path traversal disabled by limit 0".to_string()]
    );
    assert_eq!(
        disabled.diagnostics.fallback_reason_codes,
        vec![KnowledgeTraversalFallbackReasonCode::PathLimitZero]
    );
    assert_eq!(disabled.diagnostics.fanout_reasons, disabled.fanout_reasons);

    let missing = db
        .query_neighbors_via_cypher(&KnowledgeNeighborsRequest {
            label: "Memory".to_string(),
            external_id: "missing".to_string(),
            relationship_type: None,
            direction: KnowledgeNeighborDirection::Both,
            limit: 8,
            max_hops: 1,
        })
        .unwrap();
    assert_eq!(missing.seed_node_id, None);
    assert!(!missing.diagnostics.seed_found);
    assert_eq!(missing.diagnostics.path_count, 0);
    assert_eq!(missing.diagnostics.fanout_reason_count, 0);
    assert_eq!(
        missing.diagnostics.fallback_reasons,
        vec!["seed Memory:missing not found".to_string()]
    );
    assert_eq!(
        missing.diagnostics.fallback_reason_codes,
        vec![KnowledgeTraversalFallbackReasonCode::SeedNotFound]
    );
    assert_eq!(missing.diagnostics.path_limit, Some(8));
    assert!(missing.paths.is_empty());
    assert!(missing.fanout_reasons.is_empty());
}

#[test]
fn typed_knowledge_navigation_reports_dense_adjacency_groups() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root', title: 'Root'})")
        .unwrap();
    for index in 0..DENSE_ADJACENCY_DEGREE_THRESHOLD {
        let target = db
            .store
            .create_node(
                &mut db.catalog,
                "Entity",
                BTreeMap::from([
                    ("id".to_string(), Value::String(format!("entity-{index}"))),
                    ("name".to_string(), Value::String(format!("Entity {index}"))),
                ]),
            )
            .unwrap();
        db.store
            .create_relationship(&mut db.catalog, NodeId(0), target, "LINKS", BTreeMap::new())
            .unwrap();
    }

    let neighbors = db
        .query_neighbors_via_cypher(&KnowledgeNeighborsRequest {
            label: "Memory".to_string(),
            external_id: "root".to_string(),
            relationship_type: Some("LINKS".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            limit: DENSE_ADJACENCY_DEGREE_THRESHOLD,
            max_hops: 1,
        })
        .unwrap();
    assert_eq!(neighbors.paths.len(), DENSE_ADJACENCY_DEGREE_THRESHOLD);
    assert_eq!(neighbors.diagnostics.fanout_reason_count, 1);
    assert_eq!(neighbors.fanout_reasons.len(), 1);
    assert!(neighbors.fanout_reasons[0]
        .contains("knowledge_neighbors dense_adjacency LINKS outgoing node 0 degree"));
    assert_eq!(
        neighbors.fanout_reason_codes,
        vec![KnowledgeFanoutReasonCode::DenseAdjacency]
    );
    assert_eq!(
        neighbors.fanout_reason_details[0].operation.as_deref(),
        Some("knowledge_neighbors")
    );
    assert_eq!(
        neighbors.fanout_reason_details[0]
            .relationship_type
            .as_deref(),
        Some("LINKS")
    );
    assert_eq!(
        neighbors.fanout_reason_details[0].direction.as_deref(),
        Some("outgoing")
    );
    assert_eq!(neighbors.fanout_reason_details[0].node_id, Some(0));
    assert_eq!(
        neighbors.fanout_reason_details[0].degree,
        Some(DENSE_ADJACENCY_DEGREE_THRESHOLD)
    );
    assert_eq!(
        neighbors.diagnostics.fanout_reason_codes,
        neighbors.fanout_reason_codes
    );
    assert_eq!(
        neighbors.diagnostics.fanout_reason_details,
        neighbors.fanout_reason_details
    );
    assert_eq!(
        neighbors.diagnostics.fanout_reasons,
        neighbors.fanout_reasons
    );

    let untyped_neighbors = db
        .query_neighbors_via_cypher(&KnowledgeNeighborsRequest {
            label: "Memory".to_string(),
            external_id: "root".to_string(),
            relationship_type: None,
            direction: KnowledgeNeighborDirection::Outgoing,
            limit: DENSE_ADJACENCY_DEGREE_THRESHOLD,
            max_hops: 1,
        })
        .unwrap();
    assert_eq!(
        untyped_neighbors.paths.len(),
        DENSE_ADJACENCY_DEGREE_THRESHOLD
    );
    assert_eq!(untyped_neighbors.diagnostics.fanout_reason_count, 1);
    assert!(untyped_neighbors.fanout_reasons[0]
        .contains("knowledge_neighbors dense_adjacency LINKS outgoing node 0 degree"));
    assert_eq!(
        untyped_neighbors.fanout_reason_codes,
        vec![KnowledgeFanoutReasonCode::DenseAdjacency]
    );

    let subgraph = db
        .query_subgraph_via_cypher(&KnowledgeSubgraphRequest {
            label: "Memory".to_string(),
            external_id: "root".to_string(),
            relationship_type: Some("LINKS".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            max_hops: 1,
            node_limit: DENSE_ADJACENCY_DEGREE_THRESHOLD + 1,
            relationship_limit: DENSE_ADJACENCY_DEGREE_THRESHOLD,
        })
        .unwrap();
    assert_eq!(
        subgraph.relationships.len(),
        DENSE_ADJACENCY_DEGREE_THRESHOLD
    );
    assert_eq!(subgraph.diagnostics.fanout_reason_count, 1);
    assert_eq!(subgraph.fanout_reasons.len(), 1);
    assert!(subgraph.fanout_reasons[0]
        .contains("knowledge_subgraph dense_adjacency LINKS outgoing node 0 degree"));
}

#[test]
fn retrieves_bounded_knowledge_paths_without_search_projection() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {weight: 1}]->(:Entity {id: 'mid', name: 'Mid'})",
    )
    .unwrap();
    let leaf = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("leaf".to_string())),
                ("name".to_string(), Value::String("Leaf".to_string())),
            ]),
        )
        .unwrap();
    db.store
        .create_relationship(
            &mut db.catalog,
            NodeId(1),
            leaf,
            "LINKS",
            BTreeMap::from([("weight".to_string(), Value::Int(2))]),
        )
        .unwrap();

    let output = db
        .query_paths_via_cypher(&KnowledgePathRequest {
            source_label: "Memory".to_string(),
            source_external_id: "root".to_string(),
            target_label: "Entity".to_string(),
            target_external_id: "leaf".to_string(),
            relationship_type: Some("LINKS".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            max_hops: 2,
            limit: 4,
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch, 3);
    assert_eq!(output.source_node_id, Some(0));
    assert_eq!(output.target_node_id, Some(2));
    assert_eq!(output.paths.len(), 1);
    assert!(output.diagnostics.seed_found);
    assert_eq!(output.diagnostics.target_found, Some(true));
    assert_eq!(output.diagnostics.path_count, 1);
    assert_eq!(output.diagnostics.node_count, 3);
    assert_eq!(output.diagnostics.relationship_count, 2);
    assert_eq!(output.diagnostics.fanout_reason_count, 0);
    assert_eq!(output.diagnostics.max_hops, 2);
    assert_eq!(output.diagnostics.path_limit, Some(4));
    assert_eq!(
        output.diagnostics.input_candidate_set.representation,
        "traversal_seed_node_ids"
    );
    assert_eq!(output.diagnostics.input_candidate_set.cardinality, 2);
    assert_eq!(
        output.diagnostics.candidate_set.id_space,
        "canonical_graph_path"
    );
    assert_eq!(
        output.diagnostics.candidate_set.representation,
        "bounded_paths"
    );
    assert_eq!(output.diagnostics.candidate_set.cardinality, 1);
    assert_eq!(
        output
            .diagnostics
            .input_candidate_set
            .snapshot_source_graph_commit_epoch,
        Some(output.graph_commit_epoch)
    );
    assert!(output.fanout_reasons.is_empty());
    let path = &output.paths[0];
    assert_eq!(path.segments.len(), 2);
    assert_eq!(path.segments[0].source_external_id.as_deref(), Some("root"));
    assert_eq!(path.segments[0].target_external_id.as_deref(), Some("mid"));
    assert_eq!(
        path.segments[0].relationship_properties.get("weight"),
        Some(&Value::Int(1))
    );
    assert_eq!(path.segments[1].source_external_id.as_deref(), Some("mid"));
    assert_eq!(path.segments[1].target_external_id.as_deref(), Some("leaf"));
    assert_eq!(
        path.segments[1].relationship_properties.get("weight"),
        Some(&Value::Int(2))
    );
    assert!(path
        .segments
        .iter()
        .all(|segment| segment.relationship_type == "LINKS"));
}

#[test]
fn scoped_knowledge_paths_filter_source_and_target_by_metadata() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query(
        "CREATE (:Memory {id: 'root', title: 'Root', source_id: 'thread_1'})-[:LINKS]->(:Entity {id: 'leaf', name: 'Leaf', space_id: 'default'})",
    )
    .unwrap();

    let scoped_request = KnowledgeScopedPathRequest {
        navigation: KnowledgePathRequest {
            source_label: "Memory".to_string(),
            source_external_id: "root".to_string(),
            target_label: "Entity".to_string(),
            target_external_id: "leaf".to_string(),
            relationship_type: Some("LINKS".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            max_hops: 1,
            limit: 4,
        },
        source_metadata_filters: BTreeMap::from([(
            "source_id".to_string(),
            "thread_1".to_string(),
        )]),
        target_metadata_filters: BTreeMap::from([("space_id".to_string(), "default".to_string())]),
    };
    let scoped = db.query_scoped_paths_via_cypher(&scoped_request).unwrap();

    assert_eq!(scoped.paths.len(), 1);
    assert!(scoped.diagnostics.seed_found);
    assert_eq!(scoped.diagnostics.target_found, Some(true));
    assert_eq!(scoped.diagnostics.input_candidate_set.filtered_out_count, 0);
    assert_eq!(
        scoped
            .diagnostics
            .input_candidate_set
            .metadata_filters
            .get("source.source_id")
            .map(String::as_str),
        Some("thread_1")
    );
    assert_eq!(
        scoped
            .diagnostics
            .input_candidate_set
            .metadata_filters
            .get("target.space_id")
            .map(String::as_str),
        Some("default")
    );

    let stats = db.plan_cache_stats();
    let repeated_scoped = db.query_scoped_paths_via_cypher(&scoped_request).unwrap();
    assert_eq!(repeated_scoped, scoped);
    let repeated_stats = db.plan_cache_stats();
    assert_eq!(repeated_stats.entries, stats.entries);
    assert_eq!(repeated_stats.misses, stats.misses);
    assert!(repeated_stats.hits > stats.hits);

    let filtered = db
        .query_scoped_paths_via_cypher(&KnowledgeScopedPathRequest {
            navigation: KnowledgePathRequest {
                source_label: "Memory".to_string(),
                source_external_id: "root".to_string(),
                target_label: "Entity".to_string(),
                target_external_id: "leaf".to_string(),
                relationship_type: Some("LINKS".to_string()),
                direction: KnowledgeNeighborDirection::Outgoing,
                max_hops: 1,
                limit: 4,
            },
            source_metadata_filters: BTreeMap::from([(
                "source_id".to_string(),
                "thread_1".to_string(),
            )]),
            target_metadata_filters: BTreeMap::from([(
                "space_id".to_string(),
                "archive".to_string(),
            )]),
        })
        .unwrap();

    assert_eq!(filtered.source_node_id, Some(0));
    assert_eq!(filtered.target_node_id, Some(1));
    assert!(filtered.diagnostics.seed_found);
    assert_eq!(filtered.diagnostics.target_found, Some(false));
    assert!(filtered.paths.is_empty());
    assert_eq!(
        filtered.diagnostics.input_candidate_set.filtered_out_count,
        1
    );
    assert_eq!(
        filtered
            .diagnostics
            .input_candidate_set
            .metadata_filters
            .get("target.space_id")
            .map(String::as_str),
        Some("archive")
    );
    assert!(filtered.diagnostics.fallback_reasons.is_empty());
}

#[test]
fn knowledge_paths_respects_direction_type_limit_and_missing_endpoint() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS]->(:Entity {id: 'left', name: 'Left'})")
            .unwrap();
    let right = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("right".to_string())),
                ("name".to_string(), Value::String("Right".to_string())),
            ]),
        )
        .unwrap();
    db.store
        .create_relationship(&mut db.catalog, NodeId(0), right, "LINKS", BTreeMap::new())
        .unwrap();
    db.store
        .create_relationship(
            &mut db.catalog,
            NodeId(0),
            right,
            "MENTIONS",
            BTreeMap::new(),
        )
        .unwrap();

    let wrong_direction = db
        .query_paths_via_cypher(&KnowledgePathRequest {
            source_label: "Memory".to_string(),
            source_external_id: "root".to_string(),
            target_label: "Entity".to_string(),
            target_external_id: "right".to_string(),
            relationship_type: Some("LINKS".to_string()),
            direction: KnowledgeNeighborDirection::Incoming,
            max_hops: 1,
            limit: 4,
        })
        .unwrap();
    assert!(wrong_direction.paths.is_empty());
    assert!(wrong_direction.diagnostics.seed_found);
    assert_eq!(wrong_direction.diagnostics.target_found, Some(true));
    assert_eq!(wrong_direction.diagnostics.path_count, 0);

    let unknown_type = db
        .query_paths_via_cypher(&KnowledgePathRequest {
            source_label: "Memory".to_string(),
            source_external_id: "root".to_string(),
            target_label: "Entity".to_string(),
            target_external_id: "right".to_string(),
            relationship_type: Some("DOES_NOT_EXIST".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            max_hops: 1,
            limit: 4,
        })
        .unwrap();
    assert!(unknown_type.paths.is_empty());
    assert!(unknown_type.diagnostics.seed_found);
    assert_eq!(unknown_type.diagnostics.target_found, Some(true));
    assert_eq!(
        unknown_type.diagnostics.fallback_reasons,
        vec!["relationship type DOES_NOT_EXIST not found".to_string()]
    );
    assert_eq!(
        unknown_type.diagnostics.fallback_reason_codes,
        vec![KnowledgeTraversalFallbackReasonCode::RelationshipTypeNotFound]
    );

    let limited_request = KnowledgePathRequest {
        source_label: "Memory".to_string(),
        source_external_id: "root".to_string(),
        target_label: "Entity".to_string(),
        target_external_id: "right".to_string(),
        relationship_type: None,
        direction: KnowledgeNeighborDirection::Outgoing,
        max_hops: 1,
        limit: 1,
    };
    let limited = db.query_paths_via_cypher(&limited_request).unwrap();
    assert_eq!(limited.paths.len(), 1);
    assert_eq!(limited.diagnostics.path_count, 1);
    assert_eq!(limited.diagnostics.node_count, 2);
    assert_eq!(limited.diagnostics.relationship_count, 1);
    assert_eq!(limited.diagnostics.fanout_reason_count, 1);
    assert!(limited.diagnostics.fallback_reasons.is_empty());
    assert_eq!(limited.diagnostics.path_limit, Some(1));
    assert_eq!(limited.fanout_reasons.len(), 1);
    assert!(limited.fanout_reasons[0].contains("knowledge_paths limit 1"));
    assert_eq!(
        limited.fanout_reason_codes,
        vec![KnowledgeFanoutReasonCode::PathLimitReached]
    );
    assert_eq!(
        limited.diagnostics.fanout_reason_codes,
        limited.fanout_reason_codes
    );
    assert_eq!(limited.diagnostics.fanout_reasons, limited.fanout_reasons);

    let stats = db.plan_cache_stats();
    let repeated_limited = db.query_paths_via_cypher(&limited_request).unwrap();
    assert_eq!(repeated_limited, limited);
    let repeated_stats = db.plan_cache_stats();
    assert_eq!(repeated_stats.entries, stats.entries);
    assert_eq!(repeated_stats.misses, stats.misses);
    assert!(repeated_stats.hits > stats.hits);

    let disabled = db
        .query_paths_via_cypher(&KnowledgePathRequest {
            source_label: "Memory".to_string(),
            source_external_id: "root".to_string(),
            target_label: "Entity".to_string(),
            target_external_id: "right".to_string(),
            relationship_type: None,
            direction: KnowledgeNeighborDirection::Outgoing,
            max_hops: 0,
            limit: 4,
        })
        .unwrap();
    assert!(disabled.paths.is_empty());
    assert_eq!(disabled.diagnostics.fanout_reason_count, 0);
    assert_eq!(
        disabled.diagnostics.fallback_reasons,
        vec!["traversal disabled by max_hops 0".to_string()]
    );
    assert_eq!(
        disabled.diagnostics.fallback_reason_codes,
        vec![KnowledgeTraversalFallbackReasonCode::MaxHopsZero]
    );

    let missing = db
        .query_paths_via_cypher(&KnowledgePathRequest {
            source_label: "Memory".to_string(),
            source_external_id: "root".to_string(),
            target_label: "Entity".to_string(),
            target_external_id: "missing".to_string(),
            relationship_type: None,
            direction: KnowledgeNeighborDirection::Both,
            max_hops: 2,
            limit: 4,
        })
        .unwrap();
    assert_eq!(missing.source_node_id, Some(0));
    assert_eq!(missing.target_node_id, None);
    assert!(missing.diagnostics.seed_found);
    assert_eq!(missing.diagnostics.target_found, Some(false));
    assert_eq!(missing.diagnostics.path_count, 0);
    assert_eq!(
        missing.diagnostics.fallback_reasons,
        vec!["target Entity:missing not found".to_string()]
    );
    assert_eq!(
        missing.diagnostics.fallback_reason_codes,
        vec![KnowledgeTraversalFallbackReasonCode::TargetNotFound]
    );
    assert!(missing.paths.is_empty());

    let missing_source = db
        .query_paths_via_cypher(&KnowledgePathRequest {
            source_label: "Memory".to_string(),
            source_external_id: "missing".to_string(),
            target_label: "Entity".to_string(),
            target_external_id: "right".to_string(),
            relationship_type: None,
            direction: KnowledgeNeighborDirection::Both,
            max_hops: 2,
            limit: 4,
        })
        .unwrap();
    assert_eq!(missing_source.source_node_id, None);
    assert_eq!(missing_source.target_node_id, Some(2));
    assert!(!missing_source.diagnostics.seed_found);
    assert_eq!(missing_source.diagnostics.target_found, Some(true));
    assert_eq!(missing_source.diagnostics.path_count, 0);
    assert_eq!(
        missing_source.diagnostics.fallback_reasons,
        vec!["seed Memory:missing not found".to_string()]
    );
    assert_eq!(
        missing_source.diagnostics.fallback_reason_codes,
        vec![KnowledgeTraversalFallbackReasonCode::SeedNotFound]
    );
    assert!(missing_source.paths.is_empty());

    let missing_both = db
        .query_paths_via_cypher(&KnowledgePathRequest {
            source_label: "Memory".to_string(),
            source_external_id: "missing-source".to_string(),
            target_label: "Entity".to_string(),
            target_external_id: "missing-target".to_string(),
            relationship_type: None,
            direction: KnowledgeNeighborDirection::Both,
            max_hops: 2,
            limit: 4,
        })
        .unwrap();
    assert_eq!(missing_both.source_node_id, None);
    assert_eq!(missing_both.target_node_id, None);
    assert!(!missing_both.diagnostics.seed_found);
    assert_eq!(missing_both.diagnostics.target_found, Some(false));
    assert_eq!(missing_both.diagnostics.path_count, 0);
    assert_eq!(
        missing_both.diagnostics.fallback_reasons,
        vec![
            "seed Memory:missing-source not found".to_string(),
            "target Entity:missing-target not found".to_string()
        ]
    );
    assert_eq!(
        missing_both.diagnostics.fallback_reason_codes,
        vec![
            KnowledgeTraversalFallbackReasonCode::SeedNotFound,
            KnowledgeTraversalFallbackReasonCode::TargetNotFound
        ]
    );
    assert!(missing_both.paths.is_empty());
}

#[test]
fn retrieves_bounded_knowledge_subgraph_without_search_projection() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS]->(:Entity {id: 'mid', name: 'Mid'})",
    )
    .unwrap();
    let leaf = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("leaf".to_string())),
                ("name".to_string(), Value::String("Leaf".to_string())),
            ]),
        )
        .unwrap();
    let mention = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("mention".to_string())),
                ("name".to_string(), Value::String("Mention".to_string())),
            ]),
        )
        .unwrap();
    db.store
        .create_relationship(&mut db.catalog, NodeId(1), leaf, "LINKS", BTreeMap::new())
        .unwrap();
    db.store
        .create_relationship(
            &mut db.catalog,
            NodeId(0),
            mention,
            "MENTIONS",
            BTreeMap::new(),
        )
        .unwrap();

    let output = db
        .query_subgraph_via_cypher(&KnowledgeSubgraphRequest {
            label: "Memory".to_string(),
            external_id: "root".to_string(),
            relationship_type: Some("LINKS".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            max_hops: 2,
            node_limit: 8,
            relationship_limit: 8,
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch, 5);
    assert_eq!(output.seed_node_id, Some(0));
    assert_eq!(output.nodes.len(), 3);
    assert_eq!(output.relationships.len(), 2);
    assert!(output.diagnostics.seed_found);
    assert_eq!(output.diagnostics.target_found, None);
    assert_eq!(output.diagnostics.node_count, 3);
    assert_eq!(output.diagnostics.relationship_count, 2);
    assert_eq!(output.diagnostics.path_count, 2);
    assert_eq!(output.diagnostics.fanout_reason_count, 0);
    assert_eq!(output.diagnostics.max_hops, 2);
    assert_eq!(output.diagnostics.node_limit, Some(8));
    assert_eq!(output.diagnostics.relationship_limit, Some(8));
    assert_eq!(
        output.diagnostics.input_candidate_set.representation,
        "traversal_seed_node_ids"
    );
    assert_eq!(output.diagnostics.input_candidate_set.cardinality, 1);
    assert_eq!(output.diagnostics.candidate_set.id_space, "mixed_graph_id");
    assert_eq!(
        output.diagnostics.candidate_set.representation,
        "subgraph_node_and_relationship_ids"
    );
    assert_eq!(output.diagnostics.candidate_set.cardinality, 5);
    assert_eq!(
        output
            .diagnostics
            .candidate_set
            .snapshot_source_graph_commit_epoch,
        Some(output.graph_commit_epoch)
    );
    assert!(output.fanout_reasons.is_empty());
    assert!(output
        .nodes
        .iter()
        .any(|node| node.external_id.as_deref() == Some("root")));
    assert!(output
        .nodes
        .iter()
        .any(|node| node.external_id.as_deref() == Some("leaf")));
    assert!(output
        .relationships
        .iter()
        .all(|relationship| relationship.relationship_type == "LINKS"));
    assert!(output
        .relationships
        .iter()
        .all(|relationship| relationship.direction == KnowledgeGraphPathDirection::Outgoing));
}

#[test]
fn scoped_knowledge_subgraph_filters_seed_by_metadata() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query(
        "CREATE (:Memory {id: 'root', title: 'Root', source_id: 'thread_1'})-[:LINKS]->(:Entity {id: 'leaf', name: 'Leaf'})",
    )
    .unwrap();

    let scoped_request = KnowledgeScopedSubgraphRequest {
        navigation: KnowledgeSubgraphRequest {
            label: "Memory".to_string(),
            external_id: "root".to_string(),
            relationship_type: Some("LINKS".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            max_hops: 1,
            node_limit: 4,
            relationship_limit: 4,
        },
        metadata_filters: BTreeMap::from([("source_id".to_string(), "thread_1".to_string())]),
    };
    let scoped = db
        .query_scoped_subgraph_via_cypher(&scoped_request)
        .unwrap();

    assert_eq!(scoped.nodes.len(), 2);
    assert_eq!(scoped.relationships.len(), 1);
    assert!(scoped.diagnostics.seed_found);
    assert_eq!(scoped.diagnostics.input_candidate_set.filtered_out_count, 0);
    assert_eq!(
        scoped
            .diagnostics
            .input_candidate_set
            .metadata_filters
            .get("source_id")
            .map(String::as_str),
        Some("thread_1")
    );

    let stats = db.plan_cache_stats();
    let repeated_scoped = db
        .query_scoped_subgraph_via_cypher(&scoped_request)
        .unwrap();
    assert_eq!(repeated_scoped, scoped);
    let repeated_stats = db.plan_cache_stats();
    assert_eq!(repeated_stats.entries, stats.entries);
    assert_eq!(repeated_stats.misses, stats.misses);
    assert!(repeated_stats.hits > stats.hits);

    let filtered = db
        .query_scoped_subgraph_via_cypher(&KnowledgeScopedSubgraphRequest {
            navigation: KnowledgeSubgraphRequest {
                label: "Memory".to_string(),
                external_id: "root".to_string(),
                relationship_type: Some("LINKS".to_string()),
                direction: KnowledgeNeighborDirection::Outgoing,
                max_hops: 1,
                node_limit: 4,
                relationship_limit: 4,
            },
            metadata_filters: BTreeMap::from([("source_id".to_string(), "thread_2".to_string())]),
        })
        .unwrap();

    assert_eq!(filtered.seed_node_id, Some(0));
    assert!(filtered.nodes.is_empty());
    assert!(filtered.relationships.is_empty());
    assert!(!filtered.diagnostics.seed_found);
    assert_eq!(
        filtered.diagnostics.input_candidate_set.filtered_out_count,
        1
    );
    assert_eq!(
        filtered
            .diagnostics
            .input_candidate_set
            .metadata_filters
            .get("source_id")
            .map(String::as_str),
        Some("thread_2")
    );
    assert!(filtered.diagnostics.fallback_reasons.is_empty());
}

#[test]
fn knowledge_subgraph_reports_limits_and_missing_seed() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS]->(:Entity {id: 'left', name: 'Left'})")
            .unwrap();
    let right = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("right".to_string())),
                ("name".to_string(), Value::String("Right".to_string())),
            ]),
        )
        .unwrap();
    db.store
        .create_relationship(&mut db.catalog, NodeId(0), right, "LINKS", BTreeMap::new())
        .unwrap();

    let node_limited = db
        .query_subgraph_via_cypher(&KnowledgeSubgraphRequest {
            label: "Memory".to_string(),
            external_id: "root".to_string(),
            relationship_type: None,
            direction: KnowledgeNeighborDirection::Outgoing,
            max_hops: 1,
            node_limit: 1,
            relationship_limit: 8,
        })
        .unwrap();
    assert_eq!(node_limited.nodes.len(), 1);
    assert!(node_limited.diagnostics.seed_found);
    assert_eq!(node_limited.diagnostics.node_count, 1);
    assert_eq!(node_limited.diagnostics.relationship_count, 0);
    assert_eq!(node_limited.diagnostics.fanout_reason_count, 1);
    assert!(node_limited.diagnostics.fallback_reasons.is_empty());
    assert_eq!(node_limited.diagnostics.node_limit, Some(1));
    assert!(node_limited.relationships.is_empty());
    assert!(node_limited.fanout_reasons[0].contains("node_limit 1"));
    assert_eq!(
        node_limited.fanout_reason_codes,
        vec![KnowledgeFanoutReasonCode::NodeLimitReached]
    );
    assert_eq!(
        node_limited.diagnostics.fanout_reason_codes,
        node_limited.fanout_reason_codes
    );
    assert_eq!(
        node_limited.diagnostics.fanout_reasons,
        node_limited.fanout_reasons
    );

    let relationship_limited_request = KnowledgeSubgraphRequest {
        label: "Memory".to_string(),
        external_id: "root".to_string(),
        relationship_type: None,
        direction: KnowledgeNeighborDirection::Outgoing,
        max_hops: 1,
        node_limit: 8,
        relationship_limit: 1,
    };
    let relationship_limited = db
        .query_subgraph_via_cypher(&relationship_limited_request)
        .unwrap();
    assert_eq!(relationship_limited.relationships.len(), 1);
    assert_eq!(relationship_limited.diagnostics.node_count, 2);
    assert_eq!(relationship_limited.diagnostics.relationship_count, 1);
    assert_eq!(relationship_limited.diagnostics.fanout_reason_count, 1);
    assert!(relationship_limited.diagnostics.fallback_reasons.is_empty());
    assert_eq!(relationship_limited.diagnostics.relationship_limit, Some(1));
    assert!(relationship_limited.fanout_reasons[0].contains("relationship_limit 1"));
    assert_eq!(
        relationship_limited.fanout_reason_codes,
        vec![KnowledgeFanoutReasonCode::RelationshipLimitReached]
    );
    assert_eq!(
        relationship_limited.diagnostics.fanout_reason_codes,
        relationship_limited.fanout_reason_codes
    );
    assert_eq!(
        relationship_limited.diagnostics.fanout_reasons,
        relationship_limited.fanout_reasons
    );

    let stats = db.plan_cache_stats();
    let repeated_relationship_limited = db
        .query_subgraph_via_cypher(&relationship_limited_request)
        .unwrap();
    assert_eq!(repeated_relationship_limited, relationship_limited);
    let repeated_stats = db.plan_cache_stats();
    assert_eq!(repeated_stats.entries, stats.entries);
    assert_eq!(repeated_stats.misses, stats.misses);
    assert!(repeated_stats.hits > stats.hits);

    let node_disabled = db
        .query_subgraph_via_cypher(&KnowledgeSubgraphRequest {
            label: "Memory".to_string(),
            external_id: "root".to_string(),
            relationship_type: None,
            direction: KnowledgeNeighborDirection::Outgoing,
            max_hops: 1,
            node_limit: 0,
            relationship_limit: 8,
        })
        .unwrap();
    assert!(node_disabled.nodes.is_empty());
    assert!(node_disabled.relationships.is_empty());
    assert_eq!(
        node_disabled.diagnostics.fallback_reasons,
        vec!["subgraph traversal disabled by node_limit 0".to_string()]
    );
    assert_eq!(
        node_disabled.diagnostics.fallback_reason_codes,
        vec![KnowledgeTraversalFallbackReasonCode::NodeLimitZero]
    );

    let relationship_disabled = db
        .query_subgraph_via_cypher(&KnowledgeSubgraphRequest {
            label: "Memory".to_string(),
            external_id: "root".to_string(),
            relationship_type: None,
            direction: KnowledgeNeighborDirection::Outgoing,
            max_hops: 1,
            node_limit: 8,
            relationship_limit: 0,
        })
        .unwrap();
    assert_eq!(relationship_disabled.nodes.len(), 1);
    assert!(relationship_disabled.relationships.is_empty());
    assert_eq!(
        relationship_disabled.diagnostics.fallback_reasons,
        vec!["subgraph traversal disabled by relationship_limit 0".to_string()]
    );
    assert_eq!(
        relationship_disabled.diagnostics.fallback_reason_codes,
        vec![KnowledgeTraversalFallbackReasonCode::RelationshipLimitZero]
    );

    let unknown_type = db
        .query_subgraph_via_cypher(&KnowledgeSubgraphRequest {
            label: "Memory".to_string(),
            external_id: "root".to_string(),
            relationship_type: Some("DOES_NOT_EXIST".to_string()),
            direction: KnowledgeNeighborDirection::Both,
            max_hops: 1,
            node_limit: 8,
            relationship_limit: 8,
        })
        .unwrap();
    assert_eq!(unknown_type.seed_node_id, Some(0));
    assert!(unknown_type.diagnostics.seed_found);
    assert_eq!(unknown_type.diagnostics.node_count, 0);
    assert_eq!(unknown_type.diagnostics.relationship_count, 0);
    assert!(unknown_type.nodes.is_empty());
    assert!(unknown_type.relationships.is_empty());
    assert!(unknown_type.fanout_reasons.is_empty());
    assert_eq!(
        unknown_type.diagnostics.fallback_reasons,
        vec!["relationship type DOES_NOT_EXIST not found".to_string()]
    );
    assert_eq!(
        unknown_type.diagnostics.fallback_reason_codes,
        vec![KnowledgeTraversalFallbackReasonCode::RelationshipTypeNotFound]
    );

    let missing = db
        .query_subgraph_via_cypher(&KnowledgeSubgraphRequest {
            label: "Memory".to_string(),
            external_id: "missing".to_string(),
            relationship_type: None,
            direction: KnowledgeNeighborDirection::Both,
            max_hops: 1,
            node_limit: 8,
            relationship_limit: 8,
        })
        .unwrap();
    assert_eq!(missing.seed_node_id, None);
    assert!(!missing.diagnostics.seed_found);
    assert_eq!(missing.diagnostics.node_count, 0);
    assert_eq!(missing.diagnostics.relationship_count, 0);
    assert_eq!(missing.diagnostics.fanout_reason_count, 0);
    assert_eq!(
        missing.diagnostics.fallback_reasons,
        vec!["seed Memory:missing not found".to_string()]
    );
    assert_eq!(
        missing.diagnostics.fallback_reason_codes,
        vec![KnowledgeTraversalFallbackReasonCode::SeedNotFound]
    );
    assert!(missing.nodes.is_empty());
    assert!(missing.relationships.is_empty());
    assert!(missing.fanout_reasons.is_empty());
}
