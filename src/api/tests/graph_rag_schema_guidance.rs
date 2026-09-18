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
use crate::{
    GraphRagQueryBinding, GraphRagQueryDraft, GraphRagQueryPattern, GraphRagQueryPredicate,
    GraphRagQueryPredicateOperator, GraphRagQueryProjection, GraphRagSchemaContextOptions,
};

#[test]
fn graph_rag_schema_context_guides_queries_through_the_read_runtime() {
    let mut db = Database::new_with_config(DatabaseConfig {
        slow_query_log_threshold_micros: 0,
        ..DatabaseConfig::default()
    });
    db.query("CREATE NODE TABLE Memory").unwrap();
    db.query("CREATE NODE TABLE Entity").unwrap();
    db.query("CREATE RELATIONSHIP TABLE MENTIONS").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE STRING NOT NULL")
        .unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Entity(name) TYPE STRING")
        .unwrap();
    db.query("CREATE PROPERTY ON RELATIONSHIP TABLE MENTIONS(confidence) TYPE FLOAT")
        .unwrap();
    db.query(
        "CREATE (:Memory {id: 'memory-1', private_payload: 'do-not-render'})\
         -[:MENTIONS {confidence: 0.9}]->(:Entity {id: 'entity-1', name: 'HawDB'})",
    )
    .unwrap();

    let context = db.graph_rag_schema_context(GraphRagSchemaContextOptions::default());
    assert_eq!(context.labels.len(), 2);
    assert_eq!(context.relationship_types.len(), 1);
    assert_eq!(context.routes.len(), 1);
    assert!(context
        .properties
        .iter()
        .any(|property| property.subject_name == "Memory" && property.name == "id"));
    let guidance = context.render_compact_cypher_guidance();
    assert!(guidance.contains("RULES read_only=true"));
    assert!(guidance.contains("ROUTE (Memory)-[:MENTIONS]->(Entity)"));
    assert!(!guidance.contains("do-not-render"));

    let generated = context
        .generate_query(&GraphRagQueryDraft {
            schema_fingerprint: context.fingerprint,
            pattern: GraphRagQueryPattern::Route {
                source_label: "Memory".to_string(),
                relationship_type: "MENTIONS".to_string(),
                target_label: "Entity".to_string(),
            },
            predicates: vec![GraphRagQueryPredicate {
                binding: GraphRagQueryBinding::Source,
                property: "id".to_string(),
                operator: GraphRagQueryPredicateOperator::Eq,
                parameter: Some("id".to_string()),
            }],
            projections: vec![GraphRagQueryProjection {
                binding: GraphRagQueryBinding::Target,
                property: "name".to_string(),
                alias: "name".to_string(),
            }],
            limit: 5,
        })
        .unwrap();
    let slow_query_count = db.slow_query_log_snapshot().len();
    let output = db
        .query_with_params(
            generated.cypher(),
            &BTreeMap::from([("id".to_string(), Value::String("memory-1".to_string()))]),
        )
        .unwrap();
    assert_eq!(
        output.rows[0].get("name"),
        Some(&Value::String("HawDB".to_string()))
    );
    assert_eq!(db.slow_query_log_snapshot().len(), slow_query_count + 1);

    let mut read = db.begin_read_transaction();
    let error = read.query("CREATE (:InventedByModel)").unwrap_err();
    assert!(error
        .to_string()
        .contains("read transaction query must not be a mutation"));
}

#[test]
fn graph_rag_two_hop_draft_runs_through_the_query_runtime() {
    let mut db = Database::new();
    db.query("CREATE NODE TABLE Memory").unwrap();
    db.query("CREATE NODE TABLE Entity").unwrap();
    db.query("CREATE NODE TABLE Source").unwrap();
    db.query("CREATE RELATIONSHIP TABLE MENTIONS").unwrap();
    db.query("CREATE RELATIONSHIP TABLE SOURCED_FROM").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE STRING NOT NULL")
        .unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Entity(id) TYPE STRING NOT NULL")
        .unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Entity(name) TYPE STRING")
        .unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Source(id) TYPE STRING NOT NULL")
        .unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Source(uri) TYPE STRING NOT NULL")
        .unwrap();
    db.query(
        "CREATE (:Memory {id: 'memory-1'})-[:MENTIONS]->(:Entity {id: 'entity-1', name: 'HawDB'})",
    )
    .unwrap();
    db.query("CREATE (:Source {id: 'source-1', uri: 'https://example.test/hawdb'})")
        .unwrap();
    db.query(
        "MATCH (e:Entity {id: 'entity-1'}), (s:Source {id: 'source-1'}) \
         CREATE (e)-[:SOURCED_FROM]->(s)",
    )
    .unwrap();

    let context = db.graph_rag_schema_context(GraphRagSchemaContextOptions::default());
    let generated = context
        .generate_query(&GraphRagQueryDraft {
            schema_fingerprint: context.fingerprint,
            pattern: GraphRagQueryPattern::TwoHopRoute {
                source_label: "Memory".to_string(),
                first_relationship_type: "MENTIONS".to_string(),
                intermediate_label: "Entity".to_string(),
                second_relationship_type: "SOURCED_FROM".to_string(),
                target_label: "Source".to_string(),
            },
            predicates: vec![GraphRagQueryPredicate {
                binding: GraphRagQueryBinding::Intermediate,
                property: "name".to_string(),
                operator: GraphRagQueryPredicateOperator::Eq,
                parameter: Some("entity_name".to_string()),
            }],
            projections: vec![GraphRagQueryProjection {
                binding: GraphRagQueryBinding::Target,
                property: "uri".to_string(),
                alias: "source_uri".to_string(),
            }],
            limit: 5,
        })
        .unwrap();

    hawdb_cypher::parse(generated.cypher()).unwrap();
    let output = db
        .query_with_params(
            generated.cypher(),
            &BTreeMap::from([(
                "entity_name".to_string(),
                Value::String("HawDB".to_string()),
            )]),
        )
        .unwrap();
    assert_eq!(
        output.rows[0].get("source_uri"),
        Some(&Value::String("https://example.test/hawdb".to_string()))
    );
}

#[test]
fn graph_rag_schema_context_is_pinned_to_the_read_snapshot() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory-1'})").unwrap();
    let read = db.begin_read_transaction();
    let pinned = read.graph_rag_schema_context(GraphRagSchemaContextOptions::default());

    db.query("CREATE (:Entity {id: 'entity-1'})").unwrap();
    let latest = db.graph_rag_schema_context(GraphRagSchemaContextOptions::default());
    let pinned_again = read.graph_rag_schema_context(GraphRagSchemaContextOptions::default());

    assert_eq!(pinned, pinned_again);
    assert_ne!(pinned.fingerprint, latest.fingerprint);
    assert_eq!(pinned.labels.len(), 1);
    assert_eq!(latest.labels.len(), 2);
}

#[test]
fn graph_rag_generated_predicates_follow_the_cypher_parser_contract() {
    let mut db = Database::new();
    db.query("CREATE NODE TABLE Memory").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Memory(title) TYPE STRING")
        .unwrap();
    let context = db.graph_rag_schema_context(GraphRagSchemaContextOptions::default());
    let operators = [
        GraphRagQueryPredicateOperator::Eq,
        GraphRagQueryPredicateOperator::NotEq,
        GraphRagQueryPredicateOperator::Lt,
        GraphRagQueryPredicateOperator::Lte,
        GraphRagQueryPredicateOperator::Gt,
        GraphRagQueryPredicateOperator::Gte,
        GraphRagQueryPredicateOperator::In,
        GraphRagQueryPredicateOperator::Contains,
        GraphRagQueryPredicateOperator::StartsWith,
        GraphRagQueryPredicateOperator::EndsWith,
        GraphRagQueryPredicateOperator::IsNull,
        GraphRagQueryPredicateOperator::IsNotNull,
    ];

    for operator in operators {
        let parameter = (!matches!(
            operator,
            GraphRagQueryPredicateOperator::IsNull | GraphRagQueryPredicateOperator::IsNotNull
        ))
        .then(|| "value".to_string());
        let generated = context
            .generate_query(&GraphRagQueryDraft {
                schema_fingerprint: context.fingerprint,
                pattern: GraphRagQueryPattern::Node {
                    label: "Memory".to_string(),
                },
                predicates: vec![GraphRagQueryPredicate {
                    binding: GraphRagQueryBinding::Source,
                    property: "title".to_string(),
                    operator,
                    parameter,
                }],
                projections: vec![GraphRagQueryProjection {
                    binding: GraphRagQueryBinding::Source,
                    property: "title".to_string(),
                    alias: "title".to_string(),
                }],
                limit: 5,
            })
            .unwrap();

        hawdb_cypher::parse(generated.cypher()).unwrap();
    }
}

#[test]
fn generated_graph_rag_query_validates_parameters_before_canonical_execution() {
    let mut db = Database::new();
    db.query("CREATE NODE TABLE Memory").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE STRING NOT NULL")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory-1'})").unwrap();
    let context = db.graph_rag_schema_context(GraphRagSchemaContextOptions::default());
    let generated = context
        .generate_query(&GraphRagQueryDraft {
            schema_fingerprint: context.fingerprint,
            pattern: GraphRagQueryPattern::Node {
                label: "Memory".to_string(),
            },
            predicates: vec![GraphRagQueryPredicate {
                binding: GraphRagQueryBinding::Source,
                property: "id".to_string(),
                operator: GraphRagQueryPredicateOperator::Eq,
                parameter: Some("memory_id".to_string()),
            }],
            projections: vec![GraphRagQueryProjection {
                binding: GraphRagQueryBinding::Source,
                property: "id".to_string(),
                alias: "id".to_string(),
            }],
            limit: 5,
        })
        .unwrap();

    let mut read = db.begin_read_transaction();
    let output = read
        .query_generated_graph_rag(
            &generated,
            &BTreeMap::from([(
                "memory_id".to_string(),
                Value::String("memory-1".to_string()),
            )]),
        )
        .unwrap();
    assert_eq!(
        output.rows[0].get("id"),
        Some(&Value::String("memory-1".to_string()))
    );

    let error = read
        .query_generated_graph_rag(
            &generated,
            &BTreeMap::from([("memory_id".to_string(), Value::Int(1))]),
        )
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("GraphRAG query parameter memory_id must be string"));

    let error = read
        .query_generated_graph_rag(&generated, &BTreeMap::new())
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("missing GraphRAG query parameter: memory_id"));

    let error = read
        .query_generated_graph_rag(
            &generated,
            &BTreeMap::from([
                (
                    "memory_id".to_string(),
                    Value::String("memory-1".to_string()),
                ),
                ("invented".to_string(), Value::Bool(true)),
            ]),
        )
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("unexpected GraphRAG query parameter: invented"));

    let generated_in = context
        .generate_query(&GraphRagQueryDraft {
            schema_fingerprint: context.fingerprint,
            pattern: GraphRagQueryPattern::Node {
                label: "Memory".to_string(),
            },
            predicates: vec![GraphRagQueryPredicate {
                binding: GraphRagQueryBinding::Source,
                property: "id".to_string(),
                operator: GraphRagQueryPredicateOperator::In,
                parameter: Some("memory_ids".to_string()),
            }],
            projections: vec![GraphRagQueryProjection {
                binding: GraphRagQueryBinding::Source,
                property: "id".to_string(),
                alias: "id".to_string(),
            }],
            limit: 5,
        })
        .unwrap();
    assert_eq!(generated_in.required_parameters(), ["memory_ids"]);

    let error = read
        .query_generated_graph_rag(
            &generated_in,
            &BTreeMap::from([(
                "memory_ids".to_string(),
                Value::String("memory-1".to_string()),
            )]),
        )
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("GraphRAG query parameter memory_ids must be list<string>"));

    let output = read
        .query_generated_graph_rag(
            &generated_in,
            &BTreeMap::from([(
                "memory_ids".to_string(),
                Value::List(vec![Value::String("memory-1".to_string())]),
            )]),
        )
        .unwrap();
    assert_eq!(
        output.rows[0].get("id"),
        Some(&Value::String("memory-1".to_string()))
    );
}

#[test]
fn generated_graph_rag_query_rejects_a_newer_pinned_schema_epoch() {
    let mut db = Database::new();
    db.query("CREATE NODE TABLE Memory").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE STRING NOT NULL")
        .unwrap();
    let context = db.graph_rag_schema_context(GraphRagSchemaContextOptions::default());
    let generated = context
        .generate_query(&GraphRagQueryDraft {
            schema_fingerprint: context.fingerprint,
            pattern: GraphRagQueryPattern::Node {
                label: "Memory".to_string(),
            },
            predicates: Vec::new(),
            projections: vec![GraphRagQueryProjection {
                binding: GraphRagQueryBinding::Source,
                property: "id".to_string(),
                alias: "id".to_string(),
            }],
            limit: 5,
        })
        .unwrap();

    db.query("CREATE (:Memory {id: 'memory-1'})").unwrap();
    let mut newer_read = db.begin_read_transaction();
    let error = newer_read
        .query_generated_graph_rag(&generated, &BTreeMap::new())
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("GraphRAG schema context is stale"));
}
