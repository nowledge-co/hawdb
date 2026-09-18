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
use crate::schema::{PropertyType, TableKind};
use crate::Value;

fn schema_context() -> GraphRagSchemaContext {
    let mut catalog = Catalog::default();
    let memory = catalog.get_or_create_label("Memory");
    let entity = catalog.get_or_create_label("Entity");
    let source = catalog.get_or_create_label("Source");
    let mentions = catalog.get_or_create_rel_type("MENTIONS");
    let sourced_from = catalog.get_or_create_rel_type("SOURCED_FROM");
    let memory_table = catalog.get_or_create_table(TableKind::Node, "Memory");
    let entity_table = catalog.get_or_create_table(TableKind::Node, "Entity");
    let source_table = catalog.get_or_create_table(TableKind::Node, "Source");
    let mentions_table = catalog.get_or_create_table(TableKind::Relationship, "MENTIONS");
    let sourced_from_table = catalog.get_or_create_table(TableKind::Relationship, "SOURCED_FROM");
    catalog.get_or_create_property(memory_table, "id", PropertyType::String, false);
    catalog.get_or_create_property(entity_table, "name", PropertyType::String, true);
    catalog.get_or_create_property(source_table, "uri", PropertyType::String, false);
    catalog.get_or_create_property(mentions_table, "confidence", PropertyType::Float, true);
    catalog.get_or_create_property(sourced_from_table, "observed_at", PropertyType::Int, false);
    let statistics = GraphStatistics {
        computed_at_commit_epoch: 9,
        label_counts: BTreeMap::from([(memory, 4), (entity, 2), (source, 1)]),
        rel_type_counts: BTreeMap::from([(mentions, 3), (sourced_from, 2)]),
        path_counts: BTreeMap::from([
            ((memory, mentions, entity), 3),
            ((entity, sourced_from, source), 2),
        ]),
        ..GraphStatistics::default()
    };
    build_graph_rag_schema_context(
        &catalog,
        &statistics,
        GraphRagSchemaContextOptions::default(),
    )
}

#[test]
fn generates_bounded_parameterized_route_query() {
    let context = schema_context();
    let generated = context
        .generate_query(&GraphRagQueryDraft {
            schema_fingerprint: context.fingerprint,
            pattern: GraphRagQueryPattern::Route {
                source_label: "Memory".to_string(),
                relationship_type: "MENTIONS".to_string(),
                target_label: "Entity".to_string(),
            },
            predicates: vec![
                GraphRagQueryPredicate {
                    binding: GraphRagQueryBinding::Source,
                    property: "id".to_string(),
                    operator: GraphRagQueryPredicateOperator::Eq,
                    parameter: Some("memory_id".to_string()),
                },
                GraphRagQueryPredicate {
                    binding: GraphRagQueryBinding::Relationship,
                    property: "confidence".to_string(),
                    operator: GraphRagQueryPredicateOperator::Gte,
                    parameter: Some("minimum_confidence".to_string()),
                },
            ],
            projections: vec![GraphRagQueryProjection {
                binding: GraphRagQueryBinding::Target,
                property: "name".to_string(),
                alias: "entity_name".to_string(),
            }],
            limit: 5,
        })
        .unwrap();

    assert_eq!(
        generated.cypher(),
        "MATCH (n0:Memory)-[r0:MENTIONS]->(n1:Entity) \
         WHERE n0.id = $memory_id AND r0.confidence >= $minimum_confidence \
         RETURN n1.name AS entity_name LIMIT 5"
    );
    assert_eq!(
        generated.required_parameters(),
        ["memory_id".to_string(), "minimum_confidence".to_string()]
    );
}

#[test]
fn generates_schema_validated_two_hop_query() {
    let context = schema_context();
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
            predicates: vec![
                GraphRagQueryPredicate {
                    binding: GraphRagQueryBinding::Intermediate,
                    property: "name".to_string(),
                    operator: GraphRagQueryPredicateOperator::Eq,
                    parameter: Some("entity_name".to_string()),
                },
                GraphRagQueryPredicate {
                    binding: GraphRagQueryBinding::SecondRelationship,
                    property: "observed_at".to_string(),
                    operator: GraphRagQueryPredicateOperator::Gte,
                    parameter: Some("minimum_observed_at".to_string()),
                },
            ],
            projections: vec![
                GraphRagQueryProjection {
                    binding: GraphRagQueryBinding::Source,
                    property: "id".to_string(),
                    alias: "memory_id".to_string(),
                },
                GraphRagQueryProjection {
                    binding: GraphRagQueryBinding::Target,
                    property: "uri".to_string(),
                    alias: "source_uri".to_string(),
                },
            ],
            limit: 10,
        })
        .unwrap();

    assert_eq!(
        generated.cypher(),
        "MATCH (n0:Memory)-[r0:MENTIONS]->(n1:Entity)-[r1:SOURCED_FROM]->(n2:Source) \
         WHERE n1.name = $entity_name AND r1.observed_at >= $minimum_observed_at \
         RETURN n0.id AS memory_id, n2.uri AS source_uri LIMIT 10"
    );
    assert_eq!(
        generated.required_parameters(),
        ["entity_name".to_string(), "minimum_observed_at".to_string()]
    );
}

#[test]
fn rejects_two_hop_draft_when_either_leg_is_unobserved() {
    let context = schema_context();
    let error = context
        .generate_query(&GraphRagQueryDraft {
            schema_fingerprint: context.fingerprint,
            pattern: GraphRagQueryPattern::TwoHopRoute {
                source_label: "Memory".to_string(),
                first_relationship_type: "MENTIONS".to_string(),
                intermediate_label: "Entity".to_string(),
                second_relationship_type: "MENTIONS".to_string(),
                target_label: "Source".to_string(),
            },
            predicates: Vec::new(),
            projections: vec![GraphRagQueryProjection {
                binding: GraphRagQueryBinding::Target,
                property: "uri".to_string(),
                alias: "source_uri".to_string(),
            }],
            limit: 10,
        })
        .unwrap_err();

    assert_eq!(
        error,
        GraphRagQueryGenerationError::UnknownRoute {
            source_label: "Entity".to_string(),
            relationship_type: "MENTIONS".to_string(),
            target_label: "Source".to_string(),
        }
    );
}

#[test]
fn rejects_stale_schema_and_invented_identifiers() {
    let context = schema_context();
    let mut draft = GraphRagQueryDraft {
        schema_fingerprint: context.fingerprint.wrapping_add(1),
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
    };
    assert!(matches!(
        context.generate_query(&draft),
        Err(GraphRagQueryGenerationError::SchemaFingerprintMismatch { .. })
    ));

    draft.schema_fingerprint = context.fingerprint;
    draft.pattern = GraphRagQueryPattern::Node {
        label: "InventedByModel".to_string(),
    };
    assert_eq!(
        context.generate_query(&draft).unwrap_err(),
        GraphRagQueryGenerationError::UnknownLabel("InventedByModel".to_string())
    );
}

#[test]
fn rejects_mutated_schema_context_before_generation() {
    let mut context = schema_context();
    let fingerprint = context.fingerprint;
    context.routes.push(GraphRagRouteSummary {
        source_label: "Memory".to_string(),
        relationship_type: "INVENTED".to_string(),
        target_label: "Source".to_string(),
        observed_count: 1,
        distinct_source_count: 1,
        distinct_target_count: 1,
    });

    let error = context
        .generate_query(&GraphRagQueryDraft {
            schema_fingerprint: fingerprint,
            pattern: GraphRagQueryPattern::Route {
                source_label: "Memory".to_string(),
                relationship_type: "INVENTED".to_string(),
                target_label: "Source".to_string(),
            },
            predicates: Vec::new(),
            projections: vec![GraphRagQueryProjection {
                binding: GraphRagQueryBinding::Target,
                property: "uri".to_string(),
                alias: "source_uri".to_string(),
            }],
            limit: 5,
        })
        .unwrap_err();

    assert!(matches!(
        error,
        GraphRagQueryGenerationError::SchemaContextIntegrityMismatch {
            expected,
            actual
        } if expected == fingerprint && actual != fingerprint
    ));
}

#[test]
fn rejects_unavailable_bindings_and_unbounded_limits() {
    let context = schema_context();
    let draft = GraphRagQueryDraft {
        schema_fingerprint: context.fingerprint,
        pattern: GraphRagQueryPattern::Node {
            label: "Memory".to_string(),
        },
        predicates: Vec::new(),
        projections: vec![GraphRagQueryProjection {
            binding: GraphRagQueryBinding::Target,
            property: "name".to_string(),
            alias: "name".to_string(),
        }],
        limit: 5,
    };
    assert_eq!(
        context.generate_query(&draft).unwrap_err(),
        GraphRagQueryGenerationError::BindingUnavailable(GraphRagQueryBinding::Target)
    );

    let mut unbounded = draft;
    unbounded.limit = MAX_GRAPH_RAG_QUERY_LIMIT + 1;
    assert!(matches!(
        context.generate_query(&unbounded),
        Err(GraphRagQueryGenerationError::InvalidLimit { .. })
    ));
}

#[test]
fn validates_generated_query_parameter_shapes() {
    let context = schema_context();
    let generated = context
        .generate_query(&GraphRagQueryDraft {
            schema_fingerprint: context.fingerprint,
            pattern: GraphRagQueryPattern::Route {
                source_label: "Memory".to_string(),
                relationship_type: "MENTIONS".to_string(),
                target_label: "Entity".to_string(),
            },
            predicates: vec![
                GraphRagQueryPredicate {
                    binding: GraphRagQueryBinding::Source,
                    property: "id".to_string(),
                    operator: GraphRagQueryPredicateOperator::In,
                    parameter: Some("memory_ids".to_string()),
                },
                GraphRagQueryPredicate {
                    binding: GraphRagQueryBinding::Relationship,
                    property: "confidence".to_string(),
                    operator: GraphRagQueryPredicateOperator::Gte,
                    parameter: Some("minimum_confidence".to_string()),
                },
            ],
            projections: vec![GraphRagQueryProjection {
                binding: GraphRagQueryBinding::Target,
                property: "name".to_string(),
                alias: "entity_name".to_string(),
            }],
            limit: 5,
        })
        .unwrap();

    assert_eq!(generated.context_commit_epoch(), 9);
    assert_eq!(
        generated.parameter_requirements(),
        [
            GraphRagQueryParameterRequirement {
                name: "memory_ids".to_string(),
                value_type: PropertyType::String,
                cardinality: GraphRagQueryParameterCardinality::List,
            },
            GraphRagQueryParameterRequirement {
                name: "minimum_confidence".to_string(),
                value_type: PropertyType::Float,
                cardinality: GraphRagQueryParameterCardinality::Scalar,
            },
        ]
    );
    generated
        .validate_parameters(&BTreeMap::from([
            (
                "memory_ids".to_string(),
                Value::List(vec![Value::String("memory-1".to_string())]),
            ),
            ("minimum_confidence".to_string(), Value::Int(1)),
        ]))
        .unwrap();

    assert!(matches!(
        generated.validate_parameters(&BTreeMap::from([(
            "memory_ids".to_string(),
            Value::List(vec![Value::String("memory-1".to_string())]),
        )])),
        Err(GraphRagQueryParameterError::Missing { parameter })
            if parameter == "minimum_confidence"
    ));
    assert!(matches!(
        generated.validate_parameters(&BTreeMap::from([(
            "memory_ids".to_string(),
            Value::String("memory-1".to_string()),
        )])),
        Err(GraphRagQueryParameterError::TypeMismatch {
            parameter,
            actual: "string",
            ..
        }) if parameter == "memory_ids"
    ));
    assert!(matches!(
        generated.validate_parameters(&BTreeMap::from([
            (
                "memory_ids".to_string(),
                Value::List(vec![Value::String("memory-1".to_string())]),
            ),
            ("minimum_confidence".to_string(), Value::Float(0.5)),
            ("invented".to_string(), Value::Bool(true)),
        ])),
        Err(GraphRagQueryParameterError::Unexpected { parameter })
            if parameter == "invented"
    ));
}

#[test]
fn rejects_reused_parameter_with_conflicting_schema_types() {
    let context = schema_context();
    let error = context
        .generate_query(&GraphRagQueryDraft {
            schema_fingerprint: context.fingerprint,
            pattern: GraphRagQueryPattern::TwoHopRoute {
                source_label: "Memory".to_string(),
                first_relationship_type: "MENTIONS".to_string(),
                intermediate_label: "Entity".to_string(),
                second_relationship_type: "SOURCED_FROM".to_string(),
                target_label: "Source".to_string(),
            },
            predicates: vec![
                GraphRagQueryPredicate {
                    binding: GraphRagQueryBinding::Source,
                    property: "id".to_string(),
                    operator: GraphRagQueryPredicateOperator::Eq,
                    parameter: Some("value".to_string()),
                },
                GraphRagQueryPredicate {
                    binding: GraphRagQueryBinding::SecondRelationship,
                    property: "observed_at".to_string(),
                    operator: GraphRagQueryPredicateOperator::Gte,
                    parameter: Some("value".to_string()),
                },
            ],
            projections: vec![GraphRagQueryProjection {
                binding: GraphRagQueryBinding::Target,
                property: "uri".to_string(),
                alias: "source_uri".to_string(),
            }],
            limit: 5,
        })
        .unwrap_err();

    assert!(matches!(
        error,
        GraphRagQueryGenerationError::ConflictingParameterRequirement {
            parameter,
            ..
        } if parameter == "value"
    ));
}
