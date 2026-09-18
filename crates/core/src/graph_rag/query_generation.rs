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

use super::{GraphRagPropertySubject, GraphRagSchemaContext};
use crate::PropertyType;
use std::collections::BTreeMap;
use std::fmt::Write;

mod model;

pub use model::{
    GraphRagGeneratedQuery, GraphRagQueryBinding, GraphRagQueryDraft, GraphRagQueryGenerationError,
    GraphRagQueryParameterCardinality, GraphRagQueryParameterError,
    GraphRagQueryParameterRequirement, GraphRagQueryPattern, GraphRagQueryPredicate,
    GraphRagQueryPredicateOperator, GraphRagQueryProjection, MAX_GRAPH_RAG_QUERY_LIMIT,
};

struct ResolvedBinding<'a> {
    subject: GraphRagPropertySubject,
    subject_name: &'a str,
    variable: &'static str,
}

struct ResolvedPattern<'a> {
    cypher: String,
    bindings: BTreeMap<GraphRagQueryBinding, ResolvedBinding<'a>>,
}

pub(super) fn generate_query(
    context: &GraphRagSchemaContext,
    draft: &GraphRagQueryDraft,
) -> Result<GraphRagGeneratedQuery, GraphRagQueryGenerationError> {
    let actual_fingerprint = super::schema_context_fingerprint(context);
    if actual_fingerprint != context.fingerprint {
        return Err(
            GraphRagQueryGenerationError::SchemaContextIntegrityMismatch {
                expected: context.fingerprint,
                actual: actual_fingerprint,
            },
        );
    }
    if draft.schema_fingerprint != context.fingerprint {
        return Err(GraphRagQueryGenerationError::SchemaFingerprintMismatch {
            expected: context.fingerprint,
            actual: draft.schema_fingerprint,
        });
    }
    if draft.projections.is_empty() {
        return Err(GraphRagQueryGenerationError::EmptyProjection);
    }
    if draft.limit == 0 || draft.limit > MAX_GRAPH_RAG_QUERY_LIMIT {
        return Err(GraphRagQueryGenerationError::InvalidLimit {
            limit: draft.limit,
            maximum: MAX_GRAPH_RAG_QUERY_LIMIT,
        });
    }

    let pattern = resolve_pattern(context, &draft.pattern)?;
    let mut parameter_requirements = BTreeMap::new();
    let mut cypher = pattern.cypher.clone();

    if !draft.predicates.is_empty() {
        cypher.push_str(" WHERE ");
        for (index, predicate) in draft.predicates.iter().enumerate() {
            if index > 0 {
                cypher.push_str(" AND ");
            }
            let value_type =
                validate_property(context, &pattern, predicate.binding, &predicate.property)?;
            if predicate.operator.requires_string_property()
                && !matches!(
                    value_type,
                    PropertyType::String | PropertyType::Text | PropertyType::Any
                )
            {
                return Err(
                    GraphRagQueryGenerationError::OperatorRequiresStringProperty {
                        binding: predicate.binding,
                        property: predicate.property.clone(),
                    },
                );
            }
            render_predicate(
                &mut cypher,
                &pattern,
                predicate,
                value_type,
                &mut parameter_requirements,
            )?;
        }
    }

    cypher.push_str(" RETURN ");
    for (index, projection) in draft.projections.iter().enumerate() {
        if index > 0 {
            cypher.push_str(", ");
        }
        validate_property(context, &pattern, projection.binding, &projection.property)?;
        validate_identifier("projection alias", &projection.alias)?;
        let variable = resolve_binding(&pattern, projection.binding)?.variable;
        let _ = write!(
            cypher,
            "{}.{} AS {}",
            variable, projection.property, projection.alias
        );
    }
    let _ = write!(cypher, " LIMIT {}", draft.limit);

    Ok(GraphRagGeneratedQuery {
        cypher,
        schema_fingerprint: context.fingerprint,
        context_commit_epoch: context.computed_at_commit_epoch,
        required_parameters: parameter_requirements.keys().cloned().collect(),
        parameter_requirements: parameter_requirements.into_values().collect(),
    })
}

fn resolve_pattern<'a>(
    context: &GraphRagSchemaContext,
    pattern: &'a GraphRagQueryPattern,
) -> Result<ResolvedPattern<'a>, GraphRagQueryGenerationError> {
    match pattern {
        GraphRagQueryPattern::Node { label } => {
            validate_identifier("label", label)?;
            validate_label(context, label)?;
            Ok(ResolvedPattern {
                cypher: format!("MATCH (n0:{label})"),
                bindings: BTreeMap::from([(
                    GraphRagQueryBinding::Source,
                    ResolvedBinding {
                        subject: GraphRagPropertySubject::Node,
                        subject_name: label,
                        variable: "n0",
                    },
                )]),
            })
        }
        GraphRagQueryPattern::Route {
            source_label,
            relationship_type,
            target_label,
        } => {
            validate_identifier("source label", source_label)?;
            validate_identifier("relationship type", relationship_type)?;
            validate_identifier("target label", target_label)?;
            validate_label(context, source_label)?;
            validate_label(context, target_label)?;
            validate_route(context, source_label, relationship_type, target_label)?;
            Ok(ResolvedPattern {
                cypher: format!(
                    "MATCH (n0:{source_label})-[r0:{relationship_type}]->(n1:{target_label})"
                ),
                bindings: route_bindings(source_label, relationship_type, target_label),
            })
        }
        GraphRagQueryPattern::TwoHopRoute {
            source_label,
            first_relationship_type,
            intermediate_label,
            second_relationship_type,
            target_label,
        } => {
            validate_identifier("source label", source_label)?;
            validate_identifier("first relationship type", first_relationship_type)?;
            validate_identifier("intermediate label", intermediate_label)?;
            validate_identifier("second relationship type", second_relationship_type)?;
            validate_identifier("target label", target_label)?;
            validate_label(context, source_label)?;
            validate_label(context, intermediate_label)?;
            validate_label(context, target_label)?;
            validate_route(
                context,
                source_label,
                first_relationship_type,
                intermediate_label,
            )?;
            validate_route(
                context,
                intermediate_label,
                second_relationship_type,
                target_label,
            )?;

            Ok(ResolvedPattern {
                cypher: format!(
                    "MATCH (n0:{source_label})-[r0:{first_relationship_type}]->\
                     (n1:{intermediate_label})-[r1:{second_relationship_type}]->\
                     (n2:{target_label})"
                ),
                bindings: BTreeMap::from([
                    (
                        GraphRagQueryBinding::Source,
                        ResolvedBinding {
                            subject: GraphRagPropertySubject::Node,
                            subject_name: source_label,
                            variable: "n0",
                        },
                    ),
                    (
                        GraphRagQueryBinding::Relationship,
                        ResolvedBinding {
                            subject: GraphRagPropertySubject::Relationship,
                            subject_name: first_relationship_type,
                            variable: "r0",
                        },
                    ),
                    (
                        GraphRagQueryBinding::Intermediate,
                        ResolvedBinding {
                            subject: GraphRagPropertySubject::Node,
                            subject_name: intermediate_label,
                            variable: "n1",
                        },
                    ),
                    (
                        GraphRagQueryBinding::SecondRelationship,
                        ResolvedBinding {
                            subject: GraphRagPropertySubject::Relationship,
                            subject_name: second_relationship_type,
                            variable: "r1",
                        },
                    ),
                    (
                        GraphRagQueryBinding::Target,
                        ResolvedBinding {
                            subject: GraphRagPropertySubject::Node,
                            subject_name: target_label,
                            variable: "n2",
                        },
                    ),
                ]),
            })
        }
    }
}

fn validate_route(
    context: &GraphRagSchemaContext,
    source_label: &str,
    relationship_type: &str,
    target_label: &str,
) -> Result<(), GraphRagQueryGenerationError> {
    if context.routes.iter().any(|route| {
        route.source_label == source_label
            && route.relationship_type == relationship_type
            && route.target_label == target_label
    }) {
        Ok(())
    } else {
        Err(GraphRagQueryGenerationError::UnknownRoute {
            source_label: source_label.to_string(),
            relationship_type: relationship_type.to_string(),
            target_label: target_label.to_string(),
        })
    }
}

fn route_bindings<'a>(
    source_label: &'a str,
    relationship_type: &'a str,
    target_label: &'a str,
) -> BTreeMap<GraphRagQueryBinding, ResolvedBinding<'a>> {
    BTreeMap::from([
        (
            GraphRagQueryBinding::Source,
            ResolvedBinding {
                subject: GraphRagPropertySubject::Node,
                subject_name: source_label,
                variable: "n0",
            },
        ),
        (
            GraphRagQueryBinding::Relationship,
            ResolvedBinding {
                subject: GraphRagPropertySubject::Relationship,
                subject_name: relationship_type,
                variable: "r0",
            },
        ),
        (
            GraphRagQueryBinding::Target,
            ResolvedBinding {
                subject: GraphRagPropertySubject::Node,
                subject_name: target_label,
                variable: "n1",
            },
        ),
    ])
}

fn render_predicate(
    output: &mut String,
    pattern: &ResolvedPattern<'_>,
    predicate: &GraphRagQueryPredicate,
    value_type: PropertyType,
    parameter_requirements: &mut BTreeMap<String, GraphRagQueryParameterRequirement>,
) -> Result<(), GraphRagQueryGenerationError> {
    validate_identifier("property", &predicate.property)?;
    let variable = resolve_binding(pattern, predicate.binding)?.variable;
    let _ = write!(
        output,
        "{}.{} {}",
        variable,
        predicate.property,
        predicate.operator.token()
    );
    match (
        predicate.operator.requires_parameter(),
        predicate.parameter.as_deref(),
    ) {
        (true, Some(parameter)) => {
            validate_identifier("parameter", parameter)?;
            let _ = write!(output, " ${parameter}");
            let requirement = GraphRagQueryParameterRequirement {
                name: parameter.to_string(),
                value_type,
                cardinality: if predicate.operator == GraphRagQueryPredicateOperator::In {
                    GraphRagQueryParameterCardinality::List
                } else {
                    GraphRagQueryParameterCardinality::Scalar
                },
            };
            if let Some(first) = parameter_requirements.get(parameter) {
                if first != &requirement {
                    return Err(
                        GraphRagQueryGenerationError::ConflictingParameterRequirement {
                            parameter: parameter.to_string(),
                            first: first.clone(),
                            second: requirement,
                        },
                    );
                }
            } else {
                parameter_requirements.insert(parameter.to_string(), requirement);
            }
        }
        (true, None) => {
            return Err(GraphRagQueryGenerationError::MissingParameter {
                property: predicate.property.clone(),
            });
        }
        (false, Some(_)) => {
            return Err(GraphRagQueryGenerationError::UnexpectedParameter {
                property: predicate.property.clone(),
            });
        }
        (false, None) => {}
    }
    Ok(())
}

fn validate_label(
    context: &GraphRagSchemaContext,
    label: &str,
) -> Result<(), GraphRagQueryGenerationError> {
    if context
        .labels
        .iter()
        .any(|candidate| candidate.name == label)
    {
        Ok(())
    } else {
        Err(GraphRagQueryGenerationError::UnknownLabel(
            label.to_string(),
        ))
    }
}

fn validate_property(
    context: &GraphRagSchemaContext,
    pattern: &ResolvedPattern<'_>,
    binding: GraphRagQueryBinding,
    property: &str,
) -> Result<PropertyType, GraphRagQueryGenerationError> {
    validate_identifier("property", property)?;
    let resolved = resolve_binding(pattern, binding)?;
    context
        .properties
        .iter()
        .find(|candidate| {
            candidate.subject == resolved.subject
                && candidate.subject_name == resolved.subject_name
                && candidate.name == property
        })
        .map(|property| property.value_type)
        .ok_or_else(|| GraphRagQueryGenerationError::PropertyUnavailable {
            binding,
            property: property.to_string(),
        })
}

fn resolve_binding<'a>(
    pattern: &'a ResolvedPattern<'_>,
    binding: GraphRagQueryBinding,
) -> Result<&'a ResolvedBinding<'a>, GraphRagQueryGenerationError> {
    pattern
        .bindings
        .get(&binding)
        .ok_or(GraphRagQueryGenerationError::BindingUnavailable(binding))
}

fn validate_identifier(
    kind: &'static str,
    identifier: &str,
) -> Result<(), GraphRagQueryGenerationError> {
    let mut chars = identifier.chars();
    let valid = chars
        .next()
        .is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
        && chars.all(|character| character.is_ascii_alphanumeric() || character == '_');
    if valid {
        Ok(())
    } else {
        Err(GraphRagQueryGenerationError::InvalidIdentifier {
            kind,
            value: identifier.to_string(),
        })
    }
}
