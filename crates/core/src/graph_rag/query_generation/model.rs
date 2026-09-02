use crate::{PropertyType, Value};
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};

pub const MAX_GRAPH_RAG_QUERY_LIMIT: usize = 100;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphRagQueryPattern {
    Node {
        label: String,
    },
    Route {
        source_label: String,
        relationship_type: String,
        target_label: String,
    },
    TwoHopRoute {
        source_label: String,
        first_relationship_type: String,
        intermediate_label: String,
        second_relationship_type: String,
        target_label: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum GraphRagQueryBinding {
    Source,
    Relationship,
    Intermediate,
    SecondRelationship,
    Target,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphRagQueryPredicateOperator {
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
    In,
    Contains,
    StartsWith,
    EndsWith,
    IsNull,
    IsNotNull,
}

impl GraphRagQueryPredicateOperator {
    pub(super) const fn token(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::NotEq => "<>",
            Self::Lt => "<",
            Self::Lte => "<=",
            Self::Gt => ">",
            Self::Gte => ">=",
            Self::In => "IN",
            Self::Contains => "CONTAINS",
            Self::StartsWith => "STARTS WITH",
            Self::EndsWith => "ENDS WITH",
            Self::IsNull => "IS NULL",
            Self::IsNotNull => "IS NOT NULL",
        }
    }

    pub(super) const fn requires_parameter(self) -> bool {
        !matches!(self, Self::IsNull | Self::IsNotNull)
    }

    pub(super) const fn requires_string_property(self) -> bool {
        matches!(self, Self::Contains | Self::StartsWith | Self::EndsWith)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphRagQueryPredicate {
    pub binding: GraphRagQueryBinding,
    pub property: String,
    pub operator: GraphRagQueryPredicateOperator,
    pub parameter: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphRagQueryProjection {
    pub binding: GraphRagQueryBinding,
    pub property: String,
    pub alias: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphRagQueryDraft {
    pub schema_fingerprint: u64,
    pub pattern: GraphRagQueryPattern,
    pub predicates: Vec<GraphRagQueryPredicate>,
    pub projections: Vec<GraphRagQueryProjection>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphRagGeneratedQuery {
    pub(super) cypher: String,
    pub(super) schema_fingerprint: u64,
    pub(super) context_commit_epoch: u64,
    pub(super) required_parameters: Vec<String>,
    pub(super) parameter_requirements: Vec<GraphRagQueryParameterRequirement>,
}

impl GraphRagGeneratedQuery {
    pub fn cypher(&self) -> &str {
        &self.cypher
    }

    pub const fn schema_fingerprint(&self) -> u64 {
        self.schema_fingerprint
    }

    pub const fn context_commit_epoch(&self) -> u64 {
        self.context_commit_epoch
    }

    pub fn required_parameters(&self) -> &[String] {
        &self.required_parameters
    }

    pub fn parameter_requirements(&self) -> &[GraphRagQueryParameterRequirement] {
        &self.parameter_requirements
    }

    pub fn validate_parameters(
        &self,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<(), GraphRagQueryParameterError> {
        for requirement in &self.parameter_requirements {
            let value = parameters.get(&requirement.name).ok_or_else(|| {
                GraphRagQueryParameterError::Missing {
                    parameter: requirement.name.clone(),
                }
            })?;
            if !requirement.accepts(value) {
                return Err(GraphRagQueryParameterError::TypeMismatch {
                    parameter: requirement.name.clone(),
                    expected: requirement.clone(),
                    actual: value_type_name(value),
                });
            }
        }
        if let Some(parameter) = parameters
            .keys()
            .find(|parameter| !self.required_parameters.contains(parameter))
        {
            return Err(GraphRagQueryParameterError::Unexpected {
                parameter: parameter.clone(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphRagQueryParameterCardinality {
    Scalar,
    List,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphRagQueryParameterRequirement {
    pub name: String,
    pub value_type: PropertyType,
    pub cardinality: GraphRagQueryParameterCardinality,
}

impl GraphRagQueryParameterRequirement {
    fn accepts(&self, value: &Value) -> bool {
        match self.cardinality {
            GraphRagQueryParameterCardinality::Scalar => {
                value_matches_property_type(value, self.value_type)
            }
            GraphRagQueryParameterCardinality::List => match value {
                Value::List(values) => values
                    .iter()
                    .all(|value| value_matches_property_type(value, self.value_type)),
                _ => false,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphRagQueryParameterError {
    Missing {
        parameter: String,
    },
    Unexpected {
        parameter: String,
    },
    TypeMismatch {
        parameter: String,
        expected: GraphRagQueryParameterRequirement,
        actual: &'static str,
    },
}

impl Display for GraphRagQueryParameterError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing { parameter } => {
                write!(formatter, "missing GraphRAG query parameter: {parameter}")
            }
            Self::Unexpected { parameter } => {
                write!(
                    formatter,
                    "unexpected GraphRAG query parameter: {parameter}"
                )
            }
            Self::TypeMismatch {
                parameter,
                expected,
                actual,
            } => write!(
                formatter,
                "GraphRAG query parameter {parameter} must be {}, got {actual}",
                parameter_requirement_name(expected)
            ),
        }
    }
}

impl std::error::Error for GraphRagQueryParameterError {}

fn value_matches_property_type(value: &Value, value_type: PropertyType) -> bool {
    matches!(value, Value::Null)
        || match value_type {
            PropertyType::Any => true,
            PropertyType::Bool => matches!(value, Value::Bool(_)),
            PropertyType::Int => matches!(value, Value::Int(_)),
            PropertyType::Float => matches!(value, Value::Int(_) | Value::Float(_)),
            PropertyType::String | PropertyType::Text => matches!(value, Value::String(_)),
            PropertyType::List => matches!(value, Value::List(_)),
        }
}

fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Int(_) => "int",
        Value::Float(_) => "float",
        Value::String(_) => "string",
        Value::Binary(_) => "binary",
        Value::Uuid(_) => "uuid",
        Value::List(_) => "list",
        Value::Map(_) => "map",
    }
}

fn parameter_requirement_name(requirement: &GraphRagQueryParameterRequirement) -> String {
    let value_type = match requirement.value_type {
        PropertyType::Any => "any",
        PropertyType::Bool => "bool",
        PropertyType::Int => "int",
        PropertyType::Float => "float",
        PropertyType::String => "string",
        PropertyType::Text => "text",
        PropertyType::List => "list",
    };
    match requirement.cardinality {
        GraphRagQueryParameterCardinality::Scalar => value_type.to_string(),
        GraphRagQueryParameterCardinality::List => format!("list<{value_type}>"),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphRagQueryGenerationError {
    SchemaContextIntegrityMismatch {
        expected: u64,
        actual: u64,
    },
    SchemaFingerprintMismatch {
        expected: u64,
        actual: u64,
    },
    InvalidIdentifier {
        kind: &'static str,
        value: String,
    },
    UnknownLabel(String),
    UnknownRoute {
        source_label: String,
        relationship_type: String,
        target_label: String,
    },
    BindingUnavailable(GraphRagQueryBinding),
    PropertyUnavailable {
        binding: GraphRagQueryBinding,
        property: String,
    },
    OperatorRequiresStringProperty {
        binding: GraphRagQueryBinding,
        property: String,
    },
    MissingParameter {
        property: String,
    },
    UnexpectedParameter {
        property: String,
    },
    ConflictingParameterRequirement {
        parameter: String,
        first: GraphRagQueryParameterRequirement,
        second: GraphRagQueryParameterRequirement,
    },
    EmptyProjection,
    InvalidLimit {
        limit: usize,
        maximum: usize,
    },
}

impl Display for GraphRagQueryGenerationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SchemaContextIntegrityMismatch { expected, actual } => write!(
                formatter,
                "schema context integrity mismatch: expected {expected:016x}, got {actual:016x}"
            ),
            Self::SchemaFingerprintMismatch { expected, actual } => write!(
                formatter,
                "schema fingerprint mismatch: expected {expected:016x}, got {actual:016x}"
            ),
            Self::InvalidIdentifier { kind, value } => {
                write!(formatter, "invalid {kind} identifier: {value}")
            }
            Self::UnknownLabel(label) => write!(formatter, "label is not in schema context: {label}"),
            Self::UnknownRoute {
                source_label,
                relationship_type,
                target_label,
            } => write!(
                formatter,
                "route is not in schema context: ({source_label})-[:{relationship_type}]->({target_label})"
            ),
            Self::BindingUnavailable(binding) => {
                write!(formatter, "query binding is unavailable: {binding:?}")
            }
            Self::PropertyUnavailable { binding, property } => write!(
                formatter,
                "property is not in schema context for {binding:?}: {property}"
            ),
            Self::OperatorRequiresStringProperty { binding, property } => write!(
                formatter,
                "predicate requires a string property for {binding:?}: {property}"
            ),
            Self::MissingParameter { property } => {
                write!(formatter, "predicate parameter is required for: {property}")
            }
            Self::UnexpectedParameter { property } => {
                write!(formatter, "predicate parameter is not allowed for: {property}")
            }
            Self::ConflictingParameterRequirement {
                parameter,
                first,
                second,
            } => write!(
                formatter,
                "parameter {parameter} has conflicting requirements: {} and {}",
                parameter_requirement_name(first),
                parameter_requirement_name(second)
            ),
            Self::EmptyProjection => formatter.write_str("at least one projection is required"),
            Self::InvalidLimit { limit, maximum } => {
                write!(formatter, "query limit must be between 1 and {maximum}, got {limit}")
            }
        }
    }
}

impl std::error::Error for GraphRagQueryGenerationError {}
