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

use crate::{Parameters, QueryInvocation, ResultSemantics};
use hawdb::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScalarType {
    Integer,
    String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GeneratedSchema {
    nodes: BTreeMap<&'static str, BTreeMap<&'static str, ScalarType>>,
    relationships: BTreeMap<&'static str, BTreeMap<&'static str, ScalarType>>,
}

impl GeneratedSchema {
    pub(crate) fn nowledge_fixture() -> Self {
        Self {
            nodes: BTreeMap::from([
                (
                    "Memory",
                    BTreeMap::from([
                        ("id", ScalarType::String),
                        ("kind", ScalarType::String),
                        ("title", ScalarType::String),
                        ("importance", ScalarType::Integer),
                        ("optional_note", ScalarType::String),
                        ("optional_score", ScalarType::Integer),
                    ]),
                ),
                (
                    "Entity",
                    BTreeMap::from([("id", ScalarType::String), ("name", ScalarType::String)]),
                ),
            ]),
            relationships: BTreeMap::from([
                (
                    "MENTIONS",
                    BTreeMap::from([("weight", ScalarType::Integer)]),
                ),
                (
                    "RELATES_TO",
                    BTreeMap::from([("weight", ScalarType::Integer)]),
                ),
            ]),
        }
    }

    fn node_property_type(&self, label: &str, property: &str) -> Option<ScalarType> {
        self.nodes
            .get(label)
            .and_then(|properties| properties.get(property))
            .copied()
    }

    fn relationship_property_type(&self, relationship: &str, property: &str) -> Option<ScalarType> {
        self.relationships
            .get(relationship)
            .and_then(|properties| properties.get(property))
            .copied()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PatternDirection {
    Outgoing,
    Incoming,
}

impl PatternDirection {
    fn reversed(self) -> Self {
        match self {
            Self::Outgoing => Self::Incoming,
            Self::Incoming => Self::Outgoing,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NodePattern {
    pub variable: &'static str,
    pub label: &'static str,
    pub property_parameters: Vec<(&'static str, &'static str)>,
}

impl NodePattern {
    pub(crate) const fn new(variable: &'static str, label: &'static str) -> Self {
        Self {
            variable,
            label,
            property_parameters: Vec::new(),
        }
    }

    pub(crate) fn with_property_parameter(
        mut self,
        property: &'static str,
        parameter: &'static str,
    ) -> Self {
        self.property_parameters.push((property, parameter));
        self
    }

    fn render(&self) -> String {
        let properties = if self.property_parameters.is_empty() {
            String::new()
        } else {
            format!(
                " {{{}}}",
                self.property_parameters
                    .iter()
                    .map(|(property, parameter)| format!("{property}: ${parameter}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        format!("({}:{}{})", self.variable, self.label, properties)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MatchPattern {
    Node(NodePattern),
    Relationship {
        source: NodePattern,
        relationship_variable: &'static str,
        relationship_type: &'static str,
        direction: PatternDirection,
        target: NodePattern,
    },
}

impl MatchPattern {
    fn render(&self) -> String {
        match self {
            Self::Node(node) => node.render(),
            Self::Relationship {
                source,
                relationship_variable,
                relationship_type,
                direction,
                target,
            } => match direction {
                PatternDirection::Outgoing => format!(
                    "{}-[{relationship_variable}:{relationship_type}]->{}",
                    source.render(),
                    target.render()
                ),
                PatternDirection::Incoming => format!(
                    "{}<-[{relationship_variable}:{relationship_type}]-{}",
                    source.render(),
                    target.render()
                ),
            },
        }
    }

    fn reverse_direction(&mut self) -> bool {
        let Self::Relationship { direction, .. } = self else {
            return false;
        };
        *direction = direction.reversed();
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PropertyExpression {
    pub variable: &'static str,
    pub property: &'static str,
}

impl PropertyExpression {
    pub(crate) const fn new(variable: &'static str, property: &'static str) -> Self {
        Self { variable, property }
    }

    fn render(self) -> String {
        format!("{}.{}", self.variable, self.property)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum QueryPredicate {
    Equal(PropertyExpression, &'static str),
    In(PropertyExpression, &'static str),
    GreaterThanOrEqual(PropertyExpression, &'static str),
    IsNull(PropertyExpression),
}

impl QueryPredicate {
    fn render(&self) -> String {
        match self {
            Self::Equal(property, parameter) => {
                format!("{} = ${parameter}", property.render())
            }
            Self::In(property, parameter) => format!("{} IN ${parameter}", property.render()),
            Self::GreaterThanOrEqual(property, parameter) => {
                format!("{} >= ${parameter}", property.render())
            }
            Self::IsNull(property) => format!("{} IS NULL", property.render()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReturnExpression {
    Property(PropertyExpression),
    Count(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReturnItem {
    pub expression: ReturnExpression,
    pub alias: &'static str,
}

impl ReturnItem {
    pub(crate) const fn property(
        variable: &'static str,
        property: &'static str,
        alias: &'static str,
    ) -> Self {
        Self {
            expression: ReturnExpression::Property(PropertyExpression::new(variable, property)),
            alias,
        }
    }

    pub(crate) const fn count(variable: &'static str, alias: &'static str) -> Self {
        Self {
            expression: ReturnExpression::Count(variable),
            alias,
        }
    }

    fn render(&self) -> String {
        let expression = match self.expression {
            ReturnExpression::Property(property) => property.render(),
            ReturnExpression::Count(variable) => format!("count({variable})"),
        };
        format!("{expression} AS {}", self.alias)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OrderDirection {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OrderItem {
    pub alias: &'static str,
    pub direction: OrderDirection,
}

impl OrderItem {
    pub(crate) const fn ascending(alias: &'static str) -> Self {
        Self {
            alias,
            direction: OrderDirection::Ascending,
        }
    }

    pub(crate) const fn descending(alias: &'static str) -> Self {
        Self {
            alias,
            direction: OrderDirection::Descending,
        }
    }

    fn render(&self) -> String {
        let direction = match self.direction {
            OrderDirection::Ascending => "ASC",
            OrderDirection::Descending => "DESC",
        };
        format!("{} {direction}", self.alias)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueryAst {
    pub matches: Vec<MatchPattern>,
    pub predicate: Option<QueryPredicate>,
    pub returns: Vec<ReturnItem>,
    pub distinct: bool,
    pub order_by: Vec<OrderItem>,
    pub limit: Option<usize>,
    pub parameters: Parameters,
}

impl QueryAst {
    pub(crate) fn invocation(&self) -> QueryInvocation {
        QueryInvocation {
            cypher: self.render(),
            parameters: self.parameters.clone(),
            result_semantics: if self.order_by.is_empty() {
                ResultSemantics::Bag
            } else {
                ResultSemantics::Ordered
            },
        }
    }

    pub(crate) fn validate(&self, schema: &GeneratedSchema) -> Result<(), String> {
        if self.matches.is_empty() || self.returns.is_empty() {
            return Err("query requires at least one match and return item".to_string());
        }
        let mut variables = BTreeMap::new();
        for pattern in &self.matches {
            match pattern {
                MatchPattern::Node(node) => {
                    validate_node_pattern(node, schema, &self.parameters)?;
                    variables.insert(node.variable, VariableType::Node(node.label));
                }
                MatchPattern::Relationship {
                    source,
                    relationship_variable,
                    relationship_type,
                    target,
                    ..
                } => {
                    validate_node_pattern(source, schema, &self.parameters)?;
                    validate_node_pattern(target, schema, &self.parameters)?;
                    if !schema.relationships.contains_key(relationship_type) {
                        return Err(format!("unknown relationship type {relationship_type}"));
                    }
                    variables.insert(source.variable, VariableType::Node(source.label));
                    variables.insert(target.variable, VariableType::Node(target.label));
                    variables.insert(
                        relationship_variable,
                        VariableType::Relationship(relationship_type),
                    );
                }
            }
        }
        if let Some(predicate) = &self.predicate {
            validate_predicate(predicate, schema, &variables, &self.parameters)?;
        }
        for item in &self.returns {
            match item.expression {
                ReturnExpression::Property(property) => {
                    property_type(property, schema, &variables)?;
                }
                ReturnExpression::Count(variable) if !variables.contains_key(variable) => {
                    return Err(format!("unknown count variable {variable}"));
                }
                ReturnExpression::Count(_) => {}
            }
        }
        let aliases = self
            .returns
            .iter()
            .map(|item| item.alias)
            .collect::<BTreeSet<_>>();
        if let Some(order) = self
            .order_by
            .iter()
            .find(|order| !aliases.contains(order.alias))
        {
            return Err(format!("ORDER BY references unknown alias {}", order.alias));
        }
        Ok(())
    }

    pub(crate) fn node_count(&self) -> usize {
        self.matches.len()
            + usize::from(self.predicate.is_some())
            + self.returns.len()
            + self.order_by.len()
            + usize::from(self.limit.is_some())
            + usize::from(self.distinct)
    }

    pub(crate) fn reduction_candidates(&self) -> Vec<Self> {
        let mut candidates = Vec::new();
        if self.predicate.is_some() {
            let mut candidate = self.clone();
            candidate.predicate = None;
            candidates.push(candidate);
        }
        if self.limit.is_some() {
            let mut candidate = self.clone();
            candidate.limit = None;
            candidates.push(candidate);
        }
        if self.distinct {
            let mut candidate = self.clone();
            candidate.distinct = false;
            candidates.push(candidate);
        }
        for index in 0..self.order_by.len() {
            let mut candidate = self.clone();
            candidate.order_by.remove(index);
            candidates.push(candidate);
        }
        if self.returns.len() > 1 {
            for index in 0..self.returns.len() {
                let mut candidate = self.clone();
                let removed_alias = candidate.returns.remove(index).alias;
                candidate
                    .order_by
                    .retain(|order| order.alias != removed_alias);
                candidates.push(candidate);
            }
        }
        candidates
    }

    pub(crate) fn reversed_directions(&self) -> Option<Self> {
        let mut reversed = self.clone();
        let mut applicable = false;
        for pattern in &mut reversed.matches {
            applicable |= pattern.reverse_direction();
        }
        applicable.then_some(reversed)
    }

    pub(crate) fn map_identifier_parameters(&self, prefix: &str) -> Self {
        let mut mapped = self.clone();
        for value in mapped.parameters.values_mut() {
            map_identifier_value(value, prefix);
        }
        mapped
    }

    pub(crate) fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "cypher": self.render(),
            "node_count": self.node_count(),
            "match_count": self.matches.len(),
            "predicate_present": self.predicate.is_some(),
            "return_count": self.returns.len(),
            "order_count": self.order_by.len(),
            "distinct": self.distinct,
            "limit": self.limit,
        })
    }

    fn render(&self) -> String {
        let mut query = format!(
            "MATCH {}",
            self.matches
                .iter()
                .map(MatchPattern::render)
                .collect::<Vec<_>>()
                .join(", ")
        );
        if let Some(predicate) = &self.predicate {
            query.push_str(" WHERE ");
            query.push_str(&predicate.render());
        }
        query.push_str(" RETURN ");
        if self.distinct {
            query.push_str("DISTINCT ");
        }
        query.push_str(
            &self
                .returns
                .iter()
                .map(ReturnItem::render)
                .collect::<Vec<_>>()
                .join(", "),
        );
        if !self.order_by.is_empty() {
            query.push_str(" ORDER BY ");
            query.push_str(
                &self
                    .order_by
                    .iter()
                    .map(OrderItem::render)
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }
        if let Some(limit) = self.limit {
            query.push_str(&format!(" LIMIT {limit}"));
        }
        query
    }
}

#[derive(Debug, Clone, Copy)]
enum VariableType {
    Node(&'static str),
    Relationship(&'static str),
}

fn validate_node_pattern(
    node: &NodePattern,
    schema: &GeneratedSchema,
    parameters: &Parameters,
) -> Result<(), String> {
    if !schema.nodes.contains_key(node.label) {
        return Err(format!("unknown node label {}", node.label));
    }
    for (property, parameter) in &node.property_parameters {
        let expected = schema
            .node_property_type(node.label, property)
            .ok_or_else(|| format!("unknown property {}.{property}", node.label))?;
        validate_parameter_type(parameter, expected, parameters, false)?;
    }
    Ok(())
}

fn validate_predicate(
    predicate: &QueryPredicate,
    schema: &GeneratedSchema,
    variables: &BTreeMap<&'static str, VariableType>,
    parameters: &Parameters,
) -> Result<(), String> {
    match predicate {
        QueryPredicate::Equal(property, parameter)
        | QueryPredicate::GreaterThanOrEqual(property, parameter) => {
            let expected = property_type(*property, schema, variables)?;
            validate_parameter_type(parameter, expected, parameters, false)
        }
        QueryPredicate::In(property, parameter) => {
            let expected = property_type(*property, schema, variables)?;
            validate_parameter_type(parameter, expected, parameters, true)
        }
        QueryPredicate::IsNull(property) => {
            property_type(*property, schema, variables)?;
            Ok(())
        }
    }
}

fn property_type(
    property: PropertyExpression,
    schema: &GeneratedSchema,
    variables: &BTreeMap<&'static str, VariableType>,
) -> Result<ScalarType, String> {
    match variables.get(property.variable) {
        Some(VariableType::Node(label)) => schema
            .node_property_type(label, property.property)
            .ok_or_else(|| format!("unknown property {label}.{}", property.property)),
        Some(VariableType::Relationship(relationship)) => schema
            .relationship_property_type(relationship, property.property)
            .ok_or_else(|| {
                format!(
                    "unknown relationship property {relationship}.{}",
                    property.property
                )
            }),
        None => Err(format!("unknown variable {}", property.variable)),
    }
}

fn validate_parameter_type(
    parameter: &str,
    expected: ScalarType,
    parameters: &Parameters,
    list: bool,
) -> Result<(), String> {
    let value = parameters
        .get(parameter)
        .ok_or_else(|| format!("missing parameter ${parameter}"))?;
    let values = if list {
        let Value::List(values) = value else {
            return Err(format!("parameter ${parameter} must be a list"));
        };
        values.as_slice()
    } else {
        std::slice::from_ref(value)
    };
    if values.iter().all(|value| {
        matches!(
            (expected, value),
            (ScalarType::Integer, Value::Int(_)) | (ScalarType::String, Value::String(_))
        )
    }) {
        Ok(())
    } else {
        Err(format!("parameter ${parameter} has the wrong scalar type"))
    }
}

fn map_identifier_value(value: &mut Value, prefix: &str) {
    match value {
        Value::String(identifier)
            if identifier.starts_with("mem-") || identifier.starts_with("entity-") =>
        {
            identifier.insert_str(0, prefix);
        }
        Value::List(values) => {
            for value in values {
                map_identifier_value(value, prefix);
            }
        }
        _ => {}
    }
}
