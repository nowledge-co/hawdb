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

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use hawdb_sql_syntax::{
    BinaryOperatorSyntax, ExpressionKindSyntax, ExpressionSyntax, GraphElementPatternSyntax,
    GraphPathPrimarySyntax, GraphPathSyntax, GraphTable, Identifier, LiteralSyntax,
    PostgresFromItemSyntax, PostgresSelectSyntax, QualifiedName, Span, TableAlias,
};

use super::{
    BoundPgqColumn, BoundPgqExpression, BoundPgqExpressionKind, BoundPgqGraphPattern,
    BoundPgqGraphTable, BoundPgqLiteral, BoundPgqPath, BoundPgqPathFactor, BoundPgqPathPrimary,
    BoundPgqVariable, PgqBindingContext, PgqCatalog, PgqDataType, PgqElementKind, PgqSlotId,
    PropertyGraphElementSchema, PropertyGraphSchema,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgqBindErrorCode {
    UnknownGraph,
    UnknownLabel,
    DuplicateVariable,
    ConflictingVariable,
    InvalidPath,
    UnknownReference,
    UnknownProperty,
    MissingColumnName,
    DuplicateColumn,
    ColumnAliasCountMismatch,
    UnsupportedExpression,
    InvalidLiteral,
    TypeMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgqBindError {
    pub code: PgqBindErrorCode,
    pub span: Span,
    pub message: String,
}

impl PgqBindError {
    fn new(code: PgqBindErrorCode, span: Span, message: impl Into<String>) -> Self {
        Self {
            code,
            span,
            message: message.into(),
        }
    }
}

impl fmt::Display for PgqBindError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "SQL/PGQ binding error {:?} at bytes {}..{}: {}",
            self.code, self.span.start, self.span.end, self.message
        )
    }
}

impl std::error::Error for PgqBindError {}

pub fn bind_postgres_graph_tables(
    input: &str,
    select: &PostgresSelectSyntax,
    catalog: &dyn PgqCatalog,
    context: &PgqBindingContext,
) -> Result<Vec<BoundPgqGraphTable>, PgqBindError> {
    let mut bound = Vec::new();
    for source in &select.from {
        bind_from_item(input, &source.relation, catalog, context, &mut bound)?;
        for join in &source.joins {
            bind_from_item(input, &join.relation, catalog, context, &mut bound)?;
        }
    }
    Ok(bound)
}

fn bind_from_item(
    input: &str,
    item: &PostgresFromItemSyntax,
    catalog: &dyn PgqCatalog,
    context: &PgqBindingContext,
    bound: &mut Vec<BoundPgqGraphTable>,
) -> Result<(), PgqBindError> {
    let PostgresFromItemSyntax::GraphTable(table) = item else {
        return Ok(());
    };
    let graph_name = normalize_qualified_name(input, &table.graph);
    let graph = catalog.property_graph(&graph_name).ok_or_else(|| {
        PgqBindError::new(
            PgqBindErrorCode::UnknownGraph,
            table.graph.span,
            format!("property graph {} does not exist", graph_name.join(".")),
        )
    })?;
    bound.push(GraphTableBinder::new(input, graph, context).bind(table, graph_name)?);
    Ok(())
}

struct GraphTableBinder<'a> {
    input: &'a str,
    graph: &'a PropertyGraphSchema,
    context: &'a PgqBindingContext,
    variables: Vec<BoundPgqVariable>,
    named_slots: BTreeMap<String, PgqSlotId>,
    slots_by_span: BTreeMap<(usize, usize), PgqSlotId>,
}

impl<'a> GraphTableBinder<'a> {
    fn new(input: &'a str, graph: &'a PropertyGraphSchema, context: &'a PgqBindingContext) -> Self {
        Self {
            input,
            graph,
            context,
            variables: Vec::new(),
            named_slots: BTreeMap::new(),
            slots_by_span: BTreeMap::new(),
        }
    }

    fn bind(
        mut self,
        table: &GraphTable,
        graph_name: Vec<String>,
    ) -> Result<BoundPgqGraphTable, PgqBindError> {
        if table.pattern.paths.len() != 1 {
            return Err(PgqBindError::new(
                PgqBindErrorCode::InvalidPath,
                table.pattern.span,
                "the qualified GRAPH_TABLE slice requires exactly one graph path",
            ));
        }
        for path in &table.pattern.paths {
            self.validate_path_shape(path)?;
            self.register_path(path)?;
        }

        let paths = table
            .pattern
            .paths
            .iter()
            .map(|path| self.bind_path(path))
            .collect::<Result<Vec<_>, _>>()?;
        let predicate = table
            .pattern
            .predicate
            .as_ref()
            .map(|predicate| self.bind_predicate(predicate))
            .transpose()?;
        let mut columns = self.bind_columns(&table.columns)?;
        if let Some(alias) = &table.alias {
            self.apply_table_column_aliases(alias, &mut columns)?;
        }
        let alias = table
            .alias
            .as_ref()
            .map(|alias| normalize_table_alias(self.input, alias));

        Ok(BoundPgqGraphTable {
            graph_name,
            variables: self.variables,
            pattern: BoundPgqGraphPattern { paths, predicate },
            columns,
            alias,
            span: table.span,
        })
    }

    fn validate_path_shape(&self, path: &GraphPathSyntax) -> Result<(), PgqBindError> {
        if path.factors.len() == 1
            && matches!(
                path.factors[0].primary,
                GraphPathPrimarySyntax::Parenthesized { .. }
            )
        {
            return Ok(());
        }
        let mut expect_vertex = true;
        for factor in &path.factors {
            let is_vertex = matches!(factor.primary, GraphPathPrimarySyntax::Vertex(_));
            let is_edge = matches!(factor.primary, GraphPathPrimarySyntax::Edge(_));
            if !is_vertex && !is_edge {
                return Err(PgqBindError::new(
                    PgqBindErrorCode::InvalidPath,
                    factor.span,
                    "a parenthesized path must be a standalone path factor in this slice",
                ));
            }
            if is_vertex != expect_vertex {
                return Err(PgqBindError::new(
                    PgqBindErrorCode::InvalidPath,
                    factor.span,
                    "graph paths must alternate vertex and edge patterns and end at a vertex",
                ));
            }
            if factor.quantifier.is_some() && is_vertex {
                return Err(PgqBindError::new(
                    PgqBindErrorCode::InvalidPath,
                    factor.span,
                    "vertex pattern quantifiers are not supported",
                ));
            }
            if let Some(quantifier) = &factor.quantifier
                && quantifier.min > quantifier.max
            {
                return Err(PgqBindError::new(
                    PgqBindErrorCode::InvalidPath,
                    quantifier.span,
                    "graph quantifier lower bound exceeds upper bound",
                ));
            }
            expect_vertex = !expect_vertex;
        }
        if expect_vertex {
            return Err(PgqBindError::new(
                PgqBindErrorCode::InvalidPath,
                path.span,
                "graph paths must end at a vertex pattern",
            ));
        }
        Ok(())
    }

    fn register_path(&mut self, path: &GraphPathSyntax) -> Result<(), PgqBindError> {
        for factor in &path.factors {
            match &factor.primary {
                GraphPathPrimarySyntax::Vertex(vertex) => {
                    self.register_element(vertex, PgqElementKind::Vertex)?;
                }
                GraphPathPrimarySyntax::Edge(edge) => {
                    self.register_element_fields(
                        edge.variable.as_ref(),
                        &edge.labels,
                        PgqElementKind::Edge,
                        edge.span,
                    )?;
                }
                GraphPathPrimarySyntax::Parenthesized { path, .. } => {
                    self.validate_path_shape(path)?;
                    self.register_path(path)?;
                }
            }
        }
        Ok(())
    }

    fn register_element(
        &mut self,
        element: &GraphElementPatternSyntax,
        kind: PgqElementKind,
    ) -> Result<PgqSlotId, PgqBindError> {
        self.register_element_fields(
            element.variable.as_ref(),
            &element.labels,
            kind,
            element.span,
        )
    }

    fn register_element_fields(
        &mut self,
        variable: Option<&Identifier>,
        labels: &[Identifier],
        kind: PgqElementKind,
        span: Span,
    ) -> Result<PgqSlotId, PgqBindError> {
        let labels = labels
            .iter()
            .map(|label| normalize_identifier(self.input, label))
            .collect::<Vec<_>>();
        self.validate_labels(kind, &labels, span)?;
        let name = variable.map(|variable| normalize_identifier(self.input, variable));
        let slot = if let Some(name) = &name {
            if let Some(slot) = self.named_slots.get(name).copied() {
                let existing = &mut self.variables[slot.0 as usize];
                if existing.kind != kind {
                    return Err(PgqBindError::new(
                        PgqBindErrorCode::ConflictingVariable,
                        span,
                        format!("graph variable {name} is used as both vertex and edge"),
                    ));
                }
                if !labels.is_empty() {
                    if existing.labels.is_empty() {
                        existing.labels.clone_from(&labels);
                    } else if existing.labels != labels {
                        return Err(PgqBindError::new(
                            PgqBindErrorCode::ConflictingVariable,
                            span,
                            format!("graph variable {name} has conflicting label predicates"),
                        ));
                    }
                }
                slot
            } else {
                let slot = self.next_slot(span)?;
                self.named_slots.insert(name.clone(), slot);
                self.variables.push(BoundPgqVariable {
                    slot,
                    name: Some(name.clone()),
                    kind,
                    labels,
                    span,
                });
                slot
            }
        } else {
            let slot = self.next_slot(span)?;
            self.variables.push(BoundPgqVariable {
                slot,
                name: None,
                kind,
                labels,
                span,
            });
            slot
        };
        self.slots_by_span.insert((span.start, span.end), slot);
        Ok(slot)
    }

    fn next_slot(&self, span: Span) -> Result<PgqSlotId, PgqBindError> {
        u32::try_from(self.variables.len())
            .map(PgqSlotId)
            .map_err(|_| {
                PgqBindError::new(
                    PgqBindErrorCode::DuplicateVariable,
                    span,
                    "too many graph variables",
                )
            })
    }

    fn validate_labels(
        &self,
        kind: PgqElementKind,
        labels: &[String],
        span: Span,
    ) -> Result<(), PgqBindError> {
        let schemas = self.element_schemas(kind);
        for label in labels {
            if !schemas.contains_key(label) {
                return Err(PgqBindError::new(
                    PgqBindErrorCode::UnknownLabel,
                    span,
                    format!("unknown {} label {label}", element_kind_name(kind)),
                ));
            }
        }
        Ok(())
    }

    fn bind_path(&self, path: &GraphPathSyntax) -> Result<BoundPgqPath, PgqBindError> {
        let factors = path
            .factors
            .iter()
            .map(|factor| {
                let primary = match &factor.primary {
                    GraphPathPrimarySyntax::Vertex(vertex) => BoundPgqPathPrimary::Vertex {
                        slot: self.slot_for_span(vertex.span)?,
                        predicate: vertex
                            .predicate
                            .as_ref()
                            .map(|predicate| self.bind_predicate(predicate))
                            .transpose()?,
                    },
                    GraphPathPrimarySyntax::Edge(edge) => BoundPgqPathPrimary::Edge {
                        slot: self.slot_for_span(edge.span)?,
                        direction: edge.direction,
                        predicate: edge
                            .predicate
                            .as_ref()
                            .map(|predicate| self.bind_predicate(predicate))
                            .transpose()?,
                    },
                    GraphPathPrimarySyntax::Parenthesized {
                        path, predicate, ..
                    } => BoundPgqPathPrimary::Parenthesized {
                        path: Box::new(self.bind_path(path)?),
                        predicate: predicate
                            .as_ref()
                            .map(|predicate| self.bind_predicate(predicate))
                            .transpose()?,
                    },
                };
                Ok(BoundPgqPathFactor {
                    primary,
                    quantifier: factor.quantifier.clone(),
                })
            })
            .collect::<Result<Vec<_>, PgqBindError>>()?;
        Ok(BoundPgqPath { factors })
    }

    fn slot_for_span(&self, span: Span) -> Result<PgqSlotId, PgqBindError> {
        self.slots_by_span
            .get(&(span.start, span.end))
            .copied()
            .ok_or_else(|| {
                PgqBindError::new(
                    PgqBindErrorCode::UnknownReference,
                    span,
                    "graph element has no registered slot",
                )
            })
    }

    fn bind_columns(
        &self,
        columns: &[hawdb_sql_syntax::GraphTableColumn],
    ) -> Result<Vec<BoundPgqColumn>, PgqBindError> {
        let mut names = BTreeSet::new();
        columns
            .iter()
            .map(|column| {
                let expression = self.bind_expression(&column.expression)?;
                let name = if let Some(alias) = &column.alias {
                    normalize_identifier(self.input, alias)
                } else {
                    derive_column_name(self.input, &column.expression).ok_or_else(|| {
                        PgqBindError::new(
                            PgqBindErrorCode::MissingColumnName,
                            column.span,
                            "complex GRAPH_TABLE output expressions require an explicit alias",
                        )
                    })?
                };
                if !names.insert(name.clone()) {
                    return Err(PgqBindError::new(
                        PgqBindErrorCode::DuplicateColumn,
                        column.span,
                        format!("duplicate GRAPH_TABLE output column {name}"),
                    ));
                }
                Ok(BoundPgqColumn {
                    name,
                    data_type: expression.data_type,
                    expression,
                })
            })
            .collect()
    }

    fn apply_table_column_aliases(
        &self,
        alias: &TableAlias,
        columns: &mut [BoundPgqColumn],
    ) -> Result<(), PgqBindError> {
        if alias.columns.len() > columns.len() {
            return Err(PgqBindError::new(
                PgqBindErrorCode::ColumnAliasCountMismatch,
                alias.span,
                "GRAPH_TABLE alias has more column names than its output schema",
            ));
        }
        for (column, alias) in columns.iter_mut().zip(&alias.columns) {
            column.name = normalize_identifier(self.input, alias);
        }
        let mut names = BTreeSet::new();
        for column in columns {
            if !names.insert(column.name.clone()) {
                return Err(PgqBindError::new(
                    PgqBindErrorCode::DuplicateColumn,
                    alias.span,
                    format!("duplicate GRAPH_TABLE output column {}", column.name),
                ));
            }
        }
        Ok(())
    }

    fn bind_predicate(
        &self,
        expression: &ExpressionSyntax,
    ) -> Result<BoundPgqExpression, PgqBindError> {
        let bound = self.bind_expression(expression)?;
        if !matches!(bound.data_type, PgqDataType::Boolean | PgqDataType::Unknown) {
            return Err(PgqBindError::new(
                PgqBindErrorCode::TypeMismatch,
                expression.span,
                "graph predicates must have boolean type",
            ));
        }
        Ok(bound)
    }

    fn bind_expression(
        &self,
        expression: &ExpressionSyntax,
    ) -> Result<BoundPgqExpression, PgqBindError> {
        let (kind, data_type) = match &expression.kind {
            ExpressionKindSyntax::Column(name) => self.bind_column_reference(name)?,
            ExpressionKindSyntax::Wildcard(_) => {
                return Err(PgqBindError::new(
                    PgqBindErrorCode::UnsupportedExpression,
                    expression.span,
                    "wildcards are not scalar GRAPH_TABLE expressions",
                ));
            }
            ExpressionKindSyntax::Parameter(position) => (
                BoundPgqExpressionKind::Parameter(*position),
                PgqDataType::Unknown,
            ),
            ExpressionKindSyntax::Literal(literal) => {
                let (literal, data_type) = bind_literal(self.input, literal)?;
                (BoundPgqExpressionKind::Literal(literal), data_type)
            }
            ExpressionKindSyntax::TypedString { data_type, value } => {
                let data_type_name = normalize_identifier(self.input, data_type);
                let value = decode_string(self.input, *value).ok_or_else(|| {
                    PgqBindError::new(
                        PgqBindErrorCode::InvalidLiteral,
                        *value,
                        "invalid PostgreSQL string literal",
                    )
                })?;
                (
                    BoundPgqExpressionKind::Literal(BoundPgqLiteral::TypedString {
                        data_type: data_type_name,
                        value,
                    }),
                    PgqDataType::String,
                )
            }
            ExpressionKindSyntax::Unary {
                operator,
                expression: inner,
            } => {
                let inner = self.bind_expression(inner)?;
                let data_type = match operator {
                    hawdb_sql_syntax::UnaryOperatorSyntax::Not => {
                        require_boolean(inner.data_type, expression.span)?;
                        PgqDataType::Boolean
                    }
                    hawdb_sql_syntax::UnaryOperatorSyntax::Plus
                    | hawdb_sql_syntax::UnaryOperatorSyntax::Minus => {
                        require_numeric(inner.data_type, expression.span)?;
                        inner.data_type
                    }
                };
                (
                    BoundPgqExpressionKind::Unary {
                        operator: *operator,
                        expression: Box::new(inner),
                    },
                    data_type,
                )
            }
            ExpressionKindSyntax::Binary {
                left,
                operator,
                right,
            } => {
                let left = self.bind_expression(left)?;
                let right = self.bind_expression(right)?;
                validate_binary_operands(
                    *operator,
                    left.data_type,
                    right.data_type,
                    expression.span,
                )?;
                let data_type = binary_result_type(*operator, left.data_type, right.data_type);
                (
                    BoundPgqExpressionKind::Binary {
                        left: Box::new(left),
                        operator: *operator,
                        right: Box::new(right),
                    },
                    data_type,
                )
            }
            ExpressionKindSyntax::IsNull {
                expression: inner,
                negated,
            } => (
                BoundPgqExpressionKind::IsNull {
                    expression: Box::new(self.bind_expression(inner)?),
                    negated: *negated,
                },
                PgqDataType::Boolean,
            ),
            ExpressionKindSyntax::InList {
                expression: inner,
                values,
                negated,
            } => {
                let inner = self.bind_expression(inner)?;
                let values = values
                    .iter()
                    .map(|value| self.bind_expression(value))
                    .collect::<Result<Vec<_>, _>>()?;
                for value in &values {
                    require_comparable(inner.data_type, value.data_type, expression.span)?;
                }
                (
                    BoundPgqExpressionKind::InList {
                        expression: Box::new(inner),
                        values,
                        negated: *negated,
                    },
                    PgqDataType::Boolean,
                )
            }
            ExpressionKindSyntax::Between {
                expression: inner,
                low,
                high,
                negated,
            } => {
                let inner = self.bind_expression(inner)?;
                let low = self.bind_expression(low)?;
                let high = self.bind_expression(high)?;
                require_comparable(inner.data_type, low.data_type, expression.span)?;
                require_comparable(inner.data_type, high.data_type, expression.span)?;
                (
                    BoundPgqExpressionKind::Between {
                        expression: Box::new(inner),
                        low: Box::new(low),
                        high: Box::new(high),
                        negated: *negated,
                    },
                    PgqDataType::Boolean,
                )
            }
            ExpressionKindSyntax::Function {
                name,
                arguments,
                distinct,
            } => {
                let name = normalize_qualified_name(self.input, name);
                let function = name.last().map(String::as_str).unwrap_or_default();
                if is_disallowed_graph_table_function(function) {
                    return Err(PgqBindError::new(
                        PgqBindErrorCode::UnsupportedExpression,
                        expression.span,
                        format!("function {function} is not allowed in GRAPH_TABLE expressions"),
                    ));
                }
                let arguments = arguments
                    .iter()
                    .map(|argument| self.bind_expression(argument))
                    .collect::<Result<Vec<_>, _>>()?;
                let data_type = function_result_type(function, &arguments);
                (
                    BoundPgqExpressionKind::Function {
                        name,
                        arguments,
                        distinct: *distinct,
                    },
                    data_type,
                )
            }
            ExpressionKindSyntax::Cast {
                expression: inner,
                data_type,
            } => {
                let inner = self.bind_expression(inner)?;
                let data_type_name = normalize_qualified_name(self.input, data_type);
                let result_type = cast_result_type(&data_type_name);
                (
                    BoundPgqExpressionKind::Cast {
                        expression: Box::new(inner),
                        data_type: data_type_name,
                    },
                    result_type,
                )
            }
            ExpressionKindSyntax::Collate {
                expression: inner,
                collation,
            } => {
                let inner = self.bind_expression(inner)?;
                require_string(inner.data_type, expression.span)?;
                let data_type = inner.data_type;
                (
                    BoundPgqExpressionKind::Collate {
                        expression: Box::new(inner),
                        collation: normalize_qualified_name(self.input, collation),
                    },
                    data_type,
                )
            }
            ExpressionKindSyntax::Parenthesized(inner) => {
                let inner = self.bind_expression(inner)?;
                return Ok(BoundPgqExpression {
                    span: expression.span,
                    ..inner
                });
            }
        };
        Ok(BoundPgqExpression {
            kind,
            data_type,
            span: expression.span,
        })
    }

    fn bind_column_reference(
        &self,
        name: &QualifiedName,
    ) -> Result<(BoundPgqExpressionKind, PgqDataType), PgqBindError> {
        let parts = normalize_qualified_name(self.input, name);
        if let Some(variable) = parts
            .first()
            .and_then(|variable| self.named_slots.get(variable))
            .and_then(|slot| self.variables.get(slot.0 as usize))
        {
            return match parts.as_slice() {
                [_] => Ok((
                    BoundPgqExpressionKind::Variable(variable.slot),
                    PgqDataType::GraphElement(variable.kind),
                )),
                [_, property] => Ok((
                    BoundPgqExpressionKind::Property {
                        slot: variable.slot,
                        property: property.clone(),
                    },
                    self.property_type(variable, property, name.span)?,
                )),
                _ => Err(PgqBindError::new(
                    PgqBindErrorCode::UnsupportedExpression,
                    name.span,
                    "nested graph property paths are not supported",
                )),
            };
        }
        if let Some(data_type) = self.context.outer_columns.get(&parts).copied() {
            return Ok((BoundPgqExpressionKind::OuterColumn(parts), data_type));
        }
        Err(PgqBindError::new(
            PgqBindErrorCode::UnknownReference,
            name.span,
            format!(
                "unknown GRAPH_TABLE expression reference {}",
                parts.join(".")
            ),
        ))
    }

    fn property_type(
        &self,
        variable: &BoundPgqVariable,
        property: &str,
        span: Span,
    ) -> Result<PgqDataType, PgqBindError> {
        let schemas = self.element_schemas(variable.kind);
        let candidates = if variable.labels.is_empty() {
            schemas.values().collect::<Vec<_>>()
        } else {
            variable
                .labels
                .iter()
                .filter_map(|label| schemas.get(label))
                .collect::<Vec<_>>()
        };
        let mut types = candidates
            .into_iter()
            .filter_map(|schema| schema.properties.get(property).copied());
        let Some(first) = types.next() else {
            return Err(PgqBindError::new(
                PgqBindErrorCode::UnknownProperty,
                span,
                format!("unknown property {property} for graph variable"),
            ));
        };
        Ok(if types.all(|data_type| data_type == first) {
            first
        } else {
            PgqDataType::Unknown
        })
    }

    fn element_schemas(
        &self,
        kind: PgqElementKind,
    ) -> &BTreeMap<String, PropertyGraphElementSchema> {
        match kind {
            PgqElementKind::Vertex => &self.graph.vertex_labels,
            PgqElementKind::Edge => &self.graph.edge_labels,
        }
    }
}

fn normalize_table_alias(input: &str, alias: &TableAlias) -> String {
    normalize_identifier(input, &alias.name)
}

fn normalize_qualified_name(input: &str, name: &QualifiedName) -> Vec<String> {
    name.parts
        .iter()
        .map(|part| normalize_identifier(input, part))
        .collect()
}

fn normalize_identifier(input: &str, identifier: &Identifier) -> String {
    let text = &input[identifier.span.start..identifier.span.end];
    if identifier.quoted {
        text[1..text.len() - 1].replace("\"\"", "\"")
    } else {
        text.to_ascii_lowercase()
    }
}

fn derive_column_name(input: &str, expression: &ExpressionSyntax) -> Option<String> {
    let ExpressionKindSyntax::Column(name) = &expression.kind else {
        return None;
    };
    name.parts
        .last()
        .map(|identifier| normalize_identifier(input, identifier))
}

fn bind_literal(
    input: &str,
    literal: &LiteralSyntax,
) -> Result<(BoundPgqLiteral, PgqDataType), PgqBindError> {
    match literal {
        LiteralSyntax::Null => Ok((BoundPgqLiteral::Null, PgqDataType::Null)),
        LiteralSyntax::Boolean(value) => {
            Ok((BoundPgqLiteral::Boolean(*value), PgqDataType::Boolean))
        }
        LiteralSyntax::Number(span) => {
            let text = &input[span.start..span.end];
            if let Ok(value) = text.parse::<i64>() {
                Ok((BoundPgqLiteral::Int64(value), PgqDataType::Int64))
            } else if let Ok(value) = text.parse::<f64>() {
                Ok((BoundPgqLiteral::Float64(value), PgqDataType::Float64))
            } else {
                Err(PgqBindError::new(
                    PgqBindErrorCode::InvalidLiteral,
                    *span,
                    "numeric literal is outside the supported range",
                ))
            }
        }
        LiteralSyntax::String(span) => decode_string(input, *span)
            .map(|value| (BoundPgqLiteral::String(value), PgqDataType::String))
            .ok_or_else(|| {
                PgqBindError::new(
                    PgqBindErrorCode::InvalidLiteral,
                    *span,
                    "invalid PostgreSQL string literal",
                )
            }),
    }
}

fn decode_string(input: &str, span: Span) -> Option<String> {
    let text = input.get(span.start..span.end)?;
    if text.starts_with('\'') && text.ends_with('\'') && text.len() >= 2 {
        return Some(text[1..text.len() - 1].replace("''", "'"));
    }
    if let Some(without_prefix) = text.strip_prefix('$') {
        let delimiter_end = without_prefix.find('$')? + 1;
        let delimiter = &text[..=delimiter_end];
        if text.ends_with(delimiter) && text.len() >= delimiter.len() * 2 {
            return Some(text[delimiter.len()..text.len() - delimiter.len()].to_string());
        }
    }
    None
}

fn binary_result_type(
    operator: BinaryOperatorSyntax,
    left: PgqDataType,
    right: PgqDataType,
) -> PgqDataType {
    match operator {
        BinaryOperatorSyntax::Or
        | BinaryOperatorSyntax::And
        | BinaryOperatorSyntax::Equal
        | BinaryOperatorSyntax::NotEqual
        | BinaryOperatorSyntax::Less
        | BinaryOperatorSyntax::LessOrEqual
        | BinaryOperatorSyntax::Greater
        | BinaryOperatorSyntax::GreaterOrEqual => PgqDataType::Boolean,
        BinaryOperatorSyntax::Concat => PgqDataType::String,
        BinaryOperatorSyntax::Add
        | BinaryOperatorSyntax::Subtract
        | BinaryOperatorSyntax::Multiply
        | BinaryOperatorSyntax::Divide
        | BinaryOperatorSyntax::Modulo => numeric_common_type(left, right),
    }
}

fn validate_binary_operands(
    operator: BinaryOperatorSyntax,
    left: PgqDataType,
    right: PgqDataType,
    span: Span,
) -> Result<(), PgqBindError> {
    match operator {
        BinaryOperatorSyntax::Or | BinaryOperatorSyntax::And => {
            require_boolean(left, span)?;
            require_boolean(right, span)
        }
        BinaryOperatorSyntax::Concat => {
            require_string(left, span)?;
            require_string(right, span)
        }
        BinaryOperatorSyntax::Add
        | BinaryOperatorSyntax::Subtract
        | BinaryOperatorSyntax::Multiply
        | BinaryOperatorSyntax::Divide
        | BinaryOperatorSyntax::Modulo => {
            require_numeric(left, span)?;
            require_numeric(right, span)
        }
        BinaryOperatorSyntax::Equal
        | BinaryOperatorSyntax::NotEqual
        | BinaryOperatorSyntax::Less
        | BinaryOperatorSyntax::LessOrEqual
        | BinaryOperatorSyntax::Greater
        | BinaryOperatorSyntax::GreaterOrEqual => require_comparable(left, right, span),
    }
}

fn require_comparable(
    left: PgqDataType,
    right: PgqDataType,
    span: Span,
) -> Result<(), PgqBindError> {
    if matches!(left, PgqDataType::Unknown | PgqDataType::Null)
        || matches!(right, PgqDataType::Unknown | PgqDataType::Null)
        || left == right
        || matches!(
            (left, right),
            (PgqDataType::Int64, PgqDataType::Float64) | (PgqDataType::Float64, PgqDataType::Int64)
        )
    {
        return Ok(());
    }
    Err(PgqBindError::new(
        PgqBindErrorCode::TypeMismatch,
        span,
        format!("cannot compare {left:?} with {right:?}"),
    ))
}

fn require_boolean(data_type: PgqDataType, span: Span) -> Result<(), PgqBindError> {
    require_type(
        data_type,
        &[PgqDataType::Boolean],
        span,
        "boolean expression",
    )
}

fn require_numeric(data_type: PgqDataType, span: Span) -> Result<(), PgqBindError> {
    require_type(
        data_type,
        &[PgqDataType::Int64, PgqDataType::Float64],
        span,
        "numeric expression",
    )
}

fn require_string(data_type: PgqDataType, span: Span) -> Result<(), PgqBindError> {
    require_type(data_type, &[PgqDataType::String], span, "string expression")
}

fn require_type(
    data_type: PgqDataType,
    accepted: &[PgqDataType],
    span: Span,
    expected: &'static str,
) -> Result<(), PgqBindError> {
    if matches!(data_type, PgqDataType::Unknown | PgqDataType::Null)
        || accepted.contains(&data_type)
    {
        return Ok(());
    }
    Err(PgqBindError::new(
        PgqBindErrorCode::TypeMismatch,
        span,
        format!("expected {expected}, found {data_type:?}"),
    ))
}

fn numeric_common_type(left: PgqDataType, right: PgqDataType) -> PgqDataType {
    match (left, right) {
        (PgqDataType::Float64, PgqDataType::Int64 | PgqDataType::Float64)
        | (PgqDataType::Int64, PgqDataType::Float64) => PgqDataType::Float64,
        (PgqDataType::Int64, PgqDataType::Int64) => PgqDataType::Int64,
        _ => PgqDataType::Unknown,
    }
}

fn function_result_type(function: &str, arguments: &[BoundPgqExpression]) -> PgqDataType {
    if ["lower", "upper", "pg_typeof"]
        .into_iter()
        .any(|candidate| function.eq_ignore_ascii_case(candidate))
    {
        PgqDataType::String
    } else {
        arguments
            .first()
            .map_or(PgqDataType::Unknown, |argument| argument.data_type)
    }
}

fn cast_result_type(name: &[String]) -> PgqDataType {
    match name.last().map(String::as_str) {
        Some("bool" | "boolean") => PgqDataType::Boolean,
        Some("bigint" | "int8" | "integer" | "int4") => PgqDataType::Int64,
        Some("float8" | "double" | "real" | "float4") => PgqDataType::Float64,
        Some("text" | "varchar" | "character") => PgqDataType::String,
        _ => PgqDataType::Unknown,
    }
}

fn is_disallowed_graph_table_function(function: &str) -> bool {
    [
        "avg",
        "count",
        "generate_series",
        "max",
        "min",
        "row_number",
        "sum",
    ]
    .into_iter()
    .any(|candidate| function.eq_ignore_ascii_case(candidate))
}

fn element_kind_name(kind: PgqElementKind) -> &'static str {
    match kind {
        PgqElementKind::Vertex => "vertex",
        PgqElementKind::Edge => "edge",
    }
}
