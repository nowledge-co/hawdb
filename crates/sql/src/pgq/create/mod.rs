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

//! Read-only creation binding. Catalog publication and source snapshot ownership
//! belong to the caller, independently of query binding and execution.

mod expression;
mod source;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use hawdb_sql_syntax::{
    CreatePropertyGraph, ElementExposure, ElementLabel, Identifier, PropertyExposure,
    QualifiedName, Span,
};

use super::{PgqDataType, PropertyGraphElementSchema, PropertyGraphSchema};
use crate::SqlDataType;
use expression::BoundExpression;

/// A coherent, read-only source metadata snapshot for one creation binding.
/// The caller resolves search paths and owns its lifetime; no rows are read.
pub trait PgqSourceCatalog {
    fn source_table(&self, name: &[String]) -> Option<&PgqSourceTableSchema>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgqSourceTableSchema {
    /// Canonical decoded identity, also used by foreign-key targets.
    pub name: Vec<String>,
    /// Source order is retained for ALL COLUMNS expansion.
    pub columns: Vec<PgqSourceColumnSchema>,
    pub primary_key: Vec<String>,
    pub foreign_keys: Vec<PgqSourceForeignKeySchema>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgqSourceColumnSchema {
    pub name: String,
    pub data_type: SqlDataType,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgqSourceForeignKeySchema {
    pub columns: Vec<String>,
    pub referenced_table: Vec<String>,
    pub referenced_columns: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PgqCreateBindErrorCode {
    InvalidSyntaxTree,
    InvalidSourceSchema,
    UnknownTable,
    UnknownColumn,
    DuplicateAlias,
    MissingKey,
    InvalidKey,
    UnknownVertex,
    MissingForeignKey,
    AmbiguousForeignKey,
    DuplicateLabel,
    MissingPropertyName,
    DuplicateProperty,
    LabelMismatch,
    PropertyTypeMismatch,
    PropertyExpressionMismatch,
    UnsupportedExpression,
    InvalidLiteral,
    TypeMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgqCreateBindError {
    pub code: PgqCreateBindErrorCode,
    pub span: Span,
    pub message: String,
}

impl fmt::Display for PgqCreateBindError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "SQL/PGQ creation binding error {:?} at bytes {}..{}: {}",
            self.code, self.span.start, self.span.end, self.message
        )
    }
}

impl std::error::Error for PgqCreateBindError {}

type Result<T> = std::result::Result<T, PgqCreateBindError>;
use PgqCreateBindErrorCode as Code;

/// Validate a parsed creation against a source snapshot and return its schema.
///
/// `input` must be the text from which `create` was parsed. This operation neither
/// installs the schema nor changes query routing, storage, or catalog state.
pub fn bind_postgres_create_property_graph(
    input: &str,
    create: &CreatePropertyGraph,
    catalog: &dyn PgqSourceCatalog,
) -> Result<PropertyGraphSchema> {
    let binder = Binder {
        input,
        enclosing: create.span,
    };
    binder.text(create.span)?;
    let mut graph = PropertyGraphSchema::new(binder.name(&create.name)?);
    let mut aliases = BTreeSet::new();
    let mut vertices = BTreeMap::new();
    let mut elements = Vec::new();
    for vertex in &create.vertex_tables {
        binder.text(vertex.span)?;
        let source = binder.source(catalog, &vertex.table)?;
        let alias = binder.alias(source, vertex.alias.as_ref(), vertex.span)?;
        binder.unique_alias(&mut aliases, &alias, vertex.span)?;
        binder.key(source, &vertex.key, vertex.span)?;
        vertices.insert(alias.clone(), source);
        elements.push((true, source, alias, vertex.exposure.as_ref(), vertex.span));
    }
    for edge in &create.edge_tables {
        binder.text(edge.span)?;
        let source = binder.source(catalog, &edge.table)?;
        let alias = binder.alias(source, edge.alias.as_ref(), edge.span)?;
        binder.unique_alias(&mut aliases, &alias, edge.span)?;
        binder.key(source, &edge.key, edge.span)?;
        binder.endpoint(source, &edge.source, &vertices)?;
        binder.endpoint(source, &edge.destination, &vertices)?;
        elements.push((false, source, alias, edge.exposure.as_ref(), edge.span));
    }
    let mut labels = BTreeMap::new();
    let mut property_types = BTreeMap::new();
    for (vertex, source, alias, exposure, span) in elements {
        let bound = binder.exposure(source, &alias, exposure, span)?;
        for (label, properties) in bound {
            let mut schema = PropertyGraphElementSchema::default();
            for (name, expression) in properties {
                if property_types
                    .get(&name)
                    .is_some_and(|previous| *previous != expression.data_type)
                {
                    return Err(binder.error(
                        Code::PropertyTypeMismatch,
                        span,
                        format!("property {name} has inconsistent scalar types"),
                    ));
                }
                property_types.insert(name.clone(), expression.data_type);
                schema.properties.insert(name, expression.data_type);
            }
            if labels
                .get(&label)
                .is_some_and(|previous| previous != &schema)
            {
                return Err(binder.error(
                    Code::LabelMismatch,
                    span,
                    format!("label {label} has inconsistent property sets"),
                ));
            }
            labels.insert(label.clone(), schema.clone());
            if vertex {
                graph.add_vertex_label(label, schema);
            } else {
                graph.add_edge_label(label, schema);
            }
        }
    }
    Ok(graph)
}

struct Binder<'a> {
    input: &'a str,
    enclosing: Span,
}

impl Binder<'_> {
    fn error(&self, code: Code, span: Span, message: impl Into<String>) -> PgqCreateBindError {
        let span = if self.input.get(span.start..span.end).is_some() {
            span
        } else if self
            .input
            .get(self.enclosing.start..self.enclosing.end)
            .is_some()
        {
            self.enclosing
        } else {
            Span::default()
        };
        PgqCreateBindError {
            code,
            span,
            message: message.into(),
        }
    }

    fn text(&self, span: Span) -> Result<&str> {
        self.input
            .get(span.start..span.end)
            .ok_or_else(|| self.error(Code::InvalidSyntaxTree, span, "invalid source span"))
    }

    fn identifier(&self, identifier: &Identifier) -> Result<String> {
        let text = self.text(identifier.span)?;
        let decoded = if identifier.quoted {
            text.strip_prefix('"')
                .and_then(|text| text.strip_suffix('"'))
                .filter(|text| !text.replace("\"\"", "").contains('"'))
                .map(|text| text.replace("\"\"", "\""))
        } else {
            let mut chars = text.chars();
            chars
                .next()
                .filter(|c| *c == '_' || c.is_alphabetic())
                .filter(|_| chars.all(|c| c == '_' || c == '$' || c.is_alphanumeric()))
                .map(|_| text.to_ascii_lowercase())
        };
        decoded
            .filter(|name| !name.is_empty() && !name.contains('\0'))
            .ok_or_else(|| {
                self.error(
                    Code::InvalidSyntaxTree,
                    identifier.span,
                    "invalid identifier",
                )
            })
    }

    fn name(&self, name: &QualifiedName) -> Result<Vec<String>> {
        self.text(name.span)?;
        if name.parts.is_empty() {
            return Err(self.error(Code::InvalidSyntaxTree, name.span, "empty qualified name"));
        }
        name.parts
            .iter()
            .map(|part| self.identifier(part))
            .collect()
    }

    fn unique_alias(&self, aliases: &mut BTreeSet<String>, alias: &str, span: Span) -> Result<()> {
        if !aliases.insert(alias.to_owned()) {
            return Err(self.error(
                Code::DuplicateAlias,
                span,
                format!("duplicate element alias {alias}"),
            ));
        }
        Ok(())
    }

    fn exposure(
        &self,
        source: &PgqSourceTableSchema,
        alias: &str,
        exposure: Option<&ElementExposure>,
        span: Span,
    ) -> Result<BTreeMap<String, BTreeMap<String, BoundExpression>>> {
        let Some(exposure) = exposure else {
            return Ok(BTreeMap::from([(
                alias.to_owned(),
                self.properties(source, &PropertyExposure::AllColumns, span)?,
            )]));
        };
        self.text(exposure.span)?;
        if exposure.labels.is_empty() {
            return Err(self.error(
                Code::InvalidSyntaxTree,
                exposure.span,
                "empty label exposure",
            ));
        }
        let mut labels = BTreeMap::new();
        let mut expressions = BTreeMap::new();
        for label in &exposure.labels {
            self.text(label.span)?;
            let name = match &label.label {
                ElementLabel::Implicit | ElementLabel::Default => alias.to_owned(),
                ElementLabel::Named(name) => self.identifier(name)?,
            };
            if labels.contains_key(&name) {
                return Err(self.error(
                    Code::DuplicateLabel,
                    label.span,
                    format!("duplicate label {name}"),
                ));
            }
            let properties = self.properties(source, &label.properties, label.span)?;
            for (property, expression) in &properties {
                if let Some(previous) = expressions.get(property)
                    && previous != expression
                {
                    return Err(self.error(
                        Code::PropertyExpressionMismatch,
                        label.span,
                        format!("property {property} uses different expressions on one element"),
                    ));
                }
                expressions.insert(property.clone(), expression.clone());
            }
            labels.insert(name, properties);
        }
        Ok(labels)
    }

    fn properties(
        &self,
        source: &PgqSourceTableSchema,
        exposure: &PropertyExposure,
        span: Span,
    ) -> Result<BTreeMap<String, BoundExpression>> {
        let mut properties = BTreeMap::new();
        match exposure {
            PropertyExposure::AllColumns => {
                for (index, column) in source.columns.iter().enumerate() {
                    properties.insert(
                        column.name.clone(),
                        BoundExpression::column(index, scalar_type(column.data_type)),
                    );
                }
            }
            PropertyExposure::NoProperties => {}
            PropertyExposure::Expressions(expressions) => {
                if expressions.is_empty() {
                    return Err(self.error(
                        Code::InvalidSyntaxTree,
                        span,
                        "empty property expression list",
                    ));
                }
                for property in expressions {
                    self.text(property.span)?;
                    let mut expression = self.expression(source, &property.expression, 0)?;
                    let name = match &property.alias {
                        Some(alias) => self.identifier(alias)?,
                        None => expression
                            .implicit_column_name(source, &property.expression)
                            .ok_or_else(|| {
                                self.error(
                                    Code::MissingPropertyName,
                                    property.span,
                                    "non-column property requires AS",
                                )
                            })?,
                    };
                    self.resolve_unknown(&mut expression, PgqDataType::String, property.span)?;
                    if properties.insert(name.clone(), expression).is_some() {
                        return Err(self.error(
                            Code::DuplicateProperty,
                            property.span,
                            format!("duplicate property {name}"),
                        ));
                    }
                }
            }
        }
        Ok(properties)
    }
}

fn scalar_type(data_type: SqlDataType) -> PgqDataType {
    match data_type {
        SqlDataType::Boolean => PgqDataType::Boolean,
        SqlDataType::BigInt => PgqDataType::Int64,
        SqlDataType::DoublePrecision => PgqDataType::Float64,
        SqlDataType::Text => PgqDataType::String,
        SqlDataType::Bytea => PgqDataType::Binary,
        SqlDataType::Uuid => PgqDataType::Uuid,
    }
}

#[cfg(test)]
mod tests;
