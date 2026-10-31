// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Cypher schema syntax is matched completely before constructing schema ASTs.
use hawdb_core::{Result, VectorDimension};
use hawdb_sql_syntax::peg::{Expression as E, NodeId};
use hawdb_sql_syntax::TokenKind as T;

use super::Transformer;
use crate::*;

fn r(name: &'static str) -> E {
    E::Rule(name)
}
fn k(name: &'static str) -> E {
    E::Keyword(name)
}
fn t(kind: T) -> E {
    E::Token(kind, "a Cypher schema token")
}
fn s(items: Vec<E>) -> E {
    E::Sequence(items)
}
fn c(items: Vec<E>) -> E {
    E::Choice(items)
}
fn o(item: E) -> E {
    E::Optional(Box::new(item))
}
fn kw(names: &[&'static str]) -> E {
    c(names.iter().copied().map(k).collect())
}
fn group(item: E) -> E {
    s(vec![t(T::LeftParen), item, t(T::RightParen)])
}

pub(super) fn rules() -> Vec<(&'static str, E)> {
    vec![
        (
            "schema_create",
            c(vec![
                r("schema_index"),
                r("schema_constraint"),
                r("schema_property"),
                r("schema_table"),
            ]),
        ),
        ("schema_identifier", t(T::Word)),
        ("schema_kind", kw(&["NODE", "RELATIONSHIP"])),
        (
            "schema_table",
            s(vec![
                r("schema_kind"),
                r("schema_object_kind"),
                r("schema_identifier"),
            ]),
        ),
        ("schema_object_kind", kw(&["TABLE", "LABEL", "TYPE"])),
        ("schema_range", k("RANGE")),
        ("schema_fulltext", k("FULLTEXT")),
        (
            "schema_index",
            s(vec![
                o(c(vec![r("schema_range"), r("schema_fulltext")])),
                k("INDEX"),
                k("ON"),
                t(T::Colon),
                r("schema_identifier"),
                group(r("schema_index_keys")),
            ]),
        ),
        (
            "schema_index_keys",
            s(vec![
                r("schema_identifier"),
                E::Repeat(Box::new(s(vec![t(T::Comma), r("schema_identifier")]))),
            ]),
        ),
        (
            "schema_property_path",
            s(vec![
                k("ON"),
                r("schema_kind"),
                k("TABLE"),
                r("schema_identifier"),
                group(r("schema_identifier")),
            ]),
        ),
        (
            "schema_property",
            s(vec![
                k("PROPERTY"),
                r("schema_property_path"),
                k("TYPE"),
                r("schema_type"),
                o(r("schema_not_null")),
            ]),
        ),
        ("schema_not_null", s(vec![k("NOT"), k("NULL")])),
        (
            "schema_type",
            c(vec![
                s(vec![k("VECTOR"), group(r("schema_dimension"))]),
                s(vec![k("CHARACTER"), k("VARYING")]),
                kw(&[
                    "ANY", "BOOL", "BOOLEAN", "INT", "INTEGER", "FLOAT", "DOUBLE", "STRING",
                    "TEXT", "LIST", "VARCHAR",
                ]),
            ]),
        ),
        ("schema_dimension", t(T::Number)),
        (
            "schema_constraint",
            s(vec![
                k("CONSTRAINT"),
                k("ON"),
                c(vec![
                    r("schema_relationship_constraint"),
                    r("schema_node_constraint"),
                ]),
                k("ASSERT"),
                r("schema_assertion"),
            ]),
        ),
        (
            "schema_constraint_key",
            s(vec![
                t(T::Colon),
                r("schema_identifier"),
                group(r("schema_identifier")),
            ]),
        ),
        ("schema_node_constraint", r("schema_constraint_key")),
        (
            "schema_relationship_constraint",
            s(vec![
                t(T::Minus),
                t(T::LeftBracket),
                r("schema_constraint_key"),
                t(T::RightBracket),
                t(T::Minus),
                t(T::Greater),
            ]),
        ),
        (
            "schema_assertion",
            c(vec![kw(&["UNIQUE", "EXISTS"]), r("schema_not_null")]),
        ),
        (
            "schema_alter",
            c(vec![r("schema_alter_property"), r("schema_alter_table")]),
        ),
        (
            "schema_alter_property",
            s(vec![
                k("PROPERTY"),
                r("schema_property_path"),
                r("schema_state_clause"),
            ]),
        ),
        (
            "schema_alter_table",
            s(vec![
                r("schema_kind"),
                k("TABLE"),
                r("schema_identifier"),
                r("schema_state_clause"),
            ]),
        ),
        (
            "schema_state_clause",
            s(vec![k("SET"), k("STATE"), r("schema_state")]),
        ),
        (
            "schema_state",
            kw(&[
                "DELETE_ONLY",
                "WRITE_ONLY",
                "BACKFILL",
                "VALIDATING",
                "PUBLIC",
                "GC",
            ]),
        ),
    ]
}

impl Transformer<'_> {
    fn schema_text(&self, node: NodeId) -> &str {
        self.tokens[self.tree.nodes[node].start].text(self.input)
    }
    fn schema_ids(&self, node: NodeId) -> Vec<String> {
        self.tree
            .children(node)
            .iter()
            .filter(|id| self.tree.nodes[**id].name == Some("schema_identifier"))
            .map(|id| self.schema_text(*id).to_owned())
            .collect()
    }
    fn schema_kind(&self, node: NodeId) -> SchemaTableKind {
        if self
            .schema_text(self.tree.required(node, "schema_kind"))
            .eq_ignore_ascii_case("NODE")
        {
            SchemaTableKind::Node
        } else {
            SchemaTableKind::Relationship
        }
    }
    pub(super) fn schema_statement(&self, root: NodeId) -> Result<Statement> {
        let node = self.tree.children(root)[0];
        let child = |name| self.tree.required(node, name);
        Ok(match self.tree.nodes[node].name.expect("schema rule") {
            "schema_table" => {
                let kind = self.schema_kind(node);
                let object = self.schema_text(child("schema_object_kind"));
                let name = self.schema_text(child("schema_identifier")).to_owned();
                match (kind, object.to_ascii_uppercase().as_str()) {
                    (SchemaTableKind::Node, "TABLE") => Statement::CreateNodeTable(name),
                    (SchemaTableKind::Node, "LABEL") => Statement::CreateNodeLabel(name),
                    (SchemaTableKind::Relationship, "TABLE") => {
                        Statement::CreateRelationshipTable(name)
                    }
                    (SchemaTableKind::Relationship, "TYPE") => {
                        Statement::CreateRelationshipType(name)
                    }
                    _ => {
                        return Err(self.error(
                            node,
                            "expected a schema table, node label, or relationship type",
                        ))
                    }
                }
            }
            "schema_index" => {
                let label = self.schema_text(child("schema_identifier")).to_owned();
                let mut properties = self.schema_ids(child("schema_index_keys"));
                let range = self.tree.child(node, "schema_range").is_some();
                let fulltext = self.tree.child(node, "schema_fulltext").is_some();
                if properties.len() > 1 {
                    if range || fulltext {
                        return Err(self.error(node, "index expects one property"));
                    }
                    return Ok(Statement::CreateCompositeIndex(CreateCompositeIndex {
                        label,
                        properties,
                    }));
                }
                let index = CreateIndex {
                    label,
                    property: properties.remove(0),
                };
                if range {
                    Statement::CreateRangeIndex(index)
                } else if fulltext {
                    Statement::CreateFullTextIndex(index)
                } else {
                    Statement::CreateIndex(index)
                }
            }
            "schema_constraint" => {
                let rel = self.tree.child(node, "schema_relationship_constraint");
                let shape = rel.unwrap_or_else(|| child("schema_node_constraint"));
                let ids = self.schema_ids(self.tree.required(shape, "schema_constraint_key"));
                let index = CreateIndex {
                    label: ids[0].clone(),
                    property: ids[1].clone(),
                };
                let unique = self
                    .schema_text(child("schema_assertion"))
                    .eq_ignore_ascii_case("UNIQUE");
                match (rel.is_some(), unique) {
                    (false, true) => Statement::CreateUniqueConstraint(index),
                    (false, false) => Statement::CreateNodePropertyExistsConstraint(index),
                    (true, true) => Statement::CreateRelationshipUniqueConstraint(index),
                    (true, false) => Statement::CreateRelationshipPropertyExistsConstraint(index),
                }
            }
            "schema_property" => {
                let path = child("schema_property_path");
                let ids = self.schema_ids(path);
                let type_node = child("schema_type");
                let value_type = match self.schema_text(type_node).to_ascii_uppercase().as_str() {
                    "ANY" => SchemaPropertyType::Any,
                    "BOOL" | "BOOLEAN" => SchemaPropertyType::Bool,
                    "INT" | "INTEGER" => SchemaPropertyType::Int,
                    "FLOAT" | "DOUBLE" => SchemaPropertyType::Float,
                    "STRING" | "VARCHAR" | "CHARACTER" => SchemaPropertyType::String,
                    "TEXT" => SchemaPropertyType::Text,
                    "LIST" => SchemaPropertyType::List,
                    "VECTOR" => {
                        let dim = self.tree.required(type_node, "schema_dimension");
                        let dimension = self
                            .schema_text(dim)
                            .parse::<u32>()
                            .ok()
                            .and_then(VectorDimension::new)
                            .ok_or_else(|| {
                                self.error(dim, "VECTOR dimension must be a positive u32")
                            })?;
                        SchemaPropertyType::Vector(dimension)
                    }
                    _ => unreachable!("matched property type"),
                };
                Statement::CreateProperty(CreateProperty {
                    table_kind: self.schema_kind(path),
                    table: ids[0].clone(),
                    property: ids[1].clone(),
                    value_type,
                    nullable: self.tree.child(node, "schema_not_null").is_none(),
                })
            }
            "schema_alter_property" | "schema_alter_table" => {
                let state = self
                    .tree
                    .required(child("schema_state_clause"), "schema_state");
                let state = match self.schema_text(state).to_ascii_uppercase().as_str() {
                    "DELETE_ONLY" => SchemaObjectState::DeleteOnly,
                    "WRITE_ONLY" => SchemaObjectState::WriteOnly,
                    "BACKFILL" => SchemaObjectState::Backfill,
                    "VALIDATING" => SchemaObjectState::Validating,
                    "PUBLIC" => SchemaObjectState::Public,
                    "GC" => SchemaObjectState::Gc,
                    _ => unreachable!("matched schema state"),
                };
                if let Some(path) = self.tree.child(node, "schema_property_path") {
                    let ids = self.schema_ids(path);
                    Statement::AlterPropertyState(AlterPropertyState {
                        table_kind: self.schema_kind(path),
                        table: ids[0].clone(),
                        property: ids[1].clone(),
                        state,
                    })
                } else {
                    Statement::AlterTableState(AlterTableState {
                        table_kind: self.schema_kind(node),
                        table: self.schema_text(child("schema_identifier")).to_owned(),
                        state,
                    })
                }
            }
            _ => unreachable!("matched schema statement"),
        })
    }
}
