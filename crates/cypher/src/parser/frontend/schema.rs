// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Cypher schema syntax is matched completely before constructing schema ASTs.
use hawdb_core::{Result, VectorDimension};
use hawdb_sql_syntax::peg::NodeId;

use super::Transformer;
use crate::*;

impl Transformer<'_> {
    fn schema_text(&self, node: NodeId) -> &str {
        self.tokens[self.tree.node(node).start].text(self.input)
    }
    fn schema_ids(&self, node: NodeId) -> Vec<String> {
        self.tree
            .children(node)
            .iter()
            .filter(|id| self.tree.rule_id(**id) == rule_id!("schema_identifier"))
            .map(|id| self.schema_text(*id).to_owned())
            .collect()
    }
    fn schema_kind(&self, node: NodeId) -> SchemaTableKind {
        if self
            .schema_text(self.tree.required(node, rule_id!("schema_kind")))
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
        Ok(match self.tree.node(node).name.expect("schema rule") {
            "schema_table" => {
                let kind = self.schema_kind(node);
                let object = self.schema_text(child(rule_id!("schema_object_kind")));
                let name = self
                    .schema_text(child(rule_id!("schema_identifier")))
                    .to_owned();
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
                let label = self
                    .schema_text(child(rule_id!("schema_identifier")))
                    .to_owned();
                let mut properties = self.schema_ids(child(rule_id!("schema_index_keys")));
                let range = self.tree.child(node, rule_id!("schema_range")).is_some();
                let fulltext = self.tree.child(node, rule_id!("schema_fulltext")).is_some();
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
                let rel = self
                    .tree
                    .child(node, rule_id!("schema_relationship_constraint"));
                let shape = rel.unwrap_or_else(|| child(rule_id!("schema_node_constraint")));
                let ids =
                    self.schema_ids(self.tree.required(shape, rule_id!("schema_constraint_key")));
                let index = CreateIndex {
                    label: ids[0].clone(),
                    property: ids[1].clone(),
                };
                let unique = self
                    .schema_text(child(rule_id!("schema_assertion")))
                    .eq_ignore_ascii_case("UNIQUE");
                match (rel.is_some(), unique) {
                    (false, true) => Statement::CreateUniqueConstraint(index),
                    (false, false) => Statement::CreateNodePropertyExistsConstraint(index),
                    (true, true) => Statement::CreateRelationshipUniqueConstraint(index),
                    (true, false) => Statement::CreateRelationshipPropertyExistsConstraint(index),
                }
            }
            "schema_property" => {
                let path = child(rule_id!("schema_property_path"));
                let ids = self.schema_ids(path);
                let type_node = child(rule_id!("schema_type"));
                let value_type = match self.schema_text(type_node).to_ascii_uppercase().as_str() {
                    "ANY" => SchemaPropertyType::Any,
                    "BOOL" | "BOOLEAN" => SchemaPropertyType::Bool,
                    "INT" | "INTEGER" => SchemaPropertyType::Int,
                    "FLOAT" | "DOUBLE" => SchemaPropertyType::Float,
                    "STRING" | "VARCHAR" | "CHARACTER" => SchemaPropertyType::String,
                    "TEXT" => SchemaPropertyType::Text,
                    "LIST" => SchemaPropertyType::List,
                    "VECTOR" => {
                        let dim = self.tree.required(type_node, rule_id!("schema_dimension"));
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
                    nullable: self.tree.child(node, rule_id!("schema_not_null")).is_none(),
                })
            }
            "schema_alter_property" | "schema_alter_table" => {
                let state = self.tree.required(
                    child(rule_id!("schema_state_clause")),
                    rule_id!("schema_state"),
                );
                let state = match self.schema_text(state).to_ascii_uppercase().as_str() {
                    "DELETE_ONLY" => SchemaObjectState::DeleteOnly,
                    "WRITE_ONLY" => SchemaObjectState::WriteOnly,
                    "BACKFILL" => SchemaObjectState::Backfill,
                    "VALIDATING" => SchemaObjectState::Validating,
                    "PUBLIC" => SchemaObjectState::Public,
                    "GC" => SchemaObjectState::Gc,
                    _ => unreachable!("matched schema state"),
                };
                if let Some(path) = self.tree.child(node, rule_id!("schema_property_path")) {
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
                        table: self
                            .schema_text(child(rule_id!("schema_identifier")))
                            .to_owned(),
                        state,
                    })
                }
            }
            _ => unreachable!("matched schema statement"),
        })
    }
}
