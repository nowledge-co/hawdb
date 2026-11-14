// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Pure PEG data-mutation and system-variable statements.
use hawdb_core::{Result, Value};
use hawdb_sql_syntax::peg::NodeId;

use super::Transformer;
use crate::*;

impl<'a> Transformer<'a> {
    pub(super) fn iter_named_children(
        &self,
        node: NodeId,
        name: super::tree::RuleId,
    ) -> impl Iterator<Item = NodeId> + use<'a> {
        let tree = self.tree;
        tree.children(node)
            .iter()
            .copied()
            .filter(move |id| tree.rule_id(*id) == name)
    }
    pub(super) fn named_children(&self, node: NodeId, name: super::tree::RuleId) -> Vec<NodeId> {
        self.iter_named_children(node, name).collect()
    }
    pub(super) fn system_assignment(
        &mut self,
        node: NodeId,
        depth: usize,
    ) -> Result<SetSystemVariable> {
        let names = self
            .tree
            .child(node, rule_id!("d_system_variable"))
            .unwrap_or(node);
        let name = self
            .text(self.tree.required(names, rule_id!("v_identifier")))
            .to_ascii_lowercase();
        Ok(SetSystemVariable {
            name,
            value: self.value(self.tree.required(node, rule_id!("v_value")), depth)?,
        })
    }
    pub(super) fn data_node(&mut self, node: NodeId, depth: usize) -> Result<CreateNode> {
        let label = self
            .text(self.tree.required(node, rule_id!("v_identifier")))
            .to_owned();
        let properties = self
            .tree
            .child(node, rule_id!("v_properties"))
            .map(|id| self.properties(id, depth))
            .transpose()?
            .unwrap_or_default();
        Ok(CreateNode { label, properties })
    }
    pub(super) fn data_statement(
        &mut self,
        node: NodeId,
        merge: bool,
        depth: usize,
    ) -> Result<Statement> {
        let mut nodes = self.iter_named_children(node, rule_id!("d_node"));
        let source_node = nodes.next().expect("data source node");
        let source = self.data_node(source_node, depth)?;
        if let Some(edge) = self.tree.child(node, rule_id!("d_edge")) {
            let rel_type = self
                .text(self.tree.required(edge, rule_id!("v_identifier")))
                .to_owned();
            let properties = self
                .tree
                .child(edge, rule_id!("v_properties"))
                .map(|id| self.properties(id, depth))
                .transpose()?
                .unwrap_or_default();
            let target = self.data_node(nodes.next().expect("data target node"), depth)?;
            let relationship = CreateRelationship {
                source,
                rel_type,
                properties,
                target,
            };
            return Ok(if merge {
                Statement::MergeRelationship(relationship)
            } else {
                Statement::CreateRelationship(relationship)
            });
        }
        if !merge {
            return Ok(Statement::CreateNode(source));
        }
        let variable = self
            .tree
            .child(source_node, rule_id!("d_variable"))
            .map(|id| self.text(id).to_owned());
        let mut on_create_sets = Vec::new();
        let mut on_match_sets = Vec::new();
        for sets in self.iter_named_children(node, rule_id!("d_on_sets")) {
            let create = self
                .text(self.tree.required(sets, rule_id!("d_on_kind")))
                .eq_ignore_ascii_case("CREATE");
            let assignments = self.sets(self.tree.required(sets, rule_id!("d_sets")), depth)?;
            if create {
                on_create_sets = assignments;
            } else {
                on_match_sets = assignments;
            }
        }
        let post_merge_sets = self
            .tree
            .child(node, rule_id!("d_post_sets"))
            .map(|id| self.sets(self.tree.required(id, rule_id!("d_sets")), depth))
            .transpose()?
            .unwrap_or_default();
        Ok(Statement::MergeNode(MergeNode {
            variable,
            label: source.label,
            properties: source.properties,
            on_create_sets,
            on_match_sets,
            post_merge_sets,
        }))
    }
    fn reference(&self, node: NodeId) -> (String, String) {
        self.identifier_pair(node)
    }
    pub(super) fn sets(&mut self, node: NodeId, depth: usize) -> Result<Vec<SetProperty>> {
        let children = self.tree.children(node).iter().copied();
        children
            .map(|assignment| {
                let (variable, property) =
                    self.reference(self.tree.required(assignment, rule_id!("d_ref")));
                let value = self.tree.required(assignment, rule_id!("d_set_value"));
                let value = self.set_value(self.tree.children(value)[0], depth)?;
                Ok(SetProperty {
                    variable,
                    property,
                    value,
                })
            })
            .collect()
    }
    fn set_value(&mut self, node: NodeId, depth: usize) -> Result<SetValueExpression> {
        match self.tree.node(node).name.expect("set value syntax") {
            "v_value" => Ok(SetValueExpression::Value(self.value(node, depth)?)),
            "v_property_value" => Ok(SetValueExpression::Value(self.property_value(node, depth)?)),
            "d_property_value" | "d_coalesce" => {
                let (variable, property) =
                    self.reference(self.tree.required(node, rule_id!("d_ref")));
                let default = self
                    .tree
                    .child(node, rule_id!("v_value"))
                    .map(|id| self.value(id, depth))
                    .transpose()?;
                let addition = self
                    .tree
                    .child(node, rule_id!("d_add"))
                    .map(|id| self.value(self.tree.required(id, rule_id!("v_value")), depth))
                    .transpose()?;
                Ok(match (default, addition) {
                    (None, None) => SetValueExpression::Property { variable, property },
                    (None, Some(value)) => SetValueExpression::PropertyAdd {
                        variable,
                        property,
                        value,
                    },
                    (Some(default), None) => SetValueExpression::CoalesceProperty {
                        variable,
                        property,
                        default,
                    },
                    (Some(default), Some(value)) => SetValueExpression::CoalescePropertyAdd {
                        variable,
                        property,
                        default,
                        value,
                    },
                })
            }
            "d_case_decrement" | "d_case_preserve" => {
                let refs = self
                    .iter_named_children(node, rule_id!("d_ref"))
                    .map(|id| self.reference(id))
                    .collect::<Vec<_>>();
                let mut values = Vec::new();
                for value in self.iter_named_children(node, rule_id!("v_value")) {
                    values.push(self.value(value, depth)?);
                }
                if refs.iter().any(|reference| *reference != refs[0]) {
                    return Err(self.error(node, "CASE SET must use the same property"));
                }
                let (variable, property) = refs[0].clone();
                if self.tree.rule_id(node) == rule_id!("d_case_decrement") {
                    let ints = [0, 1, 0];
                    if values.iter().zip(ints).any(|(value, expected)| {
                        value.kind != ValueExpressionKind::Literal(Value::Int(expected))
                    }) {
                        return Err(self.error(
                            node,
                            "CASE decrement SET requires zero threshold/floor and decrement by one",
                        ));
                    }
                    Ok(SetValueExpression::DecrementFloorZero { variable, property })
                } else if values.len() == 2 {
                    if values[0] != values[1] {
                        return Err(self.error(
                            node,
                            "CASE null-preserving SET ELSE must return the incoming value",
                        ));
                    }
                    Ok(SetValueExpression::PreserveNewerExisting {
                        variable,
                        property,
                        incoming: values.remove(0),
                        preserve: AstNode::synthetic(ValueExpressionKind::Literal(Value::Bool(
                            false,
                        ))),
                    })
                } else {
                    if values[2].kind != ValueExpressionKind::Literal(Value::Bool(true))
                        || values[0] != values[3]
                        || values[0] != values[4]
                    {
                        return Err(self.error(
                            node,
                            "CASE preserve SET requires true and the same incoming value",
                        ));
                    }
                    let compared = self
                        .iter_named_children(node, rule_id!("d_ref"))
                        .nth(2)
                        .expect("comparison property");
                    let preserve_older = self.tokens[self.tree.node(compared).end].kind
                        == super::lexer::TokenKind::Less;
                    let incoming = values.remove(0);
                    let preserve = values.remove(0);
                    Ok(if preserve_older {
                        SetValueExpression::PreserveOlderExisting {
                            variable,
                            property,
                            incoming,
                            preserve,
                        }
                    } else {
                        SetValueExpression::PreserveNewerExisting {
                            variable,
                            property,
                            incoming,
                            preserve,
                        }
                    })
                }
            }

            _ => unreachable!("set value rule"),
        }
    }
}
