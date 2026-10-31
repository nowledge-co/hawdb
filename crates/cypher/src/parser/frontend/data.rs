// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Pure PEG data-mutation and system-variable statements.
use hawdb_core::{Result, Value};
use hawdb_sql_syntax::peg::{Expression as E, NodeId};
use hawdb_sql_syntax::TokenKind as T;

use super::{values::*, Transformer};
use crate::*;

pub(super) fn rules() -> Vec<(&'static str, E)> {
    vec![
        (
            "d_create_data",
            s(vec![r("d_node"), o(s(vec![r("d_edge"), r("d_node")]))]),
        ),
        (
            "d_node",
            group(s(vec![
                o(r("d_variable")),
                t(T::Colon),
                r("v_identifier"),
                o(r("v_properties")),
            ])),
        ),
        ("d_variable", r("v_identifier")),
        (
            "d_edge",
            s(vec![
                t(T::Minus),
                t(T::LeftBracket),
                t(T::Colon),
                r("v_identifier"),
                o(r("v_properties")),
                t(T::RightBracket),
                t(T::Minus),
                t(T::Greater),
            ]),
        ),
        (
            "d_merge_data",
            c(vec![
                s(vec![r("d_node"), r("d_edge"), r("d_node")]),
                s(vec![r("d_node"), many(r("d_on_sets")), o(r("d_post_sets"))]),
            ]),
        ),
        (
            "d_on_sets",
            s(vec![k("ON"), r("d_on_kind"), k("SET"), r("d_sets")]),
        ),
        ("d_on_kind", c(vec![k("CREATE"), k("MATCH")])),
        ("d_post_sets", s(vec![k("SET"), r("d_sets")])),
        ("d_sets", list("d_assignment")),
        (
            "d_assignment",
            s(vec![r("d_ref"), t(T::Equal), r("d_set_value")]),
        ),
        (
            "d_ref",
            s(vec![r("v_identifier"), t(T::Dot), r("v_identifier")]),
        ),
        (
            "d_set_value",
            c(vec![
                r("d_case_preserve"),
                r("d_case_decrement"),
                r("d_coalesce"),
                r("d_property_value"),
                r("v_value"),
            ]),
        ),
        (
            "d_property_value",
            s(vec![
                E::Not(
                    Box::new(c(vec![k("CASE"), k("COALESCE")])),
                    "a property value",
                ),
                r("d_ref"),
                o(r("d_add")),
            ]),
        ),
        ("d_add", s(vec![t(T::Plus), r("v_value")])),
        (
            "d_coalesce",
            s(vec![
                k("COALESCE"),
                group(s(vec![r("d_ref"), t(T::Comma), r("v_value")])),
                o(r("d_add")),
            ]),
        ),
        (
            "d_case_preserve",
            s(vec![
                k("CASE"),
                k("WHEN"),
                E::Ahead(Box::new(t(T::Parameter(0))), "an incoming parameter"),
                r("v_value"),
                k("IS"),
                k("NULL"),
                k("THEN"),
                r("d_ref"),
                k("WHEN"),
                r("v_value"),
                t(T::Equal),
                r("v_value"),
                k("AND"),
                r("d_ref"),
                k("IS"),
                k("NOT"),
                k("NULL"),
                k("AND"),
                r("d_ref"),
                t(T::Greater),
                r("v_value"),
                k("THEN"),
                r("d_ref"),
                k("ELSE"),
                r("v_value"),
                k("END"),
            ]),
        ),
        (
            "d_case_decrement",
            s(vec![
                k("CASE"),
                k("WHEN"),
                r("d_ref"),
                t(T::Greater),
                r("v_value"),
                k("THEN"),
                r("d_ref"),
                t(T::Minus),
                r("v_value"),
                k("ELSE"),
                r("v_value"),
                k("END"),
            ]),
        ),
        (
            "hint",
            s(vec![
                k("SYSTEM"),
                t(T::Dot),
                r("v_identifier"),
                t(T::Equal),
                r("v_value"),
            ]),
        ),
        (
            "set",
            s(vec![
                k("SET"),
                k("SYSTEM"),
                c(vec![
                    r("d_system_variable"),
                    s(vec![t(T::Dot), r("v_identifier")]),
                ]),
                t(T::Equal),
                r("v_value"),
            ]),
        ),
        (
            "d_system_variable",
            s(vec![
                k("VARIABLE"),
                c(vec![
                    s(vec![k("SYSTEM"), t(T::Dot), r("v_identifier")]),
                    s(vec![
                        E::Not(Box::new(k("SYSTEM")), "a variable name"),
                        r("v_identifier"),
                    ]),
                ]),
            ]),
        ),
    ]
}

impl Transformer<'_> {
    pub(super) fn named_children(&self, node: NodeId, name: &str) -> Vec<NodeId> {
        self.tree
            .children(node)
            .iter()
            .copied()
            .filter(|id| self.tree.nodes[*id].name == Some(name))
            .collect()
    }
    pub(super) fn system_assignment(
        &mut self,
        node: NodeId,
        depth: usize,
    ) -> Result<SetSystemVariable> {
        let names = self.tree.child(node, "d_system_variable").unwrap_or(node);
        let name = self
            .text(self.tree.required(names, "v_identifier"))
            .to_ascii_lowercase();
        Ok(SetSystemVariable {
            name,
            value: self.value(self.tree.required(node, "v_value"), depth)?,
        })
    }
    pub(super) fn data_node(&mut self, node: NodeId, depth: usize) -> Result<CreateNode> {
        let label = self
            .text(self.tree.required(node, "v_identifier"))
            .to_owned();
        let properties = self
            .tree
            .child(node, "v_properties")
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
        let nodes = self.named_children(node, "d_node");
        let source = self.data_node(nodes[0], depth)?;
        if let Some(edge) = self.tree.child(node, "d_edge") {
            let rel_type = self
                .text(self.tree.required(edge, "v_identifier"))
                .to_owned();
            let properties = self
                .tree
                .child(edge, "v_properties")
                .map(|id| self.properties(id, depth))
                .transpose()?
                .unwrap_or_default();
            let target = self.data_node(nodes[1], depth)?;
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
            .child(nodes[0], "d_variable")
            .map(|id| self.text(id).to_owned());
        let mut on_create_sets = Vec::new();
        let mut on_match_sets = Vec::new();
        for sets in self.named_children(node, "d_on_sets") {
            let create = self
                .text(self.tree.required(sets, "d_on_kind"))
                .eq_ignore_ascii_case("CREATE");
            let assignments = self.sets(self.tree.required(sets, "d_sets"), depth)?;
            if create {
                on_create_sets = assignments;
            } else {
                on_match_sets = assignments;
            }
        }
        let post_merge_sets = self
            .tree
            .child(node, "d_post_sets")
            .map(|id| self.sets(self.tree.required(id, "d_sets"), depth))
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
        let names = self.ids(node);
        (names[0].clone(), names[1].clone())
    }
    pub(super) fn sets(&mut self, node: NodeId, depth: usize) -> Result<Vec<SetProperty>> {
        let children = self.tree.children(node).to_vec();
        children
            .into_iter()
            .map(|assignment| {
                let (variable, property) = self.reference(self.tree.required(assignment, "d_ref"));
                let value = self.tree.required(assignment, "d_set_value");
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
        match self.tree.nodes[node].name.expect("set value syntax") {
            "v_value" => Ok(SetValueExpression::Value(self.value(node, depth)?)),
            "d_property_value" | "d_coalesce" => {
                let (variable, property) = self.reference(self.tree.required(node, "d_ref"));
                let default = self
                    .tree
                    .child(node, "v_value")
                    .map(|id| self.value(id, depth))
                    .transpose()?;
                let addition = self
                    .tree
                    .child(node, "d_add")
                    .map(|id| self.value(self.tree.required(id, "v_value"), depth))
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
                    .named_children(node, "d_ref")
                    .into_iter()
                    .map(|id| self.reference(id))
                    .collect::<Vec<_>>();
                let mut values = Vec::new();
                for value in self.named_children(node, "v_value") {
                    values.push(self.value(value, depth)?);
                }
                if refs.iter().any(|reference| *reference != refs[0]) {
                    return Err(self.error(node, "CASE SET must use the same property"));
                }
                let (variable, property) = refs[0].clone();
                if self.tree.nodes[node].name == Some("d_case_decrement") {
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
                    Ok(SetValueExpression::PreserveNewerExisting {
                        variable,
                        property,
                        incoming: values[0].clone(),
                        preserve: values[1].clone(),
                    })
                }
            }
            _ => unreachable!("set value rule"),
        }
    }
}
