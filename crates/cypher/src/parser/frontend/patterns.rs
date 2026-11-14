// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Graph patterns and ordered mutation clauses, matched before name allocation.
use hawdb_core::{Result, Value};
use hawdb_sql_syntax::peg::NodeId;

use super::Transformer;
use crate::*;

impl Transformer<'_> {
    fn check_computed_unwind(&self, node: NodeId) -> Result<()> {
        let next = self.tree.node(node).end;
        if ["CREATE", "MERGE", "SET", "DELETE"]
            .iter()
            .any(|word| self.tokens[next].is_keyword(self.input, word))
        {
            return Err(self.error_at(
                self.tokens[next].span().start,
                "computed UNWIND sources require a read pipeline",
            ));
        }
        Ok(())
    }

    pub(super) fn source<Ty>(&self, node: NodeId, kind: Ty) -> AstNode<Ty> {
        let span = self.tree.span(node, self.tokens);
        AstNode::from_source(
            kind,
            SourceSpan {
                start: span.start,
                end: span.end,
            },
        )
    }
    fn pattern_node(
        &mut self,
        node: NodeId,
        depth: usize,
        anonymous: &mut usize,
    ) -> Result<NodePattern> {
        let explicit = self.tree.child(node, rule_id!("p_variable"));
        let variable = if let Some(id) = explicit {
            self.text(id).to_owned()
        } else {
            let name = format!("__anon{anonymous}");
            *anonymous += 1;
            name
        };
        let label = self
            .tree
            .child(node, rule_id!("p_labels"))
            .map(|id| self.joined_ids(id, ":"))
            .unwrap_or_default();
        let properties = self
            .tree
            .child(node, rule_id!("v_properties"))
            .map(|id| self.properties(id, depth))
            .transpose()?
            .unwrap_or_default();
        Ok(self.source(
            node,
            NodePatternKind {
                variable,
                anonymous: explicit.is_none(),
                label,
                properties,
            },
        ))
    }
    pub(super) fn pattern(
        &mut self,
        node: NodeId,
        depth: usize,
        anonymous: &mut usize,
    ) -> Result<MatchPattern> {
        let function = self.tree.child(node, rule_id!("p_function")).map(|id| {
            if self.text(id).eq_ignore_ascii_case("shortestPath") {
                PathSearch::AnyShortest
            } else {
                PathSearch::AllShortest
            }
        });
        let variable = self.tree.child(node, rule_id!("p_binding")).map(|id| {
            self.text(self.tree.required(id, rule_id!("v_identifier")))
                .to_owned()
        });
        let chain = self.tree.required(node, rule_id!("p_chain"));
        let first = self.pattern_node(
            self.tree.required(chain, rule_id!("p_node")),
            depth,
            anonymous,
        )?;
        let mut steps = Vec::new();
        for step in self.iter_named_children(chain, rule_id!("p_step")) {
            let edge = self.tree.required(step, rule_id!("p_relationship"));
            let shape = self.tree.children(edge)[0];
            let direction = if self.tree.rule_id(shape) == rule_id!("p_incoming") {
                RelationshipDirection::Incoming
            } else if self.tree.child(shape, rule_id!("p_arrow")).is_some() {
                RelationshipDirection::Outgoing
            } else {
                RelationshipDirection::Undirected
            };
            let body = self.tree.required(shape, rule_id!("p_rel_body"));
            let variable = self
                .tree
                .child(body, rule_id!("p_variable"))
                .map(|id| self.text(id).to_owned());
            let mut rel_types = Vec::<String>::new();
            if let Some(id) = self.tree.child(body, rule_id!("p_rel_type")) {
                for &child in self.tree.children(id) {
                    if self.tree.rule_id(child) == rule_id!("v_identifier") {
                        let name = self.text(child).to_owned();
                        if !rel_types.contains(&name) {
                            rel_types.push(name);
                        }
                    }
                }
            }
            let rel_type = rel_types.first().cloned().unwrap_or_default();
            let rel_type_disjunction = if rel_types.len() > 1 {
                rel_types
            } else {
                Vec::new()
            };
            let mut search = PathSearch::All;
            let (mut min_hops, mut max_hops) = (1, 1);
            if let Some(hops) = self.tree.child(body, rule_id!("p_hops")) {
                if let Some(selector) = self.tree.child(hops, rule_id!("p_selector")) {
                    search = if self.tokens[self.tree.node(selector).start]
                        .is_keyword(self.input, "ANY")
                    {
                        PathSearch::AnyShortest
                    } else {
                        PathSearch::AllShortest
                    };
                }
                if function.is_some() && search.is_shortest() {
                    return Err(self.error(
                        hops,
                        "shortest-path functions do not accept a nested SHORTEST selector",
                    ));
                }
                let min = self
                    .tree
                    .child(hops, rule_id!("p_min"))
                    .map(|id| {
                        self.text(id)
                            .parse::<usize>()
                            .map_err(|_| self.error(id, "invalid unsigned integer"))
                    })
                    .transpose()?;
                let range = self.tree.child(hops, rule_id!("p_range"));
                let max = range
                    .and_then(|id| self.tree.child(id, rule_id!("p_max")))
                    .map(|id| {
                        self.text(id)
                            .parse::<usize>()
                            .map_err(|_| self.error(id, "invalid unsigned integer"))
                    })
                    .transpose()?;
                if range.is_some() {
                    min_hops = min.unwrap_or(1);
                    max_hops = max.unwrap_or(UNBOUNDED_MAX_HOPS);
                } else if let Some(exact) = min {
                    min_hops = exact;
                    max_hops = exact;
                } else {
                    max_hops = UNBOUNDED_MAX_HOPS;
                }
                // `*` and `*n..` are unbounded: trails stay finite because a
                // trail never repeats a relationship (#1489).
                if min_hops > max_hops {
                    return Err(self.error(hops, "relationship min hop must not exceed max hop"));
                }
            }
            if let Some(function) = function {
                search = function;
            }
            let properties = self
                .tree
                .child(body, rule_id!("v_properties"))
                .map(|id| self.properties(id, depth))
                .transpose()?
                .unwrap_or_default();
            let relationship = self.source(
                edge,
                RelationshipPatternKind {
                    variable,
                    rel_type,
                    rel_type_disjunction,
                    properties,
                    direction,
                    min_hops,
                    max_hops,
                    search,
                },
            );
            let target = self.pattern_node(
                self.tree.required(step, rule_id!("p_node")),
                depth,
                anonymous,
            )?;
            steps.push(self.source(
                step,
                PatternStepKind {
                    relationship,
                    target,
                },
            ));
        }
        if function.is_some() && steps.is_empty() {
            return Err(self.error(
                node,
                "shortest-path functions require a relationship pattern",
            ));
        }
        Ok(self.source(
            node,
            MatchPatternKind {
                variable,
                first,
                steps,
            },
        ))
    }
    pub(super) fn graph_clause(
        &mut self,
        node: NodeId,
        depth: usize,
        anonymous: &mut usize,
    ) -> Result<Clause> {
        let name = self.tree.node(node).name.expect("clause rule");
        let kind = match name {
            "match_clause" | "optional_clause" | "create_clause" => {
                let patterns = self.tree.required(node, rule_id!("p_patterns"));
                let initial_capacity = self
                    .iter_named_children(patterns, rule_id!("p_pattern"))
                    .count()
                    .min(4);
                let mut result = Vec::new();
                for pattern in self.iter_named_children(patterns, rule_id!("p_pattern")) {
                    let pattern = self.pattern(pattern, depth, anonymous)?;
                    if result.is_empty() {
                        result.reserve_exact(initial_capacity);
                    }
                    result.push(pattern);
                }
                if name == "create_clause" {
                    ClauseKind::Create(result)
                } else {
                    let predicate = self.fold_where(node, depth, anonymous)?;
                    ClauseKind::Match {
                        optional: name == "optional_clause",
                        patterns: result,
                        predicate,
                    }
                }
            }
            "load_clause" => {
                let values = self.named_children(node, rule_id!("v_value"));
                let source = self.value(values[0], depth)?;
                if !matches!(
                    source.kind,
                    ValueExpressionKind::Parameter(_)
                        | ValueExpressionKind::Literal(Value::String(_))
                ) {
                    return Err(self.error(
                        values[0],
                        "LOAD CSV FROM accepts only a string literal or parameter source",
                    ));
                }
                let field_terminator = if let Some(value) = values.get(1) {
                    let value = *value;
                    let parsed = self.value(value, depth)?;
                    let ValueExpressionKind::Literal(Value::String(text)) = parsed.kind else {
                        return Err(self.error(value, "FIELDTERMINATOR requires a string literal"));
                    };
                    let mut chars = text.chars();
                    match (chars.next(), chars.next()) {
                        (Some(ch), None) if ch.is_ascii() && !matches!(ch, '"' | '\r' | '\n') => ch,
                        _ => return Err(self.error(value, "FIELDTERMINATOR must be one ASCII character other than a double quote or line break")),
                    }
                } else {
                    ','
                };
                ClauseKind::LoadCsv(LoadCsvClause {
                    with_headers: self.tokens[self.tree.node(node).start + 2]
                        .is_keyword(self.input, "WITH"),
                    source,
                    variable: self
                        .text(self.tree.required(node, rule_id!("v_identifier")))
                        .to_owned(),
                    field_terminator,
                })
            }
            "unwind_clause" => {
                let variable = self
                    .text(self.tree.required(node, rule_id!("v_identifier")))
                    .to_owned();
                if let Some(value) = self.tree.child(node, rule_id!("v_value")) {
                    let source = self.value(value, depth)?;
                    if matches!(
                        source.kind,
                        ValueExpressionKind::Parameter(_)
                            | ValueExpressionKind::List(_)
                            | ValueExpressionKind::Binding(_)
                            | ValueExpressionKind::BindingProperty { .. }
                    ) {
                        ClauseKind::Unwind { source, variable }
                    } else {
                        self.check_computed_unwind(node)?;
                        ClauseKind::UnwindExpression {
                            source: self.source(value, ScalarExpressionKind::Value(source)),
                            variable,
                        }
                    }
                } else {
                    let source = self
                        .case_scalar(self.tree.required(node, rule_id!("x_case_scalar")), depth)?;
                    self.check_computed_unwind(node)?;
                    ClauseKind::UnwindExpression { source, variable }
                }
            }

            "merge_clause" => {
                let pattern = self.pattern(
                    self.tree.required(node, rule_id!("p_pattern")),
                    depth,
                    anonymous,
                )?;
                let mut on_create = Vec::new();
                let mut on_match = Vec::new();
                for sets in self.iter_named_children(node, rule_id!("d_on_sets")) {
                    let create = self
                        .text(self.tree.required(sets, rule_id!("d_on_kind")))
                        .eq_ignore_ascii_case("CREATE");
                    let assignments =
                        self.sets(self.tree.required(sets, rule_id!("d_sets")), depth)?;
                    let slot = if create {
                        &mut on_create
                    } else {
                        &mut on_match
                    };
                    if !slot.is_empty() {
                        return Err(self.error(sets, "duplicate MERGE action"));
                    }
                    *slot = assignments;
                }
                ClauseKind::Merge {
                    pattern,
                    on_create,
                    on_match,
                }
            }
            "set_clause" => {
                ClauseKind::Set(self.sets(self.tree.required(node, rule_id!("d_sets")), depth)?)
            }
            "delete_clause" | "detach_clause" => ClauseKind::Delete {
                detach: name == "detach_clause",
                variables: self.ids(node),
            },
            _ => unreachable!("graph clause rule"),
        };
        Ok(self.source(node, kind))
    }
}
