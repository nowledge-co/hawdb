// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Graph patterns and ordered mutation clauses, matched before name allocation.
use hawdb_core::Result;
use hawdb_sql_syntax::peg::{Expression as E, NodeId};
use hawdb_sql_syntax::TokenKind as T;

use super::{values::*, Transformer};
use crate::*;

pub(super) fn rules() -> Vec<(&'static str, E)> {
    vec![
        ("p_patterns", list("p_pattern")),
        (
            "p_pattern",
            s(vec![
                o(r("p_binding")),
                c(vec![
                    s(vec![r("p_function"), group(r("p_chain"))]),
                    r("p_chain"),
                ]),
            ]),
        ),
        ("p_binding", s(vec![r("v_identifier"), t(T::Equal)])),
        (
            "p_function",
            c(vec![k("shortestPath"), k("allShortestPaths")]),
        ),
        ("p_chain", s(vec![r("p_node"), many(r("p_step"))])),
        (
            "p_node",
            group(s(vec![
                c(vec![
                    r("p_variable"),
                    E::Ahead(
                        Box::new(c(vec![t(T::Colon), t(T::LeftBrace), t(T::RightParen)])),
                        "identifier",
                    ),
                ]),
                o(r("p_labels")),
                o(r("v_properties")),
            ])),
        ),
        ("p_variable", r("v_identifier")),
        (
            "p_labels",
            s(vec![
                t(T::Colon),
                r("v_identifier"),
                many(s(vec![t(T::Colon), r("v_identifier")])),
            ]),
        ),
        ("p_step", s(vec![r("p_relationship"), r("p_node")])),
        ("p_relationship", c(vec![r("p_incoming"), r("p_outgoing")])),
        (
            "p_incoming",
            s(vec![t(T::Less), t(T::Minus), r("p_rel_body"), t(T::Minus)]),
        ),
        (
            "p_outgoing",
            s(vec![
                t(T::Minus),
                r("p_rel_body"),
                t(T::Minus),
                o(r("p_arrow")),
            ]),
        ),
        ("p_arrow", t(T::Greater)),
        (
            "p_rel_body",
            s(vec![
                t(T::LeftBracket),
                c(vec![
                    r("p_rel_type"),
                    s(vec![r("p_variable"), o(r("p_rel_type"))]),
                    E::Ahead(
                        Box::new(c(vec![t(T::RightBracket), t(T::Star)])),
                        "a relationship variable or type",
                    ),
                ]),
                o(r("p_hops")),
                o(r("v_properties")),
                t(T::RightBracket),
            ]),
        ),
        ("p_rel_type", s(vec![t(T::Colon), r("v_identifier")])),
        (
            "p_hops",
            s(vec![
                t(T::Star),
                o(r("p_selector")),
                o(r("p_min")),
                o(r("p_range")),
            ]),
        ),
        (
            "p_selector",
            s(vec![c(vec![k("ANY"), k("ALL")]), k("SHORTEST")]),
        ),
        ("p_min", t(T::Number)),
        (
            "p_range",
            s(vec![
                t(T::Dot),
                c(vec![
                    E::Tight(Box::new(s(vec![t(T::Dot), r("p_max")]))),
                    t(T::Dot),
                ]),
            ]),
        ),
        ("p_max", t(T::Number)),
        (
            "match_clause",
            s(vec![k("MATCH"), r("p_patterns"), o(r("p_where"))]),
        ),
        (
            "optional_clause",
            s(vec![
                k("OPTIONAL"),
                k("MATCH"),
                r("p_patterns"),
                o(r("p_where")),
            ]),
        ),
        (
            "unwind_clause",
            s(vec![k("UNWIND"), r("v_value"), k("AS"), r("v_identifier")]),
        ),
        ("create_clause", s(vec![k("CREATE"), r("p_patterns")])),
        (
            "merge_clause",
            s(vec![k("MERGE"), r("p_pattern"), many(r("d_on_sets"))]),
        ),
        ("set_clause", s(vec![k("SET"), r("d_sets")])),
        ("delete_clause", s(vec![k("DELETE"), list("v_identifier")])),
        (
            "detach_clause",
            s(vec![k("DETACH"), k("DELETE"), list("v_identifier")]),
        ),
    ]
}

impl Transformer<'_> {
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
        let explicit = self.tree.child(node, "p_variable");
        let variable = if let Some(id) = explicit {
            self.text(id).to_owned()
        } else {
            let name = format!("__anon{anonymous}");
            *anonymous += 1;
            name
        };
        let label = self
            .tree
            .child(node, "p_labels")
            .map(|id| self.joined_ids(id, ":"))
            .unwrap_or_default();
        let properties = self
            .tree
            .child(node, "v_properties")
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
        let function = self.tree.child(node, "p_function").map(|id| {
            if self.text(id).eq_ignore_ascii_case("shortestPath") {
                PathSearch::AnyShortest
            } else {
                PathSearch::AllShortest
            }
        });
        let variable = self
            .tree
            .child(node, "p_binding")
            .map(|id| self.text(self.tree.required(id, "v_identifier")).to_owned());
        let chain = self.tree.required(node, "p_chain");
        let first = self.pattern_node(self.tree.required(chain, "p_node"), depth, anonymous)?;
        let mut steps = Vec::new();
        for step in self.iter_named_children(chain, "p_step") {
            let edge = self.tree.required(step, "p_relationship");
            let shape = self.tree.children(edge)[0];
            let direction = if self.tree.nodes[shape].name == Some("p_incoming") {
                RelationshipDirection::Incoming
            } else if self.tree.child(shape, "p_arrow").is_some() {
                RelationshipDirection::Outgoing
            } else {
                RelationshipDirection::Undirected
            };
            let body = self.tree.required(shape, "p_rel_body");
            let variable = self
                .tree
                .child(body, "p_variable")
                .map(|id| self.text(id).to_owned());
            let rel_type = self
                .tree
                .child(body, "p_rel_type")
                .map(|id| self.text(self.tree.required(id, "v_identifier")).to_owned())
                .unwrap_or_default();
            let mut search = PathSearch::All;
            let (mut min_hops, mut max_hops) = (1, 1);
            if let Some(hops) = self.tree.child(body, "p_hops") {
                if let Some(selector) = self.tree.child(hops, "p_selector") {
                    search = if self.tokens[self.tree.nodes[selector].start]
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
                    .child(hops, "p_min")
                    .map(|id| {
                        self.text(id)
                            .parse::<usize>()
                            .map_err(|_| self.error(id, "invalid unsigned integer"))
                    })
                    .transpose()?;
                let range = self.tree.child(hops, "p_range");
                let max = range
                    .and_then(|id| self.tree.child(id, "p_max"))
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
                if (if range.is_some() {
                    max.is_none()
                } else {
                    min.is_none()
                }) && !search.is_shortest()
                    && function.is_none()
                {
                    return Err(self.error(
                        hops,
                        "bounded relationship pattern requires a finite max hop",
                    ));
                }
                if min_hops > max_hops {
                    return Err(self.error(hops, "relationship min hop must not exceed max hop"));
                }
            }
            if let Some(function) = function {
                search = function;
            }
            let properties = self
                .tree
                .child(body, "v_properties")
                .map(|id| self.properties(id, depth))
                .transpose()?
                .unwrap_or_default();
            let relationship = self.source(
                edge,
                RelationshipPatternKind {
                    variable,
                    rel_type,
                    properties,
                    direction,
                    min_hops,
                    max_hops,
                    search,
                },
            );
            let target = self.pattern_node(self.tree.required(step, "p_node"), depth, anonymous)?;
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
        let name = self.tree.nodes[node].name.expect("clause rule");
        let kind = match name {
            "match_clause" | "optional_clause" | "create_clause" => {
                let patterns =
                    self.named_children(self.tree.required(node, "p_patterns"), "p_pattern");
                let mut result = Vec::new();
                for pattern in patterns {
                    result.push(self.pattern(pattern, depth, anonymous)?);
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
            "unwind_clause" => {
                let source = self.value(self.tree.required(node, "v_value"), depth)?;
                if !matches!(
                    source.kind,
                    ValueExpressionKind::Parameter(_) | ValueExpressionKind::List(_)
                ) {
                    return Err(self.error(
                        node,
                        "UNWIND accepts only a parameter or list literal source",
                    ));
                }
                ClauseKind::Unwind {
                    source,
                    variable: self
                        .text(self.tree.required(node, "v_identifier"))
                        .to_owned(),
                }
            }
            "merge_clause" => {
                let pattern =
                    self.pattern(self.tree.required(node, "p_pattern"), depth, anonymous)?;
                let mut on_create = Vec::new();
                let mut on_match = Vec::new();
                for sets in self.iter_named_children(node, "d_on_sets") {
                    let create = self
                        .text(self.tree.required(sets, "d_on_kind"))
                        .eq_ignore_ascii_case("CREATE");
                    let assignments = self.sets(self.tree.required(sets, "d_sets"), depth)?;
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
            "set_clause" => ClauseKind::Set(self.sets(self.tree.required(node, "d_sets"), depth)?),
            "delete_clause" | "detach_clause" => ClauseKind::Delete {
                detach: name == "detach_clause",
                variables: self.ids(node),
            },
            _ => unreachable!("graph clause rule"),
        };
        Ok(self.source(node, kind))
    }
}
