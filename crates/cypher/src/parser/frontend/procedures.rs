// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Procedure argument grammar and typed semantic lowering, without reparsing.
use hawdb_core::{Result, Value, VectorIndexQueryMode};
use hawdb_sql_syntax::peg::{Expression as E, NodeId};
use hawdb_sql_syntax::TokenKind as T;
use std::collections::{BTreeMap, BTreeSet};

use super::{values::*, Transformer};
use crate::*;

pub(super) fn rules() -> Vec<(&'static str, E)> {
    vec![
        (
            "u_legacy_call",
            s(vec![
                k("CALL"),
                E::Ahead(
                    Box::new(c(vec![k("page_rank"), k("pagerank"), k("louvain")])),
                    "a graph algorithm",
                ),
                r("u_procedure"),
                o(r("u_legacy_return")),
                E::Ahead(Box::new(c(vec![t(T::Semicolon), t(T::End)])), "','"),
            ]),
        ),
        (
            "u_legacy_return",
            s(vec![
                k("RETURN"),
                list("v_identifier"),
                o(r("u_legacy_trailing_comma")),
            ]),
        ),
        ("u_legacy_trailing_comma", t(T::Comma)),
        (
            "call_clause",
            s(vec![k("CALL"), r("u_procedure"), o(r("u_yields"))]),
        ),
        (
            "u_procedure",
            s(vec![r("v_identifier"), group(o(list("u_arg")))]),
        ),
        ("u_arg", c(vec![r("u_named"), r("u_option")])),
        (
            "u_named",
            s(vec![
                r("v_identifier"),
                E::Tight(Box::new(s(vec![t(T::Colon), t(T::Equal)]))),
                r("u_option"),
            ]),
        ),
        (
            "u_option",
            c(vec![r("v_value"), r("u_map"), r("v_identifier")]),
        ),
        (
            "u_map",
            s(vec![t(T::LeftBrace), r("u_map_contents"), t(T::RightBrace)]),
        ),
        (
            "u_map_contents",
            o(s(vec![list("u_map_entry"), o(t(T::Comma))])),
        ),
        (
            "u_map_entry",
            s(vec![r("u_key"), t(T::Colon), r("u_option")]),
        ),
        ("u_key", c(vec![r("v_string"), r("v_identifier")])),
        ("u_yields", s(vec![k("YIELD"), list("u_yield")])),
        ("u_yield", s(vec![r("v_identifier"), o(r("u_yield_alias"))])),
        ("u_yield_alias", s(vec![k("AS"), r("v_identifier")])),
    ]
}

impl Transformer<'_> {
    fn procedure_value(&mut self, option: NodeId, depth: usize) -> Result<ValueExpression> {
        let node = self.tree.required(option, "v_value");
        self.value(node, depth)
    }
    fn required_procedure_value(
        &mut self,
        option: NodeId,
        depth: usize,
    ) -> Result<ValueExpression> {
        if self.tree.child(option, "v_value").is_none() {
            return Err(self.error(option, "expected value"));
        }
        self.procedure_value(option, depth)
    }
    fn string_value(&mut self, option: NodeId, depth: usize) -> Result<String> {
        let value = self.required_procedure_value(option, depth)?;
        if let ValueExpressionKind::Literal(Value::String(value)) = value.kind {
            Ok(value)
        } else {
            Err(self.error(option, "expected string"))
        }
    }
    fn projected_labels(
        &mut self,
        option: NodeId,
        relationships: bool,
        depth: usize,
    ) -> Result<(Vec<String>, BTreeMap<String, PropertyPredicate>)> {
        let mut labels = Vec::new();
        let mut predicates = BTreeMap::new();
        if let Some(map) = self.tree.child(option, "u_map") {
            let entries = self
                .tree
                .children(self.tree.required(map, "u_map_contents"))
                .to_vec();
            let mut seen = BTreeSet::new();
            for entry in entries {
                let key = self.tree.required(entry, "u_key");
                let Some(key) = self.tree.child(key, "v_string") else {
                    return Err(self.error(entry, "expected string"));
                };
                let label = self.decode_string(key)?;
                if !seen.insert(label.clone()) {
                    return Err(self.error(
                        entry,
                        if relationships {
                            "duplicate projected relationship type map key"
                        } else {
                            "duplicate projected node label map key"
                        },
                    ));
                }
                let filter = self.string_value(self.tree.required(entry, "u_option"), depth)?;
                if !filter.trim().is_empty() {
                    if !relationships {
                        return Err(
                            self.error(entry, "projected node predicates are not supported")
                        );
                    }
                    predicates.insert(
                        label.clone(),
                        super::super::projected::parse_projected_relationship_predicate(&filter)?,
                    );
                }
                labels.push(label);
            }
        } else {
            let value = self.required_procedure_value(option, depth)?;
            let ValueExpressionKind::List(values) = value.kind else {
                return Err(self.error(option, "expected a string list or map"));
            };
            for value in values {
                let ValueExpressionKind::Literal(Value::String(value)) = value.kind else {
                    return Err(self.error(option, "expected string"));
                };
                labels.push(value);
            }
        }
        Ok((labels, predicates))
    }
    fn ignored_procedure_option(&mut self, node: NodeId, depth: usize) -> Result<()> {
        self.check_depth(node, depth, 1)?;
        if let Some(map) = self.tree.child(node, "u_map") {
            for entry in self
                .tree
                .children(self.tree.required(map, "u_map_contents"))
                .to_vec()
            {
                let key = self.tree.required(entry, "u_key");
                if let Some(string) = self.tree.child(key, "v_string") {
                    self.decode_string(string)?;
                }
                self.ignored_procedure_option(self.tree.required(entry, "u_option"), depth + 1)?;
            }
        } else if let Some(value) = self.tree.child(node, "v_value") {
            let selected = self.tree.children(value)[0];
            match self.tree.nodes[selected].name {
                Some("v_number") => {
                    self.text(selected)
                        .parse::<f64>()
                        .map_err(|_| self.error(selected, "invalid float"))?;
                }
                Some("v_list") => {
                    self.projected_labels(node, false, depth)?;
                }
                Some("v_parameter" | "v_string" | "v_boolean" | "v_null") => {
                    self.value(value, depth)?;
                }
                _ => return Err(self.error(node, "unsupported procedure option value")),
            }
        }
        Ok(())
    }
    pub(super) fn procedure_clause(&mut self, node: NodeId, depth: usize) -> Result<Clause> {
        let procedure = self.tree.required(node, "u_procedure");
        let name = self
            .text(self.tree.required(procedure, "v_identifier"))
            .to_ascii_lowercase();
        let args = self.named_children(procedure, "u_arg");
        let positional = match name.as_str() {
            "vector_index_search" => 2,
            "project_graph" => 3,
            _ => 1,
        };
        if args.len() < positional {
            return Err(self.error(procedure, "missing procedure arguments"));
        }
        let mut positions = Vec::new();
        for arg in &args[..positional] {
            let Some(value) = self.tree.child(*arg, "u_option") else {
                return Err(self.error(*arg, "expected positional argument"));
            };
            positions.push(value);
        }
        let kind = if name == "project_graph" {
            let name = self.string_value(positions[0], depth)?;
            let (node_labels, _) = self.projected_labels(positions[1], false, depth)?;
            let (rel_types, relationship_predicates) =
                self.projected_labels(positions[2], true, depth)?;
            for arg in &args[3..] {
                let Some(option) = self.tree.child(*arg, "u_option") else {
                    return Err(self.error(*arg, "expected procedure option value"));
                };
                self.ignored_procedure_option(option, depth)?;
            }
            ProcedureCallKind::ProjectGraph {
                name,
                node_labels,
                rel_types,
                relationship_predicates,
            }
        } else {
            let mut options = BTreeMap::new();
            for arg in &args[positional..] {
                let Some(named) = self.tree.child(*arg, "u_named") else {
                    return Err(self.error(*arg, "expected named procedure option"));
                };
                let raw = self
                    .text(self.tree.required(named, "v_identifier"))
                    .to_ascii_lowercase();
                let canonical = match (name.as_str(), raw.as_str()) {
                    ("vector_search" | "text_search" | "graph_seed_search", "topk" | "limit") => {
                        "topk"
                    }
                    ("page_rank" | "pagerank" | "louvain", "dampingfactor" | "damping") => {
                        "damping"
                    }
                    ("page_rank" | "pagerank" | "louvain", "maxiterations" | "iterations") => {
                        "iterations"
                    }
                    ("page_rank" | "pagerank" | "louvain", "maxlevels" | "levels") => "levels",
                    ("page_rank" | "pagerank" | "louvain", "maxphases" | "phases") => "phases",
                    _ => raw.as_str(),
                }
                .to_owned();
                if options
                    .insert(canonical, self.tree.required(named, "u_option"))
                    .is_some()
                {
                    return Err(self.error(
                        named,
                        if name == "vector_index_search" {
                            "duplicate vector index search option"
                        } else {
                            "duplicate procedure option"
                        },
                    ));
                }
            }
            let allowed: &[&str] = match name.as_str() {
                "vector_search" => &["topk"],
                "text_search" => &["topk", "complete", "maxcandidates"],
                "graph_seed_search" => &["label", "topk"],
                "vector_index_search" => &["candidates", "topk", "mode", "eligible", "maxwork"],
                "page_rank" | "pagerank" | "louvain" => &[
                    "damping",
                    "iterations",
                    "levels",
                    "phases",
                    "tolerance",
                    "normalizeinitial",
                    "resolution",
                ],
                _ => return Err(self.error(procedure, "unsupported procedure")),
            };
            if options.keys().any(|key| !allowed.contains(&key.as_str())) {
                return Err(self.error(
                    procedure,
                    match name.as_str() {
                        "vector_index_search" => "unsupported vector index search option",
                        "page_rank" | "pagerank" | "louvain" => {
                            "unsupported graph algorithm option"
                        }
                        _ => "unsupported procedure option",
                    },
                ));
            }
            let mut value = |key: &str| {
                options
                    .get(key)
                    .copied()
                    .map(|id| self.required_procedure_value(id, depth))
                    .transpose()
            };
            match name.as_str() {
                "vector_search" => {
                    let top_k = value("topk")?;
                    ProcedureCallKind::VectorSearch(VectorSearch {
                        embedding: self.required_procedure_value(positions[0], depth)?,
                        top_k,
                    })
                }
                "text_search" => {
                    let top_k = value("topk")?;
                    let complete = value("complete")?;
                    let max_candidates = value("maxcandidates")?;
                    ProcedureCallKind::TextSearch(TextSearch {
                        query: self.required_procedure_value(positions[0], depth)?,
                        top_k,
                        complete,
                        max_candidates,
                    })
                }
                "graph_seed_search" => {
                    let label = value("label")?;
                    let top_k = value("topk")?;
                    let label = label
                        .ok_or_else(|| self.error(procedure, "graph seed search requires label"))?;
                    ProcedureCallKind::GraphSeedSearch(GraphSeedSearch {
                        query: self.required_procedure_value(positions[0], depth)?,
                        label,
                        top_k,
                    })
                }
                "vector_index_search" => {
                    let candidates = value("candidates")?;
                    let top_k = value("topk")?;
                    let max_work = value("maxwork")?;
                    let mode = match value("mode")? {
                        None => VectorIndexQueryMode::AnnWindow,
                        Some(ValueExpression {
                            kind: ValueExpressionKind::Literal(Value::String(mode)),
                            ..
                        }) => VectorIndexQueryMode::parse(&mode)?,
                        _ => {
                            return Err(
                                self.error(procedure, "vector index mode must be a string literal")
                            )
                        }
                    };
                    let eligible = if let Some(id) = options.get("eligible") {
                        let selected = self.tree.children(*id)[0];
                        let span = &self.tree.nodes[selected];
                        if span.end != span.start + 1 || self.tokens[span.start].kind != T::Word {
                            return Err(self.error(selected, "expected identifier"));
                        }
                        Some(self.text(selected).to_owned())
                    } else {
                        None
                    };
                    match mode {
                        VectorIndexQueryMode::AnnWindow => {
                            if candidates.is_none() { return Err(self.error(procedure, "vector index search requires candidates")); }
                            if eligible.is_some() { return Err(self.error(procedure, "ann_window does not accept an eligible input")); }
                        }
                        VectorIndexQueryMode::Exact if candidates.is_some() || eligible.is_none() => return Err(self.error(procedure, "exact vector search requires eligible and does not accept candidates")),
                        VectorIndexQueryMode::FilteredAnn if candidates.is_none() || eligible.is_none() => return Err(self.error(procedure, "filtered_ann vector search requires eligible and candidates")),
                        _ => {}
                    }
                    if mode.requires_eligible_domain() != max_work.is_some() {
                        return Err(self.error(
                            procedure,
                            "maxWork is required for exact and filtered_ann vector search only",
                        ));
                    }
                    let top_k = top_k.ok_or_else(|| {
                        self.error(procedure, "vector index search requires topK")
                    })?;
                    ProcedureCallKind::VectorIndexSearch(VectorIndexSearch {
                        index: self.required_procedure_value(positions[0], depth)?,
                        embedding: self.required_procedure_value(positions[1], depth)?,
                        mode,
                        eligible,
                        max_work,
                        candidates,
                        top_k,
                    })
                }
                _ => {
                    let damping = value("damping")?;
                    let max_iterations = value("iterations")?;
                    let max_levels = value("levels")?;
                    let max_phases = value("phases")?;
                    let tolerance = value("tolerance")?;
                    let normalize_initial = value("normalizeinitial")?;
                    let resolution = value("resolution")?;
                    if max_levels.is_some() && max_phases.is_some() {
                        return Err(self.error(
                            procedure,
                            "duplicate graph algorithm option: maxLevels and maxPhases",
                        ));
                    }
                    ProcedureCallKind::GraphAlgorithm {
                        algorithm: if name == "louvain" {
                            GraphAlgorithmKind::Louvain
                        } else {
                            GraphAlgorithmKind::PageRank
                        },
                        graph_name: self.string_value(positions[0], depth)?,
                        options: Box::new(GraphAlgorithmOptions {
                            damping,
                            max_iterations,
                            max_levels,
                            max_phases,
                            tolerance,
                            normalize_initial,
                            resolution,
                        }),
                    }
                }
            }
        };
        let mut yields = Vec::new();
        if let Some(items) = self.tree.child(node, "u_yields") {
            for item in self.tree.children(items) {
                let name = self
                    .text(self.tree.required(*item, "v_identifier"))
                    .to_owned();
                let alias = self
                    .tree
                    .child(*item, "u_yield_alias")
                    .map(|id| self.text(self.tree.required(id, "v_identifier")).to_owned());
                yields.push(self.source(*item, YieldItemKind { name, alias }));
            }
        }
        let procedure = self.source(procedure, kind);
        Ok(self.source(node, ClauseKind::Call { procedure, yields }))
    }
    pub(super) fn call_statement(&mut self, node: NodeId, depth: usize) -> Result<Statement> {
        let mut trailing_comma = None;
        let pipeline = if let Some(legacy) = self.tree.child(node, "u_legacy_call") {
            let mut call = self.procedure_clause(legacy, depth)?;
            let ClauseKind::Call { procedure, .. } = &call.kind else {
                unreachable!()
            };
            call.span.as_mut().expect("source call").end =
                procedure.span.expect("source procedure").end;
            let mut clauses = vec![call];
            if let Some(returned) = self.tree.child(legacy, "u_legacy_return") {
                trailing_comma = self.tree.child(returned, "u_legacy_trailing_comma");
                let items = self
                    .named_children(returned, "v_identifier")
                    .into_iter()
                    .map(|id| {
                        let expression = self
                            .source(id, ScalarExpressionKind::Variable(self.text(id).to_owned()));
                        let expression = self.source(id, ReturnExpressionKind::Value(expression));
                        self.source(
                            id,
                            ReturnItemKind {
                                expression,
                                alias: None,
                            },
                        )
                    })
                    .collect();
                clauses.push(self.source(
                    returned,
                    ClauseKind::Return(ProjectionClause {
                        distinct: false,
                        items,
                        predicate: None,
                        order_by: Vec::new(),
                        offset: None,
                        limit: None,
                    }),
                ));
            }
            self.source(legacy, QueryPipelineKind { clauses })
        } else {
            let pipeline_node = self.tree.required(node, "pipeline");
            self.pipeline(pipeline_node, depth, &mut 0)?
        };
        let ClauseKind::Call { procedure, yields } = &pipeline.clauses[0].kind else {
            unreachable!("CALL prefix")
        };
        match &procedure.kind {
            ProcedureCallKind::ProjectGraph {
                name,
                node_labels,
                rel_types,
                relationship_predicates,
            } => {
                if pipeline.clauses.len() != 1 || !yields.is_empty() {
                    return Err(self.error(node, "unexpected trailing input"));
                }
                Ok(Statement::ProjectGraph(ProjectGraph {
                    name: name.clone(),
                    node_labels: node_labels.clone(),
                    rel_types: rel_types.clone(),
                    relationship_predicates: relationship_predicates.clone(),
                }))
            }
            ProcedureCallKind::VectorSearch(search) => {
                if !yields.is_empty() {
                    if yields.len() != 2
                        || !yields[0].name.eq_ignore_ascii_case("id")
                        || !yields[1].name.eq_ignore_ascii_case("score")
                        || yields.iter().any(|item| item.alias.is_some())
                        || !matches!(
                            pipeline.clauses.get(1).map(|c| &c.kind),
                            Some(ClauseKind::Match {
                                optional: false,
                                ..
                            })
                        )
                        || !matches!(
                            pipeline.clauses.last().map(|c| &c.kind),
                            Some(ClauseKind::Return(_))
                        )
                        || pipeline.clauses[1..].iter().any(|c| {
                            !matches!(
                                c.kind,
                                ClauseKind::Match { .. }
                                    | ClauseKind::With(_)
                                    | ClauseKind::Return(_)
                            )
                        })
                    {
                        return Err(
                            self.error(node, "vector search YIELD must feed a MATCH read query")
                        );
                    }
                    super::transform::admit_optional(&pipeline)
                        .map_err(|message| self.error(node, message))?;
                    return Ok(Statement::Pipeline(Box::new(pipeline)));
                }
                if pipeline.clauses.len() > 1 {
                    let [_, Clause {
                        kind: ClauseKind::Return(projection),
                        ..
                    }] = pipeline.clauses.as_slice()
                    else {
                        return Err(self.error(node, "vector search RETURN must be id, score"));
                    };
                    let names = plain_columns(projection);
                    if names.as_deref().map(|items| {
                        items
                            .iter()
                            .map(|s| s.to_ascii_lowercase())
                            .collect::<Vec<_>>()
                    }) != Some(vec!["id".into(), "score".into()])
                    {
                        return Err(self.error(node, "vector search RETURN must be id, score"));
                    }
                }
                Ok(Statement::VectorSearch(search.clone()))
            }
            ProcedureCallKind::GraphAlgorithm {
                algorithm,
                graph_name,
                options,
            } => {
                if yields.is_empty() {
                    let projection = match pipeline.clauses.as_slice() {
                        [_] => None,
                        [_, Clause {
                            kind: ClauseKind::Return(projection),
                            ..
                        }] => Some(projection),
                        _ => return Ok(Statement::Pipeline(Box::new(pipeline))),
                    };
                    if trailing_comma.is_some()
                        || projection
                            .is_none_or(|p| !self.algorithm_projection_is_pipeline(p, *algorithm))
                    {
                        let default = if *algorithm == GraphAlgorithmKind::PageRank {
                            "pagerank_score"
                        } else {
                            "louvain_id"
                        };
                        let (score_column, return_node_identity) = if let Some(projection) =
                            projection
                        {
                            let columns = plain_columns(projection).ok_or_else(|| {
                                self.error(node, "unexpected procedure RETURN column")
                            })?;
                            let mut index = 0;
                            let identifier_end = |index: usize| {
                                projection
                                    .items
                                    .get(index)
                                    .and_then(|item| item.span)
                                    .map(|span| span.end)
                                    .unwrap_or_else(|| {
                                        trailing_comma
                                            .map(|id| self.tree.span(id, self.tokens).end)
                                            .unwrap_or(self.input.len())
                                    })
                            };
                            let next_token = |position| {
                                self.tokens[self
                                    .tokens
                                    .partition_point(|token| token.span.start < position)]
                                .span
                                .start
                            };
                            if !columns[0].eq_ignore_ascii_case("node") {
                                return Err(self.error_at(
                                    identifier_end(0),
                                    "expected node in procedure RETURN",
                                ));
                            }
                            index += 1;
                            let return_node_identity = columns
                                .get(index)
                                .is_some_and(|s| s.eq_ignore_ascii_case("node_id"));
                            if return_node_identity {
                                index += 1;
                                let label = columns.get(index).ok_or_else(|| {
                                    self.error_at(
                                        next_token(identifier_end(index - 1)),
                                        "expected identifier",
                                    )
                                })?;
                                if !label.eq_ignore_ascii_case("node_label") {
                                    return Err(self.error_at(
                                        identifier_end(index),
                                        "expected node_label after node_id",
                                    ));
                                }
                                index += 1;
                            }
                            if *algorithm == GraphAlgorithmKind::Louvain
                                && columns
                                    .get(index)
                                    .is_some_and(|s| s.eq_ignore_ascii_case("level"))
                                && (index + 1 < columns.len() || trailing_comma.is_some())
                            {
                                index += 1;
                            }
                            let score = columns.get(index).ok_or_else(|| {
                                self.error_at(
                                    trailing_comma
                                        .map(|id| self.tokens[self.tree.nodes[id].end].span.start)
                                        .unwrap_or(self.input.len()),
                                    "expected identifier",
                                )
                            })?;
                            if !score.eq_ignore_ascii_case(default)
                                && !(*algorithm == GraphAlgorithmKind::PageRank
                                    && score.eq_ignore_ascii_case("rank"))
                            {
                                return Err(self.error_at(
                                    next_token(identifier_end(index)),
                                    "unexpected procedure RETURN column",
                                ));
                            }
                            if index + 1 < columns.len() || trailing_comma.is_some() {
                                return Err(self.error_at(
                                    next_token(identifier_end(index)),
                                    "unexpected trailing input",
                                ));
                            }
                            (score.clone(), return_node_identity)
                        } else {
                            (default.to_owned(), false)
                        };
                        return Ok(Statement::GraphAlgorithm(GraphAlgorithm {
                            algorithm: *algorithm,
                            graph_name: graph_name.clone(),
                            options: options.clone(),
                            score_column,
                            return_node_identity,
                        }));
                    }
                }
                Ok(Statement::Pipeline(Box::new(pipeline)))
            }
            _ => {
                super::transform::admit_optional(&pipeline)
                    .map_err(|message| self.error(node, message))?;
                Ok(Statement::Pipeline(Box::new(pipeline)))
            }
        }
    }
    fn algorithm_projection_is_pipeline(
        &self,
        projection: &ProjectionClause,
        algorithm: GraphAlgorithmKind,
    ) -> bool {
        if projection.distinct {
            return true;
        }
        let mut columns = Vec::new();
        for item in &projection.items {
            let span = item.span.expect("source return item");
            let start = self
                .tokens
                .partition_point(|token| token.span.start < span.start);
            let first = self.tokens[start];
            if matches!(
                first.kind,
                T::Number
                    | T::String
                    | T::Parameter(_)
                    | T::LeftBracket
                    | T::LeftParen
                    | T::Plus
                    | T::Minus
            ) || first.is_keyword(self.input, "CASE")
            {
                return true;
            }
            let name = first.text(self.input).to_ascii_lowercase();
            let category = match name.as_str() {
                "node" => "node",
                "node_id" => "node_id",
                "node_label" => "node_label",
                "node_labels" => "node_labels",
                "rank" | "pagerank_score" if algorithm == GraphAlgorithmKind::PageRank => "score",
                "louvain_id" if algorithm == GraphAlgorithmKind::Louvain => "score",
                "level" if algorithm == GraphAlgorithmKind::Louvain => "level",
                _ => return self.tokens[start + 1].kind == T::LeftParen,
            };
            if columns.len() == 5 {
                return true;
            }
            columns.push(category);
            if item.alias.is_some()
                || !matches!(
                    item.expression.kind,
                    ReturnExpressionKind::Value(ScalarExpression {
                        kind: ScalarExpressionKind::Variable(_),
                        ..
                    })
                )
            {
                return true;
            }
        }
        if !projection.order_by.is_empty()
            || projection.offset.is_some()
            || projection.limit.is_some()
        {
            return true;
        }
        !matches!(
            columns.as_slice(),
            ["node", "score"]
                | ["node", "node_id", "node_label", "score"]
                | ["node", "level", "score"]
                | ["node", "node_id", "node_label", "level", "score"]
        )
    }
}
fn plain_columns(projection: &ProjectionClause) -> Option<Vec<String>> {
    if projection.distinct
        || projection.predicate.is_some()
        || !projection.order_by.is_empty()
        || projection.offset.is_some()
        || projection.limit.is_some()
    {
        return None;
    }
    projection
        .items
        .iter()
        .map(|item| {
            if item.alias.is_some() {
                return None;
            }
            match &item.expression.kind {
                ReturnExpressionKind::Value(ScalarExpression {
                    kind: ScalarExpressionKind::Variable(name),
                    ..
                }) => Some(name.clone()),
                _ => None,
            }
        })
        .collect()
}
