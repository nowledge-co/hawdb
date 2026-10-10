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

use hawdb_core::Result;

use super::super::ast::*;
use super::Parser;

const MAX_PROJECTED_RELATIONSHIP_PREDICATE_BYTES: usize = 16 * 1024;
const MAX_PROJECTED_RELATIONSHIP_PREDICATE_CONJUNCTS: usize = 16;

impl Parser<'_> {
    pub(super) fn parse_call_statement(&mut self, call_start: usize) -> Result<Statement> {
        self.skip_ws();
        let procedure_start = self.pos;
        let procedure = self.parse_ident()?;
        self.expect_char('(')?;
        let lower = procedure.to_ascii_lowercase();
        if lower == "vector_search" {
            return self.parse_vector_search(call_start, procedure_start);
        }
        if lower == "text_search" {
            return self.parse_text_search(call_start, procedure_start);
        }
        if lower == "graph_seed_search" {
            return self.parse_graph_seed_search(call_start, procedure_start);
        }
        let graph_name = self.parse_string()?;
        if lower == "project_graph" {
            self.expect_char(',')?;
            let node_labels = self.parse_project_graph_node_labels()?;
            self.expect_char(',')?;
            let (rel_types, relationship_predicates) = self.parse_project_graph_rel_types()?;
            self.skip_procedure_args_tail()?;
            return Ok(Statement::ProjectGraph(ProjectGraph {
                name: graph_name,
                node_labels,
                rel_types,
                relationship_predicates,
            }));
        }

        let algorithm = match lower.as_str() {
            "page_rank" | "pagerank" => GraphAlgorithmKind::PageRank,
            "louvain" => GraphAlgorithmKind::Louvain,
            _ => return Err(self.error("unsupported procedure")),
        };
        let options = self.parse_graph_algorithm_options()?;
        let (score_column, return_node_identity) = self.parse_algorithm_return_clause(algorithm)?;
        Ok(Statement::GraphAlgorithm(GraphAlgorithm {
            algorithm,
            graph_name,
            options: Box::new(options),
            score_column,
            return_node_identity,
        }))
    }

    pub(super) fn parse_graph_seed_search_arguments(&mut self) -> Result<GraphSeedSearch> {
        let query = self.parse_value()?;
        let mut label = None;
        let mut top_k = None;
        loop {
            self.skip_ws();
            if self.consume_char(')') {
                break;
            }
            self.expect_char(',')?;
            let name = self.parse_ident()?;
            self.expect_token(":=")?;
            let slot = match name.to_ascii_lowercase().as_str() {
                "label" => &mut label,
                "topk" | "limit" => &mut top_k,
                _ => return Err(self.error("unsupported graph seed search option")),
            };
            if slot.is_some() {
                return Err(self.error("duplicate graph seed search option"));
            }
            *slot = Some(self.parse_value()?);
        }
        Ok(GraphSeedSearch {
            query,
            label: label.ok_or_else(|| self.error("graph seed search requires label"))?,
            top_k,
        })
    }

    fn parse_graph_seed_search(
        &mut self,
        call_start: usize,
        procedure_start: usize,
    ) -> Result<Statement> {
        let search = self.parse_graph_seed_search_arguments()?;
        let procedure =
            self.source_node(ProcedureCallKind::GraphSeedSearch(search), procedure_start);
        let yields = self.parse_procedure_yields()?;
        let call = self.source_node(ClauseKind::Call { procedure, yields }, call_start);
        self.skip_ws();
        let mut clauses = vec![call];
        if self.peek_char().is_some_and(|ch| ch != ';') {
            clauses.extend(self.parse_public_query_pipeline()?.kind.clauses);
        }
        Ok(Statement::Pipeline(Box::new(
            self.source_node(QueryPipelineKind { clauses }, call_start),
        )))
    }

    pub(super) fn parse_text_search_arguments(&mut self) -> Result<TextSearch> {
        let query = self.parse_value()?;
        let mut top_k = None;
        loop {
            self.skip_ws();
            if self.consume_char(')') {
                break;
            }
            self.expect_char(',')?;
            let name = self.parse_ident()?;
            self.expect_token(":=")?;
            if !name.eq_ignore_ascii_case("topK") && !name.eq_ignore_ascii_case("limit") {
                return Err(self.error("unsupported text search option"));
            }
            if top_k.is_some() {
                return Err(self.error("duplicate text search topK option"));
            }
            top_k = Some(self.parse_value()?);
        }
        Ok(TextSearch { query, top_k })
    }

    fn parse_text_search(
        &mut self,
        call_start: usize,
        procedure_start: usize,
    ) -> Result<Statement> {
        let search = self.parse_text_search_arguments()?;
        let procedure = self.source_node(ProcedureCallKind::TextSearch(search), procedure_start);
        let yields = self.parse_procedure_yields()?;
        let call = self.source_node(ClauseKind::Call { procedure, yields }, call_start);
        self.skip_ws();
        let mut clauses = vec![call];
        if self.peek_char().is_some_and(|ch| ch != ';') {
            clauses.extend(self.parse_public_query_pipeline()?.kind.clauses);
        }
        Ok(Statement::Pipeline(Box::new(
            self.source_node(QueryPipelineKind { clauses }, call_start),
        )))
    }

    pub(super) fn parse_vector_search_arguments(&mut self) -> Result<VectorSearch> {
        let embedding = self.parse_value()?;
        let mut top_k = None;
        loop {
            self.skip_ws();
            if self.consume_char(')') {
                break;
            }
            self.expect_char(',')?;
            let name = self.parse_ident()?;
            self.expect_token(":=")?;
            if name.eq_ignore_ascii_case("topK") || name.eq_ignore_ascii_case("limit") {
                if top_k.is_some() {
                    return Err(self.error("duplicate vector search topK option"));
                }
                top_k = Some(self.parse_value()?);
            } else {
                return Err(self.error("unsupported vector search option"));
            }
        }
        Ok(VectorSearch { embedding, top_k })
    }

    fn parse_vector_search(
        &mut self,
        call_start: usize,
        procedure_start: usize,
    ) -> Result<Statement> {
        let search = self.parse_vector_search_arguments()?;
        let procedure_span = SourceSpan {
            start: procedure_start,
            end: self.pos,
        };
        self.skip_ws();
        if self.consume_keyword("YIELD") {
            let procedure =
                AstNode::from_source(ProcedureCallKind::VectorSearch(search), procedure_span);
            let mut yields = Vec::new();
            for name in ["id", "score"] {
                self.skip_ws();
                let start = self.pos;
                let parsed = self.parse_ident()?;
                if !parsed.eq_ignore_ascii_case(name) {
                    return Err(self.error("vector search YIELD must be id, score"));
                }
                yields.push(self.source_node(
                    YieldItemKind {
                        name: parsed,
                        alias: None,
                    },
                    start,
                ));
                if name == "id" {
                    self.expect_char(',')?;
                }
            }
            let call = self.source_node(ClauseKind::Call { procedure, yields }, call_start);
            if !self.next_keyword_is("MATCH") {
                return Err(self.error("vector search YIELD must feed a MATCH read query"));
            }
            let mut query = self.parse_public_query_pipeline()?;
            if !matches!(
                query.clauses.last().map(|clause| &clause.kind),
                Some(ClauseKind::Return(_))
            ) || !query.clauses.iter().all(|clause| {
                matches!(
                    clause.kind,
                    ClauseKind::Match { .. } | ClauseKind::With(_) | ClauseKind::Return(_)
                )
            }) {
                return Err(self.error("vector search YIELD must feed a MATCH read query"));
            }
            query.clauses.insert(0, call);
            return Ok(Statement::Pipeline(Box::new(
                self.source_node(query.kind, call_start),
            )));
        }
        if self.consume_keyword("RETURN") {
            self.skip_ws();
            let id = self.parse_ident()?;
            self.expect_char(',')?;
            let score = self.parse_ident()?;
            if !id.eq_ignore_ascii_case("id") || !score.eq_ignore_ascii_case("score") {
                return Err(self.error("vector search RETURN must be id, score"));
            }
        }
        Ok(Statement::VectorSearch(search))
    }

    pub(super) fn parse_string_list(&mut self) -> Result<Vec<String>> {
        self.expect_char('[')?;
        let mut values = Vec::new();
        loop {
            self.skip_ws();
            if self.consume_char(']') {
                break;
            }
            values.push(self.parse_string()?);
            if self.consume_separator_or_end(',', ']')? {
                break;
            }
        }
        Ok(values)
    }

    pub(super) fn parse_project_graph_node_labels(&mut self) -> Result<Vec<String>> {
        self.skip_ws();
        if self.peek_char() == Some('[') {
            return self.parse_string_list();
        }
        self.expect_char('{')?;
        let mut labels = Vec::new();
        let mut seen_labels = std::collections::BTreeSet::new();
        loop {
            self.skip_ws();
            if self.consume_char('}') {
                break;
            }
            let label = self.parse_string()?;
            if !seen_labels.insert(label.clone()) {
                return Err(self.error("duplicate projected node label map key"));
            }
            labels.push(label);
            self.expect_char(':')?;
            self.skip_ws();
            let predicate = self.parse_string()?;
            if !predicate.trim().is_empty() {
                return Err(self.error("projected node predicates are not supported"));
            }
            if self.consume_separator_or_end(',', '}')? {
                break;
            }
        }
        Ok(labels)
    }

    pub(super) fn parse_project_graph_rel_types(
        &mut self,
    ) -> Result<(
        Vec<String>,
        std::collections::BTreeMap<String, PropertyPredicate>,
    )> {
        self.skip_ws();
        if self.peek_char() == Some('[') {
            return self
                .parse_string_list()
                .map(|rel_types| (rel_types, std::collections::BTreeMap::new()));
        }
        self.expect_char('{')?;
        let mut values = Vec::new();
        let mut seen_types = std::collections::BTreeSet::new();
        let mut predicates = std::collections::BTreeMap::new();
        loop {
            self.skip_ws();
            if self.consume_char('}') {
                break;
            }
            let rel_type = self.parse_string()?;
            if !seen_types.insert(rel_type.clone()) {
                return Err(self.error("duplicate projected relationship type map key"));
            }
            self.expect_char(':')?;
            self.skip_ws();
            let filter = self.parse_string()?;
            if !filter.trim().is_empty() {
                let predicate = parse_projected_relationship_predicate(&filter)?;
                predicates.insert(rel_type.clone(), predicate);
            }
            values.push(rel_type);
            if self.consume_separator_or_end(',', '}')? {
                break;
            }
        }
        Ok((values, predicates))
    }

    pub(super) fn parse_graph_algorithm_options(&mut self) -> Result<GraphAlgorithmOptions> {
        let mut options = GraphAlgorithmOptions {
            damping: None,
            max_iterations: None,
            max_levels: None,
            max_phases: None,
            tolerance: None,
            normalize_initial: None,
            resolution: None,
        };
        loop {
            self.skip_ws();
            if self.consume_char(')') {
                break;
            }
            self.expect_char(',')?;
            self.skip_ws();
            let name = self.parse_ident()?;
            self.skip_ws();
            self.expect_token(":=")?;
            let normalized = name.to_ascii_lowercase();
            let option = match normalized.as_str() {
                "dampingfactor" | "damping" => &mut options.damping,
                "maxiterations" | "iterations" => &mut options.max_iterations,
                "maxlevels" | "levels" => &mut options.max_levels,
                "maxphases" | "phases" => &mut options.max_phases,
                "tolerance" => &mut options.tolerance,
                "normalizeinitial" => &mut options.normalize_initial,
                "resolution" => &mut options.resolution,
                _ => return Err(self.error("unsupported graph algorithm option")),
            };
            if option.is_some() {
                return Err(self.error("duplicate graph algorithm option"));
            }
            *option = Some(self.parse_value()?);
            if options.max_levels.is_some() && options.max_phases.is_some() {
                return Err(self.error("duplicate graph algorithm option: maxLevels and maxPhases"));
            }
        }
        Ok(options)
    }

    pub(super) fn skip_procedure_args_tail(&mut self) -> Result<()> {
        loop {
            self.skip_ws();
            if self.consume_char(')') {
                return Ok(());
            }
            self.expect_char(',')?;
            self.skip_procedure_option_value()?;
        }
    }

    pub(super) fn skip_procedure_option_value(&mut self) -> Result<()> {
        self.skip_ws();
        match self.peek_char() {
            Some('\'') | Some('"') => {
                self.parse_string()?;
            }
            Some('[') => {
                self.parse_string_list()?;
            }
            Some('{') => {
                self.skip_procedure_map_value()?;
            }
            Some(ch) if ch.is_ascii_digit() || ch == '-' => {
                self.parse_float()?;
            }
            Some('$') => {
                self.parse_value()?;
            }
            _ => {
                self.parse_ident()?;
            }
        }
        Ok(())
    }

    pub(super) fn skip_procedure_map_value(&mut self) -> Result<()> {
        self.expect_char('{')?;
        loop {
            self.skip_ws();
            if self.consume_char('}') {
                break;
            }
            if matches!(self.peek_char(), Some('\'') | Some('"')) {
                self.parse_string()?;
            } else {
                self.parse_ident()?;
            }
            self.expect_char(':')?;
            self.skip_procedure_option_value()?;
            if self.consume_separator_or_end(',', '}')? {
                break;
            }
        }
        Ok(())
    }

    pub(super) fn parse_algorithm_return_clause(
        &mut self,
        algorithm: GraphAlgorithmKind,
    ) -> Result<(String, bool)> {
        if !self.consume_keyword("RETURN") {
            return Ok((
                match algorithm {
                    GraphAlgorithmKind::PageRank => "pagerank_score".to_string(),
                    GraphAlgorithmKind::Louvain => "louvain_id".to_string(),
                },
                false,
            ));
        }
        self.skip_ws();
        let first = self.parse_ident()?;
        if !first.eq_ignore_ascii_case("node") {
            return Err(self.error("expected node in procedure RETURN"));
        }
        self.expect_char(',')?;
        let mut second = self.parse_ident()?;
        self.skip_ws();
        let return_node_identity = second.eq_ignore_ascii_case("node_id");
        if return_node_identity {
            self.expect_char(',')?;
            self.skip_ws();
            let label = self.parse_ident()?;
            if !label.eq_ignore_ascii_case("node_label") {
                return Err(self.error("expected node_label after node_id"));
            }
            self.expect_char(',')?;
            self.skip_ws();
            second = self.parse_ident()?;
            self.skip_ws();
        }
        if matches!(algorithm, GraphAlgorithmKind::Louvain)
            && second.eq_ignore_ascii_case("level")
            && self.consume_char(',')
        {
            self.skip_ws();
            second = self.parse_ident()?;
        }
        let expected = match algorithm {
            GraphAlgorithmKind::PageRank => "pagerank_score",
            GraphAlgorithmKind::Louvain => "louvain_id",
        };
        if matches!(algorithm, GraphAlgorithmKind::PageRank) && second.eq_ignore_ascii_case("rank")
        {
            return Ok((second, return_node_identity));
        }
        if !second.eq_ignore_ascii_case(expected) {
            return Err(self.error("unexpected procedure RETURN column"));
        }
        Ok((second, return_node_identity))
    }
}

fn parse_projected_relationship_predicate(input: &str) -> Result<PropertyPredicate> {
    if input.len() > MAX_PROJECTED_RELATIONSHIP_PREDICATE_BYTES {
        return Err(hawdb_core::HawDBError::Semantic(format!(
            "projected relationship predicate exceeds limit of {MAX_PROJECTED_RELATIONSHIP_PREDICATE_BYTES} UTF-8 bytes"
        )));
    }
    let mut parser = Parser::new(input);
    let predicate = parser.parse_predicate(false)?;
    parser.expect_eof()?;
    validate_projected_relationship_predicate(&predicate)?;
    Ok(predicate)
}

fn validate_projected_relationship_predicate(predicate: &PropertyPredicate) -> Result<usize> {
    match predicate {
        PropertyPredicate::And(predicates) if !predicates.is_empty() => {
            let mut conjuncts = 0;
            for predicate in predicates {
                conjuncts += validate_projected_relationship_predicate(predicate)?;
                if conjuncts > MAX_PROJECTED_RELATIONSHIP_PREDICATE_CONJUNCTS {
                    return Err(hawdb_core::HawDBError::Semantic(format!(
                        "projected relationship predicate exceeds limit of {MAX_PROJECTED_RELATIONSHIP_PREDICATE_CONJUNCTS} conjuncts"
                    )));
                }
            }
            Ok(conjuncts)
        }
        PropertyPredicate::Eq {
            variable, value, ..
        } if variable == "r" && matches!(&value.kind, ValueExpressionKind::Literal(_)) => Ok(1),
        PropertyPredicate::Compare {
            variable,
            op: ComparisonOp::Gte,
            value,
            ..
        } if variable == "r" && matches!(&value.kind, ValueExpressionKind::Literal(_)) => Ok(1),
        _ => Err(hawdb_core::HawDBError::Semantic(
            "projected relationship predicates support only literal r.property comparisons joined by AND"
                .to_string(),
        )),
    }
}
