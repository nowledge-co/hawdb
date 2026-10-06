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
        let graph_name = self.parse_string()?;
        if lower == "project_graph" {
            self.expect_char(',')?;
            let node_labels = self.parse_string_list()?;
            self.expect_char(',')?;
            let rel_types = self.parse_project_graph_rel_types()?;
            self.skip_procedure_args_tail()?;
            return Ok(Statement::ProjectGraph(ProjectGraph {
                name: graph_name,
                node_labels,
                rel_types,
            }));
        }

        let algorithm = match lower.as_str() {
            "page_rank" | "pagerank" => GraphAlgorithmKind::PageRank,
            "louvain" => GraphAlgorithmKind::Louvain,
            _ => return Err(self.error("unsupported procedure")),
        };
        let options = self.parse_graph_algorithm_options()?;
        let score_column = self.parse_algorithm_return_clause(algorithm)?;
        Ok(Statement::GraphAlgorithm(GraphAlgorithm {
            algorithm,
            graph_name,
            options,
            score_column,
        }))
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

    pub(super) fn parse_project_graph_rel_types(&mut self) -> Result<Vec<String>> {
        self.skip_ws();
        if self.peek_char() == Some('[') {
            return self.parse_string_list();
        }
        self.expect_char('{')?;
        let mut values = Vec::new();
        loop {
            self.skip_ws();
            if self.consume_char('}') {
                break;
            }
            values.push(self.parse_string()?);
            self.expect_char(':')?;
            self.skip_procedure_option_value()?;
            if self.consume_separator_or_end(',', '}')? {
                break;
            }
        }
        Ok(values)
    }

    pub(super) fn parse_graph_algorithm_options(&mut self) -> Result<GraphAlgorithmOptions> {
        let mut options = GraphAlgorithmOptions {
            damping: None,
            max_iterations: None,
            max_levels: None,
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
            if normalized == "dampingfactor" || normalized == "damping" {
                options.damping = Some(self.parse_value()?);
            } else if normalized == "maxiterations" || normalized == "iterations" {
                options.max_iterations = Some(self.parse_value()?);
            } else if normalized == "maxlevels" || normalized == "levels" {
                options.max_levels = Some(self.parse_value()?);
            } else {
                self.skip_procedure_option_value()?;
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
    ) -> Result<String> {
        if !self.consume_keyword("RETURN") {
            return Ok(match algorithm {
                GraphAlgorithmKind::PageRank => "pagerank_score".to_string(),
                GraphAlgorithmKind::Louvain => "louvain_id".to_string(),
            });
        }
        self.skip_ws();
        let first = self.parse_ident()?;
        if !first.eq_ignore_ascii_case("node") {
            return Err(self.error("expected node in procedure RETURN"));
        }
        self.expect_char(',')?;
        let mut second = self.parse_ident()?;
        self.skip_ws();
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
            return Ok(second);
        }
        if !second.eq_ignore_ascii_case(expected) {
            return Err(self.error("unexpected procedure RETURN column"));
        }
        Ok(second)
    }
}
