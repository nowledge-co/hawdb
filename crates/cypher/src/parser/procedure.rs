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
    pub(super) fn parse_call_statement(&mut self) -> Result<Statement> {
        let procedure = self.parse_ident()?;
        self.expect_char('(')?;
        let lower = procedure.to_ascii_lowercase();
        if lower == "vector_search" {
            return self.parse_vector_search();
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
            options,
            score_column,
            return_node_identity,
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

    fn parse_vector_search(&mut self) -> Result<Statement> {
        let search = self.parse_vector_search_arguments()?;
        self.skip_ws();
        if self.consume_keyword("YIELD") {
            self.skip_ws();
            let id = self.parse_ident()?;
            self.expect_char(',')?;
            let score = self.parse_ident()?;
            if !id.eq_ignore_ascii_case("id") || !score.eq_ignore_ascii_case("score") {
                return Err(self.error("vector search YIELD must be id, score"));
            }
            self.expect_keyword("MATCH")?;
            let Statement::MatchReturn(mut query) = self.parse_match_statement()? else {
                return Err(self.error("vector search YIELD must feed a MATCH read query"));
            };
            query.vector_seed = Some(search);
            return Ok(Statement::MatchReturn(query));
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
        loop {
            self.skip_ws();
            if self.consume_char('}') {
                break;
            }
            labels.push(self.parse_string()?);
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
        let mut predicates = std::collections::BTreeMap::new();
        loop {
            self.skip_ws();
            if self.consume_char('}') {
                break;
            }
            let rel_type = self.parse_string()?;
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
            if normalized == "dampingfactor" || normalized == "damping" {
                options.damping = Some(self.parse_value()?);
            } else if normalized == "maxiterations" || normalized == "iterations" {
                options.max_iterations = Some(self.parse_value()?);
            } else if matches!(
                normalized.as_str(),
                "maxlevels" | "levels" | "maxphases" | "phases"
            ) {
                options.max_levels = Some(self.parse_value()?);
            } else if normalized == "tolerance" {
                options.tolerance = Some(self.parse_value()?);
            } else if normalized == "normalizeinitial" {
                options.normalize_initial = Some(self.parse_value()?);
            } else if normalized == "resolution" {
                options.resolution = Some(self.parse_value()?);
            } else {
                return Err(self.error("unsupported graph algorithm option"));
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
    let mut parser = Parser::new(input);
    let predicate = parser.parse_property_predicate()?;
    parser.expect_eof()?;
    validate_projected_relationship_predicate(&predicate)?;
    Ok(predicate)
}

fn validate_projected_relationship_predicate(predicate: &PropertyPredicate) -> Result<()> {
    match predicate {
        PropertyPredicate::And(predicates) if !predicates.is_empty() => {
            for predicate in predicates {
                validate_projected_relationship_predicate(predicate)?;
            }
            Ok(())
        }
        PropertyPredicate::Eq {
            variable, value, ..
        } if variable == "r" && matches!(&value.kind, ValueExpressionKind::Literal(_)) => Ok(()),
        PropertyPredicate::Compare {
            variable,
            op: ComparisonOp::Gte,
            value,
            ..
        } if variable == "r" && matches!(&value.kind, ValueExpressionKind::Literal(_)) => Ok(()),
        _ => Err(hawdb_core::HawDBError::Semantic(
            "projected relationship predicates support only literal r.property comparisons joined by AND"
                .to_string(),
        )),
    }
}
