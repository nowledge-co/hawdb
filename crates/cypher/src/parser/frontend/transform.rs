// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use hawdb_core::Result;
use hawdb_sql_syntax::peg::NodeId;

use super::Transformer;
use crate::*;

impl Transformer<'_> {
    pub(super) fn statement(&mut self, node: NodeId, depth: usize) -> Result<Statement> {
        self.check_depth(node, depth, 0)?;
        let selected = self.tree.children(node)[0];
        match self.tree.nodes[selected]
            .name
            .expect("named statement rule")
        {
            "explain" => {
                let analyze = self.tree.child(selected, "analyze").is_some();
                let child = self.tree.required(selected, "statement");
                let statement = self.statement(child, depth + 1)?;
                let body = explain_statement_body(&statement);
                if control_statement(body) {
                    return Err(
                        self.error(selected, "EXPLAIN requires a query or mutation statement")
                    );
                }
                Ok(Statement::Explain(Box::new(Explain { analyze, statement })))
            }
            "cypher" => {
                let children = self.tree.children(selected).to_vec();
                let mut system_variables = Vec::new();
                for child in children {
                    if self.tree.nodes[child].name == Some("hint") {
                        system_variables.push(self.system_assignment(child, depth)?);
                    }
                }
                let child = self.tree.required(selected, "statement");
                let statement = self.statement(child, depth + 1)?;
                if control_statement(&statement) {
                    return Err(self.error(
                        selected,
                        "CYPHER system hints require a query or mutation statement",
                    ));
                }
                Ok(Statement::CypherQuery(Box::new(CypherQuery {
                    system_variables,
                    statement,
                })))
            }
            name @ ("multi_stage" | "match" | "unwind") => {
                let pipeline = self.tree.required(selected, "pipeline");
                let pipeline = self.pipeline(pipeline, depth, &mut 0)?;
                if name == "match" {
                    admit_optional(&pipeline).map_err(|message| self.error(selected, message))?;
                }
                if name == "unwind" {
                    Ok(Statement::UnwindMutation(Box::new(pipeline)))
                } else {
                    Ok(Statement::Pipeline(Box::new(pipeline)))
                }
            }
            "begin" => Ok(Statement::BeginTransaction),
            "checkpoint" => Ok(Statement::Checkpoint),
            "commit" => Ok(Statement::Commit),
            "rollback" => Ok(Statement::Rollback),
            "alter" => self.schema_statement(self.tree.required(selected, "schema_alter")),
            "create" if self.tree.child(selected, "schema_create").is_some() => {
                self.schema_statement(self.tree.required(selected, "schema_create"))
            }
            "create" => {
                self.data_statement(self.tree.required(selected, "d_create_data"), false, depth)
            }
            "merge" => {
                self.data_statement(self.tree.required(selected, "d_merge_data"), true, depth)
            }
            "set" => self
                .system_assignment(selected, depth)
                .map(Statement::SetSystemVariable),
            "call" => self.call_statement(selected, depth),
            _ => unreachable!("known statement grammar rule"),
        }
    }

    pub(super) fn pipeline(
        &mut self,
        node: NodeId,
        depth: usize,
        anonymous: &mut usize,
    ) -> Result<QueryPipeline> {
        let mut nodes = Vec::new();
        self.clause_nodes(node, &mut nodes);
        let mut clauses = Vec::with_capacity(nodes.len());
        for child in nodes {
            let name = self.tree.nodes[child].name.expect("clause rule");
            if name == "call_clause" {
                clauses.push(self.procedure_clause(child, depth)?);
                continue;
            }
            if matches!(name, "with_clause" | "return_clause") {
                clauses.push(self.projection_clause(child, depth, anonymous)?);
            } else {
                clauses.push(self.graph_clause(child, depth, anonymous)?);
            }
        }
        if !matches!(
            clauses.last().map(|clause| &clause.kind),
            Some(
                ClauseKind::Return(_)
                    | ClauseKind::Call { .. }
                    | ClauseKind::Create(_)
                    | ClauseKind::Merge { .. }
                    | ClauseKind::Set(_)
                    | ClauseKind::Delete { .. }
            )
        ) {
            return Err(self.error(node, "query requires RETURN or a concluding mutation"));
        }
        Ok(AstNode::from_source(
            QueryPipelineKind { clauses },
            SourceSpan {
                start: self.tree.span(node, self.tokens).start,
                end: self.tree.span(node, self.tokens).end,
            },
        ))
    }

    fn clause_nodes(&self, node: NodeId, output: &mut Vec<NodeId>) {
        if self.tree.nodes[node]
            .name
            .is_some_and(|name| name.ends_with("_clause"))
        {
            output.push(node);
        } else {
            for child in self.tree.children(node) {
                self.clause_nodes(*child, output);
            }
        }
    }
}

fn control_statement(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::BeginTransaction
            | Statement::Checkpoint
            | Statement::Commit
            | Statement::CypherQuery(_)
            | Statement::Explain(_)
            | Statement::Rollback
            | Statement::SetSystemVariable(_)
    )
}

pub(super) fn admit_optional(pipeline: &QueryPipeline) -> std::result::Result<(), &'static str> {
    if pipeline
        .clauses
        .iter()
        .filter(|clause| matches!(clause.kind, ClauseKind::With(_)))
        .count()
        < 2
        && pipeline.clauses.iter().any(|clause| {
            matches!(&clause.kind,
            ClauseKind::Match { optional: true, patterns, .. } if patterns.iter().any(|pattern|
                pattern.steps.iter().any(|step| !step.relationship.search.is_shortest()
                    && (step.relationship.min_hops != 1 || step.relationship.max_hops != 1))))
        })
    {
        return Err("OPTIONAL MATCH supports only one-hop relationships");
    }
    Ok(())
}

fn explain_statement_body(statement: &Statement) -> &Statement {
    match statement {
        Statement::CypherQuery(query) => &query.statement,
        _ => statement,
    }
}
