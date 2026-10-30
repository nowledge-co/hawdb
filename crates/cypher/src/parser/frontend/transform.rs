// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use hawdb_core::Result;
use hawdb_sql_syntax::peg::NodeId;

use super::{native::Value, Transformer};
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
                let body = super::super::explain_statement_body(&statement);
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
                        let Value::Hint {
                            hint,
                            depth: native_depth,
                        } = self.tree.take(child)
                        else {
                            unreachable!()
                        };
                        self.check_depth(child, depth, native_depth)?;
                        system_variables.push(hint);
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
            "create" | "alter" | "merge" | "set" | "call" => {
                let Value::Statement {
                    statement,
                    depth: native_depth,
                } = self.tree.take(selected)
                else {
                    unreachable!()
                };
                self.check_depth(selected, depth, native_depth)?;
                Ok(*statement)
            }
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
            let Value::Clause {
                mut clause,
                anonymous: count,
                depth: native_depth,
            } = self.tree.take(child)
            else {
                unreachable!()
            };
            self.check_depth(child, depth, native_depth)?;
            rebase_clause(&mut clause.kind, *anonymous);
            *anonymous += count;
            clauses.push(*clause);
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
    if !super::super::Parser::is_multi_stage_pipeline(pipeline)
        && pipeline.clauses.iter().any(|clause| matches!(&clause.kind,
            ClauseKind::Match { optional: true, patterns, .. } if patterns.iter().any(|pattern|
                pattern.steps.iter().any(|step| step.relationship.min_hops != 1 || step.relationship.max_hops != 1)))) {
        return Err("OPTIONAL MATCH supports only one-hop relationships");
    }
    Ok(())
}

// Deferred names cannot collide with source identifiers: NUL is not legal in
// an identifier. Rebase only fields populated by parse_match_node_pattern;
// explicit __anonN variables and string/property values remain untouched.
fn rebase_name(variable: &mut String, offset: usize) {
    if let Some(index) = variable.strip_prefix("\0anon") {
        *variable = format!(
            "__anon{}",
            offset + index.parse::<usize>().expect("generated ordinal")
        );
    }
}
fn rebase_node(node: &mut NodePattern, offset: usize) {
    if node.anonymous {
        rebase_name(&mut node.variable, offset);
    }
}
fn rebase_pattern(pattern: &mut MatchPattern, offset: usize) {
    rebase_node(&mut pattern.first, offset);
    for step in &mut pattern.steps {
        rebase_node(&mut step.target, offset);
    }
}
fn rebase_predicate(predicate: &mut PropertyPredicate, offset: usize) {
    match predicate {
        PropertyPredicate::And(items) | PropertyPredicate::Or(items) => {
            for item in items {
                rebase_predicate(item, offset);
            }
        }
        PropertyPredicate::Not(inner) => rebase_predicate(inner, offset),
        PropertyPredicate::RelationshipExists { variable, .. } => rebase_name(variable, offset),
        PropertyPredicate::BoundRelationshipExists {
            source_variable,
            target_variable,
            ..
        } => {
            rebase_name(source_variable, offset);
            rebase_name(target_variable, offset);
        }
        _ => {}
    }
}
fn rebase_clause(clause: &mut ClauseKind, offset: usize) {
    match clause {
        ClauseKind::Match {
            patterns,
            predicate,
            ..
        } => {
            for pattern in patterns {
                rebase_pattern(pattern, offset);
            }
            if let Some(predicate) = predicate {
                rebase_predicate(&mut predicate.kind, offset);
            }
        }
        ClauseKind::Create(patterns) => {
            for pattern in patterns {
                rebase_pattern(pattern, offset);
            }
        }
        ClauseKind::Merge { pattern, .. } => rebase_pattern(pattern, offset),
        ClauseKind::With(projection) => {
            if let Some(predicate) = &mut projection.predicate {
                rebase_predicate(&mut predicate.kind, offset);
            }
        }
        _ => {}
    }
}
