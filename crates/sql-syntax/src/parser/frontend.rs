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

use std::sync::OnceLock;

use crate::peg::{Grammar, Limits, ParseTree};
use crate::{tokenize, SyntaxError, Token};

pub(crate) mod grammar;
mod transform;

pub(super) fn parse(input: &str, root: &'static str) -> Result<Output, SyntaxError> {
    static GRAMMAR: OnceLock<Grammar> = OnceLock::new();
    let grammar = GRAMMAR.get_or_init(|| grammar::build().expect("valid built-in SQL/PGQ grammar"));
    let tokens = tokenize(input)?;
    let tree = grammar.parse(
        input,
        &tokens,
        root,
        Limits {
            depth: 8192,
            ..Limits::default()
        },
    )?;
    crate::peg::validate_sql_expression_depth(&tree, &tokens)?;
    let mut pending = vec![tree.root];
    while let Some(id) = pending.pop() {
        let node = &tree.nodes[id];
        pending.extend_from_slice(tree.children(id));
        if node.name == Some("GraphQuantifier") {
            for child in tree.children(id) {
                let token = tokens[tree.nodes[*child].start];
                if token.text(input).parse::<u32>().is_err() {
                    return Err(SyntaxError::new(
                        crate::SyntaxErrorCode::UnexpectedToken,
                        token.span,
                    )
                    .expected("a non-negative integer")
                    .found(token.text(input)));
                }
            }
        }
    }
    let output = Transformer {
        input,
        tree: &tree,
        tokens: &tokens,
    }
    .output(root);
    validate_subquery_placement(&output)?;
    Ok(output)
}

fn validate_subquery_placement(output: &Output) -> Result<(), SyntaxError> {
    match output {
        Output::Explain { query, .. } | Output::Query(query) => validate_query(query),
        Output::Select(select) => validate_select(select),
        Output::Create(create) => validate_create_property_graph(create),
        Output::GraphTable(table) => validate_graph_table(table),
    }
}

fn validate_query(query: &crate::PostgresQuerySyntax) -> Result<(), SyntaxError> {
    for cte in &query.with {
        validate_query(&cte.query)?;
    }
    validate_query_body(&query.body)?;
    for order in &query.order_by {
        reject_subquery(&order.expression)?;
    }
    if let Some(limit) = &query.limit {
        reject_subquery(limit)?;
    }
    if let Some(offset) = &query.offset {
        reject_subquery(offset)?;
    }
    Ok(())
}

fn validate_query_body(body: &crate::PostgresQueryBodySyntax) -> Result<(), SyntaxError> {
    match body {
        crate::PostgresQueryBodySyntax::Select(select) => validate_select(select),
        crate::PostgresQueryBodySyntax::Values { rows, .. } => {
            for expression in rows.iter().flatten() {
                reject_subquery(expression)?;
            }
            Ok(())
        }
        crate::PostgresQueryBodySyntax::Nested(query) => validate_query(query),
        crate::PostgresQueryBodySyntax::SetOperation { left, right, .. } => {
            validate_query_body(left)?;
            validate_query_body(right)
        }
    }
}

fn validate_select(select: &crate::PostgresSelectSyntax) -> Result<(), SyntaxError> {
    for projection in &select.projection {
        reject_subquery(&projection.expression)?;
    }
    for from in &select.from {
        validate_from_item(&from.relation)?;
        for join in &from.joins {
            validate_from_item(&join.relation)?;
            if let Some(condition) = &join.condition {
                reject_subquery(condition)?;
            }
        }
    }
    for expression in &select.group_by {
        reject_subquery(expression)?;
    }
    if let Some(having) = &select.having {
        reject_subquery(having)?;
    }
    for order in &select.order_by {
        reject_subquery(&order.expression)?;
    }
    if let Some(limit) = &select.limit {
        reject_subquery(limit)?;
    }
    if let Some(offset) = &select.offset {
        reject_subquery(offset)?;
    }
    Ok(())
}

fn validate_from_item(item: &crate::PostgresFromItemSyntax) -> Result<(), SyntaxError> {
    match item {
        crate::PostgresFromItemSyntax::Relation(relation) => {
            if let Some(arguments) = &relation.arguments {
                for argument in arguments {
                    reject_subquery(&argument.expression)?;
                }
            }
            Ok(())
        }
        crate::PostgresFromItemSyntax::Derived(derived) => validate_query(&derived.query),
        crate::PostgresFromItemSyntax::GraphTable(table) => validate_graph_table(table),
    }
}

fn validate_create_property_graph(graph: &crate::CreatePropertyGraph) -> Result<(), SyntaxError> {
    for exposure in graph
        .vertex_tables
        .iter()
        .filter_map(|table| table.exposure.as_ref())
        .chain(
            graph
                .edge_tables
                .iter()
                .filter_map(|table| table.exposure.as_ref()),
        )
    {
        for label in &exposure.labels {
            if let crate::PropertyExposure::Expressions(expressions) = &label.properties {
                for expression in expressions {
                    reject_subquery(&expression.expression)?;
                }
            }
        }
    }
    Ok(())
}

fn validate_graph_table(table: &crate::GraphTable) -> Result<(), SyntaxError> {
    validate_graph_pattern(&table.pattern)?;
    for column in &table.columns {
        reject_subquery(&column.expression)?;
    }
    Ok(())
}

fn validate_graph_pattern(pattern: &crate::GraphPatternSyntax) -> Result<(), SyntaxError> {
    if let Some(predicate) = &pattern.predicate {
        reject_subquery(predicate)?;
    }
    for path in &pattern.paths {
        validate_graph_path(path)?;
    }
    Ok(())
}

fn validate_graph_path(path: &crate::GraphPathSyntax) -> Result<(), SyntaxError> {
    for factor in &path.factors {
        match &factor.primary {
            crate::GraphPathPrimarySyntax::Vertex(vertex) => {
                if let Some(predicate) = &vertex.predicate {
                    reject_subquery(predicate)?;
                }
            }
            crate::GraphPathPrimarySyntax::Edge(edge) => {
                if let Some(predicate) = &edge.predicate {
                    reject_subquery(predicate)?;
                }
            }
            crate::GraphPathPrimarySyntax::Parenthesized {
                path, predicate, ..
            } => {
                validate_graph_path(path)?;
                if let Some(predicate) = predicate {
                    reject_subquery(predicate)?;
                }
            }
        }
    }
    Ok(())
}

fn reject_subquery(expression: &crate::ExpressionSyntax) -> Result<(), SyntaxError> {
    if let Some(span) = first_subquery_span(expression) {
        return Err(
            SyntaxError::new(crate::SyntaxErrorCode::UnexpectedToken, span)
                .expected("a scalar expression")
                .found("subquery"),
        );
    }
    Ok(())
}

fn first_subquery_span(expression: &crate::ExpressionSyntax) -> Option<crate::Span> {
    use crate::ExpressionKindSyntax as E;
    match &expression.kind {
        E::Exists { .. } | E::InSubquery { .. } => Some(expression.span),
        E::Unary { expression, .. }
        | E::IsNull { expression, .. }
        | E::Cast { expression, .. }
        | E::Collate { expression, .. }
        | E::Parenthesized(expression) => first_subquery_span(expression),
        E::Binary { left, right, .. } => {
            first_subquery_span(left).or_else(|| first_subquery_span(right))
        }
        E::Like {
            expression,
            pattern,
            escape,
            ..
        } => first_subquery_span(expression)
            .or_else(|| first_subquery_span(pattern))
            .or_else(|| escape.as_deref().and_then(first_subquery_span)),
        E::Filtered {
            function,
            predicate,
        } => first_subquery_span(function).or_else(|| first_subquery_span(predicate)),
        E::InList {
            expression, values, ..
        } => {
            first_subquery_span(expression).or_else(|| values.iter().find_map(first_subquery_span))
        }
        E::Row(fields) => fields.iter().find_map(first_subquery_span),
        E::Between {
            expression,
            low,
            high,
            ..
        } => first_subquery_span(expression)
            .or_else(|| first_subquery_span(low))
            .or_else(|| first_subquery_span(high)),
        E::Function { arguments, .. } => arguments.iter().find_map(first_subquery_span),
        E::Column(_) | E::Wildcard(_) | E::Parameter(_) | E::Literal(_) | E::TypedString { .. } => {
            None
        }
    }
}

struct Transformer<'a> {
    input: &'a str,
    tree: &'a ParseTree,
    tokens: &'a [Token],
}

pub(super) enum Output {
    Explain {
        analyze: bool,
        query: Box<crate::PostgresQuerySyntax>,
    },
    Select(Box<crate::PostgresSelectSyntax>),
    Query(Box<crate::PostgresQuerySyntax>),
    Create(Box<crate::CreatePropertyGraph>),
    GraphTable(Box<crate::GraphTable>),
}

#[cfg(test)]
mod tests;

pub(crate) fn select_from_tree(
    input: &str,
    tokens: &[Token],
    tree: &ParseTree,
    node: crate::peg::NodeId,
) -> crate::PostgresSelectSyntax {
    Transformer {
        input,
        tokens,
        tree,
    }
    .select(node)
}

pub(crate) fn query_from_tree(
    input: &str,
    tokens: &[Token],
    tree: &ParseTree,
    node: crate::peg::NodeId,
) -> crate::PostgresQuerySyntax {
    Transformer {
        input,
        tokens,
        tree,
    }
    .query(node)
}
