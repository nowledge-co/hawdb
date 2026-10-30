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

//! Bounded native terminals preserve Pratt expressions and graph subgrammars.

use super::super::Parser;
use crate::peg::NativeMatch;
use crate::*;

pub(super) enum Value {
    Projection(Vec<LabeledExpressionSyntax>),
    Expression(Box<ExpressionSyntax>),
    Expressions(Vec<ExpressionSyntax>),
    OrderItems(Vec<OrderBySyntax>),
    Relation(RelationTableSyntax),
    Name(QualifiedName),
    Alias(Option<TableAlias>),
    Pattern(GraphPatternSyntax),
    Columns(Vec<GraphTableColumn>),
    Vertices(Vec<VertexTableDefinition>),
    Edges(Vec<EdgeTableDefinition>),
}

fn run(
    input: &str,
    tokens: &[Token],
    start: usize,
    parse: impl FnOnce(&mut Parser<'_>) -> Result<Value, SyntaxError>,
) -> NativeMatch<Value> {
    let mut parser = Parser::new(input, tokens);
    parser.position = start;
    let value = parse(&mut parser);
    NativeMatch {
        end: parser.position,
        value,
    }
}

pub(super) fn relational_projection(
    input: &str,
    tokens: &[Token],
    start: usize,
) -> NativeMatch<Value> {
    run(input, tokens, start, |parser| {
        let mut items = Vec::new();
        if parser.at_keyword("FROM") {
            return Ok(Value::Projection(items));
        }
        loop {
            let begin = parser.position;
            if parser.at_select_clause_boundary() && parser.current().kind == TokenKind::Word {
                parser.advance();
            }
            let end = parser.scan_select_expression_end(
                &[
                    "FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "FOR", "FETCH",
                ],
                true,
            )?;
            let item = match parser.labeled_expression_from_range(begin, end) {
                Ok(item) => item,
                Err(error)
                    if matches!(
                        error.code,
                        SyntaxErrorCode::UnexpectedToken | SyntaxErrorCode::UnexpectedEnd
                    ) && end > begin + 1
                        && super::super::is_identifier_kind(tokens[end - 1].kind) =>
                {
                    let mut item = parser.labeled_expression_from_range(begin, end - 1)?;
                    if item.alias.is_some() {
                        return Err(error);
                    }
                    item.alias = Some(super::super::identifier_from_token(tokens[end - 1]));
                    item.span.end = tokens[end - 1].span.end;
                    item
                }
                Err(error) => return Err(error),
            };
            items.push(item);
            if !parser.consume_kind(TokenKind::Comma) {
                break;
            }
        }
        Ok(Value::Projection(items))
    })
}

pub(super) fn projection(input: &str, tokens: &[Token], start: usize) -> NativeMatch<Value> {
    run(input, tokens, start, |parser| {
        parser.parse_select_projection().map(Value::Projection)
    })
}

pub(super) fn relation(input: &str, tokens: &[Token], start: usize) -> NativeMatch<Value> {
    run(input, tokens, start, |parser| {
        // An empty FROM list must not consume a clause keyword as a table name.
        if parser.at_select_clause_boundary() {
            return Err(parser.unexpected("at least one FROM item"));
        }
        let begin = parser.current().span.start;
        let name = parser.parse_qualified_name()?;
        let alias = parser.parse_optional_table_alias()?;
        Ok(Value::Relation(RelationTableSyntax {
            name,
            arguments: None,
            alias,
            span: Span::new(begin, parser.previous_end()),
        }))
    })
}

pub(super) fn relational_relation(
    input: &str,
    tokens: &[Token],
    start: usize,
) -> NativeMatch<Value> {
    run(input, tokens, start, |parser| {
        if parser.at_select_clause_boundary() {
            return Err(parser.unexpected("at least one FROM item"));
        }
        let begin = parser.current().span.start;
        let name = parser.parse_qualified_name()?;
        let arguments = if parser.consume_kind(TokenKind::LeftParen) {
            let mut arguments = Vec::new();
            if parser.current().kind != TokenKind::RightParen {
                loop {
                    let name = if matches!(
                        (
                            parser
                                .tokens
                                .get(parser.position + 1)
                                .map(|token| token.kind),
                            parser
                                .tokens
                                .get(parser.position + 2)
                                .map(|token| token.kind)
                        ),
                        (Some(TokenKind::Equal), Some(TokenKind::Greater))
                    ) {
                        let name = parser.parse_identifier()?;
                        parser.advance();
                        parser.advance();
                        Some(name)
                    } else {
                        None
                    };
                    let begin = parser.position;
                    let mut depths = super::super::Depths::default();
                    while parser.current().kind != TokenKind::End {
                        if depths.is_zero()
                            && matches!(
                                parser.current().kind,
                                TokenKind::Comma | TokenKind::RightParen
                            )
                        {
                            break;
                        }
                        parser.update_expression_depths(&mut depths, parser.current())?;
                        parser.advance();
                    }
                    if !depths.is_zero() {
                        return Err(parser.unclosed_expression_error(&depths));
                    }
                    let expression = parser.parse_expression_range(begin, parser.position)?;
                    arguments.push(TableFunctionArgumentSyntax { name, expression });
                    if !parser.consume_kind(TokenKind::Comma) {
                        break;
                    }
                }
            }
            parser.expect_kind(TokenKind::RightParen, ")")?;
            Some(arguments)
        } else {
            None
        };
        let alias = if parser.consume_keyword("AS") {
            let begin = parser.current().span.start;
            let name = parser.parse_identifier()?;
            let columns = if parser.current().kind == TokenKind::LeftParen {
                parser.parse_identifier_list()?
            } else {
                Vec::new()
            };
            Some(TableAlias {
                name,
                columns,
                span: Span::new(begin, parser.previous_end()),
            })
        } else {
            parser.parse_optional_table_alias()?
        };
        Ok(Value::Relation(RelationTableSyntax {
            name,
            arguments,
            alias,
            span: Span::new(begin, parser.previous_end()),
        }))
    })
}

pub(super) fn name(input: &str, tokens: &[Token], start: usize) -> NativeMatch<Value> {
    run(input, tokens, start, |parser| {
        parser.parse_qualified_name().map(Value::Name)
    })
}
pub(super) fn alias(input: &str, tokens: &[Token], start: usize) -> NativeMatch<Value> {
    run(input, tokens, start, |parser| {
        parser.parse_optional_table_alias().map(Value::Alias)
    })
}
pub(super) fn pattern(input: &str, tokens: &[Token], start: usize) -> NativeMatch<Value> {
    run(input, tokens, start, |parser| {
        parser.parse_graph_pattern().map(Value::Pattern)
    })
}
pub(super) fn columns(input: &str, tokens: &[Token], start: usize) -> NativeMatch<Value> {
    run(input, tokens, start, |parser| {
        parser.parse_graph_table_columns().map(Value::Columns)
    })
}
pub(super) fn clause_expression(input: &str, tokens: &[Token], start: usize) -> NativeMatch<Value> {
    run(input, tokens, start, |parser| {
        parser
            .parse_select_clause_expression(
                super::super::select::SELECT_CLAUSE_KEYWORDS,
                "a SELECT clause expression",
            )
            .map(|value| Value::Expression(Box::new(value)))
    })
}
pub(super) fn offset_expression(input: &str, tokens: &[Token], start: usize) -> NativeMatch<Value> {
    run(input, tokens, start, |parser| {
        parser
            .parse_select_clause_expression(
                &[
                    "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "FOR", "FETCH", "ROW",
                    "ROWS",
                ],
                "an OFFSET expression",
            )
            .map(|value| Value::Expression(Box::new(value)))
    })
}

pub(super) fn join_expression(input: &str, tokens: &[Token], start: usize) -> NativeMatch<Value> {
    run(input, tokens, start, |parser| {
        parser
            .parse_select_clause_expression(
                super::super::select::JOIN_BOUNDARY_KEYWORDS,
                "an expression after JOIN ON",
            )
            .map(|value| Value::Expression(Box::new(value)))
    })
}
pub(super) fn group_expressions(input: &str, tokens: &[Token], start: usize) -> NativeMatch<Value> {
    run(input, tokens, start, |parser| {
        parser
            .parse_select_expression_list(
                super::super::select::SELECT_CLAUSE_KEYWORDS,
                "an expression list after GROUP BY",
            )
            .map(Value::Expressions)
    })
}
pub(super) fn order_items(input: &str, tokens: &[Token], start: usize) -> NativeMatch<Value> {
    run(input, tokens, start, |parser| {
        parser.parse_order_by_items().map(Value::OrderItems)
    })
}
pub(super) fn vertices(input: &str, tokens: &[Token], start: usize) -> NativeMatch<Value> {
    run(input, tokens, start, |parser| {
        parser
            .parse_nonempty_comma_separated(
                TokenKind::RightParen,
                Parser::parse_vertex_table_definition,
                "at least one vertex table definition",
            )
            .map(Value::Vertices)
    })
}
pub(super) fn edges(input: &str, tokens: &[Token], start: usize) -> NativeMatch<Value> {
    run(input, tokens, start, |parser| {
        parser
            .parse_nonempty_comma_separated(
                TokenKind::RightParen,
                Parser::parse_edge_table_definition,
                "at least one edge table definition",
            )
            .map(Value::Edges)
    })
}
