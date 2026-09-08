use crate::{
    tokenize, ExpressionSyntax, GraphEdgeDirection, GraphEdgePatternSyntax,
    GraphElementPatternSyntax, GraphPathFactorSyntax, GraphPathPrimarySyntax, GraphPathSyntax,
    GraphPatternQuantifier, GraphPatternSyntax, GraphTable, GraphTableColumn, Identifier,
    LabeledExpressionSyntax, PgqStatement, PostgresSelectSyntax, PostgresStatementSyntax,
    QualifiedName, Span, SyntaxError, SyntaxErrorCode, TableAlias, Token, TokenKind,
};

mod expression;
mod property_graph;
mod select;

pub fn parse_postgres_statement(input: &str) -> Result<PostgresStatementSyntax, SyntaxError> {
    let tokens = tokenize(input)?;
    let mut parser = Parser::new(input, &tokens);
    let statement = if parser.at_keyword("CREATE") {
        PostgresStatementSyntax::CreatePropertyGraph(parser.parse_create_property_graph()?)
    } else if parser.at_keyword("SELECT") {
        PostgresStatementSyntax::Select(Box::new(parser.parse_postgres_select()?))
    } else {
        return Err(parser.unexpected("CREATE PROPERTY GRAPH or SELECT"));
    };
    parser.consume_kind(TokenKind::Semicolon);
    parser.expect_kind(TokenKind::End, "end of statement")?;
    Ok(statement)
}

pub fn parse_postgres_select(input: &str) -> Result<PostgresSelectSyntax, SyntaxError> {
    let tokens = tokenize(input)?;
    let mut parser = Parser::new(input, &tokens);
    let select = parser.parse_postgres_select()?;
    parser.consume_kind(TokenKind::Semicolon);
    parser.expect_kind(TokenKind::End, "end of SELECT")?;
    Ok(select)
}

pub fn parse_pgq_statement(input: &str) -> Result<PgqStatement, SyntaxError> {
    let tokens = tokenize(input)?;
    let mut parser = Parser::new(input, &tokens);
    let statement = if parser.at_keyword("CREATE") {
        PgqStatement::CreatePropertyGraph(parser.parse_create_property_graph()?)
    } else {
        return Err(parser.unexpected("CREATE PROPERTY GRAPH"));
    };
    parser.consume_kind(TokenKind::Semicolon);
    parser.expect_kind(TokenKind::End, "end of statement")?;
    Ok(statement)
}

pub fn parse_graph_table(input: &str) -> Result<GraphTable, SyntaxError> {
    let tokens = tokenize(input)?;
    let mut parser = Parser::new(input, &tokens);
    let table = parser.parse_graph_table()?;
    parser.consume_kind(TokenKind::Semicolon);
    parser.expect_kind(TokenKind::End, "end of GRAPH_TABLE clause")?;
    Ok(table)
}

const MAX_GRAPH_PATTERN_NESTING: usize = 128;
const MAX_EXPRESSION_NESTING: usize = 128;

struct Parser<'a> {
    input: &'a str,
    tokens: &'a [Token],
    position: usize,
    graph_pattern_nesting: usize,
}

struct ParsedGraphElementFields {
    variable: Option<Identifier>,
    labels: Vec<Identifier>,
    predicate: Option<ExpressionSyntax>,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str, tokens: &'a [Token]) -> Self {
        Self {
            input,
            tokens,
            position: 0,
            graph_pattern_nesting: 0,
        }
    }

    fn parse_identifier_list(&mut self) -> Result<Vec<Identifier>, SyntaxError> {
        self.expect_kind(TokenKind::LeftParen, "(")?;
        let identifiers = self.parse_nonempty_comma_separated(
            TokenKind::RightParen,
            Self::parse_identifier,
            "at least one identifier",
        )?;
        self.expect_kind(TokenKind::RightParen, ")")?;
        Ok(identifiers)
    }

    fn parse_labeled_expressions(
        &mut self,
        expected: &'static str,
    ) -> Result<Vec<LabeledExpressionSyntax>, SyntaxError> {
        if self.current().kind == TokenKind::RightParen {
            return Err(self.unexpected(expected));
        }
        let mut expressions = Vec::new();
        loop {
            let start_position = self.position;
            let end_position = self.scan_to_top_level_delimiter()?;
            expressions.push(self.labeled_expression_from_range(start_position, end_position)?);
            if !self.consume_kind(TokenKind::Comma) {
                break;
            }
        }
        Ok(expressions)
    }

    fn parse_graph_table(&mut self) -> Result<GraphTable, SyntaxError> {
        let start = self.expect_keyword("GRAPH_TABLE")?.span.start;
        self.expect_kind(TokenKind::LeftParen, "(")?;
        let graph = self.parse_qualified_name()?;
        self.expect_keyword("MATCH")?;
        let pattern = self.parse_graph_pattern()?;
        self.expect_keyword("COLUMNS")?;

        self.expect_kind(TokenKind::LeftParen, "(")?;
        let columns = self.parse_graph_table_columns()?;
        self.expect_kind(TokenKind::RightParen, ")")?;
        self.expect_kind(TokenKind::RightParen, ")")?;
        let alias = self.parse_optional_table_alias()?;

        Ok(GraphTable {
            graph,
            pattern,
            columns,
            alias,
            span: Span::new(start, self.previous_end()),
        })
    }

    fn parse_graph_pattern(&mut self) -> Result<GraphPatternSyntax, SyntaxError> {
        let start = self.current().span.start;
        let mut paths = vec![self.parse_graph_path()?];
        while self.consume_kind(TokenKind::Comma) {
            paths.push(self.parse_graph_path()?);
        }
        let predicate = if self.consume_keyword("WHERE") {
            Some(self.parse_expression_until_keyword("COLUMNS", "an expression after WHERE")?)
        } else {
            None
        };
        let end = predicate.as_ref().map_or_else(
            || paths.last().expect("graph path exists").span.end,
            |value| value.span.end,
        );
        Ok(GraphPatternSyntax {
            paths,
            predicate,
            span: Span::new(start, end),
        })
    }

    fn parse_graph_path(&mut self) -> Result<GraphPathSyntax, SyntaxError> {
        let start = self.current().span.start;
        let mut factors = Vec::new();
        while self.can_start_graph_primary() {
            factors.push(self.parse_graph_path_factor()?);
        }
        if factors.is_empty() {
            return Err(self.unexpected("a graph path pattern"));
        }
        Ok(GraphPathSyntax {
            span: Span::new(start, self.previous_end()),
            factors,
        })
    }

    fn parse_graph_path_factor(&mut self) -> Result<GraphPathFactorSyntax, SyntaxError> {
        let start = self.current().span.start;
        let primary = self.parse_graph_path_primary()?;
        let quantifier = self.parse_optional_graph_quantifier()?;
        Ok(GraphPathFactorSyntax {
            primary,
            quantifier,
            span: Span::new(start, self.previous_end()),
        })
    }

    fn parse_graph_path_primary(&mut self) -> Result<GraphPathPrimarySyntax, SyntaxError> {
        if self.current().kind == TokenKind::LeftParen {
            return self.parse_parenthesized_or_vertex_pattern();
        }
        self.parse_graph_edge_pattern()
            .map(GraphPathPrimarySyntax::Edge)
    }

    fn parse_parenthesized_or_vertex_pattern(
        &mut self,
    ) -> Result<GraphPathPrimarySyntax, SyntaxError> {
        let start = self.expect_kind(TokenKind::LeftParen, "(")?.span.start;
        if self.can_start_graph_primary() {
            if self.graph_pattern_nesting >= MAX_GRAPH_PATTERN_NESTING {
                return Err(SyntaxError::new(
                    SyntaxErrorCode::GraphPatternNestingLimitExceeded,
                    Span::new(start, self.current().span.end),
                ));
            }
            self.graph_pattern_nesting += 1;
            let nested = self.parse_nested_graph_path(start);
            self.graph_pattern_nesting -= 1;
            return nested;
        }

        let fields = self.parse_graph_element_fields(TokenKind::RightParen)?;
        self.expect_kind(TokenKind::RightParen, ")")?;
        Ok(GraphPathPrimarySyntax::Vertex(GraphElementPatternSyntax {
            variable: fields.variable,
            labels: fields.labels,
            predicate: fields.predicate,
            span: Span::new(start, self.previous_end()),
        }))
    }

    fn parse_nested_graph_path(
        &mut self,
        start: usize,
    ) -> Result<GraphPathPrimarySyntax, SyntaxError> {
        let path = Box::new(self.parse_graph_path()?);
        let predicate =
            if self.consume_keyword("WHERE") {
                Some(self.parse_expression_until_kind(
                    TokenKind::RightParen,
                    "an expression after WHERE",
                )?)
            } else {
                None
            };
        self.expect_kind(TokenKind::RightParen, ")")?;
        Ok(GraphPathPrimarySyntax::Parenthesized {
            path,
            predicate,
            span: Span::new(start, self.previous_end()),
        })
    }

    fn parse_graph_edge_pattern(&mut self) -> Result<GraphEdgePatternSyntax, SyntaxError> {
        let start = self.current().span.start;
        match self.current().kind {
            TokenKind::ArrowLeft => {
                self.advance();
                self.parse_left_edge_tail(start)
            }
            TokenKind::Less => {
                self.advance();
                self.expect_kind(TokenKind::Minus, "- after <")?;
                self.parse_left_edge_tail(start)
            }
            TokenKind::ArrowRight => {
                self.advance();
                Ok(self.abbreviated_edge(start, GraphEdgeDirection::Right))
            }
            TokenKind::Minus => {
                self.advance();
                if self.consume_kind(TokenKind::LeftBracket) {
                    let fields = self.parse_graph_element_fields(TokenKind::RightBracket)?;
                    self.expect_kind(TokenKind::RightBracket, "]")?;
                    let direction = if self.consume_kind(TokenKind::ArrowRight) {
                        GraphEdgeDirection::Right
                    } else {
                        self.expect_kind(TokenKind::Minus, "- or -> after edge pattern")?;
                        if self.consume_kind(TokenKind::Greater) {
                            GraphEdgeDirection::Right
                        } else {
                            GraphEdgeDirection::Any
                        }
                    };
                    Ok(GraphEdgePatternSyntax {
                        direction,
                        variable: fields.variable,
                        labels: fields.labels,
                        predicate: fields.predicate,
                        abbreviated: false,
                        span: Span::new(start, self.previous_end()),
                    })
                } else {
                    let direction = if self.consume_kind(TokenKind::Greater) {
                        GraphEdgeDirection::Right
                    } else {
                        GraphEdgeDirection::Any
                    };
                    Ok(self.abbreviated_edge(start, direction))
                }
            }
            _ => Err(self.unexpected("a graph vertex or edge pattern")),
        }
    }

    fn parse_left_edge_tail(
        &mut self,
        start: usize,
    ) -> Result<GraphEdgePatternSyntax, SyntaxError> {
        if !self.consume_kind(TokenKind::LeftBracket) {
            return Ok(self.abbreviated_edge(start, GraphEdgeDirection::Left));
        }
        let fields = self.parse_graph_element_fields(TokenKind::RightBracket)?;
        self.expect_kind(TokenKind::RightBracket, "]")?;
        self.expect_kind(TokenKind::Minus, "- after left edge pattern")?;
        Ok(GraphEdgePatternSyntax {
            direction: GraphEdgeDirection::Left,
            variable: fields.variable,
            labels: fields.labels,
            predicate: fields.predicate,
            abbreviated: false,
            span: Span::new(start, self.previous_end()),
        })
    }

    fn abbreviated_edge(
        &self,
        start: usize,
        direction: GraphEdgeDirection,
    ) -> GraphEdgePatternSyntax {
        GraphEdgePatternSyntax {
            direction,
            variable: None,
            labels: Vec::new(),
            predicate: None,
            abbreviated: true,
            span: Span::new(start, self.previous_end()),
        }
    }

    fn parse_graph_element_fields(
        &mut self,
        terminator: TokenKind,
    ) -> Result<ParsedGraphElementFields, SyntaxError> {
        let variable = if is_identifier_kind(self.current().kind)
            && !self.at_keyword("IS")
            && !self.at_keyword("WHERE")
        {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        let labels = if self.consume_keyword("IS") {
            let mut labels = vec![self.parse_identifier()?];
            while self.consume_kind(TokenKind::Pipe) {
                labels.push(self.parse_identifier()?);
            }
            labels
        } else {
            Vec::new()
        };
        let predicate = if self.consume_keyword("WHERE") {
            Some(self.parse_expression_until_kind(terminator, "an expression after WHERE")?)
        } else {
            None
        };
        Ok(ParsedGraphElementFields {
            variable,
            labels,
            predicate,
        })
    }

    fn parse_optional_graph_quantifier(
        &mut self,
    ) -> Result<Option<GraphPatternQuantifier>, SyntaxError> {
        if !self.consume_kind(TokenKind::LeftBrace) {
            return Ok(None);
        }
        let start = self.tokens[self.position - 1].span.start;
        let (min, max) = if self.consume_kind(TokenKind::Comma) {
            (0, self.parse_nonnegative_integer()?)
        } else {
            let min = self.parse_nonnegative_integer()?;
            if self.consume_kind(TokenKind::RightBrace) {
                return Ok(Some(GraphPatternQuantifier {
                    min,
                    max: min,
                    span: Span::new(start, self.previous_end()),
                }));
            }
            self.expect_kind(TokenKind::Comma, ", or } in graph pattern quantifier")?;
            (min, self.parse_nonnegative_integer()?)
        };
        self.expect_kind(TokenKind::RightBrace, "}")?;
        Ok(Some(GraphPatternQuantifier {
            min,
            max,
            span: Span::new(start, self.previous_end()),
        }))
    }

    fn parse_nonnegative_integer(&mut self) -> Result<u32, SyntaxError> {
        let token = self.current();
        if token.kind != TokenKind::Number {
            return Err(self.unexpected("a non-negative integer"));
        }
        let value = token
            .text(self.input)
            .parse::<u32>()
            .map_err(|_| self.unexpected("a non-negative integer"))?;
        self.advance();
        Ok(value)
    }

    fn can_start_graph_primary(&self) -> bool {
        matches!(
            self.current().kind,
            TokenKind::LeftParen
                | TokenKind::ArrowLeft
                | TokenKind::ArrowRight
                | TokenKind::Less
                | TokenKind::Minus
        )
    }

    fn parse_graph_table_columns(&mut self) -> Result<Vec<GraphTableColumn>, SyntaxError> {
        if self.current().kind == TokenKind::RightParen {
            return Err(self.unexpected("at least one GRAPH_TABLE column"));
        }
        let mut columns = Vec::new();
        loop {
            let start_position = self.position;
            let end_position = self.scan_to_top_level_delimiter()?;
            columns.push(self.graph_table_column_from_range(start_position, end_position)?);
            if !self.consume_kind(TokenKind::Comma) {
                break;
            }
        }
        Ok(columns)
    }

    fn graph_table_column_from_range(
        &self,
        start_position: usize,
        end_position: usize,
    ) -> Result<GraphTableColumn, SyntaxError> {
        let expression = self.labeled_expression_from_range(start_position, end_position)?;
        Ok(GraphTableColumn {
            expression: expression.expression,
            alias: expression.alias,
            span: expression.span,
        })
    }

    fn labeled_expression_from_range(
        &self,
        start_position: usize,
        end_position: usize,
    ) -> Result<LabeledExpressionSyntax, SyntaxError> {
        if start_position == end_position {
            return Err(self.unexpected("an expression"));
        }
        let mut alias_position = None;
        let mut depths = Depths::default();
        for position in start_position..end_position {
            let token = self.tokens[position];
            self.update_expression_depths(&mut depths, token)?;
            if depths.is_zero() && token.is_keyword(self.input, "AS") {
                alias_position = Some(position);
            }
        }
        let (expression_end_position, alias) = if let Some(as_position) = alias_position {
            let alias_token = self.tokens.get(as_position + 1).copied().ok_or_else(|| {
                SyntaxError::new(SyntaxErrorCode::UnexpectedEnd, self.current().span)
                    .expected("a GRAPH_TABLE column alias")
            })?;
            if as_position + 2 != end_position || !is_identifier_kind(alias_token.kind) {
                return Err(
                    SyntaxError::new(SyntaxErrorCode::UnexpectedToken, alias_token.span)
                        .expected("one column alias after AS")
                        .found(alias_token.text(self.input)),
                );
            }
            (as_position, Some(identifier_from_token(alias_token)))
        } else {
            (end_position, None)
        };
        let start = self.tokens[start_position].span.start;
        let end = self.tokens[end_position - 1].span.end;
        if start_position == expression_end_position {
            return Err(
                SyntaxError::new(SyntaxErrorCode::UnexpectedToken, Span::new(start, end))
                    .expected("an expression before AS"),
            );
        }
        Ok(LabeledExpressionSyntax {
            expression: self.parse_expression_range(start_position, expression_end_position)?,
            alias,
            span: Span::new(start, end),
        })
    }

    fn parse_optional_table_alias(&mut self) -> Result<Option<TableAlias>, SyntaxError> {
        let has_as = self.consume_keyword("AS");
        if !has_as && (!is_identifier_kind(self.current().kind) || self.at_table_alias_boundary()) {
            return Ok(None);
        }
        if has_as && self.at_table_alias_boundary() {
            return Err(self.unexpected("a table alias"));
        }
        let start = self.current().span.start;
        let name = self.parse_identifier()?;
        let columns = if self.current().kind == TokenKind::LeftParen {
            self.parse_identifier_list()?
        } else {
            Vec::new()
        };
        Ok(Some(TableAlias {
            name,
            columns,
            span: Span::new(start, self.previous_end()),
        }))
    }

    fn at_table_alias_boundary(&self) -> bool {
        matches!(
            self.current().kind,
            TokenKind::Comma | TokenKind::RightParen | TokenKind::Semicolon | TokenKind::End
        ) || [
            "WHERE",
            "GROUP",
            "HAVING",
            "ORDER",
            "LIMIT",
            "OFFSET",
            "FETCH",
            "FOR",
            "JOIN",
            "INNER",
            "LEFT",
            "RIGHT",
            "FULL",
            "CROSS",
            "ON",
            "USING",
            "NATURAL",
            "UNION",
            "EXCEPT",
            "INTERSECT",
            "LATERAL",
            "TABLESAMPLE",
            "WINDOW",
        ]
        .into_iter()
        .any(|keyword| self.at_keyword(keyword))
    }

    fn parse_expression_until_keyword(
        &mut self,
        keyword: &'static str,
        expected: &'static str,
    ) -> Result<ExpressionSyntax, SyntaxError> {
        let start_position = self.position;
        let mut depths = Depths::default();
        while self.current().kind != TokenKind::End {
            if depths.is_zero() && self.at_keyword(keyword) {
                if start_position == self.position {
                    return Err(self.unexpected(expected));
                }
                return self.parse_expression_range(start_position, self.position);
            }
            self.update_expression_depths(&mut depths, self.current())?;
            self.advance();
        }
        if !depths.is_zero() {
            return Err(self.unclosed_expression_error(&depths));
        }
        Err(SyntaxError::new(SyntaxErrorCode::UnexpectedEnd, self.current().span).expected(keyword))
    }

    fn parse_expression_until_kind(
        &mut self,
        terminator: TokenKind,
        expected: &'static str,
    ) -> Result<ExpressionSyntax, SyntaxError> {
        let start_position = self.position;
        let mut depths = Depths::default();
        while self.current().kind != TokenKind::End {
            if depths.is_zero() && self.current().kind == terminator {
                if start_position == self.position {
                    return Err(self.unexpected(expected));
                }
                return self.parse_expression_range(start_position, self.position);
            }
            self.update_expression_depths(&mut depths, self.current())?;
            self.advance();
        }
        if !depths.is_zero() {
            return Err(self.unclosed_expression_error(&depths));
        }
        Err(
            SyntaxError::new(SyntaxErrorCode::UnexpectedEnd, self.current().span)
                .expected("a closing graph element delimiter"),
        )
    }

    fn scan_to_top_level_delimiter(&mut self) -> Result<usize, SyntaxError> {
        let mut depths = Depths::default();
        while self.current().kind != TokenKind::End {
            if depths.is_zero()
                && matches!(
                    self.current().kind,
                    TokenKind::Comma | TokenKind::RightParen
                )
            {
                return Ok(self.position);
            }
            self.update_expression_depths(&mut depths, self.current())?;
            self.advance();
        }
        if !depths.is_zero() {
            return Err(self.unclosed_expression_error(&depths));
        }
        Err(
            SyntaxError::new(SyntaxErrorCode::UnexpectedEnd, self.current().span)
                .expected("a comma or closing parenthesis"),
        )
    }

    fn parse_qualified_name(&mut self) -> Result<QualifiedName, SyntaxError> {
        let first = self.parse_identifier()?;
        let start = first.span.start;
        let mut end = first.span.end;
        let mut parts = vec![first];
        while self.consume_kind(TokenKind::Dot) {
            let part = self.parse_identifier()?;
            end = part.span.end;
            parts.push(part);
        }
        Ok(QualifiedName {
            parts,
            span: Span::new(start, end),
        })
    }

    fn parse_identifier(&mut self) -> Result<Identifier, SyntaxError> {
        let token = self.current();
        if !is_identifier_kind(token.kind) {
            return Err(self.unexpected("an identifier"));
        }
        self.advance();
        Ok(identifier_from_token(token))
    }

    fn parse_comma_separated<T>(
        &mut self,
        terminator: TokenKind,
        parse_item: fn(&mut Self) -> Result<T, SyntaxError>,
    ) -> Result<Vec<T>, SyntaxError> {
        if self.current().kind == terminator {
            return Ok(Vec::new());
        }
        let mut items = Vec::new();
        loop {
            items.push(parse_item(self)?);
            if !self.consume_kind(TokenKind::Comma) {
                break;
            }
        }
        Ok(items)
    }

    fn parse_nonempty_comma_separated<T>(
        &mut self,
        terminator: TokenKind,
        parse_item: fn(&mut Self) -> Result<T, SyntaxError>,
        expected: &'static str,
    ) -> Result<Vec<T>, SyntaxError> {
        if self.current().kind == terminator {
            return Err(self.unexpected(expected));
        }
        self.parse_comma_separated(terminator, parse_item)
    }

    fn current(&self) -> Token {
        self.tokens
            .get(self.position)
            .copied()
            .unwrap_or(Token::new(
                TokenKind::End,
                Span::new(self.input.len(), self.input.len()),
            ))
    }

    fn previous_end(&self) -> usize {
        self.position
            .checked_sub(1)
            .and_then(|position| self.tokens.get(position))
            .map_or(0, |token| token.span.end)
    }

    fn advance(&mut self) -> Token {
        let token = self.current();
        if token.kind != TokenKind::End {
            self.position += 1;
        }
        token
    }

    fn at_keyword(&self, keyword: &str) -> bool {
        self.current().is_keyword(self.input, keyword)
    }

    fn consume_keyword(&mut self, keyword: &str) -> bool {
        if self.at_keyword(keyword) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn expect_keyword(&mut self, keyword: &'static str) -> Result<Token, SyntaxError> {
        if self.at_keyword(keyword) {
            Ok(self.advance())
        } else {
            Err(self.unexpected(keyword))
        }
    }

    fn consume_kind(&mut self, kind: TokenKind) -> bool {
        if self.current().kind == kind {
            self.advance();
            true
        } else {
            false
        }
    }

    fn expect_kind(
        &mut self,
        kind: TokenKind,
        expected: &'static str,
    ) -> Result<Token, SyntaxError> {
        if self.current().kind == kind {
            Ok(self.advance())
        } else {
            Err(self.unexpected(expected))
        }
    }

    fn unexpected(&self, expected: &'static str) -> SyntaxError {
        let token = self.current();
        let code = if token.kind == TokenKind::End {
            SyntaxErrorCode::UnexpectedEnd
        } else {
            SyntaxErrorCode::UnexpectedToken
        };
        SyntaxError::new(code, token.span)
            .expected(expected)
            .found(token.text(self.input))
    }

    fn update_expression_depths(
        &self,
        depths: &mut Depths,
        token: Token,
    ) -> Result<(), SyntaxError> {
        depths.update(token.kind).map_err(|error| match error {
            DepthError::MismatchedDelimiter(expected) => {
                SyntaxError::new(SyntaxErrorCode::UnexpectedToken, token.span)
                    .expected(expected)
                    .found(token.text(self.input))
            }
            DepthError::NestingLimitExceeded => {
                SyntaxError::new(SyntaxErrorCode::ExpressionNestingLimitExceeded, token.span)
                    .expected("expression nesting within the configured parser limit")
            }
        })
    }

    fn unclosed_expression_error(&self, depths: &Depths) -> SyntaxError {
        SyntaxError::new(SyntaxErrorCode::UnexpectedEnd, self.current().span)
            .expected(depths.expected_closing().unwrap_or("a closing delimiter"))
    }
}

#[derive(Debug)]
struct Depths {
    closing_delimiters: [TokenKind; MAX_EXPRESSION_NESTING],
    len: usize,
}

impl Default for Depths {
    fn default() -> Self {
        Self {
            closing_delimiters: [TokenKind::End; MAX_EXPRESSION_NESTING],
            len: 0,
        }
    }
}

enum DepthError {
    MismatchedDelimiter(&'static str),
    NestingLimitExceeded,
}

impl Depths {
    fn update(&mut self, kind: TokenKind) -> Result<(), DepthError> {
        match kind {
            TokenKind::LeftParen => self.push(TokenKind::RightParen)?,
            TokenKind::LeftBracket => self.push(TokenKind::RightBracket)?,
            TokenKind::LeftBrace => self.push(TokenKind::RightBrace)?,
            TokenKind::RightParen | TokenKind::RightBracket | TokenKind::RightBrace => {
                if self.last() != Some(kind) {
                    return Err(DepthError::MismatchedDelimiter(
                        self.expected_closing().unwrap_or("an opening delimiter"),
                    ));
                }
                self.len -= 1;
            }
            _ => return Ok(()),
        }
        Ok(())
    }

    fn is_zero(&self) -> bool {
        self.len == 0
    }

    fn expected_closing(&self) -> Option<&'static str> {
        match self.last() {
            Some(TokenKind::RightParen) => Some(")"),
            Some(TokenKind::RightBracket) => Some("]"),
            Some(TokenKind::RightBrace) => Some("}"),
            _ => None,
        }
    }

    fn last(&self) -> Option<TokenKind> {
        self.len
            .checked_sub(1)
            .map(|position| self.closing_delimiters[position])
    }

    fn push(&mut self, kind: TokenKind) -> Result<(), DepthError> {
        if self.len == MAX_EXPRESSION_NESTING {
            return Err(DepthError::NestingLimitExceeded);
        }
        self.closing_delimiters[self.len] = kind;
        self.len += 1;
        Ok(())
    }
}

fn is_identifier_kind(kind: TokenKind) -> bool {
    matches!(kind, TokenKind::Word | TokenKind::QuotedIdentifier)
}

fn identifier_from_token(token: Token) -> Identifier {
    Identifier {
        span: token.span,
        quoted: token.kind == TokenKind::QuotedIdentifier,
    }
}
