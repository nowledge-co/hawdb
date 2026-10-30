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

//! Token PEG matching is independent of syntax AST construction.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use crate::{Span, SyntaxError, SyntaxErrorCode, Token, TokenKind};

use memo::MemoTable;

pub type NodeId = usize;
pub type NativeMatcher<T> = fn(&str, &[Token], usize) -> NativeMatch<T>;

pub struct NativeMatch<T> {
    pub end: usize,
    pub value: Result<T, SyntaxError>,
}

pub enum Expression<T, R = &'static str> {
    Keyword(&'static str),
    Token(TokenKind, &'static str),
    Rule(R),
    Sequence(Vec<Self>),
    Choice(Vec<Self>),
    Optional(Box<Self>),
    Repeat(Box<Self>),
    Ahead(Box<Self>, &'static str),
    Not(Box<Self>, &'static str),
    Native {
        matcher: NativeMatcher<T>,
        nullable: bool,
    },
}

pub struct Grammar<T> {
    rules: Vec<(&'static str, Expression<T, usize>)>,
    names: HashMap<&'static str, usize>,
}

impl<T> Grammar<T> {
    pub fn compile(rules: Vec<(&'static str, Expression<T>)>) -> Result<Self, String> {
        if rules.len() > 256 {
            return Err("too many PEG rules".into());
        }
        let mut names = HashMap::new();
        for (index, (name, _)) in rules.iter().enumerate() {
            if names.insert(*name, index).is_some() {
                return Err(format!("duplicate PEG rule: {name}"));
            }
        }
        let rules = rules
            .into_iter()
            .map(|(name, expression)| Ok((name, expression.resolve(&names)?)))
            .collect::<Result<_, String>>()?;
        let grammar = Self { rules, names };
        grammar.validate()?;
        Ok(grammar)
    }

    pub fn parse(
        &self,
        input: &str,
        tokens: &[Token],
        root: &'static str,
        limits: Limits,
    ) -> Result<ParseTree<T>, SyntaxError> {
        let mut parser = Matcher {
            grammar: self,
            input,
            tokens,
            limits,
            memo: MemoTable::new(self.rules.len()),
            nodes: Vec::new(),
            edges: Vec::new(),
            captures: Vec::new(),
            values: Vec::new(),
            steps: 0,
            depth: 0,
            farthest: None,
            #[cfg(test)]
            memo_hits: 0,
        };
        let root_rule = self.names[root];
        let matched = parser.rule(root_rule, 0);
        match matched {
            Ok((_, root)) => Ok(ParseTree {
                nodes: parser.nodes,
                edges: parser.edges,
                values: parser.values,
                root,
                #[cfg(test)]
                steps: parser.steps,
                #[cfg(test)]
                memo_entries: parser.memo.len(),
                #[cfg(test)]
                memo_hits: parser.memo_hits,
            }),
            Err(Failure::Fatal(error)) => Err(error),
            Err(Failure::Mismatch) => Err(parser.farthest.as_ref().map_or_else(
                || parser.error(0, "a matching PEG rule"),
                |error| parser.render(error),
            )),
        }
    }
}

#[derive(Clone, Copy)]
pub struct Limits {
    pub steps: usize,
    pub nodes: usize,
    pub memo_entries: usize,
    pub depth: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            steps: 4_000_000,
            nodes: 262_144,
            memo_entries: 262_144,
            depth: 128,
        }
    }
}

pub struct Node {
    pub name: Option<&'static str>,
    pub start: usize,
    pub end: usize,
    children: Range<usize>,
    value: Option<usize>,
}

pub struct ParseTree<T> {
    pub nodes: Vec<Node>,
    edges: Vec<NodeId>,
    values: Vec<Option<T>>,
    pub root: NodeId,
    #[cfg(test)]
    pub steps: usize,
    #[cfg(test)]
    pub memo_entries: usize,
    #[cfg(test)]
    pub memo_hits: usize,
}

impl<T> ParseTree<T> {
    pub fn child(&self, node: NodeId, name: &str) -> Option<NodeId> {
        self.children(node)
            .iter()
            .copied()
            .find(|child| self.nodes[*child].name == Some(name))
    }

    pub fn required(&self, node: NodeId, name: &str) -> NodeId {
        self.child(node, name)
            .expect("matched grammar has the required child")
    }

    pub fn children(&self, node: NodeId) -> &[NodeId] {
        &self.edges[self.nodes[node].children.clone()]
    }

    pub fn take(&mut self, node: NodeId) -> T {
        let native = self.children(node)[0];
        let value = self.nodes[native].value.expect("native node has a value");
        self.values[value]
            .take()
            .expect("native value is transformed exactly once")
    }

    pub fn span(&self, node: NodeId, tokens: &[Token]) -> Span {
        let node = &self.nodes[node];
        let start = tokens[node.start].span.start;
        let end = if node.end == node.start {
            start
        } else {
            tokens[node.end - 1].span.end
        };
        Span::new(start, end)
    }
}

// Token failures borrow source text by position. Caching a rendered SyntaxError
// per rule would multiply a long invalid token by the number of alternatives.
#[derive(Clone)]
enum Diagnostic {
    Token {
        position: usize,
        expected: &'static str,
    },
    Native(Arc<SyntaxError>),
}

enum Memo {
    Pending,
    Matched(usize, NodeId, Option<Diagnostic>),
    Failed(Option<Diagnostic>),
}

enum Failure {
    Mismatch,
    Fatal(SyntaxError),
}

type MatchResult = Result<usize, Failure>;

struct Matcher<'a, T> {
    grammar: &'a Grammar<T>,
    input: &'a str,
    tokens: &'a [Token],
    limits: Limits,
    memo: MemoTable,
    nodes: Vec<Node>,
    edges: Vec<NodeId>,
    // Only captures roll back. Memoized nodes, edges and native values remain
    // append-only so a later alternative can reuse their IDs.
    captures: Vec<NodeId>,
    values: Vec<Option<T>>,
    steps: usize,
    depth: usize,
    farthest: Option<Diagnostic>,
    #[cfg(test)]
    memo_hits: usize,
}

impl<T> Matcher<'_, T> {
    fn token(&self, position: usize) -> Token {
        self.tokens.get(position).copied().unwrap_or(Token::new(
            TokenKind::End,
            Span::new(self.input.len(), self.input.len()),
        ))
    }

    fn error(&self, position: usize, expected: &'static str) -> SyntaxError {
        let token = self.token(position);
        SyntaxError::new(
            if token.kind == TokenKind::End {
                SyntaxErrorCode::UnexpectedEnd
            } else {
                SyntaxErrorCode::UnexpectedToken
            },
            token.span,
        )
        .expected(expected)
        .found(token.text(self.input))
    }

    fn limit(&self, position: usize, expected: &'static str) -> Failure {
        Failure::Fatal(
            SyntaxError::new(
                SyntaxErrorCode::ParserLimitExceeded,
                self.token(position).span,
            )
            .expected(expected),
        )
    }

    fn charge(&mut self, amount: usize, position: usize) -> Result<(), Failure> {
        if amount > self.limits.steps.saturating_sub(self.steps) {
            return Err(self.limit(position, "PEG work within the parser limit"));
        }
        self.steps += amount;
        Ok(())
    }

    fn diagnostic_start(&self, diagnostic: &Diagnostic) -> usize {
        match diagnostic {
            Diagnostic::Token { position, .. } => self.token(*position).span.start,
            Diagnostic::Native(error) => error.span.start,
        }
    }

    fn render(&self, diagnostic: &Diagnostic) -> SyntaxError {
        match diagnostic {
            Diagnostic::Token { position, expected } => self.error(*position, expected),
            Diagnostic::Native(error) => (**error).clone(),
        }
    }

    fn remember(&mut self, error: Diagnostic) {
        if self
            .farthest
            .as_ref()
            .is_none_or(|old| self.diagnostic_start(&error) > self.diagnostic_start(old))
        {
            self.farthest = Some(error);
        }
    }

    fn mismatch(&mut self, position: usize, expected: &'static str) -> Failure {
        self.remember(Diagnostic::Token { position, expected });
        Failure::Mismatch
    }

    fn push(
        &mut self,
        name: Option<&'static str>,
        start: usize,
        end: usize,
        children_start: usize,
        value: Option<T>,
    ) -> Result<NodeId, Failure> {
        let children = &self.captures[children_start..];
        if self.nodes.len() >= self.limits.nodes
            || children.len() > self.limits.nodes.saturating_sub(self.edges.len())
        {
            return Err(self.limit(start, "PEG nodes and edges within the parser limit"));
        }
        let edges_start = self.edges.len();
        self.edges.extend_from_slice(children);
        self.captures.truncate(children_start);
        let value = value.map(|value| {
            let id = self.values.len();
            self.values.push(Some(value));
            id
        });
        let id = self.nodes.len();
        self.nodes.push(Node {
            name,
            start,
            end,
            children: edges_start..self.edges.len(),
            value,
        });
        Ok(id)
    }

    fn rule(&mut self, rule: usize, position: usize) -> Result<(usize, NodeId), Failure> {
        self.charge(1, position)?;
        if let Some(memo) = self.memo.get(rule, position) {
            #[cfg(test)]
            {
                self.memo_hits += 1;
            }
            return match memo {
                Memo::Pending => Err(self.limit(position, "a PEG grammar without left recursion")),
                Memo::Matched(end, node, error) => {
                    let result = (*end, *node);
                    if let Some(error) = error.clone() {
                        self.remember(error);
                    }
                    Ok(result)
                }
                Memo::Failed(error) => {
                    if let Some(error) = error.clone() {
                        self.remember(error);
                    }
                    Err(Failure::Mismatch)
                }
            };
        }
        if self.memo.len() >= self.limits.memo_entries {
            return Err(self.limit(position, "PEG memo entries within the parser limit"));
        }
        self.memo.insert(rule, position, Memo::Pending);
        // Cache rule-local failures so lookahead can discard diagnostics without
        // making a later memo hit lose its source location.
        let outer_error = self.farthest.take();
        let children_start = self.captures.len();
        let result = self.expression(&self.grammar.rules[rule].1, position);
        let local_error = self.farthest.take();
        self.farthest = outer_error;
        if let Some(error) = local_error.clone() {
            self.remember(error);
        }
        match result {
            Ok(end) => {
                let node = self.push(
                    Some(self.grammar.rules[rule].0),
                    position,
                    end,
                    children_start,
                    None,
                )?;
                self.memo
                    .insert(rule, position, Memo::Matched(end, node, local_error));
                Ok((end, node))
            }
            Err(Failure::Mismatch) => {
                self.memo.insert(rule, position, Memo::Failed(local_error));
                Err(Failure::Mismatch)
            }
            Err(fatal) => Err(fatal),
        }
    }

    fn expression(&mut self, expression: &Expression<T, usize>, position: usize) -> MatchResult {
        self.charge(1, position)?;
        if self.depth >= self.limits.depth {
            return Err(self.limit(position, "PEG nesting within the parser limit"));
        }
        self.depth += 1;
        let captures_start = self.captures.len();
        let result = self.match_expression(expression, position);
        if result.is_err() {
            self.captures.truncate(captures_start);
        }
        self.depth -= 1;
        result
    }

    fn optional_attempt(
        &mut self,
        expression: &Expression<T, usize>,
        position: usize,
    ) -> MatchResult {
        let saved = self.farthest.clone();
        let result = self.expression(expression, position);
        if matches!(result, Err(Failure::Mismatch))
            && self
                .farthest
                .as_ref()
                .is_none_or(|error| self.diagnostic_start(error) <= self.token(position).span.start)
        {
            // Absence is expected. Keep deeper failures (e.g. WHERE without an
            // expression), but do not suggest an absent optional keyword.
            self.farthest = saved;
        }
        result
    }

    fn match_expression(
        &mut self,
        expression: &Expression<T, usize>,
        position: usize,
    ) -> MatchResult {
        match expression {
            Expression::Keyword(keyword) => {
                if self.token(position).is_keyword(self.input, keyword) {
                    Ok(position + 1)
                } else {
                    Err(self.mismatch(position, keyword))
                }
            }
            Expression::Token(kind, expected) => {
                if self.token(position).kind == *kind {
                    Ok(position + usize::from(*kind != TokenKind::End))
                } else {
                    Err(self.mismatch(position, expected))
                }
            }
            Expression::Rule(rule) => {
                let (end, node) = self.rule(*rule, position)?;
                self.captures.push(node);
                Ok(end)
            }
            Expression::Sequence(expressions) => {
                let mut end = position;
                for expression in expressions {
                    end = self.expression(expression, end)?;
                }
                Ok(end)
            }
            Expression::Choice(expressions) => {
                for expression in expressions {
                    match self.expression(expression, position) {
                        Err(Failure::Mismatch) => (),
                        result => return result,
                    }
                }
                Err(Failure::Mismatch)
            }
            Expression::Optional(expression) => match self.optional_attempt(expression, position) {
                Err(Failure::Mismatch) => Ok(position),
                result => result,
            },
            Expression::Repeat(expression) => {
                let mut end = position;
                loop {
                    match self.optional_attempt(expression, end) {
                        Ok(next) if next > end => end = next,
                        Ok(_) => return Err(self.limit(end, "a consuming PEG repetition")),
                        Err(Failure::Mismatch) => return Ok(end),
                        Err(fatal) => return Err(fatal),
                    }
                }
            }
            Expression::Ahead(inner, expected) | Expression::Not(inner, expected) => {
                let positive = matches!(expression, Expression::Ahead(..));
                let saved = self.farthest.take();
                let captures_start = self.captures.len();
                let result = self.expression(inner, position);
                self.captures.truncate(captures_start);
                self.farthest = saved;
                match result {
                    Ok(_) if positive => Ok(position),
                    Err(Failure::Mismatch) if !positive => Ok(position),
                    Ok(_) | Err(Failure::Mismatch) => Err(self.mismatch(position, expected)),
                    Err(fatal) => Err(fatal),
                }
            }
            Expression::Native { matcher, nullable } => {
                let matched = matcher(self.input, self.tokens, position);
                if matched.end < position || matched.end >= self.tokens.len() {
                    return Err(self.limit(position, "a valid native matcher token range"));
                }
                self.charge(matched.end - position, position)?;
                match matched.value {
                    Ok(value) => {
                        if !nullable && matched.end == position {
                            return Err(self.limit(position, "a consuming native terminal"));
                        }
                        let node = self.push(
                            None,
                            position,
                            matched.end,
                            self.captures.len(),
                            Some(value),
                        )?;
                        self.captures.push(node);
                        Ok(matched.end)
                    }
                    Err(error)
                        if matches!(
                            error.code,
                            SyntaxErrorCode::UnexpectedToken | SyntaxErrorCode::UnexpectedEnd
                        ) =>
                    {
                        self.remember(Diagnostic::Native(Arc::new(error)));
                        Err(Failure::Mismatch)
                    }
                    Err(error) => Err(Failure::Fatal(error)),
                }
            }
        }
    }
}

mod memo;
#[cfg(test)]
mod tests;
mod validation;
