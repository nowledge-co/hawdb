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

use crate::{Span, SyntaxError, SyntaxErrorCode, Token, TokenKind};

use memo::MemoTable;

pub type NodeId = usize;
pub enum Expression<R = &'static str> {
    Keyword(&'static str),
    Token(TokenKind, &'static str),
    Parameter,
    Rule(R),
    Sequence(Vec<Self>),
    Choice(Vec<Self>),
    Optional(Box<Self>),
    Repeat(Box<Self>),
    /// Match a token sequence with no source whitespace between its tokens.
    Tight(Box<Self>),
    Ahead(Box<Self>, &'static str),
    Not(Box<Self>, &'static str),
}

pub struct Grammar {
    rules: Vec<(&'static str, Expression<usize>)>,
    names: HashMap<&'static str, usize>,
    nesting_rules: Vec<u8>,
    nesting_limits: [(usize, SyntaxErrorCode); 2],
}

impl Grammar {
    pub fn compile(rules: Vec<(&'static str, Expression)>) -> Result<Self, String> {
        if rules.len() > 256 {
            return Err("too many PEG rules".into());
        }
        let mut names = HashMap::new();
        for (index, (name, _)) in rules.iter().enumerate() {
            if names.insert(*name, index).is_some() {
                return Err(format!("duplicate PEG rule: {name}"));
            }
        }
        let rules: Vec<_> = rules
            .into_iter()
            .map(|(name, expression)| Ok((name, expression.resolve(&names)?)))
            .collect::<Result<_, String>>()?;
        let nesting_rules = vec![0; rules.len()];
        let grammar = Self {
            rules,
            names,
            nesting_rules,
            nesting_limits: [(usize::MAX, SyntaxErrorCode::ParserLimitExceeded); 2],
        };
        grammar.validate()?;
        Ok(grammar)
    }

    /// Bounds language recursion independently of PEG combinator frames.
    /// Memoized subtrees retain their guarded height, so lookahead reuse cannot
    /// bypass the limit when the same subtree is entered in a deeper context.
    pub fn compile_bounded(
        rules: Vec<(&'static str, Expression)>,
        nesting_rules: &[&'static str],
        nesting_limit: usize,
    ) -> Result<Self, String> {
        Self::compile_with_nesting(
            rules,
            &[(
                nesting_rules,
                nesting_limit,
                SyntaxErrorCode::ParserLimitExceeded,
            )],
        )
    }

    /// Independent expression and graph recursion budgets. The CST stores both
    /// subtree heights to enforce the same admission on memoized rule reuse.
    pub fn compile_with_nesting(
        rules: Vec<(&'static str, Expression)>,
        budgets: &[(&[&'static str], usize, SyntaxErrorCode)],
    ) -> Result<Self, String> {
        if budgets.len() > 2 {
            return Err("at most two independent PEG nesting budgets are supported".into());
        }
        let mut grammar = Self::compile(rules)?;
        for (domain, (names, limit, code)) in budgets.iter().enumerate() {
            grammar.nesting_limits[domain] = (*limit, *code);
            for name in *names {
                let id = grammar
                    .names
                    .get(name)
                    .ok_or_else(|| format!("unknown nesting rule: {name}"))?;
                grammar.nesting_rules[*id] |= 1 << domain;
            }
        }
        Ok(grammar)
    }

    pub fn parse(
        &self,
        input: &str,
        tokens: &[Token],
        root: &'static str,
        limits: Limits,
    ) -> Result<ParseTree, SyntaxError> {
        let mut parser = Matcher {
            grammar: self,
            input,
            tokens,
            limits,
            memo: MemoTable::new(self.rules.len()),
            nodes: Vec::new(),
            edges: Vec::new(),
            captures: Vec::new(),
            steps: 0,
            depth: 0,
            nesting_depth: [0; 2],
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
    nesting_height: [usize; 2],
}

pub struct ParseTree {
    pub nodes: Vec<Node>,
    edges: Vec<NodeId>,
    pub root: NodeId,
    #[cfg(test)]
    pub steps: usize,
    #[cfg(test)]
    pub memo_entries: usize,
    #[cfg(test)]
    pub memo_hits: usize,
}

impl ParseTree {
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

// Continuations refer only to immutable compiled grammar nodes. Pending frames
// are bounded by the expression-depth budget; repetitions reuse their frame.
enum Frame<'a> {
    Rule(usize, usize),
    RuleDone {
        rule: usize,
        position: usize,
        captures: usize,
        outer_error: Option<Diagnostic>,
    },
    Expression(&'a Expression<usize>, usize),
    ExpressionDone(usize),
    Sequence(&'a [Expression<usize>], usize),
    Choice {
        items: &'a [Expression<usize>],
        next: usize,
        position: usize,
    },
    OptionalDone {
        position: usize,
        saved: Option<Diagnostic>,
    },
    Repeat(&'a Expression<usize>, usize),
    RepeatDone {
        inner: &'a Expression<usize>,
        position: usize,
        saved: Option<Diagnostic>,
    },
    TightDone(usize),
    LookDone {
        position: usize,
        positive: bool,
        expected: &'static str,
        saved: Option<Diagnostic>,
        captures: usize,
    },
}

struct Matcher<'a> {
    grammar: &'a Grammar,
    input: &'a str,
    tokens: &'a [Token],
    limits: Limits,
    memo: MemoTable,
    nodes: Vec<Node>,
    edges: Vec<NodeId>,
    // Only captures roll back. Memoized nodes and edges remain append-only
    // so a later alternative can reuse their IDs.
    captures: Vec<NodeId>,
    steps: usize,
    depth: usize,
    nesting_depth: [usize; 2],
    farthest: Option<Diagnostic>,
    #[cfg(test)]
    memo_hits: usize,
}

impl<'a> Matcher<'a> {
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

    fn nesting_limit(&self, position: usize, domain: usize) -> Failure {
        Failure::Fatal(
            SyntaxError::new(
                self.grammar.nesting_limits[domain].1,
                self.token(position).span,
            )
            .expected("language nesting within the parser limit"),
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
        }
    }

    fn render(&self, diagnostic: &Diagnostic) -> SyntaxError {
        match diagnostic {
            Diagnostic::Token { position, expected } => self.error(*position, expected),
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
        nesting_weight: u8,
    ) -> Result<NodeId, Failure> {
        let children = &self.captures[children_start..];
        if self.nodes.len() >= self.limits.nodes
            || children.len() > self.limits.nodes.saturating_sub(self.edges.len())
        {
            return Err(self.limit(start, "PEG nodes and edges within the parser limit"));
        }
        let nesting_height = std::array::from_fn(|domain| {
            children
                .iter()
                .map(|id| self.nodes[*id].nesting_height[domain])
                .max()
                .unwrap_or(0)
                .saturating_add(usize::from(nesting_weight & (1 << domain) != 0))
        });
        let edges_start = self.edges.len();
        self.edges.extend_from_slice(children);
        self.captures.truncate(children_start);
        let id = self.nodes.len();
        self.nodes.push(Node {
            name,
            start,
            end,
            children: edges_start..self.edges.len(),
            nesting_height,
        });
        Ok(id)
    }

    fn rule(&mut self, root: usize, start: usize) -> Result<(usize, NodeId), Failure> {
        // Grammar recursion lives on this bounded work stack, never on the host
        // thread stack. Expression depth and language-domain admission still
        // apply before scheduling children.
        let mut work = vec![Frame::Rule(root, start)];
        let mut result: MatchResult = Err(Failure::Mismatch);
        while let Some(frame) = work.pop() {
            match frame {
                Frame::Rule(rule, position) => {
                    self.charge(1, position)?;
                    if let Some(memo) = self.memo.get(rule, position) {
                        #[cfg(test)]
                        {
                            self.memo_hits += 1;
                        }
                        match memo {
                            Memo::Pending => {
                                return Err(
                                    self.limit(position, "a PEG grammar without left recursion")
                                )
                            }
                            Memo::Matched(end, node, error) => {
                                for domain in 0..2 {
                                    if self.nesting_depth[domain]
                                        .saturating_add(self.nodes[*node].nesting_height[domain])
                                        > self.grammar.nesting_limits[domain].0
                                    {
                                        return Err(self.nesting_limit(position, domain));
                                    }
                                }
                                let (end, node, error) = (*end, *node, error.clone());
                                if let Some(error) = error {
                                    self.remember(error);
                                }
                                self.captures.push(node);
                                result = Ok(end);
                            }
                            Memo::Failed(error) => {
                                let error = error.clone();
                                if let Some(error) = error {
                                    self.remember(error);
                                }
                                result = Err(Failure::Mismatch);
                            }
                        }
                        continue;
                    }
                    if self.memo.len() >= self.limits.memo_entries {
                        return Err(
                            self.limit(position, "PEG memo entries within the parser limit")
                        );
                    }
                    let weight = self.grammar.nesting_rules[rule];
                    for domain in 0..2 {
                        if self.nesting_depth[domain]
                            .saturating_add(usize::from(weight & (1 << domain) != 0))
                            > self.grammar.nesting_limits[domain].0
                        {
                            return Err(self.nesting_limit(position, domain));
                        }
                    }
                    self.memo.insert(rule, position, Memo::Pending);
                    let outer_error = self.farthest.take();
                    for domain in 0..2 {
                        self.nesting_depth[domain] += usize::from(weight & (1 << domain) != 0);
                    }
                    work.push(Frame::RuleDone {
                        rule,
                        position,
                        captures: self.captures.len(),
                        outer_error,
                    });
                    work.push(Frame::Expression(&self.grammar.rules[rule].1, position));
                }
                Frame::RuleDone {
                    rule,
                    position,
                    captures,
                    outer_error,
                } => {
                    let weight = self.grammar.nesting_rules[rule];
                    for domain in 0..2 {
                        self.nesting_depth[domain] -= usize::from(weight & (1 << domain) != 0);
                    }
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
                                captures,
                                weight,
                            )?;
                            self.memo
                                .insert(rule, position, Memo::Matched(end, node, local_error));
                            self.captures.push(node);
                        }
                        Err(Failure::Mismatch) => {
                            self.memo.insert(rule, position, Memo::Failed(local_error));
                        }
                        Err(fatal) => return Err(fatal),
                    }
                }
                Frame::Expression(expression, position) => {
                    self.charge(1, position)?;
                    if self.depth >= self.limits.depth {
                        return Err(self.limit(position, "PEG nesting within the parser limit"));
                    }
                    // Lexical atoms have no child frames or captures. Check the
                    // same logical depth/work budget, then finish them here;
                    // an ExpressionDone continuation would only undo the depth.
                    let terminal = match expression {
                        Expression::Keyword(keyword) => {
                            Some(if self.token(position).is_keyword(self.input, keyword) {
                                Ok(position + 1)
                            } else {
                                Err(self.mismatch(position, keyword))
                            })
                        }
                        Expression::Token(kind, expected) => {
                            Some(if self.token(position).kind == *kind {
                                Ok(position + usize::from(*kind != TokenKind::End))
                            } else {
                                Err(self.mismatch(position, expected))
                            })
                        }
                        Expression::Parameter => Some(
                            if matches!(self.token(position).kind, TokenKind::Parameter(_)) {
                                Ok(position + 1)
                            } else {
                                Err(self.mismatch(position, "a positional parameter"))
                            },
                        ),
                        _ => None,
                    };
                    if let Some(terminal) = terminal {
                        result = terminal;
                        continue;
                    }
                    self.depth += 1;
                    work.push(Frame::ExpressionDone(self.captures.len()));
                    match expression {
                        Expression::Keyword(_) | Expression::Token(..) | Expression::Parameter => {
                            unreachable!("lexical atom finished above")
                        }
                        Expression::Rule(rule) => work.push(Frame::Rule(*rule, position)),
                        Expression::Sequence(items) => {
                            result = Ok(position);
                            work.push(Frame::Sequence(items, 0));
                        }
                        Expression::Choice(items) => {
                            result = Err(Failure::Mismatch);
                            work.push(Frame::Choice {
                                items,
                                next: 0,
                                position,
                            });
                        }
                        Expression::Optional(inner) => {
                            work.push(Frame::OptionalDone {
                                position,
                                saved: self.farthest.clone(),
                            });
                            work.push(Frame::Expression(inner, position));
                        }
                        Expression::Repeat(inner) => work.push(Frame::Repeat(inner, position)),
                        Expression::Tight(inner) => {
                            work.push(Frame::TightDone(position));
                            work.push(Frame::Expression(inner, position));
                        }
                        Expression::Ahead(inner, expected) | Expression::Not(inner, expected) => {
                            work.push(Frame::LookDone {
                                position,
                                positive: matches!(expression, Expression::Ahead(..)),
                                expected,
                                saved: self.farthest.take(),
                                captures: self.captures.len(),
                            });
                            work.push(Frame::Expression(inner, position));
                        }
                    }
                }
                Frame::ExpressionDone(captures) => {
                    if result.is_err() {
                        self.captures.truncate(captures);
                    }
                    self.depth -= 1;
                }
                Frame::Sequence(items, next) => {
                    if let Ok(position) = result
                        && let Some(item) = items.get(next)
                    {
                        work.push(Frame::Sequence(items, next + 1));
                        work.push(Frame::Expression(item, position));
                    }
                }
                Frame::Choice {
                    items,
                    next,
                    position,
                } => {
                    if matches!(result, Err(Failure::Mismatch))
                        && let Some(item) = items.get(next)
                    {
                        work.push(Frame::Choice {
                            items,
                            next: next + 1,
                            position,
                        });
                        work.push(Frame::Expression(item, position));
                    }
                }
                Frame::OptionalDone { position, saved } => {
                    if matches!(result, Err(Failure::Mismatch)) {
                        self.restore_absence(position, saved);
                        result = Ok(position);
                    }
                }
                Frame::Repeat(inner, position) => {
                    work.push(Frame::RepeatDone {
                        inner,
                        position,
                        saved: self.farthest.clone(),
                    });
                    work.push(Frame::Expression(inner, position));
                }
                Frame::RepeatDone {
                    inner,
                    position,
                    saved,
                } => match result {
                    Ok(next) if next > position => work.push(Frame::Repeat(inner, next)),
                    Ok(_) => return Err(self.limit(position, "a consuming PEG repetition")),
                    Err(Failure::Mismatch) => {
                        self.restore_absence(position, saved);
                        result = Ok(position);
                    }
                    Err(fatal) => return Err(fatal),
                },
                Frame::TightDone(start) => {
                    if let Ok(end) = result {
                        for position in start.saturating_add(1)..end {
                            self.charge(1, position)?;
                            if self.token(position - 1).span.end != self.token(position).span.start
                            {
                                result = Err(self.mismatch(position, "adjacent tokens"));
                                break;
                            }
                        }
                    }
                }
                Frame::LookDone {
                    position,
                    positive,
                    expected,
                    saved,
                    captures,
                } => {
                    self.captures.truncate(captures);
                    self.farthest = saved;
                    result = match result {
                        Ok(_) if positive => Ok(position),
                        Err(Failure::Mismatch) if !positive => Ok(position),
                        Ok(_) | Err(Failure::Mismatch) => Err(self.mismatch(position, expected)),
                        Err(fatal) => return Err(fatal),
                    };
                }
            }
        }
        result.map(|end| (end, self.captures.pop().expect("successful root capture")))
    }

    fn restore_absence(&mut self, position: usize, saved: Option<Diagnostic>) {
        if self
            .farthest
            .as_ref()
            .is_none_or(|error| self.diagnostic_start(error) <= self.token(position).span.start)
        {
            self.farthest = saved;
        }
    }
}

mod memo;
#[cfg(test)]
mod tests;
mod validation;

/// Compose the executable relational grammar with additional HawDB statement
/// rules. These internal building blocks are not a host query API.
pub fn sql_grammar(
    extra: Vec<(&'static str, Expression)>,
    root: &'static str,
) -> Result<Grammar, String> {
    crate::parser::frontend::grammar::build_with_rules(extra, &[root], true)
}

pub fn sql_expression(tree: &ParseTree, tokens: &[Token], node: NodeId) -> crate::ExpressionSyntax {
    crate::parser::expression::Fold { tree, tokens }.expression(node)
}

pub fn sql_select(
    input: &str,
    tree: &ParseTree,
    tokens: &[Token],
    node: NodeId,
) -> crate::PostgresSelectSyntax {
    crate::parser::frontend::select_from_tree(input, tokens, tree, node)
}
