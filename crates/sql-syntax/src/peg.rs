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
use std::num::NonZeroUsize;
use std::ops::Range;

use crate::{Span, SyntaxError, SyntaxErrorCode, Token, TokenKind};

use memo::MemoTable;

pub type NodeId = usize;
const MAX_RULES: usize = 256;
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
    rules: Vec<&'static str>,
    names: HashMap<&'static str, usize>,
    nesting_rules: Vec<u8>,
    nesting_limits: [(usize, SyntaxErrorCode); 2],
    program: program::Program,
}

impl Grammar {
    pub fn compile(rules: Vec<(&'static str, Expression)>) -> Result<Self, String> {
        if rules.len() > MAX_RULES {
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
        Self::validate(&rules)?;
        let (program, rules) = program::Program::compile(rules);
        let grammar = Self {
            rules,
            names,
            nesting_rules,
            nesting_limits: [(usize::MAX, SyntaxErrorCode::ParserLimitExceeded); 2],
            program,
        };
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
        let mut memo = MemoTable::new(self.rules.len());
        let mut parser = Matcher {
            grammar: self,
            input,
            tokens,
            limits,
            memo: &mut memo,
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
// Borrow the grammar's static label field through a thin pointer while matching;
// rendered errors and parse trees remain independent of the grammar lifetime.
#[derive(Clone, Copy)]
struct Diagnostic<'a> {
    position: usize,
    expected: &'a &'static str,
}

// Reserve zero for Option's empty slot; arena-derived node IDs leave room
// for the pending and failed states without another enum discriminant.
struct Memo<'a> {
    state: NonZeroUsize,
    end: usize,
    error: Option<Diagnostic<'a>>,
}

impl<'a> Memo<'a> {
    const PENDING: usize = 1;
    const FAILED: usize = 2;
    const NODE_OFFSET: usize = 3;

    fn pending() -> Self {
        Self {
            state: NonZeroUsize::MIN,
            end: 0,
            error: None,
        }
    }

    fn failed(error: Option<Diagnostic<'a>>) -> Self {
        Self {
            state: NonZeroUsize::new(Self::FAILED).expect("failed state is nonzero"),
            end: 0,
            error,
        }
    }

    fn matched(end: usize, node: NodeId, error: Option<Diagnostic<'a>>) -> Self {
        // Node IDs come from Vec<Node>, whose allocation limit is below
        // isize::MAX / size_of::<Node>(); this adds no parser admission cap.
        let state = node
            .checked_add(Self::NODE_OFFSET)
            .expect("arena node ID fits memo state");
        Self {
            state: NonZeroUsize::new(state).expect("matched state is nonzero"),
            end,
            error,
        }
    }

    #[cfg(test)]
    fn value(&self) -> MemoState<'a> {
        match self.state.get() {
            Self::PENDING => MemoState::Pending,
            Self::FAILED => MemoState::Failed(self.error),
            node => MemoState::Matched(self.end, node - Self::NODE_OFFSET, self.error),
        }
    }
}

#[cfg(test)]
enum MemoState<'a> {
    Pending,
    Matched(usize, NodeId, Option<Diagnostic<'a>>),
    Failed(Option<Diagnostic<'a>>),
}

enum Failure {
    Mismatch,
    Fatal(SyntaxError),
}

// Compiled instructions determine the frame kind: CALL/RETURN use all fields;
// ENTER/LEAVE use position, captures and outer_error. One stack preserves nested
// lifetimes without allocating separate rule and expression stacks.
struct ProgramFrame<'a> {
    rule: usize,
    position: usize,
    captures: usize,
    outer_error: Option<Diagnostic<'a>>,
    pc: usize,
}

struct Matcher<'a, 'm> {
    grammar: &'a Grammar,
    input: &'a str,
    tokens: &'a [Token],
    limits: Limits,
    memo: &'m mut MemoTable<'a>,
    nodes: Vec<Node>,
    edges: Vec<NodeId>,
    // Only captures roll back. Memoized nodes and edges remain append-only
    // so a later alternative can reuse their IDs.
    captures: Vec<NodeId>,
    steps: usize,
    depth: usize,
    nesting_depth: [usize; 2],
    farthest: Option<Diagnostic<'a>>,
    #[cfg(test)]
    memo_hits: usize,
}

impl<'a> Matcher<'a, '_> {
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

    fn diagnostic_start(&self, diagnostic: &Diagnostic<'a>) -> usize {
        self.token(diagnostic.position).span.start
    }

    fn render(&self, diagnostic: &Diagnostic<'a>) -> SyntaxError {
        self.error(diagnostic.position, diagnostic.expected)
    }

    fn remember(&mut self, error: Diagnostic<'a>) {
        if let Some(old) = self.farthest
            && (error.position == old.position
                || self.diagnostic_start(&error) <= self.diagnostic_start(&old))
        {
            return;
        }
        self.farthest = Some(error);
    }

    fn mismatch(&mut self, position: usize, expected: &'a &'static str) {
        self.remember(Diagnostic { position, expected });
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
        use program::*;
        let mut work: Vec<ProgramFrame<'a>> = Vec::new();
        let mut pc = 0;
        let mut position = start;
        let mut matched = false;
        loop {
            let (opcode, argument) = self.grammar.program.decode(pc);
            match opcode {
                HALT => {
                    return if matched {
                        Ok((
                            position,
                            self.captures.pop().expect("successful root capture"),
                        ))
                    } else {
                        Err(Failure::Mismatch)
                    }
                }
                CALL => {
                    let rule = if argument == ROOT_CALL {
                        root
                    } else {
                        argument
                    };
                    self.charge(1, position)?;
                    if let Some(memo) = self.memo.get(rule, position) {
                        #[cfg(test)]
                        {
                            self.memo_hits += 1;
                        }
                        match memo.state.get() {
                            Memo::PENDING => {
                                return Err(
                                    self.limit(position, "a PEG grammar without left recursion")
                                )
                            }
                            Memo::FAILED => {
                                let error = memo.error;
                                if let Some(error) = error {
                                    self.remember(error);
                                }
                                matched = false;
                            }
                            state => {
                                let node = state - Memo::NODE_OFFSET;
                                for domain in 0..2 {
                                    if self.nesting_depth[domain]
                                        .saturating_add(self.nodes[node].nesting_height[domain])
                                        > self.grammar.nesting_limits[domain].0
                                    {
                                        return Err(self.nesting_limit(position, domain));
                                    }
                                }
                                let (end, error) = (memo.end, memo.error);
                                if let Some(error) = error {
                                    self.remember(error);
                                }
                                self.captures.push(node);
                                position = end;
                                matched = true;
                            }
                        }
                    } else {
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
                        self.memo.insert(rule, position, Memo::pending());
                        let outer_error = self.farthest.take();
                        for domain in 0..2 {
                            self.nesting_depth[domain] += usize::from(weight & (1 << domain) != 0);
                        }
                        work.push(ProgramFrame {
                            rule,
                            position,
                            captures: self.captures.len(),
                            outer_error,
                            pc: pc + 1,
                        });
                        pc = self.grammar.program.entries[rule];
                        continue;
                    }
                }
                RETURN => {
                    let frame = work.pop().expect("rule return");
                    let weight = self.grammar.nesting_rules[frame.rule];
                    for domain in 0..2 {
                        self.nesting_depth[domain] -= usize::from(weight & (1 << domain) != 0);
                    }
                    let local_error = self.farthest.take();
                    self.farthest = frame.outer_error;
                    if let Some(error) = local_error {
                        self.remember(error);
                    }
                    if matched {
                        let node = self.push(
                            Some(self.grammar.rules[frame.rule]),
                            frame.position,
                            position,
                            frame.captures,
                            weight,
                        )?;
                        self.memo.insert(
                            frame.rule,
                            frame.position,
                            Memo::matched(position, node, local_error),
                        );
                        self.captures.push(node);
                    } else {
                        self.memo
                            .insert(frame.rule, frame.position, Memo::failed(local_error));
                    }
                    pc = frame.pc;
                    continue;
                }
                ENTER => {
                    self.charge(1, position)?;
                    if self.depth >= self.limits.depth {
                        return Err(self.limit(position, "PEG nesting within the parser limit"));
                    }
                    self.depth += 1;
                    let saved = if argument == LOOK_SCOPE {
                        self.farthest.take()
                    } else {
                        self.farthest
                    };
                    work.push(ProgramFrame {
                        rule: 0,
                        position,
                        captures: self.captures.len(),
                        outer_error: saved,
                        pc: 0,
                    });
                    matched = argument != CHOICE_SCOPE;
                }
                LEAVE => {
                    let scope = work.pop().expect("expression scope");
                    if !matched {
                        self.captures.truncate(scope.captures);
                    }
                    self.depth -= 1;
                }
                KEYWORD | TOKEN | PARAMETER => {
                    self.charge(1, position)?;
                    if self.depth >= self.limits.depth {
                        return Err(self.limit(position, "PEG nesting within the parser limit"));
                    }
                    let (accepts, advances, expected) = match opcode {
                        KEYWORD => {
                            let keyword = &self.grammar.program.keywords[argument];
                            (
                                self.token(position).is_keyword(self.input, keyword),
                                true,
                                keyword,
                            )
                        }
                        TOKEN => {
                            let (kind, expected) = &self.grammar.program.tokens[argument];
                            (
                                self.token(position).kind == *kind,
                                *kind != TokenKind::End,
                                expected,
                            )
                        }
                        _ => (
                            matches!(self.token(position).kind, TokenKind::Parameter(_)),
                            true,
                            &"a positional parameter",
                        ),
                    };
                    matched = accepts;
                    if accepts {
                        position += usize::from(advances);
                    } else {
                        self.mismatch(position, expected);
                    }
                }
                IF_FAILED => {
                    if !matched {
                        pc = argument;
                        continue;
                    }
                }
                CHOICE_NEXT => {
                    if matched {
                        pc = argument;
                        continue;
                    }
                    position = work.last().expect("choice scope").position;
                }
                OPTIONAL_END => {
                    if !matched {
                        let scope = work.last().expect("optional scope");
                        self.restore_absence(scope.position, scope.outer_error);
                        position = scope.position;
                        matched = true;
                    }
                }
                REPEAT_NEXT => {
                    let scope = work.last_mut().expect("repeat scope");
                    if matched {
                        if position <= scope.position {
                            return Err(self.limit(scope.position, "a consuming PEG repetition"));
                        }
                        scope.position = position;
                        scope.outer_error = self.farthest;
                        pc = argument;
                        continue;
                    }
                    self.restore_absence(scope.position, scope.outer_error);
                    position = scope.position;
                    matched = true;
                }
                TIGHT_END => {
                    if matched {
                        for token in
                            work.last().expect("tight scope").position.saturating_add(1)..position
                        {
                            self.charge(1, token)?;
                            if self.token(token - 1).span.end != self.token(token).span.start {
                                self.mismatch(token, &"adjacent tokens");
                                matched = false;
                                break;
                            }
                        }
                    }
                }
                LOOK_END => {
                    let scope = work.last().expect("lookahead scope");
                    self.captures.truncate(scope.captures);
                    self.farthest = scope.outer_error;
                    position = scope.position;
                    let (expected, positive) = &self.grammar.program.looks[argument];
                    matched = matched == *positive;
                    if !matched {
                        self.mismatch(position, expected);
                    }
                }
                _ => unreachable!("compiled PEG opcode"),
            }
            pc += 1;
        }
    }

    fn restore_absence(&mut self, position: usize, saved: Option<Diagnostic<'a>>) {
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
mod program;
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
