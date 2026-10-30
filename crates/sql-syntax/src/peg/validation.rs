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

use std::collections::HashMap;

use super::{Expression, Grammar};
use crate::TokenKind;

impl<T> Grammar<T> {
    pub(super) fn validate(&self) -> Result<(), String> {
        let mut nullable = vec![false; self.rules.len()];
        loop {
            let mut changed = false;
            for (index, (_, expression)) in self.rules.iter().enumerate() {
                if !nullable[index] && Self::nullable(expression, &nullable) {
                    nullable[index] = true;
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
        let mut edges = Vec::new();
        for (_, expression) in &self.rules {
            Self::check_repetition(expression, &nullable)?;
            let mut next = Vec::new();
            Self::leading_rules(expression, &nullable, &mut next);
            edges.push(next);
        }
        let mut visiting = vec![false; self.rules.len()];
        let mut visited = vec![false; self.rules.len()];
        for root in 0..self.rules.len() {
            if cycle(root, &edges, &mut visiting, &mut visited) {
                return Err(format!(
                    "left recursion in PEG rule: {}",
                    self.rules[root].0
                ));
            }
        }
        Ok(())
    }

    fn nullable(expression: &Expression<T, usize>, nullable: &[bool]) -> bool {
        match expression {
            Expression::Rule(rule) => nullable[*rule],
            Expression::Sequence(items) => items.iter().all(|item| Self::nullable(item, nullable)),
            Expression::Choice(items) => items.iter().any(|item| Self::nullable(item, nullable)),
            Expression::Optional(_)
            | Expression::Repeat(_)
            | Expression::Ahead(_, _)
            | Expression::Not(_, _) => true,
            Expression::Native { nullable, .. } => *nullable,
            Expression::Token(TokenKind::End, _) => true,
            _ => false,
        }
    }

    fn check_repetition(
        expression: &Expression<T, usize>,
        nullable: &[bool],
    ) -> Result<(), String> {
        match expression {
            Expression::Repeat(item) if Self::nullable(item, nullable) => {
                Err("nullable PEG repetition".into())
            }
            Expression::Choice(items) | Expression::Sequence(items) => {
                for item in items {
                    Self::check_repetition(item, nullable)?;
                }
                Ok(())
            }
            Expression::Optional(item)
            | Expression::Repeat(item)
            | Expression::Ahead(item, _)
            | Expression::Not(item, _) => Self::check_repetition(item, nullable),
            _ => Ok(()),
        }
    }

    fn leading_rules(expression: &Expression<T, usize>, nullable: &[bool], edges: &mut Vec<usize>) {
        match expression {
            Expression::Rule(rule) => edges.push(*rule),
            Expression::Sequence(items) => {
                for item in items {
                    Self::leading_rules(item, nullable, edges);
                    if !Self::nullable(item, nullable) {
                        break;
                    }
                }
            }
            Expression::Choice(items) => {
                for item in items {
                    Self::leading_rules(item, nullable, edges);
                }
            }
            Expression::Optional(item)
            | Expression::Repeat(item)
            | Expression::Ahead(item, _)
            | Expression::Not(item, _) => {
                Self::leading_rules(item, nullable, edges);
            }
            _ => (),
        }
    }
}

fn cycle(node: usize, edges: &[Vec<usize>], visiting: &mut [bool], visited: &mut [bool]) -> bool {
    if visiting[node] {
        return true;
    }
    if visited[node] {
        return false;
    }
    visiting[node] = true;
    for child in &edges[node] {
        if cycle(*child, edges, visiting, visited) {
            return true;
        }
    }
    visiting[node] = false;
    visited[node] = true;
    false
}

impl<T> Expression<T> {
    pub(super) fn resolve(
        self,
        names: &HashMap<&'static str, usize>,
    ) -> Result<Expression<T, usize>, String> {
        Ok(match self {
            Self::Parameter => Expression::Parameter,
            Self::Keyword(keyword) => Expression::Keyword(keyword),
            Self::Token(kind, expected) => Expression::Token(kind, expected),
            Self::Rule(name) => Expression::Rule(
                *names
                    .get(name)
                    .ok_or_else(|| format!("unknown PEG rule: {name}"))?,
            ),
            Self::Sequence(items) => Expression::Sequence(
                items
                    .into_iter()
                    .map(|item| item.resolve(names))
                    .collect::<Result<_, _>>()?,
            ),
            Self::Choice(items) => Expression::Choice(
                items
                    .into_iter()
                    .map(|item| item.resolve(names))
                    .collect::<Result<_, _>>()?,
            ),
            Self::Optional(item) => Expression::Optional(Box::new(item.resolve(names)?)),
            Self::Repeat(item) => Expression::Repeat(Box::new(item.resolve(names)?)),
            Self::Ahead(item, expected) => {
                Expression::Ahead(Box::new(item.resolve(names)?), expected)
            }
            Self::Not(item, expected) => Expression::Not(Box::new(item.resolve(names)?), expected),
            Self::Native { matcher, nullable } => Expression::Native { matcher, nullable },
        })
    }
}
