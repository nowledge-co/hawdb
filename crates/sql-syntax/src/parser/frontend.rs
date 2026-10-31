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
    Ok(Transformer {
        input,
        tree: &tree,
        tokens: &tokens,
    }
    .output(root))
}

struct Transformer<'a> {
    input: &'a str,
    tree: &'a ParseTree,
    tokens: &'a [Token],
}

pub(super) enum Output {
    Explain {
        analyze: bool,
        select: Box<crate::PostgresSelectSyntax>,
    },
    Select(Box<crate::PostgresSelectSyntax>),
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
