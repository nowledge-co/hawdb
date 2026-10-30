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

mod grammar;
mod native;
mod transform;

use native::Value;

pub(super) fn parse(input: &str, root: &'static str) -> Result<Output, SyntaxError> {
    static GRAMMAR: OnceLock<Grammar<Value>> = OnceLock::new();
    let grammar = GRAMMAR.get_or_init(|| grammar::build().expect("valid built-in SQL/PGQ grammar"));
    let tokens = tokenize(input)?;
    let tree = grammar.parse(input, &tokens, root, Limits::default())?;
    Ok(Transformer {
        tree,
        tokens: &tokens,
    }
    .output(root))
}

struct Transformer<'a> {
    tree: ParseTree<Value>,
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
