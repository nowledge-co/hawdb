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

//! HawDB-owned PostgreSQL SQL/PGQ syntax frontend.
//!
//! This crate owns tokens, byte spans, structured syntax errors, and syntax ASTs.
//! Statement envelopes use a bounded token PEG with packrat memoization, composed
//! SQL/PGQ rules, and transformation into the existing syntax AST. Native terminals
//! retain the bounded Pratt expression and graph-pattern parsers.
//! Semantic binding and logical-plan lowering belong to `hawdb-sql`.
//! The internal PEG machinery is also reused by `hawdb-cypher`, with its own
//! lexer and native terminals; SQL tokenization rules do not apply to Cypher.
//!
//! `Database::query_sql*` selects the relational PEG entry for SELECT and EXPLAIN
//! SELECT. Relational DDL/DML still use upstream `sqlparser`. PGQ syntax acceptance
//! alone does not qualify a statement family for production execution.
//!
//! The [SQL/PGQ specification] defines the compatibility contract. The
//! [implementation and routing checklist] tracks the remaining parser/binder
//! differential coverage, catalog durability, and shared query-pipeline
//! qualification required before enabling production routing. Parser selection
//! must be explicit by statement family, never a retry after another parser fails.
//!
//! [SQL/PGQ specification]: https://github.com/nowledge-co/hawdb/blob/main/docs/specs/POSTGRES_SQL_PGQ_SPEC.md
//! [implementation and routing checklist]: https://github.com/nowledge-co/hawdb/blob/main/TODO.md#p1-postgresql-sqlpgq-compatibility

mod ast;
mod error;
mod lexer;
mod parser;
/// Shared bounded token PEG machinery for HawDB syntax frontends.
/// This implementation module is not a host-facing query API.
#[doc(hidden)]
pub mod peg;
mod span;
mod token;

pub use ast::*;
pub use error::{SyntaxError, SyntaxErrorCode};
pub use lexer::{first_token, tokenize, tokenize_with_config, LexerConfig};
pub use parser::{
    parse_graph_table, parse_pgq_statement, parse_postgres_select, parse_postgres_statement,
    parse_relational_statement,
};
pub use span::Span;
pub use token::{Token, TokenKind};
