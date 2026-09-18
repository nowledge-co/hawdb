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

//! In-progress HawDB-owned PostgreSQL SQL/PGQ syntax frontend.
//!
//! This crate owns tokens, byte spans, structured syntax errors, and syntax ASTs.
//! Semantic binding and logical-plan lowering belong to `hawdb-sql`.
//!
//! This frontend is not yet selected by `Database::query_sql*`; the existing
//! relational execution path still uses upstream `sqlparser`. Syntax acceptance
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
mod span;
mod token;

pub use ast::*;
pub use error::{SyntaxError, SyntaxErrorCode};
pub use lexer::{tokenize, tokenize_with_config, LexerConfig};
pub use parser::{
    parse_graph_table, parse_pgq_statement, parse_postgres_select, parse_postgres_statement,
};
pub use span::Span;
pub use token::{Token, TokenKind};
