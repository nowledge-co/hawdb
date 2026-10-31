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

use crate::ast::*;
use hawdb_core::{HawDBError, Result, Value};

mod branch;
#[cfg(test)]
mod clause_tests;
mod clauses;
#[cfg(test)]
mod compatibility_tests;
mod full;
mod owned;

pub fn parse_postgres_sql(input: &str) -> Result<SqlStatement> {
    if let Some(statement) = branch::parse_branch_statement(input)? {
        return Ok(statement);
    }
    let token = hawdb_sql_syntax::first_token(input)
        .map_err(|error| HawDBError::Parse(error.to_string()))?;
    if token.is_keyword(input, "SELECT") || token.is_keyword(input, "EXPLAIN") {
        return owned::parse(input);
    }
    full::parse(input)
}

#[derive(Clone, Copy)]
pub(super) enum ExpressionPosition {
    Predicate,
    Having,
    HavingScalar,
    Scalar,
    Column,
    Value,
}

fn lower_number(raw: &str) -> Result<Value> {
    if raw.contains('.') {
        raw.parse::<f64>().map(Value::Float).map_err(|error| {
            HawDBError::Semantic(format!("invalid PostgreSQL number {raw}: {error}"))
        })
    } else {
        raw.parse::<i64>().map(Value::Int).map_err(|error| {
            HawDBError::Semantic(format!("invalid PostgreSQL integer {raw}: {error}"))
        })
    }
}
