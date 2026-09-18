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

mod ast;
mod parameters;
mod parser;
mod pgq;
#[doc(hidden)]
pub mod template_cache;
pub mod timing;

pub mod syntax {
    pub use hawdb_sql_syntax::*;
}

pub use ast::*;
pub use parameters::{prepare_postgres_sql, PostgresParameterMetadata, PreparedPostgresStatement};
pub use parser::parse_postgres_sql;
pub use pgq::*;
pub use template_cache::{PreparedRelationalSql, RelationalPlanTemplateCache};
pub use timing::RelationalSqlStageTimings;

#[cfg(test)]
mod tests;
