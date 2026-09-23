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

use crate::timing::elapsed_nanos;
use crate::{prepare_postgres_sql, PreparedPostgresStatement, SqlStatement};
use hawdb_core::time::Instant;
use hawdb_core::Result;
use hawdb_plan_cache::{LfuCache, PlanCacheStats};
use std::sync::{Arc, Mutex, MutexGuard};

#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct PreparedRelationalSql {
    pub template: Arc<PreparedPostgresStatement>,
    pub parse_nanos: u64,
}

impl PreparedRelationalSql {
    pub fn statement(&self) -> &SqlStatement {
        &self.template.statement
    }
}

/// Caches only parsed, parameter-neutral relational query templates.
///
/// Templates deliberately contain no schema binding, statistics-derived
/// estimates, access paths, or execution properties. Every lookup therefore
/// remains valid across data, statistics, schema, and snapshot changes;
/// planning still runs against the current relational state after the
/// template is cloned.
#[doc(hidden)]
#[derive(Debug)]
pub struct RelationalPlanTemplateCache {
    entries: Mutex<LfuCache<String, Arc<PreparedPostgresStatement>>>,
}

impl RelationalPlanTemplateCache {
    pub fn new(max_entries: Option<usize>) -> Self {
        Self {
            entries: Mutex::new(LfuCache::new(max_entries)),
        }
    }

    pub fn prepare(&self, sql: &str) -> Result<PreparedRelationalSql> {
        let key = sql.to_string();
        if let Some(template) = self.entries().get(&key) {
            return Ok(PreparedRelationalSql {
                template,
                parse_nanos: 0,
            });
        }

        // Parse outside the cache lock. Concurrent misses may duplicate work,
        // but unrelated statements never serialize behind the parser.
        let started = Instant::now();
        let template = Arc::new(prepare_postgres_sql(sql)?);
        let parse_nanos = elapsed_nanos(started);
        let mut entries = self.entries();
        if is_cacheable_query(&template.statement) {
            entries.insert(key, Arc::clone(&template));
        } else {
            // Literal-heavy mutations must not evict reusable query templates.
            entries.record_bypass();
        }
        Ok(PreparedRelationalSql {
            template,
            parse_nanos,
        })
    }

    pub fn stats(&self) -> PlanCacheStats {
        self.entries().stats()
    }

    fn entries(&self) -> MutexGuard<'_, LfuCache<String, Arc<PreparedPostgresStatement>>> {
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn is_cacheable_query(statement: &SqlStatement) -> bool {
    match statement {
        SqlStatement::Select(_) => true,
        SqlStatement::Explain(explain) => {
            matches!(explain.statement.as_ref(), SqlStatement::Select(_))
        }
        SqlStatement::Insert(_)
        | SqlStatement::Update(_)
        | SqlStatement::Delete(_)
        | SqlStatement::CreateTable(_)
        | SqlStatement::CreateIndex(_)
        | SqlStatement::AlterTableAddColumn(_) => false,
    }
}
