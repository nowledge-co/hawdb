use crate::error::Result;
use crate::relational_sql::timing::elapsed_nanos;
use crate::sql::SqlStatement;
use skein_plan_cache::{LfuCache, PlanCacheStats};
use skein_sql::PreparedPostgresStatement;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

#[derive(Debug, Clone)]
pub(crate) struct PreparedRelationalSql {
    pub(crate) template: Arc<PreparedPostgresStatement>,
    pub(crate) parse_nanos: u64,
}

impl PreparedRelationalSql {
    pub(crate) fn statement(&self) -> &SqlStatement {
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
#[derive(Debug)]
pub(crate) struct RelationalPlanTemplateCache {
    entries: Mutex<LfuCache<String, Arc<PreparedPostgresStatement>>>,
}

impl RelationalPlanTemplateCache {
    pub(crate) fn new(max_entries: Option<usize>) -> Self {
        Self {
            entries: Mutex::new(LfuCache::new(max_entries)),
        }
    }

    pub(crate) fn prepare(&self, sql: &str) -> Result<PreparedRelationalSql> {
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
        let template = Arc::new(skein_sql::prepare_postgres_sql(sql)?);
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

    pub(crate) fn stats(&self) -> PlanCacheStats {
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
