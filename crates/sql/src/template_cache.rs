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

mod mutation_retention;

const MAX_MUTATION_TEMPLATES: usize = 256;
const MAX_MUTATION_TEMPLATE_BYTES: usize = 4 * 1024 * 1024;
const MAX_MUTATION_ENTRY_BYTES: usize = 256 * 1024;

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

/// Caches parsed, parameter-neutral relational statement templates.
///
/// Templates deliberately contain no schema binding, statistics-derived
/// estimates, access paths, or execution properties. Every lookup therefore
/// remains valid across data, statistics, schema, and snapshot changes;
/// planning still runs against the current relational state after the
/// template is cloned.
///
/// DML has a separate pool capped at 256 entries, 4 MiB total requested heap,
/// and 256 KiB per entry. The existing entry configuration can reduce or disable
/// that pool. DDL and branch commands bypass retention; `stats` keeps reporting
/// the original data-query pool.
#[doc(hidden)]
#[derive(Debug)]
pub struct RelationalPlanTemplateCache {
    entries: Mutex<TemplateCaches>,
}

#[derive(Debug)]
struct TemplateCaches {
    queries: LfuCache<String, Arc<PreparedPostgresStatement>>,
    mutations: LfuCache<String, Arc<PreparedPostgresStatement>>,
}

impl RelationalPlanTemplateCache {
    pub fn new(max_entries: Option<usize>) -> Self {
        Self {
            entries: Mutex::new(TemplateCaches {
                queries: LfuCache::new(max_entries),
                mutations: LfuCache::with_byte_limits(
                    Some(
                        max_entries
                            .unwrap_or(MAX_MUTATION_TEMPLATES)
                            .min(MAX_MUTATION_TEMPLATES),
                    ),
                    Some(MAX_MUTATION_TEMPLATE_BYTES),
                    Some(MAX_MUTATION_ENTRY_BYTES),
                ),
            }),
        }
    }

    pub fn prepare(&self, sql: &str) -> Result<PreparedRelationalSql> {
        let key = sql.to_string();
        {
            let mut entries = self.entries();
            let template = entries.queries.get(&key).or_else(|| {
                let template = entries.mutations.get(&key)?;
                entries.queries.record_bypass();
                Some(template)
            });
            if let Some(template) = template {
                return Ok(PreparedRelationalSql {
                    template,
                    parse_nanos: 0,
                });
            }
        }

        // Parse outside the cache lock. Concurrent misses may duplicate work,
        // but unrelated statements never serialize behind the parser.
        let started = Instant::now();
        let template = Arc::new(prepare_postgres_sql(sql)?);
        let parse_nanos = elapsed_nanos(started);
        let mut entries = self.entries();
        if is_cacheable_query(&template.statement) {
            entries.queries.insert(key, Arc::clone(&template));
        } else {
            // Mutations and project-catalog commands must not evict reusable
            // data-query templates from the selected branch's cache.
            entries.queries.record_bypass();
            if matches!(
                template.statement,
                SqlStatement::Insert(_) | SqlStatement::Update(_) | SqlStatement::Delete(_)
            ) {
                if let Some(charge) = mutation_retention::heap_charge(&key, &template) {
                    entries
                        .mutations
                        .insert_weighted(key, Arc::clone(&template), charge);
                } else {
                    entries.mutations.record_rejected_admission();
                }
            }
        }
        Ok(PreparedRelationalSql {
            template,
            parse_nanos,
        })
    }

    pub fn stats(&self) -> PlanCacheStats {
        // Preserve the existing data-query statistics contract. Mutation
        // retention is isolated and parse reuse is visible in stage timings.
        self.entries().queries.stats()
    }

    fn entries(&self) -> MutexGuard<'_, TemplateCaches> {
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
        | SqlStatement::CreateMaterializedView(_)
        | SqlStatement::DropMaterializedView(_)
        | SqlStatement::Update(_)
        | SqlStatement::Delete(_)
        | SqlStatement::CreateTable(_)
        | SqlStatement::CreateIndex(_)
        | SqlStatement::AlterTableAddColumn(_)
        | SqlStatement::Branch(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branch_inspection_does_not_replace_data_query_templates() {
        let cache = RelationalPlanTemplateCache::new(Some(1));
        let first = cache.prepare("SELECT id FROM items WHERE id = $1").unwrap();
        let before = cache.stats();
        for sql in [
            "SHOW BRANCHES LIMIT 2",
            "SHOW BRANCHES LIMIT 2",
            "SHOW CURRENT BRANCH",
        ] {
            assert!(matches!(
                cache.prepare(sql).unwrap().statement(),
                SqlStatement::Branch(_)
            ));
        }
        let inspected = cache.stats();
        assert_eq!(inspected.entries, before.entries);
        assert_eq!(inspected.hits, before.hits);
        let reused = cache.prepare("SELECT id FROM items WHERE id = $1").unwrap();
        assert!(Arc::ptr_eq(&first.template, &reused.template));
        assert_eq!(cache.stats().hits, before.hits + 1);
    }

    #[test]
    fn mutation_templates_reuse_unbound_parameters_without_evicting_queries() {
        let select = "SELECT id FROM items WHERE id = $1";
        for sql in [
            "INSERT INTO items (id, body) VALUES ($1, $2)",
            "INSERT INTO items (id, body) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET body = EXCLUDED.body",
            "UPDATE items SET body = $1 WHERE id = $2",
            "DELETE FROM items WHERE id = $1",
        ] {
            let cache = RelationalPlanTemplateCache::new(Some(1));
            let query = cache.prepare(select).unwrap();
            let first = cache.prepare(sql).unwrap();
            let reused = cache.prepare(sql).unwrap();
            assert!(Arc::ptr_eq(&first.template, &reused.template), "{sql}");
            assert_eq!(reused.parse_nanos, 0);
            assert_eq!(first.template.parameters[0].position, 1);
            let entries = cache.entries();
            let mutations = entries.mutations.stats();
            assert_eq!(mutations.entries, 1);
            assert_eq!(mutations.hits, 1);
            assert_eq!(mutations.evictions, 0);
            assert!(mutations.charged_bytes.unwrap() <= MAX_MUTATION_TEMPLATE_BYTES);
            drop(entries);
            assert!(Arc::ptr_eq(
                &query.template,
                &cache.prepare(select).unwrap().template
            ));
        }
    }

    #[test]
    fn mutation_template_retention_refuses_oversize_and_disabled_caches() {
        let cache = RelationalPlanTemplateCache::new(None);
        let small = "UPDATE items SET body = $1 WHERE id = $2";
        let retained = cache.prepare(small).unwrap();
        let large = format!(
            "INSERT INTO items (body) VALUES ('{}')",
            "x".repeat(MAX_MUTATION_ENTRY_BYTES)
        );
        let first = cache.prepare(&large).unwrap();
        let second = cache.prepare(&large).unwrap();
        assert!(!Arc::ptr_eq(&first.template, &second.template));
        assert!(Arc::ptr_eq(
            &retained.template,
            &cache.prepare(small).unwrap().template
        ));
        assert_eq!(cache.entries().mutations.stats().rejected_admissions, 2);

        let disabled = RelationalPlanTemplateCache::new(Some(0));
        let first = disabled.prepare(small).unwrap();
        let second = disabled.prepare(small).unwrap();
        assert!(!Arc::ptr_eq(&first.template, &second.template));
        assert_eq!(disabled.entries().mutations.stats().entries, 0);
    }

    #[test]
    fn mutation_templates_bound_churn_and_share_immutable_concurrent_hits() {
        let cache = Arc::new(RelationalPlanTemplateCache::new(None));
        for id in 0..MAX_MUTATION_TEMPLATES + 20 {
            cache
                .prepare(&format!("DELETE FROM items WHERE id = {id}"))
                .unwrap();
        }
        let stats = cache.entries().mutations.stats();
        assert_eq!(stats.entries, MAX_MUTATION_TEMPLATES);
        assert_eq!(stats.evictions, 20);
        assert!(stats.charged_bytes.unwrap() <= MAX_MUTATION_TEMPLATE_BYTES);
        let sql = "UPDATE items SET body = $1 WHERE id = $2";
        let template = cache.prepare(sql).unwrap().template;
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let cache = &cache;
                let template = &template;
                scope.spawn(move || {
                    for _ in 0..32 {
                        assert!(Arc::ptr_eq(template, &cache.prepare(sql).unwrap().template));
                    }
                });
            }
        });
    }
}
