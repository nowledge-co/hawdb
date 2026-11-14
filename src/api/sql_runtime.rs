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

use super::{Database, DatabaseConfig, DatabaseInstanceIdentity};
use crate::relational_sql::{PreparedRelationalSql, RelationalPlanTemplateCache};
use crate::{HawDBError, Result};
use hawdb_qos::{RuntimeRetainedMemory, RuntimeWorkKind, RuntimeWorkRequest};
use std::sync::Arc;

/// Parameter-neutral SQL templates contain no schema bindings or source pins.
pub(crate) struct RuntimeSqlPlanningSnapshot {
    identity: DatabaseInstanceIdentity,
    cache: Arc<RelationalPlanTemplateCache>,
    config: DatabaseConfig,
}

pub(crate) struct PreparedRuntimeSql {
    identity: DatabaseInstanceIdentity,
    config: DatabaseConfig,
    statement: PreparedRelationalSql,
    planning_memory: Option<RuntimeRetainedMemory>,
}

impl RuntimeSqlPlanningSnapshot {
    pub(crate) fn prepare(&self, sql: &str) -> Result<PreparedRuntimeSql> {
        Ok(PreparedRuntimeSql {
            identity: self.identity.clone(),
            config: self.config.clone(),
            statement: self.cache.prepare(sql)?,
            planning_memory: None,
        })
    }
}

impl PreparedRuntimeSql {
    pub(crate) fn with_planning_memory(mut self, memory: RuntimeRetainedMemory) -> Self {
        self.planning_memory = Some(memory);
        self
    }
    pub(crate) fn planning_memory_bytes(&self) -> u64 {
        self.planning_memory
            .as_ref()
            .map_or(0, RuntimeRetainedMemory::bytes)
    }
    pub(crate) fn uses_snapshot(&self) -> bool {
        super::concurrent::sql_statement_uses_snapshot(self.statement.statement())
    }
    #[cfg(feature = "tokio-runtime")]
    pub(crate) fn uses_catalog(&self) -> bool {
        matches!(
            self.statement.statement(),
            crate::sql::SqlStatement::Branch(_)
        )
    }
    pub(crate) fn work_request(&self, result_bytes: u64) -> RuntimeWorkRequest {
        // SQL binds access paths to the execution snapshot. Admit the configured
        // working ceiling until relational physical preparation exposes a shared
        // estimator; never inspect a source snapshot while queued for capacity.
        let memory = u64::try_from(self.config.execution_memory.query_memory_bytes.get())
            .unwrap_or(u64::MAX);
        RuntimeWorkRequest::foreground_query(memory, result_bytes)
            .with_kind(if self.uses_snapshot() {
                RuntimeWorkKind::Query
            } else {
                RuntimeWorkKind::Mutation
            })
            .with_io_wave_slots(1)
    }
    pub(crate) fn into_statement(
        self,
        database: &Database,
    ) -> Result<(PreparedRelationalSql, Option<RuntimeRetainedMemory>)> {
        if self.identity != database.runtime.instance_identity() || self.config != database.config {
            return Err(HawDBError::Execution(
                "SQL execution context changed during admission; retry the query".into(),
            ));
        }
        Ok((self.statement, self.planning_memory))
    }
}

impl Database {
    pub(crate) fn runtime_sql_planning_snapshot(&self) -> RuntimeSqlPlanningSnapshot {
        RuntimeSqlPlanningSnapshot {
            identity: self.runtime.instance_identity(),
            cache: self.runtime.peek().map_or_else(
                || Arc::new(RelationalPlanTemplateCache::new(Some(0))),
                |runtime| Arc::clone(&runtime.relational_plan_template_cache),
            ),
            config: self.config.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{QueryOutput, QueryStreamOptions, SqlQueryRequest, Value};

    // This trusted internal boundary already owns the syntax. An invalid
    // diagnostic string makes any accidental parser fallback observable.
    const DIAGNOSTIC_SQL: &str = "/* already prepared SQL */";

    fn exercise_prepared_mutations(
        mut execute: impl FnMut(&str, &[Value], QueryStreamOptions) -> Result<QueryOutput>,
    ) {
        let limits = QueryStreamOptions {
            max_rows: Some(2),
            max_payload_bytes: Some(4096),
        };
        execute(
            "CREATE TABLE items (id BIGINT PRIMARY KEY, counter BIGINT NOT NULL DEFAULT 0)",
            &[],
            limits,
        )
        .unwrap();
        let insert = "INSERT INTO items (id) VALUES ($1) RETURNING id";
        let first = execute(insert, &[Value::Int(1)], limits).unwrap();
        assert_eq!(first.rows.len(), 1);
        assert_eq!(first.rows[0]["id"], Value::Int(1));
        let zero = QueryStreamOptions {
            max_rows: Some(0),
            max_payload_bytes: Some(0),
        };
        assert!(matches!(
            execute(insert, &[Value::Int(2)], zero),
            Err(HawDBError::ReadBudgetExceeded(_))
        ));
        // Refusal must leave the key absent and retain the earlier statement.
        execute("INSERT INTO items (id) VALUES ($1)", &[Value::Int(2)], zero).unwrap();
        execute(
            "UPDATE items SET counter = counter + $1 WHERE id = $2",
            &[Value::Int(7), Value::Int(1)],
            zero,
        )
        .unwrap();
        execute("DELETE FROM items WHERE id = $1", &[Value::Int(2)], zero).unwrap();

        execute(
            "CREATE TABLE events (tenant TEXT NOT NULL, sequence BIGINT NOT NULL) \
             WITH (storage_mode = 'strict_append', partition_key = 'tenant', order_key = 'sequence')",
            &[], limits,
        ).unwrap();
        execute(
            "INSERT INTO events (tenant, sequence) VALUES ($1, $2)",
            &[Value::String("a".into()), Value::Int(1)],
            limits,
        )
        .unwrap();
    }

    fn assert_published_mutations(database: &mut Database) {
        let rows = database
            .query_sql("SELECT id, counter FROM items ORDER BY id")
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"], Value::Int(1));
        assert_eq!(rows[0]["counter"], Value::Int(7));
        let events = database
            .query_sql("SELECT sequence FROM events WHERE tenant = 'a' ORDER BY sequence LIMIT 2")
            .unwrap();
        assert_eq!(events.rows.len(), 1);
        assert_eq!(events.rows[0]["sequence"], Value::Int(1));
    }

    #[test]
    fn database_mutations_consume_prepared_syntax_and_preserve_refusal() {
        let mut database = Database::new();
        let cache = RelationalPlanTemplateCache::new(Some(0));
        exercise_prepared_mutations(|sql, parameters, limits| {
            database.query_sql_with_prepared_request(
                SqlQueryRequest::new(DIAGNOSTIC_SQL)
                    .with_params(parameters)
                    .with_output_limits(limits),
                cache.prepare(sql)?,
            )
        });
        assert_published_mutations(&mut database);
    }

    #[test]
    fn transaction_mutations_consume_prepared_syntax_and_preserve_refusal() {
        let mut database = Database::new();
        let cache = RelationalPlanTemplateCache::new(Some(0));
        let mut transaction = database.begin_transaction().unwrap();
        exercise_prepared_mutations(|sql, parameters, limits| {
            super::super::execute_database_transaction_prepared_sql(
                &transaction.runtime,
                &mut transaction.state,
                DIAGNOSTIC_SQL,
                cache.prepare(sql)?,
                parameters,
                super::super::DatabaseTransactionSqlOptions {
                    allow_system_schema_registry_write: false,
                    allow_locking_select: false,
                    task_context: None,
                    output_limits: limits,
                    join_planning: crate::RelationalJoinPlanningDirective::Auto,
                },
            )
            .map(|result| result.output)
        });
        transaction.commit().unwrap();
        assert_published_mutations(&mut database);
    }

    #[test]
    fn cached_mutations_rebind_parameters_and_current_schema_after_rollback() {
        let cache = RelationalPlanTemplateCache::new(Some(8));
        let upsert = "INSERT INTO items (id, body) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET body = EXCLUDED.body";
        let mut database = Database::new();
        database
            .query_sql("CREATE TABLE items (id BIGINT PRIMARY KEY, body TEXT)")
            .unwrap();
        for (id, body) in [(1, "first"), (2, "second"), (1, "replacement")] {
            database
                .query_sql_with_prepared_request(
                    SqlQueryRequest::new(DIAGNOSTIC_SQL)
                        .with_params(&[Value::Int(id), Value::String(body.into())]),
                    cache.prepare(upsert).unwrap(),
                )
                .unwrap();
        }
        let mut transaction = database.begin_transaction().unwrap();
        super::super::execute_database_transaction_prepared_sql(
            &transaction.runtime,
            &mut transaction.state,
            DIAGNOSTIC_SQL,
            cache.prepare(upsert).unwrap(),
            &[Value::Int(1), Value::String("rolled back".into())],
            super::super::DatabaseTransactionSqlOptions {
                allow_system_schema_registry_write: false,
                allow_locking_select: false,
                task_context: None,
                output_limits: QueryStreamOptions::default(),
                join_planning: crate::RelationalJoinPlanningDirective::Auto,
            },
        )
        .unwrap();
        transaction.rollback();
        let rows = database
            .query_sql("SELECT id, body FROM items ORDER BY id")
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["body"], Value::String("replacement".into()));
        assert_eq!(rows[1]["body"], Value::String("second".into()));

        let mut other = Database::new();
        other
            .query_sql("CREATE TABLE items (id TEXT PRIMARY KEY, body TEXT)")
            .unwrap();
        other
            .query_sql_with_prepared_request(
                SqlQueryRequest::new(DIAGNOSTIC_SQL).with_params(&[
                    Value::String("current-schema".into()),
                    Value::String("text key".into()),
                ]),
                cache.prepare(upsert).unwrap(),
            )
            .unwrap();
        assert_eq!(
            other.query_sql("SELECT id FROM items").unwrap().rows[0]["id"],
            Value::String("current-schema".into())
        );
    }
}
