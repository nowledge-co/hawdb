// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::super::publication::PublishedConcurrentRead;
use super::*;
use crate::api::{
    observability::sql_statement_kind, query_runtime::query_runtime_checkpoint,
    reject_locking_select_without_manager,
};
use crate::relational_sql::PreparedRelationalSql;
use crate::SqlQueryRequest;

#[cfg(test)]
mod tests;

// Derive protection from syntax and schema only. Binding INSERT defaults here
// would generate different values from execution, and reading old DELETE rows
// would make the scope stale when a writer commits while we wait for locks.
fn mutation_scope(
    statement: &SqlStatement,
    parameters: &[Value],
    state: &RelationalState,
    append_state: &hawdb_storage::append_table::AppendState,
) -> Vec<LockRequest> {
    let table = match statement {
        SqlStatement::Insert(insert) => &insert.table,
        SqlStatement::Update(update) => &update.table,
        SqlStatement::Delete(delete) => &delete.table,
        _ => return vec![LockRequest::database(LockMode::Exclusive)],
    };
    let Some(schema) = state.table_schema(&table.name) else {
        return vec![LockRequest::database(LockMode::Exclusive)];
    };
    if !is_public_table(table)
        || append_state.schema(&table.name).is_some()
        || !schema.foreign_keys.is_empty()
        || state.table_schemas().any(|other| {
            other
                .foreign_keys
                .iter()
                .any(|key| key.referenced_table == table.name)
        })
    {
        // FK checks/cascades span other tables. DDL and Strict Append likewise
        // retain the established database scope until narrower scopes are proven.
        return vec![LockRequest::database(LockMode::Exclusive)];
    }
    let requests = match statement {
        SqlStatement::Update(update) => update_lock_requests(update, parameters, state),
        _ => vec![LockRequest::relational_table(
            LockMode::Exclusive,
            table.name.clone(),
        )],
    };
    if requests.is_empty() {
        vec![LockRequest::relational_table(
            LockMode::Exclusive,
            table.name.clone(),
        )]
    } else {
        requests
    }
}

impl ConcurrentDatabase {
    pub(in crate::api::concurrent) fn query_prepared_sql_mutation(
        &self,
        request: SqlQueryRequest<'_>,
        prepared: PreparedRelationalSql,
        view: Arc<PublishedConcurrentRead>,
        started: Instant,
    ) -> Result<QueryOutput> {
        let statement_kind = sql_statement_kind(prepared.statement());
        let recorder = view.recorder.clone();
        let result = (|| {
            // Autocommit must not silently turn a locking SELECT into an
            // explicit transaction whose locks disappear before its caller acts.
            reject_locking_select_without_manager(prepared.statement(), false)?;
            let requests = mutation_scope(
                prepared.statement(),
                request.parameters,
                view.snapshot.0.store.relational_state(),
                view.snapshot.0.store.append_state(),
            );
            drop(view);
            #[cfg(test)]
            self.wait_before_autocommit_mutation(AutocommitMutationPhase::BeforeLocks)?;
            let locks = MutationLocks {
                inner: &self.inner,
                transaction_id: self.inner.transaction_ids.allocate()?,
                started,
            };
            query_runtime_checkpoint(request.task_context)?;
            locks.acquire(&requests)?;
            // Neither the scope nor the template retains a source generation.
            // The first workspace sees commits completed during lock waiting.
            let mut transaction =
                self.begin_transaction_with_id(Default::default(), locks.transaction_id)?;
            let current_scope = mutation_scope(
                prepared.statement(),
                request.parameters,
                &transaction.state.relational_state,
                &transaction.state.append_state,
            );
            if !self
                .inner
                .locks
                .covers_all(locks.transaction_id, &current_scope)?
            {
                // Schema-neutral SQL may bind to a changed schema, but only if
                // the new constraint/predicate domain is already protected.
                // Never upgrade from a pinned workspace under an obsolete scope.
                return Err(preparation_changed());
            }
            transaction.task_context = request.task_context.cloned();
            if matches!(prepared.statement(), SqlStatement::Update(_)) {
                // The syntax guard is only the first phase for indexed updates.
                // Coordinate discovered primary rows and old/new index entries
                // through the same pinned-view and refresh loop as explicit SQL.
                transaction.acquire_sql_locks(
                    &prepared,
                    request.parameters,
                    request.task_context,
                )?;
            }
            #[cfg(test)]
            self.wait_before_autocommit_mutation(AutocommitMutationPhase::BeforeExecution)?;
            execute_database_transaction_prepared_sql(
                &transaction.runtime,
                &mut transaction.state,
                request.sql,
                prepared,
                request.parameters,
                DatabaseTransactionSqlOptions {
                    allow_system_schema_registry_write: false,
                    allow_locking_select: false,
                    task_context: request.task_context,
                    output_limits: request.output_limits,
                    join_planning: request.join_planning,
                },
            )?;
            transaction.successful_statements = 1;
            #[cfg(test)]
            self.wait_before_autocommit_mutation(AutocommitMutationPhase::Staged)?;
            // RETURNING was checked privately. Publication uses the already bound
            // writes and the existing WAL/group-durability boundary; no retry can
            // duplicate a commit whose acknowledgment was lost.
            transaction.commit()
        })();
        recorder.record_statement_execution(
            "sql",
            request.sql,
            statement_kind,
            started,
            result.as_ref(),
            StatementExecutionContext::default(),
        );
        result
    }
}
