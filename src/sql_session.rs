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

//! In-process PostgreSQL-dialect SQL sessions over the embedded transaction path.

use crate::value::Value;
use crate::{
    Database, DatabaseReadTransaction, DatabaseTransaction, HawDBError, Result, SessionConfig,
    SqlStatementResult, TransactionCommitResult,
};
use hawdb_core::RuntimeTaskContext;

/// A scoped SQL session borrowing one embedded database.
///
/// SQL and `$1`, `$2`, ... parameters go unchanged to the existing frontend.
/// Unsupported PostgreSQL constructs retain their native Parse/Semantic errors.
/// Each [`Self::query`] commits its writes before returning. Callback statements
/// share one native transaction; statement/callback errors and unwinding discard
/// staged writes. Commit failures retain the native uncertain-outcome contract.
#[derive(Debug)]
pub struct SqlSession<'a> {
    config: SessionConfig,
    database: &'a mut Database,
    task_context: Option<RuntimeTaskContext>,
}

impl<'a> SqlSession<'a> {
    pub fn new(database: &'a mut Database) -> Self {
        Self {
            config: SessionConfig::default(),
            database,
            task_context: None,
        }
    }

    pub fn config(&self) -> &SessionConfig {
        &self.config
    }

    /// Sets the row-count policy for subsequent transactions in this session.
    /// Zero disables the session ceiling; positive host ceilings still apply.
    pub fn set_max_result_rows(&mut self, max_result_rows: u64) {
        self.config.max_result_rows = max_result_rows;
    }

    /// Supplies the existing cooperative cancellation/deadline and admission
    /// context to every transaction opened by this session.
    pub fn with_task_context(mut self, task_context: &RuntimeTaskContext) -> Self {
        self.task_context = Some(task_context.clone());
        self
    }

    /// Executes and commits one statement before returning its result.
    ///
    /// `output.rows` holds the query/RETURNING rows. `mutation` carries the
    /// native affected/conflict counts for RowPage DML, confirmed at commit.
    /// Read/schema statements and Strict Append retain the native result
    /// envelope; they do not manufacture a RowPage mutation count.
    pub fn query(&mut self, sql: &str, parameters: &[Value]) -> Result<SqlStatementResult> {
        let mut transaction = self.begin_transaction()?;
        let mut result = transaction.query(sql, parameters)?;
        let committed = transaction.commit()?;
        if let Some(committed) = committed
            && result.mutation.is_some()
        {
            let mut mutations = committed.mutations.into_iter();
            let mutation = mutations.next().ok_or_else(|| {
                HawDBError::StorageIntegrity(
                    "SQL session commit omitted its mutation outcome".into(),
                )
            })?;
            result.output = committed.output;
            result.mutation = Some(mutation);
        }
        Ok(result)
    }

    /// Runs multiple statements against one native transaction.
    ///
    /// Rows and mutation counts observed inside the callback are provisional
    /// until this method succeeds. An error from any query aborts the transaction
    /// even if the callback catches it and returns `Ok`. A callback error or
    /// panic also discards all staged writes. Commit errors propagate unchanged;
    /// a lost response or post-publication failure is not proof of rollback.
    pub fn transaction<T>(
        &mut self,
        operation: impl FnOnce(&mut SqlSessionTransaction<'_>) -> Result<T>,
    ) -> Result<T> {
        let mut transaction = SqlSessionTransaction {
            transaction: Some(self.begin_transaction()?),
            failure: None,
        };
        let output = operation(&mut transaction);
        if let Some(failure) = transaction.failure.take() {
            return Err(failure);
        }
        match output {
            Ok(output) => {
                transaction
                    .transaction
                    .take()
                    .expect("active SQL transaction")
                    .commit()?;
                Ok(output)
            }
            Err(error) => {
                transaction
                    .transaction
                    .take()
                    .expect("active SQL transaction")
                    .rollback();
                Err(error)
            }
        }
    }

    fn begin_transaction(&mut self) -> Result<SessionTransaction<'_>> {
        if self.database.config().read_only {
            let mut transaction = match &self.task_context {
                Some(context) => self.database.begin_read_transaction_with_context(context),
                None => self.database.begin_read_transaction(),
            }?;
            transaction.apply_session_config(self.config);
            return Ok(SessionTransaction {
                reader: Some(transaction),
                writer: None,
                task_context: self.task_context.clone(),
            });
        }
        let mut transaction = match &self.task_context {
            Some(context) => self.database.begin_transaction_with_context(context),
            None => self.database.begin_transaction(),
        }?;
        transaction.apply_session_config(self.config);
        Ok(SessionTransaction {
            writer: Some(transaction),
            reader: None,
            task_context: self.task_context.clone(),
        })
    }
}

/// The SQL-only query scope supplied to [`SqlSession::transaction`].
/// It cannot commit independently or replace the enclosing transaction.
#[derive(Debug)]
pub struct SqlSessionTransaction<'a> {
    transaction: Option<SessionTransaction<'a>>,
    failure: Option<HawDBError>,
}

impl SqlSessionTransaction<'_> {
    /// Executes one statement in the enclosing transaction. The first failure
    /// immediately rolls back and is returned again by subsequent calls.
    pub fn query(&mut self, sql: &str, parameters: &[Value]) -> Result<SqlStatementResult> {
        if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }
        let result = self
            .transaction
            .as_mut()
            .expect("active SQL transaction")
            .query(sql, parameters);
        if let Err(error) = &result {
            self.failure = Some(error.clone());
            self.transaction
                .take()
                .expect("active SQL transaction")
                .rollback();
        }
        result
    }
}

// Exactly one native owner is populated. Keeping the owners inline avoids a
// separate allocation merely to wrap the existing transaction workspace.
#[derive(Debug)]
struct SessionTransaction<'a> {
    writer: Option<DatabaseTransaction<'a>>,
    reader: Option<DatabaseReadTransaction>,
    task_context: Option<RuntimeTaskContext>,
}

impl SessionTransaction<'_> {
    fn query(&mut self, sql: &str, parameters: &[Value]) -> Result<SqlStatementResult> {
        if let Some(writer) = &mut self.writer {
            return writer.query_sql_with_result_and_params(sql, parameters);
        }
        self.reader
            .as_ref()
            .expect("active SQL read transaction")
            .query_sql_with_params(sql, parameters)
            .map(|output| SqlStatementResult {
                output,
                mutation: None,
            })
    }

    fn commit(self) -> Result<Option<TransactionCommitResult>> {
        if let Some(context) = &self.task_context {
            context
                .checkpoint()
                .map_err(HawDBError::RuntimeTaskStopped)?;
        }
        self.writer
            .map(DatabaseTransaction::commit_with_result)
            .transpose()
    }

    fn rollback(self) {
        if let Some(writer) = self.writer {
            writer.rollback();
        }
    }
}

impl Database {
    /// Borrows this database for an in-process PostgreSQL-dialect SQL session.
    pub fn sql_session(&mut self) -> SqlSession<'_> {
        SqlSession::new(self)
    }
}

#[cfg(test)]
mod tests;
