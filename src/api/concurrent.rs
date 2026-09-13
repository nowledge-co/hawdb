use crate::sql::{Expr, ExprKind};
mod coordinator;
mod group_commit;
#[cfg(test)]
mod key_range_tests;

use self::coordinator::{CommitSequencer, LockManager, TransactionIdAllocator};
pub use self::group_commit::{
    WalGroupCommitActivation, WalGroupCommitAdaptiveColdStartEvidence,
    WalGroupCommitAdaptivePolicyEvidence, WalGroupCommitAdaptiveSteadyStateEvidence,
    WalGroupCommitConfig, WalGroupCommitDelayPolicy, WalGroupCommitEvidence,
    WalGroupCommitSnapshot, WalGroupCommitTailLatencyEvidence, WalGroupCommitWaitDecision,
    DEFAULT_WAL_GROUP_COMMIT_MAX_BYTES, DEFAULT_WAL_GROUP_COMMIT_MAX_DELAY,
    DEFAULT_WAL_GROUP_COMMIT_MAX_ENTRIES,
};
use super::system_sql;
use super::transaction_locks::{
    GraphAdjacencyDirection, GraphAllocationKind, LockMode, LockRequest, LockTarget,
    DEFAULT_LOCK_ESCALATION_ENTRIES_PER_TABLE,
};
use super::{
    commit_database_transaction_state, execute_concurrent_graph_transaction_query,
    execute_database_transaction_prepared_sql, BoundedReadQueryOutput, Database, DatabaseConfig,
    DatabaseReadTransaction, DatabaseTransactionRuntime, DatabaseTransactionSqlOptions,
    DatabaseTransactionState, QueryOutput, StatementExecutionContext, TransactionCommitResult,
};
use crate::error::{Result, SkeinError};
use crate::sql::{
    SelectStatement, SqlComparisonOp, SqlLockStrength, SqlPredicate, SqlStatement, SqlTableName,
    SqlValue, UpdateStatement,
};
use crate::store::DurabilityPolicy;
use crate::value::Value;
use skein_storage::{
    AppendTransaction, RelationalConflictAction, RelationalIndexRole, RelationalKey, RelationalRow,
    RelationalState, RelationalTableSchema, RelationalTransaction, RelationalValue,
    RelationalWrite, StoragePressureSnapshot, StorageRecoveryReport,
};
use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::Path;
#[cfg(test)]
use std::sync::{mpsc::Sender, Barrier, Condvar};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const DEFAULT_PESSIMISTIC_LOCK_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ConcurrentTransactionMode {
    Optimistic,
    #[default]
    Pessimistic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConcurrentTransactionOptions {
    pub mode: ConcurrentTransactionMode,
    pub lock_timeout: Duration,
}

impl ConcurrentTransactionOptions {
    pub const fn optimistic() -> Self {
        Self {
            mode: ConcurrentTransactionMode::Optimistic,
            lock_timeout: DEFAULT_PESSIMISTIC_LOCK_TIMEOUT,
        }
    }

    pub const fn pessimistic(lock_timeout: Duration) -> Self {
        Self {
            mode: ConcurrentTransactionMode::Pessimistic,
            lock_timeout,
        }
    }
}

impl Default for ConcurrentTransactionOptions {
    fn default() -> Self {
        Self::pessimistic(DEFAULT_PESSIMISTIC_LOCK_TIMEOUT)
    }
}

#[derive(Debug, Clone)]
pub struct ConcurrentDatabase {
    inner: Arc<ConcurrentDatabaseInner>,
}

#[derive(Debug)]
struct ConcurrentDatabaseInner {
    commits: CommitSequencer,
    locks: LockManager,
    transaction_ids: TransactionIdAllocator,
    checkpoint_serial: Mutex<()>,
    #[cfg(test)]
    autocommit_read_gate: Mutex<Option<AutocommitReadGate>>,
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct AutocommitReadGate {
    snapshot_acquired: Sender<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

#[derive(Debug)]
pub struct ConcurrentDatabaseTransaction {
    inner: Arc<ConcurrentDatabaseInner>,
    transaction_id: u64,
    base_commit_epoch: u64,
    options: ConcurrentTransactionOptions,
    runtime: DatabaseTransactionRuntime,
    state: DatabaseTransactionState,
    successful_statements: usize,
    abort_reason: Option<String>,
    finished: bool,
}

impl ConcurrentDatabase {
    pub fn new(database: Database) -> Self {
        Self::new_with_wal_group_commit(database, WalGroupCommitConfig::default())
    }

    pub fn new_with_wal_group_commit(
        database: Database,
        wal_group_commit: WalGroupCommitConfig,
    ) -> Self {
        Self {
            inner: Arc::new(ConcurrentDatabaseInner {
                commits: CommitSequencer::new(database, wal_group_commit),
                locks: LockManager::default(),
                transaction_ids: TransactionIdAllocator::default(),
                checkpoint_serial: Mutex::new(()),
                #[cfg(test)]
                autocommit_read_gate: Mutex::new(None),
            }),
        }
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Database::open(path).map(Self::new)
    }

    pub fn open_with_config(path: impl AsRef<Path>, config: DatabaseConfig) -> Result<Self> {
        Database::open_with_config(path, config).map(Self::new)
    }

    pub fn open_with_durability(
        path: impl AsRef<Path>,
        durability: DurabilityPolicy,
    ) -> Result<Self> {
        Database::open_with_durability(path, durability).map(Self::new)
    }

    pub fn open_with_durability_and_config(
        path: impl AsRef<Path>,
        durability: DurabilityPolicy,
        config: DatabaseConfig,
    ) -> Result<Self> {
        Database::open_with_durability_and_config(path, durability, config).map(Self::new)
    }

    pub fn commit_epoch(&self) -> Result<u64> {
        Ok(self.inner.commits.lock()?.commit_epoch())
    }

    pub fn published_read_view(&self) -> Result<crate::store::PublishedReadView> {
        Ok(self.inner.commits.lock()?.published_read_view())
    }

    pub fn wal_group_commit_snapshot(&self) -> Result<WalGroupCommitSnapshot> {
        self.inner.commits.group_commit_snapshot()
    }

    #[cfg(test)]
    pub(crate) fn set_group_commit_post_enqueue_barrier(
        &self,
        barrier: Arc<Barrier>,
    ) -> Result<()> {
        self.inner
            .commits
            .set_group_commit_post_enqueue_barrier(barrier)
    }

    #[cfg(test)]
    pub(crate) fn set_autocommit_read_gate(
        &self,
        snapshot_acquired: Sender<()>,
        release: Arc<(Mutex<bool>, Condvar)>,
    ) -> Result<()> {
        let mut gate = self
            .inner
            .autocommit_read_gate
            .lock()
            .map_err(|_| autocommit_read_gate_poisoned_error())?;
        *gate = Some(AutocommitReadGate {
            snapshot_acquired,
            release,
        });
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn clear_autocommit_read_gate(&self) -> Result<()> {
        self.inner
            .autocommit_read_gate
            .lock()
            .map_err(|_| autocommit_read_gate_poisoned_error())?
            .take();
        Ok(())
    }

    /// Returns storage debt and cache accounting from the same serialized
    /// commit view used by writers.
    pub fn storage_pressure_snapshot(&self) -> Result<StoragePressureSnapshot> {
        Ok(self.inner.commits.lock()?.storage_pressure_snapshot())
    }

    /// Returns the generation-pinned storage residency view without scanning
    /// candidate artifacts or materializing rows.
    pub fn storage_residency_report(&self) -> Result<crate::store::StorageResidencyReport> {
        Ok(self.inner.commits.lock()?.storage_residency_report())
    }

    /// Returns the recovery boundary observed by the currently published
    /// database handle.
    pub fn storage_recovery_report(&self) -> Result<StorageRecoveryReport> {
        Ok(self.inner.commits.lock()?.storage_recovery_report())
    }

    pub fn begin_read_transaction(&self) -> Result<DatabaseReadTransaction> {
        Ok(self.inner.commits.lock()?.begin_read_transaction())
    }

    pub fn checkpoint(&self) -> Result<()> {
        let _checkpoint_serial = self
            .inner
            .checkpoint_serial
            .lock()
            .map_err(|_| checkpoint_coordinator_poisoned_error())?;
        let source = self.inner.commits.lock()?.checkpoint_source()?;
        let prepared = source.prepare()?;
        let Some(prepared) = prepared else {
            return Ok(());
        };
        self.inner
            .commits
            .lock()?
            .publish_prepared_checkpoint(prepared)
    }

    pub fn begin_transaction(
        &self,
        options: ConcurrentTransactionOptions,
    ) -> Result<ConcurrentDatabaseTransaction> {
        let transaction_id = self.inner.transaction_ids.allocate()?;
        let database = self.inner.commits.lock()?;
        let base_commit_epoch = database.commit_epoch();
        Ok(ConcurrentDatabaseTransaction {
            inner: Arc::clone(&self.inner),
            transaction_id,
            base_commit_epoch,
            options,
            runtime: DatabaseTransactionRuntime::from_database(&database),
            state: DatabaseTransactionState::from_database(&database),
            successful_statements: 0,
            abort_reason: None,
            finished: false,
        })
    }

    pub fn query(&self, cypher_text: &str) -> Result<QueryOutput> {
        self.query_with_params(cypher_text, &BTreeMap::new())
    }

    pub fn query_with_params(
        &self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        let database = self.inner.commits.lock()?;
        let started = Instant::now();
        let prepared = match database.prepare_runtime_query(cypher_text.to_string(), parameters) {
            Ok(prepared) => prepared,
            Err(_) => {
                // Failed preparation cannot prove that the statement is read-only. Re-run it
                // through the original exclusive path so planning failures retain their
                // statement-observability behavior and uncertain statements fail closed.
                drop(database);
                return self.with_autocommit_exclusive(|database| {
                    database.query_with_params(cypher_text, parameters)
                });
            }
        };
        if !prepared.uses_read_snapshot() {
            drop(database);
            return self.with_autocommit_exclusive(move |database| {
                database.query_prepared_with_params(prepared, parameters)
            });
        }
        let statement_kind = prepared.statement_kind();
        let parse_nanos = prepared.parse_nanos();
        let mut snapshot = database.begin_read_transaction();
        drop(database);

        #[cfg(test)]
        self.wait_after_autocommit_read_snapshot()?;
        let result = snapshot.query_prepared_with_params_bounded_profile(prepared, parameters);
        drop(snapshot);
        self.record_cypher_autocommit_read(
            cypher_text,
            statement_kind,
            started,
            parse_nanos,
            &result,
        )?;
        result.map(|profiled| profiled.output)
    }

    pub fn query_sql(&self, sql_text: &str) -> Result<QueryOutput> {
        self.query_sql_with_params(sql_text, &[])
    }

    pub fn query_sql_with_params(
        &self,
        sql_text: &str,
        parameters: &[Value],
    ) -> Result<QueryOutput> {
        let database = self.inner.commits.lock()?;
        let started = Instant::now();
        let prepared = database.relational_plan_template_cache.prepare(sql_text)?;
        if !sql_statement_uses_snapshot(prepared.statement()) {
            drop(database);
            return self.with_autocommit_exclusive(move |database| {
                database.query_sql_with_prepared_params(sql_text, parameters, prepared)
            });
        }
        let statement_kind = super::observability::sql_statement_kind(prepared.statement());
        let snapshot = database.begin_read_transaction();
        drop(database);

        #[cfg(test)]
        self.wait_after_autocommit_read_snapshot()?;
        let result = snapshot.query_sql_with_prepared_params(sql_text, parameters, prepared);
        drop(snapshot);
        self.record_sql_autocommit_read(sql_text, statement_kind, started, &result)?;
        result
    }

    /// Commits one strict append transaction through the same serialized WAL
    /// sequencer used by other concurrent writers. When group commit is
    /// enabled, success is returned only after the shared durability barrier.
    pub fn append_transaction(&self, transaction: AppendTransaction) -> Result<()> {
        self.append_transaction_with_result(transaction).map(|_| ())
    }

    /// Commits a strict append transaction and returns durable generated keys.
    pub fn append_transaction_with_result(
        &self,
        transaction: AppendTransaction,
    ) -> Result<super::AppendCommitResult> {
        let committed_result = Arc::new(Mutex::new(None));
        let result_slot = Arc::clone(&committed_result);
        self.inner
            .commits
            .execute_grouped(move |database| {
                let result = database.append_transaction_with_result(transaction)?;
                *result_slot.lock().map_err(|_| {
                    SkeinError::Execution("concurrent append result slot is poisoned".to_string())
                })? = Some(result);
                Ok(QueryOutput {
                    rows: Vec::new().into(),
                })
            })
            .map(|_| ())?;
        committed_result
            .lock()
            .map_err(|_| {
                SkeinError::Execution("concurrent append result slot is poisoned".to_string())
            })?
            .take()
            .ok_or_else(|| {
                SkeinError::Execution(
                    "concurrent append completed without a commit result".to_string(),
                )
            })
    }

    fn with_autocommit_exclusive(
        &self,
        execute: impl FnOnce(&mut Database) -> Result<QueryOutput>,
    ) -> Result<QueryOutput> {
        let transaction_id = self.inner.transaction_ids.allocate()?;
        self.inner.locks.acquire(
            transaction_id,
            &[LockRequest::database(LockMode::Exclusive)],
            Instant::now(),
            DEFAULT_PESSIMISTIC_LOCK_TIMEOUT,
        )?;
        let result = self
            .inner
            .commits
            .lock()
            .and_then(|mut database| execute(&mut database));
        self.inner.locks.release(transaction_id);
        result
    }

    fn record_cypher_autocommit_read(
        &self,
        cypher_text: &str,
        statement_kind: &'static str,
        started: Instant,
        parse_nanos: u64,
        result: &Result<BoundedReadQueryOutput>,
    ) -> Result<()> {
        let elapsed = started.elapsed();
        let database = self.inner.commits.lock()?;
        let observed_started = Instant::now().checked_sub(elapsed).unwrap_or(started);
        let statement_result = match result {
            Ok(profiled) => Ok(&profiled.output),
            Err(error) => Err(error),
        };
        database.record_statement_execution(
            "cypher",
            cypher_text,
            statement_kind,
            observed_started,
            statement_result,
            StatementExecutionContext {
                execution_profile: result
                    .as_ref()
                    .ok()
                    .map(|profiled| &profiled.execution_profile),
                access_control: None,
                parse_nanos,
            },
        );
        Ok(())
    }

    fn record_sql_autocommit_read(
        &self,
        sql_text: &str,
        statement_kind: &'static str,
        started: Instant,
        result: &Result<QueryOutput>,
    ) -> Result<()> {
        let elapsed = started.elapsed();
        let database = self.inner.commits.lock()?;
        let observed_started = Instant::now().checked_sub(elapsed).unwrap_or(started);
        database.record_statement_execution(
            "sql",
            sql_text,
            statement_kind,
            observed_started,
            result.as_ref(),
            StatementExecutionContext::default(),
        );
        Ok(())
    }

    #[cfg(test)]
    fn wait_after_autocommit_read_snapshot(&self) -> Result<()> {
        let gate = self
            .inner
            .autocommit_read_gate
            .lock()
            .map_err(|_| autocommit_read_gate_poisoned_error())?
            .clone();
        if let Some(gate) = gate {
            gate.snapshot_acquired.send(()).map_err(|_| {
                SkeinError::Execution(
                    "concurrent autocommit read gate receiver was dropped".to_string(),
                )
            })?;
            let (released, available) = &*gate.release;
            let mut released = released
                .lock()
                .map_err(|_| autocommit_read_gate_poisoned_error())?;
            while !*released {
                released = available
                    .wait(released)
                    .map_err(|_| autocommit_read_gate_poisoned_error())?;
            }
        }
        Ok(())
    }
}

impl ConcurrentDatabaseTransaction {
    pub fn transaction_id(&self) -> u64 {
        self.transaction_id
    }

    pub fn base_commit_epoch(&self) -> u64 {
        self.base_commit_epoch
    }

    pub fn mode(&self) -> ConcurrentTransactionMode {
        self.options.mode
    }

    pub fn query(&mut self, cypher_text: &str) -> Result<QueryOutput> {
        self.query_with_params(cypher_text, &BTreeMap::new())
    }

    pub fn query_with_params(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        self.ensure_active()?;
        let lock_savepoint = (self.options.mode == ConcurrentTransactionMode::Pessimistic)
            .then(|| self.inner.locks.savepoint(self.transaction_id))
            .transpose()?;
        let execution = execute_concurrent_graph_transaction_query(
            &self.runtime,
            &mut self.state,
            cypher_text,
            parameters,
        );
        let outcome = match execution {
            Ok(outcome) => outcome,
            Err(error) => {
                if let Some(lock_savepoint) = lock_savepoint {
                    self.inner
                        .locks
                        .restore(self.transaction_id, lock_savepoint);
                }
                return Err(error);
            }
        };
        if self.options.mode == ConcurrentTransactionMode::Pessimistic {
            let requests = graph_lock_requests(&outcome.lock_footprint);
            if requests.is_empty() {
                self.successful_statements = self.successful_statements.saturating_add(1);
                return Ok(outcome.output);
            }
            let graph_savepoint = outcome
                .savepoint
                .expect("a staged graph mutation must retain its statement savepoint");
            self.state.restore_graph_statement(graph_savepoint);
            self.acquire_graph_statement_locks(&requests, lock_savepoint.clone())?;
            let replayed = execute_concurrent_graph_transaction_query(
                &self.runtime,
                &mut self.state,
                cypher_text,
                parameters,
            );
            let replayed = match replayed {
                Ok(replayed) => replayed,
                Err(error) => {
                    if let Some(lock_savepoint) = lock_savepoint {
                        self.inner
                            .locks
                            .restore(self.transaction_id, lock_savepoint);
                    }
                    return Err(error);
                }
            };
            self.successful_statements = self.successful_statements.saturating_add(1);
            return Ok(replayed.output);
        }
        self.successful_statements = self.successful_statements.saturating_add(1);
        Ok(outcome.output)
    }

    pub fn query_sql(&mut self, sql_text: &str) -> Result<QueryOutput> {
        self.query_sql_with_params(sql_text, &[])
    }

    pub fn query_sql_with_params(
        &mut self,
        sql_text: &str,
        parameters: &[Value],
    ) -> Result<QueryOutput> {
        self.query_sql_with_result_and_params(sql_text, parameters)
            .map(|result| result.output)
    }

    /// Executes SQL and returns both PostgreSQL rows and a provisional mutation outcome.
    pub fn query_sql_with_result(&mut self, sql_text: &str) -> Result<super::SqlStatementResult> {
        self.query_sql_with_result_and_params(sql_text, &[])
    }

    /// Parameterized form of [`Self::query_sql_with_result`].
    pub fn query_sql_with_result_and_params(
        &mut self,
        sql_text: &str,
        parameters: &[Value],
    ) -> Result<super::SqlStatementResult> {
        self.ensure_active()?;
        let prepared = self
            .runtime
            .relational_plan_template_cache
            .prepare(sql_text)?;
        if self.options.mode == ConcurrentTransactionMode::Pessimistic {
            let requests = sql_lock_requests(
                sql_text,
                &prepared,
                parameters,
                &self.state.relational_state,
                &self.state.append_state,
            )?;
            if !requests.is_empty() {
                self.acquire_locks(&requests)?;
            }
        } else {
            reject_optimistic_locking_select(&prepared)?;
        }
        let result = execute_database_transaction_prepared_sql(
            &self.runtime,
            &mut self.state,
            sql_text,
            prepared,
            parameters,
            DatabaseTransactionSqlOptions {
                allow_system_schema_registry_write: false,
                allow_locking_select: true,
                task_context: None,
            },
        );
        if result.is_ok() {
            self.successful_statements = self.successful_statements.saturating_add(1);
        }
        result
    }

    pub fn commit(self) -> Result<QueryOutput> {
        self.commit_with_result().map(|result| result.output)
    }

    /// Commits and returns outcomes recomputed by the serialized commit sequencer.
    pub fn commit_with_result(mut self) -> Result<TransactionCommitResult> {
        self.ensure_active()?;
        let started = Instant::now();
        if self.options.mode == ConcurrentTransactionMode::Optimistic
            && let Err(error) = self.inner.locks.acquire(
                self.transaction_id,
                &[LockRequest::database(LockMode::Exclusive)],
                started,
                self.options.lock_timeout,
            )
        {
            self.finished = true;
            return Err(error);
        }

        let retry_safe_conflict_noop = self.options.mode == ConcurrentTransactionMode::Optimistic
            && self.successful_statements == self.state.relational_transaction.writes.len()
            && self
                .state
                .graph_transaction
                .as_ref()
                .is_some_and(crate::store::GraphMutationTransaction::is_read_only)
            && self.state.append_transaction.writes.is_empty()
            && self.state.relational_transaction.is_conflict_noop_only();
        let allow_stale_rebase =
            self.options.mode == ConcurrentTransactionMode::Pessimistic || retry_safe_conflict_noop;
        let inner = Arc::clone(&self.inner);
        let mut state = self.state.take_for_commit();
        let committed_result = Arc::new(Mutex::new(None));
        let result_slot = Arc::clone(&committed_result);
        let result = inner
            .commits
            .execute_grouped(move |database| {
                let result =
                    commit_database_transaction_state(database, &mut state, allow_stale_rebase)?;
                let output = result.output.clone();
                *result_slot.lock().map_err(|_| {
                    SkeinError::Execution(
                        "concurrent transaction result slot is poisoned".to_string(),
                    )
                })? = Some(result);
                Ok(output)
            })
            .map_err(|error| self.map_commit_error(error));
        self.inner.locks.release(self.transaction_id);
        self.finished = true;
        result?;
        committed_result
            .lock()
            .map_err(|_| {
                SkeinError::Execution("concurrent transaction result slot is poisoned".to_string())
            })?
            .take()
            .ok_or_else(|| {
                SkeinError::Execution(
                    "concurrent transaction completed without a commit result".to_string(),
                )
            })
    }

    pub fn rollback(mut self) {
        self.state.rollback();
        self.finish_without_commit();
    }

    fn acquire_locks(&mut self, requests: &[LockRequest]) -> Result<()> {
        let started = Instant::now();
        let inner = Arc::clone(&self.inner);
        let requests_already_covered = inner.locks.covers_all(self.transaction_id, requests)?;
        if let Err(error) = inner.locks.acquire(
            self.transaction_id,
            requests,
            started,
            self.options.lock_timeout,
        ) {
            self.abort_after_lock_failure(error.to_string());
            return Err(error);
        }

        let database = match inner.commits.lock() {
            Ok(database) => database,
            Err(error) => {
                self.abort_after_lock_failure(error.to_string());
                return Err(error);
            }
        };
        let current_epoch = database.commit_epoch();
        if current_epoch != self.base_commit_epoch && !requests_already_covered {
            if self.successful_statements == 0 {
                self.base_commit_epoch = current_epoch;
                self.runtime = DatabaseTransactionRuntime::from_database(&database);
                self.state = DatabaseTransactionState::from_database(&database);
            } else {
                let error = SkeinError::Execution(format!(
                    "pessimistic transaction {} cannot acquire a new lock after its snapshot changed from commit epoch {} to {}; retry the transaction",
                    self.transaction_id, self.base_commit_epoch, current_epoch
                ));
                drop(database);
                self.inner.locks.release(self.transaction_id);
                self.state.rollback();
                self.abort_reason = Some(error.to_string());
                return Err(error);
            }
        }
        Ok(())
    }

    fn acquire_graph_statement_locks(
        &mut self,
        requests: &[LockRequest],
        lock_savepoint: Option<coordinator::LockSavepoint>,
    ) -> Result<()> {
        let started = Instant::now();
        let inner = Arc::clone(&self.inner);
        let requests_already_covered = inner.locks.covers_all(self.transaction_id, requests)?;
        if let Err(error) = inner.locks.acquire(
            self.transaction_id,
            requests,
            started,
            self.options.lock_timeout,
        ) {
            if let Some(lock_savepoint) = lock_savepoint {
                inner.locks.restore(self.transaction_id, lock_savepoint);
            }
            if error.to_string().contains("deadlock detected")
                || error.to_string().contains("resource budget exceeded")
            {
                self.abort_after_lock_failure(error.to_string());
            }
            return Err(error);
        }

        let database = match inner.commits.lock() {
            Ok(database) => database,
            Err(error) => {
                self.abort_after_lock_failure(error.to_string());
                return Err(error);
            }
        };
        let current_epoch = database.commit_epoch();
        if current_epoch != self.base_commit_epoch && !requests_already_covered {
            // The requests were derived by executing the statement against the
            // pinned snapshot. Refreshing here could change the matched graph
            // entities and make that access set incomplete. Fail closed and let
            // the caller retry from a new transaction instead.
            let error = SkeinError::Execution(format!(
                "pessimistic transaction {} cannot acquire a graph lock after its snapshot changed from commit epoch {} to {}; retry the transaction",
                self.transaction_id, self.base_commit_epoch, current_epoch
            ));
            drop(database);
            self.inner.locks.release(self.transaction_id);
            self.state.rollback();
            self.abort_reason = Some(error.to_string());
            return Err(error);
        }
        Ok(())
    }

    fn abort_after_lock_failure(&mut self, reason: String) {
        self.state.rollback();
        self.abort_reason = Some(reason);
        self.inner.locks.release(self.transaction_id);
    }

    fn ensure_active(&self) -> Result<()> {
        if self.finished {
            return Err(SkeinError::Execution(format!(
                "concurrent transaction {} is already finished",
                self.transaction_id
            )));
        }
        if let Some(reason) = &self.abort_reason {
            return Err(SkeinError::Execution(format!(
                "concurrent transaction {} is aborted: {reason}",
                self.transaction_id
            )));
        }
        Ok(())
    }

    fn map_commit_error(&self, error: SkeinError) -> SkeinError {
        if self.options.mode == ConcurrentTransactionMode::Optimistic
            && error.to_string().contains("transaction snapshot is stale")
        {
            return SkeinError::Execution(format!(
                "optimistic transaction conflict for transaction {}: {}",
                self.transaction_id, error
            ));
        }
        error
    }

    fn finish_without_commit(&mut self) {
        if self.finished {
            return;
        }
        self.inner.locks.release(self.transaction_id);
        self.finished = true;
    }
}

fn reject_optimistic_locking_select(
    prepared: &crate::relational_sql::PreparedRelationalSql,
) -> Result<()> {
    let locking_select = match prepared.statement() {
        SqlStatement::Select(select) => select.lock_strength.is_some(),
        SqlStatement::Explain(explain) => {
            matches!(explain.statement.as_ref(), SqlStatement::Select(select) if select.lock_strength.is_some())
        }
        _ => false,
    };
    if locking_select {
        return Err(SkeinError::Semantic(
            "FOR UPDATE/SHARE requires a pessimistic concurrent transaction".to_string(),
        ));
    }
    Ok(())
}

fn sql_statement_uses_snapshot(statement: &SqlStatement) -> bool {
    match statement {
        SqlStatement::Select(select) => select.lock_strength.is_none(),
        SqlStatement::Explain(explain) => sql_statement_uses_snapshot(&explain.statement),
        SqlStatement::Insert(_)
        | SqlStatement::Update(_)
        | SqlStatement::Delete(_)
        | SqlStatement::CreateTable(_)
        | SqlStatement::CreateIndex(_)
        | SqlStatement::AlterTableAddColumn(_) => false,
    }
}

fn graph_lock_requests(footprint: &crate::store::GraphMutationLockFootprint) -> Vec<LockRequest> {
    if footprint.requires_database_lock {
        return vec![LockRequest::database(LockMode::Exclusive)];
    }
    let mut requests = Vec::new();
    if footprint.allocates_node_ids {
        requests.push(LockRequest::graph_allocation(GraphAllocationKind::Node));
    }
    if footprint.allocates_relationship_ids {
        requests.push(LockRequest::graph_allocation(
            GraphAllocationKind::Relationship,
        ));
    }
    requests.extend(footprint.node_label_names.iter().cloned().map(|label| {
        LockRequest::graph_label(
            if footprint.exclusive_node_label_names.contains(&label) {
                LockMode::Exclusive
            } else {
                LockMode::Shared
            },
            label,
        )
    }));
    requests.extend(
        footprint
            .node_label_read_names
            .difference(&footprint.node_label_names)
            .cloned()
            .map(|label| LockRequest::graph_label(LockMode::Shared, label)),
    );
    requests.extend(
        footprint
            .relationship_type_names
            .iter()
            .cloned()
            .map(|rel_type| {
                LockRequest::graph_relationship_type(
                    if footprint
                        .exclusive_relationship_type_names
                        .contains(&rel_type)
                    {
                        LockMode::Exclusive
                    } else {
                        LockMode::Shared
                    },
                    rel_type,
                )
            }),
    );
    requests.extend(
        footprint
            .node_writes
            .iter()
            .map(|id| LockRequest::graph_node(LockMode::Exclusive, id.0)),
    );
    requests.extend(
        footprint
            .relationship_writes
            .iter()
            .map(|id| LockRequest::graph_relationship(LockMode::Exclusive, id.0)),
    );
    requests.extend(
        footprint
            .node_delete_guard_reads
            .difference(&footprint.node_delete_guard_writes)
            .map(|id| LockRequest::graph_node_delete_guard(LockMode::Shared, id.0)),
    );
    requests.extend(
        footprint
            .node_delete_guard_writes
            .iter()
            .map(|id| LockRequest::graph_node_delete_guard(LockMode::Exclusive, id.0)),
    );
    requests.extend(footprint.adjacency_writes.iter().map(|adjacency| {
        LockRequest::graph_adjacency(
            LockMode::Exclusive,
            adjacency.node_id.0,
            adjacency.rel_type.map(|rel_type| rel_type.0),
            match adjacency.direction {
                crate::store::AdjacencyDirection::Outgoing => GraphAdjacencyDirection::Outgoing,
                crate::store::AdjacencyDirection::Incoming => GraphAdjacencyDirection::Incoming,
            },
        )
    }));
    requests
}

impl Drop for ConcurrentDatabaseTransaction {
    fn drop(&mut self) {
        if !self.finished {
            self.state.rollback();
            self.finish_without_commit();
        }
    }
}

fn sql_lock_requests(
    sql_text: &str,
    prepared: &crate::relational_sql::PreparedRelationalSql,
    parameters: &[Value],
    state: &RelationalState,
    append_state: &skein_storage::AppendState,
) -> Result<Vec<LockRequest>> {
    if prepared.template.parameters.len() != parameters.len() {
        return Err(SkeinError::Semantic(format!(
            "PostgreSQL statement requires {} parameters, but {} parameters were supplied",
            prepared.template.parameters.len(),
            parameters.len()
        )));
    }
    let append_lock_mode = match prepared.statement() {
        SqlStatement::Select(select) if append_state.schema(&select.from.name).is_some() => {
            Some(LockMode::Shared)
        }
        SqlStatement::Explain(explain)
            if explain.analyze
                && matches!(
                    explain.statement.as_ref(),
                    SqlStatement::Select(select)
                        if append_state.schema(&select.from.name).is_some()
                ) =>
        {
            Some(LockMode::Shared)
        }
        SqlStatement::Insert(insert) if append_state.schema(&insert.table.name).is_some() => {
            Some(LockMode::Exclusive)
        }
        SqlStatement::Update(update) if append_state.schema(&update.table.name).is_some() => {
            Some(LockMode::Exclusive)
        }
        SqlStatement::Delete(delete) if append_state.schema(&delete.table.name).is_some() => {
            Some(LockMode::Exclusive)
        }
        SqlStatement::CreateIndex(create) if append_state.schema(&create.table.name).is_some() => {
            Some(LockMode::Exclusive)
        }
        SqlStatement::AlterTableAddColumn(alter)
            if append_state.schema(&alter.table.name).is_some() =>
        {
            Some(LockMode::Exclusive)
        }
        SqlStatement::CreateTable(create)
            if matches!(
                create.storage,
                crate::sql::SqlTableStorage::StrictAppend { .. }
            ) =>
        {
            Some(LockMode::Exclusive)
        }
        _ => None,
    };
    if let Some(mode) = append_lock_mode {
        return Ok(vec![LockRequest::database(mode)]);
    }
    match prepared.statement() {
        SqlStatement::Select(select) => {
            if select.lock_strength.is_some() && system_sql::is_virtual_catalog_select(select) {
                return Err(SkeinError::Semantic(
                    "system SQL does not support locking clauses".to_string(),
                ));
            }
            Ok(select_lock_requests(select, parameters, state))
        }
        SqlStatement::Explain(explain) => {
            if !explain.analyze {
                return Ok(Vec::new());
            }
            let SqlStatement::Select(select) = explain.statement.as_ref() else {
                return Ok(Vec::new());
            };
            Ok(select_lock_requests(select, parameters, state))
        }
        SqlStatement::Insert(_) => {
            let transaction = crate::relational_sql::compile_relational_statement_sql(
                sql_text, parameters, state,
            )?;
            Ok(insert_lock_requests(&transaction, state)
                .unwrap_or_else(|| vec![LockRequest::database(LockMode::Exclusive)]))
        }
        SqlStatement::Update(update) => {
            crate::relational_sql::compile_relational_statement_sql(sql_text, parameters, state)?;
            Ok(update_lock_requests(update, parameters, state))
        }
        SqlStatement::Delete(delete) => {
            crate::relational_sql::compile_relational_statement_sql(sql_text, parameters, state)?;
            Ok(delete_lock_requests(delete, parameters, state))
        }
        SqlStatement::CreateTable(_)
        | SqlStatement::CreateIndex(_)
        | SqlStatement::AlterTableAddColumn(_) => {
            Ok(vec![LockRequest::database(LockMode::Exclusive)])
        }
    }
}

fn delete_lock_requests(
    delete: &crate::sql::DeleteStatement,
    parameters: &[Value],
    state: &RelationalState,
) -> Vec<LockRequest> {
    let mut requests = mutation_target_lock_requests(
        &delete.table,
        delete.alias.as_deref(),
        delete.selection.as_ref(),
        parameters,
        state,
    );
    if requests.len() > DEFAULT_LOCK_ESCALATION_ENTRIES_PER_TABLE {
        return vec![LockRequest::relational_table(
            LockMode::Exclusive,
            delete.table.name.clone(),
        )];
    }
    let Some(schema) = state.table_schema(&delete.table.name) else {
        return requests;
    };
    let point_keys = requests
        .iter()
        .map(lock_request_point_key)
        .collect::<Option<Vec<_>>>();
    let Some(point_keys) = point_keys else {
        return vec![LockRequest::relational_table(
            LockMode::Exclusive,
            delete.table.name.clone(),
        )];
    };
    let mut unique_columns = schema.unique_constraints.clone();
    unique_columns.extend(
        schema
            .indexes
            .iter()
            .filter(|index| index.unique)
            .map(|index| index.columns.clone()),
    );
    for primary_key in point_keys {
        let Some(row) = state.row(&delete.table.name, &primary_key) else {
            continue;
        };
        for columns in &unique_columns {
            let Some(key) = relational_row_key(schema, row, columns) else {
                continue;
            };
            if key.0.iter().any(|value| value == &RelationalValue::Null) {
                continue;
            }
            push_unique_lock_request(
                &mut requests,
                LockRequest::relational_point(
                    LockMode::Exclusive,
                    delete.table.name.clone(),
                    columns.clone(),
                    key,
                ),
            );
        }
    }
    requests
}

fn lock_request_point_key(request: &LockRequest) -> Option<RelationalKey> {
    let LockTarget::RelationalRange { lower, upper, .. } = &request.target else {
        return None;
    };
    match (lower, upper) {
        (Bound::Included(lower), Bound::Included(upper)) if lower == upper => Some(lower.clone()),
        _ => None,
    }
}

fn select_lock_requests(
    select: &SelectStatement,
    parameters: &[Value],
    state: &RelationalState,
) -> Vec<LockRequest> {
    let Some(strength) = select.lock_strength else {
        return Vec::new();
    };
    let mode = match strength {
        SqlLockStrength::Share => LockMode::Shared,
        SqlLockStrength::Update => LockMode::Exclusive,
    };
    if !select.joins.is_empty() {
        let mut requests = vec![LockRequest::relational_table(
            mode,
            select.from.name.clone(),
        )];
        requests.extend(
            select
                .joins
                .iter()
                .map(|join| LockRequest::relational_table(mode, join.table.name.clone())),
        );
        return requests;
    }
    if !is_public_table(&select.from) {
        return vec![LockRequest::database(mode)];
    }
    let Some(schema) = state.table_schema(&select.from.name) else {
        return vec![LockRequest::database(mode)];
    };
    let ranges = match &select.selection {
        None => Some(vec![(Bound::Unbounded, Bound::Unbounded)]),
        Some(predicate) if schema.primary_key.len() == 1 => single_key_ranges(
            predicate,
            &schema.primary_key[0],
            &select.from,
            select.from_alias.as_deref(),
            parameters,
        ),
        Some(predicate) => composite_key_point(
            predicate,
            &schema.primary_key,
            &select.from,
            select.from_alias.as_deref(),
            parameters,
        )
        .map(|range| vec![range]),
    };
    let Some(ranges) = ranges else {
        return vec![LockRequest::relational_table(
            mode,
            select.from.name.clone(),
        )];
    };
    ranges
        .into_iter()
        .map(|(lower, upper)| {
            LockRequest::relational_range(
                mode,
                select.from.name.clone(),
                schema.primary_key.clone(),
                lower,
                upper,
            )
        })
        .collect()
}

fn update_lock_requests(
    update: &UpdateStatement,
    parameters: &[Value],
    state: &RelationalState,
) -> Vec<LockRequest> {
    let Some(schema) = state.table_schema(&update.table.name) else {
        return vec![LockRequest::database(LockMode::Exclusive)];
    };
    let assignment_changes_constraint_key = update.assignments.iter().any(|assignment| {
        schema.primary_key.contains(&assignment.column)
            || schema
                .unique_constraints
                .iter()
                .any(|columns| columns.contains(&assignment.column))
            || schema
                .indexes
                .iter()
                .any(|index| index.unique && index.columns.contains(&assignment.column))
            || schema
                .foreign_keys
                .iter()
                .any(|foreign_key| foreign_key.columns.contains(&assignment.column))
    });
    if assignment_changes_constraint_key {
        return vec![LockRequest::database(LockMode::Exclusive)];
    }
    mutation_target_lock_requests(
        &update.table,
        update.alias.as_deref(),
        update.selection.as_ref(),
        parameters,
        state,
    )
}

fn mutation_target_lock_requests(
    table: &SqlTableName,
    alias: Option<&str>,
    selection: Option<&SqlPredicate>,
    parameters: &[Value],
    state: &RelationalState,
) -> Vec<LockRequest> {
    if !is_public_table(table) {
        return vec![LockRequest::database(LockMode::Exclusive)];
    }
    let Some(schema) = state.table_schema(&table.name) else {
        return vec![LockRequest::database(LockMode::Exclusive)];
    };
    let Some(selection) = selection else {
        return vec![LockRequest::relational_table(
            LockMode::Exclusive,
            table.name.clone(),
        )];
    };
    let ranges = if schema.primary_key.len() == 1 {
        single_key_ranges(selection, &schema.primary_key[0], table, alias, parameters)
    } else {
        composite_key_point(selection, &schema.primary_key, table, alias, parameters)
            .map(|range| vec![range])
    };
    let Some(ranges) = ranges else {
        return vec![LockRequest::relational_table(
            LockMode::Exclusive,
            table.name.clone(),
        )];
    };
    ranges
        .into_iter()
        .map(|(lower, upper)| {
            LockRequest::relational_range(
                LockMode::Exclusive,
                table.name.clone(),
                schema.primary_key.clone(),
                lower,
                upper,
            )
        })
        .collect()
}

fn insert_lock_requests(
    transaction: &RelationalTransaction,
    state: &RelationalState,
) -> Option<Vec<LockRequest>> {
    let mut requests = Vec::new();
    for write in &transaction.writes {
        let (table, rows) = match write {
            RelationalWrite::Insert { table, rows, .. } => (table, rows),
            RelationalWrite::Upsert {
                table,
                rows,
                action,
                ..
            } if matches!(action, RelationalConflictAction::DoNothing)
                || upsert_update_can_use_insert_locks(write, state) =>
            {
                (table, rows)
            }
            _ => return None,
        };
        let schema = state.table_schema(table)?;
        let mut unique_keys = vec![schema.primary_key.clone()];
        unique_keys.extend(schema.unique_constraints.iter().cloned());
        unique_keys.extend(
            schema
                .indexes
                .iter()
                .filter(|index| index.unique)
                .map(|index| index.columns.clone()),
        );
        for row in rows {
            for columns in &unique_keys {
                let key = relational_row_key(schema, row, columns)?;
                if key.0.iter().any(|value| value == &RelationalValue::Null) {
                    continue;
                }
                push_unique_lock_request(
                    &mut requests,
                    LockRequest::relational_point(
                        LockMode::Exclusive,
                        table.clone(),
                        columns.clone(),
                        key,
                    ),
                );
            }
            for foreign_key in &schema.foreign_keys {
                let key = relational_row_key(schema, row, &foreign_key.columns)?;
                if key.0.iter().any(|value| value == &RelationalValue::Null) {
                    continue;
                }
                push_unique_lock_request(
                    &mut requests,
                    LockRequest::relational_point(
                        LockMode::Shared,
                        foreign_key.referenced_table.clone(),
                        foreign_key.referenced_columns.clone(),
                        key,
                    ),
                );
            }
        }
    }
    Some(requests)
}

fn upsert_update_can_use_insert_locks(write: &RelationalWrite, state: &RelationalState) -> bool {
    let RelationalWrite::Upsert {
        table,
        rows,
        conflict_columns,
        action: RelationalConflictAction::Update(_),
    } = write
    else {
        return false;
    };
    let Some(schema) = state.table_schema(table) else {
        return false;
    };
    let Some(conflict_index) = schema.unique_index_definition(conflict_columns) else {
        return false;
    };
    if conflict_index.role != RelationalIndexRole::Primary
        && !state.materialized_index_postings_resident()
    {
        return false;
    }
    rows.iter().all(|row| {
        let Some(conflict_key) = relational_row_key(schema, row, conflict_columns) else {
            return false;
        };
        if conflict_key
            .0
            .iter()
            .any(|value| value == &RelationalValue::Null)
        {
            return true;
        }
        match conflict_index.role {
            RelationalIndexRole::Primary => state.row(table, &conflict_key).is_none(),
            _ => state
                .index_lookup(table, &conflict_index.name, &conflict_key)
                .is_none_or(|postings| postings.is_empty()),
        }
    })
}

fn push_unique_lock_request(requests: &mut Vec<LockRequest>, request: LockRequest) {
    if !requests.contains(&request) {
        requests.push(request);
    }
}

fn relational_row_key(
    schema: &RelationalTableSchema,
    row: &RelationalRow,
    columns: &[String],
) -> Option<RelationalKey> {
    columns
        .iter()
        .map(|column| {
            schema
                .column_position(column)
                .and_then(|position| row.values().get(position))
                .cloned()
        })
        .collect::<Option<Vec<_>>>()
        .map(RelationalKey)
}

fn single_key_ranges(
    predicate: &SqlPredicate,
    primary_key: &str,
    table: &SqlTableName,
    alias: Option<&str>,
    parameters: &[Value],
) -> Option<Vec<(Bound<RelationalKey>, Bound<RelationalKey>)>> {
    match predicate {
        Expr {
            kind: ExprKind::And(left, right),
            ..
        } => {
            let left = single_key_ranges(left, primary_key, table, alias, parameters);
            let right = single_key_ranges(right, primary_key, table, alias, parameters);
            match (left, right) {
                (Some(left), Some(right)) => Some(
                    left.into_iter()
                        .flat_map(|left| {
                            right
                                .iter()
                                .filter_map(move |right| intersect_ranges(&left, right))
                        })
                        .collect(),
                ),
                // A conjunct can only narrow its sibling's key set. Retain that
                // conservative bound even when the residual has no key range.
                (Some(ranges), None) | (None, Some(ranges)) => Some(ranges),
                (None, None) => None,
            }
        }
        Expr {
            kind: ExprKind::Or(left, right),
            ..
        } => {
            let mut ranges = single_key_ranges(left, primary_key, table, alias, parameters)?;
            ranges.extend(single_key_ranges(
                right,
                primary_key,
                table,
                alias,
                parameters,
            )?);
            Some(ranges)
        }
        Expr {
            kind: ExprKind::Compare { left, op, right },
            ..
        } if left
            .as_column()
            .is_some_and(|left| column_matches(left, primary_key, table, alias)) =>
        {
            let key = RelationalKey(vec![bind_sql_lock_value(right.as_value()?, parameters)?]);
            match op {
                SqlComparisonOp::Eq => {
                    Some(vec![(Bound::Included(key.clone()), Bound::Included(key))])
                }
                SqlComparisonOp::Lt => Some(vec![(Bound::Unbounded, Bound::Excluded(key))]),
                SqlComparisonOp::Lte => Some(vec![(Bound::Unbounded, Bound::Included(key))]),
                SqlComparisonOp::Gt => Some(vec![(Bound::Excluded(key), Bound::Unbounded)]),
                SqlComparisonOp::Gte => Some(vec![(Bound::Included(key), Bound::Unbounded)]),
                SqlComparisonOp::NotEq => None,
            }
        }
        Expr {
            kind:
                ExprKind::InList {
                    left,
                    values,
                    negated: false,
                },
            ..
        } if left
            .as_column()
            .is_some_and(|left| column_matches(left, primary_key, table, alias)) =>
        {
            values
                .iter()
                .map(|value| {
                    let key =
                        RelationalKey(vec![bind_sql_lock_value(value.as_value()?, parameters)?]);
                    Some((Bound::Included(key.clone()), Bound::Included(key)))
                })
                .collect()
        }
        _ => None,
    }
}

fn composite_key_point(
    predicate: &SqlPredicate,
    primary_key: &[String],
    table: &SqlTableName,
    alias: Option<&str>,
    parameters: &[Value],
) -> Option<(Bound<RelationalKey>, Bound<RelationalKey>)> {
    let mut values = BTreeMap::new();
    collect_composite_equalities(
        predicate,
        primary_key,
        table,
        alias,
        parameters,
        &mut values,
    )?;
    let key = RelationalKey(
        primary_key
            .iter()
            .map(|column| values.remove(column))
            .collect::<Option<Vec<_>>>()?,
    );
    Some((Bound::Included(key.clone()), Bound::Included(key)))
}

fn collect_composite_equalities(
    predicate: &SqlPredicate,
    primary_key: &[String],
    table: &SqlTableName,
    alias: Option<&str>,
    parameters: &[Value],
    values: &mut BTreeMap<String, RelationalValue>,
) -> Option<()> {
    match predicate {
        Expr {
            kind: ExprKind::And(left, right),
            ..
        } => {
            collect_composite_equalities(left, primary_key, table, alias, parameters, values)?;
            collect_composite_equalities(right, primary_key, table, alias, parameters, values)
        }
        Expr {
            kind:
                ExprKind::Compare {
                    left,
                    op: SqlComparisonOp::Eq,
                    right,
                },
            ..
        } if primary_key.iter().any(|column| {
            left.as_column()
                .is_some_and(|left| column_matches(left, column, table, alias))
        }) =>
        {
            let left = left.as_column()?;
            if values.contains_key(&left.name) {
                return None;
            }
            values.insert(
                left.name.clone(),
                bind_sql_lock_value(right.as_value()?, parameters)?,
            );
            Some(())
        }
        _ => None,
    }
}

fn bind_sql_lock_value(value: &SqlValue, parameters: &[Value]) -> Option<RelationalValue> {
    crate::relational_sql::bind_relational_value(value.clone(), parameters).ok()
}

fn column_matches(
    column: &crate::sql::SqlColumnRef,
    expected: &str,
    table: &SqlTableName,
    alias: Option<&str>,
) -> bool {
    column.name == expected
        && column
            .qualifier
            .as_deref()
            .is_none_or(|qualifier| Some(qualifier) == alias || qualifier == table.name)
}

fn is_public_table(table: &SqlTableName) -> bool {
    table
        .schema
        .as_deref()
        .is_none_or(|schema| schema.eq_ignore_ascii_case("public"))
}

fn intersect_ranges(
    left: &(Bound<RelationalKey>, Bound<RelationalKey>),
    right: &(Bound<RelationalKey>, Bound<RelationalKey>),
) -> Option<(Bound<RelationalKey>, Bound<RelationalKey>)> {
    let lower = maximum_lower_bound(&left.0, &right.0);
    let upper = minimum_upper_bound(&left.1, &right.1);
    (!upper_is_before_lower(&upper, &lower)).then_some((lower, upper))
}

fn maximum_lower_bound(
    left: &Bound<RelationalKey>,
    right: &Bound<RelationalKey>,
) -> Bound<RelationalKey> {
    match (left, right) {
        (Bound::Unbounded, bound) | (bound, Bound::Unbounded) => bound.clone(),
        (Bound::Included(left), Bound::Included(right)) => Bound::Included(left.max(right).clone()),
        (Bound::Excluded(left), Bound::Excluded(right)) => Bound::Excluded(left.max(right).clone()),
        (Bound::Included(left), Bound::Excluded(right)) => match left.cmp(right) {
            std::cmp::Ordering::Greater => Bound::Included(left.clone()),
            _ => Bound::Excluded(right.clone()),
        },
        (Bound::Excluded(left), Bound::Included(right)) => match left.cmp(right) {
            std::cmp::Ordering::Less => Bound::Included(right.clone()),
            _ => Bound::Excluded(left.clone()),
        },
    }
}

fn minimum_upper_bound(
    left: &Bound<RelationalKey>,
    right: &Bound<RelationalKey>,
) -> Bound<RelationalKey> {
    match (left, right) {
        (Bound::Unbounded, bound) | (bound, Bound::Unbounded) => bound.clone(),
        (Bound::Included(left), Bound::Included(right)) => Bound::Included(left.min(right).clone()),
        (Bound::Excluded(left), Bound::Excluded(right)) => Bound::Excluded(left.min(right).clone()),
        (Bound::Included(left), Bound::Excluded(right)) => match left.cmp(right) {
            std::cmp::Ordering::Less => Bound::Included(left.clone()),
            _ => Bound::Excluded(right.clone()),
        },
        (Bound::Excluded(left), Bound::Included(right)) => match left.cmp(right) {
            std::cmp::Ordering::Greater => Bound::Included(right.clone()),
            _ => Bound::Excluded(left.clone()),
        },
    }
}

fn upper_is_before_lower(upper: &Bound<RelationalKey>, lower: &Bound<RelationalKey>) -> bool {
    match (upper, lower) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
        (Bound::Included(upper), Bound::Included(lower)) => upper < lower,
        (Bound::Included(upper), Bound::Excluded(lower))
        | (Bound::Excluded(upper), Bound::Included(lower))
        | (Bound::Excluded(upper), Bound::Excluded(lower)) => upper <= lower,
    }
}

fn checkpoint_coordinator_poisoned_error() -> SkeinError {
    SkeinError::Execution("concurrent checkpoint coordinator is poisoned".to_string())
}

#[cfg(test)]
fn autocommit_read_gate_poisoned_error() -> SkeinError {
    SkeinError::Execution("concurrent autocommit read gate is poisoned".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use skein_storage::{
        AppendTableSchema, AppendWrite, RelationalColumnSchema, RelationalScalarType,
    };

    fn key(value: i64) -> RelationalKey {
        RelationalKey(vec![RelationalValue::BigInt(value)])
    }

    #[test]
    fn intersects_adjacent_half_open_ranges_correctly() {
        assert!(intersect_ranges(
            &(Bound::Included(key(10)), Bound::Excluded(key(20))),
            &(Bound::Included(key(20)), Bound::Included(key(30))),
        )
        .is_none());
        assert_eq!(
            intersect_ranges(
                &(Bound::Included(key(10)), Bound::Included(key(20))),
                &(Bound::Included(key(20)), Bound::Included(key(30))),
            ),
            Some((Bound::Included(key(20)), Bound::Included(key(20))))
        );
    }

    #[test]
    fn concurrent_append_transaction_uses_the_shared_commit_sequencer() {
        let path = std::env::temp_dir().join(format!(
            "skein-concurrent-append-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let mut database = Database::open(&path).expect("open append database");
        database
            .append_transaction(AppendTransaction {
                writes: vec![AppendWrite::CreateTable {
                    schema: AppendTableSchema {
                        name: "events".to_string(),
                        columns: vec![
                            append_column("stream", RelationalScalarType::Text),
                            append_column("sequence", RelationalScalarType::BigInt),
                        ],
                        partition_key: vec!["stream".to_string()],
                        order_key: vec!["sequence".to_string()],
                        order_mode: Default::default(),
                    },
                }],
            })
            .expect("create append table");
        let before = database.commit_epoch();
        let database = ConcurrentDatabase::new(database);

        database
            .append_transaction(AppendTransaction {
                writes: vec![AppendWrite::Append {
                    table: "events".to_string(),
                    rows: vec![RelationalRow::new(vec![
                        RelationalValue::Text("alpha".to_string()),
                        RelationalValue::BigInt(1),
                    ])],
                }],
            })
            .expect("commit concurrent append");

        assert_eq!(
            database.commit_epoch().expect("read commit epoch"),
            before + 1
        );
        drop(database);
        std::fs::remove_dir_all(path).expect("remove append database");
    }

    #[test]
    fn append_explain_analyze_takes_a_shared_database_lock() {
        let append_state = skein_storage::AppendState::default()
            .stage_transaction(
                &AppendTransaction {
                    writes: vec![AppendWrite::CreateTable {
                        schema: AppendTableSchema {
                            name: "events".to_string(),
                            columns: vec![
                                append_column("stream", RelationalScalarType::Text),
                                append_column("sequence", RelationalScalarType::BigInt),
                            ],
                            partition_key: vec!["stream".to_string()],
                            order_key: vec!["sequence".to_string()],
                            order_mode: Default::default(),
                        },
                    }],
                },
                skein_storage::AppendMutationLimits::default(),
            )
            .expect("stage append schema");

        const SQL: &str = "EXPLAIN ANALYZE SELECT * FROM events \
                           WHERE stream = 'alpha' ORDER BY sequence LIMIT 10";
        let cache = crate::relational_sql::RelationalPlanTemplateCache::new(Some(4));
        let prepared = cache.prepare(SQL).expect("prepare append explain analyze");
        let requests = sql_lock_requests(
            SQL,
            &prepared,
            &[],
            &RelationalState::default(),
            &append_state,
        )
        .expect("derive append explain analyze locks");

        assert_eq!(requests, vec![LockRequest::database(LockMode::Shared)]);
    }

    fn append_column(name: &str, scalar_type: RelationalScalarType) -> RelationalColumnSchema {
        RelationalColumnSchema {
            name: name.to_string(),
            scalar_type,
            nullable: false,
            default: None,
        }
    }
}
