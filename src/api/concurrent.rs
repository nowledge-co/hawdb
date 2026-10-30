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

use crate::sql::{Expr, ExprKind};
use hawdb_core::{TransactionRetryError, TransactionRetryReason};
mod autocommit;
#[cfg(test)]
pub(crate) use autocommit::AutocommitMutationPhase;
mod coordinator;
#[cfg(test)]
mod graph_preparation_tests;
#[cfg(test)]
mod key_range_tests;
mod metrics;
mod owner;
mod publication;

pub use metrics::{ConcurrentDatabaseMetrics, ConcurrentPhaseMetrics};
pub(crate) use owner::{SharedDatabaseGuard, SharedDatabaseOwner};
pub(crate) use publication::PublishedConcurrentRead;

use self::coordinator::{CommitSequencer, LockManager, TransactionIdAllocator};
use super::system_sql;
use super::transaction_locks::{
    GraphAdjacencyDirection, GraphAllocationKind, LockMode, LockRequest, LockTarget,
    DEFAULT_LOCK_ESCALATION_ENTRIES_PER_TABLE,
};
use super::{
    commit_prepared_database_transaction_state, execute_database_transaction_prepared_sql,
    Database, DatabaseConfig, DatabaseReadTransaction, DatabaseTransactionRuntime,
    DatabaseTransactionSqlOptions, DatabaseTransactionState, PreparedGraphTransactionRequest,
    QueryOutput, StatementExecutionContext, TransactionCommitResult,
};
use crate::error::{HawDBError, Result};
use crate::sql::{
    SelectStatement, SqlComparisonOp, SqlLockStrength, SqlPredicate, SqlStatement, SqlTableName,
    SqlValue, UpdateStatement,
};
use crate::store::DurabilityPolicy;
use crate::value::Value;
use hawdb_core::time::{Duration, Instant};
pub use hawdb_storage::wal::{
    WalGroupCommitActivation, WalGroupCommitAdaptiveColdStartEvidence,
    WalGroupCommitAdaptivePolicyEvidence, WalGroupCommitAdaptiveSteadyStateEvidence,
    WalGroupCommitConfig, WalGroupCommitDelayPolicy, WalGroupCommitEvidence,
    WalGroupCommitSnapshot, WalGroupCommitTailLatencyEvidence, WalGroupCommitWaitDecision,
    DEFAULT_WAL_GROUP_COMMIT_MAX_BYTES, DEFAULT_WAL_GROUP_COMMIT_MAX_DELAY,
    DEFAULT_WAL_GROUP_COMMIT_MAX_ENTRIES,
};
use hawdb_storage::{
    append_table::AppendTransaction,
    pressure::StoragePressureSnapshot,
    projection::StorageRecoveryReport,
    relational::{
        RelationalConflictAction, RelationalIndexRole, RelationalKey, RelationalRow,
        RelationalState, RelationalTableSchema, RelationalTransaction, RelationalValue,
        RelationalWrite,
    },
};
use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::Path;
#[cfg(test)]
use std::sync::{mpsc::Sender, Barrier, Condvar};
use std::sync::{Arc, Mutex};

#[cfg(test)]
thread_local! {
    static GRAPH_COMMIT_PREPARATION_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn set_graph_commit_preparation_hook(hook: Box<dyn FnOnce()>) {
    GRAPH_COMMIT_PREPARATION_HOOK.with(|slot| *slot.borrow_mut() = Some(hook));
}

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
    pub isolation: super::TransactionIsolation,
    pub read_dependency_limits: super::TransactionReadDependencyLimits,
}

impl ConcurrentTransactionOptions {
    pub const fn with_isolation(mut self, isolation: super::TransactionIsolation) -> Self {
        self.isolation = isolation;
        self
    }

    pub const fn optimistic() -> Self {
        Self {
            isolation: super::TransactionIsolation::SnapshotIsolation,
            read_dependency_limits: super::TransactionReadDependencyLimits::DEFAULT,
            mode: ConcurrentTransactionMode::Optimistic,
            lock_timeout: DEFAULT_PESSIMISTIC_LOCK_TIMEOUT,
        }
    }

    pub const fn pessimistic(lock_timeout: Duration) -> Self {
        Self {
            isolation: super::TransactionIsolation::SnapshotIsolation,
            read_dependency_limits: super::TransactionReadDependencyLimits::DEFAULT,
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

pub(crate) struct ExclusiveDatabaseLease {
    inner: Arc<ConcurrentDatabaseInner>,
    transaction_id: u64,
}

impl Drop for ExclusiveDatabaseLease {
    fn drop(&mut self) {
        self.inner.locks.release(self.transaction_id);
    }
}

#[derive(Debug)]
struct ConcurrentDatabaseInner {
    commits: CommitSequencer,
    locks: LockManager,
    transaction_ids: TransactionIdAllocator,
    checkpoint_serial: Mutex<()>,
    #[cfg(test)]
    autocommit_read_gate: Mutex<Option<AutocommitReadGate>>,
    #[cfg(test)]
    autocommit_mutation_gate: Mutex<Option<autocommit::AutocommitMutationGate>>,
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
    abort_reason: Option<TransactionAbortReason>,
    finished: bool,
    task_context: Option<hawdb_core::RuntimeTaskContext>,
    // Dropped after workspace fields; queued requests retain another owner.
    admission: Option<Arc<hawdb_qos::RuntimePermit>>,
}

impl ConcurrentDatabase {
    pub fn new(database: Database) -> Self {
        Self::new_with_wal_group_commit(database, WalGroupCommitConfig::default())
    }

    pub fn new_with_wal_group_commit(
        database: Database,
        wal_group_commit: WalGroupCommitConfig,
    ) -> Self {
        Self::with_sequencer(CommitSequencer::new(database, wal_group_commit))
    }

    pub(crate) fn with_shared_owner(owner: Arc<dyn SharedDatabaseOwner>) -> Result<Self> {
        Ok(Self::with_sequencer(CommitSequencer::shared(
            owner,
            WalGroupCommitConfig::default(),
        )?))
    }

    fn with_sequencer(commits: CommitSequencer) -> Self {
        Self {
            inner: Arc::new(ConcurrentDatabaseInner {
                commits,
                locks: LockManager::default(),
                transaction_ids: TransactionIdAllocator::default(),
                checkpoint_serial: Mutex::new(()),
                #[cfg(test)]
                autocommit_read_gate: Mutex::new(None),
                #[cfg(test)]
                autocommit_mutation_gate: Mutex::new(None),
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

    /// Returns the last completed publication, without waiting for an active writer.
    pub fn commit_epoch(&self) -> Result<u64> {
        Ok(self.published_read_view()?.visible_commit_epoch())
    }

    /// Includes the physical generation refreshed by completed checkpoints.
    pub fn published_read_view(&self) -> Result<crate::store::PublishedReadView> {
        Ok(self
            .inner
            .commits
            .read_view()?
            .snapshot
            .0
            .published_read_view())
    }

    pub fn wal_group_commit_snapshot(&self) -> Result<WalGroupCommitSnapshot> {
        self.inner.commits.group_commit_snapshot()
    }

    /// Enables recording for future coordinator phases and graph executions.
    /// This is idempotent and shared by cloned handles. Recording starts at
    /// phase entry; already-active phases are not included retroactively.
    pub fn enable_concurrency_metrics(&self) {
        self.inner.commits.metrics.enable();
    }

    /// Returns cumulative phase timings and graph replay counts without waiting
    /// for an active writer. Recording is disabled by default; enable it with
    /// [`Self::enable_concurrency_metrics`]. Clones observe the same counters.
    pub fn concurrency_metrics(&self) -> ConcurrentDatabaseMetrics {
        self.inner.commits.metrics.snapshot()
    }

    #[cfg(test)]
    pub(crate) fn cancel_after_next_group_flush(
        &self,
        token: hawdb_core::RuntimeCancellationToken,
    ) -> Result<()> {
        self.inner.commits.cancel_after_next_group_flush(token)
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
    pub(crate) fn set_group_commit_enqueue_gate(
        &self,
        gate: Arc<(Mutex<bool>, Condvar)>,
    ) -> Result<()> {
        self.inner.commits.set_group_commit_enqueue_gate(gate)
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
    /// commit view used by writers. The reader epoch excludes the idle internal
    /// publication, but includes borrowed queries and explicit reader/writer pins.
    pub fn storage_pressure_snapshot(&self) -> Result<StoragePressureSnapshot> {
        self.inner.commits.storage_pressure_snapshot()
    }

    /// Returns the generation-pinned storage residency view without scanning
    /// candidate artifacts or materializing rows.
    pub fn storage_residency_report(&self) -> Result<crate::store::StorageResidencyReport> {
        self.inner.commits.lock()?.storage_residency_report()
    }

    /// Returns the recovery boundary observed by the currently published
    /// database handle.
    pub fn storage_recovery_report(&self) -> Result<StorageRecoveryReport> {
        self.inner.commits.lock()?.storage_recovery_report()
    }

    pub fn automatic_checkpoint_report(&self) -> Result<Option<super::AutomaticCheckpointReport>> {
        self.inner.commits.lock()?.automatic_checkpoint_report()
    }

    /// Pins the last completed publication while a writer or group sync is active.
    pub fn begin_read_transaction(&self) -> Result<DatabaseReadTransaction> {
        let view = self.inner.commits.read_view()?;
        let snapshot = view.begin_read_transaction();
        self.inner.commits.finish_read(&view, snapshot)
    }

    /// Completes a caller-triggered checkpoint through the writer coordinator.
    ///
    /// Like [`Database::checkpoint`], this blocking call works below automatic
    /// thresholds and with background maintenance disabled. A successful return
    /// follows durable publication of the captured source. It serializes manual
    /// callers and coordinates with the automatic owner, preserving read pins.
    /// Concurrent source changes and storage/resource errors remain errors; a
    /// failed attempt does not undo already committed writes.
    pub fn checkpoint(&self) -> Result<()> {
        let _checkpoint_serial = self
            .inner
            .checkpoint_serial
            .lock()
            .map_err(|_| checkpoint_coordinator_poisoned_error())?;
        let source = self.inner.commits.lock()?.checkpoint_source()?;
        let prepared = source.prepare()?;
        let Some(prepared) = prepared else {
            self.inner
                .commits
                .lock()?
                .runtime
                .get_mut()?
                .store
                .reclaim_version_history();
            return Ok(());
        };
        self.inner
            .commits
            .lock()?
            .publish_prepared_checkpoint(prepared)
    }

    /// Runs project reclamation through the writer coordinator. Active readers,
    /// transactions and checkpoint candidates retain their reachability pins.
    pub fn reclaim_branch_storage(
        &self,
        limits: super::BranchReclamationLimits,
    ) -> Result<super::BranchReclamationReport> {
        self.inner.commits.reclaim_branch_storage(limits)
    }

    /// Reads the last pending-create scan without admitting a data runtime.
    pub fn branch_create_recovery_report(
        &self,
    ) -> Result<Option<super::BranchCreateRecoveryReport>> {
        Ok(self
            .inner
            .commits
            .lock()?
            .branch_create_recovery_report()
            .cloned())
    }

    /// Retries pending child receipts under the writer coordinator. Existing
    /// data publications and reader pins are unaffected by metadata recovery.
    pub fn recover_pending_branch_creates(
        &self,
        limits: super::BranchCreateRecoveryLimits,
    ) -> Result<super::BranchCreateRecoveryReport> {
        self.inner
            .commits
            .lock()?
            .recover_pending_branch_creates(limits)
    }

    pub fn begin_transaction(
        &self,
        options: ConcurrentTransactionOptions,
    ) -> Result<ConcurrentDatabaseTransaction> {
        let transaction_id = self.inner.transaction_ids.allocate()?;
        self.begin_transaction_with_id(options, transaction_id)
    }

    fn begin_transaction_with_id(
        &self,
        options: ConcurrentTransactionOptions,
        transaction_id: u64,
    ) -> Result<ConcurrentDatabaseTransaction> {
        let database = self.inner.commits.lock()?;
        let base_commit_epoch = database.commit_epoch()?;
        Ok(ConcurrentDatabaseTransaction {
            inner: Arc::clone(&self.inner),
            transaction_id,
            base_commit_epoch,
            options,
            runtime: DatabaseTransactionRuntime::from_database(&database)?,
            state: DatabaseTransactionState::from_database_with_isolation(
                &database,
                options.isolation,
                options.read_dependency_limits,
            )?,
            successful_statements: 0,
            abort_reason: None,
            finished: false,
            task_context: None,
            admission: None,
        })
    }

    /// Begins one transaction using a permit already admitted by the host's
    /// RuntimeGovernor. The transaction does not acquire a second permit.
    ///
    /// Admission precedes snapshot creation and lasts through queued commit,
    /// durability and result delivery. The caller owns admission waiting and
    /// must supply a Mutation request with suitable statement/result budgets.
    /// Existing mutation limits still bound private staging; this permit does
    /// not account every COW allocation or bound host thread count.
    pub fn begin_admitted_transaction(
        &self,
        options: ConcurrentTransactionOptions,
        permit: hawdb_qos::RuntimePermit,
        task_context: hawdb_core::RuntimeTaskContext,
    ) -> Result<ConcurrentDatabaseTransaction> {
        if permit.request().kind != hawdb_qos::RuntimeWorkKind::Mutation {
            return Err(HawDBError::Execution(
                "concurrent transaction admission requires a mutation permit".into(),
            ));
        }
        let task_context = permit.bind_task_context(task_context);
        super::query_runtime::query_runtime_checkpoint(Some(&task_context))?;
        let mut transaction = self.begin_transaction(options)?;
        super::query_runtime::query_runtime_checkpoint(Some(&task_context))?;
        transaction.task_context = Some(task_context);
        transaction.admission = Some(Arc::new(permit));
        Ok(transaction)
    }

    pub fn query(&self, cypher_text: &str) -> Result<QueryOutput> {
        self.query_with_params(cypher_text, &BTreeMap::new())
    }

    pub fn query_with_params(
        &self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        self.query_request(super::QueryRequest::new(cypher_text).with_params(parameters))
    }

    pub fn query_request(&self, request: super::QueryRequest<'_>) -> Result<QueryOutput> {
        super::query_runtime::query_runtime_checkpoint(request.task_context)?;
        let cypher_text = request.cypher;
        let started = Instant::now();
        let view = self.inner.commits.read_view()?;
        let mut prepared = match view.prepare(request) {
            Ok(prepared) => prepared,
            Err(_) => {
                // Failed preparation cannot prove that the statement is read-only. Re-run it
                // through the original exclusive path so planning failures retain their
                // statement-observability behavior and uncertain statements fail closed.
                drop(view);
                return self.with_autocommit_exclusive(|database| database.query_request(request));
            }
        };
        if prepared.uses_transaction_kernel() {
            drop(view);
            return self
                .query_prepared_mutation(prepared, request, false, None, None)
                .map(|(output, _)| output);
        }
        if !prepared.uses_read_snapshot() {
            drop(view);
            return self.with_autocommit_exclusive(move |database| {
                database.query_prepared_request(prepared, request)
            });
        }
        let statement_kind = prepared.statement_kind();
        let parse_nanos = prepared.parse_nanos();
        let query_identity = prepared.take_query_identity(cypher_text);
        #[cfg(test)]
        self.wait_after_autocommit_read_snapshot()?;
        let result = self
            .inner
            .commits
            .finish_read(&view, view.query_prepared_request(prepared, request));
        view.recorder.record_statement_execution(
            "cypher",
            cypher_text,
            statement_kind,
            started,
            result.as_ref().map(|profiled| &profiled.output),
            StatementExecutionContext {
                query_identity: query_identity.as_ref(),
                execution_profile: result
                    .as_ref()
                    .ok()
                    .map(|profiled| &profiled.execution_profile),
                retained_profile: None,
                access_control: request.access_control,
                parse_nanos,
            },
        );
        self.inner
            .commits
            .finish_read(&view, result.map(|profiled| profiled.output))
    }

    pub fn query_sql(&self, sql_text: &str) -> Result<QueryOutput> {
        self.query_sql_with_params(sql_text, &[])
    }

    pub fn query_sql_with_params(
        &self,
        sql_text: &str,
        parameters: &[Value],
    ) -> Result<QueryOutput> {
        self.query_sql_request(super::SqlQueryRequest::new(sql_text).with_params(parameters))
    }

    pub fn query_sql_request(&self, request: super::SqlQueryRequest<'_>) -> Result<QueryOutput> {
        super::query_runtime::query_runtime_checkpoint(request.task_context)?;
        let sql_text = request.sql;
        let started = Instant::now();
        let prepared = self.inner.commits.prepare_sql(sql_text)?;
        self.query_prepared_sql_request(request, prepared, started)
    }

    pub(crate) fn query_prepared_sql_request(
        &self,
        request: super::SqlQueryRequest<'_>,
        prepared: crate::relational_sql::PreparedRelationalSql,
        started: Instant,
    ) -> Result<QueryOutput> {
        super::query_runtime::query_runtime_checkpoint(request.task_context)?;
        let sql_text = request.sql;
        if let SqlStatement::Branch(statement) = prepared.statement() {
            if matches!(statement, crate::sql::BranchSqlStatement::UseBranch(_)) {
                return Err(HawDBError::BranchCommandUnsupported {
                    command: "USE BRANCH",
                    context: "shared concurrent runtime",
                });
            }
            // Catalog statements have no schema/data dependency. Keep them
            // usable while the default branch is busy or requires repair.
            #[cfg(test)]
            if !statement.changes_context_or_catalog() {
                self.wait_after_autocommit_read_snapshot()?;
            }
            return self.with_autocommit_exclusive(|database| {
                database.query_sql_with_prepared_request(request, prepared)
            });
        }
        let view = self.inner.commits.read_view()?;
        if !sql_statement_uses_snapshot(prepared.statement()) {
            return self.query_prepared_sql_mutation(request, prepared, view, started);
        }
        let statement_kind = super::observability::sql_statement_kind(prepared.statement());
        #[cfg(test)]
        self.wait_after_autocommit_read_snapshot()?;
        let result = self
            .inner
            .commits
            .finish_read(&view, view.query_sql(request, prepared));
        view.recorder.record_statement_execution(
            "sql",
            sql_text,
            statement_kind,
            started,
            result.as_ref(),
            StatementExecutionContext::default(),
        );
        self.inner.commits.finish_read(&view, result)
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
                    HawDBError::Execution("concurrent append result slot is poisoned".to_string())
                })? = Some(result);
                Ok(QueryOutput {
                    rows: Vec::new().into(),
                })
            })
            .map(|_| ())?;
        committed_result
            .lock()
            .map_err(|_| {
                HawDBError::Execution("concurrent append result slot is poisoned".to_string())
            })?
            .take()
            .ok_or_else(|| {
                HawDBError::Execution(
                    "concurrent append completed without a commit result".to_string(),
                )
            })
    }

    pub(crate) fn with_autocommit_exclusive(
        &self,
        execute: impl FnOnce(&mut Database) -> Result<QueryOutput>,
    ) -> Result<QueryOutput> {
        let _lease = self.exclusive_database_lease()?;
        self.inner
            .commits
            .lock()
            .and_then(|mut database| execute(&mut database))
    }

    pub(crate) fn exclusive_database_lease(&self) -> Result<ExclusiveDatabaseLease> {
        let transaction_id = self.inner.transaction_ids.allocate()?;
        self.inner.locks.acquire(
            transaction_id,
            &[LockRequest::database(LockMode::Exclusive)],
            Instant::now(),
            DEFAULT_PESSIMISTIC_LOCK_TIMEOUT,
        )?;
        Ok(ExclusiveDatabaseLease {
            inner: Arc::clone(&self.inner),
            transaction_id,
        })
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
                HawDBError::Execution(
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

#[derive(Debug)]
enum TransactionAbortReason {
    Retryable(Box<TransactionRetryError>),
    Other(String),
}

impl ConcurrentDatabaseTransaction {
    pub fn read_dependency_usage(&self) -> Option<super::TransactionReadDependencyUsage> {
        self.state.read_dependency_usage()
    }

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
        self.query_request(super::QueryRequest::new(cypher_text).with_params(parameters))
    }

    pub fn query_request(&mut self, request: super::QueryRequest<'_>) -> Result<QueryOutput> {
        let result = self.query_request_inner(request);
        if result.is_err() {
            self.state.fail_read_dependencies();
        }
        result
    }

    fn query_request_inner(&mut self, request: super::QueryRequest<'_>) -> Result<QueryOutput> {
        self.ensure_active()?;
        let lock_savepoint = (self.options.mode == ConcurrentTransactionMode::Pessimistic)
            .then(|| self.inner.locks.savepoint(self.transaction_id))
            .transpose()?;
        self.inner.commits.metrics.graph_execution();
        let task = super::query_request::bind_request_context(
            self.task_context.as_ref(),
            request.task_context,
        )?;
        let request = super::QueryRequest {
            task_context: task.as_ref(),
            ..request
        };
        let prepared =
            PreparedGraphTransactionRequest::prepare(&self.runtime, &mut self.state, request)?;
        let execution = prepared.execute(&self.runtime, &mut self.state);
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
            let covered = if lock_savepoint
                .as_ref()
                .is_some_and(coordinator::LockSavepoint::is_empty)
            {
                // A first mutation has no reusable locks; avoid a second
                // lock-manager acquisition just to establish that fact.
                Ok(false)
            } else {
                self.inner.locks.covers_all(self.transaction_id, &requests)
            };
            match covered {
                Ok(true) => {
                    // This execution already ran under every required lock. Keep
                    // its staged result and read dependencies; no lock is released
                    // until this transaction finishes. New targets/upgrades still
                    // use the acquisition, epoch fence and replay below.
                    self.inner.commits.metrics.graph_reuse();
                    self.successful_statements = self.successful_statements.saturating_add(1);
                    return Ok(outcome.output);
                }
                Ok(false) => {}
                Err(error) => {
                    self.state.restore_graph_statement(graph_savepoint);
                    if let Some(lock_savepoint) = lock_savepoint {
                        self.inner
                            .locks
                            .restore(self.transaction_id, lock_savepoint);
                    }
                    return Err(error);
                }
            }
            self.state.restore_graph_statement(graph_savepoint);
            self.acquire_graph_statement_locks(&requests, lock_savepoint.clone())?;
            self.inner.commits.metrics.graph_replay();
            // Restoring the statement savepoint preserves the preparation's
            // schema and parameters. The epoch fence prevents a new snapshot
            // from being substituted under the already-derived lock set.
            let replayed = prepared.execute(&self.runtime, &mut self.state);
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
        self.query_sql_request_with_result(
            super::SqlQueryRequest::new(sql_text).with_params(parameters),
        )
    }

    pub fn query_sql_request(
        &mut self,
        request: super::SqlQueryRequest<'_>,
    ) -> Result<QueryOutput> {
        self.query_sql_request_with_result(request)
            .map(|result| result.output)
    }

    pub fn query_sql_request_with_result(
        &mut self,
        request: super::SqlQueryRequest<'_>,
    ) -> Result<super::SqlStatementResult> {
        let result = self.query_sql_request_inner(request);
        if result.is_err() {
            self.state.fail_read_dependencies();
        }
        result
    }

    fn query_sql_request_inner(
        &mut self,
        request: super::SqlQueryRequest<'_>,
    ) -> Result<super::SqlStatementResult> {
        self.ensure_active()?;
        let task = super::query_request::bind_request_context(
            self.task_context.as_ref(),
            request.task_context,
        )?;
        super::query_runtime::query_runtime_checkpoint(task.as_ref())?;
        let sql_text = request.sql;
        let parameters = request.parameters;
        if let Some(dependencies) = &mut self.state.read_dependencies {
            super::query_runtime::query_runtime_checkpoint(task.as_ref())?;
            dependencies.admit_sql(sql_text, parameters)?;
        }
        let prepared = self
            .runtime
            .relational_plan_template_cache
            .prepare(sql_text)?;
        if self.options.mode == ConcurrentTransactionMode::Pessimistic {
            let requests = sql_lock_requests(
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
        self.ensure_active()?;
        let result = execute_database_transaction_prepared_sql(
            &self.runtime,
            &mut self.state,
            sql_text,
            prepared,
            parameters,
            DatabaseTransactionSqlOptions {
                allow_system_schema_registry_write: false,
                allow_locking_select: true,
                task_context: task.as_ref(),
                output_limits: request.output_limits,
                join_planning: request.join_planning,
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
        if let Some(dependencies) = &self.state.read_dependencies {
            dependencies.ensure_complete()?;
        }
        let started = Instant::now();
        if self.options.mode == ConcurrentTransactionMode::Optimistic
            && let Err(error) = self.inner.locks.acquire(
                self.transaction_id,
                &[LockRequest::database(LockMode::OptimisticCommit)],
                started,
                self.options.lock_timeout,
            )
        {
            self.finished = true;
            return Err(error.into());
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
        super::query_runtime::query_runtime_checkpoint(self.task_context.as_ref())?;
        let graph_only = state.relational_transaction.writes.is_empty()
            && state.append_transaction.writes.is_empty();
        let mut prepared = state
            .graph_transaction
            .take()
            .expect("concurrent transaction owns a graph workspace")
            .compact_for_commit(self.runtime.config.mutation_limits)?;
        #[cfg(test)]
        GRAPH_COMMIT_PREPARATION_HOOK.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook();
            }
        });
        prepared.encode_graph_wal(graph_only)?;
        super::query_runtime::query_runtime_checkpoint(self.task_context.as_ref())?;
        let committed_result = Arc::new(Mutex::new(None));
        let result_slot = Arc::clone(&committed_result);
        let task_context = self.task_context.clone();
        let result =
            inner
                .commits
                .execute_grouped_with_admission(self.admission.clone(), move |database| {
                    // Cancellation before the commit boundary rejects without WAL.
                    // Never turn a durable success into cancellation afterward.
                    super::query_runtime::query_runtime_checkpoint(task_context.as_ref())?;
                    let result = commit_prepared_database_transaction_state(
                        database,
                        &mut state,
                        prepared,
                        allow_stale_rebase,
                    )?;
                    let output = result.output.clone();
                    *result_slot.lock().map_err(|_| {
                        HawDBError::Execution(
                            "concurrent transaction result slot is poisoned".to_string(),
                        )
                    })? = Some(result);
                    Ok(output)
                });
        self.inner.locks.release(self.transaction_id);
        self.finished = true;
        result?;
        committed_result
            .lock()
            .map_err(|_| {
                HawDBError::Execution("concurrent transaction result slot is poisoned".to_string())
            })?
            .take()
            .ok_or_else(|| {
                HawDBError::Execution(
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
            let error: HawDBError = error.into();
            self.abort_after_lock_failure(&error);
            return Err(error);
        }

        let database = match inner.commits.lock() {
            Ok(database) => database,
            Err(error) => {
                self.abort_after_lock_failure(&error);
                return Err(error);
            }
        };
        let current_epoch = database.commit_epoch()?;
        if current_epoch != self.base_commit_epoch
            && !requests_already_covered
            && self.options.isolation == super::TransactionIsolation::SnapshotIsolation
        {
            if self.successful_statements == 0 {
                self.base_commit_epoch = current_epoch;
                self.runtime = DatabaseTransactionRuntime::from_database(&database)?;
                self.state = DatabaseTransactionState::from_database(&database)?;
            } else {
                let error = HawDBError::TransactionRetryRequired(Box::new(TransactionRetryError {
                    transaction_id: self.transaction_id,
                    reason: TransactionRetryReason::SnapshotChanged {
                        read_epoch: self.base_commit_epoch,
                        committed_epoch: current_epoch,
                    },
                    already_aborted: false,
                }));
                drop(database);
                self.abort_after_lock_failure(&error);
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
            let abort_transaction = error.aborts_transaction();
            let error: HawDBError = error.into();
            if let Some(lock_savepoint) = lock_savepoint {
                inner.locks.restore(self.transaction_id, lock_savepoint);
            }
            if abort_transaction {
                self.abort_after_lock_failure(&error);
            }
            return Err(error);
        }

        let database = match inner.commits.lock() {
            Ok(database) => database,
            Err(error) => {
                self.abort_after_lock_failure(&error);
                return Err(error);
            }
        };
        let current_epoch = database.commit_epoch()?;
        if current_epoch != self.base_commit_epoch && !requests_already_covered {
            // The requests were derived by executing the statement against the
            // pinned snapshot. Refreshing here could change the matched graph
            // entities and make that access set incomplete. Fail closed and let
            // the caller retry from a new transaction instead.
            let error = HawDBError::TransactionRetryRequired(Box::new(TransactionRetryError {
                transaction_id: self.transaction_id,
                reason: TransactionRetryReason::SnapshotChanged {
                    read_epoch: self.base_commit_epoch,
                    committed_epoch: current_epoch,
                },
                already_aborted: false,
            }));
            drop(database);
            self.abort_after_lock_failure(&error);
            return Err(error);
        }
        Ok(())
    }

    fn abort_after_lock_failure(&mut self, error: &HawDBError) {
        self.state.rollback();
        self.abort_reason = Some(match error {
            HawDBError::TransactionRetryRequired(error) => {
                TransactionAbortReason::Retryable(error.clone())
            }
            _ => TransactionAbortReason::Other(error.to_string()),
        });
        self.inner.locks.release(self.transaction_id);
    }

    fn ensure_active(&self) -> Result<()> {
        if self.finished {
            return Err(HawDBError::Execution(format!(
                "concurrent transaction {} is already finished",
                self.transaction_id
            )));
        }
        if let Some(reason) = &self.abort_reason {
            return Err(match reason {
                TransactionAbortReason::Retryable(error) => {
                    let mut error = error.clone();
                    error.already_aborted = true;
                    HawDBError::TransactionRetryRequired(error)
                }
                TransactionAbortReason::Other(reason) => HawDBError::Execution(format!(
                    "concurrent transaction {} is aborted: {reason}",
                    self.transaction_id
                )),
            });
        }
        super::query_runtime::query_runtime_checkpoint(self.task_context.as_ref())
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
        return Err(HawDBError::Semantic(
            "FOR UPDATE/SHARE requires a pessimistic concurrent transaction".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn sql_statement_uses_snapshot(statement: &SqlStatement) -> bool {
    match statement {
        SqlStatement::Select(select) => select.lock_strength.is_none(),
        SqlStatement::Explain(explain) => sql_statement_uses_snapshot(&explain.statement),
        SqlStatement::Insert(_)
        | SqlStatement::CreateMaterializedView(_)
        | SqlStatement::DropMaterializedView(_)
        | SqlStatement::Update(_)
        | SqlStatement::Delete(_)
        | SqlStatement::CreateTable(_)
        | SqlStatement::CreateIndex(_)
        | SqlStatement::AlterTableAddColumn(_) => false,
        // Branch catalog inspection uses the immutable read view. Lifecycle
        // mutations use the exclusive sequencer; USE BRANCH is rejected before
        // routing because a shared runtime cannot change its branch context.
        SqlStatement::Branch(statement) => !statement.changes_context_or_catalog(),
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
    prepared: &crate::relational_sql::PreparedRelationalSql,
    parameters: &[Value],
    state: &RelationalState,
    append_state: &hawdb_storage::append_table::AppendState,
) -> Result<Vec<LockRequest>> {
    if prepared.template.parameters.len() != parameters.len() {
        return Err(HawDBError::Semantic(format!(
            "PostgreSQL statement requires {} parameters, but {} parameters were supplied",
            prepared.template.parameters.len(),
            parameters.len()
        )));
    }
    let append_lock_mode = match prepared.statement() {
        SqlStatement::Select(select)
            if select
                .from
                .as_ref()
                .is_some_and(|from| append_state.schema(&from.name).is_some()) =>
        {
            Some(LockMode::Shared)
        }
        SqlStatement::Explain(explain)
            if explain.analyze
                && matches!(
                    explain.statement.as_ref(),
                    SqlStatement::Select(select)
                        if select
                            .from
                            .as_ref()
                            .is_some_and(|from| append_state.schema(&from.name).is_some())
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
                return Err(HawDBError::Semantic(
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
            let compiled =
                crate::relational_sql::compile_prepared_relational_statement_with_result(
                    &prepared.template,
                    parameters,
                    state,
                )?;
            Ok(insert_lock_requests(&compiled.transaction, state)
                .unwrap_or_else(|| vec![LockRequest::database(LockMode::Exclusive)]))
        }
        SqlStatement::Update(update) => {
            crate::relational_sql::compile_prepared_relational_statement_with_result(
                &prepared.template,
                parameters,
                state,
            )?;
            Ok(update_lock_requests(update, parameters, state))
        }
        SqlStatement::Delete(delete) => {
            crate::relational_sql::compile_prepared_relational_statement_with_result(
                &prepared.template,
                parameters,
                state,
            )?;
            Ok(delete_lock_requests(delete, parameters, state))
        }
        SqlStatement::CreateTable(_)
        | SqlStatement::CreateMaterializedView(_)
        | SqlStatement::DropMaterializedView(_)
        | SqlStatement::CreateIndex(_)
        | SqlStatement::AlterTableAddColumn(_) => {
            Ok(vec![LockRequest::database(LockMode::Exclusive)])
        }
        SqlStatement::Branch(statement) if statement.changes_context_or_catalog() => {
            Ok(vec![LockRequest::database(LockMode::Exclusive)])
        }
        // Only catalog inspection can participate with a shared lock. The
        // executor separately rejects lifecycle changes in explicit transactions.
        SqlStatement::Branch(_) => Ok(vec![LockRequest::database(LockMode::Shared)]),
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
            select.from_table().name.clone(),
        )];
        requests.extend(
            select
                .joins
                .iter()
                .map(|join| LockRequest::relational_table(mode, join.table.name.clone())),
        );
        return requests;
    }
    if !is_public_table(select.from_table()) {
        return vec![LockRequest::database(mode)];
    }
    let Some(schema) = state.table_schema(&select.from_table().name) else {
        return vec![LockRequest::database(mode)];
    };
    let ranges = match &select.selection {
        None => Some(vec![(Bound::Unbounded, Bound::Unbounded)]),
        Some(predicate) if schema.primary_key.len() == 1 => single_key_ranges(
            predicate,
            schema,
            select.from_table(),
            select.from_alias.as_deref(),
            parameters,
        ),
        Some(predicate) => composite_key_point(
            predicate,
            schema,
            select.from_table(),
            select.from_alias.as_deref(),
            parameters,
        )
        .map(|range| vec![range]),
    };
    let Some(ranges) = ranges else {
        return vec![LockRequest::relational_table(
            mode,
            select.from_table().name.clone(),
        )];
    };
    ranges
        .into_iter()
        .map(|(lower, upper)| {
            LockRequest::relational_range(
                mode,
                select.from_table().name.clone(),
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
        single_key_ranges(selection, schema, table, alias, parameters)
    } else {
        composite_key_point(selection, schema, table, alias, parameters).map(|range| vec![range])
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
    schema: &RelationalTableSchema,
    table: &SqlTableName,
    alias: Option<&str>,
    parameters: &[Value],
) -> Option<Vec<(Bound<RelationalKey>, Bound<RelationalKey>)>> {
    match predicate {
        Expr {
            kind: ExprKind::And(left, right),
            ..
        } => {
            let left = single_key_ranges(left, schema, table, alias, parameters);
            let right = single_key_ranges(right, schema, table, alias, parameters);
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
            let mut ranges = single_key_ranges(left, schema, table, alias, parameters)?;
            ranges.extend(single_key_ranges(right, schema, table, alias, parameters)?);
            Some(ranges)
        }
        Expr {
            kind: ExprKind::Compare { left, op, right },
            ..
        } if left
            .as_column()
            .is_some_and(|left| column_matches(left, &schema.primary_key[0], table, alias)) =>
        {
            let key = RelationalKey(vec![bind_sql_lock_value(
                right.as_value()?,
                parameters,
                schema.columns[schema.column_position(&schema.primary_key[0])?].scalar_type,
            )?]);
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
            .is_some_and(|left| column_matches(left, &schema.primary_key[0], table, alias)) =>
        {
            values
                .iter()
                .map(|value| {
                    let key = RelationalKey(vec![bind_sql_lock_value(
                        value.as_value()?,
                        parameters,
                        schema.columns[schema.column_position(&schema.primary_key[0])?].scalar_type,
                    )?]);
                    Some((Bound::Included(key.clone()), Bound::Included(key)))
                })
                .collect()
        }
        _ => None,
    }
}

fn composite_key_point(
    predicate: &SqlPredicate,
    schema: &RelationalTableSchema,
    table: &SqlTableName,
    alias: Option<&str>,
    parameters: &[Value],
) -> Option<(Bound<RelationalKey>, Bound<RelationalKey>)> {
    let mut values = BTreeMap::new();
    collect_composite_equalities(predicate, schema, table, alias, parameters, &mut values)?;
    let key = RelationalKey(
        schema
            .primary_key
            .iter()
            .map(|column| values.remove(column))
            .collect::<Option<Vec<_>>>()?,
    );
    Some((Bound::Included(key.clone()), Bound::Included(key)))
}

fn collect_composite_equalities(
    predicate: &SqlPredicate,
    schema: &RelationalTableSchema,
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
            collect_composite_equalities(left, schema, table, alias, parameters, values)?;
            collect_composite_equalities(right, schema, table, alias, parameters, values)
        }
        Expr {
            kind:
                ExprKind::Compare {
                    left,
                    op: SqlComparisonOp::Eq,
                    right,
                },
            ..
        } if schema.primary_key.iter().any(|column| {
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
                bind_sql_lock_value(
                    right.as_value()?,
                    parameters,
                    schema.columns[schema.column_position(&left.name)?].scalar_type,
                )?,
            );
            Some(())
        }
        _ => None,
    }
}

fn bind_sql_lock_value(
    value: &SqlValue,
    parameters: &[Value],
    scalar_type: hawdb_storage::relational::RelationalScalarType,
) -> Option<RelationalValue> {
    let value = crate::relational_sql::bind_relational_value(value.clone(), parameters).ok()?;
    hawdb_relational::coerce_relational_value(value, scalar_type).ok()
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

fn checkpoint_coordinator_poisoned_error() -> HawDBError {
    HawDBError::Execution("concurrent checkpoint coordinator is poisoned".to_string())
}

#[cfg(test)]
fn autocommit_read_gate_poisoned_error() -> HawDBError {
    HawDBError::Execution("concurrent autocommit read gate is poisoned".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use hawdb_storage::{
        append_table::{AppendTableSchema, AppendWrite},
        relational::{RelationalColumnSchema, RelationalScalarType},
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
            "hawdb-concurrent-append-{}-{}",
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
        let before = database.commit_epoch().unwrap();
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
        let append_state = hawdb_storage::append_table::AppendState::default()
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
                hawdb_storage::append_table::AppendMutationLimits::default(),
            )
            .expect("stage append schema");

        const SQL: &str = "EXPLAIN ANALYZE SELECT * FROM events \
                           WHERE stream = 'alpha' ORDER BY sequence LIMIT 10";
        let cache = crate::relational_sql::RelationalPlanTemplateCache::new(Some(4));
        let prepared = cache.prepare(SQL).expect("prepare append explain analyze");
        let requests =
            sql_lock_requests(&prepared, &[], &RelationalState::default(), &append_state)
                .expect("derive append explain analyze locks");

        assert_eq!(requests, vec![LockRequest::database(LockMode::Shared)]);
    }

    #[test]
    fn branch_lifecycle_lock_requests_are_exclusive_while_inspection_is_shared() {
        let cache = crate::relational_sql::RelationalPlanTemplateCache::new(Some(8));
        for (sql, mode) in [
            ("SHOW CURRENT BRANCH", LockMode::Shared),
            ("SHOW BRANCH NAME 'main'", LockMode::Shared),
            ("SHOW BRANCHES LIMIT 10", LockMode::Shared),
            ("USE BRANCH main", LockMode::Exclusive),
            (
                "CREATE BRANCH child FROM main AT REVISION 1 REQUEST KEY 'key'",
                LockMode::Exclusive,
            ),
            (
                "DROP BRANCH ID '00000000-0000-0000-0000-000000000001' AT REVISION 1",
                LockMode::Exclusive,
            ),
        ] {
            let prepared = cache.prepare(sql).unwrap();
            let requests = sql_lock_requests(
                &prepared,
                &[],
                &RelationalState::default(),
                &hawdb_storage::append_table::AppendState::default(),
            )
            .unwrap();
            assert_eq!(requests, vec![LockRequest::database(mode)], "{sql}");
        }
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
