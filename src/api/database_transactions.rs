// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Database transactions implementation behind the embedded facade.

use super::*;

impl Database {
    /// Begins a transaction after recovering the selected branch. Admission
    /// failure leaves the project handle available for retry or SQL selection.
    pub fn begin_transaction(&mut self) -> Result<DatabaseTransaction<'_>> {
        self.begin_transaction_inner(None)
    }

    /// Begins a transaction with explicit isolation and retained-evidence limits.
    pub fn begin_transaction_with_isolation(
        &mut self,
        isolation: TransactionIsolation,
        limits: TransactionReadDependencyLimits,
    ) -> Result<DatabaseTransaction<'_>> {
        self.runtime.get_mut()?;
        let state =
            DatabaseTransactionState::from_database_with_isolation(self, isolation, limits)?;
        let runtime = DatabaseTransactionRuntime::from_database(self)?;
        Ok(DatabaseTransaction {
            db: self,
            runtime,
            state,
            task_context: None,
        })
    }

    /// Begins an explicitly isolated transaction with cooperative host cancellation.
    pub fn begin_transaction_with_isolation_and_context(
        &mut self,
        isolation: TransactionIsolation,
        limits: TransactionReadDependencyLimits,
        task_context: &hawdb_core::RuntimeTaskContext,
    ) -> Result<DatabaseTransaction<'_>> {
        query_runtime::query_runtime_checkpoint(Some(task_context))?;
        let mut transaction = self.begin_transaction_with_isolation(isolation, limits)?;
        transaction.task_context = Some(task_context.clone());
        Ok(transaction)
    }

    pub fn begin_transaction_with_context(
        &mut self,
        task_context: &hawdb_core::RuntimeTaskContext,
    ) -> Result<DatabaseTransaction<'_>> {
        self.begin_transaction_inner(Some(task_context.clone()))
    }

    pub(super) fn begin_transaction_inner(
        &mut self,
        task_context: Option<hawdb_core::RuntimeTaskContext>,
    ) -> Result<DatabaseTransaction<'_>> {
        let (runtime, state) = self.transaction_parts()?;
        Ok(DatabaseTransaction {
            db: self,
            runtime,
            state,
            task_context,
        })
    }

    /// Builds the owned runtime and statement state for one transaction.
    /// Embedded hosts that must own the transaction object (rather than
    /// borrow `Database`) assemble the same parts and commit through
    /// [`commit_database_transaction_state`].
    pub(crate) fn transaction_parts(
        &mut self,
    ) -> Result<(DatabaseTransactionRuntime, DatabaseTransactionState)> {
        self.runtime.get_mut()?;
        Ok((
            DatabaseTransactionRuntime::from_database(self)?,
            DatabaseTransactionState::from_database(self)?,
        ))
    }

    pub fn session(&mut self) -> DatabaseSession<'_> {
        let system_variables = self.system_variables.clone();
        DatabaseSession {
            config: SessionConfig::default(),
            db: self,
            graph_transaction: None,
            transaction_runtime: None,
            system_variables,
        }
    }

    #[cfg(test)]
    pub(crate) fn read_snapshot(&self) -> Result<DatabaseReadSnapshot> {
        Ok(DatabaseReadSnapshot(self.begin_read_transaction()?))
    }

    pub(super) fn read_snapshot_without_observations(&self) -> Result<DatabaseReadSnapshot> {
        Ok(DatabaseReadSnapshot(
            self.read_transaction_state(None, None, None)?,
        ))
    }

    /// Pins the selected branch's schema and data after successful admission.
    pub fn begin_read_transaction(&self) -> Result<DatabaseReadTransaction> {
        self.begin_read_transaction_inner(None, None, None)
    }

    pub(crate) fn begin_read_transaction_with_shared_plan_cache(
        &self,
    ) -> Result<DatabaseReadTransaction> {
        self.begin_read_transaction_inner(
            None,
            None,
            Some(Arc::clone(&self.runtime.get()?.plan_cache)),
        )
    }

    pub fn begin_read_transaction_with_context(
        &self,
        task_context: &hawdb_core::RuntimeTaskContext,
    ) -> Result<DatabaseReadTransaction> {
        self.begin_read_transaction_inner(None, Some(task_context.clone()), None)
    }

    /// Pins both the database read view and one active projection generation.
    /// PostgreSQL SQL issued through the returned transaction reads the bound
    /// tables from that generation and all other tables from the same database
    /// snapshot.
    pub fn begin_projection_read_transaction(
        &self,
        binding: ProjectionRelationalReadBinding,
    ) -> Result<DatabaseReadTransaction> {
        let reader = self
            .projection_generation_store()?
            .open_active(binding.projection(), binding.owner_key())
            .map_err(|error| HawDBError::Storage(error.to_string()))?;
        if reader.manifest().begin.projection_version != binding.projection_version() {
            return Err(HawDBError::StorageIntegrity(format!(
                "projection {} version {} does not match required version {}",
                binding.projection(),
                reader.manifest().begin.projection_version,
                binding.projection_version()
            )));
        }
        for table in binding.tables() {
            if self
                .runtime
                .get()?
                .store
                .relational_state()
                .table_schema(table)
                .is_none()
            {
                return Err(HawDBError::Semantic(format!(
                    "projection relational table {table} has no durable PostgreSQL schema"
                )));
            }
            let canonical_rows = self
                .runtime
                .get()?
                .store
                .relational_state()
                .row_count(table);
            if canonical_rows != 0 {
                return Err(HawDBError::StorageIntegrity(format!(
                    "projection relational table {table} contains {canonical_rows} canonical rows"
                )));
            }
        }
        self.begin_read_transaction_inner(
            Some(ProjectionRelationalReadSnapshot { binding, reader }),
            None,
            None,
        )
    }

    pub(super) fn begin_read_transaction_inner(
        &self,
        projection_relational: Option<ProjectionRelationalReadSnapshot>,
        task_context: Option<hawdb_core::RuntimeTaskContext>,
        plan_cache: Option<Arc<SharedState<PlanCache>>>,
    ) -> Result<DatabaseReadTransaction> {
        let mut snapshot =
            self.read_transaction_state(projection_relational, task_context, plan_cache)?;
        snapshot.slow_query_snapshot = self.slow_query_log.borrow().snapshot();
        snapshot.statement_summary_snapshot = self.statement_summary.borrow().snapshot();
        Ok(snapshot)
    }

    pub(super) fn read_transaction_state(
        &self,
        projection_relational: Option<ProjectionRelationalReadSnapshot>,
        task_context: Option<hawdb_core::RuntimeTaskContext>,
        plan_cache: Option<Arc<SharedState<PlanCache>>>,
    ) -> Result<DatabaseReadTransaction> {
        let runtime = self.runtime.get()?;
        Ok({
            let (published_read_view, pin) = Self::pin_runtime_read_view(&runtime);
            DatabaseReadTransaction {
                session_config: SessionConfig::default(),
                system_variables: self.system_variables.clone(),
                catalog: runtime.catalog.clone(),
                store: runtime.store.snapshot_for_read(),
                published_read_view,
                optimizer: runtime.optimizer.clone(),
                plan_cache: plan_cache.map_or_else(
                    || {
                        ReadPlanCache::Private(SharedState::new(PlanCache::with_byte_limits(
                            self.config.max_plan_cache_entries,
                            Some(self.config.max_physical_plan_cache_bytes),
                            Some(self.config.max_physical_plan_cache_entry_bytes),
                        )))
                    },
                    ReadPlanCache::Shared,
                ),
                relational_plan_template_cache: Arc::clone(&runtime.relational_plan_template_cache),
                optimizer_planning_cache: Arc::clone(&runtime.optimizer_planning_cache),
                slow_query_snapshot: Vec::new(),
                statement_summary_snapshot: Vec::new(),
                config: self.config.clone(),
                branch_catalog_path: self.branch_catalog_path_from_runtime(&runtime).ok(),
                current_branch: self.current_branch_from_runtime(&runtime),
                projection_relational,
                task_context,
                _pin: Arc::new(pin),
                retained_runtime: Arc::clone(&self.retained_runtime),
            }
        })
    }

    pub(super) fn pin_runtime_read_view(
        runtime: &runtime_cell::AdmittedBranchRuntime,
    ) -> (PublishedReadView, ReaderPin) {
        let published_read_view = runtime.store.published_read_view();
        let mut pins = runtime
            .reader_pins
            .lock()
            .expect("database reader pins lock should not be poisoned");
        let id = pins.next_reader_id;
        pins.next_reader_id += 1;
        pins.active_views.insert(id, published_read_view);
        (
            published_read_view,
            ReaderPin::new(id, Arc::clone(&runtime.reader_pins)),
        )
    }
}
