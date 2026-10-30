// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Original embedded owner, read publications and maintenance admission.

use super::*;

#[derive(Debug, Clone)]
pub struct NowledgeMemEmbeddedStoreHandle {
    pub(super) inner: Arc<crate::DatabasePublicationOwner<NowledgeMemEmbeddedStore>>,
    pub(super) concurrent: crate::ConcurrentDatabase,
    pub(super) published_read: Arc<Mutex<Option<PublishedCanonicalRead>>>,
    // One-way: a failed or panicking write guard sets this and leaves the
    // last-good publication untouched, so readers fail closed instead of
    // silently serving pre-write state. Mirrors CommitGuard's poison flag.
    pub(super) read_publication_failed: Arc<AtomicBool>,
}

#[derive(Debug)]
pub(super) struct PublishedCanonicalRead {
    pub(super) instance_identity: crate::DatabaseInstanceIdentity,
    pub(super) view: Arc<crate::api::PublishedConcurrentRead>,
    pub(super) governor: RuntimeGovernor,
    pub(super) mode: NowledgeMemGraphMode,
    pub(super) resident_projection: Option<crate::search::SearchIndexReadSnapshot>,
    pub(super) external_seed_io_slots: usize,
    pub(super) retrieval_projection_advisor: NowledgeMemRetrievalProjectionAdvisor,
    pub(super) out_of_core_projection: Option<NowledgeMemOutOfCoreSearchProjection>,
}

impl PublishedCanonicalRead {
    pub(super) fn capture(store: &NowledgeMemEmbeddedStore) -> Result<Self> {
        Ok(Self {
            instance_identity: store.graph.database().instance_identity()?,
            view: Arc::new(crate::api::PublishedConcurrentRead::capture(
                store.graph.database(),
            )?),
            governor: store.graph.runtime_governor().clone(),
            mode: store.graph.mode,
            resident_projection: store
                .search_projection
                .as_ref()
                .map(|projection| projection.index().read_snapshot()),
            external_seed_io_slots: store.external_seed_io_slots(),
            retrieval_projection_advisor: store.retrieval_projection_advisor.clone(),
            out_of_core_projection: store
                .out_of_core_search_projection
                .as_ref()
                .map(NowledgeMemOutOfCoreSearchProjection::read_snapshot),
        })
    }
}

pub(super) struct EmbeddedStoreWriteGuard<'a> {
    pub(super) store:
        crate::api::publication::DatabasePublicationWriteGuard<'a, NowledgeMemEmbeddedStore>,
    pub(super) _exclusive: Option<crate::api::ExclusiveDatabaseLease>,
    pub(super) retired: Option<PublishedCanonicalRead>,
    pub(super) published: &'a Mutex<Option<PublishedCanonicalRead>>,
    pub(super) read_publication_failed: &'a AtomicBool,
}

impl std::ops::Deref for EmbeddedStoreWriteGuard<'_> {
    type Target = NowledgeMemEmbeddedStore;

    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

impl std::ops::DerefMut for EmbeddedStoreWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.store
    }
}

impl Drop for EmbeddedStoreWriteGuard<'_> {
    fn drop(&mut self) {
        // A panicking writer or a failed capture must never overwrite the
        // last known-good publication with an empty or stale one. Record the
        // failure so readers fail closed instead of silently serving
        // pre-write state; the committed write itself is not undone.
        if std::thread::panicking() {
            self.read_publication_failed.store(true, Ordering::Release);
            return;
        }
        if self.read_publication_failed.load(Ordering::Acquire) {
            return;
        }
        // Capture before publication and before releasing the writer guard.
        // Refresh even when a checkpoint changes only the physical generation.
        let next = match PublishedCanonicalRead::capture(&self.store) {
            Ok(next) => next,
            Err(_) => {
                self.read_publication_failed.store(true, Ordering::Release);
                return;
            }
        };
        self.retired = self
            .published
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .replace(next);
        // Releasing the last generation pin can acquire the pin registry lock.
        // Keep that destructor outside the publication critical section.
    }
}

pub struct NowledgeMemReadSnapshot<'a> {
    instance_identity: crate::DatabaseInstanceIdentity,
    transaction: crate::BudgetedReadSnapshot,
    external: SearchProjectionExternalReadOperator<'a>,
    search_projection_source_graph_commit_epoch: Option<u64>,
    search_projection_durable_source_graph_commit_epoch: Option<u64>,
    budget: NowledgeMemReadSnapshotBudget,
    task_context: RuntimeTaskContext,
}

impl<'a> NowledgeMemReadSnapshot<'a> {
    pub(super) fn new(
        transaction: crate::DatabaseReadTransaction,
        instance_identity: crate::DatabaseInstanceIdentity,
        budget: NowledgeMemReadSnapshotBudget,
        projection: Option<ResidentProjectionRef<'a>>,
        out_of_core_projection: Option<&'a NowledgeMemOutOfCoreSearchProjection>,
        task_context: RuntimeTaskContext,
    ) -> Self {
        let freshness = match (projection, out_of_core_projection) {
            (Some(projection), None) => Some(projection.freshness()),
            (None, Some(projection)) => Some(projection.freshness()),
            (None, None) | (Some(_), Some(_)) => None,
        };
        Self {
            transaction: transaction.into_bounded(crate::ReadSnapshotBudget {
                max_rows: budget.max_rows,
                max_payload_bytes: budget.max_payload_bytes,
            }),
            instance_identity,
            external: SearchProjectionExternalReadOperator {
                projection,
                out_of_core_projection,
                vector_seed_execution_count: 0,
            },
            search_projection_source_graph_commit_epoch: freshness
                .as_ref()
                .and_then(|value| value.source_graph_commit_epoch),
            search_projection_durable_source_graph_commit_epoch: freshness
                .as_ref()
                .and_then(|value| value.durable_source_graph_commit_epoch),
            budget,
            task_context,
        }
    }

    pub fn commit_epoch(&self) -> u64 {
        self.transaction.commit_epoch()
    }

    /// Identity of the database supplying this exact immutable read view.
    pub fn database_instance_identity(&self) -> &crate::DatabaseInstanceIdentity {
        &self.instance_identity
    }

    pub fn query_cypher(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        max_rows: usize,
    ) -> Result<QueryOutput> {
        Ok(self
            .query_cypher_profiled(cypher, parameters, max_rows)?
            .output)
    }

    pub fn query_cypher_profiled(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        max_rows: usize,
    ) -> Result<BoundedReadQueryOutput> {
        self.query_request_profiled(
            crate::QueryRequest::new(cypher).with_params(parameters),
            max_rows,
        )
    }

    /// Typed query attachment under the same pinned snapshot and cumulative
    /// result budgets as ordinary Cypher. Scoring does not widen caller caps.
    pub fn query_request(
        &mut self,
        request: crate::QueryRequest<'_>,
        max_rows: usize,
    ) -> Result<QueryOutput> {
        Ok(self.query_request_profiled(request, max_rows)?.output)
    }

    pub fn query_request_profiled(
        &mut self,
        request: crate::QueryRequest<'_>,
        max_rows: usize,
    ) -> Result<BoundedReadQueryOutput> {
        let max_rows = self.statement_row_budget(max_rows)?;
        let max_payload_bytes = self.remaining_payload_bytes()?;
        let result = self.transaction.query_request_profiled_with_external(
            request.with_restrictive_output_limits(QueryStreamOptions {
                max_rows: Some(max_rows),
                max_payload_bytes: Some(max_payload_bytes),
            }),
            &mut self.external,
        )?;
        Ok(result)
    }

    pub fn query_sql(
        &mut self,
        sql: &str,
        parameters: &[Value],
        max_rows: usize,
    ) -> Result<QueryOutput> {
        let max_rows = self.statement_row_budget(max_rows)?;
        let max_payload_bytes = self.remaining_payload_bytes()?;
        self.transaction.query_sql_request(
            crate::SqlQueryRequest::new(sql)
                .with_params(parameters)
                .with_output_limits(QueryStreamOptions {
                    max_rows: Some(max_rows),
                    max_payload_bytes: Some(max_payload_bytes),
                }),
        )
    }

    /// Visits the complete current projection under this snapshot's retained pin.
    /// Both resident and out-of-core visitors own an immutable generation and
    /// may overlap writers without retaining the store read guard.
    ///
    /// Order is unspecified. A visitor prefix is not exhaustion evidence: only
    /// the successful returned report certifies completion. Retained copies are
    /// caller-owned and require independent accounting. Encoded-record scratch
    /// has its own finite cap, distinct from cumulative logical output bytes.
    /// Scans charge UTF-8 strings, metadata and four bytes per embedding value;
    /// Cypher/SQL reads charge `Value` payload estimates, including value overhead.
    /// These charges share the cumulative cap but differ by output representation.
    /// Out-of-core scans read all physical payloads twice and probe visible ID
    /// routes during preflight, without retaining the publication lock.
    /// Emit callbacks may precede final checksum validation, so a later error
    /// invalidates completion even after callbacks have run.
    pub fn visit_search_projection(
        &mut self,
        max_encoded_record_bytes: usize,
        mut visitor: impl FnMut(&SearchDocument) -> Result<()>,
    ) -> Result<crate::search::SearchProjectionScanReport> {
        let budget = crate::search::SearchProjectionScanBudget {
            max_rows: self.statement_row_budget(self.budget.max_rows)?,
            max_payload_bytes: self.remaining_payload_bytes()?,
            max_encoded_record_bytes,
        };
        let report = match (
            self.external.projection,
            self.external.out_of_core_projection,
        ) {
            (Some(projection), None) => {
                projection.visit_projection_documents(budget, &self.task_context, &mut visitor)?
            }
            (None, Some(projection)) => projection.reader().visit_projection_documents(
                budget,
                &self.task_context,
                &mut visitor,
            )?,
            _ => {
                return Err(HawDBError::Execution(
                    "bounded projection scan requires exactly one pinned Search projection".into(),
                ))
            }
        };
        self.consume(report.document_count, report.payload_bytes)?;
        Ok(report)
    }

    pub fn report(&self) -> NowledgeMemReadSnapshotReport {
        NowledgeMemReadSnapshotReport {
            commit_epoch: self.commit_epoch(),
            search_projection_present: self.external.projection.is_some()
                || self.external.out_of_core_projection.is_some(),
            search_projection_source_graph_commit_epoch: self
                .search_projection_source_graph_commit_epoch,
            search_projection_durable_source_graph_commit_epoch: self
                .search_projection_durable_source_graph_commit_epoch,
            max_rows: self.budget.max_rows,
            max_payload_bytes: self.budget.max_payload_bytes,
            cypher_statement_count: self.transaction.usage().cypher_statement_count,
            sql_statement_count: self.transaction.usage().sql_statement_count,
            vector_seed_execution_count: self.external.vector_seed_execution_count,
            output_rows: self.transaction.usage().output_rows,
            output_payload_bytes: self.transaction.usage().output_payload_bytes,
            remaining_rows: self
                .budget
                .max_rows
                .saturating_sub(self.transaction.usage().output_rows),
            remaining_payload_bytes: self
                .budget
                .max_payload_bytes
                .saturating_sub(self.transaction.usage().output_payload_bytes),
        }
    }

    pub(super) fn statement_row_budget(&self, requested: usize) -> Result<usize> {
        if requested == 0 {
            return Err(HawDBError::Execution(
                "bounded read statement requires max_rows greater than zero".to_string(),
            ));
        }
        let remaining = self
            .budget
            .max_rows
            .saturating_sub(self.transaction.usage().output_rows);
        if remaining == 0 {
            return Err(HawDBError::read_budget_exceeded(
                ReadBudgetResource::Rows,
                self.budget.max_rows,
                "bounded read snapshot exhausted max_rows",
            ));
        }
        Ok(requested.min(remaining))
    }

    pub(super) fn remaining_payload_bytes(&self) -> Result<usize> {
        let remaining = self
            .budget
            .max_payload_bytes
            .saturating_sub(self.transaction.usage().output_payload_bytes);
        if remaining == 0 {
            return Err(HawDBError::read_budget_exceeded(
                ReadBudgetResource::PayloadBytes,
                self.budget.max_payload_bytes,
                "bounded read snapshot exhausted max_payload_bytes",
            ));
        }
        Ok(remaining)
    }

    pub(super) fn consume(&mut self, rows: usize, payload_bytes: usize) -> Result<()> {
        self.transaction.consume_output(rows, payload_bytes)
    }
}

impl NowledgeMemEmbeddedStoreHandle {
    pub fn new(store: NowledgeMemEmbeddedStore) -> Result<Self> {
        let published_read = PublishedCanonicalRead::capture(&store)?;
        let inner = Arc::new(crate::DatabasePublicationOwner::new(store));
        let published_read = Arc::new(Mutex::new(Some(published_read)));
        let read_publication_failed = Arc::new(AtomicBool::new(false));
        let concurrent = crate::ConcurrentDatabase::with_shared_owner(Arc::new(
            concurrent_owner::MemDatabaseOwner {
                inner: Arc::clone(&inner),
                published_read: Arc::clone(&published_read),
                read_publication_failed: Arc::clone(&read_publication_failed),
            },
        ))?;
        Ok(Self {
            inner,
            concurrent,
            published_read,
            read_publication_failed,
        })
    }

    pub fn open_with_options(
        options: NowledgeMemOpenOptions,
    ) -> Result<(Self, NowledgeMemOpenReport)> {
        let (store, report) = NowledgeMemEmbeddedStore::open_with_options(options)?;
        Ok((Self::new(store)?, report))
    }

    pub fn open_with_options_and_runtime_governor(
        options: NowledgeMemOpenOptions,
        runtime_governor: RuntimeGovernor,
    ) -> Result<(Self, NowledgeMemOpenReport)> {
        let (store, report) = NowledgeMemEmbeddedStore::open_with_options_and_runtime_governor(
            options,
            runtime_governor,
        )?;
        Ok((Self::new(store)?, report))
    }

    /// Qualification counters for the actual graph/projection publication owner.
    #[cfg(feature = "qualification")]
    pub fn publication_lock_metrics(&self) -> crate::PublicationLockMetricsSnapshot {
        self.inner.lock_metrics()
    }

    pub fn runtime_governor_snapshot(&self) -> Result<RuntimeGovernorSnapshot> {
        Ok(self.read_store()?.runtime_governor_snapshot())
    }

    /// Returns a point-in-time storage residency snapshot through the admitted
    /// embedded facade. Qualification uses this to prove cancellation does not
    /// leak cache pins without reaching through to the storage engine.
    pub fn storage_residency_report(&self) -> Result<crate::StorageResidencyReport> {
        self.read_store()?
            .graph
            .database()
            .storage_residency_report()
    }

    /// Reports whether a fail-closed storage error poisoned this handle.
    pub fn storage_handle_poisoned(&self) -> Result<bool> {
        self.read_store()?
            .graph
            .database()
            .storage_handle_poisoned()
    }

    /// Reports the admitted facade contract. Production hosts must bind this
    /// report to their long-lived runtime identity before using it as evidence.
    pub fn serving_path_readiness(&self) -> NowledgeMemServingPathReadiness {
        NowledgeMemServingPathReadiness::embedded_store_handle()
    }

    pub fn refresh_runtime_resources(&self) -> Result<bool> {
        Ok(self.read_store()?.refresh_runtime_resources())
    }

    /// Persist the current graph generation through the admitted embedded
    /// maintenance path.
    ///
    /// Import and qualification hosts use this typed boundary instead of
    /// issuing a textual `CHECKPOINT` statement or reaching through the
    /// facade to the storage engine. The single maintenance admission's
    /// memory request is extended by the columnar shadow's builder-lifetime
    /// reservation, and that pre-admitted context travels into the
    /// checkpoint so the shadow build never issues a nested admission
    /// against the permit this method already holds.
    pub fn checkpoint(&self) -> Result<()> {
        let shadow_admission_bytes = self
            .read_store()?
            .graph
            .database()
            .columnar_shadow_admission_bytes()?;
        let _permit = self.admit_typed_maintenance(
            RuntimeWorkPriority::Foreground,
            usize::try_from(shadow_admission_bytes).unwrap_or(usize::MAX),
            1,
        )?;
        self.write_store()?
            .graph_mut()
            .database_mut()
            .checkpoint_with_shadow_admission(crate::store::ColumnarShadowAdmission::pre_admitted(
                shadow_admission_bytes,
            ))
    }

    pub fn query_with_report(&self, cypher: &str) -> Result<NowledgeMemQueryOutput> {
        self.query_with_params_with_report(cypher, &BTreeMap::new())
    }

    pub fn set_telemetry_sink(&self, telemetry: Option<Arc<dyn TelemetrySink>>) -> Result<()> {
        self.write_store()?.set_telemetry_sink(telemetry);
        Ok(())
    }

    pub fn query_with_report_options(
        &self,
        cypher: &str,
        options: NowledgeMemQueryReportOptions,
    ) -> Result<NowledgeMemQueryOutput> {
        self.query_with_params_with_report_options(cypher, &BTreeMap::new(), options)
    }

    pub fn query_with_params_with_report(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<NowledgeMemQueryOutput> {
        self.query_with_params_with_report_options(
            cypher,
            parameters,
            NowledgeMemQueryReportOptions::default(),
        )
    }

    /// Hydrates an explicitly bounded set of search-projection documents.
    ///
    /// Candidate search intentionally returns compact identities. Embedded
    /// hosts use this method for projection-owned payloads, such as source
    /// chunks, that have no authoritative graph node to hydrate from.
    pub fn search_projection_documents(
        &self,
        document_ids: &[String],
        max_documents: usize,
    ) -> Result<Vec<SearchDocument>> {
        Ok(self
            .search_projection_documents_with_report(document_ids, max_documents)?
            .documents)
    }

    pub fn search_projection_documents_with_report(
        &self,
        document_ids: &[String],
        max_documents: usize,
    ) -> Result<NowledgeMemSearchHydrationOutput> {
        if document_ids.len() > max_documents {
            return Err(HawDBError::Execution(format!(
                "search projection document hydration requested {} rows, limit is {max_documents}",
                document_ids.len()
            )));
        }
        self.with_admitted_search(|read| read.hydrate(document_ids))
    }
}

impl NowledgeMemEmbeddedStoreHandle {
    pub fn transaction(
        &self,
        statements: &[NowledgeGraphStatement],
    ) -> Result<crate::NowledgeGraphTransactionOutput> {
        let permit = self.admit_transaction(statements)?;
        let _unwind = concurrent_owner::PublicationUnwindGuard(&self.read_publication_failed);
        let mut transaction = self.concurrent.begin_admitted_transaction(
            crate::ConcurrentTransactionOptions::default(),
            permit,
            RuntimeTaskContext::default(),
        )?;
        let mut statement_outputs = Vec::with_capacity(statements.len());
        for statement in statements {
            statement_outputs
                .push(transaction.query_with_params(&statement.cypher, &statement.parameters)?);
        }
        let commit_output = transaction.commit()?;
        Ok(crate::NowledgeGraphTransactionOutput {
            statement_outputs,
            commit_output,
        })
    }

    /// Executes caller-owned Cypher and SQL statements in one canonical
    /// graph/relational transaction.
    ///
    /// The callback receives the ordinary query-first transaction surface;
    /// no route-specific mutation API or second durability boundary is
    /// introduced. Returning an error rolls back every staged statement.
    pub fn with_transaction<T>(
        &self,
        operation: impl FnOnce(&mut crate::ConcurrentDatabaseTransaction) -> Result<T>,
    ) -> Result<T> {
        let permit = self.admit_typed_transaction()?;
        let _unwind = concurrent_owner::PublicationUnwindGuard(&self.read_publication_failed);
        let mut transaction = self.concurrent.begin_admitted_transaction(
            crate::ConcurrentTransactionOptions::default(),
            permit,
            RuntimeTaskContext::default(),
        )?;
        match operation(&mut transaction) {
            Ok(output) => transaction.commit().map(|_| output),
            Err(error) => {
                transaction.rollback();
                Err(error)
            }
        }
    }

    /// Executes a caller-owned group of bounded Cypher reads against one
    /// immutable graph snapshot.
    pub fn with_read_transaction<T>(
        &self,
        max_estimated_payload_bytes: usize,
        operation: impl FnOnce(&mut crate::DatabaseReadTransaction) -> Result<T>,
    ) -> Result<T> {
        let (_permit, mut transaction) =
            self.canonical_read_transaction(max_estimated_payload_bytes, None)?;
        let result = operation(&mut transaction);
        self.finish_canonical_read(result)
    }

    /// Executes App-owned Cypher and PostgreSQL reads against one immutable
    /// graph/relational snapshot while the search projection is pinned.
    /// Budgets are cumulative across every statement in the callback.
    pub fn with_bounded_read_snapshot<T>(
        &self,
        budget: NowledgeMemReadSnapshotBudget,
        operation: impl FnOnce(&mut NowledgeMemReadSnapshot<'_>) -> Result<T>,
    ) -> Result<T> {
        self.with_bounded_read_snapshot_context(budget, &RuntimeTaskContext::default(), operation)
    }

    /// Executes one bounded App read snapshot under the caller's cancellation,
    /// deadline, and resource reservation context.
    pub fn with_bounded_read_snapshot_context<T>(
        &self,
        budget: NowledgeMemReadSnapshotBudget,
        task_context: &RuntimeTaskContext,
        operation: impl FnOnce(&mut NowledgeMemReadSnapshot<'_>) -> Result<T>,
    ) -> Result<T> {
        self.with_bounded_read_snapshot_kind(budget, true, task_context, operation)
    }

    /// Executes bounded graph and relational reads from the published committed
    /// view. Acquisition and callback execution can overlap a staging writer.
    ///
    /// External search projections are deliberately unavailable: vector queries
    /// fail and the report contains no projection epochs. Use
    /// `with_bounded_read_snapshot` when a pinned projection is required.
    pub fn with_bounded_graph_read_snapshot<T>(
        &self,
        budget: NowledgeMemReadSnapshotBudget,
        operation: impl FnOnce(&mut NowledgeMemReadSnapshot<'_>) -> Result<T>,
    ) -> Result<T> {
        self.with_bounded_graph_read_snapshot_context(
            budget,
            &RuntimeTaskContext::default(),
            operation,
        )
    }

    /// Executes one published graph snapshot under the caller's cancellation,
    /// deadline, and resource reservation context.
    pub fn with_bounded_graph_read_snapshot_context<T>(
        &self,
        budget: NowledgeMemReadSnapshotBudget,
        task_context: &RuntimeTaskContext,
        operation: impl FnOnce(&mut NowledgeMemReadSnapshot<'_>) -> Result<T>,
    ) -> Result<T> {
        self.with_bounded_read_snapshot_kind(budget, false, task_context, operation)
    }

    pub(super) fn with_bounded_read_snapshot_kind<T>(
        &self,
        budget: NowledgeMemReadSnapshotBudget,
        pin_projection: bool,
        task_context: &RuntimeTaskContext,
        operation: impl FnOnce(&mut NowledgeMemReadSnapshot<'_>) -> Result<T>,
    ) -> Result<T> {
        if budget.max_rows == 0 {
            return Err(HawDBError::Execution(
                "bounded read snapshot requires max_rows greater than zero".to_string(),
            ));
        }
        if budget.max_payload_bytes == 0 {
            return Err(HawDBError::Execution(
                "bounded read snapshot requires max_payload_bytes greater than zero".to_string(),
            ));
        }
        if !pin_projection {
            let (_permit, transaction, instance_identity) = self
                .canonical_read_transaction_with_instance(
                    budget.max_payload_bytes,
                    Some(budget.max_rows),
                    task_context,
                )?;
            let mut snapshot = NowledgeMemReadSnapshot::new(
                transaction,
                instance_identity,
                budget,
                None,
                None,
                _permit.bind_task_context(task_context.clone()),
            );
            return self.finish_canonical_read(operation(&mut snapshot));
        }
        let permit = self.admit_typed_read(budget.max_payload_bytes)?;
        let task_context = permit.bind_task_context(task_context.clone());
        let _permit = permit;
        let (view, identity, resident, projection) = {
            let current = self.lock_published_read()?;
            let current = current.as_ref().ok_or_else(|| {
                HawDBError::StorageIntegrity("canonical read publication is unavailable".into())
            })?;
            (
                Arc::clone(&current.view),
                current.instance_identity.clone(),
                current.resident_projection.clone(),
                current
                    .out_of_core_projection
                    .as_ref()
                    .map(NowledgeMemOutOfCoreSearchProjection::read_snapshot),
            )
        };
        let resident = resident
            .map(|snapshot| snapshot.retain_with_permit(&_permit))
            .transpose()?;
        let configured_rows = view
            .snapshot
            .config()
            .read_result_row_limit()
            .unwrap_or(usize::MAX);
        if budget.max_rows > configured_rows {
            return Err(HawDBError::Execution(format!(
                "bounded read snapshot row budget {} exceeds configured limit {configured_rows}",
                budget.max_rows
            )));
        }
        let transaction = view.begin_read_transaction_with_context(&task_context)?;
        drop(view);
        let mut snapshot = NowledgeMemReadSnapshot::new(
            transaction,
            identity,
            budget,
            resident
                .as_ref()
                .map(|lease| ResidentProjectionRef::Snapshot(lease.snapshot())),
            projection.as_ref(),
            task_context,
        );
        self.finish_canonical_read(operation(&mut snapshot))
    }

    pub fn hawdb_lightning_initial_import_apply_with_document_identities(
        &self,
        encoded_graph_stream: &str,
        encoded_relational_stream: &[u8],
        manifest: &HawDBLightningBootstrapManifest,
        projection_freshness: Option<&SearchProjectionFreshness>,
        checkpoint: Option<&HawDBLightningInitialImportCheckpoint>,
        document_identities: &[HawDBLightningInitialImportDocumentIdentity],
    ) -> Result<HawDBLightningInitialImportApplyReport> {
        let estimated_input_bytes = encoded_graph_stream
            .len()
            .saturating_add(encoded_relational_stream.len())
            .saturating_add(
                document_identities
                    .iter()
                    .map(|identity| identity.document_id.len())
                    .sum::<usize>(),
            );
        let _permit = self.admit_typed_maintenance(
            RuntimeWorkPriority::Background,
            estimated_input_bytes,
            1,
        )?;
        self.write_store()?
            .graph_mut()
            .database_mut()
            .hawdb_lightning_initial_import_apply_with_document_identities(
                encoded_graph_stream,
                encoded_relational_stream,
                manifest,
                projection_freshness,
                checkpoint,
                document_identities,
            )
    }

    /// Applies an externally materialized projection batch and makes it
    /// durable before returning. This is the library boundary used when Mem
    /// imports a legacy LanceDB projection without rebuilding embeddings.
    pub fn apply_search_projection_delta_and_checkpoint(
        &self,
        delta: SearchProjectionDelta,
    ) -> Result<SearchProjectionDeltaReport> {
        let permit =
            self.admit_search_projection_maintenance(search_projection_delta_bytes(&delta))?;
        let mut store = self.write_store()?;
        if store.incremental_search_enabled() {
            store.apply_incremental_search_delta(
                delta,
                permit.bind_task_context(RuntimeTaskContext::default()),
            )
        } else {
            store.apply_search_projection_delta_and_checkpoint(delta)
        }
    }

    /// Applies one externally materialized import batch, records its immutable
    /// source provenance, and checkpoints both before acknowledging success.
    pub fn apply_initial_import_projection_delta_and_checkpoint(
        &self,
        delta: SearchProjectionDelta,
        import_source_graph_commit_epoch: u64,
    ) -> Result<SearchProjectionDeltaReport> {
        let _permit = self.admit_typed_maintenance(
            RuntimeWorkPriority::Background,
            search_projection_delta_bytes(&delta),
            1,
        )?;
        self.write_store()?
            .apply_initial_import_projection_delta_and_checkpoint(
                delta,
                import_source_graph_commit_epoch,
            )
    }

    pub fn query_with_params_with_report_options(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: NowledgeMemQueryReportOptions,
    ) -> Result<NowledgeMemQueryOutput> {
        self.query_with_params_with_report_options_context(
            cypher,
            parameters,
            options,
            &RuntimeTaskContext::default(),
        )
    }

    pub fn query_with_params_with_report_options_context(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: NowledgeMemQueryReportOptions,
        task_context: &RuntimeTaskContext,
    ) -> Result<NowledgeMemQueryOutput> {
        self.query_with_admitted_report(cypher, parameters, options, task_context)
    }

    pub fn read_query(
        &self,
        cypher: &str,
        options: &NowledgeMemReadOptions,
    ) -> Result<NowledgeMemReadOutput> {
        self.read_query_with_params(cypher, &BTreeMap::new(), options)
    }

    pub fn read_query_with_params(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemReadOptions,
    ) -> Result<NowledgeMemReadOutput> {
        let mut output = self
            .read_query_with_params_streaming_collect(cypher, parameters, options)
            .map_err(|error| legacy_nowledge_mem_read_error(error, options))?;
        output.report.streaming = false;
        Ok(output)
    }

    pub fn read_query_with_params_streaming(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemReadOptions,
        consumer: impl FnMut(BTreeMap<String, Value>) -> Result<()>,
    ) -> Result<QueryStreamReport> {
        self.read_query_with_params_streaming_context(
            cypher,
            parameters,
            options,
            &RuntimeTaskContext::default(),
            consumer,
        )
    }

    pub fn read_query_with_params_streaming_context(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemReadOptions,
        task_context: &RuntimeTaskContext,
        consumer: impl FnMut(BTreeMap<String, Value>) -> Result<()>,
    ) -> Result<QueryStreamReport> {
        self.with_admitted_graph_read(
            cypher,
            parameters,
            options,
            task_context,
            |read, prepared, limits, context, _| {
                read.query_prepared_with_params_streaming_context(
                    prepared,
                    parameters,
                    limits,
                    crate::executor::StreamDelivery::Validated,
                    context,
                    consumer,
                )
            },
        )
    }

    pub fn read_query_with_params_streaming_collect(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemReadOptions,
    ) -> Result<NowledgeMemReadOutput> {
        self.with_admitted_graph_read(
            cypher,
            parameters,
            options,
            &RuntimeTaskContext::default(),
            |read, prepared, limits, context, mode| {
                let bounded = read.query_prepared_with_params_profiled_context(
                    prepared, parameters, limits, context,
                )?;
                let mut output = bounded_nowledge_mem_read_output(mode, bounded, options)?;
                output.report.streaming = true;
                Ok(output)
            },
        )
    }

    pub fn graph_rag_schema_context(
        &self,
        options: GraphRagSchemaContextOptions,
    ) -> Result<GraphRagSchemaContext> {
        let view = {
            let publication = self.lock_published_read()?;
            let publication = publication.as_ref().ok_or_else(|| {
                HawDBError::Execution("embedded schema source is unavailable".into())
            })?;
            Arc::clone(&publication.view)
        };
        // Schema guidance is metadata, not an executable query. Preserve its
        // existing options while constructing statistics outside publication.
        let read = view
            .snapshot
            .begin_read_transaction(&RuntimeTaskContext::default())?;
        drop(view);
        self.finish_canonical_read(Ok(read.graph_rag_schema_context(options)))
    }

    pub fn read_generated_graph_rag(
        &self,
        query: &GraphRagGeneratedQuery,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemReadOptions,
    ) -> Result<NowledgeMemReadOutput> {
        query
            .validate_parameters(parameters)
            .map_err(|error| HawDBError::Semantic(error.to_string()))?;
        self.with_admitted_graph_read(
            query.cypher(),
            parameters,
            options,
            &RuntimeTaskContext::default(),
            |read, prepared, limits, context, mode| {
                read.validate_generated_graph_rag(query, parameters)?;
                let bounded = read.query_prepared_with_params_profiled_context(
                    prepared, parameters, limits, context,
                )?;
                bounded_nowledge_mem_read_output(mode, bounded, options)
            },
        )
    }
}
impl NowledgeMemEmbeddedStoreHandle {
    pub fn search_candidates(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
    ) -> Result<SearchResultSet> {
        Ok(self.search_candidates_with_report(request)?.result)
    }

    pub fn search_candidates_with_report(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
    ) -> Result<NowledgeMemSearchCandidateOutput> {
        self.with_admitted_search(|read| read.search_candidates(request))
    }

    pub fn validate_sampled_vector_recall(
        &self,
        options: VectorRecallValidationOptions,
    ) -> Result<VectorRecallValidationReport> {
        self.read_store()?.validate_sampled_vector_recall(options)
    }

    pub fn qualify_sampled_vector_recall_for_production(
        &self,
        options: VectorRecallValidationOptions,
        evidence_binding: crate::ProductionEvidenceBinding,
        expected_identity: crate::ProductionQualificationIdentity,
    ) -> Result<VectorRecallProductionQualificationReport> {
        self.read_store()?
            .qualify_sampled_vector_recall_for_production(
                options,
                evidence_binding,
                expected_identity,
            )
    }

    pub fn retrieve_knowledge(
        &self,
        request: &KnowledgeRetrievalRequest,
    ) -> Result<KnowledgeRetrievalOutput> {
        Ok(self.retrieve_knowledge_with_report(request)?.output)
    }

    pub fn retrieve_knowledge_with_report(
        &self,
        request: &KnowledgeRetrievalRequest,
    ) -> Result<NowledgeMemRetrievalOutput> {
        self.with_admitted_search(|read| read.retrieve(request))
    }

    pub fn search_candidate_readiness(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
        options: &NowledgeMemSearchCandidateReadinessOptions,
    ) -> Result<NowledgeMemSearchCandidateReadinessReport> {
        Ok(self
            .search_candidates_with_report(request)?
            .readiness_report(options))
    }

    pub fn search_candidate_shadow_evidence_json<I, S>(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
        primary_candidate_ids: I,
    ) -> Result<serde_json::Value>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.with_admitted_search(|read| read.shadow_evidence_json(request, primary_candidate_ids))
    }

    pub fn slow_query_report(&self) -> Result<NowledgeMemSlowQueryReport> {
        Ok(self.read_store()?.slow_query_report())
    }

    pub fn slow_query_report_json(&self) -> Result<serde_json::Value> {
        Ok(self.read_store()?.slow_query_report_json())
    }

    pub fn runtime_status(&self) -> Result<NowledgeMemRuntimeStatus> {
        self.read_store()?.runtime_status()
    }

    /// Current admitted database instance; does not retain a target fence.
    pub fn database_instance_identity(&self) -> Result<crate::DatabaseInstanceIdentity> {
        self.read_store()?.graph.database().instance_identity()
    }

    pub fn production_resource_profile(
        &self,
        statement: &NowledgeGraphStatement,
        mut limits: StorageResourceProfileLimits,
        evidence_binding: crate::ProductionEvidenceBinding,
        expected_identity: crate::ProductionQualificationIdentity,
    ) -> Result<StorageResourceProfileReport> {
        limits.validate()?;
        evidence_binding.validate_for(&expected_identity)?;
        self.with_admitted_graph_read(
            &statement.cypher,
            &statement.parameters,
            &NowledgeMemReadOptions {
                max_rows: Some(limits.max_output_rows),
                max_estimated_payload_bytes: Some(limits.max_output_payload_bytes),
            },
            &RuntimeTaskContext::default(),
            |read, prepared, admitted, context, _| {
                limits.max_output_rows = limits
                    .max_output_rows
                    .min(admitted.max_rows.unwrap_or(usize::MAX));
                limits.max_output_payload_bytes = limits
                    .max_output_payload_bytes
                    .min(admitted.max_payload_bytes.unwrap_or(usize::MAX));
                read.storage_resource_profile_for_production_with_context(
                    prepared,
                    &statement.parameters,
                    limits,
                    evidence_binding,
                    expected_identity,
                    context,
                )
            },
        )
    }

    pub fn production_status(
        &self,
        route_ownership: Option<&NowledgeMemRouteOwnershipReadinessReport>,
    ) -> Result<NowledgeMemProductionStatus> {
        self.read_store()?.production_status(route_ownership)
    }

    pub fn production_status_json(
        &self,
        route_ownership: Option<&NowledgeMemRouteOwnershipReadinessReport>,
    ) -> Result<serde_json::Value> {
        self.read_store()?.production_status_json(route_ownership)
    }

    pub fn cutover_controls_report(
        &self,
        controls: NowledgeMemCutoverControls,
        route_ownership: Option<&NowledgeMemRouteOwnershipReadinessReport>,
    ) -> Result<NowledgeMemCutoverControlsReport> {
        self.read_store()?
            .cutover_controls_report(controls, route_ownership)
    }

    pub fn cutover_controls_report_with_initial_import_cutover_catch_up(
        &self,
        controls: NowledgeMemCutoverControls,
        route_ownership: Option<&NowledgeMemRouteOwnershipReadinessReport>,
        initial_import_cutover_catch_up: Option<&HawDBLightningInitialImportCutoverCatchUpReport>,
    ) -> Result<NowledgeMemCutoverControlsReport> {
        self.read_store()?
            .cutover_controls_report_with_initial_import_cutover_catch_up(
                controls,
                route_ownership,
                initial_import_cutover_catch_up,
            )
    }

    pub fn cutover_controls_report_with_initial_import_recovery(
        &self,
        controls: NowledgeMemCutoverControls,
        route_ownership: Option<&NowledgeMemRouteOwnershipReadinessReport>,
        initial_import_recovery: Option<&HawDBLightningInitialImportRecoveryReadinessReport>,
    ) -> Result<NowledgeMemCutoverControlsReport> {
        self.read_store()?
            .cutover_controls_report_with_initial_import_recovery(
                controls,
                route_ownership,
                initial_import_recovery,
            )
    }

    pub fn cutover_controls_report_json(
        &self,
        controls: NowledgeMemCutoverControls,
        route_ownership: Option<&NowledgeMemRouteOwnershipReadinessReport>,
    ) -> Result<serde_json::Value> {
        self.read_store()?
            .cutover_controls_report_json(controls, route_ownership)
    }

    pub fn cutover_controls_report_json_with_initial_import_cutover_catch_up(
        &self,
        controls: NowledgeMemCutoverControls,
        route_ownership: Option<&NowledgeMemRouteOwnershipReadinessReport>,
        initial_import_cutover_catch_up: Option<&HawDBLightningInitialImportCutoverCatchUpReport>,
    ) -> Result<serde_json::Value> {
        self.read_store()?
            .cutover_controls_report_json_with_initial_import_cutover_catch_up(
                controls,
                route_ownership,
                initial_import_cutover_catch_up,
            )
    }

    pub fn library_readiness(
        &self,
        options: &NowledgeMemReadinessOptions,
    ) -> Result<NowledgeMemLibraryReadinessReport> {
        self.read_store()?.library_readiness(options)
    }

    pub fn library_readiness_json(
        &self,
        options: &NowledgeMemReadinessOptions,
    ) -> Result<serde_json::Value> {
        self.read_store()?.library_readiness_json(options)
    }

    pub fn readiness_dashboard(
        &self,
        options: &NowledgeMemReadinessOptions,
    ) -> Result<NowledgeMemReadinessDashboard> {
        self.read_store()?.readiness_dashboard(options)
    }

    pub fn readiness_dashboard_json(
        &self,
        options: &NowledgeMemReadinessOptions,
    ) -> Result<serde_json::Value> {
        self.read_store()?.readiness_dashboard_json(options)
    }

    pub fn operations_readiness(
        &self,
        options: &NowledgeMemReadinessOptions,
    ) -> Result<NowledgeMemOperationsReadinessReport> {
        self.read_store()?.operations_readiness(options)
    }

    pub fn operations_readiness_json(
        &self,
        options: &NowledgeMemReadinessOptions,
    ) -> Result<serde_json::Value> {
        self.read_store()?.operations_readiness_json(options)
    }

    pub fn query_runtime_preflight(
        &self,
        probes: &[NowledgeQueryRuntimePreflightProbe],
    ) -> Result<NowledgeQueryRuntimePreflightReport> {
        Ok(self.write_store()?.query_runtime_preflight(probes))
    }

    pub fn query_runtime_preflight_json(
        &self,
        probes: &[NowledgeQueryRuntimePreflightProbe],
    ) -> Result<serde_json::Value> {
        Ok(self.write_store()?.query_runtime_preflight_json(probes))
    }

    pub fn catch_up_search_projection(
        &self,
        max_operations_per_batch: usize,
        max_batches: usize,
    ) -> Result<SearchProjectionCatchUpReport> {
        let estimated_operations = max_operations_per_batch.saturating_mul(max_batches);
        let estimated_input_bytes =
            estimated_operations.saturating_mul(SEARCH_PROJECTION_CHANGEFEED_OPERATION_BYTES);
        let _permit = self.admit_typed_maintenance(
            RuntimeWorkPriority::Background,
            estimated_input_bytes,
            1,
        )?;
        self.write_store()?
            .catch_up_search_projection(max_operations_per_batch, max_batches)
    }

    /// Runs bounded unified graph-and-relational projection catch-up through
    /// the embedded library handle.
    pub fn catch_up_search_projection_with_relational<F>(
        &self,
        max_operations_per_batch: usize,
        max_batches: usize,
        relational_hydrator: F,
    ) -> Result<SearchProjectionCatchUpReport>
    where
        F: FnMut(
            &mut DatabaseReadTransaction,
            &SearchProjectionChangeBatch,
        ) -> Result<SearchProjectionRelationalDelta>,
    {
        let estimated_operations = max_operations_per_batch.saturating_mul(max_batches);
        let estimated_input_bytes =
            estimated_operations.saturating_mul(SEARCH_PROJECTION_CHANGEFEED_OPERATION_BYTES);
        let _permit = self.admit_typed_maintenance(
            RuntimeWorkPriority::Background,
            estimated_input_bytes,
            1,
        )?;
        self.write_store()?
            .catch_up_search_projection_with_relational(
                max_operations_per_batch,
                max_batches,
                relational_hydrator,
            )
    }

    /// Runs legacy unified catch-up and invokes the host hydrator per batch.
    /// The selected interval must reach the pinned source epoch; a shorter
    /// prefix is refused before publication. For bounded resumable private work,
    /// use `open_or_create_search_projection_consumer` and `begin_catch_up`.
    /// This compatibility rule applies to both full-residency and incremental
    /// maintenance modes; increasing limits is not a bounded-progress protocol.
    pub fn catch_up_search_projection_with_batch_hydrator<F>(
        &self,
        max_change_operations_per_batch: usize,
        max_projection_operations_per_batch: usize,
        max_batches: usize,
        batch_hydrator: F,
    ) -> Result<SearchProjectionCatchUpReport>
    where
        F: FnMut(
            &mut DatabaseReadTransaction,
            &SearchProjectionChangeBatch,
        ) -> Result<SearchProjectionRelationalDelta>,
    {
        self.catch_up_search_projection_with_batch_hydrator_inner(
            RuntimeWorkPriority::Background,
            max_change_operations_per_batch,
            max_projection_operations_per_batch,
            max_batches,
            None::<fn(&mut DatabaseReadTransaction) -> Result<()>>,
            batch_hydrator,
        )
    }

    /// Runs bounded Search catch-up in the caller-selected runtime lane.
    ///
    /// Explicit host work may select `Foreground` without competing for the
    /// background maintenance lane. Admission still enforces the selected
    /// lane's task, memory and I/O limits. Existing catch-up methods retain
    /// their background priority; this does not change scheduled maintenance.
    pub fn catch_up_search_projection_with_priority_and_batch_hydrator<F>(
        &self,
        priority: RuntimeWorkPriority,
        max_change_operations_per_batch: usize,
        max_projection_operations_per_batch: usize,
        max_batches: usize,
        batch_hydrator: F,
    ) -> Result<SearchProjectionCatchUpReport>
    where
        F: FnMut(
            &mut DatabaseReadTransaction,
            &SearchProjectionChangeBatch,
        ) -> Result<SearchProjectionRelationalDelta>,
    {
        self.catch_up_search_projection_with_batch_hydrator_inner(
            priority,
            max_change_operations_per_batch,
            max_projection_operations_per_batch,
            max_batches,
            None::<fn(&mut DatabaseReadTransaction) -> Result<()>>,
            batch_hydrator,
        )
    }

    /// Validates caller-owned target state and runs bounded Search catch-up
    /// under one embedded write owner. A rejected guard cannot publish a
    /// projection batch, and another mutation through this handle cannot
    /// change the guarded Graph state before catch-up completes.
    ///
    /// The guard must use bounded reads and must not reenter this handle.
    pub fn catch_up_search_projection_with_guard_and_batch_hydrator<G, F>(
        &self,
        max_change_operations_per_batch: usize,
        max_projection_operations_per_batch: usize,
        max_batches: usize,
        guard: G,
        batch_hydrator: F,
    ) -> Result<SearchProjectionCatchUpReport>
    where
        G: FnOnce(&mut DatabaseReadTransaction) -> Result<()>,
        F: FnMut(
            &mut DatabaseReadTransaction,
            &SearchProjectionChangeBatch,
        ) -> Result<SearchProjectionRelationalDelta>,
    {
        self.catch_up_search_projection_with_batch_hydrator_inner(
            RuntimeWorkPriority::Background,
            max_change_operations_per_batch,
            max_projection_operations_per_batch,
            max_batches,
            Some(guard),
            batch_hydrator,
        )
    }

    pub(super) fn catch_up_search_projection_with_batch_hydrator_inner<G, F>(
        &self,
        priority: RuntimeWorkPriority,
        max_change_operations_per_batch: usize,
        max_projection_operations_per_batch: usize,
        max_batches: usize,
        guard: Option<G>,
        batch_hydrator: F,
    ) -> Result<SearchProjectionCatchUpReport>
    where
        G: FnOnce(&mut DatabaseReadTransaction) -> Result<()>,
        F: FnMut(
            &mut DatabaseReadTransaction,
            &SearchProjectionChangeBatch,
        ) -> Result<SearchProjectionRelationalDelta>,
    {
        let estimated_operations = max_change_operations_per_batch
            .max(max_projection_operations_per_batch)
            .saturating_mul(max_batches);
        let estimated_input_bytes =
            estimated_operations.saturating_mul(SEARCH_PROJECTION_CHANGEFEED_OPERATION_BYTES);
        let permit = self
            .admit_search_projection_maintenance_with_priority(priority, estimated_input_bytes)?;
        let mut store = self.write_store()?;
        if let Some(guard) = guard {
            store.store.validate(guard)?;
        }
        if store.incremental_search_enabled() {
            store.catch_up_incremental_search_with_batch_hydrator(
                max_change_operations_per_batch,
                max_projection_operations_per_batch,
                max_batches,
                batch_hydrator,
                permit.bind_task_context(RuntimeTaskContext::default()),
            )
        } else {
            store.catch_up_search_projection_with_batch_hydrator(
                max_change_operations_per_batch,
                max_projection_operations_per_batch,
                max_batches,
                batch_hydrator,
            )
        }
    }

    pub fn search_projection_changefeed_readiness(
        &self,
        require_restart_recoverable: bool,
        max_operations: Option<usize>,
    ) -> Result<SearchProjectionChangefeedReadiness> {
        self.read_store()?
            .search_projection_changefeed_readiness(require_restart_recoverable, max_operations)
    }

    pub fn catch_up_search_projection_scheduled(
        &self,
        max_operations_per_batch: usize,
        max_batches: usize,
    ) -> Result<ScheduledSearchProjectionCatchUpReport> {
        let estimated_operations = max_operations_per_batch.saturating_mul(max_batches);
        let estimated_input_bytes =
            estimated_operations.saturating_mul(SEARCH_PROJECTION_CHANGEFEED_OPERATION_BYTES);
        let _permit = self.admit_typed_maintenance(
            RuntimeWorkPriority::Background,
            estimated_input_bytes,
            1,
        )?;
        self.write_store()?
            .catch_up_search_projection_scheduled(max_operations_per_batch, max_batches)
    }

    pub(super) fn admit_typed_transaction(&self) -> Result<RuntimePermit> {
        let (governor, request) = {
            let published = self.lock_published_read()?;
            let published = published.as_ref().ok_or_else(|| {
                HawDBError::StorageIntegrity("canonical read publication is unavailable".into())
            })?;
            let config = published.view.snapshot.config();
            let memory_bytes = crate::executor::estimated_mutation_memory_bytes(
                config.mutation_limits,
                config.max_wal_record_bytes,
            )
            .max(
                u64::try_from(config.execution_memory.query_memory_bytes.get()).unwrap_or(u64::MAX),
            );
            let result_bytes = u64::try_from(
                config
                    .max_read_result_payload_bytes
                    .unwrap_or(config.mutation_limits.max_result_payload_bytes.get()),
            )
            .unwrap_or(u64::MAX);
            (
                published.governor.clone(),
                RuntimeWorkRequest::new(RuntimeWorkPriority::Foreground, RuntimeWorkKind::Mutation)
                    .with_cpu_slots(1)
                    .with_memory_bytes(memory_bytes)
                    .with_result_bytes(result_bytes)
                    .with_blocking(true),
            )
        };
        try_admit_runtime(&governor, request)
    }

    pub(super) fn admit_typed_read(
        &self,
        max_estimated_payload_bytes: usize,
    ) -> Result<RuntimePermit> {
        let (governor, request) = {
            let publication = self.lock_published_read()?;
            let publication = publication.as_ref().ok_or_else(|| {
                HawDBError::StorageIntegrity("canonical read publication is unavailable".into())
            })?;
            publication.view.snapshot.ensure_usable()?;
            (
                publication.governor.clone(),
                typed_read_request(
                    publication.view.snapshot.config(),
                    max_estimated_payload_bytes,
                )?,
            )
        };
        try_admit_runtime(&governor, request)
    }

    pub(super) fn lock_published_read(
        &self,
    ) -> Result<MutexGuard<'_, Option<PublishedCanonicalRead>>> {
        // A failed or panicking write guard leaves the last-good value in
        // place but poisons future reads, since that value no longer proves
        // it reflects the most recent committed write.
        if self.read_publication_failed.load(Ordering::Acquire) {
            return Err(HawDBError::Execution(
                "nowledge mem embedded store read lock poisoned".to_string(),
            ));
        }
        self.published_read.lock().map_err(|_| {
            HawDBError::Execution("embedded snapshot publication lock poisoned".to_string())
        })
    }

    pub(super) fn canonical_read_transaction(
        &self,
        max_estimated_payload_bytes: usize,
        max_rows: Option<usize>,
    ) -> Result<(RuntimePermit, crate::DatabaseReadTransaction)> {
        self.canonical_read_transaction_with_context(
            max_estimated_payload_bytes,
            max_rows,
            &RuntimeTaskContext::default(),
        )
    }

    pub(super) fn canonical_read_transaction_with_context(
        &self,
        max_estimated_payload_bytes: usize,
        max_rows: Option<usize>,
        task_context: &RuntimeTaskContext,
    ) -> Result<(RuntimePermit, crate::DatabaseReadTransaction)> {
        self.canonical_read_transaction_with_instance(
            max_estimated_payload_bytes,
            max_rows,
            task_context,
        )
        .map(|(permit, transaction, _)| (permit, transaction))
    }

    pub(super) fn canonical_read_transaction_with_instance(
        &self,
        max_estimated_payload_bytes: usize,
        max_rows: Option<usize>,
        task_context: &RuntimeTaskContext,
    ) -> Result<(
        RuntimePermit,
        crate::DatabaseReadTransaction,
        crate::DatabaseInstanceIdentity,
    )> {
        // Admission emits host telemetry. Never invoke it while holding the
        // publication lock: a sink may reenter the embedded handle.
        let (governor, request) = {
            let published = self.lock_published_read()?;
            let published = published.as_ref().ok_or_else(|| {
                HawDBError::Execution("nowledge mem embedded store read lock poisoned".to_string())
            })?;
            published.view.snapshot.ensure_usable()?;
            (
                published.governor.clone(),
                typed_read_request(
                    published.view.snapshot.config(),
                    max_estimated_payload_bytes,
                )?,
            )
        };
        let permit = governor.try_admit(request).map_err(|error| {
            if error.is_retryable() {
                governor.record_admission_wait(request, error.code, 0);
            }
            HawDBError::Execution(error.to_string())
        })?;
        // Capture after admission so a write completed during admission is visible.
        // Snapshot construction and observation refresh run after unlocking.
        let (view, identity) = {
            let published = self.lock_published_read()?;
            let published = published.as_ref().ok_or_else(|| {
                HawDBError::Execution("nowledge mem embedded store read lock poisoned".to_string())
            })?;
            (
                Arc::clone(&published.view),
                published.instance_identity.clone(),
            )
        };
        view.snapshot.ensure_usable()?;
        let config = view.snapshot.config();
        if let Some(max_rows) = max_rows {
            let configured_rows = config.read_result_row_limit().unwrap_or(usize::MAX);
            if max_rows > configured_rows {
                return Err(HawDBError::Execution(format!(
                    "bounded read snapshot row budget {max_rows} exceeds configured limit {configured_rows}"
                )));
            }
        }
        let task_context = permit.bind_task_context(task_context.clone());
        let transaction = view.begin_read_transaction_with_context(&task_context)?;
        Ok((permit, transaction, identity))
    }

    pub(super) fn finish_canonical_read<T>(&self, result: Result<T>) -> Result<T> {
        if result.is_ok() {
            let published = self.lock_published_read()?;
            let published = published.as_ref().ok_or_else(|| {
                HawDBError::Execution("nowledge mem embedded store read lock poisoned".to_string())
            })?;
            published.view.snapshot.ensure_usable()?;
        }
        result
    }

    pub(super) fn admit_typed_maintenance(
        &self,
        priority: RuntimeWorkPriority,
        estimated_input_bytes: usize,
        io_slots: usize,
    ) -> Result<RuntimePermit> {
        let store = self.read_store()?;
        let config = store.graph.database().config();
        let working_memory_bytes =
            u64::try_from(config.execution_memory.blocking_operator_bytes.get())
                .unwrap_or(u64::MAX);
        let estimated_input_bytes = u64::try_from(estimated_input_bytes).unwrap_or(u64::MAX);
        store.graph.try_admit_runtime(
            RuntimeWorkRequest::new(priority, RuntimeWorkKind::Maintenance)
                .with_cpu_slots(1)
                .with_memory_bytes(working_memory_bytes.saturating_add(estimated_input_bytes))
                .with_io_slots(io_slots)
                .with_blocking(true),
        )
    }

    pub(super) fn admit_transaction(
        &self,
        statements: &[NowledgeGraphStatement],
    ) -> Result<RuntimePermit> {
        let store = self.read_store()?;
        let db = store.graph.database();
        let config = db.config();
        let mut priority = RuntimeWorkPriority::Background;
        let mut estimated_memory_bytes = TYPED_CONTROL_STATEMENT_MEMORY_BYTES;
        let mut result_bytes = 0u64;
        let mut io_slots = 0usize;
        for statement in statements {
            let admission = db.runtime_admission_plan(&statement.cypher, &statement.parameters)?;
            if admission.work_request.priority == WorkPriority::Foreground {
                priority = RuntimeWorkPriority::Foreground;
            }
            estimated_memory_bytes = estimated_memory_bytes.max(admission.estimated_memory_bytes);
            io_slots = io_slots.max(admission.required_io_slots);
            let statement_result_bytes = if admission.is_mutation {
                config.mutation_limits.max_result_payload_bytes.get()
            } else {
                config.max_read_result_payload_bytes.ok_or_else(|| {
                    HawDBError::Execution(
                        "admitted transaction requires max_read_result_payload_bytes".to_string(),
                    )
                })?
            };
            result_bytes = result_bytes
                .saturating_add(u64::try_from(statement_result_bytes).unwrap_or(u64::MAX));
        }
        let request = RuntimeWorkRequest::new(priority, RuntimeWorkKind::Mutation)
            .with_cpu_slots(1)
            .with_memory_bytes(estimated_memory_bytes)
            .with_io_slots(io_slots)
            .with_result_bytes(result_bytes)
            .with_blocking(true);
        store.graph.try_admit_runtime(request)
    }

    pub(super) fn read_store(
        &self,
    ) -> Result<crate::api::publication::DatabasePublicationReadGuard<'_, NowledgeMemEmbeddedStore>>
    {
        self.inner.read().map_err(|_| {
            HawDBError::Execution("nowledge mem embedded store read lock poisoned".to_string())
        })
    }

    pub(super) fn write_store(&self) -> Result<EmbeddedStoreWriteGuard<'_>> {
        if self.read_publication_failed.load(Ordering::Acquire) {
            return Err(HawDBError::StorageIntegrity(
                "canonical read publication is poisoned".into(),
            ));
        }
        let exclusive = self.concurrent.exclusive_database_lease()?;
        let store = self.inner.write().map_err(|_| {
            HawDBError::Execution("nowledge mem embedded store write lock poisoned".to_string())
        })?;
        if self.read_publication_failed.load(Ordering::Acquire) {
            return Err(HawDBError::StorageIntegrity(
                "canonical read publication is poisoned".into(),
            ));
        }
        Ok(EmbeddedStoreWriteGuard {
            store,
            _exclusive: Some(exclusive),
            retired: None,
            published: &self.published_read,
            read_publication_failed: &self.read_publication_failed,
        })
    }
}

pub(super) fn try_admit_runtime(
    governor: &RuntimeGovernor,
    request: RuntimeWorkRequest,
) -> Result<RuntimePermit> {
    governor.try_admit(request).map_err(|error| {
        if error.is_retryable() {
            governor.record_admission_wait(request, error.code, 0);
        }
        HawDBError::Execution(error.to_string())
    })
}

pub(super) fn typed_read_request(
    config: &DatabaseConfig,
    max_estimated_payload_bytes: usize,
) -> Result<RuntimeWorkRequest> {
    let configured_result_bytes = config.max_read_result_payload_bytes.ok_or_else(|| {
        HawDBError::Execution(
            "admitted typed read requires max_read_result_payload_bytes".to_string(),
        )
    })?;
    if max_estimated_payload_bytes > configured_result_bytes {
        return Err(HawDBError::Execution(format!(
                "typed read payload budget {max_estimated_payload_bytes} exceeds configured limit {configured_result_bytes}"
            )));
    }
    let result_bytes = u64::try_from(configured_result_bytes).unwrap_or(u64::MAX);
    let working_memory_bytes =
        u64::try_from(config.execution_memory.query_memory_bytes.get()).unwrap_or(u64::MAX);
    Ok(
        RuntimeWorkRequest::foreground_query(working_memory_bytes, result_bytes)
            .with_io_slots(1)
            .with_blocking(true),
    )
}

pub(super) fn search_projection_delta_bytes(delta: &SearchProjectionDelta) -> usize {
    let upsert_bytes = delta.upserts.iter().fold(0usize, |total, row| {
        let embedding_bytes = row.embedding.as_ref().map_or(0, |embedding| {
            embedding.len().saturating_mul(size_of::<f32>())
        });
        let metadata_bytes = row
            .metadata
            .iter()
            .fold(0usize, |metadata_total, (key, value)| {
                metadata_total
                    .saturating_add(key.len())
                    .saturating_add(value.len())
            });
        total
            .saturating_add(size_of::<crate::SearchProjectionRow>())
            .saturating_add(row.external_id.len())
            .saturating_add(row.title.len())
            .saturating_add(row.body.len())
            .saturating_add(row.source_id.as_ref().map_or(0, String::len))
            .saturating_add(embedding_bytes)
            .saturating_add(metadata_bytes)
    });
    delta.deletes.iter().fold(upsert_bytes, |total, id| {
        total
            .saturating_add(size_of::<String>())
            .saturating_add(id.len())
    })
}
