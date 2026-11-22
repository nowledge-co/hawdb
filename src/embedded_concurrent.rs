// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Shared embedded ownership with admitted MVCC reads and concurrent writers.

pub(crate) mod owner;

use crate::embedded::{EmbeddedQueryError, HawDBEmbedded};
use crate::{
    ConcurrentDatabase, HawDBError, QueryOutput, QueryRequest, QueryStreamOptions, Result,
    RuntimeGovernor, SqlQueryRequest,
};
use hawdb_core::RuntimeTaskContext;
use hawdb_qos::{RuntimePermit, RuntimeWorkPriority, RuntimeWorkRequest};
use owner::{ConcurrentRuntime, EmbeddedOwner};
use std::sync::{Arc, Mutex};

/// Cloneable synchronous facade over one embedded database and runtime governor.
/// Reads pin the last durable publication; writers stage privately and share
/// the existing commit sequencer. Administrative callbacks remain exclusive.
#[derive(Debug, Clone)]
pub struct HawDBConcurrentEmbedded {
    owner: Arc<EmbeddedOwner>,
    concurrent: Arc<Mutex<Option<ConcurrentRuntime>>>,
}

impl HawDBConcurrentEmbedded {
    pub fn new(embedded: HawDBEmbedded) -> Self {
        Self {
            owner: Arc::new(EmbeddedOwner::new(embedded)),
            concurrent: Arc::new(Mutex::new(None)),
        }
    }

    pub fn runtime_governor(&self) -> &RuntimeGovernor {
        self.owner.governor()
    }

    /// Exclusive administration, including context replacement and configuration.
    /// The callback must not reenter this facade or wait for another query on it.
    /// A replacement handle is rebound to this facade's governor on return.
    pub fn with_embedded_mut<T>(&self, operation: impl FnOnce(&mut HawDBEmbedded) -> T) -> T {
        let _administration = self.owner.administration();
        operation(&mut self.owner.direct_write())
    }

    pub fn begin_read_transaction(&self) -> Result<crate::DatabaseReadTransaction> {
        self.owner.view()?.begin_read_transaction()
    }

    pub fn query_with_params_retained_node(
        &self,
        cypher: &str,
        parameters: &std::collections::BTreeMap<String, crate::Value>,
        options: crate::RetainedQueryOptions,
    ) -> std::result::Result<crate::RetainedNodeSnapshotCursor, crate::RetainedQueryError> {
        self.owner
            .view()?
            .retained_node_query(cypher, parameters, options)
    }

    pub fn query_request_admitted(
        &self,
        request: QueryRequest<'_>,
    ) -> std::result::Result<QueryOutput, EmbeddedQueryError> {
        let default_context = RuntimeTaskContext::default();
        let context = request.task_context.unwrap_or(&default_context);
        self.check_context(context)?;
        let planning_request = crate::api::runtime_planning_request(
            request.cypher.len(),
            RuntimeWorkPriority::Foreground,
        );
        let (prepared, identity) = {
            let permit = self.admit(planning_request)?;
            let view = self.owner.view()?;
            (
                view.prepare(request)?.with_planning_memory(
                    permit.into_retained_memory().expect("planning owns memory"),
                ),
                view.sql_planning.identity.clone(),
            )
        };
        let admission = prepared.admission().clone();
        let result_budget = self.result_budget()?;
        let (prepared, permit) = self.admit_prepared(prepared, |snapshot, prepared| {
            let request = admission.runtime_work_request_for_snapshot(result_budget, snapshot);
            request
                .with_memory_bytes(request.memory_bytes.max(planning_request.memory_bytes))
                .with_retained_memory_bytes(prepared.map_or(0, |p| p.planning_memory_bytes()))
        })?;
        let context = permit.bind_task_context(context.clone());
        self.check_context(&context)?;
        let view = self.owner.view()?;
        if view.sql_planning.identity != identity {
            return Err(HawDBError::Execution(
                "database instance changed during admission; retry the query".into(),
            )
            .into());
        }
        let prepared = match prepared {
            Some(prepared) if view.accepts_prepared(&prepared) => prepared,
            stale => {
                drop(stale);
                view.prepare(request.with_task_context(&context))?
            }
        };
        if prepared.admission() != &admission {
            return Err(HawDBError::Execution(
                "query admission changed during preparation; retry the query".into(),
            )
            .into());
        }
        let query = request.with_task_context(&context);
        let result = if prepared.uses_read_snapshot() {
            let limits = QueryStreamOptions {
                max_rows: view.snapshot.config().read_result_row_limit(),
                max_payload_bytes: Some(
                    usize::try_from(permit.request().result_bytes).unwrap_or(usize::MAX),
                ),
            };
            let result = view
                .query_prepared_with_trace(
                    prepared,
                    query.with_restrictive_output_limits(limits),
                    false,
                    &mut crate::executor::NoExternalReadOperator,
                )
                .map(|(output, _)| output);
            self.owner.finish_read(result)
        } else {
            drop(view);
            let _execution = self.owner.execution()?;
            let concurrent = self.concurrent()?;
            if prepared.uses_transaction_kernel() {
                concurrent
                    .query_prepared_mutation(prepared, query, false, None, Some(&identity))
                    .map(|(output, _)| output)
            } else {
                concurrent.with_autocommit_exclusive(|database| {
                    if database.instance_identity()? != identity
                        || !database.accepts_prepared_query(&prepared)?
                    {
                        return Err(HawDBError::Execution(
                            "query execution context changed during admission; retry the query"
                                .into(),
                        ));
                    }
                    database.query_prepared_request(prepared, query)
                })
            }
        };
        // A completed mutation remains successful after cancellation;
        // failed/cancelled work retains the synchronous facade's error kind.
        if !admission.is_mutation || result.is_err() {
            self.check_context(&context)?;
        }
        result.map_err(EmbeddedQueryError::Database)
    }

    pub fn query_sql_request_admitted(
        &self,
        request: SqlQueryRequest<'_>,
    ) -> std::result::Result<QueryOutput, EmbeddedQueryError> {
        let default_context = RuntimeTaskContext::default();
        let context = request.task_context.unwrap_or(&default_context);
        self.check_context(context)?;
        let planning_request = crate::api::runtime_planning_request(
            request.sql.len(),
            RuntimeWorkPriority::Foreground,
        );
        let planning = self.owner.sql_planning()?;
        let prepared = {
            let permit = self.admit(planning_request)?;
            planning
                .prepare(request.sql)?
                .with_planning_memory(permit.into_retained_memory().expect("planning owns memory"))
        };
        let work = prepared.work_request(self.result_budget()?);
        let (prepared, permit) = self.admit_prepared(prepared, |_, prepared| {
            work.with_memory_bytes(work.memory_bytes.max(planning_request.memory_bytes))
                .with_retained_memory_bytes(prepared.map_or(0, |p| p.planning_memory_bytes()))
        })?;
        let prepared = match prepared {
            Some(prepared) => prepared,
            None => planning.prepare(request.sql)?,
        };
        let context = permit.bind_task_context(context.clone());
        self.check_context(&context)?;
        let request = request.with_task_context(&context);
        if prepared.uses_catalog() {
            return self
                .with_embedded_mut(|embedded| {
                    ensure_autocommit(embedded)?;
                    let (prepared, _memory) = prepared.into_statement(embedded.database())?;
                    embedded
                        .database_mut()
                        .query_sql_with_prepared_request(request, prepared)
                })
                .map_err(EmbeddedQueryError::Database);
        }
        let view = self.owner.view()?;
        let uses_snapshot = prepared.uses_snapshot();
        let (prepared, _memory) =
            prepared.into_statement_for(&view.sql_planning.identity, view.snapshot.config())?;
        let result = if uses_snapshot {
            self.owner
                .finish_read(view.query_sql_observed(request, prepared))
        } else {
            drop(view);
            let _execution = self.owner.execution()?;
            // Check after the administration gate: a replacement must never
            // redirect a prepared mutation to a different database.
            let current = self.owner.sql_planning()?;
            if current.identity != planning.identity || current.config != planning.config {
                return Err(HawDBError::Execution(
                    "SQL execution context changed during admission; retry the query".into(),
                )
                .into());
            }
            self.concurrent()?.query_prepared_sql_request(
                request,
                prepared,
                hawdb_core::time::Instant::now(),
            )
        };
        if uses_snapshot {
            self.check_context(&context)?;
        }
        result.map_err(EmbeddedQueryError::Database)
    }

    fn concurrent(&self) -> Result<ConcurrentDatabase> {
        self.owner.concurrent(&self.concurrent)
    }

    fn result_budget(&self) -> Result<u64> {
        Ok(crate::api::runtime_result_memory_budget(
            &self.owner.sql_planning()?.config,
            self.runtime_governor()
                .snapshot()
                .limits
                .result_budget_bytes,
        ))
    }

    fn admit(
        &self,
        request: RuntimeWorkRequest,
    ) -> std::result::Result<RuntimePermit, EmbeddedQueryError> {
        crate::embedded::admit_query(self.runtime_governor(), request)
    }

    fn admit_prepared<T>(
        &self,
        prepared: T,
        request: impl Fn(hawdb_qos::RuntimeGovernorSnapshot, Option<&T>) -> RuntimeWorkRequest,
    ) -> std::result::Result<(Option<T>, RuntimePermit), EmbeddedQueryError> {
        crate::embedded::admit_prepared(self.runtime_governor(), prepared, request)
    }

    fn check_context(
        &self,
        context: &RuntimeTaskContext,
    ) -> std::result::Result<(), EmbeddedQueryError> {
        crate::embedded::check_query_context(self.runtime_governor(), context)
    }
}

fn ensure_autocommit(embedded: &HawDBEmbedded) -> Result<()> {
    if embedded.transaction_active() {
        Err(HawDBError::Execution(
            "a user transaction is open on this database".into(),
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
