// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::api::{PreparedRuntimeQuery, QueryExecutionTrace, QueryRequest};
use crate::planner::{BatchMutationOperation, PhysicalPlan, Predicate};
use crate::schema::{Catalog, ConstraintSubject};
use hawdb_executor::{QueryMemoryClass, QueryMemoryLedger};
use hawdb_storage::read_view::GraphReadAllocator;
use hawdb_storage::scan::GraphScanControl;
use std::collections::BTreeSet;

mod sql;

#[cfg(test)]
mod tests;

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AutocommitMutationPhase {
    BeforeLocks,
    BeforeExecution,
    Staged,
}

#[cfg(test)]
#[derive(Debug)]
pub(super) struct AutocommitMutationGate {
    phase: AutocommitMutationPhase,
    reached: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

struct MutationScope<'a> {
    requests: Vec<LockRequest>,
    point: Option<(&'a str, &'a str, &'a Value)>,
}

impl<'a> MutationScope<'a> {
    fn for_plan(plan: &'a PhysicalPlan, catalog: &Catalog) -> Self {
        match plan {
            // RETURN reads only the entities its mutation creates or changes,
            // and the endpoints that mutation already protects.
            PhysicalPlan::MutationReturn { mutation, .. } => Self::for_plan(mutation, catalog),
            PhysicalPlan::SetNodeProperty {
                label,
                predicate,
                property,
                ..
            } => Self::node_update(label, predicate.as_ref(), [property.as_str()], catalog),
            PhysicalPlan::SetNodeProperties {
                label,
                predicate,
                assignments,
                ..
            }
            | PhysicalPlan::SetNodePropertiesReturn {
                label,
                predicate,
                assignments,
                ..
            } => Self::node_update(
                label,
                predicate.as_ref(),
                assignments.iter().map(|item| item.property.as_str()),
                catalog,
            ),
            PhysicalPlan::CreateNode { label, .. }
            | PhysicalPlan::MergeNode { label, .. }
            | PhysicalPlan::UnwindMutation {
                operation:
                    BatchMutationOperation::CreateNode { label, .. }
                    | BatchMutationOperation::MergeNode { label, .. },
                ..
            } if !label.is_empty() => Self {
                requests: vec![
                    LockRequest::graph_allocation(GraphAllocationKind::Node),
                    LockRequest::graph_label(LockMode::Exclusive, label),
                ],
                point: None,
            },
            // Relationship mutations can read other endpoints/types, and DDL
            // changes planning capabilities. Keep their complete scope protected.
            _ => Self {
                requests: vec![LockRequest::database(LockMode::Exclusive)],
                point: None,
            },
        }
    }

    fn node_update(
        label: &'a str,
        predicate: Option<&'a Predicate>,
        properties: impl IntoIterator<Item = &'a str>,
        catalog: &Catalog,
    ) -> Self {
        if label.is_empty()
            || label.contains(':')
            || predicate.is_some_and(|predicate| !local_predicate(predicate))
        {
            return Self {
                requests: vec![LockRequest::database(LockMode::Exclusive)],
                point: None,
            };
        }
        let unique = |property: &str| {
            catalog.label_id(label).is_some_and(|id| {
                catalog.unique_constraints().any(|constraint| {
                    constraint.subject == ConstraintSubject::Node(id)
                        && constraint.property == property
                })
            })
        };
        // Unique-key changes require the label domain lock used by explicit
        // transaction footprints. Never upgrade a shared key-membership guard.
        let changes_unique_key = properties.into_iter().any(unique);
        if !changes_unique_key && let Some(predicate) = predicate {
            if let Some(id) = exact_node_id(predicate) {
                return Self {
                    requests: vec![
                        LockRequest::graph_label(LockMode::Shared, label),
                        LockRequest::graph_node(LockMode::Exclusive, id),
                    ],
                    point: None,
                };
            }
            if let Some((property, value)) = unique_equality(predicate, &unique) {
                return Self {
                    requests: vec![LockRequest::graph_label(LockMode::Shared, label)],
                    point: Some((label, property, value)),
                };
            }
        }
        Self {
            requests: vec![LockRequest::graph_label(LockMode::Exclusive, label)],
            point: None,
        }
    }
}

fn local_predicate(predicate: &Predicate) -> bool {
    match predicate {
        Predicate::And(items) | Predicate::Or(items) => items.iter().all(local_predicate),
        Predicate::Not(item) => local_predicate(item),
        Predicate::ConstantBool(_)
        | Predicate::IdEq { .. }
        | Predicate::IdNotEq { .. }
        | Predicate::IdCompare { .. }
        | Predicate::IdIn { .. }
        | Predicate::PropertyEq { .. }
        | Predicate::PropertyNotEq { .. }
        | Predicate::PropertyCompare { .. }
        | Predicate::PropertyListContains { .. }
        | Predicate::PropertyListContainsLower { .. }
        | Predicate::PropertyContains { .. }
        | Predicate::PropertyStartsWith { .. }
        | Predicate::PropertyEndsWith { .. }
        | Predicate::PropertyRegexMatch { .. }
        | Predicate::PropertyIsNull { .. }
        | Predicate::PropertyIsNotNull { .. }
        | Predicate::PropertyIn { .. } => true,
        _ => false,
    }
}

fn exact_node_id(predicate: &Predicate) -> Option<u64> {
    match predicate {
        Predicate::IdEq {
            value: Value::Int(id),
            ..
        } => u64::try_from(*id).ok(),
        Predicate::And(items) => items.iter().find_map(exact_node_id),
        _ => None,
    }
}

fn unique_equality<'a>(
    predicate: &'a Predicate,
    unique: &impl Fn(&str) -> bool,
) -> Option<(&'a str, &'a Value)> {
    match predicate {
        Predicate::PropertyEq {
            property, value, ..
        } if *value != Value::Null && unique(property) => Some((property, value)),
        Predicate::And(items) => items
            .iter()
            .find_map(|predicate| unique_equality(predicate, unique)),
        _ => None,
    }
}

struct MutationLocks<'a> {
    inner: &'a ConcurrentDatabaseInner,
    transaction_id: u64,
    started: Instant,
}

impl MutationLocks<'_> {
    fn acquire(&self, requests: &[LockRequest]) -> Result<()> {
        self.inner
            .locks
            .acquire(
                self.transaction_id,
                requests,
                self.started,
                DEFAULT_PESSIMISTIC_LOCK_TIMEOUT,
            )
            .map_err(Into::into)
    }
}

impl Drop for MutationLocks<'_> {
    fn drop(&mut self) {
        self.inner.locks.release(self.transaction_id);
    }
}

impl ConcurrentDatabase {
    #[cfg(test)]
    pub(crate) fn pause_next_autocommit_mutation(
        &self,
        phase: AutocommitMutationPhase,
    ) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (reached, events) = std::sync::mpsc::channel();
        let (resume, release) = std::sync::mpsc::channel();
        *self.inner.autocommit_mutation_gate.lock().unwrap() = Some(AutocommitMutationGate {
            phase,
            reached,
            release,
        });
        (events, resume)
    }

    #[cfg(test)]
    fn wait_before_autocommit_mutation(&self, phase: AutocommitMutationPhase) -> Result<()> {
        let gate = {
            let mut gate = self.inner.autocommit_mutation_gate.lock().unwrap();
            if gate.as_ref().is_some_and(|gate| gate.phase == phase) {
                gate.take()
            } else {
                None
            }
        };
        if let Some(gate) = gate {
            gate.reached
                .send(())
                .map_err(|_| HawDBError::Execution("mutation gate receiver dropped".into()))?;
            gate.release
                .recv()
                .map_err(|_| HawDBError::Execution("mutation gate sender dropped".into()))?;
        }
        Ok(())
    }

    pub(crate) fn query_prepared_mutation(
        &self,
        mut prepared: PreparedRuntimeQuery,
        request: QueryRequest<'_>,
        capture_trace: bool,
        admission: Option<Arc<hawdb_qos::RuntimePermit>>,
        expected_identity: Option<&crate::DatabaseInstanceIdentity>,
    ) -> Result<(QueryOutput, QueryExecutionTrace)> {
        let started = Instant::now();
        let statement_kind = prepared.statement_kind();
        let parse_nanos = prepared.parse_nanos();
        let query_identity = prepared.take_query_identity(request.cypher);
        let view = self.inner.commits.read_view()?;
        let recorder = view.recorder.clone();
        let result = (|| {
            if !view.accepts_prepared(&prepared) || !prepared.matches_request(request) {
                return Err(preparation_changed());
            }
            let plan = prepared
                .physical_plan()
                .filter(|_| prepared.uses_transaction_kernel())
                .ok_or_else(preparation_changed)?;
            let scope = MutationScope::for_plan(plan, &view.snapshot.0.catalog);
            drop(view);
            #[cfg(test)]
            self.wait_before_autocommit_mutation(AutocommitMutationPhase::BeforeLocks)?;
            let locks = MutationLocks {
                inner: &self.inner,
                transaction_id: self.inner.transaction_ids.allocate()?,
                started,
            };
            super::super::query_runtime::query_runtime_checkpoint(request.task_context)?;
            locks.acquire(&scope.requests)?;
            if let Some((label, property, value)) = scope.point {
                // Label membership is now stable: inserts and unique-key changes
                // need its exclusive lock. Lock the unique candidate even when
                // additional predicates currently exclude it, then take a fresh
                // snapshot after its node lock has been granted.
                let view = self.inner.commits.read_view()?;
                if !view.accepts_prepared(&prepared) {
                    return Err(preparation_changed());
                }
                let read = &view.snapshot.0;
                let label_id = read
                    .catalog
                    .label_id(label)
                    .ok_or_else(preparation_changed)?;
                let node_id =
                    unique_candidate(read, label_id, property, value, request.task_context)?;
                drop(view);
                if let Some(node_id) = node_id {
                    locks.acquire(&[LockRequest::graph_node(LockMode::Exclusive, node_id)])?;
                }
            }
            // No private workspace or generation pin survives the lock wait.
            let mut transaction =
                self.begin_transaction_with_id(Default::default(), locks.transaction_id)?;
            if expected_identity
                .is_some_and(|identity| identity != &transaction.runtime.instance_identity)
            {
                return Err(preparation_changed());
            }
            transaction.task_context = request.task_context.cloned();
            transaction.admission = admission;
            let prepared = PreparedGraphTransactionRequest::from_admission(
                &transaction.runtime,
                &transaction.state,
                prepared,
                request,
            )?;
            #[cfg(test)]
            self.wait_before_autocommit_mutation(AutocommitMutationPhase::BeforeExecution)?;
            self.inner.commits.metrics.graph_execution();
            let outcome = prepared.execute(&transaction.runtime, &mut transaction.state)?;
            // Semantic scopes protect predicate reads; the actual footprint
            // additionally participates in explicit-transaction key locking.
            locks.acquire(&graph_lock_requests(&outcome.lock_footprint))?;
            transaction.successful_statements = 1;
            let returns_rows = prepared.returns_statement_rows();
            let committed = transaction.commit()?;
            let output = if returns_rows {
                outcome.output
            } else {
                // The commit publishes the staged statement; its writes were
                // counted when the statement executed.
                committed.with_counters(outcome.output.counters)
            };
            Ok((output, prepared.into_trace(capture_trace)))
        })();
        recorder.record_statement_execution(
            "cypher",
            request.cypher,
            statement_kind,
            started,
            result.as_ref().map(|(output, _)| output),
            StatementExecutionContext {
                query_identity: query_identity.as_ref(),
                execution_profile: None,
                retained_profile: None,
                access_control: request.access_control,
                parse_nanos,
            },
        );
        result
    }
}

fn preparation_changed() -> HawDBError {
    HawDBError::Execution(
        "prepared mutation changed before execution; prepare and admit again".into(),
    )
}

fn unique_candidate(
    read: &DatabaseReadTransaction,
    label: hawdb_core::LabelId,
    property: &str,
    value: &Value,
    task_context: Option<&hawdb_core::RuntimeTaskContext>,
) -> Result<Option<u64>> {
    super::super::query_runtime::query_runtime_checkpoint(task_context)?;
    let ledger = QueryMemoryLedger::for_query(&read.config.execution_memory, task_context)?;
    let account = ledger.account(
        QueryMemoryClass::ExternalRead,
        "autocommit unique candidate",
        read.config.execution_memory.blocking_operator_bytes,
    );
    if !read.store.is_out_of_core() && read.catalog.property_index_id(label, property).is_some() {
        // The resident equality index is complete. Borrow its record and avoid
        // scanning/hydrating the label merely to acquire one node lock.
        let _key = account.reserve(
            property
                .len()
                .saturating_add(hawdb_core::ids::projected_value_allocation_bytes(value)),
        )?;
        if let Some(task) = task_context {
            task.charge_work(1)?;
        }
        return Ok(read
            .store
            .seek_nodes_by_property(label, property, value)
            .next()
            .map(|node| node.id.0));
    }
    let mut node_id = None;
    read.store.visit_projected_nodes_by_property_admitted(
        label,
        property,
        std::slice::from_ref(value),
        &BTreeSet::new(),
        &mut GraphReadAllocator::new(
            &mut |bytes| hawdb_executor::store::admit_graph_read(&account, task_context, bytes),
            task_context,
        ),
        &mut |node| {
            node_id = Some(node.id.0);
            Ok(GraphScanControl::Stop)
        },
    )?;
    Ok(node_id)
}
