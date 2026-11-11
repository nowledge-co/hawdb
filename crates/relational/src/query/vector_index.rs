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

//! Explicit candidate input for a SQL table function. Identities are hydrated
//! through the caller's canonical row snapshot; ordinary relational predicates,
//! projections and Top-N execute after membership is selected. Explicit
//! eligible modes collect WHERE matches before exact or approximate ranking.

use super::*;

use hawdb_storage::relational::{decode_relational_primary_key, RelationalColumnSchema};
use projection::{
    add_relational_order_keys, execute_relational_order, strip_relational_sort_columns,
};

pub trait VectorIndexCandidates {
    fn row_count(&self) -> usize;
    fn row(&self, ordinal: usize) -> Result<(&str, f64)>;
}

pub struct VectorIndexInput<'a> {
    pub table: &'a str,
    pub candidates: &'a dyn VectorIndexCandidates,
}

/// How a candidate table function exposes its per-row score.
#[derive(Clone, Copy)]
pub(super) struct CandidateScore {
    /// Default qualifier when the call has no alias.
    pub function: &'static str,
    /// Subject of user-facing refusals.
    pub context: &'static str,
    /// Virtual DOUBLE PRECISION output column; a source column of the same
    /// name is refused.
    pub column: &'static str,
    pub valid: fn(f64) -> bool,
    pub invalid: &'static str,
    /// Integrity refusal when a ranked identity has no row in the snapshot.
    pub absent: &'static str,
    /// Stop after this many candidates satisfy WHERE (filter before ranking).
    pub max_accepted: Option<usize>,
}

const VECTOR_DISTANCE: CandidateScore = CandidateScore {
    function: "vector_index_search",
    context: "vector index SQL",
    column: "distance",
    valid: |distance| distance.is_finite() && (0.0..=2.0).contains(&distance),
    invalid: "invalid vector index cosine distance",
    absent: "vector candidate is absent from the bound SQL snapshot",
    max_accepted: None,
};

pub struct VectorIndexSqlRows {
    rows: Vec<Row>,
    pub row_execution: RelationalRowExecutionEvidence,
    pub output_payload_bytes: usize,
    _memory: QueryMemoryLease,
}

impl VectorIndexSqlRows {
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
    pub(super) fn into_parts(self) -> (Vec<Row>, RelationalRowExecutionEvidence) {
        (self.rows, self.row_execution)
    }
    pub fn deliver(self, mut consumer: impl FnMut(Row) -> Result<()>) -> Result<()> {
        for row in self.rows {
            consumer(row)?;
        }
        Ok(())
    }
}

pub struct VectorIndexSqlExecution<'state, 'runtime, R: RelationalRowStoreReader> {
    pub state: &'state RelationalState,
    pub row_mode: RelationalRowReadMode<'state, R>,
    pub resources: RelationalQueryResourceContext<'runtime>,
    pub memory_ledger: &'runtime QueryMemoryLedger,
}

impl<'runtime, R: RelationalRowStoreReader> VectorIndexSqlExecution<'_, 'runtime, R> {
    fn admitted_ledger(&self) -> Result<&'runtime QueryMemoryLedger> {
        let limit = hawdb_executor::memory::enforced_query_memory_budget(
            self.resources.execution_memory,
            self.resources.task_context,
        )?;
        if self.memory_ledger.snapshot().budget_bytes > limit.get() {
            return Err(HawDBError::Execution(
                "vector SQL ledger exceeds the canonical query memory grant".into(),
            ));
        }
        Ok(self.memory_ledger)
    }
}

fn validate_vector_index_select(select: &SelectStatement) -> Result<()> {
    if select.vector_index().is_none() {
        return Err(HawDBError::Semantic("vector index SQL supports a single source with WHERE, projection, ORDER BY, LIMIT and OFFSET".into()));
    }
    validate_candidate_select(select, "vector index SQL")
}

pub(super) fn validate_candidate_select(select: &SelectStatement, context: &str) -> Result<()> {
    if !select.joins.is_empty()
        || select.distinct
        || !select.group_by.is_empty()
        || select.having.is_some()
        || select.lock_strength.is_some()
        || select.projection.iter().any(projection_contains_aggregate)
    {
        return Err(HawDBError::Semantic(format!(
            "{context} supports a single source with WHERE, projection, ORDER BY, LIMIT and OFFSET"
        )));
    }
    Ok(())
}

pub fn collect_vector_eligible_domain<'query, R: RelationalRowStoreReader>(
    select: &SelectStatement,
    parameters: &[Value],
    definition: &'query hawdb_core::VectorIndexDescriptor,
    identity: hawdb_core::VectorIndexReadIdentity,
    embedding: hawdb_core::VectorRef<'query>,
    max_work: usize,
    execution: VectorIndexSqlExecution<'_, 'query, R>,
) -> Result<(
    hawdb_executor::vector_eligibility::VectorEligibleDomain<'query>,
    RelationalRowExecutionEvidence,
)> {
    use hawdb_executor::vector_eligibility::VectorEligibleDomain;
    validate_vector_index_select(select)?;
    let hawdb_core::VectorIndexTarget::Relational {
        table,
        column_ordinal,
        ..
    } = definition.target()
    else {
        return Err(HawDBError::Semantic(
            "SQL eligibility requires a relational index".into(),
        ));
    };
    let resources = execution.resources;
    let task = resources.task_context;
    let schema = execution.state.table_schema(table).ok_or_else(|| {
        HawDBError::StorageIntegrity("vector index source table is absent".into())
    })?;
    if schema.column_position("distance").is_some() {
        return Err(HawDBError::Semantic(
            "vector index SQL reserves the distance output column".into(),
        ));
    }
    let qualifier = select
        .from_alias
        .as_deref()
        .unwrap_or("vector_index_search");
    let ledger = execution.admitted_ledger()?;
    let account = ledger.account(
        QueryMemoryClass::BlockingState,
        "VectorIndexSqlScan eligibility",
        resources.execution_memory.blocking_operator_bytes,
    );
    let mut domain = VectorEligibleDomain::new(
        definition,
        identity,
        embedding,
        select.vector_index().expect("validated vector source").mode,
        max_work.min(resources.limits.max_candidate_work),
        &account,
        task,
    )?;
    let _setup_memory = account.reserve(
        schema
            .columns
            .len()
            .saturating_mul(256)
            .saturating_add(table.len())
            .saturating_add(qualifier.len())
            .saturating_add(4096),
    )?;
    let empty = BoundRow {
        bindings: vec![Binding {
            binding: BindingId::new(0),
            table,
            qualifier,
            schema,
            row: None,
        }],
    };
    // The generated distance column does not exist in this input schema.
    // Validate names and predicate types before looking at any source rows.
    let _predicate_memory = select
        .selection
        .as_ref()
        .map(|predicate| {
            projection_memory::admit_predicate(predicate, &empty, parameters, &account)
        })
        .transpose()?;
    let _predicate = select
        .selection
        .as_ref()
        .map(|predicate| {
            BoundStreamingPredicate::bind(predicate, parameters, schema, table, qualifier)
        })
        .transpose()?;
    let original_task = domain.task().clone();
    let row_task = original_task
        .clone()
        .with_memory_controller(Arc::new(CanonicalPointMemory {
            account: ledger.account(
                QueryMemoryClass::PipelineBatch,
                "VectorIndexSqlScan eligible rows",
                resources.execution_memory.batch_payload_bytes,
            ),
            original_task,
        }));
    let row_runtime = execution.row_mode.open_runtime(
        execution.state,
        crate::field_plan::RelationalFieldPlan::all_columns(table, schema.columns.len()),
        resources.limits.row_read,
        resources.limits.hydration,
        &row_task,
    )?;
    row_runtime.visit_all(table, |row| {
        domain.observe_input()?;
        let _row_memory =
            account.reserve(row.resident_bytes().saturating_mul(4).saturating_add(4096))?;
        let binding = BoundRow {
            bindings: vec![Binding {
                binding: BindingId::new(0),
                table,
                qualifier,
                schema,
                row: Some(row),
            }],
        };
        let _scratch = select
            .selection
            .as_ref()
            .map(|predicate| {
                projection_memory::admit_predicate(predicate, &binding, parameters, &account)
            })
            .transpose()?;
        if let Some(predicate) = &select.selection
            && predicate_truth(predicate, &binding, parameters)? != Some(true)
        {
            return Ok(true);
        }
        let row = binding.bindings[0]
            .row
            .as_ref()
            .expect("canonical eligible row");
        let vector = match row.value(*column_ordinal as usize)? {
            RelationalValue::Null => return Ok(true),
            RelationalValue::Vector(vector) => vector.as_ref(),
            _ => {
                return Err(HawDBError::StorageIntegrity(
                    "eligible SQL field is not a vector".into(),
                ))
            }
        };
        let encoded = hawdb_storage::relational::encode_relational_primary_key(row.primary_key())
            .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
        let mut identity = String::with_capacity(encoded.len().saturating_mul(2).saturating_add(2));
        identity.push_str("r:");
        use std::fmt::Write;
        for byte in encoded {
            write!(&mut identity, "{byte:02x}").expect("String write");
        }
        domain.insert(&identity, vector)?;
        Ok(true)
    })?;
    domain.finish()?;
    Ok((domain, row_runtime.evidence()))
}

pub(super) fn residual_selection(select: &SelectStatement) -> Option<&SqlExpression> {
    if select
        .vector_index()
        .is_some_and(|search| search.mode.requires_eligible_domain())
    {
        None
    } else {
        select.selection.as_ref()
    }
}

pub fn execute_vector_index_select<R: RelationalRowStoreReader>(
    select: &SelectStatement,
    parameters: &[Value],
    input: VectorIndexInput<'_>,
    execution: VectorIndexSqlExecution<'_, '_, R>,
) -> Result<VectorIndexSqlRows> {
    validate_vector_index_select(select)?;
    execute_candidate_select(select, parameters, input, execution, VECTOR_DISTANCE)
}

/// Hydrates ranked candidate identities through the canonical row snapshot,
/// then applies WHERE, projection, ORDER BY, LIMIT and OFFSET.
pub(super) fn execute_candidate_select<R: RelationalRowStoreReader>(
    select: &SelectStatement,
    parameters: &[Value],
    input: VectorIndexInput<'_>,
    execution: VectorIndexSqlExecution<'_, '_, R>,
    score: CandidateScore,
) -> Result<VectorIndexSqlRows> {
    let resources = execution.resources;
    let task = resources.task_context;
    hawdb_executor::pipeline::runtime_checkpoint(task)?;
    if input.candidates.row_count() > resources.limits.max_candidate_work {
        return Err(HawDBError::read_budget_exceeded(
            hawdb_core::ReadBudgetResource::Rows,
            resources.limits.max_candidate_work,
            "vector candidate input exceeds SQL work admission",
        ));
    }
    let canonical_schema = execution.state.table_schema(input.table).ok_or_else(|| {
        HawDBError::StorageIntegrity("vector index source table is absent".into())
    })?;
    if canonical_schema.column_position(score.column).is_some() {
        return Err(HawDBError::Semantic(format!(
            "{} reserves the {} output column",
            score.context, score.column
        )));
    }
    let ledger = execution.admitted_ledger()?;
    let account = ledger.account(
        QueryMemoryClass::BlockingState,
        "VectorIndexSqlScan",
        resources.execution_memory.blocking_operator_bytes,
    );
    // This projection schema excludes canonical index/FK metadata. Prepay names,
    // field maps and column slots before cloning the narrow query-owned schema.
    let schema_bytes = canonical_schema
        .columns
        .iter()
        .try_fold(4096usize, |bytes, column| {
            bytes
                .checked_add(column.name.len())
                .and_then(|n| n.checked_add(1024))
                .ok_or_else(|| HawDBError::Execution("vector SQL schema size overflow".into()))
        })?;
    let _schema_memory = account.reserve(schema_bytes)?;
    let mut schema = RelationalTableSchema {
        materialized_view: None,
        name: input.table.to_owned(),
        columns: canonical_schema
            .columns
            .iter()
            .map(|column| RelationalColumnSchema {
                name: column.name.clone(),
                scalar_type: column.scalar_type,
                nullable: column.nullable,
                default: None,
            })
            .collect(),
        primary_key: Vec::new(),
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
    };
    let distance_ordinal = schema.columns.len();
    schema.columns.push(RelationalColumnSchema {
        name: score.column.into(),
        scalar_type: RelationalScalarType::DoublePrecision,
        nullable: false,
        default: None,
    });
    let qualifier = select.from_alias.as_deref().unwrap_or(score.function);
    // Bind even when the candidate stream is empty; invalid names must not
    // become data-dependent successes.
    let empty = BoundRow {
        bindings: vec![Binding {
            binding: BindingId::new(0),
            table: input.table,
            qualifier,
            schema: &schema,
            row: None,
        }],
    };
    {
        let _memory = projection_memory::admit(select, &empty, parameters, &account)?;
        let mut empty_projection = project_bound_row(&empty, &select.projection, parameters)?;
        add_relational_order_keys(select, &empty, &mut empty_projection)?;
        let _predicate = residual_selection(select)
            .map(|predicate| {
                BoundStreamingPredicate::bind(
                    predicate,
                    parameters,
                    &schema,
                    input.table,
                    qualifier,
                )
            })
            .transpose()?;
    }
    let original_task = task.cloned().unwrap_or_default();
    let row_task = original_task
        .clone()
        .with_memory_controller(std::sync::Arc::new(CanonicalPointMemory {
            account: ledger.account(
                QueryMemoryClass::PipelineBatch,
                "VectorIndexSqlScan canonical point",
                resources.execution_memory.batch_payload_bytes,
            ),
            original_task,
        }));
    let row_runtime = execution.row_mode.open_runtime(
        execution.state,
        crate::field_plan::RelationalFieldPlan::all_columns(input.table, distance_ordinal),
        resources.limits.row_read,
        resources.limits.hydration,
        &row_task,
    )?;
    let mut source = CandidateRows {
        select,
        parameters,
        input,
        schema: &schema,
        qualifier,
        row_runtime: &row_runtime,
        resources,
        ledger,
        score,
    };
    let offset = usize::try_from(bind_bound(select.offset, parameters, "OFFSET")?.unwrap_or(0))
        .map_err(|_| HawDBError::Semantic("SQL OFFSET is too large".into()))?;
    let limit = bind_bound(select.limit, parameters, "LIMIT")?
        .map(usize::try_from)
        .transpose()
        .map_err(|_| HawDBError::Semantic("SQL LIMIT is too large".into()))?
        .unwrap_or(usize::MAX);
    let result_budget =
        hawdb_executor::memory::enforced_result_memory_budget(resources.execution_memory, task)?;
    let result_account = ledger.account(
        QueryMemoryClass::BlockingState,
        "VectorIndexSqlScan result",
        result_budget,
    );
    let mut result_memory = result_account.reserve(0)?;
    let mut rows = Vec::new();
    let mut payload_bytes = 0usize;
    let mut skipped = 0usize;
    let mut emit = |batch: BindingBatch| {
        for mut row in batch {
            if select.order_by.is_empty() && skipped < offset {
                skipped += 1;
                continue;
            }
            if rows.len() >= limit {
                return Ok(BatchControl::Stop);
            }
            strip_relational_sort_columns(&mut row.values);
            let bytes = hawdb_executor::binding::binding_memory_bytes(&row)
                .saturating_add(std::mem::size_of::<Row>().saturating_mul(4));
            result_memory.grow(bytes)?;
            push_relational_output(row.values, &mut rows, &mut payload_bytes, resources.limits)?;
        }
        Ok(BatchControl::Continue)
    };
    if limit != 0 {
        if select.order_by.is_empty() {
            source.execute(&(), ExecutionLimit::unlimited(), &mut emit)?;
        } else {
            execute_relational_order(
                &(),
                &mut source,
                select,
                offset,
                limit.min(resources.limits.max_output_rows.saturating_add(1)),
                &Catalog::default(),
                resources.execution_memory,
                ledger,
                task,
                &RelationalBlockingObserver::default(),
                &mut emit,
            )?;
        }
    }
    hawdb_executor::pipeline::runtime_checkpoint(task)?;
    Ok(VectorIndexSqlRows {
        rows,
        row_execution: row_runtime.evidence(),
        output_payload_bytes: payload_bytes,
        _memory: result_memory,
    })
}

struct CandidateRows<'a> {
    select: &'a SelectStatement,
    parameters: &'a [Value],
    input: VectorIndexInput<'a>,
    schema: &'a RelationalTableSchema,
    qualifier: &'a str,
    row_runtime: &'a RelationalRowRuntime<'a>,
    resources: RelationalQueryResourceContext<'a>,
    ledger: &'a QueryMemoryLedger,
    score: CandidateScore,
}

impl BindingBatchSource<()> for CandidateRows<'_> {
    fn execute(
        &mut self,
        _: &(),
        _: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let task = self.resources.task_context;
        let memory = self.resources.execution_memory;
        let account = self.ledger.account(
            QueryMemoryClass::PipelineBatch,
            "VectorIndexSqlScan source",
            memory.batch_payload_bytes,
        );
        let mut batch = AccountedBindingBatch::with_ledger(
            "VectorIndexSqlScan output",
            memory.batch_rows.get(),
            memory.batch_payload_bytes,
            self.ledger,
        );
        let mut accepted = 0usize;
        for ordinal in 0..self.input.candidates.row_count() {
            if self
                .score
                .max_accepted
                .is_some_and(|maximum| accepted >= maximum)
            {
                break;
            }
            hawdb_executor::pipeline::runtime_checkpoint(task)?;
            let (identity, distance) = self.input.candidates.row(ordinal)?;
            let hex = identity
                .strip_prefix("r:")
                .filter(|hex| hex.len().is_multiple_of(2))
                .ok_or_else(|| {
                    HawDBError::StorageIntegrity(
                        "vector index has an invalid SQL row identity".into(),
                    )
                })?;
            let _key_memory = account.reserve(hex.len().saturating_mul(16).saturating_add(4096))?;
            let bytes = hex
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| {
                    let digit = |b: u8| (b as char).to_digit(16).map(|n| n as u8);
                    digit(pair[0])
                        .zip(digit(pair[1]))
                        .map(|(a, b)| a * 16 + b)
                        .ok_or_else(|| {
                            HawDBError::StorageIntegrity(
                                "invalid vector index primary-key encoding".into(),
                            )
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            let key = decode_relational_primary_key(&bytes)
                .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
            if !(self.score.valid)(distance) {
                return Err(HawDBError::StorageIntegrity(self.score.invalid.into()));
            }
            let (row, _point_allocation) = self
                .row_runtime
                .read_point_admitted(self.input.table, &key)?;
            let row = row.ok_or_else(|| HawDBError::StorageIntegrity(self.score.absent.into()))?;
            // The decoded row remains admitted while adding the virtual column.
            // Projection and sort copies are admitted per expression below.
            let _row_memory =
                account.reserve(row.resident_bytes().saturating_mul(2).saturating_add(8192))?;
            let row = row.with_vector_distance(self.schema.columns.len() - 1, distance);
            let binding = BoundRow {
                bindings: vec![Binding {
                    binding: BindingId::new(0),
                    table: self.input.table,
                    qualifier: self.qualifier,
                    schema: self.schema,
                    row: Some(row),
                }],
            };
            let _projection_memory =
                projection_memory::admit(self.select, &binding, self.parameters, &account)?;
            if let Some(predicate) = residual_selection(self.select)
                && predicate_truth(predicate, &binding, self.parameters)? != Some(true)
            {
                continue;
            }
            accepted += 1;
            let mut projected =
                project_bound_row(&binding, &self.select.projection, self.parameters)?;
            add_relational_order_keys(self.select, &binding, &mut projected)?;
            if batch.push(ExecutorBinding::values(projected), emit)? == BatchControl::Stop {
                return Ok(BatchControl::Stop);
            }
        }
        batch.emit(emit)
    }
}

/// Compose storage allocation ownership with the query's shared ledger. The
/// original controller still owns process admission, cancellation and closure.
#[derive(Debug)]
struct CanonicalPointMemory {
    account: hawdb_executor::QueryMemoryAccount,
    original_task: hawdb_core::RuntimeTaskContext,
}

#[derive(Debug)]
struct CanonicalPointPermit {
    lease: QueryMemoryLease,
    _runtime: Option<Box<dyn hawdb_core::RuntimeMemoryPermit>>,
}

impl hawdb_core::RuntimeMemoryPermit for CanonicalPointPermit {
    fn bytes(&self) -> u64 {
        self.lease.bytes() as u64
    }
}

impl hawdb_core::RuntimeMemoryController for CanonicalPointMemory {
    fn reserve(
        &self,
        bytes: u64,
        ceiling: u64,
    ) -> std::result::Result<Box<dyn hawdb_core::RuntimeMemoryPermit>, hawdb_core::RuntimeMemoryError>
    {
        let available = (self.account.available_bytes() as u64).min(ceiling);
        let denied = || hawdb_core::RuntimeMemoryError::ReservationExceeded {
            requested_bytes: bytes,
            available_bytes: available,
        };
        if bytes > available {
            return Err(denied());
        }
        let lease = self
            .account
            .reserve(usize::try_from(bytes).map_err(|_| denied())?)
            .map_err(|_| denied())?;
        let runtime = self.original_task.reserve_working_memory(bytes)?;
        Ok(Box::new(CanonicalPointPermit {
            lease,
            _runtime: runtime,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hawdb_core::{RuntimeMemoryController, RuntimeTaskContext};

    #[test]
    fn vector_index_canonical_point_retains_memory_and_refuses_before_wide_copy() {
        let mut state = RelationalState::default();
        for (sql, parameters) in [
            (
                "CREATE TABLE docs (id BIGINT PRIMARY KEY, body TEXT)",
                vec![],
            ),
            (
                "INSERT INTO docs (id, body) VALUES ($1, $2)",
                vec![Value::Int(1), Value::String("x".repeat(256 * 1024))],
            ),
        ] {
            let transaction =
                crate::compile_relational_statement_sql(sql, &parameters, &state).unwrap();
            state = state
                .stage_transaction(transaction, Default::default(), Default::default())
                .unwrap();
        }
        for capacity in [128 * 1024, 1024 * 1024] {
            let ledger = QueryMemoryLedger::new(NonZeroUsize::new(capacity).unwrap());
            let controller = std::sync::Arc::new(CanonicalPointMemory {
                account: ledger.account(
                    QueryMemoryClass::PipelineBatch,
                    "point test",
                    NonZeroUsize::new(capacity).unwrap(),
                ),
                original_task: RuntimeTaskContext::default(),
            });
            let task = RuntimeTaskContext::default().with_memory_controller(controller.clone());
            let runtime = RelationalRowRuntime::new(
                &state,
                None,
                None,
                crate::field_plan::RelationalFieldPlan::all_columns("docs", 2),
                Default::default(),
                Default::default(),
                &task,
            );
            let result = runtime.read_point_admitted(
                "docs",
                &hawdb_storage::relational::RelationalKey(vec![
                    hawdb_storage::relational::RelationalValue::BigInt(1),
                ]),
            );
            if capacity == 128 * 1024 {
                assert!(
                    matches!(result, Err(HawDBError::ResourceBudgetExceeded(_))),
                    "{result:?}"
                );
                assert!(ledger.snapshot().peak_bytes < 256 * 1024);
            } else {
                let (row, memory) = result.unwrap();
                assert!(row.unwrap().resident_bytes() >= 256 * 1024);
                assert!(ledger.snapshot().used_bytes >= 256 * 1024);
                assert!(controller
                    .reserve(capacity as u64, capacity as u64)
                    .is_err());
                drop(memory);
            }
            assert_eq!(ledger.snapshot().used_bytes, 0);
        }
    }
}
