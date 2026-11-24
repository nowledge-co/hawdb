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

use super::{
    bind_bound, common_type, execute_query_typed, plan_values, relational_to_value, retained_bytes,
    select_output_types, size_of, validate_select_relations, Expr, ExprKind, HawDBError,
    OperatorMemoryTracker, QueryMemoryClass, QueryMemoryLedger, QueryRows, RelationalQueryOutput,
    RelationalQueryReadModes, RelationalQueryResourceContext, RelationalQueryStoreReader,
    RelationalScalarType, RelationalSqlStageTimings, RelationalState, RelationalValue, Result,
    SelectProjection, SelectStatement, SqlBound, SqlCommonTableExpressionBody, SqlQueryBody,
    SqlQueryStatement, SqlSubqueryPredicate, SqlValue, TypedOutput, Value,
};
use crate::subquery_scope::{join_on_scope, CteScope, RelationColumns};
use std::collections::{HashMap, HashSet};

mod names;

struct SubqueryEvaluator<'a, R: RelationalQueryStoreReader> {
    parameters: &'a [Value],
    state: &'a RelationalState,
    read_modes: RelationalQueryReadModes<'a, R>,
    resources: RelationalQueryResourceContext<'a>,
    work: SubqueryWork<'a>,
    memory: OperatorMemoryTracker,
    // Keys identify syntax nodes in the statement's predicate tree. The tree
    // lives for the whole evaluation, and the keys are never dereferenced.
    uncorrelated_in: HashMap<*const SqlQueryStatement, InRows>,
}

impl<'a, R: RelationalQueryStoreReader> SubqueryEvaluator<'a, R> {
    fn evaluate(
        &mut self,
        predicate: &SqlSubqueryPredicate,
        row: &super::super::BoundRow<'_>,
    ) -> Result<Option<bool>> {
        match predicate {
            SqlSubqueryPredicate::Scalar(predicate) => {
                super::super::predicate_truth(predicate, row, self.parameters)
            }
            SqlSubqueryPredicate::And(left, right) => match self.evaluate(left, row)? {
                Some(false) => Ok(Some(false)),
                Some(true) => self.evaluate(right, row),
                None => match self.evaluate(right, row)? {
                    Some(false) => Ok(Some(false)),
                    Some(true) | None => Ok(None),
                },
            },
            SqlSubqueryPredicate::Or(left, right) => match self.evaluate(left, row)? {
                Some(true) => Ok(Some(true)),
                Some(false) => self.evaluate(right, row),
                None => match self.evaluate(right, row)? {
                    Some(true) => Ok(Some(true)),
                    Some(false) | None => Ok(None),
                },
            },
            SqlSubqueryPredicate::Not(predicate) => {
                Ok(self.evaluate(predicate, row)?.map(|value| !value))
            }
            SqlSubqueryPredicate::Exists { query } => {
                let mut query = query.as_ref().clone();
                bind_outer_query_references(&mut query, row, self.state)?;
                prepare_existence_query(&mut query, self.parameters, self.state)?;
                self.execute(query, true)
                    .map(|output| Some(!output.output.rows.is_empty()))
            }
            SqlSubqueryPredicate::In {
                left,
                query,
                negated,
            } => {
                let result = self.in_truth(left, query, row)?;
                Ok(result.map(|value| value != *negated))
            }
        }
    }

    fn in_truth(
        &mut self,
        left: &[Expr],
        query: &SqlQueryStatement,
        row: &super::super::BoundRow<'_>,
    ) -> Result<Option<bool>> {
        let key = std::ptr::from_ref(query);
        if let Some(cached) = self.uncorrelated_in.get_mut(&key) {
            return cached.matches(left, row, self.parameters, &mut self.work, &mut self.memory);
        }

        let mut bound = query.clone();
        bind_outer_query_references(&mut bound, row, self.state)?;
        let reusable = bound == *query && reusable_membership_query(query);
        let output = self.execute(bound, false)?;
        let rows = output.output.rows;
        if rows.schema().len() != left.len() {
            return Err(HawDBError::Semantic(format!(
                "IN subquery returns {} columns but the left row has arity {}",
                rows.schema().len(),
                left.len()
            )));
        }
        let row_bytes = retained_bytes(&rows);
        self.memory.try_charge(row_bytes)?;
        if !reusable
            || !try_charge_cache(
                &mut self.memory,
                4 * (size_of::<(*const SqlQueryStatement, InRows)>() + 1),
            )?
        {
            let result = scan_in_rows(&rows, left, row, self.parameters, &mut self.work);
            self.memory.release(row_bytes);
            return result;
        }

        let hash_type = if rows.len() > 1 {
            hash_membership_type(left, &output.types, row)?
        } else {
            None
        };
        let mut rows = InRows::Scan {
            rows,
            hash_type,
            extra_comparisons: 0,
        };
        self.uncorrelated_in
            .try_reserve(1)
            .map_err(|_| HawDBError::Execution("could not reserve SQL subquery cache".into()))?;
        let result = rows.matches(left, row, self.parameters, &mut self.work, &mut self.memory)?;
        self.uncorrelated_in.insert(key, rows);
        Ok(result)
    }

    fn execute(&mut self, query: SqlQueryStatement, exists: bool) -> Result<TypedOutput> {
        self.work.charge(1)?;
        let mut resources = self.resources;
        resources.limits.max_output_rows = if exists {
            1
        } else {
            resources.limits.max_intermediate_rows
        };
        resources.limits.max_output_payload_bytes =
            resources.execution_memory.query_memory_bytes.get();
        resources.limits.max_candidate_work = self.work.remaining().max(1);
        let output = execute_query_typed(
            query,
            self.parameters,
            self.state,
            self.read_modes,
            resources,
            RelationalSqlStageTimings::default(),
        )?;
        self.work.charge(
            output
                .output
                .intermediate_rows
                .max(output.output.rows.len()),
        )?;
        Ok(output)
    }
}

fn try_charge_cache(memory: &mut OperatorMemoryTracker, bytes: usize) -> Result<bool> {
    match memory.try_charge(bytes) {
        Ok(()) => Ok(true),
        Err(HawDBError::ResourceBudgetExceeded(_)) => Ok(false),
        Err(error) => Err(error),
    }
}

struct SubqueryWork<'a> {
    limit: usize,
    used: usize,
    task: Option<&'a hawdb_core::RuntimeTaskContext>,
}

impl SubqueryWork<'_> {
    fn remaining(&self) -> usize {
        self.limit - self.used
    }

    fn charge(&mut self, requested: usize) -> Result<()> {
        hawdb_executor::pipeline::runtime_checkpoint(self.task)?;
        let next = self
            .used
            .checked_add(requested)
            .ok_or_else(|| HawDBError::Execution("subquery work count overflow".into()))?;
        if next > self.limit {
            return Err(super::super::expression::work_budget_exceeded(
                "max_candidate_work",
                self.used,
                requested,
                self.limit,
            ));
        }
        self.used = next;
        Ok(())
    }
}

enum InRows {
    Hash {
        keys: HashSet<RelationalValue>,
        has_null: bool,
    },
    Scan {
        rows: QueryRows,
        hash_type: Option<RelationalScalarType>,
        extra_comparisons: usize,
    },
}

impl InRows {
    fn matches(
        &mut self,
        left: &[Expr],
        row: &super::super::BoundRow<'_>,
        parameters: &[Value],
        work: &mut SubqueryWork<'_>,
        memory: &mut OperatorMemoryTracker,
    ) -> Result<Option<bool>> {
        if let Self::Scan {
            rows,
            hash_type,
            extra_comparisons,
        } = self
            && let Some(kind) = *hash_type
            && *extra_comparisons >= rows.len()
        {
            // Each probe would cost at least one lookup with hashing. Build
            // only after the comparisons beyond that baseline reach the cost
            // of visiting every RHS row once. The first probe always scans.
            if rows.len() < work.remaining()
                && let Some(hashed) = Self::try_build_hash(rows, kind, work, memory)?
            {
                *self = hashed;
            } else {
                // Remaining work only decreases. Keep the admitted scan if
                // either budget cannot cover promotion.
                *hash_type = None;
            }
        }

        match self {
            Self::Hash { keys, has_null } => {
                work.charge(1)?;
                if keys.is_empty() && !*has_null {
                    return Ok(Some(false));
                }
                let value =
                    super::super::expression::resolve_column(row, left[0].require_column()?)?;
                match value {
                    RelationalValue::Null => return Ok(None),
                    RelationalValue::Overflow(_) => {
                        return Err(HawDBError::Execution(
                            "relational filter or join requires overflow hydration before qualification"
                                .into(),
                        ));
                    }
                    _ => {}
                }
                Ok(if keys.contains(value) {
                    Some(true)
                } else if *has_null {
                    None
                } else {
                    Some(false)
                })
            }
            Self::Scan {
                rows,
                hash_type,
                extra_comparisons,
            } => {
                let before = work.used;
                let result = scan_in_rows(rows, left, row, parameters, work)?;
                if hash_type.is_some() {
                    *extra_comparisons += (work.used - before).saturating_sub(1);
                }
                Ok(result)
            }
        }
    }

    fn try_build_hash(
        rows: &QueryRows,
        kind: RelationalScalarType,
        work: &mut SubqueryWork<'_>,
        memory: &mut OperatorMemoryTracker,
    ) -> Result<Option<Self>> {
        let row_bytes = retained_bytes(rows);
        let hash_bytes = row_bytes.saturating_add(
            rows.len()
                .saturating_mul(4 * (size_of::<RelationalValue>() + 1)),
        );
        if !try_charge_cache(memory, hash_bytes)? {
            return Ok(None);
        }
        let mut keys = HashSet::new();
        keys.try_reserve(rows.len())
            .map_err(|_| HawDBError::Execution("could not reserve SQL IN hash table".into()))?;
        let mut has_null = false;
        work.charge(rows.len())?;
        for values in rows.value_rows() {
            hawdb_executor::pipeline::runtime_checkpoint(work.task)?;
            let value = crate::query_value::value_to_relational_as(values[0].clone(), kind)?;
            if value == RelationalValue::Null {
                has_null = true;
            } else {
                keys.insert(value);
            }
        }
        memory.release(row_bytes);
        Ok(Some(Self::Hash { keys, has_null }))
    }
}

fn hash_membership_type(
    left: &[Expr],
    right_types: &[Option<RelationalScalarType>],
    row: &super::super::BoundRow<'_>,
) -> Result<Option<RelationalScalarType>> {
    let ([expression], [Some(right)]) = (left, right_types) else {
        return Ok(None);
    };
    let Some(column) = expression.as_column() else {
        return Ok(None);
    };
    let (_, left) = super::super::expression::resolve_column_with_type(row, column)?;
    Ok((left == *right
        && matches!(
            left,
            RelationalScalarType::BigInt
                | RelationalScalarType::Boolean
                | RelationalScalarType::Text
                | RelationalScalarType::Bytea
                | RelationalScalarType::Uuid
        ))
    .then_some(left))
}

fn scan_in_rows(
    rows: &QueryRows,
    left: &[Expr],
    row: &super::super::BoundRow<'_>,
    parameters: &[Value],
    work: &mut SubqueryWork<'_>,
) -> Result<Option<bool>> {
    let mut result = Some(false);
    for candidate in rows.value_rows() {
        work.charge(1)?;
        match crate::predicate::compare_subquery_row(left, candidate, parameters, &|column| {
            super::super::expression::resolve_column_with_type(row, column)
        })? {
            Some(true) => return Ok(Some(true)),
            Some(false) => {}
            None => result = None,
        }
    }
    Ok(result)
}

/// Cache simple deterministic queries after outer binding confirms that they
/// read no outer values. Function-bearing and nested predicate bodies stay
/// on the existing execution path, including volatile projections.
fn reusable_membership_query(query: &SqlQueryStatement) -> bool {
    fn reusable_body(body: &SqlQueryBody) -> bool {
        match body {
            SqlQueryBody::Values(rows) => rows.iter().flatten().all(reusable_expression),
            SqlQueryBody::Nested(query) => reusable_membership_query(query),
            SqlQueryBody::SetOperation { left, right, .. } => {
                reusable_body(left) && reusable_body(right)
            }
            SqlQueryBody::Select(select) => {
                select.table_function.is_none()
                    && select.subquery_predicate.is_none()
                    && select.projection.iter().all(|item| match item {
                        SelectProjection::Wildcard => true,
                        SelectProjection::Expression { expression, .. } => {
                            reusable_expression(expression)
                        }
                    })
                    && select
                        .joins
                        .iter()
                        .all(|join| reusable_expression(&join.on))
                    && select.selection.iter().all(reusable_expression)
                    && select.having.iter().all(reusable_expression)
            }
        }
    }
    fn reusable_expression(expression: &Expr) -> bool {
        let mut reusable = true;
        expression.visit(&mut |expression| {
            reusable &= !matches!(expression.kind, ExprKind::Function { .. });
        });
        reusable
    }
    query.with.is_empty()
        && query.lock_strength.is_none()
        && query
            .order_by
            .iter()
            .all(|item| reusable_expression(&item.expression))
        && reusable_body(&query.body)
}

fn prepare_existence_query(
    query: &mut SqlQueryStatement,
    parameters: &[Value],
    state: &RelationalState,
) -> Result<()> {
    query.limit = Some(SqlBound::Literal(
        bind_bound(query.limit, parameters, "LIMIT")?
            .unwrap_or(1)
            .min(1),
    ));
    let offset = bind_bound(query.offset, parameters, "OFFSET")?;
    query.offset = offset.map(SqlBound::Literal);

    // A simple SELECT can stop at its first surviving row. Set operations,
    // grouping and DISTINCT with an offset may depend on projected values.
    if query.with.is_empty()
        && let SqlQueryBody::Select(select) = &mut query.body
        && select.table_function.is_none()
        && select.group_by.is_empty()
        && select.having.is_none()
        && (!select.distinct || offset.unwrap_or_default() == 0)
        && !select
            .projection
            .iter()
            .any(crate::field_plan::projection_contains_aggregate)
    {
        use super::super::expression::{
            resolve_projection_column_type, validate_projection_columns,
            validate_scalar_expressions,
        };
        use crate::field_plan::{resolve_relational_order_target, RelationalOrderTarget};

        validate_select_relations(select, state)?;
        validate_projection_columns(select, state)?;
        validate_scalar_expressions(select, parameters, state)?;
        for item in &query.order_by {
            match resolve_relational_order_target(select, item)? {
                RelationalOrderTarget::InputColumn(column)
                | RelationalOrderTarget::ProjectionColumn { column, .. } => {
                    resolve_projection_column_type(select, state, column)?;
                }
                RelationalOrderTarget::ProjectionExpression { .. } => {}
            }
        }
        // The order of surviving rows does not affect the existence of a row
        // after OFFSET. The DISTINCT case that can affect that count stayed
        // on the normal projection and sorting path above.
        query.order_by.clear();
        select.distinct = false;
        select.projection = vec![SelectProjection::Expression {
            expression: Expr::value(SqlValue::Literal(Value::Int(1))),
            alias: None,
        }];
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn execute_select_with_subqueries<'a, R: RelationalQueryStoreReader>(
    select: SelectStatement,
    parameters: &'a [Value],
    state: &'a RelationalState,
    read_modes: RelationalQueryReadModes<'a, R>,
    resources: RelationalQueryResourceContext<'a>,
    stage_timings: RelationalSqlStageTimings,
) -> Result<RelationalQueryOutput> {
    let predicate = select
        .subquery_predicate
        .clone()
        .expect("subquery execution requires a subquery predicate");
    names::validate(&select, state)?;
    validate_subquery_shapes(&predicate, &select, parameters, state, &CteScope::default())?;
    let ledger = QueryMemoryLedger::for_query(resources.execution_memory, resources.task_context)?;
    let tracker = OperatorMemoryTracker::with_account(
        resources.execution_memory.blocking_operator_bytes,
        ledger.account(
            QueryMemoryClass::BlockingState,
            "relational subquery predicate",
            resources.execution_memory.blocking_operator_bytes,
        ),
    );
    let mut evaluator = SubqueryEvaluator {
        parameters,
        state,
        read_modes,
        resources,
        work: SubqueryWork {
            limit: resources.limits.max_candidate_work,
            used: 0,
            task: resources.task_context,
        },
        memory: tracker,
        uncorrelated_in: HashMap::new(),
    };
    let mut output = {
        let mut evaluate = |row: &super::super::BoundRow<'_>| evaluator.evaluate(&predicate, row);
        let filter = super::super::RelationalSubqueryFilter::new(&mut evaluate);
        super::super::execute_select_statement_with_filter(
            select,
            parameters,
            state,
            read_modes,
            resources,
            stage_timings,
            Some(&filter),
        )?
    };
    let strategy = if evaluator
        .uncorrelated_in
        .values()
        .any(|rows| matches!(rows, InRows::Hash { .. }))
    {
        "SubqueryPredicateExec (hashed membership)"
    } else {
        "SubqueryPredicateExec"
    };
    output.access_path.name = format!("{strategy} -> {}", output.access_path.name);
    output
        .blocking_operator_memory_reports
        .push(hawdb_executor::blocking::in_memory_report(
            "SubqueryPredicateExec",
            &evaluator.memory,
            evaluator.memory.peak_bytes,
            evaluator.work.used,
            resources.execution_memory,
        ));
    Ok(output)
}

fn validate_subquery_shapes(
    predicate: &SqlSubqueryPredicate,
    owner: &SelectStatement,
    parameters: &[Value],
    state: &RelationalState,
    scope: &CteScope,
) -> Result<()> {
    match predicate {
        SqlSubqueryPredicate::Scalar(_) => Ok(()),
        SqlSubqueryPredicate::And(left, right) | SqlSubqueryPredicate::Or(left, right) => {
            validate_subquery_shapes(left, owner, parameters, state, scope)?;
            validate_subquery_shapes(right, owner, parameters, state, scope)
        }
        SqlSubqueryPredicate::Not(predicate) => {
            validate_subquery_shapes(predicate, owner, parameters, state, scope)
        }
        SqlSubqueryPredicate::Exists { query } => {
            validate_nested_subquery_shapes(query, parameters, state, scope)
        }
        SqlSubqueryPredicate::In { left, query, .. } => {
            if let Some(arity) = scope
                .query_columns(query, state)?
                .map(|columns| columns.len())
                && arity != left.len()
            {
                return Err(HawDBError::Semantic(format!(
                    "IN subquery returns {arity} columns but the left row has arity {}",
                    left.len()
                )));
            }
            if catalog_relations_only(owner, state, scope)
                && let Some(right_types) = static_query_types(query, parameters, state, scope)?
            {
                for (column, (left, right)) in left.iter().zip(right_types).enumerate() {
                    let left = super::super::expression::infer_coalesce_scalar_type(
                        left, owner, parameters, state,
                    )?;
                    if let (Some(left), Some(right)) = (left, right)
                        && !subquery_types_comparable(left, right)
                    {
                        return Err(HawDBError::Semantic(format!(
                            "IN subquery column {} has incompatible types {left:?} and {right:?}",
                            column + 1
                        )));
                    }
                }
            }
            validate_nested_subquery_shapes(query, parameters, state, scope)
        }
    }
}

fn validate_nested_subquery_shapes(
    query: &SqlQueryStatement,
    parameters: &[Value],
    state: &RelationalState,
    scope: &CteScope,
) -> Result<()> {
    let mut local = scope.clone();
    for cte in &query.with {
        local.seed_recursive(query.recursive, cte, state)?;
        if let SqlCommonTableExpressionBody::Query(child) = &cte.body {
            validate_nested_subquery_shapes(child, parameters, state, &local)?;
        }
        local.define(cte, state)?;
    }
    validate_nested_body_shapes(&query.body, parameters, state, &local)
}

fn validate_nested_body_shapes(
    body: &SqlQueryBody,
    parameters: &[Value],
    state: &RelationalState,
    scope: &CteScope,
) -> Result<()> {
    match body {
        SqlQueryBody::Select(select) => select
            .subquery_predicate
            .as_ref()
            .map_or(Ok(()), |predicate| {
                validate_subquery_shapes(predicate, select, parameters, state, scope)
            }),
        SqlQueryBody::Values(_) => Ok(()),
        SqlQueryBody::Nested(query) => {
            validate_nested_subquery_shapes(query, parameters, state, scope)
        }
        SqlQueryBody::SetOperation { left, right, .. } => {
            validate_nested_body_shapes(left, parameters, state, scope)?;
            validate_nested_body_shapes(right, parameters, state, scope)
        }
    }
}

fn catalog_relations_only(
    select: &SelectStatement,
    state: &RelationalState,
    scope: &CteScope,
) -> bool {
    select
        .from
        .iter()
        .chain(select.joins.iter().map(|join| &join.table))
        .all(|table| !scope.contains(&table.name) && state.table_schema(&table.name).is_some())
}

fn static_query_types(
    query: &SqlQueryStatement,
    parameters: &[Value],
    state: &RelationalState,
    scope: &CteScope,
) -> Result<Option<Vec<Option<RelationalScalarType>>>> {
    if !query.with.is_empty() {
        return Ok(None);
    }
    static_body_types(&query.body, parameters, state, scope)
}

fn static_body_types(
    body: &SqlQueryBody,
    parameters: &[Value],
    state: &RelationalState,
    scope: &CteScope,
) -> Result<Option<Vec<Option<RelationalScalarType>>>> {
    match body {
        SqlQueryBody::Select(select) => {
            if !catalog_relations_only(select, state, scope) {
                return Ok(None);
            }
            select_output_types(select, parameters, state).map(Some)
        }
        SqlQueryBody::Values(rows) => {
            plan_values(rows, parameters, state).map(|plan| Some(plan.types))
        }
        SqlQueryBody::Nested(query) => static_query_types(query, parameters, state, scope),
        SqlQueryBody::SetOperation { left, right, .. } => {
            let Some(left) = static_body_types(left, parameters, state, scope)? else {
                return Ok(None);
            };
            let Some(right) = static_body_types(right, parameters, state, scope)? else {
                return Ok(None);
            };
            if left.len() != right.len() {
                return Err(HawDBError::Semantic(format!(
                    "set-operation inputs have arity {} and {}",
                    left.len(),
                    right.len()
                )));
            }
            left.into_iter()
                .zip(right)
                .enumerate()
                .map(|(column, (left, right))| common_type(left, right, column + 1))
                .collect::<Result<Vec<_>>>()
                .map(Some)
        }
    }
}

fn subquery_types_comparable(left: RelationalScalarType, right: RelationalScalarType) -> bool {
    use RelationalScalarType::{BigInt, Decimal, DoublePrecision, Timestamp, TimestampTz};
    left == right
        || matches!(
            (left, right),
            (
                BigInt | Decimal(_) | DoublePrecision,
                BigInt | Decimal(_) | DoublePrecision
            ) | (Timestamp(_), Timestamp(_))
                | (TimestampTz(_), TimestampTz(_))
        )
}

fn bind_outer_query_references(
    query: &mut SqlQueryStatement,
    row: &super::super::BoundRow<'_>,
    state: &RelationalState,
) -> Result<()> {
    bind_outer_query_references_scoped(query, row, state, &CteScope::default(), &[])
}

fn bind_outer_query_references_scoped(
    query: &mut SqlQueryStatement,
    row: &super::super::BoundRow<'_>,
    state: &RelationalState,
    scope: &CteScope,
    shadowing: &[LocalRelation<'_>],
) -> Result<()> {
    if query.with.is_empty() {
        return bind_outer_body_references(&mut query.body, row, state, scope, shadowing);
    }
    let mut local = scope.clone();
    for cte in &mut query.with {
        local.seed_recursive(query.recursive, cte, state)?;
        if let SqlCommonTableExpressionBody::Query(child) = &mut cte.body {
            bind_outer_query_references_scoped(child, row, state, &local, shadowing)?;
        }
        local.define(cte, state)?;
    }
    bind_outer_body_references(&mut query.body, row, state, &local, shadowing)
}

fn bind_outer_body_references(
    body: &mut SqlQueryBody,
    row: &super::super::BoundRow<'_>,
    state: &RelationalState,
    scope: &CteScope,
    shadowing: &[LocalRelation<'_>],
) -> Result<()> {
    match body {
        SqlQueryBody::Select(select) => {
            bind_outer_select_references(select, row, state, scope, shadowing)
        }
        SqlQueryBody::Values(rows) => {
            for expression in rows.iter_mut().flatten() {
                bind_outer_expression(expression, shadowing, row)?;
            }
            Ok(())
        }
        SqlQueryBody::Nested(query) => {
            bind_outer_query_references_scoped(query, row, state, scope, shadowing)
        }
        SqlQueryBody::SetOperation { left, right, .. } => {
            bind_outer_body_references(left, row, state, scope, shadowing)?;
            bind_outer_body_references(right, row, state, scope, shadowing)
        }
    }
}

fn bind_outer_select_references(
    select: &mut SelectStatement,
    row: &super::super::BoundRow<'_>,
    state: &RelationalState,
    scope: &CteScope,
    shadowing: &[LocalRelation<'_>],
) -> Result<()> {
    let current = select
        .from
        .iter()
        .map(|table| {
            (
                table.name.clone(),
                select
                    .from_alias
                    .clone()
                    .unwrap_or_else(|| table.name.clone()),
                scope.columns(state, &table.name),
            )
        })
        .chain(select.joins.iter().map(|join| {
            (
                join.table.name.clone(),
                join.alias
                    .clone()
                    .unwrap_or_else(|| join.table.name.clone()),
                scope.columns(state, &join.table.name),
            )
        }))
        .collect::<Vec<_>>();
    let mut local = shadowing.to_vec();
    for (ordinal, join) in select.joins.iter_mut().enumerate() {
        local.extend_from_slice(join_on_scope(&current, join.on_scope_start, ordinal)?);
        bind_outer_expression(&mut join.on, &local, row)?;
        local.truncate(shadowing.len());
    }
    local.extend(current);
    for projection in &mut select.projection {
        if let SelectProjection::Expression { expression, alias } = projection {
            let column_name = expression.as_column().map(|column| column.name.clone());
            bind_outer_expression(expression, &local, row)?;
            if alias.is_none() && expression.as_column().is_none() {
                *alias = column_name;
            }
        }
    }
    if let Some(selection) = &mut select.selection {
        bind_outer_expression(selection, &local, row)?;
    }
    if let Some(predicate) = &mut select.subquery_predicate {
        bind_outer_predicate_expressions(predicate, &local, row, state, scope)?;
    }
    if let Some(having) = &mut select.having {
        bind_outer_expression(having, &local, row)?;
    }
    for order in &mut select.order_by {
        bind_outer_expression(&mut order.expression, &local, row)?;
    }
    Ok(())
}

type LocalRelation<'a> = (String, String, RelationColumns<'a>);

fn bind_outer_predicate_expressions(
    predicate: &mut SqlSubqueryPredicate,
    local: &[LocalRelation<'_>],
    row: &super::super::BoundRow<'_>,
    state: &RelationalState,
    scope: &CteScope,
) -> Result<()> {
    match predicate {
        SqlSubqueryPredicate::Scalar(expression) => bind_outer_expression(expression, local, row),
        SqlSubqueryPredicate::And(left, right) | SqlSubqueryPredicate::Or(left, right) => {
            bind_outer_predicate_expressions(left, local, row, state, scope)?;
            bind_outer_predicate_expressions(right, local, row, state, scope)
        }
        SqlSubqueryPredicate::Not(predicate) => {
            bind_outer_predicate_expressions(predicate, local, row, state, scope)
        }
        SqlSubqueryPredicate::Exists { query } => {
            bind_outer_query_references_scoped(query, row, state, scope, local)
        }
        SqlSubqueryPredicate::In { left, query, .. } => {
            for expression in left {
                bind_outer_expression(expression, local, row)?;
            }
            bind_outer_query_references_scoped(query, row, state, scope, local)
        }
    }
}

fn bind_outer_expression(
    expression: &mut Expr,
    local: &[LocalRelation<'_>],
    row: &super::super::BoundRow<'_>,
) -> Result<()> {
    expression.try_visit_mut(&mut |expression| {
        let ExprKind::Column(column) = &expression.kind else {
            return Ok(());
        };
        if local
            .iter()
            .any(|(table, alias, columns)| columns.matches(table, alias, column))
        {
            return Ok(());
        }
        let bindings = row
            .bindings
            .iter()
            .filter(|binding| {
                column.qualifier.as_deref().is_none_or(|qualifier| {
                    qualifier == binding.qualifier || qualifier == binding.table
                }) && binding.schema.column_position(&column.name).is_some()
            })
            .collect::<Vec<_>>();
        match bindings.as_slice() {
            [] => Ok(()),
            [binding] => {
                let position = binding
                    .schema
                    .column_position(&column.name)
                    .expect("matched outer column has an ordinal");
                let value = relational_to_value(binding.value(position)?)?;
                expression.kind = ExprKind::Value(SqlValue::Literal(value));
                Ok(())
            }
            _ => Err(HawDBError::Semantic(format!(
                "column {} is unknown or ambiguous",
                column.name
            ))),
        }
    })
}
