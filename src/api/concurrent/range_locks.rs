// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_storage::relational::{RelationalComparisonOp, RelationalKeyRange, RelationalPredicate};
use hawdb_storage::transaction_locks::LockNamespace;

const DISCOVERY_PAYLOAD_BYTES: usize = 1024 * 1024;
const DISCOVERY_CANDIDATE_ROWS: usize = 4096;

fn discovery_budget_exhausted(
    error: &HawDBError,
    caller: &crate::relational_sql::RelationalQueryLimits,
) -> bool {
    match error {
        HawDBError::ResourceBudgetExceeded(cause)
            if cause.resource == hawdb_core::ResourceBudgetKind::WorkUnits
                && cause.scope == hawdb_core::ResourceBudgetScope::Query
                && cause.owner == "relational SQL"
                && cause.budget == DISCOVERY_CANDIDATE_ROWS as u64 =>
        {
            match cause.budget_option {
                "max_candidate_work" => caller.max_candidate_work > DISCOVERY_CANDIDATE_ROWS,
                "max_intermediate_rows" => caller.max_intermediate_rows > DISCOVERY_CANDIDATE_ROWS,
                _ => false,
            }
        }
        HawDBError::ReadBudgetExceeded(cause) => {
            cause.resource == hawdb_core::ReadBudgetResource::PayloadBytes
                && cause.limit == DISCOVERY_PAYLOAD_BYTES
                && caller.max_output_payload_bytes > DISCOVERY_PAYLOAD_BYTES
        }
        _ => false,
    }
}

/// Keep only necessary conjuncts. An unsupported residual can narrow the result,
/// but an unsupported disjunct must never be dropped from a union.
fn necessary_predicate(
    predicate: &SqlPredicate,
    schema: &RelationalTableSchema,
    table: &SqlTableName,
    alias: Option<&str>,
    parameters: &[Value],
) -> Option<RelationalPredicate> {
    match &predicate.kind {
        ExprKind::And(left, right) => {
            let left = necessary_predicate(left, schema, table, alias, parameters);
            let right = necessary_predicate(right, schema, table, alias, parameters);
            match (left, right) {
                (Some(left), Some(right)) => {
                    Some(RelationalPredicate::And(Box::new(left), Box::new(right)))
                }
                (left, right) => left.or(right),
            }
        }
        ExprKind::Compare { left, op, right } => {
            let (column, value, op) =
                if let (Some(column), Some(value)) = (left.as_column(), right.as_value()) {
                    (column, value, *op)
                } else {
                    let column = right.as_column()?;
                    let value = left.as_value()?;
                    let op = match op {
                        SqlComparisonOp::Eq => SqlComparisonOp::Eq,
                        SqlComparisonOp::NotEq => SqlComparisonOp::NotEq,
                        SqlComparisonOp::Lt => SqlComparisonOp::Gt,
                        SqlComparisonOp::Lte => SqlComparisonOp::Gte,
                        SqlComparisonOp::Gt => SqlComparisonOp::Lt,
                        SqlComparisonOp::Gte => SqlComparisonOp::Lte,
                    };
                    (column, value, op)
                };
            if !column_matches(column, &column.name, table, alias) {
                return None;
            }
            let scalar_type = schema.columns[schema.column_position(&column.name)?].scalar_type;
            let value = bind_sql_lock_value(value, parameters, scalar_type)?;
            let op = match op {
                SqlComparisonOp::Eq => RelationalComparisonOp::Eq,
                SqlComparisonOp::NotEq => RelationalComparisonOp::NotEq,
                SqlComparisonOp::Lt => RelationalComparisonOp::Lt,
                SqlComparisonOp::Lte => RelationalComparisonOp::Lte,
                SqlComparisonOp::Gt => RelationalComparisonOp::Gt,
                SqlComparisonOp::Gte => RelationalComparisonOp::Gte,
            };
            Some(RelationalPredicate::Compare {
                column: column.name.clone(),
                op,
                value,
            })
        }
        _ => None,
    }
}

fn equality_on(predicate: &RelationalPredicate, column: &str) -> bool {
    match predicate {
        RelationalPredicate::And(left, right) => {
            equality_on(left, column) || equality_on(right, column)
        }
        RelationalPredicate::Compare {
            column: name,
            op: RelationalComparisonOp::Eq,
            value,
        } => name == column && !matches!(value, RelationalValue::Null),
        _ => false,
    }
}

fn inequality_on(predicate: &RelationalPredicate, column: &str) -> bool {
    match predicate {
        RelationalPredicate::And(left, right) => {
            inequality_on(left, column) || inequality_on(right, column)
        }
        RelationalPredicate::Compare {
            column: name,
            op,
            value,
        } => {
            name == column
                && matches!(
                    op,
                    RelationalComparisonOp::Lt
                        | RelationalComparisonOp::Lte
                        | RelationalComparisonOp::Gt
                        | RelationalComparisonOp::Gte
                )
                && !matches!(value, RelationalValue::Null)
        }
        _ => false,
    }
}

fn key_columns(schema: &RelationalTableSchema) -> Vec<Vec<String>> {
    let mut columns = vec![schema.primary_key.clone()];
    for key in schema
        .unique_constraints
        .iter()
        .chain(schema.indexes.iter().map(|index| &index.columns))
    {
        if !columns.contains(key) {
            columns.push(key.clone());
        }
    }
    columns
}

pub(super) fn target_lock_requests(
    mode: LockMode,
    table: &SqlTableName,
    alias: Option<&str>,
    selection: Option<&SqlPredicate>,
    parameters: &[Value],
    schema: &RelationalTableSchema,
) -> Vec<LockRequest> {
    let fallback = || vec![LockRequest::relational_table(mode, table.name.clone())];
    let Some(selection) = selection else {
        return fallback();
    };
    let predicate = necessary_predicate(selection, schema, table, alias, parameters);
    let mut best = None;
    for columns in key_columns(schema) {
        let Some(predicate) = &predicate else { break };
        let Ok(Some(range)) = RelationalKeyRange::from_predicate(&columns, predicate) else {
            continue;
        };
        if range.is_empty() {
            return Vec::new();
        }
        let prefix = columns
            .iter()
            .take_while(|column| equality_on(predicate, column))
            .count();
        let next_is_bounded = columns
            .get(prefix)
            .is_some_and(|column| inequality_on(predicate, column));
        let primary = columns == schema.primary_key;
        let rank = (
            primary && prefix == columns.len(),
            prefix,
            next_is_bounded,
            primary,
        );
        if best.as_ref().is_none_or(|(previous, _)| rank > *previous) {
            best = Some((
                rank,
                LockRequest::relational_range(
                    mode,
                    table.name.clone(),
                    columns,
                    range.lower,
                    range.upper,
                ),
            ));
        }
    }
    // Preserve single-key IN/OR unions, including a conservative sibling range
    // under AND. Composite bounds above share the mutation boundary algebra.
    if schema.primary_key.len() == 1
        && let Some(ranges) = single_key_ranges(selection, schema, table, alias, parameters)
        && (ranges.is_empty() || best.as_ref().is_none_or(|(rank, _)| rank.0 || rank.1 == 0))
    {
        return ranges
            .into_iter()
            .map(|(lower, upper)| {
                LockRequest::relational_range(
                    mode,
                    table.name.clone(),
                    schema.primary_key.clone(),
                    lower,
                    upper,
                )
            })
            .collect();
    }
    best.map_or_else(fallback, |(_, request)| vec![request])
}

struct DiscoverySource<'a> {
    table: &'a SqlTableName,
    alias: Option<&'a str>,
    selection: Option<&'a SqlPredicate>,
    mode: LockMode,
    write: bool,
}

fn discovery_source(statement: &SqlStatement) -> Option<DiscoverySource<'_>> {
    match statement {
        SqlStatement::Select(select) if select.joins.is_empty() => {
            let mode = match select.lock_strength? {
                SqlLockStrength::Share => LockMode::Shared,
                SqlLockStrength::Update => LockMode::Exclusive,
            };
            Some(DiscoverySource {
                table: select.from.as_ref()?,
                alias: select.from_alias.as_deref(),
                selection: select.selection.as_ref(),
                mode,
                write: false,
            })
        }
        SqlStatement::Explain(explain) if explain.analyze => discovery_source(&explain.statement),
        SqlStatement::Update(update) => Some(DiscoverySource {
            table: &update.table,
            alias: update.alias.as_deref(),
            selection: update.selection.as_ref(),
            mode: LockMode::Exclusive,
            write: true,
        }),
        SqlStatement::Delete(delete) => Some(DiscoverySource {
            table: &delete.table,
            alias: delete.alias.as_deref(),
            selection: delete.selection.as_ref(),
            mode: LockMode::Exclusive,
            write: true,
        }),
        _ => None,
    }
}

impl ConcurrentDatabaseTransaction {
    pub(super) fn acquire_sql_locks(
        &mut self,
        prepared: &crate::relational_sql::PreparedRelationalSql,
        parameters: &[Value],
        task: Option<&hawdb_core::RuntimeTaskContext>,
    ) -> Result<()> {
        loop {
            super::super::query_runtime::query_runtime_checkpoint(task)?;
            let requests = sql_lock_requests(
                prepared,
                parameters,
                &self.state.relational_state,
                &self.state.append_state,
            )?;
            if requests.is_empty() {
                return Ok(());
            }
            let epoch = self.base_commit_epoch;
            self.acquire_locks(&requests)?;
            if self.base_commit_epoch != epoch {
                // A first-statement wait may replace the complete snapshot.
                // Never reuse candidates or schema bindings from the old view.
                continue;
            }
            let additional = self.discover_sql_locks(prepared, parameters, &requests, task)?;
            if !additional.is_empty() {
                self.acquire_locks(&additional)?;
            }
            if self.base_commit_epoch == epoch {
                return Ok(());
            }
        }
    }

    fn discover_sql_locks(
        &self,
        prepared: &crate::relational_sql::PreparedRelationalSql,
        parameters: &[Value],
        guards: &[LockRequest],
        task: Option<&hawdb_core::RuntimeTaskContext>,
    ) -> Result<Vec<LockRequest>> {
        if guards.iter().any(|request| {
            matches!(
                request.target,
                LockTarget::Database | LockTarget::RelationalTable { .. }
            )
        }) {
            return Ok(Vec::new());
        }
        let Some(source) = discovery_source(prepared.statement()) else {
            return Ok(Vec::new());
        };
        let Some(schema) = self.state.relational_state.table_schema(&source.table.name) else {
            return Ok(Vec::new());
        };
        let namespaces = key_columns(schema);
        let secondary_guard = guards.iter().any(|request| matches!(&request.target,
            LockTarget::RelationalRange { namespace: LockNamespace::RelationalIndex { columns, .. }, .. }
                if columns != &schema.primary_key));
        if !secondary_guard && (!source.write || namespaces.len() == 1) {
            return Ok(Vec::new());
        }
        let requested_columns = if source.write {
            &namespaces[..]
        } else {
            &namespaces[..1]
        };
        let mut replacements = BTreeMap::new();
        if let SqlStatement::Update(update) = prepared.statement() {
            for assignment in &update.assignments {
                if !schema
                    .indexes
                    .iter()
                    .any(|index| index.columns.contains(&assignment.column))
                {
                    continue;
                }
                let crate::sql::SqlAssignmentValue::Value(value) = &assignment.value else {
                    return Ok(vec![LockRequest::relational_table(
                        source.mode,
                        source.table.name.clone(),
                    )]);
                };
                let scalar_type = schema.columns[schema
                    .column_position(&assignment.column)
                    .expect("compiled assignment column")]
                .scalar_type;
                // Protect the stored new key using assignment coercion. Query
                // boundaries may deliberately retain extra numeric precision.
                let value = crate::relational_sql::bind_relational_value(value.clone(), parameters)
                    .and_then(|value| {
                        hawdb_relational::coerce_relational_value(value, scalar_type)
                    })?;
                replacements.insert(assignment.column.clone(), value);
            }
        }
        let columns = requested_columns
            .iter()
            .flatten()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let limit = DEFAULT_LOCK_ESCALATION_ENTRIES_PER_TABLE;
        let probe_row_limit = self
            .runtime
            .read_result_row_limit()
            .unwrap_or(limit + 1)
            .min(limit + 1);
        let fallback = || {
            vec![LockRequest::relational_table(
                source.mode,
                source.table.name.clone(),
            )]
        };
        if probe_row_limit == 0 {
            return Ok(fallback());
        }
        let select = SelectStatement {
            vector_index: None,
            projection: columns
                .iter()
                .map(|name| crate::sql::SelectProjection::Expression {
                    expression: Expr::column(crate::sql::SqlColumnRef {
                        qualifier: source.alias.map(str::to_owned),
                        name: name.clone(),
                    }),
                    alias: Some(name.clone()),
                })
                .collect(),
            distinct: false,
            from: Some(source.table.clone()),
            from_alias: source.alias.map(str::to_owned),
            joins: Vec::new(),
            selection: source.selection.cloned(),
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: Some(crate::sql::SqlBound::Literal(probe_row_limit as u64)),
            offset: None,
            lock_strength: None,
        };
        // This internal AST is never cached. Retain the caller's dense parameter
        // metadata even when a SET-only parameter is unused by the key probe.
        let probe = crate::relational_sql::PreparedRelationalSql {
            template: Arc::new(crate::sql::PreparedPostgresStatement {
                statement: SqlStatement::Select(select),
                parameters: prepared.template.parameters.clone(),
            }),
            parse_nanos: 0,
        };
        let caller_limits = super::super::config::relational_query_limits_with_payload(
            &self.runtime.config,
            None,
            None,
        );
        let mut limits = super::super::config::relational_query_limits_with_payload(
            &self.runtime.config,
            Some(probe_row_limit),
            Some(DISCOVERY_PAYLOAD_BYTES),
        );
        limits.max_candidate_work = limits.max_candidate_work.min(DISCOVERY_CANDIDATE_ROWS);
        limits.max_intermediate_rows = limits.max_intermediate_rows.min(DISCOVERY_CANDIDATE_ROWS);
        // The simple streaming probe accounts each visited row before applying
        // the residual. Its work limits bound discovery; retain the caller's
        // hydration limits so a second, untyped cutoff cannot mask that refusal.
        let output = crate::relational_sql::execute_prepared_relational_query_with_resources(
            probe,
            parameters,
            &self.state.relational_state,
            super::super::transactions::transaction_relational_read_modes(
                &self.runtime,
                &self.state,
            )?,
            crate::relational_sql::RelationalQueryResourceContext::new(
                super::super::config::relational_join_enumeration_config_from_database_config(
                    &self.runtime.config,
                ),
                limits,
                &self.runtime.config.execution_memory,
                task,
            ),
        );
        let output = match output {
            Ok(output) => output,
            Err(error) if discovery_budget_exhausted(&error, &caller_limits) => {
                return Ok(fallback());
            }
            Err(error) => return Err(error),
        };
        if output.rows.len() >= probe_row_limit {
            return Ok(fallback());
        }
        let mut requests = Vec::new();
        let mut key_bytes = 0usize;
        for row in &output.rows {
            for columns in requested_columns {
                let key = columns
                    .iter()
                    .map(|column| {
                        let value = row.get(column).ok_or_else(|| {
                            HawDBError::Execution(format!(
                                "lock discovery omitted key column {column}"
                            ))
                        })?;
                        crate::relational_sql::bind_relational_value(
                            SqlValue::Literal(value.clone()),
                            &[],
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                // Account both point bounds before copying replacement values
                // across candidates. A large SET parameter must not multiply
                // into an unbounded temporary lock vector.
                let values_bytes = key
                    .iter()
                    .chain(
                        columns
                            .iter()
                            .zip(&key)
                            .map(|(column, value)| replacements.get(column).unwrap_or(value)),
                    )
                    .fold(0usize, |bytes, value| {
                        bytes
                            .saturating_add(std::mem::size_of::<RelationalValue>())
                            .saturating_add(value.estimated_payload_bytes())
                    });
                key_bytes = key_bytes.saturating_add(values_bytes.saturating_mul(2));
                if key_bytes > DISCOVERY_PAYLOAD_BYTES {
                    return Ok(fallback());
                }
                let new_key = columns
                    .iter()
                    .zip(&key)
                    .map(|(column, value)| replacements.get(column).unwrap_or(value).clone())
                    .collect::<Vec<_>>();
                if new_key != key {
                    push_unique_lock_request(
                        &mut requests,
                        LockRequest::relational_point(
                            source.mode,
                            source.table.name.clone(),
                            columns.clone(),
                            RelationalKey(new_key),
                        ),
                    );
                }
                push_unique_lock_request(
                    &mut requests,
                    LockRequest::relational_point(
                        source.mode,
                        source.table.name.clone(),
                        columns.clone(),
                        RelationalKey(key),
                    ),
                );
                if requests.len() > limit {
                    return Ok(fallback());
                }
            }
        }
        Ok(requests)
    }
}
