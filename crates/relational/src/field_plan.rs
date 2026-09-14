//! Storage-neutral SQL field planning and output-alias resolution.
//!
//! This internal ownership seam does not open stores, read rows, or hydrate values.

use skein_core::{Result, SkeinError};
use skein_sql::{
    Expr, ExprKind, SelectProjection, SelectStatement, SqlColumnRef, SqlExpression,
    SqlFunctionArgument, SqlOrderItem, SqlPredicate,
};
use skein_storage::{RelationalState, RelationalTableSchema};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
pub enum RelationalOrderTarget<'a> {
    InputColumn(&'a SqlColumnRef),
    ProjectionColumn {
        column: &'a SqlColumnRef,
        alias: &'a str,
    },
    ProjectionExpression {
        expression: &'a SqlExpression,
        alias: &'a str,
    },
}

pub fn resolve_relational_order_target<'a>(
    select: &'a SelectStatement,
    item: &'a SqlOrderItem,
) -> Result<RelationalOrderTarget<'a>> {
    let column = item.expression.require_column()?;
    if column.qualifier.is_some() {
        return Ok(RelationalOrderTarget::InputColumn(column));
    }
    let mut aliases = select
        .projection
        .iter()
        .filter_map(|projection| match projection {
            SelectProjection::Expression {
                expression:
                    Expr {
                        kind: ExprKind::Column(name),
                        ..
                    },
                alias: Some(alias),
                ..
            } if alias == &column.name => Some(RelationalOrderTarget::ProjectionColumn {
                column: name,
                alias,
            }),
            SelectProjection::Expression {
                expression,
                alias: Some(alias),
            } if alias == &column.name => {
                Some(RelationalOrderTarget::ProjectionExpression { expression, alias })
            }
            SelectProjection::Wildcard | SelectProjection::Expression { .. } => None,
        });
    let Some(target) = aliases.next() else {
        return Ok(RelationalOrderTarget::InputColumn(column));
    };
    if aliases.next().is_some() {
        return Err(SkeinError::Semantic(format!(
            "ambiguous relational ORDER BY alias {}",
            column.name
        )));
    }
    Ok(target)
}

pub struct RelationalFieldPlan {
    scan_fields: BTreeMap<String, Arc<[usize]>>,
    scan_hydration_fields: BTreeMap<String, Arc<[usize]>>,
    output_fields: BTreeMap<String, Arc<[usize]>>,
}

impl RelationalFieldPlan {
    fn new(
        scan_fields: BTreeMap<String, Arc<[usize]>>,
        scan_hydration_fields: BTreeMap<String, Arc<[usize]>>,
        output_fields: BTreeMap<String, Arc<[usize]>>,
    ) -> Self {
        Self {
            scan_fields,
            scan_hydration_fields,
            output_fields,
        }
    }

    pub fn scan_fields(&self, table: &str) -> Result<&[usize]> {
        Self::fields(&self.scan_fields, table)
    }

    pub fn scan_hydration_fields(&self, table: &str) -> Result<&[usize]> {
        Self::fields(&self.scan_hydration_fields, table)
    }

    pub fn output_fields(&self, table: &str) -> Result<&[usize]> {
        Self::fields(&self.output_fields, table)
    }

    fn fields<'fields>(
        plan: &'fields BTreeMap<String, Arc<[usize]>>,
        table: &str,
    ) -> Result<&'fields [usize]> {
        plan.get(table).map(AsRef::as_ref).ok_or_else(|| {
            SkeinError::StorageIntegrity(format!(
                "relational query has no field plan for table {table}"
            ))
        })
    }

    pub fn uses_any_table(&self, tables: &BTreeSet<String>) -> bool {
        self.scan_fields.keys().any(|table| tables.contains(table))
    }

    pub fn index_covers_table(
        &self,
        table: &str,
        schema: &RelationalTableSchema,
        index_columns: &[String],
    ) -> Result<bool> {
        let fields = self.scan_fields.get(table).ok_or_else(|| {
            SkeinError::StorageIntegrity(format!(
                "relational query has no field plan for table {table}"
            ))
        })?;
        let mut covered = BTreeSet::new();
        for column in index_columns.iter().chain(&schema.primary_key) {
            let Some(ordinal) = schema.column_position(column) else {
                return Ok(false);
            };
            covered.insert(ordinal);
        }
        Ok(fields.iter().all(|field| covered.contains(field)))
    }
}

pub fn plan_relational_field_plan(
    select: &SelectStatement,
    state: &RelationalState,
) -> Result<RelationalFieldPlan> {
    let has_aggregate =
        select.having.is_some() || select.projection.iter().any(projection_contains_aggregate);
    let output_fields = plan_requested_fields(select, state)?;
    let projects_before_order = order_by_uses_expression_alias(select)?;
    let scan_fields = if (!select.order_by.is_empty()
        && !select.distinct
        && !has_aggregate
        && !projects_before_order)
        || has_aggregate
        || !select.group_by.is_empty()
    {
        plan_scan_fields(select, state)?
    } else {
        output_fields.clone()
    };
    let scan_hydration_fields = plan_scan_hydration_fields(select, state, &scan_fields)?;
    Ok(RelationalFieldPlan::new(
        scan_fields,
        scan_hydration_fields,
        output_fields,
    ))
}

pub fn single_count_distinct_column(
    select: &SelectStatement,
) -> Option<(&SqlColumnRef, String, Option<&SqlPredicate>)> {
    let [SelectProjection::Expression { expression, alias }] = select.projection.as_slice() else {
        return None;
    };
    let Expr {
        kind:
            ExprKind::Function {
                name,
                arguments,
                distinct: true,
                filter,
            },
        ..
    } = expression
    else {
        return None;
    };
    let [SqlFunctionArgument::Expression(Expr {
        kind: ExprKind::Column(column),
        ..
    })] = arguments.as_slice()
    else {
        return None;
    };
    (name == "count"
        && select.having.is_none()
        && select.group_by.is_empty()
        && select.order_by.is_empty())
    .then(|| {
        (
            column,
            alias.clone().unwrap_or_else(|| "count".to_string()),
            filter.as_deref(),
        )
    })
}

pub fn projection_contains_aggregate(projection: &SelectProjection) -> bool {
    match projection {
        SelectProjection::Expression { expression, .. } => {
            expression_contains_aggregate(expression)
        }
        SelectProjection::Wildcard => false,
    }
}

pub fn order_by_uses_expression_alias(select: &SelectStatement) -> Result<bool> {
    select.order_by.iter().try_fold(false, |found, item| {
        Ok(found
            || matches!(
                resolve_relational_order_target(select, item)?,
                RelationalOrderTarget::ProjectionExpression { .. }
            ))
    })
}

fn expression_contains_aggregate(expression: &SqlExpression) -> bool {
    let mut aggregate = false;
    expression.visit(&mut |expression| {
        if let ExprKind::Function { name, .. } = &expression.kind {
            aggregate |= matches!(name.as_str(), "count" | "sum" | "max");
        }
    });
    aggregate
}

fn plan_requested_fields(
    select: &SelectStatement,
    state: &RelationalState,
) -> Result<BTreeMap<String, Arc<[usize]>>> {
    plan_fields(select, state, true)
}

fn plan_scan_fields(
    select: &SelectStatement,
    state: &RelationalState,
) -> Result<BTreeMap<String, Arc<[usize]>>> {
    plan_fields(select, state, false)
}

fn plan_scan_hydration_fields(
    select: &SelectStatement,
    state: &RelationalState,
    scan_fields: &BTreeMap<String, Arc<[usize]>>,
) -> Result<BTreeMap<String, Arc<[usize]>>> {
    let base_schema = state.table_schema(&select.from.name).ok_or_else(|| {
        SkeinError::Semantic(format!("unknown relational table {}", select.from.name))
    })?;
    let mut bindings = Vec::with_capacity(select.joins.len() + 1);
    bindings.push(FieldBinding {
        table: &select.from.name,
        qualifier: select.from_alias.as_deref().unwrap_or(&select.from.name),
        schema: base_schema,
    });
    for join in &select.joins {
        let schema = state.table_schema(&join.table.name).ok_or_else(|| {
            SkeinError::Semantic(format!("unknown relational table {}", join.table.name))
        })?;
        bindings.push(FieldBinding {
            table: &join.table.name,
            qualifier: join.alias.as_deref().unwrap_or(&join.table.name),
            schema,
        });
    }

    let mut metadata_columns = Vec::new();
    let mut value_columns = Vec::new();
    for projection in &select.projection {
        match projection {
            SelectProjection::Wildcard => {
                for binding in &bindings {
                    value_columns.extend(binding.schema.columns.iter().map(|column| {
                        SqlColumnRef {
                            qualifier: Some(binding.qualifier.to_string()),
                            name: column.name.clone(),
                        }
                    }));
                }
            }
            SelectProjection::Expression {
                expression:
                    Expr {
                        kind: ExprKind::Column(name),
                        ..
                    },
                ..
            } => value_columns.push(name.clone()),
            SelectProjection::Expression { expression, .. } => {
                collect_expression_hydration_columns(
                    expression,
                    &mut metadata_columns,
                    &mut value_columns,
                );
            }
        }
    }
    if let Some(having) = &select.having {
        collect_expression_hydration_columns(having, &mut metadata_columns, &mut value_columns);
    }
    let mut raw_references = Vec::new();
    if let Some(selection) = &select.selection {
        collect_expression_columns(selection, &mut raw_references);
    }
    for join in &select.joins {
        collect_expression_columns(&join.on, &mut raw_references);
    }
    raw_references.extend(select.group_by.iter());
    for item in &select.order_by {
        match resolve_relational_order_target(select, item)? {
            RelationalOrderTarget::InputColumn(column) => raw_references.push(column),
            RelationalOrderTarget::ProjectionColumn { column, .. } => {
                raw_references.push(column);
            }
            RelationalOrderTarget::ProjectionExpression { expression, .. } => {
                collect_expression_hydration_columns(
                    expression,
                    &mut metadata_columns,
                    &mut value_columns,
                )
            }
        }
    }
    value_columns.extend(raw_references.into_iter().cloned());

    let resolve = |column: &SqlColumnRef| {
        resolve_field_binding(column, &bindings)
            .map(|(table, ordinal)| (table.to_string(), ordinal))
    };
    let metadata_fields = metadata_columns
        .iter()
        .map(resolve)
        .collect::<Result<BTreeSet<_>>>()?;
    let value_fields = value_columns
        .iter()
        .map(resolve)
        .collect::<Result<BTreeSet<_>>>()?;
    let metadata_only = metadata_fields
        .difference(&value_fields)
        .cloned()
        .collect::<BTreeSet<_>>();

    Ok(scan_fields
        .iter()
        .map(|(table, fields)| {
            let hydration_fields = fields
                .iter()
                .copied()
                .filter(|ordinal| !metadata_only.contains(&(table.clone(), *ordinal)))
                .collect::<Vec<_>>();
            (table.clone(), Arc::from(hydration_fields))
        })
        .collect())
}

fn plan_fields(
    select: &SelectStatement,
    state: &RelationalState,
    include_projection: bool,
) -> Result<BTreeMap<String, Arc<[usize]>>> {
    let mut bindings = Vec::with_capacity(select.joins.len() + 1);
    let base_schema = state.table_schema(&select.from.name).ok_or_else(|| {
        SkeinError::Semantic(format!("unknown relational table {}", select.from.name))
    })?;
    bindings.push(FieldBinding {
        table: &select.from.name,
        qualifier: select.from_alias.as_deref().unwrap_or(&select.from.name),
        schema: base_schema,
    });
    for join in &select.joins {
        let schema = state.table_schema(&join.table.name).ok_or_else(|| {
            SkeinError::Semantic(format!("unknown relational table {}", join.table.name))
        })?;
        bindings.push(FieldBinding {
            table: &join.table.name,
            qualifier: join.alias.as_deref().unwrap_or(&join.table.name),
            schema,
        });
    }
    let mut planned = bindings
        .iter()
        .map(|binding| (binding.table.to_string(), BTreeSet::new()))
        .collect::<BTreeMap<_, _>>();
    let mut columns = Vec::new();
    if include_projection {
        for projection in &select.projection {
            match projection {
                SelectProjection::Wildcard => {
                    for binding in &bindings {
                        planned
                            .get_mut(binding.table)
                            .expect("field plan contains every binding")
                            .extend(0..binding.schema.columns.len());
                    }
                }
                SelectProjection::Expression {
                    expression:
                        Expr {
                            kind: ExprKind::Column(name),
                            ..
                        },
                    ..
                } => columns.push(name),
                SelectProjection::Expression { expression, .. } => {
                    collect_expression_columns(expression, &mut columns)
                }
            }
        }
    } else {
        // A grouped aggregate can defer output-only columns until after the
        // blocking operator, but every aggregate input is still part of the
        // scan contract. Omitting it would make the row-page reader produce a
        // projection that the aggregate executor cannot evaluate.
        for projection in &select.projection {
            if let SelectProjection::Expression { expression, .. } = projection
                && (select.having.is_some() || expression_contains_aggregate(expression))
            {
                collect_expression_columns(expression, &mut columns);
            }
        }
    }
    if let Some(selection) = &select.selection {
        collect_expression_columns(selection, &mut columns);
    }
    if let Some(having) = &select.having {
        collect_expression_columns(having, &mut columns);
    }
    for join in &select.joins {
        collect_expression_columns(&join.on, &mut columns);
    }
    columns.extend(select.group_by.iter());
    for item in &select.order_by {
        match resolve_relational_order_target(select, item)? {
            RelationalOrderTarget::InputColumn(column) => columns.push(column),
            RelationalOrderTarget::ProjectionColumn { column, .. } => {
                columns.push(column);
            }
            RelationalOrderTarget::ProjectionExpression { expression, .. } => {
                collect_expression_columns(expression, &mut columns)
            }
        }
    }
    for column in columns {
        let first = resolve_field_binding(column, &bindings)?;
        planned
            .get_mut(first.0)
            .expect("resolved table has a field plan")
            .insert(first.1);
    }
    Ok(planned
        .into_iter()
        .map(|(table, fields)| (table, Arc::from(fields.into_iter().collect::<Vec<_>>())))
        .collect())
}

struct FieldBinding<'a> {
    table: &'a str,
    qualifier: &'a str,
    schema: &'a RelationalTableSchema,
}

fn resolve_field_binding<'a>(
    column: &SqlColumnRef,
    bindings: &'a [FieldBinding<'a>],
) -> Result<(&'a str, usize)> {
    let mut matches = bindings.iter().filter_map(|binding| {
        let qualifier_matches = column
            .qualifier
            .as_deref()
            .is_none_or(|qualifier| qualifier == binding.qualifier || qualifier == binding.table);
        qualifier_matches
            .then(|| binding.schema.column_position(&column.name))
            .flatten()
            .map(|ordinal| (binding.table, ordinal))
    });
    let first = matches.next().ok_or_else(|| {
        SkeinError::Semantic(format!("unknown relational column {}", column.name))
    })?;
    if matches.next().is_some() {
        return Err(SkeinError::Semantic(format!(
            "ambiguous relational column {}",
            column.name
        )));
    }
    Ok(first)
}

fn collect_expression_hydration_columns(
    expression: &SqlExpression,
    metadata_columns: &mut Vec<SqlColumnRef>,
    value_columns: &mut Vec<SqlColumnRef>,
) {
    match expression {
        Expr {
            kind: ExprKind::Column(column),
            ..
        } => value_columns.push(column.clone()),
        Expr {
            kind:
                ExprKind::Function {
                    name,
                    arguments,
                    distinct: false,
                    filter: None,
                },
            ..
        } if matches!(name.as_str(), "count" | "octet_length")
            && matches!(
                arguments.as_slice(),
                [SqlFunctionArgument::Expression(Expr {
                    kind: ExprKind::Column(_),
                    ..
                })]
            ) =>
        {
            let [SqlFunctionArgument::Expression(Expr {
                kind: ExprKind::Column(column),
                ..
            })] = arguments.as_slice()
            else {
                unreachable!("metadata-only function shape was checked above")
            };
            metadata_columns.push(column.clone());
        }
        Expr {
            kind: ExprKind::Function {
                arguments, filter, ..
            },
            ..
        } => {
            for argument in arguments {
                if let SqlFunctionArgument::Expression(expression) = argument {
                    collect_expression_hydration_columns(
                        expression,
                        metadata_columns,
                        value_columns,
                    );
                }
            }
            if let Some(filter) = filter {
                collect_predicate_hydration_columns(filter, value_columns);
            }
        }
        Expr {
            kind: ExprKind::Value(_),
            ..
        } => {}
        Expr {
            kind:
                ExprKind::And(left, right)
                | ExprKind::Or(left, right)
                | ExprKind::Compare { left, right, .. },
            ..
        } => {
            collect_expression_hydration_columns(left, metadata_columns, value_columns);
            collect_expression_hydration_columns(right, metadata_columns, value_columns);
        }
        Expr {
            kind:
                ExprKind::Not(inner)
                | ExprKind::IsNull {
                    expression: inner, ..
                },
            ..
        } => {
            collect_expression_hydration_columns(inner, metadata_columns, value_columns);
        }
        Expr {
            kind: ExprKind::InList { left, values, .. },
            ..
        } => {
            collect_expression_hydration_columns(left, metadata_columns, value_columns);
            for value in values {
                collect_expression_hydration_columns(value, metadata_columns, value_columns);
            }
        }
        Expr {
            kind: ExprKind::Like { left, pattern, .. },
            ..
        } => {
            collect_expression_hydration_columns(left, metadata_columns, value_columns);
            collect_expression_hydration_columns(pattern, metadata_columns, value_columns);
        }
    }
}

fn collect_predicate_hydration_columns(
    predicate: &SqlPredicate,
    value_columns: &mut Vec<SqlColumnRef>,
) {
    predicate.visit(&mut |expression| {
        if let Some(column) = expression.as_column() {
            value_columns.push(column.clone());
        }
    });
}

fn collect_expression_columns<'a>(
    expression: &'a SqlExpression,
    output: &mut Vec<&'a SqlColumnRef>,
) {
    expression.visit(&mut |expression| {
        if let Some(column) = expression.as_column() {
            output.push(column);
        }
    });
}

#[cfg(test)]
mod tests;
