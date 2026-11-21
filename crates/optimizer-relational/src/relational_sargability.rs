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

//! Extracts index-compatible constraints without binding values or accessing storage.
//!
//! Extraction is deliberately conservative: conjunctive equalities/ranges and the
//! PostgreSQL row comparisons and the legacy two-column keyset shape qualify.
//! Callers retain residual evaluation,
//! schema/type checks, parameter binding, index enumeration, and cost selection.

use hawdb_expression::sql::{
    ExprKind, SqlColumnRef, SqlComparisonOp, SqlOrderDirection, SqlOrderItem, SqlPredicate,
    SqlValue,
};
use std::collections::{BTreeMap, BTreeSet};

/// Extract only column-to-value range comparisons under conjunctions. OR, NOT,
/// column-to-column comparisons and scalar expressions remain residuals.
pub fn collect_conjunctive_ranges<'a>(
    predicate: &'a SqlPredicate,
    output: &mut Vec<(&'a SqlColumnRef, SqlComparisonOp, &'a SqlValue)>,
) {
    match &predicate.kind {
        ExprKind::And(left, right) => {
            collect_conjunctive_ranges(left, output);
            collect_conjunctive_ranges(right, output);
        }
        ExprKind::Compare { left, op, right }
            if matches!(
                op,
                SqlComparisonOp::Lt
                    | SqlComparisonOp::Lte
                    | SqlComparisonOp::Gt
                    | SqlComparisonOp::Gte
            ) =>
        {
            if let (Some(column), Some(value)) = (left.as_column(), right.as_value()) {
                output.push((column, *op, value));
            }
        }
        _ => {}
    }
}

/// Proves that the bound interval and distinct leading equalities cover every
/// predicate. Repeated equality constraints remain residuals; range constraints
/// may repeat because their bound intersection includes every comparison.
pub fn predicate_is_covered_by_range(
    predicate: Option<&SqlPredicate>,
    equalities: &BTreeSet<String>,
    range_column: &str,
    table: &str,
    qualifier: &str,
) -> bool {
    let Some(predicate) = predicate else {
        return false;
    };
    let mut terms = Vec::new();
    collect_conjuncts(predicate, &mut terms);
    let mut seen = BTreeSet::new();
    let mut range = false;
    for term in terms {
        let ExprKind::Compare { left, op, right } = &term.kind else {
            return false;
        };
        let Some(column) = left.as_column() else {
            return false;
        };
        if !column_matches(column, table, qualifier) || right.as_value().is_none() {
            return false;
        }
        if *op == SqlComparisonOp::Eq && equalities.contains(&column.name) {
            if !seen.insert(column.name.clone()) {
                return false;
            }
        } else if column.name == range_column
            && matches!(
                op,
                SqlComparisonOp::Lt
                    | SqlComparisonOp::Lte
                    | SqlComparisonOp::Gt
                    | SqlComparisonOp::Gte
            )
        {
            range = true;
        } else {
            return false;
        }
    }
    range && seen == *equalities
}

/// Whether distinct conjunctive equalities exactly cover the access columns.
/// An absent predicate is already covered; duplicate constraints remain residuals.
pub fn predicate_is_covered_by_equalities(
    predicate: Option<&SqlPredicate>,
    access_columns: &BTreeSet<String>,
    table: &str,
    qualifier: &str,
) -> bool {
    fn covered(
        predicate: &SqlPredicate,
        access_columns: &BTreeSet<String>,
        table: &str,
        qualifier: &str,
        columns: &mut BTreeSet<String>,
    ) -> bool {
        match &predicate.kind {
            ExprKind::And(left, right) => {
                covered(left, access_columns, table, qualifier, columns)
                    && covered(right, access_columns, table, qualifier, columns)
            }
            ExprKind::Compare {
                left,
                op: SqlComparisonOp::Eq,
                right,
            } => {
                right.as_value().is_some()
                    && left.as_column().is_some_and(|left| {
                        column_matches(left, table, qualifier)
                            && access_columns.contains(&left.name)
                            && columns.insert(left.name.clone())
                    })
            }
            _ => false,
        }
    }

    predicate.is_none_or(|predicate| {
        let mut columns = BTreeSet::new();
        covered(predicate, access_columns, table, qualifier, &mut columns)
            && columns == *access_columns
    })
}

/// Recognizes a strict row-comparison keyset cursor (or the legacy expanded
/// two-column form) plus an equality prefix.
/// Values remain borrowed and unbound so nullability and types are checked by the caller.
pub fn canonical_keyset_values<'a>(
    predicate: Option<&'a SqlPredicate>,
    access_columns: &BTreeSet<String>,
    order_by: &[SqlOrderItem],
    table: &str,
    qualifier: &str,
) -> Option<Vec<&'a SqlValue>> {
    canonical_keyset_values_with_range(predicate, access_columns, order_by, table, qualifier, None)
}

/// Recognizes the same keyset cursor intersected with already-bound conjunctive
/// comparisons on its first ordered column. The caller must apply every bound.
pub fn canonical_keyset_values_with_range<'a>(
    predicate: Option<&'a SqlPredicate>,
    access_columns: &BTreeSet<String>,
    order_by: &[SqlOrderItem],
    table: &str,
    qualifier: &str,
    range_column: Option<&str>,
) -> Option<Vec<&'a SqlValue>> {
    let predicate = predicate?;
    let first_order = order_by.first()?;
    if order_by
        .iter()
        .any(|item| item.direction != first_order.direction)
    {
        return None;
    }
    let first_order_column = first_order.expression.as_column()?;
    let expected = match first_order.direction {
        SqlOrderDirection::Asc => SqlComparisonOp::Gt,
        SqlOrderDirection::Desc => SqlComparisonOp::Lt,
    };
    let mut terms = Vec::new();
    collect_conjuncts(predicate, &mut terms);
    let mut equality_columns = BTreeSet::new();
    let mut cursor = None;
    for term in terms {
        if let ExprKind::Compare {
            left,
            op: SqlComparisonOp::Eq,
            right,
        } = &term.kind
            && right.as_value().is_some()
            && let Some(left) = left.as_column()
            && column_matches(left, table, qualifier)
            && access_columns.contains(&left.name)
        {
            if !equality_columns.insert(left.name.clone()) {
                return None;
            }
            continue;
        }
        if let ExprKind::Compare { left, op, right } = &term.kind
            && matches!(
                op,
                SqlComparisonOp::Lt
                    | SqlComparisonOp::Lte
                    | SqlComparisonOp::Gt
                    | SqlComparisonOp::Gte
            )
            && right.as_value().is_some()
            && let Some(column) = left.as_column()
            && column_matches(column, table, qualifier)
            && range_column == Some(column.name.as_str())
            && column.name == first_order_column.name
        {
            continue;
        }
        if cursor.is_some() {
            return None;
        }
        cursor = match_row_keyset(term, order_by, expected, table, qualifier).or_else(|| {
            let [first, second] = order_by else {
                return None;
            };
            match_keyset_or(
                term,
                first.expression.as_column()?,
                second.expression.as_column()?,
                expected,
                table,
                qualifier,
            )
            .map(|(first, second)| vec![first, second])
        });
        cursor.as_ref()?;
    }
    if equality_columns != *access_columns {
        return None;
    }
    cursor
}

fn match_row_keyset<'a>(
    predicate: &'a SqlPredicate,
    order_by: &[SqlOrderItem],
    comparison: SqlComparisonOp,
    table: &str,
    qualifier: &str,
) -> Option<Vec<&'a SqlValue>> {
    let ExprKind::RowCompare { left, op, right } = &predicate.kind else {
        return None;
    };
    if *op != comparison || left.len() != order_by.len() || right.len() != order_by.len() {
        return None;
    }
    left.iter()
        .zip(right)
        .zip(order_by)
        .map(|((left, right), order)| {
            let left = left.as_column()?;
            let expected = order.expression.as_column()?;
            (left.name == expected.name && column_matches(left, table, qualifier))
                .then(|| right.as_value())
                .flatten()
        })
        .collect()
}

/// Appends column/value equalities reachable only through conjunctions.
/// Preserves traversal order and duplicates for the caller's conflict checks.
pub fn collect_conjunctive_equalities<'a>(
    predicate: &'a SqlPredicate,
    output: &mut Vec<(&'a SqlColumnRef, &'a SqlValue)>,
) {
    match &predicate.kind {
        ExprKind::And(left, right) => {
            collect_conjunctive_equalities(left, output);
            collect_conjunctive_equalities(right, output);
        }
        ExprKind::Compare {
            left,
            op: SqlComparisonOp::Eq,
            right,
        } => {
            if let (Some(left), Some(right)) = (left.as_column(), right.as_value()) {
                output.push((left, right));
            }
        }
        _ => {}
    }
}

/// Collects join-column equalities with exactly one side qualified for this input.
/// Keeps the first outer column for each join column, matching access selection.
pub fn collect_conjunctive_join_equalities(
    predicate: &SqlPredicate,
    table: &str,
    qualifier: &str,
    output: &mut BTreeMap<String, SqlColumnRef>,
) {
    match &predicate.kind {
        ExprKind::And(left, right) => {
            collect_conjunctive_join_equalities(left, table, qualifier, output);
            collect_conjunctive_join_equalities(right, table, qualifier, output);
        }
        ExprKind::Compare {
            left,
            op: SqlComparisonOp::Eq,
            right,
        } => {
            let (Some(left), Some(right)) = (left.as_column(), right.as_column()) else {
                return;
            };
            let left_is_join = column_targets_join(left, table, qualifier);
            let right_is_join = column_targets_join(right, table, qualifier);
            match (left_is_join, right_is_join) {
                (true, false) => {
                    output
                        .entry(left.name.clone())
                        .or_insert_with(|| right.clone());
                }
                (false, true) => {
                    output
                        .entry(right.name.clone())
                        .or_insert_with(|| left.clone());
                }
                (true, true) | (false, false) => {}
            }
        }
        _ => {}
    }
}

fn collect_conjuncts<'a>(predicate: &'a SqlPredicate, output: &mut Vec<&'a SqlPredicate>) {
    match &predicate.kind {
        ExprKind::And(left, right) => {
            collect_conjuncts(left, output);
            collect_conjuncts(right, output);
        }
        _ => output.push(predicate),
    }
}

fn match_keyset_or<'a>(
    predicate: &'a SqlPredicate,
    first_column: &SqlColumnRef,
    second_column: &SqlColumnRef,
    comparison: SqlComparisonOp,
    table: &str,
    qualifier: &str,
) -> Option<(&'a SqlValue, &'a SqlValue)> {
    let ExprKind::Or(left, right) = &predicate.kind else {
        return None;
    };
    match_keyset_branches(
        left,
        right,
        first_column,
        second_column,
        comparison,
        table,
        qualifier,
    )
    .or_else(|| {
        match_keyset_branches(
            right,
            left,
            first_column,
            second_column,
            comparison,
            table,
            qualifier,
        )
    })
}

fn match_keyset_branches<'a>(
    first_branch: &'a SqlPredicate,
    tie_branch: &'a SqlPredicate,
    first_column: &SqlColumnRef,
    second_column: &SqlColumnRef,
    comparison: SqlComparisonOp,
    table: &str,
    qualifier: &str,
) -> Option<(&'a SqlValue, &'a SqlValue)> {
    let first = match_column_comparison(first_branch, first_column, comparison, table, qualifier)?;
    let ExprKind::And(left, right) = &tie_branch.kind else {
        return None;
    };
    let tie = match_column_comparison(left, first_column, SqlComparisonOp::Eq, table, qualifier)
        .zip(match_column_comparison(
            right,
            second_column,
            comparison,
            table,
            qualifier,
        ))
        .or_else(|| {
            match_column_comparison(right, first_column, SqlComparisonOp::Eq, table, qualifier).zip(
                match_column_comparison(left, second_column, comparison, table, qualifier),
            )
        })?;
    (first == tie.0).then_some((first, tie.1))
}

fn match_column_comparison<'a>(
    predicate: &'a SqlPredicate,
    expected_column: &SqlColumnRef,
    expected_op: SqlComparisonOp,
    table: &str,
    qualifier: &str,
) -> Option<&'a SqlValue> {
    let ExprKind::Compare { left, op, right } = &predicate.kind else {
        return None;
    };
    let left = left.as_column()?;
    let right = right.as_value()?;
    (*op == expected_op
        && left.name == expected_column.name
        && column_matches(left, table, qualifier))
    .then_some(right)
}

fn column_matches(column: &SqlColumnRef, table: &str, qualifier: &str) -> bool {
    column
        .qualifier
        .as_deref()
        .is_none_or(|candidate| candidate == table || candidate == qualifier)
}

fn column_targets_join(column: &SqlColumnRef, table: &str, qualifier: &str) -> bool {
    column
        .qualifier
        .as_deref()
        .is_some_and(|candidate| candidate == table || candidate == qualifier)
}

#[cfg(test)]
mod tests;
