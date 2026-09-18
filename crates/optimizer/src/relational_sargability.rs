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
//! Extraction is deliberately conservative: only conjunctive equalities and the
//! existing two-column keyset shape qualify. Callers retain residual evaluation,
//! schema/type checks, parameter binding, index enumeration, and cost selection.

use hawdb_expression::sql::{
    ExprKind, SqlColumnRef, SqlComparisonOp, SqlOrderDirection, SqlOrderItem, SqlPredicate,
    SqlValue,
};
use std::collections::{BTreeMap, BTreeSet};

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

/// Recognizes the existing strict, two-column keyset cursor plus equality prefix.
/// Values remain borrowed and unbound so nullability and types are checked by the caller.
pub fn canonical_keyset_values<'a>(
    predicate: Option<&'a SqlPredicate>,
    access_columns: &BTreeSet<String>,
    order_by: &[SqlOrderItem],
    table: &str,
    qualifier: &str,
) -> Option<(&'a SqlValue, &'a SqlValue)> {
    let predicate = predicate?;
    if order_by.len() != 2 || order_by[0].direction != order_by[1].direction {
        return None;
    }
    let first_order_column = order_by[0].expression.as_column()?;
    let second_order_column = order_by[1].expression.as_column()?;
    let expected = match order_by[0].direction {
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
        if cursor.is_some() {
            return None;
        }
        cursor = match_keyset_or(
            term,
            first_order_column,
            second_order_column,
            expected,
            table,
            qualifier,
        );
        cursor?;
    }
    if equality_columns != *access_columns {
        return None;
    }
    cursor
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
