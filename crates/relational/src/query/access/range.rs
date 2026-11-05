// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_optimizer::relational_sargability::collect_conjunctive_ranges;
use hawdb_sql::SqlComparisonOp;
use hawdb_storage::relational::{
    RelationalIndexBound, RelationalIndexBounds, RelationalPrimaryKeyRange,
};
use std::ops::Bound;

pub(super) fn bind_range(
    planning: &RelationalBaseAccessPlanning<
        '_,
        impl crate::index_runtime::RelationalIndexStoreReader,
    >,
    columns: &[String],
    prefix: &RelationalKey,
) -> Result<Option<RelationalIndexBounds>> {
    let Some(column) = columns.get(prefix.0.len()) else {
        return Ok(None);
    };
    let Some(predicate) = planning.predicate else {
        return Ok(None);
    };
    let position = planning
        .schema
        .column_position(column)
        .expect("validated index column");
    let scalar_type = planning.schema.columns[position].scalar_type;
    let mut terms = Vec::new();
    collect_conjunctive_ranges(predicate, &mut terms);
    let mut bounds = RelationalIndexBounds::default();
    let mut found = false;
    for (target, op, value) in terms {
        if target.name != *column
            || target
                .qualifier
                .as_deref()
                .is_some_and(|q| q != planning.table && q != planning.qualifier)
        {
            continue;
        }
        let value =
            value_to_relational_as(bind_sql_value(value, planning.parameters)?, scalar_type)?;
        if value == RelationalValue::Null {
            found = true;
            bounds.empty = true;
            continue;
        }
        if !scalar_type.same_value_domain(value.scalar_type()) {
            // Coverage proofs assume that every range term entered the
            // intersection. Leave all of them residual if one domain differs.
            return Ok(None);
        }
        found = true;
        let mut key = prefix.0.clone();
        key.push(value);
        let bound = RelationalIndexBound {
            key: RelationalKey(key),
            inclusive: matches!(op, SqlComparisonOp::Lte | SqlComparisonOp::Gte),
        };
        let (slot, lower) = if matches!(op, SqlComparisonOp::Gt | SqlComparisonOp::Gte) {
            (&mut bounds.lower, true)
        } else {
            (&mut bounds.upper, false)
        };
        match slot {
            Some(existing) if existing.key == bound.key => existing.inclusive &= bound.inclusive,
            Some(existing)
                if (lower && existing.key > bound.key) || (!lower && existing.key < bound.key) => {}
            slot => *slot = Some(bound),
        }
    }
    if !found {
        return Ok(None);
    }
    // SQL range predicates reject NULL even when they only specify an upper
    // bound. Exclude the entire NULL prefix before visiting any postings.
    if bounds.lower.is_none() {
        let mut key = prefix.0.clone();
        key.push(RelationalValue::Null);
        bounds.lower = Some(RelationalIndexBound {
            key: RelationalKey(key),
            inclusive: false,
        });
    }
    if let (Some(lower), Some(upper)) = (&bounds.lower, &bounds.upper) {
        bounds.empty |= lower.key > upper.key
            || (lower.key == upper.key && (!lower.inclusive || !upper.inclusive));
    }
    Ok(Some(bounds))
}

/// The bound value on the next key column. The binder closes a missing lower
/// bound at NULL; primary-key columns are never NULL, so the equality prefix
/// itself is that boundary.
fn bound_value(
    bound: &Option<RelationalIndexBound>,
    prefix_len: usize,
) -> Option<(&RelationalValue, bool)> {
    bound.as_ref().and_then(|bound| {
        let value = &bound.key.0[prefix_len];
        (*value != RelationalValue::Null).then_some((value, bound.inclusive))
    })
}

/// Reuses the secondary-index interval binding for the clustered primary key:
/// an equality prefix shorter than the key plus optional bounds on its next
/// column. Complete keys remain point reads; an unconstrained key is a scan.
pub(super) fn primary_key_range_candidate(
    planning: &RelationalBaseAccessPlanning<
        '_,
        impl crate::index_runtime::RelationalIndexStoreReader,
    >,
    bound: &BTreeMap<String, RelationalValue>,
) -> Result<Option<RelationalAccessCandidate>> {
    let RelationalBaseAccessPlanning {
        order_by,
        prefer_ordered_access,
        state,
        schema,
        table,
        qualifier,
        cardinality_limit,
        projection,
        ..
    } = planning;
    let columns = &schema.primary_key;
    let prefix = RelationalKey(
        columns
            .iter()
            .map_while(|column| bound.get(column).cloned())
            .collect(),
    );
    let prefix_len = prefix.0.len();
    if columns.is_empty() || prefix_len == columns.len() {
        return Ok(None);
    }
    let bounds = bind_range(planning, columns, &prefix)?;
    if prefix_len == 0 && bounds.is_none() {
        return Ok(None);
    }
    let range = match &bounds {
        Some(bounds) if bounds.empty => RelationalPrimaryKeyRange {
            lower: Bound::Excluded(prefix.clone()),
            upper: Bound::Included(prefix.clone()),
        },
        Some(bounds) => RelationalPrimaryKeyRange::from_prefix_bounds(
            prefix.clone(),
            bound_value(&bounds.lower, prefix_len),
            bound_value(&bounds.upper, prefix_len),
        )
        .map_err(|error| HawDBError::Execution(error.to_string()))?,
        None => RelationalPrimaryKeyRange::from_prefix_bounds(prefix.clone(), None, None)
            .map_err(|error| HawDBError::Execution(error.to_string()))?,
    };
    let range_column = bounds.as_ref().map(|_| columns[prefix_len].as_str());
    // Row-page snapshots stream intervals forward only; a descending order
    // keeps the interval and sorts it.
    let (order_prefix_len, direction) = index_order_prefix(
        order_by,
        columns,
        prefix_len,
        schema,
        table,
        qualifier,
        range_column,
    );
    let order_prefix_len =
        if *prefer_ordered_access && direction == RelationalIndexScanDirection::Forward {
            order_prefix_len
        } else {
            0
        };
    // Count at most a small resident interval. Detached rows use the same
    // one-third range heuristic as bounded secondary intervals.
    let row_count = projection
        .row_count_override
        .unwrap_or_else(|| state.row_count(table));
    let probe_limit = (*cardinality_limit).min(64);
    let fallback = row_count.div_ceil(3).min(*cardinality_limit);
    let estimated_rows = if range.is_empty() {
        0
    } else if state.materialized_rows_resident() {
        let observed = state.rows_in_range(table, &range).take(probe_limit).count();
        if observed < probe_limit {
            observed
        } else {
            fallback.max(observed)
        }
    } else {
        fallback
    }
    .max(1);
    Ok(Some(RelationalAccessCandidate {
        descriptor: RelationalAccessPathDescriptor {
            kind: RelationalAccessPathKind::PrimaryKeyRange,
            name: "__primary_key_range".to_string(),
            index_columns: columns.clone(),
            access_columns: columns[..prefix_len].iter().cloned().collect(),
            equality_prefix_len: prefix_len,
            order_prefix_len,
            exclusive_range: false,
            range: bounds
                .as_ref()
                .map(|bounds| hawdb_optimizer::RelationalRangeAccess {
                    column: columns[prefix_len].clone(),
                    lower_inclusive: bounds.lower.as_ref().map(|b| b.inclusive),
                    upper_inclusive: bounds.upper.as_ref().map(|b| b.inclusive),
                    empty: bounds.empty,
                }),
            reverse_order: false,
            unique_point: false,
            covering: false,
            requires_row_fetch: false,
            estimated_rows,
        },
        access: RelationalBaseAccess::PrimaryKeyRange(range),
    }))
}
