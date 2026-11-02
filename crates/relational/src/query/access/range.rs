// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_optimizer::relational_sargability::collect_conjunctive_ranges;
use hawdb_sql::SqlComparisonOp;
use hawdb_storage::relational::{RelationalIndexBound, RelationalIndexBounds};

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
