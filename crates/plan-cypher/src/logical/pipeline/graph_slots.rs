// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Carry grouped graph variables through `WITH` as graph slots (#1615).
//!
//! An aggregating `WITH` that groups by a bound node or relationship variable,
//! and a `WITH DISTINCT` that selects one, groups those keys by entity
//! identity and keeps the first row's record. The record then leaves in the
//! output row's graph slot of the same name instead of a materialized map
//! column, so later clauses read the variable as a bound graph variable, as
//! they do after `MATCH`. `WITH DISTINCT` is planned as such a grouping with
//! no aggregates, which keeps its first-occurrence row order in memory.
//!
//! `RETURN`, an aggregating `WITH DISTINCT` and every other shape keep
//! materialized map columns.

use super::*;
use std::cell::Cell;

thread_local! {
    static LEGACY_MAP_COLUMNS: Cell<bool> = const { Cell::new(false) };
}

/// Plans `f` with the map-column projection of grouped graph variables used
/// before #1615, on this thread only. Differential tests compare it with
/// the default planning; it is not a production control.
#[doc(hidden)]
pub fn with_legacy_graph_group_keys<T>(f: impl FnOnce() -> T) -> T {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            LEGACY_MAP_COLUMNS.with(|legacy| legacy.set(self.0));
        }
    }
    let _restore = Restore(LEGACY_MAP_COLUMNS.with(|legacy| legacy.replace(true)));
    f()
}

/// The output names of `projection` that leave as graph slots, with the kind
/// of the variable each one carries. Empty when the clause keeps map columns.
pub(super) fn slot_names(
    planned: &PlannedReturns,
    scope: &Scope,
    projection: &ProjectionClause,
    names: &[String],
    with_clause: bool,
) -> BTreeMap<String, GraphEntityKind> {
    let mut slots = BTreeMap::new();
    if !with_clause
        || LEGACY_MAP_COLUMNS.with(Cell::get)
        // Runtime suffixes duplicate names; keep those projections unchanged.
        || planned.names() != names
        || projection.items.len() != names.len()
    {
        return slots;
    }
    let native = |variable: &str| match scope.0.get(variable) {
        Some(BindingType::Graph { kind, column: None }) => Some(*kind),
        _ => None,
    };
    match planned {
        PlannedReturns::AggregateProjection {
            group_keys,
            items,
            projections,
        } if !projection.distinct
            && !items
                .iter()
                .any(|item| matches!(item.target, AggregateTarget::Ranked { .. })) =>
        {
            for (projection, name) in projections.iter().zip(names) {
                let ProjectionExpression::Column(column) = &projection.expression else {
                    continue;
                };
                let Some(ProjectionExpression::Variable { variable }) = group_keys
                    .iter()
                    .find(|key| &key.name == column)
                    .map(|key| &key.expression)
                else {
                    continue;
                };
                if let Some(kind) = native(variable) {
                    slots.insert(name.clone(), kind);
                }
            }
        }
        PlannedReturns::Projections(items) if projection.distinct => {
            for (item, name) in items.iter().zip(names) {
                if let ProjectionExpression::Variable { variable } = &item.expression
                    && let Some(kind) = native(variable)
                {
                    slots.insert(name.clone(), kind);
                }
            }
        }
        _ => {}
    }
    slots
}

/// Builds the grouping that carries `slots`; the caller adds no `Distinct`.
pub(super) fn into_logical(
    planned: PlannedReturns,
    slots: &BTreeMap<String, GraphEntityKind>,
    input: LogicalPlan,
) -> LogicalPlan {
    match planned {
        PlannedReturns::Projections(group_keys) => LogicalPlan::Aggregate {
            group_keys,
            items: Vec::new(),
            graph_keys: GraphGroupKeys::DistinctSlots,
            input: Box::new(input),
        },
        PlannedReturns::AggregateProjection {
            mut group_keys,
            items,
            projections,
        } => {
            // A slot key takes its output name; the projection above it no
            // longer names it, because projections keep their input's graph
            // slots.
            let mut remaining = Vec::with_capacity(projections.len());
            for projection in projections {
                let key = match (&projection.expression, slots.contains_key(&projection.name)) {
                    (ProjectionExpression::Column(column), true) => {
                        group_keys.iter_mut().find(|key| &key.name == column)
                    }
                    _ => None,
                };
                match key {
                    Some(key) => key.name = projection.name,
                    None => remaining.push(projection),
                }
            }
            collapse_identity(group_keys, items, remaining, input)
        }
        PlannedReturns::Aggregations { group_keys, items } => LogicalPlan::Aggregate {
            group_keys,
            items,
            graph_keys: GraphGroupKeys::Slots,
            input: Box::new(input),
        },
    }
}

/// Renames internal output columns when the remaining projection only
/// renames them, as normalization does for value-only groupings.
fn collapse_identity(
    mut group_keys: Vec<Projection>,
    mut items: Vec<Aggregation>,
    projections: Vec<Projection>,
    input: LogicalPlan,
) -> LogicalPlan {
    let internal = group_keys
        .iter()
        .map(|key| &key.name)
        .chain(items.iter().map(|item| &item.name))
        .filter(|name| name.starts_with('\0'))
        .count();
    let mut renames = BTreeMap::new();
    let identity = projections.len() == internal
        && projections.iter().all(|projection| {
            matches!(&projection.expression, ProjectionExpression::Column(column)
                if column.starts_with('\0')
                    && renames.insert(column.clone(), projection.name.clone()).is_none())
        });
    if !identity {
        return LogicalPlan::Project {
            items: projections,
            input: Box::new(LogicalPlan::Aggregate {
                group_keys,
                items,
                graph_keys: GraphGroupKeys::Slots,
                input: Box::new(input),
            }),
        };
    }
    for name in group_keys
        .iter_mut()
        .map(|key| &mut key.name)
        .chain(items.iter_mut().map(|item| &mut item.name))
    {
        if let Some(renamed) = renames.remove(name.as_str()) {
            *name = renamed;
        }
    }
    LogicalPlan::Aggregate {
        group_keys,
        items,
        graph_keys: GraphGroupKeys::Slots,
        input: Box::new(input),
    }
}
