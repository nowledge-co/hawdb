// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Statistical property lineage for pure scalar projections. This does not
//! prove keys, non-NULL values, constants or safe logical rewrites.

use super::{record_merge_entry, PhysicalPlan, PlanBindings};
use hawdb_plan_cypher::{Projection, ProjectionExpression};
use std::collections::BTreeMap;

type Columns<'a> = BTreeMap<&'a str, Option<(&'a str, &'a str)>>;

pub(super) fn merge_columns<'a>(left: &mut Columns<'a>, mut right: Columns<'a>) {
    if left.len() < right.len() {
        std::mem::swap(left, &mut right);
    }
    for (column, source) in right {
        record_merge_entry();
        left.entry(column)
            .and_modify(|previous| {
                if *previous != source {
                    *previous = None;
                }
            })
            .or_insert(source);
    }
}

impl<'a> PlanBindings<'a> {
    pub(in crate::graph) fn property_source<'b>(
        &'b self,
        expression: &'b ProjectionExpression,
    ) -> Option<(&'b str, &'b str)> {
        match expression {
            ProjectionExpression::Property { variable, property } => Some((variable, property)),
            ProjectionExpression::Column(column) => self
                .property_columns
                .get(column.as_str())
                .copied()
                .flatten(),
            _ => None,
        }
    }

    fn projected_columns(&self, items: &'a [Projection]) -> Columns<'a> {
        let mut columns = Columns::new();
        for item in items {
            let source = match &item.expression {
                ProjectionExpression::Property { variable, property } => {
                    Some((variable.as_str(), property.as_str()))
                }
                ProjectionExpression::Column(column) => self
                    .property_columns
                    .get(column.as_str())
                    .copied()
                    .flatten(),
                _ => None,
            };
            // Duplicate output names never pick an arbitrary source, including
            // one property alias colliding with a computed expression.
            columns
                .entry(item.name.as_str())
                .and_modify(|previous| *previous = None)
                .or_insert(source);
        }
        columns
    }

    pub(super) fn advance_columns(&mut self, plan: &'a PhysicalPlan) {
        match plan {
            PhysicalPlan::ProjectExec { items, .. }
            | PhysicalPlan::NodeProjectionScanExec { items, .. } => {
                self.property_columns = self.projected_columns(items)
            }
            PhysicalPlan::AggregateExec {
                group_keys, items, ..
            } => {
                let mut columns = self.projected_columns(group_keys);
                for item in items {
                    columns.insert(item.name.as_str(), None);
                }
                self.property_columns = columns;
            }
            _ => {}
        }
    }
}
