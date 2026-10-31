// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Bounded retained-shape metadata in the existing cost fold. Generic Project
//! retains graph bindings; fused projection scans return values alone.

use super::{width, OptimizerCatalog, PhysicalPlan};
use crate::graph::cardinality::PlanBindings;
use hawdb_core::TableKind;
use hawdb_plan_cypher::{Projection, ProjectionExpression, SortItem, SortKey};
use std::collections::BTreeMap;

const MAX_COLUMNS: usize = 64;
const BINDING_HEADER_BYTES: u64 = 128;
const OPAQUE_ROW_BYTES: u64 = 4096;

#[derive(Default)]
pub(super) struct RowShape<'p> {
    pub(super) graph_bytes: u64,
    graph_proxies: usize,
    graph_samples: usize,
    columns: BTreeMap<&'p str, width::FieldWidth>,
    extra_value_bytes: u64,
}

impl<'p> RowShape<'p> {
    #[cfg(test)]
    pub(super) fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub(super) fn clone_work_units(&self) -> u64 {
        self.columns
            .values()
            .filter_map(|width| width.copy_units())
            .fold(0u64, u64::saturating_add)
    }

    pub(super) fn values_bytes(&self) -> u64 {
        self.columns.iter().fold(
            (std::mem::size_of::<BTreeMap<String, hawdb_core::Value>>() as u64)
                .saturating_add(self.extra_value_bytes),
            |bytes, (name, width)| bytes.saturating_add(width::field_cell_bytes(name, *width)),
        )
    }

    pub(super) fn retained_bytes(&self) -> u64 {
        BINDING_HEADER_BYTES
            .saturating_add(self.graph_bytes)
            .saturating_add(self.values_bytes())
    }

    pub(super) fn uncertain_fields(&self) -> usize {
        self.columns
            .values()
            .filter(|width| {
                matches!(
                    width.source,
                    width::WidthSource::Unknown | width::WidthSource::TypeProxy
                )
            })
            .count()
            + usize::from(self.extra_value_bytes != 0)
            + self.graph_proxies
    }

    pub(super) fn sample_values(&self) -> usize {
        self.graph_samples
            + self
                .columns
                .values()
                .map(|width| width.samples)
                .sum::<usize>()
    }

    fn expression_width(
        &self,
        expression: &ProjectionExpression,
        catalog: &OptimizerCatalog,
        bindings: Option<&PlanBindings<'_>>,
        scan: Option<(&str, &str)>,
    ) -> width::FieldWidth {
        match expression {
            ProjectionExpression::Literal(value) => width::literal_width(value),
            ProjectionExpression::Column(column) => self
                .columns
                .get(column.as_str())
                .copied()
                .unwrap_or_else(width::unknown_width),
            ProjectionExpression::Id { .. } => {
                width::FieldWidth::fixed(0, width::WidthSource::DeclaredScalar)
            }
            ProjectionExpression::Property { variable, property } => {
                let label = scan
                    .filter(|(candidate, _)| *candidate == variable)
                    .map(|(_, label)| label)
                    .or_else(|| bindings.and_then(|bindings| bindings.node_label(variable)));
                label.map_or_else(width::unknown_width, |label| {
                    width::field_width(catalog, TableKind::Node, label, property)
                })
            }
            // Expression types with data-dependent sizes retain an explicit
            // proxy; resolving their values is execution's responsibility.
            _ => width::unknown_width(),
        }
    }

    fn project(
        &self,
        items: &'p [Projection],
        catalog: &OptimizerCatalog,
        bindings: Option<&PlanBindings<'_>>,
        scan: Option<(&str, &str)>,
    ) -> Self {
        let mut output = Self {
            graph_bytes: self.graph_bytes,
            graph_proxies: self.graph_proxies,
            graph_samples: self.graph_samples,
            ..Self::default()
        };
        for item in items.iter().take(MAX_COLUMNS) {
            output.columns.insert(
                &item.name,
                self.expression_width(&item.expression, catalog, bindings, scan),
            );
        }
        output.extra_value_bytes = (items.len().saturating_sub(MAX_COLUMNS) as u64)
            .saturating_mul(width::field_cell_bytes("", width::unknown_width()));
        output
    }

    pub(super) fn for_operator(
        plan: &'p PhysicalPlan,
        catalog: &OptimizerCatalog,
        mut inputs: [Option<Self>; 2],
        bindings: Option<&PlanBindings<'_>>,
    ) -> Self {
        let has_source = inputs[0].is_some();
        let mut shape = inputs[0].take().unwrap_or_default();
        match plan {
            PhysicalPlan::NodeProjectionScanExec {
                variable,
                label,
                required_properties,
                items,
                ..
            } => {
                if items.is_empty() {
                    shape.graph_bytes = 32;
                    for property in required_properties.iter().take(MAX_COLUMNS) {
                        let field = width::field_width(catalog, TableKind::Node, label, property);
                        shape.graph_bytes = shape
                            .graph_bytes
                            .saturating_add(width::field_cell_bytes(property, field));
                        shape.graph_proxies += usize::from(matches!(
                            field.source,
                            width::WidthSource::Unknown | width::WidthSource::TypeProxy
                        ));
                        shape.graph_samples += field.samples;
                    }
                    shape.graph_proxies += usize::from(required_properties.len() > MAX_COLUMNS);
                    shape.graph_bytes = shape.graph_bytes.saturating_add(
                        (required_properties.len().saturating_sub(MAX_COLUMNS) as u64)
                            .saturating_mul(width::field_cell_bytes("", width::unknown_width())),
                    );
                    shape
                } else {
                    Self::default().project(items, catalog, None, Some((variable, label)))
                }
            }
            PhysicalPlan::SeqNodeScan { label, .. }
            | PhysicalPlan::IndexNodeSeek { label, .. }
            | PhysicalPlan::IndexNodeMultiSeek { label, .. }
            | PhysicalPlan::IndexNodeUnionSeek { label, .. }
            | PhysicalPlan::IndexNodeCompositeSeek { label, .. }
            | PhysicalPlan::IndexNodeCompositeRangeSeek { label, .. }
            | PhysicalPlan::IndexNodeRangeSeek { label, .. }
            | PhysicalPlan::IndexNodeTextSeek { label, .. } => {
                let record = width::record_width(catalog, TableKind::Node, label);
                shape.graph_bytes = record.bytes;
                shape.graph_proxies = record.proxies;
                shape.graph_samples = record.samples;
                shape
            }
            PhysicalPlan::ProjectExec { items, .. } => {
                shape.project(items, catalog, bindings, None)
            }
            PhysicalPlan::NodeColumnLookupExec { label, .. } => {
                let record = width::record_width(catalog, TableKind::Node, label);
                shape.graph_bytes = shape.graph_bytes.saturating_add(record.bytes);
                shape.graph_proxies += record.proxies;
                shape.graph_samples += record.samples;
                shape
            }
            PhysicalPlan::NodeCartesianProductExec { .. } | PhysicalPlan::HashJoinExec { .. } => {
                if let Some(right) = inputs[1].take() {
                    shape.graph_bytes = shape.graph_bytes.saturating_add(right.graph_bytes);
                    shape.graph_proxies += right.graph_proxies;
                    shape.graph_samples += right.graph_samples;
                    shape.extra_value_bytes = shape
                        .extra_value_bytes
                        .saturating_add(right.extra_value_bytes);
                    for (name, field) in right.columns {
                        if shape.columns.contains_key(name) || shape.columns.len() < MAX_COLUMNS {
                            shape.columns.insert(name, field);
                        } else {
                            shape.extra_value_bytes = shape
                                .extra_value_bytes
                                .saturating_add(width::field_cell_bytes(name, field));
                        }
                    }
                }
                shape
            }
            PhysicalPlan::AggregateExec {
                group_keys, items, ..
            } => {
                let mut output = shape.project(group_keys, catalog, bindings, None);
                output.graph_bytes = 0;
                output.graph_proxies = 0;
                output.graph_samples = 0;
                // Accumulator/result payloads can be data dependent (e.g.
                // collect). Keep an opaque component until their state model
                // supplies a more specific estimate.
                output.extra_value_bytes = output.extra_value_bytes.saturating_add(
                    (items.len() as u64)
                        .saturating_mul(width::field_cell_bytes("", width::unknown_width())),
                );
                output
            }
            PhysicalPlan::AdjacencyExpandExec { .. } | PhysicalPlan::GraphMatchExec { .. } => {
                shape.graph_bytes = shape.graph_bytes.saturating_add(OPAQUE_ROW_BYTES);
                shape.graph_proxies += 1;
                shape
            }
            _ if !has_source => {
                shape.extra_value_bytes = OPAQUE_ROW_BYTES;
                shape
            }
            _ => shape,
        }
    }

    pub(super) fn sort_key_bytes(
        &self,
        items: &[SortItem],
        catalog: &OptimizerCatalog,
        bindings: &PlanBindings<'_>,
    ) -> (u64, u64) {
        let mut retained = std::mem::size_of::<
            Vec<(hawdb_core::Value, hawdb_plan_cypher::SortDirection)>,
        >() as u64;
        let mut comparison = 1u64; // ordinal tie breaker
        for item in items.iter().take(MAX_COLUMNS) {
            let field = match &item.key {
                SortKey::Expression(expression) => {
                    self.expression_width(expression, catalog, Some(bindings), None)
                }
                SortKey::Column(name) => self
                    .columns
                    .get(name.as_str())
                    .copied()
                    .unwrap_or_else(width::unknown_width),
                SortKey::Property { variable, property } => bindings
                    .node_label(variable)
                    .map_or_else(width::unknown_width, |label| {
                        width::field_width(catalog, TableKind::Node, label, property)
                    }),
                SortKey::Id { .. } => {
                    width::FieldWidth::fixed(0, width::WidthSource::DeclaredScalar)
                }
            };
            retained = retained
                .saturating_add(std::mem::size_of::<(
                    hawdb_core::Value,
                    hawdb_plan_cypher::SortDirection,
                )>() as u64)
                .saturating_add(field.payload_bytes);
            comparison = comparison
                .saturating_add(1)
                .saturating_add(field.payload_bytes.div_ceil(64));
        }
        let extra = items.len().saturating_sub(MAX_COLUMNS) as u64;
        (
            retained.saturating_add(extra.saturating_mul(128)),
            comparison.saturating_add(extra.saturating_mul(2)),
        )
    }
}
