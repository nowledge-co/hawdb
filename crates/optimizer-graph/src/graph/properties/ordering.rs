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
    sort_ordering_keys, PhysicalPlan, PhysicalProperties, SortDirection, SortItem, SortKey,
};
use crate::{OptimizerCatalog, RequiredProperties};
use hawdb_plan_cypher::{NodeProjectionAccess, Projection, ProjectionExpression};
use std::collections::BTreeSet;

fn unique_projection_names(items: &[Projection]) -> bool {
    let mut names = BTreeSet::new();
    items.iter().all(|item| names.insert(&item.name))
}

fn canonical_key(key: &SortKey) -> SortKey {
    match key {
        SortKey::Expression(ProjectionExpression::Property { variable, property }) => {
            SortKey::Property {
                variable: variable.clone(),
                property: property.clone(),
            }
        }
        SortKey::Expression(ProjectionExpression::Column(column)) => {
            SortKey::Column(column.clone())
        }
        _ => key.clone(),
    }
}

pub(super) fn selected_ordering(plan: &PhysicalPlan, catalog: &OptimizerCatalog) -> Vec<SortItem> {
    match plan {
        PhysicalPlan::IndexNodeRangeSeek {
            variable,
            label,
            property,
            ..
        } if catalog
            .ordered_range_indexes
            .contains(&(label.clone(), property.clone())) =>
        {
            vec![SortItem {
                key: SortKey::Property {
                    variable: variable.clone(),
                    property: property.clone(),
                },
                direction: SortDirection::Asc,
            }]
        }
        PhysicalPlan::NodeProjectionScanExec {
            variable,
            label,
            access: NodeProjectionAccess::PropertyRange { property, .. },
            items,
            ..
        } if catalog
            .ordered_range_indexes
            .contains(&(label.clone(), property.clone())) =>
        {
            // A fused projection emits scalar values only; it no longer has
            // the native node binding advertised by the unfused range scan.
            let key = if items.is_empty() {
                Some(SortKey::Property {
                    variable: variable.clone(),
                    property: property.clone(),
                })
            } else if !unique_projection_names(items) {
                // Execution suffixes duplicate aliases, including collisions
                // with explicit "#2" names. Do not infer those scalar keys.
                None
            } else {
                items.iter().find(|item| {
                    matches!(&item.expression, ProjectionExpression::Property { variable: source, property: key } if source == variable && key == property)
                }).map(|item| SortKey::Column(item.name.clone()))
            };
            key.into_iter()
                .map(|key| SortItem {
                    key,
                    direction: SortDirection::Asc,
                })
                .collect()
        }
        PhysicalPlan::FilterExec { input, .. } | PhysicalPlan::LimitExec { input, .. } => {
            selected_ordering(input, catalog)
        }
        PhysicalPlan::ProjectExec { items, input } => {
            let mut ordering = Vec::new();
            for mut key in selected_ordering(input, catalog) {
                if let SortKey::Column(column) = &key.key {
                    if !unique_projection_names(items) {
                        break;
                    }
                    // Projection replaces scalar values, but retains native node
                    // bindings. Only an identity column survives.
                    let Some(item) = items.iter().find(|item| {
                        matches!(&item.expression, ProjectionExpression::Column(source) if source == column)
                    }) else { break };
                    key.key = SortKey::Column(item.name.clone());
                } else if !matches!(key.key, SortKey::Property { .. } | SortKey::Id { .. }) {
                    break;
                }
                ordering.push(key);
            }
            ordering
        }
        PhysicalPlan::SortExec { items, .. } | PhysicalPlan::TopNExec { items, .. } => items
            .iter()
            .map(|item| SortItem {
                key: canonical_key(&item.key),
                direction: item.direction,
            })
            .collect(),
        // Lookups can replace a native binding. Expansion, products,
        // aggregation and composite access also have no ordering guarantee.
        _ => Vec::new(),
    }
}

pub(in crate::graph) fn satisfies_ordering(
    plan: &PhysicalPlan,
    required: &[SortItem],
    catalog: &OptimizerCatalog,
) -> bool {
    match plan {
        PhysicalPlan::FilterExec { input, .. } | PhysicalPlan::LimitExec { input, .. } => {
            return satisfies_ordering(input, required, catalog)
        }
        PhysicalPlan::ProjectExec { items, input } => {
            let unique_names = unique_projection_names(items);
            let remapped: Option<Vec<_>> = required
                .iter()
                .map(|item| {
                    let key = match canonical_key(&item.key) {
                        SortKey::Expression(ProjectionExpression::ColumnProperty {
                            column,
                            property,
                        }) => {
                            if !unique_names {
                                return None;
                            }
                            let projection = items.iter().rev().find(|item| item.name == column)?;
                            match &projection.expression {
                                ProjectionExpression::Variable { variable }
                                    if !matches!(
                                        property.as_str(),
                                        "_id" | "labels" | "source_id" | "target_id" | "type"
                                    ) =>
                                {
                                    SortKey::Property {
                                        variable: variable.clone(),
                                        property,
                                    }
                                }
                                ProjectionExpression::Column(source) => {
                                    SortKey::Expression(ProjectionExpression::ColumnProperty {
                                        column: source.clone(),
                                        property,
                                    })
                                }
                                _ => return None,
                            }
                        }
                        SortKey::Column(column) => {
                            if !unique_names {
                                return None;
                            }
                            let projection = items.iter().rev().find(|item| item.name == column)?;
                            match &projection.expression {
                                ProjectionExpression::Property { variable, property } => {
                                    SortKey::Property {
                                        variable: variable.clone(),
                                        property: property.clone(),
                                    }
                                }
                                ProjectionExpression::Column(column) => {
                                    SortKey::Column(column.clone())
                                }
                                _ => return None,
                            }
                        }
                        key @ (SortKey::Property { .. } | SortKey::Id { .. }) => key,
                        _ => return None,
                    };
                    Some(SortItem {
                        key,
                        direction: item.direction,
                    })
                })
                .collect();
            return remapped.is_some_and(|required| satisfies_ordering(input, &required, catalog));
        }
        _ => {}
    }
    let provided = selected_ordering(plan, catalog);
    let required: Vec<_> = required
        .iter()
        .map(|item| SortItem {
            key: canonical_key(&item.key),
            direction: item.direction,
        })
        .collect();
    // Report strings are not identifiers: e.g. dotted/backtick names can
    // render alike. Establish typed key and direction equality first.
    if !provided.starts_with(&required) {
        return false;
    }
    PhysicalProperties {
        ordering: sort_ordering_keys(&provided),
        ..PhysicalProperties::default()
    }
    .satisfies(&RequiredProperties {
        ordering: sort_ordering_keys(&required),
        ..RequiredProperties::default()
    })
}
