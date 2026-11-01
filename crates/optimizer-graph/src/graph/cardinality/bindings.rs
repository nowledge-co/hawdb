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

use super::super::{OptimizerCatalog, PhysicalPlan};
use hawdb_core::Value;
use hawdb_plan_cypher::NodeProjectionAccess;
use std::collections::BTreeMap;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod oracle;

#[cfg(test)]
mod fixtures;

mod lineage;

#[derive(Default)]
pub(in crate::graph) struct PlanBindings<'a> {
    node_labels: BTreeMap<&'a str, &'a str>,
    relationship_types: BTreeMap<&'a str, &'a str>,
    node_populations: BTreeMap<&'a str, NodePopulation<'a>>,
    covered_properties: BTreeMap<(&'a str, &'a str), Option<&'a Value>>,
    property_columns: BTreeMap<&'a str, Option<(&'a str, &'a str)>>,
}

enum NodePopulation<'a> {
    #[cfg(test)]
    OracleCount(u64),
    Label {
        label: &'a str,
        joint_compatible: bool,
    },
    Expansion {
        plan: &'a PhysicalPlan,
        source: bool,
    },
}

impl<'a> PlanBindings<'a> {
    pub(in crate::graph) fn for_operator(
        plan: &'a PhysicalPlan,
        mut inputs: [Option<Self>; 2],
    ) -> Self {
        #[cfg(test)]
        super::METADATA_VISITS.with(|count| count.set(count.get() + 1));

        let mut bindings = inputs[0].take().unwrap_or_default();
        if let Some(right) = inputs[1].take() {
            merge_left_biased(&mut bindings.node_labels, right.node_labels);
            merge_left_biased(&mut bindings.relationship_types, right.relationship_types);
            merge_left_biased(&mut bindings.node_populations, right.node_populations);
            merge_left_biased(&mut bindings.covered_properties, right.covered_properties);
            lineage::merge_columns(&mut bindings.property_columns, right.property_columns);
        }

        match plan {
            PhysicalPlan::UnwindRowsExec { variable, .. } => {
                bindings.node_labels.remove(variable.as_str());
                bindings.node_populations.remove(variable.as_str());
                bindings.relationship_types.remove(variable.as_str());
                bindings
                    .covered_properties
                    .retain(|(name, _), _| *name != variable);
                bindings.property_columns.remove(variable.as_str());
            }
            PhysicalPlan::SeqNodeScan { variable, label }
            | PhysicalPlan::IndexNodeSeek {
                variable, label, ..
            }
            | PhysicalPlan::IndexNodeMultiSeek {
                variable, label, ..
            }
            | PhysicalPlan::IndexNodeUnionSeek {
                variable, label, ..
            }
            | PhysicalPlan::IndexNodeCompositeSeek {
                variable, label, ..
            }
            | PhysicalPlan::IndexNodeCompositeRangeSeek {
                variable, label, ..
            }
            | PhysicalPlan::IndexNodeRangeSeek {
                variable, label, ..
            }
            | PhysicalPlan::IndexNodeTextSeek {
                variable, label, ..
            }
            | PhysicalPlan::NodeProjectionScanExec {
                variable, label, ..
            } => {
                bindings.node_labels.insert(variable, label);
                bindings.node_populations.insert(
                    variable,
                    NodePopulation::Label {
                        label,
                        joint_compatible: true,
                    },
                );
            }
            PhysicalPlan::GraphMatchExec { program, .. } => {
                for variable in program
                    .introduced
                    .iter()
                    .chain(program.imports.iter().map(|import| &import.variable))
                {
                    bindings
                        .covered_properties
                        .retain(|(name, _), _| *name != variable);
                    bindings.node_labels.remove(variable.as_str());
                    bindings.node_populations.remove(variable.as_str());
                    bindings.relationship_types.remove(variable.as_str());
                }
                for step in &program.steps {
                    let node = match step {
                        // Path endpoints are bound by preceding Node steps.
                        hawdb_plan_cypher::GraphMatchStep::ShortestPath(_) => continue,
                        hawdb_plan_cypher::GraphMatchStep::Node(node) => node,
                        hawdb_plan_cypher::GraphMatchStep::Expand {
                            target,
                            relationship,
                            rel_type,
                            ..
                        } => {
                            if let Some(variable) = relationship {
                                bindings.relationship_types.insert(variable, rel_type);
                            }
                            target
                        }
                    };
                    bindings.node_labels.insert(&node.variable, &node.label);
                    bindings.node_populations.insert(
                        &node.variable,
                        NodePopulation::Label {
                            label: &node.label,
                            joint_compatible: false,
                        },
                    );
                }
            }
            PhysicalPlan::NodeColumnLookupExec {
                variable, label, ..
            } => {
                // The original label/type lookups stop here, whereas population
                // and coverage lookups continue through the input.
                bindings.node_labels.clear();
                bindings.node_labels.insert(variable, label);
                bindings.relationship_types.clear();
                if let Some(NodePopulation::Label {
                    joint_compatible, ..
                }) = bindings.node_populations.get_mut(variable.as_str())
                {
                    *joint_compatible = false;
                }
                for ((source, _), value) in bindings
                    .covered_properties
                    .range_mut((variable.as_str(), "")..)
                {
                    if *source != variable {
                        break;
                    }
                    *value = None;
                }
            }
            PhysicalPlan::AdjacencyExpandExec {
                source_variable,
                source_label,
                target_variable,
                target_label,
                rel_variable,
                rel_type,
                ..
            } => {
                // Source wins when both endpoints use the same variable.
                bindings.node_labels.insert(target_variable, target_label);
                bindings.node_labels.insert(source_variable, source_label);
                if target_variable != source_variable {
                    for ((source, _), value) in bindings
                        .covered_properties
                        .range_mut((target_variable.as_str(), "")..)
                    {
                        if *source != target_variable {
                            break;
                        }
                        *value = None;
                    }
                }
                bindings.node_populations.insert(
                    target_variable,
                    NodePopulation::Expansion {
                        plan,
                        source: false,
                    },
                );
                bindings.node_populations.insert(
                    source_variable,
                    NodePopulation::Expansion { plan, source: true },
                );
                if let Some(variable) = rel_variable {
                    bindings.relationship_types.insert(variable, rel_type);
                } else {
                    bindings.relationship_types.clear();
                }
            }
            _ => {}
        }

        match plan {
            PhysicalPlan::IndexNodeSeek {
                variable, property, ..
            }
            | PhysicalPlan::IndexNodeMultiSeek {
                variable, property, ..
            }
            | PhysicalPlan::IndexNodeRangeSeek {
                variable, property, ..
            }
            | PhysicalPlan::IndexNodeTextSeek {
                variable, property, ..
            }
            | PhysicalPlan::NodeProjectionScanExec {
                variable,
                access: NodeProjectionAccess::FullText { property, .. },
                ..
            } => {
                bindings
                    .covered_properties
                    .insert((variable, property), None);
            }
            PhysicalPlan::IndexNodeCompositeSeek {
                variable,
                predicates,
                ..
            } => {
                bindings.covered_properties.extend(
                    predicates
                        .iter()
                        .map(|(property, _)| ((variable.as_str(), property.as_str()), None)),
                );
            }
            PhysicalPlan::IndexNodeCompositeRangeSeek { variable, seek, .. } => {
                bindings
                    .covered_properties
                    .insert((variable, &seek.range_property), None);
                bindings.covered_properties.extend(
                    seek.equality_prefix
                        .iter()
                        .map(|(property, _)| ((variable.as_str(), property.as_str()), None)),
                );
            }
            PhysicalPlan::IndexNodeUnionSeek {
                variable, branches, ..
            } => {
                bindings.covered_properties.extend(
                    branches
                        .iter()
                        .map(|branch| ((variable.as_str(), branch.property.as_str()), None)),
                );
            }
            _ => {}
        }

        match plan {
            PhysicalPlan::IndexNodeSeek {
                variable,
                property,
                value,
                ..
            } => {
                bindings
                    .covered_properties
                    .insert((variable, property), Some(value));
            }
            PhysicalPlan::IndexNodeMultiSeek {
                variable,
                property,
                values,
                ..
            } => {
                if let [value] = values.as_slice() {
                    bindings
                        .covered_properties
                        .insert((variable, property), Some(value));
                }
            }
            PhysicalPlan::IndexNodeCompositeSeek {
                variable,
                predicates,
                ..
            } => {
                for (property, value) in predicates {
                    bindings
                        .covered_properties
                        .insert((variable, property), Some(value));
                }
            }
            PhysicalPlan::IndexNodeCompositeRangeSeek { variable, seek, .. } => {
                for (property, value) in &seek.equality_prefix {
                    bindings
                        .covered_properties
                        .insert((variable, property), Some(value));
                }
            }
            PhysicalPlan::NodeProjectionScanExec {
                variable, access, ..
            } => {
                match access {
                    NodeProjectionAccess::PropertyValues { property, values } => {
                        bindings
                            .covered_properties
                            .insert((variable, property), None);
                        if let [value] = values.as_slice() {
                            bindings
                                .covered_properties
                                .insert((variable, property), Some(value));
                        }
                    }
                    NodeProjectionAccess::CompositeEquality { predicates } => {
                        for (property, value) in predicates {
                            bindings
                                .covered_properties
                                .insert((variable, property), None);
                            bindings
                                .covered_properties
                                .insert((variable, property), Some(value));
                        }
                    }
                    NodeProjectionAccess::CompositeRange { seek } => {
                        bindings
                            .covered_properties
                            .insert((variable, &seek.range_property), None);
                        for (property, value) in &seek.equality_prefix {
                            bindings
                                .covered_properties
                                .insert((variable, property), None);
                            bindings
                                .covered_properties
                                .insert((variable, property), Some(value));
                        }
                    }
                    NodeProjectionAccess::CompositeOrdered { seek } => {
                        for (property, value) in &seek.equality_prefix {
                            bindings
                                .covered_properties
                                .insert((variable, property), None);
                            bindings
                                .covered_properties
                                .insert((variable, property), Some(value));
                        }
                    }
                    NodeProjectionAccess::PropertyRange { property, .. } => {
                        bindings
                            .covered_properties
                            .insert((variable, property), None);
                    }
                    // A union cannot individually prove each branch's key.
                    _ => {}
                }
            }
            _ => {}
        }
        bindings.advance_columns(plan);
        bindings
    }

    pub(in crate::graph) fn node_label(&self, variable: &str) -> Option<&'a str> {
        self.node_labels.get(variable).copied()
    }

    pub(in crate::graph) fn relationship_type(&self, variable: &str) -> Option<&'a str> {
        self.relationship_types.get(variable).copied()
    }

    pub(in crate::graph) fn covers_property(&self, variable: &str, property: &str) -> bool {
        self.covered_properties.contains_key(&(variable, property))
    }

    pub(in crate::graph) fn fixed_access_value(
        &self,
        variable: &str,
        property: &str,
    ) -> Option<&'a Value> {
        self.covered_properties
            .get(&(variable, property))
            .copied()
            .flatten()
    }

    pub(in crate::graph) fn has_label_population(&self, variable: &str) -> bool {
        matches!(
            self.node_populations.get(variable),
            Some(NodePopulation::Label {
                joint_compatible: true,
                ..
            })
        )
    }

    pub(in crate::graph) fn node_distinct_count(
        &self,
        variable: &str,
        catalog: &OptimizerCatalog,
    ) -> Option<u64> {
        match self.node_populations.get(variable)? {
            #[cfg(test)]
            NodePopulation::OracleCount(count) => Some(*count),
            NodePopulation::Label { label, .. } => Some(catalog.label_count(label)),
            NodePopulation::Expansion { plan, source } => {
                let PhysicalPlan::AdjacencyExpandExec {
                    source_label,
                    target_label,
                    rel_type,
                    min_hops,
                    max_hops,
                    ..
                } = plan
                else {
                    unreachable!("population binding must reference an expansion")
                };
                // Resolve path statistics only when a distinct aggregate uses
                // them; eagerly walking every hop would add work to plain scans.
                if *source {
                    catalog
                        .bounded_path_source_distinct_count(
                            source_label,
                            rel_type,
                            target_label,
                            *min_hops,
                            *max_hops,
                        )
                        .or_else(|| {
                            catalog.path_source_distinct_count(source_label, rel_type, target_label)
                        })
                        .or_else(|| Some(catalog.label_count(source_label)))
                } else {
                    catalog
                        .bounded_path_target_distinct_count(
                            source_label,
                            rel_type,
                            target_label,
                            *min_hops,
                            *max_hops,
                        )
                        .or_else(|| {
                            catalog.path_target_distinct_count(source_label, rel_type, target_label)
                        })
                        .or_else(|| Some(catalog.label_count(target_label)))
                }
            }
        }
    }

    #[cfg(test)]
    pub(in crate::graph) fn for_plan(plan: &'a PhysicalPlan) -> Self {
        let inputs = super::super::costing::estimate_input_costs(plan, Self::for_plan);
        Self::for_operator(plan, inputs)
    }
}

// Move only the smaller map. Unary nodes retain their maps without clones;
// repeatedly joining a large left tree to one leaf must not recopy that tree.
fn merge_left_biased<K: Ord, V>(left: &mut BTreeMap<K, V>, mut right: BTreeMap<K, V>) {
    if left.len() >= right.len() {
        for (key, value) in right {
            record_merge_entry();
            left.entry(key).or_insert(value);
        }
    } else {
        for (key, value) in std::mem::take(left) {
            record_merge_entry();
            right.insert(key, value);
        }
        *left = right;
    }
}

#[cfg(test)]
thread_local! {
    static MERGED_ENTRIES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn record_merge_entry() {
    #[cfg(test)]
    MERGED_ENTRIES.with(|count| count.set(count.get() + 1));
}
