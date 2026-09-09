use super::super::{OptimizerCatalog, PhysicalPlan};
use skein_plan::NodeProjectionAccess;
use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod oracle;

#[cfg(test)]
mod fixtures;

#[derive(Default)]
pub(in crate::graph) struct PlanBindings<'a> {
    node_labels: BTreeMap<&'a str, &'a str>,
    relationship_types: BTreeMap<&'a str, &'a str>,
    node_populations: BTreeMap<&'a str, NodePopulation<'a>>,
    covered_properties: BTreeSet<(&'a str, &'a str)>,
}

enum NodePopulation<'a> {
    #[cfg(test)]
    OracleCount(u64),
    Label(&'a str),
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
            merge_coverage(&mut bindings.covered_properties, right.covered_properties);
        }

        match plan {
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
                bindings
                    .node_populations
                    .insert(variable, NodePopulation::Label(label));
            }
            PhysicalPlan::NodeColumnLookupExec {
                variable, label, ..
            } => {
                // The original label/type lookups stop here, whereas population
                // and coverage lookups continue through the input.
                bindings.node_labels.clear();
                bindings.node_labels.insert(variable, label);
                bindings.relationship_types.clear();
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
                bindings.covered_properties.insert((variable, property));
            }
            PhysicalPlan::IndexNodeCompositeSeek {
                variable,
                predicates,
                ..
            } => {
                bindings.covered_properties.extend(
                    predicates
                        .iter()
                        .map(|(property, _)| (variable.as_str(), property.as_str())),
                );
            }
            PhysicalPlan::IndexNodeCompositeRangeSeek { variable, seek, .. } => {
                bindings
                    .covered_properties
                    .insert((variable, &seek.range_property));
                bindings.covered_properties.extend(
                    seek.equality_prefix
                        .iter()
                        .map(|(property, _)| (variable.as_str(), property.as_str())),
                );
            }
            PhysicalPlan::IndexNodeUnionSeek {
                variable, branches, ..
            } => {
                bindings.covered_properties.extend(
                    branches
                        .iter()
                        .map(|branch| (variable.as_str(), branch.property.as_str())),
                );
            }
            _ => {}
        }
        bindings
    }

    pub(in crate::graph) fn node_label(&self, variable: &str) -> Option<&'a str> {
        self.node_labels.get(variable).copied()
    }

    pub(in crate::graph) fn relationship_type(&self, variable: &str) -> Option<&'a str> {
        self.relationship_types.get(variable).copied()
    }

    pub(in crate::graph) fn covers_property(&self, variable: &str, property: &str) -> bool {
        self.covered_properties.contains(&(variable, property))
    }

    pub(in crate::graph) fn node_distinct_count(
        &self,
        variable: &str,
        catalog: &OptimizerCatalog,
    ) -> Option<u64> {
        match self.node_populations.get(variable)? {
            #[cfg(test)]
            NodePopulation::OracleCount(count) => Some(*count),
            NodePopulation::Label(label) => Some(catalog.label_count(label)),
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

fn merge_coverage<K: Ord>(left: &mut BTreeSet<K>, mut right: BTreeSet<K>) {
    if left.len() < right.len() {
        std::mem::swap(left, &mut right);
    }
    for key in right {
        record_merge_entry();
        left.insert(key);
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
