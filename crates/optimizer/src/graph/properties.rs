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
    Distribution, MemoryBudgetClass, PhysicalPlan, PhysicalProperties, ScanPruningSupport,
    VectorPrecision,
};
use hawdb_plan::write_projection_expression;
use hawdb_plan::{SortDirection, SortItem, SortKey};

pub(super) fn selected_plan_properties(plan: &PhysicalPlan) -> PhysicalProperties {
    match plan {
        PhysicalPlan::EmptyExec => PhysicalProperties {
            distribution: Distribution::Single,
            scan_pruning: ScanPruningSupport::ExactEmpty,
            vector_precision: VectorPrecision::NotVector,
            memory_budget: MemoryBudgetClass::Constant,
            ..PhysicalProperties::default()
        },
        PhysicalPlan::NodeCountExec { .. } | PhysicalPlan::RelationshipCountExec { .. } => {
            PhysicalProperties {
                distribution: Distribution::Single,
                vector_precision: VectorPrecision::NotVector,
                memory_budget: MemoryBudgetClass::Constant,
                ..PhysicalProperties::default()
            }
        }
        PhysicalPlan::FilterExec { input, .. }
        | PhysicalPlan::ProjectExec { input, .. }
        | PhysicalPlan::LimitExec { input, .. }
        | PhysicalPlan::NodeColumnLookupExec { input, .. }
        | PhysicalPlan::AdjacencyExpandExec { input, .. }
        | PhysicalPlan::AdjacencyExistsExec { input, .. }
        | PhysicalPlan::OptionalDegreeExec { input, .. } => selected_plan_properties(input),
        PhysicalPlan::SortExec { items, input } => {
            let mut properties = selected_plan_properties(input);
            properties.ordering = sort_ordering_keys(items);
            properties.memory_budget = MemoryBudgetClass::Blocking;
            properties
        }
        PhysicalPlan::TopNExec { items, input, .. } => {
            let mut properties = selected_plan_properties(input);
            properties.ordering = sort_ordering_keys(items);
            properties.memory_budget = MemoryBudgetClass::RowLinear;
            properties
        }
        PhysicalPlan::AggregateExec { input, .. } | PhysicalPlan::DistinctExec { input } => {
            let mut properties = selected_plan_properties(input);
            properties.ordering.clear();
            properties.memory_budget = MemoryBudgetClass::Blocking;
            properties
        }
        PhysicalPlan::SeqNodeScan { .. } => PhysicalProperties {
            distribution: Distribution::Single,
            scan_pruning: ScanPruningSupport::Label,
            vector_precision: VectorPrecision::NotVector,
            memory_budget: MemoryBudgetClass::RowLinear,
            ..PhysicalProperties::default()
        },
        PhysicalPlan::NodeProjectionScanExec {
            label,
            access,
            required_properties,
            ..
        } => {
            let scan_pruning = if access.is_label_scan() {
                ScanPruningSupport::Label
            } else {
                ScanPruningSupport::Index
            };
            PhysicalProperties {
                distribution: Distribution::Single,
                covering_fields: required_properties
                    .iter()
                    .map(|property| plan_property_key(label, property))
                    .collect(),
                scan_pruning,
                vector_precision: VectorPrecision::NotVector,
                memory_budget: MemoryBudgetClass::RowLinear,
                ..PhysicalProperties::default()
            }
        }
        PhysicalPlan::SourceSegmentScan { .. } => PhysicalProperties {
            distribution: Distribution::Single,
            scan_pruning: ScanPruningSupport::Segment,
            vector_precision: VectorPrecision::NotVector,
            memory_budget: MemoryBudgetClass::RowLinear,
            ..PhysicalProperties::default()
        },
        PhysicalPlan::IndexNodeSeek {
            label, property, ..
        }
        | PhysicalPlan::IndexNodeMultiSeek {
            label, property, ..
        }
        | PhysicalPlan::IndexNodeRangeSeek {
            label, property, ..
        }
        | PhysicalPlan::IndexNodeTextSeek {
            label, property, ..
        } => PhysicalProperties {
            distribution: Distribution::Single,
            covering_fields: vec![plan_property_key(label, property)],
            scan_pruning: ScanPruningSupport::Index,
            vector_precision: VectorPrecision::NotVector,
            memory_budget: MemoryBudgetClass::RowLinear,
            ..PhysicalProperties::default()
        },
        PhysicalPlan::IndexNodeCompositeSeek {
            label, predicates, ..
        } => PhysicalProperties {
            distribution: Distribution::Single,
            covering_fields: predicates
                .iter()
                .map(|(property, _)| plan_property_key(label, property))
                .collect(),
            scan_pruning: ScanPruningSupport::Index,
            vector_precision: VectorPrecision::NotVector,
            memory_budget: MemoryBudgetClass::RowLinear,
            ..PhysicalProperties::default()
        },
        PhysicalPlan::IndexNodeCompositeRangeSeek { label, seek, .. } => PhysicalProperties {
            distribution: Distribution::Single,
            covering_fields: seek
                .equality_prefix
                .iter()
                .map(|(property, _)| plan_property_key(label, property))
                .chain(std::iter::once(plan_property_key(
                    label,
                    &seek.range_property,
                )))
                .collect(),
            scan_pruning: ScanPruningSupport::Index,
            vector_precision: VectorPrecision::NotVector,
            memory_budget: MemoryBudgetClass::RowLinear,
            ..PhysicalProperties::default()
        },
        PhysicalPlan::IndexNodeUnionSeek {
            label, branches, ..
        } => PhysicalProperties {
            distribution: Distribution::Single,
            covering_fields: branches
                .iter()
                .map(|branch| plan_property_key(label, &branch.property))
                .collect(),
            scan_pruning: ScanPruningSupport::Index,
            vector_precision: VectorPrecision::NotVector,
            memory_budget: MemoryBudgetClass::RowLinear,
            ..PhysicalProperties::default()
        },
        PhysicalPlan::NodeCartesianProductExec { left, right }
        | PhysicalPlan::HashJoinExec { left, right, .. } => {
            let left = selected_plan_properties(left);
            let right = selected_plan_properties(right);
            PhysicalProperties {
                distribution: Distribution::Single,
                covering_fields: left
                    .covering_fields
                    .into_iter()
                    .chain(right.covering_fields)
                    .collect(),
                scan_pruning: combine_scan_pruning(left.scan_pruning, right.scan_pruning),
                vector_precision: combine_vector_precision(
                    left.vector_precision,
                    right.vector_precision,
                ),
                memory_budget: if matches!(plan, PhysicalPlan::HashJoinExec { .. }) {
                    MemoryBudgetClass::Blocking
                } else {
                    combine_memory_budget(left.memory_budget, right.memory_budget)
                },
                ..PhysicalProperties::default()
            }
        }
        PhysicalPlan::OptionalRelationshipCountSumExec { .. }
        | PhysicalPlan::ThreadRepairStatsExec { .. }
        | PhysicalPlan::ShortestPathExec { .. } => PhysicalProperties {
            distribution: Distribution::Single,
            vector_precision: VectorPrecision::NotVector,
            memory_budget: MemoryBudgetClass::RowLinear,
            ..PhysicalProperties::default()
        },
        _ => PhysicalProperties {
            distribution: Distribution::Single,
            vector_precision: VectorPrecision::NotVector,
            memory_budget: MemoryBudgetClass::Unknown,
            ..PhysicalProperties::default()
        },
    }
}

fn plan_property_key(label: &str, property: &str) -> String {
    format!("{label}.{property}")
}

fn combine_scan_pruning(left: ScanPruningSupport, right: ScanPruningSupport) -> ScanPruningSupport {
    if left == ScanPruningSupport::ExactEmpty || right == ScanPruningSupport::ExactEmpty {
        ScanPruningSupport::ExactEmpty
    } else if left == ScanPruningSupport::Index || right == ScanPruningSupport::Index {
        ScanPruningSupport::Index
    } else if left == ScanPruningSupport::Segment || right == ScanPruningSupport::Segment {
        ScanPruningSupport::Segment
    } else if left == ScanPruningSupport::Label || right == ScanPruningSupport::Label {
        ScanPruningSupport::Label
    } else if left == ScanPruningSupport::None && right == ScanPruningSupport::None {
        ScanPruningSupport::None
    } else {
        ScanPruningSupport::Unknown
    }
}

fn combine_vector_precision(left: VectorPrecision, right: VectorPrecision) -> VectorPrecision {
    match (left, right) {
        (VectorPrecision::ApproximateCandidate, _) | (_, VectorPrecision::ApproximateCandidate) => {
            VectorPrecision::ApproximateCandidate
        }
        (VectorPrecision::RawReranked, _) | (_, VectorPrecision::RawReranked) => {
            VectorPrecision::RawReranked
        }
        (VectorPrecision::Exact, _) | (_, VectorPrecision::Exact) => VectorPrecision::Exact,
        _ => VectorPrecision::NotVector,
    }
}

fn combine_memory_budget(left: MemoryBudgetClass, right: MemoryBudgetClass) -> MemoryBudgetClass {
    match (left, right) {
        (MemoryBudgetClass::Blocking, _) | (_, MemoryBudgetClass::Blocking) => {
            MemoryBudgetClass::Blocking
        }
        (MemoryBudgetClass::RowLinear, _) | (_, MemoryBudgetClass::RowLinear) => {
            MemoryBudgetClass::RowLinear
        }
        (MemoryBudgetClass::Constant, _) | (_, MemoryBudgetClass::Constant) => {
            MemoryBudgetClass::Constant
        }
        _ => MemoryBudgetClass::Unknown,
    }
}

fn sort_ordering_keys(items: &[SortItem]) -> Vec<String> {
    items.iter().map(sort_ordering_key).collect()
}

fn sort_ordering_key(item: &SortItem) -> String {
    let mut output = String::new();
    match &item.key {
        SortKey::Property { variable, property } => {
            output.push_str(variable);
            output.push('.');
            output.push_str(property);
        }
        SortKey::Id { variable } => {
            output.push_str("id(");
            output.push_str(variable);
            output.push(')');
        }
        SortKey::Expression(expression) => write_projection_expression(&mut output, expression),
        SortKey::Column(column) => output.push_str(column),
    }
    output.push(' ');
    match item.direction {
        SortDirection::Asc => output.push_str("asc"),
        SortDirection::Desc => output.push_str("desc"),
    }
    output
}
