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

use super::super::PhysicalPlan;
use hawdb_plan::{ComparisonOp, LogicalPlan, Predicate};

pub(super) fn lower(logical: &LogicalPlan) -> Option<PhysicalPlan> {
    match logical {
        LogicalPlan::NodeScan { variable, label } => Some(PhysicalPlan::SeqNodeScan {
            variable: variable.clone(),
            label: label.clone(),
        }),
        _ => None,
    }
}

/// Recognizes the Source filter shape that can use a checkpoint-published
/// storage sidecar. The original FilterExec remains above this operator, so
/// unsupported terms are never silently dropped.
pub(super) fn source_segment_scan_from_filter(
    predicate: &Predicate,
    input: &LogicalPlan,
) -> Option<PhysicalPlan> {
    let LogicalPlan::NodeScan { variable, label } = input else {
        return None;
    };
    (label == "Source" && contains_storage_prunable_term(predicate, variable)).then(|| {
        PhysicalPlan::SourceSegmentScan {
            variable: variable.clone(),
            predicate: predicate.clone(),
        }
    })
}

fn contains_storage_prunable_term(predicate: &Predicate, variable: &str) -> bool {
    match predicate {
        Predicate::And(predicates) => predicates
            .iter()
            .any(|predicate| contains_storage_prunable_term(predicate, variable)),
        Predicate::Or(predicates) => {
            !predicates.is_empty()
                && predicates
                    .iter()
                    .all(|predicate| contains_storage_prunable_term(predicate, variable))
        }
        Predicate::PropertyEq {
            variable: candidate,
            ..
        }
        | Predicate::PropertyIn {
            variable: candidate,
            ..
        }
        | Predicate::PropertyIsNull {
            variable: candidate,
            ..
        } => candidate == variable,
        Predicate::PropertyCompare {
            variable: candidate,
            op,
            ..
        } => {
            candidate == variable
                && matches!(
                    op,
                    ComparisonOp::Lt | ComparisonOp::Lte | ComparisonOp::Gt | ComparisonOp::Gte
                )
        }
        _ => false,
    }
}
