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
