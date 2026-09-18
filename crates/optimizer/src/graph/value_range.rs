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

use hawdb_core::Value;
use hawdb_plan::ComparisonOp;

pub(super) type ValueRangeBound = (Value, bool);
pub(super) type ValueRangeBounds = (Option<ValueRangeBound>, Option<ValueRangeBound>);

pub(super) fn merge_lower_bound(
    current: &mut Option<ValueRangeBound>,
    candidate: Option<ValueRangeBound>,
) {
    let Some(candidate) = candidate else {
        return;
    };
    let Some(existing) = current else {
        *current = Some(candidate);
        return;
    };
    match comparable_value_ordering(&candidate.0, &existing.0) {
        Some(std::cmp::Ordering::Greater) => *existing = candidate,
        Some(std::cmp::Ordering::Equal) => existing.1 = existing.1 && candidate.1,
        Some(std::cmp::Ordering::Less) | None => {}
    }
}

pub(super) fn merge_upper_bound(
    current: &mut Option<ValueRangeBound>,
    candidate: Option<ValueRangeBound>,
) {
    let Some(candidate) = candidate else {
        return;
    };
    let Some(existing) = current else {
        *current = Some(candidate);
        return;
    };
    match comparable_value_ordering(&candidate.0, &existing.0) {
        Some(std::cmp::Ordering::Less) => *existing = candidate,
        Some(std::cmp::Ordering::Equal) => existing.1 = existing.1 && candidate.1,
        Some(std::cmp::Ordering::Greater) | None => {}
    }
}

pub(super) fn range_bounds_for_comparison(op: ComparisonOp, value: Value) -> ValueRangeBounds {
    match op {
        ComparisonOp::Lt => (None, Some((value, false))),
        ComparisonOp::Lte => (None, Some((value, true))),
        ComparisonOp::Gt => (Some((value, false)), None),
        ComparisonOp::Gte => (Some((value, true)), None),
    }
}

pub(super) fn compare_histogram_value(candidate: &Value, op: ComparisonOp, value: &Value) -> bool {
    let Some(ordering) = comparable_value_ordering(candidate, value) else {
        return false;
    };
    match op {
        ComparisonOp::Lt => ordering == std::cmp::Ordering::Less,
        ComparisonOp::Lte => ordering != std::cmp::Ordering::Greater,
        ComparisonOp::Gt => ordering == std::cmp::Ordering::Greater,
        ComparisonOp::Gte => ordering != std::cmp::Ordering::Less,
    }
}

pub(super) fn range_bound_matches(
    value: &Value,
    lower: Option<&ValueRangeBound>,
    upper: Option<&ValueRangeBound>,
) -> bool {
    if let Some((bound, inclusive)) = lower {
        let Some(ordering) = comparable_value_ordering(value, bound) else {
            return false;
        };
        if ordering == std::cmp::Ordering::Less
            || (ordering == std::cmp::Ordering::Equal && !inclusive)
        {
            return false;
        }
    }
    if let Some((bound, inclusive)) = upper {
        let Some(ordering) = comparable_value_ordering(value, bound) else {
            return false;
        };
        if ordering == std::cmp::Ordering::Greater
            || (ordering == std::cmp::Ordering::Equal && !inclusive)
        {
            return false;
        }
    }
    true
}

fn comparable_value_ordering(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Int(left), Value::Int(right)) => Some(left.cmp(right)),
        (Value::Float(left), Value::Float(right)) => Some(left.total_cmp(right)),
        (Value::Int(left), Value::Float(right)) => Some((*left as f64).total_cmp(right)),
        (Value::Float(left), Value::Int(right)) => Some(left.total_cmp(&(*right as f64))),
        (Value::String(left), Value::String(right)) => Some(left.cmp(right)),
        _ => None,
    }
}
