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
use hawdb_plan_cypher::ComparisonOp;

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
        (Value::Decimal(_), Value::Decimal(_) | Value::Int(_))
        | (Value::Int(_), Value::Decimal(_)) => {
            hawdb_core::numeric::compare_numeric(left.as_ref(), right.as_ref()).ok()
        }
        (Value::Int(left), Value::Int(right)) => Some(left.cmp(right)),
        (Value::Float(left), Value::Float(right)) => Some(left.total_cmp(right)),
        (Value::Int(left), Value::Float(right)) => Some((*left as f64).total_cmp(right)),
        (Value::Float(left), Value::Int(right)) => Some(left.total_cmp(&(*right as f64))),
        (Value::String(left), Value::String(right)) => Some(left.cmp(right)),
        // Same-kind temporal instants; durations and mixed kinds are NULL.
        _ => hawdb_core::temporal::comparable_ordering(left, right),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_and_integer_bounds_keep_the_strongest_exact_constraint() {
        let decimal = Value::Decimal("9007199254740993.01".parse().unwrap());
        let integer = Value::Int(9_007_199_254_740_993);
        for candidates in [
            [integer.clone(), decimal.clone()],
            [decimal.clone(), integer.clone()],
        ] {
            let mut lower = None;
            let mut upper = None;
            for candidate in candidates {
                merge_lower_bound(&mut lower, Some((candidate.clone(), true)));
                merge_upper_bound(&mut upper, Some((candidate, true)));
            }
            assert_eq!(lower, Some((decimal.clone(), true)));
            assert_eq!(upper, Some((integer.clone(), true)));
        }
        let mut lower = Some((Value::Int(1), true));
        merge_lower_bound(
            &mut lower,
            Some((Value::Decimal("1.000".parse().unwrap()), false)),
        );
        assert_eq!(lower, Some((Value::Int(1), false)));
        assert!(!range_bound_matches(&Value::Int(1), lower.as_ref(), None));
        assert!(range_bound_matches(&decimal, lower.as_ref(), None));
        assert_eq!(
            comparable_value_ordering(&decimal, &Value::Float(1.0)),
            None
        );
    }
}
