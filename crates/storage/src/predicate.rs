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

//! Record predicates shared by graph mutations, execution, and persistent projections.

use crate::mutation::PropertyFilter;
use hawdb_core::Value;
use std::collections::BTreeMap;

/// Whether `filter` holds for a record: only a definite `true` matches.
pub fn property_filter_matches(
    filter: &PropertyFilter,
    id: u64,
    properties: &BTreeMap<String, Value>,
) -> bool {
    property_filter_truth(filter, id, properties) == Some(true)
}

/// The three-valued truth of `filter`, with `None` for Cypher's NULL.
///
/// A leaf is `Some(true)` exactly where it matches, `None` where Cypher
/// evaluation yields NULL (a missing or NULL property, a NULL operand, or an
/// ordering between two durations) and `Some(false)` otherwise. `And`, `Or`
/// and `Not` follow Kleene logic, so `Not` keeps NULL rows out just as the
/// executor's predicate evaluation does: `NOT n.d < $limit` is NULL, not
/// true, when `n.d` is missing or both sides are durations. Filters without
/// `Not` match exactly the same records as a two-valued evaluation.
pub fn property_filter_truth(
    filter: &PropertyFilter,
    id: u64,
    properties: &BTreeMap<String, Value>,
) -> Option<bool> {
    let property = |property: &str| properties.get(property).filter(|value| !is_null(value));
    match filter {
        PropertyFilter::And(filters) => {
            let mut truth = Some(true);
            for filter in filters {
                match property_filter_truth(filter, id, properties) {
                    Some(false) => return Some(false),
                    None => truth = None,
                    Some(true) => {}
                }
            }
            truth
        }
        PropertyFilter::Or(filters) => {
            let mut truth = Some(false);
            for filter in filters {
                match property_filter_truth(filter, id, properties) {
                    Some(true) => return Some(true),
                    None => truth = None,
                    Some(false) => {}
                }
            }
            truth
        }
        PropertyFilter::Not(filter) => {
            property_filter_truth(filter, id, properties).map(|truth| !truth)
        }
        PropertyFilter::IdEq { value } => {
            leaf_truth(&Value::Int(id as i64) == value, is_null(value))
        }
        PropertyFilter::IdNotEq { value } => {
            leaf_truth(&Value::Int(id as i64) != value, is_null(value))
        }
        PropertyFilter::IdRange { lower, upper } => {
            range_bounds_truth(&Value::Int(id as i64), lower.as_ref(), upper.as_ref())
        }
        PropertyFilter::IdIn { values } => in_truth(Some(&Value::Int(id as i64)), values),
        PropertyFilter::Eq {
            property: name,
            value,
        } => leaf_truth(
            properties.get(name) == Some(value),
            property(name).is_none() || is_null(value),
        ),
        PropertyFilter::NotEq {
            property: name,
            value,
        } => leaf_truth(
            properties.get(name).is_some_and(|actual| actual != value),
            property(name).is_none() || is_null(value),
        ),
        PropertyFilter::IsNull { property: name } => Some(property(name).is_none()),
        PropertyFilter::IsNotNull { property: name } => Some(property(name).is_some()),
        PropertyFilter::In {
            property: name,
            values,
        } => in_truth(property(name), values),
        PropertyFilter::ListContains {
            property: name,
            value,
        } => property(name).map(|actual| match actual {
            Value::List(values) => values.iter().any(|actual| actual == value),
            _ => false,
        }),
        PropertyFilter::ListContainsLower {
            property: name,
            value,
        } => property(name).map(|actual| match actual {
            Value::List(values) => values.iter().any(|actual| match actual {
                Value::String(actual) => actual.to_lowercase().contains(value),
                _ => false,
            }),
            _ => false,
        }),
        PropertyFilter::Contains {
            property: name,
            value,
        } => property(name).map(|actual| match actual {
            Value::String(actual) => actual.contains(value),
            _ => false,
        }),
        PropertyFilter::StartsWith {
            property: name,
            value,
        } => property(name).map(|actual| match actual {
            Value::String(actual) => actual.starts_with(value),
            _ => false,
        }),
        PropertyFilter::EndsWith {
            property: name,
            value,
        } => property(name).map(|actual| match actual {
            Value::String(actual) => actual.ends_with(value),
            _ => false,
        }),
        PropertyFilter::RegexMatch {
            property: name,
            pattern,
        } => property(name).map(|actual| match actual {
            Value::String(actual) => pattern.is_match(actual),
            _ => false,
        }),
        PropertyFilter::DefaultIfNullOrEq {
            property: name,
            empty,
            default,
            value,
            negated,
        } => {
            let actual = properties.get(name).unwrap_or(&Value::Null);
            let normalized = if actual == &Value::Null || actual == empty {
                default
            } else {
                actual
            };
            leaf_truth(
                (normalized == value) != *negated,
                is_null(normalized) || is_null(value),
            )
        }
        PropertyFilter::Range {
            property: name,
            lower,
            upper,
        } => property(name)
            .and_then(|actual| range_bounds_truth(actual, lower.as_ref(), upper.as_ref())),
    }
}

/// A leaf that matched is true; one that did not is NULL when an operand is
/// NULL and false otherwise.
const fn leaf_truth(matches: bool, null_operand: bool) -> Option<bool> {
    if matches {
        Some(true)
    } else if null_operand {
        None
    } else {
        Some(false)
    }
}

fn in_truth(actual: Option<&Value>, values: &[Value]) -> Option<bool> {
    let actual = actual.filter(|actual| !is_null(actual))?;
    if values.iter().any(|value| value == actual) {
        Some(true)
    } else if values.iter().any(is_null) {
        None
    } else {
        Some(false)
    }
}

/// `lower <(=) value <(=) upper` as the conjunction of one comparison per
/// bound; a NULL bound or a duration pair makes that comparison NULL.
fn range_bounds_truth(
    value: &Value,
    lower: Option<&(Value, bool)>,
    upper: Option<&(Value, bool)>,
) -> Option<bool> {
    let comparisons = [
        lower.map(|lower| (&lower.0, range_bounds_match(value, Some(lower), None))),
        upper.map(|upper| (&upper.0, range_bounds_match(value, None, Some(upper)))),
    ];
    let mut truth = Some(true);
    for (bound, matches) in comparisons.into_iter().flatten() {
        if is_null(bound) || hawdb_core::temporal::ordering_is_null(value, bound) {
            truth = None;
        } else if !matches {
            return Some(false);
        }
    }
    truth
}

const fn is_null(value: &Value) -> bool {
    matches!(value, Value::Null)
}

pub fn properties_contain_all(
    properties: &BTreeMap<String, Value>,
    required: &BTreeMap<String, Value>,
) -> bool {
    required
        .iter()
        .all(|(property, value)| properties.get(property) == Some(value))
}

pub fn range_bounds_match(
    value: &Value,
    lower: Option<&(Value, bool)>,
    upper: Option<&(Value, bool)>,
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

pub fn comparable_value_ordering(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
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
    fn property_filters_preserve_null_and_numeric_range_semantics() {
        let properties = BTreeMap::from([
            ("score".to_string(), Value::Int(7)),
            ("optional".to_string(), Value::Null),
        ]);

        assert!(property_filter_matches(
            &PropertyFilter::Range {
                property: "score".to_string(),
                lower: Some((Value::Float(6.5), false)),
                upper: Some((Value::Int(7), true)),
            },
            11,
            &properties,
        ));
        assert!(property_filter_matches(
            &PropertyFilter::IsNull {
                property: "missing".to_string(),
            },
            11,
            &properties,
        ));
    }

    #[test]
    fn not_is_three_valued_over_null_operands_and_duration_ordering() {
        let duration =
            |days| Value::duration(hawdb_core::temporal::Duration::from_parts(0, days, 0).unwrap());
        let properties = BTreeMap::from([
            ("d".to_string(), duration(1)),
            ("n".to_string(), Value::Int(1)),
            ("s".to_string(), Value::String("a".into())),
        ]);
        let below = |property: &str, value: Value| PropertyFilter::Range {
            property: property.to_string(),
            lower: None,
            upper: Some((value, false)),
        };
        let not = |filter| PropertyFilter::Not(Box::new(filter));
        let truth = |filter: &PropertyFilter| property_filter_truth(filter, 7, &properties);

        // Two durations have no order: the comparison and its negation are NULL.
        assert_eq!(truth(&below("d", duration(2))), None);
        assert!(!property_filter_matches(
            &not(below("d", duration(2))),
            7,
            &properties
        ));
        // A missing property or a NULL bound is NULL as well.
        assert_eq!(truth(&not(below("missing", Value::Int(2)))), None);
        assert_eq!(truth(&not(below("n", Value::Null))), None);
        let missing_eq = PropertyFilter::Eq {
            property: "missing".to_string(),
            value: Value::Int(1),
        };
        assert_eq!(truth(&not(missing_eq.clone())), None);
        // Mixed kinds keep HawDB's `false`, so their negation is true.
        assert_eq!(truth(&not(below("s", Value::Int(2)))), Some(true));
        assert_eq!(truth(&not(below("n", Value::Int(1)))), Some(true));
        // Kleene connectives: NULL AND false is false, NULL OR true is true.
        assert_eq!(
            truth(&not(PropertyFilter::And(vec![
                below("d", duration(2)),
                below("n", Value::Int(0)),
            ]))),
            Some(true)
        );
        assert_eq!(
            truth(&PropertyFilter::Or(vec![
                missing_eq,
                below("n", Value::Int(2)),
            ])),
            Some(true)
        );
        // An IN list with NULL and no match is NULL.
        assert_eq!(
            truth(&PropertyFilter::In {
                property: "n".to_string(),
                values: vec![Value::Int(5), Value::Null],
            }),
            None
        );
    }
}
