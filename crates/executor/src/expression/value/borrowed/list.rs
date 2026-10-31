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

//! openCypher list functions, indexing, slicing and `IN`.
//!
//! Every newly built list is charged before its elements are copied: the
//! list header and each element slot are reserved on the caller's memory
//! account, and computed elements transfer their own reservation into the
//! list. Results derived from an already owned list reuse that list's charge.

use super::*;
use crate::binding::value_memory_bytes;
use hawdb_core::{ListFunction, MAX_RANGE_LENGTH};
use std::mem::size_of;

/// Builds a list under the evaluator's working-memory account, or under the
/// evaluation's unaccounted-bytes limit when there is no account.
pub(super) struct ListBuilder<'w> {
    values: Vec<Value>,
    allocation: Option<QueryMemoryLease>,
    work: &'w super::ListWork,
}

impl<'w> ListBuilder<'w> {
    /// `capacity` is an allocation hint; slots are charged as they are filled.
    pub(super) fn new(
        account: Option<&QueryMemoryAccount>,
        work: &'w super::ListWork,
        capacity: usize,
    ) -> Result<Self> {
        let header = size_of::<Value>() + size_of::<Vec<Value>>();
        let allocation = match account {
            Some(account) => Some(account.reserve(header)?),
            None => {
                work.charge_unaccounted(header)?;
                None
            }
        };
        Ok(Self {
            // Cap the uncharged up-front hint; larger lists grow as their
            // charged slots are filled.
            values: Vec::with_capacity(capacity.min(1024)),
            allocation,
            work,
        })
    }

    fn reserve(&mut self, bytes: usize) -> Result<()> {
        match &mut self.allocation {
            Some(allocation) => allocation.grow(bytes),
            None => self.work.charge_unaccounted(bytes),
        }
    }

    pub(super) fn push_cloned(&mut self, value: &Value) -> Result<()> {
        self.reserve(value_memory_bytes(value))?;
        self.values.push(value.clone());
        Ok(())
    }

    pub(super) fn push_projected(&mut self, value: ProjectedValue<'_>) -> Result<()> {
        let Some(allocation) = &mut self.allocation else {
            self.work
                .charge_unaccounted(value_memory_bytes(value.as_ref()))?;
            self.values.push(value.into_owned());
            return Ok(());
        };
        allocation.grow(size_of::<Value>())?;
        let ProjectedValue {
            value,
            _allocation: element,
        } = value;
        match element {
            // A computed element's charge already covers its payload; the
            // list takes it over without a second reservation.
            Some(element) => {
                if let Err(element) = allocation.absorb(element) {
                    ProjectedValue {
                        value,
                        _allocation: Some(element),
                    }
                    .into_owned_accounted(allocation)
                    .map(|value| self.values.push(value))?;
                    return Ok(());
                }
            }
            None => allocation
                .grow(value_memory_bytes(value.as_ref()).saturating_sub(size_of::<Value>()))?,
        }
        self.values.push(value.into_owned());
        Ok(())
    }

    pub(super) fn finish<'a>(self) -> ProjectedValue<'a> {
        ProjectedValue {
            value: Cow::Owned(Value::List(self.values)),
            _allocation: self.allocation,
        }
    }
}

pub(super) fn function<'a>(
    function: ListFunction,
    arguments: &'a [ProjectionExpression],
    evaluate: impl Fn(&'a ProjectionExpression) -> Result<ProjectedValue<'a>>,
    account: Option<&QueryMemoryAccount>,
    work: &super::ListWork,
) -> Result<ProjectedValue<'a>> {
    let (min, max) = function.arity();
    if !(min..=max).contains(&arguments.len()) {
        // Unreachable from parsed queries (the parser checks arity first);
        // kept as a defensive check with the parser's wording.
        let expected = if min == max {
            format!("{min}")
        } else {
            format!("{min} or {max}")
        };
        return Err(HawDBError::Execution(format!(
            "{}() expects {expected} argument(s), got {}",
            function.function_name(),
            arguments.len()
        )));
    }
    if function == ListFunction::Range {
        let mut bounds = [0i64; 3];
        bounds[2] = 1;
        for (slot, argument) in bounds.iter_mut().zip(arguments) {
            match evaluate(argument)?.as_ref() {
                Value::Null => return Ok(ProjectedValue::scalar(Value::Null)),
                Value::Int(value) => *slot = *value,
                value => {
                    return Err(HawDBError::Execution(format!(
                        "range() requires integer arguments, got {:?}",
                        description(value, account)
                    )))
                }
            }
        }
        return range(bounds[0], bounds[1], bounds[2], account, work);
    }
    let input = evaluate(&arguments[0])?;
    match (function, input.as_ref()) {
        (_, Value::Null) => Ok(ProjectedValue::scalar(Value::Null)),
        (ListFunction::Size, Value::List(values)) => Ok(ProjectedValue::scalar(Value::Int(
            i64::try_from(values.len()).unwrap_or(i64::MAX),
        ))),
        (ListFunction::Size, Value::String(value)) => {
            work.charge(value.len())?;
            Ok(ProjectedValue::scalar(Value::Int(
                i64::try_from(value.chars().count()).unwrap_or(i64::MAX),
            )))
        }
        (ListFunction::Head, Value::List(values)) => {
            if values.is_empty() {
                Ok(ProjectedValue::scalar(Value::Null))
            } else {
                Ok(element(input, 0))
            }
        }
        (ListFunction::Last, Value::List(values)) => match values.len().checked_sub(1) {
            Some(last) => Ok(element(input, last)),
            None => Ok(ProjectedValue::scalar(Value::Null)),
        },
        (ListFunction::Tail, Value::List(values)) => {
            let len = values.len();
            sublist(input, 1.min(len)..len, false, account, work)
        }
        (ListFunction::Reverse, Value::List(values)) => {
            let len = values.len();
            sublist(input, 0..len, true, account, work)
        }
        (ListFunction::Reverse, Value::String(value)) => {
            let bytes = size_of::<Value>().saturating_add(value.len());
            work.charge(value.len())?;
            if account.is_none() {
                work.charge_unaccounted(bytes)?;
            }
            allocated(bytes, account, || {
                Value::String(value.chars().rev().collect())
            })
        }
        (function, value) => Err(HawDBError::Execution(format!(
            "{}() requires {}, got {:?}",
            function.function_name(),
            match function {
                ListFunction::Size | ListFunction::Reverse => "a list or string",
                _ => "a list",
            },
            description(value, account)
        ))),
    }
}

/// `range(start, end, step)` with an inclusive end and a bounded length.
fn range<'a>(
    start: i64,
    end: i64,
    step: i64,
    account: Option<&QueryMemoryAccount>,
    work: &super::ListWork,
) -> Result<ProjectedValue<'a>> {
    if step == 0 {
        return Err(HawDBError::Execution(
            "range() step must not be zero".to_string(),
        ));
    }
    let (start, end, step) = (i128::from(start), i128::from(end), i128::from(step));
    let len = if (step > 0 && start > end) || (step < 0 && start < end) {
        0
    } else {
        (end - start) / step + 1
    };
    if len > MAX_RANGE_LENGTH as i128 {
        return Err(HawDBError::Execution(format!(
            "range() would produce {len} elements, exceeding the limit of {MAX_RANGE_LENGTH}"
        )));
    }
    let len = len as usize;
    work.charge(len)?;
    let mut output = ListBuilder::new(account, work, 0)?;
    output.reserve(len.saturating_mul(size_of::<Value>()))?;
    output.values.reserve_exact(len);
    // Every produced value lies between start and end, so it fits in i64.
    output
        .values
        .extend((0..len as i128).map(|offset| Value::Int((start + offset * step) as i64)));
    Ok(output.finish())
}

/// Selects one element; `index` must be in bounds of a list value.
fn element(list: ProjectedValue<'_>, index: usize) -> ProjectedValue<'_> {
    let ProjectedValue { value, _allocation } = list;
    match value {
        Cow::Borrowed(Value::List(values)) => ProjectedValue::borrowed(&values[index]),
        Cow::Owned(Value::List(mut values)) => ProjectedValue {
            // The element keeps the list's (larger) charge until it is owned.
            value: Cow::Owned(values.swap_remove(index)),
            _allocation,
        },
        _ => unreachable!("list element selection requires a list"),
    }
}

/// Copies `range` of a list, optionally reversed. `range` must be valid.
fn sublist<'a>(
    list: ProjectedValue<'a>,
    range: std::ops::Range<usize>,
    reverse: bool,
    account: Option<&QueryMemoryAccount>,
    work: &super::ListWork,
) -> Result<ProjectedValue<'a>> {
    work.charge(range.len())?;
    let ProjectedValue { value, _allocation } = list;
    match value {
        Cow::Borrowed(Value::List(values)) => {
            let selected = &values[range];
            let mut output = ListBuilder::new(account, work, selected.len())?;
            if reverse {
                for value in selected.iter().rev() {
                    output.push_cloned(value)?;
                }
            } else {
                for value in selected {
                    output.push_cloned(value)?;
                }
            }
            Ok(output.finish())
        }
        Cow::Owned(Value::List(mut values)) => {
            // Shrinking an owned list in place stays within its existing charge.
            values.truncate(range.end);
            values.drain(..range.start);
            if reverse {
                values.reverse();
            }
            Ok(ProjectedValue {
                value: Cow::Owned(Value::List(values)),
                _allocation,
            })
        }
        _ => unreachable!("sublist selection requires a list"),
    }
}

fn integer_operand(
    value: &Value,
    context: &str,
    account: Option<&QueryMemoryAccount>,
) -> Result<i64> {
    match value {
        Value::Int(value) => Ok(*value),
        value => Err(HawDBError::Execution(format!(
            "{context} requires an integer, got {:?}",
            description(value, account)
        ))),
    }
}

/// Resolves a possibly negative index against `len`; `None` is out of range.
fn resolve_index(index: i64, len: usize) -> Option<usize> {
    let len = i128::try_from(len).ok()?;
    let index = i128::from(index);
    let index = if index < 0 { len + index } else { index };
    (0..len).contains(&index).then_some(index as usize)
}

pub(super) fn index<'a>(
    list: ProjectedValue<'a>,
    index: &Value,
    account: Option<&QueryMemoryAccount>,
) -> Result<ProjectedValue<'a>> {
    let len = match list.as_ref() {
        Value::Null => return Ok(ProjectedValue::scalar(Value::Null)),
        Value::List(values) => values.len(),
        value => {
            return Err(HawDBError::Execution(format!(
                "list index requires a list, got {:?}",
                description(value, account)
            )))
        }
    };
    if index == &Value::Null {
        return Ok(ProjectedValue::scalar(Value::Null));
    }
    let index = integer_operand(index, "list index", account)?;
    Ok(match resolve_index(index, len) {
        Some(index) => element(list, index),
        None => ProjectedValue::scalar(Value::Null),
    })
}

pub(super) fn slice<'a>(
    list: ProjectedValue<'a>,
    start: Option<&Value>,
    end: Option<&Value>,
    account: Option<&QueryMemoryAccount>,
    work: &super::ListWork,
) -> Result<ProjectedValue<'a>> {
    let len = match list.as_ref() {
        Value::Null => return Ok(ProjectedValue::scalar(Value::Null)),
        Value::List(values) => values.len(),
        value => {
            return Err(HawDBError::Execution(format!(
                "list slice requires a list, got {:?}",
                description(value, account)
            )))
        }
    };
    if start == Some(&Value::Null) || end == Some(&Value::Null) {
        return Ok(ProjectedValue::scalar(Value::Null));
    }
    let bound = |value: Option<&Value>, default: usize| -> Result<usize> {
        let Some(value) = value else {
            return Ok(default);
        };
        let value = i128::from(integer_operand(value, "list slice bound", account)?);
        let len = len as i128;
        let value = if value < 0 { len + value } else { value };
        Ok(value.clamp(0, len) as usize)
    };
    let start = bound(start, 0)?;
    let end = bound(end, len)?;
    sublist(list, start..end.max(start), false, account, work)
}

/// Three-valued openCypher equality used by `IN`: comparisons involving
/// `NULL`, including inside nested lists, are unknown.
/// Nested list elements are charged to `work` as they are compared.
fn equals(left: &Value, right: &Value, work: &super::ListWork) -> Result<Option<bool>> {
    Ok(match (left, right) {
        (Value::Null, _) | (_, Value::Null) => None,
        (Value::List(left), Value::List(right)) => {
            if left.len() != right.len() {
                return Ok(Some(false));
            }
            work.charge(left.len())?;
            let mut unknown = false;
            for (left, right) in left.iter().zip(right) {
                match equals(left, right, work)? {
                    Some(false) => return Ok(Some(false)),
                    None => unknown = true,
                    Some(true) => {}
                }
            }
            (!unknown).then_some(true)
        }
        (left, right) => Some(left == right),
    })
}

/// `element IN list`: `NULL` for a `NULL` list or when no element matches and
/// some comparison was unknown; `false` for an empty list.
pub(super) fn contains(
    element: &Value,
    list: &Value,
    account: Option<&QueryMemoryAccount>,
    work: &super::ListWork,
) -> Result<Value> {
    let values = match list {
        Value::Null => return Ok(Value::Null),
        Value::List(values) => values,
        value => {
            return Err(HawDBError::Execution(format!(
                "IN requires a list on its right side, got {:?}",
                description(value, account)
            )))
        }
    };
    work.charge(values.len())?;
    let mut unknown = false;
    for value in values {
        match equals(element, value, work)? {
            Some(true) => return Ok(Value::Bool(true)),
            Some(false) => {}
            None => unknown = true,
        }
    }
    Ok(if unknown {
        Value::Null
    } else {
        Value::Bool(false)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ints(values: &[i64]) -> Value {
        Value::List(values.iter().copied().map(Value::Int).collect())
    }

    #[test]
    fn range_is_inclusive_bounded_and_rejects_zero_step() {
        let value = |start, end, step| {
            range(start, end, step, None, &super::ListWork::default())
                .map(ProjectedValue::into_owned)
        };
        assert_eq!(value(0, 3, 1).unwrap(), ints(&[0, 1, 2, 3]));
        assert_eq!(value(3, 0, -1).unwrap(), ints(&[3, 2, 1, 0]));
        assert_eq!(value(0, 10, 4).unwrap(), ints(&[0, 4, 8]));
        assert_eq!(value(5, 1, 1).unwrap(), ints(&[]));
        assert_eq!(value(1, 5, -1).unwrap(), ints(&[]));
        assert_eq!(
            value(i64::MAX - 1, i64::MAX, 1).unwrap(),
            ints(&[i64::MAX - 1, i64::MAX])
        );
        assert!(value(0, 1, 0)
            .unwrap_err()
            .to_string()
            .contains("must not be zero"));
        assert!(value(i64::MIN, i64::MAX, 1)
            .unwrap_err()
            .to_string()
            .contains("exceeding the limit"));
        assert!(matches!(
            value(1, MAX_RANGE_LENGTH as i64, 1).unwrap(),
            Value::List(values) if values.len() == MAX_RANGE_LENGTH
        ));
    }

    #[test]
    fn index_resolution_counts_negative_offsets_from_the_end() {
        assert_eq!(resolve_index(0, 3), Some(0));
        assert_eq!(resolve_index(-1, 3), Some(2));
        assert_eq!(resolve_index(-3, 3), Some(0));
        assert_eq!(resolve_index(-4, 3), None);
        assert_eq!(resolve_index(3, 3), None);
        assert_eq!(resolve_index(i64::MIN, 3), None);
        assert_eq!(resolve_index(0, 0), None);
    }

    #[test]
    fn in_uses_three_valued_equality() {
        let check = |element: Value, list: Value| {
            contains(&element, &list, None, &super::ListWork::default()).unwrap()
        };
        assert_eq!(check(Value::Int(1), ints(&[1, 2])), Value::Bool(true));
        assert_eq!(check(Value::Int(3), ints(&[1, 2])), Value::Bool(false));
        assert_eq!(check(Value::Null, ints(&[])), Value::Bool(false));
        assert_eq!(check(Value::Null, ints(&[1])), Value::Null);
        assert_eq!(
            check(Value::Int(3), Value::List(vec![Value::Int(1), Value::Null])),
            Value::Null
        );
        assert_eq!(
            check(Value::Int(1), Value::List(vec![Value::Null, Value::Int(1)])),
            Value::Bool(true)
        );
        assert_eq!(check(Value::Int(1), Value::Null), Value::Null);
        assert_eq!(
            check(
                ints(&[1, 2]),
                Value::List(vec![Value::List(vec![Value::Int(1), Value::Null])])
            ),
            Value::Null
        );
        assert_eq!(
            check(ints(&[1, 2]), Value::List(vec![ints(&[1, 2])])),
            Value::Bool(true)
        );
        assert!(contains(
            &Value::Int(1),
            &Value::Int(1),
            None,
            &super::ListWork::default()
        )
        .is_err());
    }

    mod evaluation {
        use super::*;
        use crate::{QueryMemoryClass, QueryMemoryLedger};
        use hawdb_plan_cypher::ListComprehensionProjection;
        use std::num::NonZeroUsize;

        fn literal(value: Value) -> ProjectionExpression {
            ProjectionExpression::Literal(value)
        }

        fn column(name: &str) -> ProjectionExpression {
            ProjectionExpression::Column(name.to_string())
        }

        fn local(name: &str, property: Option<&str>) -> ProjectionExpression {
            ProjectionExpression::LocalVariable {
                name: name.to_string(),
                property: property.map(str::to_string),
            }
        }

        fn call(
            function: ListFunction,
            arguments: Vec<ProjectionExpression>,
        ) -> ProjectionExpression {
            ProjectionExpression::ListFunction {
                function,
                arguments,
            }
        }

        fn comprehension(
            variable: &str,
            list: ProjectionExpression,
            predicate: Option<ProjectionExpression>,
            projection: Option<ProjectionExpression>,
        ) -> ProjectionExpression {
            ProjectionExpression::ListComprehension(Box::new(ListComprehensionProjection {
                variable: variable.to_string(),
                list,
                predicate,
                projection,
            }))
        }

        fn binding() -> Binding {
            Binding::values(BTreeMap::from([
                ("xs".to_string(), ints(&[1, 2, 3])),
                ("empty".to_string(), ints(&[])),
                ("missing".to_string(), Value::Null),
                ("name".to_string(), Value::String("h\u{e9}llo".to_string())),
                (
                    "maps".to_string(),
                    Value::List(vec![
                        Value::Map(BTreeMap::from([("id".to_string(), Value::Int(1))])),
                        Value::Map(BTreeMap::new()),
                        Value::Null,
                    ]),
                ),
                (
                    "nested".to_string(),
                    Value::List(vec![ints(&[1, 2]), ints(&[3])]),
                ),
            ]))
        }

        fn evaluate(expression: &ProjectionExpression) -> Result<Value> {
            evaluate_projection_expression(expression, &Catalog::default(), &binding())
        }

        fn error(expression: &ProjectionExpression) -> String {
            evaluate(expression).unwrap_err().to_string()
        }

        #[test]
        fn list_functions_cover_values_nulls_and_type_errors() {
            use ListFunction::*;
            let cases = [
                (call(Size, vec![column("xs")]), Value::Int(3)),
                (call(Size, vec![column("empty")]), Value::Int(0)),
                (call(Size, vec![column("name")]), Value::Int(5)),
                (call(Size, vec![column("missing")]), Value::Null),
                (call(Head, vec![column("xs")]), Value::Int(1)),
                (call(Head, vec![column("empty")]), Value::Null),
                (call(Head, vec![column("missing")]), Value::Null),
                (call(Last, vec![column("xs")]), Value::Int(3)),
                (call(Last, vec![column("empty")]), Value::Null),
                (call(Tail, vec![column("xs")]), ints(&[2, 3])),
                (call(Tail, vec![column("empty")]), ints(&[])),
                (call(Tail, vec![column("missing")]), Value::Null),
                (call(Reverse, vec![column("xs")]), ints(&[3, 2, 1])),
                (
                    call(Reverse, vec![column("name")]),
                    Value::String("oll\u{e9}h".to_string()),
                ),
                (call(Reverse, vec![column("missing")]), Value::Null),
                (
                    call(Range, vec![literal(Value::Int(1)), literal(Value::Int(3))]),
                    ints(&[1, 2, 3]),
                ),
                (
                    call(Range, vec![literal(Value::Null), literal(Value::Int(3))]),
                    Value::Null,
                ),
                // Functions compose over computed (owned) lists.
                (
                    call(
                        Head,
                        vec![call(Tail, vec![call(Reverse, vec![column("xs")])])],
                    ),
                    Value::Int(2),
                ),
                (
                    call(
                        Last,
                        vec![call(
                            Range,
                            vec![literal(Value::Int(0)), literal(Value::Int(4))],
                        )],
                    ),
                    Value::Int(4),
                ),
                (
                    ProjectionExpression::Coalesce(vec![
                        call(Head, vec![column("empty")]),
                        literal(Value::Int(9)),
                    ]),
                    Value::Int(9),
                ),
            ];
            for (expression, expected) in cases {
                assert_eq!(evaluate(&expression).unwrap(), expected, "{expression:?}");
            }
            assert!(error(&call(Size, vec![literal(Value::Int(1))]))
                .contains("size() requires a list or string"));
            assert!(error(&call(Head, vec![column("name")])).contains("head() requires a list"));
            assert!(error(&call(
                Range,
                vec![
                    literal(Value::Int(0)),
                    literal(Value::Int(3)),
                    literal(Value::Int(0))
                ]
            ))
            .contains("step must not be zero"));
            assert!(error(&call(
                Range,
                vec![literal(Value::Int(0)), literal(Value::Float(3.0))]
            ))
            .contains("range() requires integer arguments"));
        }

        #[test]
        fn comprehensions_filter_map_nest_and_propagate_null() {
            let gt = |left, right| ProjectionExpression::Binary {
                left: Box::new(left),
                op: hawdb_plan_cypher::ScalarBinaryOp::Gt,
                right: Box::new(right),
            };
            let cases = [
                (
                    comprehension("x", column("xs"), None, None),
                    ints(&[1, 2, 3]),
                ),
                (
                    comprehension(
                        "x",
                        column("xs"),
                        Some(gt(local("x", None), literal(Value::Int(1)))),
                        None,
                    ),
                    ints(&[2, 3]),
                ),
                (
                    comprehension(
                        "x",
                        column("xs"),
                        None,
                        Some(call(
                            ListFunction::Range,
                            vec![literal(Value::Int(1)), local("x", None)],
                        )),
                    ),
                    Value::List(vec![ints(&[1]), ints(&[1, 2]), ints(&[1, 2, 3])]),
                ),
                (
                    comprehension(
                        "x",
                        column("xs"),
                        Some(gt(local("x", None), literal(Value::Int(2)))),
                        Some(literal(Value::String("big".into()))),
                    ),
                    Value::List(vec![Value::String("big".into())]),
                ),
                (
                    comprehension("x", column("missing"), None, None),
                    Value::Null,
                ),
                (
                    comprehension("x", column("empty"), None, Some(local("x", None))),
                    ints(&[]),
                ),
                // NULL predicate results drop the element.
                (
                    comprehension(
                        "m",
                        column("maps"),
                        Some(gt(local("m", Some("id")), literal(Value::Int(0)))),
                        Some(local("m", Some("id"))),
                    ),
                    ints(&[1]),
                ),
                (
                    comprehension("m", column("maps"), None, Some(local("m", Some("id")))),
                    Value::List(vec![Value::Int(1), Value::Null, Value::Null]),
                ),
                // Nested comprehensions see the outer element.
                (
                    comprehension(
                        "outer",
                        column("nested"),
                        None,
                        Some(comprehension(
                            "inner",
                            local("outer", None),
                            None,
                            Some(call(ListFunction::Size, vec![local("outer", None)])),
                        )),
                    ),
                    Value::List(vec![ints(&[2, 2]), ints(&[1])]),
                ),
            ];
            for (expression, expected) in cases {
                assert_eq!(evaluate(&expression).unwrap(), expected, "{expression:?}");
            }
            assert!(error(&comprehension("x", column("name"), None, None))
                .contains("list comprehension requires a list"));
            assert!(error(&comprehension(
                "x",
                column("xs"),
                Some(local("x", None)),
                None
            ))
            .contains("WHERE requires a boolean"));
            assert!(error(&comprehension(
                "x",
                column("xs"),
                None,
                Some(local("x", Some("p")))
            ))
            .contains("requires a map value"));
            assert!(error(&local("x", None)).contains("missing list comprehension variable"));
        }

        #[test]
        fn indexing_and_slicing_follow_cypher_bounds() {
            let index = |list, value| ProjectionExpression::ListIndex {
                list: Box::new(list),
                index: Box::new(literal(value)),
            };
            let slice =
                |start: Option<Value>, end: Option<Value>| ProjectionExpression::ListSlice {
                    list: Box::new(column("xs")),
                    start: start.map(|value| Box::new(literal(value))),
                    end: end.map(|value| Box::new(literal(value))),
                };
            let cases = [
                (index(column("xs"), Value::Int(0)), Value::Int(1)),
                (index(column("xs"), Value::Int(-1)), Value::Int(3)),
                (index(column("xs"), Value::Int(-3)), Value::Int(1)),
                (index(column("xs"), Value::Int(3)), Value::Null),
                (index(column("xs"), Value::Int(-4)), Value::Null),
                (index(column("empty"), Value::Int(0)), Value::Null),
                (index(column("missing"), Value::Int(0)), Value::Null),
                (index(column("xs"), Value::Null), Value::Null),
                (
                    index(index(column("nested"), Value::Int(0)), Value::Int(1)),
                    Value::Int(2),
                ),
                (
                    index(
                        call(
                            ListFunction::Range,
                            vec![literal(Value::Int(5)), literal(Value::Int(9))],
                        ),
                        Value::Int(-2),
                    ),
                    Value::Int(8),
                ),
                (
                    slice(Some(Value::Int(1)), Some(Value::Int(3))),
                    ints(&[2, 3]),
                ),
                (slice(None, Some(Value::Int(2))), ints(&[1, 2])),
                (slice(Some(Value::Int(-2)), None), ints(&[2, 3])),
                (slice(Some(Value::Int(2)), Some(Value::Int(1))), ints(&[])),
                (
                    slice(Some(Value::Int(-10)), Some(Value::Int(10))),
                    ints(&[1, 2, 3]),
                ),
                (slice(Some(Value::Null), None), Value::Null),
            ];
            for (expression, expected) in cases {
                assert_eq!(evaluate(&expression).unwrap(), expected, "{expression:?}");
            }
            assert!(error(&index(column("xs"), Value::String("0".into())))
                .contains("list index requires an integer"));
            assert!(
                error(&index(column("name"), Value::Int(0))).contains("list index requires a list")
            );
        }

        #[test]
        fn constructed_lists_are_charged_and_released() {
            let ledger = QueryMemoryLedger::new(NonZeroUsize::new(1 << 20).unwrap());
            let account = ledger.account(
                QueryMemoryClass::PipelineBatch,
                "list test",
                NonZeroUsize::new(64 * 1024).unwrap(),
            );
            let catalog = Catalog::default();
            let binding = binding();
            let available = account.available_bytes();
            let expression = comprehension(
                "x",
                call(
                    ListFunction::Range,
                    vec![literal(Value::Int(1)), literal(Value::Int(100))],
                ),
                None,
                Some(call(ListFunction::Reverse, vec![column("name")])),
            );
            let value =
                evaluate_projection_borrowed(&expression, &catalog, &binding, Some(&account))
                    .unwrap();
            assert!(
                available - account.available_bytes() >= value_memory_bytes(value.as_ref()),
                "list charge must cover the materialized value"
            );
            drop(value);
            assert_eq!(account.available_bytes(), available);

            let error = evaluate_projection_borrowed(
                &call(
                    ListFunction::Range,
                    vec![literal(Value::Int(1)), literal(Value::Int(100_000))],
                ),
                &catalog,
                &binding,
                Some(&account),
            )
            .err()
            .expect("range beyond the account budget must fail before allocation")
            .to_string();
            assert!(error.contains("memory"), "{error}");
            assert_eq!(account.available_bytes(), available);
        }

        #[test]
        fn list_literals_evaluate_elements_in_order_under_the_memory_account() {
            let list = ProjectionExpression::List(vec![
                column("name"),
                literal(Value::Null),
                column("missing"),
                ProjectionExpression::List(vec![column("xs"), literal(Value::Int(4))]),
                call(ListFunction::Size, vec![column("xs")]),
            ]);
            let expected = Value::List(vec![
                Value::String("h\u{e9}llo".to_string()),
                Value::Null,
                Value::Null,
                Value::List(vec![ints(&[1, 2, 3]), Value::Int(4)]),
                Value::Int(3),
            ]);
            assert_eq!(evaluate(&list).unwrap(), expected);
            assert_eq!(
                evaluate(&ProjectionExpression::List(Vec::new())).unwrap(),
                ints(&[])
            );

            let ledger = QueryMemoryLedger::new(NonZeroUsize::new(1 << 20).unwrap());
            let account = ledger.account(
                QueryMemoryClass::PipelineBatch,
                "list literal test",
                NonZeroUsize::new(64 * 1024).unwrap(),
            );
            let catalog = Catalog::default();
            let binding = binding();
            let available = account.available_bytes();
            let value =
                evaluate_projection_borrowed(&list, &catalog, &binding, Some(&account)).unwrap();
            assert_eq!(value.as_ref(), &expected);
            assert!(
                available - account.available_bytes() >= value_memory_bytes(value.as_ref()),
                "list literal charge must cover the materialized value"
            );
            drop(value);
            assert_eq!(account.available_bytes(), available);

            // Elements are charged as they are copied, so an oversized
            // element fails before the list is complete.
            let small = ledger.account(
                QueryMemoryClass::PipelineBatch,
                "small list literal test",
                NonZeroUsize::new(64).unwrap(),
            );
            let error = evaluate_projection_borrowed(&list, &catalog, &binding, Some(&small))
                .err()
                .expect("a list literal beyond the account budget must fail")
                .to_string();
            assert!(error.contains("memory"), "{error}");
        }
    }
}
