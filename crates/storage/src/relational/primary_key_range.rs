// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::ops::Bound;

/// A conservative interval in one ordered key namespace. Short bounds are
/// lexicographic boundaries, never wildcard keys. An equality prefix is encoded
/// as Included(prefix)..Excluded(successor(prefix)), including every full key
/// extending that prefix. The original predicate must still be evaluated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalKeyRange {
    pub lower: Bound<RelationalKey>,
    pub upper: Bound<RelationalKey>,
}

/// Primary-row mutation access uses the same boundaries as index-range locks.
pub type RelationalPrimaryKeyRange = RelationalKeyRange;

impl RelationalKeyRange {
    /// Derive bounds for validated values in the supplied ordered columns.
    /// Unsupported predicates remain residuals; `None` requires a broader lock
    /// or access path, while an empty interval proves there are no candidates.
    pub fn from_predicate(
        columns: &[String],
        predicate: &RelationalPredicate,
    ) -> Result<Option<Self>, RelationalError> {
        key_range(columns, predicate, None)
    }

    pub fn is_empty(&self) -> bool {
        match (&self.lower, &self.upper) {
            (Bound::Included(left), Bound::Included(right)) => left > right,
            (
                Bound::Included(left) | Bound::Excluded(left),
                Bound::Included(right) | Bound::Excluded(right),
            ) => left >= right,
            _ => false,
        }
    }
}

pub(super) fn equality<'a>(
    predicate: &'a RelationalPredicate,
    column: &str,
) -> Option<&'a RelationalValue> {
    match predicate {
        RelationalPredicate::And(left, right) => {
            equality(left, column).or_else(|| equality(right, column))
        }
        RelationalPredicate::Compare {
            column: name,
            op: RelationalComparisonOp::Eq,
            value,
        } if name == column && !matches!(value, RelationalValue::Null) => Some(value),
        _ => None,
    }
}

fn visit_comparisons<'a>(
    predicate: &'a RelationalPredicate,
    column: &str,
    visit: &mut impl FnMut(RelationalComparisonOp, &'a RelationalValue),
) {
    match predicate {
        RelationalPredicate::And(left, right) => {
            visit_comparisons(left, column, visit);
            visit_comparisons(right, column, visit);
        }
        RelationalPredicate::Compare {
            column: name,
            op,
            value,
        } if name == column && !matches!(value, RelationalValue::Null) => visit(*op, value),
        _ => {}
    }
}

fn copy_key<'a>(
    values: impl Iterator<Item = &'a RelationalValue>,
    count: usize,
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<RelationalKey, RelationalError> {
    collect_results_with_allocations(
        values.map(|value| match allocations {
            Some(context) => value.as_ref().to_owned_value_with_allocations(context),
            None => Ok(value.clone()),
        }),
        count,
        allocations,
    )
    .map(RelationalKey)
}

/// Advance in the schema's scalar domain, carrying across maximal components.
/// Variable-length values have an immediate successor formed by appending NUL.
/// Floating-point keys follow total_cmp, including signed zero and NaN payloads.
fn prefix_successor(
    mut key: RelationalKey,
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<Option<RelationalKey>, RelationalError> {
    while let Some(value) = key.0.last_mut() {
        let advanced = match value {
            RelationalValue::Boolean(value) => {
                let advanced = !*value;
                *value = true;
                advanced
            }
            RelationalValue::BigInt(value) => match value.checked_add(1) {
                Some(next) => {
                    *value = next;
                    true
                }
                None => false,
            },
            RelationalValue::DoublePrecision(value) => {
                let bits = value.to_bits();
                if bits == i64::MAX as u64 {
                    false
                } else {
                    let next = if bits == 1 << 63 {
                        0
                    } else if bits >> 63 == 1 {
                        bits - 1
                    } else {
                        bits + 1
                    };
                    *value = f64::from_bits(next);
                    true
                }
            }
            RelationalValue::Text(value) => {
                if let Some(context) = allocations {
                    let bytes = append_zero_with_allocations(value.as_bytes(), context)?;
                    *value = String::from_utf8(bytes).expect("NUL preserves valid UTF-8");
                } else {
                    value.push('\0');
                }
                true
            }
            RelationalValue::Bytea(value) => {
                if let Some(context) = allocations {
                    *value = append_zero_with_allocations(value, context)?;
                } else {
                    value.push(0);
                }
                true
            }
            RelationalValue::Uuid(value) => match value.as_u128().checked_add(1) {
                Some(next) => {
                    *value = Uuid::from_u128(next);
                    true
                }
                None => false,
            },
            RelationalValue::Null | RelationalValue::Vector(_) | RelationalValue::Overflow(_) => {
                return Err(RelationalError::Schema(
                    "unsupported primary-key boundary value".into(),
                ));
            }
        };
        if advanced {
            return Ok(Some(key));
        }
        key.0.pop();
    }
    Ok(None)
}

fn append_zero_with_allocations(
    value: &[u8],
    context: &crate::background::CheckpointDecodeContext,
) -> Result<Vec<u8>, RelationalError> {
    let capacity = value.len().checked_add(1).ok_or_else(|| {
        RelationalError::Admission("primary-key boundary capacity overflow".into())
    })?;
    let mut next = context
        .vector_capacity(capacity)
        .map_err(|error| RelationalError::Admission(error.to_string()))?;
    next.extend_from_slice(value);
    next.push(0);
    Ok(next)
}

/// Extract an equality prefix and a range on its immediately following column.
/// OR, NOT, gaps in the key prefix, and non-key predicates remain residuals.
pub(super) fn mutation_primary_key_range(
    schema: &RelationalTableSchema,
    predicate: &RelationalPredicate,
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<Option<RelationalPrimaryKeyRange>, RelationalError> {
    key_range(&schema.primary_key, predicate, allocations)
}

fn key_range(
    columns: &[String],
    predicate: &RelationalPredicate,
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<Option<RelationalKeyRange>, RelationalError> {
    let prefix_len = columns
        .iter()
        .take_while(|column| equality(predicate, column).is_some())
        .count();
    let prefix = copy_key(
        columns
            .iter()
            .take(prefix_len)
            .map(|column| equality(predicate, column).expect("proven equality prefix")),
        prefix_len,
        allocations,
    )?;
    if prefix_len == columns.len() && prefix_len != 0 {
        return Ok(Some(RelationalPrimaryKeyRange {
            lower: Bound::Included(copy_key(prefix.0.iter(), prefix_len, allocations)?),
            upper: Bound::Included(prefix),
        }));
    }
    let mut lower = None;
    let mut upper = None;
    if let Some(column) = columns.get(prefix_len) {
        visit_comparisons(predicate, column, &mut |op, value| match op {
            RelationalComparisonOp::Gt | RelationalComparisonOp::Gte => {
                let inclusive = op == RelationalComparisonOp::Gte;
                if lower.is_none_or(|(old, included)| {
                    value > old || value == old && !inclusive && included
                }) {
                    lower = Some((value, inclusive));
                }
            }
            RelationalComparisonOp::Lt | RelationalComparisonOp::Lte => {
                let inclusive = op == RelationalComparisonOp::Lte;
                if upper.is_none_or(|(old, included)| {
                    value < old || value == old && !inclusive && included
                }) {
                    upper = Some((value, inclusive));
                }
            }
            _ => {}
        });
    }
    if prefix_len == 0 && lower.is_none() && upper.is_none() {
        return Ok(None);
    }
    let prefix_upper = if prefix_len == 0 {
        Bound::Unbounded
    } else {
        prefix_successor(
            copy_key(prefix.0.iter(), prefix_len, allocations)?,
            allocations,
        )?
        .map_or(Bound::Unbounded, Bound::Excluded)
    };
    let lower = if let Some((value, inclusive)) = lower {
        let key = copy_key(
            prefix.0.iter().chain(std::iter::once(value)),
            prefix_len + 1,
            allocations,
        )?;
        if inclusive {
            Bound::Included(key)
        } else if let Some(next) = prefix_successor(key, allocations)? {
            Bound::Included(next)
        } else {
            // Greater than the maximum complete prefix is an empty interval.
            let maximum = copy_key(
                prefix.0.iter().chain(std::iter::once(value)),
                prefix_len + 1,
                allocations,
            )?;
            return Ok(Some(RelationalPrimaryKeyRange {
                lower: Bound::Excluded(copy_key(maximum.0.iter(), maximum.0.len(), allocations)?),
                upper: Bound::Included(maximum),
            }));
        }
    } else if prefix_len == 0 {
        Bound::Unbounded
    } else {
        Bound::Included(copy_key(prefix.0.iter(), prefix_len, allocations)?)
    };
    let upper = if let Some((value, inclusive)) = upper {
        let key = copy_key(
            prefix.0.iter().chain(std::iter::once(value)),
            prefix_len + 1,
            allocations,
        )?;
        if inclusive {
            prefix_successor(key, allocations)?.map_or(Bound::Unbounded, Bound::Excluded)
        } else {
            Bound::Excluded(key)
        }
    } else {
        prefix_upper
    };
    Ok(Some(RelationalPrimaryKeyRange { lower, upper }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_prefix_bounds_preserve_predicate_truth_and_encoded_key_order() {
        let domains = [
            vec![
                RelationalValue::Boolean(false),
                RelationalValue::Boolean(true),
            ],
            [i64::MIN, -1, 0, 1, i64::MAX]
                .into_iter()
                .map(RelationalValue::BigInt)
                .collect(),
            [
                u64::MAX,
                0xfff8_0000_0000_0001,
                f64::NEG_INFINITY.to_bits(),
                (-1.0_f64).to_bits(),
                (-0.0_f64).to_bits(),
                0,
                1,
                1.0_f64.to_bits(),
                f64::INFINITY.to_bits(),
                0x7ff8_0000_0000_0001,
                i64::MAX as u64,
            ]
            .into_iter()
            .map(|bits| RelationalValue::DoublePrecision(f64::from_bits(bits)))
            .collect(),
            ["", "\0", "a", "a\0", "a\0\0", "aa", "é", "\u{10ffff}"]
                .into_iter()
                .map(|value| RelationalValue::Text(value.into()))
                .collect(),
            [
                vec![],
                vec![0],
                vec![0, 0],
                vec![1],
                vec![255],
                vec![255, 0],
            ]
            .into_iter()
            .map(RelationalValue::Bytea)
            .collect(),
            [0, 1, u128::MAX - 1, u128::MAX]
                .into_iter()
                .map(|value| RelationalValue::Uuid(Uuid::from_u128(value)))
                .collect(),
        ];
        for domain in domains {
            let schema = RelationalTableSchema {
                name: "t".into(),
                columns: vec![
                    RelationalColumnSchema {
                        name: "a".into(),
                        scalar_type: domain[0].scalar_type().unwrap(),
                        nullable: false,
                        default: None,
                    },
                    RelationalColumnSchema {
                        name: "b".into(),
                        scalar_type: RelationalScalarType::BigInt,
                        nullable: false,
                        default: None,
                    },
                ],
                primary_key: vec!["a".into(), "b".into()],
                unique_constraints: vec![],
                foreign_keys: vec![],
                indexes: vec![],
                materialized_view: None,
            };
            let rows = RelationalRowPages::from_map(
                domain
                    .iter()
                    .flat_map(|value| {
                        [i64::MIN, 0, i64::MAX].into_iter().map(move |tail| {
                            let values = vec![value.clone(), RelationalValue::BigInt(tail)];
                            (RelationalKey(values.clone()), RelationalRow::new(values))
                        })
                    })
                    .collect(),
            );
            for value in &domain {
                for op in [
                    RelationalComparisonOp::Eq,
                    RelationalComparisonOp::Lt,
                    RelationalComparisonOp::Lte,
                    RelationalComparisonOp::Gt,
                    RelationalComparisonOp::Gte,
                ] {
                    let predicate = RelationalPredicate::Compare {
                        column: "a".into(),
                        op,
                        value: value.clone(),
                    };
                    let range = mutation_primary_key_range(&schema, &predicate, None)
                        .unwrap()
                        .unwrap();
                    let expected: Vec<_> = rows
                        .iter()
                        .filter(|(_, row)| {
                            predicate_truth(&schema, row, &predicate).unwrap() == Some(true)
                        })
                        .collect();
                    let actual: Vec<_> = rows.range(Some(range.clone())).collect();
                    assert_eq!(
                        actual, expected,
                        "value={value:?}, op={op:?}, range={range:?}"
                    );
                    if !range.is_empty() {
                        for (key, row) in rows.iter() {
                            let encoded = encode_relational_primary_key(key).unwrap();
                            let after_lower = match &range.lower {
                                Bound::Included(lower) => {
                                    encoded >= encode_relational_primary_key(lower).unwrap()
                                }
                                Bound::Excluded(lower) => {
                                    encoded > encode_relational_primary_key(lower).unwrap()
                                }
                                Bound::Unbounded => true,
                            };
                            let before_upper = match &range.upper {
                                Bound::Included(upper) => {
                                    encoded <= encode_relational_primary_key(upper).unwrap()
                                }
                                Bound::Excluded(upper) => {
                                    encoded < encode_relational_primary_key(upper).unwrap()
                                }
                                Bound::Unbounded => true,
                            };
                            assert_eq!(
                                after_lower && before_upper,
                                predicate_truth(&schema, row, &predicate).unwrap() == Some(true)
                            );
                        }
                    }
                }
            }
        }
    }
}
