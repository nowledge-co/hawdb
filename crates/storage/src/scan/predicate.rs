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

use super::{DateTimeMinMax, MembershipVerdict, NumericMinMax, ScanScalar, SegmentSummary};
use hawdb_core::Value;
use roaring::RoaringTreemap;

#[derive(Debug, Clone, PartialEq)]
pub struct RangeBound {
    pub value: Value,
    pub inclusive: bool,
}

impl RangeBound {
    pub fn inclusive(value: Value) -> Self {
        Self {
            value,
            inclusive: true,
        }
    }

    pub fn exclusive(value: Value) -> Self {
        Self {
            value,
            inclusive: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScanPredicate {
    True,
    False,
    And(Vec<ScanPredicate>),
    Or(Vec<ScanPredicate>),
    Eq {
        property: String,
        value: Value,
    },
    In {
        property: String,
        values: Vec<Value>,
    },
    Range {
        property: String,
        lower: Option<RangeBound>,
        upper: Option<RangeBound>,
    },
    IsNull {
        property: String,
    },
    IsMissing {
        property: String,
    },
    Exists {
        property: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PruningReason {
    PredicateAlwaysFalse,
    FieldAbsent,
    NoNullValues,
    NoMissingValues,
    RangeDisjoint,
    DictionaryNegative,
    MembershipNegative,
    ExactLookup,
    SummaryMayMatch,
    SummaryUnavailable,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PruningDecision {
    Skip {
        reason: PruningReason,
    },
    Read {
        reason: PruningReason,
    },
    Candidates {
        reason: PruningReason,
        row_ids: RoaringTreemap,
    },
}

impl PruningDecision {
    pub fn should_open_payload(&self) -> bool {
        !matches!(self, Self::Skip { .. })
    }
}

pub struct SegmentPruner<'a> {
    summary: &'a SegmentSummary,
}

impl<'a> SegmentPruner<'a> {
    pub fn new(summary: &'a SegmentSummary) -> Self {
        Self { summary }
    }

    pub fn evaluate(&self, predicate: &ScanPredicate) -> PruningDecision {
        match predicate {
            ScanPredicate::True => read(PruningReason::SummaryMayMatch),
            ScanPredicate::False => skip(PruningReason::PredicateAlwaysFalse),
            ScanPredicate::And(predicates) => self.evaluate_and(predicates),
            ScanPredicate::Or(predicates) => self.evaluate_or(predicates),
            ScanPredicate::Eq { property, value } => self.evaluate_eq(property, value),
            ScanPredicate::In { property, values } => self.evaluate_in(property, values),
            ScanPredicate::Range {
                property,
                lower,
                upper,
            } => self.evaluate_range(property, lower.as_ref(), upper.as_ref()),
            ScanPredicate::IsNull { property } => self.summary.field(property).map_or_else(
                || skip(PruningReason::FieldAbsent),
                |field| {
                    if field.null_count == 0 {
                        skip(PruningReason::NoNullValues)
                    } else {
                        read(PruningReason::SummaryMayMatch)
                    }
                },
            ),
            ScanPredicate::IsMissing { property } => self.summary.field(property).map_or_else(
                || read(PruningReason::SummaryUnavailable),
                |field| {
                    if field.missing_count == 0 {
                        skip(PruningReason::NoMissingValues)
                    } else {
                        read(PruningReason::SummaryMayMatch)
                    }
                },
            ),
            ScanPredicate::Exists { property } => self.summary.field(property).map_or_else(
                || skip(PruningReason::FieldAbsent),
                |field| {
                    if field.present_count == 0 {
                        skip(PruningReason::FieldAbsent)
                    } else {
                        read(PruningReason::SummaryMayMatch)
                    }
                },
            ),
        }
    }

    fn evaluate_eq(&self, property: &str, value: &Value) -> PruningDecision {
        let Some(field) = self.summary.field(property) else {
            return skip(PruningReason::FieldAbsent);
        };
        let Some(summary_value) = ScanScalar::from_value(value) else {
            return read(PruningReason::SummaryUnavailable);
        };
        if let Some(row_ids) = field.exact_values.get(&summary_value) {
            return PruningDecision::Candidates {
                reason: PruningReason::ExactLookup,
                row_ids: row_ids.clone(),
            };
        }
        if field
            .enum_dictionary
            .as_ref()
            .is_some_and(|dictionary| !dictionary.may_contain(value))
        {
            return skip(PruningReason::DictionaryNegative);
        }
        if field
            .membership
            .as_ref()
            .is_some_and(|filter| filter.contains(value) == MembershipVerdict::DefinitelyNot)
        {
            return skip(PruningReason::MembershipNegative);
        }
        if !numeric_value_may_match(field.numeric_min_max, value)
            || !datetime_value_may_match(field.datetime_min_max, value)
        {
            return skip(PruningReason::RangeDisjoint);
        }
        read(PruningReason::SummaryMayMatch)
    }

    fn evaluate_in(&self, property: &str, values: &[Value]) -> PruningDecision {
        if values.is_empty() {
            return skip(PruningReason::PredicateAlwaysFalse);
        }
        let decisions = values
            .iter()
            .map(|value| self.evaluate_eq(property, value))
            .collect::<Vec<_>>();
        union_decisions(decisions)
    }

    fn evaluate_range(
        &self,
        property: &str,
        lower: Option<&RangeBound>,
        upper: Option<&RangeBound>,
    ) -> PruningDecision {
        let Some(field) = self.summary.field(property) else {
            return skip(PruningReason::FieldAbsent);
        };
        if range_disjoint_numeric(field.numeric_min_max, lower, upper)
            || range_disjoint_datetime(field.datetime_min_max, lower, upper)
        {
            skip(PruningReason::RangeDisjoint)
        } else if field.numeric_min_max.is_some() || field.datetime_min_max.is_some() {
            read(PruningReason::SummaryMayMatch)
        } else {
            read(PruningReason::SummaryUnavailable)
        }
    }

    fn evaluate_and(&self, predicates: &[ScanPredicate]) -> PruningDecision {
        let mut candidates: Option<RoaringTreemap> = None;
        for predicate in predicates {
            match self.evaluate(predicate) {
                decision @ PruningDecision::Skip { .. } => return decision,
                PruningDecision::Candidates { row_ids, .. } => {
                    candidates = Some(match candidates {
                        Some(current) => &current & &row_ids,
                        None => row_ids,
                    });
                }
                PruningDecision::Read { .. } => {}
            }
        }
        match candidates {
            Some(row_ids) if row_ids.is_empty() => skip(PruningReason::PredicateAlwaysFalse),
            Some(row_ids) => PruningDecision::Candidates {
                reason: PruningReason::ExactLookup,
                row_ids,
            },
            None => read(PruningReason::SummaryMayMatch),
        }
    }

    fn evaluate_or(&self, predicates: &[ScanPredicate]) -> PruningDecision {
        union_decisions(
            predicates
                .iter()
                .map(|predicate| self.evaluate(predicate))
                .collect(),
        )
    }
}

fn union_decisions(decisions: Vec<PruningDecision>) -> PruningDecision {
    let mut candidates = RoaringTreemap::new();
    let mut saw_candidate = false;
    let mut saw_read = false;
    for decision in decisions {
        match decision {
            PruningDecision::Skip { .. } => {}
            PruningDecision::Read { .. } => saw_read = true,
            PruningDecision::Candidates { row_ids, .. } => {
                saw_candidate = true;
                candidates |= row_ids;
            }
        }
    }
    if saw_read {
        read(PruningReason::SummaryMayMatch)
    } else if saw_candidate && !candidates.is_empty() {
        PruningDecision::Candidates {
            reason: PruningReason::ExactLookup,
            row_ids: candidates,
        }
    } else {
        skip(PruningReason::PredicateAlwaysFalse)
    }
}

fn numeric_value_may_match(range: Option<NumericMinMax>, value: &Value) -> bool {
    let Some(range) = range else {
        return true;
    };
    match value {
        Value::Int(value) => range.contains(*value as f64),
        Value::Float(value) => range.contains(*value),
        _ => true,
    }
}

fn datetime_value_may_match(range: Option<DateTimeMinMax>, value: &Value) -> bool {
    let Some(range) = range else {
        return true;
    };
    match value {
        Value::String(value) => DateTimeMinMax::parse_rfc3339(value)
            .is_none_or(|value| range.min_epoch_millis <= value && value <= range.max_epoch_millis),
        _ => true,
    }
}

fn range_disjoint_numeric(
    range: Option<NumericMinMax>,
    lower: Option<&RangeBound>,
    upper: Option<&RangeBound>,
) -> bool {
    let Some(range) = range else {
        return false;
    };
    lower.is_some_and(|bound| {
        numeric_bound(&bound.value)
            .is_some_and(|value| value > range.max || (!bound.inclusive && value == range.max))
    }) || upper.is_some_and(|bound| {
        numeric_bound(&bound.value)
            .is_some_and(|value| value < range.min || (!bound.inclusive && value == range.min))
    })
}

fn range_disjoint_datetime(
    range: Option<DateTimeMinMax>,
    lower: Option<&RangeBound>,
    upper: Option<&RangeBound>,
) -> bool {
    let Some(range) = range else {
        return false;
    };
    lower.is_some_and(|bound| {
        datetime_bound(&bound.value).is_some_and(|value| {
            value > range.max_epoch_millis || (!bound.inclusive && value == range.max_epoch_millis)
        })
    }) || upper.is_some_and(|bound| {
        datetime_bound(&bound.value).is_some_and(|value| {
            value < range.min_epoch_millis || (!bound.inclusive && value == range.min_epoch_millis)
        })
    })
}

fn numeric_bound(value: &Value) -> Option<f64> {
    match value {
        Value::Int(value) => Some(*value as f64),
        Value::Float(value) if value.is_finite() => Some(*value),
        _ => None,
    }
}

fn datetime_bound(value: &Value) -> Option<i64> {
    match value {
        Value::String(value) => DateTimeMinMax::parse_rfc3339(value),
        _ => None,
    }
}

fn skip(reason: PruningReason) -> PruningDecision {
    PruningDecision::Skip { reason }
}

fn read(reason: PruningReason) -> PruningDecision {
    PruningDecision::Read { reason }
}
