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

//! Conservative SQL three-valued-logic analysis for join legality proofs.

use std::{collections::BTreeSet, ops::Not};

/// A stable binder-assigned identity for one relational or graph binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BindingId(u32);

impl BindingId {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

/// A set of bindings used by predicates and null-extension proofs.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BindingSet(BTreeSet<BindingId>);

impl BindingSet {
    pub const fn new() -> Self {
        Self(BTreeSet::new())
    }

    pub fn insert(&mut self, binding: BindingId) -> bool {
        self.0.insert(binding)
    }

    pub fn contains(&self, binding: BindingId) -> bool {
        self.0.contains(&binding)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_subset(&self, other: &Self) -> bool {
        self.0.is_subset(&other.0)
    }

    pub fn intersects(&self, other: &Self) -> bool {
        self.0.iter().any(|binding| other.contains(*binding))
    }

    pub fn iter(&self) -> impl Iterator<Item = BindingId> + '_ {
        self.0.iter().copied()
    }

    pub fn without(&self, binding: BindingId) -> Self {
        Self(
            self.0
                .iter()
                .copied()
                .filter(|candidate| *candidate != binding)
                .collect(),
        )
    }

    fn extend(&mut self, other: &Self) {
        self.0.extend(other.iter());
    }
}

impl From<BindingId> for BindingSet {
    fn from(binding: BindingId) -> Self {
        Self::from([binding])
    }
}

impl<const N: usize> From<[BindingId; N]> for BindingSet {
    fn from(bindings: [BindingId; N]) -> Self {
        Self(bindings.into_iter().collect())
    }
}

impl FromIterator<BindingId> for BindingSet {
    fn from_iter<T: IntoIterator<Item = BindingId>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

/// One SQL boolean result, including the `UNKNOWN` value produced by `NULL`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TruthValue {
    True,
    False,
    Unknown,
}

impl TruthValue {
    pub const fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::False, _) | (_, Self::False) => Self::False,
            (Self::True, Self::True) => Self::True,
            _ => Self::Unknown,
        }
    }

    pub const fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::True, _) | (_, Self::True) => Self::True,
            (Self::False, Self::False) => Self::False,
            _ => Self::Unknown,
        }
    }
}

impl Not for TruthValue {
    type Output = Self;

    fn not(self) -> Self::Output {
        match self {
            Self::True => Self::False,
            Self::False => Self::True,
            Self::Unknown => Self::Unknown,
        }
    }
}

/// An over-approximation of the truth values an expression can produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TruthSet(u8);

impl TruthSet {
    const TRUE_BIT: u8 = 1 << 0;
    const FALSE_BIT: u8 = 1 << 1;
    const UNKNOWN_BIT: u8 = 1 << 2;

    pub const fn empty() -> Self {
        Self(0)
    }

    pub const fn all() -> Self {
        Self(Self::TRUE_BIT | Self::FALSE_BIT | Self::UNKNOWN_BIT)
    }

    pub const fn singleton(value: TruthValue) -> Self {
        Self(Self::bit(value))
    }

    pub const fn contains(self, value: TruthValue) -> bool {
        self.0 & Self::bit(value) != 0
    }

    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    pub fn and(self, other: Self) -> Self {
        self.combine(other, TruthValue::and)
    }

    pub fn or(self, other: Self) -> Self {
        self.combine(other, TruthValue::or)
    }

    const fn bit(value: TruthValue) -> u8 {
        match value {
            TruthValue::True => Self::TRUE_BIT,
            TruthValue::False => Self::FALSE_BIT,
            TruthValue::Unknown => Self::UNKNOWN_BIT,
        }
    }

    fn map(self, operation: impl Fn(TruthValue) -> TruthValue) -> Self {
        let mut result = Self::empty();
        for value in truth_values() {
            if self.contains(value) {
                result = result.union(Self::singleton(operation(value)));
            }
        }
        result
    }

    fn combine(
        self,
        other: Self,
        operation: impl Fn(TruthValue, TruthValue) -> TruthValue,
    ) -> Self {
        let mut result = Self::empty();
        for left in truth_values() {
            if !self.contains(left) {
                continue;
            }
            for right in truth_values() {
                if other.contains(right) {
                    result = result.union(Self::singleton(operation(left, right)));
                }
            }
        }
        result
    }
}

impl Not for TruthSet {
    type Output = Self;

    fn not(self) -> Self::Output {
        self.map(Not::not)
    }
}

fn truth_values() -> [TruthValue; 3] {
    [TruthValue::True, TruthValue::False, TruthValue::Unknown]
}

/// The possible null states of a scalar expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarNullability {
    AlwaysNull,
    AlwaysNonNull,
    MaybeNull,
}

impl ScalarNullability {
    pub const fn can_be_null(self) -> bool {
        matches!(self, Self::AlwaysNull | Self::MaybeNull)
    }

    pub const fn can_be_non_null(self) -> bool {
        matches!(self, Self::AlwaysNonNull | Self::MaybeNull)
    }

    fn from_possibilities(can_be_null: bool, can_be_non_null: bool) -> Self {
        match (can_be_null, can_be_non_null) {
            (true, false) => Self::AlwaysNull,
            (false, true) => Self::AlwaysNonNull,
            (true, true) => Self::MaybeNull,
            (false, false) => unreachable!("a scalar expression must have a possible value"),
        }
    }
}

/// Bound scalar nodes required by the null-rejection analysis.
///
/// Binders should use [`Self::Opaque`] for unsupported expressions. A strict
/// call promises only that a null argument produces a null result; callers
/// must describe the result's nullability when every argument is non-null.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundScalarExpression {
    BindingValue {
        binding: BindingId,
        nullability: ScalarNullability,
    },
    LiteralNull,
    LiteralNonNull,
    Parameter {
        nullability: ScalarNullability,
    },
    StrictCall {
        arguments: Vec<Self>,
        result_nullability_when_arguments_non_null: ScalarNullability,
    },
    Coalesce(Vec<Self>),
    Opaque {
        referenced_bindings: BindingSet,
        nullability: ScalarNullability,
    },
}

impl BoundScalarExpression {
    pub fn referenced_bindings(&self) -> BindingSet {
        match self {
            Self::BindingValue { binding, .. } => (*binding).into(),
            Self::LiteralNull | Self::LiteralNonNull | Self::Parameter { .. } => BindingSet::new(),
            Self::StrictCall { arguments, .. } | Self::Coalesce(arguments) => {
                referenced_scalar_bindings(arguments)
            }
            Self::Opaque {
                referenced_bindings,
                ..
            } => referenced_bindings.clone(),
        }
    }

    fn nullability_when_null_extended(&self, null_bindings: &BindingSet) -> ScalarNullability {
        match self {
            Self::BindingValue {
                binding,
                nullability,
            } => {
                if null_bindings.contains(*binding) {
                    ScalarNullability::AlwaysNull
                } else {
                    *nullability
                }
            }
            Self::LiteralNull => ScalarNullability::AlwaysNull,
            Self::LiteralNonNull => ScalarNullability::AlwaysNonNull,
            Self::Parameter { nullability } => *nullability,
            Self::StrictCall {
                arguments,
                result_nullability_when_arguments_non_null,
            } => strict_call_nullability(
                arguments,
                *result_nullability_when_arguments_non_null,
                null_bindings,
            ),
            Self::Coalesce(arguments) => {
                let nullabilities = arguments
                    .iter()
                    .map(|argument| argument.nullability_when_null_extended(null_bindings));
                ScalarNullability::from_possibilities(
                    nullabilities.clone().all(ScalarNullability::can_be_null),
                    nullabilities
                        .clone()
                        .any(ScalarNullability::can_be_non_null),
                )
            }
            Self::Opaque { nullability, .. } => *nullability,
        }
    }
}

/// Bound boolean nodes required by the null-rejection analysis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundPredicate {
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
    Constant(TruthValue),
    Comparison {
        left: BoundScalarExpression,
        right: BoundScalarExpression,
    },
    IsNull(BoundScalarExpression),
    IsNotNull(BoundScalarExpression),
    InList {
        expression: BoundScalarExpression,
        values: Vec<BoundScalarExpression>,
        negated: bool,
    },
    IsDistinctFrom {
        left: BoundScalarExpression,
        right: BoundScalarExpression,
    },
    IsNotDistinctFrom {
        left: BoundScalarExpression,
        right: BoundScalarExpression,
    },
    /// Tests whether every listed binding is present in the current row.
    ///
    /// This is not a general SQL `EXISTS` subquery. Unsupported subqueries must
    /// be represented by [`Self::Opaque`].
    BindingsPresent {
        required_bindings: BindingSet,
    },
    Opaque {
        referenced_bindings: BindingSet,
    },
}

impl BoundPredicate {
    pub fn referenced_bindings(&self) -> BindingSet {
        match self {
            Self::And(predicates) | Self::Or(predicates) => {
                let mut bindings = BindingSet::new();
                for predicate in predicates {
                    bindings.extend(&predicate.referenced_bindings());
                }
                bindings
            }
            Self::Not(predicate) => predicate.referenced_bindings(),
            Self::Constant(_) => BindingSet::new(),
            Self::Comparison { left, right }
            | Self::IsDistinctFrom { left, right }
            | Self::IsNotDistinctFrom { left, right } => binary_scalar_bindings(left, right),
            Self::IsNull(expression) | Self::IsNotNull(expression) => {
                expression.referenced_bindings()
            }
            Self::InList {
                expression, values, ..
            } => {
                let mut bindings = expression.referenced_bindings();
                bindings.extend(&referenced_scalar_bindings(values));
                bindings
            }
            Self::BindingsPresent { required_bindings }
            | Self::Opaque {
                referenced_bindings: required_bindings,
            } => required_bindings.clone(),
        }
    }

    fn possible_truths_when_null_extended(&self, null_bindings: &BindingSet) -> TruthSet {
        match self {
            Self::And(predicates) => predicates.iter().fold(
                TruthSet::singleton(TruthValue::True),
                |truths, predicate| {
                    truths.and(predicate.possible_truths_when_null_extended(null_bindings))
                },
            ),
            Self::Or(predicates) => predicates.iter().fold(
                TruthSet::singleton(TruthValue::False),
                |truths, predicate| {
                    truths.or(predicate.possible_truths_when_null_extended(null_bindings))
                },
            ),
            Self::Not(predicate) => predicate
                .possible_truths_when_null_extended(null_bindings)
                .not(),
            Self::Constant(value) => TruthSet::singleton(*value),
            Self::Comparison { left, right } => strict_comparison_truths(
                left.nullability_when_null_extended(null_bindings),
                right.nullability_when_null_extended(null_bindings),
            ),
            Self::IsNull(expression) => {
                is_null_truths(expression.nullability_when_null_extended(null_bindings))
            }
            Self::IsNotNull(expression) => {
                !is_null_truths(expression.nullability_when_null_extended(null_bindings))
            }
            Self::InList {
                expression,
                values,
                negated,
            } => {
                let truths = in_list_truths(expression, values, null_bindings);
                if *negated {
                    !truths
                } else {
                    truths
                }
            }
            Self::IsDistinctFrom { left, right } => null_safe_comparison_truths(
                left.nullability_when_null_extended(null_bindings),
                right.nullability_when_null_extended(null_bindings),
            ),
            Self::IsNotDistinctFrom { left, right } => null_safe_comparison_truths(
                left.nullability_when_null_extended(null_bindings),
                right.nullability_when_null_extended(null_bindings),
            )
            .not(),
            Self::BindingsPresent { required_bindings } => {
                if required_bindings.intersects(null_bindings) {
                    TruthSet::singleton(TruthValue::False)
                } else {
                    TruthSet::singleton(TruthValue::True)
                        .union(TruthSet::singleton(TruthValue::False))
                }
            }
            Self::Opaque { .. } => TruthSet::all(),
        }
    }
}

/// The result of attempting to prove that a predicate rejects null extension.
///
/// A proof is valid only when `TRUE` is absent from the over-approximated truth
/// set. `NotProven` is intentionally not a claim that the predicate accepts a
/// null-extended row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullRejectionProof {
    Proven { possible_truths: TruthSet },
    NotProven { possible_truths: TruthSet },
}

impl NullRejectionProof {
    pub const fn is_proven(self) -> bool {
        matches!(self, Self::Proven { .. })
    }

    pub const fn possible_truths(self) -> TruthSet {
        match self {
            Self::Proven { possible_truths } | Self::NotProven { possible_truths } => {
                possible_truths
            }
        }
    }
}

/// Proves whether `predicate` can never be `TRUE` after null-extending bindings.
///
/// The analysis is conservative: unsupported expressions retain all possible
/// truth values and therefore cannot independently authorize a join rewrite.
pub fn prove_null_rejecting(
    predicate: &BoundPredicate,
    null_bindings: &BindingSet,
) -> NullRejectionProof {
    let possible_truths = predicate.possible_truths_when_null_extended(null_bindings);
    if possible_truths.contains(TruthValue::True) {
        NullRejectionProof::NotProven { possible_truths }
    } else {
        NullRejectionProof::Proven { possible_truths }
    }
}

fn referenced_scalar_bindings(expressions: &[BoundScalarExpression]) -> BindingSet {
    let mut bindings = BindingSet::new();
    for expression in expressions {
        bindings.extend(&expression.referenced_bindings());
    }
    bindings
}

fn binary_scalar_bindings(
    left: &BoundScalarExpression,
    right: &BoundScalarExpression,
) -> BindingSet {
    let mut bindings = left.referenced_bindings();
    bindings.extend(&right.referenced_bindings());
    bindings
}

fn strict_call_nullability(
    arguments: &[BoundScalarExpression],
    result_when_non_null: ScalarNullability,
    null_bindings: &BindingSet,
) -> ScalarNullability {
    let nullabilities = arguments
        .iter()
        .map(|argument| argument.nullability_when_null_extended(null_bindings));
    let arguments_can_all_be_non_null = nullabilities
        .clone()
        .all(ScalarNullability::can_be_non_null);
    if !arguments_can_all_be_non_null {
        return ScalarNullability::AlwaysNull;
    }

    let an_argument_can_be_null = nullabilities.clone().any(ScalarNullability::can_be_null);
    ScalarNullability::from_possibilities(
        an_argument_can_be_null || result_when_non_null.can_be_null(),
        result_when_non_null.can_be_non_null(),
    )
}

fn strict_comparison_truths(left: ScalarNullability, right: ScalarNullability) -> TruthSet {
    let mut truths = TruthSet::empty();
    if left.can_be_null() || right.can_be_null() {
        truths = truths.union(TruthSet::singleton(TruthValue::Unknown));
    }
    if left.can_be_non_null() && right.can_be_non_null() {
        truths = truths
            .union(TruthSet::singleton(TruthValue::True))
            .union(TruthSet::singleton(TruthValue::False));
    }
    truths
}

fn is_null_truths(nullability: ScalarNullability) -> TruthSet {
    let mut truths = TruthSet::empty();
    if nullability.can_be_null() {
        truths = truths.union(TruthSet::singleton(TruthValue::True));
    }
    if nullability.can_be_non_null() {
        truths = truths.union(TruthSet::singleton(TruthValue::False));
    }
    truths
}

fn in_list_truths(
    expression: &BoundScalarExpression,
    values: &[BoundScalarExpression],
    null_bindings: &BindingSet,
) -> TruthSet {
    let expression = expression.nullability_when_null_extended(null_bindings);
    let value_nullabilities: Vec<_> = values
        .iter()
        .map(|value| value.nullability_when_null_extended(null_bindings))
        .collect();
    let mut truths = TruthSet::empty();

    if expression.can_be_null() {
        truths = truths.union(TruthSet::singleton(TruthValue::Unknown));
    }
    if !expression.can_be_non_null() {
        return truths;
    }
    if value_nullabilities.is_empty() {
        return truths.union(TruthSet::singleton(TruthValue::False));
    }
    if value_nullabilities
        .iter()
        .any(|value| value.can_be_non_null())
    {
        truths = truths.union(TruthSet::singleton(TruthValue::True));
    }
    if value_nullabilities.iter().any(|value| value.can_be_null()) {
        truths = truths.union(TruthSet::singleton(TruthValue::Unknown));
    }
    if value_nullabilities
        .iter()
        .all(|value| value.can_be_non_null())
    {
        truths = truths.union(TruthSet::singleton(TruthValue::False));
    }
    truths
}

fn null_safe_comparison_truths(left: ScalarNullability, right: ScalarNullability) -> TruthSet {
    let both_null = left.can_be_null() && right.can_be_null();
    let exactly_one_null = (left.can_be_null() && right.can_be_non_null())
        || (left.can_be_non_null() && right.can_be_null());
    let both_non_null = left.can_be_non_null() && right.can_be_non_null();
    let mut truths = TruthSet::empty();
    if both_null || both_non_null {
        truths = truths.union(TruthSet::singleton(TruthValue::False));
    }
    if exactly_one_null || both_non_null {
        truths = truths.union(TruthSet::singleton(TruthValue::True));
    }
    truths
}

#[cfg(test)]
mod tests {
    use super::*;

    const LEFT: BindingId = BindingId::new(1);
    const RIGHT: BindingId = BindingId::new(2);

    fn binding_value(binding: BindingId) -> BoundScalarExpression {
        BoundScalarExpression::BindingValue {
            binding,
            nullability: ScalarNullability::MaybeNull,
        }
    }

    fn non_null_literal() -> BoundScalarExpression {
        BoundScalarExpression::LiteralNonNull
    }

    fn comparison(binding: BindingId) -> BoundPredicate {
        BoundPredicate::Comparison {
            left: binding_value(binding),
            right: non_null_literal(),
        }
    }

    fn assert_proven(predicate: &BoundPredicate, bindings: impl Into<BindingSet>) {
        assert!(prove_null_rejecting(predicate, &bindings.into()).is_proven());
    }

    fn assert_not_proven(predicate: &BoundPredicate, bindings: impl Into<BindingSet>) {
        assert!(!prove_null_rejecting(predicate, &bindings.into()).is_proven());
    }

    #[test]
    fn truth_value_implements_sql_three_valued_logic() {
        let values = [TruthValue::True, TruthValue::False, TruthValue::Unknown];
        let expected_not = [TruthValue::False, TruthValue::True, TruthValue::Unknown];
        let expected_and = [
            [TruthValue::True, TruthValue::False, TruthValue::Unknown],
            [TruthValue::False, TruthValue::False, TruthValue::False],
            [TruthValue::Unknown, TruthValue::False, TruthValue::Unknown],
        ];
        let expected_or = [
            [TruthValue::True, TruthValue::True, TruthValue::True],
            [TruthValue::True, TruthValue::False, TruthValue::Unknown],
            [TruthValue::True, TruthValue::Unknown, TruthValue::Unknown],
        ];

        for (left_index, left) in values.into_iter().enumerate() {
            assert_eq!(!left, expected_not[left_index]);
            for (right_index, right) in values.into_iter().enumerate() {
                assert_eq!(left.and(right), expected_and[left_index][right_index]);
                assert_eq!(left.or(right), expected_or[left_index][right_index]);
            }
        }

        let true_or_unknown =
            TruthSet::singleton(TruthValue::True).union(TruthSet::singleton(TruthValue::Unknown));
        assert_eq!(
            !true_or_unknown,
            TruthSet::singleton(TruthValue::False).union(TruthSet::singleton(TruthValue::Unknown))
        );
    }

    #[test]
    fn strict_predicates_reject_null_extended_binding() {
        assert_proven(&comparison(RIGHT), RIGHT);
        assert_proven(&BoundPredicate::Not(Box::new(comparison(RIGHT))), RIGHT);
        assert_proven(&BoundPredicate::IsNotNull(binding_value(RIGHT)), RIGHT);
        assert_not_proven(&BoundPredicate::IsNull(binding_value(RIGHT)), RIGHT);
    }

    #[test]
    fn conjunction_and_disjunction_preserve_possible_true_rows() {
        let left_comparison = comparison(LEFT);
        let right_comparison = comparison(RIGHT);
        assert_proven(
            &BoundPredicate::And(vec![left_comparison.clone(), right_comparison.clone()]),
            RIGHT,
        );
        assert_not_proven(
            &BoundPredicate::Or(vec![left_comparison, right_comparison]),
            RIGHT,
        );
    }

    #[test]
    fn coalesce_can_make_a_null_extended_value_non_null() {
        let predicate = BoundPredicate::Comparison {
            left: BoundScalarExpression::Coalesce(vec![binding_value(RIGHT), non_null_literal()]),
            right: non_null_literal(),
        };

        assert_not_proven(&predicate, RIGHT);
    }

    #[test]
    fn null_safe_comparisons_distinguish_null_and_non_null_values() {
        let distinct = BoundPredicate::IsDistinctFrom {
            left: binding_value(RIGHT),
            right: non_null_literal(),
        };
        let not_distinct = BoundPredicate::IsNotDistinctFrom {
            left: binding_value(RIGHT),
            right: non_null_literal(),
        };
        let nulls_not_distinct = BoundPredicate::IsNotDistinctFrom {
            left: binding_value(RIGHT),
            right: BoundScalarExpression::LiteralNull,
        };

        assert_not_proven(&distinct, RIGHT);
        assert_proven(&not_distinct, RIGHT);
        assert_not_proven(&nulls_not_distinct, RIGHT);
    }

    #[test]
    fn in_and_not_in_reject_a_null_search_value() {
        let in_list = BoundPredicate::InList {
            expression: binding_value(RIGHT),
            values: vec![non_null_literal(), BoundScalarExpression::LiteralNull],
            negated: false,
        };
        let not_in_list = BoundPredicate::InList {
            expression: binding_value(RIGHT),
            values: vec![non_null_literal(), BoundScalarExpression::LiteralNull],
            negated: true,
        };

        assert_proven(&in_list, RIGHT);
        assert_proven(&not_in_list, RIGHT);
    }

    #[test]
    fn a_binding_set_can_be_rejected_when_each_singleton_is_not() {
        let predicate = BoundPredicate::Or(vec![
            BoundPredicate::IsNotNull(binding_value(LEFT)),
            BoundPredicate::IsNotNull(binding_value(RIGHT)),
        ]);

        assert_not_proven(&predicate, LEFT);
        assert_not_proven(&predicate, RIGHT);
        assert_proven(&predicate, [LEFT, RIGHT]);
    }

    #[test]
    fn existence_tracks_null_extended_bindings() {
        let exists = BoundPredicate::BindingsPresent {
            required_bindings: RIGHT.into(),
        };

        assert_proven(&exists, RIGHT);
        assert_not_proven(&BoundPredicate::Not(Box::new(exists)), RIGHT);
    }

    #[test]
    fn opaque_predicates_never_claim_a_proof() {
        let predicate = BoundPredicate::Opaque {
            referenced_bindings: RIGHT.into(),
        };

        let proof = prove_null_rejecting(&predicate, &RIGHT.into());
        assert_eq!(
            proof,
            NullRejectionProof::NotProven {
                possible_truths: TruthSet::all(),
            }
        );
    }

    #[test]
    fn referenced_bindings_remain_separate_from_null_rejection() {
        let constant_false = BoundPredicate::Constant(TruthValue::False);

        assert!(constant_false.referenced_bindings().iter().next().is_none());
        assert_proven(&constant_false, RIGHT);
    }

    #[test]
    fn strict_calls_do_not_assume_non_null_output() {
        let maybe_null_call = BoundScalarExpression::StrictCall {
            arguments: vec![non_null_literal()],
            result_nullability_when_arguments_non_null: ScalarNullability::MaybeNull,
        };
        let nullable_result = BoundPredicate::IsNull(maybe_null_call);
        let null_argument = BoundPredicate::Comparison {
            left: BoundScalarExpression::StrictCall {
                arguments: vec![binding_value(RIGHT)],
                result_nullability_when_arguments_non_null: ScalarNullability::AlwaysNonNull,
            },
            right: non_null_literal(),
        };

        assert_not_proven(&nullable_result, RIGHT);
        assert_proven(&null_argument, RIGHT);
    }
}
