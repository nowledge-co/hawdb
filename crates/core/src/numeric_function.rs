// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! openCypher numeric scalar functions over one argument.
//!
//! Semantics follow the Neo4j Cypher Manual ("Mathematical functions -
//! numeric", <https://neo4j.com/docs/cypher-manual/current/functions/mathematical-numeric/>)
//! and the openCypher TCK (`expressions/mathematical/Mathematical11.feature`:
//! `abs(-1)` is the integer `1`; `Mathematical13.feature`: `sqrt(12.96)` is
//! `3.6`):
//! - every function returns `NULL` for a `NULL` argument;
//! - `abs` keeps the argument type (`INTEGER` stays `INTEGER`);
//! - `ceil`, `floor`, `round` and `sqrt` return `FLOAT`, also for an
//!   `INTEGER` argument (which is widened, as `toFloat` does);
//! - `sign` returns the `INTEGER` `-1`, `0` or `1`;
//! - `round` rounds ties toward positive infinity, as Neo4j documents for its
//!   one-argument form ("Ties are rounded towards positive infinity, therefore
//!   `-1.0` is returned" for `round(-1.5)`); `round(2.5)` is `3.0`.
//!
//! Other argument types are errors, like the conversion functions.

use crate::conversion_function::type_name;
use crate::{HawDBError, Result, Value, ValueRef};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum NumericFunction {
    /// `abs(n)`: the absolute value, in the argument's type.
    Abs,
    /// `ceil(n)`: the smallest integral `FLOAT` not below `n`.
    Ceil,
    /// `floor(n)`: the largest integral `FLOAT` not above `n`.
    Floor,
    /// `round(n)`: the nearest integral `FLOAT`, ties toward +infinity.
    Round,
    /// `sign(n)`: `-1`, `0` or `1` as an `INTEGER`.
    Sign,
    /// `sqrt(n)`: the square root as a `FLOAT` (`NaN` for a negative `n`).
    Sqrt,
}

impl NumericFunction {
    pub const ALL: [Self; 6] = [
        Self::Abs,
        Self::Ceil,
        Self::Floor,
        Self::Round,
        Self::Sign,
        Self::Sqrt,
    ];

    pub fn from_function_name(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|function| name.eq_ignore_ascii_case(function.function_name()))
    }

    pub const fn function_name(self) -> &'static str {
        match self {
            Self::Abs => "abs",
            Self::Ceil => "ceil",
            Self::Floor => "floor",
            Self::Round => "round",
            Self::Sign => "sign",
            Self::Sqrt => "sqrt",
        }
    }

    pub fn apply(self, value: &Value) -> Result<Value> {
        self.apply_ref(value.as_ref())
    }

    pub fn apply_ref(self, value: ValueRef<'_>) -> Result<Value> {
        let value = match value {
            ValueRef::Null => return Ok(Value::Null),
            ValueRef::Int(value) => Number::Int(value),
            ValueRef::Float(value) => Number::Float(value),
            value => {
                return Err(HawDBError::Execution(format!(
                    "{}() requires an INTEGER or FLOAT argument, got {}",
                    self.function_name(),
                    type_name(value)
                )))
            }
        };
        Ok(match (self, value) {
            (Self::Abs, Number::Int(value)) => Value::Int(
                value
                    .checked_abs()
                    .ok_or_else(|| HawDBError::Execution("abs() integer overflow".to_string()))?,
            ),
            (Self::Abs, Number::Float(value)) => Value::Float(value.abs()),
            (Self::Sign, Number::Int(value)) => Value::Int(value.signum()),
            (Self::Sign, Number::Float(value)) => Value::Int(if value > 0.0 {
                1
            } else if value < 0.0 {
                -1
            } else {
                // Zero, negative zero and NaN.
                0
            }),
            (function, value) => {
                let value = match value {
                    Number::Int(value) => value as f64,
                    Number::Float(value) => value,
                };
                Value::Float(match function {
                    Self::Ceil => value.ceil(),
                    Self::Floor => value.floor(),
                    Self::Round => round_half_up(value),
                    Self::Sqrt => value.sqrt(),
                    Self::Abs | Self::Sign => unreachable!("handled above"),
                })
            }
        })
    }
}

enum Number {
    Int(i64),
    Float(f64),
}

/// Nearest integer with ties toward positive infinity. `value - floor(value)`
/// is exact for every finite float, so ties are detected without the
/// `floor(value + 0.5)` error at 0.49999999999999994.
fn round_half_up(value: f64) -> f64 {
    let floor = value.floor();
    if value - floor >= 0.5 {
        floor + 1.0
    } else {
        floor
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn apply(function: NumericFunction, value: Value) -> Value {
        function.apply(&value).unwrap()
    }

    #[test]
    fn names_round_trip_case_insensitively() {
        for function in NumericFunction::ALL {
            assert_eq!(
                NumericFunction::from_function_name(&function.function_name().to_uppercase()),
                Some(function)
            );
        }
        assert_eq!(NumericFunction::from_function_name("toFloat"), None);
    }

    #[test]
    fn null_arguments_are_null() {
        for function in NumericFunction::ALL {
            assert_eq!(apply(function, Value::Null), Value::Null);
        }
    }

    #[test]
    fn abs_and_sign_keep_integers_and_rounding_returns_floats() {
        use NumericFunction::*;
        assert_eq!(apply(Abs, Value::Int(-1)), Value::Int(1));
        assert_eq!(apply(Abs, Value::Float(-2.5)), Value::Float(2.5));
        assert!(Abs.apply(&Value::Int(i64::MIN)).is_err());
        assert_eq!(apply(Sign, Value::Int(-7)), Value::Int(-1));
        assert_eq!(apply(Sign, Value::Float(0.0)), Value::Int(0));
        assert_eq!(apply(Sign, Value::Float(0.1)), Value::Int(1));
        assert_eq!(apply(Floor, Value::Int(3)), Value::Float(3.0));
        assert_eq!(apply(Floor, Value::Float(-1.5)), Value::Float(-2.0));
        assert_eq!(apply(Ceil, Value::Float(-1.5)), Value::Float(-1.0));
        assert_eq!(apply(Ceil, Value::Float(1.2)), Value::Float(2.0));
        assert_eq!(apply(Sqrt, Value::Float(12.96)), Value::Float(3.6));
        assert_eq!(apply(Sqrt, Value::Int(4)), Value::Float(2.0));
    }

    #[test]
    fn round_ties_toward_positive_infinity() {
        use NumericFunction::Round;
        for (input, expected) in [
            (2.5, 3.0),
            (-1.5, -1.0),
            (-2.5, -2.0),
            (-1.6, -2.0),
            (1.4, 1.0),
            (0.499_999_999_999_999_94, 0.0),
            (3.0, 3.0),
        ] {
            assert_eq!(apply(Round, Value::Float(input)), Value::Float(expected));
        }
        assert_eq!(apply(Round, Value::Int(-3)), Value::Float(-3.0));
    }

    #[test]
    fn other_types_are_errors() {
        let error = NumericFunction::Floor
            .apply(&Value::String("1".into()))
            .unwrap_err()
            .to_string();
        assert!(error.contains("floor() requires an INTEGER or FLOAT argument, got STRING"));
    }
}
