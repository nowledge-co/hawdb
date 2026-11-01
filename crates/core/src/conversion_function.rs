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

//! openCypher scalar type-conversion functions.
//!
//! The functions follow Neo4j 5 semantics for the value types HawDB stores:
//! `NULL` converts to `NULL`, an unparseable string converts to `NULL`, and an
//! argument type the function does not accept is an error rather than `NULL`.

use crate::{HawDBError, Result, Value, ValueRef};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ConversionFunction {
    /// `toInteger(value)`: integers, floats truncated toward zero, booleans as
    /// `1`/`0`, and decimal integer or float strings.
    ToInteger,
    /// `toFloat(value)`: integers, floats and decimal number strings.
    ToFloat,
    /// `toBoolean(value)`: booleans, integers (`0` is false) and the strings
    /// `true`/`false` in any letter case.
    ToBoolean,
    /// `toString(value)`: strings, numbers, booleans, UUIDs and temporal
    /// values (rendered in ISO 8601, as Neo4j does).
    ToString,
}

impl ConversionFunction {
    pub const ALL: [Self; 4] = [
        Self::ToInteger,
        Self::ToFloat,
        Self::ToBoolean,
        Self::ToString,
    ];

    pub fn from_function_name(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|function| name.eq_ignore_ascii_case(function.function_name()))
    }

    pub const fn function_name(self) -> &'static str {
        match self {
            Self::ToInteger => "toInteger",
            Self::ToFloat => "toFloat",
            Self::ToBoolean => "toBoolean",
            Self::ToString => "toString",
        }
    }

    pub fn apply(self, value: &Value) -> Result<Value> {
        self.apply_ref(value.as_ref())
    }

    pub fn apply_ref(self, value: ValueRef<'_>) -> Result<Value> {
        if matches!(value, ValueRef::Null) {
            return Ok(Value::Null);
        }
        let converted = match (self, value) {
            (Self::ToInteger, ValueRef::Int(value)) => Some(Value::Int(value)),
            (Self::ToInteger, ValueRef::Float(value)) => float_to_integer(value).map(Value::Int),
            (Self::ToInteger, ValueRef::Bool(value)) => Some(Value::Int(i64::from(value))),
            (Self::ToInteger, ValueRef::String(value)) => {
                Some(string_to_integer(value).map_or(Value::Null, Value::Int))
            }
            (Self::ToFloat, ValueRef::Float(value)) => Some(Value::Float(value)),
            (Self::ToFloat, ValueRef::Int(value)) => Some(Value::Float(value as f64)),
            (Self::ToFloat, ValueRef::String(value)) => {
                Some(string_to_float(value).map_or(Value::Null, Value::Float))
            }
            (Self::ToBoolean, ValueRef::Bool(value)) => Some(Value::Bool(value)),
            (Self::ToBoolean, ValueRef::Int(value)) => Some(Value::Bool(value != 0)),
            (Self::ToBoolean, ValueRef::String(value)) => {
                let value = value.trim();
                Some(if value.eq_ignore_ascii_case("true") {
                    Value::Bool(true)
                } else if value.eq_ignore_ascii_case("false") {
                    Value::Bool(false)
                } else {
                    Value::Null
                })
            }
            (Self::ToString, ValueRef::String(value)) => Some(Value::String(value.to_owned())),
            (Self::ToString, ValueRef::Int(value)) => Some(Value::String(value.to_string())),
            (Self::ToString, ValueRef::Float(value)) => Some(Value::String(format_float(value))),
            (Self::ToString, ValueRef::Bool(value)) => Some(Value::String(value.to_string())),
            (Self::ToString, ValueRef::Uuid(value)) => Some(Value::String(value.to_string())),
            (Self::ToString, ValueRef::Date(value)) => {
                Some(Value::String(crate::temporal::format_date(value)))
            }
            (Self::ToString, ValueRef::LocalTime(value)) => {
                Some(Value::String(crate::temporal::format_local_time(value)))
            }
            (Self::ToString, ValueRef::LocalDateTime(value)) => {
                Some(Value::String(crate::temporal::format_local_datetime(value)))
            }
            (Self::ToString, ValueRef::DateTime(value)) => {
                Some(Value::String(crate::temporal::format_datetime(value)))
            }
            (Self::ToString, ValueRef::Duration(value)) => Some(Value::String(value.to_string())),
            _ => None,
        };
        // A float outside the i64 range has no integer value; Neo4j reports
        // such arguments as errors rather than silently saturating.
        converted.ok_or_else(|| {
            HawDBError::Execution(format!(
                "{}() cannot convert a {} value",
                self.function_name(),
                type_name(value)
            ))
        })
    }
}

fn float_to_integer(value: f64) -> Option<i64> {
    let truncated = value.trunc();
    // i64::MAX is not exactly representable; 2^63 is the first excluded value.
    (truncated.is_finite() && truncated >= -(2f64.powi(63)) && truncated < 2f64.powi(63))
        .then_some(truncated as i64)
}

fn string_to_integer(value: &str) -> Option<i64> {
    let value = value.trim();
    if let Ok(integer) = value.parse::<i64>() {
        return Some(integer);
    }
    // Neo4j accepts float strings and truncates them; out-of-range values
    // and non-numbers in a string are NULL, not errors.
    string_to_float(value).and_then(float_to_integer)
}

fn string_to_float(value: &str) -> Option<f64> {
    let value = value.trim();
    // Rust also parses `inf`, `infinity` and `nan`; keep the decimal grammar
    // plus Java's spelled-out `NaN`/`Infinity` that Neo4j's parser accepts.
    let numeric = value
        .strip_prefix(['+', '-'])
        .unwrap_or(value)
        .bytes()
        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'.' | b'e' | b'E' | b'+' | b'-'));
    if numeric && value.bytes().any(|byte| byte.is_ascii_digit()) {
        return value.parse::<f64>().ok();
    }
    match value {
        "NaN" => Some(f64::NAN),
        "Infinity" | "+Infinity" => Some(f64::INFINITY),
        "-Infinity" => Some(f64::NEG_INFINITY),
        _ => None,
    }
}

/// Formats a float like Java's `Double.toString`, which Neo4j uses.
fn format_float(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_owned();
    }
    if value.is_infinite() {
        return if value > 0.0 { "Infinity" } else { "-Infinity" }.to_owned();
    }
    let magnitude = value.abs();
    if magnitude == 0.0 || (1e-3..1e7).contains(&magnitude) {
        let text = value.to_string();
        return if text.contains('.') {
            text
        } else {
            format!("{text}.0")
        };
    }
    let text = format!("{value:e}");
    let (mantissa, exponent) = text.split_once('e').expect("exponent format");
    if mantissa.contains('.') {
        format!("{mantissa}E{exponent}")
    } else {
        format!("{mantissa}.0E{exponent}")
    }
}

fn type_name(value: ValueRef<'_>) -> &'static str {
    match value {
        ValueRef::Null => "NULL",
        ValueRef::Bool(_) => "BOOLEAN",
        ValueRef::Int(_) => "INTEGER",
        ValueRef::Float(_) => "FLOAT",
        ValueRef::String(_) => "STRING",
        ValueRef::Binary(_) => "BYTES",
        ValueRef::Vector(_) => "VECTOR",
        ValueRef::Uuid(_) => "UUID",
        ValueRef::List(_) => "LIST",
        ValueRef::Map(_) => "MAP",
        ValueRef::Date(_) => "DATE",
        ValueRef::LocalTime(_) => "LOCAL TIME",
        ValueRef::LocalDateTime(_) => "LOCAL DATETIME",
        ValueRef::DateTime(_) => "ZONED DATETIME",
        ValueRef::Duration(_) => "DURATION",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ConversionFunction::*;

    fn apply(function: ConversionFunction, value: Value) -> Value {
        function.apply(&value).unwrap()
    }

    fn string(value: &str) -> Value {
        Value::String(value.to_owned())
    }

    #[test]
    fn function_names_round_trip_case_insensitively() {
        for function in ConversionFunction::ALL {
            assert_eq!(
                ConversionFunction::from_function_name(&function.function_name().to_uppercase()),
                Some(function)
            );
        }
        assert_eq!(ConversionFunction::from_function_name("toList"), None);
    }

    #[test]
    fn null_converts_to_null() {
        for function in ConversionFunction::ALL {
            assert_eq!(apply(function, Value::Null), Value::Null);
        }
    }

    #[test]
    fn to_integer_parses_truncates_and_rejects_invalid_strings_as_null() {
        assert_eq!(apply(ToInteger, string("42")), Value::Int(42));
        assert_eq!(apply(ToInteger, string(" -7 ")), Value::Int(-7));
        assert_eq!(apply(ToInteger, string("2.9")), Value::Int(2));
        assert_eq!(apply(ToInteger, string("-2.9")), Value::Int(-2));
        assert_eq!(apply(ToInteger, string("1e3")), Value::Int(1000));
        assert_eq!(apply(ToInteger, string("abc")), Value::Null);
        assert_eq!(apply(ToInteger, string("")), Value::Null);
        assert_eq!(apply(ToInteger, string("inf")), Value::Null);
        assert_eq!(apply(ToInteger, string("1e300")), Value::Null);
        assert_eq!(
            apply(ToInteger, string("9223372036854775807")),
            Value::Int(i64::MAX)
        );
        assert_eq!(apply(ToInteger, Value::Float(3.7)), Value::Int(3));
        assert_eq!(apply(ToInteger, Value::Bool(true)), Value::Int(1));
        assert!(ToInteger.apply(&Value::Float(f64::NAN)).is_err());
        assert!(ToInteger.apply(&Value::List(Vec::new())).is_err());
    }

    #[test]
    fn to_float_parses_numbers_and_rejects_invalid_strings_as_null() {
        assert_eq!(apply(ToFloat, string("1.5")), Value::Float(1.5));
        assert_eq!(apply(ToFloat, string(" 2 ")), Value::Float(2.0));
        assert_eq!(apply(ToFloat, Value::Int(3)), Value::Float(3.0));
        assert_eq!(apply(ToFloat, string("1.5x")), Value::Null);
        assert_eq!(apply(ToFloat, string("inf")), Value::Null);
        assert_eq!(
            apply(ToFloat, string("Infinity")),
            Value::Float(f64::INFINITY)
        );
        assert!(ToFloat.apply(&Value::Bool(true)).is_err());
    }

    #[test]
    fn to_boolean_accepts_case_insensitive_words_and_integers() {
        assert_eq!(apply(ToBoolean, string("TRUE")), Value::Bool(true));
        assert_eq!(apply(ToBoolean, string(" false")), Value::Bool(false));
        assert_eq!(apply(ToBoolean, string("yes")), Value::Null);
        assert_eq!(apply(ToBoolean, Value::Int(0)), Value::Bool(false));
        assert_eq!(apply(ToBoolean, Value::Int(-3)), Value::Bool(true));
        assert!(ToBoolean.apply(&Value::Float(1.0)).is_err());
    }

    #[test]
    fn to_string_formats_like_neo4j() {
        assert_eq!(apply(ToString, Value::Int(-5)), string("-5"));
        assert_eq!(apply(ToString, Value::Float(1.0)), string("1.0"));
        assert_eq!(apply(ToString, Value::Float(0.25)), string("0.25"));
        assert_eq!(apply(ToString, Value::Float(1e20)), string("1.0E20"));
        assert_eq!(apply(ToString, Value::Float(1.5e-5)), string("1.5E-5"));
        assert_eq!(apply(ToString, Value::Bool(false)), string("false"));
        assert_eq!(apply(ToString, string("x")), string("x"));
        assert!(ToString.apply(&Value::Map(Default::default())).is_err());
    }

    #[test]
    fn to_string_renders_temporal_values_in_iso_8601() {
        use crate::temporal::{FixedOffset, NaiveDate, NaiveTime};
        let date = NaiveDate::from_ymd_opt(2026, 10, 30).unwrap();
        let time = NaiveTime::from_hms_milli_opt(12, 34, 56, 789).unwrap();
        let local = date.and_time(time);
        let zoned = local
            .and_local_timezone(FixedOffset::east_opt(3_600).unwrap())
            .unwrap();
        assert_eq!(apply(ToString, Value::Date(date)), string("2026-10-30"));
        assert_eq!(
            apply(ToString, Value::LocalTime(time)),
            string("12:34:56.789")
        );
        assert_eq!(
            apply(ToString, Value::LocalDateTime(local)),
            string("2026-10-30T12:34:56.789")
        );
        assert_eq!(
            apply(ToString, Value::DateTime(zoned)),
            string("2026-10-30T12:34:56.789+01:00")
        );
        let duration = crate::temporal::Duration::new(14, 3, crate::temporal::TimeDelta::hours(5));
        assert_eq!(
            apply(ToString, Value::duration(duration)),
            string("P1Y2M3DT5H")
        );
        for function in [ToInteger, ToFloat, ToBoolean] {
            let error = function.apply(&Value::Date(date)).unwrap_err().to_string();
            assert!(error.contains("cannot convert a DATE value"), "{error}");
        }
    }
}
