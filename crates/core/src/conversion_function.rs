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
//! `string(x)` (alias `to_string`) is the Kùzu cast instead: it renders the
//! text Kùzu 0.11 produces, which differs from `toString` for floats,
//! booleans, timestamps and composite values.

mod kuzu_text;

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
    /// Kùzu `string(value)`, also spelled `to_string(value)`: the exact text
    /// Kùzu 0.11 renders for the value, e.g. `2024-01-02 03:04:05.12` for a
    /// timestamp, `1.000000` for a float and `True` for a boolean.
    KuzuString,
}

impl ConversionFunction {
    pub const ALL: [Self; 5] = [
        Self::ToInteger,
        Self::ToFloat,
        Self::ToBoolean,
        Self::ToString,
        Self::KuzuString,
    ];

    /// The Kùzu spelling `to_string` of [`Self::KuzuString`].
    pub const KUZU_STRING_ALIAS: &'static str = "to_string";

    pub fn from_function_name(name: &str) -> Option<Self> {
        if name.eq_ignore_ascii_case(Self::KUZU_STRING_ALIAS) {
            return Some(Self::KuzuString);
        }
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
            Self::KuzuString => "string",
        }
    }

    pub fn apply(self, value: &Value) -> Result<Value> {
        self.apply_ref(value.as_ref())
    }

    /// [`Self::apply_ref`] that calls `charge` with the byte length of a
    /// string result before allocating it, so a budget refusal happens
    /// first. Results whose size can exceed a small constant (`string(x)` of
    /// composite or float values, `toString` of a string) are measured
    /// without allocating; the remaining string results are a few dozen
    /// bytes and are charged right after conversion.
    pub fn apply_ref_charged(
        self,
        value: ValueRef<'_>,
        charge: &mut dyn FnMut(usize) -> Result<()>,
    ) -> Result<Value> {
        match (self, value) {
            (_, ValueRef::Null) => Ok(Value::Null),
            (Self::KuzuString, value) => {
                let len = kuzu_text::rendered_len(value)?;
                charge(len)?;
                kuzu_text::render(value, len).map(Value::String)
            }
            (Self::ToString, ValueRef::String(text)) => {
                charge(text.len())?;
                Ok(Value::String(text.to_owned()))
            }
            _ => {
                let converted = self.apply_ref(value)?;
                if let Value::String(text) = &converted {
                    charge(text.len())?;
                }
                Ok(converted)
            }
        }
    }

    /// Conversion outside a query memory account (plan-time parameter binding
    /// and batch mutation rows): a string result longer than
    /// [`crate::MAX_UNACCOUNTED_LIST_BYTES`] is refused before allocation.
    pub fn apply_unaccounted(self, value: &Value) -> Result<Value> {
        self.apply_ref_charged(value.as_ref(), &mut |bytes| {
            if bytes > crate::MAX_UNACCOUNTED_LIST_BYTES {
                return Err(HawDBError::Execution(format!(
                    "{}() result of {bytes} bytes exceeds {} bytes without a query memory account",
                    self.function_name(),
                    crate::MAX_UNACCOUNTED_LIST_BYTES
                )));
            }
            Ok(())
        })
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
            (Self::KuzuString, value) => {
                let len = kuzu_text::rendered_len(value)?;
                return kuzu_text::render(value, len).map(Value::String);
            }
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

pub(crate) fn type_name(value: ValueRef<'_>) -> &'static str {
    match value {
        ValueRef::Null => "NULL",
        ValueRef::Bool(_) => "BOOLEAN",
        ValueRef::Int(_) => "INTEGER",
        ValueRef::Decimal(_) => "DECIMAL",
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

    fn timestamp(text: &str) -> Value {
        Value::LocalDateTime(
            crate::temporal::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f").unwrap(),
        )
    }

    fn interval(months: i64, days: i64, micros: i128) -> Value {
        Value::duration(
            crate::temporal::Duration::from_parts(months, days, micros * 1_000).unwrap(),
        )
    }

    /// Every expected string was measured with Kùzu 0.11.3 `RETURN string(x)`
    /// (issue #1682 table plus the edge cases listed in its PR).
    #[test]
    fn kuzu_string_matches_kuzu_0_11_text() {
        use crate::temporal::{FixedOffset, NaiveDate};
        let date = |y, m, d| NaiveDate::from_ymd_opt(y, m, d).unwrap();
        let zoned = |text: &str| {
            Value::DateTime(
                crate::temporal::DateTime::<FixedOffset>::parse_from_rfc3339(text).unwrap(),
            )
        };
        let map = |entries: &[(&str, Value)]| {
            Value::Map(
                entries
                    .iter()
                    .map(|(key, value)| ((*key).to_owned(), value.clone()))
                    .collect(),
            )
        };
        let cases: Vec<(Value, &str)> = vec![
            // Timestamps: space separator, no zone, trimmed microseconds.
            (timestamp("2024-01-02 03:04:05"), "2024-01-02 03:04:05"),
            (
                timestamp("2024-01-02 03:04:05.120"),
                "2024-01-02 03:04:05.12",
            ),
            (
                timestamp("2024-01-02 03:04:05.123456"),
                "2024-01-02 03:04:05.123456",
            ),
            (
                timestamp("2024-01-02 03:04:05.000001"),
                "2024-01-02 03:04:05.000001",
            ),
            (
                timestamp("2024-01-02 03:04:05.1234567"),
                "2024-01-02 03:04:05.123456",
            ),
            (timestamp("0099-01-02 03:04:05"), "0099-01-02 03:04:05"),
            (
                Value::LocalDateTime(date(10_000, 1, 2).and_hms_opt(3, 4, 5).unwrap()),
                "10000-01-02 03:04:05",
            ),
            (zoned("2024-01-02T03:04:05Z"), "2024-01-02 03:04:05"),
            (zoned("2024-01-02T03:04:05+02:00"), "2024-01-02 01:04:05"),
            (
                Value::LocalDateTime(date(0, 12, 31).and_hms_opt(0, 0, 0).unwrap()),
                "0001-12-31 (BC) 00:00:00",
            ),
            // Dates.
            (Value::Date(date(2024, 1, 2)), "2024-01-02"),
            (Value::Date(date(99, 1, 2)), "0099-01-02"),
            (Value::Date(date(10_000, 1, 2)), "10000-01-02"),
            (Value::Date(date(0, 12, 31)), "0001-12-31 (BC)"),
            // Integers, floats (`%f`), booleans, strings.
            (Value::Int(42), "42"),
            (Value::Int(-7), "-7"),
            (Value::Int(i64::MAX), "9223372036854775807"),
            (Value::Int(i64::MIN), "-9223372036854775808"),
            (Value::Float(-0.5), "-0.500000"),
            (Value::Float(1.0), "1.000000"),
            (Value::Float(-0.0), "-0.000000"),
            (Value::Float(1e20), "100000000000000000000.000000"),
            (Value::Float(1.5e-5), "0.000015"),
            (Value::Float(0.123_456_7), "0.123457"),
            (Value::Float(5e-7), "0.000000"),
            (Value::Float(2.5e-6), "0.000003"),
            (Value::Float(123_456_789.125), "123456789.125000"),
            (Value::Float(f64::INFINITY), "inf"),
            (Value::Float(f64::NEG_INFINITY), "-inf"),
            (Value::Float(f64::NAN), "nan"),
            (Value::Float(-f64::NAN), "-nan"),
            (Value::Bool(true), "True"),
            (Value::Bool(false), "False"),
            (string("abc"), "abc"),
            // Lists and structs: unquoted members, NULL members empty.
            (Value::List(vec![Value::Int(1), Value::Int(2)]), "[1,2]"),
            (Value::List(vec![string("a"), string("b")]), "[a,b]"),
            (Value::List(vec![string("a b"), string("c,d")]), "[a b,c,d]"),
            (Value::List(vec![Value::Int(1), Value::Null]), "[1,]"),
            (Value::List(Vec::new()), "[]"),
            (
                Value::List(vec![
                    Value::List(vec![Value::Int(1)]),
                    Value::List(vec![Value::Int(2), Value::Int(3)]),
                ]),
                "[[1],[2,3]]",
            ),
            (Value::List(vec![Value::Float(1.5)]), "[1.500000]"),
            (Value::List(vec![Value::Bool(true)]), "[True]"),
            (
                Value::List(vec![Value::Date(date(2024, 1, 2))]),
                "[2024-01-02]",
            ),
            (
                Value::List(vec![timestamp("2024-01-02 03:04:05.5")]),
                "[2024-01-02 03:04:05.5]",
            ),
            (map(&[("a", Value::Int(1))]), "{a: 1}"),
            (
                map(&[("a", Value::Int(1)), ("b", string("x"))]),
                "{a: 1, b: x}",
            ),
            (map(&[("a", map(&[("b", Value::Int(2))]))]), "{a: {b: 2}}"),
            (
                map(&[
                    ("a", Value::Float(1.5)),
                    ("b", Value::Bool(true)),
                    ("c", Value::List(vec![Value::Int(1), Value::Null])),
                    ("d", map(&[("e", Value::Null)])),
                ]),
                "{a: 1.500000, b: True, c: [1,], d: {e: }}",
            ),
            (Value::List(vec![map(&[("a", Value::Int(1))])]), "[{a: 1}]"),
            (
                map(&[("t", timestamp("2024-01-02 03:04:05"))]),
                "{t: 2024-01-02 03:04:05}",
            ),
            // Intervals.
            (interval(0, 1, 2 * 3_600_000_000), "1 day 02:00:00"),
            (interval(0, 0, 3 * 3_600_000_000), "03:00:00"),
            (interval(14, 3, 0), "1 year 2 months 3 days"),
            (interval(0, 0, 1_500_000), "00:00:01.5"),
            (interval(0, 0, 0), "00:00:00"),
            (interval(0, 2, 0), "2 days"),
            (interval(0, 0, 1), "00:00:00.000001"),
            (interval(0, 0, 1_000), "00:00:00.001"),
            (interval(1, 0, 0), "1 month"),
            (interval(13, 0, 0), "1 year 1 month"),
            (interval(12, 0, 0), "1 year"),
            (interval(24, 1, 180_000_000), "2 years 1 day 00:03:00"),
            (interval(0, 0, 100 * 3_600_000_000), "100:00:00"),
            (interval(0, 1, 11_045_000_000), "1 day 03:04:05"),
            (interval(0, -1, -11_045_000_000), "-1 days -03:04:05"),
            (interval(0, 0, -11_045_500_000), "-03:04:05.5"),
            (interval(0, -1, 0), "-1 days"),
            (interval(0, -2, 3_600_000_000), "-2 days 01:00:00"),
            (interval(-1, 0, 0), "-1 months"),
            (interval(-1, 0, -3_600_000_000), "-1 months -01:00:00"),
            (interval(-25, 0, 0), "-2 years -1 months"),
            (interval(0, -1, 86_399_000_000), "-1 days 23:59:59"),
            (interval(0, 1, 7_384_250_000), "1 day 02:03:04.25"),
            // UUIDs and blobs.
            (
                Value::Uuid("0F5C7B3A-0A43-4FA8-9C4B-6E9A2A0D1E7C".parse().unwrap()),
                "0f5c7b3a-0a43-4fa8-9c4b-6e9a2a0d1e7c",
            ),
            (Value::Binary(b"abc".to_vec()), "abc"),
            (
                Value::Binary(vec![
                    0x00, 0x1f, b' ', b'A', b'~', 0x7f, 0x80, b'\\', b'\'', b'"', 0xff,
                ]),
                r#"\x00\x1F A~\x7F\x80\x5C\x27\x22\xFF"#,
            ),
            (
                Value::Vector(crate::Vector::new(&[0.1, 0.25]).unwrap()),
                "[0.100000,0.250000]",
            ),
        ];
        for (value, expected) in cases {
            assert_eq!(
                apply(KuzuString, value.clone()),
                string(expected),
                "{value:?}"
            );
            // The non-allocating length pass is exact.
            assert_eq!(
                kuzu_text::rendered_len(value.as_ref()).unwrap(),
                expected.len(),
                "{value:?}"
            );
        }
    }

    #[test]
    fn charged_conversion_is_refused_before_rendering() {
        let large = Value::List(vec![Value::Float(1e308); 1_000]);
        let expected = kuzu_text::rendered_len(large.as_ref()).unwrap();
        assert!(expected > 300_000, "{expected}");
        let mut requested = Vec::new();
        let error = KuzuString
            .apply_ref_charged(large.as_ref(), &mut |bytes| {
                requested.push(bytes);
                Err(HawDBError::Execution("budget exhausted".into()))
            })
            .unwrap_err();
        assert_eq!(error, HawDBError::Execution("budget exhausted".into()));
        assert_eq!(requested, [expected]);
        // Successful conversions are charged their exact string length.
        for (function, value, text) in [
            (KuzuString, Value::Float(1.0), "1.000000"),
            (
                KuzuString,
                Value::List(vec![Value::Int(1), Value::Null]),
                "[1,]",
            ),
            (ToString, string("abc"), "abc"),
            (ToString, Value::Float(1.0), "1.0"),
        ] {
            let mut charged = 0;
            let converted = function
                .apply_ref_charged(value.as_ref(), &mut |bytes| {
                    charged += bytes;
                    Ok(())
                })
                .unwrap();
            assert_eq!(converted, string(text));
            assert_eq!(charged, text.len());
        }
        let mut charged = 0;
        assert_eq!(
            ToInteger
                .apply_ref_charged(string("7").as_ref(), &mut |bytes| {
                    charged += bytes;
                    Ok(())
                })
                .unwrap(),
            Value::Int(7)
        );
        assert_eq!(charged, 0);
    }

    #[test]
    fn unaccounted_conversion_refuses_oversized_text() {
        // Each zero byte renders as `\x00`: 17 MiB of input is 68 MiB of
        // text, above the 64 MiB unaccounted limit.
        let large = Value::Binary(vec![0; 17 * 1024 * 1024]);
        let error = KuzuString
            .apply_unaccounted(&large)
            .unwrap_err()
            .to_string();
        assert!(error.contains("without a query memory account"), "{error}");
        assert_eq!(
            KuzuString.apply_unaccounted(&Value::Float(1.0)).unwrap(),
            string("1.000000")
        );
    }

    #[test]
    fn kuzu_string_alias_null_and_unsupported_types() {
        assert_eq!(
            ConversionFunction::from_function_name("to_string"),
            Some(KuzuString)
        );
        assert_eq!(
            ConversionFunction::from_function_name("TO_STRING"),
            Some(KuzuString)
        );
        assert_eq!(
            ConversionFunction::from_function_name("String"),
            Some(KuzuString)
        );
        assert_eq!(apply(KuzuString, Value::Null), Value::Null);
        // toString keeps its Neo4j text.
        assert_eq!(apply(ToString, Value::Float(1.0)), string("1.0"));
        assert_eq!(apply(ToString, Value::Bool(true)), string("true"));
        let time = crate::temporal::NaiveTime::from_hms_opt(1, 2, 3).unwrap();
        for value in [
            Value::LocalTime(time),
            Value::Decimal("1.50".parse().unwrap()),
            Value::List(vec![Value::LocalTime(time)]),
        ] {
            let error = KuzuString.apply(&value).unwrap_err().to_string();
            assert!(
                error.contains("string() has no Kùzu-compatible text"),
                "{error}"
            );
        }
    }
}
