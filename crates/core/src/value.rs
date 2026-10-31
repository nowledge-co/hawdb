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

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use uuid::Uuid;

use crate::temporal::{self, DateTime, Duration, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use crate::{LogicalType, Vector, VectorRef};

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Binary(Vec<u8>),
    Vector(Vector),
    Uuid(Uuid),
    List(Vec<Value>),
    Map(BTreeMap<String, Value>),
    /// An openCypher `DATE`.
    Date(NaiveDate),
    /// An openCypher `LOCAL TIME` (time of day without an offset).
    LocalTime(NaiveTime),
    /// An openCypher `LOCAL DATETIME`.
    LocalDateTime(NaiveDateTime),
    /// An openCypher `ZONED DATETIME` with a fixed UTC offset. Values order
    /// by instant and then by offset; equal instants in different offsets
    /// are distinct values.
    DateTime(DateTime<FixedOffset>),
    /// An openCypher `DURATION`, boxed so that `Value` stays 32 bytes; every
    /// value in memory pays for the largest variant.
    Duration(Box<Duration>),
}

// Every row value pays for the largest variant; keep `Value` at 32 bytes on
// 64-bit hosts so new variants cannot silently grow all in-memory data.
#[cfg(target_pointer_width = "64")]
const _: () = assert!(std::mem::size_of::<Value>() == 32);

/// An allocation-free borrowed view of a [`Value`].
///
/// Scalar values are copied, while variable-width payloads retain references
/// to their owner. Use [`ValueRef::to_owned_value`] only at an ownership
/// boundary such as final result materialization.
#[derive(Debug, Clone, Copy)]
pub enum ValueRef<'a> {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(&'a str),
    Binary(&'a [u8]),
    Vector(VectorRef<'a>),
    Uuid(Uuid),
    List(&'a [Value]),
    Map(&'a BTreeMap<String, Value>),
    Date(NaiveDate),
    LocalTime(NaiveTime),
    LocalDateTime(NaiveDateTime),
    DateTime(DateTime<FixedOffset>),
    Duration(&'a Duration),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        self.kind_rank()
            .cmp(&other.kind_rank())
            .then_with(|| match (self, other) {
                (Value::Null, Value::Null) => Ordering::Equal,
                (Value::Bool(left), Value::Bool(right)) => left.cmp(right),
                (Value::Int(left), Value::Int(right)) => left.cmp(right),
                (Value::Float(left), Value::Float(right)) => left.total_cmp(right),
                (Value::String(left), Value::String(right)) => left.cmp(right),
                (Value::Binary(left), Value::Binary(right)) => left.cmp(right),
                (Value::Vector(left), Value::Vector(right)) => left.cmp(right),
                (Value::Uuid(left), Value::Uuid(right)) => left.cmp(right),
                (Value::List(left), Value::List(right)) => left.cmp(right),
                (Value::Map(left), Value::Map(right)) => left.cmp(right),
                (Value::Date(left), Value::Date(right)) => left.cmp(right),
                (Value::LocalTime(left), Value::LocalTime(right)) => left.cmp(right),
                (Value::LocalDateTime(left), Value::LocalDateTime(right)) => left.cmp(right),
                (Value::DateTime(left), Value::DateTime(right)) => {
                    temporal::datetime_cmp(left, right)
                }
                (Value::Duration(left), Value::Duration(right)) => left.cmp(right),
                _ => Ordering::Equal,
            })
    }
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind_rank().hash(state);
        match self {
            Value::Null => {}
            Value::Bool(value) => value.hash(state),
            Value::Int(value) => value.hash(state),
            Value::Float(value) => value.to_bits().hash(state),
            Value::String(value) => value.hash(state),
            Value::Binary(value) => value.hash(state),
            Value::Vector(value) => value.hash(state),
            Value::Uuid(value) => value.hash(state),
            Value::List(values) => values.hash(state),
            Value::Map(values) => values.hash(state),
            Value::Date(value) => value.hash(state),
            Value::LocalTime(value) => value.hash(state),
            Value::LocalDateTime(value) => value.hash(state),
            Value::DateTime(value) => hash_datetime(value, state),
            Value::Duration(value) => value.hash(state),
        }
    }
}

impl Value {
    /// Builds a [`Value::Duration`], boxing the payload so `Value` stays
    /// 32 bytes.
    pub fn duration(value: Duration) -> Self {
        Value::Duration(Box::new(value))
    }

    pub fn as_ref(&self) -> ValueRef<'_> {
        self.into()
    }

    pub fn logical_type(&self) -> Option<LogicalType> {
        self.as_ref().logical_type()
    }

    fn kind_rank(&self) -> u8 {
        match self {
            Value::Null => 0,
            Value::Bool(_) => 1,
            Value::Int(_) => 2,
            Value::Float(_) => 3,
            Value::String(_) => 4,
            Value::Binary(_) => 5,
            Value::Vector(_) => 9,
            Value::Uuid(_) => 6,
            Value::List(_) => 7,
            Value::Map(_) => 8,
            Value::Date(_) => 10,
            Value::LocalTime(_) => 11,
            Value::LocalDateTime(_) => 12,
            Value::DateTime(_) => 13,
            Value::Duration(_) => 14,
        }
    }
}

/// Hashes the components [`temporal::datetime_cmp`] compares, so that
/// `Hash` agrees with `Eq` for zoned datetimes.
fn hash_datetime<H: Hasher>(value: &DateTime<FixedOffset>, state: &mut H) {
    value.naive_utc().hash(state);
    value.offset().local_minus_utc().hash(state);
}

impl<'a> ValueRef<'a> {
    pub const fn is_null(self) -> bool {
        matches!(self, Self::Null)
    }

    pub const fn logical_type(self) -> Option<LogicalType> {
        match self {
            Self::Null => None,
            Self::Bool(_) => Some(LogicalType::Boolean),
            Self::Int(_) => Some(LogicalType::Int64),
            Self::Float(_) => Some(LogicalType::Float64),
            Self::String(_) => Some(LogicalType::String),
            Self::Binary(_) => Some(LogicalType::Binary),
            Self::Vector(value) => Some(LogicalType::Vector(value.dimension())),
            Self::Uuid(_) => Some(LogicalType::Uuid),
            Self::List(_) => Some(LogicalType::List),
            Self::Map(_) => Some(LogicalType::Map),
            Self::Date(_) => Some(LogicalType::Date),
            Self::LocalTime(_) => Some(LogicalType::LocalTime),
            Self::LocalDateTime(_) => Some(LogicalType::LocalDateTime),
            Self::DateTime(_) => Some(LogicalType::DateTime),
            Self::Duration(_) => Some(LogicalType::Duration),
        }
    }

    pub const fn as_bool(self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(value),
            _ => None,
        }
    }

    pub const fn as_i64(self) -> Option<i64> {
        match self {
            Self::Int(value) => Some(value),
            _ => None,
        }
    }

    pub const fn as_f64(self) -> Option<f64> {
        match self {
            Self::Float(value) => Some(value),
            _ => None,
        }
    }

    pub const fn as_str(self) -> Option<&'a str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    pub const fn as_binary(self) -> Option<&'a [u8]> {
        match self {
            Self::Binary(value) => Some(value),
            _ => None,
        }
    }

    pub const fn as_uuid(self) -> Option<Uuid> {
        match self {
            Self::Uuid(value) => Some(value),
            _ => None,
        }
    }

    pub const fn as_list(self) -> Option<&'a [Value]> {
        match self {
            Self::List(value) => Some(value),
            _ => None,
        }
    }

    pub const fn as_map(self) -> Option<&'a BTreeMap<String, Value>> {
        match self {
            Self::Map(value) => Some(value),
            _ => None,
        }
    }

    pub fn to_owned_value(self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Bool(value) => Value::Bool(value),
            Self::Int(value) => Value::Int(value),
            Self::Float(value) => Value::Float(value),
            Self::String(value) => Value::String(value.to_owned()),
            Self::Binary(value) => Value::Binary(value.to_vec()),
            Self::Vector(value) => Value::Vector(value.to_owned()),
            Self::Uuid(value) => Value::Uuid(value),
            Self::List(value) => Value::List(value.to_vec()),
            Self::Map(value) => Value::Map(value.clone()),
            Self::Date(value) => Value::Date(value),
            Self::LocalTime(value) => Value::LocalTime(value),
            Self::LocalDateTime(value) => Value::LocalDateTime(value),
            Self::DateTime(value) => Value::DateTime(value),
            Self::Duration(value) => Value::duration(*value),
        }
    }

    const fn kind_rank(self) -> u8 {
        match self {
            Self::Null => 0,
            Self::Bool(_) => 1,
            Self::Int(_) => 2,
            Self::Float(_) => 3,
            Self::String(_) => 4,
            Self::Binary(_) => 5,
            Self::Vector(_) => 9,
            Self::Uuid(_) => 6,
            Self::List(_) => 7,
            Self::Map(_) => 8,
            Self::Date(_) => 10,
            Self::LocalTime(_) => 11,
            Self::LocalDateTime(_) => 12,
            Self::DateTime(_) => 13,
            Self::Duration(_) => 14,
        }
    }
}

impl<'a> From<&'a Value> for ValueRef<'a> {
    fn from(value: &'a Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Bool(value) => Self::Bool(*value),
            Value::Int(value) => Self::Int(*value),
            Value::Float(value) => Self::Float(*value),
            Value::String(value) => Self::String(value),
            Value::Binary(value) => Self::Binary(value),
            Value::Vector(value) => Self::Vector(value.as_ref()),
            Value::Uuid(value) => Self::Uuid(*value),
            Value::List(value) => Self::List(value),
            Value::Map(value) => Self::Map(value),
            Value::Date(value) => Self::Date(*value),
            Value::LocalTime(value) => Self::LocalTime(*value),
            Value::LocalDateTime(value) => Self::LocalDateTime(*value),
            Value::DateTime(value) => Self::DateTime(*value),
            Value::Duration(value) => Self::Duration(value),
        }
    }
}

impl PartialEq for ValueRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for ValueRef<'_> {}

impl PartialOrd for ValueRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ValueRef<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.kind_rank()
            .cmp(&other.kind_rank())
            .then_with(|| match (self, other) {
                (Self::Null, Self::Null) => Ordering::Equal,
                (Self::Bool(left), Self::Bool(right)) => left.cmp(right),
                (Self::Int(left), Self::Int(right)) => left.cmp(right),
                (Self::Float(left), Self::Float(right)) => left.total_cmp(right),
                (Self::String(left), Self::String(right)) => left.cmp(right),
                (Self::Binary(left), Self::Binary(right)) => left.cmp(right),
                (Self::Vector(left), Self::Vector(right)) => left.cmp(right),
                (Self::Uuid(left), Self::Uuid(right)) => left.cmp(right),
                (Self::List(left), Self::List(right)) => left.cmp(right),
                (Self::Map(left), Self::Map(right)) => left.cmp(right),
                (Self::Date(left), Self::Date(right)) => left.cmp(right),
                (Self::LocalTime(left), Self::LocalTime(right)) => left.cmp(right),
                (Self::LocalDateTime(left), Self::LocalDateTime(right)) => left.cmp(right),
                (Self::DateTime(left), Self::DateTime(right)) => {
                    temporal::datetime_cmp(left, right)
                }
                (Self::Duration(left), Self::Duration(right)) => left.cmp(right),
                _ => Ordering::Equal,
            })
    }
}

impl Hash for ValueRef<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind_rank().hash(state);
        match self {
            Self::Null => {}
            Self::Bool(value) => value.hash(state),
            Self::Int(value) => value.hash(state),
            Self::Float(value) => value.to_bits().hash(state),
            Self::String(value) => value.hash(state),
            Self::Binary(value) => value.hash(state),
            Self::Vector(value) => value.hash(state),
            Self::Uuid(value) => value.hash(state),
            Self::List(value) => value.hash(state),
            Self::Map(value) => value.hash(state),
            Self::Date(value) => value.hash(state),
            Self::LocalTime(value) => value.hash(state),
            Self::LocalDateTime(value) => value.hash(state),
            Self::DateTime(value) => hash_datetime(value, state),
            Self::Duration(value) => value.hash(state),
        }
    }
}

impl PartialEq<Value> for ValueRef<'_> {
    fn eq(&self, other: &Value) -> bool {
        *self == other.as_ref()
    }
}

impl PartialEq<&Value> for ValueRef<'_> {
    fn eq(&self, other: &&Value) -> bool {
        *self == other.as_ref()
    }
}

impl PartialEq<ValueRef<'_>> for Value {
    fn eq(&self, other: &ValueRef<'_>) -> bool {
        self.as_ref() == *other
    }
}

impl PartialEq<ValueRef<'_>> for &Value {
    fn eq(&self, other: &ValueRef<'_>) -> bool {
        self.as_ref() == *other
    }
}

impl Display for ValueRef<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "null"),
            Self::Bool(value) => write!(f, "{value}"),
            Self::Int(value) => write!(f, "{value}"),
            Self::Float(value) => write!(f, "{value}"),
            Self::String(value) => write!(f, "{value}"),
            Self::Binary(value) => write_binary(f, value),
            Self::Vector(value) => {
                f.write_str("VECTOR")?;
                f.debug_list().entries(value.coordinates()).finish()
            }
            Self::Uuid(value) => write!(f, "{value}"),
            Self::Date(value) => f.write_str(&temporal::format_date(*value)),
            Self::LocalTime(value) => f.write_str(&temporal::format_local_time(*value)),
            Self::LocalDateTime(value) => f.write_str(&temporal::format_local_datetime(*value)),
            Self::DateTime(value) => f.write_str(&temporal::format_datetime(*value)),
            Self::Duration(value) => write!(f, "{value}"),
            Self::List(values) => {
                let values = values
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "[{values}]")
            }
            Self::Map(values) => {
                let values = values
                    .iter()
                    .map(|(key, value)| format!("{key}: {value}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{{{values}}}")
            }
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Bool(value) => write!(f, "{value}"),
            Value::Int(value) => write!(f, "{value}"),
            Value::Float(value) => write!(f, "{value}"),
            Value::String(value) => write!(f, "{value}"),
            Value::Binary(value) => write_binary(f, value),
            Value::Vector(value) => ValueRef::Vector(value.as_ref()).fmt(f),
            Value::Uuid(value) => write!(f, "{value}"),
            Value::Date(_)
            | Value::LocalTime(_)
            | Value::LocalDateTime(_)
            | Value::DateTime(_)
            | Value::Duration(_) => self.as_ref().fmt(f),
            Value::List(values) => {
                let values = values
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "[{values}]")
            }
            Value::Map(values) => {
                let values = values
                    .iter()
                    .map(|(key, value)| format!("{key}: {value}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{{{values}}}")
            }
        }
    }
}

fn write_binary(f: &mut Formatter<'_>, value: &[u8]) -> std::fmt::Result {
    f.write_str("\\x")?;
    for byte in value {
        write!(f, "{byte:02x}")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_ref_borrows_variable_width_payloads() {
        let value = Value::String("borrowed payload".to_string());
        let reference = value.as_ref();

        assert_eq!(reference.as_str(), Some("borrowed payload"));
        let Value::String(owned) = &value else {
            unreachable!();
        };
        assert_eq!(reference.as_str().unwrap().as_ptr(), owned.as_ptr());
        assert_eq!(reference.to_owned_value(), value);
    }

    #[test]
    fn value_ref_preserves_owned_value_order_and_hash_semantics() {
        let values = [
            Value::Null,
            Value::Bool(true),
            Value::Int(1),
            Value::Float(f64::NAN),
            Value::String("value".into()),
            Value::Binary(vec![0, 0xff]),
            Value::Vector(Vector::new(&[0.0, -0.0]).unwrap()),
            Value::Vector(Vector::new(&[-0.0, 0.0]).unwrap()),
            Value::Vector(Vector::new(&[1.0]).unwrap()),
            Value::List(vec![Value::Int(2)]),
            Value::Map(BTreeMap::from([("key".into(), Value::Int(3))])),
            Value::Date(temporal::parse_date("2026-10-30").unwrap()),
            Value::LocalTime(temporal::parse_local_time("12:00").unwrap()),
            Value::LocalDateTime(temporal::parse_local_datetime("2026-10-30T12:00").unwrap()),
            Value::DateTime(temporal::parse_datetime("2026-10-30T12:00+02:00").unwrap()),
            Value::DateTime(temporal::parse_datetime("2026-10-30T10:00Z").unwrap()),
            Value::duration(temporal::parse_duration("P1M2DT3S").unwrap()),
        ];

        for left in &values {
            use std::hash::BuildHasher;
            let state = std::collections::hash_map::RandomState::new();
            assert_eq!(state.hash_one(left), state.hash_one(left.as_ref()));
            for right in &values {
                assert_eq!(left.cmp(right), left.as_ref().cmp(&right.as_ref()));
                assert_eq!(left == right, left.as_ref() == *right);
            }
        }
    }

    #[test]
    fn temporal_values_rank_after_existing_kinds_and_order_chronologically() {
        use std::hash::BuildHasher;
        let date = |text| Value::Date(temporal::parse_date(text).unwrap());
        let datetime = |text| Value::DateTime(temporal::parse_datetime(text).unwrap());
        let duration = |text| Value::duration(temporal::parse_duration(text).unwrap());
        let ordered = [
            Value::Null,
            Value::Map(BTreeMap::new()),
            Value::Vector(Vector::new(&[1.0]).unwrap()),
            date("-0001-12-31"),
            date("2026-10-30"),
            Value::LocalTime(temporal::parse_local_time("00:00").unwrap()),
            Value::LocalTime(temporal::parse_local_time("23:59:59.999999999").unwrap()),
            Value::LocalDateTime(temporal::parse_local_datetime("2026-10-30T00:00").unwrap()),
            // By instant first; the same instant orders west-to-east.
            datetime("2026-10-30T09:00:00+00:00"),
            datetime("2026-10-30T05:00:00-05:00"),
            datetime("2026-10-30T10:00:00Z"),
            datetime("2026-10-30T12:00:00+02:00"),
            datetime("2026-10-30T10:00:00.000000001Z"),
            duration("-P1M"),
            // Durations order by (months, days, time): a day sorts after 25 hours.
            duration("PT25H"),
            duration("P1D"),
            duration("P1M"),
        ];
        for pair in ordered.windows(2) {
            assert!(pair[0] < pair[1], "{} < {}", pair[0], pair[1]);
        }
        let state = std::collections::hash_map::RandomState::new();
        let east = datetime("2026-10-30T12:00:00+02:00");
        let same = datetime("2026-10-30T12:00:00.000+02:00");
        assert_eq!(east, same);
        assert_eq!(state.hash_one(&east), state.hash_one(&same));
        assert_ne!(east, datetime("2026-10-30T10:00:00Z"));
        assert_eq!(east.logical_type(), Some(LogicalType::DateTime));
        assert_eq!(east.to_string(), "2026-10-30T12:00:00+02:00");
        assert_eq!(duration("P1DT2H").to_string(), "P1DT2H");
        assert_eq!(LogicalType::LocalDateTime.to_string(), "LOCAL DATETIME");
        assert!(LogicalType::Date.accepts(date("2026-01-01").as_ref()));
        assert!(!LogicalType::Date.accepts(east.as_ref()));
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn value_layout_stays_compact() {
        assert_eq!(std::mem::size_of::<Value>(), 32);
        assert!(std::mem::size_of::<ValueRef<'static>>() <= 32);
    }

    #[test]
    fn boxed_duration_round_trips_through_value_ref() {
        let duration = temporal::parse_duration("-P1Y2M3DT4.5S").unwrap();
        let value = Value::duration(duration);
        let ValueRef::Duration(borrowed) = value.as_ref() else {
            unreachable!();
        };
        assert_eq!(*borrowed, duration);
        assert_eq!(value.as_ref().to_owned_value(), value);
        assert_eq!(value.logical_type(), Some(LogicalType::Duration));
    }

    #[test]
    fn null_has_no_intrinsic_logical_type() {
        assert_eq!(Value::Null.logical_type(), None);
        assert_eq!(Value::Int(42).logical_type(), Some(LogicalType::Int64));
        assert_eq!(
            Value::Binary(vec![0, 0xff]).logical_type(),
            Some(LogicalType::Binary)
        );
    }

    #[test]
    fn binary_value_ref_borrows_and_formats_postgres_hex() {
        let value = Value::Binary(vec![0, 1, 0xfe, 0xff]);
        let reference = value.as_ref();

        assert_eq!(reference.as_binary(), Some(&[0, 1, 0xfe, 0xff][..]));
        let Value::Binary(owned) = &value else {
            unreachable!();
        };
        assert_eq!(reference.as_binary().unwrap().as_ptr(), owned.as_ptr());
        assert_eq!(reference.to_owned_value(), value);
        assert_eq!(value.to_string(), "\\x0001feff");
    }
}
