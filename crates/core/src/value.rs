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

use crate::LogicalType;

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Binary(Vec<u8>),
    Uuid(Uuid),
    List(Vec<Value>),
    Map(BTreeMap<String, Value>),
}

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
    Uuid(Uuid),
    List(&'a [Value]),
    Map(&'a BTreeMap<String, Value>),
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
                (Value::Uuid(left), Value::Uuid(right)) => left.cmp(right),
                (Value::List(left), Value::List(right)) => left.cmp(right),
                (Value::Map(left), Value::Map(right)) => left.cmp(right),
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
            Value::Uuid(value) => value.hash(state),
            Value::List(values) => values.hash(state),
            Value::Map(values) => values.hash(state),
        }
    }
}

impl Value {
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
            Value::Uuid(_) => 6,
            Value::List(_) => 7,
            Value::Map(_) => 8,
        }
    }
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
            Self::Uuid(_) => Some(LogicalType::Uuid),
            Self::List(_) => Some(LogicalType::List),
            Self::Map(_) => Some(LogicalType::Map),
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
            Self::Uuid(value) => Value::Uuid(value),
            Self::List(value) => Value::List(value.to_vec()),
            Self::Map(value) => Value::Map(value.clone()),
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
            Self::Uuid(_) => 6,
            Self::List(_) => 7,
            Self::Map(_) => 8,
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
            Value::Uuid(value) => Self::Uuid(*value),
            Value::List(value) => Self::List(value),
            Value::Map(value) => Self::Map(value),
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
                (Self::Uuid(left), Self::Uuid(right)) => left.cmp(right),
                (Self::List(left), Self::List(right)) => left.cmp(right),
                (Self::Map(left), Self::Map(right)) => left.cmp(right),
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
            Self::Uuid(value) => value.hash(state),
            Self::List(value) => value.hash(state),
            Self::Map(value) => value.hash(state),
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
            Self::Uuid(value) => write!(f, "{value}"),
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
            Value::Uuid(value) => write!(f, "{value}"),
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
            Value::List(vec![Value::Int(2)]),
            Value::Map(BTreeMap::from([("key".into(), Value::Int(3))])),
        ];

        for left in &values {
            for right in &values {
                assert_eq!(left.cmp(right), left.as_ref().cmp(&right.as_ref()));
                assert_eq!(left == right, left.as_ref() == *right);
            }
        }
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
