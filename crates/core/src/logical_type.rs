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

use crate::ValueRef;
use std::fmt::{Display, Formatter};
use std::num::NonZeroU32;

/// A portable, nonzero dimension for a fixed-width Float32 vector column.
/// Declaring a dimension does not allocate a vector or admit its payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VectorDimension(NonZeroU32);

impl VectorDimension {
    pub const fn new(dimension: u32) -> Option<Self> {
        match NonZeroU32::new(dimension) {
            Some(dimension) => Some(Self(dimension)),
            None => None,
        }
    }

    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// Query-visible value types shared by schemas, planning, and execution.
///
/// Nullability is deliberately kept outside this enum. `NULL` has no runtime
/// type of its own and is accepted by every nullable slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LogicalType {
    Any,
    Boolean,
    Int64,
    Float64,
    /// A bounded application string that is eligible for value statistics.
    String,
    /// An unbounded UTF-8 payload that is excluded from value statistics.
    Text,
    Binary,
    Uuid,
    Vector(VectorDimension),
    List,
    Map,
    Date,
    LocalTime,
    LocalDateTime,
    DateTime,
    Duration,
}

impl LogicalType {
    pub const fn is_numeric(self) -> bool {
        matches!(self, Self::Int64 | Self::Float64)
    }

    pub const fn is_utf8(self) -> bool {
        matches!(self, Self::String | Self::Text)
    }

    /// Returns whether a runtime value can inhabit this logical type.
    ///
    /// `NULL` is accepted here because nullability is a slot property. Callers
    /// that enforce a non-null constraint must reject it before this check.
    pub fn accepts(self, value: ValueRef<'_>) -> bool {
        if value.is_null() || self == Self::Any {
            return true;
        }
        match (self, value.logical_type()) {
            (Self::Text, Some(Self::String)) => true,
            (_, Some(actual)) => self == actual,
            (_, None) => true,
        }
    }
}

impl Display for LogicalType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Any => "ANY",
            Self::Boolean => "BOOLEAN",
            Self::Int64 => "BIGINT",
            Self::Float64 => "DOUBLE PRECISION",
            Self::String => "STRING",
            Self::Text => "TEXT",
            Self::Binary => "BYTEA",
            Self::Uuid => "UUID",
            Self::Vector(dimension) => return write!(f, "VECTOR({})", dimension.get()),
            Self::List => "LIST",
            Self::Map => "MAP",
            Self::Date => "DATE",
            Self::LocalTime => "LOCAL TIME",
            Self::LocalDateTime => "LOCAL DATETIME",
            Self::DateTime => "ZONED DATETIME",
            Self::Duration => "DURATION",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;

    #[test]
    fn vector_schema_dimensions_and_nullability_are_explicit() {
        assert_eq!(VectorDimension::new(0), None);
        for size in [1, 1024, u32::MAX] {
            let dimension = VectorDimension::new(size).unwrap();
            let kind = LogicalType::Vector(dimension);
            assert_eq!(dimension.get(), size);
            assert_eq!(kind.to_string(), format!("VECTOR({size})"));
            assert!(kind.accepts(ValueRef::Null));
            assert!(!kind.accepts(Value::List(vec![Value::Float(1.0)]).as_ref()));
            assert!(!kind.accepts(ValueRef::Binary(&[0; 4])));
        }
        assert_ne!(VectorDimension::new(3), VectorDimension::new(4));
    }

    #[test]
    fn logical_type_compatibility_keeps_nullability_external() {
        assert!(LogicalType::Int64.accepts(ValueRef::Null));
        assert!(LogicalType::Any.accepts(ValueRef::String("dynamic")));
        assert!(LogicalType::Text.accepts(ValueRef::String("payload")));
        assert!(!LogicalType::String.accepts(ValueRef::Int(7)));
        assert!(!LogicalType::Binary.accepts(Value::String("bytes".into()).as_ref()));
    }
}
