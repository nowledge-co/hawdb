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

use chrono::DateTime;
use hawdb_core::Value;
use roaring::RoaringTreemap;
use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NumericMinMax {
    pub min: f64,
    pub max: f64,
}

impl NumericMinMax {
    pub fn new(min: f64, max: f64) -> Option<Self> {
        (min.is_finite() && max.is_finite() && min <= max).then_some(Self { min, max })
    }

    pub fn contains(&self, value: f64) -> bool {
        value.is_finite() && self.min <= value && value <= self.max
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DateTimeMinMax {
    pub min_epoch_millis: i64,
    pub max_epoch_millis: i64,
}

impl DateTimeMinMax {
    pub fn new(min_epoch_millis: i64, max_epoch_millis: i64) -> Option<Self> {
        (min_epoch_millis <= max_epoch_millis).then_some(Self {
            min_epoch_millis,
            max_epoch_millis,
        })
    }

    pub fn parse_rfc3339(value: &str) -> Option<i64> {
        DateTime::parse_from_rfc3339(value)
            .ok()
            .map(|value| value.timestamp_millis())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ScanScalar {
    Bool(bool),
    Int(i64),
    Float(u64),
    String(String),
    Binary(Vec<u8>),
    Uuid(hawdb_core::Uuid),
}

impl ScanScalar {
    pub fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Bool(value) => Some(Self::Bool(*value)),
            Value::Int(value) => Some(Self::Int(*value)),
            Value::Float(value) if value.is_finite() => Some(Self::Float(value.to_bits())),
            Value::String(value) => Some(Self::String(value.clone())),
            Value::Binary(value) => Some(Self::Binary(value.clone())),
            Value::Uuid(value) => Some(Self::Uuid(*value)),
            Value::Null | Value::Float(_) | Value::List(_) | Value::Map(_) => None,
        }
    }

    fn stable_bytes(&self) -> Vec<u8> {
        match self {
            Self::Bool(value) => vec![0, u8::from(*value)],
            Self::Int(value) => {
                let mut bytes = vec![1];
                bytes.extend(value.to_le_bytes());
                bytes
            }
            Self::Float(value) => {
                let mut bytes = vec![2];
                bytes.extend(value.to_le_bytes());
                bytes
            }
            Self::String(value) => {
                let mut bytes = vec![3];
                bytes.extend(value.as_bytes());
                bytes
            }
            Self::Binary(value) => {
                let mut bytes = vec![4];
                bytes.extend(value);
                bytes
            }
            Self::Uuid(value) => {
                let mut bytes = vec![5];
                bytes.extend(value.as_bytes());
                bytes
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnumDictionaryStats {
    pub values: BTreeSet<ScanScalar>,
    pub complete: bool,
}

impl EnumDictionaryStats {
    pub fn complete(values: impl IntoIterator<Item = Value>) -> Self {
        Self {
            values: values
                .into_iter()
                .filter_map(|value| ScanScalar::from_value(&value))
                .collect(),
            complete: true,
        }
    }

    pub fn may_contain(&self, value: &Value) -> bool {
        !self.complete
            || ScanScalar::from_value(value).is_some_and(|value| self.values.contains(&value))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MembershipVerdict {
    DefinitelyNot,
    Maybe,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembershipFilterSummary {
    Bloom { bits: Vec<u64>, hash_count: u8 },
    Cuckoo { fingerprints: BTreeSet<u16> },
}

impl MembershipFilterSummary {
    pub fn bloom(
        values: impl IntoIterator<Item = Value>,
        bit_count: usize,
        hash_count: u8,
    ) -> Self {
        let bit_count = bit_count.max(64).next_multiple_of(64);
        let hash_count = hash_count.clamp(1, 8);
        let mut bits = vec![0_u64; bit_count / 64];
        for value in values {
            let Some(value) = ScanScalar::from_value(&value) else {
                continue;
            };
            for bit in bloom_bits(&value, bit_count, hash_count) {
                bits[bit / 64] |= 1_u64 << (bit % 64);
            }
        }
        Self::Bloom { bits, hash_count }
    }

    pub fn cuckoo(values: impl IntoIterator<Item = Value>) -> Self {
        Self::Cuckoo {
            fingerprints: values
                .into_iter()
                .filter_map(|value| ScanScalar::from_value(&value))
                .map(|value| stable_hash(&value, 0x9e37_79b9_7f4a_7c15) as u16)
                .collect(),
        }
    }

    pub fn contains(&self, value: &Value) -> MembershipVerdict {
        let Some(value) = ScanScalar::from_value(value) else {
            return MembershipVerdict::Maybe;
        };
        match self {
            Self::Bloom { bits, hash_count } => {
                let bit_count = bits.len().saturating_mul(64);
                if bit_count == 0
                    || bloom_bits(&value, bit_count, *hash_count)
                        .any(|bit| bits[bit / 64] & (1_u64 << (bit % 64)) == 0)
                {
                    MembershipVerdict::DefinitelyNot
                } else {
                    MembershipVerdict::Maybe
                }
            }
            Self::Cuckoo { fingerprints } => {
                let fingerprint = stable_hash(&value, 0x9e37_79b9_7f4a_7c15) as u16;
                if fingerprints.contains(&fingerprint) {
                    MembershipVerdict::Maybe
                } else {
                    MembershipVerdict::DefinitelyNot
                }
            }
        }
    }
}

fn bloom_bits(value: &ScanScalar, bit_count: usize, hash_count: u8) -> impl Iterator<Item = usize> {
    let first = stable_hash(value, 0xcbf2_9ce4_8422_2325);
    let second = stable_hash(value, 0x517c_c1b7_2722_0a95) | 1;
    (0..hash_count).map(move |index| {
        first
            .wrapping_add(u64::from(index).wrapping_mul(second))
            .wrapping_rem(bit_count as u64) as usize
    })
}

fn stable_hash(value: &ScanScalar, seed: u64) -> u64 {
    struct Fnv64(u64);

    impl Hasher for Fnv64 {
        fn finish(&self) -> u64 {
            self.0
        }

        fn write(&mut self, bytes: &[u8]) {
            for byte in bytes {
                self.0 ^= u64::from(*byte);
                self.0 = self.0.wrapping_mul(0x100_0000_01b3);
            }
        }
    }

    let mut hasher = Fnv64(seed);
    value.stable_bytes().hash(&mut hasher);
    hasher.finish()
}

#[derive(Debug, Clone, PartialEq)]
pub struct FieldSummary {
    pub row_count: u64,
    pub present_count: u64,
    pub null_count: u64,
    pub missing_count: u64,
    pub numeric_min_max: Option<NumericMinMax>,
    pub datetime_min_max: Option<DateTimeMinMax>,
    pub enum_dictionary: Option<EnumDictionaryStats>,
    pub membership: Option<MembershipFilterSummary>,
    pub exact_values: BTreeMap<ScanScalar, RoaringTreemap>,
}

impl FieldSummary {
    pub fn new(row_count: u64) -> Self {
        Self {
            row_count,
            present_count: row_count,
            null_count: 0,
            missing_count: 0,
            numeric_min_max: None,
            datetime_min_max: None,
            enum_dictionary: None,
            membership: None,
            exact_values: BTreeMap::new(),
        }
    }

    pub fn with_presence_counts(
        mut self,
        present_count: u64,
        null_count: u64,
        missing_count: u64,
    ) -> Option<Self> {
        (present_count <= self.row_count
            && null_count <= present_count
            && present_count.saturating_add(missing_count) == self.row_count)
            .then(|| {
                self.present_count = present_count;
                self.null_count = null_count;
                self.missing_count = missing_count;
                self
            })
    }

    pub fn with_numeric_min_max(mut self, min: f64, max: f64) -> Option<Self> {
        self.numeric_min_max = NumericMinMax::new(min, max);
        self.numeric_min_max.map(|_| self)
    }

    pub fn with_datetime_min_max(mut self, min: i64, max: i64) -> Option<Self> {
        self.datetime_min_max = DateTimeMinMax::new(min, max);
        self.datetime_min_max.map(|_| self)
    }

    pub fn with_enum_dictionary(mut self, dictionary: EnumDictionaryStats) -> Self {
        self.enum_dictionary = Some(dictionary);
        self
    }

    pub fn with_bloom_values(
        mut self,
        values: impl IntoIterator<Item = Value>,
        bit_count: usize,
        hash_count: u8,
    ) -> Self {
        self.membership = Some(MembershipFilterSummary::bloom(
            values, bit_count, hash_count,
        ));
        self
    }

    pub fn with_cuckoo_values(mut self, values: impl IntoIterator<Item = Value>) -> Self {
        self.membership = Some(MembershipFilterSummary::cuckoo(values));
        self
    }

    pub fn with_exact_value(mut self, value: Value, row_ids: RoaringTreemap) -> Self {
        if let Some(value) = ScanScalar::from_value(&value) {
            self.exact_values.insert(value, row_ids);
        }
        self
    }

    /// Records exact row positions without exposing the bitmap implementation
    /// to projection builders outside the storage crate.
    pub fn with_exact_row_ids(self, value: Value, row_ids: impl IntoIterator<Item = u64>) -> Self {
        let row_ids = row_ids.into_iter().collect();
        self.with_exact_value(value, row_ids)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SegmentSummary {
    pub segment_id: u64,
    pub row_count: u64,
    pub fields: BTreeMap<String, FieldSummary>,
}

impl SegmentSummary {
    pub fn new(segment_id: u64, row_count: u64) -> Self {
        Self {
            segment_id,
            row_count,
            fields: BTreeMap::new(),
        }
    }

    pub fn insert_field(&mut self, property: impl Into<String>, summary: FieldSummary) {
        debug_assert_eq!(summary.row_count, self.row_count);
        self.fields.insert(property.into(), summary);
    }

    pub fn field(&self, property: &str) -> Option<&FieldSummary> {
        self.fields.get(property)
    }
}
