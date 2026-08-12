//! Per-chunk zone maps (§3.2.2, §3.5.3(b)).
//!
//! Reuses the `FieldSummary` model from `scan::summary` for presence counts
//! and numeric/datetime min-max. `FieldSummary` has no string bounds, so a
//! chunk zone map pairs the summary with a `StringPrefixMinMax`: 16-byte
//! min/max prefixes with truncation markers, the one statistic the reused
//! model cannot carry. Zone maps serialize as fixed-size records inside the
//! group directory so scan planning reads directories, never chunks.
//!
//! Pruning soundness contract (mirror of `SkeinPropertyIndexPruning`): every
//! `may_match_*` method returns `false` only when no row of the chunk can
//! satisfy the predicate under the value-match semantics documented on
//! `ColumnPredicate` in `group.rs`.

use super::encoding::Cursor;
use super::{corrupt, ColumnGroupError};
use crate::scan::{DateTimeMinMax, FieldSummary, NumericMinMax, RangeBound};
use skein_core::Value;

/// Serialized size of one zone-map record in the group directory.
pub const ZONE_MAP_RECORD_BYTES: usize = 88;
/// Stored string min/max prefixes are capped at this many bytes.
pub const STRING_PREFIX_BYTES: usize = 16;

const PRESENT_NUMERIC: u8 = 1;
const PRESENT_DATETIME: u8 = 1 << 1;
const PRESENT_STRING: u8 = 1 << 2;

const HAS_BOOL: u8 = 1;
const HAS_INT: u8 = 1 << 1;
const HAS_FLOAT: u8 = 1 << 2;
const HAS_STRING: u8 = 1 << 3;
const HAS_NESTED: u8 = 1 << 4;
const HAS_NONFINITE: u8 = 1 << 5;

const MIN_TRUNCATED: u8 = 1;
const MAX_TRUNCATED: u8 = 1 << 1;

/// Byte-wise min/max prefixes over the string values of one chunk.
///
/// A truncated prefix is a lower bound of the string it was cut from, so
/// every comparison here is phrased to stay sound under truncation: the
/// stored min is `<=` the true minimum, and when `max_truncated` holds the
/// true maximum extends the stored prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StringPrefixMinMax {
    min: Vec<u8>,
    max: Vec<u8>,
    min_truncated: bool,
    max_truncated: bool,
}

impl StringPrefixMinMax {
    fn from_bounds(min: &str, max: &str) -> Self {
        let (min, min_truncated) = truncate_prefix(min.as_bytes());
        let (max, max_truncated) = truncate_prefix(max.as_bytes());
        Self {
            min,
            max,
            min_truncated,
            max_truncated,
        }
    }

    /// True only when the true maximum is provably `< bound`.
    fn max_definitely_less_than(&self, bound: &[u8]) -> bool {
        if self.max_truncated {
            // The true max extends the stored prefix; only a bound whose own
            // prefix is strictly greater dominates every such extension.
            truncate_slice(bound) > self.max.as_slice()
        } else {
            bound > self.max.as_slice()
        }
    }

    /// True only when the true maximum is provably `<= bound`.
    fn max_definitely_less_equal(&self, bound: &[u8]) -> bool {
        if self.max_truncated {
            truncate_slice(bound) > self.max.as_slice()
        } else {
            bound >= self.max.as_slice()
        }
    }

    /// True only when the true minimum is provably `> bound`.
    fn min_definitely_greater_than(&self, bound: &[u8]) -> bool {
        // The stored min never exceeds the true min, and a truncated min is
        // a proper prefix of it, hence strictly below it.
        self.min.as_slice() > bound || (self.min_truncated && self.min.as_slice() == bound)
    }

    /// True only when the true minimum is provably `>= bound`.
    fn min_definitely_greater_equal(&self, bound: &[u8]) -> bool {
        self.min.as_slice() >= bound
    }
}

fn truncate_prefix(bytes: &[u8]) -> (Vec<u8>, bool) {
    if bytes.len() > STRING_PREFIX_BYTES {
        (bytes[..STRING_PREFIX_BYTES].to_vec(), true)
    } else {
        (bytes.to_vec(), false)
    }
}

fn truncate_slice(bytes: &[u8]) -> &[u8] {
    &bytes[..bytes.len().min(STRING_PREFIX_BYTES)]
}

/// The zone map of one column chunk: the reused `FieldSummary` statistics
/// plus string prefix bounds and value-type presence bits.
#[derive(Debug, Clone, PartialEq)]
pub struct ChunkZoneMap {
    pub summary: FieldSummary,
    pub string_prefix: Option<StringPrefixMinMax>,
    type_bits: u8,
}

impl ChunkZoneMap {
    /// Builds the zone map for one chunk's values (`Value::Null` = null).
    pub fn build(values: &[Value]) -> Self {
        let row_count = values.len() as u64;
        let mut null_count = 0u64;
        let mut type_bits = 0u8;
        let mut numeric: Option<(f64, f64)> = None;
        let mut datetime: Option<(i64, i64)> = None;
        let mut strings: Option<(&str, &str)> = None;
        for value in values {
            match value {
                Value::Null => null_count += 1,
                Value::Bool(_) => type_bits |= HAS_BOOL,
                Value::Int(value) => {
                    type_bits |= HAS_INT;
                    fold_numeric(
                        &mut numeric,
                        f64_at_or_below(*value),
                        f64_at_or_above(*value),
                    );
                }
                Value::Float(value) => {
                    type_bits |= HAS_FLOAT;
                    if value.is_finite() {
                        fold_numeric(&mut numeric, *value, *value);
                    } else {
                        type_bits |= HAS_NONFINITE;
                    }
                }
                Value::String(value) => {
                    type_bits |= HAS_STRING;
                    if let Some(millis) = DateTimeMinMax::parse_rfc3339(value) {
                        datetime = Some(datetime.map_or((millis, millis), |(min, max)| {
                            (min.min(millis), max.max(millis))
                        }));
                    }
                    strings = Some(strings.map_or(
                        (value.as_str(), value.as_str()),
                        |(min, max)| {
                            (
                                if value.as_str() < min { value } else { min },
                                if value.as_str() > max { value } else { max },
                            )
                        },
                    ));
                }
                Value::List(_) | Value::Map(_) => type_bits |= HAS_NESTED,
            }
        }
        let mut summary = FieldSummary::new(row_count)
            .with_presence_counts(row_count, null_count, 0)
            .expect("null count never exceeds the row count");
        if let Some((min, max)) = numeric {
            summary.numeric_min_max = NumericMinMax::new(min, max);
        }
        if let Some((min, max)) = datetime {
            summary.datetime_min_max = DateTimeMinMax::new(min, max);
        }
        Self {
            summary,
            string_prefix: strings.map(|(min, max)| StringPrefixMinMax::from_bounds(min, max)),
            type_bits,
        }
    }

    pub fn row_count(&self) -> u64 {
        self.summary.row_count
    }

    pub fn null_count(&self) -> u64 {
        self.summary.null_count
    }

    /// Non-null values in the chunk.
    pub fn value_count(&self) -> u64 {
        self.summary.row_count - self.summary.null_count
    }

    /// Whether an equality predicate on `value` may match any chunk row.
    pub fn may_match_equals(&self, value: &Value) -> bool {
        if self.value_count() == 0 {
            return false;
        }
        match value {
            // Equality with null matches nothing.
            Value::Null => false,
            Value::Bool(_) => self.type_bits & HAS_BOOL != 0,
            Value::Int(value) => self.may_match_numeric_equals(*value as f64),
            Value::Float(value) => {
                if value.is_finite() {
                    self.may_match_numeric_equals(*value)
                } else {
                    self.type_bits & HAS_NONFINITE != 0
                }
            }
            Value::String(value) => {
                if self.type_bits & HAS_STRING == 0 {
                    return false;
                }
                if let (Some(millis), Some(range)) = (
                    DateTimeMinMax::parse_rfc3339(value),
                    self.summary.datetime_min_max,
                ) && !(range.min_epoch_millis..=range.max_epoch_millis).contains(&millis)
                {
                    return false;
                }
                let Some(prefix) = &self.string_prefix else {
                    return true;
                };
                let bytes = value.as_bytes();
                !(prefix.min_definitely_greater_than(bytes)
                    || prefix.max_definitely_less_than(bytes))
            }
            // Nested equality is never pruned by zone maps.
            Value::List(_) | Value::Map(_) => self.type_bits & HAS_NESTED != 0,
        }
    }

    fn may_match_numeric_equals(&self, value: f64) -> bool {
        if self.type_bits & (HAS_INT | HAS_FLOAT) == 0 {
            return false;
        }
        if self.type_bits & HAS_NONFINITE != 0 {
            // Non-finite floats live outside the min-max; stay conservative.
            return true;
        }
        self.summary
            .numeric_min_max
            .is_none_or(|range| range.contains(value))
    }

    /// Whether a range predicate may match any chunk row. Bounds follow the
    /// per-bound typed semantics documented on `ColumnPredicate`.
    pub fn may_match_range(&self, lower: Option<&RangeBound>, upper: Option<&RangeBound>) -> bool {
        if self.value_count() == 0 {
            return false;
        }
        if let Some(bound) = lower
            && self.excluded_by_lower(bound)
        {
            return false;
        }
        if let Some(bound) = upper
            && self.excluded_by_upper(bound)
        {
            return false;
        }
        true
    }

    fn excluded_by_lower(&self, bound: &RangeBound) -> bool {
        match &bound.value {
            Value::Int(value) => self.numeric_excluded_by_lower(
                f64_at_or_below(*value),
                bound.inclusive,
                f64_is_exact(*value),
            ),
            Value::Float(value) if value.is_finite() => {
                self.numeric_excluded_by_lower(*value, bound.inclusive, true)
            }
            Value::String(value) => {
                if self.type_bits & HAS_STRING == 0 {
                    return true;
                }
                if let Some(millis) = DateTimeMinMax::parse_rfc3339(value) {
                    // A parseable bound compares as a datetime, never as raw
                    // bytes (mirrors `range_disjoint_datetime`), so a chunk
                    // with no parseable strings is excluded outright.
                    return match self.summary.datetime_min_max {
                        Some(range) => {
                            millis > range.max_epoch_millis
                                || (!bound.inclusive && millis == range.max_epoch_millis)
                        }
                        None => true,
                    };
                }
                let Some(prefix) = &self.string_prefix else {
                    return true;
                };
                if bound.inclusive {
                    prefix.max_definitely_less_than(value.as_bytes())
                } else {
                    prefix.max_definitely_less_equal(value.as_bytes())
                }
            }
            Value::Bool(_) => self.type_bits & HAS_BOOL == 0,
            // Null or nested bounds are never used to prune.
            _ => false,
        }
    }

    fn excluded_by_upper(&self, bound: &RangeBound) -> bool {
        match &bound.value {
            Value::Int(value) => self.numeric_excluded_by_upper(
                f64_at_or_above(*value),
                bound.inclusive,
                f64_is_exact(*value),
            ),
            Value::Float(value) if value.is_finite() => {
                self.numeric_excluded_by_upper(*value, bound.inclusive, true)
            }
            Value::String(value) => {
                if self.type_bits & HAS_STRING == 0 {
                    return true;
                }
                if let Some(millis) = DateTimeMinMax::parse_rfc3339(value) {
                    return match self.summary.datetime_min_max {
                        Some(range) => {
                            millis < range.min_epoch_millis
                                || (!bound.inclusive && millis == range.min_epoch_millis)
                        }
                        None => true,
                    };
                }
                let Some(prefix) = &self.string_prefix else {
                    return true;
                };
                if bound.inclusive {
                    prefix.min_definitely_greater_than(value.as_bytes())
                } else {
                    prefix.min_definitely_greater_equal(value.as_bytes())
                }
            }
            Value::Bool(_) => self.type_bits & HAS_BOOL == 0,
            _ => false,
        }
    }

    fn numeric_excluded_by_lower(&self, bound: f64, inclusive: bool, exact: bool) -> bool {
        if self.type_bits & (HAS_INT | HAS_FLOAT) == 0 {
            return true;
        }
        if self.type_bits & HAS_NONFINITE != 0 {
            return false;
        }
        let Some(range) = self.summary.numeric_min_max else {
            return false;
        };
        bound > range.max || (!inclusive && exact && bound == range.max)
    }

    fn numeric_excluded_by_upper(&self, bound: f64, inclusive: bool, exact: bool) -> bool {
        if self.type_bits & (HAS_INT | HAS_FLOAT) == 0 {
            return true;
        }
        if self.type_bits & HAS_NONFINITE != 0 {
            return false;
        }
        let Some(range) = self.summary.numeric_min_max else {
            return false;
        };
        bound < range.min || (!inclusive && exact && bound == range.min)
    }

    /// Serializes into the fixed-size directory record.
    pub fn encode(&self) -> Result<[u8; ZONE_MAP_RECORD_BYTES], ColumnGroupError> {
        let mut record = [0u8; ZONE_MAP_RECORD_BYTES];
        let mut presence = 0u8;
        let mut truncation = 0u8;
        if self.summary.numeric_min_max.is_some() {
            presence |= PRESENT_NUMERIC;
        }
        if self.summary.datetime_min_max.is_some() {
            presence |= PRESENT_DATETIME;
        }
        if let Some(prefix) = &self.string_prefix {
            presence |= PRESENT_STRING;
            record[2] = prefix.min.len() as u8;
            record[3] = prefix.max.len() as u8;
            if prefix.min_truncated {
                truncation |= MIN_TRUNCATED;
            }
            if prefix.max_truncated {
                truncation |= MAX_TRUNCATED;
            }
            record[56..56 + prefix.min.len()].copy_from_slice(&prefix.min);
            record[72..72 + prefix.max.len()].copy_from_slice(&prefix.max);
        }
        record[0] = presence;
        record[1] = self.type_bits;
        record[4] = truncation;
        let row_count = u32::try_from(self.summary.row_count)
            .map_err(|_| corrupt("zone map row count exceeds u32".to_string()))?;
        let null_count = self.summary.null_count as u32;
        let value_count = self.value_count() as u32;
        record[8..12].copy_from_slice(&row_count.to_le_bytes());
        record[12..16].copy_from_slice(&value_count.to_le_bytes());
        record[16..20].copy_from_slice(&null_count.to_le_bytes());
        if let Some(range) = self.summary.numeric_min_max {
            record[24..32].copy_from_slice(&range.min.to_le_bytes());
            record[32..40].copy_from_slice(&range.max.to_le_bytes());
        }
        if let Some(range) = self.summary.datetime_min_max {
            record[40..48].copy_from_slice(&range.min_epoch_millis.to_le_bytes());
            record[48..56].copy_from_slice(&range.max_epoch_millis.to_le_bytes());
        }
        Ok(record)
    }

    /// Decodes one fixed-size directory record.
    pub(super) fn decode(cursor: &mut Cursor<'_>) -> Result<Self, ColumnGroupError> {
        let record = cursor.read_bytes(ZONE_MAP_RECORD_BYTES, "zone map record")?;
        let presence = record[0];
        let type_bits = record[1];
        if presence & !(PRESENT_NUMERIC | PRESENT_DATETIME | PRESENT_STRING) != 0 {
            return Err(corrupt("zone map presence flags are invalid".to_string()));
        }
        if type_bits & !(HAS_BOOL | HAS_INT | HAS_FLOAT | HAS_STRING | HAS_NESTED | HAS_NONFINITE)
            != 0
        {
            return Err(corrupt("zone map type bits are invalid".to_string()));
        }
        let min_len = usize::from(record[2]);
        let max_len = usize::from(record[3]);
        let truncation = record[4];
        if min_len > STRING_PREFIX_BYTES
            || max_len > STRING_PREFIX_BYTES
            || truncation & !(MIN_TRUNCATED | MAX_TRUNCATED) != 0
        {
            return Err(corrupt("zone map string prefix is malformed".to_string()));
        }
        let row_count = u64::from(u32::from_le_bytes(record[8..12].try_into().expect("4B")));
        let value_count = u64::from(u32::from_le_bytes(record[12..16].try_into().expect("4B")));
        let null_count = u64::from(u32::from_le_bytes(record[16..20].try_into().expect("4B")));
        if null_count > row_count || value_count != row_count - null_count {
            return Err(corrupt(
                "zone map presence counts are inconsistent".to_string(),
            ));
        }
        let mut summary = FieldSummary::new(row_count)
            .with_presence_counts(row_count, null_count, 0)
            .ok_or_else(|| corrupt("zone map presence counts are inconsistent".to_string()))?;
        if presence & PRESENT_NUMERIC != 0 {
            let min = f64::from_le_bytes(record[24..32].try_into().expect("8B"));
            let max = f64::from_le_bytes(record[32..40].try_into().expect("8B"));
            summary.numeric_min_max = Some(
                NumericMinMax::new(min, max)
                    .ok_or_else(|| corrupt("zone map numeric bounds are invalid".to_string()))?,
            );
        }
        if presence & PRESENT_DATETIME != 0 {
            let min = i64::from_le_bytes(record[40..48].try_into().expect("8B"));
            let max = i64::from_le_bytes(record[48..56].try_into().expect("8B"));
            summary.datetime_min_max = Some(
                DateTimeMinMax::new(min, max)
                    .ok_or_else(|| corrupt("zone map datetime bounds are invalid".to_string()))?,
            );
        }
        let string_prefix = if presence & PRESENT_STRING != 0 {
            let min = record[56..56 + min_len].to_vec();
            let max = record[72..72 + max_len].to_vec();
            let prefix = StringPrefixMinMax {
                min,
                max,
                min_truncated: truncation & MIN_TRUNCATED != 0,
                max_truncated: truncation & MAX_TRUNCATED != 0,
            };
            if !prefix.min_truncated && !prefix.max_truncated && prefix.min > prefix.max {
                return Err(corrupt("zone map string bounds are inverted".to_string()));
            }
            Some(prefix)
        } else {
            None
        };
        Ok(Self {
            summary,
            string_prefix,
            type_bits,
        })
    }
}

fn fold_numeric(current: &mut Option<(f64, f64)>, low: f64, high: f64) {
    *current = Some(current.map_or((low, high), |(min, max)| (min.min(low), max.max(high))));
}

/// The largest f64 that is `<=` the integer (i64-to-f64 rounds to nearest).
fn f64_at_or_below(value: i64) -> f64 {
    let converted = value as f64;
    if converted as i128 > i128::from(value) {
        converted.next_down()
    } else {
        converted
    }
}

/// The smallest f64 that is `>=` the integer.
fn f64_at_or_above(value: i64) -> f64 {
    let converted = value as f64;
    if (converted as i128) < i128::from(value) {
        converted.next_up()
    } else {
        converted
    }
}

/// Whether the integer converts to f64 without rounding.
fn f64_is_exact(value: i64) -> bool {
    (value as f64) as i128 == i128::from(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_decode(zone: &ChunkZoneMap) -> ChunkZoneMap {
        let record = zone.encode().unwrap();
        let mut cursor = Cursor::new(&record);
        let decoded = ChunkZoneMap::decode(&mut cursor).unwrap();
        cursor.expect_exhausted("zone map record").unwrap();
        decoded
    }

    #[test]
    fn zone_map_round_trips_through_its_fixed_record() {
        let values = vec![
            Value::Int(-5),
            Value::Null,
            Value::Float(2.5),
            Value::String("2024-06-01T00:00:00Z".to_string()),
            Value::String("z".repeat(40)),
            Value::Bool(true),
        ];
        let zone = ChunkZoneMap::build(&values);
        assert_eq!(encode_decode(&zone), zone);
        assert_eq!(zone.row_count(), 6);
        assert_eq!(zone.null_count(), 1);
        assert_eq!(zone.value_count(), 5);
    }

    #[test]
    fn numeric_bounds_widen_soundly_for_large_integers() {
        let huge = i64::MAX - 1;
        let zone = ChunkZoneMap::build(&[Value::Int(huge), Value::Int(0)]);
        // The widened bounds must contain the true value even though the
        // f64 conversion of `huge` rounds up past it.
        assert!(zone.may_match_equals(&Value::Int(huge)));
        assert!(zone.may_match_range(Some(&RangeBound::inclusive(Value::Int(huge))), None));
        assert!(!zone.may_match_equals(&Value::Int(-1)));
        let low = i64::MIN + 1;
        let zone = ChunkZoneMap::build(&[Value::Int(low)]);
        assert!(zone.may_match_equals(&Value::Int(low)));
        assert!(zone.may_match_range(None, Some(&RangeBound::inclusive(Value::Int(low)))));
    }

    #[test]
    fn string_prefix_truncation_never_prunes_a_matching_value() {
        let long_low = format!("aaaa{}", "x".repeat(30));
        let long_high = format!("mmmm{}", "y".repeat(30));
        let zone = ChunkZoneMap::build(&[
            Value::String(long_low.clone()),
            Value::String(long_high.clone()),
        ]);
        assert!(zone.may_match_equals(&Value::String(long_low.clone())));
        assert!(zone.may_match_equals(&Value::String(long_high.clone())));
        // Same 16-byte prefix as the max but a different tail: may match.
        assert!(zone.may_match_equals(&Value::String(format!("mmmm{}z", "y".repeat(30)))));
        // Prefix strictly above the max prefix: provably absent.
        assert!(!zone.may_match_equals(&Value::String("n".to_string())));
        // Prefix strictly below the min: provably absent.
        assert!(!zone.may_match_equals(&Value::String("aaa".to_string())));
        // Upper-bound range below the min prunes; at the min it must not.
        assert!(!zone.may_match_range(
            None,
            Some(&RangeBound::exclusive(Value::String("aaaa".to_string())))
        ));
        assert!(zone.may_match_range(None, Some(&RangeBound::inclusive(Value::String(long_low)))));
        // Lower bound above every extension of the truncated max prunes.
        assert!(!zone.may_match_range(
            Some(&RangeBound::inclusive(Value::String("mmmn".to_string()))),
            None
        ));
        // Lower bound equal to the truncated max prefix must stay open.
        assert!(zone.may_match_range(
            Some(&RangeBound::inclusive(Value::String("mmmm".to_string()))),
            None
        ));
    }

    #[test]
    fn typed_pruning_uses_presence_bits() {
        let ints = ChunkZoneMap::build(&[Value::Int(1), Value::Int(9)]);
        assert!(!ints.may_match_equals(&Value::String("1".to_string())));
        assert!(!ints.may_match_equals(&Value::Bool(true)));
        assert!(!ints.may_match_range(
            Some(&RangeBound::inclusive(Value::String("a".to_string()))),
            None
        ));
        let strings = ChunkZoneMap::build(&[Value::String("alpha".to_string())]);
        assert!(!strings.may_match_equals(&Value::Int(5)));
        assert!(!strings.may_match_range(Some(&RangeBound::inclusive(Value::Int(5))), None));
        let all_null = ChunkZoneMap::build(&[Value::Null, Value::Null]);
        assert!(!all_null.may_match_equals(&Value::Int(1)));
        assert!(!all_null.may_match_range(None, None));
        assert!(!all_null.may_match_equals(&Value::Null));
    }

    #[test]
    fn nonfinite_floats_disable_numeric_pruning() {
        let zone = ChunkZoneMap::build(&[Value::Float(1.0), Value::Float(f64::INFINITY)]);
        assert!(zone.may_match_equals(&Value::Float(f64::INFINITY)));
        assert!(zone.may_match_range(Some(&RangeBound::inclusive(Value::Int(1_000_000))), None));
        let finite = ChunkZoneMap::build(&[Value::Float(1.0)]);
        assert!(!finite.may_match_equals(&Value::Float(f64::NAN)));
        assert!(!finite.may_match_range(Some(&RangeBound::inclusive(Value::Int(1_000_000))), None));
    }

    #[test]
    fn datetime_bounds_prune_parseable_string_ranges() {
        let zone = ChunkZoneMap::build(&[
            Value::String("2024-01-01T00:00:00Z".to_string()),
            Value::String("2024-06-01T00:00:00Z".to_string()),
        ]);
        assert!(!zone.may_match_range(
            Some(&RangeBound::inclusive(Value::String(
                "2025-01-01T00:00:00Z".to_string()
            ))),
            None
        ));
        assert!(!zone.may_match_range(
            None,
            Some(&RangeBound::exclusive(Value::String(
                "2024-01-01T00:00:00Z".to_string()
            )))
        ));
        assert!(zone.may_match_range(
            Some(&RangeBound::inclusive(Value::String(
                "2024-03-01T00:00:00Z".to_string()
            ))),
            None
        ));
        assert!(!zone.may_match_equals(&Value::String("2025-01-01T00:00:00Z".to_string())));
    }

    #[test]
    fn corrupt_zone_records_are_rejected() {
        let zone = ChunkZoneMap::build(&[Value::Int(3), Value::Null]);
        let record = zone.encode().unwrap();
        // Invalid presence flags.
        let mut tampered = record;
        tampered[0] = 0xff;
        assert!(ChunkZoneMap::decode(&mut Cursor::new(&tampered)).is_err());
        // Inconsistent counts.
        let mut tampered = record;
        tampered[16..20].copy_from_slice(&100u32.to_le_bytes());
        assert!(ChunkZoneMap::decode(&mut Cursor::new(&tampered)).is_err());
        // Oversized string prefix length.
        let mut tampered = record;
        tampered[0] |= PRESENT_STRING;
        tampered[2] = 40;
        assert!(ChunkZoneMap::decode(&mut Cursor::new(&tampered)).is_err());
        // Truncated record.
        assert!(ChunkZoneMap::decode(&mut Cursor::new(&record[..40])).is_err());
    }
}
