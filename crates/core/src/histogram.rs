// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Frequency-bearing property distributions. Counts inside the distribution
//! describe the retained row sample, not a set of distinct values.

use crate::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

pub const MAX_COMPRESSED_HISTOGRAM_BUCKETS: usize = 64;
pub const MAX_COMPRESSED_HISTOGRAM_MCV: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MostCommonValue {
    pub value: Value,
    pub sample_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistogramBucket {
    pub lower: Value,
    pub upper: Value,
    pub sample_count: u64,
    /// Distinct values in this sample bucket; never a population NDV.
    pub sample_distinct_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompressedHistogram {
    pub population_count: u64,
    pub non_null_count: u64,
    pub null_count: u64,
    pub missing_count: u64,
    pub sample_size: u64,
    pub most_common: Arc<[MostCommonValue]>,
    pub buckets: Arc<[HistogramBucket]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PropertyEstimateSource {
    MostCommonValue,
    ResidualDistinctCount,
    CompressedHistogram,
    NullCount,
    SampleFallback,
}

impl PropertyEstimateSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MostCommonValue => "mcv",
            Self::ResidualDistinctCount => "residual_ndv",
            Self::CompressedHistogram => "compressed_histogram",
            Self::NullCount => "null_count",
            Self::SampleFallback => "sample_fallback",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PropertyCardinalityEstimate {
    pub rows: u64,
    pub source: PropertyEstimateSource,
    pub sample_size: u64,
    pub population_count: u64,
}

crate::impl_retained_heap_for_struct!(MostCommonValue {
    value,
    sample_count
});
crate::impl_retained_heap_for_struct!(HistogramBucket {
    lower,
    upper,
    sample_count,
    sample_distinct_count
});
crate::impl_retained_heap_for_struct!(CompressedHistogram {
    population_count,
    non_null_count,
    null_count,
    missing_count,
    sample_size,
    most_common,
    buckets
});

impl CompressedHistogram {
    /// Builds disjoint MCV and approximately equal-depth residual buckets.
    /// Sampling, ownership and byte admission belong to the storage collector.
    pub fn from_sample(
        population_count: u64,
        non_null_count: u64,
        null_count: u64,
        missing_count: u64,
        sample: Vec<Value>,
    ) -> Option<Self> {
        if population_count == 0
            || non_null_count
                .checked_add(null_count)?
                .checked_add(missing_count)?
                != population_count
            || sample.len() as u64 > non_null_count
            || (sample.is_empty() && non_null_count != 0)
            || sample.iter().any(|value| matches!(value, Value::Null))
        {
            return None;
        }
        let sample_size = sample.len() as u64;
        let mut frequencies = BTreeMap::<Value, u64>::new();
        for value in sample {
            *frequencies.entry(value).or_default() += 1;
        }
        let mut most_common = Vec::new();
        if sample_size == non_null_count && frequencies.len() <= MAX_COMPRESSED_HISTOGRAM_BUCKETS {
            most_common.extend(frequencies.into_iter().map(|(value, sample_count)| {
                MostCommonValue {
                    value,
                    sample_count,
                }
            }));
            return Some(Self {
                population_count,
                non_null_count,
                null_count,
                missing_count,
                sample_size,
                most_common: most_common.into(),
                buckets: Arc::from([]),
            });
        }
        let threshold = sample_size.div_ceil(MAX_COMPRESSED_HISTOGRAM_BUCKETS as u64);
        let mut hot = frequencies
            .iter()
            .filter(|(_, count)| **count > threshold)
            .map(|(value, count)| (value.clone(), *count))
            .collect::<Vec<_>>();
        hot.sort_by(|(left_value, left_count), (right_value, right_count)| {
            right_count
                .cmp(left_count)
                .then_with(|| left_value.cmp(right_value))
        });
        hot.truncate(MAX_COMPRESSED_HISTOGRAM_MCV);
        for (value, sample_count) in hot {
            frequencies.remove(&value);
            most_common.push(MostCommonValue {
                value,
                sample_count,
            });
        }
        most_common.sort_by(|left, right| left.value.cmp(&right.value));
        let residual_count = frequencies.values().sum::<u64>();
        let bucket_limit = MAX_COMPRESSED_HISTOGRAM_BUCKETS - most_common.len();
        let target = residual_count.div_ceil(bucket_limit as u64).max(1);
        let mut buckets = Vec::<HistogramBucket>::new();
        for (value, count) in frequencies {
            let at_limit = buckets.len() == bucket_limit;
            if let Some(bucket) = buckets.last_mut()
                && (bucket.sample_count < target || at_limit)
            {
                bucket.upper = value;
                bucket.sample_count += count;
                bucket.sample_distinct_count += 1;
            } else {
                buckets.push(HistogramBucket {
                    lower: value.clone(),
                    upper: value,
                    sample_count: count,
                    sample_distinct_count: 1,
                });
            }
        }
        Some(Self {
            population_count,
            non_null_count,
            null_count,
            missing_count,
            sample_size,
            most_common: most_common.into(),
            buckets: buckets.into(),
        })
    }

    pub fn is_sampled(&self) -> bool {
        self.sample_size < self.non_null_count
    }

    /// Recovered metadata is optional derived state, never a semantic proof.
    pub fn is_valid(&self) -> bool {
        if self.population_count == 0
            || self
                .non_null_count
                .checked_add(self.null_count)
                .and_then(|count| count.checked_add(self.missing_count))
                != Some(self.population_count)
            || self.sample_size > self.non_null_count
            || (self.sample_size == 0 && self.non_null_count != 0)
            || self.most_common.len() + self.buckets.len() > MAX_COMPRESSED_HISTOGRAM_BUCKETS
            || self
                .most_common
                .windows(2)
                .any(|pair| pair[0].value >= pair[1].value)
            || self
                .buckets
                .windows(2)
                .any(|pair| pair[0].upper >= pair[1].lower)
        {
            return false;
        }
        let mut weight = 0_u64;
        for entry in self.most_common.iter() {
            if entry.sample_count == 0 || matches!(entry.value, Value::Null) {
                return false;
            }
            let Some(next) = weight.checked_add(entry.sample_count) else {
                return false;
            };
            weight = next;
        }
        for bucket in self.buckets.iter() {
            if bucket.lower > bucket.upper
                || bucket.sample_count == 0
                || bucket.sample_distinct_count == 0
                || bucket.sample_distinct_count > bucket.sample_count
                || (bucket.lower == bucket.upper && bucket.sample_distinct_count != 1)
                || self
                    .most_common
                    .binary_search_by(|entry| entry.value.cmp(&bucket.lower))
                    .is_ok()
                || self
                    .most_common
                    .binary_search_by(|entry| entry.value.cmp(&bucket.upper))
                    .is_ok()
                || matches!(bucket.lower, Value::Null)
                || matches!(bucket.upper, Value::Null)
            {
                return false;
            }
            let Some(next) = weight.checked_add(bucket.sample_count) else {
                return false;
            };
            weight = next;
        }
        weight == self.sample_size
    }

    fn estimate(
        &self,
        input_rows: u64,
        numerator: u64,
        denominator: u64,
        source: PropertyEstimateSource,
    ) -> PropertyCardinalityEstimate {
        let rows = if denominator == 0 {
            0
        } else {
            (u128::from(input_rows) * u128::from(numerator))
                .div_ceil(u128::from(denominator))
                .min(u128::from(input_rows)) as u64
        };
        PropertyCardinalityEstimate {
            rows,
            source,
            sample_size: self.sample_size,
            population_count: self.population_count,
        }
    }

    fn sample_estimate(
        &self,
        input_rows: u64,
        sample_weight: u64,
        source: PropertyEstimateSource,
    ) -> PropertyCardinalityEstimate {
        // First scale to the non-NULL population, then apply the sample fraction.
        let non_null_rows = self
            .estimate(
                input_rows,
                self.non_null_count,
                self.population_count,
                source,
            )
            .rows;
        self.estimate(non_null_rows, sample_weight, self.sample_size, source)
    }

    pub fn equality(
        &self,
        value: &Value,
        input_rows: u64,
        distinct_count: Option<u64>,
    ) -> PropertyCardinalityEstimate {
        if matches!(value, Value::Null) {
            return self.estimate(input_rows, 0, 1, PropertyEstimateSource::NullCount);
        }
        if let Ok(index) = self
            .most_common
            .binary_search_by(|entry| entry.value.cmp(value))
        {
            return self.sample_estimate(
                input_rows,
                self.most_common[index].sample_count,
                PropertyEstimateSource::MostCommonValue,
            );
        }
        let hot_weight = self
            .most_common
            .iter()
            .map(|entry| entry.sample_count)
            .sum::<u64>();
        let residual_weight = self.sample_size.saturating_sub(hot_weight);
        let source = if distinct_count.is_some() {
            PropertyEstimateSource::ResidualDistinctCount
        } else {
            PropertyEstimateSource::SampleFallback
        };
        if let Some(ndv) = distinct_count
            .or(Some(self.non_null_count))
            .and_then(|ndv| ndv.checked_sub(self.most_common.len() as u64))
            .filter(|ndv| *ndv > 0)
        {
            let mut estimate = self.sample_estimate(input_rows, residual_weight, source);
            estimate.rows = estimate.rows.div_ceil(ndv);
            if estimate.rows != 0 || !self.is_sampled() {
                return estimate;
            }
        }
        // An unobserved sample value is unknown, not evidence of absence.
        if self.is_sampled() {
            self.sample_estimate(input_rows, 1, PropertyEstimateSource::SampleFallback)
        } else {
            self.estimate(
                input_rows,
                0,
                1,
                PropertyEstimateSource::ResidualDistinctCount,
            )
        }
    }

    pub fn nulls(&self, input_rows: u64) -> PropertyCardinalityEstimate {
        self.estimate(
            input_rows,
            self.null_count.saturating_add(self.missing_count),
            self.population_count,
            PropertyEstimateSource::NullCount,
        )
    }

    pub fn range(
        &self,
        lower: Option<(&Value, bool)>,
        upper: Option<(&Value, bool)>,
        input_rows: u64,
    ) -> PropertyCardinalityEstimate {
        let accepts = |value: &Value| {
            lower.is_none_or(|(bound, inclusive)| value > bound || (inclusive && value == bound))
                && upper
                    .is_none_or(|(bound, inclusive)| value < bound || (inclusive && value == bound))
        };
        let mut weight = self
            .most_common
            .iter()
            .filter(|entry| accepts(&entry.value))
            .map(|entry| entry.sample_count as f64)
            .sum::<f64>();
        for bucket in self.buckets.iter() {
            let fraction = bucket_fraction(bucket, lower, upper);
            weight += bucket.sample_count as f64 * fraction;
        }
        self.sample_estimate(
            input_rows,
            (weight.ceil() as u64).min(self.sample_size),
            PropertyEstimateSource::CompressedHistogram,
        )
    }
}

fn numeric_position(lower: &Value, upper: &Value, value: &Value) -> Option<f64> {
    let position = match (lower, upper, value) {
        (Value::Int(low), Value::Int(high), Value::Int(value)) if high > low => {
            (i128::from(*value) - i128::from(*low)) as f64
                / (i128::from(*high) - i128::from(*low)) as f64
        }
        (Value::Float(low), Value::Float(high), Value::Float(value))
            if low.is_finite() && high.is_finite() && value.is_finite() && high > low =>
        {
            (value - low) / (high - low)
        }
        _ => return None,
    };
    position.is_finite().then(|| position.clamp(0.0, 1.0))
}

fn bucket_fraction(
    bucket: &HistogramBucket,
    lower: Option<(&Value, bool)>,
    upper: Option<(&Value, bool)>,
) -> f64 {
    let cdf = |value: &Value, inclusive: bool| {
        if value < &bucket.lower {
            return 0.0;
        }
        if value > &bucket.upper {
            return 1.0;
        }
        if bucket.lower == bucket.upper {
            return if inclusive { 1.0 } else { 0.0 };
        }
        let atom = 1.0 / bucket.sample_distinct_count as f64;
        let position = numeric_position(&bucket.lower, &bucket.upper, value).unwrap_or(0.5);
        (position * (1.0 - atom) + if inclusive { atom } else { 0.0 }).clamp(0.0, 1.0)
    };
    let low = lower.map_or(0.0, |(value, inclusive)| cdf(value, !inclusive));
    let high = upper.map_or(1.0, |(value, inclusive)| cdf(value, inclusive));
    (high - low).clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_nulls_and_singletons_conserve_population() {
        let nulls = CompressedHistogram::from_sample(10, 0, 4, 6, vec![]).unwrap();
        assert!(nulls.is_valid());
        assert_eq!(nulls.nulls(100).rows, 100);
        assert_eq!(nulls.range(None, None, 100).rows, 0);
        let histogram = CompressedHistogram::from_sample(
            10,
            6,
            2,
            2,
            vec![
                Value::Int(1),
                Value::Int(1),
                Value::Int(2),
                Value::Int(3),
                Value::Int(3),
                Value::Int(3),
            ],
        )
        .unwrap();
        assert!(histogram.is_valid());
        assert_eq!(histogram.equality(&Value::Int(3), 10, Some(3)).rows, 3);
        assert_eq!(
            histogram
                .range(
                    Some((&Value::Int(1), false)),
                    Some((&Value::Int(3), false)),
                    10
                )
                .rows,
            1
        );
        assert_eq!(
            histogram
                .range(
                    Some((&Value::Int(3), true)),
                    Some((&Value::Int(1), true)),
                    10
                )
                .rows,
            0
        );
        assert_eq!(
            histogram.range(None, None, 10).rows + histogram.nulls(10).rows,
            10
        );
    }

    #[test]
    fn rare_sample_values_are_not_population_mcv_or_ndv() {
        let histogram = CompressedHistogram::from_sample(
            10_000,
            10_000,
            0,
            0,
            (0..64).map(Value::Int).collect(),
        )
        .unwrap();
        assert!(histogram.most_common.is_empty());
        assert!(histogram.is_sampled());
        assert_eq!(
            histogram
                .equality(&Value::Int(1), 10_000, Some(10_000))
                .rows,
            1
        );
        assert!(histogram.equality(&Value::Int(10_001), 10_000, None).rows > 0);
    }

    #[test]
    fn hot_values_are_separate_from_residual_buckets() {
        let sample = std::iter::repeat_n(Value::Int(1000), 900)
            .chain((0..100).map(Value::Int))
            .collect();
        let histogram = CompressedHistogram::from_sample(1000, 1000, 0, 0, sample).unwrap();
        assert!(histogram.is_valid());
        assert_eq!(
            histogram.equality(&Value::Int(1000), 1000, Some(101)).rows,
            900
        );
        assert_eq!(
            histogram
                .range(None, Some((&Value::Int(100), false)), 1000)
                .rows,
            100
        );
        assert_eq!(histogram.most_common.len(), 1);
        assert!(histogram
            .buckets
            .iter()
            .all(|bucket| bucket.upper < Value::Int(1000)));
    }

    #[test]
    fn null_missing_and_unobserved_sample_values_are_distinct_cases() {
        let histogram =
            CompressedHistogram::from_sample(1000, 800, 100, 100, vec![Value::Bool(true); 64])
                .unwrap();
        assert!(histogram.is_sampled());
        assert_eq!(histogram.nulls(1000).rows, 200);
        assert_eq!(
            histogram.equality(&Value::Bool(true), 1000, Some(2)).rows,
            800
        );
        assert!(histogram.equality(&Value::Bool(false), 1000, Some(2)).rows > 0);
        assert_eq!(histogram.equality(&Value::Null, 1000, Some(2)).rows, 0);
    }

    #[test]
    fn integer_ranges_near_i64_max_keep_monotone_estimates() {
        let sample = (0..256).map(|i| Value::Int(i64::MAX - 255 + i)).collect();
        let histogram = CompressedHistogram::from_sample(256, 256, 0, 0, sample).unwrap();
        let mut previous = 0;
        for offset in 0..256 {
            let rows = histogram
                .range(
                    None,
                    Some((&Value::Int(i64::MAX - 255 + offset), true)),
                    256,
                )
                .rows;
            assert!(rows >= previous && rows <= 256);
            previous = rows;
        }
        assert_eq!(previous, 256);
    }
}
