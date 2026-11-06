// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Bounded, frequency-preserving row samples. Reserved charges include spare
//! capacity and construction scratch; they do not claim an allocator RSS cap.

use hawdb_core::{CompressedHistogram, RetainedHeapBytes, Value};
use std::collections::BTreeMap;

pub const PROPERTY_DISTRIBUTION_SAMPLE_ROWS: usize = 256;
pub const PROPERTY_DISTRIBUTION_BYTES: usize = 128 * 1024;
pub const PROPERTY_DISTRIBUTIONS_BYTES: usize = 8 * 1024 * 1024;
const SCRATCH_FACTOR: usize = 6;

pub struct PropertyValueBudget {
    bytes: usize,
    limit: usize,
    admission_closed: bool,
}

impl Default for PropertyValueBudget {
    fn default() -> Self {
        Self {
            bytes: 0,
            limit: PROPERTY_DISTRIBUTIONS_BYTES,
            admission_closed: false,
        }
    }
}

impl PropertyValueBudget {
    pub fn admit_group(&mut self, key: &impl RetainedHeapBytes) -> bool {
        if self.admission_closed {
            return false;
        }
        let Some(charge) = key
            .retained_heap_bytes()
            .and_then(|bytes| bytes.checked_mul(4)?.checked_add(512))
        else {
            self.admission_closed = true;
            return false;
        };
        if self.bytes.saturating_add(charge) > self.limit {
            self.admission_closed = true;
            return false;
        }
        self.bytes += charge;
        true
    }

    fn value_charge(value: &Value) -> Option<usize> {
        value
            .retained_heap_bytes()?
            .checked_add(std::mem::size_of::<Value>() + 96)
    }

    pub fn admit_value(&mut self, value: &Value) -> bool {
        let Some(charge) = Self::value_charge(value) else {
            return false;
        };
        if self.bytes.saturating_add(charge) > self.limit {
            return false;
        }
        self.bytes += charge;
        true
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct DistributionImage {
    version: u32,
    population: u64,
    non_null: u64,
    nulls: u64,
    missing: u64,
    sample_size: u64,
    most_common: Vec<(String, u64)>,
    buckets: Vec<(String, String, u64, u64)>,
}

pub fn encode(histogram: &CompressedHistogram) -> crate::Result<String> {
    let image = DistributionImage {
        version: 1,
        population: histogram.population_count,
        non_null: histogram.non_null_count,
        nulls: histogram.null_count,
        missing: histogram.missing_count,
        sample_size: histogram.sample_size,
        most_common: histogram
            .most_common
            .iter()
            .map(|entry| (crate::text::encode_value(&entry.value), entry.sample_count))
            .collect(),
        buckets: histogram
            .buckets
            .iter()
            .map(|bucket| {
                (
                    crate::text::encode_value(&bucket.lower),
                    crate::text::encode_value(&bucket.upper),
                    bucket.sample_count,
                    bucket.sample_distinct_count,
                )
            })
            .collect(),
    };
    serde_json::to_string(&image).map_err(|error| {
        hawdb_core::HawDBError::Storage(format!("cannot encode optimizer distribution: {error}"))
    })
}

pub fn decode(raw: &str) -> Option<CompressedHistogram> {
    use hawdb_core::{HistogramBucket, MostCommonValue};
    if raw.len() > PROPERTY_DISTRIBUTION_BYTES * 2 {
        return None;
    }
    let image: DistributionImage = serde_json::from_str(raw).ok()?;
    if image.version != 1
        || image.sample_size > PROPERTY_DISTRIBUTION_SAMPLE_ROWS as u64
        || image.most_common.len() + image.buckets.len()
            > hawdb_core::histogram::MAX_COMPRESSED_HISTOGRAM_BUCKETS
    {
        return None;
    }
    let histogram = CompressedHistogram {
        population_count: image.population,
        non_null_count: image.non_null,
        null_count: image.nulls,
        missing_count: image.missing,
        sample_size: image.sample_size,
        most_common: image
            .most_common
            .into_iter()
            .map(|(value, sample_count)| {
                Some(MostCommonValue {
                    value: decode_scalar(&value)?,
                    sample_count,
                })
            })
            .collect::<Option<Vec<_>>>()?
            .into(),
        buckets: image
            .buckets
            .into_iter()
            .map(|(lower, upper, sample_count, sample_distinct_count)| {
                Some(HistogramBucket {
                    lower: decode_scalar(&lower)?,
                    upper: decode_scalar(&upper)?,
                    sample_count,
                    sample_distinct_count,
                })
            })
            .collect::<Option<Vec<_>>>()?
            .into(),
    };
    (histogram.is_valid() && histogram.retained_heap_bytes()? <= PROPERTY_DISTRIBUTION_BYTES)
        .then_some(histogram)
}

fn decode_scalar(raw: &str) -> Option<Value> {
    // Reject nested/large value families before entering the recursive codec.
    matches!(raw.as_bytes().first(), Some(b'b' | b'i' | b'f' | b's'))
        .then(|| crate::text::decode_value(raw).ok())?
}

#[derive(Debug)]
pub struct PropertyRowSample {
    observed: u64,
    nulls: u64,
    non_null: u64,
    values: Vec<Value>,
    payload_bytes: usize,
    random: u64,
    excluded: bool,
}

impl Default for PropertyRowSample {
    fn default() -> Self {
        Self {
            observed: 0,
            nulls: 0,
            non_null: 0,
            values: Vec::new(),
            payload_bytes: 0,
            random: 0x48e7_103d_826a_59bf,
            excluded: false,
        }
    }
}

impl PropertyRowSample {
    pub fn charged_bytes(&self) -> usize {
        self.values
            .capacity()
            .saturating_mul(std::mem::size_of::<Value>())
            .saturating_add(self.payload_bytes)
            .saturating_mul(SCRATCH_FACTOR)
            .saturating_add(std::mem::size_of::<Self>())
    }

    fn exclude(&mut self) {
        self.values = Vec::new();
        self.payload_bytes = 0;
        self.excluded = true;
    }

    fn random_below(&mut self, bound: u64) -> u64 {
        // SplitMix64 plus rejection avoids modulo bias. A fixed seed makes
        // snapshot collection reproducible without hashing away duplicates.
        let threshold = bound.wrapping_neg() % bound;
        loop {
            self.random = self.random.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut value = self.random;
            value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            value ^= value >> 31;
            if value >= threshold {
                return value % bound;
            }
        }
    }

    pub fn observe(&mut self, value: &Value, eligible: bool, byte_limit: usize) {
        if self.excluded {
            return;
        }
        if !eligible {
            self.exclude();
            return;
        }
        self.observed = self.observed.saturating_add(1);
        if matches!(value, Value::Null) {
            self.nulls = self.nulls.saturating_add(1);
            return;
        }
        self.non_null = self.non_null.saturating_add(1);
        let position = if self.values.len() < PROPERTY_DISTRIBUTION_SAMPLE_ROWS {
            self.values.len()
        } else {
            let position = self.random_below(self.non_null);
            if position >= PROPERTY_DISTRIBUTION_SAMPLE_ROWS as u64 {
                return;
            }
            position as usize
        };
        let Some(payload) = value.retained_heap_bytes() else {
            self.exclude();
            return;
        };
        let retired = self
            .values
            .get(position)
            .and_then(RetainedHeapBytes::retained_heap_bytes)
            .unwrap_or(0);
        let next_payload = self
            .payload_bytes
            .saturating_sub(retired)
            .saturating_add(payload);
        // Reserve the complete vector once, before its first allocation.
        let next_charge = PROPERTY_DISTRIBUTION_SAMPLE_ROWS
            .saturating_mul(std::mem::size_of::<Value>())
            .saturating_add(next_payload)
            .saturating_mul(SCRATCH_FACTOR)
            .saturating_add(std::mem::size_of::<Self>());
        if next_charge > byte_limit.min(PROPERTY_DISTRIBUTION_BYTES) {
            self.exclude();
            return;
        }
        if self.values.capacity() == 0 {
            self.values = Vec::with_capacity(PROPERTY_DISTRIBUTION_SAMPLE_ROWS);
        }
        if position == self.values.len() {
            self.values.push(value.clone());
        } else {
            self.values[position] = value.clone();
        }
        self.payload_bytes = next_payload;
    }

    pub fn finish(self, population_count: u64) -> Option<CompressedHistogram> {
        if self.excluded || self.observed > population_count {
            return None;
        }
        CompressedHistogram::from_sample(
            population_count,
            self.non_null,
            self.nulls,
            population_count - self.observed,
            self.values,
        )
    }
}

pub struct PropertyDistributionCollector<K> {
    groups: BTreeMap<K, PropertyRowSample>,
    charged: usize,
    admission_closed: bool,
}

impl<K: Ord> Default for PropertyDistributionCollector<K> {
    fn default() -> Self {
        Self {
            groups: BTreeMap::new(),
            charged: 0,
            admission_closed: false,
        }
    }
}

impl<K: Ord + Clone> PropertyDistributionCollector<K> {
    pub fn observe(&mut self, key: K, key_bytes: usize, value: &Value, eligible: bool) {
        if !self.groups.contains_key(&key) {
            if self.admission_closed {
                return;
            }
            let reservation = key_bytes.saturating_mul(4).saturating_add(512);
            if self
                .charged
                .saturating_add(reservation)
                .saturating_add(PROPERTY_DISTRIBUTION_BYTES)
                > PROPERTY_DISTRIBUTIONS_BYTES
            {
                // Never admit a target halfway through its source population.
                self.admission_closed = true;
                return;
            }
            self.charged = self.charged.saturating_add(reservation);
            self.groups
                .insert(key.clone(), PropertyRowSample::default());
        }
        let sample = self.groups.get_mut(&key).expect("admitted property group");
        let before = sample.charged_bytes();
        let allowance = PROPERTY_DISTRIBUTIONS_BYTES
            .saturating_sub(self.charged)
            .saturating_add(before);
        sample.observe(value, eligible, allowance);
        self.charged = self
            .charged
            .saturating_sub(before)
            .saturating_add(sample.charged_bytes());
    }

    pub fn finish(self, mut population: impl FnMut(&K) -> u64) -> BTreeMap<K, CompressedHistogram> {
        self.groups
            .into_iter()
            .filter_map(|(key, sample)| {
                let histogram = sample.finish(population(&key))?;
                Some((key, histogram))
            })
            .collect()
    }

    pub fn finish_with_work_context(
        self,
        mut population: impl FnMut(&K) -> u64,
        work: &crate::background::CheckpointWorkContext,
    ) -> Result<BTreeMap<K, CompressedHistogram>, crate::background::CheckpointWorkError> {
        let mut result = BTreeMap::new();
        for (key, sample) in self.groups {
            let unit = work.start_unit()?;
            if let Some(histogram) = sample.finish(population(&key)) {
                result.insert(key, histogram);
            }
            unit.finish();
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distribution_images_round_trip_and_refuse_invalid_optional_metadata() {
        let histogram = CompressedHistogram::from_sample(
            5,
            3,
            1,
            1,
            vec![Value::Int(1), Value::Int(1), Value::Int(2)],
        )
        .unwrap();
        let encoded = encode(&histogram).unwrap();
        assert_eq!(decode(&encoded), Some(histogram));
        for (field, value) in [("version", 2), ("sample_size", 4), ("population", 0)] {
            let mut image: serde_json::Value = serde_json::from_str(&encoded).unwrap();
            image[field] = value.into();
            assert!(decode(&image.to_string()).is_none());
        }
        assert!(decode(&"x".repeat(PROPERTY_DISTRIBUTION_BYTES * 2 + 1)).is_none());
        assert!(decode("{}").is_none());
    }

    #[test]
    fn target_admission_and_distinct_value_budgets_remain_bounded() {
        let mut collector = PropertyDistributionCollector::default();
        for index in 0..10_000 {
            collector.observe(index, 0, &Value::Int(1), true);
            assert!(collector.charged <= PROPERTY_DISTRIBUTIONS_BYTES);
        }
        assert!(collector.admission_closed);
        assert!(collector.groups.len() < 10_000);
        let existing = *collector.groups.first_key_value().unwrap().0;
        collector.observe(existing, 0, &Value::Int(1), false);
        assert!(!collector.finish(|_| 1).contains_key(&existing));

        let mut budget = PropertyValueBudget {
            bytes: 0,
            limit: 1024,
            admission_closed: false,
        };
        assert!(budget.admit_group(&String::from("property")));
        assert!(!budget.admit_value(&Value::String("x".repeat(2048))));
        assert!(budget.bytes <= budget.limit);
    }

    #[test]
    fn repeated_rows_keep_their_frequency_and_sampling_is_reproducible() {
        let collect = || {
            let mut sample = PropertyRowSample::default();
            for index in 0..100_000 {
                sample.observe(
                    &Value::Bool(index < 99_000),
                    true,
                    PROPERTY_DISTRIBUTION_BYTES,
                );
                assert!(sample.charged_bytes() <= PROPERTY_DISTRIBUTION_BYTES);
            }
            sample.finish(100_000).unwrap()
        };
        let histogram = collect();
        assert_eq!(histogram, collect());
        assert!(histogram.is_sampled());
        assert!(
            histogram
                .equality(&Value::Bool(true), 100_000, Some(2))
                .rows
                > 95_000
        );
    }

    #[test]
    fn refusal_excludes_the_whole_target_without_partial_population() {
        let mut sample = PropertyRowSample::default();
        sample.observe(&Value::Int(1), true, PROPERTY_DISTRIBUTION_BYTES);
        sample.observe(
            &Value::String("x".repeat(PROPERTY_DISTRIBUTION_BYTES)),
            true,
            PROPERTY_DISTRIBUTION_BYTES,
        );
        assert!(sample.finish(2).is_none());
    }
}
