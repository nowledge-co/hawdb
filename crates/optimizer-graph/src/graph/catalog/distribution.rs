// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! One distribution estimate feeds access ranking, residual selectivity and
//! final costing. Exact query-local point counts retain precedence.

use super::*;
use hawdb_core::{CompressedHistogram, PropertyCardinalityEstimate};

impl OptimizerCatalog {
    pub fn with_statistics_parameter_values(mut self, values: BTreeMap<Value, Value>) -> Self {
        self.statistics_parameter_values = values;
        self
    }

    pub(in crate::graph) fn statistics_value<'a>(&'a self, value: &'a Value) -> &'a Value {
        self.statistics_parameter_values.get(value).unwrap_or(value)
    }

    pub(in crate::graph) fn property_distribution(
        &self,
        label: &str,
        property: &str,
    ) -> Option<&CompressedHistogram> {
        self.property_distributions
            .get(&(label.to_owned(), property.to_owned()))
    }

    pub(in crate::graph) fn rel_property_distribution(
        &self,
        rel_type: &str,
        property: &str,
    ) -> Option<&CompressedHistogram> {
        self.rel_property_distributions
            .get(&(rel_type.to_owned(), property.to_owned()))
    }

    pub(in crate::graph) fn property_equality_estimate(
        &self,
        label: &str,
        property: &str,
        value: &Value,
        input_rows: u64,
    ) -> Option<PropertyCardinalityEstimate> {
        if self.query_local_property_eq_rows(label, property).is_some() {
            return None;
        }
        let histogram = self.property_distribution(label, property)?;
        let value = self.statistics_value(value);
        compatible(histogram, value).then(|| {
            histogram.equality(
                value,
                input_rows,
                self.known_distinct_count(label, property),
            )
        })
    }

    pub(in crate::graph) fn estimate_property_value_rows(
        &self,
        label: &str,
        property: &str,
        value: &Value,
        input_rows: u64,
    ) -> u64 {
        self.property_equality_estimate(label, property, value, input_rows)
            .map_or_else(
                || self.estimate_property_eq_rows(label, property, input_rows),
                |estimate| estimate.rows.max(1),
            )
    }

    pub(in crate::graph) fn estimate_property_index_value_rows(
        &self,
        label: &str,
        property: &str,
        value: &Value,
    ) -> u64 {
        self.property_equality_estimate(label, property, value, self.label_count(label))
            .map_or_else(
                || self.estimate_property_index_eq_rows(label, property),
                |estimate| estimate.rows.max(1),
            )
    }

    pub(in crate::graph) fn estimate_property_index_values_rows(
        &self,
        label: &str,
        property: &str,
        values: &[Value],
    ) -> u64 {
        self.distribution_in_rows(label, property, values, self.label_count(label))
            .unwrap_or_else(|| {
                self.estimate_property_index_in_rows(label, property, values.len() as u64)
            })
    }

    pub(in crate::graph) fn distribution_in_rows(
        &self,
        label: &str,
        property: &str,
        values: &[Value],
        input_rows: u64,
    ) -> Option<u64> {
        let mut seen = BTreeSet::new();
        let mut rows = 0_u64;
        for value in values {
            let value = self.statistics_value(value);
            if !seen.insert(value) {
                continue;
            }
            rows = rows.saturating_add(
                self.property_equality_estimate(label, property, value, input_rows)?
                    .rows,
            );
        }
        Some(rows.min(input_rows).max(1))
    }

    pub(in crate::graph) fn rel_property_equality_estimate(
        &self,
        rel_type: &str,
        property: &str,
        value: &Value,
        input_rows: u64,
    ) -> Option<PropertyCardinalityEstimate> {
        let histogram = self.rel_property_distribution(rel_type, property)?;
        let value = self.statistics_value(value);
        compatible(histogram, value).then(|| {
            histogram.equality(
                value,
                input_rows,
                self.known_rel_property_distinct_count(rel_type, property),
            )
        })
    }

    pub(in crate::graph) fn estimate_rel_property_value_rows(
        &self,
        rel_type: &str,
        property: &str,
        value: &Value,
        input_rows: u64,
    ) -> u64 {
        self.rel_property_equality_estimate(rel_type, property, value, input_rows)
            .map_or_else(
                || self.estimate_rel_property_eq_rows(rel_type, property, input_rows),
                |estimate| estimate.rows.max(1),
            )
    }

    pub(in crate::graph) fn estimate_property_not_value_rows(
        &self,
        label: &str,
        property: &str,
        value: &Value,
        input_rows: u64,
    ) -> u64 {
        if matches!(self.statistics_value(value), Value::Null) {
            return 1;
        }
        self.property_equality_estimate(label, property, value, input_rows)
            .map_or_else(
                || self.estimate_property_not_eq_rows(label, property, input_rows),
                |estimate| {
                    input_rows
                        .saturating_sub(
                            self.property_distribution(label, property)
                                .unwrap()
                                .nulls(input_rows)
                                .rows,
                        )
                        .saturating_sub(estimate.rows)
                        .max(1)
                },
            )
    }

    pub(in crate::graph) fn estimate_rel_property_not_value_rows(
        &self,
        rel_type: &str,
        property: &str,
        value: &Value,
        input_rows: u64,
    ) -> u64 {
        let value = self.statistics_value(value);
        if matches!(value, Value::Null) {
            return 1;
        }
        let Some(histogram) = self
            .rel_property_distribution(rel_type, property)
            .filter(|histogram| compatible(histogram, value))
        else {
            return self.estimate_rel_property_not_eq_rows(rel_type, property, input_rows);
        };
        input_rows
            .saturating_sub(histogram.nulls(input_rows).rows)
            .saturating_sub(
                self.estimate_rel_property_value_rows(rel_type, property, value, input_rows),
            )
            .max(1)
    }

    pub(in crate::graph) fn distribution_comparison_rows(
        &self,
        label: &str,
        property: &str,
        op: ComparisonOp,
        value: &Value,
        input_rows: u64,
        relationship: bool,
    ) -> Option<u64> {
        let (lower, upper) = match op {
            ComparisonOp::Lt => (None, Some((value, false))),
            ComparisonOp::Lte => (None, Some((value, true))),
            ComparisonOp::Gt => (Some((value, false)), None),
            ComparisonOp::Gte => (Some((value, true)), None),
        };
        self.distribution_range_rows(label, property, lower, upper, input_rows, relationship)
    }

    pub(in crate::graph) fn distribution_range_rows(
        &self,
        label: &str,
        property: &str,
        lower: Option<(&Value, bool)>,
        upper: Option<(&Value, bool)>,
        input_rows: u64,
        relationship: bool,
    ) -> Option<u64> {
        let histogram = if relationship {
            self.rel_property_distribution(label, property)?
        } else {
            self.property_distribution(label, property)?
        };
        let lower = lower.map(|(value, inclusive)| (self.statistics_value(value), inclusive));
        let upper = upper.map(|(value, inclusive)| (self.statistics_value(value), inclusive));
        for value in lower.into_iter().chain(upper).map(|(value, _)| value) {
            if !matches!(
                value,
                Value::Int(_)
                    | Value::Float(_)
                    | Value::String(_)
                    | Value::Date(_)
                    | Value::LocalTime(_)
                    | Value::LocalDateTime(_)
                    | Value::DateTime(_)
            ) || !compatible(histogram, value)
            {
                return None;
            }
        }
        Some(histogram.range(lower, upper, input_rows).rows.max(1))
    }
}

pub(super) fn compatible(histogram: &CompressedHistogram, value: &Value) -> bool {
    matches!(value, Value::Null)
        || histogram
            .most_common
            .iter()
            .map(|entry| &entry.value)
            .chain(
                histogram
                    .buckets
                    .iter()
                    .flat_map(|bucket| [&bucket.lower, &bucket.upper]),
            )
            .all(|sample| std::mem::discriminant(sample) == std::mem::discriminant(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_overrides_share_statistics_and_fixture_mutation_forks_the_snapshot() {
        let mut catalog = OptimizerCatalog::default();
        catalog.label_counts.insert("Memory".into(), 100);
        catalog.property_histograms.insert(
            ("Memory".into(), "space".into()),
            vec![Value::String("x".repeat(8192))],
        );
        let mut query = catalog
            .clone()
            .with_statistics_parameter_values(BTreeMap::new())
            .with_property_equality_row_count("Memory".into(), "space".into(), 2);
        assert!(std::sync::Arc::ptr_eq(
            &catalog.statistics,
            &query.statistics
        ));
        assert_eq!(
            catalog
                .property_histograms
                .values()
                .next()
                .unwrap()
                .as_ptr(),
            query.property_histograms.values().next().unwrap().as_ptr()
        );
        query.label_counts.insert("Memory".into(), 200);
        assert_eq!(catalog.label_count("Memory"), 100);
        assert_eq!(query.label_count("Memory"), 200);
        assert!(!std::sync::Arc::ptr_eq(
            &catalog.statistics,
            &query.statistics
        ));
    }

    #[test]
    fn shared_distribution_handles_skew_nulls_and_point_override() {
        let histogram = CompressedHistogram::from_sample(
            100,
            80,
            5,
            15,
            std::iter::repeat_n(Value::String("hot".into()), 70)
                .chain(std::iter::repeat_n(Value::String("cold".into()), 10))
                .collect(),
        )
        .unwrap();
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::default(),
            OptimizerCatalogStatistics {
                label_counts: BTreeMap::from([("Memory".into(), 100)]),
                ..OptimizerCatalogStatistics::default()
            }
            .with_distributions(
                [(("Memory".into(), "space".into()), histogram.clone())],
                [(("LINK".into(), "space".into()), histogram)],
            ),
        );
        let hot = Value::String("hot".into());
        let cold = Value::String("cold".into());
        assert_eq!(
            catalog.estimate_property_value_rows("Memory", "space", &hot, 100),
            70
        );
        assert_eq!(
            catalog.estimate_property_index_value_rows("Memory", "space", &cold),
            10
        );
        assert_eq!(
            catalog.distribution_in_rows(
                "Memory",
                "space",
                &[cold.clone(), cold.clone(), Value::Null],
                100
            ),
            Some(10)
        );
        assert_eq!(
            catalog.estimate_property_not_value_rows("Memory", "space", &hot, 100),
            10
        );
        assert_eq!(
            catalog.estimate_property_not_value_rows("Memory", "space", &Value::Null, 100),
            1
        );
        assert_eq!(
            catalog.estimate_rel_property_value_rows("LINK", "space", &hot, 100),
            70
        );
        assert_eq!(
            catalog.estimate_rel_property_not_value_rows("LINK", "space", &hot, 100),
            10
        );
        assert_eq!(
            catalog.estimate_rel_property_in_rows("LINK", "space", &[cold.clone(), cold], 100),
            10
        );
        let local = catalog.with_property_equality_row_count("Memory".into(), "space".into(), 3);
        assert_eq!(
            local.estimate_property_index_value_rows("Memory", "space", &hot),
            3
        );
    }
}
