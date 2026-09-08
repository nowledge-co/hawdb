use super::value_range::{compare_histogram_value, range_bound_matches, ValueRangeBound};
use crate::cardinality_defaults::{
    NULL_SELECTIVITY_DIVISOR_CAP, RANGE_SELECTIVITY_DIVISOR, SAMPLED_HISTOGRAM_MATCH_PSEUDOCOUNT,
    SAMPLED_HISTOGRAM_TOTAL_PSEUDOCOUNT,
};
use skein_core::Value;
use skein_plan::ComparisonOp;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ExpandEstimate {
    pub(super) path_count: Option<u64>,
    pub(super) rel_count: u64,
    pub(super) source_count: u64,
    pub(super) average_fanout: u64,
    pub(super) property_distinct_product: u64,
    pub(super) estimated_rows: u64,
    pub(super) hop_estimates: Vec<HopEstimate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct HopEstimate {
    pub(super) hop: usize,
    pub(super) rows: u64,
    pub(super) exact: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OptimizerIndexStatistics {
    pub index_size: u64,
    pub distinct_count: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct OptimizerCatalog {
    pub(super) assume_all_indexes: bool,
    pub(super) equality_property_indexes: BTreeSet<(String, String)>,
    pub(super) composite_property_indexes: BTreeSet<(String, Vec<String>)>,
    pub(super) range_property_indexes: BTreeSet<(String, String)>,
    pub(super) full_text_property_indexes: BTreeSet<(String, String)>,
    pub(super) label_counts: BTreeMap<String, u64>,
    pub(super) rel_type_counts: BTreeMap<String, u64>,
    pub(super) rel_type_source_counts: BTreeMap<String, u64>,
    pub(super) rel_type_target_counts: BTreeMap<String, u64>,
    pub(super) path_counts: BTreeMap<(String, String, String), u64>,
    pub(super) path_source_distinct_counts: BTreeMap<(String, String, String), u64>,
    pub(super) path_target_distinct_counts: BTreeMap<(String, String, String), u64>,
    pub(super) bounded_path_counts: BTreeMap<(String, String, String, usize), u64>,
    pub(super) bounded_path_source_distinct_counts: BTreeMap<(String, String, String, usize), u64>,
    pub(super) bounded_path_target_distinct_counts: BTreeMap<(String, String, String, usize), u64>,
    pub(super) property_index_statistics: BTreeMap<(String, String), OptimizerIndexStatistics>,
    pub(super) composite_index_statistics:
        BTreeMap<(String, Vec<String>), OptimizerIndexStatistics>,
    pub(super) property_distinct_counts: BTreeMap<(String, String), u64>,
    pub(super) rel_property_distinct_counts: BTreeMap<(String, String), u64>,
    pub(super) property_histograms: BTreeMap<(String, String), Vec<Value>>,
    pub(super) rel_property_histograms: BTreeMap<(String, String), Vec<Value>>,
    pub(super) sampled_property_histograms: BTreeMap<(String, String), bool>,
    pub(super) sampled_rel_property_histograms: BTreeMap<(String, String), bool>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct OptimizerCatalogIndexes {
    pub(super) equality_property_indexes: BTreeSet<(String, String)>,
    pub(super) composite_property_indexes: BTreeSet<(String, Vec<String>)>,
    pub(super) range_property_indexes: BTreeSet<(String, String)>,
    pub(super) full_text_property_indexes: BTreeSet<(String, String)>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct OptimizerCatalogStatistics {
    pub(super) label_counts: BTreeMap<String, u64>,
    pub(super) rel_type_counts: BTreeMap<String, u64>,
    pub(super) rel_type_source_counts: BTreeMap<String, u64>,
    pub(super) rel_type_target_counts: BTreeMap<String, u64>,
    pub(super) path_counts: BTreeMap<(String, String, String), u64>,
    pub(super) path_source_distinct_counts: BTreeMap<(String, String, String), u64>,
    pub(super) path_target_distinct_counts: BTreeMap<(String, String, String), u64>,
    pub(super) bounded_path_counts: BTreeMap<(String, String, String, usize), u64>,
    pub(super) bounded_path_source_distinct_counts: BTreeMap<(String, String, String, usize), u64>,
    pub(super) bounded_path_target_distinct_counts: BTreeMap<(String, String, String, usize), u64>,
    pub(super) property_index_statistics: BTreeMap<(String, String), OptimizerIndexStatistics>,
    pub(super) composite_index_statistics:
        BTreeMap<(String, Vec<String>), OptimizerIndexStatistics>,
    pub(super) property_distinct_counts: BTreeMap<(String, String), u64>,
    pub(super) rel_property_distinct_counts: BTreeMap<(String, String), u64>,
    pub(super) property_histograms: BTreeMap<(String, String), Vec<Value>>,
    pub(super) rel_property_histograms: BTreeMap<(String, String), Vec<Value>>,
    pub(super) sampled_property_histograms: BTreeMap<(String, String), bool>,
    pub(super) sampled_rel_property_histograms: BTreeMap<(String, String), bool>,
}

impl OptimizerCatalog {
    pub fn new(indexes: OptimizerCatalogIndexes, statistics: OptimizerCatalogStatistics) -> Self {
        Self {
            assume_all_indexes: false,
            equality_property_indexes: indexes.equality_property_indexes,
            composite_property_indexes: indexes.composite_property_indexes,
            range_property_indexes: indexes.range_property_indexes,
            full_text_property_indexes: indexes.full_text_property_indexes,
            label_counts: statistics.label_counts,
            rel_type_counts: statistics.rel_type_counts,
            rel_type_source_counts: statistics.rel_type_source_counts,
            rel_type_target_counts: statistics.rel_type_target_counts,
            path_counts: statistics.path_counts,
            path_source_distinct_counts: statistics.path_source_distinct_counts,
            path_target_distinct_counts: statistics.path_target_distinct_counts,
            bounded_path_counts: statistics.bounded_path_counts,
            bounded_path_source_distinct_counts: statistics.bounded_path_source_distinct_counts,
            bounded_path_target_distinct_counts: statistics.bounded_path_target_distinct_counts,
            property_index_statistics: statistics.property_index_statistics,
            composite_index_statistics: statistics.composite_index_statistics,
            property_distinct_counts: statistics.property_distinct_counts,
            rel_property_distinct_counts: statistics.rel_property_distinct_counts,
            property_histograms: statistics.property_histograms,
            rel_property_histograms: statistics.rel_property_histograms,
            sampled_property_histograms: statistics.sampled_property_histograms,
            sampled_rel_property_histograms: statistics.sampled_rel_property_histograms,
        }
    }

    pub(super) fn optimistic() -> Self {
        Self {
            assume_all_indexes: true,
            ..Self::default()
        }
    }

    pub(super) fn has_property_index(&self, label: &str, property: &str) -> bool {
        self.assume_all_indexes
            || self
                .equality_property_indexes
                .contains(&(label.to_string(), property.to_string()))
    }

    pub(super) fn has_range_property_index(&self, label: &str, property: &str) -> bool {
        self.assume_all_indexes
            || self
                .range_property_indexes
                .contains(&(label.to_string(), property.to_string()))
    }

    pub(super) fn has_full_text_property_index(&self, label: &str, property: &str) -> bool {
        self.assume_all_indexes
            || self
                .full_text_property_indexes
                .contains(&(label.to_string(), property.to_string()))
    }

    pub(super) fn has_composite_property_index(&self, label: &str, properties: &[String]) -> bool {
        self.assume_all_indexes
            || self
                .composite_property_indexes
                .contains(&(label.to_string(), properties.to_vec()))
    }

    pub(super) fn composite_property_indexes_for_label(&self, label: &str) -> Vec<Vec<String>> {
        if self.assume_all_indexes {
            return Vec::new();
        }
        self.composite_property_indexes
            .iter()
            .filter_map(|(candidate_label, properties)| {
                if candidate_label == label {
                    Some(properties.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub(super) fn label_count(&self, label: &str) -> u64 {
        self.label_counts.get(label).copied().unwrap_or(1).max(1)
    }

    pub(super) fn relationship_count(&self, rel_type: &str) -> u64 {
        self.rel_type_counts
            .get(rel_type)
            .copied()
            .unwrap_or(1)
            .max(1)
    }

    pub(super) fn path_source_distinct_count(
        &self,
        source_label: &str,
        rel_type: &str,
        target_label: &str,
    ) -> Option<u64> {
        self.path_source_distinct_counts
            .get(&(
                source_label.to_string(),
                rel_type.to_string(),
                target_label.to_string(),
            ))
            .copied()
    }

    pub(super) fn path_target_distinct_count(
        &self,
        source_label: &str,
        rel_type: &str,
        target_label: &str,
    ) -> Option<u64> {
        self.path_target_distinct_counts
            .get(&(
                source_label.to_string(),
                rel_type.to_string(),
                target_label.to_string(),
            ))
            .copied()
    }

    pub(super) fn bounded_path_source_distinct_count(
        &self,
        source_label: &str,
        rel_type: &str,
        target_label: &str,
        min_hops: usize,
        max_hops: usize,
    ) -> Option<u64> {
        self.bounded_path_distinct_count(
            &self.bounded_path_source_distinct_counts,
            source_label,
            rel_type,
            target_label,
            min_hops,
            max_hops,
        )
    }

    pub(super) fn bounded_path_target_distinct_count(
        &self,
        source_label: &str,
        rel_type: &str,
        target_label: &str,
        min_hops: usize,
        max_hops: usize,
    ) -> Option<u64> {
        self.bounded_path_distinct_count(
            &self.bounded_path_target_distinct_counts,
            source_label,
            rel_type,
            target_label,
            min_hops,
            max_hops,
        )
    }

    pub(super) fn bounded_path_distinct_count(
        &self,
        counts: &BTreeMap<(String, String, String, usize), u64>,
        source_label: &str,
        rel_type: &str,
        target_label: &str,
        min_hops: usize,
        max_hops: usize,
    ) -> Option<u64> {
        let mut total = 0_u64;
        let mut found = false;
        for hop in min_hops..=max_hops.max(min_hops) {
            if let Some(count) = counts.get(&(
                source_label.to_string(),
                rel_type.to_string(),
                target_label.to_string(),
                hop,
            )) {
                found = true;
                total = total.saturating_add(*count);
            }
        }
        found.then_some(total.max(1))
    }

    pub(super) fn distinct_count(&self, label: &str, property: &str) -> u64 {
        self.property_index_statistics
            .get(&(label.to_string(), property.to_string()))
            .map(|statistics| statistics.distinct_count)
            .or_else(|| {
                self.property_distinct_counts
                    .get(&(label.to_string(), property.to_string()))
                    .copied()
            })
            .unwrap_or_else(|| self.label_count(label).max(1))
    }

    pub(super) fn estimate_property_index_eq_rows(&self, label: &str, property: &str) -> u64 {
        self.property_index_statistics
            .get(&(label.to_string(), property.to_string()))
            .map(|statistics| {
                statistics
                    .index_size
                    .div_ceil(statistics.distinct_count.max(1))
                    .min(self.label_count(label))
                    .max(1)
            })
            .unwrap_or_else(|| {
                self.label_count(label)
                    .div_ceil(self.distinct_count(label, property).max(1))
                    .max(1)
            })
    }

    pub(super) fn estimate_property_index_in_rows(
        &self,
        label: &str,
        property: &str,
        value_count: u64,
    ) -> u64 {
        let upper_bound = self
            .property_index_statistics
            .get(&(label.to_string(), property.to_string()))
            .map_or_else(
                || self.label_count(label),
                |statistics| statistics.index_size.min(self.label_count(label)),
            );
        self.estimate_property_index_eq_rows(label, property)
            .saturating_mul(value_count)
            .min(upper_bound)
            .max(1)
    }

    pub(super) fn composite_distinct_count(&self, label: &str, properties: &[String]) -> u64 {
        self.composite_index_statistics
            .get(&(label.to_string(), properties.to_vec()))
            .map(|statistics| statistics.distinct_count)
            .unwrap_or_else(|| {
                properties
                    .iter()
                    .map(|property| self.distinct_count(label, property).max(1))
                    .fold(1_u64, |product, distinct| product.saturating_mul(distinct))
                    .max(1)
            })
    }

    pub(super) fn estimate_composite_property_index_rows(
        &self,
        label: &str,
        properties: &[String],
    ) -> u64 {
        self.composite_index_statistics
            .get(&(label.to_string(), properties.to_vec()))
            .map(|statistics| {
                statistics
                    .index_size
                    .div_ceil(statistics.distinct_count.max(1))
                    .min(self.label_count(label))
                    .max(1)
            })
            .unwrap_or_else(|| {
                self.label_count(label)
                    .div_ceil(self.composite_distinct_count(label, properties))
                    .max(1)
            })
    }

    pub(super) fn estimate_composite_prefix_range_rows(
        &self,
        label: &str,
        index_properties: &[String],
        equality_properties: &[String],
        range_property: &str,
        lower: Option<&ValueRangeBound>,
        upper: Option<&ValueRangeBound>,
    ) -> u64 {
        let label_count = self.label_count(label);
        let equality_distinct = equality_properties
            .iter()
            .map(|property| self.distinct_count(label, property).max(1))
            .fold(1u64, u64::saturating_mul);
        let equality_rows = label_count.div_ceil(equality_distinct).max(1);
        let range_rows = self.estimate_range_bounds_rows(label, range_property, lower, upper);
        let estimated = equality_rows
            .saturating_mul(range_rows)
            .div_ceil(label_count)
            .max(1);
        self.composite_index_statistics
            .get(&(label.to_string(), index_properties.to_vec()))
            .map_or(estimated, |statistics| {
                estimated.min(statistics.index_size.max(1))
            })
    }

    pub(super) fn known_distinct_count(&self, label: &str, property: &str) -> Option<u64> {
        self.property_index_statistics
            .get(&(label.to_string(), property.to_string()))
            .map(|statistics| statistics.distinct_count)
            .or_else(|| {
                self.property_distinct_counts
                    .get(&(label.to_string(), property.to_string()))
                    .copied()
            })
    }

    pub(super) fn known_rel_property_distinct_count(
        &self,
        rel_type: &str,
        property: &str,
    ) -> Option<u64> {
        self.rel_property_distinct_counts
            .get(&(rel_type.to_string(), property.to_string()))
            .copied()
    }

    pub(super) fn rel_property_distinct_count(&self, rel_type: &str, property: &str) -> u64 {
        self.rel_property_distinct_counts
            .get(&(rel_type.to_string(), property.to_string()))
            .copied()
            .unwrap_or_else(|| {
                self.rel_type_counts
                    .get(rel_type)
                    .copied()
                    .unwrap_or(1)
                    .max(1)
            })
    }

    pub(super) fn estimate_rel_property_eq_rows(
        &self,
        rel_type: &str,
        property: &str,
        input_rows: u64,
    ) -> u64 {
        input_rows
            .div_ceil(self.rel_property_distinct_count(rel_type, property).max(1))
            .max(1)
    }

    pub(super) fn estimate_rel_property_not_eq_rows(
        &self,
        rel_type: &str,
        property: &str,
        input_rows: u64,
    ) -> u64 {
        input_rows
            .saturating_sub(self.estimate_rel_property_eq_rows(rel_type, property, input_rows))
    }

    pub(super) fn estimate_rel_property_in_rows(
        &self,
        rel_type: &str,
        property: &str,
        values: &[Value],
        input_rows: u64,
    ) -> u64 {
        if values.is_empty() {
            return 0;
        }
        let distinct_count = self.rel_property_distinct_count(rel_type, property).max(1);
        let value_count = values
            .iter()
            .collect::<BTreeSet<_>>()
            .len()
            .min(distinct_count as usize) as u64;
        input_rows
            .saturating_mul(value_count)
            .div_ceil(distinct_count)
            .max(1)
    }

    pub(super) fn estimate_rel_property_string_match_rows(
        &self,
        rel_type: &str,
        property: &str,
        input_rows: u64,
        max_divisor: u64,
    ) -> u64 {
        let divisor = self
            .rel_property_distinct_count(rel_type, property)
            .max(1)
            .min(max_divisor)
            .max(1);
        input_rows.div_ceil(divisor).max(1)
    }

    pub(super) fn estimate_rel_property_null_rows(
        &self,
        rel_type: &str,
        property: &str,
        input_rows: u64,
    ) -> u64 {
        let distinct_count = self.rel_property_distinct_count(rel_type, property).max(1);
        input_rows
            .div_ceil(distinct_count.min(NULL_SELECTIVITY_DIVISOR_CAP))
            .max(1)
    }

    pub(super) fn estimate_rel_property_not_null_rows(
        &self,
        rel_type: &str,
        property: &str,
        input_rows: u64,
    ) -> u64 {
        input_rows
            .saturating_sub(self.estimate_rel_property_null_rows(rel_type, property, input_rows))
            .max(1)
    }

    pub(super) fn estimate_rel_property_range_rows(
        &self,
        rel_type: &str,
        property: &str,
        op: ComparisonOp,
        value: &Value,
        input_rows: u64,
    ) -> u64 {
        let key = (rel_type.to_string(), property.to_string());
        let Some(histogram) = self
            .rel_property_histograms
            .get(&key)
            .filter(|values| !values.is_empty())
        else {
            return input_rows.div_ceil(RANGE_SELECTIVITY_DIVISOR).max(1);
        };
        let matching_values = histogram
            .iter()
            .filter(|candidate| compare_histogram_value(candidate, op, value))
            .count() as u64;
        estimate_histogram_rows(
            input_rows,
            matching_values,
            histogram.len() as u64,
            self.sampled_rel_property_histograms
                .get(&key)
                .copied()
                .unwrap_or(false),
        )
    }

    pub(super) fn estimate_range_rows(
        &self,
        label: &str,
        property: &str,
        op: ComparisonOp,
        value: &Value,
    ) -> u64 {
        let label_count = self.label_count(label);
        let key = (label.to_string(), property.to_string());
        let Some(histogram) = self
            .property_histograms
            .get(&key)
            .filter(|values| !values.is_empty())
        else {
            return label_count.div_ceil(RANGE_SELECTIVITY_DIVISOR).max(1);
        };
        let matching_values = histogram
            .iter()
            .filter(|candidate| compare_histogram_value(candidate, op, value))
            .count() as u64;
        estimate_histogram_rows(
            label_count,
            matching_values,
            histogram.len() as u64,
            self.sampled_property_histograms
                .get(&key)
                .copied()
                .unwrap_or(false),
        )
    }

    pub(super) fn estimate_property_eq_rows(
        &self,
        label: &str,
        property: &str,
        input_rows: u64,
    ) -> u64 {
        input_rows
            .div_ceil(self.distinct_count(label, property).max(1))
            .max(1)
    }

    pub(super) fn estimate_property_not_eq_rows(
        &self,
        label: &str,
        property: &str,
        input_rows: u64,
    ) -> u64 {
        input_rows.saturating_sub(self.estimate_property_eq_rows(label, property, input_rows))
    }

    pub(super) fn estimate_property_in_rows(
        &self,
        label: &str,
        property: &str,
        values: &[Value],
        input_rows: u64,
    ) -> u64 {
        if values.is_empty() {
            return 0;
        }
        let distinct_count = self.distinct_count(label, property).max(1);
        let value_count = values
            .iter()
            .collect::<BTreeSet<_>>()
            .len()
            .min(distinct_count as usize) as u64;
        input_rows
            .saturating_mul(value_count)
            .div_ceil(distinct_count)
            .max(1)
    }

    pub(super) fn estimate_property_string_match_rows(
        &self,
        label: &str,
        property: &str,
        input_rows: u64,
        max_divisor: u64,
    ) -> u64 {
        let divisor = self
            .distinct_count(label, property)
            .max(1)
            .min(max_divisor)
            .max(1);
        input_rows.div_ceil(divisor).max(1)
    }

    pub(super) fn estimate_property_null_rows(
        &self,
        label: &str,
        property: &str,
        input_rows: u64,
    ) -> u64 {
        let distinct_count = self.distinct_count(label, property).max(1);
        input_rows
            .div_ceil(distinct_count.min(NULL_SELECTIVITY_DIVISOR_CAP))
            .max(1)
    }

    pub(super) fn estimate_property_not_null_rows(
        &self,
        label: &str,
        property: &str,
        input_rows: u64,
    ) -> u64 {
        input_rows
            .saturating_sub(self.estimate_property_null_rows(label, property, input_rows))
            .max(1)
    }

    pub(super) fn estimate_property_range_rows(
        &self,
        label: &str,
        property: &str,
        op: ComparisonOp,
        value: &Value,
        input_rows: u64,
    ) -> u64 {
        let key = (label.to_string(), property.to_string());
        let Some(histogram) = self
            .property_histograms
            .get(&key)
            .filter(|values| !values.is_empty())
        else {
            return input_rows.div_ceil(RANGE_SELECTIVITY_DIVISOR).max(1);
        };
        let matching_values = histogram
            .iter()
            .filter(|candidate| compare_histogram_value(candidate, op, value))
            .count() as u64;
        estimate_histogram_rows(
            input_rows,
            matching_values,
            histogram.len() as u64,
            self.sampled_property_histograms
                .get(&key)
                .copied()
                .unwrap_or(false),
        )
    }

    pub(super) fn estimate_range_bounds_rows(
        &self,
        label: &str,
        property: &str,
        lower: Option<&ValueRangeBound>,
        upper: Option<&ValueRangeBound>,
    ) -> u64 {
        let label_count = self.label_count(label);
        let key = (label.to_string(), property.to_string());
        let Some(histogram) = self
            .property_histograms
            .get(&key)
            .filter(|values| !values.is_empty())
        else {
            return label_count.div_ceil(RANGE_SELECTIVITY_DIVISOR).max(1);
        };
        let matching_values = histogram
            .iter()
            .filter(|candidate| range_bound_matches(candidate, lower, upper))
            .count() as u64;
        estimate_histogram_rows(
            label_count,
            matching_values,
            histogram.len() as u64,
            self.sampled_property_histograms
                .get(&key)
                .copied()
                .unwrap_or(false),
        )
    }

    pub(super) fn estimate_expand_rows(
        &self,
        source_label: &str,
        rel_type: &str,
        rel_properties: &BTreeMap<String, Value>,
        target_label: &str,
        min_hops: usize,
        max_hops: usize,
    ) -> ExpandEstimate {
        let path_count = self
            .path_counts
            .get(&(
                source_label.to_string(),
                rel_type.to_string(),
                target_label.to_string(),
            ))
            .copied();
        let rel_count = self.rel_type_counts.get(rel_type).copied().unwrap_or(1);
        let source_count = self
            .rel_type_source_counts
            .get(rel_type)
            .copied()
            .unwrap_or_else(|| self.label_count(source_label).max(1));
        let average_fanout = rel_count.div_ceil(source_count.max(1)).max(1);
        let one_hop = path_count.unwrap_or_else(|| {
            self.label_count(source_label)
                .min(self.label_count(target_label))
                .max(1)
        });
        let property_distinct_product = rel_properties
            .keys()
            .map(|property| self.rel_property_distinct_count(rel_type, property).max(1))
            .fold(1_u64, |acc, value| acc.saturating_mul(value))
            .max(1);
        let mut estimated_rows = 0_u64;
        let mut hop_rows = one_hop.max(1);
        let mut hop_estimates = Vec::new();
        for hop in 1..=max_hops.max(1) {
            let exact_hop_rows = self
                .bounded_path_counts
                .get(&(
                    source_label.to_string(),
                    rel_type.to_string(),
                    target_label.to_string(),
                    hop,
                ))
                .copied();
            let current_hop_rows = exact_hop_rows
                .unwrap_or(hop_rows)
                .div_ceil(property_distinct_product)
                .max(1);
            hop_estimates.push(HopEstimate {
                hop,
                rows: current_hop_rows,
                exact: exact_hop_rows.is_some(),
            });
            if hop >= min_hops {
                estimated_rows = estimated_rows.saturating_add(current_hop_rows);
            }
            hop_rows = current_hop_rows.max(1).saturating_mul(average_fanout);
        }
        ExpandEstimate {
            path_count,
            rel_count,
            source_count,
            average_fanout,
            property_distinct_product,
            estimated_rows: estimated_rows.max(1),
            hop_estimates,
        }
    }
}

fn estimate_histogram_rows(
    input_rows: u64,
    matching_values: u64,
    histogram_values: u64,
    sampled: bool,
) -> u64 {
    // A sampled CDF is finite evidence rather than an exhaustive rank table. Add-one
    // smoothing shrinks it toward the existing 50% fallback by a weight determined by
    // the actual sample size, while leaving complete histograms exact.
    let (matching_values, histogram_values) = if sampled {
        (
            matching_values.saturating_add(SAMPLED_HISTOGRAM_MATCH_PSEUDOCOUNT),
            histogram_values.saturating_add(SAMPLED_HISTOGRAM_TOTAL_PSEUDOCOUNT),
        )
    } else {
        (matching_values, histogram_values)
    };
    input_rows
        .saturating_mul(matching_values)
        .div_ceil(histogram_values.max(1))
        .max(1)
}

impl OptimizerCatalogIndexes {
    pub fn new(
        equality_property_indexes: impl IntoIterator<Item = (String, String)>,
        composite_property_indexes: impl IntoIterator<Item = (String, Vec<String>)>,
        range_property_indexes: impl IntoIterator<Item = (String, String)>,
        full_text_property_indexes: impl IntoIterator<Item = (String, String)>,
    ) -> Self {
        Self {
            equality_property_indexes: equality_property_indexes.into_iter().collect(),
            composite_property_indexes: composite_property_indexes.into_iter().collect(),
            range_property_indexes: range_property_indexes.into_iter().collect(),
            full_text_property_indexes: full_text_property_indexes.into_iter().collect(),
        }
    }
}

impl OptimizerCatalogStatistics {
    pub fn new(
        label_counts: impl IntoIterator<Item = (String, u64)>,
        rel_type_counts: impl IntoIterator<Item = (String, u64)>,
        rel_type_source_counts: impl IntoIterator<Item = (String, u64)>,
        path_counts: impl IntoIterator<Item = ((String, String, String), u64)>,
        bounded_path_counts: impl IntoIterator<Item = ((String, String, String, usize), u64)>,
        property_distinct_counts: impl IntoIterator<Item = ((String, String), u64)>,
        property_histograms: impl IntoIterator<Item = ((String, String), Vec<Value>)>,
    ) -> Self {
        Self {
            label_counts: label_counts.into_iter().collect(),
            rel_type_counts: rel_type_counts.into_iter().collect(),
            rel_type_source_counts: rel_type_source_counts.into_iter().collect(),
            rel_type_target_counts: BTreeMap::new(),
            path_counts: path_counts.into_iter().collect(),
            path_source_distinct_counts: BTreeMap::new(),
            path_target_distinct_counts: BTreeMap::new(),
            bounded_path_counts: bounded_path_counts.into_iter().collect(),
            bounded_path_source_distinct_counts: BTreeMap::new(),
            bounded_path_target_distinct_counts: BTreeMap::new(),
            property_index_statistics: BTreeMap::new(),
            composite_index_statistics: BTreeMap::new(),
            property_distinct_counts: property_distinct_counts.into_iter().collect(),
            rel_property_distinct_counts: BTreeMap::new(),
            property_histograms: property_histograms.into_iter().collect(),
            rel_property_histograms: BTreeMap::new(),
            sampled_property_histograms: BTreeMap::new(),
            sampled_rel_property_histograms: BTreeMap::new(),
        }
    }

    pub fn with_property_index_statistics(
        mut self,
        statistics: impl IntoIterator<Item = ((String, String), OptimizerIndexStatistics)>,
    ) -> Self {
        self.property_index_statistics = statistics.into_iter().collect();
        self
    }

    pub fn with_composite_index_statistics(
        mut self,
        statistics: impl IntoIterator<Item = ((String, Vec<String>), OptimizerIndexStatistics)>,
    ) -> Self {
        self.composite_index_statistics = statistics.into_iter().collect();
        self
    }

    pub fn with_relationship_property_distinct_counts(
        mut self,
        rel_property_distinct_counts: impl IntoIterator<Item = ((String, String), u64)>,
    ) -> Self {
        self.rel_property_distinct_counts = rel_property_distinct_counts.into_iter().collect();
        self
    }

    pub fn with_relationship_type_target_counts(
        mut self,
        rel_type_target_counts: impl IntoIterator<Item = (String, u64)>,
    ) -> Self {
        self.rel_type_target_counts = rel_type_target_counts.into_iter().collect();
        self
    }

    pub fn with_path_source_distinct_counts(
        mut self,
        path_source_distinct_counts: impl IntoIterator<Item = ((String, String, String), u64)>,
    ) -> Self {
        self.path_source_distinct_counts = path_source_distinct_counts.into_iter().collect();
        self
    }

    pub fn with_path_target_distinct_counts(
        mut self,
        path_target_distinct_counts: impl IntoIterator<Item = ((String, String, String), u64)>,
    ) -> Self {
        self.path_target_distinct_counts = path_target_distinct_counts.into_iter().collect();
        self
    }

    pub fn with_bounded_path_source_distinct_counts(
        mut self,
        bounded_path_source_distinct_counts: impl IntoIterator<
            Item = ((String, String, String, usize), u64),
        >,
    ) -> Self {
        self.bounded_path_source_distinct_counts =
            bounded_path_source_distinct_counts.into_iter().collect();
        self
    }

    pub fn with_bounded_path_target_distinct_counts(
        mut self,
        bounded_path_target_distinct_counts: impl IntoIterator<
            Item = ((String, String, String, usize), u64),
        >,
    ) -> Self {
        self.bounded_path_target_distinct_counts =
            bounded_path_target_distinct_counts.into_iter().collect();
        self
    }

    pub fn with_relationship_property_histograms(
        mut self,
        rel_property_histograms: impl IntoIterator<Item = ((String, String), Vec<Value>)>,
    ) -> Self {
        self.rel_property_histograms = rel_property_histograms.into_iter().collect();
        self
    }

    pub fn with_sampled_property_histograms(
        mut self,
        sampled_property_histograms: impl IntoIterator<Item = ((String, String), bool)>,
    ) -> Self {
        self.sampled_property_histograms = sampled_property_histograms.into_iter().collect();
        self
    }

    pub fn with_sampled_relationship_property_histograms(
        mut self,
        sampled_rel_property_histograms: impl IntoIterator<Item = ((String, String), bool)>,
    ) -> Self {
        self.sampled_rel_property_histograms =
            sampled_rel_property_histograms.into_iter().collect();
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_cardinality_honors_half_open_histogram_bounds() {
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::default(),
            OptimizerCatalogStatistics::new(
                [("Memory".to_string(), 1_000)],
                [],
                [],
                [],
                [],
                [],
                [(
                    ("Memory".to_string(), "created_at".to_string()),
                    (0..100).map(Value::Int).collect::<Vec<_>>(),
                )],
            ),
        );

        assert_eq!(
            catalog.estimate_range_bounds_rows(
                "Memory",
                "created_at",
                Some(&(Value::Int(10), true)),
                Some(&(Value::Int(20), false)),
            ),
            100
        );
    }

    #[test]
    fn sampled_histograms_shrink_range_selectivity_toward_default() {
        let property_key = ("Memory".to_string(), "score".to_string());
        let rel_property_key = ("MENTIONS".to_string(), "score".to_string());
        let histogram = (0..100).map(Value::Int).collect::<Vec<_>>();
        let statistics = OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 1_000)],
            [],
            [],
            [],
            [],
            [(property_key.clone(), 100)],
            [(property_key.clone(), histogram.clone())],
        )
        .with_relationship_property_histograms([(rel_property_key.clone(), histogram)]);
        let exhaustive =
            OptimizerCatalog::new(OptimizerCatalogIndexes::default(), statistics.clone());
        let sampled = OptimizerCatalog::new(
            OptimizerCatalogIndexes::default(),
            statistics
                .with_sampled_property_histograms([(property_key, true)])
                .with_sampled_relationship_property_histograms([(rel_property_key, true)]),
        );

        assert_eq!(
            exhaustive.estimate_range_rows("Memory", "score", ComparisonOp::Lt, &Value::Int(10)),
            100
        );
        assert_eq!(
            sampled.estimate_range_rows("Memory", "score", ComparisonOp::Lt, &Value::Int(10)),
            108
        );
        assert_eq!(
            sampled.estimate_property_range_rows(
                "Memory",
                "score",
                ComparisonOp::Lt,
                &Value::Int(10),
                1_000,
            ),
            108
        );
        assert_eq!(
            sampled.estimate_range_bounds_rows(
                "Memory",
                "score",
                None,
                Some(&(Value::Int(10), false)),
            ),
            108
        );
        assert_eq!(
            exhaustive.estimate_rel_property_range_rows(
                "MENTIONS",
                "score",
                ComparisonOp::Lt,
                &Value::Int(10),
                1_000,
            ),
            100
        );
        assert_eq!(
            sampled.estimate_rel_property_range_rows(
                "MENTIONS",
                "score",
                ComparisonOp::Lt,
                &Value::Int(10),
                1_000,
            ),
            108
        );
    }

    #[test]
    fn index_cardinality_uses_sparse_index_size_and_joint_ndv() {
        let statistics = OptimizerCatalogStatistics {
            label_counts: BTreeMap::from([("Memory".to_string(), 100)]),
            property_index_statistics: BTreeMap::from([(
                ("Memory".to_string(), "kind".to_string()),
                OptimizerIndexStatistics {
                    index_size: 10,
                    distinct_count: 2,
                },
            )]),
            composite_index_statistics: BTreeMap::from([(
                (
                    "Memory".to_string(),
                    vec!["kind".to_string(), "source_id".to_string()],
                ),
                OptimizerIndexStatistics {
                    index_size: 8,
                    distinct_count: 4,
                },
            )]),
            ..OptimizerCatalogStatistics::default()
        };
        let catalog = OptimizerCatalog::new(OptimizerCatalogIndexes::default(), statistics);

        assert_eq!(catalog.estimate_property_index_eq_rows("Memory", "kind"), 5);
        assert_eq!(
            catalog.estimate_property_index_in_rows("Memory", "kind", 10),
            10
        );
        assert_eq!(
            catalog.estimate_composite_property_index_rows(
                "Memory",
                &["kind".to_string(), "source_id".to_string()],
            ),
            2
        );
    }
}
