// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Bounded unordered key-set views of existing, snapshot-local index samples.
//! These are statistical estimates, never uniqueness or non-NULL proofs.

use super::{OptimizerCatalog, OptimizerIndexStatistics};
use std::collections::{BTreeMap, BTreeSet};

pub(in crate::graph) const MAX_JOINT_ARITY: usize = 4;
const MAX_JOINT_SETS_PER_LABEL: usize = 32;

pub(super) type Statistics = BTreeMap<String, Vec<JointEntry>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct JointEntry {
    properties: Vec<String>,
    statistics: Option<OptimizerIndexStatistics>,
}

pub(in crate::graph) struct JointEqualityEstimate {
    pub(in crate::graph) rows: u64,
    pub(in crate::graph) population: u64,
    pub(in crate::graph) indexed: u64,
    pub(in crate::graph) ndv: u64,
}

pub(super) fn canonical_statistics(
    source: &BTreeMap<(String, Vec<String>), OptimizerIndexStatistics>,
    indexes: &BTreeSet<(String, Vec<String>)>,
) -> Statistics {
    let mut result = Statistics::new();
    for ((label, properties), statistics) in source {
        if !indexes.contains(&(label.clone(), properties.clone())) {
            continue;
        }
        if !(2..=MAX_JOINT_ARITY).contains(&properties.len()) {
            continue;
        }
        let original_len = properties.len();
        let mut properties = properties.clone();
        properties.sort_unstable();
        properties.dedup();
        if properties.len() != original_len {
            continue;
        }
        let entries = result.entry(label.clone()).or_default();
        if let Some(existing) = entries
            .iter_mut()
            .find(|entry| entry.properties == properties)
        {
            // Differently ordered indexes can have distinct sample epochs or
            // estimates. Do not choose one silently for the same key set.
            if existing.statistics != Some(*statistics) {
                existing.statistics = None;
            }
        } else if entries.len() < MAX_JOINT_SETS_PER_LABEL {
            entries.push(JointEntry {
                properties,
                statistics: Some(*statistics),
            });
        }
    }
    for entries in result.values_mut() {
        entries.sort_unstable_by(|left, right| left.properties.cmp(&right.properties));
    }
    result
}

impl OptimizerCatalog {
    pub(in crate::graph) fn has_joint_statistics(&self) -> bool {
        !self.joint_statistics.is_empty()
    }

    pub(in crate::graph) fn joint_statistics(
        &self,
        label: &str,
        properties: &[&str],
    ) -> Option<OptimizerIndexStatistics> {
        if !(2..=MAX_JOINT_ARITY).contains(&properties.len()) {
            return None;
        }
        let mut keys = [""; MAX_JOINT_ARITY];
        keys[..properties.len()].copy_from_slice(properties);
        keys[..properties.len()].sort_unstable();
        let mut unique = 0;
        for index in 0..properties.len() {
            if unique == 0 || keys[index] != keys[unique - 1] {
                keys[unique] = keys[index];
                unique += 1;
            }
        }
        if unique < 2 {
            return None;
        }
        let entries = self.joint_statistics.get(label)?;
        let position = entries
            .binary_search_by(|entry| {
                entry
                    .properties
                    .iter()
                    .map(String::as_str)
                    .cmp(keys[..unique].iter().copied())
            })
            .ok()?;
        let statistics = entries[position].statistics?;
        let population = self.label_counts.get(label).copied()?;
        (statistics.distinct_count > 0
            && statistics.distinct_count <= statistics.index_size
            && statistics.index_size <= population)
            .then_some(statistics)
    }

    pub(in crate::graph) fn estimate_joint_equality_rows(
        &self,
        label: &str,
        properties: &[&str],
        covered: &[&str],
        input_rows: u64,
    ) -> Option<JointEqualityEstimate> {
        let statistics = self.joint_statistics(label, properties)?;
        let population = self.label_count(label);
        let rows = if covered.len() == properties.len() {
            input_rows
        } else {
            if statistics.index_size == population
                && properties.iter().any(|property| {
                    self.known_distinct_count(label, property)
                        .is_some_and(|ndv| ndv > statistics.distinct_count)
                })
            {
                return None;
            }
            let rows = if covered.is_empty() {
                (u128::from(input_rows) * u128::from(statistics.index_size))
                    .div_ceil(u128::from(population) * u128::from(statistics.distinct_count))
            } else {
                // Conditional domains need the same complete non-NULL population.
                if statistics.index_size != population {
                    return None;
                }
                let covered_ndv = if let [property] = covered {
                    self.known_distinct_count(label, property)?
                } else {
                    let subset = self.joint_statistics(label, covered)?;
                    if subset.index_size != population {
                        return None;
                    }
                    subset.distinct_count
                };
                if covered_ndv == 0 || covered_ndv > statistics.distinct_count {
                    return None;
                }
                (u128::from(input_rows) * u128::from(covered_ndv))
                    .div_ceil(u128::from(statistics.distinct_count))
            };
            rows.min(u128::from(input_rows)).max(1) as u64
        };
        Some(JointEqualityEstimate {
            rows,
            population,
            indexed: statistics.index_size,
            ndv: statistics.distinct_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{
        optimizer_catalog_from_graph_statistics, OptimizerCatalogIndexes,
        OptimizerCatalogStatistics,
    };
    use hawdb_core::{Catalog, GraphStatistics, IndexStatisticsSample};

    fn from_entries(
        entries: Vec<((String, Vec<String>), OptimizerIndexStatistics)>,
    ) -> OptimizerCatalog {
        OptimizerCatalog::new(
            OptimizerCatalogIndexes::new([], entries.iter().map(|(key, _)| key.clone()), [], []),
            OptimizerCatalogStatistics::new(
                [("Item".into(), 1000), ("Other".into(), 1000)],
                [],
                [],
                [],
                [],
                [],
                [],
            )
            .with_composite_index_statistics(entries),
        )
    }

    #[test]
    fn unordered_key_identity_rejects_conflicting_samples_and_bounds_view_size() {
        let first = ("Item".into(), vec!["a".into(), "b".into()]);
        let reverse = ("Item".into(), vec!["b".into(), "a".into()]);
        let sample = OptimizerIndexStatistics {
            index_size: 1000,
            distinct_count: 10,
        };
        let catalog = from_entries(vec![(first.clone(), sample), (reverse.clone(), sample)]);
        assert_eq!(catalog.joint_statistics("Item", &["b", "a"]), Some(sample));
        let ambiguous = from_entries(vec![
            (first, sample),
            (
                reverse,
                OptimizerIndexStatistics {
                    distinct_count: 12,
                    ..sample
                },
            ),
        ]);
        assert_eq!(ambiguous.joint_statistics("Item", &["a", "b"]), None);
        let entries = ["Item", "Other"]
            .into_iter()
            .flat_map(|label| {
                (0..33).map(move |n| ((label.into(), vec![format!("p{n:02}"), "z".into()]), sample))
            })
            .collect();
        let catalog = from_entries(entries);
        assert_eq!(
            catalog.joint_statistics.len(),
            2,
            "retain one label key per group owner"
        );
        let query = catalog
            .clone()
            .with_property_equality_row_count("Item".into(), "id".into(), 1);
        assert!(std::sync::Arc::ptr_eq(
            &catalog.joint_statistics,
            &query.joint_statistics
        ));
        for label in ["Item", "Other"] {
            let available = (0..33)
                .filter(|n| {
                    catalog
                        .joint_statistics(label, &[&format!("p{n:02}"), "z"])
                        .is_some()
                })
                .count();
            assert_eq!(
                available, 32,
                "bound the statistical view independently per label"
            );
        }
        assert_eq!(
            catalog.composite_index_statistics.len(),
            66,
            "the joint view must not remove ordinary access statistics"
        );
    }

    #[test]
    #[ignore = "manual bounded joint-key lookup measurement"]
    fn joint_key_lookup_measurement() {
        use std::{hint::black_box, time::Instant};
        let sample = OptimizerIndexStatistics {
            index_size: 1000,
            distinct_count: 10,
        };
        let entries = (0..32)
            .map(|n| {
                (
                    ("Item".into(), vec![format!("p{n:02}"), "z".into()]),
                    sample,
                )
            })
            .collect::<Vec<_>>();
        let legacy = entries.iter().cloned().collect::<BTreeMap<_, _>>();
        let catalog = from_entries(entries);
        let repeats = 100_000;
        let started = Instant::now();
        for _ in 0..repeats {
            let mut keys = black_box(["z", "p16"])
                .iter()
                .map(|key| (*key).to_owned())
                .collect::<Vec<_>>();
            keys.sort_unstable();
            assert_eq!(
                black_box(legacy.get(&(black_box("Item").to_owned(), keys))).copied(),
                Some(sample)
            );
        }
        let legacy_ns = started.elapsed().as_nanos();
        let started = Instant::now();
        for _ in 0..repeats {
            assert_eq!(
                black_box(&catalog).joint_statistics(black_box("Item"), black_box(&["z", "p16"])),
                Some(sample)
            );
        }
        println!("repeats={repeats} legacy_ns={legacy_ns} borrowed_ns={} retained_label_keys_before={} retained_label_keys_after={}",
            started.elapsed().as_nanos(), legacy.len(), catalog.joint_statistics.len());
    }

    #[test]
    fn incomplete_invalid_and_stale_samples_remain_unusable_and_sampled_ndv_stays_an_estimate() {
        let mut schema = Catalog::default();
        let label = schema.get_or_create_label("Item");
        let index = schema.get_or_create_composite_property_index(label, &["a".into(), "b".into()]);
        let mut statistics = GraphStatistics {
            advanced_statistics_complete: true,
            label_counts: BTreeMap::from([(label, 1000)]),
            index_samples: BTreeMap::from([(index, IndexStatisticsSample::exact(1000, 10))]),
            ..GraphStatistics::default()
        };
        assert_eq!(
            optimizer_catalog_from_graph_statistics(&schema, &statistics)
                .joint_statistics("Item", &["a", "b"])
                .unwrap()
                .distinct_count,
            10
        );
        statistics.advanced_statistics_complete = false;
        let incomplete = optimizer_catalog_from_graph_statistics(&schema, &statistics);
        assert_eq!(incomplete.label_count("Item"), 1000);
        assert_eq!(incomplete.joint_statistics("Item", &["a", "b"]), None);
        statistics.advanced_statistics_complete = true;
        let sampled = IndexStatisticsSample {
            index_size: 1000,
            unique_values: 10,
            sample_size: 100,
            updates_since_sample: 0,
        };
        statistics.index_samples.insert(index, sampled);
        assert_eq!(
            optimizer_catalog_from_graph_statistics(&schema, &statistics)
                .joint_statistics("Item", &["a", "b"])
                .unwrap()
                .distinct_count,
            100,
            "forward estimated NDV, not observed sample unique count"
        );
        for unusable in [
            IndexStatisticsSample {
                updates_since_sample: 51,
                ..sampled
            },
            IndexStatisticsSample {
                sample_size: 1001,
                ..sampled
            },
            IndexStatisticsSample {
                unique_values: 101,
                ..sampled
            },
            IndexStatisticsSample::default(),
        ] {
            statistics.index_samples.insert(index, unusable);
            assert_eq!(
                optimizer_catalog_from_graph_statistics(&schema, &statistics)
                    .joint_statistics("Item", &["a", "b"]),
                None
            );
        }
    }
}
