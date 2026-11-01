// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Bounded unordered key-set views of existing, snapshot-local index samples.
//! These are statistical estimates, never uniqueness or non-NULL proofs.

use super::{OptimizerCatalog, OptimizerIndexStatistics};
use std::collections::{BTreeMap, BTreeSet};

pub(in crate::graph) const MAX_JOINT_ARITY: usize = 4;
const MAX_JOINT_SETS_PER_LABEL: usize = 32;

type Statistics = BTreeMap<(String, Vec<String>), Option<OptimizerIndexStatistics>>;

pub(super) fn canonical_statistics(
    source: &BTreeMap<(String, Vec<String>), OptimizerIndexStatistics>,
    indexes: &BTreeSet<(String, Vec<String>)>,
) -> Statistics {
    let mut result = BTreeMap::new();
    let mut previous_label = None;
    let mut count = 0;
    for ((label, properties), statistics) in source {
        if !indexes.contains(&(label.clone(), properties.clone())) {
            continue;
        }
        if previous_label != Some(label) {
            previous_label = Some(label);
            count = 0;
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
        let key = (label.clone(), properties);
        if let Some(existing) = result.get_mut(&key) {
            // Differently ordered indexes can have distinct sample epochs or
            // estimates. Do not choose one silently for the same key set.
            if *existing != Some(*statistics) {
                *existing = None;
            }
        } else if count < MAX_JOINT_SETS_PER_LABEL {
            result.insert(key, Some(*statistics));
            count += 1;
        }
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
        let mut keys = properties
            .iter()
            .map(|key| (*key).to_owned())
            .collect::<Vec<_>>();
        keys.sort_unstable();
        keys.dedup();
        if keys.len() < 2 {
            return None;
        }
        let statistics = self
            .joint_statistics
            .get(&(label.to_owned(), keys))
            .copied()
            .flatten()?;
        let population = self.label_counts.get(label).copied()?;
        (statistics.distinct_count > 0
            && statistics.distinct_count <= statistics.index_size
            && statistics.index_size <= population)
            .then_some(statistics)
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
