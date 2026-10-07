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

//! Cheap store captures keep mutable counters separate from advanced facts.
//!
//! Public statistics remain owned DTOs. Page directories, DDL detachment and
//! final-owner destruction still need resource/time accounting.

use super::*;
use crate::background::{CheckpointWorkContext, CheckpointWorkError};
use std::ops::{Deref, DerefMut};
use std::result::Result;

#[derive(Debug, Clone, Default)]
pub(super) struct BasicStatisticsState {
    pub(super) computed_at_commit_epoch: u64,
    pub(super) node_count: u64,
    pub(super) relationship_count: u64,
    pub(super) label_counts: CowSegmentedMap<LabelId, u64>,
    pub(super) rel_type_counts: CowSegmentedMap<RelTypeId, u64>,
}

impl From<BasicGraphStatistics> for BasicStatisticsState {
    fn from(statistics: BasicGraphStatistics) -> Self {
        Self {
            computed_at_commit_epoch: statistics.computed_at_commit_epoch,
            node_count: statistics.node_count,
            relationship_count: statistics.relationship_count,
            label_counts: statistics.label_counts.into(),
            rel_type_counts: statistics.rel_type_counts.into(),
        }
    }
}

impl BasicStatisticsState {
    pub(super) fn materialize(&self, commit_epoch: u64) -> BasicGraphStatistics {
        BasicGraphStatistics {
            computed_at_commit_epoch: commit_epoch,
            node_count: self.node_count,
            relationship_count: self.relationship_count,
            label_counts: self
                .label_counts
                .iter()
                .map(|(key, value)| (*key, *value))
                .collect(),
            rel_type_counts: self
                .rel_type_counts
                .iter()
                .map(|(key, value)| (*key, *value))
                .collect(),
        }
    }

    pub(super) fn materialize_with_work_context(
        &self,
        commit_epoch: u64,
        work: &CheckpointWorkContext,
    ) -> Result<BasicGraphStatistics, CheckpointWorkError> {
        let mut statistics = BasicGraphStatistics {
            computed_at_commit_epoch: commit_epoch,
            node_count: self.node_count,
            relationship_count: self.relationship_count,
            ..BasicGraphStatistics::default()
        };
        for (label, count) in self.label_counts.iter() {
            let unit = work.start_unit()?;
            statistics.label_counts.insert(*label, *count);
            unit.finish();
        }
        for (rel_type, count) in self.rel_type_counts.iter() {
            let unit = work.start_unit()?;
            statistics.rel_type_counts.insert(*rel_type, *count);
            unit.finish();
        }
        work.checkpoint()?;
        Ok(statistics)
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct CheckpointStatisticsState {
    advanced: CowSegment<GraphStatistics>,
    pub(super) index_samples: CowSegmentedMap<IndexId, IndexStatisticsSample>,
}

impl From<GraphStatistics> for CheckpointStatisticsState {
    fn from(mut statistics: GraphStatistics) -> Self {
        let index_samples = std::mem::take(&mut statistics.index_samples).into();
        Self {
            advanced: statistics.into(),
            index_samples,
        }
    }
}

impl Deref for CheckpointStatisticsState {
    type Target = GraphStatistics;
    fn deref(&self) -> &GraphStatistics {
        &self.advanced
    }
}

impl DerefMut for CheckpointStatisticsState {
    fn deref_mut(&mut self) -> &mut GraphStatistics {
        &mut self.advanced
    }
}

impl CheckpointStatisticsState {
    pub(super) fn materialize(&self) -> GraphStatistics {
        let mut statistics = (*self.advanced).clone();
        statistics.index_samples = self
            .index_samples
            .iter()
            .map(|(key, value)| (*key, *value))
            .collect();
        statistics
    }

    pub(super) fn with_work_context(
        mut statistics: GraphStatistics,
        work: &CheckpointWorkContext,
    ) -> Result<Self, CheckpointWorkError> {
        let mut index_samples = CowSegmentedMap::default();
        for (id, sample) in std::mem::take(&mut statistics.index_samples) {
            let unit = work.start_unit()?;
            index_samples.insert(id, sample);
            unit.finish();
        }
        let unit = work.start_unit()?;
        let result = Self {
            advanced: statistics.into(),
            index_samples,
        };
        unit.finish();
        work.checkpoint()?;
        Ok(result)
    }

    pub(super) fn retain_valid_index_samples(&mut self, catalog: &Catalog) {
        self.index_samples
            .retain(|id, sample| catalog.supports_index_statistics(*id) && sample.is_valid());
    }
}

pub(super) fn decrement_statistic_counter<K: Ord + Clone + crate::cow::CowPageWeight>(
    counters: &mut CowSegmentedMap<K, u64>,
    key: &K,
) {
    let Some(count) = counters.get_mut(key) else {
        return;
    };
    *count = count.saturating_sub(1);
    if *count == 0 {
        counters.remove(key);
    }
}

#[cfg(test)]
#[path = "statistics_state/tests.rs"]
mod tests;
