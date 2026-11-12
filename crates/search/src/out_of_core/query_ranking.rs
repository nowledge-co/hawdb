// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Ranking stages share the query's working account and borrow score IDs.

use super::*;
use crate::build_memory::{
    checked_add as add, checked_mul as mul, reserve_capacity, MAP_ENTRY_BYTES, SET_ENTRY_BYTES,
};
use crate::lexical_projection::QueryContext;
use hawdb_executor::QueryMemoryLease;

#[cfg(test)]
mod tests;

pub(super) struct Ranks<'a> {
    values: BTreeMap<&'a str, usize>,
    _memory: Option<QueryMemoryLease>,
}
impl<'a> std::ops::Deref for Ranks<'a> {
    type Target = BTreeMap<&'a str, usize>;
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}
impl<'a> Ranks<'a> {
    pub(super) fn new(
        scores: &'a BTreeMap<String, f64>,
        context: Option<QueryContext<'_>>,
    ) -> Result<Self> {
        if let Some(context) = context {
            context.checkpoint()?;
        }
        let mut output = Self {
            values: BTreeMap::new(),
            _memory: context
                .map(|context| context.memory.reserve(mul(scores.len(), MAP_ENTRY_BYTES)?))
                .transpose()?,
        };
        let mut sort_memory = context
            .map(|context| context.memory.reserve(0))
            .transpose()?;
        let mut sorted = Vec::new();
        if let Some(memory) = &mut sort_memory {
            reserve_capacity(&mut sorted, scores.len(), memory)?;
        } else {
            sorted.reserve_exact(scores.len());
        }
        for (id, score) in scores {
            if let Some(context) = context {
                context.checkpoint()?;
            }
            sorted.push((id.as_str(), *score));
        }
        sorted.sort_unstable_by(|(left_id, left_score), (right_id, right_score)| {
            right_score
                .partial_cmp(left_score)
                .unwrap_or(CmpOrdering::Equal)
                .then_with(|| left_id.cmp(right_id))
        });
        for (index, (id, _)) in sorted.into_iter().enumerate() {
            if let Some(context) = context {
                context.checkpoint()?;
            }
            output.values.insert(id, index + 1);
        }
        if let Some(context) = context {
            context.checkpoint()?;
        }
        Ok(output)
    }
    pub(super) fn window(
        &self,
        limit: Option<usize>,
        context: Option<QueryContext<'_>>,
    ) -> Result<Self> {
        if let Some(context) = context {
            context.checkpoint()?;
        }
        let count = self
            .values
            .values()
            .filter(|rank| limit.is_none_or(|limit| **rank <= limit))
            .count();
        let mut output = Self {
            values: BTreeMap::new(),
            _memory: context
                .map(|context| context.memory.reserve(mul(count, MAP_ENTRY_BYTES)?))
                .transpose()?,
        };
        for (&id, rank) in &self.values {
            if let Some(context) = context {
                context.checkpoint()?;
            }
            if limit.is_none_or(|limit| *rank <= limit) {
                output.values.insert(id, *rank);
            }
        }
        if let Some(context) = context {
            context.checkpoint()?;
        }
        Ok(output)
    }
}

fn copy_id(id: &str) -> Result<String> {
    let value = id.to_owned();
    if value.capacity() > id.len() {
        return Err(HawDBError::Execution(
            "ranking ID copy exceeded admission".into(),
        ));
    }
    Ok(value)
}

pub(super) struct Inputs<'a> {
    pub(super) vector_scores: &'a BTreeMap<String, f64>,
    pub(super) text_scores: &'a BTreeMap<String, f64>,
    pub(super) vector_ranks: &'a Ranks<'a>,
    pub(super) text_ranks: &'a Ranks<'a>,
    pub(super) vector_window: &'a Ranks<'a>,
    pub(super) text_window: &'a Ranks<'a>,
    pub(super) mode: SearchMode,
    pub(super) weights: crate::SearchFusionWeights,
}
pub(super) struct Candidates {
    pub(super) values: Vec<SearchScoredCandidate>,
    ids: BTreeSet<String>,
    memory: Option<QueryMemoryLease>,
}
impl Candidates {
    pub(super) fn new(inputs: Inputs<'_>, context: Option<QueryContext<'_>>) -> Result<Self> {
        let mut output = Self {
            values: Vec::new(),
            ids: BTreeSet::new(),
            memory: context
                .map(|context| context.memory.reserve(0))
                .transpose()?,
        };
        for id in inputs.vector_scores.keys().chain(inputs.text_scores.keys()) {
            if let Some(context) = context {
                context.checkpoint()?;
            }
            if !output.ids.contains(id) {
                if let Some(memory) = &mut output.memory {
                    memory.grow(add(SET_ENTRY_BYTES, id.len())?)?;
                }
                output.ids.insert(copy_id(id)?);
            }
        }
        let node_bytes = mul(output.ids.len(), SET_ENTRY_BYTES)?;
        while let Some(id) = output.ids.pop_first() {
            if let Some(context) = context {
                context.checkpoint()?;
            }
            let vector_score = inputs
                .vector_scores
                .get(id.as_str())
                .copied()
                .unwrap_or(0.0);
            let text_score = inputs.text_scores.get(id.as_str()).copied().unwrap_or(0.0);
            let vector_rank = if inputs.mode == SearchMode::Hybrid {
                inputs.vector_window.get(id.as_str()).copied()
            } else {
                inputs.vector_ranks.get(id.as_str()).copied()
            };
            let text_rank = if inputs.mode == SearchMode::Hybrid {
                inputs.text_window.get(id.as_str()).copied()
            } else {
                inputs.text_ranks.get(id.as_str()).copied()
            };
            let vector_rrf_score = rrf_child_score(vector_rank);
            let text_rrf_score = rrf_child_score(text_rank);
            let rrf_score = weighted_rrf_score(vector_rrf_score, text_rrf_score, inputs.weights);
            let score = match inputs.mode {
                SearchMode::Hybrid => rrf_score,
                SearchMode::Vector => vector_score,
                SearchMode::Text => text_score,
            };
            let bytes = id.capacity();
            if score > 0.0 {
                if let Some(memory) = &mut output.memory
                    && output.values.len() == output.values.capacity()
                {
                    // Grow geometrically only when full: doubling on every
                    // push would allocate and charge 2^n slots for n hits.
                    let required = add(output.values.len(), 1)?;
                    let capacity = required.max(output.values.capacity().saturating_mul(2));
                    reserve_capacity(&mut output.values, capacity, memory)?;
                    // Moving an ID within this owner adds no payload charge.
                }
                output.values.push(SearchScoredCandidate {
                    id,
                    score,
                    vector_score,
                    text_score,
                    rrf_score,
                    vector_rrf_score,
                    text_rrf_score,
                    vector_rank,
                    text_rank,
                });
            } else {
                drop(id);
                if let Some(memory) = &mut output.memory {
                    memory.shrink(bytes);
                }
            }
        }
        // Empty B-trees can retain a root node: destroy it before refund.
        output.ids = BTreeSet::new();
        if let Some(memory) = &mut output.memory {
            memory.shrink(node_bytes);
        }
        output.values.sort_unstable_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(CmpOrdering::Equal)
                .then_with(|| left.id.cmp(&right.id))
        });
        if let Some(context) = context {
            context.checkpoint()?;
        }
        Ok(output)
    }
    pub(super) fn page(&mut self, offset: usize, limit: usize) -> Result<()> {
        // Compaction keeps the vector allocation: refund only destroyed IDs.
        let start = offset.min(self.values.len());
        let end = start.saturating_add(limit).min(self.values.len());
        let removed = self.values[..start]
            .iter()
            .chain(&self.values[end..])
            .try_fold(0, |bytes, value| add(bytes, value.id.capacity()))?;
        drop(self.values.drain(..start));
        self.values.truncate(limit);
        if let Some(memory) = &mut self.memory {
            memory.shrink(removed);
        }
        Ok(())
    }
}
