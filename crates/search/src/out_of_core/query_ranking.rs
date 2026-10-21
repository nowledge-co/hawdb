// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Ranking stages share the query's working account and own their payloads.

use super::*;
use crate::build_memory::{
    checked_add as add, checked_mul as mul, reserve_capacity, MAP_ENTRY_BYTES, SET_ENTRY_BYTES,
};
use crate::lexical_projection::QueryContext;
use hawdb_executor::QueryMemoryLease;

#[cfg(test)]
mod tests;

pub(super) struct Ranks {
    values: BTreeMap<String, usize>,
    _memory: Option<QueryMemoryLease>,
}
impl std::ops::Deref for Ranks {
    type Target = BTreeMap<String, usize>;
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}
impl Ranks {
    pub(super) fn new(
        scores: &BTreeMap<String, f64>,
        context: Option<QueryContext<'_>>,
    ) -> Result<Self> {
        let Some(context) = context else {
            return Ok(Self {
                values: ranked_scores(scores),
                _memory: None,
            });
        };
        context.checkpoint()?;
        let bytes = scores
            .keys()
            .try_fold(mul(scores.len(), MAP_ENTRY_BYTES)?, |bytes, id| {
                add(bytes, id.len())
            })?;
        let mut output = Self {
            values: BTreeMap::new(),
            _memory: Some(context.memory.reserve(bytes)?),
        };
        let mut sort_memory = context.memory.reserve(0)?;
        let mut sorted = Vec::new();
        reserve_capacity(&mut sorted, scores.len(), &mut sort_memory)?;
        for (id, score) in scores {
            context.checkpoint()?;
            sorted.push((id.as_str(), *score));
        }
        // IDs fully break score ties, so the in-place sort preserves ordering
        // without allocating an unadmitted stable-sort scratch buffer.
        sorted.sort_unstable_by(|(left_id, left_score), (right_id, right_score)| {
            right_score
                .partial_cmp(left_score)
                .unwrap_or(CmpOrdering::Equal)
                .then_with(|| left_id.cmp(right_id))
        });
        for (index, (id, _)) in sorted.into_iter().enumerate() {
            context.checkpoint()?;
            output.values.insert(copy_id(id)?, index + 1);
        }
        context.checkpoint()?;
        Ok(output)
    }
    pub(super) fn window(
        &self,
        limit: Option<usize>,
        context: Option<QueryContext<'_>>,
    ) -> Result<Self> {
        let Some(context) = context else {
            return Ok(Self {
                values: window_ranks(&self.values, limit),
                _memory: None,
            });
        };
        context.checkpoint()?;
        let bytes = self
            .values
            .iter()
            .filter(|(_, rank)| limit.is_none_or(|limit| **rank <= limit))
            .try_fold(0, |bytes, (id, _)| {
                add(bytes, add(MAP_ENTRY_BYTES, id.len())?)
            })?;
        let mut output = Self {
            values: BTreeMap::new(),
            _memory: Some(context.memory.reserve(bytes)?),
        };
        for (id, rank) in &self.values {
            context.checkpoint()?;
            if limit.is_none_or(|limit| *rank <= limit) {
                output.values.insert(copy_id(id)?, *rank);
            }
        }
        context.checkpoint()?;
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
    pub(super) vector_ranks: &'a Ranks,
    pub(super) text_ranks: &'a Ranks,
    pub(super) vector_window: &'a Ranks,
    pub(super) text_window: &'a Ranks,
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
            let vector_score = inputs.vector_scores.get(&id).copied().unwrap_or(0.0);
            let text_score = inputs.text_scores.get(&id).copied().unwrap_or(0.0);
            let vector_rank = if inputs.mode == SearchMode::Hybrid {
                inputs.vector_window.get(&id).copied()
            } else {
                inputs.vector_ranks.get(&id).copied()
            };
            let text_rank = if inputs.mode == SearchMode::Hybrid {
                inputs.text_window.get(&id).copied()
            } else {
                inputs.text_ranks.get(&id).copied()
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
                if let Some(memory) = &mut output.memory {
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
