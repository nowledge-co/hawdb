// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Score payloads keep their query capacity through heap-to-map transfer.

use super::*;
use crate::build_memory::{
    checked_add as add, checked_mul as mul, reserve_capacity, MAP_ENTRY_BYTES,
};
use crate::lexical_projection::QueryContext;
use hawdb_executor::QueryMemoryLease;

#[cfg(test)]
mod tests;

#[derive(Debug)]
struct RankedScore {
    id: String,
    score: f64,
}
impl PartialEq for RankedScore {
    fn eq(&self, other: &Self) -> bool {
        self.score.to_bits() == other.score.to_bits() && self.id == other.id
    }
}
impl Eq for RankedScore {}
impl PartialOrd for RankedScore {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}
impl Ord for RankedScore {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| other.id.cmp(&self.id))
    }
}

enum Storage {
    Full(BTreeMap<String, f64>),
    TopK {
        limit: usize,
        heap: BinaryHeap<Reverse<RankedScore>>,
    },
}

pub(super) struct Scores {
    pub(super) values: BTreeMap<String, f64>,
    pub(super) memory: Option<QueryMemoryLease>,
}

pub(super) struct BoundedScoreCollector {
    storage: Storage,
    pub(super) matching_count: usize,
    max_entries: usize,
    memory: Option<QueryMemoryLease>,
}

impl BoundedScoreCollector {
    pub(super) fn new(limit: Option<usize>, max_entries: usize) -> Result<Self> {
        Self::with_context(limit, max_entries, None)
    }
    pub(super) fn with_context(
        limit: Option<usize>,
        max_entries: usize,
        context: Option<QueryContext<'_>>,
    ) -> Result<Self> {
        if limit.is_some_and(|limit| limit > max_entries) {
            return Err(HawDBError::Storage(format!(
                "search rank window exceeds the admitted {max_entries} score entries"
            )));
        }
        let mut memory = context
            .map(|context| context.memory.reserve(0))
            .transpose()?;
        let storage = match limit {
            Some(limit) => {
                let capacity = add(limit, 1)?;
                let mut values = Vec::new();
                if let Some(memory) = &mut memory {
                    reserve_capacity(&mut values, capacity, memory)?;
                } else {
                    values = Vec::with_capacity(capacity);
                }
                Storage::TopK {
                    limit,
                    heap: BinaryHeap::from(values),
                }
            }
            None => Storage::Full(BTreeMap::new()),
        };
        Ok(Self {
            storage,
            matching_count: 0,
            max_entries,
            memory,
        })
    }

    fn retain(&mut self, id: &str, score: f64) -> Result<bool> {
        self.matching_count = self.matching_count.saturating_add(1);
        match &self.storage {
            Storage::Full(values) => {
                if values.len() >= self.max_entries {
                    return Err(HawDBError::Storage(format!("vector query matched more than {} documents; provide a rank window or narrow the candidate set", self.max_entries)));
                }
                Ok(true)
            }
            Storage::TopK { limit, heap } => Ok(*limit != 0
                && (heap.len() < *limit
                    || heap.peek().is_some_and(|Reverse(worst)| {
                        score
                            .total_cmp(&worst.score)
                            .then_with(|| worst.id.as_str().cmp(id))
                            .is_gt()
                    }))),
        }
    }
    fn admit_id(&mut self, bytes: usize) -> Result<()> {
        if let Some(memory) = &mut self.memory {
            memory.grow(add(
                bytes,
                if matches!(self.storage, Storage::Full(_)) {
                    MAP_ENTRY_BYTES
                } else {
                    0
                },
            )?)?;
        }
        Ok(())
    }
    fn insert(&mut self, id: String, score: f64) {
        match &mut self.storage {
            Storage::Full(values) => {
                values.insert(id, score);
            }
            Storage::TopK { limit, heap } => {
                if heap.len() == *limit {
                    let previous = heap.pop().expect("full score heap");
                    let bytes = previous.0.id.capacity();
                    drop(previous);
                    if let Some(memory) = &mut self.memory {
                        memory.shrink(bytes);
                    }
                }
                heap.push(Reverse(RankedScore { id, score }));
            }
        }
    }
    pub(super) fn push(&mut self, id: String, score: f64) -> Result<()> {
        if self.retain(&id, score)? {
            self.admit_id(id.capacity())?;
            self.insert(id, score);
        }
        Ok(())
    }
    pub(super) fn push_borrowed(&mut self, id: &str, score: f64) -> Result<()> {
        if self.retain(id, score)? {
            self.admit_id(id.len())?;
            let owned = id.to_owned();
            if owned.capacity() > id.len() {
                return Err(HawDBError::Execution(
                    "vector score ID exceeded admission".into(),
                ));
            }
            self.insert(owned, score);
        }
        Ok(())
    }
    pub(super) fn finish(mut self) -> Result<Scores> {
        if let Storage::TopK { heap, .. } = &self.storage
            && let Some(memory) = &mut self.memory
        {
            memory.grow(mul(heap.len(), MAP_ENTRY_BYTES)?)?;
        }
        let storage = std::mem::replace(&mut self.storage, Storage::Full(BTreeMap::new()));
        let values = match storage {
            Storage::Full(values) => values,
            Storage::TopK { heap, .. } => {
                let bytes = mul(heap.capacity(), size_of::<Reverse<RankedScore>>())?;
                let mut values = BTreeMap::new();
                for Reverse(candidate) in heap {
                    values.insert(candidate.id, candidate.score);
                }
                if let Some(memory) = &mut self.memory {
                    memory.shrink(bytes);
                }
                values
            }
        };
        Ok(Scores {
            values,
            memory: self.memory.take(),
        })
    }
}
