//! Shared retained-score ownership for lexical and vector consumers.

use crate::{Result, SkeinError};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};

#[derive(Debug, Clone)]
struct ScoredDocument {
    id: String,
    score: f64,
}

impl PartialEq for ScoredDocument {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.score.to_bits() == other.score.to_bits()
    }
}

impl Eq for ScoredDocument {}

impl PartialOrd for ScoredDocument {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredDocument {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| other.id.cmp(&self.id))
    }
}

enum ScoreStorage {
    Full(BTreeMap<String, f64>),
    TopK {
        limit: usize,
        heap: BinaryHeap<Reverse<ScoredDocument>>,
    },
}

pub(crate) struct ScoreCollector {
    storage: ScoreStorage,
    pub(crate) matching_count: usize,
    max_entries: usize,
    bytes: ScoreBudget,
}

struct ScoreBudget {
    label: &'static str,
    used: u64,
    limit: u64,
    memory: skein_executor::QueryMemoryLease,
}

impl ScoreBudget {
    fn replace(&mut self, removed: u64, added: u64) -> Result<()> {
        let next = self
            .used
            .checked_sub(removed)
            .and_then(|bytes| bytes.checked_add(added))
            .filter(|&bytes| bytes <= self.limit)
            .ok_or_else(|| {
                SkeinError::Storage(format!(
                    "{} score byte budget exceeded: limit {}",
                    self.label, self.limit
                ))
            })?;
        if next > self.used {
            self.memory
                .grow(usize::try_from(next - self.used).map_err(|_| {
                    SkeinError::Storage("lexical score capacity exceeds address space".to_string())
                })?)?;
        } else {
            self.memory.shrink((self.used - next) as usize);
        }
        self.used = next;
        Ok(())
    }
}

fn score_entry_bytes(id: &String) -> u64 {
    // Requested capacity for the owned ID, B-tree node slack and the temporary
    // tree produced while consuming a top-k heap. Not allocator/RSS telemetry.
    256u64.saturating_add(id.capacity() as u64)
}

impl ScoreCollector {
    pub(crate) fn new(
        retained_limit: Option<usize>,
        max_entries: usize,
        max_bytes: u64,
        account: &skein_executor::QueryMemoryAccount,
    ) -> Result<Self> {
        Self::with_label(retained_limit, max_entries, max_bytes, account, "lexical")
    }

    pub(crate) fn for_vector(
        retained_limit: Option<usize>,
        max_entries: usize,
        max_bytes: u64,
        account: &skein_executor::QueryMemoryAccount,
    ) -> Result<Self> {
        Self::with_label(retained_limit, max_entries, max_bytes, account, "vector")
    }

    fn with_label(
        retained_limit: Option<usize>,
        max_entries: usize,
        max_bytes: u64,
        account: &skein_executor::QueryMemoryAccount,
        label: &'static str,
    ) -> Result<Self> {
        if retained_limit.is_some_and(|limit| limit > max_entries) {
            return Err(SkeinError::Storage(format!(
                "{label} rank window exceeds the admitted {max_entries} score entries"
            )));
        }
        let mut bytes = ScoreBudget {
            label,
            used: 0,
            limit: max_bytes,
            memory: account.reserve(0)?,
        };
        let storage = match retained_limit {
            Some(limit) => {
                let allocation = (limit as u64)
                    .checked_mul(std::mem::size_of::<Reverse<ScoredDocument>>() as u64)
                    .ok_or_else(|| {
                        SkeinError::Storage(format!("{label} score allocation overflow"))
                    })?;
                bytes.replace(0, allocation)?;
                let mut heap = BinaryHeap::new();
                heap.try_reserve_exact(limit)
                    .map_err(|_| SkeinError::Storage(format!("{label} score allocation failed")))?;
                bytes.replace(
                    allocation,
                    (heap.capacity() as u64)
                        .saturating_mul(std::mem::size_of::<Reverse<ScoredDocument>>() as u64),
                )?;
                ScoreStorage::TopK { limit, heap }
            }
            None => ScoreStorage::Full(BTreeMap::new()),
        };
        Ok(Self {
            storage,
            matching_count: 0,
            max_entries,
            bytes,
        })
    }

    pub(crate) fn push(&mut self, id: String, score: f64) -> Result<()> {
        self.matching_count = self.matching_count.saturating_add(1);
        match &mut self.storage {
            ScoreStorage::Full(scores) => {
                if scores.len() >= self.max_entries {
                    return Err(SkeinError::Storage(format!(
                        "{} query matched more than {} documents; provide a rank window or narrow the candidate set",
                        self.bytes.label, self.max_entries
                    )));
                }
                self.bytes.replace(0, score_entry_bytes(&id))?;
                scores.insert(id, score);
            }
            ScoreStorage::TopK { limit, heap } => {
                if *limit == 0 {
                    return Ok(());
                }
                let candidate = ScoredDocument { id, score };
                if heap.len() < *limit {
                    self.bytes.replace(0, score_entry_bytes(&candidate.id))?;
                    heap.push(Reverse(candidate));
                } else if heap
                    .peek()
                    .is_some_and(|Reverse(worst)| candidate.cmp(worst).is_gt())
                {
                    let removed = score_entry_bytes(&heap.peek().unwrap().0.id);
                    let added = score_entry_bytes(&candidate.id);
                    // Preflight growth before mutation. If the replacement is
                    // smaller, keep the old charge until its ID actually drops.
                    self.bytes.replace(removed.min(added), added)?;
                    drop(heap.pop());
                    if removed > added {
                        self.bytes.replace(removed - added, 0)?;
                    }
                    heap.push(Reverse(candidate));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> crate::query_memory::AdmittedScores {
        let scores = match self.storage {
            ScoreStorage::Full(scores) => scores,
            ScoreStorage::TopK { heap, .. } => heap
                .into_iter()
                .map(|Reverse(candidate)| (candidate.id, candidate.score))
                .collect(),
        };
        crate::query_memory::AdmittedScores::new(scores, self.bytes.memory)
    }
}
