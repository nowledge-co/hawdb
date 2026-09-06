use super::query_io::{add, checkpoint, mul};
use crate::query_memory::{Admitted, QueryMemory};
use crate::{
    rrf_child_score, weighted_rrf_score, Result, RuntimeTaskContext, SearchFusionWeights,
    SearchMode, SearchQueryOptions, SearchRetrieverCandidate, SearchScoredCandidate, SkeinError,
};
use skein_executor::QueryMemoryAccount;
use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BinaryHeap};
use std::mem::size_of;

struct RankedScore<'a> {
    id: &'a str,
    score: f64,
    rank: usize,
}

struct RankStorage<'a> {
    by_id: Vec<RankedScore<'a>>,
    order: Vec<usize>,
}

/// Both arrays borrow IDs from score owners that must outlive this workspace.
pub(super) struct RankedScores<'a>(Admitted<RankStorage<'a>>);

impl<'a> RankedScores<'a> {
    pub fn new(
        scores: &'a BTreeMap<String, f64>,
        memory: &QueryMemoryAccount,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(task)?;
        let lease = memory.reserve(add(
            buffer_bytes::<RankedScore<'_>>(scores.len())?,
            buffer_bytes::<usize>(scores.len())?,
        )?)?;
        #[cfg(test)]
        tests::record_ranks();
        let mut by_id = Vec::with_capacity(scores.len());
        let mut order = Vec::with_capacity(scores.len());
        for (id, score) in scores {
            checkpoint(task)?;
            order.push(by_id.len());
            by_id.push(RankedScore {
                id,
                score: *score,
                rank: 0,
            });
        }
        order.sort_unstable_by(|&left, &right| {
            by_id[right]
                .score
                .partial_cmp(&by_id[left].score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| by_id[left].id.cmp(by_id[right].id))
        });
        for (index, &ordinal) in order.iter().enumerate() {
            checkpoint(task)?;
            by_id[ordinal].rank = index + 1;
        }
        checkpoint(task)?;
        Ok(Self(Admitted::new(RankStorage { by_id, order }, lease)))
    }

    pub fn window_len(&self, window: Option<usize>) -> usize {
        self.0.by_id.len().min(window.unwrap_or(usize::MAX))
    }

    fn top(&self, window: Option<usize>, limit: usize) -> impl Iterator<Item = &RankedScore<'a>> {
        self.0
            .order
            .iter()
            .take(self.window_len(window).min(limit))
            .map(|&ordinal| &self.0.by_id[ordinal])
    }

    // These are public-report payloads, not temporary ranking workspace. Only
    // selected IDs are cloned; their retained ownership remains a separate API gate.
    pub fn top_ids(&self, window: Option<usize>, limit: usize) -> Vec<String> {
        self.top(window, limit)
            .map(|entry| entry.id.to_owned())
            .collect()
    }

    pub fn top_candidates(
        &self,
        window: Option<usize>,
        limit: usize,
    ) -> Vec<SearchRetrieverCandidate> {
        self.top(window, limit)
            .map(|entry| SearchRetrieverCandidate {
                id: entry.id.to_owned(),
                rank: entry.rank,
                score: entry.score,
            })
            .collect()
    }
}

fn buffer_bytes<T>(count: usize) -> Result<usize> {
    let bytes = mul(count, size_of::<T>())?;
    if bytes > isize::MAX as usize {
        return Err(SkeinError::Execution(
            "search ranking capacity exceeds address space".to_owned(),
        ));
    }
    Ok(bytes)
}

struct Candidate<'a>(SearchScoredCandidate<&'a str>);

impl PartialEq for Candidate<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for Candidate<'_> {}
impl PartialOrd for Candidate<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Candidate<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .score
            .total_cmp(&other.0.score)
            .then_with(|| other.0.id.cmp(self.0.id))
    }
}

fn fuse<'a>(
    vector: Option<&RankedScore<'a>>,
    text: Option<&RankedScore<'a>>,
    mode: SearchMode,
    window: Option<usize>,
    weights: SearchFusionWeights,
) -> Candidate<'a> {
    let rank = |entry: Option<&RankedScore<'_>>| {
        entry
            .filter(|entry| {
                mode != SearchMode::Hybrid || window.is_none_or(|end| entry.rank <= end)
            })
            .map(|entry| entry.rank)
    };
    let vector_rank = rank(vector);
    let text_rank = rank(text);
    let vector_score = vector.map_or(0.0, |entry| entry.score);
    let text_score = text.map_or(0.0, |entry| entry.score);
    let vector_rrf_score = rrf_child_score(vector_rank);
    let text_rrf_score = rrf_child_score(text_rank);
    let rrf_score = weighted_rrf_score(vector_rrf_score, text_rrf_score, weights);
    Candidate(SearchScoredCandidate {
        id: vector.or(text).expect("union must contain a score").id,
        score: match mode {
            SearchMode::Hybrid => rrf_score,
            SearchMode::Vector => vector_score,
            SearchMode::Text => text_score,
        },
        vector_score,
        text_score,
        rrf_score,
        vector_rrf_score,
        text_rrf_score,
        vector_rank,
        text_rank,
    })
}

pub(super) struct RankedPage {
    pub candidates: Admitted<Vec<SearchScoredCandidate>>,
    pub matching_count: usize,
}

impl RankedPage {
    pub fn build(
        vector: &RankedScores<'_>,
        text: &RankedScores<'_>,
        mode: SearchMode,
        options: &SearchQueryOptions,
        memory: &QueryMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(task)?;
        let page_end = add(options.offset, options.limit)?;
        let capacity = if options.limit == 0 {
            0
        } else {
            page_end.min(add(vector.0.by_id.len(), text.0.by_id.len())?)
        };
        let _heap_memory = memory
            .working
            .reserve(buffer_bytes::<Reverse<Candidate<'_>>>(capacity)?)?;
        #[cfg(test)]
        tests::record_heap();
        let mut heap = BinaryHeap::<Reverse<Candidate<'_>>>::with_capacity(capacity);
        let mut vector = vector.0.by_id.iter().peekable();
        let mut text = text.0.by_id.iter().peekable();
        let mut matching_count = 0;
        // Merge the two ID-ordered slices without a union set or ID copies.
        while vector.peek().is_some() || text.peek().is_some() {
            checkpoint(task)?;
            let pair = match (vector.peek(), text.peek()) {
                (Some(left), Some(right)) => match left.id.cmp(right.id) {
                    Ordering::Less => (vector.next(), None),
                    Ordering::Greater => (None, text.next()),
                    Ordering::Equal => (vector.next(), text.next()),
                },
                (Some(_), None) => (vector.next(), None),
                (None, Some(_)) => (None, text.next()),
                (None, None) => unreachable!(),
            };
            let candidate = fuse(
                pair.0,
                pair.1,
                mode,
                options.rank_window,
                options.fusion_weights,
            );
            if candidate.0.score <= 0.0 || candidate.0.score.is_nan() {
                continue;
            }
            matching_count = add(matching_count, 1)?;
            if heap.len() < capacity {
                heap.push(Reverse(candidate));
            } else if let Some(mut worst) = heap.peek_mut()
                && candidate > worst.0
            {
                *worst = Reverse(candidate);
            }
        }
        let mut ordered = heap.into_vec();
        #[cfg(test)]
        tests::record_heap_capacity(ordered.capacity());
        ordered.sort_unstable_by(|left, right| right.0.cmp(&left.0));
        checkpoint(task)?;
        let start = options.offset.min(ordered.len());
        let selected = &ordered[start..];
        let mut bytes = buffer_bytes::<SearchScoredCandidate>(selected.len())?;
        for Reverse(Candidate(value)) in selected {
            checkpoint(task)?;
            bytes = add(bytes, value.id.len())?;
        }
        let page_memory = memory.scores.reserve(bytes)?;
        #[cfg(test)]
        tests::record_page();
        let mut candidates = Vec::with_capacity(selected.len());
        for Reverse(Candidate(value)) in selected {
            checkpoint(task)?;
            candidates.push(SearchScoredCandidate {
                id: value.id.to_owned(),
                score: value.score,
                vector_score: value.vector_score,
                text_score: value.text_score,
                rrf_score: value.rrf_score,
                vector_rrf_score: value.vector_rrf_score,
                text_rrf_score: value.text_rrf_score,
                vector_rank: value.vector_rank,
                text_rank: value.text_rank,
            });
        }
        checkpoint(task)?;
        Ok(Self {
            candidates: Admitted::new(candidates, page_memory),
            matching_count,
        })
    }
}

#[cfg(test)]
pub(super) mod tests;
