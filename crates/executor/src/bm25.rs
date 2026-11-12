// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Okapi BM25 scoring and exact Block-Max WAND top-k retrieval.
//!
//! This is the one BM25 scorer shared by the graph-side lexical projection
//! and the SQL search index access path: the same constants, the same IDF and
//! the same floating-point operations in the same order, so a score computed
//! by either path is bit-identical to a brute-force evaluation that sums
//! per-term contributions in query-term order.
//!
//! [`block_max_wand`] finds the exact top-k of a disjunctive query over
//! posting cursors that expose per-block upper bounds (Ding and Suel, "Faster
//! top-k document retrieval using block-max indexes", SIGIR 2011). The
//! posting format stays behind [`Bm25PostingCursor`]: the kernel never sees
//! storage rows, only ordinals, frequencies and block bounds. Documents are
//! ranked by score descending, ties broken by ascending encoded key, which
//! is a total order; the result is therefore exactly the first `k` entries of
//! a full ranking, whatever was pruned.

use hawdb_core::{HawDBError, Result};
use std::collections::BinaryHeap;

/// Term-frequency saturation.
pub const BM25_K1: f64 = 1.2;
/// Document-length normalization.
pub const BM25_B: f64 = 0.75;

/// Relative margin added to every block upper bound. Bounds and scores are
/// each a handful of correctly rounded operations, so their rounding error is
/// a few units in the last place (about 1e-15 relative); a bound inflated by
/// this margin is never below the score of a document it covers, whatever
/// the summation order. The cost is pruning nothing within 1e-12 of the floor.
pub const BM25_UPPER_BOUND_MARGIN: f64 = 1e-12;

/// Lucene's non-negative IDF, `ln(1 + (N - n + 0.5) / (n + 0.5))`.
pub fn idf(documents: u64, document_frequency: u64) -> f64 {
    (1.0 + (documents as f64 - document_frequency as f64 + 0.5) / (document_frequency as f64 + 0.5))
        .ln()
}

/// One term's contribution to a document's score.
pub fn term_score(idf: f64, frequency: f64, document_length: f64, average_length: f64) -> f64 {
    let denominator =
        frequency + BM25_K1 * (1.0 - BM25_B + BM25_B * document_length / average_length.max(1.0));
    idf * (frequency * (BM25_K1 + 1.0)) / denominator
}

/// An upper bound on [`term_score`] for every posting with a frequency of at
/// most `max_frequency` in a document of at least `min_length` tokens. BM25
/// grows with frequency and falls with length, so the corner is the bound.
pub fn term_upper_bound(idf: f64, max_frequency: u64, min_length: u64, average_length: f64) -> f64 {
    term_score(idf, max_frequency as f64, min_length as f64, average_length)
        * (1.0 + BM25_UPPER_BOUND_MARGIN)
}

/// The skip entry of the block that serves a cursor's next posting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bm25Block {
    /// The block's last posting ordinal.
    pub last: u64,
    /// No posting in the block has a higher frequency.
    pub max_frequency: u64,
    /// No document in the block is shorter.
    pub min_length: u64,
}

/// An ordinal-ordered posting list read block by block.
///
/// `position` is a lower bound on the next posting's ordinal; it becomes
/// exact after [`Self::materialize`]. [`Self::skip_to`] and
/// [`Self::block_at`] must not decode blocks: skipping a block whose bound
/// cannot reach the top-k is the point of the algorithm.
pub trait Bm25PostingCursor {
    /// Lower bound on the next posting's ordinal; `None` when exhausted.
    fn position(&self) -> Option<u64>;
    /// Whether `position` is the ordinal of an actual posting.
    fn is_exact(&self) -> bool;
    /// The block holding the first posting at or after `target` (at least
    /// `position`), without decoding it; `None` past the last block.
    fn block_at(&mut self, target: u64) -> Result<Option<Bm25Block>>;
    /// Raises `position` to `target` without decoding.
    fn skip_to(&mut self, target: u64) -> Result<()>;
    /// Decodes as needed so that `position` is the next posting's exact
    /// ordinal (or `None`).
    fn materialize(&mut self) -> Result<()>;
    /// The exact current posting's term frequency and its block's bounds.
    fn current(&self) -> Result<(u64, Bm25Block)>;
    /// A block of this term whose upper bound is below `threshold` cannot
    /// hold a top-k document, whatever the other terms contribute. Cursors
    /// may use it to avoid reading such blocks ahead; it never changes the
    /// result. Called whenever the floor rises.
    fn competitive_threshold(&mut self, _threshold: f64) {}
}

/// One query term: its cursor, IDF and the bound over all its blocks.
pub struct Bm25Term<C> {
    pub cursor: C,
    pub idf: f64,
    /// At least [`term_upper_bound`] of every block of the term.
    pub max_bound: f64,
}

/// Per-document state and the scan's admission hooks.
pub trait Bm25Documents {
    /// The token count of a document that has postings.
    fn length(&mut self, ordinal: u64) -> Result<u64>;
    /// The document's encoded key, the tie-breaker (ascending).
    fn key(&mut self, ordinal: u64) -> Result<Vec<u8>>;
    /// Cancellation and work admission, called once per loop iteration.
    fn checkpoint(&mut self) -> Result<()>;
    /// Admits `bytes` of retained heap state before it is allocated.
    fn admit(&mut self, bytes: usize) -> Result<()>;
    /// Returns bytes of an evicted heap entry.
    fn release(&mut self, bytes: usize);
}

/// One ranked document.
#[derive(Debug, Clone, PartialEq)]
pub struct Bm25Hit {
    pub ordinal: u64,
    pub key: Vec<u8>,
    pub score: f64,
}

impl Bm25Hit {
    /// Retained bytes of a heap entry, admitted before it is allocated.
    pub fn retained_bytes(key_len: usize) -> usize {
        std::mem::size_of::<Self>().saturating_add(key_len)
    }

    fn worse_than(&self, score: f64, key: &[u8]) -> bool {
        self.score
            .total_cmp(&score)
            .then_with(|| key.cmp(&self.key))
            .is_lt()
    }
}

/// Heap order: the worst retained hit (lowest score, then greatest key) on top.
struct Worst(Bm25Hit);

impl PartialEq for Worst {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for Worst {}

impl PartialOrd for Worst {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Worst {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .0
            .score
            .total_cmp(&self.0.score)
            .then_with(|| self.0.key.cmp(&other.0.key))
    }
}

/// Work the kernel did, for query reports and EXPLAIN ANALYZE.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Bm25KernelStats {
    /// Documents whose exact score was computed.
    pub documents_scored: u64,
    /// Candidate documents whose block bounds fell below the floor, so the
    /// cursors jumped past the blocks without decoding them.
    pub block_skips: u64,
    /// Hits that entered the k-sized heap (including later evictions).
    pub heap_insertions: u64,
    /// The scan ended because no remaining document could reach the floor.
    pub terminated_early: bool,
}

/// The exact top-k and the work it took.
#[derive(Debug, Clone, PartialEq)]
pub struct Bm25TopK {
    /// Best first: score descending, then key ascending.
    pub hits: Vec<Bm25Hit>,
    pub stats: Bm25KernelStats,
}

/// Exact top-`k` of the disjunction of `terms` by BM25, with Block-Max WAND
/// pruning once `k` documents are retained. `terms` are in query-term order,
/// which is the order a document's contributions are summed in.
pub fn block_max_wand<C: Bm25PostingCursor, D: Bm25Documents>(
    terms: &mut [Bm25Term<C>],
    average_length: f64,
    k: usize,
    documents: &mut D,
) -> Result<Bm25TopK> {
    let mut stats = Bm25KernelStats::default();
    if k == 0 {
        return Ok(Bm25TopK {
            hits: Vec::new(),
            stats,
        });
    }
    let capacity = k.min(1 << 20);
    documents.admit(capacity.saturating_mul(std::mem::size_of::<Worst>()))?;
    let mut heap = BinaryHeap::<Worst>::with_capacity(capacity);
    let mut order: Vec<usize> = Vec::with_capacity(terms.len());
    let mut in_group = vec![false; terms.len()];
    let total_bound = terms.iter().fold(0.0, |sum, term| sum + term.max_bound);
    loop {
        documents.checkpoint()?;
        order.clear();
        order.extend((0..terms.len()).filter(|&term| terms[term].cursor.position().is_some()));
        if order.is_empty() {
            break;
        }
        let position = |terms: &[Bm25Term<C>], term: usize| {
            terms[term]
                .cursor
                .position()
                .expect("active cursors have a position")
        };
        order.sort_by_key(|&term| (position(terms, term), term));
        let floor = (heap.len() >= k).then(|| heap.peek().map(|worst| worst.0.score));
        let floor = floor.flatten();
        let pivot = match floor {
            None => position(terms, order[0]),
            Some(floor) => {
                // WAND pivot: the first candidate whose cursors (all those at
                // or before it) can together reach the floor. Every document
                // before it scores below the floor. Bounds are summed in term
                // order, like scores.
                let reach = |candidate: u64| {
                    terms
                        .iter()
                        .fold(0.0, |sum, term| match term.cursor.position() {
                            Some(position) if position <= candidate => sum + term.max_bound,
                            _ => sum,
                        })
                };
                let pivot_index =
                    order.partition_point(|&term| reach(position(terms, term)) < floor);
                let Some(&pivot_term) = order.get(pivot_index) else {
                    stats.terminated_early = true;
                    break;
                };
                position(terms, pivot_term)
            }
        };
        // The pivot group: every cursor at or before the pivot.
        let group_len = order.partition_point(|&term| position(terms, term) <= pivot);
        if let Some(floor) = floor {
            // Block-max check: the blocks that hold the group's postings at
            // the pivot bound every document from the pivot up to the end of
            // the first of those blocks.
            let mut bound = 0.0;
            let mut boundary = u64::MAX;
            for flag in in_group.iter_mut() {
                *flag = false;
            }
            for &term in &order[..group_len] {
                in_group[term] = true;
            }
            for (term, &member) in terms.iter_mut().zip(&in_group) {
                if !member {
                    continue;
                }
                if let Some(block) = term.cursor.block_at(pivot)? {
                    bound += term_upper_bound(
                        term.idf,
                        block.max_frequency,
                        block.min_length,
                        average_length,
                    );
                    boundary = boundary.min(block.last.saturating_add(1));
                }
            }
            if bound < floor {
                // No document in [pivot, boundary) can reach the floor, nor
                // any before the next cursor outside the group.
                let target = order
                    .get(group_len)
                    .map_or(boundary, |&next| boundary.min(position(terms, next)));
                if target <= pivot {
                    return Err(HawDBError::StorageIntegrity(
                        "BM25 block bounds do not advance past their pivot".into(),
                    ));
                }
                for &term in &order[..group_len] {
                    terms[term].cursor.skip_to(target)?;
                }
                stats.block_skips = stats.block_skips.saturating_add(1);
                continue;
            }
        }
        if position(terms, order[0]) < pivot {
            // Documents before the pivot cannot reach the floor.
            for &term in &order[..group_len] {
                if position(terms, term) < pivot {
                    terms[term].cursor.skip_to(pivot)?;
                }
            }
            continue;
        }
        // Every group cursor is at the pivot; make that exact before scoring.
        let mut moved = false;
        for &term in &order[..group_len] {
            let cursor = &mut terms[term].cursor;
            if !cursor.is_exact() {
                cursor.materialize()?;
                moved |= cursor.position() != Some(pivot);
            }
        }
        if moved {
            continue;
        }
        let length = documents.length(pivot)?;
        let mut score = 0.0;
        for flag in in_group.iter_mut() {
            *flag = false;
        }
        for &term in &order[..group_len] {
            in_group[term] = true;
        }
        for (term, &member) in terms.iter().zip(&in_group) {
            if !member {
                continue;
            }
            let (frequency, block) = term.cursor.current()?;
            if frequency > block.max_frequency || length < block.min_length {
                return Err(HawDBError::StorageIntegrity(
                    "BM25 posting exceeds its block bounds".into(),
                ));
            }
            score += term_score(term.idf, frequency as f64, length as f64, average_length);
        }
        stats.documents_scored = stats.documents_scored.saturating_add(1);
        if !score.is_finite() || score <= 0.0 {
            return Err(HawDBError::StorageIntegrity(
                "BM25 score is not a positive finite number".into(),
            ));
        }
        let full = heap.len() >= k;
        let competitive = match heap.peek() {
            Some(worst) if full => worst.0.score <= score,
            _ => true,
        };
        if competitive {
            let key = documents.key(pivot)?;
            let enters = match heap.peek() {
                Some(worst) if full => worst.0.worse_than(score, &key),
                _ => true,
            };
            if enters {
                documents.admit(Bm25Hit::retained_bytes(key.len()))?;
                if full && let Some(evicted) = heap.pop() {
                    documents.release(Bm25Hit::retained_bytes(evicted.0.key.len()));
                }
                heap.push(Worst(Bm25Hit {
                    ordinal: pivot,
                    key,
                    score,
                }));
                stats.heap_insertions = stats.heap_insertions.saturating_add(1);
                if heap.len() >= k
                    && let Some(worst) = heap.peek()
                {
                    let floor = worst.0.score;
                    for term in terms.iter_mut() {
                        let others = total_bound - term.max_bound;
                        term.cursor.competitive_threshold(floor - others);
                    }
                }
            }
        }
        let next = pivot
            .checked_add(1)
            .ok_or_else(|| HawDBError::StorageIntegrity("BM25 ordinal overflow".into()))?;
        for &term in &order[..group_len] {
            terms[term].cursor.skip_to(next)?;
        }
    }
    let mut hits = heap.into_iter().map(|worst| worst.0).collect::<Vec<_>>();
    hits.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.key.cmp(&right.key))
    });
    Ok(Bm25TopK { hits, stats })
}

#[cfg(test)]
mod tests;
