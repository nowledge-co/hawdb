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

//! Per-query posting-block ranges resolved by binary search over the manifest.
//!
//! Manifest validation guarantees that every document block precedes every
//! posting block, and that posting blocks are ordered with non-decreasing,
//! non-overlapping key ranges: a block's `max_key` never exceeds the next
//! block's `min_key`, which it may equal only when one term's postings span
//! the boundary. The blocks covering one term are therefore contiguous, and
//! two partition points find them in `O(log n)` key comparisons instead of a
//! linear manifest scan per term.

use super::{query_term, BlockDescriptor, BlockKind, LexicalProjectionReader};
use std::borrow::Borrow;
use std::collections::BTreeSet;
use std::ops::Range;

#[cfg(test)]
thread_local! {
    /// Manifest key comparisons made while resolving posting-block ranges.
    pub(super) static MANIFEST_KEY_COMPARISONS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

#[inline]
fn counted_key_comparison(result: bool) -> bool {
    #[cfg(test)]
    MANIFEST_KEY_COMPARISONS.with(|count| count.set(count.get() + 1));
    result
}

impl LexicalProjectionReader {
    /// Manifest index of the first posting block.
    fn posting_blocks_start(&self) -> usize {
        self.manifest
            .blocks
            .partition_point(|block| block.kind == BlockKind::Documents)
    }

    /// The document mapping blocks, which validation places before postings.
    pub(super) fn document_blocks(&self) -> &[BlockDescriptor] {
        &self.manifest.blocks[..self.posting_blocks_start()]
    }

    /// Manifest indices of the posting blocks whose key range covers `term`.
    pub(super) fn posting_block_range(&self, term: &str) -> Range<usize> {
        let start = self.posting_blocks_start();
        let postings = &self.manifest.blocks[start..];
        let end = postings
            .partition_point(|block| counted_key_comparison(block.min_key.as_str() <= term));
        let first = postings[..end]
            .partition_point(|block| counted_key_comparison(block.max_key.as_str() < term));
        start + first..start + end
    }

    /// One range per query term, in the set's iteration order. Because terms
    /// and blocks are both ordered, both range bounds are non-decreasing.
    pub(super) fn posting_block_ranges<T: Borrow<str> + Ord>(
        &self,
        query_terms: &BTreeSet<T>,
    ) -> Vec<Range<usize>> {
        query_terms
            .iter()
            .map(|term| self.posting_block_range(query_term(term)))
            .collect()
    }

    pub(super) fn posting_blocks_in(&self, range: Range<usize>) -> &[BlockDescriptor] {
        &self.manifest.blocks[range]
    }

    /// Largest encoded posting block any query term will read.
    pub(super) fn statistics_block_bytes(&self, ranges: &[Range<usize>]) -> u64 {
        ranges
            .iter()
            .flat_map(|range| self.posting_blocks_in(range.clone()))
            .map(|block| block.length)
            .max()
            .unwrap_or_default()
    }
}
