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

use super::*;
use std::mem::{align_of, size_of};
use std::ptr::NonNull;

/// Conservative node coverage for the pinned Rust 1.97.1 BTreeMap insertion
/// path, independent of key/value heap buffers (which have their own leases).
/// Allocator bookkeeping/rounding and standard insertion latency remain
/// assumptions, as do the pinned standard-library node/split implementation.
#[derive(Default)]
pub(super) struct MapMemory {
    admitted_nodes: usize,
}

impl MapMemory {
    pub(super) fn node_bytes() -> usize {
        // Rust 1.97.1 uses B=6: 11 keys/values, parent pointer, parent index,
        // length, and 12 child pointers in an internal node. Bound leaf field
        // reordering and both struct tails with eight worst alignment gaps.
        let alignment = align_of::<String>()
            .max(align_of::<Value>())
            .max(align_of::<NonNull<u8>>())
            .max(align_of::<u16>());
        size_of::<Option<NonNull<u8>>>()
            + 2 * size_of::<u16>()
            + 11 * (size_of::<String>() + size_of::<Value>())
            + 12 * size_of::<NonNull<u8>>()
            + 8 * (alignment - 1)
    }

    pub(super) fn before_insert(&mut self, len: usize, work: &DecodeContext) -> Result<()> {
        // A finished non-root node has at least five keys. During splitting,
        // one insertion can temporarily leave four. Cover a special root and
        // one freshly allocated empty split node separately; other allocated
        // nodes hold >=4 of the at-most len+1 keys. No map removals occur here.
        let keys = len
            .checked_add(1)
            .ok_or_else(|| allocation("WAL map cardinality overflows usize", usize::MAX, work))?;
        let nodes = keys / 4 + 2;
        if nodes > self.admitted_nodes {
            let bytes = (nodes - self.admitted_nodes)
                .checked_mul(Self::node_bytes())
                .ok_or_else(|| {
                    allocation("WAL map node capacity overflows usize", usize::MAX, work)
                })?;
            // The record inventory keeps coverage across insertion, moves,
            // replay and snapshots. No untracked per-map token list is needed.
            let _token = work.reserve(bytes)?;
            self.admitted_nodes = nodes;
        }
        Ok(())
    }
}
