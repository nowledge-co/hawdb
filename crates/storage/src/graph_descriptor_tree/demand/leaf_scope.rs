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

//! Reuse of verified descriptor leaves across the prefix seeks of one scope.
//!
//! Every adjacency expansion seeks its `(direction, endpoint, type)` prefix
//! from the descriptor tree root: each page on the path is leased from the
//! shared cache and binary searched. Expansions of a label scan visit
//! endpoints in id order, so consecutive seeks usually land in the same leaf
//! (#1615).
//!
//! While a [`DescriptorLeafScope`] is open on a thread, retained-prefix scans
//! on that thread keep the last few leaves they reached: the selecting page
//! reference and the verified page view. A later prefix whose complete key
//! range lies strictly inside a retained leaf's key range (the leaf's first
//! key sorts before the prefix, and its last key sorts after every key that
//! starts with it) is answered from that leaf without the root traversal or
//! any cache lookup. Leaves partition the ordered key space, so no other leaf
//! can hold a matching key. A prefix that touches a leaf edge, spans leaves or
//! lies outside every retained leaf takes the ordinary root traversal.
//!
//! * Snapshots: a slot belongs to one opened tree root (one immutable artifact
//!   generation) and the codec limits that validated it. Reader clones share
//!   it; a reader of another generation or with other limits never matches.
//! * Integrity: only leaves reached by a complete verified read are retained.
//!   The reader's poison flag is checked before every scan.
//! * Memory: only cache-resident pages are retained, so their bytes are
//!   already charged to the cache capacity; retention only pins them. Pins are
//!   bounded by [`SCOPE_SLOTS`] and an eighth of the reader's cache capacity,
//!   and are released when the outermost scope on the thread closes.
//! * Concurrency: the state is thread-local and the guard cannot leave its
//!   thread, so readers on other threads neither share nor contend for it.
//!
//! Outside a scope retained-prefix scans behave exactly like ordinary prefix
//! scans and retain nothing.

use super::*;
use crate::graph_descriptor_page::GraphDescriptorPageLimits;
use std::cell::{Cell, RefCell};
use std::marker::PhantomData;

/// Leaves retained by one thread's open scope.
const SCOPE_SLOTS: usize = 8;

/// Retained pins never exceed this fraction of the reader's cache capacity.
const SCOPE_CAPACITY_DIVISOR: u64 = 8;

/// A verified leaf and the reference that selected it.
#[derive(Clone)]
pub(super) struct RetainedLeaf {
    pub(super) reference: Arc<GraphDescriptorPageRef>,
    pub(super) page: GraphDescriptorPageView,
}

impl RetainedLeaf {
    /// Every key starting with `prefix` lies strictly between this leaf's
    /// first and last keys, so this leaf alone holds all of them.
    fn strictly_contains_prefix(&self, prefix: &[u8]) -> bool {
        let first = self.reference.lower_bound.as_slice();
        let last = self.reference.upper_bound.as_slice();
        first < prefix && last > prefix && !last.starts_with(prefix)
    }
}

struct ScopeSlot {
    root: Arc<GraphDescriptorTreeRoot>,
    limits: GraphDescriptorPageLimits,
    leaf: RetainedLeaf,
}

impl ScopeSlot {
    fn belongs_to(
        &self,
        root: &Arc<GraphDescriptorTreeRoot>,
        limits: GraphDescriptorPageLimits,
    ) -> bool {
        Arc::ptr_eq(&self.root, root) && self.limits == limits
    }
}

struct ScopeState {
    depth: usize,
    // Least recently used first.
    slots: Vec<ScopeSlot>,
}

thread_local! {
    static SCOPE: RefCell<ScopeState> = const {
        RefCell::new(ScopeState {
            depth: 0,
            slots: Vec::new(),
        })
    };
    static RETAINED_LEAF_HITS: Cell<u64> = const { Cell::new(0) };
}

/// Lets retained-prefix descriptor scans on this thread reuse verified leaves
/// until the guard drops. Scopes nest; the outermost one releases the retained
/// leaves. Open one around a bounded unit of work, such as one query
/// execution, never around an idle handle.
#[must_use = "descriptor scans reuse leaves only while the scope is open"]
pub struct DescriptorLeafScope {
    // The state is per thread, so the guard must close on its own thread.
    _thread_bound: PhantomData<*const ()>,
}

impl DescriptorLeafScope {
    pub fn enter() -> Self {
        // During thread teardown there is no state; scans then retain nothing.
        let _ = SCOPE.try_with(|scope| {
            let mut scope = scope.borrow_mut();
            scope.depth = scope.depth.saturating_add(1);
        });
        Self {
            _thread_bound: PhantomData,
        }
    }
}

impl Drop for DescriptorLeafScope {
    fn drop(&mut self) {
        let released = SCOPE
            .try_with(|scope| {
                let mut scope = scope.borrow_mut();
                scope.depth = scope.depth.saturating_sub(1);
                if scope.depth == 0 {
                    std::mem::take(&mut scope.slots)
                } else {
                    Vec::new()
                }
            })
            .unwrap_or_default();
        // Releasing a final pin takes a cache shard lock; never under the borrow.
        drop(released);
    }
}

/// Prefix scans on this thread answered from a retained leaf. Diagnostic for
/// tests; always compiled so dependent crates' test builds still link.
#[doc(hidden)]
pub fn descriptor_retained_leaf_hits_for_test() -> u64 {
    RETAINED_LEAF_HITS.with(Cell::get)
}

/// The retained leaf of `root` that alone holds every key with `prefix`.
pub(super) fn retained_leaf(
    root: &Arc<GraphDescriptorTreeRoot>,
    limits: GraphDescriptorPageLimits,
    prefix: &[u8],
) -> Option<RetainedLeaf> {
    let leaf = SCOPE
        .try_with(|scope| {
            let mut scope = scope.borrow_mut();
            if scope.depth == 0 {
                return None;
            }
            let index = scope.slots.iter().rposition(|slot| {
                slot.belongs_to(root, limits) && slot.leaf.strictly_contains_prefix(prefix)
            })?;
            let last = scope.slots.len() - 1;
            if index != last {
                scope.slots[index..].rotate_left(1);
            }
            Some(scope.slots[last].leaf.clone())
        })
        .ok()
        .flatten();
    if leaf.is_some() {
        RETAINED_LEAF_HITS.with(|hits| hits.set(hits.get().saturating_add(1)));
    }
    leaf
}

/// Whether a scope is open on this thread.
pub(super) fn is_open() -> bool {
    SCOPE
        .try_with(|scope| scope.borrow().depth > 0)
        .unwrap_or(false)
}

/// Retains `leaf` for later scans of `root` when a scope is open, the page is
/// cache-resident and the pins stay within budget.
pub(super) fn retain_leaf(
    root: &Arc<GraphDescriptorTreeRoot>,
    limits: GraphDescriptorPageLimits,
    leaf: RetainedLeaf,
    cache_capacity_bytes: u64,
) {
    if !leaf.page.is_cache_resident() {
        return;
    }
    let budget = cache_capacity_bytes / SCOPE_CAPACITY_DIVISOR;
    let bytes = leaf.page.resident_charge();
    let evicted = SCOPE
        .try_with(|scope| {
            let mut scope = scope.borrow_mut();
            if scope.depth == 0 || bytes > budget {
                return (Vec::new(), Some(leaf));
            }
            let mut evicted = Vec::new();
            // Leaves of one tree never overlap; keep one slot per page.
            if let Some(index) = scope.slots.iter().position(|slot| {
                slot.belongs_to(root, limits)
                    && slot.leaf.reference.page_id == leaf.reference.page_id
            }) {
                evicted.push(scope.slots.remove(index));
            }
            let mut retained = scope
                .slots
                .iter()
                .map(|slot| slot.leaf.page.resident_charge())
                .fold(bytes, u64::saturating_add);
            while !scope.slots.is_empty() && (scope.slots.len() >= SCOPE_SLOTS || retained > budget)
            {
                let slot = scope.slots.remove(0);
                retained = retained.saturating_sub(slot.leaf.page.resident_charge());
                evicted.push(slot);
            }
            scope.slots.push(ScopeSlot {
                root: Arc::clone(root),
                limits,
                leaf,
            });
            (evicted, None)
        })
        .unwrap_or_default();
    // Final pin releases take cache shard locks; never under the borrow.
    drop(evicted);
}

/// Retained leaves on this thread (tests only).
#[cfg(test)]
pub(super) fn retained_leaves() -> usize {
    SCOPE.with(|scope| scope.borrow().slots.len())
}
