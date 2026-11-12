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

//! Reuse of selected canonical segments across the point reads of one scope.
//!
//! Every canonical point read selects its segment from the descriptor tree,
//! then leases the verified segment bytes and point directory from the shared
//! segment cache. Expansions read thousands of clustered target ids, so
//! consecutive reads usually select the same segment again (#1482).
//!
//! While a [`CanonicalPointReadScope`] is open on a thread, point reads on that
//! thread keep the last few selected segments: the validated descriptor, the
//! verified bytes lease and the point directory lease. A read whose id falls in
//! a retained segment of the same reader skips the descriptor seek and both
//! cache lookups; integrity checks that run per read (segment header, framing,
//! record decode) still run.
//!
//! * Snapshots: a slot belongs to one opened reader (one immutable artifact
//!   generation). Reader clones share it; a reader opened for another
//!   generation never matches it.
//! * Memory: only cache-resident leases are retained, so the bytes are already
//!   charged to the cache capacity; retention only pins them. Pins are bounded
//!   by [`SCOPE_SLOTS`] and a quarter of the reader's cache capacity, and are
//!   released when the outermost scope on the thread closes. Detached bytes
//!   (admission refused) serve one read only, as before.
//! * Concurrency: the state is thread-local and the guard cannot leave its
//!   thread, so readers on other threads neither share nor contend for it.
//!
//! Outside a scope point reads behave exactly as before and retain nothing.

use super::*;
use crate::cache::SegmentBytes;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;

/// Segments retained by one thread's open scope.
const SCOPE_SLOTS: usize = 4;

/// Retained pins never exceed this fraction of the reader's cache capacity.
const SCOPE_CAPACITY_DIVISOR: u64 = 4;

/// A selected segment and the leases one point read needs from it. Reads
/// share a retained segment through an `Rc`, so a hit costs one non-atomic
/// count instead of cloning and dropping both pinned cache leases.
pub(super) struct PointSegment {
    pub(super) descriptor: CanonicalSegmentDescriptor,
    pub(super) bytes: SegmentBytes,
    /// Present for segments of more than one directory stride. A directory is
    /// only built after every record of the segment passed validation.
    pub(super) directory: Option<SegmentBytes>,
}

impl PointSegment {
    fn contains(&self, kind: CanonicalSegmentKind, id: u64) -> bool {
        self.descriptor.kind == kind
            && self.descriptor.min_record_id <= id
            && id <= self.descriptor.max_record_id
    }

    fn retained_bytes(&self) -> u64 {
        let directory = self
            .directory
            .as_ref()
            .map_or(0, |directory| directory.len());
        (self.bytes.len() as u64).saturating_add(directory as u64)
    }
}

struct ScopeSlot {
    reader: u64,
    segment: Rc<PointSegment>,
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
    static DESCRIPTOR_SEEKS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Lets canonical point reads on this thread reuse their selected segments
/// until the guard drops. Scopes nest; the outermost one releases the
/// retained leases. Open one around a bounded unit of work, such as one query
/// execution, never around an idle handle.
#[must_use = "point reads reuse segments only while the scope is open"]
pub struct CanonicalPointReadScope {
    // The state is per thread, so the guard must close on its own thread.
    _thread_bound: PhantomData<*const ()>,
}

impl CanonicalPointReadScope {
    pub fn enter() -> Self {
        // During thread teardown there is no state; reads then retain nothing.
        let _ = SCOPE.try_with(|scope| {
            let mut scope = scope.borrow_mut();
            scope.depth = scope.depth.saturating_add(1);
        });
        Self {
            _thread_bound: PhantomData,
        }
    }
}

impl Drop for CanonicalPointReadScope {
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

/// Canonical descriptor-tree seeks by point reads on this thread. Diagnostic
/// for tests; always compiled so test builds without the `test-support`
/// feature still link.
#[doc(hidden)]
pub fn point_descriptor_seeks_for_test() -> u64 {
    DESCRIPTOR_SEEKS.with(std::cell::Cell::get)
}

pub(super) fn record_descriptor_seek() {
    DESCRIPTOR_SEEKS.with(|seeks| seeks.set(seeks.get().saturating_add(1)));
}

/// The retained segment of `reader` that covers `id`, if a scope is open.
pub(super) fn retained_segment(
    reader: u64,
    kind: CanonicalSegmentKind,
    id: u64,
) -> Option<Rc<PointSegment>> {
    SCOPE
        .try_with(|scope| {
            let mut scope = scope.borrow_mut();
            if scope.depth == 0 {
                return None;
            }
            let index = scope
                .slots
                .iter()
                .rposition(|slot| slot.reader == reader && slot.segment.contains(kind, id))?;
            let last = scope.slots.len() - 1;
            if index != last {
                scope.slots[index..].rotate_left(1);
            }
            Some(Rc::clone(&scope.slots[last].segment))
        })
        .ok()
        .flatten()
}

/// Retains `segment` for later reads of `reader` when a scope is open, every
/// lease is cache-resident and the pins stay within budget.
pub(super) fn retain_segment(reader: u64, segment: &Rc<PointSegment>, cache_capacity_bytes: u64) {
    if !segment.bytes.is_cache_resident()
        || segment
            .directory
            .as_ref()
            .is_some_and(|directory| !directory.is_cache_resident())
    {
        return;
    }
    let budget = cache_capacity_bytes / SCOPE_CAPACITY_DIVISOR;
    let evicted = SCOPE
        .try_with(|scope| {
            let mut scope = scope.borrow_mut();
            if scope.depth == 0 || segment.retained_bytes() > budget {
                return Vec::new();
            }
            let mut evicted = Vec::new();
            // Segments of one reader never overlap, so a covering slot is the
            // same segment; keep one slot per segment.
            if let Some(index) = scope.slots.iter().position(|slot| {
                slot.reader == reader
                    && slot.segment.descriptor.segment_id == segment.descriptor.segment_id
            }) {
                evicted.push(scope.slots.remove(index));
            }
            let mut retained = scope
                .slots
                .iter()
                .map(|slot| slot.segment.retained_bytes())
                .fold(segment.retained_bytes(), u64::saturating_add);
            while !scope.slots.is_empty() && (scope.slots.len() >= SCOPE_SLOTS || retained > budget)
            {
                let slot = scope.slots.remove(0);
                retained = retained.saturating_sub(slot.segment.retained_bytes());
                evicted.push(slot);
            }
            scope.slots.push(ScopeSlot {
                reader,
                segment: Rc::clone(segment),
            });
            evicted
        })
        .unwrap_or_default();
    drop(evicted);
}

/// Retained segments on this thread (tests only).
#[cfg(test)]
pub(super) fn retained_segments() -> usize {
    SCOPE.with(|scope| scope.borrow().slots.len())
}
