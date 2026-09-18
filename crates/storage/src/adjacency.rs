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

use crate::{NodeId, RelId};
use hawdb_core::RelTypeId;
use std::collections::{btree_map, btree_set, BTreeMap, BTreeSet};
use std::iter::{FusedIterator, Peekable};
use std::sync::{Arc, OnceLock};

pub const ADJACENCY_PIVOT_MIN_DEGREE: usize = 64;
pub const ADJACENCY_MINI_DELTA_MAX_ENTRIES: usize = 64;
pub const ADJACENCY_DELTA_CONSOLIDATION_ENTRIES: usize = ADJACENCY_MINI_DELTA_MAX_ENTRIES * 2;
pub const ADJACENCY_DELTA_HARD_MAX_ENTRIES: usize = ADJACENCY_MINI_DELTA_MAX_ENTRIES * 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct OrderedAdjacencyEntry {
    pub neighbor_id: NodeId,
    pub relationship_id: RelId,
}

/// A snapshot-friendly adjacency posting list with an immutable pivot and a
/// bounded mini-delta for high-degree mutations.
///
/// Unshared pivots and small shared pivots retain the direct `BTreeSet` update
/// path. A shared high-degree pivot records overrides in a mini-delta, avoiding
/// a full posting-list clone for every mutation while an older snapshot is
/// alive. Consolidation materializes one replacement pivot after a bounded
/// number of overrides. Full scans merge the pivot and delta blocks without
/// materializing a replacement pivot. The legacy borrowed iterator keeps a
/// lazily materialized view for API compatibility.
#[derive(Debug, Clone)]
pub struct AdjacencyPostingList {
    state: Arc<AdjacencyPostingState>,
}

#[derive(Debug)]
enum AdjacencyPostingState {
    Pivot(BTreeSet<OrderedAdjacencyEntry>),
    Delta {
        pivot: Arc<AdjacencyPostingState>,
        sealed: Vec<Arc<BTreeMap<OrderedAdjacencyEntry, bool>>>,
        active: BTreeMap<OrderedAdjacencyEntry, bool>,
        len: usize,
        read_view: OnceLock<Arc<BTreeSet<OrderedAdjacencyEntry>>>,
    },
}

impl Clone for AdjacencyPostingState {
    fn clone(&self) -> Self {
        match self {
            Self::Pivot(pivot) => Self::Pivot(pivot.clone()),
            Self::Delta {
                pivot,
                sealed,
                active,
                len,
                ..
            } => Self::Delta {
                pivot: Arc::clone(pivot),
                sealed: sealed.clone(),
                active: active.clone(),
                len: *len,
                read_view: OnceLock::new(),
            },
        }
    }
}

impl Default for AdjacencyPostingList {
    fn default() -> Self {
        Self {
            state: Arc::new(AdjacencyPostingState::Pivot(BTreeSet::new())),
        }
    }
}

impl From<BTreeSet<OrderedAdjacencyEntry>> for AdjacencyPostingList {
    fn from(pivot: BTreeSet<OrderedAdjacencyEntry>) -> Self {
        Self {
            state: Arc::new(AdjacencyPostingState::Pivot(pivot)),
        }
    }
}

impl PartialEq for AdjacencyPostingList {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter_copied().eq(other.iter_copied())
    }
}

impl Eq for AdjacencyPostingList {}

impl AdjacencyPostingList {
    #[inline]
    pub fn len(&self) -> usize {
        match self.state.as_ref() {
            AdjacencyPostingState::Pivot(pivot) => pivot.len(),
            AdjacencyPostingState::Delta { len, .. } => *len,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn contains(&self, entry: &OrderedAdjacencyEntry) -> bool {
        match self.state.as_ref() {
            AdjacencyPostingState::Pivot(pivot) => pivot.contains(entry),
            AdjacencyPostingState::Delta {
                pivot,
                sealed,
                active,
                ..
            } => override_presence(sealed, active, entry)
                .unwrap_or_else(|| pivot_set(pivot).contains(entry)),
        }
    }

    pub fn insert(&mut self, entry: OrderedAdjacencyEntry) -> bool {
        if self.contains(&entry) {
            return false;
        }
        match self.state.as_ref() {
            AdjacencyPostingState::Pivot(pivot)
                if Arc::strong_count(&self.state) > 1
                    && pivot.len() >= ADJACENCY_PIVOT_MIN_DEGREE =>
            {
                self.state = Arc::new(AdjacencyPostingState::Delta {
                    pivot: Arc::clone(&self.state),
                    sealed: Vec::new(),
                    active: BTreeMap::from([(entry, true)]),
                    len: pivot.len().saturating_add(1),
                    read_view: OnceLock::new(),
                });
            }
            AdjacencyPostingState::Pivot(_) => {
                let AdjacencyPostingState::Pivot(pivot) = Arc::make_mut(&mut self.state) else {
                    unreachable!("matched pivot state");
                };
                pivot.insert(entry);
            }
            AdjacencyPostingState::Delta { .. } => {
                let AdjacencyPostingState::Delta {
                    pivot,
                    sealed,
                    active,
                    len,
                    read_view,
                } = Arc::make_mut(&mut self.state)
                else {
                    unreachable!("matched delta state");
                };
                read_view.take();
                if presence_before_active(pivot, sealed, &entry) {
                    active.remove(&entry);
                } else {
                    active.insert(entry, true);
                }
                *len = len.saturating_add(1);
            }
        }
        self.finish_mutation();
        true
    }

    pub fn remove(&mut self, entry: &OrderedAdjacencyEntry) -> bool {
        if !self.contains(entry) {
            return false;
        }
        match self.state.as_ref() {
            AdjacencyPostingState::Pivot(pivot)
                if Arc::strong_count(&self.state) > 1
                    && pivot.len() >= ADJACENCY_PIVOT_MIN_DEGREE =>
            {
                self.state = Arc::new(AdjacencyPostingState::Delta {
                    pivot: Arc::clone(&self.state),
                    sealed: Vec::new(),
                    active: BTreeMap::from([(*entry, false)]),
                    len: pivot.len().saturating_sub(1),
                    read_view: OnceLock::new(),
                });
            }
            AdjacencyPostingState::Pivot(_) => {
                let AdjacencyPostingState::Pivot(pivot) = Arc::make_mut(&mut self.state) else {
                    unreachable!("matched pivot state");
                };
                pivot.remove(entry);
            }
            AdjacencyPostingState::Delta { .. } => {
                let AdjacencyPostingState::Delta {
                    pivot,
                    sealed,
                    active,
                    len,
                    read_view,
                } = Arc::make_mut(&mut self.state)
                else {
                    unreachable!("matched delta state");
                };
                read_view.take();
                if presence_before_active(pivot, sealed, entry) {
                    active.insert(*entry, false);
                } else {
                    active.remove(entry);
                }
                *len = len.saturating_sub(1);
            }
        }
        self.finish_mutation();
        true
    }

    #[inline]
    pub fn iter(&self) -> btree_set::Iter<'_, OrderedAdjacencyEntry> {
        self.read_view().iter()
    }

    /// Iterates adjacency entries in neighbor/relationship order without
    /// constructing a full read view for delta-backed postings.
    pub fn iter_copied(&self) -> AdjacencyPostingIter<'_> {
        AdjacencyPostingIter::new(self)
    }

    pub fn pivot_len(&self) -> usize {
        match self.state.as_ref() {
            AdjacencyPostingState::Pivot(pivot) => pivot.len(),
            AdjacencyPostingState::Delta { pivot, .. } => pivot_set(pivot).len(),
        }
    }

    pub fn mini_delta_len(&self) -> usize {
        match self.state.as_ref() {
            AdjacencyPostingState::Pivot(_) => 0,
            AdjacencyPostingState::Delta { sealed, active, .. } => {
                sealed.iter().map(|block| block.len()).sum::<usize>() + active.len()
            }
        }
    }

    pub fn sealed_delta_count(&self) -> usize {
        match self.state.as_ref() {
            AdjacencyPostingState::Pivot(_) => 0,
            AdjacencyPostingState::Delta { sealed, .. } => sealed.len(),
        }
    }

    pub fn needs_consolidation(&self) -> bool {
        self.mini_delta_len() >= ADJACENCY_DELTA_CONSOLIDATION_ENTRIES
    }

    pub fn consolidate(&mut self) -> bool {
        if !matches!(self.state.as_ref(), AdjacencyPostingState::Delta { .. }) {
            return false;
        }
        self.state = Arc::new(AdjacencyPostingState::Pivot(self.materialize()));
        true
    }

    pub fn shares_pivot_with(&self, other: &Self) -> bool {
        std::ptr::eq(self.pivot_identity(), other.pivot_identity())
    }

    fn finish_mutation(&mut self) {
        if let AdjacencyPostingState::Delta {
            sealed,
            active,
            read_view,
            ..
        } = Arc::make_mut(&mut self.state)
            && active.len() >= ADJACENCY_MINI_DELTA_MAX_ENTRIES
        {
            read_view.take();
            sealed.push(Arc::new(std::mem::take(active)));
        }

        let next_state = match self.state.as_ref() {
            AdjacencyPostingState::Pivot(_) => None,
            AdjacencyPostingState::Delta {
                pivot,
                sealed,
                active,
                ..
            } if sealed.is_empty() && active.is_empty() => Some(Arc::clone(pivot)),
            AdjacencyPostingState::Delta { sealed, active, .. }
                if sealed.iter().map(|block| block.len()).sum::<usize>() + active.len()
                    >= ADJACENCY_DELTA_HARD_MAX_ENTRIES =>
            {
                Some(Arc::new(AdjacencyPostingState::Pivot(self.materialize())))
            }
            AdjacencyPostingState::Delta { .. } => None,
        };
        if let Some(next_state) = next_state {
            self.state = next_state;
        }
    }

    fn materialize(&self) -> BTreeSet<OrderedAdjacencyEntry> {
        self.iter_copied().collect()
    }

    #[inline]
    fn read_view(&self) -> &BTreeSet<OrderedAdjacencyEntry> {
        match self.state.as_ref() {
            AdjacencyPostingState::Pivot(pivot) => pivot,
            AdjacencyPostingState::Delta { read_view, .. } => read_view
                .get_or_init(|| Arc::new(self.materialize()))
                .as_ref(),
        }
    }

    fn pivot_identity(&self) -> *const AdjacencyPostingState {
        match self.state.as_ref() {
            AdjacencyPostingState::Pivot(_) => Arc::as_ptr(&self.state),
            AdjacencyPostingState::Delta { pivot, .. } => Arc::as_ptr(pivot),
        }
    }
}

pub struct AdjacencyPostingIter<'a> {
    inner: AdjacencyPostingIterInner<'a>,
}

enum AdjacencyPostingIterInner<'a> {
    Pivot(btree_set::Iter<'a, OrderedAdjacencyEntry>),
    Delta(DeltaAdjacencyPostingIter<'a>),
}

struct DeltaAdjacencyPostingIter<'a> {
    pivot: Peekable<btree_set::Iter<'a, OrderedAdjacencyEntry>>,
    deltas: Vec<Peekable<btree_map::Iter<'a, OrderedAdjacencyEntry, bool>>>,
    remaining: usize,
}

impl<'a> AdjacencyPostingIter<'a> {
    fn new(posting: &'a AdjacencyPostingList) -> Self {
        match posting.state.as_ref() {
            AdjacencyPostingState::Pivot(pivot) => Self {
                inner: AdjacencyPostingIterInner::Pivot(pivot.iter()),
            },
            AdjacencyPostingState::Delta {
                pivot,
                sealed,
                active,
                len,
                ..
            } => {
                let mut deltas = Vec::with_capacity(sealed.len() + usize::from(!active.is_empty()));
                deltas.extend(sealed.iter().map(|block| block.iter().peekable()));
                if !active.is_empty() {
                    deltas.push(active.iter().peekable());
                }
                Self {
                    inner: AdjacencyPostingIterInner::Delta(DeltaAdjacencyPostingIter {
                        pivot: pivot_set(pivot).iter().peekable(),
                        deltas,
                        remaining: *len,
                    }),
                }
            }
        }
    }
}

impl Iterator for DeltaAdjacencyPostingIter<'_> {
    type Item = OrderedAdjacencyEntry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let mut next_entry = self.pivot.peek().map(|entry| **entry);
            for delta in &mut self.deltas {
                if let Some(entry) = delta.peek().map(|entry| *entry.0) {
                    next_entry = Some(next_entry.map_or(entry, |current| current.min(entry)));
                }
            }
            let next_entry = next_entry?;
            let mut present = false;
            if self.pivot.peek().is_some_and(|entry| **entry == next_entry) {
                self.pivot.next();
                present = true;
            }
            for delta in &mut self.deltas {
                if delta.peek().is_some_and(|entry| *entry.0 == next_entry) {
                    let (_, override_present) =
                        delta.next().expect("peeked delta entry must exist");
                    present = *override_present;
                }
            }
            if present {
                self.remaining = self.remaining.saturating_sub(1);
                return Some(next_entry);
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl Iterator for AdjacencyPostingIter<'_> {
    type Item = OrderedAdjacencyEntry;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            AdjacencyPostingIterInner::Pivot(pivot) => pivot.next().copied(),
            AdjacencyPostingIterInner::Delta(delta) => delta.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.inner {
            AdjacencyPostingIterInner::Pivot(pivot) => pivot.size_hint(),
            AdjacencyPostingIterInner::Delta(delta) => delta.size_hint(),
        }
    }
}

impl ExactSizeIterator for AdjacencyPostingIter<'_> {}
impl FusedIterator for AdjacencyPostingIter<'_> {}

fn pivot_set(state: &AdjacencyPostingState) -> &BTreeSet<OrderedAdjacencyEntry> {
    let AdjacencyPostingState::Pivot(pivot) = state else {
        unreachable!("mini-delta pivots are always consolidated states");
    };
    pivot
}

fn presence_before_active(
    pivot: &AdjacencyPostingState,
    sealed: &[Arc<BTreeMap<OrderedAdjacencyEntry, bool>>],
    entry: &OrderedAdjacencyEntry,
) -> bool {
    sealed
        .iter()
        .rev()
        .find_map(|block| block.get(entry).copied())
        .unwrap_or_else(|| pivot_set(pivot).contains(entry))
}

fn override_presence(
    sealed: &[Arc<BTreeMap<OrderedAdjacencyEntry, bool>>],
    active: &BTreeMap<OrderedAdjacencyEntry, bool>,
    entry: &OrderedAdjacencyEntry,
) -> Option<bool> {
    active.get(entry).copied().or_else(|| {
        sealed
            .iter()
            .rev()
            .find_map(|block| block.get(entry).copied())
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AdjacencyDirection {
    Outgoing,
    Incoming,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AdjacencyLayout {
    Sparse,
    Dense,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdjacencyGroupStats {
    pub node_id: NodeId,
    pub rel_type: RelTypeId,
    pub direction: AdjacencyDirection,
    pub degree: usize,
    pub layout: AdjacencyLayout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct AdjacencyGroupKey {
    pub node_id: NodeId,
    pub rel_type: RelTypeId,
    pub direction: AdjacencyDirection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdjacencyGroupConsistencyMismatch {
    pub key: AdjacencyGroupKey,
    pub maintained_relationship_ids: Vec<RelId>,
    pub recomputed_relationship_ids: Vec<RelId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn posting_handle_stays_pointer_sized() {
        assert_eq!(
            std::mem::size_of::<AdjacencyPostingList>(),
            std::mem::size_of::<Arc<()>>()
        );
    }

    #[test]
    fn small_shared_posting_detaches_its_pivot_directly() {
        let mut posting = AdjacencyPostingList::from(rel_ids(0..8));
        let snapshot = posting.clone();

        assert!(posting.insert(entry(8)));

        assert!(!posting.shares_pivot_with(&snapshot));
        assert_eq!(posting.mini_delta_len(), 0);
        assert_eq!(
            posting.iter().copied().collect::<Vec<_>>(),
            rel_ids_vec(0..9)
        );
        assert_eq!(
            snapshot.iter().copied().collect::<Vec<_>>(),
            rel_ids_vec(0..8)
        );
    }

    #[test]
    fn posting_order_uses_neighbor_before_relationship_identity() {
        let posting = AdjacencyPostingList::from(BTreeSet::from([
            OrderedAdjacencyEntry {
                neighbor_id: NodeId(9),
                relationship_id: RelId(1),
            },
            OrderedAdjacencyEntry {
                neighbor_id: NodeId(3),
                relationship_id: RelId(8),
            },
            OrderedAdjacencyEntry {
                neighbor_id: NodeId(3),
                relationship_id: RelId(2),
            },
        ]));

        assert_eq!(
            posting.iter_copied().collect::<Vec<_>>(),
            vec![
                OrderedAdjacencyEntry {
                    neighbor_id: NodeId(3),
                    relationship_id: RelId(2),
                },
                OrderedAdjacencyEntry {
                    neighbor_id: NodeId(3),
                    relationship_id: RelId(8),
                },
                OrderedAdjacencyEntry {
                    neighbor_id: NodeId(9),
                    relationship_id: RelId(1),
                },
            ]
        );
    }

    #[test]
    fn shared_dense_posting_buffers_changes_without_detaching_pivot() {
        let mut posting = AdjacencyPostingList::from(rel_ids(0..128));
        let snapshot = posting.clone();

        assert!(posting.remove(&entry(2)));
        assert!(posting.insert(entry(256)));

        assert!(posting.shares_pivot_with(&snapshot));
        assert_eq!(posting.mini_delta_len(), 2);
        assert!(!posting.contains(&entry(2)));
        assert!(posting.contains(&entry(256)));
        assert!(snapshot.contains(&entry(2)));
        assert!(!snapshot.contains(&entry(256)));
    }

    #[test]
    fn full_active_delta_seals_without_detaching_the_pivot() {
        let mut posting = AdjacencyPostingList::from(rel_ids(0..128));
        let snapshot = posting.clone();
        for id in 0..ADJACENCY_MINI_DELTA_MAX_ENTRIES {
            assert!(posting.insert(entry(1_000 + id as u64)));
        }

        assert!(posting.shares_pivot_with(&snapshot));
        assert_eq!(posting.sealed_delta_count(), 1);
        assert_eq!(posting.mini_delta_len(), ADJACENCY_MINI_DELTA_MAX_ENTRIES);
        assert_eq!(posting.pivot_len(), 128);
        assert_eq!(posting.len(), 128 + ADJACENCY_MINI_DELTA_MAX_ENTRIES);
        assert_eq!(snapshot.len(), 128);
    }

    #[test]
    fn soft_limit_requests_explicit_consolidation() {
        let mut posting = AdjacencyPostingList::from(rel_ids(0..128));
        let snapshot = posting.clone();
        for id in 0..ADJACENCY_DELTA_CONSOLIDATION_ENTRIES {
            assert!(posting.insert(entry(1_000 + id as u64)));
        }

        assert!(posting.shares_pivot_with(&snapshot));
        assert!(posting.needs_consolidation());
        assert_eq!(posting.sealed_delta_count(), 2);
        assert!(posting.consolidate());
        assert!(!posting.shares_pivot_with(&snapshot));
        assert!(!posting.needs_consolidation());
        assert_eq!(posting.mini_delta_len(), 0);
        assert_eq!(posting.len(), 128 + ADJACENCY_DELTA_CONSOLIDATION_ENTRIES);
        assert_eq!(snapshot.len(), 128);
    }

    #[test]
    fn hard_limit_bounds_delta_growth_without_maintenance() {
        let mut posting = AdjacencyPostingList::from(rel_ids(0..128));
        let snapshot = posting.clone();
        for id in 0..ADJACENCY_DELTA_HARD_MAX_ENTRIES {
            assert!(posting.insert(entry(1_000 + id as u64)));
        }

        assert!(!posting.shares_pivot_with(&snapshot));
        assert_eq!(posting.mini_delta_len(), 0);
        assert_eq!(posting.len(), 128 + ADJACENCY_DELTA_HARD_MAX_ENTRIES);
        assert_eq!(snapshot.len(), 128);
    }

    #[test]
    fn streaming_read_merges_multiple_blocks_without_materializing_read_view() {
        let mut posting = AdjacencyPostingList::from(rel_ids(0..256));
        let snapshot = posting.clone();
        let mut reference = rel_ids(0..256);
        for id in (0..128_u64).step_by(2) {
            assert!(posting.remove(&entry(id)));
            reference.remove(&entry(id));
        }
        for id in 1_000..1_064_u64 {
            assert!(posting.insert(entry(id)));
            reference.insert(entry(id));
        }
        assert!(posting.insert(entry(2)));
        reference.insert(entry(2));
        assert!(posting.remove(&entry(1_001)));
        reference.remove(&entry(1_001));

        let AdjacencyPostingState::Delta { read_view, .. } = posting.state.as_ref() else {
            panic!("posting should retain delta state");
        };
        assert!(read_view.get().is_none());
        let mut iter = posting.iter_copied();
        assert_eq!(iter.len(), reference.len());
        assert_eq!(iter.by_ref().collect::<BTreeSet<_>>(), reference);
        assert_eq!(iter.len(), 0);
        let AdjacencyPostingState::Delta { read_view, .. } = posting.state.as_ref() else {
            panic!("posting should retain delta state");
        };
        assert!(read_view.get().is_none());
        assert_eq!(
            snapshot.iter_copied().collect::<BTreeSet<_>>(),
            rel_ids(0..256)
        );
    }

    #[test]
    fn mutation_sequence_matches_btree_set_reference_and_snapshots() {
        let mut posting = AdjacencyPostingList::default();
        let mut reference = BTreeSet::new();
        let mut snapshots = Vec::new();
        for step in 0..512_u64 {
            let entry = entry((step.wrapping_mul(73).wrapping_add(19)) % 181);
            if step % 3 == 0 {
                assert_eq!(posting.remove(&entry), reference.remove(&entry));
            } else {
                assert_eq!(posting.insert(entry), reference.insert(entry));
            }
            if step % 37 == 0 {
                snapshots.push((posting.clone(), reference.clone()));
            }
            assert_eq!(posting.len(), reference.len());
            assert_eq!(posting.iter_copied().collect::<BTreeSet<_>>(), reference);
        }

        for (snapshot, expected) in snapshots {
            assert_eq!(snapshot.iter_copied().collect::<BTreeSet<_>>(), expected);
        }
    }

    fn entry(id: u64) -> OrderedAdjacencyEntry {
        OrderedAdjacencyEntry {
            neighbor_id: NodeId(id),
            relationship_id: RelId(id),
        }
    }

    fn rel_ids(range: std::ops::Range<u64>) -> BTreeSet<OrderedAdjacencyEntry> {
        range.map(entry).collect()
    }

    fn rel_ids_vec(range: std::ops::Range<u64>) -> Vec<OrderedAdjacencyEntry> {
        range.map(entry).collect()
    }
}
