//! Copy-on-write segment primitives backing the in-memory graph store
//! snapshots.

use super::{
    estimated_node_record_bytes, estimated_relationship_record_bytes, estimated_value_bytes,
};
use crate::schema::{LabelId, RelTypeId};
use crate::value::Value;
use skein_storage::{AdjacencyPostingList, NodeId, NodeRecord, RelId, RelRecord};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

/// An immutable snapshot segment that is cloned only when a writer mutates it.
///
/// Read transactions clone the `Arc`, so creating a graph snapshot is
/// proportional to the number of segments rather than the number of graph
/// records. Writers retain the existing `&mut GraphStore` API and detach only
/// the segment they modify.
#[derive(Debug, PartialEq, Eq)]
#[repr(transparent)]
pub(super) struct CowSegment<T>(Arc<T>);

impl<T> Clone for CowSegment<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T: Default> Default for CowSegment<T> {
    fn default() -> Self {
        Self(Arc::new(T::default()))
    }
}

impl<T> From<T> for CowSegment<T> {
    fn from(value: T) -> Self {
        Self(Arc::new(value))
    }
}

impl<T> Deref for CowSegment<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl<T: Clone> DerefMut for CowSegment<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::make_mut(&mut self.0)
    }
}

#[cfg(test)]
impl<T> CowSegment<T> {
    pub(super) fn shares_storage_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

const COW_MAP_MAX_SEGMENT_ENTRIES: usize = 512;
pub(super) const COW_MAP_TARGET_SEGMENT_BYTES: usize = 128 * 1024;

pub(super) trait CowPageWeight {
    fn cow_page_bytes(&self) -> usize;
}

impl CowPageWeight for NodeId {
    fn cow_page_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl CowPageWeight for RelId {
    fn cow_page_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl CowPageWeight for LabelId {
    fn cow_page_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl CowPageWeight for RelTypeId {
    fn cow_page_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl CowPageWeight for String {
    fn cow_page_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(self.len())
    }
}

impl CowPageWeight for Value {
    fn cow_page_bytes(&self) -> usize {
        usize::try_from(estimated_value_bytes(self)).unwrap_or(usize::MAX)
    }
}

impl<T: CowPageWeight> CowPageWeight for Vec<T> {
    fn cow_page_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(
            self.iter()
                .map(CowPageWeight::cow_page_bytes)
                .fold(0usize, usize::saturating_add),
        )
    }
}

impl<T: CowPageWeight + Ord> CowPageWeight for BTreeSet<T> {
    fn cow_page_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(
            self.iter()
                .map(CowPageWeight::cow_page_bytes)
                .fold(0usize, usize::saturating_add),
        )
    }
}

impl<A: CowPageWeight, B: CowPageWeight> CowPageWeight for (A, B) {
    fn cow_page_bytes(&self) -> usize {
        self.0
            .cow_page_bytes()
            .saturating_add(self.1.cow_page_bytes())
    }
}

impl<A: CowPageWeight, B: CowPageWeight, C: CowPageWeight> CowPageWeight for (A, B, C) {
    fn cow_page_bytes(&self) -> usize {
        self.0
            .cow_page_bytes()
            .saturating_add(self.1.cow_page_bytes())
            .saturating_add(self.2.cow_page_bytes())
    }
}

impl CowPageWeight for NodeRecord {
    fn cow_page_bytes(&self) -> usize {
        usize::try_from(estimated_node_record_bytes(self)).unwrap_or(usize::MAX)
    }
}

impl CowPageWeight for RelRecord {
    fn cow_page_bytes(&self) -> usize {
        usize::try_from(estimated_relationship_record_bytes(self)).unwrap_or(usize::MAX)
    }
}

impl CowPageWeight for AdjacencyPostingList {
    fn cow_page_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl<T> CowPageWeight for CowSegment<T> {
    fn cow_page_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

fn cow_map_entry_bytes<K: CowPageWeight, V: CowPageWeight>(key: &K, value: &V) -> usize {
    std::mem::size_of::<usize>()
        .saturating_mul(4)
        .saturating_add(key.cow_page_bytes())
        .saturating_add(value.cow_page_bytes())
}

fn cow_map_segment_bytes<K: CowPageWeight, V: CowPageWeight>(segment: &BTreeMap<K, V>) -> usize {
    segment
        .iter()
        .map(|(key, value)| cow_map_entry_bytes(key, value))
        .fold(0usize, usize::saturating_add)
}

/// An ordered map backed by immutable COW pages.
///
/// A snapshot clones one outer `Arc`. A writer clones the small page directory
/// and only the page containing the modified key, avoiding an O(graph size)
/// first write while a read snapshot is alive.
#[derive(Debug)]
pub(super) struct CowSegmentedMap<K, V> {
    segments: Arc<Vec<Arc<BTreeMap<K, V>>>>,
    len: usize,
}

impl<K, V> Clone for CowSegmentedMap<K, V> {
    fn clone(&self) -> Self {
        Self {
            segments: Arc::clone(&self.segments),
            len: self.len,
        }
    }
}

impl<K, V> Default for CowSegmentedMap<K, V> {
    fn default() -> Self {
        Self {
            segments: Arc::new(Vec::new()),
            len: 0,
        }
    }
}

impl<K: Ord + CowPageWeight, V: CowPageWeight> From<BTreeMap<K, V>> for CowSegmentedMap<K, V> {
    fn from(values: BTreeMap<K, V>) -> Self {
        let len = values.len();
        let mut segments = Vec::new();
        let mut segment = BTreeMap::new();
        let mut segment_bytes = 0usize;
        for (key, value) in values {
            let entry_bytes = cow_map_entry_bytes(&key, &value);
            if !segment.is_empty()
                && (segment.len() >= COW_MAP_MAX_SEGMENT_ENTRIES
                    || segment_bytes.saturating_add(entry_bytes) > COW_MAP_TARGET_SEGMENT_BYTES)
            {
                segments.push(Arc::new(std::mem::take(&mut segment)));
                segment_bytes = 0;
            }
            segment_bytes = segment_bytes.saturating_add(entry_bytes);
            segment.insert(key, value);
        }
        if !segment.is_empty() {
            segments.push(Arc::new(segment));
        }
        Self {
            segments: Arc::new(segments),
            len,
        }
    }
}

impl<K: Ord, V> CowSegmentedMap<K, V> {
    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    fn segment_index(&self, key: &K) -> Option<usize> {
        if self.segments.is_empty() {
            return None;
        }
        let index = self.segments.partition_point(|segment| {
            segment
                .last_key_value()
                .is_some_and(|(last_key, _)| last_key < key)
        });
        Some(index.min(self.segments.len() - 1))
    }

    #[inline]
    pub(super) fn get(&self, key: &K) -> Option<&V> {
        self.segment_index(key)
            .and_then(|index| self.segments[index].get(key))
    }

    pub(super) fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.segments.iter().flat_map(|segment| segment.iter())
    }

    pub(super) fn keys(&self) -> impl Iterator<Item = &K> {
        self.iter().map(|(key, _)| key)
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &V> {
        self.iter().map(|(_, value)| value)
    }
}

impl<K: Ord + Clone + CowPageWeight, V: Clone + CowPageWeight> CowSegmentedMap<K, V> {
    pub(super) fn insert(&mut self, key: K, value: V) -> Option<V> {
        if self.segments.is_empty() {
            self.segments = Arc::new(vec![Arc::new(BTreeMap::from([(key, value)]))]);
            self.len = 1;
            return None;
        }
        let index = self
            .segment_index(&key)
            .expect("non-empty segmented map has a target page");
        let segments = Arc::make_mut(&mut self.segments);
        let segment = Arc::make_mut(&mut segments[index]);
        let previous = segment.insert(key, value);
        if previous.is_none() {
            self.len = self.len.saturating_add(1);
        }
        Self::split_oversized_segment(segments, index);
        previous
    }

    fn split_oversized_segment(segments: &mut Vec<Arc<BTreeMap<K, V>>>, index: usize) {
        let segment = Arc::make_mut(&mut segments[index]);
        let segment_bytes = cow_map_segment_bytes(segment);
        if segment.len() <= 1
            || (segment.len() <= COW_MAP_MAX_SEGMENT_ENTRIES
                && segment_bytes <= COW_MAP_TARGET_SEGMENT_BYTES)
        {
            return;
        }
        let split_after_bytes = segment_bytes / 2;
        let mut bytes = 0usize;
        let split_key = segment
            .iter()
            .enumerate()
            .find_map(|(entry_index, (key, value))| {
                if entry_index > 0
                    && (bytes >= split_after_bytes
                        || entry_index >= COW_MAP_MAX_SEGMENT_ENTRIES / 2)
                {
                    return Some(key.clone());
                }
                bytes = bytes.saturating_add(cow_map_entry_bytes(key, value));
                None
            })
            .unwrap_or_else(|| {
                segment
                    .keys()
                    .nth(segment.len() / 2)
                    .cloned()
                    .expect("oversized segmented map page is non-empty")
            });
        let right = segment.split_off(&split_key);
        segments.insert(index + 1, Arc::new(right));
    }

    pub(super) fn rebalance_key(&mut self, key: &K) {
        let Some(index) = self.segment_index(key) else {
            return;
        };
        let segments = Arc::make_mut(&mut self.segments);
        Self::split_oversized_segment(segments, index);
    }

    pub(super) fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let index = self.segment_index(key)?;
        if !self.segments[index].contains_key(key) {
            return None;
        }
        let segments = Arc::make_mut(&mut self.segments);
        Arc::make_mut(&mut segments[index]).get_mut(key)
    }

    pub(super) fn entry_or_default(&mut self, key: K) -> &mut V
    where
        V: Default,
    {
        if !self.contains_key(&key) {
            self.insert(key.clone(), V::default());
        }
        self.get_mut(&key)
            .expect("inserted segmented map entry must be available")
    }

    pub(super) fn remove(&mut self, key: &K) -> Option<V> {
        let index = self.segment_index(key)?;
        if !self.segments[index].contains_key(key) {
            return None;
        }
        let segments = Arc::make_mut(&mut self.segments);
        let removed = Arc::make_mut(&mut segments[index]).remove(key);
        if removed.is_some() {
            self.len = self.len.saturating_sub(1);
        }
        if segments[index].is_empty() {
            segments.remove(index);
        }
        removed
    }

    pub(super) fn retain(&mut self, mut keep: impl FnMut(&K, &mut V) -> bool) {
        let segments = Arc::make_mut(&mut self.segments);
        for segment in segments.iter_mut() {
            let segment = Arc::make_mut(segment);
            let previous_len = segment.len();
            segment.retain(|key, value| keep(key, value));
            self.len = self
                .len
                .saturating_sub(previous_len.saturating_sub(segment.len()));
        }
        segments.retain(|segment| !segment.is_empty());
    }
}

#[cfg(test)]
impl<K, V> CowSegmentedMap<K, V> {
    pub(super) fn shares_storage_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.segments, &other.segments)
    }

    pub(super) fn segment_count(&self) -> usize {
        self.segments.len()
    }

    pub(super) fn shared_segment_count_with(&self, other: &Self) -> usize {
        self.segments
            .iter()
            .filter(|segment| {
                other
                    .segments
                    .iter()
                    .any(|other_segment| Arc::ptr_eq(segment, other_segment))
            })
            .count()
    }
}
