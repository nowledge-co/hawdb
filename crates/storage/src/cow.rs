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

//! Copy-on-write segment primitives backing the in-memory graph store
//! snapshots.

use crate::{adjacency::AdjacencyPostingList, NodeId, NodeRecord, RelId, RelRecord};
use hawdb_core::{IndexId, IndexStatisticsSample, LabelId, RelTypeId, Value};
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
pub struct CowSegment<T>(Arc<CowData<T>>);

#[derive(Debug)]
struct CowData<T> {
    // Destroy the actual data before refunding its allocation ownership.
    value: T,
    growth: Option<Arc<checkpoint::CheckpointGrowthBudget>>,
    _memory: crate::background::CheckpointAllocationOwner,
}

impl<T: Clone> Clone for CowData<T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            growth: None,
            // Ordinary frontend copying does not manufacture a second lease
            // for the old allocation. Its original snapshots keep that owner.
            _memory: Default::default(),
        }
    }
}

impl<T: PartialEq> PartialEq for CowData<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<T: Eq> Eq for CowData<T> {}

impl<T> Clone for CowSegment<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T: Default> Default for CowSegment<T> {
    fn default() -> Self {
        Self::from(T::default())
    }
}

impl<T> From<T> for CowSegment<T> {
    fn from(value: T) -> Self {
        Self(Arc::new(CowData {
            value,
            growth: None,
            _memory: Default::default(),
        }))
    }
}

impl<T> Deref for CowSegment<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0.value
    }
}

impl<T: Clone> DerefMut for CowSegment<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut Arc::make_mut(&mut self.0).value
    }
}

#[doc(hidden)]
impl<T> CowSegment<T> {
    pub fn shares_storage_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

const COW_MAP_MAX_SEGMENT_ENTRIES: usize = 512;
pub const COW_MAP_TARGET_SEGMENT_BYTES: usize = 128 * 1024;

fn estimated_value_bytes(value: &Value) -> u64 {
    match value {
        Value::Null | Value::Bool(_) => 1,
        Value::Int(_) | Value::Float(_) => 8,
        Value::String(value) => value.len() as u64,
        Value::Binary(value) => value.len() as u64,
        Value::Uuid(_) => 16,
        Value::List(values) => values.iter().fold(16u64, |bytes, value| {
            bytes.saturating_add(estimated_value_bytes(value))
        }),
        Value::Map(values) => estimated_properties_bytes(values),
    }
}

fn estimated_properties_bytes(properties: &BTreeMap<String, Value>) -> u64 {
    properties.iter().fold(0u64, |bytes, (key, value)| {
        bytes
            .saturating_add(key.len() as u64)
            .saturating_add(estimated_value_bytes(value))
            .saturating_add(16)
    })
}

fn estimated_node_record_bytes(node: &NodeRecord) -> u64 {
    32u64
        .saturating_add((node.labels.len() as u64).saturating_mul(4))
        .saturating_add(estimated_properties_bytes(&node.properties))
}

fn estimated_relationship_record_bytes(relationship: &RelRecord) -> u64 {
    40u64.saturating_add(estimated_properties_bytes(&relationship.properties))
}

pub trait CowPageWeight {
    fn cow_page_bytes(&self) -> usize;

    /// The existing storage-pressure estimate for a graph delta value.
    /// This is scheduling metadata, not an allocation-capacity bound.
    /// Implementations must not panic and must return the same weight until
    /// the value changes through an exclusive mutable borrow. Map mutations
    /// cache this weight; interior changes through a shared borrow are unsupported.
    fn delta_pressure_bytes(&self) -> u64 {
        0
    }
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

impl CowPageWeight for IndexId {
    fn cow_page_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl CowPageWeight for IndexStatisticsSample {
    fn cow_page_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl CowPageWeight for u64 {
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

    fn delta_pressure_bytes(&self) -> u64 {
        64u64.saturating_add((self.len() as u64).saturating_mul(24))
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

    fn delta_pressure_bytes(&self) -> u64 {
        estimated_node_record_bytes(self)
    }
}

impl CowPageWeight for RelRecord {
    fn cow_page_bytes(&self) -> usize {
        usize::try_from(estimated_relationship_record_bytes(self)).unwrap_or(usize::MAX)
    }

    fn delta_pressure_bytes(&self) -> u64 {
        estimated_relationship_record_bytes(self)
    }
}

impl CowPageWeight for AdjacencyPostingList {
    fn cow_page_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn delta_pressure_bytes(&self) -> u64 {
        48u64.saturating_add((self.len() as u64).saturating_mul(24))
    }
}

impl<T: CowPageWeight> CowPageWeight for CowSegment<T> {
    fn cow_page_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn delta_pressure_bytes(&self) -> u64 {
        self.0.value.delta_pressure_bytes()
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
pub struct CowSegmentedMap<K, V> {
    segments: CowSegment<Vec<CowSegment<BTreeMap<K, V>>>>,
    len: usize,
    // Keep the exact sum before narrowing, so removing a large value can
    // recover from a saturated public estimate without undercounting.
    delta_pressure_bytes: u128,
}

impl<K, V> Clone for CowSegmentedMap<K, V> {
    fn clone(&self) -> Self {
        Self {
            segments: self.segments.clone(),
            len: self.len,
            delta_pressure_bytes: self.delta_pressure_bytes,
        }
    }
}

impl<K, V> Default for CowSegmentedMap<K, V> {
    fn default() -> Self {
        Self {
            segments: CowSegment::from(Vec::new()),
            len: 0,
            delta_pressure_bytes: 0,
        }
    }
}

impl<K: Ord + CowPageWeight, V: CowPageWeight> From<BTreeMap<K, V>> for CowSegmentedMap<K, V> {
    fn from(values: BTreeMap<K, V>) -> Self {
        let len = values.len();
        let mut segments = Vec::new();
        let mut segment = BTreeMap::new();
        let mut segment_bytes = 0usize;
        let mut delta_pressure_bytes = 0u128;
        for (key, value) in values {
            delta_pressure_bytes += u128::from(value.delta_pressure_bytes());
            let entry_bytes = cow_map_entry_bytes(&key, &value);
            if !segment.is_empty()
                && (segment.len() >= COW_MAP_MAX_SEGMENT_ENTRIES
                    || segment_bytes.saturating_add(entry_bytes) > COW_MAP_TARGET_SEGMENT_BYTES)
            {
                segments.push(CowSegment::from(std::mem::take(&mut segment)));
                segment_bytes = 0;
            }
            segment_bytes = segment_bytes.saturating_add(entry_bytes);
            segment.insert(key, value);
        }
        if !segment.is_empty() {
            segments.push(CowSegment::from(segment));
        }
        Self {
            segments: CowSegment::from(segments),
            len,
            delta_pressure_bytes,
        }
    }
}

impl<K: Ord, V> CowSegmentedMap<K, V> {
    /// Constant-time copy of the maintained graph-delta pressure estimate.
    pub fn delta_pressure_bytes(&self) -> u64 {
        u64::try_from(self.delta_pressure_bytes).unwrap_or(u64::MAX)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
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
    pub fn get(&self, key: &K) -> Option<&V> {
        self.segment_index(key)
            .and_then(|index| self.segments[index].get(key))
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.segments.iter().flat_map(|segment| segment.iter())
    }

    /// Borrow a key interval without scanning unrelated immutable segments.
    pub(crate) fn range<'a>(
        &'a self,
        first: &'a K,
        last: &'a K,
    ) -> impl Iterator<Item = (&'a K, &'a V)> {
        assert!(first <= last, "invalid segmented-map key interval");
        let start = self.segments.partition_point(|segment| {
            segment.last_key_value().is_some_and(|(key, _)| key < first)
        });
        let end = self.segments.partition_point(|segment| {
            segment
                .first_key_value()
                .is_some_and(|(key, _)| key <= last)
        });
        self.segments[start..end]
            .iter()
            .flat_map(move |segment| segment.range(first..=last))
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.iter().map(|(key, _)| key)
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.iter().map(|(_, value)| value)
    }
}

impl<K: Ord + Clone + CowPageWeight, V: Clone + CowPageWeight> CowSegmentedMap<K, V> {
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let pressure = u128::from(value.delta_pressure_bytes());
        if self.segments.is_empty() {
            self.segments =
                CowSegment::from(vec![CowSegment::from(BTreeMap::from([(key, value)]))]);
            self.len = 1;
            self.delta_pressure_bytes = pressure;
            return None;
        }
        let index = self
            .segment_index(&key)
            .expect("non-empty segmented map has a target page");
        let segments = &mut *self.segments;
        let segment = &mut *segments[index];
        let previous = segment.insert(key, value);
        self.delta_pressure_bytes = self.delta_pressure_bytes
            - previous
                .as_ref()
                .map_or(0, |value| u128::from(value.delta_pressure_bytes()))
            + pressure;
        if previous.is_none() {
            self.len = self.len.saturating_add(1);
        }
        // Replacement records can also exceed the byte limit and split a
        // page. Every insertion consumes its preflight allowance.
        if let Some(growth) = &segments[index].0.growth {
            growth.consume_insertion();
        }
        Self::split_oversized_segment(segments, index);
        previous
    }

    fn split_oversized_segment(segments: &mut Vec<CowSegment<BTreeMap<K, V>>>, index: usize) {
        let memory = segments[index].0._memory.clone();
        let growth = segments[index].0.growth.clone();
        let segment = &mut *segments[index];
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
        segments.insert(
            index + 1,
            CowSegment(Arc::new(CowData {
                value: right,
                growth,
                _memory: memory,
            })),
        );
    }

    pub fn rebalance_key(&mut self, key: &K) {
        let Some(index) = self.segment_index(key) else {
            return;
        };
        let segments = &mut *self.segments;
        Self::split_oversized_segment(segments, index);
    }

    pub fn get_mut(&mut self, key: &K) -> Option<CowMapValueMut<'_, V>> {
        let index = self.segment_index(key)?;
        if !self.segments[index].contains_key(key) {
            return None;
        }
        let segments = &mut *self.segments;
        let value = segments[index].get_mut(key)?;
        let previous = value.delta_pressure_bytes();
        Some(CowMapValueMut {
            value,
            pressure: &mut self.delta_pressure_bytes,
            previous,
        })
    }

    pub fn entry_or_default(&mut self, key: K) -> CowMapValueMut<'_, V>
    where
        V: Default,
    {
        if !self.contains_key(&key) {
            self.insert(key.clone(), V::default());
        }
        self.get_mut(&key)
            .expect("inserted segmented map entry must be available")
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let index = self.segment_index(key)?;
        if !self.segments[index].contains_key(key) {
            return None;
        }
        let segments = &mut *self.segments;
        let removed = segments[index].remove(key);
        if removed.is_some() {
            self.len = self.len.saturating_sub(1);
        }
        self.delta_pressure_bytes -= removed
            .as_ref()
            .map_or(0, |value| u128::from(value.delta_pressure_bytes()));
        if segments[index].is_empty() {
            segments.remove(index);
        }
        removed
    }

    pub fn retain(&mut self, mut keep: impl FnMut(&K, &mut V) -> bool) {
        let segments = &mut *self.segments;
        let directory = RetainDirectory(segments);
        for segment in directory.0.iter_mut() {
            let segment = &mut **segment;
            let before_len = segment.len();
            let before_pressure = segment
                .values()
                .map(|value| u128::from(value.delta_pressure_bytes()))
                .sum();
            let guard = RetainPressure {
                segment,
                len: &mut self.len,
                pressure: &mut self.delta_pressure_bytes,
                before_len,
                before_pressure,
            };
            guard.segment.retain(|key, value| keep(key, value));
        }
    }
}

#[doc(hidden)]
impl<K, V> CowSegmentedMap<K, V> {
    pub fn shares_storage_with(&self, other: &Self) -> bool {
        self.segments.shares_storage_with(&other.segments)
    }

    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    pub fn shared_segment_count_with(&self, other: &Self) -> usize {
        self.segments
            .iter()
            .filter(|segment| {
                other
                    .segments
                    .iter()
                    .any(|other_segment| segment.shares_storage_with(other_segment))
            })
            .count()
    }
}

/// A mutable map value whose pressure accounting follows its actual mutation.
/// Release this guard before mutating the same map again.
#[derive(Debug)]
pub struct CowMapValueMut<'a, V: CowPageWeight> {
    value: &'a mut V,
    pressure: &'a mut u128,
    previous: u64,
}

impl<V: CowPageWeight> Deref for CowMapValueMut<'_, V> {
    type Target = V;

    fn deref(&self) -> &V {
        self.value
    }
}

impl<V: CowPageWeight> DerefMut for CowMapValueMut<'_, V> {
    fn deref_mut(&mut self) -> &mut V {
        self.value
    }
}

impl<V: CowPageWeight> Drop for CowMapValueMut<'_, V> {
    fn drop(&mut self) {
        *self.pressure = *self.pressure - u128::from(self.previous)
            + u128::from(self.value.delta_pressure_bytes());
    }
}

struct RetainPressure<'a, K, V: CowPageWeight> {
    segment: &'a mut BTreeMap<K, V>,
    len: &'a mut usize,
    pressure: &'a mut u128,
    before_len: usize,
    before_pressure: u128,
}

struct RetainDirectory<'a, K, V>(&'a mut Vec<CowSegment<BTreeMap<K, V>>>);

impl<K, V> Drop for RetainDirectory<'_, K, V> {
    fn drop(&mut self) {
        self.0.retain(|segment| !segment.is_empty());
    }
}

impl<K, V: CowPageWeight> Drop for RetainPressure<'_, K, V> {
    fn drop(&mut self) {
        *self.len = *self.len - self.before_len + self.segment.len();
        *self.pressure = *self.pressure - self.before_pressure
            + self
                .segment
                .values()
                .map(|value| u128::from(value.delta_pressure_bytes()))
                .sum::<u128>();
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod segmented_range_tests {
    use super::*;

    #[test]
    fn borrowed_key_interval_matches_btree_across_segments_and_snapshots() {
        let mut oracle: BTreeMap<NodeId, NodeId> = (0..3000)
            .map(|key| (NodeId(key * 2), NodeId(key)))
            .collect();
        let mut map = CowSegmentedMap::from(oracle.clone());
        assert!(map.segment_count() > 1);
        let snapshot = map.clone();
        let previous = oracle.clone();
        for key in [0, 512, 2048, 5998] {
            assert_eq!(map.remove(&NodeId(key)), oracle.remove(&NodeId(key)));
        }
        for key in [1, 513, 2049, 6000] {
            assert_eq!(
                map.insert(NodeId(key), NodeId(9)),
                oracle.insert(NodeId(key), NodeId(9))
            );
        }
        for (first, last) in [
            (0, 0),
            (1, 1),
            (3, 3),
            (511, 514),
            (1023, 4097),
            (5998, 7000),
            (7001, u64::MAX),
        ] {
            let (first, last) = (NodeId(first), NodeId(last));
            let actual: Vec<_> = map
                .range(&first, &last)
                .map(|(key, value)| (*key, *value))
                .collect();
            let expected: Vec<_> = oracle
                .range(first..=last)
                .map(|(key, value)| (*key, *value))
                .collect();
            assert_eq!(actual, expected);
            assert_eq!(
                snapshot
                    .range(&first, &last)
                    .map(|(key, value)| (*key, *value))
                    .collect::<Vec<_>>(),
                previous
                    .range(first..=last)
                    .map(|(key, value)| (*key, *value))
                    .collect::<Vec<_>>()
            );
        }
        let empty = CowSegmentedMap::<NodeId, NodeId>::default();
        assert_eq!(empty.range(&NodeId(0), &NodeId(u64::MAX)).count(), 0);
    }
}

mod checkpoint;
