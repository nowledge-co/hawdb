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

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};
use std::num::NonZeroUsize;

use crate::kernel::OperatorMemoryTracker;
use hawdb_core::Result;

/// Hash the structural key once, including when a probe is followed by a merge
/// or insertion. Hash collisions never replace full-key equality.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct HashedKey<K> {
    hash: u64,
    pub(super) key: K,
}

impl<K: Hash> HashedKey<K> {
    pub(super) fn new(key: K, state: &RandomState) -> Self {
        Self {
            hash: state.hash_one(&key),
            key,
        }
    }
}

impl<K> Hash for HashedKey<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

impl<K> HashedKey<K> {
    pub(super) fn with_hash(key: K, hash: u64) -> Self {
        Self { hash, key }
    }
}

struct GroupEntry<K, V> {
    key: HashedKey<K>,
    value: V,
    next: Option<NonZeroUsize>,
}

/// Dense groups keep their keys and states in one sortable allocation. Buckets
/// contain only chain heads; collisions always compare complete keys. Unlike
/// collecting a HashMap into a sorted Vec, finishing needs no second array of
/// group headers. The tracker retains array capacity until that array is freed.
pub(super) struct HashGroups<K, V> {
    entries: Vec<GroupEntry<K, V>>,
    buckets: Vec<Option<NonZeroUsize>>,
}

impl<K, V> Default for HashGroups<K, V> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            buckets: Vec::new(),
        }
    }
}

impl<K: Eq, V> HashGroups<K, V> {
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Candidate enumeration for a multimap whose full keys live in its rows.
    /// The caller must compare those keys before producing a join result.
    pub(super) fn hashed_values(&self, hash: u64) -> impl Iterator<Item = &V> {
        let mut cursor = if self.buckets.is_empty() {
            None
        } else {
            self.buckets[hash as usize & (self.buckets.len() - 1)]
        };
        std::iter::from_fn(move || {
            while let Some(index) = cursor {
                let entry = &self.entries[index.get() - 1];
                cursor = entry.next;
                if entry.key.hash == hash {
                    return Some(&entry.value);
                }
            }
            None
        })
    }

    pub(super) fn into_values(self) -> impl Iterator<Item = (u64, V)> {
        self.entries
            .into_iter()
            .map(|entry| (entry.key.hash, entry.value))
    }

    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn find(&self, key: &HashedKey<K>) -> Option<usize> {
        if self.buckets.is_empty() {
            return None;
        }
        let mut cursor = self.buckets[key.hash as usize & (self.buckets.len() - 1)];
        while let Some(index) = cursor {
            let index = index.get() - 1;
            let entry = &self.entries[index];
            if entry.key == *key {
                return Some(index);
            }
            cursor = entry.next;
        }
        None
    }

    pub(super) fn get(&self, key: &HashedKey<K>) -> Option<&V> {
        self.find(key).map(|index| &self.entries[index].value)
    }

    pub(super) fn get_mut(&mut self, key: &HashedKey<K>) -> Option<&mut V> {
        self.find(key).map(|index| &mut self.entries[index].value)
    }

    fn next_capacity(&self) -> usize {
        self.buckets.len().saturating_mul(2).max(4)
    }

    fn array_bytes(capacity: usize) -> usize {
        capacity.saturating_mul(
            std::mem::size_of::<GroupEntry<K, V>>()
                .saturating_add(std::mem::size_of::<Option<NonZeroUsize>>()),
        )
    }

    pub(super) fn insertion_bytes(&self, payload_bytes: usize) -> usize {
        if self.entries.len() == self.buckets.len() {
            // Old arrays stay charged while the new arrays are allocated.
            payload_bytes.saturating_add(Self::array_bytes(self.next_capacity()))
        } else {
            payload_bytes
        }
    }

    pub(super) fn insert(
        &mut self,
        key: HashedKey<K>,
        value: V,
        payload_bytes: usize,
        tracker: &mut OperatorMemoryTracker,
    ) -> Result<()> {
        debug_assert!(self.find(&key).is_none());
        tracker.try_charge(self.insertion_bytes(payload_bytes))?;
        if self.entries.len() == self.buckets.len() {
            let capacity = self.next_capacity();
            let old_bytes = Self::array_bytes(self.buckets.len());
            let mut entries = Vec::with_capacity(capacity);
            let mut buckets = vec![None; capacity];
            entries.append(&mut self.entries);
            for (index, entry) in entries.iter_mut().enumerate() {
                let bucket = entry.key.hash as usize & (capacity - 1);
                entry.next = buckets[bucket];
                buckets[bucket] = NonZeroUsize::new(index + 1);
            }
            self.entries = entries;
            self.buckets = buckets;
            tracker.release(old_bytes);
        }
        let bucket = key.hash as usize & (self.buckets.len() - 1);
        let next = self.buckets[bucket];
        self.entries.push(GroupEntry { key, value, next });
        self.buckets[bucket] = NonZeroUsize::new(self.entries.len());
        Ok(())
    }
}

impl<K: Ord, V> HashGroups<K, V> {
    pub(super) fn into_sorted(self) -> (impl Iterator<Item = (K, V)>, usize) {
        let Self {
            mut entries,
            buckets,
        } = self;
        let released_bytes = buckets
            .len()
            .saturating_mul(std::mem::size_of::<Option<NonZeroUsize>>());
        drop(buckets);
        entries.sort_unstable_by(|left, right| left.key.key.cmp(&right.key.key));
        (
            entries
                .into_iter()
                .map(|entry| (entry.key.key, entry.value)),
            released_bytes,
        )
    }
}

/// Conservative per-entry headroom for buckets, growth, and the temporary
/// sorted output/run vector. Variable-sized key/state payloads are charged by
/// the operator separately. Keeping this charge per entry also retains it
/// while the table is consumed into a sorted vector.
pub(super) const fn hash_entry_overhead<K, V>() -> usize {
    std::mem::size_of::<(HashedKey<K>, V)>()
        .saturating_add(1)
        .saturating_mul(4)
        .saturating_add(32)
}

/// Charge capacity rather than a worst-case initial table for every distinct
/// value. Double-capacity headroom covers bucket/control storage and the old
/// table while an insertion grows the allocation.
pub(super) const fn hash_set_capacity_bytes<K>(capacity: usize) -> usize {
    if capacity == 0 {
        0
    } else {
        capacity
            .saturating_mul(2)
            .saturating_mul(std::mem::size_of::<K>().saturating_add(1))
            .saturating_add(32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{QueryMemoryClass, QueryMemoryLedger};
    use hawdb_core::Value;
    use std::collections::{BTreeMap, HashMap};

    #[test]
    fn colliding_hashes_compare_complete_structural_keys() {
        let keys = [
            vec![Value::Null],
            vec![Value::Int(1)],
            vec![Value::Float(1.0)],
            vec![Value::Float(-0.0)],
            vec![Value::Float(0.0)],
            vec![Value::Float(f64::from_bits(0x7ff8_0000_0000_0001))],
            vec![Value::Float(f64::from_bits(0x7ff8_0000_0000_0002))],
            vec![Value::List(vec![Value::String("nested".to_string())])],
            vec![Value::Map(BTreeMap::from([(
                "key".to_string(),
                Value::Null,
            )]))],
        ];
        let mut groups = HashMap::new();
        for (index, key) in keys.iter().enumerate() {
            groups.insert(
                HashedKey {
                    hash: 0,
                    key: key.clone(),
                },
                index,
            );
        }
        assert_eq!(groups.len(), keys.len());
        for (index, key) in keys.into_iter().enumerate() {
            assert_eq!(groups.get(&HashedKey { hash: 0, key }), Some(&index));
        }
    }

    #[test]
    fn equal_keys_share_a_precomputed_hash() {
        let state = RandomState::new();
        let key = vec![Value::List(vec![Value::Float(f64::NAN), Value::Null])];
        assert_eq!(
            HashedKey::new(key.clone(), &state),
            HashedKey::new(key, &state)
        );
    }

    #[test]
    fn dense_groups_preserve_collisions_across_growth_and_sort() {
        let mut groups = HashGroups::<u64, u64>::default();
        let mut tracker = OperatorMemoryTracker::new(NonZeroUsize::new(1024 * 1024).unwrap());
        let keys = (0..129).rev().collect::<Vec<u64>>();
        for &key in &keys {
            groups
                .insert(HashedKey { hash: 0, key }, key * 2, 0, &mut tracker)
                .unwrap();
            assert_eq!(
                tracker.used_bytes,
                HashGroups::<u64, u64>::array_bytes(groups.buckets.len())
            );
            for existing in key..129 {
                assert_eq!(
                    groups.get(&HashedKey {
                        hash: 0,
                        key: existing
                    }),
                    Some(&(existing * 2))
                );
            }
        }
        *groups.get_mut(&HashedKey { hash: 0, key: 42 }).unwrap() = 999;
        assert_eq!(groups.get(&HashedKey { hash: 0, key: 999 }), None);
        let entry_capacity = groups.entries.capacity();
        let (sorted, released_bytes) = groups.into_sorted();
        tracker.release(released_bytes);
        assert_eq!(
            tracker.used_bytes,
            entry_capacity * std::mem::size_of::<GroupEntry<u64, u64>>()
        );
        let output = sorted.collect::<Vec<_>>();
        assert_eq!(output.len(), 129);
        for (key, (actual_key, value)) in output.into_iter().enumerate() {
            assert_eq!(actual_key, key as u64);
            assert_eq!(value, if key == 42 { 999 } else { key as u64 * 2 });
        }
        tracker.reset();
        assert_eq!(tracker.used_bytes, 0);
    }

    #[test]
    fn dense_group_growth_is_admitted_before_changing_capacity_or_chains() {
        type Groups = HashGroups<u64, u64>;
        const PAYLOAD_BYTES: usize = 16;
        let growth_peak = Groups::array_bytes(4) + Groups::array_bytes(8) + 5 * PAYLOAD_BYTES;
        for root_limited in [false, true] {
            for allowed in [false, true] {
                let limit = NonZeroUsize::new(growth_peak - usize::from(!allowed)).unwrap();
                let large = NonZeroUsize::new(1024 * 1024).unwrap();
                let ledger = QueryMemoryLedger::new(if root_limited { limit } else { large });
                let operator_limit = if root_limited { large } else { limit };
                let mut tracker = OperatorMemoryTracker::with_account(
                    operator_limit,
                    ledger.account(
                        QueryMemoryClass::BlockingState,
                        "dense groups test",
                        operator_limit,
                    ),
                );
                let mut groups = Groups::default();
                for key in 0..4 {
                    groups
                        .insert(HashedKey { hash: 0, key }, key, PAYLOAD_BYTES, &mut tracker)
                        .unwrap();
                }
                let old_used = tracker.used_bytes;
                let result = groups.insert(
                    HashedKey { hash: 0, key: 4 },
                    4,
                    PAYLOAD_BYTES,
                    &mut tracker,
                );
                if allowed {
                    result.unwrap();
                    assert_eq!(groups.buckets.len(), 8);
                    assert_eq!(tracker.peak_bytes, growth_peak);
                    assert_eq!(
                        tracker.used_bytes,
                        Groups::array_bytes(8) + 5 * PAYLOAD_BYTES
                    );
                    assert_eq!(groups.get(&HashedKey { hash: 0, key: 4 }), Some(&4));
                } else {
                    assert!(result.is_err());
                    assert_eq!(groups.buckets.len(), 4);
                    assert_eq!(groups.entries.len(), 4);
                    assert_eq!(tracker.used_bytes, old_used);
                    assert_eq!(groups.get(&HashedKey { hash: 0, key: 4 }), None);
                }
                for key in 0..4 {
                    assert_eq!(groups.get(&HashedKey { hash: 0, key }), Some(&key));
                }
                drop(groups);
                drop(tracker);
                assert_eq!(ledger.snapshot().used_bytes, 0);
            }
        }
    }
}
