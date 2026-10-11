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
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Clone, Default)]
struct Weight {
    bytes: u64,
    visits: Arc<AtomicUsize>,
}

impl CowPageWeight for Weight {
    fn cow_page_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn delta_pressure_bytes(&self) -> u64 {
        self.visits.fetch_add(1, Ordering::Relaxed);
        self.bytes
    }
}

fn verify(map: &CowSegmentedMap<u64, Weight>) {
    let expected = map
        .values()
        .map(|value| u128::from(value.bytes))
        .sum::<u128>();
    assert_eq!(
        map.delta_pressure_bytes(),
        u64::try_from(expected).unwrap_or(u64::MAX)
    );
    assert_eq!(map.len(), map.iter().count());
    for (key, value) in map.iter() {
        assert_eq!(map.get(key).unwrap().bytes, value.bytes);
    }
}

#[test]
fn checkpoint_delta_pressure_tracks_all_mutations_and_snapshots_without_read_hydration() {
    let visits = Arc::new(AtomicUsize::new(0));
    let mut map = (0..1057)
        .map(|key| {
            (
                key,
                Weight {
                    bytes: key + 1,
                    visits: visits.clone(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>()
        .into();
    verify(&map);
    let snapshot = map.clone();
    let original = map.delta_pressure_bytes();
    map.insert(
        1057,
        Weight {
            bytes: 2000,
            visits: visits.clone(),
        },
    );
    map.insert(
        50,
        Weight {
            bytes: 0,
            visits: visits.clone(),
        },
    );
    map.get_mut(&100).unwrap().bytes = 3000;
    map.entry_or_default(2000).bytes = 7000;
    map.remove(&10);
    map.remove(&u64::MAX);
    assert!(map.get_mut(&u64::MAX).is_none());
    map.rebalance_key(&100);
    map.retain(|key, value| {
        value.bytes /= 2;
        key % 5 != 0
    });
    verify(&map);
    verify(&snapshot);
    assert_eq!(snapshot.delta_pressure_bytes(), original);
    assert_eq!(snapshot.len(), 1057);
    let before = visits.load(Ordering::Relaxed);
    for _ in 0..1025 {
        assert_eq!(snapshot.delta_pressure_bytes(), original);
        assert_eq!(
            map.clone().delta_pressure_bytes(),
            map.delta_pressure_bytes()
        );
    }
    assert_eq!(visits.load(Ordering::Relaxed), before);
}

#[test]
fn checkpoint_delta_pressure_recovers_exactly_after_saturated_totals() {
    let mut map = CowSegmentedMap::default();
    for key in 0..2u64 {
        map.insert(
            key,
            Weight {
                bytes: u64::MAX,
                ..Weight::default()
            },
        );
    }
    verify(&map);
    map.remove(&0);
    assert_eq!(map.delta_pressure_bytes(), u64::MAX);
    map.get_mut(&1).unwrap().bytes = 5;
    assert_eq!(map.delta_pressure_bytes(), 5);
    verify(&map);
}

#[test]
fn checkpoint_delta_pressure_unwind_updates_mutated_values_and_retains_a_searchable_directory() {
    let mut map = (0..1057)
        .map(|key| {
            (
                key,
                Weight {
                    bytes: key,
                    ..Weight::default()
                },
            )
        })
        .collect::<BTreeMap<_, _>>()
        .into();
    verify(&map);
    let snapshot = map.clone();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        map.get_mut(&5).unwrap().bytes = 1005;
        panic!("after pressure-tracked mutation");
    }));
    assert!(result.is_err());
    verify(&map);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        map.retain(|key, value| {
            value.bytes += 17;
            assert!(*key < 600, "after preceding pages were removed");
            false
        });
    }));
    assert!(result.is_err());
    verify(&map);
    verify(&snapshot);
    assert_eq!(snapshot.len(), 1057);
    for key in 0..1057 {
        assert_eq!(snapshot.get(&key).unwrap().bytes, key);
    }
    map.insert(
        0,
        Weight {
            bytes: 99,
            ..Weight::default()
        },
    );
    assert_eq!(map.get(&0).unwrap().bytes, 99);
    verify(&map);
}
