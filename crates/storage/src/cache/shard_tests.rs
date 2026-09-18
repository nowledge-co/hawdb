use super::*;
use std::collections::BTreeSet;
use std::sync::{mpsc, Barrier};
use std::time::Duration;

fn key(segment_id: u64, bytes: &[u8]) -> SegmentCacheKey {
    SegmentCacheKey {
        store_id: StoreId(9),
        manifest_generation: ManifestGeneration(7),
        segment_id,
        content_digest: content_digest(bytes),
        representation: RepresentationKind::RawBytes,
    }
}

fn keys_in_shard(cache: &SegmentCache, shard: usize, count: usize) -> Vec<SegmentCacheKey> {
    let keys: Vec<_> = (0..1_000_000)
        .map(|id| key(id, b"12345678"))
        .filter(|key| cache.shard_index(&key.identity()) == shard)
        .take(count)
        .collect();
    assert_eq!(keys.len(), count);
    keys
}

#[test]
fn hits_do_not_acquire_admission_or_an_unrelated_shard() {
    let cache = SegmentCache::new(64);
    let raw = keys_in_shard(&cache, 1, 1)[0];
    let verified = keys_in_shard(&cache, 2, 1)[0];
    drop(cache.insert(raw, b"12345678".to_vec()).unwrap());
    drop(
        cache
            .insert_verified(verified, [3; 32], b"compact".to_vec())
            .unwrap(),
    );
    std::thread::scope(|scope| {
        let admission = cache.lock_admission();
        let unrelated = lock_unpoisoned(&cache.shards[0]);
        let (tx, rx) = mpsc::channel();
        let cache = &cache;
        scope.spawn(move || {
            assert_eq!(&*cache.get(&raw).unwrap(), b"12345678");
            assert_eq!(
                &*cache.get_by_identity(&raw.identity()).unwrap(),
                b"12345678"
            );
            assert_eq!(
                &*cache.get_verified(&verified, [3; 32]).unwrap(),
                b"compact"
            );
            tx.send(()).unwrap();
        });
        let completed = rx.recv_timeout(Duration::from_secs(5)).is_ok();
        // Release locks before assertions so a global-lock regression terminates.
        drop(unrelated);
        drop(admission);
        assert!(completed, "unrelated locks blocked cache hits");
    });
    assert_eq!(cache.snapshot().hit_count, 3);
}

#[test]
fn skewed_shard_can_use_the_entire_global_budget() {
    let cache = SegmentCache::new(512);
    let keys = keys_in_shard(&cache, 11, 65);
    for key in &keys[..64] {
        drop(cache.insert(*key, b"12345678".to_vec()).unwrap());
    }
    let full = cache.snapshot();
    assert_eq!(full.resident_bytes, 512);
    assert_eq!(full.entry_count, 64);
    assert_eq!(full.eviction_count, 0);
    drop(cache.insert(keys[64], b"12345678".to_vec()).unwrap());
    let after = cache.snapshot();
    assert_eq!(after.resident_bytes, 512);
    assert_eq!(after.entry_count, 64);
    assert_eq!(after.eviction_count, 1);
}

#[test]
fn eviction_reclaims_other_shards_without_evicting_exported_handles() {
    let cache = SegmentCache::new(32);
    let keys: Vec<_> = [0, 5, 10, 15]
        .map(|index| keys_in_shard(&cache, index, 1)[0])
        .into();
    let mut pins = Vec::new();
    for (index, key) in keys.iter().enumerate() {
        let lease = cache.insert(*key, b"12345678".to_vec()).unwrap();
        if index < 2 {
            pins.push(lease.into_bytes());
        }
    }
    let next = key(1_000_001, b"abcdefghijklmnop");
    let lease = cache.insert(next, b"abcdefghijklmnop".to_vec()).unwrap();
    let snapshot = cache.snapshot();
    assert_eq!(snapshot.resident_bytes, 32);
    assert_eq!(snapshot.pinned_bytes, 32);
    assert_eq!(snapshot.eviction_count, 2);
    for key in &keys[..2] {
        assert!(cache.get(key).is_some());
    }
    assert!(matches!(
        cache.insert(key(1_000_002, b"x"), b"x".to_vec()),
        Err(SegmentCacheAdmissionError {
            error: SegmentCacheError::PinnedCapacity {
                pinned_bytes: 32,
                ..
            },
            ..
        })
    ));
    drop(pins);
    assert_eq!(cache.snapshot().pinned_bytes, 16);
    drop(lease);
    assert_eq!(cache.snapshot().pinned_bytes, 0);
}

#[test]
fn zero_capacity_and_oversized_admission_preserve_existing_entries() {
    let empty = SegmentCache::new(0);
    drop(empty.insert(key(1, b""), b"".to_vec()).unwrap());
    assert!(matches!(
        empty.insert(key(2, b"x"), b"x".to_vec()),
        Err(SegmentCacheAdmissionError {
            error: SegmentCacheError::EntryTooLarge { .. },
            ..
        })
    ));
    assert_eq!(empty.snapshot().entry_count, 1);
    let cache = SegmentCache::new(8);
    drop(
        cache
            .insert(key(1, b"12345678"), b"12345678".to_vec())
            .unwrap(),
    );
    assert!(matches!(
        cache.insert(key(2, b"123456789"), b"123456789".to_vec()),
        Err(SegmentCacheAdmissionError {
            error: SegmentCacheError::EntryTooLarge { .. },
            ..
        })
    ));
    assert!(cache.get(&key(1, b"12345678")).is_some());
    assert_eq!(cache.snapshot().eviction_count, 0);
}

#[test]
fn concurrent_identity_collision_has_exactly_one_winner() {
    let cache = SegmentCache::new(128);
    let barrier = Barrier::new(16);
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..16u8)
            .map(|byte| {
                let cache = &cache;
                let barrier = &barrier;
                scope.spawn(move || {
                    let bytes = [byte; 8];
                    barrier.wait();
                    match cache.insert(key(1, &bytes), bytes.to_vec()) {
                        Ok(lease) => {
                            assert_eq!(&*lease, &bytes);
                            true
                        }
                        Err(SegmentCacheAdmissionError {
                            error: SegmentCacheError::IdentityCollision { .. },
                            ..
                        }) => false,
                        other => panic!("unexpected collision result: {other:?}"),
                    }
                })
            })
            .collect();
        assert_eq!(
            handles
                .into_iter()
                .map(|handle| usize::from(handle.join().unwrap()))
                .sum::<usize>(),
            1
        );
    });
    let snapshot = cache.snapshot();
    assert_eq!(snapshot.entry_count, 1);
    assert_eq!(snapshot.resident_bytes, 8);
    assert_eq!(snapshot.digest_mismatch_count, 15);
}

fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn payload(id: u64) -> Vec<u8> {
    vec![id as u8; (id % 4 + 1) as usize]
}

// The oracle permits any unpinned victim under pressure, not one implementation's
// CLOCK order. It checks the complete resident set through public lookups.
fn serial_campaign(seed: u64, steps: usize) {
    let capacity = 16;
    let cache = SegmentCache::new(capacity);
    let mut resident = BTreeMap::<u64, Vec<u8>>::new();
    let mut pins = Vec::<(u64, SegmentBytes)>::new();
    let mut state = seed;
    let mut expected = SegmentCacheSnapshot {
        capacity_bytes: capacity,
        ..Default::default()
    };
    for step in 0..steps {
        let random = next_random(&mut state);
        let id = (random >> 8) % 24;
        let bytes = payload(id);
        let cache_key = key(id, &bytes);
        let pinned_ids: BTreeSet<_> = pins.iter().map(|(id, _)| *id).collect();
        let pinned_bytes: u64 = resident
            .iter()
            .filter(|(id, _)| pinned_ids.contains(id))
            .map(|(_, bytes)| bytes.len() as u64)
            .sum();
        let mut may_evict = false;
        match random % 8 {
            0 | 3 => {
                let existing = resident.contains_key(&id);
                let before_bytes: u64 = resident.values().map(|bytes| bytes.len() as u64).sum();
                may_evict = !existing && before_bytes + bytes.len() as u64 > capacity;
                let result = cache.insert(cache_key, bytes.clone());
                if existing {
                    assert_eq!(&*result.unwrap(), bytes.as_slice());
                    expected.hit_count += 1;
                } else if pinned_bytes + bytes.len() as u64 > capacity {
                    assert!(
                        matches!(
                            result,
                            Err(SegmentCacheAdmissionError {
                                error: SegmentCacheError::PinnedCapacity { .. },
                                ..
                            })
                        ),
                        "seed={seed} step={step}"
                    );
                    expected.admission_rejection_count += 1;
                } else {
                    let lease = result.unwrap();
                    assert_eq!(&*lease, bytes.as_slice());
                    resident.insert(id, bytes.clone());
                    expected.insertion_count += 1;
                    if random % 8 == 3 {
                        pins.push((id, lease.into_bytes()));
                    }
                }
            }
            1 | 4 => match cache.get(&cache_key) {
                Some(lease) => {
                    assert!(resident.contains_key(&id));
                    assert_eq!(&*lease, bytes.as_slice());
                    expected.hit_count += 1;
                    pins.push((id, lease.into_bytes()));
                }
                None => {
                    assert!(!resident.contains_key(&id));
                    expected.miss_count += 1;
                }
            },
            2 => {
                if !pins.is_empty() {
                    pins.swap_remove((random >> 16) as usize % pins.len());
                }
            }
            5 => {
                let bad = SegmentCacheKey {
                    content_digest: ContentDigest(cache_key.content_digest.0 ^ 1),
                    ..cache_key
                };
                assert!(matches!(
                    cache.insert(bad, bytes),
                    Err(SegmentCacheAdmissionError {
                        error: SegmentCacheError::DigestMismatch { .. },
                        ..
                    })
                ));
                expected.digest_mismatch_count += 1;
            }
            6 if !pins.is_empty() => {
                let (id, bytes) = &pins[(random >> 16) as usize % pins.len()];
                pins.push((*id, bytes.clone()));
            }
            _ => {}
        }
        resident.retain(|id, bytes| {
            if let Some(lease) = cache.get(&key(*id, bytes)) {
                assert_eq!(&*lease, bytes.as_slice());
                expected.hit_count += 1;
                true
            } else {
                assert!(
                    may_evict && !pinned_ids.contains(id),
                    "lost protected entry: seed={seed} step={step} id={id}"
                );
                expected.miss_count += 1;
                expected.eviction_count += 1;
                false
            }
        });
        let pinned_ids: BTreeSet<_> = pins.iter().map(|(id, _)| *id).collect();
        expected.entry_count = resident.len();
        expected.resident_bytes = resident.values().map(|bytes| bytes.len() as u64).sum();
        expected.pinned_bytes = resident
            .iter()
            .filter(|(id, _)| pinned_ids.contains(id))
            .map(|(_, bytes)| bytes.len() as u64)
            .sum();
        expected.reclaimable_bytes = expected.resident_bytes - expected.pinned_bytes;
        assert!(expected.resident_bytes <= capacity);
        assert_eq!(cache.snapshot(), expected, "seed={seed} step={step}");
    }
    drop(pins);
    assert_eq!(cache.snapshot().pinned_bytes, 0);
}

fn concurrent_campaign(seed: u64, steps: usize) {
    let cache = SegmentCache::new(64);
    let barrier = Barrier::new(8);
    std::thread::scope(|scope| {
        for worker in 0..8 {
            let cache = &cache;
            let barrier = &barrier;
            scope.spawn(move || {
                let mut state = seed + worker + 1;
                let mut pins = Vec::<(SegmentCacheKey, SegmentBytes)>::new();
                barrier.wait();
                for _ in 0..steps {
                    let random = next_random(&mut state);
                    let id = random % 32;
                    let bytes = [id as u8; 8];
                    let cache_key = key(id, &bytes);
                    match cache.insert(cache_key, bytes.to_vec()) {
                        Ok(lease) => {
                            assert_eq!(&*lease, &bytes);
                            if random.is_multiple_of(3) {
                                pins.push((cache_key, lease.into_bytes()));
                            }
                        }
                        Err(SegmentCacheAdmissionError {
                            error:
                                SegmentCacheError::PinnedCapacity {
                                    resident_bytes,
                                    pinned_bytes,
                                    ..
                                },
                            ..
                        }) => {
                            assert!(pinned_bytes <= resident_bytes && resident_bytes <= 64);
                        }
                        other => panic!("unexpected admission: {other:?}"),
                    }
                    for (key, arc) in &pins {
                        assert_eq!(
                            &*cache.get(key).expect("pinned entry must remain resident"),
                            arc.as_ref()
                        );
                    }
                    if pins.len() > 3 {
                        pins.remove(0);
                    }
                    let snapshot = cache.snapshot();
                    assert!(snapshot.resident_bytes <= 64);
                    assert!(snapshot.pinned_bytes <= snapshot.resident_bytes);
                    assert_eq!(snapshot.resident_bytes, snapshot.entry_count as u64 * 8);
                    assert_eq!(
                        snapshot.pinned_bytes + snapshot.reclaimable_bytes,
                        snapshot.resident_bytes
                    );
                    assert_eq!(
                        snapshot.insertion_count - snapshot.eviction_count,
                        snapshot.entry_count as u64
                    );
                }
            });
        }
    });
    assert_eq!(cache.snapshot().pinned_bytes, 0);
}

#[test]
fn sharded_cache_matches_serial_model_and_concurrent_invariants() {
    for seed in [7, 31, 127] {
        serial_campaign(seed, 128);
        concurrent_campaign(seed, 64);
    }
}

#[test]
#[ignore = "extended local cache state-machine and concurrent campaign"]
fn sharded_cache_state_machine_campaign() {
    for seed in 1..=64 {
        serial_campaign(seed, 512);
        concurrent_campaign(seed, 128);
    }
    eprintln!("cache-sharding campaign: 32768 serial operations and 65536 concurrent operations");
}
