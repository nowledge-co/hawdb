use super::*;
use crate::cache::{
    content_digest, ManifestGeneration, RepresentationKind, SegmentCache,
    SegmentCacheAdmissionError, SegmentCacheError, SegmentCacheKey, SegmentCacheLease, StoreId,
};
use std::time::{Duration, Instant};

fn key(id: u64, bytes: &[u8]) -> SegmentCacheKey {
    SegmentCacheKey {
        store_id: StoreId(1),
        manifest_generation: ManifestGeneration(1),
        segment_id: id,
        content_digest: content_digest(bytes),
        representation: RepresentationKind::RawBytes,
    }
}

#[test]
fn independent_gets_and_clones_share_one_pin_and_payload() {
    let cache = SegmentCache::new(4);
    let key = key(1, b"page");
    let bytes = b"page".to_vec();
    let pointer = bytes.as_ptr();
    let admitted = cache.insert(key, bytes).unwrap();
    let lease_clone = admitted.clone();
    let exported = admitted.into_bytes();
    let independent = cache.get(&key).unwrap().into_bytes();
    let clone = exported.clone();
    for bytes in [&exported, &independent, &clone] {
        assert_eq!(
            bytes.as_ptr(),
            pointer,
            "ownership must not copy the payload"
        );
    }
    assert_eq!(cache.snapshot().pinned_bytes, 4);
    drop((exported, independent, lease_clone));
    assert_eq!(cache.snapshot().pinned_bytes, 4);
    assert!(matches!(
        cache
            .insert(self::key(2, b"next"), b"next".to_vec())
            .unwrap_err()
            .error(),
        SegmentCacheError::PinnedCapacity {
            pinned_bytes: 4,
            ..
        }
    ));
    drop(clone);
    assert_eq!(cache.snapshot().pinned_bytes, 0);
    assert_eq!(cache.snapshot().reclaimable_bytes, 4);
    let repinned = cache.get(&key).unwrap();
    assert_eq!(cache.snapshot().pinned_bytes, 4);
    drop(repinned);
    assert_eq!(cache.snapshot().pinned_bytes, 0);
}

#[test]
fn every_rejected_admission_returns_the_original_vec_allocation() {
    fn rejected(
        admit: impl FnOnce(Vec<u8>) -> Result<SegmentCacheLease, SegmentCacheAdmissionError>,
    ) -> SegmentCacheError {
        let mut input = Vec::with_capacity(128);
        input.extend_from_slice(b"next");
        let pointer = input.as_ptr();
        let capacity = input.capacity();
        let error = admit(input).unwrap_err();
        assert_eq!(error.to_string(), error.error().to_string());
        assert!(std::error::Error::source(&error).is_some());
        let (cause, returned) = error.into_parts();
        assert_eq!(returned, b"next");
        assert_eq!(returned.as_ptr(), pointer);
        assert_eq!(returned.capacity(), capacity);
        cause
    }

    let cache = SegmentCache::new(4);
    let pinned = cache.insert(key(1, b"page"), b"page".to_vec()).unwrap();
    assert!(matches!(
        rejected(|input| cache.insert(key(2, b"wrong"), input)),
        SegmentCacheError::DigestMismatch { .. }
    ));
    assert!(matches!(
        rejected(|input| cache.insert(key(1, b"next"), input)),
        SegmentCacheError::IdentityCollision { .. }
    ));
    let verified = SegmentCache::new(4);
    drop(verified.insert(key(1, b"next"), b"next".to_vec()).unwrap());
    assert!(matches!(
        rejected(|input| verified.insert_verified(key(1, b"next"), [7; 32], input)),
        SegmentCacheError::DigestCollision { .. }
    ));
    assert!(matches!(
        rejected(|input| SegmentCache::new(3).insert(key(2, b"next"), input)),
        SegmentCacheError::EntryTooLarge { .. }
    ));
    assert!(matches!(
        rejected(|input| cache.insert(key(2, b"next"), input)),
        SegmentCacheError::PinnedCapacity { .. }
    ));
    assert_eq!(&*pinned, b"page");
    assert_eq!(cache.snapshot().resident_bytes, 4);
    assert_eq!(cache.snapshot().pinned_bytes, 4);
}

#[test]
fn handles_outlive_cache_without_retaining_other_entries() {
    let cache = SegmentCache::new(8);
    let retained = cache
        .insert(key(1, b"page"), b"page".to_vec())
        .unwrap()
        .into_bytes();
    let unrelated = cache
        .insert(key(2, b"next"), b"next".to_vec())
        .unwrap()
        .into_bytes();
    let weak = Arc::downgrade(&unrelated.payload);
    drop(unrelated);
    assert!(weak.upgrade().is_some());
    drop(cache);
    assert!(weak.upgrade().is_none());
    assert_eq!(&*retained.clone(), b"page");
    let weak = Arc::downgrade(&retained.payload);
    drop(retained);
    assert!(weak.upgrade().is_none());
}

#[test]
fn uncached_owned_bytes_have_content_equality_and_independent_lifetime() {
    fn send_sync<T: Send + Sync>() {}
    send_sync::<SegmentBytes>();
    let owned = SegmentBytes::from(b"page".to_vec());
    let boxed = SegmentBytes::from(b"page".to_vec().into_boxed_slice());
    assert_eq!(owned, boxed);
    assert_ne!(owned, SegmentBytes::from(b"next".to_vec()));
    assert_eq!(format!("{owned:?}"), format!("{:?}", b"page"));
    let clone = owned.clone();
    assert_eq!(owned.as_ptr(), clone.as_ptr());
    drop(owned);
    assert_eq!(&*clone, b"page");
}

// Hold the actual shard across the last external decrement, then call the same
// entry leasing operation as get. This forces the race without scheduler luck
// or a production-only hook. Release locks before asserting timeout failures.
fn final_drop_overlap(drop_replacement: bool) {
    let cache = SegmentCache::new(4);
    let key = key(1, b"page");
    let bytes = cache.insert(key, b"page".to_vec()).unwrap().into_bytes();
    let payload = Arc::clone(&bytes.payload);
    let pin = payload.pin.as_ref().unwrap();
    let mut admission = cache.lock_admission();
    let mut shard = cache.lock_shard(&key.identity());
    std::thread::scope(|scope| {
        let dropping = scope.spawn(move || drop(bytes));
        let wait_for_drop = || {
            let deadline = Instant::now() + Duration::from_secs(5);
            while pin.external_handles.load(Ordering::Acquire) != 0 && Instant::now() < deadline {
                std::thread::yield_now();
            }
            pin.external_handles.load(Ordering::Acquire) == 0
        };
        if !wait_for_drop() {
            drop(shard);
            dropping.join().unwrap();
            panic!("final drop did not reach the owning shard");
        }
        shard.evict_bytes(4, &mut admission);
        let retained_entries = shard.entries.len();
        if retained_entries != 1 {
            drop(shard);
            dropping.join().unwrap();
            assert_eq!(
                retained_entries, 1,
                "waiting final drop still owns the pin charge"
            );
            return;
        }
        let (replacement, charge) = shard.entries[&key.identity()].lease();
        shard.pinned_bytes += charge;
        let repinned_bytes = shard.pinned_bytes;
        let replacement = if drop_replacement {
            let replacement_drop = scope.spawn(move || drop(replacement));
            let reached = wait_for_drop();
            drop(shard);
            replacement_drop.join().unwrap();
            assert!(reached, "replacement final drop did not reach the shard");
            None
        } else {
            drop(shard);
            Some(replacement)
        };
        dropping.join().unwrap();
        drop(admission);
        assert_eq!(
            repinned_bytes, 4,
            "repinning must inherit the existing charge"
        );
        assert_eq!(
            cache.snapshot().pinned_bytes,
            if drop_replacement { 0 } else { 4 }
        );
        drop(replacement);
    });
    assert_eq!(cache.snapshot().pinned_bytes, 0);
    // The old generation must not affect a subsequent admission's charge.
    drop(
        cache
            .insert(self::key(2, b"next"), b"next".to_vec())
            .unwrap(),
    );
    let next_generation = SegmentCacheKey {
        manifest_generation: ManifestGeneration(2),
        ..key
    };
    let replacement = cache.insert(next_generation, b"page".to_vec()).unwrap();
    assert_eq!(cache.snapshot().pinned_bytes, 4);
    drop(payload);
    assert_eq!(cache.snapshot().pinned_bytes, 4);
    drop(replacement);
    assert_eq!(cache.snapshot().pinned_bytes, 0);
}

#[test]
fn final_drop_and_new_get_preserve_one_charge() {
    final_drop_overlap(false);
}

#[test]
fn overlapping_final_drops_release_the_charge_once() {
    final_drop_overlap(true);
}
