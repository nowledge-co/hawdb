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

use hawdb_integrity::checksum_u64;
use std::collections::{hash_map::RandomState, BTreeMap, VecDeque};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::hash::BuildHasher;
use std::ops::Deref;
use std::sync::{Arc, Mutex, MutexGuard};

mod bytes;
pub use bytes::SegmentBytes;
use bytes::SegmentPayload;

#[cfg(test)]
thread_local! {
    pub(crate) static PAGE_INTEGRITY_CHECKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn record_page_integrity_check() {
    PAGE_INTEGRITY_CHECKS.set(PAGE_INTEGRITY_CHECKS.get() + 1);
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StoreId(pub u128);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ManifestGeneration(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentDigest(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum RepresentationKind {
    RawBytes,
    DecodedMetadata,
    NodeSegment,
    RelationshipSegment,
    PropertySegment,
    AdjacencySegment,
    RelationalIndexPageSlot,
    RelationalIndexRecoveryDelta,
    RelationalRowPageSlot,
    StableIdentityPageSlot,
    CanonicalSegmentDescriptorPage,
    CanonicalAdjacencyDescriptorPage,
    PropertyProjectionDescriptorPage,
    PropertySpillDescriptorPage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SegmentCacheIdentity {
    pub(crate) store_id: StoreId,
    pub(crate) manifest_generation: ManifestGeneration,
    pub(crate) segment_id: u64,
    pub(crate) representation: RepresentationKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentCacheKey {
    pub store_id: StoreId,
    pub manifest_generation: ManifestGeneration,
    pub segment_id: u64,
    pub content_digest: ContentDigest,
    pub representation: RepresentationKind,
}

impl SegmentCacheKey {
    pub(crate) const fn identity(self) -> SegmentCacheIdentity {
        SegmentCacheIdentity {
            store_id: self.store_id,
            manifest_generation: self.manifest_generation,
            segment_id: self.segment_id,
            representation: self.representation,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentCacheSnapshot {
    pub capacity_bytes: u64,
    pub resident_bytes: u64,
    pub pinned_bytes: u64,
    pub reclaimable_bytes: u64,
    pub entry_count: usize,
    pub hit_count: u64,
    pub miss_count: u64,
    pub insertion_count: u64,
    pub eviction_count: u64,
    pub digest_mismatch_count: u64,
    pub admission_rejection_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SegmentCacheError {
    DigestMismatch {
        expected: ContentDigest,
        actual: ContentDigest,
    },
    DigestCollision {
        key: SegmentCacheKey,
    },
    IdentityCollision {
        requested_key: SegmentCacheKey,
        resident_digest: ContentDigest,
    },
    EntryTooLarge {
        entry_bytes: u64,
        capacity_bytes: u64,
    },
    PinnedCapacity {
        requested_bytes: u64,
        resident_bytes: u64,
        pinned_bytes: u64,
        capacity_bytes: u64,
    },
}

impl Display for SegmentCacheError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::DigestMismatch { expected, actual } => write!(
                formatter,
                "segment content digest mismatch: expected {}, got {}",
                expected.0, actual.0
            ),
            Self::DigestCollision { key } => write!(
                formatter,
                "segment cache key collision for generation {} segment {}",
                key.manifest_generation.0, key.segment_id
            ),
            Self::IdentityCollision {
                requested_key,
                resident_digest,
            } => write!(
                formatter,
                "segment cache immutable identity collision for generation {} segment {}: resident digest {}, requested digest {}",
                requested_key.manifest_generation.0,
                requested_key.segment_id,
                resident_digest.0,
                requested_key.content_digest.0
            ),
            Self::EntryTooLarge {
                entry_bytes,
                capacity_bytes,
            } => write!(
                formatter,
                "segment cache entry uses {entry_bytes} bytes, exceeding the {capacity_bytes} byte capacity"
            ),
            Self::PinnedCapacity {
                requested_bytes,
                resident_bytes,
                pinned_bytes,
                capacity_bytes,
            } => write!(
                formatter,
                "segment cache cannot admit {requested_bytes} bytes with {resident_bytes} resident and {pinned_bytes} pinned under the {capacity_bytes} byte capacity"
            ),
        }
    }
}

impl Error for SegmentCacheError {}

/// A rejected admission, retaining the original owned input for uncached use.
#[derive(Debug)]
pub struct SegmentCacheAdmissionError {
    error: SegmentCacheError,
    bytes: Vec<u8>,
}

impl SegmentCacheAdmissionError {
    pub fn error(&self) -> &SegmentCacheError {
        &self.error
    }

    pub fn into_parts(self) -> (SegmentCacheError, Vec<u8>) {
        (self.error, self.bytes)
    }
}

impl Display for SegmentCacheAdmissionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl Error for SegmentCacheAdmissionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.error)
    }
}

#[derive(Debug, Clone)]
pub struct SegmentCacheLease {
    bytes: SegmentBytes,
    page_integrity_verified: bool,
}

impl SegmentCacheLease {
    pub fn into_bytes(self) -> SegmentBytes {
        self.bytes
    }

    pub(crate) const fn page_integrity_verified(&self) -> bool {
        self.page_integrity_verified
    }
}

impl Deref for SegmentCacheLease {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

#[derive(Debug)]
pub struct SegmentCache {
    // Admission precedes any shard lock. Hits take only their own shard;
    // snapshots lock all shards in index order while holding admission.
    admission: Mutex<SegmentCacheAdmission>,
    shards: [Arc<Mutex<SegmentCacheShard>>; CACHE_SHARD_COUNT],
    shard_hasher: RandomState,
}

const CACHE_SHARD_COUNT: usize = 16;

#[derive(Debug)]
struct SegmentCacheAdmission {
    capacity_bytes: u64,
    resident_bytes: u64,
    eviction_cursor: usize,
    insertion_count: u64,
    eviction_count: u64,
    digest_mismatch_count: u64,
    admission_rejection_count: u64,
}

#[derive(Debug, Default)]
struct SegmentCacheShard {
    entries: BTreeMap<SegmentCacheIdentity, SegmentCacheEntry>,
    clock: VecDeque<SegmentCacheIdentity>,
    hit_count: u64,
    miss_count: u64,
    pinned_bytes: u64,
}

#[derive(Debug)]
struct SegmentCacheEntry {
    key: SegmentCacheKey,
    payload: Arc<SegmentPayload>,
    verification_tag: Option<[u8; 32]>,
    page_integrity_verified: bool,
    referenced: bool,
}

impl SegmentCacheEntry {
    fn lease(&self) -> (SegmentCacheLease, u64) {
        let (bytes, charge) = self.payload.lease();
        (
            SegmentCacheLease {
                bytes,
                page_integrity_verified: self.page_integrity_verified,
            },
            charge,
        )
    }
}

impl SegmentCache {
    pub fn new(capacity_bytes: u64) -> Self {
        Self {
            admission: Mutex::new(SegmentCacheAdmission {
                capacity_bytes,
                resident_bytes: 0,
                eviction_cursor: 0,
                insertion_count: 0,
                eviction_count: 0,
                digest_mismatch_count: 0,
                admission_rejection_count: 0,
            }),
            shards: std::array::from_fn(|_| Arc::new(Mutex::new(SegmentCacheShard::default()))),
            shard_hasher: RandomState::new(),
        }
    }

    pub fn get(&self, key: &SegmentCacheKey) -> Option<SegmentCacheLease> {
        let mut inner = self.lock_shard(&key.identity());
        let (lease, charge) = match inner.entries.get_mut(&key.identity()) {
            Some(entry)
                if entry.key.content_digest == key.content_digest
                    && entry.verification_tag.is_none() =>
            {
                entry.referenced = true;
                entry.lease()
            }
            Some(_) | None => {
                inner.miss_count = inner.miss_count.saturating_add(1);
                return None;
            }
        };
        inner.pinned_bytes = inner.pinned_bytes.saturating_add(charge);
        inner.hit_count = inner.hit_count.saturating_add(1);
        Some(lease)
    }

    /// Looks up an immutable representation that was fully validated before
    /// cache admission.
    ///
    /// The compact cached bytes do not need to have the same digest as their
    /// physical source extent. Callers must bind the immutable physical digest
    /// through `verification_tag` and must use the same tag for lookup and
    /// insertion.
    pub(crate) fn get_verified(
        &self,
        key: &SegmentCacheKey,
        verification_tag: [u8; 32],
    ) -> Option<SegmentCacheLease> {
        let mut inner = self.lock_shard(&key.identity());
        let (lease, charge) = match inner.entries.get_mut(&key.identity()) {
            Some(entry)
                if entry.key.content_digest == key.content_digest
                    && entry.verification_tag == Some(verification_tag) =>
            {
                entry.referenced = true;
                entry.lease()
            }
            Some(_) | None => {
                inner.miss_count = inner.miss_count.saturating_add(1);
                return None;
            }
        };
        inner.pinned_bytes = inner.pinned_bytes.saturating_add(charge);
        inner.hit_count = inner.hit_count.saturating_add(1);
        Some(lease)
    }

    /// Looks up bytes by their immutable physical identity after a prior
    /// insertion established and verified the content digest.
    pub(crate) fn get_by_identity(
        &self,
        identity: &SegmentCacheIdentity,
    ) -> Option<SegmentCacheLease> {
        let mut inner = self.lock_shard(identity);
        let (lease, charge) = match inner.entries.get_mut(identity) {
            Some(entry) if entry.verification_tag.is_none() => {
                entry.referenced = true;
                entry.lease()
            }
            Some(_) | None => {
                inner.miss_count = inner.miss_count.saturating_add(1);
                return None;
            }
        };
        inner.pinned_bytes = inner.pinned_bytes.saturating_add(charge);
        inner.hit_count = inner.hit_count.saturating_add(1);
        Some(lease)
    }

    pub fn insert(
        &self,
        key: SegmentCacheKey,
        bytes: Vec<u8>,
    ) -> Result<SegmentCacheLease, SegmentCacheAdmissionError> {
        self.insert_checked(key, bytes, false)
    }

    /// Admits an immutable physical page slot after its codec has verified both
    /// CRC32C and SHA-256. Raw public insertion never establishes this proof.
    /// Readers must still enforce their own identity, structure, and read limits.
    pub(crate) fn insert_page_verified(
        &self,
        key: SegmentCacheKey,
        bytes: Vec<u8>,
    ) -> Result<SegmentCacheLease, SegmentCacheAdmissionError> {
        self.insert_checked(key, bytes, true)
    }

    fn insert_checked(
        &self,
        key: SegmentCacheKey,
        bytes: Vec<u8>,
        page_integrity_verified: bool,
    ) -> Result<SegmentCacheLease, SegmentCacheAdmissionError> {
        let actual_digest = content_digest(&bytes);
        if actual_digest != key.content_digest {
            self.record_digest_mismatch();
            return Err(SegmentCacheAdmissionError {
                error: SegmentCacheError::DigestMismatch {
                    expected: key.content_digest,
                    actual: actual_digest,
                },
                bytes,
            });
        }
        self.insert_inner(key, bytes, None, page_integrity_verified)
    }

    /// Admits compact bytes after the caller has validated their immutable
    /// physical source against a strong digest.
    pub(crate) fn insert_verified(
        &self,
        key: SegmentCacheKey,
        verification_tag: [u8; 32],
        bytes: Vec<u8>,
    ) -> Result<SegmentCacheLease, SegmentCacheAdmissionError> {
        self.insert_inner(key, bytes, Some(verification_tag), false)
    }

    fn insert_inner(
        &self,
        key: SegmentCacheKey,
        bytes: Vec<u8>,
        verification_tag: Option<[u8; 32]>,
        page_integrity_verified: bool,
    ) -> Result<SegmentCacheLease, SegmentCacheAdmissionError> {
        let mut admission = self.lock_admission();
        let identity = key.identity();
        let mut inner = self.lock_shard(&identity);
        if let Some(resident_digest) = inner
            .entries
            .get(&identity)
            .map(|entry| entry.key.content_digest)
        {
            if resident_digest != key.content_digest {
                admission.digest_mismatch_count = admission.digest_mismatch_count.saturating_add(1);
                return Err(SegmentCacheAdmissionError {
                    error: SegmentCacheError::IdentityCollision {
                        requested_key: key,
                        resident_digest,
                    },
                    bytes,
                });
            }
            if inner.entries.get(&identity).is_some_and(|entry| {
                entry.verification_tag != verification_tag
                    || entry.payload.data() != bytes.as_slice()
            }) {
                admission.digest_mismatch_count = admission.digest_mismatch_count.saturating_add(1);
                return Err(SegmentCacheAdmissionError {
                    error: SegmentCacheError::DigestCollision { key },
                    bytes,
                });
            }
            let entry = inner
                .entries
                .get_mut(&identity)
                .expect("cache identity remains resident");
            entry.referenced = true;
            // Promotion is safe only after the exact resident bytes matched.
            // Re-inserting raw bytes must not downgrade an existing proof.
            entry.page_integrity_verified |= page_integrity_verified;
            let (lease, charge) = entry.lease();
            inner.pinned_bytes = inner.pinned_bytes.saturating_add(charge);
            inner.hit_count = inner.hit_count.saturating_add(1);
            return Ok(lease);
        }

        let entry_bytes = bytes.len() as u64;
        if entry_bytes > admission.capacity_bytes {
            admission.admission_rejection_count =
                admission.admission_rejection_count.saturating_add(1);
            return Err(SegmentCacheAdmissionError {
                error: SegmentCacheError::EntryTooLarge {
                    entry_bytes,
                    capacity_bytes: admission.capacity_bytes,
                },
                bytes,
            });
        }
        // Eviction can visit this shard too. Admission keeps identities stable
        // while the shard lock is released, without blocking unrelated hits.
        drop(inner);
        if !self.evict_for(&mut admission, entry_bytes) {
            admission.admission_rejection_count =
                admission.admission_rejection_count.saturating_add(1);
            return Err(SegmentCacheAdmissionError {
                error: SegmentCacheError::PinnedCapacity {
                    requested_bytes: entry_bytes,
                    resident_bytes: admission.resident_bytes,
                    pinned_bytes: self
                        .lock_shards()
                        .iter()
                        .map(|shard| shard.pinned_bytes)
                        .fold(0, u64::saturating_add),
                    capacity_bytes: admission.capacity_bytes,
                },
                bytes,
            });
        }

        let mut inner = self.lock_shard(&identity);
        admission.resident_bytes = admission.resident_bytes.saturating_add(entry_bytes);
        admission.insertion_count = admission.insertion_count.saturating_add(1);
        inner.clock.push_back(identity);
        inner.entries.insert(
            identity,
            SegmentCacheEntry {
                key,
                payload: SegmentPayload::cached(
                    bytes,
                    Arc::downgrade(&self.shards[self.shard_index(&identity)]),
                ),
                verification_tag,
                page_integrity_verified,
                referenced: true,
            },
        );
        let (lease, charge) = inner
            .entries
            .get(&identity)
            .expect("admitted cache identity remains resident")
            .lease();
        inner.pinned_bytes = inner.pinned_bytes.saturating_add(charge);
        Ok(lease)
    }

    pub fn snapshot(&self) -> SegmentCacheSnapshot {
        let inner = self.lock_admission();
        let shards = self.lock_shards();
        // Admission and the fixed shard set provide one coherent snapshot;
        // handle lifecycle updates maintain each shard's pin total incrementally.
        let pinned_bytes = shards
            .iter()
            .map(|shard| shard.pinned_bytes)
            .fold(0, u64::saturating_add);
        SegmentCacheSnapshot {
            capacity_bytes: inner.capacity_bytes,
            resident_bytes: inner.resident_bytes,
            pinned_bytes,
            reclaimable_bytes: inner.resident_bytes.saturating_sub(pinned_bytes),
            entry_count: shards.iter().map(|shard| shard.entries.len()).sum(),
            hit_count: shards
                .iter()
                .map(|shard| shard.hit_count)
                .fold(0, u64::saturating_add),
            miss_count: shards
                .iter()
                .map(|shard| shard.miss_count)
                .fold(0, u64::saturating_add),
            insertion_count: inner.insertion_count,
            eviction_count: inner.eviction_count,
            digest_mismatch_count: inner.digest_mismatch_count,
            admission_rejection_count: inner.admission_rejection_count,
        }
    }

    pub(crate) fn record_digest_mismatch(&self) {
        let mut inner = self.lock_admission();
        inner.digest_mismatch_count = inner.digest_mismatch_count.saturating_add(1);
    }

    fn lock_admission(&self) -> MutexGuard<'_, SegmentCacheAdmission> {
        lock_unpoisoned(&self.admission)
    }

    fn shard_index(&self, identity: &SegmentCacheIdentity) -> usize {
        // Different requested digests for one immutable identity must collide
        // in the same shard so the existing entry cannot be bypassed.
        self.shard_hasher.hash_one(identity) as usize % CACHE_SHARD_COUNT
    }

    fn lock_shard(&self, identity: &SegmentCacheIdentity) -> MutexGuard<'_, SegmentCacheShard> {
        lock_unpoisoned(&self.shards[self.shard_index(identity)])
    }

    fn lock_shards(&self) -> [MutexGuard<'_, SegmentCacheShard>; CACHE_SHARD_COUNT] {
        self.shards.each_ref().map(|shard| lock_unpoisoned(shard))
    }

    fn evict_for(&self, admission: &mut SegmentCacheAdmission, requested_bytes: u64) -> bool {
        let allowed_resident = admission.capacity_bytes - requested_bytes;
        let start = admission.eviction_cursor;
        for offset in 0..CACHE_SHARD_COUNT {
            if admission.resident_bytes <= allowed_resident {
                return true;
            }
            let index = (start + offset) % CACHE_SHARD_COUNT;
            let mut shard = lock_unpoisoned(&self.shards[index]);
            shard.evict_bytes(admission.resident_bytes - allowed_resident, admission);
            admission.eviction_cursor = (index + 1) % CACHE_SHARD_COUNT;
        }
        admission.resident_bytes <= allowed_resident
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl SegmentCacheShard {
    fn evict_bytes(&mut self, requested_bytes: u64, admission: &mut SegmentCacheAdmission) {
        let mut reclaimed_bytes = 0u64;
        // Both CLOCK passes stay under this shard lock: concurrent hits cannot
        // continually re-arm the reference bits and cause a false rejection.
        let max_scans = self.clock.len().saturating_mul(2).saturating_add(1);
        for _ in 0..max_scans {
            if reclaimed_bytes >= requested_bytes {
                return;
            }
            let Some(key) = self.clock.pop_front() else {
                break;
            };
            let Some(entry) = self.entries.get_mut(&key) else {
                continue;
            };
            if entry.payload.is_pinned() {
                self.clock.push_back(key);
                continue;
            }
            if entry.referenced {
                entry.referenced = false;
                self.clock.push_back(key);
                continue;
            }
            let entry = self
                .entries
                .remove(&key)
                .expect("clock key remains resident");
            reclaimed_bytes = reclaimed_bytes.saturating_add(entry.payload.data().len() as u64);
            admission.resident_bytes = admission
                .resident_bytes
                .saturating_sub(entry.payload.data().len() as u64);
            admission.eviction_count = admission.eviction_count.saturating_add(1);
        }
    }
}

pub fn content_digest(bytes: &[u8]) -> ContentDigest {
    ContentDigest(checksum_u64(bytes))
}

#[cfg(test)]
mod shard_tests;

#[cfg(test)]
mod tests {
    use super::*;

    fn key(segment_id: u64, bytes: &[u8]) -> SegmentCacheKey {
        SegmentCacheKey {
            store_id: StoreId(7),
            manifest_generation: ManifestGeneration(3),
            segment_id,
            content_digest: content_digest(bytes),
            representation: RepresentationKind::RawBytes,
        }
    }

    #[test]
    fn clock_evicts_unpinned_entries_under_a_byte_budget() {
        let cache = SegmentCache::new(8);
        let first = cache.insert(key(1, b"aaaa"), b"aaaa".to_vec()).unwrap();
        drop(first);
        let second = cache.insert(key(2, b"bbbb"), b"bbbb".to_vec()).unwrap();
        drop(second);
        let third = cache.insert(key(3, b"cccc"), b"cccc".to_vec()).unwrap();
        drop(third);

        let snapshot = cache.snapshot();
        assert_eq!(snapshot.resident_bytes, 8);
        assert_eq!(snapshot.entry_count, 2);
        assert_eq!(snapshot.eviction_count, 1);
    }

    #[test]
    fn pinned_leases_are_not_evicted_or_hidden_from_accounting() {
        let cache = SegmentCache::new(4);
        let pinned = cache.insert(key(1, b"aaaa"), b"aaaa".to_vec()).unwrap();
        let error = cache.insert(key(2, b"bbbb"), b"bbbb".to_vec()).unwrap_err();

        assert!(matches!(
            error.error(),
            SegmentCacheError::PinnedCapacity { .. }
        ));
        assert_eq!(&*pinned, b"aaaa");
        let snapshot = cache.snapshot();
        assert_eq!(snapshot.pinned_bytes, 4);
        assert_eq!(snapshot.reclaimable_bytes, 0);
        assert_eq!(snapshot.admission_rejection_count, 1);
    }

    #[test]
    fn digest_mismatch_fails_closed_without_residency() {
        let cache = SegmentCache::new(16);
        let mut wrong_key = key(1, b"expected");
        wrong_key.content_digest = ContentDigest(0);
        let error = cache.insert(wrong_key, b"actual".to_vec()).unwrap_err();

        assert!(matches!(
            error.error(),
            SegmentCacheError::DigestMismatch { .. }
        ));
        let snapshot = cache.snapshot();
        assert_eq!(snapshot.resident_bytes, 0);
        assert_eq!(snapshot.digest_mismatch_count, 1);
    }

    #[test]
    fn cache_identity_separates_store_generation_and_representation() {
        let cache = SegmentCache::new(64);
        let bytes = &b"same"[..];
        let base = key(1, bytes);
        let first = cache.insert(base, bytes.to_vec()).unwrap();
        drop(first);
        let mut next_generation = base;
        next_generation.manifest_generation = ManifestGeneration(4);
        let second = cache.insert(next_generation, bytes.to_vec()).unwrap();
        drop(second);
        let mut decoded = base;
        decoded.representation = RepresentationKind::DecodedMetadata;
        let third = cache.insert(decoded, bytes.to_vec()).unwrap();
        drop(third);

        assert_eq!(cache.snapshot().entry_count, 3);
    }

    #[test]
    fn immutable_identity_lookup_reuses_the_verified_digest() {
        let cache = SegmentCache::new(16);
        let key = key(1, b"page");
        drop(cache.insert(key, b"page".to_vec()).unwrap());

        let lease = cache
            .get_by_identity(&key.identity())
            .expect("verified immutable identity remains cached");

        assert_eq!(&*lease, b"page");
        assert_eq!(cache.snapshot().hit_count, 1);
    }

    #[test]
    fn immutable_identity_rejects_different_content_in_one_generation() {
        let cache = SegmentCache::new(16);
        let first = key(1, b"page-a");
        drop(cache.insert(first, b"page-a".to_vec()).unwrap());
        let second = SegmentCacheKey {
            content_digest: content_digest(b"page-b"),
            ..first
        };

        let error = cache.insert(second, b"page-b".to_vec()).unwrap_err();

        assert!(matches!(
            error.error(),
            SegmentCacheError::IdentityCollision { .. }
        ));
        assert_eq!(cache.snapshot().entry_count, 1);
        assert_eq!(cache.snapshot().digest_mismatch_count, 1);
    }

    #[test]
    fn verified_compact_entries_require_the_strong_source_tag() {
        let cache = SegmentCache::new(16);
        let mut cache_key = key(1, b"physical-slot");
        cache_key.representation = RepresentationKind::RelationalRowPageSlot;
        let tag = [7; 32];
        drop(
            cache
                .insert_verified(cache_key, tag, b"page".to_vec())
                .unwrap(),
        );

        assert!(cache.get(&cache_key).is_none());
        assert!(cache.get_verified(&cache_key, [8; 32]).is_none());
        let lease = cache
            .get_verified(&cache_key, tag)
            .expect("matching verified page remains cached");

        assert_eq!(&*lease, b"page");
        assert!(!lease.page_integrity_verified());
        let snapshot = cache.snapshot();
        assert_eq!(snapshot.resident_bytes, 4);
        assert_eq!(snapshot.hit_count, 1);
        assert_eq!(snapshot.miss_count, 2);
    }

    #[test]
    fn page_verification_promotes_only_identical_resident_bytes() {
        let cache = SegmentCache::new(16);
        let cache_key = key(1, b"page");
        let raw = cache.insert(cache_key, b"page".to_vec()).unwrap();
        assert!(!raw.page_integrity_verified());
        let verified = cache
            .insert_page_verified(cache_key, b"page".to_vec())
            .unwrap();
        assert!(verified.page_integrity_verified());
        assert!(verified.clone().page_integrity_verified());
        assert!(!raw.page_integrity_verified());
        assert!(cache
            .insert(cache_key, b"page".to_vec())
            .unwrap()
            .page_integrity_verified());
        assert!(cache.get(&cache_key).unwrap().page_integrity_verified());
        assert!(cache
            .get_by_identity(&cache_key.identity())
            .unwrap()
            .page_integrity_verified());

        assert!(matches!(
            cache.insert_page_verified(cache_key, b"different".to_vec()),
            Err(SegmentCacheAdmissionError {
                error: SegmentCacheError::DigestMismatch { .. },
                ..
            })
        ));
        assert!(matches!(
            cache.insert_inner(cache_key, b"collision".to_vec(), None, true),
            Err(SegmentCacheAdmissionError {
                error: SegmentCacheError::DigestCollision { .. },
                ..
            })
        ));
        let mut exported = verified.into_bytes().to_vec();
        exported[0] = b'P';
        assert_eq!(&*cache.get(&cache_key).unwrap(), b"page");
        let other_cache = SegmentCache::new(16);
        assert!(!other_cache
            .insert(key(1, &exported), exported)
            .unwrap()
            .page_integrity_verified());
    }

    #[test]
    fn verified_page_pins_and_eviction_preserve_tracked_byte_ownership() {
        let cache = SegmentCache::new(4);
        let first = key(1, b"page");
        let lease = cache.insert_page_verified(first, b"page".to_vec()).unwrap();
        let exported = lease.into_bytes();
        let shared = exported.clone();
        assert_eq!(cache.snapshot().pinned_bytes, 4);
        drop(exported);
        assert_eq!(cache.snapshot().pinned_bytes, 4);
        assert!(matches!(
            cache.insert(key(2, b"next"), b"next".to_vec()),
            Err(SegmentCacheAdmissionError {
                error: SegmentCacheError::PinnedCapacity { .. },
                ..
            })
        ));
        drop(shared);
        assert_eq!(cache.snapshot().pinned_bytes, 0);
        drop(cache.insert(key(2, b"next"), b"next".to_vec()).unwrap());
        assert!(cache.get(&first).is_none());
        assert!(!cache
            .insert(first, b"page".to_vec())
            .unwrap()
            .page_integrity_verified());
        assert_eq!(cache.snapshot().eviction_count, 2);
    }
}
