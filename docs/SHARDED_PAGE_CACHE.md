# Sharded page cache

`SegmentCache` hashes immutable physical identity into 16 private shards. Each
shard owns its entries, CLOCK reference bits, hit/miss counters, and mutex.
`get`, `get_verified`, and `get_by_identity` lock only that shard. Requested
digests and compact-source verification tags are not part of shard selection:
changing either cannot bypass a resident immutable-identity collision check.

## Capacity and lock order

One admission mutex protects total resident payload bytes, capacity, admission
counters, and the next eviction shard. There are no fixed shard quotas. A
skewed workload may use the entire capacity in one shard, and admission may
reclaim space from any other shard.

The lock order is admission before shards. A hit never acquires admission.
Insertion releases its initial shard lock before cross-shard eviction, then
reacquires it to publish the entry while admission still prevents another
insertion of that identity. CLOCK completes both passes under each visited
shard's lock so hits cannot continually re-arm reference bits during that scan.
The starting shard rotates after a pressure scan; victim order is not a global
LRU or global CLOCK guarantee.

Snapshots acquire admission and then all shard locks in ascending index order.
This keeps residency, entry counts, pin totals and hit/miss counters coherent
with cache operations. Each shard maintains its pinned-byte total through
tracked payload lifetimes. Snapshot reads the fixed 16 counters and entry
counts; it never visits resident entries, so its work is independent of entry
count. Admission rejection uses those same counters.

The byte capacity still accounts for resident payloads, not BTreeMap metadata,
mutexes or the fixed shard-array overhead. Admission and snapshots remain
serialized cold paths; one hot identity still contends on one shard. These
limits should be visible in performance comparisons rather than hidden by
aggregate throughput claims.

## Tracked ownership and caller migration

`SegmentCache::insert` consumes a `Vec<u8>`. A successful new admission owns its
payload, discards spare capacity, and charges the retained byte extent. An
identical resident insertion reuses that entry. Every rejection returns the
original Vec, including its allocation and capacity, through
`SegmentCacheAdmissionError::into_parts`; `error()` borrows the existing
integrity/collision/capacity classification. Capacity-limited readers can use
the returned input without keeping a second payload or rereading the file.

`SegmentCacheLease::into_bytes` replaces `into_arc`. It returns `SegmentBytes`,
an immutable cloneable handle with `Deref<Target = [u8]>`, `AsRef<[u8]>`, Debug
and content equality. All independent gets and clones share the payload and
one pin charge. There is no mutable access or raw Arc conversion/export.
Uncached handles can be constructed from an owned Vec or boxed slice; `to_vec`
explicitly creates a detached copy when independent ownership is required.

The range-reader trait retains its `Sync` bound and returns `SegmentBytes`.
`SegmentRangeRead.payload`, `SegmentReadPayload.bytes`, and private verified
row pages retain that same tracked handle. Host implementations can return
`Ok(owned_vec.into())`; the `hawdb` facade exports `SegmentBytes` alongside the
range-reader contract. Checkpoint overflow publication retains its existing
owned Arc output: file-backed envelopes are detached only after the existing
materialization budget admits them. Ordinary hydration borrows inline bytes
or retains a tracked file-read handle.

The payload tracks external handles separately from its private resident Arc.
Gets establish a charge under the existing shard lock, and clones do not lock
or allocate. The final external drop locks only its owning shard to release
the charge. If another get acquires the shard first, it inherits that charge;
the waiting drop rechecks the handle count before releasing it. CLOCK cannot
evict an entry while this charge remains held. A weak shard reference lets a
handle outlive the cache without retaining unrelated entries. No new lock is
added to the global hit path.

This is the coordinated ownership API change approved for #196. It does not
change query behavior or persisted formats. Fixed-cost pin accounting alone
does not establish the parent's multi-threaded scaling acceptance criterion.
The [qualification report](CACHE_OWNERSHIP_VALIDATION.md) records the complete
baseline/candidate comparison, padding-only control, allocation checks and
concurrency evidence.

## Integrity and verification

The [verified-page admission contract](VERIFIED_PAGE_CACHE.md) is unchanged:
public raw admission does not establish codec proof, promotion requires exact
resident-byte equality, compact source tags must match, and deep scrub bypasses
the cache. The ownership migration adds no dependency or persisted format.

Ordinary storage tests include independent-hit lock checks, skewed capacity,
cross-shard eviction, tracked pins, concurrent identity collisions and a bounded
serial model/concurrent invariant campaign. The serial oracle allows any
unpinned eviction victim under pressure and checks the entire modeled resident
set through public cache lookups; it does not duplicate the implementation's
CLOCK order.

Ownership tests force final-drop/new-get and overlapping-final-drop races
against the actual shard and payload code. They also check all five rejection
classes preserve the input allocation, cache destruction releases unrelated
entries, exported scan payload clones retain pins, and consumer failure releases
the complete read wave.

The extended campaign runs 32,768 serial operations and 65,536 concurrent
operations. It is manual-only and registered in the existing local fuzz suite:

```sh
bazel test //crates/storage:hawdb_storage_tests //crates/storage:hawdb_storage_verified_page_cache_fuzz_tests //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests //:hawdb_linux_ci_fuzz_smoke_test --nocache_test_results
```

For an identical baseline/candidate release comparison, run the ignored
`sharded_cache_reader_benchmark` test. It warms 64 separate stable-identity
mapping readers sharing one cache, then uses the real public `lookup` API for
50,000 reads per thread at 1/2/4/8 threads. It compares distributed identities
with a deliberately hot single identity, requires zero storage reads and zero
repeated integrity hashes, and excludes fixture publication/warmup from timing.
It is not a mixed write/read, snapshot-heavy or cold-I/O benchmark, and is not
a CI test or a substitute for the full parent acceptance criteria.
