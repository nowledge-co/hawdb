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
This keeps residency, entry counts and hit/miss counters coherent with cache
operations. Exported/shared raw Arcs still pin resident bytes, so exact pin
accounting retains the existing `Arc::strong_count` scan. Raw Arc owners may
drop independently during that scan, as before. This change does **not** make
snapshot cost O(1), replace raw Arc ownership, or complete issue #196.

The byte capacity still accounts for resident payloads, not BTreeMap metadata,
mutexes or the fixed shard-array overhead. Admission and snapshots remain
serialized cold paths; one hot identity still contends on one shard. These
limits should be visible in performance comparisons rather than hidden by
aggregate throughput claims.

## Integrity and verification

The [verified-page admission contract](VERIFIED_PAGE_CACHE.md) is unchanged:
public raw admission does not establish codec proof, promotion requires exact
resident-byte equality, compact source tags must match, and deep scrub bypasses
the cache. No public API, persisted format or dependency changes are required.

Ordinary storage tests include independent-hit lock checks, skewed capacity,
cross-shard eviction, raw Arc pins, concurrent identity collisions and a bounded
serial model/concurrent invariant campaign. The serial oracle allows any
unpinned eviction victim under pressure and checks the entire modeled resident
set through public cache lookups; it does not duplicate the implementation's
CLOCK order.

The extended campaign runs 32,768 serial operations and 65,536 concurrent
operations. It is manual-only and registered in the existing local fuzz suite:

```sh
bazel test //crates/storage:skein_storage_tests //crates/storage:skein_storage_verified_page_cache_fuzz_tests //crates/fuzz:skein_fuzz_tests //crates/fuzz:skein_fuzz_cli_tests //:skein_linux_ci_fuzz_smoke_test --nocache_test_results
```

For an identical baseline/candidate release comparison, run the ignored
`sharded_cache_reader_benchmark` test. It warms 64 separate stable-identity
mapping readers sharing one cache, then uses the real public `lookup` API for
50,000 reads per thread at 1/2/4/8 threads. It compares distributed identities
with a deliberately hot single identity, requires zero storage reads and zero
repeated integrity hashes, and excludes fixture publication/warmup from timing.
It is not a mixed write/read, snapshot-heavy or cold-I/O benchmark, and is not
a CI test or a substitute for the full parent acceptance criteria.
