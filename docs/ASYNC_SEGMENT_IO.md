# Async Segment I/O

Issue: [#302](https://github.com/nowledge-co/hawdb/issues/302)

Status: experimental adapter delivered in [#304](https://github.com/nowledge-co/hawdb/pull/304);
not active in the query data path.

## Decision

HawDB's portable async segment-read API uses Tokio's bounded blocking lane over
the existing positioned file reader. It does not use `io_uring`, a dedicated
platform runtime, or a second storage implementation.

This is the same portability model documented by Tokio for ordinary files:
filesystem syscalls remain blocking and are executed by `spawn_blocking`. HawDB
uses `spawn_blocking` directly because `FileSegmentRangeReader` already owns the
cross-platform positioned-read, cache, digest, and error semantics that
`tokio::fs::File` would otherwise duplicate.

The async API is valuable even though the underlying syscall is blocking:

- the host async worker is yielded while the physical read is running;
- owned and borrowed Tokio runtimes use one integration path;
- the governor, rather than an additional Rayon pool, bounds HawDB's submitted
  blocking reads;
- result ordering, wave byte budgets, cancellation checkpoints, cache behavior,
  and typed read errors stay aligned with the synchronous executor.

It is not completion-based kernel I/O, and the benchmark must not describe it
as such.

## Ownership Boundary

`hawdb-storage` remains runtime-independent. `SegmentRangeReader` and
`FileSegmentRangeReader` continue to own synchronous positioned reads:

- Unix uses `FileExt::read_at`;
- Windows uses `FileExt::seek_read`;
- other supported Rust targets clone, seek, and read the file handle.

`hawdb-runtime-tokio::TokioSegmentReadExecutor` owns only async scheduling. It
accepts an `Arc<R>` where `R: SegmentRangeReader + Send + Sync`, submits bounded
range reads to the selected Tokio runtime, awaits them, and returns payloads in
schedule order. The root `hawdb` facade re-exports this API only with the
`tokio-runtime` feature.

The existing synchronous `SegmentRangeReader` is not by itself an async seam.
The async boundary is the wave executor: callers must be able to await a wave.
Wrapping this executor in `block_on`, or calling it from the existing
`execute_blocking` query closure, is forbidden because a saturated blocking
lane could deadlock on its own nested work.

### Storage Injection Boundary

An injected `Arc<R>` is one reader implementation, not a database-level registry
of storage systems. `FileSegmentRangeReader::register` registers local file
artifacts; its `StoreId` separates cache identity. Neither establishes routing
between multiple providers or a provider lifecycle/recovery contract.

Multi-storage injection remains follow-up work under #302. It needs an explicit
library contract for store/artifact/generation identity, routing, shared resource
budgets, live-handle ownership and read-only versus durable capabilities. The
existing local-file store remains the default. An async scheduling adapter alone
does not provide this contract for reads, writes, WAL and checkpoints.

## Admission and Cancellation

The runtime task context now supports a non-blocking I/O-wave acquisition:

1. `try_acquire_io_wave` returns `Pending` when the admitted capacity is busy.
2. The Tokio executor yields for an I/O-specific retry interval of at most
   5 ms, shortened by the remaining deadline. Cancellation (including a parent
   token) wakes this wait immediately. I/O-wave controllers currently expose no
   readiness notification; this bounded retry is separate from the event-driven
   task admission queue and does not restore its removed polling configuration.
3. A physical chunk contains at most the task's admitted parallelism and global
   executor-thread ceiling.
4. The I/O-wave permit remains live until every submitted blocking read in that
   chunk has joined, including error paths. Each blocking task also shares
   ownership of that permit so dropping the awaiting future or shutting down a
   borrowed runtime cannot release capacity while reads remain in flight.

This preserves the existing distinction between task-scoped and wave-scoped
I/O reservations without blocking a Tokio worker on a condition variable.

An ordinary file read cannot be interrupted safely after the blocking syscall
starts. Cancellation is therefore cooperative: it is observed before
submission, between chunks, before payload delivery, and after the wave. While
the execution future is awaited, all already-submitted reads are joined before
their permit is released. This is consistent with Tokio's documented
`spawn_blocking` cancellation behavior.

Dropping the execution future stops further submission and payload delivery,
but does not interrupt submitted file operations. Those tasks keep the chunk's
permit until the last read finishes, even when there is no caller left to join
them. No cleanup task or extra runtime is required to release the capacity.

## Cross-Platform Contract

The semantic backend is identical on Linux, macOS, and Windows. A platform may
implement the underlying positioned syscall differently, but it must preserve:

- exact range length or a typed `SegmentReadError`;
- schedule-order delivery;
- wave byte and concurrency ceilings;
- cache and content-digest behavior;
- cooperative cancellation boundaries;
- no runtime creation inside a borrowed host runtime.

There is no Linux-only capability flag and no platform-specific fallback to
select. Platforms without Unix or Windows positioned-read extensions retain the
existing clone/seek/read implementation.

## Benchmark

The manual benchmark compares the current Tokio `execute_blocking` plus
`SegmentReadPool` path with `TokioSegmentReadExecutor` using the same runtime
limits, reader, schedule, range geometry, and payload checksum:

```bash
cargo bench --bench storage_async_segment_read --features tokio-runtime
bazel run //:hawdb_bench_storage_async_segment_read
```

The default workload creates a 640 MiB fixture, reads 8,192 non-coalescing 4 KiB
ranges per sample, tests depths 1, 4, and 16, and runs three child-isolated
samples for each backend. It reports wall time, process CPU time, steady and
peak RSS, page faults, and observed thread count. Linux additionally requests
random access and page-cache eviction through `posix_fadvise`; the output states
whether that request succeeded.

Small smoke runs can override the fixture without changing the benchmark
protocol:

```bash
HAWDB_ASYNC_IO_FIXTURE_MIB=8 \
HAWDB_ASYNC_IO_RANGE_COUNT=256 \
HAWDB_ASYNC_IO_SAMPLES=1 \
cargo bench --bench storage_async_segment_read --features tokio-runtime
```

The benchmark is manual because it creates an out-of-core fixture. Its report
sets `production_eligible` to `false`; it is decision evidence, not a release
qualification artifact.

### Local macOS evidence

The default workload was run on macOS 26.6.2, Apple M5 Max (18 cores, 36 GB),
with Rust 1.97.1. These are warm-cache scheduling results: macOS cannot provide
the Linux `posix_fadvise` cold-cache signal, and every sample therefore reported
`cold_cache_requested=false`.

| Depth | Backend | Wall p50 | CPU p50 | Peak RSS p50 | Peak threads p50 |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | current blocking pool | 50.40 ms | 59.51 ms | 10.56 MiB | 4 |
| 1 | Tokio async | 65.72 ms | 65.47 ms | 10.44 MiB | 3 |
| 4 | current blocking pool | 33.73 ms | 104.65 ms | 9.22 MiB | 8 |
| 4 | Tokio async | 45.89 ms | 121.18 ms | 9.02 MiB | 7 |
| 16 | current blocking pool | 37.93 ms | 408.54 ms | 10.02 MiB | 20 |
| 16 | Tokio async | 54.58 ms | 259.58 ms | 9.42 MiB | 19 |

Payload byte counts and checksums matched for every sample. The async path used
one fewer workload thread and slightly less peak RSS at every depth. At depth
16 it used about 36% less process CPU, but wall time was 30% to 44% slower
across the tested depths. This does not justify replacing the current query
path. These macOS results alone do not establish an out-of-core I/O benefit;
the later Linux baseline is recorded below.

### Historical Linux Evidence

The [Linux baseline workflow](https://github.com/nowledge-co/hawdb/actions/runs/33928122832)
completed for `3cce649162daa370bfe7382f2131f40794c69a10` during #304. Its
[measurement receipt](https://github.com/nowledge-co/hawdb/pull/304#issuecomment-5547435118)
records the same 640 MiB fixture, 8,192 non-coalescing 4 KiB ranges, depths 1/4/16
and three isolated samples per backend/depth. All 18 samples reported
`cold_cache_requested=true` and matching payload checksums. This reports the
eviction request, not a guarantee that every physical read missed every cache.

The async path used one fewer workload thread at each depth. Depth 4 wall time
was about 0.5% lower; depth 16 CPU was about 5.4% lower with wall time about 4.4%
higher. Depth 1 was materially slower and peak RSS was effectively unchanged.
These mixed results do not justify switching the synchronous query path.

This baseline predates the later cancellation-lifetime and main-integration
follow-ups in #304. It is historical adapter evidence, not performance
qualification for the current head or a future async query integration. The
query-integration gates below still require evidence for the implementation
being considered for activation.

## Query Integration Gate

The current `HawDBTokioEmbedded` query path still executes the synchronous
query engine through `execute_blocking`. Enabling the new executor there first
requires an awaitable boundary between physical segment scheduling and payload
consumption. Until that boundary exists, the synchronous executor remains
authoritative.

Production integration requires all of the following:

1. representative benchmark evidence on Linux and macOS, plus Windows compile
   and storage-test coverage;
2. no regression in wall time or CPU that outweighs the async scheduling value;
3. an async query-execution boundary with no nested `block_on` or nested
   blocking-pool wait;
4. cancellation, panic, read-error, ordering, and governor-saturation tests;
5. an observable execution report that identifies the synchronous or Tokio
   segment-read backend.

## References

- [Tokio filesystem documentation](https://docs.rs/tokio/latest/tokio/fs/)
- [Tokio `spawn_blocking` documentation](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html)
- [Tokio runtime blocking-thread configuration](https://docs.rs/tokio/latest/tokio/runtime/struct.Builder.html#method.max_blocking_threads)
