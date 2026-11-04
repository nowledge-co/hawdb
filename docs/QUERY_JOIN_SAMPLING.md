# Bounded index join sampling

Issue: [#1560](https://github.com/nowledge-co/hawdb/issues/1560).

`DatabaseConfig::join_sampling: Option<JoinSamplingLimits>` enables query-local
statistics for a small directed `MATCH` path. The default is `None`: hosts opt
in after qualifying their workload. This is an embedded Rust configuration,
not a new process, environment variable, query protocol or persisted format.

Hosts configure this through the `hawdb` facade without depending on internal
storage crates:

```rust
use hawdb::{Database, DatabaseConfig, JoinSamplingLimits};

let db = Database::new_with_config(DatabaseConfig {
    join_sampling: Some(JoinSamplingLimits::default()),
    ..Default::default()
});
```

## Eligibility and plan choice

The initial capability accepts one closed inner path of one or two fixed,
directed hops. Every node must have exactly one mandatory equality on a declared
equality index, bound to an integer, boolean or string of at most 256 bytes.
Names are at most 128 bytes. Two-hop relationship types must differ. This
matches the existing normalizer's relationship-isomorphism boundary.

OPTIONAL, undirected or variable-length paths, repeated bindings/types, LIMIT,
additional predicates, access-controlled/scored populations and larger paths
retain ordinary planning. Pure projection, aggregate, DISTINCT and ordering
wrappers preserve their existing output semantics.

Both endpoint orientations are measured on the same immutable read view.
Seed node positions are drawn uniformly without replacement. For each hop,
the sampler counts adjacency fanout, builds cumulative sums, samples output
positions without replacement and selects the corresponding relationship by
ordinal. Target equalities filter the selected neighbors. Repeated relationship
results and intermediate endpoints retain their multiplicity.

Estimated visited rows include rejected adjacency candidates, not only final
cardinality. Reverse orientation activates only when its estimated work is less
than half the forward estimate and final estimates are positive and within a
factor of four. These gates are heuristics, not confidence intervals or a proof
of unbiased multi-hop estimates. An empty sample, arithmetic refusal or incomplete
attempt discards both estimates. Sampling never proves exact emptiness,
uniqueness or another semantic rewrite condition.

The optimizer records `transformation:sampled_match_orientation` and a
`join sampling:` decision containing epoch, direction, refusal, predicted work
and actual sampling counters. Statistics attach to a cloned query catalog,
never the shared statistics catalog.

## Storage cost and bounds

`GraphReadEngine::sample_join_path` is the typed capability; engines without
support return an explicit refusal. The concrete graph store uses these paths:

| Backend | Preparation | Selection |
| --- | --- | --- |
| Resident | Copy an admitted equality posting and each distinct requested adjacency posting once. The existing BTreeSet has no rank index. | Vector indexing and binary membership lookup. |
| Canonical checkpoint | Read matching property projection blocks without node hydration; prepare adjacency descriptor prefixes and cumulative block counts. | Binary-search the selected block, decode its keys once and reuse them within the attempt. Other adjacency blocks are not decoded. |

Preparation is charged. This does not claim constant-time source preparation
or an on-disk global rank index. Canonical sampling currently refuses a view
containing any node/relationship overlay or tombstone: checkpoint counts must
not stand in for a merged population. Ordinary execution still supports these
views.

One attempt, including both orientations, shares these default ceilings:

| Limit | Default |
| --- | --- |
| Sample positions per stage | 32; supported range 1–256 |
| Probes | 1,024 |
| Prepared entries | 16,384 |
| Physical read bytes | 8 MiB |
| Physical range/page reads | 256 |
| Cumulative admitted scratch capacity | 4 MiB |
| Cooperative duration | 20 ms |

Descriptor pages and posting blocks are admitted before physical reads.
Verified cache hits consume preparation/probes but no physical-read credits;
a cache insertion racing admission can conservatively consume a credit.
Scratch accounting includes requested vector capacities, decoded-block scratch
and conservative posting-map charges. It is not process RSS or allocator
rounding; immutable storage caches retain their separate limits.
Duration checks are cooperative and cannot interrupt one blocking filesystem
read. Query task cancellation takes precedence over the sampling deadline.
The configured query memory ceiling and runtime memory target can tighten the
scratch ceiling. Governed calls reserve that whole ceiling conservatively for
the attempt and release it before execution. Probes/preparation charge the
parent task's work budget. Physical misses acquire one nonblocking runtime I/O
permit held through the attempt; saturated admission falls back without spinning.

Budget/unsupported refusals fall back. Corruption and query cancellation remain
errors, not apparently successful fallbacks. Refusal must not poison a healthy
descriptor or posting reader.

## Cache and snapshot identity

A sampled physical-cache key adds exact bound values and their opaque templates,
limits/seed, visible commit epoch, checkpoint epoch and physical generation.
Prepared-query reuse checks parameters and read identity. Retained readers keep
their original population after writes/checkpoints; new reads cannot inherit
their sampled costs. Retained statistics/key allocations participate in existing
physical-cache heap accounting.

## Qualification

See [the test index](TEST_INDEX.md) for finite regression owners and replay.
Build the manual native benchmark with the repository release profile:

```console
cargo build --locked --release --features qualification --bench index_join_sampling
target/release/deps/index_join_sampling-<hash> two materialized 256 7 skewed
target/release/deps/index_join_sampling-<hash> two out-of-core 256 7 skewed candidate-first
```

Arguments: `one|two|point materialized|out-of-core hot_rows repeats
skewed|uniform|no-index|refuse [candidate-first]`. Each process creates one
checkpoint, opens it separately with sampling disabled/enabled and checks full
query counts. Alternate baseline/candidate order across independent processes.
JSON lines separate setup/open, first-call and warm wall time, parsing,
planning including sampling, sampling time, cache hits/misses and selected plans.
Do not add sampling time to planning a second time.
The uniform control has degree four everywhere and distributes each selected
source population across the complete target population. Its exact one/two-hop
count is `hot_rows`; the skewed workload's exact count is `4 * hot_rows`.

Report wins and regressions, cold storage-cache first calls, warm calls,
point/no-index/budget-refusal controls and first-build cost. `OutOfCore` denotes
residency: an in-RAM fixture does not establish larger-than-RAM performance, and
cold storage caches do not establish a cold OS page cache. Larger-than-RAM
qualification and default activation remain separate work.
