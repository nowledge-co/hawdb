# Search cleanup and publication exclusion

This component is a prerequisite for safely installing private MVCC Search
artifacts before activation. It does not implement the native C-ready/E session
or establish constant-size activation, actual Mem adoption, or power-loss safety.

## Observed failure

A materialized SearchIndex can outlive a peer publication. In the native
`stale_cleanup_preserves_the_current_vector_generation` reproduction, an empty
resident handle checkpoints generation 1, then an independent generation writer
publishes a vector-bearing generation 2. Retrying cleanup through the old handle
previously removed the current `search_rabitq.2.hawdb` file. The reproduction
failed before the fix: the cleanup report recorded one deletion even though disk
discovery retained generation 2. The stale handle's empty document collection
supplied a remove-all hint, which bypassed the retained-reference check.

Cleanup also discovered and removed files without publication exclusion. Thus
an invisible installed candidate could be mistaken for an obsolete artifact
between installation and selector publication. The regression holds the actual
root lease while invoking the public cleanup API and verifies zero scans and
zero delete attempts, followed by successful cleanup after release.

## Native correspondence

[The public entry point](../../crates/search/src/lib.rs) now acquires the same
[root publication lease](../../crates/search/src/out_of_core/publish_lease.rs)
through discovery and sweep. An unavailable lease returns a bounded deferred
report. Retry names beyond the caller's queue cap can be dropped because the
next full sweep rediscovers them; deferral performs no filesystem scan or delete.
The ordinary open and checkpoint paths already hold the lease and call a private
helper without reacquiring it. ConsumerProjection retains the root lease for its
lifetime. Only its valid owned-mutation scope may reuse that authority through
the public API; its shared read view defers. Initialization and reopening use the
existing private leased open path. No public lock-bypass parameter is added.

[The sweep](../../crates/search/src/generation_cleanup.rs) protects explicit
retained RaBitQ references before considering a remove-all hint. Failed active
closure discovery protects ordinary and quarantined files, retaining evidence.
Disk manifest references determine the current immutable generation and its
retained files. A stale in-memory empty document map cannot override those
references.

Resident checkpoints additionally write standalone RaBitQ artifacts that are
not listed in the immutable manifest. An empty reference set therefore does not
prove absence of vectors. Resident vector generation/remove-all hints are used
only when the existing resident-snapshot validation matches the published
snapshot. If it does not match, and no manifest vector generation is available,
cleanup conservatively retains vector files. It can still clean independently
qualified immutable artifacts. Snapshot identity validation is the existing
count/digest/source-epoch/model contract, not a new collision-free or ABA-proof
authority. This component does not strengthen that contract. Discovery and
identity validation still perform manifest and artifact I/O; no short activation
latency claim follows.

## Conditional safety argument

While publication exclusion is held, a conforming peer cannot install or publish
new files. Under a validated active closure, each explicitly referenced vector
file is retained regardless of a contradictory hint. Standalone resident hints
are accepted only for the matching published snapshot; an unqualified stale
handle has neither a vector current-generation threshold nor remove-all
authority. With no qualified threshold, the sweep retains vector candidates.
If the closure is uncertain, no candidate, including diagnostic quarantine,
is deleted. Unavailable exclusion produces deferral before discovery.

This assumes all writers use the same native root lease and the existing
snapshot identity checks are sound for the workload. Filesystem mutation outside
that protocol is outside the argument. Future private installation must retain
an operation slot until publication or cancellation. The consumer's lifetime
lease alone cannot serialize internal operations once work escapes its current
exclusive host owner; native session design must add that lifecycle explicitly.

## Finite model

[HawDBSearchCleanupPublication](HawDBSearchCleanupPublication.tla) covers three
heads, stale and refreshed resident views, manifest versus standalone vector
publication, installed but unpublished candidates, synchronization, cancellation,
lease release, cleanup deferral, contradictory hints and unknown closure state.
The positive direct pinned-TLC run checked 31,679 generated / 6,890 distinct
states to complete depth 21. Ten additional checks include fault counterexamples
and three reachability witnesses. The stale-resident fault checks actual current
file survival separately from the remove-all authority invariant. The raw sweep
abstraction assumes its caller supplies complete retained references; resident
current-generation thresholds and independently numbered native vector files
are abstracted by that protected set.

The tool jar SHA-256 is
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`.
The model is a bounded protocol abstraction, not mechanized Rust refinement or
a physical torn-write/reordering model. Synchronization is represented by an
abstract action; storage platform behavior remains a separate gate.

## Validation status

The initial focused native selection passed 47 tests. The first full vector
Search library run then found seven failures: treating an empty manifest vector
reference set as remove-all authority deleted valid standalone resident files.
Those adverse results are preserved and are not passing qualification. The
revised native implementation includes stale standalone-peer, consumer authority
reuse, open/checkpoint non-reentrant cleanup, explicit-reference precedence,
unknown-closure quarantine retention and bounded retry-queue regressions.

The expanded direct model passed all eleven checks. The corresponding eleven
Bazel targets passed (two test wrappers executed, nine cached); archived model
action evidence was checked byte-for-byte against the current TLA module. The
revised full vector Search library run passed 877 tests, zero failures and 20
existing ignored tests (214.86 seconds). Both stale-peer regressions and the
consumer-authority test were selected. The minimal cleanup selection passed
47 tests with 672 filtered (12.14 seconds). Strict locked workspace/all-target/
all-feature Clippy passed on the revised source. The final native Bazel cleanup selection passed 49 tests with 848 filtered
(14.37 seconds). A primary-worktree facade selection passed all 23 registered
consumer tests with 1,981 filtered (83.71 seconds), including checkpoint/registry
recovery boundaries. Runtime/test/model bytes match the isolated qualified tree;
only existing result/design prose differs. Primary formatting, strict Clippy and
all-file prek also passed. The unfiltered required local fuzz suites passed all 98 targets (96 executed,
two cached; 1,549.694 seconds wall / 203.33 seconds critical path). The archive
retains 294 fuzz log/XML/cache-status files in addition to native and eleven
exact-source model receipts. Source tree `f5b73e69b4a7c1ad65504aab366a5a86cdbf9499`
is the isolated qualified runtime/test/model snapshot. The primary tree differs
only in existing result/design prose, verified by a complete tree comparison.
An initial mistaken invocation applied a name filter to the fuzz suites; it was
interrupted and is excluded. Only the subsequent unfiltered run is qualification. No assertions, checks,
limits, durability settings or timeouts were weakened. Native C-ready/E custody,
closed mapping/fanout, incremental preparation, vectors, ABA, durable receipts,
physical power-loss/resource and writer fairness/latency gates remain open.
