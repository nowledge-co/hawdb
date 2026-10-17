# Bounded pinned feed enumeration

The native primitive is implemented; stage-bound coverage and actual Mem
catch-up integration remain unimplemented. Enumeration success is **not**
authorization to publish arbitrary callback output. See the complete
[batch protocol](../specs/MVCC_SEARCH_BATCH_COVERAGE_SPEC.md).

The [external mutation sorter](SEARCH_UNORDERED_MUTATIONS_PROOF.md) now supplies
private output ordering and exact duplicate/conflict reduction. Its cumulative
and per-quantum sort admission are separate from this primitive's source budget;
artifact-bound coverage and incremental final artifact construction remain
outstanding. See the [ordering quantum proof](SEARCH_SORT_QUANTUM_PROOF.md).

## Source mapping and conditional argument

`store/search_projection_change_log/cursor.rs` owns an immutable COW log directory,
C/E, a private event/fragment ordinal and cumulative work/item/input-byte caps.
Capture rejects future cursors and C below the captured resume floor. The writer
and checkpoint loader enforce increasing epochs. Binary seek charges probes and
uses O(1) paged ordinal lookup: only first/last pages may be partial; prefix trim
preserves full interior pages. Append/trim detaches live boundaries and cannot
change captured input. The cursor shares records; it does not eagerly clone a
Vec of events or key payloads. Old records remain retained until cursor drop.

Each event/table boundary consumes work, including empty groups. Each graph ID,
old document ID and relational primary tuple is visited in recorded order;
large transactions can split inside every vector. Repeated keys count again.
Key-field count admits measurement work before inspecting variable-width byte
lengths. A key deferred by the remaining step bytes retains its work charge;
measuring it again next step consumes work again. Cumulative counters never
reset/refund on step boundaries. Per-step refusal cannot expose an oversized
input. Capture gaps, RequiresRebuild/overflow keys and cumulative exhaustion
fail rather than silently advancing. End is a separately admitted callback,
including for an empty interval, and is installed only after callback success.
Callback errors/panics leave the cursor permanently unusable.

`Database::begin_prepared_projection_feed` captures from the same PreparedRead
snapshot. Public callbacks receive an **immutable** ProjectionFeedRead context
with private interior borrowing; they cannot replace its PreparedRead or obtain
an untracked raw transaction. Cypher, SQL and graph hydration delegate to the
existing tracked APIs. Ignored query errors, callback errors/panics and shared
source poisoning make feed finish refuse. Finish requires End and returns only
read dependencies. Host mapping correctness and cumulative query/stage admission
remain separate obligations; a no-op callback does not earn coverage authority.

Given an ordered valid log, private ordinal transitions preserve exactly a
prefix of its input. The next input is charged before exposure and advances
only after successful processing. Exhaustion therefore includes every retained
input through captured E, independent of later append/trim. This is a claim
about the recorded feed, not a proof that all DDL/host projection effects were
captured or that the resulting projection is complete.

## Finite model and limits

`HawDBProjectionFeedCursor` abstracts a table/key, commit boundary, node and
explicit End into immutable frames. It varies key byte width and cumulative
work/item/byte caps, separates work measurement from byte admission/callback,
and allows later live-input replacement. An independent measurement oracle
checks no work refund/undercount; successful deliveries must equal the captured
prefix. Refusal/error and completion are separate terminal states.

TLC 2.19 / TLA+ 1.7.4, Java 21.0.9, pinned jar SHA-256
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`,
four workers: **1,348 generated / 916 distinct states**, depth 24, no invariant
failure. Eight controls reset totals, refund deferred work, omit metadata work,
skip delivery, ignore callback error, omit End, read mutable live input or admit
oversized bytes. Each returns exit 12 for its exact registered invariant.
CompleteWitness and DeferredWitness require reachable success, including success
after byte deferral; a vacuous stalled model does not pass these probes.

The model abstracts native binary seek/page layout, varying transaction shape,
exact encoded/retained sizes, overflow-key representations, physical I/O, source
health, Rust aliasing and query execution. It does not model a governed output
spool, host business semantics, durable operation receipts or power-loss barriers.
Native source-linked tests support those primitive mappings; they do not turn
this into an unbounded/mechanized Rust refinement proof.

## Native evidence and remaining gates

Five storage regressions exercise 193-record page boundaries and partial prefix
trims, pointer identity of borrowed strings/keys after live trim/append, large
adversarial repeated inputs, per-step key preflight, no-refund deferral, cumulative
item/byte/work caps, gaps/future cursors/rebuild, callback error and panic.
External default/minimal consumers cover graph and relational inputs on resident
and checkpointed out-of-core sources, later disjoint/relevant writes, live log
pruning, pinned SQL payloads, exact relational dependencies, explicit End,
incomplete finish, ignored read errors and panic. A real 64-node transaction
completes across small steps; repeated canonical commits to one node exhaust
the cumulative work budget despite a tiny unique-key set. A kernel source-poison test
requires refusal before visiting anything.

Feed counters bound metadata work and logical borrowed key bytes. They do not
bound retained COW/source memory, key cloning by a host callback, total allocator
peaks, parser/physical query work or RSS. Query row/payload caps apply per
statement; dependency caps are cumulative. Cross-statement work and output
staging still need session/governor admission. Cold ID-root access and encoded
block validation retain their earlier limitations. Native coverage receipt,
session ownership of the external reducer/spool, incremental final artifact
construction, complete relevant schema/host effects, actual Mem owner
handoff, fault composition and original performance/retention gates remain open.
