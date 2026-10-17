# Projection freshness with native owner facts

The shared source log retains native owner facts independently of built-in
Search document effects. A relationship of type `RELATES_TO`, or a relationship
property update, can affect native mapping input without requiring the built-in
Search mapper to rewrite documents. Raw record retention is not projection lag.

## Runtime mapping and invariant

`GraphStore::search_projection_changefeed_status` reports the latest retained
record in `newest_retained_mutation_id`, including owner-only records, and the
latest built-in effect in `newest_projection_mutation_id`. A built-in effect has
at least one graph/relational operation or explicitly requires rebuilding.
Raw counts, retained bytes, retention limits and native cursor records remain
shared and unchanged. Required built-in freshness is the maximum of the latest
built-in effect and the conservative resume floor. Owner-only records can still
consume retention and cause a real rebuild boundary; ignoring that boundary
would be unsafe.

The typed status and readiness JSON expose both mutation IDs. Existing hosts
constructing a status literal must provide the new field. No persistent format,
commit durability default or schema capture qualification changes here.

Explicit DDL already publishes a conservative capture gap. Tests for executable
point deltas use complete implicit point creation. Separate schema tests assert
that planning cannot cross the gap, the introspection checkpoint retains the
rebuild floor, and inventory output exposes lag without a partial executable
delta. This does not turn a schema gap into a certified mapping result.

## Finite model

[HawDBProjectionFreshness](HawDBProjectionFreshness.tla) explores up to three
source epochs, relevant and owner-only commits, schema/admission gaps, prefix
retention, checkpoint and restore. `NoFalseStaleness` and `NoMissedProjection`
compare the published required epoch against relevant effects and the floor.
The complete positive graph has **637 generated / 180 distinct states**, depth
9, with no states left on the queue. These are logical state transitions;
checkpoint/restore assumes an intact selected image and does not model physical
power loss, torn writes or the WAL synchronization protocol.

Two independent negative controls have the expected TLC exit 12:

- `CountOwnerOnly` violates `NoFalseStaleness` (2 generated / 2 distinct).
- `IgnoreFloor` violates `NoMissedProjection` (118 generated / 69 distinct;
  exploration stops at the counterexample, with 14 states left on the queue).

The positive and both negative checks are registered in Bazel and the mutant
inventory. Materialized TLC module/configuration bytes and named outcomes were
verified against the source; local evidence is archived separately. These finite
results do not certify arbitrary host mapping, C..E activation, GC, memory/RSS,
physical recovery or actual Mem integration.

## Native evidence and remaining qualification

The old watermark-only scheduling case fails on published `30a3249` (one actual
test, zero passed). With the changed status calculation, six focused facade
cases pass, including native facts retained without built-in lag, meaningful
updates advancing lag again, both WAL and checkpoint reopen, point scheduling,
schema refusal, introspection and inventory fallback. Broader root/CLI, storage,
readiness, system SQL, mandatory local fuzz and strict lint qualification must
finish on the final source before publication.


## Exhaustive publication cancellation campaign dispatch

The unrelated full Storage qualification tail timed out at the unchanged 900 s
limit on both the standalone freshness and integrated root/freshness snapshots.
The preserved logs and native sample identify the row-page publisher's exhaustive
CPU/I/O cancellation/retry campaign as a remaining active test; the sample reaches
real publish and file/directory synchronization, without proving a whole-run cause.

Its three existing publication modes now have independent libtest entrypoints
calling the same helper with 0, 1 and 2. The helper body is preserved modulo
indentation after extracting the outer mode loop. Every `1..=count` CPU and I/O
boundary, retry, authority/physical validation assertion and real synchronization
barrier remains. Each mode still shares its initialized source and baseline
between CPU and I/O campaigns. The full target keeps its 900 s deadline and all
other tests. Improved completion is a measurement to obtain, not an assumption;
the original timeout remains adverse evidence.
