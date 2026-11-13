# Generation cleanup and final handoff ownership

This private #392 stage follows PR513. The writer now owns cleanup capacity
through its final handoff, including failure and unwind. Query APIs, logical
limits, persisted bytes and the current/previous-generation retention rules
remain unchanged. The approved pair now exposes the integrated
[context contract](SEARCH_GENERATION_CONTEXT.md).

## Resident generation snapshots

An unregistered resident checkpoint writes its snapshot as
`search_projection.<g>.hawdb` before its selector (#1711). The cleanup grammar
classifies that name as a content-generation artifact. The current and previous
generation snapshots are therefore retained, together with any generation the
active closure still references, under the same predicate as the descriptor
and payload files. A generation newer than the selector (an interrupted
checkpoint) is never obsolete. The next checkpoint reuses or supersedes it.
`search_projection.hawdb` is a hard link to the selected generation, so
reclaiming an old generation name never removes the current snapshot.

## One-shot old-generation cleanup

A writer immediately discarded the full cleanup state's pending-name queue and
cloned report after returning three counts. Its new private pass borrows candidate
names and shares the original candidate grammar and retention predicate with the
long-lived reader cleanup state. The public build report still contains deleted
files, bounded pending files and retry-required. Reader retry queues and their
full reports retain their existing lifecycle.

Before publication, the operation reserves directory enumeration storage, owned
entry names, one joined path and native path-conversion scratch. The synchronous
pass retains that complete reservation until every temporary allocation has been
freed. It needs no new root admission when another owner consumes the remaining
capacity. Native filename bounds and startup/enumeration accounting are shared
with generation discovery.

Old-generation cleanup is optional. A denied reservation yields zero attempted
cleanup and retry-required, analogous to an uncompleted directory scan. It does
not fail a successful commit. Cancellation before cleanup or between entries
also requests a retry while preserving completed deletion counts. The existing
pending-file and delete-attempt ceilings remain authoritative. No candidate list
is retained by a completed writer.

## Incomplete closure discovery

A current generation number does not enumerate an incremental manifest's live
closure. If discovery fails, both the one-shot writer pass and the long-lived
retry state must preserve every ordinary lexical, content, mutation and RaBitQ
artifact, even when a partial report supplies current generations or requests
removal of all RaBitQ files. The shared `CleanupCandidate::is_obsolete` predicate
now checks this failure before applying generation cutoffs. Independently
quarantined names retain their existing cleanup treatment.

Let `A` be the set of ordinary artifacts actually referenced by an unknown
closure and `D` the cleanup deletion set. During failed discovery the predicate
returns false for every ordinary candidate, so `D` contains no ordinary
artifact and `A ∩ D` is empty without assuming anything about generation ages.
This also applies to candidates queued before failure because retries re-evaluate
the same predicate. After successful discovery, the existing retained-set rule
resumes. This is a conservative safety argument; it does not establish liveness
while discovery continues to fail.

`failed_closure_discovery_preserves_artifacts_in_both_cleanup_paths` admits a
real directory scan, seeds retry candidates, supplies deliberately incomplete
generation information and checks both paths. It also checks that a later valid
discovery permits reclamation again. Removing the failure gate makes the test
fail; a scan that was never admitted cannot satisfy the fixture.

## Stage and spool lifetime

Stage cleanup is mandatory owned work. Its workspace is admitted before the
stage directory is created and retained until native deletion returns. A denied
admission creates no stage. The generated stage is flat: spool, lexical spills,
segment/layout files and vector artifacts are direct children. Its removal bound
covers one native directory traversal, an entry and path conversion, independent
of the number of generated files. This is a generated-stage bound, not a contract
for arbitrary externally supplied recursive trees.

The flat-stage walker closes its counted directory iterator before unlinking
each bounded batch through project-accounted file I/O. It unlinks symlinks without
following their targets and refuses unexpected child directories rather than
recursively deleting unknown evidence. Filesystem failures retain a cleanup
ticket; Drop does not observe task cancellation. The spool descriptor closes before stage cleanup;
startup, spool-open and final metadata path conversions use the existing admitted
I/O boundary. Payload owners drop before their leases.

The new mandatory reservation establishes an explicit minimum stage working unit.
Pressure tests retain their original input/vector working capacity in addition to
this cleanup owner, then fill the remaining root or exercise the same native
admission failure. A separate test rejects insufficient cleanup capacity before
creation; the original failure assertions remain.

## Retained private stages

Each canonical search root admits at most 256 active or pending cleanup owners.
The process registry uses individually charged map entries, so permanent debt in
one root cannot consume another root's owner capacity. Ticket fields, canonical
root paths, conservative map-node capacity and shared ledger metadata are admitted
before directory creation. The attached host governor admits the same retained
allowance separately from active work. Cleanup releases ticket allocations before
their owner reservation; retained debt holds neither active task slots nor a
project FD domain.

Automatic retry remains root-scoped: at most four stages and four flat batches
per stage. Each owner retains its checked retry sequence. A fixed, admitted 4-KiB
snapshot sorts at most 256 identities by their last selection, so retries in another
root cannot reset progress. Concurrent callers skip entries selected since their
snapshot. There is no separately retained root cursor. Explicit retry preserves
the caller-error disposition if it cannot start cleanup.
The process registry still scans admitted owner metadata for lookup and reporting;
this change does not establish a constant lookup cost as total debt grows.

The public writer regression retains 256 injected, unexpected directories in one
root, verifies its own capacity refusal (including a Unix symlink alias), then
publishes and independently opens a generation in another root. It checks exact
host memory accounting, the single background slot, changed-FD-limit reopen and
preservation of every injected evidence file. Only fixture-owned injections are
removed before a successful retry releases all debt. Existing regressions retain
the sub-8-KiB allowance for their small-path fixtures, caller-failure attribution,
bounded retries, cancellation and unwind behavior.

A separate public-API subprocess regression keeps eight stages in one root and
one permanently blocked stage in another. After a four-stage bounded attempt and
three foreign-root retries, the next four-stage attempt removes the later,
fixture-unblocked stages while preserving the first four and foreign evidence.
Fixture-owned injections are then removed and both roots release all debt.

This is a partial repair for [#867](https://github.com/nowledge-co/hawdb/issues/867).
Persistent failures can still fill their own root's capacity. Operator remediation,
durable crash-orphan ownership, typed maintenance/reopen coordination and native
Windows alias/handle qualification remain separate requirements. No unknown-stage
recursive deletion or unaccounted memory growth is authorized.

## Validation and remaining work

Initial cleanup and retries retain a `CleanupAttempt` guard through native
removal. Error, cancellation, a partial pass, or unwind returns the unfinished
ticket to its pre-admitted registry entry. Confirmed removal frees the ticket
before releasing its registration. See the
[inductive ownership argument and executable negative controls](tla/SEARCH_STAGE_CLEANUP_PROOF.md)
for the proof assumptions and finite model-checking boundary.

Permanent regressions compare the one-shot counts with the original retry state
across retention identities, quarantine, success/not-found/errors and hard limits.
They cover exact/one-short admission, a full shared root, actual requested live
Rust allocations, cancellation, unwind and symlink targets. End-to-end writer
fixtures inject real competing admission after artifact construction and cancel
immediately after publication. They require successful committed generation
reports, complete reopen/hydration, no remaining stage and later cleanup progress.
The phase hooks exist only in test builds.

The public writer regression
`persistent_unlink_failure_preserves_generation_and_releases_work_resources`
injects `PermissionDenied` for one regular private-stage file immediately before
counted unlink. The test-only guard keeps the same error active through Drop,
three explicit retries and the next writer's automatic retry. It verifies intact
evidence and the previously published generation, exact retained host charging,
release of actually occupied CPU/background/blocking/I/O slots, reuse of the
sole background task slot, and close/reopen from four to eight FDs.
A later successful publication and retry release all stage debt without manual
stage deletion. Negative controls dropping retained charging or keeping the old
FD domain fail the same test.

This deterministic error injection qualifies cleanup ownership and error paths;
it does not measure native permission failures, OS handle counts or physical
power-loss behavior. Operator remediation, maintenance/reopen coordination,
process registry pressure and Windows alias isolation remain in
[#867](https://github.com/nowledge-co/hawdb/issues/867) and
[#819](https://github.com/nowledge-co/hawdb/issues/819).

Allocation probes initialize fixed ledger metadata before measuring payloads.
They measure requested Rust live capacity, not allocator overhead, native libc
allocations or process RSS. Native scratch follows the audited pinned platform
bounds; native calls remain synchronous.

Frozen Cargo/Bazel receipts, independent PR513 artifact and cleanup-report oracles,
and separate source/target mutation controls live in
`target/qualification-cache/392-cleanup-audit` on the validation host. Local
verification uses the unchanged default Bazel search and mandatory fuzz targets.

Delta input conversion and ordered hydration now retain operation admission;
see the [delta contract](SEARCH_DELTA_OWNERSHIP.md) and
[context facade](SEARCH_GENERATION_CONTEXT.md). The 4 MiB source guard remains; shared host admission/adaptive
profiles and the original #206 corpus acceptance remain distinct requirements.

## Registered Consumer generation retirement

`Database::cleanup_search_projection_consumer` uses the live Consumer's captured
parent. `Database::retry_search_projection_consumer_cleanup` admits a fresh
maintenance operation after close or unregister. Both use the same native
reclamation protocol; neither runs deletion in Drop or retains a background
permit or FD domain after returning.

Both entry points use non-strict internal descriptor admission, even when the
project configures strict foreground capacity. Occupied foreground capacity must
not block reclamation or its retry; every native handle remains counted. Memory,
I/O, scan, delete-attempt, ownership and durability limits remain in force.

The complete allocation ledger is checked under its catalog exclusion for each
candidate and synchronized with its parent before deletion. A previous lost
publication barrier must not leave an older durable retention image behind an
observed compacted ledger. Every selected branch, initial logical-target birth, unresolved
reservation and unresolved selector's previous root remains protected. Expiry,
a missing runtime handle, or a missing registry-cache row never removes a root.
The search publication/process exclusion additionally rejects pending captures
and exact reader pins, including pins held after their Consumer closes. A
completed predecessor can be discovered through its canonical, checksummed birth
marker even after its completed ledger row is compacted. Its operation must
belong to this allocation domain and fall within the durable issued boundary.
Unknown or inconsistent evidence fails closed.

Before any payload unlink, maintenance durably installs a parent-side
`.consumer-reclaim-<project>-<operation>.hawdb` intent containing the original
birth bytes. This is deletion evidence, not a new selection or serving receipt.
The flat child inventory must contain only recognized regular generated files
and lock/birth controls; unknown entries and nested directories stop that
candidate without recursive deletion. Payload removal is synchronized before
removing the birth marker. Empty-directory removal and the parent barrier
precede intent deletion and a final parent barrier. A missing birth marker is
accepted only with that durable intent and an otherwise empty/control-only
directory. A missing directory completes its parent barrier before intent
retirement. Native identity comparisons stay scoped to live handles.

The default pass examines at most 32 candidate entries and attempts at most 256
deletions. Enumeration refuses beyond 65,536 parent entries or 4,096 generation
children. At most 256 candidates can be admitted per call. The five-step
finalization unit requires at least five delete attempts. A bounded lexical
cursor permits progress past retained or blocked entries; after reaching the
end, start a fresh pass without a cursor to revisit earlier failures/pins.
Reports distinguish durable retention, runtime exclusion, failures and bounded
progress. Partial removal keeps its intent; the next attempt reacquires fresh
working, I/O and descriptor admission. No allocator identity is reused.

Maintenance prepays a 16 MiB control/inventory envelope inside the facade's
existing 64 MiB background admission. This is engine accounting, not an RSS or
allocator-overhead measurement. Windows native execution and modeled power-loss
qualification of this complete Consumer retirement protocol remain separate
from ordinary close/reopen and deterministic failure tests. Private writer-stage
remediation and crash-orphan adoption remain the separate #867 contracts above.
