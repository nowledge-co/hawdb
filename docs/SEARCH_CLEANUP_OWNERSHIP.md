# Generation cleanup and final handoff ownership

This private #392 stage follows PR513. The writer now owns cleanup capacity
through its final handoff, including failure and unwind. Query APIs, logical
limits, persisted bytes and the current/previous-generation retention rules
remain unchanged. The approved pair now exposes the integrated
[context contract](SEARCH_GENERATION_CONTEXT.md).

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

The implementation keeps the standard library's handle-relative deletion and
symlink handling. On pinned Rust 1.97.1, Unix uses fdopendir/openat/unlinkat;
Windows uses a fixed 1-KiB directory buffer and a handle stack. The generated
flat layout needs one stack entry. See the pinned
[Windows implementation](https://github.com/rust-lang/rust/blob/8bab26f4f68e0e26f0bb7960be334d5b520ea452/library/std/src/sys/fs/windows/remove_dir_all.rs)
and [Unix implementation](https://github.com/rust-lang/rust/blob/8bab26f4f68e0e26f0bb7960be334d5b520ea452/library/std/src/sys/fs/unix.rs).
Deletion remains best effort on filesystem errors and does not observe task
cancellation during Drop. The spool descriptor closes before stage cleanup;
startup, spool-open and final metadata path conversions use the existing admitted
I/O boundary. Payload owners drop before their leases.

The new mandatory reservation establishes an explicit minimum stage working unit.
Pressure tests retain their original input/vector working capacity in addition to
this cleanup owner, then fill the remaining root or exercise the same native
admission failure. A separate test rejects insufficient cleanup capacity before
creation; the original failure assertions remain.

## Validation and remaining work

Permanent regressions compare the one-shot counts with the original retry state
across retention identities, quarantine, success/not-found/errors and hard limits.
They cover exact/one-short admission, a full shared root, actual requested live
Rust allocations, cancellation, unwind and symlink targets. End-to-end writer
fixtures inject real competing admission after artifact construction and cancel
immediately after publication. They require successful committed generation
reports, complete reopen/hydration, no remaining stage and later cleanup progress.
The phase hooks exist only in test builds.

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
