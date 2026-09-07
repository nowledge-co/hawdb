# Committed generation cleanup

The generation writer runs a one-shot cleanup after successful manifest-last
publication, while its publication lease still excludes other publishers. The
pass returns only the three scalar values used by the existing build report:
deleted files, capped pending files and whether retry is required. It no longer
allocates a throwaway retry queue, known-name set or two copies of the public
cleanup report.

## Shared discovery and retention

Stateful retries and the committed pass share directory traversal, candidate
classification and generation-retention rules. The parser can borrow a native
name or consume an owned name without cloning it. Stateful retries still retain
their queue, revalidate queued generations, preserve their full public report,
and copy a discovered name only when it is an eligible candidate. No public API
or v1 filename grammar changes, including accepted numeric forms and quarantine
suffixes, are introduced.

Discovery remains after publication. The publication lock does not exclude read
paths that quarantine corrupt artifacts, so a pre-publication directory snapshot
could miss newly quarantined files. This pass does not claim an atomic filesystem
snapshot; it retains the existing native enumeration/race semantics.

The current and previous generation remain retained separately for lexical,
out-of-core and RaBitQ artifacts. Valid quarantine names remain eligible, and
the existing `rabitq_remove_all` rule remains unchanged. Unrecognized and non-UTF8
filenames are ignored as before.

## Admission and failure semantics

Each attempted deletion constructs its temporary native path through `OwnedPath`
on the writer's existing `BuildMemory` root. The native join envelope is reserved
before allocation, actual retained capacity remains charged through removal,
and the path and lease drop before processing another entry. This also covers
verbatim Windows joins without replacing standard-library path semantics.

The explicit `join_after_commit` helper shares that admission implementation but
does not observe cancellation. It is used only for best-effort work after the
commit: late cancellation must not turn a successfully published generation into
a failure. A rejected path is not deleted or silently skipped as completed work;
it is counted as deferred cleanup and sets `retry_required`. I/O failures and
directory/discovery failures likewise remain retryable. Already absent files do
not count as successful deletions or pending work.

The configured attempt limit counts actual calls to the remover. A path-admission
denial consumes no I/O attempt. Pending count is capped at `max_pending_files`,
including on complete memory denial; overflow still sets the retry flag. Files
remain in the directory for rediscovery by the existing stateful retry API. No
new allocation-backed report is needed to communicate denial.

This covers Skein-owned deletion paths and removes unnecessary one-shot state;
it is not complete filesystem, allocator or RSS accounting. Standard-library
directory enumeration and returned native filename buffers remain filesystem
inputs outside the build ledger, as do allocation-error construction internals.
The stateful public report/queue ownership contract remains separate. No private
second budget root, new capacity limit, dependency, runtime or I/O backend is added.

## Verification

Normal regressions check borrowed/moved name identity, independent exact/one-short
join envelopes (ordinary and canonical paths), a live competing owner, charge
lifetime through removal, bounded denial, real-file retry, late cancellation and
directory/discovery failure. Counts are compared with a fresh stateful pass over
multiple attempt/pending limits, retention configurations and uniform I/O outcomes.

A generation integration test creates quarantine evidence after the commit gate,
checks the new manifest before cleanup, then optionally occupies the original
writer's entire remaining budget. Both cases publish and reopen successfully;
one cleans the file despite late cancellation, the other reports deferred work
that the existing retry path later removes. The original ledger releases fully.

The manual seed `0x206c1ea` campaign generates 128 directories and 4,480 files.
Fixture metadata supplies a retention oracle independent of the parser, including
mixed artifact families, unknown generations, full-width ordinals, quarantine and
invalid names. It checks exact/one-short budgets with a 137-byte competing owner,
bounded actual deletion attempts, missing-file and permission-error outcomes,
retained files, retry counts and complete release. It is not a CI campaign.

Negative controls must fail for a separate generation/path budget root, early
path-charge release, suppressed retry, excess deletion attempts, removal of the
previous generation and a late publication checkpoint. Restore exact source
hashes before final positive checks.

```bash
cargo test -p skein-search --all-features cleanup
cargo test -p skein-search --no-default-features cleanup
cargo test -p skein-search --all-features committed_cleanup_campaign -- --ignored --nocapture
cargo test -p skein-search --no-default-features committed_cleanup_campaign -- --ignored --nocapture
bazel test --nocache_test_results \
  //crates/search:skein_search_tests \
  //crates/fuzz:skein_fuzz_tests \
  //crates/fuzz:skein_fuzz_cli_tests \
  //:skein_linux_ci_fuzz_smoke_test
```

Full #206 still requires its remaining resident/backend/host-output ownership,
combined-cap, tokenizer/Jieba, representative-corpus and native-platform gates.
