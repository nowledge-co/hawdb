# External lexical merge working-set admission

Both intermediate run compaction and final artifact construction use the same
`MergedPostings` cursor. Its `BuildMemory` is the generation operation's existing
three-account root, shared with input, analyzer, artifact and directory owners.
There is no per-run independent budget or per-record account metadata.

## Ownership and preflight

- Each run's 8 KiB `BufReader` is admitted before file opening/allocation. Run
  writers and intermediate merge outputs likewise admit their 8 KiB buffers
  before creating files. Reader/writer buffers use the existing spool account.
- Reader-vector and heap capacities are reserved for the configured fan-in
  before either container allocates. At most one heap head exists per source,
  so refill cannot grow the heap. Inputs beyond the fan-in are rejected.
- A decoded term is admitted after validating its declared length and before
  allocating its bytes. `AdmittedPosting` moves that lease through the reader,
  heap and current-posting owner. Data fields precede their leases on drop.
- The cursor yields a borrow of its current posting, not a bare owned string
  whose accounting would end at the return boundary. Current and incoming
  terms remain charged together while a source is refilled. Exhausted readers
  release their file handles and buffers immediately; end of iteration releases
  all remaining cursor heap allocations, even before cursor drop.
- Final artifact construction admits the 128-posting frame before allocation.
  Each doclist encoding admits the fixed `MAX_BLOCK_BYTES` output capacity before
  calling the codec. The encoder no longer grows geometrically, so it has no
  hidden old/new allocation overlap. Frame capacity is released before the final
  dictionary flush. Skip headers use a fixed stack array instead of `concat`.

No v1 bytes, checksums, term order, exact duplicate semantics or publication
boundary change. Corrupt descending run order is explicitly rejected.

## Failure and cancellation

Refill happens only when the consumer requests the next posting, after checking
cancellation. Dropping or cancelling a consumer does not decode a speculative
next record. Opening/seeding each source also checks cancellation. A decode or
admission error poisons the cursor: retry cannot silently resume after an
already-consumed record prefix.

Moved-out compaction inputs and completed outputs remain in RAII path owners.
Every early return, including source-deletion failure, attempts cleanup of every
owned temporary file. Cleanup remains best effort under filesystem failures;
this does not claim deletion of inaccessible files. Existing manifest-last
publication and old-reader ownership are unchanged.

## Verification

Normal regressions cover returned-posting ownership, reader/container/term
preflight, current/refill overlap, exact and one-short limits, ordered union and
independent v1 run bytes, cancellation before reading an invalid tail, poisoned
errors, corrupt ordering, multi-pass shared input/output admission, cleanup after
an already-completed output and an injected source-deletion error, doclist
encoding and unchanged published generation
after actual final-merge frame denial.

The local manual `skein_search_lexical_merge_fuzz_tests` campaign uses seed
`0x206ae12`: 6,000 groups with up to eight runs, duplicates, empty runs, Unicode
and NUL terms, full-width ordinals/TFs, an independent sorted-tuple union, exact
and one-short operation peaks, and 188 consumer cancellations. Another 12,000
mutated run byte strings are checked against a separate direct decoder and
ordering oracle. Every outcome must release all tracked working memory and keep
exactly three ledger accounts. Ordinary tests compile but ignore the campaign;
the existing mandatory local fuzz suite explicitly executes it. No fuzz CI.

Negative controls must detect omitted decoded-term admission, release of the
current posting before refill, and omitted final-merge frame admission. Restore
all controls before the final positive checks. Disabling the moved-path cleanup
owner must also fail the injected source-deletion regression.

## Remaining issue 206 boundaries

This is requested-capacity accounting, not complete allocator/RSS accounting.
The separate grouping-term clone retained by `ArtifactBuilder`, dictionary
staging and FST build/validation now share the operation root with owned leases
as described in `LEXICAL_DICTIONARY_ADMISSION.md`.
Segment codecs, descriptors and publication follow `SEGMENT_BUILD_ADMISSION.md`.
Lexical backend paths and run registries follow `LEXICAL_PATH_ADMISSION.md`.
Other control allocations, ledger metadata, dependency workspace,
published-reader reopen and outer query/delta retained state still
need their appropriate complete ownership boundaries. Stack scratch and allocator
overhead are not measured by these counters.
The generation RaBitQ sink follows `RABITQ_BUILD_ADMISSION.md`.

Jieba's private persistent HMM workspace remains a separate pending dependency
maintenance decision. No dependency patch, HMM change, helper thread, io_uring,
new backend, public API, migration or Bazel configuration/timeout override is
included. Native exact-head checks and representative-corpus size acceptance
are still required before the full issue PR, then the #291 -> #292 follow-ups.
