# Analyzer token and resident-frequency ownership

This private stage of [#392](https://github.com/nowledge-co/hawdb/issues/392)
extends the generation resource ledger through source-owned analysis. It builds
on [the opaque analyzer contract](SEARCH_ANALYZER_WORKSPACE.md) and
[the generation ownership foundation](SEARCH_BUILD_RESOURCE_OWNERSHIP.md).
Public query signatures, token order, field weights, term policies and persisted
encoding remain unchanged. The approved pair now exposes the integrated
[context contract](SEARCH_GENERATION_CONTEXT.md).

## Payload and collection owners

`build_term::Term` distinguishes legacy owned strings from admitted immutable
payloads. Admission precedes allocation of the string and its `Arc` payload.
Clones of an admitted term share both text and one lease. The final payload owner
frees the string before releasing that lease. A tracked term cannot be converted
into an untracked string; consumers retain the owner or independently admit a
copy. This avoids a release/reacquire gap across crate-private ledger APIs.

The analyzer borrows unchanged source text and lexicon aliases. Lowercased text,
adjacent combinations and owned suffixes receive their own admission before
allocation. Borrowed text becomes an owned term when emitted. Raw identifiers
continue to use contextual string lowercasing; identifier parts continue to use
scalar lowercasing. Alias and suffix selections are borrowed iterators with the
same expansion order. The legacy query and mini-delta paths retain untracked
string ownership and their existing logical limits.

Identifier deduplication admits HashMap table capacity separately. A replacement
table is admitted while the old table's lease is still held. A scope reset drops
keys but retains both the table capacity and its lease for reuse. Final owner
destruction releases the table before its capacity lease. Source-borrowed keys
need no second string;
owned keys share the emitted payload. Cancellation and consumer failures stop
traversal without revoking any term that the consumer retained.

The resident frequency map admits a conservative node/split allowance before
inserting a new term. Repeated or field-unique occurrences use the existing node.
The term moves into posting buffers or frequency spill records with its lease.
The map's capacity lease survives draining, including errors and unwind; emitted
terms remain independently owned after the map is gone. Artifact copies preserve
this distinction: shared term clones allocate no additional payload, while
untracked clones and statistics strings retain their separate copy admission.

## Private ownership boundaries

Production code opts into legacy or separately admitted string copies through
`Term::untracked`; infallible `From<String>` and `From<&str>` conversions exist
only in test fixtures. Admitted paths continue to use the fallible constructors.
This prevents an accidental production `term.into()` from silently dropping the
admission requirement; explicitly choosing an untracked constructor still needs
call-site review.

Dedup growth first admits and validates a separate replacement table. Failure
before transferring entries drops the candidate and its lease, preserving the
original table. Successful growth releases the old table before materializing
the new term, preserving the previous peak-memory boundary. If materialization
then fails, the larger table remains correctly charged and can be retried.
Spare-capacity insertions use one entry lookup; a full table checks duplicates
before requesting replacement capacity.

Consuming a resident frequency map returns an iterator that owns both the
remaining entries and their capacity lease, in that drop order. Early return,
partial iteration and unwind retain that ownership without a caller-local
keepalive binding. Tracked artifact terms share the original payload admission;
reserved merge terms still make independently admitted retained copies.

## Safety argument for scope reuse and throttling

This argument describes an admitted traversal using the same `BuildMemory`
throughout, as constructed by the current private implementation in
[`control.rs`](../crates/search/src/analyzer_stream/control.rs). Legacy traversal
without `Control::memory` has no ledger guarantee. The admitted argument assumes the
pinned capacity envelope bounds actual allocation requests, successful ledger
reservations enforce the shared budget, and Rust's field destruction order.
Allocator qualification tests check the first assumption; this is not a proof
of a future standard-library HashMap layout or an RSS bound.

Let `K` be the current scope's keys, `C` the table capacity, `L` its live capacity
lease, and `H(C)` the retained-table envelope. The owner invariant is:

1. `|K| <= C`, and every live table is covered by `L >= H(C)`.
2. Each owned key or emitted admitted term retains its payload lease until the
   last shared owner is destroyed. Table capacity and term payload are separate
   charges; borrowed keys do not require an additional string allocation.
3. The set `K` contains exactly the successfully materialized spellings admitted
   since the last reset. A failed insertion cannot mark an un-emitted spelling
   as seen. Consumer failure stops traversal; it does not revoke delivered terms.

The empty owner establishes these properties with `K = {}`, `C = L = 0`.
Inductively, a duplicate changes neither keys nor capacity. With spare capacity,
`entry` materializes before insertion; a materialization error leaves the vacant
entry uninserted. On growth, the old owner remains intact while a separate
candidate reserves `H(C_new)` and allocates/validates its table. Rejection drops
candidate storage before its lease and leaves the old owner unchanged. Success
moves keys into the validated candidate and replaces the old owner, releasing
old storage before its lease. A subsequent materialization failure may retain a
larger capacity, but leaves the key set unchanged and that capacity fully charged.
It is therefore safe to retry; rollback to the original capacity is unnecessary.

Reset maps `(K, C, L)` to `({}, C, L)`. Dropping key owners may release payloads
that have no consumer, while independently retained consumer terms remain valid.
The same spelling in a later scope can be emitted again. Destruction drops the
remaining keys and table before `L`; consumers can outlive the table. Thus reset,
failed insertion, retry, and destruction preserve all three invariants.

For a sequence of scopes whose cardinalities never exceed the current capacity,
reset causes **zero table allocations and zero table lease reservations**. Growth
only follows a new capacity requirement, rather than each scope boundary. The
tradeoff is that the largest table's charge remains resident until traversal
ends; this can reduce available budget for later work. It is not a claim of lower
wall-clock latency or lower peak memory for every input.

For checkpoint throttling, let `S = 1024` and `r` be the remaining skipped work
points. Initially `r = 0`. At each non-cancelled `Control::check`, `r = 0` performs
one full checkpoint and on success sets `r = S - 1`; otherwise it decrements `r`.
Induction gives `0 <= r < S` and full checks at work points `1, 1+S, 1+2S, ...`.
For `n` successful work points there are exactly `ceil(n/S)` full checkpoints in
this throttle (zero when `n = 0`). A failed full check returns before advancing
`r`, so retry cannot skip the failure. Other analyzer checkpoints may add checks.

Cancellation is checked before the throttle, so a cancellation flag observed at
any work point is immediately passed to the full checkpoint and returned as an
error. An asynchronously set flag can race with that observation; this is
cooperative cancellation, not an atomic guarantee about callback delivery. A
deadline that expires just after a successful check is detected within the next
`S` control work points, **if traversal reaches them**. This is neither a
wall-clock latency bound nor a guarantee to detect expiration before a shorter
traversal finishes. No new claim is made about time inside opaque analyzer calls.

## Issue #537 acceptance mapping

The production changes landed in [#651](https://github.com/nowledge-co/hawdb/pull/651),
[#666](https://github.com/nowledge-co/hawdb/pull/666), and
[#668](https://github.com/nowledge-co/hawdb/pull/668). The original issue describes
older code; its ten items map to the current implementation as follows:

| Item | Current implementation and evidence |
| --- | --- |
| 1: checkpoint density | `CheckpointThrottle` bounds full checks; `admitted_tokenizer_throttles_deadline_checks` measures actual checks over 4,096 identifiers; cancellation/unwind regression checks consumer retention and cleanup. |
| 2: per-scope allocation | `Dedup::reset` clears keys while retaining capacity/lease; `dedup_reset_reuses_its_admitted_table_capacity` checks reuse, and `dedup_shares_owned_text_and_keeps_consumers_alive_after_scope_reset` checks cross-scope re-emission and independent consumer ownership. |
| 3: duplicate probes | Spare-capacity insertion uses `HashMap::entry`; the full-capacity duplicate fast path avoids replacement admission. `duplicates_need_no_new_admission_with_full_or_spare_capacity` exhausts the remaining budget in both cases. |
| 4: HashMap envelope duplication | Dedup delegates to `bounds::retained_hash_table_bytes`. The opaque regex cache deliberately retains its separately qualified overlap envelope; these two contracts need not have identical constants. |
| 5: manual growth rollback | A separately admitted RAII candidate replaces lease grow/shrink rollback. Reusing a Vec growth helper is unnecessary because HashMap replacement transfers key ownership under a different contract. Exact/one-byte-short growth tests check shared-budget overlap and rejection. |
| 6: lowercase formula | `Text::lowercase` delegates to `bounds::growing_bytes`; exhaustive Unicode scalar and contextual-sigma regressions cover expansion and semantics. |
| 7: implicit untracked terms | Production has no `From<String>`/`From<&str>` for `Term`; fixture-only conversions are cfg-gated. Explicit `Term::untracked` remains a reviewed escape hatch, not a universal type proof that every caller is admitted. |
| 8: direct tracked artifact input | `retained_artifact_shares_a_tracked_resident_term_and_its_admission` checks a resident tracked term without relying on a disk round-trip. Shared payloads need no second payload charge. |
| 9: frequency iterator keepalive | The consuming iterator owns entries and their capacity lease. `frequency_iterator_keeps_the_map_admitted_after_its_analysis_scope_ends` and error/unwind tests exercise ownership beyond the original scope. |
| 10: failed growth and retry | Candidate rejection releases its lease; failure after committing a larger table retains the correct charge. `rejected_term_materialization_keeps_the_replacement_admitted_for_retry` verifies keys, charge and successful retry. |

The regression suite supplies executable evidence for these invariants, not a
formal proof of allocator internals or a throughput benchmark. Deadline-check
counts and capacity reuse are deterministic operation evidence; no measured
end-to-end speedup is claimed.

## Capacity model and qualification

The model is qualified against Rust 1.97.1 on x86_64 Linux and the pinned analyzer
dependencies. It accounts Rust allocation requests, including replacement
overlap; it does not measure RSS, allocator metadata or process-wide headroom.

- A tracked term charges string capacity plus the actual payload layout and two
  `Arc` counters. The current layout adds 64 bytes to string capacity.
- Checked lowercase admission allows four times the larger of eight bytes and
  three times the input byte length. This covers output expansion, amortized
  growth and old/new allocation overlap. Exhaustive Unicode scalar verification
  checks the expansion assumption; explicit Greek sigma fixtures preserve the
  distinct contextual and scalar results.
- HashMap admission uses the pinned load factor, bucket/entry layout and 32 bytes
  for control-group padding. Tests exercise exact and one-byte-short replacement
  admission with the old table and a competing root account still resident.
- Resident B-tree nodes use the existing 2,048-byte per-entry conservative
  node/split allowance. These execution charges are separate from the unchanged
  logical analyzer-map accounting, source, term and token limits.

Permanent regressions compare every emitted event and rejected prefix with an
independent legacy implementation, plus 384 seeded documents across three
lexicons. They cover field weighting, complete frequencies, consumer retention,
shared-root denial, cancellation, unwind and old-generation recovery. Existing
generation tests compare complete posting/statistics results, BM25 scores,
artifact bytes, spill reduction, reopen and failed publication.

Local allocation probes include the production term, text and dedup modules and
the real executor ledger. At payload lengths 1, 4,096 and 131,072 bytes, retained
allocation and charge are 65, 4,160 and 131,136 bytes; shared clones allocate zero.
The string deallocation hook still observes the charge. Reversing payload/lease
drop order fails this check. A 16,384-key dedup probe covers 14 table allocations;
all measured allocations fit admission and cleanup returns to zero. Removing
replacement overlap admission fails at a 216-byte allocation against only 100
admitted bytes. Lowercase allocation probes cover ASCII, expanding Unicode,
contextual sigma and supplementary scalars at short and long lengths.

Routine verification:

```sh
cargo test -p hawdb-search
cargo test -p hawdb-search --no-default-features
cargo clippy -p hawdb-search --all-targets -- -D warnings
bazel test //crates/search:all //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests //:hawdb_linux_ci_fuzz_smoke_test
```

Use separate source worktrees and Cargo target directories for positive builds
and each production-source negative control.

## Remaining boundary

The subsequent [spill ownership stage](SEARCH_SPILL_OWNERSHIP.md) covers the
physical spill handoffs below and tightens final shared-control-block destruction.
The following paragraph records this token stage's original boundary.

Frequency/posting spill buffers, native paths, merge heads/readers and reserved
progress now retain admission; see [the spill contract](SEARCH_SPILL_OWNERSHIP.md).
That guarantee applies to the admitted working set and merge topology. A new
maximum term or additional resident state can still exhaust the root before
logical spilling; general shared-pressure adaptation remains separate.

Outer publication and delta hydration now use the integrated
[context facade](SEARCH_GENERATION_CONTEXT.md). Shared host policy, general large-input support and removal of the 4 MiB
source guard remain unqualified. The original #206 corpus acceptance criteria
and approved validation-only budgets are unchanged.
