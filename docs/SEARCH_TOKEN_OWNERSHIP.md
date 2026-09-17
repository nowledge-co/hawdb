# Analyzer token and resident-frequency ownership

This private stage of [#392](https://github.com/nowledge-co/skein/issues/392)
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
table is admitted while the old table's lease is still held. Each scope releases
its table before its capacity lease. Source-borrowed keys need no second string;
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
cargo test -p skein-search
cargo test -p skein-search --no-default-features
cargo clippy -p skein-search --all-targets -- -D warnings
bazel test //crates/search:all //crates/fuzz:skein_fuzz_tests //crates/fuzz:skein_fuzz_cli_tests //:skein_linux_ci_fuzz_smoke_test
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
