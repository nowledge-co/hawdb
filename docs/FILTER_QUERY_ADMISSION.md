# Filter and ACL input admission

The out-of-core query creates its existing `QueryMemory` before metadata-filter
transformation. `filter_memory::Input` moves the requested map into an admitted
owner, reserves an optional ACL merge before cloning/encoding, and reserves
predicate payload and parser scratch before calling the existing optimizer
parser. It uses the same working account as candidate construction, ranking and
hydration, not a separate full-budget root or a per-filter account.

The caller allocated the original map before API entry. This admits operation
ownership before transformation, not that earlier allocation. The borrowed ACL
context is still caller-owned; its internal copy/serialization is covered, not
the caller's source allocation. Empty maps are dropped/replaced to discard a
potential retained B-tree leaf without charging ordinary empty queries a node.

## Capacity and dependency assumptions

These are conservative capacity envelopes, not allocator/RSS measurements.
Arithmetic is checked; typed vector slots must also fit the address space.

- Requested maps include key/value capacities (including spare capacity) and
  the existing 2,048-byte per-entry B-tree occupancy/split allowance.
- ACL copies use source lengths, since `String::clone` copies the initialized
  bytes. One additional map entry covers insertion and replacement overlap.
  A single visibility value copies the field/value directly. Multiple values
  serialize the ordered set directly, without a cloned `Vec<String>`.
- For a multi-value ACL, JSON's encoded length is bounded by two brackets,
  three delimiter/quote bytes per entry and six bytes per input byte. Three
  times the larger of that length and 128 covers the writer's initial allocation
  and overlapping growth. The formatted field key has separate growth allowance.
- Predicates reserve exactly one vector slot per effective filter, canonical
  field lengths (at least the longest current alias, `temporal_context`), input
  value bytes and boolean expansion. List elements add the existing 1,024-byte
  B-tree set allowance and five boolean-output bytes per possible element.
- Per-filter scratch is `1024 + 24 * value_bytes + 4 * key_bytes`. It covers
  temporary normalization, escaped-string decoding, native JSON error formatting
  (including unexpected strings), error-string growth/boxing and field/error
  copies. A list additionally reserves four times the larger of four and the
  possible element count in `String` slots for the sequence vector's growth.
  Filters parse serially, so scratch is the maximum per-filter envelope, not
  their sum; all retained predicate payloads remain admitted together.

These assumptions are qualified against Rust 1.97.1, serde/serde_core 1.0.228
and serde_json 1.0.150 in the lockfile, not an API guarantee from those libraries.
Relevant implementation paths are Rust `alloc/src/raw_vec/mod.rs` (doubling and
minimum capacities), serde_core `de/impls.rs` (`Vec` visitor), and serde_json
`ser.rs` (128-byte initial writer), `read.rs` (escaped-string scratch), `de.rs`
(sequence access without a size hint) and `error.rs` (formatted/boxed errors).
Debug string escaping fits six bytes per source byte; writer growth/boxing and
decoder overlap fit the larger scratch multiplier plus fixed allowance. Recheck
these assumptions when the parser, containers, dependencies or toolchain change.

The private list-prefix scan counts commas outside escaped/quoted strings. It
only bounds the number of values in a valid prefix; it does not validate JSON
or replace optimizer semantics. It checks cancellation every 4,096 source bytes.
ACL validation, merge and parsing have before/after checkpoints. The unchanged
parser cannot be interrupted per character while its call is in progress.

## Ownership and behavior

Requested allocations are moved, not cloned in the no-ACL path. An ACL effective
map is retained through parsing, checked against its preflight envelope, and
dropped with its lease afterward. Parser scratch ends after the call; parsed
predicates retain their payload lease in `Input`. Field drop order releases data
before its charge. Cancellation and admission failures release every private
owner without returning partially constructed input.
The outer query drops this input after candidate construction and report-filter
copying, before tokenization/ranking; those later phases do not retain predicates.

Optimizer constructors move field strings; list normalization reuses its input
vector and sets insert incrementally rather than collecting through a sort
vector. The embedded scan supports all current operators, so its pushdown helper
returns the parsed set directly rather than cloning it through discarded optimizer
events and cloning the pushed set again. Default pushdown parity is tested.

Policy-epoch mismatch keeps precedence over invalid ACL validation, matching the
previous outer query. Aliases, enum/boolean normalization, empty IN/NOT IN, all
operator families and ACL key replacement/intersection remain unchanged. Malformed
filters still return an unsatisfiable set with a parse-error report, not a new
public error kind. Error reports do not echo filter values. The query report still
contains the user's requested filters, not the injected ACL filter map.

## Verification and remaining boundaries

Normal tests independently derive exact/one-short raw, merged and parser budgets,
check spare capacity, moved pointers, largest-scratch reuse, retained ownership
after dropping query handles, cancellation, malformed inputs, policy errors and
known alias/operator outputs. Three optimizer regressions directly inspect field
and normalization-vector reuse and exact predicate-vector capacity.

A published-reader regression rejects insufficient task memory before filter
parsing or candidate I/O. Without full-text search it instead proves capability
rejection before either phase. With ACL/full-text features a successful query
checks the visible result set, requested report filters and policy epoch.
The preceding candidate-cleanup regression now admits filter/pruning inputs and
denies the later metadata decoder. Its phase probes require one physical read
and zero decoder entries before checking that the created spill file is removed;
it does not replace that coverage with an earlier filter rejection.

The ignored seed `0x206f117e` campaign generates 256 cases across 16 filter shapes,
optional single/multiple visibility scopes, escaped UTF-8/NUL and malformed JSON,
spare source capacities and competing result-account owners. Each case retries
exact/one-short input peaks with the same allocated capacities. Published-reader
candidate sets are checked against independent fixture-specific document rules;
default pushdown parity is separately checked using the shared parser, and is
not claimed as an independent parser oracle. Every owner releases its charge and
the ledger retains two accounts. Candidate-phase budgets are not included in the
input-only exact/one-short peak; existing candidate admission tests cover them.

Ten negative controls remove raw admission, ACL preflight or parser scratch,
release the predicate lease early, clone requested inputs, use an independent
query root, count quoted commas, accumulate serial scratch, clone optimizer
fields or clone the normalization vector. Each must compile and fail its target
assertion. Restore exact source hashes before full positive verification.

```bash
cargo test -p skein-optimizer --all-features predicate::tests
cargo test -p skein-search --all-features filter_memory -- --nocapture
cargo test -p skein-search --no-default-features filter_memory -- --nocapture
cargo test -p skein-search --all-features filter_admission_campaign -- --ignored --nocapture
cargo test -p skein-search --no-default-features filter_admission_campaign -- --ignored --nocapture
bazel test --nocache_test_results \
  //crates/optimizer:skein_optimizer_tests \
  //crates/search:skein_search_tests \
  //crates/vector-projection:skein_vector_projection_tests \
  //crates/fuzz:skein_fuzz_tests \
  //crates/fuzz:skein_fuzz_cli_tests \
  //:skein_linux_ci_fuzz_smoke_test
```

The new Bazel campaign is manual and belongs to the explicit local suite, not a
default or dedicated fuzz CI job. No public API, v1 artifact, dependency, backend,
Bazel runtime/timeout setting or release policy changes.

Public reports (including parse errors and cloned filter maps), hits and returned
documents retain their existing unowned return contract. This private input owner
does not complete shared backend/output ownership, resident delta maps, combined
component limits, matched-span/tokenizer workspace, host reader/mapping lifetime,
Jieba qualification, representative-corpus reduction or exact-head native gates.
