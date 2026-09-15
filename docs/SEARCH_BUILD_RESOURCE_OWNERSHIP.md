# Search build resource ownership

This implements a private foundation for [#392](https://github.com/nowledge-co/skein/issues/392).
An operation owns three reusable memory accounts for input, spool scratch and
retained state. Leases follow payload ownership across the streaming generation
writer, segment and vector builders, lexical blocks and lexical manifest reader.
The account registry does not grow with document count. Existing component limits
remain independent constraints.

The context constructor stays private until the remaining stages implement the
complete resource contract. Existing public entrypoints use their existing defaults.
This change does not remove the 4 MiB lexical source guard, change the finite term
policy or 256 MiB manifest default, or change query semantics and artifact formats.

## Implemented ownership

- Writer options, native paths and accepted input retain their capacity charges.
  Spool records use fixed admitted scratch; decoding admits owned fields before
  allocation and validates the complete record before invoking a consumer.
- Segment payloads retain admitted documents until encoding finishes. Descriptor
  dictionaries, normalized values, output buffers and native compression workspace
  share the operation ledger. Replacement allocations account for coexistence of
  the old and replacement capacities.
- Vector state, quantization inputs, retained directories and artifact verification
  share the ledger. Cancellation is cooperative around opaque vector-core calls;
  those calls are not interruptible internally.
- Pending lexical blocks, copied IDs/terms and retained directories retain leases
  through builder completion. Partial I/O poisons the builder.
- Lexical manifest sizing and checksumming borrow the immutable body. Output bytes
  are admitted before allocation. The private file must match that exact snapshot
  before decoding; the complete artifact checksum and header are verified before
  publishing the lexical manifest. Reader metadata remains charged until its last
  `Arc` owner drops, including when that owner outlives the builder.

## Manifest decode boundary

The decode allowance is derived from the trusted generated body's string lengths
and vector element counts. It is never inferred solely from an arbitrary file's
byte size. With pinned Rust 1.97.1, serde 1.0.228 and serde_json 1.0.150, sequence
visitors grow vectors geometrically and string visitors copy decoded slices.
The allowance includes old/replacement vector overlap, reusable escaped-string
scratch and the returned reader's allocations. Actual retained capacities are
checked before shrinking the lease to the retained size.

The opaque serde parse checks cancellation before and after the call. Encoding,
checksum I/O and metadata walks add cooperative checkpoints within their work.
This is not protection against arbitrary concurrent in-place file modification.
An artifact renamed before a later publication failure can remain unreferenced;
the outer generation's private stage owns its cleanup. Existing durability errors
can be ambiguous after rename and before directory sync completes.

## Remaining contract

The opaque Jieba/regex workspace now has operation admission and native-join
ownership; see [the analyzer contract](SEARCH_ANALYZER_WORKSPACE.md). Source-owned
token normalization, identifier deduplication and resident frequencies also retain
their admission through consumers; see [the token contract](SEARCH_TOKEN_OWNERSHIP.md).
Spill buffers/readers, registries, merge heads and reserved progress under combined
pressure still require integration. Generation discovery, outer manifest publication, delta hydration and
the two approved public context constructors also remain separate work. Do not
describe this foundation as a completed whole-operation limit or cancellation bound.

The ledger models owned Rust capacities and named native scratch allowances. It
does not measure allocator overhead, every native allocation or process RSS.
Independent operations do not share a reservation merely because their contexts
contain the same numeric budget. Shared host policy remains #186. The original
complete-corpus comparison remains #206; partial or synthetic corpus evidence
does not satisfy that acceptance gate.

## Verification

Tests cover exact and one-byte-short shared-root admission, replacement capacity,
payload/lease lifetimes, fixed account count, cancellation, failure cleanup and
poisoning. Manifest tests compare the independent legacy wire encoder, reject
equal-size corruption/growth/truncation before publication, verify complete artifact
digests, and preserve the previous projection through cancellation and unwind.
Output and scratch admission failures precede manifest temporary-file creation.

```sh
cargo test -p skein-search
cargo test -p skein-search --no-default-features --lib
cargo clippy -p skein-search --all-targets -- -D warnings
bazel test //crates/search:all //crates/fuzz:skein_fuzz_tests //crates/fuzz:skein_fuzz_cli_tests //:skein_linux_ci_fuzz_smoke_test
```

Keep local fuzz available through Bazel without adding a CI job. Native CI and
independent review remain separate from local qualification.
