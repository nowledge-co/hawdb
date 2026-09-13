# Bounded lexical manifest encoding

The lexical writer previously serialized a full manifest body for its checksum,
cloned the body's owned strings and vectors into an envelope, and serialized the
full envelope before checking the 256 MiB output limit. Reopening also serialized
another full body solely to verify its checksum. These temporary allocations grew
with the entire term dictionary even when publication would reject the manifest.

The private encoder now streams the body through CRC32C, counts the exact JSON
envelope length while borrowing the body, and checks the existing output limit.
Only an admitted envelope receives a fallible, exact-size output reservation.
The output writer refuses growth beyond the admitted length, and encoding fails
if the final length differs. Decoding uses the same streaming checksum calculation.
The writer drops the original manifest after encoding and its output bytes after
writing and syncing them, before reopening the published generation.

This trades one additional serialization pass for removal of the body-sized
checksum buffer and deep envelope clone. Field order, escaping, CRC32C coverage,
the V1 envelope, validation, and durable artifact/manifest publication order stay
the same. Failed size admission still precedes artifact publication.

## Scope and remaining limits

This bounds encoded output allocation, not total indexing or process memory.
The manifest's term statistics and block descriptors remain resident, and reopen
still needs input bytes plus the deserialized body. Counting and checksumming
visit the complete body; they do not provide a CPU or cancellation budget.
Allocator overhead and the existing owned body are outside the encoded-byte cap.

The public API, reader admission behavior, 256 MiB default manifest cap, term
policy, and document source limit are unchanged. This is a private prerequisite
for [#392](https://github.com/nowledge-co/skein/issues/392) and
[#206](https://github.com/nowledge-co/skein/issues/206). It neither admits the
previously rejected complete-corpus manifest nor establishes the posting
compression ratio or large-document lifecycle qualification.

## Verification

The tests compare exact bytes and round trips against the previous allocating
algorithm, including UTF-8, JSON escapes, empty dictionaries, long terms, and
integer boundaries. Negative controls cover exact/one-byte-short admission,
checksum corruption, unknown fields, inconsistent counts, arithmetic overflow,
serialization errors, and output growth/shrinkage after admission. A separate
512-case deterministic differential campaign is available for local fuzz runs.

```sh
cargo test --locked --offline -p skein-search --all-features
cargo test --locked --offline -p skein-search --all-features manifest_encoding -- --include-ignored
cargo clippy --locked --offline -p skein-search --all-features --all-targets -- -D warnings
bazel test //crates/search:presubmit_tests //:skein_unit_tests \
  //crates/fuzz:skein_fuzz_tests //crates/fuzz:skein_fuzz_cli_tests \
  //:skein_linux_ci_fuzz_smoke_test
```

The manifest differential target is manual and belongs to the local fuzz suite;
it is not added to a CI job.
