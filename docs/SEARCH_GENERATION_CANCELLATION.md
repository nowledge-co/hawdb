# Cooperative search generation builds

`SearchOutOfCoreGenerationWriter::create_with_context(root, options, context)`
is an additive embedded-library entry point. The original `create` delegates to
it with a default, uncancelled context. `prepare_delta_with_context` uses that
same context for the old-generation scan and retains it until the returned
update's `finish`. No environment variable, runtime thread, helper process,
storage backend or persisted version changes are involved.

The mutable writer owns a cloneable `RuntimeTaskContext`, including parent
cancellation and deadline. It does not transfer this operation context into
immutable readers. Host governor admission remains caller-owned: keep its permit
alive for the operation. The current entry point applies cancellation/deadlines;
component build-option caps remain in force, but aggregate cross-phase memory
reservation enforcement is still a separate #206 acceptance gate. Passing a
memory reservation is not yet proof of a total build working-set bound.

## Cancellation boundary

Before final publication, cancellation/deadline checks cover:

- creation, input encoding/spooling, and finish entry;
- each source spool record, including after its consumer returns;
- segment document loops, compression boundaries and RaBitQ sink operations;
- lexical analysis boundaries, posting spill writes, run merges and frame/skip
  production;
- FST build and validation callbacks, dictionary partitions and temporary copies;
- chunked artifact checksumming and final publication preparation.

Cancellation is an execution error, not storage corruption or a signal to retry
a smaller dictionary partition. An input failure poisons the writer; it cannot
publish an earlier prefix as a complete generation. Stage cleanup and lexical
temporary guards remove uncommitted outputs after the owning operation drops.
Spill writers close before their temporary file guard runs, including error paths.

The last cancellation check in `publish_generation` occurs immediately before
the first immutable artifact link is installed. Once this commit section starts,
the existing manifest-last sequence completes without observing a late cancel.
The final active manifest is still written only after all required artifacts are
installed and verified. No post-commit checkpoint converts a successfully
published generation into a cancellation error. Filesystem errors retain their
existing publication/recovery semantics; this is not a new rollback protocol.
The standalone private lexical writer uses the same pre-publication rule for its
own manifest, which is only staged data during a fused generation build.

This is cooperative, not preemptive cancellation. An ordinary filesystem call,
one bounded analyzer/compression/vector operation, or the commit section cannot
be interrupted mid-call. The vector dependency's finalization and reopen also
remain a synchronous call; checks around it do not promise bounded cancellation
latency inside that dependency. The source spool is still scanned once. The
legacy resident `SearchIndex::checkpoint` path is unchanged and host-governed.

## Regression coverage

Normal tests cover cancelled creation with no root/stage allocation, inherited
parent cancellation and poisoned input, expired finish before source scan,
consumer-triggered mid-spool cancellation, cancellation after all sinks finish
but before publication, same-generation retry, and context retention between
delta preparation and finish. Lexical regressions prove cancellation after real
spills preserves the old selector, and cancellation during skip/dictionary copy
stops without returning completed metadata. The local state-machine campaign also
checks cancelled lexical builds before its next independent BM25 comparison.
Test-only hooks trigger cancellation after actual spool reads and immediately
after the final commit gate. They prove that a partially consumed fused scan
cleans every staged sink, while late cancellation still returns the published
generation with its exact hydrated document. A negative control that adds a
post-commit checkpoint must fail the latter regression; restore it before positive
validation.

```bash
cargo test -p skein-search --lib out_of_core::generation_writer::tests::cancellation
cargo test -p skein-search --lib lexical_projection::tests::build_cancellation
cargo test -p skein-search --no-default-features --lib generation_writer
cargo test -p skein-search --lib lexical_projection::tests::fuzz:: -- --ignored
bazel test --nocache_test_results //crates/search:skein_search_tests \
  //crates/fuzz:skein_fuzz_tests //crates/fuzz:skein_fuzz_cli_tests \
  //:skein_linux_ci_fuzz_smoke_test
```

These checks do not replace native macOS/Windows evidence, aggregate build/query
resource accounting or the representative-corpus byte comparison required by #206.
