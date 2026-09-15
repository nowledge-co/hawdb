# Lexical spill ownership and reserved merge progress

This private stage of [#392](https://github.com/nowledge-co/skein/issues/392)
extends [token ownership](SEARCH_TOKEN_OWNERSHIP.md) through document-frequency
runs and corpus posting merges. Query APIs, token and field semantics, logical
source/term/token/spill limits, and persisted artifact encodings retain their
existing contracts. The approved public context constructors remain private.

## Owners and admission

One `ReservedMemory` owns an admitted lease on the operation's existing spool
account. Bounded RAII grants subdivide that lease; they do not release or
reacquire root capacity. The reservation and its allocation metadata are admitted
before construction. Grant growth is checked before changing counters. A grant
held by a decoded term keeps the root reservation alive even after its reader,
run and build pool have gone away. Concurrent grant releases and unwinding return
capacity to the same reservation.

The private `Shared` owner prevents a final-drop gap for both direct and reserved
terms. Every clone uses `Arc::into_inner` on drop, and weak references never
escape. The final holder extracts the payload and frees its control block before
dropping text and its lease. The pinned implementation explicitly drops its weak
control-block owner before returning the payload; concurrent final drops extract
that payload exactly once. See
[Rust 1.97.1's allocation implementation](https://github.com/rust-lang/rust/blob/8bab26f4f68e0e26f0bb7960be334d5b520ea452/library/alloc/src/sync.rs).
`ManuallyDrop` preserves the existing representation and prevents a second drop;
one documented private unsafe take implements this ownership transfer.

Posting buffers own separate slot and document-ID leases. Document-frequency
record vectors admit replacement capacity while the old allocation remains
charged. Terms retain their existing owners throughout sorting, reduction and
draining. The logical map/run allowance uses its historical metadata layout;
adding physical ownership fields cannot silently change that logical limit.

Spill paths are cleanup owners. Formatting, native join growth, the retained
path, registry capacity and replacement overlap are admitted before allocation.
Completed sources and destinations retain their owners until their merge level
is retired. Each reader and writer owns its fixed 8 KiB I/O buffer. Decoded terms
and IDs are admitted before allocation; reader and heap slots are reserved before
opening readers. The ID is destroyed before its grant and the term shares its
own immutable payload owner. Cancellation/deadline checks cover file operations,
record traversal and 8 KiB text reads/writes. Cleanup still runs after cancellation.

## Progress and retention

Ingestion prepares working capacity from actual emitted term/ID lengths and the
bounded fan-in, separately from the configured maximum term policy. The bound
covers three frequency I/O buffers and four simultaneous decoded term payloads,
or the corpus readers, writer, heap and simultaneous heads, whichever is larger.
It also covers frequency binary-carry paths, a corpus source level plus completed
destinations, and registry replacement. A complete preadmitted merge level uses
this capacity when another account occupies all remaining root capacity. Temporary
source/destination overlap does not trigger a new reservation during that level.

Long-lived consumers must not monopolize progress grants. A spilled document's
already sorted frequency stream writes directly to a corpus run using its
borrowed document ID. It does not rebuild a large posting vector. Artifact blocks
independently admit and copy reserved terms before retaining them; direct admitted
terms can still share their original payloads. Statistics and document-ID copies
retain the artifact builder's existing admission. Final artifact retention may
still be rejected when it cannot obtain its own capacity.

Native filesystem conversion has a separate, serially reusable scratch allowance
that grants cannot consume. On the pinned Unix implementation, paths at least
384 bytes long use heap-backed C strings (32 bytes on ESP-IDF). The allowance is
three times the encoded length plus terminator, covering conversion replacement.
See [the pinned small-C-string helper](https://github.com/rust-lang/rust/blob/8bab26f4f68e0e26f0bb7960be334d5b520ea452/library/std/src/sys/helpers/small_c_string.rs).
The Windows bound covers coexisting UTF-16 input, full-path scratch and verbatim
reconstruction, including the native current-directory bound for relative paths.
Its derivation uses [the pinned Windows path implementation](https://github.com/rust-lang/rust/blob/8bab26f4f68e0e26f0bb7960be334d5b520ea452/library/std/src/sys/path/windows.rs)
and [its buffer helper](https://github.com/rust-lang/rust/blob/8bab26f4f68e0e26f0bb7960be334d5b520ea452/library/std/src/sys/pal/windows/mod.rs).
Windows allocation behavior has not been measured locally.

## Verification and limits

Permanent tests cover exact and one-byte-short root/grant admission, replacement
overlap, failed growth, concurrent releases, consumer retention, cancellation,
corrupt/truncated runs, partial writes, flush/unlink errors and unwind. They run
complete document-frequency reduction and corpus merges at fan-ins 2, 4 and 32
while real source input and competing root leases remain live. Artifact retention
must return the entire temporary term grant as soon as the merge input drops.

The allocation probe measures Rust-requested live capacity with old/replacement
overlap, excluding allocator bookkeeping, error reports and process RSS. It checks
that merge allocations fit the admitted reservation, cleanup returns to zero,
and both text and control blocks are freed before their capacity owners. A long
native-path probe separately compares actual conversion requests with scratch.
Mutation controls must be built from isolated source trees and separate Cargo
target directories; never share a positive target with a negative build.

```sh
cargo test -p skein-search
cargo test -p skein-search --no-default-features
cargo clippy -p skein-search --all-targets -- -D warnings
bazel test //crates/search:all //crates/fuzz:skein_fuzz_tests //crates/fuzz:skein_fuzz_cli_tests //:skein_linux_ci_fuzz_smoke_test
```

This establishes progress for an admitted working set and merge topology. A larger
term, additional retained metadata or a resident map can still exhaust the root
before the logical spill threshold. Shared-pressure adaptive spilling, policy
feedback and process headroom remain later work. Outer discovery/publication,
artifact path conversion, delta hydration and the complete public context API
are also pending. This stage does not remove the 4 MiB source guard, qualify
general large-document support, or replace the original #206 corpus criteria.
