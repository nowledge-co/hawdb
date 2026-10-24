# Embedded architecture controls, 2026-10-24

Issue Number: ref #1222

The current implementation is based on main
`01793dc2923da88e8b8d053ffda8daf6d8b84f06`. Both checkouts use the pinned
Rust toolchain and ordinary repository bench profile. Identical scan and
publication harnesses exercise baseline and candidate with fresh fixtures;
source/binary/harness hashes identify each measurement. Logical scan units are
separate from physical I/O and governor-admission costs.

The earlier `720d98f` controls are preserved under [preintegration](preintegration/README.md).
They include the dense resident scan regression and the original publication
negative controls with zero active-writer/post-E advances. They describe their
recorded sources, rather than the subsequently integrated main implementation.
The follow-up controls use the current baseline and keep all repetitions.

## Measurement contract

The storage scan uses small/bulk/sparse fixtures, resident and OutOfCore-reopen
views, three repetitions and exact complete selected-ID equality. Scoped graph
coverage never samples or publishes partial completion. Reopen does not imply
an evicted device cache. Resident unrelated rows and Bloom false positives still
consume work; the implementation does not promise work proportional only to
selected labels.

Publication controls use small 32-document/128-byte and bulk
1,024-document/512-byte projections, one foreground writer, 1,024 disjoint SQL
writes, 16 shared trigger writes and four quiet successful publications. Disabled
and detached modes receive identical foreground/trigger work. The optional
`--capture-before-writers true` schedule captures E before releasing workers,
then waits for their first real commit through bounded setup handshakes outside
measured write/preparation/construction intervals. Writers continue immediately;
no rate limiter, sleep or changed progress threshold is added. Total elapsed time
includes setup. The first detached publication must witness a committed post-E
advance. Active-writer advances and the existing two-advance qualification remain
separate. This finite workload does not establish starvation freedom.

## Unchanged Search segment reuse

Cross-generation segment reuse was evaluated and is not enabled. The registered
producer in `crates/search/src/consumer/registered.rs` retains its original
native authority, operation and complete compatibility snapshot. Reopen verifies
that snapshot against the selected native generation. The vector producer in
`out_of_core/generation_writer/consumer.rs` writes the complete compatibility
snapshot and builds admitted artifacts from all documents.

Main already shares in-process resident document snapshots and reduces owner
publication overhead. Such sharing does not authorize adopting durable artifacts
from another registered generation. Durable reuse needs an explicit native
manifest/closure contract, original custody, content validation, shared GC roots,
retained byte admission and crash-safe recovery. A new E still needs a new native
binding/receipt even when content is unchanged. Copying a previous receipt or
advancing only its watermark would bypass those fences. Segment reuse alone
would also leave the complete snapshot encoding/comparison costs.

This change retains exact full-source mapping/comparison and complete native
generation construction. These controls do not waive the original-baseline
performance gate, sustained 54-case matrix, native platform qualification or
Mem release/activation policy.

## Reproduction

Copy the candidate storage scan harness and its bench declaration into a clean
baseline checkout. Copy the identical publication harness into both. Build:

```console
cargo bench --no-run --locked -p hawdb-storage --bench projection_label_source
cargo bench --no-run --locked -p hawdb --bench consumer_publication
```

Run executables sequentially with new fixture roots. The scan binary takes its
root as its sole argument. Publication commands use the recorded parameters:

```console
consumer_publication --mode detached --documents 32 --payload 128 --writers 1 --writes 1024 --quiet-samples 4 --capture-before-writers true --root NEW_ROOT
consumer_publication --mode detached --documents 1024 --payload 512 --writers 1 --writes 1024 --quiet-samples 4 --capture-before-writers true --root ANOTHER_NEW_ROOT
```

Use `--mode disabled` for the corresponding foreground controls. Keep all raw
samples, source/binary hashes, successes and refusals. The benchmark removes only
its own newly created fixture; never point it at an existing database.
