# Embedded architecture controls, 2026-10-24

Issue Number: ref #1222

These are local mechanism/latency controls for the embedded architecture change.
They do not replace the original-baseline Consumer acceptance gate, sustained
54-case matrix, native Windows runtime or power-loss qualification. The source
baseline is `720d98f8640105add1007040ac608df1e643fc3e`. Candidate source manifests,
binary/harness hashes and unfiltered raw observations identify the measured
implementation independently of this document's commit.

## Scan control

`crates/storage/benches/projection_label_source.rs` runs the same source in both
checkouts with the repository's pinned toolchain and ordinary bench profile.
Each of the six small/bulk/sparse × resident/OutOfCore-reopen cases has three
repeats. Small is 32 unrelated + 2 selected nodes with 128-byte bodies; bulk is
4,096 + 32 with 1,024-byte bodies; sparse is 256 + 4 with IDs 4,096 apart and
128-byte bodies. Every run checkpoints its independently constructed source and
compares the complete emitted selected-ID set with the fixture's exact set.
Reopen is a storage residency control, not a device-cache eviction claim.

Timing covers source capture and complete enumeration, excluding fixture
creation/checkpoint/reopen. Limits are identical: total work/items 8,000,000,
bytes 128 MiB, step work 256/items 64/bytes 1 MiB. Resource-controller-free
contexts isolate the enumerator; this is not a governor-admission benchmark.
Logical `items` count every attempted ID/actual metadata record and End;
`work` additionally charges scope checks. The new path charges complete label
comparison work, so its units can increase on dense resident data while its
hole/decoding work falls. Bytes are logical cursor input accounting, not physical
I/O traffic. No runtime cut, sampling or partial verification is accepted.

## Publication control

The original `benches/consumer_publication.rs` and workload helper first ran on
both sources, using detached preparation and SyncOnEveryWrite. The preserved
`uncontrolled_start/` results are negative controls: all 60 publications per
enabled workload completed, but every active-writer/post-E advance count was
zero. A finite foreground stream can drain before publication starts; these
observations cannot qualify progress during concurrent writes. The original
source/binary manifests and an interrupted pre-sample attempt remain in that directory.

An optional capture-before-writers schedule is copied identically into both
source checkouts for the follow-up. Workers wait for the first captured E, commit
one real disjoint SQL update each, then continue without a rate limiter. The
first-commit handoff is outside measured write intervals and outside measured
preparation/construction phases; total elapsed time retains startup cost.
The first publication must prove an actual committed post-E advance. The flag
only supports disabled/detached modes; its default preserves the original start
schedule. No existing progress threshold is lowered. Counts of advances while
writers are active remain separate from committed post-E witnesses.

The controlled workload uses 1,024 foreground writes instead of the original
256; keep their raw samples and conclusions separate. Small/bulk fixtures
retain complete document checks, fixed disjoint SQL foreground writers,
16 shared maintenance trigger writes and four quiet successful publications.
The disabled control performs the same foreground/trigger writes with no
projection maintenance. Foreground latency includes owner wait plus commit.
Reported publication callback time is a lower bound on exclusive owner hold,
not a measurement of the inner selector mutex. Successful watermark advances
are counted explicitly; rejected attempts cannot stand in for faster success.

These finite local samples expose both benefits and regressions. They do not
establish a zero-regression gate, general starvation freedom or production tail
latency. No compiler profile or per-target optimization flag is changed.

## Unchanged Search segment reuse: evaluated, not enabled

The registered producer has two coupled outputs: its exact native generation
and its already loaded resident compatibility snapshot. In
`crates/search/src/consumer/registered.rs`, `initialize_producer` binds that
private builder to its original authority/registration/checkpoint/epoch;
`load_compatibility_snapshot` and reopen verify the selected generation against
the same resident snapshot. The non-vector path writes a complete snapshot;
the vector path's `generation_writer/consumer.rs` also writes the complete
compatibility snapshot and builds admitted generation artifacts from all docs.

The existing out-of-core delta writer has its own target-bound mutation and
append capabilities. Those do not authorize the registered producer to adopt
arbitrary files from another generation or reopen a pathname as a producer.
A new covered E can contain identical document content while still requiring a
new native binding/receipt and source durability validation. Advancing only the
watermark or copying the previous receipt would bypass those fences.

Consequently segment reuse alone would not remove the current O(documents)
resident materialization/snapshot cost. Cross-generation sharing also needs a
native manifest/closure contract for original producer custody, artifact hashes,
shared reachability/GC roots, retained byte admission and crash-safe recovery.
Those changes require new lost-response, retained-reader, corruption and
reclamation qualification. This delivery keeps complete generation construction
and exact full-source comparison. It improves source enumeration and provides a
versioned mapper contract without silently adding an unqualified sharing path.
The measured construction/foreground costs below identify the remaining work.

## Reproduction

Create independent baseline/candidate checkouts; copy the exact candidate scan
harness and its `[[bench]]` declaration into the baseline only. Existing
publication harnesses must have identical SHA-256 hashes. From each checkout:

```console
cargo bench --no-run --locked -p hawdb-storage --bench projection_label_source
cargo bench --no-run --locked --bench consumer_publication
```

Run the reported executables sequentially with fresh, nonexistent fixture roots.
The scan binary takes that root as its sole argument. Publication examples:

```console
consumer_publication --mode detached --documents 32 --payload 128 --writers 1 --writes 256 --quiet-samples 4 --root NEW_ROOT
consumer_publication --mode detached --documents 1024 --payload 512 --writers 1 --writes 256 --quiet-samples 4 --root ANOTHER_NEW_ROOT
```

Use `--mode disabled` for the matched foreground control. Preserve all raw
samples, successes/refusals, source/binary hashes and workload parameters. The
benchmark deletes only its own newly created fixture; never point it at an
existing database.

## Scan observations

Three exact-reference repeats per cell; times are capture + complete enumeration
medians in microseconds. `cold` means OutOfCore reopen with an uncontrolled device
cache. All cells match the complete fixture reference, with no duplicate IDs.

| Shape | Residency | Inputs baseline → candidate | Work baseline → candidate | Time µs baseline → candidate |
| --- | --- | ---: | ---: | ---: |
| small | resident | 35 → 35 | 69 → 103 | 8.58 → 9.96 |
| small | cold | 35 → 35 | 69 → 107 | 56.46 → 39.50 |
| bulk | resident | 4,129 → 4,129 | 8,257 → 12,385 | 188.29 → 536.21 |
| bulk | cold | 4,129 → 161 | 8,257 → 487 | 83373.50 → 86.54 |
| sparse | resident | 1,060,866 → 261 | 2,121,731 → 781 | 20403.08 → 28.04 |
| sparse | cold | 1,060,866 → 261 | 2,121,731 → 785 | 948079.33 → 53.62 |

The dense resident bulk case regresses from 188 µs to 536 µs (+348 µs, 2.85×).
Its actual-record COW continuation and additional label accounting cost more
than the previous dense-ID point path. This implementation deliberately keeps a
single exact resident continuation, rather than adding a second dense-shape path
or a broad membership index. The cost is retained explicitly; this is not a
universal scan-speedup claim. Cold bulk prunes unrelated segments and avoids
property decoding; sparse cases avoid over a million ID probes. End is included
in input counts, and the candidate's work accounting charges full label checks.
Complete mapping/comparison and native generation costs remain additional.

Raw [baseline](scan-baseline.json), [candidate](scan-candidate.json),
[summary](summary.json), and [source/binary manifest](uncontrolled_start/measurement-source.json)
retain every repetition. These samples do not waive any historical strict
performance acceptance gate.
