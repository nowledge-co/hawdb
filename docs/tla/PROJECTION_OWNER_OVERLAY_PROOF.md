# Source-owner C/E overlay: algebra and implementation contract

Status: finite design model, independent algebraic oracles and a native
artifact-only owner/phase external reducer. Complete host capture and qualified
facade C/E activation are not implemented here.
This work contributes to #231/#232/#1043 and preserves every remaining gate in
[the versioned-publication ledger](../specs/MVCC_VERSIONED_PUBLICATION_SPEC.md).
It is not a replacement for baseline/capture/mapping/binding/external-vector
qualification, actual Mem migration or the writer/resource/power-loss gates.

## Why document-ID priority is insufficient

A projection document and its canonical source owner are different identities.
Owner is canonical instance plus graph physical ID, or stable table incarnation
and full typed relational PK. Native source enumeration selects one canonical
producer where Graph/SQL data mirror the same logical document. An unrelated
owner cannot replace it merely by emitting the same document ID.

For example C has owner1 -> a and owner2 -> b. At E, owner1 changes to b while
owner2 still owns b. An ID-priority reducer can replace C's b with E's b and hide
the duplicate ownership. Equal encoded bodies do not make two owners one owner.
A rename also needs to retract the old ID a, and an absent/deleted owner needs
an explicit E tombstone. Conversely, a valid ID swap between two owners must
succeed: stale C ownership must be replaced before final ID uniqueness is tested.

First resolve final state per stable source owner, then sort the resulting
nonempty documents by ID and require unique ownership. Generic ordinary fixed-E
mutation coalescing cannot substitute for this two-stage rule.

## Conditional algebraic argument

Let O be the source-owner universe, D the complete canonical document encoding,
and C,E: O -> Option(D) be the correct full projections at the two pinned source
contexts. None is a real owner tombstone, not an omitted callback. Let
Delta contain every owner o whose C(o) differs from E(o); overcapture is allowed.
The selected/native effect set must include Delta. All repeated E visits must
return exactly E(o), including body, title, metadata and original embedding;
CRC/length or ID equality alone do not establish equal canonical state.

Initialize M=C and acknowledge a set S of visited owners. Maintain:

- M(o)=E(o) for o in S;
- M(o)=C(o) for o outside S.

Initially S is empty. Processing one owner assigns M(o)=E(o), including None;
other owners are unchanged. An identical repeated E state leaves this invariant
unchanged; a conflicting repeated same-E state refuses. Thus the invariant holds
by induction for any input order and any number of admitted identical repeats.
After complete native End, Delta is a subset of S. Every unvisited owner therefore
has C(o)=E(o), so M=E everywhere. A sparse C spool may omit None entries, provided
absence means C(o)=None and every relevant E tombstone is still processed.

Now require different nonempty owners to have different document IDs. If that
holds, final unique-ID emission produces exactly the E documents, including all
payload/vector bytes. Otherwise refuse, even for identical documents. Persisted
owner state must also equal E, not merely its unordered document set: after an
ID swap, a wrong retained owner association can corrupt the next deletion even
when today's document set happens to match.

This argument assumes correct complete source mapping and effects. The model
checks the owner merge under those premises and has negative controls that break
them; it does not prove that a current WAL/host callback supplies those premises.

## Required source/contract context

Canonical instance, schema/table incarnation, mapping identity, source namespace,
complete capture contract, target binding/incarnation and independent-vector
identity must be compatible. Graph C/E and vector cursors may advance; their
respective complete effect intervals must be covered. Compatibility does not
mean ignoring vector changes or requiring a graph-only cursor to stand in for
the external source. A changed mapping can affect owners without any canonical
DML and must not silently reuse an old baseline/capture contract.

Final-E source dependencies, binding/incarnation, exact target base and source
and external durability still need the authority that serializes publication.
A coherent historical E set is not permission to ignore relevant changes after
E. Numeric cursors and a caller-supplied compatibility flag cannot mint native
activation authority. The finite model abstracts these predicates; native typed
provenance, actual host mapping and source-linked regressions remain required.

## Current native gaps and next implementation

The complete full-C replacement owns its source/stage and can leave the outer
owner/old-reader scope. It still uses a whole-C dependency and rejects all later
canonical writes; it has no owner sidecar or C/E merge. Ordinary unordered runs
carry document-ID upsert/delete intents. The artifact builder below adds admitted
owner framing and ordinal phases, owner reduction and strict final document
sorting. Native canonical owner/phase provenance remains required.
Shared actual admission must cover both stages and
intermediate I/O. Do not acquire a second permit from a one-background-slot host,
reset cumulative credit at E, or expose ordinary artifacts as coverage receipts.

`search_projection_capture.rs` currently records a built-in old document ID and
Label neighbors on DeleteNode, but does not record every deleted physical node
as an owner event. Thread has no built-in document identity in `projection.rs`.
Actual Mem Thread metadata affects SQL thread_messages and may own no document
itself. A C index containing only emitted documents therefore cannot recover
all old Thread fanout identities. Qualify bounded old-identity preimages, retained
C parent-to-child lineage, or an enforced/audited atomic child-update/immutable
identity contract. Current delete_full_thread atomically deletes child rows and
records a receipt; absence of a Thread preimage alone is not evidence that this
route is broken. Parent-filtered current joins cannot enumerate all tombstones.
See [the Mem effect audit](../specs/MVCC_MEM_PROJECTION_EFFECT_AUDIT.md).

Older checkpoints/logs missing a new capture contract must retain a gap/floor
and require full rebuild. Replaying available WAL may reconstruct physical owner
IDs, but cannot retroactively certify missing older checkpoint evidence. Inspect
capture retention separately from conflict-history pins: a C snapshot does not
contain future appends. Long full scans need a qualified bounded capture lease
or admitted phased catch-up with explicit private E1/E2 progress. No private
cursor may become a visible complete-through watermark before full closure.

Source-owner keys must use exact bounded typed identity, not a hash or a host
metadata field. Admit key/header/body ownership, repeated inputs, intermediate
runs, comparisons and cumulative query/physical costs. Preserve source poisoning,
partial-input refusal, cancellation and cleanup custody. No new crate or broad
index/optimizer feature is needed merely for this representation.

## Native artifact implementation and its boundary

`SearchGenerationAdmission::prepare_owner_replacement` constructs an owned
[`SearchOutOfCoreOwnerReplacementWriter`](../../crates/search/src/out_of_core/generation_writer/unordered/owner.rs).
It copies target policy/base information
and does not borrow the old reader. The new API builds private content; it does
not return a source-qualified C/E coverage or baseline receipt. Opaque owner keys
and phase ordinals supplied to this layer cannot establish canonical identity or
complete native source intervals. The Database full-C session retains its existing
unchanged-C dependency fence and does not yet use this artifact builder.

Source correspondence, rather than a verified machine-checked refinement:

| Conditional obligation | Native mechanism |
| --- | --- |
| Keep owner identity separate from document ID | Admitted `OwnerStamp` key and phase in private run framing; original encoded document body is not rewritten. Key bytes and ordinal range are bounded before intake. Canonical typed-key provenance remains a facade requirement. |
| Preserve older same-phase conflicts | Heap and run merge order by `(owner, phase)`; intermediate merges retain all records. Final owner reduction checks every equal-phase pair with exact encoded-body comparison before selecting a later phase. None/Some disagreement refuses. |
| Resolve owners before document uniqueness | The reduced owner run feeds a second document-ID sort. Only final nonempty states enter it; strict unique upserts reject different owners emitting the same ID, including identical bodies. |
| Bound both stages under one admitted attempt | The same `Control`, `BuildMemory`, task, stage and actual governor permit survive handoff. Input/read/spill/work/merge counters and private run sequence do not reset. Input credit includes both source owner records and transferred final documents. |
| Keep file/cleanup ownership through handoff | Owner source readers stay alive until document ordering has drained copied spool references; readers close before stage cleanup. Cancellation and any failed step poison the attempt. Prepared artifacts retain actual governor custody until publication/drop. |
| Refuse damaged or incomplete private runs | Owner/phase/body-descriptor checksum, canonical body frame checksum, expected run digest/extent and monotone key checks. Run End is validated before ordering completion; incomplete ordering cannot return a candidate. These are integrity checks, not cryptographic authority. |

The owner run retains original spool bodies; transfer copies those frames into
document runs under the same accounting rather than decoding all bodies into
host collections. One current owner/phase winner is enough after external sorting.
All temporary owner state is removed afterward: the final artifact does **not**
persist a reusable owner sidecar. The model's owner-state invariant therefore
describes the transient owner reduction, not durable baseline reuse authority.
Reports distinguish owner inputs, reduced owners (including None), final
documents and shared cumulative sorting counters. Final artifact construction
and installation remain blocking and are not bounded by an ordering quantum.

Focused all-feature native tests pass **9/9** (9.98s). They cover late C records,
rename/deletion, a repaired transient collision and valid ID swap, duplicate final
IDs with identical bodies, conflicting superseded same-phase content/title/
metadata/vector/absence, empty and partitioned replacement, actual governor
custody, old-reader closure and cross-thread completion, cumulative input credit,
key/phase/body/checksum corruption, truncation, and cancellation at both sorts,
handoff and completed ordering. Cancellation tests assert zero retained task
memory and unchanged visible heads after cleanup.

The independent native reference case exhausts **252** small C/E, input-order
and spill-buffer combinations: 7 unique C states, 9 E states, C-before/after-E
orders, and one/eight-record buffers. It compares every emitted canonical body
against a separately constructed complete E set and expects **56** duplicate-ID
refusals. It inspects private output and does not claim 252 durable publications.
The first corruption fixture assumed plaintext body encoding and failed before
injecting corruption; the corrected fixture locates the actual framed payload.
That fixture failure is not a native corruption-refusal result.

Minimal-feature focused tests pass **9/9** (7.40s). The full all-feature Cargo
Search unit suite passes **873 / 20 existing ignored**, 245.86s native. The
default-feature Bazel Search suite passes **870 / 20 existing ignored**, 236.86s;
the different feature profiles account for their different counts.

The frozen native input tree is `14067df637198c493a710ca954aa8eeea28079c0`, based
on `29557a4`. The first 122-target matrix completes **121 passed / 1 TIMEOUT**,
46 executed / 76 cached; invocation `de73d7ba-3406-4661-9619-582c1389887f`,
2,836.960s elapsed / 554.17s critical path. Minimal publication completes 63/64
native cases before its unchanged 300s deadline; the unfinished case is
`relational_prefix_pages_cover_membership_deletes_aba_off_page_and_absence`.
Its log/XML and the terminal campaign are retained before retry. This is an
action failure and not a proven code-performance cause.

After terminal completion and frozen inputs are verified, an exact-command
retry passes **122/122**, **1 executed / 121 cached**, 148.510s elapsed / 146.30s
critical path; invocation `fde4b166-7ca1-42f3-a1fc-18006f49220c`. Minimal
publication passes **64/64**, 146.07s native / 146.2s action, with the same
timeout, assertions and concurrency settings. This scope includes both profiles of the
six MVCC facade groups, default Search, all three required local fuzz suites,
ten selected positive models and the evidence-script check. It is not a fresh
all-model or whole-workspace runtime qualification. Both graph-residency halves
pass **16 seeds / 992 state checks** each, 298.21s / 388.41s native. The retry
launcher initially refused the singular `1 fails locally` summary before
starting any retry; its terminal parser was corrected without changing tests.

The finite TLC model and its controls remain conditional; they do not prove
the frame codec, file operations, complete physical owner/parent capture, native
phase Ends, mapping contract, capture retention, binding/incarnation, independent
vectors, final-E dependencies, durable receipts or actual Mem integration. Those
premises and the original resource/power-loss/writer gates remain open.

## Finite model and oracle

`HawDBProjectionOwnerOverlay.tla` uses two source owners, absent/present documents,
two document IDs, body/vector bits, valid unique C snapshots, arbitrary E states,
complete or mutated effect selection, context compatibility, owner-state mapping,
identical/malformed repeats, explicit End, cumulative admission and final owner/
document uniqueness. It separates complete owner state from document-set equality.
Positive checking passes **385,924 generated / 297,871 distinct states**, depth **8**.
The initial integer-vs-tuple None encoding was a TLC type error (exit75), not an
invariant counterexample; the corrected model uses a tagged absent tuple.

Fault controls cover omitted effect owner, ignored tombstone, document-ID phase
priority hiding a cross-phase collision, duplicate-owner coalescing, reading C
instead of pinned E, ignored conflicting same-E repeats, reset admission, skipped
End and ignored context. Witnesses require success, deletion, a valid ID swap,
identical repeated input and a complete empty result. All fourteen controls pass with
exact TLC exit 12 and their named invariant. This is not a verified native codec/reducer refinement or a proof of complete
capture, actual host authority, I/O/RSS, physical power loss or fairness.

An independent finite Python oracle checks 17,738 ordered/repeated input
cases over 49 unique C states and 81 E states, refusing 6,912 duplicate-owner
cases. It exhibits missing-owner, deleted/renamed-ID and hidden duplicate-owner
counterexamples. Those are conditional algebra checks, not native qualification.

TLC 2.19 / TLA+ tools 1.7.4, Java21.0.9; jar SHA-256
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`.
Java standard-module extraction uses an isolated temporary directory per process.

Reproducible commands:

```sh
python3 docs/tla/owner_overlay_oracle.py
bazel test //docs/tla:HawDBProjectionOwnerOverlay_check
```

The oracle is retained with this argument; it adds no production control plane
or CI fuzz target. The artifact reducer below does not supply source-qualified
owner framing, complete capture or activation authority.

## Phased catch-up during the C scan

The existing native capture log is trimmed by entry/byte limits in
`GraphStore::trim_search_projection_graph_change_log`; that method does not
consult prepared-read conflict-history pins. A snapshot at C cannot retain future
appends. Keeping C's query context alive therefore does not establish that a
later capture of C..E is available. Full-C replacement still rejects later writes;
this observation concerns the next online C/E implementation.

One possible bounded protocol captures complete successive intervals C..E1,
E1..E2, and so on while C enumeration continues. Each effect is mapped at its
own pinned Ei, and its owner state carries the native phase. Late-arriving C
records must lose to greater phases, including a tombstone. The private reducer
must check exact repeated states within each phase, even if a newer phase later
supersedes that owner; checking only the latest retained value hides malformed
input. Resolve owner state before checking final document uniqueness. A transient
collision at E1 may be repaired by E2 and cannot alone reject the final E2 set.

Conditionally, let S0=C and S1,...,Sn be the complete pinned phase projections.
Each captured interval must cover every owner changed since its predecessor.
For each owner, select its greatest phase among complete C and admitted effects.
If no interval selects that owner, its state stayed C throughout. Otherwise let
k be its last selected interval: all later unselected intervals left it unchanged,
so Sk(o)=Sn(o). Thus the selected owner state equals Sn regardless of C arrival
order. This needs actual complete intervals and native End for C and every phase;
arbitrary caller phase numbers or partial pages cannot satisfy the premises.

`owner_overlay_phases_oracle.py` independently exercises two catch-up phases,
every C/E1/E2 state over two owners and two document IDs (including absence),
complete and overcaptured effects, both owner orders, C/catch-up interleavings,
and exact repeated traces. It also checks delayed-C resurrection, a repaired
transient collision, and conflicting superseded same-phase absence/body/vector
states. The bounded run checks **209,692 scheduled/repeated traces**, including
**46,464 final duplicate-owner refusals**, over **7 unique C states** and
**9 states per catch-up phase**. Run it with:

```sh
python3 docs/tla/owner_overlay_phases_oracle.py
```

This is a finite algebraic check, separate from the single-E TLC model above.
It supplies no native capture retention, framing, shared admission, phase/work
ceilings, final-E dependency or activation evidence. The native implementation
must cap total phases and cumulative resources before intake, retain all needed
intervals or refuse gaps, and advance only private capture progress until final
closure. It must not acquire a second governor permit or reset credits per phase.
Writer progress and memory/I/O bounds remain separate qualification gates.

## Native observations of the missing capture premises

The retained [standalone Rust probe](owner_capture_probe.rs) links the real HawDB
library and checks these existing boundaries with in-memory fixtures:

- With a C read snapshot held, deleting Thread produces no physical-owner event
  and no built-in old-document delete; the feed still reaches End. Deleting
  Memory produces an old-document delete but no physical-owner delete event.
  This is the legacy built-in projection contract, not a failure of that
  contract. Neither result certifies complete generic owner effects.
- With a complete C source held and the capture entry limit set to one, three
  later property updates advance the capture floor from C=1 to floor=3. Starting
  a feed at C correctly refuses BehindFloor, while the full C source still reads
  the original property value zero. A query/history pin therefore does not
  promise retention of the future capture interval.

Both assertions passed against the combined implementation. The probe is a
manual native observation, not a discovered Cargo/Bazel test, host mapping
qualification, capture lease implementation, or persistence/power-loss test.
The initial local artifact selection found multiple feature-profile libraries;
no probe ran until the matching all-feature artifact was selected. Reproduce
with the repository's pinned toolchain and its default Cargo target directory:

```sh
cargo build --locked -p hawdb --lib --all-features
rustc --edition=2024 --crate-name owner_capture_probe docs/tla/owner_capture_probe.rs \
  -L dependency=target/debug/deps --extern hawdb=target/debug/libhawdb.rlib \
  -o /tmp/hawdb-owner-capture-probe
/tmp/hawdb-owner-capture-probe
```

These observations strengthen the prerequisites for the next native C/E work;
they do not remove the conservative unchanged-C fence from full replacement.

The later [V1 native capture component](PROJECTION_OWNER_CAPTURE_PROOF.md) now
retains owner-only deletion facts internally. The probe above still observes
the legacy public document feed, whose variants do not expose those facts.
The complete owner session and future-capture retention lease remain open.
