# Search capture closure across native write paths

The pinned C..E session cannot certify coverage when a canonical commit can
silently omit its projection effect. This closes the explicit-schema/opaque
classification and two direct graph-entry gaps found while preparing that
session. It does not implement the session or issue coverage authority.

## Source mapping and conditional argument

`store/search_projection_capture.rs::projection_wal_requires_rebuild` exhaustively
classifies the current WAL enum. Explicit graph DDL, graph projection/import
markers and opaque append effects require rebuild; no point-ID vector proves
their complete effect. Nested batches share a 65,536-visit counter and depth-64
limit. Exhaustion conservatively requires rebuild. A future WAL variant must
choose a classification at compile time instead of entering a wildcard branch.

Graph point operations still use the existing cold/live collector. Relational
records retain their typed primary-key capture or RequiresRebuild event,
including snapshot replacement. Relationship properties are irrelevant to the
built-in graph document shaper; arbitrary host mapping effects are not thereby
proved irrelevant. Internal relational exclusions retain their existing
contract. These classifications do not prove arbitrary callback semantics or
all host dependency/fanout rules.

`prepare_search_projection_changes_for_ops` stages a resume gap for unsupported
effects before WAL. Both raw single and batch append helpers independently
classify the effects, covering direct DDL entry points that do not call the
point collector. They install the same gap only after append succeeds, including
the no-WAL in-memory path's successful logical write. Failed live appends retain
the old floor/events. The enclosing exclusive store borrow keeps this update
inside the logical commit boundary. Existing uncertain/post-WAL failures retain
their fail-closed recovery contract; no cross-store rollback is introduced.

A gap at E clears the current retained log and advances its persisted resume
floor to E. C < E must rebuild. A complete verified baseline at E can resume
narrow later changes; the floor does not permanently disable incremental work.
Previously captured COW feed directories retain their own records/floor and
source epoch. WAL replay uses the same pre-application classifier. Checkpoint
serializes the existing floor with the source state. No WAL/checkpoint codec or
default synchronization policy is changed.

`graph_mutation.rs::create_relationship` and `create_connected_nodes` now use
`append_graph_wal_and_capture` before applying their data. The former captures
the relevant projected HAS_LABEL endpoint. The latter captures newly created
node IDs as well as the edge effect. Direct point capture now agrees with
replay instead of acknowledging a live mutation with no feed record.

Conditionally on complete ordered WAL/checkpoint recovery, induction over
commits preserves this alternative: each supported effect contributes its
complete point/typed record, or older cursors encounter an explicit rebuild
boundary. No failed live append can advance that boundary. Replay reconstructs
the same alternative before applying data; checkpoint preserves it. Existing
typed RequiresRebuild events must likewise block successful enumeration.

This is a source-contract refinement, not numeric proof of arbitrary physical
I/O, all schema transaction implementations, power-loss write ordering, or
version-retention/RSS bounds. Classification adds bounded metadata work and
conservative rebuilds can increase cost; no latency or scaling win is claimed.

## Finite checks

`HawDBProjectionCaptureClosure` explores two source epochs, prepared/direct
routes, nested batches, point changes, schema/opaque effects, direct edges,
connected-node creation and typed replacement records. Live append can succeed,
leave a partial tail, or return an uncertain response with a complete record.
The model reconstructs a valid prefix; checkpoint and rebuild retain their
source-boundary roles. Native strict refusal is separately allowed rather than
an availability promise of the model. An independent projection oracle
requires exact output for every accepted C..E catch-up.

TLC 2.19 / TLA+ 1.7.4, Java 21.0.9, jar SHA-256
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`:
**106,297 generated / 10,320 distinct states**, depth 11, no invariant failure.
Nine controls omit direct/nested schema, opaque, direct-edge or connected-node
capture, publish before WAL, omit replay barriers, lose checkpoint floors, or
ignore typed rebuild records. Every control returns exit 12 for its named
invariant. Two probes require reachable rebuild and narrow point progress after
rebuild, excluding a vacuous always-refuse model.

The model abstracts actual WAL codecs, CRC, descriptor state machines, physical
storage, metadata budgets, COW implementation and arbitrary host semantics.
Opaque projection changes represent an unqualified effect that may change a
mapping; they do not assert that every opaque operation changes the built-in
graph document set. Prefix repair is an abstract selected recovery outcome;
native strict torn-tail rejection remains allowed. These checks do not prove
hardware power-loss safety or the complete publication/fault composition.

## Native evidence

Nine targeted capture tests include:

- Materialized and cold checkpoint-open sources; explicit property DDL creates
  a live gap, WAL-only reopen and checkpoint/reopen preserve it, and newer
  point changes remain resumable. An older captured directory still enumerates
  its original node and End at the original epoch.
- Existing-table descriptor writes with partial append, rollback and sync
  faults: failed live operations expose no new descriptor, epoch or floor.
  Reopen yields the old state, complete DDL plus its gap, or strict torn-tail
  refusal retaining evidence. This is not qualification of every DDL API.
- Direct HAS_LABEL edge creation and connected-node creation against warm and
  cold bases, comparing live events with ordinary WAL replay.
- Memory-only/direct-single commits; nested mixed schema/data preparation;
  metadata/depth exhaustion; unchanged-schema no-op preserving epoch/floor.
  Earlier uncertain-preimage/shared-poison tests remain enabled.

Against production source `5cb756a6568468196507bca13ea1cc6233281ef8`, attaching
the native regression tests without the implementation changes produces four
named failures: direct edge, connected nodes, recovered descriptor gap and live
schema gap. The unchanged-schema no-op control passes. Final checks rebuild the
current package; shared Cargo output from the detached baseline is not accepted
as final-source evidence.

## Coverage-session prerequisites still open

Old heads/checkpoints are not retroactively certified by this change. A future
session must verify its baseline's capture/mapping contract and source/binding
identity, or require a full rebuild; an epoch/floor alone is insufficient.
Complete host graph/relational mapping and exclusions, bounded query/fanout work,
artifact ownership, stage-bound tokens, durable operation receipts, actual Mem
owner migration and the original recovery/RSS/1-4-8-writer/fairness gates remain
required by the [batch protocol](../specs/MVCC_SEARCH_BATCH_COVERAGE_SPEC.md) and
the full #231/#232 acceptance ledger. Legacy partial-prefix hydration remains
unchanged, so #1043 stays open.
