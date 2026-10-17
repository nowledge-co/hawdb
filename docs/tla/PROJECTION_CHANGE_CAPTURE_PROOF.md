# Pre-WAL Search projection change capture

`HawDBProjectionChangeCapture.tla` checks graph change capture from hot overlays
and cold canonical metadata, installed atomically with a committed mutation.
This is a finite refinement argument for full-prefix catch-up at the current
source epoch E. It does **not** certify a bounded prefix B < E hydrated from E.

The [native path-closure refinement](PROJECTION_CAPTURE_CLOSURE_PROOF.md) adds
explicit schema/opaque rebuild boundaries and repairs direct edge/connected-node
entry points. It complements this cold-preimage model rather than proving an
artifact-owning session or arbitrary host mapping.

## Native boundary

`store/search_projection_capture.rs` prepares a closed change or a resume gap
before WAL append. The collector projects only labels and, when deleting an old
document identity, the pretransaction `id`. It reads both directed HAS_LABEL
postings and validated canonical relationship identities without constructing
relationship property Values. Created node/edge metadata is borrowed from the
transaction, so a new Label and its first edge are captured together. Repeated
renames delete the original published identity, not intermediate statement IDs.

Direct graph writers and mixed graph/relational commits install this prepared
value only after successful WAL append. Replay captures before applying each
complete transaction. Partial append, rollback failure and uncertain sync never
install a prepared event or gap in the failed live operation; recovery either
replays a complete transaction or fails closed with evidence. Strict torn-tail
reopen fails; dropping an incomplete tail in the model is an explicit repair,
not an automatic recovery claim.

Checkpoint candidate replay uses the same preimage collector as ordinary replay,
with the checkpoint work context attached to each operation/metadata visit.
Both paths retain cold canonical reads, created-endpoint handling and schema
resume gaps. The checkpoint adapter prepares privately, checks cancellation,
then installs the complete event or gap before applying the transaction.
Its work permits do not claim to bound encoded block validation or physical I/O.
The checkpoint regression compares a cold ID rename and a schema gap; existing
cancellation probes cover every observed capture/replay unit and retry.

Capture has cumulative limits of 65,536 operation/metadata visits and 64 MiB of
estimated metadata/event bytes, plus a 64 KiB projected preimage and nesting 64.
A limit refusal commits canonical data but clears retained capture and advances
the existing persisted resume floor to E. Cursors below this floor must rebuild.
Physical metadata errors instead fail before WAL and poison shared service.
These limits do not bound encoded block validation, physical I/O, allocator
peaks or total RSS. They do not make the existing eager batch builder bounded.

## Model argument

Two epochs range over root payload and document-ID changes, double rename,
root/Label deletion, both edge directions, edge insertion/deletion and creating
a Label with its first edge. An independent projection oracle includes an
unrelated immutable document. Consumers bind an exact full projection at C,
then apply all retained C..E events using the current E graph, or rebuild if
C is below the floor. Equality with the independent full oracle is checked.
Events prepared before WAL cannot acquire a committed epoch before commit.

The WAL model distinguishes complete records, partial tails, uncertain responses
and loss of the last unsynchronized record. Checkpoint/replay preserve capture
and its floor. **Binding includes a source-durability barrier**: synchronizing
recovered unsynchronized records is necessary before binding a durable Search
baseline. Without that precondition an epoch can be reused after a second loss
and an epoch-only consumer can skip a rename. The separate source-durability
model/API supplies this prerequisite; this model does not prove that composition
or byte-level filesystem synchronization.

TLC 2.19 / TLA+ 1.7.4, pinned jar SHA-256
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`,
Java 21.0.9, four workers, MaxEpoch=2: **1,664,531 generated / 481,147 distinct
states, depth 10**, no invariant failure. This is exhaustive within that finite
configuration, not an unbounded theorem or machine proof of the Rust program.

Twelve negative controls disable cold nodes, cold edge identities, cold adjacency,
incoming fanout, created Label handling, preimage timing, WAL publication ordering,
gap rejection, metadata failure rejection, replay capture, checkpoint-floor
persistence or original-only deletion. Each must exit 12 for its named invariant;
parse errors/timeouts do not count. Two witness probes require reachable cold
incremental progress and rebuild progress. Configurations and expected invariants
are registered in `mutants/mutants.txt`.

## Native evidence and remaining scope

`cold_changefeed_replays_complete_search_changes_against_full_rebuild` compares
Search delta application with a fresh full build across two residency modes,
with/without WAL reopen and eight graph mutation cases (32 fresh fixtures).
`changefeed_captures_new_label_edges_and_published_ids_inside_one_transaction`
covers created nodes/edges and multiple statements. The oversized-preimage test
requires a persisted gap while preserving canonical commits. Storage tests
cover partial append/rollback/sync failures and shared poisoning before WAL.

This closes the reproduced cold first-mutation omission. It does not provide
bounded batch coverage receipts, historical B hydration, host incarnation or
binding, actual Mem owner integration, complete baseline/capture-contract and
host-effect qualification, composed power-loss
qualification, retained-source RSS or writer-scaling/fairness evidence.
