# Relationship domains and eager E guards

This component protects relationships on owners outside a selected endpoint.
It also allows a caller to pre-register complete label, relationship-type and
canonical SQL-table read scopes at the owner feed's immutable E. It is an
E-validation primitive. It does not certify a mapper's complete source list,
C..E coverage, output completeness, target incarnation or durable publication.

## Native correspondence

`ReadRelationshipDomain(Some(type_name))` protects all relationships of that
exact canonical type, including an absent type. `None` is the all-type summary
stamped by the kernel. Names remain stable across implicit catalog additions;
explicit schema and opaque legacy writes retain Schema/Database barriers.

The [collector](../../crates/storage/src/store/read_dependencies/relationship_domains.rs)
visits create, property and delete WAL effects, recursively including batches.
It borrows names of same-transaction creates so a following update/delete does
not depend on a published object. For existing relationships it reads live type
IDs or the cold canonical relationship identity header, then resolves the name
in the working catalog. It never copies relationship property values. Detach
node deletion expands its relationship deletes in the typed mutation path.

The collector admits work (65,536 visits), recursive depth (32) and estimated
metadata (64 KiB) before owning names or created-map entries. An unknown
identity or exhausted admission installs ReadBarrier, even if partial named
summaries have already been collected. Integrity errors poison the store and
return an error. Retained stamps remain subject to VersionWriteSet admission.
These are logical estimates; they do not establish cold-index I/O, physical
RSS, allocation overhead or a whole-transaction resource bound.

The existing commit path validates writer destinations first. Only then does it
add read summaries and admit the candidate version index before WAL. Successful
canonical publication installs them; a failed WAL append/sync installs neither
epoch nor summary. Read summaries never become shared writer intents, so two
pre-staged writes to different relationships of the same type can both commit.
Legacy operations that cannot supply precision retain Database protection.

The [scope constructor](../../src/api/dependency_read/read_scopes.rs) admits the
entire borrowed names/count input before owning keys. Duplicate names consume
input admission. SQL tables must exist in the canonical E snapshot; graph
labels/types may be absent. Scope registration and callback queries use the
same private PreparedRead. Ordinary queries outside a declared scope still
collect their normal dependencies; this API is not a closed-source contract.

A previously registered table domain subsumes same-table row/index-prefix keys;
a label domain subsumes same-label scalar property keys. These implications
follow from complete domain invalidation plus the shared Schema/Database and
ReadBarrier checks. They do not erase prior keys, clear a sticky refusal, or
subsume arbitrary physical node, adjacency or relationship-point reads. The
one-entry integration cases exercise repeated existing and absent point reads.
Remaining physical reads can still cause dependency growth in a future mapper.

## Conditional safety argument

Let R(k,E) contain every physical relationship of type k, including its property
values, at E. Any successful later typed transaction that changes R(k) has a
create, property update or deletion effect. Complete admitted identity resolution
stamps k at the publication epoch. Unknown identity or refused admission instead
stamps ReadBarrier. Schema/opaque changes retain their separate barriers.
Therefore accepted validation implies unchanged R(k), conditional on complete
native effect enumeration and immutable relationship type identity. Endpoint
selection does not restrict this argument. A stamp for another admitted type
alone does not conflict; a conservative fallback may conflict with either type.

## Finite model and native qualification

[HawDBRelationshipReadScope](HawDBRelationshipReadScope.tla) represents two
immutable type names, three physical identity slots per type, two disjoint
writers, create/property/delete effects, pinned reads, WAL failure, explicit DDL,
and admitted summaries versus unknown/admission fallback. Slot 2 lies outside
the selected endpoint. The positive model checks scope safety, unrelated-type
acceptance, absence of shared writer conflicts, no early stamps and pinned
observation. Its checked graph has 4,362 generated / 2,544 distinct states,
complete depth 10. This finite abstraction assumes native identity resolution;
it does not prove the Rust collector or memory/I/O behavior.

Eleven fault configurations target missing summaries, skipped deletes,
properties or off-root changes, wrong type, ignored barrier/schema, whole-epoch
fallback, shared writer intents, early stamps and live reads. Two additional
negative invariants seek successful unrelated publication and both writers
committing. Each control must obtain its expected invariant counterexample;
parsing failure is not a passing control.

Native integration tests cover off-root creates, property changes and deletes
for related and unrelated types, repeated SQL/indexed graph queries with a
one-entry scope, and input admission. Six storage regressions exercise
same-type disjoint writers, nested create/update/delete with a 1 MiB property,
cold metadata-only identity lookup, unknown/oversized fallback, sticky refusal,
and failed WAL write/rollback/sync before summary installation. Fault injection
is not a complete machine power-loss qualification.

On macOS arm64 with pinned Rust 1.97.1, complete default Cargo Storage passed
1,948 tests with zero failures and 30 existing ignored tests (520.84 seconds),
plus one compile-fail doctest. That run preceded the equivalent Clippy Batch
match-guard correction. On the final runtime,
`cargo test --locked -p hawdb-storage read_dependencies` passed 32 tests
(1,946 filtered), and default/minimal facade integration passed 4/4 each.
The separate Bazel Storage selection
`bazel test //crates/storage:hawdb_storage_tests --test_arg=read_dependencies`
also passed 32 tests (1,946 filtered); this is not full Bazel Storage coverage.

The final selected Bazel matrix passed 167/167 targets (99 executed / 68 cached),
2,786.561 seconds wall / 381.38 seconds critical path. It includes 18
default/minimal facade targets, one evidence-script test, all 98 targets in the
required local fuzz suites and 50 selected formal targets. All 14 new model
checks passed. Scope integration passed 4/4 each (6.38 / 6.70 seconds);
complete publication integration passed 64/64 each (142.89 / 152.81 seconds).
These timings are qualification observations, not performance measurements.
Runtime/test/model tree b7e29faf0db4c7181734a18fa87642b2d173cb1c was fixed;
only result/scope prose changed subsequently. Exact argv, source hashes, 501
matrix test log/XML/cache-status files, the separate Storage receipts and all
50 model evidence directories are retained in the local qualification archive.
This is not all workspace tests or all-model qualification. Final formatting,
strict locked workspace/all-target/all-feature Clippy and both prek hooks are
required again before committing the final result prose.

Initial adverse runs remain in the local evidence: a test used an unsupported
multi-pattern CREATE fixture; another referenced the wrong WalReplayConfig
module; the first model draft reused a bound variable name. Corrected fixtures,
type path and binder preserve the assertions and invariants. The first Bazel
matrix was interrupted after 68 passes / 99 skipped because
the Clippy correction changed a source file during compilation; the final
matrix uses fixed runtime sources. An initial prek attempt overlapped staging
and reported files changed; the subsequent unchanged-index run passed.

## Remaining C..E integration

At the recorded guard-only qualification, V1 graph owner capture followed the built-in graph Search mapping's
effects: relationship property updates alone do not enumerate endpoint owners
or declare a capture gap. The built-in mapping ignores those properties. A
custom mapper that reads relationship properties therefore needs an extended
capture/fanout completeness contract for changes in (C,E]. The new relationship
guard protects commits after E; it cannot repair a missing interval event.
The subsequent [capture V2 component](OWNER_ENDPOINT_CAPTURE_PROOF.md) adds
all relationship endpoint effects, while complete mapping/fanout custody remains
separate. Historical test results above do not qualify that newer source.


The native session must consume C-ready custody, retain its source identity and
history pin through E capture, qualify schema/mapping continuity, and bind one
stage/governor to E input and mapping. Complete native Ends and fanout must
precede closing input at E. Business document IDs require qualified producer
selection/uniqueness or a validated owner baseline; physical owner IDs alone do
not prove this. Source and target binding/ABA, independent vectors, durable
lost-response receipts, actual Mem adoption, physical power-loss/resource
qualification, and 1/4/8-writer latency/fairness/group-fsync gates remain open.
