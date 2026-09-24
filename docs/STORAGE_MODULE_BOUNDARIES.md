# Storage module boundaries

HawDB hosts integrate through the `hawdb` embedded facade. `hawdb-storage` is an
internal implementation crate; its module layout is not a new database plugin
contract. `#[doc(hidden)]` affects documentation, not Rust visibility or API
stability. See [the graph-engine contract](GRAPH_ENGINE_CONTRACT.md).

## Import ownership

Storage implementation imports name their owning module instead of relying on
a flat crate-root catalog. For example:

| Concern | Implementation path |
| --- | --- |
| Cache admission and leases | `hawdb_storage::cache::SegmentCache` |
| Persistent configuration | `hawdb_storage::config::StorageResidencyMode` |
| Relational rows and indexes | `hawdb_storage::relational::RelationalState` |
| Canonical graph segments | `hawdb_storage::canonical::CanonicalSegmentReader` |
| Append tables | `hawdb_storage::append_table::AppendTableSchema` |
| WAL group commit | `hawdb_storage::wal::WalGroupCommitConfig` |
| Background admission | `hawdb_storage::background::BackgroundWorkAdmission` |
| Positional I/O | `hawdb_storage::io::read_exact_at` |

The root retains shared core error, value and identity exports. Existing `hawdb`
facade export names remain available; no extra host integration point is added.
Direct consumers of removed `hawdb_storage::Type` aliases must use the owning
module path. This is a source-path change for that internal crate, even though
runtime behavior and type identity are unchanged. Column-group types may use a
submodule such as `column_group::group::ColumnGroupReader`.

## Equivalence argument for the #212 migration

At baseline `a703cc0f18f549918c73f454cda521edb4f0fb13`, the storage root exported
705 implementation names through `pub use`, in addition to shared core names.
Counting `pub use` statements instead of their leaves understates this surface.
Every removed leaf already resolves to an existing public module item; the
migration neither moves its definition nor creates a replacement type.

For each removed name `s`, let `M(s)` be the right-hand module path of its old
re-export. Rust re-export identity gives
`resolve(hawdb_storage::s) = resolve(hawdb_storage::M(s))`. Replace root-qualified
references and import-tree leaves by this equality, preserving local aliases.
Consequently type/trait identity, constants, function bodies, drop order and
borrow lifetimes remain the same wherever those references type-check. External
callers using the old root paths must migrate; this argument does not assert
source compatibility for them.

The mechanical audit reconstructed all 224 changed Rust consumer files from the
baseline using only that explicit export map, import-tree/qualified-path
replacement and the repository's pinned rustfmt settings. Every reconstructed
file matched byte-for-byte. The storage root is checked separately: the mapped
re-exports are removed, core identities retained, and module declarations kept.
No changed reference is inside `stringify!` or a string literal that would make
path spelling observable at runtime. Full workspace compilation checks module
visibility and name resolution; native and minimal WASM checks cover both host
surfaces. Test assertions are retained with the same path transformation.

There is no storage state-machine change: for the same inputs, each transition
still calls the same operation on the same nominal types, with the same control
flow. Existing WAL/checkpoint/recovery proofs therefore keep their transition
relations and invariants; no new storage algorithm or relaxed invariant is
introduced. Regression tests remain necessary evidence for the integration, not
a substitute for the path-identity argument.

## Remaining acceptance

Issue #212's other deliveries are the existing durable module split, shared
positional I/O, standard-library file locks, development overflow checks and
complete WAL value-field table. The final acceptance must include storage,
embedded recovery and downstream builds plus required local Bazel/fuzz checks.
A compile pass alone does not establish full issue completion.
