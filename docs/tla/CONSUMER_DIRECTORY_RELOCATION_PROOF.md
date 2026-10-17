# Consumer directory relocation custody

A successful rename changes the namespace before the fallible parent-directory
barrier or artifact reload. The native lease follows the original directory
identity; an old pathname is not authority for a later directory created there.

The native regression injects failure immediately after rename and before the
parent barrier. It recreates the source pathname and initializes a second real
consumer there. Before the fix, the first consumer accepted a mutation, wrote a
maintenance marker and scanned 14 entries in the second consumer's directory
while holding only the moved first directory's native lease. No deletion occurred.
The failing fixture was synthetic and its evidence is retained.

`publish_directory` now updates its resident path immediately after successful
rename and marks the binding invalid before any later fallible barrier/reload.
Only successful namespace synchronization and reader reload restore validity.
`Authority::require_index` rejects an invalid binding before apply, checkpoint,
preparation or another directory publication. Failed publication keeps the actual
root, native lease and checkpoint files; it does not remove the destination or
claim rollback. Before-rename errors retain the original path and prior binding,
with the existing dirty-operation admission semantics unchanged.

The combined consumer selection passes 27 tests. The regression checks
rejection of the old owner, absence of peer markers,
correct destination path, invalid binding, rejection of checkpoint/preparation/
repeat relocation before creating its destination, and retention of native
exclusion. After explicit owner closure, reopening the moved complete checkpoint
must return the original receipt. That reopen is a process-level observation;
it does not prove that a failed namespace barrier was durable across power loss.

[HawDBConsumerDirectoryRelocation](HawDBConsumerDirectoryRelocation.tla) models
one successful rename, namespace barrier success or failure, reuse of the source
pathname, owner operations, cleanup and explicit closure. TLC checked 18 generated,
10 distinct states to complete depth 5. Four isolated controls violate the named
invariants for retaining the old pathname, trusting failed publication, releasing
the native lease early and deleting uncertain evidence. This is intentionally a
small custody model, not a byte-level filesystem, reader decoder or general
cross-parent durability model. The atomic rename/namespace barrier are premises;
Windows execution and physical loss/reordering remain separate qualification.

This fix composes with the runtime mutation-custody owner association. It does
not supply logical target incarnation/ABA continuity, a C-ready/E selector,
durable operation ledger, installed-candidate cleanup or Mem adoption. The combined [component qualification](CONSUMER_MUTATION_CUSTODY_PROOF.md)
passed full native/selected facade/strict-target checks, the 11 generic Search
physical-fault tests and all 98 required unfiltered fuzz targets. Source tree
`9495dc1f8ef7c2d377b2752baf3e6422cbb41a24` stayed fixed; subsequent updates are
Markdown source-correspondence/evidence only. This does not qualify all directory
crash outcomes, cross-parent relocation or Windows namespace durability, nor
supply the full future activation protocol.
