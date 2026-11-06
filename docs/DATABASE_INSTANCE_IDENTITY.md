# Database instance identity

Owner: Hawkingrei. Issue: https://github.com/nowledge-co/hawdb/issues/1011.
Host composition: https://github.com/nowledge-co/mem/issues/6054.

`DatabaseInstanceIdentity` is an equality-only process-local marker for one
selected native database runtime. Hosts obtain it from `Database::instance_identity`,
`EmbeddedStoreHandle::database_instance_identity`, or the exact
`EmbeddedReadSnapshot::database_instance_identity` supplying their read.
It cannot be constructed by callers or serialized as a durable receipt.

Moves, handle clones, commits and checkpoints retain identity. Creating or
reopening a database, switching to another branch runtime, or replacing the
actual native database produces a different identity even with equal contents and
commit epochs. Selecting the already admitted branch or failing branch selection retains the
previous admitted runtime.
Identity access requires successful admission and a usable native store.

A token owns only a reference-counted marker. Retaining it prevents address
reuse from comparing equal while the token exists, without retaining database
files, locks, snapshot pins, governors or full store state. Equality exposes no
address. This is a small per-instance allocation, with reference-counted clones.

Published graph reads capture the marker with their exact committed snapshot.
Private staged-replacement fixtures verify that an earlier published view still
reports its original identity while the live database differs. Wrapped handles
do not expose arbitrary database replacement. Pinned projection
reads obtain identity under the same store guard as their native transaction.
Admission callbacks remain outside the publication mutex. This API binds the
returned view to its actual runtime; it adds no admission-to-runtime or governor
lifetime guarantee.

This token is not persistent filesystem/project/branch identity, a recovery
incarnation, a held fence or proof that no commit occurred. A current proof must
combine instance binding with complete data/schema comparison, a coherent epoch
window, retained source/target ownership and genuine durable drains. After
reopen, hosts must capture fresh proof; serialized tokens or paths/epochs cannot
restore it. The facade change alone does not qualify Mem primary/only modes or
change release/runtime defaults.
