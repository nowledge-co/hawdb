# Branch namespace durability qualification

This records the bounded fix for #834 and component evidence for #820. It does
not qualify the entire branch runtime or certify a filesystem or storage device.

## Publication contract

Before acknowledging an immutable object, synchronize its contents, its final
name, and every newly required directory name through the project root. Writable
project admission establishes the root's ancestry through the existing mount.
Directory existence, including leftovers from an interrupted attempt, is not
proof of a completed barrier. Ancestor traversal opens one directory at a time
and charges that descriptor to the initiating project.

An immutable store establishes its ancestry on its first publication, rather
than on read-only open. It remembers only successful object-kind name barriers
(at most five kinds); each new object's contents and final name still require
their own barriers. Reopening or reclamation invalidates that knowledge. Reuse
of a complete object repeats the final directory barrier because an earlier
publisher may have failed after installing the bytes.

Child WAL/head publication establishes the containing ancestry before success.
An uncertain directory barrier retains the Creating receipt and complete files.
Pending recovery validates the exact head/WAL pair, synchronizes both files and
their directory ancestry, and only then publishes Ready. A recovery failure
retains the receipt and evidence for retry.

## Executable evidence

Use the repository's pinned Rust toolchain and locked dependencies:

```sh
cargo test --locked -p hawdb-storage --all-features
cargo test --locked -p hawdb --all-features --lib api::branch_lifecycle::tests::
```

Storage unit tests include the bounded image engine and a recorder of actual
native IO. The recorder attaches to an empty, already durable project root
before handles open. Writes, truncation, shared inode identity, directory
creation, links, rename, deletion, and successful file/directory barriers drive
the model. Capture compares its visible image with the native filesystem and
rejects unknown mutations. Image limits reject before native mutation. Later
handle closure cannot strengthen an already captured image.

Crash plans can lose uncovered operations, retain them in selected order, or
retain only selected byte ranges. Completed file barriers preserve bytes and
length; completed directory barriers preserve direct names. The tests check
new immutable-store ancestry, failed-barrier retry, uncertain-object reuse,
pending Creating recovery, and reuse of completed kind-name barriers without
omitting per-object barriers. Descriptor regressions exercise ancestry with a
one-descriptor limit and read-only-to-writable admission retry.

Removing the final reuse directory barrier or either pending-recovery file or
directory barrier makes the corresponding real recovery regression fail. These
negative controls must use isolated build directories so another worktree
cannot overwrite their test executables.

The existing lifecycle suite covers actual child creation/admission, nested
schema/data branches, pending recovery, checkpoint, logical deletion and GC.
Those ordinary reopen tests complement the component crash images; they do not
alone demonstrate power-loss safety.

## Platform assumptions and limits

The model assumes completed POSIX synchronization is honored, same-directory
rename is atomic, and the existing mount is durable. Host/controller/drive
behavior outside those assumptions requires physical-device qualification.
The existing durable anchor and mount are outside capture. The facade matrix
also observes descendant project installation and its newly created parents
inside that anchor. Cross-target compilation is not execution evidence.

Windows uses atomic file replacement followed by a write-capable publication
handle flush and a counted parent-directory flush request. Directory handles are
opened with `FILE_FLAG_BACKUP_SEMANTICS` and write access; missing paths, resource
exhaustion, permissions and unsupported filesystem operations propagate rather
than becoming successful no-ops. Writable project admission requests the ancestor
barriers one handle at a time and caches only a completed ancestry walk.

[FlushFileBuffers](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers)
requires `GENERIC_WRITE`. Its file/volume documentation does not establish a
portable POSIX directory-fsync contract. A successful directory flush request or
passing reopen test therefore does not qualify newly created ancestor names.
The host may need write access to ancestor directories; failure must not silently
relax the durable default. The recorder rejects Windows attachment.
Windows namespace power-loss qualification remains open; do not infer it from
the Unix model. The [facade matrix](BRANCH_POWER_LOSS_QUALIFICATION.md) adds ordinary runtime
bootstrap, seal/rotation, create/delete, admission and residency/index evidence.
Fine-grained concurrent GC and independent job-root schedules remain #820/#819
obligations.

The default `SyncOnEveryWrite` policy and existing persistent formats are
unchanged. Relaxed DDL/DML never relaxes branch metadata publication barriers.
