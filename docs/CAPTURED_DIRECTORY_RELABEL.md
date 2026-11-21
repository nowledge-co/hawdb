# Captured same-parent directory relabel

The internal storage API addresses the directory promotion step required by
https://github.com/nowledge-co/hawdb/issues/1291. It does not implement the full
CapturedOpen constructor, grant a lease, or change ordinary pathname IO.

`SourceDirectory::prepare_directory_relabel` borrows the actual held parent and
original child; `prepare_shared_directory_relabel` retains existing Arc owners.
The fixed ASCII source/target names cannot change after preparation. The private
candidate has no path constructor, persisted representation, Clone, reset,
cleanup, or restoration-from-identity API. The root HawDB facade adds no export.

Before preparing, the caller admits and retains real memory funding covering
`directory_relabel_metadata_bound`; existing parent/child/anchor and recorder
engine retain their own funding. This estimate does not reserve memory. An
operation FD context admits temporary handles and grants no memory or namespace
permission. Caller must hold actual source namespace exclusion and retain its
original anchor through movement, uncertainty, and finish.

## Effect and evidence

`move_once` validates project/recorder domain, admits one temporary FD slot,
checks every target entry without following links, and opens the direct source
under the held parent. The opened identity must equal the still-held original
child. Early descriptor refusal leaves the same candidate unattempted and
retryable. Definite pre-effect binding/occupied-target refusal consumes its
fixed candidate without owning any new target.

Immediately before the native call it records Attempted. A native error or
unwind may follow an effect, so it permanently forbids another move and retains
original custody. A successful native reply records NativeMoved **inside** the
native closure, before recorder commit/After observation or any later probe can
fail. `native_move_completed` therefore survives a post-effect observer error
or unwind. No error rolls the filesystem back or authorizes deletion.

The native operation always uses the captured parent on both sides:

- Linux: `renameat2(..., RENAME_NOREPLACE)`.
- macOS: `renameatx_np(..., RENAME_EXCL)`, with a prior no-follow occupied-target
  check because native aliases can produce a successful no-op.
- Windows: admitted direct/no-reparse DELETE-capable source helper, verified
  against the original live child, then `NtSetInformationFile` class65 with the
  captured RootDirectory and `FILE_RENAME_POSIX_SEMANTICS` (0x2) alone;
  `FILE_RENAME_REPLACE_IF_EXISTS` (0x1) remains absent, so an existing target
  must refuse. This requested flag does not establish that an open descendant
  permits movement: native Windows qualification must demonstrate both held-anchor
  positive cases and all no-replace guards. The earlier flags0 implementation
  returned Access Denied in both held-anchor positives. The target-only probe
  reads `FileCaseSensitiveInformation` (class71) from the original held parent
  using its FILE_READ_ATTRIBUTES capability, validates the complete returned
  struct and known flag, then uses exact target lookup for a case-sensitive
  directory and OBJ_CASE_INSENSITIVE for a default insensitive directory.
  Unsupported/unavailable queries fail before effects with their original native
  error; unknown flags/short information fail validation. There is no inferred
  case policy or silent fallback. Ordinary exact component IO remains unchanged.
  Every original and helper handle shares DELETE.

Unsupported platforms refuse. There is no replacing, pathname, cross-volume
copy, close-original-and-retry, or uncounted-handle fallback. The caller excludes
source replacement during preflight/native entry. The OS supplies atomic target
exclusivity even when a target races the earlier probe.

The target probe closes before the source helper opens. The source helper closes
before post-move validation, which closes before parent sync. Peak extra admitted
FD capacity is one, while original parent/child/anchor remain held. Native OS
execution and descriptor measurements remain separate from source inspection
or cross-compilation.

## Finish and durability

`finish` accepts known native success and freshly verifies current source
absence and target identity against the original live child. An actually
observed binding mismatch is sticky: restoring the name does not revive that
candidate. Known NativeMoved evidence is retained. An admission/observation
failure that cannot establish a mismatch does not invent a binding fault.
Descriptor refusal and parent-sync error remain retryable with original custody.
Only successful retained-parent sync marks durability completed. No child sync,
root publication, source-owner release, or higher-level success is implied.

The caller may perform inter-call namespace changes under its own exclusion;
finish rechecks rather than trusting post-move validation. It must keep its
exclusion across successful validation and the actual parent barrier. Completion
records that barrier, not perpetual validity of the namespace afterward.
The evidence accessors are historical and monotone: after an earlier successful
finish, `durability_completed()` remains true if a later finish observes a
binding fault. That later call returns an error and acknowledges nothing; even
restoring the name cannot revive the faulty candidate. The historical accessor
never supplies current binding, publication, retirement or reuse authority.

## Recorder refinement and test ownership

`NativeDirectoryTrace` supplies captured logical parent and original child
inodes to `ImageEngine::rename_directory_new_at`. It rejects a wrong child or
any occupied target and commits both edge changes in one same-parent namespace
batch. Original-child trace IO continues through that inode after movement.
`opened_path` remains only an observation label; label reuse cannot redirect
native IO or the image transaction. Admission precedes the engine mutex, and
no SourceDirectory capture/open reenters that mutex during the native closure.

The existing storage unit/Bazel carrier owns native and recorder tests. Existing
native Windows failure-boundary selection includes the new test prefix, with
mandatory anchored success while original child and anchor stay open. A
move-or-refusal test or Windows cross-compile is not anchored-success evidence.
Native Windows tests separately require an actual default native alias refusal
with the original anchor held, and supported isolated per-directory NTFS
case-sensitive behavior with distinct-case occupied refusal then absent-target
success. They do not equate lexical ASCII case folding with native identity.
The case-sensitive fixture uses a native API only on its own empty directory;
unsupported OS/filesystem/permissions are setup failure, never a passing fallback.
Runtime Windows namespace durability still requires its separate platform
qualification; neither this API nor the finite model changes that boundary.

`HawDBCapturedDirectoryRelabel` is a separate bounded namespace/evidence model,
registered in the existing storage manifest. It includes eight initial rows,
two platforms, actual native effects versus replies, independent recorded image,
live owners, one helper slot and five finish cases. Thirteen producer controls
use fourteen designated oracles; five positive reachability witnesses separately
exercise normal success, FD/sync recovery, sticky restoration refusal and lost
reply custody. No fairness, liveness, state/depth truncation, history-granted
permission, or arbitrary external source-substitution guarantee is introduced.
Full CapturedOpen qualification remains a different model and gate.
