# Actual generation input-file ownership

This fixes the primary generation writer's input source, including single and
partitioned preparation. It does not complete stationary generation construction,
producer closure for auxiliary temporary files, serving activation or MVCC.

## Observed failure and runtime refinement

At main `885f47d0`, the producer flushed/synchronized/closed its spool, checked length by
path, and later reopened the label in SpoolSource::cursor. A same-length valid
replacement could therefore become input. Two synthetic native reproductions
failed on the related Draft tree `93fe514` before its fix: the single build returned success and its normal immutable
reader hydrated foreign bodies while retaining source epoch 7; the partitioned
build refused a digest mismatch. Original source, tests, executable and terminal
logs are retained separately. See issue #1103. This independent main repair adapts
the producer transfer from `7e99647f` without importing the Draft publication API.

The writer now creates its spool read/write, flushes and synchronizes that actual
file, consumes the BufWriter, and transfers the same File into an opaque HeldSpool.
The SourceFile allocation and shared metadata are admitted to the existing
operation account before ownership publication. No saved path, hash equality,
new governor or new task can construct the live source authority.

input_source requires that held owner. Both primary and partitioned preparation
use it; missing custody is an error rather than a path fallback. Positioned
cursors and records share the original file without try_clone or another open.
Each cursor checks its actual length and header, uses its own offsets, and retains
normal frame checksum, syntax, ordering and bounded-header validation. Test-only
owned-document scanning also reads through a positioned range when custody is
present. Byte-identical labels or self-consistent replacement frames cannot
replace the original producer file.

The threat boundary is namespace replacement, including rename or
unlink-and-recreate. An independent writer that overwrites the same underlying
file in place is outside the private-stage ownership contract. Length checks and
frame CRCs detect ordinary corruption; they cannot authenticate input against a
writer that preserves the length and recomputes those CRCs. The retained handle
does not make the original file immutable or establish cryptographic provenance.

After all artifact builders/partition cursors finish, the writer releases input
custody before publishing the built artifacts. Shared record/body references
retain the actual file until their own final close. SourceFile declares the file
before its memory lease, and native File closes its descriptor before refunding
capacity. This avoids retaining an unnecessary input FD during publication and
closes input before explicit stage cleanup, including partitioned publication.
The source's disk bytes remain disposable private-stage data, not a serving view.

## Bounded model and native coverage

HawDBHeldGenerationSpool bounds two record positions and two reader/reference
identities. It models sealing, a valid foreign replacement label, inherited
reader custody, independent positioned reads, source/reader closure, funding and
publication of completed artifacts. The model's built phase is the internal
prepublication boundary; it does not require a public prepared-candidate API.
Current-head execution evidence belongs in the PR. The intended counterexamples are:

| Control | Required violation |
| --- | --- |
| ReopenLabel | ActualProducer |
| ForgeMissingOwner | OwnerRequired |
| SharedOffset | IndependentPositions |
| CloseBeforeReaders | ReadersKeepNativeFile |
| RefundBeforeClose | NativeCloseBeforeRefund |
| FreshAccount | OriginalMemory |
| HoldAfterBuild | PreparedReleasesInput |

The model assumes original private producer ownership and no cooperating writes
after sealing. It does not authenticate arbitrary external in-place rewrites,
model all record/reference counts, prove unbounded progress, or qualify physical
I/O/memory ceilings. Native tests separately exercise same-inode length/checksum
corruption, retained records after cursor/source closure, and two simultaneous
cursors at an exact one-FD project cap (the single source file).
The normal immutable reader compares complete output documents in both fixed
same-length replacement scenarios, rather than trusting a report digest alone.

Existing corruption tests retain their rejection and old-selector assertions.
Extent changes now fail earlier at the held-length fence. Syntax fixtures retain
the original encoded byte length so malformed fields and duplicate ordering still
reach decoder validation after the first valid row. This preserves that coverage
instead of allowing the stronger length check to conceal the intended parser test.
Terminal broad writer, lint, Bazel and native platform results must be recorded
separately; defined or running tests do not constitute passing evidence.

## Remaining integration

Compaction auxiliary SpoolSource constructors remain explicitly legacy path
sources under the remaining producer-custody work in #231/#232. Generation artifact builders/installers also retain legacy
path operations. These must be migrated through original native namespace and
producer ownership before switching the Consumer facade to stationary operation.
The input-file fix is not target-root birth, a prebuilt serving view, a selector
receipt, a complete C..E mapping/binding/vector proof, or Mem readiness. Original
source snapshots, cooperating exclusion and source validation remain necessary.
No persistent codec, default durability or Mem stable/GA policy changes here.
