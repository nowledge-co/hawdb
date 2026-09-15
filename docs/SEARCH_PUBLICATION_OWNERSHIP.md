# Generation discovery and publication ownership

This private stage of #392 extends the writer's existing operation ledger through
active-generation discovery, lexical recovery scanning and outer publication.
It follows PR510 and includes PR512's checkpoint test synchronization prerequisite.
Public query APIs, logical limits, defaults and persisted encodings are unchanged.

## Admission and lifetime

- Borrowed JSON envelopes measure canonical body/envelope checksums and output
  lengths without cloning the body. The total published-byte limit is checked
  before allocating either layout or outer manifest output. Each output retains
  its exact capacity lease until its bytes are freed.
- Generated names, joined/temporary paths and native path-conversion scratch use
  the same ledger. Checksums, fallback copies and file reads admit an 8 KiB
  transfer buffer; reads admit the opened handle's complete measured length and
  reject growth or truncation instead of growing their vector.
- Manifest decoding preflights raw strings, the largest escaped token, fixed
  diagnostic scratch and schema-specific vector slots. Pinned serde/Rust vector
  growth and replacement overlap are included. Full checksum and schema checks
  remain authoritative; a filename alone never proves a recovery generation.
- Recovery visits directory entries serially. ReadDir/native enumeration storage,
  entry names, paths, file bytes and decode scratch overlap in the ledger. The
  conservative enumeration allowance includes the one-MiB buffer cap used by
  [glibc's Linux directory implementation](https://github.com/bminor/glibc/blob/master/sysdeps/unix/sysv/linux/opendir.c).
  Unix record extents and Windows fixed directory records bound name admission.
- Unix publishers register directory device/inode identity before opening their
  lock file. Each linked-list node owns its own charge and is freed on removal;
  an empty registry retains no container capacity. This preserves exclusion for
  process-owned native record locks and recognizes symlink aliases. Windows uses
  the native exclusive handle lock; [LockFileEx](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-lockfileex)
  excludes overlapping exclusive locks, including separately opened handles.

## Consumer control-record admission

The consumer binding check remains inside the same publication lease. Its joined
snapshot path, native open scratch, 8 KiB input buffer, reusable 1025-byte control
line and 27-byte prefix use the existing operation ledger. Compressed probes also
admit their 8 KiB decoded buffer and the same pinned frame-aware zstd owner used
by delta hydration. Native context and window capacity are admitted before native
allocation or growth and retain their leases until the decoder drops.

The probe preserves the 1024-byte control-line and 4096-byte envelope limits. It
reads only the leading binding records, including concatenated/skippable frames;
it does not verify the rest of a snapshot. Delta hydration still separately
requires complete range, envelope and payload integrity. Cancellation checkpoints
surround control reads and the shared decoder's controlled loops. An opaque read
or native call already in progress is not preempted.

Missing snapshots remain eligible for ordinary publication. A registered binding,
malformed header, cancellation or admission denial rejects ordinary publication
and releases the lease. No active snapshot is rewritten by this inspection.
Requested Rust allocation probes and exact/one-short limits qualify this path;
native admission uses the shared decoder's context/frame tests and version bound.
This neither introduces shared host admission nor claims a process RSS ceiling.

## Failure and commit boundary

Cancellation checkpoints surround discovery/decode validation and occur between
bounded checksum/copy/write chunks. Native calls and serde's in-memory parse are
synchronous; this stage does not promise interruption inside those calls.
Memory rejection and cancellation propagate from discovery and cannot become a
corrupt-manifest fallback or a silently skipped lexical candidate.

Temporary-file cleanup scratch is reserved before file creation. Failure or
unwind therefore needs no fresh admission, even when another account fills the
root. Cleanup remains best effort on filesystem errors. The active manifest is
replaced last, after artifact verification. No cancellation checkpoint follows a
successful durable replacement: committed data must not be reported as an
unpublished cancellation. Existing storage behavior for an error after rename
but before successful directory sync is unchanged.

## Qualification and remaining work

Regression fixtures cover exact/one-short admission, held competing accounts,
forced copy fallback, cancellation around replacement, full-root unwind cleanup,
complete recovery validation, malformed/escaped/sequence JSON, directory aliases,
concurrent publishers, failed lock acquisition and independent node lifetimes.
Serialized allocator probes measure requested Rust live capacity and conservative
replacement overlap after fixed ledger metadata initialization. They do not measure
allocator overhead, libc allocations, kernel resources or process RSS; native
scratch uses the audited platform bounds above.

Independent baseline artifacts and isolated negative mutations supplement the
ordinary Cargo and unchanged default Bazel/local-fuzz checks. Qualification logs
and source receipts are retained under `/tmp/skein-392-publication` on the
validation host. This document does not replace those execution receipts.

The approved context constructors remain private. Post-commit generation cleanup
still owns an independent pending queue/report, and stage-directory cleanup,
delta input/hydration and final facade lifetime require follow-up qualification.
The shared cleanup API also serves readers and ordinary checkpoints; moving it
under writer admission must preserve retry reporting and the commit fence.
This PR neither completes #392 nor removes the 4 MiB source guard, establishes
shared host admission/adaptive indexing, or qualifies the original #206 corpus.
