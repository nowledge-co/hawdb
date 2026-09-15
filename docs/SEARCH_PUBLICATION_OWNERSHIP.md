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
