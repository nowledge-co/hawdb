# Lexical artifact working-set admission

The lexical artifact builder shares the generation operation's `BuildMemory`
root. It retains the existing three accounts; document rows and directory
entries do not create their own account metadata. This extends input/analyzer
admission without changing public APIs, dependencies, the analyzer digest or
the greenfield v1 layout.

## Ownership through publication

- The 8 KiB buffered file writer is admitted before creating the temporary file.
- Document IDs are borrowed at the staging boundary. Their retained copies are
  admitted before cloning, and vector slots remain charged between block flushes.
  Growth admits both old and replacement arrays until allocation completes.
- A document block preflights its exact header/record bytes plus both directory
  key copies before allocating. Original IDs/slots remain admitted during
  encoding. Each row still checks cancellation. Failed encoding does not clear
  the original staging state or publish a partial generation.
- A successful flush releases ID ownership but retains reusable slots. Finishing
  the complete document section releases those slots before external merge.
- Document and dictionary directories share both the existing component limit
  and the operation root. Directory growth admits old/new slot overlap. Dictionary
  endpoint keys are admitted before cloning, and a populated/admitted directory
  cannot be rebound to another memory account.
- The directory budget holder outlives both vectors: it moves into the artifact
  summary and remains there while the vectors move into the manifest. It is not
  released at the builder-to-manifest boundary. Struct fields place owned data
  before leases, and fallible paths preserve this ordering.
- Manifest serialization retains an admitted output owner through the final file
  write. Buffer growth admits replacement capacity while retaining the old lease.
  Checksums and serialization still borrow the directory rather than cloning it.
- The build's final artifact checksum uses an admitted buffer, sized to the file
  length up to the existing 1 MiB maximum (at least one byte). Small artifacts no
  longer allocate an unconditional 1 MiB checksum buffer. This does not change
  digest bytes or remove cancellation checks.

Budget errors occur before the corresponding requested allocation and before the
manifest-last publication gate. Existing source-document and analyzer leases
remain live alongside these artifact owners. A component limit is not relabeled
as an independent full copy of the task reservation.

## Verification

Normal regressions cover writer admission before file creation, ID cloning,
input/payload/key overlap, old/new array growth, shared document/dictionary
directory exhaustion, attempted budget rebinding, directory handoff lifetime,
manifest growth and checksum scratch. Publication failure tests preserve the old
manifest, remove the new temporary artifact and return tracked usage to zero.

The manual local `skein_search_lexical_artifact_fuzz_tests` campaign uses seed
`0x206a47` and 12,000 input groups. Production document-map encoding is compared
with an independent direct v1 encoder over Unicode/NUL IDs and full-width u32
lengths. The groups exercise repeated block flush/reuse, exact and one-short
operation budgets, cancellation and complete lease release. It remains ignored
in ordinary test runs and is included only in explicit local fuzz verification.

Negative controls must reject missing payload admission and early directory
lease release. They are restored before the final positive verification.

## Remaining boundaries

This is not complete build/query memory admission or an allocator/RSS bound.
External run readers, merge heaps/frames and doclist encoding buffers now share
the operation root as described in `LEXICAL_MERGE_ADMISSION.md`. Grouping-term
and dictionary staging/FST lifetimes follow `LEXICAL_DICTIONARY_ADMISSION.md`.
Segment codecs, descriptors and publication follow `SEGMENT_BUILD_ADMISSION.md`.
RaBitQ still needs its complete simultaneous working set connected to the root.
Small control/path
allocations and ledger bookkeeping are not allocator-instrumented by these
requested-capacity counters. Reopening the published reader and outer query/delta
state also need their own correct persistent ownership/admission boundary.

Jieba dependency workspace/TLS admission remains a separate pending maintenance
decision. No HMM disabling, input splitting, helper threads, dependency patch or
Bazel runtime/timeout override is part of this artifact follow-up. Only the manual
fuzz target and explicit local suite were extended. Native exact-head
verification and representative-corpus posting-size acceptance remain required
before completing #206.
