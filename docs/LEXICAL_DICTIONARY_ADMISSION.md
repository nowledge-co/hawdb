# Lexical dictionary working-set admission

The final artifact merge passes its generation `BuildMemory` through term
grouping, dictionary staging, FST encoding and validation. These owners share
the existing three accounts with input, analyzer, merge and artifact state.
There is no independent copy of the task budget or per-key ledger account.

## Ownership boundaries

- `dictionary_memory::Term` reserves bytes before cloning a grouping key. The
  key and its lease move together into the writer; staging neither clones it
  again nor releases its charge at the call boundary. String data drops before
  its lease. The merge cursor's source key remains independently admitted.
- Staging slots grow from four, doubling up to 1,024 entries. Both old and new
  arrays are admitted until reallocation completes. Reused capacity remains
  charged between flushes, then drops before copying the completed dictionary.
- An incoming term stays charged while a full or component-limited partition
  flushes. All original staging keys and slots remain charged during recursive
  subdivision; splitting does not clone the input or release its live owners.
- Before entering the pinned FST builder, the writer reserves its complete
  conservative working set alongside those owners. A returned `Encoded` keeps
  the output capacity charged after the builder and registry have dropped.
- Validation reserves scratch alongside that retained output, staging and
  earlier directories. Its scratch lease ends only after both checked-node
  validation and ordered-key iteration finish. The encoded-buffer owner remains
  live through descriptor admission, endpoint cloning and the spill write.
- The shared directory owner already carries descriptors through the artifact
  summary and manifest handoff. Successful dictionary finish releases staging
  and encoded buffers, not the returned descriptors' accounting.

## Pinned dependency envelopes

For `fst` 0.4.7 on 32/64-bit targets, builder admission includes 20,000 registry
cells at 64 bytes, an 8 KiB initial unfinished-stack allowance, 512 bytes per
input key byte, and twice the configured output capacity. The variable allowance
covers unfinished-node and transition-vector growth, retained registry clones,
previous-key copies, and old/replacement allocation overlap. Total inserted trie
transitions are bounded by the sum of input key lengths. This is conservative
requested-capacity accounting, not allocator overhead or RSS measurement; recheck
the dependency implementation and layout on upgrades.

Checked-node validation reserves its summary and address arrays from the FST
extent. The subsequent ordered stream separately needs a growing byte key and
node-state stack. Its allowance uses 128 bytes per frame and 3x old/replacement
growth, including initial capacities. Validated edges point backwards, bounding
stream depth by both the key limit and serialized FST bytes. Node arrays drop
before streaming, so reserve the maximum of these two disjoint scratch phases,
not their sum. Builder and reader use the same configured validation limits;
successful publication must not require looser reader admission afterward.

## Errors and verification

Component/layout rejection can recursively split a bounded partition. Root
admission failure and cancellation are terminal: they cannot be interpreted as
poor compression and retried with smaller partitions. A failed `push` poisons
the writer; a later retry or `finish` cannot publish its partial prefix.
Normal RAII cleanup drops staging/output leases and the temporary file on error.
The existing manifest-last publication boundary remains unchanged.

Normal tests cover before-clone and before-builder rejection, old/new slot
overlap, retained output during validation, incoming-term overlap, component
versus root error classification, failed-writer retries, directory failure,
cancellation, recursive partitions, 1,025-key auto-flush/reuse, and actual
generation denial preserving the old manifest and artifact.
The complete fused-build regression also checks its observed exact peak and
one-short budget. Its preceding 1 MiB success fixture now correctly rejects:
that limit never included the FST registry's working set. Failure must leave no
published manifest or staging directory and release all tracked capacity.

The manual local `skein_search_lexical_dictionary_memory_fuzz_tests` campaign
uses seed `0x206fc7` and 2,000 groups. It compares every stored key and full-width
metadata record with an independent ordered input map, varying Unicode/NUL
keys, shared prefixes and block limits. Each group has exact and one-short root
budget retries, with complete release and exactly three accounts. Sixty-three
cancelled finishes must not enter another build or return output. Existing
dictionary-byte fuzz independently covers mutated encodings and checksums.
These campaigns are ignored by ordinary tests and explicitly included in local
fuzz only; no fuzz CI or Bazel configuration override is introduced.

Negative controls must detect omitted key admission, early encoded-buffer lease
release, missing validation scratch, and release of old staging slots before
reallocation. Restore every control before final positive verification.

## Remaining issue 206 boundaries

Segment descriptors, codec buffers, layouts and publication now share this root
as described in `SEGMENT_BUILD_ADMISSION.md`.
This does not establish complete build/query memory admission. Small path/control
allocations, dependency workspace, published-reader reopen
and persistent outer query/delta owners still need their respective boundaries.
Stack scratch and allocator overhead are not included in these ledger counters.
The generation RaBitQ sink follows `RABITQ_BUILD_ADMISSION.md`.
Jieba's private persistent HMM workspace remains a separate pending dependency
maintenance decision; no patch, HMM change or helper thread is included here.

The v1 encoding is unchanged; more conservative component admission can split a
partition earlier. There is no migration, public API, new backend or io_uring.
Native exact-head verification and measured representative-corpus posting-size
acceptance remain required before the full #206 PR, then #291 and #292.
