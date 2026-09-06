# Out-of-core hydration admission

Hydration's raw payload, decoded text, document containers and selected payloads
now retain memory charges throughout their internal operation lifetime. The
existing v1 document codec, checksums, descriptor validation and positioned I/O
are reused. This does not introduce a storage backend, public output API or a
new memory account per document.

## Three callers, one decoding path

- Query hit hydration uses the query's existing working account. The admitted
  candidate page remains live during hydration; no independent nested ledger
  substitutes for the outer query limit.
- Standalone `hydrate_documents` creates the existing default query root for
  its own operation. It returns the same plain public document vector. This
  internal limit is additional to the configured logical hydration and segment
  size limits: a segment fitting a component limit can still exceed the root.
- Delta generation visits source segments under the writer's existing retained
  build account. The source lease stays live while the callback admits a row on
  that same build root. Source and writer-input charges conservatively overlap
  during the callback; consumed payload charges shrink only after it returns.

The query still has two operation accounts and the writer has three. Sharing
source ownership does not admit the delta's separate upsert/delete containers.

## Allocation and lifetime boundaries

Request records borrow IDs and descriptor references. Their exact vector slots
are reserved before allocation, then sorted by ID to reject duplicates and
group segment reads. The caller's ordinal is retained in each record, so the
final document vector can restore request order in place without an ID map.
Destination document slots and one payload-lease slot per source segment are
reserved before allocating either destination vector.

`query_io::read` admits the raw range before allocation and acquires I/O-wave
capacity for the positioned read. `query_io::decode` admits the decompressed
text and the existing fixed 1 MiB, pinned-zstd-1.5.7 decoder envelope before
creating native state. Raw bytes and text remain charged while records decode.
The native decoder workspace is released before document decoding begins.

Record preflight shares the builder's allocation-free wire-field parser and
capacity contract: document slots, decoded string bytes, embedding capacity and
conservative metadata-container allowances. Count and address-space overflow,
header and record shape are checked before document-vector allocation. Decode
uses the existing document decoder; descriptor bounds and strict ID ordering
are validated before returning a segment. Actual retained capacities are checked
against the preflight envelope and unused slack is released. Checkpoints bracket
decoding and run through records, embedding counts and metadata preflight.

Selection moves payloads, rather than cloning decoded documents. The originating
segment lease is attached to the destination owner before the first payload
moves. After the source vector and unselected documents drop, that lease shrinks
to the selected payload bytes. Destination vector slots have their own lease.
The owner declares document data before leases so cancellation, missing IDs,
later-segment corruption and other errors drop payloads before releasing charges.

These are checked requested-capacity envelopes, not allocator or process RSS
measurements. Logical hydration-byte metrics keep their existing meaning and
are not substituted for capacity admission.

## Verification

Normal regressions cover independent exact/one-short capacity arithmetic with
competing owners, pre-allocation rejection, actual vector capacity and moved-ID
pointer identity, ownership after reader/query drop, caller order, duplicate and
missing IDs, logical limits, invalid header/count/records, later corruption and
cancellation after a selected payload moves. Published-reader tests cover direct
hydration and actual query-root denial after vector scoring. Delta tests cover
shared source/input lifetime and low-budget rejection without publication or
staging-directory changes. The ten non-vector tests also execute without default
features; the real vector-query integration requires `vector-search`.

The manual `0x206d0c51` campaign generates 128 record cases and 48 published-reader
cases. It varies Unicode/NUL/empty content, optional embeddings, metadata fields,
segment counts, selected subsets and request order. Original input documents are
the decode/selection oracle. Every case retries exact/one-short root budgets with
a competing owner and checks final release/account counts. Public direct lookup
must preserve the same documents and order. This is record/ownership coverage,
not BM25 or approximate-recall qualification.

Negative controls remove decoded/request/destination charges, drop retained
payload ownership, clone selected payloads, substitute independent query/build
roots, or omit caller-order restoration. Each must compile and fail assertions;
all controls must be restored before complete positive verification.

```bash
cargo test -p skein-search --all-features hydration_memory -- --nocapture
cargo test -p skein-search --no-default-features hydration_memory -- --nocapture
cargo test -p skein-search --all-features hydration_admission_campaign \
  -- --ignored --nocapture
bazel test --nocache_test_results \
  //crates/search:skein_search_tests \
  //crates/vector-projection:skein_vector_projection_tests \
  //crates/fuzz:skein_fuzz_tests \
  //crates/fuzz:skein_fuzz_cli_tests \
  //:skein_linux_ci_fuzz_smoke_test
```

The dedicated hydration fuzz target is manual and included in the explicit local
fuzz suite. No default or dedicated fuzz CI job is added.

## Remaining full-issue boundaries

`Documents::into_unowned_output` explicitly ends internal ownership at the
existing plain-vector API boundary. It is not retained public-output coverage.
Public hits, report copies, matched-span/tokenizer scratch and parsed filter/ACL
inputs still need ownership. Shared-root projection backend admission and retained
projection hits remain behind the pending public-contract decision. Combined
component limits, delta inputs, published-reader host/mapping ownership, Jieba
workspace, representative-corpus reduction and exact-head native qualification
remain separate #206 gates. No public signature, v1 artifact, dependency, backend,
Bazel runtime/timeout setting or release-policy change is made here.
