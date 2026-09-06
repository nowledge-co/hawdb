# Generation delta input admission

Out-of-core generation updates now create one `BuildMemory` before converting
their input rows. The converted delta, writer startup, source hydration and
writer-input/spool phases all use that root's existing three accounts. A private
writer constructor accepts the existing root; no public memory or output API is
added. The caller's original allocation predates API entry: this change admits
its operation ownership before conversion, not the caller's earlier allocation.

## Conversion envelope

`delta_memory::Input` reserves from the existing input account before allocating
the destination document vector, constructing IDs or inserting metadata. The
envelope includes:

- original upsert/delete vector capacities, including unused slots;
- owned string, embedding and metadata capacities of every input row/delete ID;
- exact destination document-vector slots while the source row vector is live;
- the new kind-prefixed ID, metadata keys and kind value, plus conservative
  metadata insertion/container allowances, including replacement overlap.

Row/document field sizing shares the existing build capacity contract. The
public `into_document` method retains its signature and content semantics, but
allocates the ID at exact capacity and moves `external_id` into metadata instead
of cloning it. Title, body, embedding, source ID and the existing metadata map
continue to move. All projection kinds and existing-key replacement semantics
are preserved; no persisted v1 bytes change.

The configured `max_delta_working_bytes` now bounds this conversion envelope,
not only logical field lengths. It applies independently of the task's shared
root limit. Spare capacity and conversion overlap can therefore reject inputs
that the old logical estimate accepted. Arithmetic/address-space checks and
cancellation checkpoints precede allocation. These are capacity envelopes with
conservative B-tree allowances, not allocator or process RSS measurements.

## Retained and consumed ownership

The original input and its lease are held together while converting. Both the
source row slots and destination slots remain charged until the source vector
is dropped. Actual output capacities are checked against the envelope before
releasing unused admission. Document/delete vectors are sorted, validated for
duplicate/conflicting IDs, then reversed so popping consumes ascending IDs
without another queue allocation.

When an upsert enters the writer callback, the delta retains its payload charge
until the callback returns. The writer admits that document on the same root;
their conservative overlap is intentional. Consumed delete strings drop before
their charges shrink. Vector slot charges remain until the owning vectors drop,
even after their last element is consumed. Fields declare data before leases.
The delta input owner is dropped before a prepared update returns; no consumed
input charge is held across the later `finish` call.

Input-conversion and writer-startup memory denial, invalid IDs and conversion
cancellation happen before staging.
Failures during source scanning or writer input preserve the published generation
and remove the operation's staging directory. Existing base-generation and final
publication guards remain authoritative.

## Verification

Nine normal regressions cover an independent exact/one-short conversion formula,
root and component denial before conversion, spare vector/string/embedding
capacity, moved payload pointers, all kinds/metadata replacement, sorted consume
order, shared callback overlap, retained vector slots after memory-handle drop,
cancellation, duplicate/conflicting IDs and consumer errors. Published-reader
regressions prove shared writer-startup denial before staging/source I/O and
publication/staging preservation for duplicate, cancellation and writer failures.
A successful update is compared with an independent document-map oracle through
finish and reopen, including missing deletes, replacement and new IDs.

The manual seed `0x206de17a` campaign covers 128 generated input cases, each with
exact/one-short root boundaries and a competing owner. Every successful input
is consumed in order and releases all charges without creating per-record
accounts. The short attempt uses a clone and the exact attempt the original
spare-capacity input; each limit is computed from that attempt's actual input.
Independent normal tests validate the capacity formula; the campaign's document
oracle is independent of conversion and merge code. Another 32 cases publish
updates and compare full reopened document sets with a map-based upsert/delete
oracle, while asserting the old manifest remains unchanged until finish.

```bash
cargo test -p skein-search --all-features delta_memory -- --nocapture
cargo test -p skein-search --no-default-features delta_memory -- --nocapture
cargo test -p skein-search --all-features delta_admission_campaign \
  -- --ignored --nocapture
bazel test --nocache_test_results \
  //crates/search:skein_search_tests \
  //crates/vector-projection:skein_vector_projection_tests \
  //crates/fuzz:skein_fuzz_tests \
  //crates/fuzz:skein_fuzz_cli_tests \
  //:skein_linux_ci_fuzz_smoke_test
```

The delta fuzz target is manual and part of the explicit local suite, not a new
default or dedicated CI job. Native-platform qualification remains separate.

Nine negative controls remove initial admission, count row length instead of
capacity, omit conversion growth or retained vector slots, bypass the component
cap, release source charge before the callback, omit order reversal, clone the
external-ID payload, or create an independent writer root. Each must compile and
fail assertions, then be restored before complete positive verification.

## Remaining #206 boundaries

This covers generation-update inputs, not every resident/persistent lexical delta
map. Build options/identity copies and returned reports are also not newly owned.
Shared-root projection backend admission and retained public output still need
the pending public-contract decision. Parsed filter/ACL inputs, reports,
matched-span/tokenizer scratch, combined component limits, reader/mapping host
ownership, Jieba workspace, representative-corpus reduction and exact-head native
qualification remain. No public API, v1 artifact, dependency, I/O backend, Bazel
runtime/timeout configuration or release-policy change is included.
