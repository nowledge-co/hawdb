# Host-controlled lexical term admission

HawDB's embedded facade exposes `SearchLexicalTermPolicy` for out-of-core
generation builds and readers. It limits the UTF-8 bytes of an **analyzed term**,
including analyzer-generated aliases and compound terms, not the source document.
The default remains 4,096 bytes. The separate default 4 MiB document-source limit
is unchanged by this API.

The policy accepts a nonzero limit no larger than `u32::MAX`, the v1 encoded
string-length ceiling. This ceiling is not a promise that a term of that size
can be built: document, block, build-memory, spill, query, and output admission
still apply. Terms are never truncated, dropped, hashed, or split to satisfy
admission. The analyzer and v1 artifact representation are unchanged.

## Configuration and runtime changes

```rust
use hawdb::{
    SearchLexicalTermPolicy, SearchOutOfCoreGenerationWriter, SearchOutOfCoreReader,
};
use std::num::NonZeroU64;

fn example(root: &std::path::Path) -> hawdb::Result<()> {
    let policy = SearchLexicalTermPolicy::new(NonZeroU64::new(8192).unwrap())?;
    let mut writer = SearchOutOfCoreGenerationWriter::create(root, Default::default())?;

    // Documents may be pushed before or after a policy change. Analysis happens
    // during finish, which validates the complete staged corpus under one policy.
    writer.set_lexical_term_policy(policy);
    writer.finish()?;

    let mut reader = SearchOutOfCoreReader::open_with_term_policy(
        root,
        Default::default(),
        Default::default(),
        policy,
    )?;
    let larger = SearchLexicalTermPolicy::new(NonZeroU64::new(16384).unwrap())?;
    reader.set_lexical_term_policy(larger)?;
    Ok(())
}
```

Existing `create`, `open`, `open_with_config`, and
`open_with_config_and_analyzer` entrypoints retain the default policy. No required
field was added to existing public option structs. Use the policy-aware entrypoint
to open an artifact that already contains terms above the default; opening with
the default and then calling the setter cannot bypass initial admission.

Both writer and reader expose `lexical_term_policy()`. Their setters require
exclusive `&mut self` access: concurrent queries cannot observe a change halfway
through an operation. This is runtime reconfiguration, not interruption or
retroactive revocation of an operation. A host sharing a reader can synchronize
changes using its own lock; no process-global setting or environment variable is
used.

- **Writer:** the last policy selected before `finish` governs the entire build,
  including already-spooled documents and spill/merge decoding. A failed build
  discards the stage and does not replace the active generation.
- **Reader:** an update below the current generation's actual term requirement
  returns an error without changing the previous policy. Increasing the limit
  takes effect for subsequent query analysis and posting decoding.
- **Generation update:** `prepare_delta` inherits a value snapshot from its
  reader. Changing that reader after preparation does not change the prepared
  update's build policy. Configure the reader before preparing the update.

## Reader authority and resource boundaries

Opening checks every dictionary term and posting-block key bound against the
host's selected policy. A generously configured writer whose actual terms are
short does not require a generously configured reader. No artifact field grants
permission to raise a host limit; per-record posting decoder checks remain active
in addition to manifest admission.

Query terms are admitted as the analyzer emits them, with byte, distinct-count,
and retained-memory checks. Query stream admission uses actual validated block
extents and includes old/new decoded-vector capacity during block transitions,
retained term copies, and block-reference vector growth;
the default 32-term count boundary remains usable without increasing its budget.
Spill merges share one variable-size head budget, including their deduplication
head, checked before allocating strings. Merge fan-in adapts downward using the
largest actual spilled posting, while the configured fan-in remains its ceiling.
Both intermediate merges and final
artifact production use the same traversal. Fixed I/O buffers remain separately
bounded by merge fan-in, and output blocks by block admission. These accounting
bounds are not a claim of whole-process RSS accounting or bounded opaque analyzer
scratch; the broader document-streaming/resource-governor work remains separate.

## Local verification

Boundary regressions cover 4,095/4,096/4,097/5,202 bytes, configured-limit neighbors,
UTF-8 bytes, runtime changes, inherited update snapshots, complete-source hydration,
multi-run merge, failed publication, corrupt manifest requirements, cancellation,
and exact/one-short resource budgets. A facade regression imports the API through
`hawdb`, not an internal production integration path.

```sh
cargo test --locked -p hawdb-search term_policy
cargo test --locked -p hawdb embedded_facade_supports_dynamic_lexical_term_policy
bazel test //crates/search:hawdb_search_tests //crates/search:hawdb_search_term_policy_fuzz_tests
bazel test //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests //:hawdb_linux_ci_fuzz_smoke_test
```

The seeded lifecycle campaign is an explicit manual Bazel target, not a CI job.
This API work addresses #325's configuration gap. It does not establish #206's
full-corpus compression ratio or qualify the independent whole-document limit
changes tracked by #392.
