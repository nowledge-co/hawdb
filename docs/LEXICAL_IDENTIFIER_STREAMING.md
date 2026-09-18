# Incremental identifier analysis

Related issue: [#392](https://github.com/nowledge-co/hawdb/issues/392).

The field visitor previously materialized every identifier part and expanded token
before presenting the first identifier token to admission. An oversized camel-case
identifier therefore allocated its complete expansion before the existing term
limit rejected it.

## Implementation and semantic boundaries

`IdentifierParts` yields borrowed source slices using one lookahead character. A
cloned cursor can replay boundaries for cross-word phrases and adjacent pairs.
Parts are emitted before pairs, preserving the historical token order without an
owned vector of all parts. Part normalization remains character-by-character:
context-sensitive string lowercasing would change Greek final-sigma behavior.
Raw-token normalization retains its existing string-lowercase behavior.

One private event generator supplies both fallible document analysis and collected
query tokens. Unchanged raw terms, parts, Jieba words and CJK n-grams borrow source
spans. N-grams retain three byte offsets instead of a full `Vec<char>` run. The
Jieba algorithm, Han filtering, suffix rules, aliases and stopwords are unchanged.

The fallible adapter deduplicates within each phrase or identifier and propagates
the first callback error immediately. Its occurrence tags retain field-wide
phrase uniqueness and repeated occurrences across identifiers. The query
collector uses existing token IDs plus temporary last-identifier markers to
implement those same scopes without a second identifier hash table. It releases
the markers before materializing output order. Standalone identifier collection
uses the same generator without applying field splitting.

This is a private allocation and lifetime improvement. Public APIs, query
semantics, term/source policies and defaults, spill records and published formats
are unchanged. Owned source strings, transformed individual terms, admitted
deduplication keys and opaque Jieba token-vector/scratch allocations remain
resident floors. The change does not provide a whole-process memory bound, a
streaming-input API or host memory governance; the existing 4 MiB default source
ceiling and full-corpus verification requirements remain in force.

## Evidence

The public generation-writer regression submits one `alphaBeta` identifier
repeated 1,024 or 16,384 times with an empty analyzer lexicon. Both implementations
reject the same oversized term at the existing 4,096-byte limit. The candidate
also verifies that no out-of-core manifest is published.

| Source bytes | Original requested bytes | Candidate requested bytes |
| ---: | ---: | ---: |
| 9,216 | 257,398 | 85,712 |
| 147,456 | 3,590,435 | 853,629 |

These are thread-local Rust allocator requests during `finish()`, including
reallocations, not peak live memory or process RSS. The regression bounds the
increase in requested bytes relative to source growth; original production code
fails it. A separate borrowed-cursor test verifies zero allocation while comparing
all boundaries with the frozen splitter for large ASCII, camel-case/digit and
Unicode inputs.

The unchanged release `search_tokenization` benchmark supplies 400,000 CJK
characters (1,200,000 bytes) to an empty in-memory index. Three alternating pairs
ran sequentially after compilation and other local tests terminated. Each sample
used a fresh process, including Jieba initialization. No workload override was
set. Original code was main `366828ec4133d10aed3b300d93cd2e04bfde5b74`;
both binaries used Rust 1.97.1 and the same release profile/features.

| Pair | Original query ms | Candidate query ms | Original peak RSS KiB | Candidate peak RSS KiB |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 1,078 | 735 | 176,700 | 156,240 |
| 2 | 941 | 742 | 176,904 | 158,244 |
| 3 | 999 | 764 | 177,148 | 157,672 |
| Median | 999 | 742 | 176,904 | 157,672 |

Query timing comes from the existing benchmark; peak RSS comes from Linux
`wait4` child resource usage. The median decreases are about 26% and 11%,
respectively. This is one host and one large-query workload, not an ingestion
throughput result or a universal speedup. The benchmark only asserts empty hits;
semantic correctness is covered separately by the frozen-reference tests.
[Raw samples and build identities](LEXICAL_IDENTIFIER_STREAMING_LINUX.json)
include wall/CPU usage and executable hashes.

## Verification

The all-feature owner suite passes 403 library and four integration tests,
including explicitly selected local campaigns. Strict all-target/all-feature
Clippy, formatting and whitespace checks pass. Reference comparisons cover
complete token sequences, every fallible event prefix, Unicode normalization,
phrase/identifier uniqueness, field weights, TF/DF, document lengths, BM25 scores,
persisted artifacts, spills, mini-delta updates, reopen and failed publication.

Six temporary fault controls independently break identifier deduplication in the
collector, phrase uniqueness in the collector, fallible event deduplication,
Unicode part normalization, callback error propagation and n-gram order. Every
variant produces real assertion failures. Source bytes are restored after each
variant; all 982 source/build hashes match the measured revision. The restored
13-test analysis suite and four integration tests pass again.

Default Bazel search profiles, root/ACL/recovery and required local fuzz pass
94 of 95 targets in the full run; graph residency times out at 300.1 seconds.
After that process exits, the unchanged failed target passes in 203.0 seconds
under its original 300-second limit (32 seeds and 1,984 state checks). All 95
selected targets have passing evidence across the two runs; the initial full run
is not clean. The new semantic regressions also pass in the default, ACL and
text-only search profiles. Initial dependency loading fails before tests because
an external `rules_shell` package lacks its BUILD file; the previously authorized
same-target `bazel fetch --force` recovery succeeds. No configuration, workload
or timeout changes are made.

```sh
cargo test -p hawdb-search --all-features --lib --tests -- --include-ignored
cargo clippy -p hawdb-search --all-features --all-targets -- -D warnings
bazel test //crates/search:presubmit_tests //:hawdb_unit_tests //:hawdb_acl_capability_tests //:hawdb_storage_crash_recovery_tests //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests //:hawdb_linux_ci_fuzz_smoke_test
```

The allocation regression is an ordinary Bazel search test. Fuzz campaigns remain
local; this change adds no fuzz CI job or runtime configuration.
