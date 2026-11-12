# Embedded API naming

HawDB's host-facing Rust API names describe database capabilities rather than
the first application that consumes them. Import these contracts from `hawdb`;
internal workspace crates remain implementation owners, not host integration
entry points.

## Source migration

The former `NowledgeMem` type prefix and `nowledge_mem` module have been
removed. This is a Rust source API change: downstream hosts must update their
imports and references together when updating the HawDB revision. There are no
aliases retaining the former names.

Most types drop the prefix directly, such as
`NowledgeMemSearchCandidateRequest` becoming `SearchCandidateRequest` and
`NowledgeMemSearchCandidateReport` becoming `SearchCandidateReport`. Associated
functions and constants also drop `nowledge_mem_` and `NOWLEDGE_MEM_`.

The following names retain additional context to distinguish their ownership
or avoid collisions with existing kernel types:

| Former API | Current API |
| --- | --- |
| `hawdb::nowledge_mem` | `hawdb::embedded_store` |
| `NowledgeMemEmbeddedStore` | `EmbeddedStore` |
| `NowledgeMemEmbeddedStoreHandle` | `EmbeddedStoreHandle` |
| `NowledgeMemOpenOptions` | `EmbeddedStoreOpenOptions` |
| `NowledgeMemOpenReport` | `EmbeddedStoreOpenReport` |
| `NowledgeMemOpenDiagnosticOptions` | `EmbeddedStoreOpenDiagnosticOptions` |
| `NowledgeMemGraph` | `EmbeddedGraph` |
| `NowledgeMemQueryOutput` | `EmbeddedQueryOutput` |
| `NowledgeMemQueryReport` | `EmbeddedQueryReport` |
| `NowledgeMemReadOptions` | `EmbeddedReadOptions` |
| `NowledgeMemReadOutput` | `EmbeddedReadOutput` |
| `NowledgeMemReadReport` | `EmbeddedReadReport` |
| `NowledgeMemReadSnapshot` | `EmbeddedReadSnapshot` |
| `NowledgeMemReadSnapshotBudget` | `EmbeddedReadSnapshotBudget` |
| `NowledgeMemReadSnapshotReport` | `EmbeddedReadSnapshotReport` |
| `NowledgeMemRuntimeStatus` | `EmbeddedRuntimeStatus` |
| `NowledgeMemProductionStatus` | `EmbeddedProductionStatus` |
| `NowledgeMemServingEntrypoint` | `EmbeddedServingEntrypoint` |
| `NowledgeMemServingPathReadiness` | `EmbeddedServingPathReadiness` |
| `NowledgeMemStorageRecoveryReport` | `EmbeddedStorageRecoveryReport` |
| `NowledgeMemSearchProjectionConsumer` | `RegisteredSearchProjectionConsumer` |
| `NowledgeMemPreparedConsumerUpdate` | `PreparedSearchProjectionConsumerUpdate` |
| `NowledgeMemSlowQueryRecord` | `SlowQueryReportRecord` |
| `HawDBEmbedded::open_nowledge_mem` | `HawDBEmbedded::open_embedded_store` |

All former `NowledgeMemQuery*` types use `EmbeddedQuery*`, including report
options, report inputs, execution paths and row-shape descriptions. Existing
unprefixed kernel contracts such as `QueryOutput`, `ReadSnapshotBudget`,
`SearchProjectionConsumer` and `StorageRecoveryReport` keep their own meanings.

## Search example

```rust
use hawdb::{EmbeddedStoreHandle, Result, SearchCandidateRequest, SearchResultSet};

fn search(handle: &EmbeddedStoreHandle, query: &str) -> Result<SearchResultSet> {
    let request = SearchCandidateRequest::text(query, 20).with_offset(0);
    handle.search_candidates(&request)
}
```

The request still selects text, vector or hybrid candidate retrieval. It does
not generate embeddings or replace the canonical graph retrieval boundary.
Vector and hybrid requests may add `.with_exact_vector_search()` to score the
complete filtered eligible set with raw vectors instead of approximate graph
candidates; `SearchCandidateReport::exact_vector_search` reports whether the
executed vector leg was exact. See the
[filtered DiskANN spec](specs/FILTERED_DISKANN_SPEC.md) for its memory bound.

## Compatibility scope

This rename does not change query execution, persistence formats, protocol
string values, JSON field names, or serialized entrypoint and storage-owner
identifiers. In particular, existing `hawdb-nowledge-mem-*` protocol values
continue to identify the same evidence schemas. Source-bound benchmark and
qualification recordings retain the names and paths of their recorded revision.

Mem-specific route catalogs and migration/readiness requirements retain their
application-specific semantics. Neutral Rust names do not make those fixed
catalogs or acceptance gates configurable for arbitrary hosts.
