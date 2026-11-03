# Schema-bound vector indexes

Index binding is tracked by https://github.com/nowledge-co/hawdb/issues/1157.
Filtered-domain execution is tracked by https://github.com/nowledge-co/hawdb/issues/1172.
Delivery requires the query, persistence and resource acceptance evidence below.

## Scope and ownership

SQL vector columns and Cypher node properties share one `VectorIndexDescriptor`.
The descriptor binds an index name, a resolved target, its declared
`VectorDimension`, metric, algorithm settings and canonical schema digest.
Relational targets use the normalized table name and column ordinal because the
relational schema has no independent persisted column identifier. Node targets
retain their `TableId` and `PropertyId`. Names remain diagnostic/resolution data;
the full resolved binding must match before a source is admitted.

The default schema-bound backend is DiskANN with cosine distance. DiskANN also
supports Euclidean distance and optional Local Scaling Graph construction;
explicit RaBitQ definitions support cosine and Euclidean distance. Their
candidate estimator uses the corresponding cosine or norm-corrected squared-L2
priority, followed by exact raw-vector reranking. Only one field
is indexed. NULL fields are omitted. Non-NULL cosine vectors must have a nonzero norm; a build
must fail rather than silently exclude an invalid non-NULL value.

The embedded facade resolves schema and captures data under the existing pinned
source snapshot. Index maintenance reuses registered Consumer publication and
reopen. It does not introduce a second directory selector, recovery ledger,
standalone ANN registry, or host graph scan. Per-operation resource policy is
not part of the persistent algorithm definition.

Registered vector Consumers keep a bounded metadata-only snapshot alongside the
native immutable closure. They do not reload the corpus into a compatibility
`SearchIndex`. Read counts, definitions and freshness through the Consumer's
`document_count`, `vector_index_descriptor` and `projection_freshness` methods;
use `bind_vector_index` for vector queries. `search_index` refuses Consumers
without resident rows. Ordinary search Consumers retain their complete resident
index. Direct `SearchIndex::open` and legacy `ConsumerProjection::open` cannot
open metadata-only vector Consumers; the embedded facade's registered native
reopen validates their selected generation and summary together.

`Database::catch_up_vector_index` maintains a fixed canonical field through a
complete pinned C..E change feed. Repeated native keys hydrate once from E;
deleted/NULL values retract the fixed native identity. It retains unchanged
immutable content and builds vector artifacts only for changed content. The
passed build options must retain the original metric/backend definition and
bound mapping/feed memory and work. A gap, schema drift or incomplete mapping
requires explicit rebuild or retry; it cannot acknowledge a partial cursor.
Only the facade's pre-admitted canonical projector may seal the origin marker.
Generic host mapping cannot claim this incremental baseline and keeps its
existing complete-source verification contract. The marker and logical summary
are sealed with the selected native generation. Old admitted read domains keep
their original source and graph; a new canonical epoch cannot bind an old index.

## Query semantics

The query mode is explicit and shared by SQL and Cypher. The default
`ann_window` mode generates bounded approximate candidates and reranks that
window by its declared exact distance. `exact` exhausts the declared eligible domain
before selecting its nearest k records. `filtered_ann` restricts approximate
candidate generation to the complete eligible domain and exactly reranks the
selected candidates. Ordinary filtered distance queries remain exact; the
optimizer cannot silently substitute approximate access.

Canonical vectors and raw projection vectors use the same core distance
implementation. The result is `1 - cosine_similarity` in [0, 2], including
negative similarities. Legacy search relevance remains positive-only and
unchanged. Neither clipped relevance nor quantized estimates may substitute for
the reported exact distance. The kernel breaks equal-distance ties by the
lexicographic encoded identity, which is not necessarily SQL primary-key order.
Use an explicit `ORDER BY distance, id` (or the corresponding graph property)
when the query requires that ordering.

In approximate modes, the candidate budget and final k are separate. Exact
reranking does not establish global exact recall. In `ann_window`, residual
query predicates apply after the explicit window; filtering cannot refill it.
In `filtered_ann`, eligibility applies before approximate candidate generation.
Outer ORDER BY reorders selected records and cannot change membership at an
equal-distance Top-K boundary.

| Mode | Execution order | Filter position |
| --- | --- | --- |
| `ann_window` (default) | ANN window, exact rerank, canonical residual filter | `after_window` |
| `exact` | Complete eligibility, exact distance, Top-K | `before_ranking` |
| `filtered_ann` | Complete eligibility, same-generation allowlist, ANN, exact rerank | `before_candidates` |

## Language entrypoints

The bound `VectorIndexReadTransaction` supplies the index and canonical snapshot
for both languages. `query_cypher_streaming` accepts an ordinary `QueryRequest`;
`query_sql_streaming` accepts a `VectorIndexSqlRequest`. Both require a fresh
same-governor permit and an explicit `VectorIndexLanguageBudget`: the index
sub-budget is deducted before the canonical query receives its allowance.
Governor identity is checked at each language entrypoint before execution or
allocation, including LIMIT 0 and plans that never reach the index operator.
The source name in the query must match the bound descriptor.

```cypher
CALL vector_index_search('documents', $embedding, candidates := 100, topK := 20)
YIELD node AS document, distance
WITH document, distance WHERE document.active = true
RETURN document.id AS id, distance
ORDER BY distance, id
```

```sql
SELECT document.id, distance
FROM vector_index_search('documents', $1, candidates => 100, top_k => 20) AS document
WHERE document.active = true
ORDER BY distance, document.id
LIMIT 10;
```

The embedding must be a canonical typed vector parameter. Both explicit limits
are required, with `0 < top_k <= candidates`. Filtering cannot refill the
already selected top-k window. Cypher binds real canonical nodes and can use
ordinary graph traversal afterward. Its node and distance output names must
remain distinct, including the default name of an omitted YIELD output. SQL decodes complete canonical primary keys,
including composite keys, and uses snapshot row point reads. It does not create
a temporary copy of the canonical table or run a host-side join/filter.

The first SQL operator supports one source with ordinary WHERE, projection,
ORDER BY, LIMIT and OFFSET. Joins, grouping, DISTINCT and locking clauses are
refused. The table function reserves the `distance` column, so a source table
already declaring that name is refused. Ordinary SQL/Cypher entrypoints do not
implicitly attach an index. SQL streaming currently accepts SELECT only; the
Cypher physical plan exposes its approximate window and post-window filtering
in EXPLAIN. Broader SQL plan integration remains subject to explicit qualification.

Language reports retain the kernel's actual backend, window, candidate/rerank
counts, source epoch and native generation. Cypher reports may omit the index
report if execution never reaches the operator. SQL currently executes candidate
selection even for LIMIT 0, whereas Cypher can skip the operator. Reports do not
yet count rows removed by canonical visibility or residual predicates; output
cardinality alone cannot distinguish a small index from post-window filtering.
These observations do not establish ANN recall, process RSS bounds or production
Mem acceptance. Native producer work remains bounded by the existing generation
writer and its component limits. Full graph capture currently point-queries each
node candidate, including nodes outside the target label. A sparse target label
can therefore exhaust the explicit feed-work budget; the build fails instead
of publishing a partial source. Label-aware source capture and plan/report cost
improvements remain subsequent work.

## Exact eligible domains

Both frontends accept a static `mode` literal. Exact mode requires `top_k` /
`topK` and a nonnegative `max_work` / `maxWork`; it rejects `candidates`. The
work option is not accepted in `ann_window`. Mode selection is part of plan
identity, not a runtime backend heuristic.

```sql
SELECT d.id, distance
FROM vector_index_search('documents', $1,
     mode => 'exact', top_k => 20, max_work => 10000) AS d
WHERE d.active = true
ORDER BY distance, d.id;
```

SQL binds WHERE against the canonical source schema before collection and
ranking, including when the source is empty. The generated `distance` column
is not available in this predicate. Projection, ORDER BY, LIMIT and OFFSET run
on the selected Top-K. Single-source restrictions still apply. Final hydration
uses the same canonical snapshot and does not evaluate the input predicate a
second time.

```cypher
MATCH (origin:Origin)-[:RELATED]->(candidate:Document)
WHERE candidate.active = true
WITH candidate LIMIT 100
CALL vector_index_search('documents', $embedding,
     mode := 'exact', eligible := candidate, topK := 20, maxWork := 10000)
YIELD node AS document, distance
RETURN document.id AS id, distance ORDER BY distance, id
```

This CALL is setwise: all upstream rows define one domain and one global Top-K.
It resets scope to the YIELD columns; it is not a per-row correlated CALL.
`eligible` must resolve to a bound node variable. Upstream LIMIT applies before
identity deduplication, so `[A, A, B] LIMIT 2` contains only A. Optional NULL
nodes, nodes outside the bound index table, and NULL vector fields contribute
no eligible vector. Canonical visibility applies to upstream reads and to the
eligible output node before ranking. No predicate may move across an existing
ANN window or upstream LIMIT. This delivery accepts canonical graph inputs;
GraphAlgorithm and external seed operators are refused within the eligible
subtree because their complete internal work is not yet metered.

The shared domain retains encoded identities and, only in exact mode, exact
scores. It does not retain complete rows or vector payloads. Exact collection
uses the core distance operator while the canonical vector is borrowed. It binds database, branch, Consumer registration,
projection, checkpoint, target incarnation, operation, publication generation
and source epoch, together with the query mode. A pin rejects a domain from a different binding even when
the descriptor and query vector are identical. A domain must be complete;
invalid query dimensions/norms and invalid bindings are errors for empty
inputs as well.

`max_work` bounds admitted source-candidate visits, point probes, graph
predicate evaluation, cached shortest-path enumeration and domain input rows.
Work units are execution operations, not physical I/O, unique records or output
rows. Repeated paths and probes consume work. SQL charges each canonical row
before evaluating its input predicate. The Cypher eligible subtree uses a
scoped canonical fallback for scan/index shortcuts that do not yet expose work
admission; ordinary queries retain those shortcuts. This can scan more records
and refuse a narrow budget even when a selective index exists. The eligible
optimizer scope clears ordered range-index properties to preserve an explicit
ORDER BY before LIMIT under canonical fallback. Work exhaustion
and cancellation fail without delivering a successful partial domain.

This is not a bound on every executor operation or CPU time. UNWIND/list
expansion, Cartesian/hash-join combinations, and Sort/Aggregate/Distinct
internal work are not charged individually. An aggregate can consume many
join combinations while emitting few charged domain rows. Deadline and
cancellation checks still apply; use them alongside the work and memory
limits. Graph-read work accounting is separate from payload admission and
does not reserve memory or invoke batch flush/Stop callbacks.

Reports expose `mode`, `filter_position`, source epoch and generation. Exact
reports name backend `exact`, set ANN `candidate_limit` to zero and include
`input_rows`, `eligible_vectors`, `scored_vectors`, `work_units`, `max_work`,
`source_strategy = bounded_canonical`, and completion. Input rows are counted
before identity deduplication and are distinct from visited-source work.
Exact source collection and final SQL hydration use the canonical allowance in
successive phases: the eligible domain is dropped after Top-K selection and
before output projection/sort allocates. Both phases use the same canonical
ledger, so overlapping lifetimes cannot each spend the complete allowance.

## Filtered approximate eligible domains

`filtered_ann` requires a static mode literal, explicit `candidates`, `top_k` /
`topK`, and nonnegative `max_work` / `maxWork`. Cypher also requires the same
upstream `eligible` node binding as exact mode. Both approximate modes enforce
`0 < top_k <= candidates` and the native configured candidate cap.

```sql
SELECT d.id, distance
FROM vector_index_search('documents', $1,
     mode => 'filtered_ann', candidates => 100, top_k => 20,
     max_work => 10000) AS d
WHERE d.active = true
ORDER BY distance, d.id;
```

```cypher
MATCH (origin:Origin)-[:RELATED]->(candidate:Document)
WHERE candidate.active = true
WITH candidate LIMIT 100
CALL vector_index_search('documents', $embedding,
     mode := 'filtered_ann', eligible := candidate,
     candidates := 100, topK := 20, maxWork := 10000)
YIELD node AS document, distance
RETURN document.id AS id, distance ORDER BY distance, id
```

The eligibility, canonical visibility, NULL, deduplication and upstream LIMIT
rules above apply unchanged. SQL WHERE is evaluated while collecting eligibility
and is not repeated after the approximate window. Cypher remains setwise; an
explicit predicate after YIELD is a separate downstream filter.

Collection retains identities without calculating their distances. Before ANN,
the pin verifies the complete source/publication identity, and native metadata
visitation maps each eligible identity to its layer-local ordinal. The existing
metadata reader validates ordering, bounds, vector counts and ordinal bases
against that pinned layer layout. Every eligible non-NULL vector must have
exactly one visible mapping; missing, ambiguous or NULL-ordinal mappings fail
before ANN. No mapping is cached across requests or reused after a rebuild,
including a rebuild at the same source epoch.

Mapping charges every visited metadata record to the eligibility work scope,
including rejected records. For a non-empty eligible domain, `max_work` /
`maxWork` must budget for canonical collection plus the index's metadata size.
Each non-empty query visits the full index metadata in this delivery, so a
small eligible set over a large index can exhaust the work limit during mapping
alone.

Existing index memory, file-descriptor, I/O and
spill admission remains separate from that work counter. The mapping proof and
candidate blocks are admitted before allocation. Non-empty explicit eligibility
uses the native custody-owned scratch stage even without a visibility mask.
Failure releases temporary mappings, spills and I/O ownership; exhaustion is
an error, never a partial allowlist.

Completed empty eligibility returns no candidates after query, binding and
window validation. It never becomes an unrestricted search. The non-empty path
reuses native per-layer ordinal allowlists and the bound backend: filter-aware
DiskANN traversal by default, or an explicitly selected RaBitQ scan. Only
the retained global candidate window is exactly reranked, at most `candidates`
records. Generated candidate counts may sum per-layer candidates and therefore
exceed that retained window. Filtering is exact, while recall remains approximate.

Reports identify mode `filtered_ann`, the executed backend (`diskann` or
`rabitq`), and
`filter_position = before_candidates`. Eligibility reports retain
`scored_vectors = 0`; `reranked_candidate_count` reports the separate exact
rerank work. `work_units` includes canonical collection and metadata mapping,
while `input_rows` still counts canonical input rows before deduplication.
Empty eligibility has zero generated/reranked candidates and no candidate spill
or vector scan. The existing work-limit exclusions for list/join expansion and
operator-internal CPU work still apply; ANN execution also retains its native
candidate, memory, I/O, deadline and cancellation limits.

## Identity and lifecycle refinement

A definition is not serving authority. The existing Consumer supplies source
database/branch identity, incarnation, operation, source epoch, immutable
generation, artifact closure and the publication receipt. The definition must
be sealed inside that closure; an independent sidecar cannot authorize it.

The lifecycle refines the existing Consumer protocol:

1. Resolve and validate the definition under the captured schema.
2. Build a complete private projection from the same pinned source epoch.
3. Seal definition and artifacts before source-fence validation and selection.
4. Publish using the existing durable selector; propagate uncertain outcomes.
5. Reopen only the registered closure and verify its definition and receipt.
6. Retained pins continue reading their immutable generation. A new canonical
   snapshot must not be joined to an older index as if the index were current.

Source epochs and publication generations are independent: a full rebuild may
select a new generation at the same source epoch. Acknowledgement first verifies
the complete native selected receipt, then refreshes the cached receipt while
rejecting source-epoch regression; cached checkpoint bytes are not a second
publication authority.

Source writes make a current-source binding stale until explicit qualified
maintenance publishes its successor. Rebuilds and deletes must not reuse old
ordinal mappings. Existing Consumer generation custody, source fencing,
uncertain-publication resolution and pin reclamation remain authoritative.
Any additional state transition requires a refinement/model update before it
is enabled; merely adding descriptor bytes does not replace those proofs.

The vector facade refines these existing transitions without adding a selector:

| Operation | Existing authority | Vector-specific precondition/result |
| --- | --- | --- |
| Bind read | Completed Consumer pin | Database UUID, branch UUID, source epoch and resolved schema all match the retained canonical snapshot |
| Begin rebuild | `reserve_search_consumer_update` and successor birth | Rebuild the same target, dimension and algorithm from a complete new source snapshot |
| Prepare | `prepare_registered` | A fresh complete source mapping replaces all documents, including deletions and NULL transitions |
| Select | Existing source fence and stationary selector | Reject a source change after capture; acknowledge only the native completed receipt |
| Uncertain selection | Existing pending-publication custody | Retain the candidate and resolve through the ordinary Consumer recovery entrypoint |
| Old read | Existing immutable pin | Keep both the old canonical snapshot and its matching generation; hold no execution slot while idle |

The generic Mem document catch-up mapper is not the maintenance mapping for a
schema-bound vector index. The facade refuses that combination through both legacy and covered catch-up entrypoints. A full vector
rebuild may refresh the schema digest after an unrelated schema change, but a
changed target identity, dimension or algorithm requires a new definition.
Dropping and recreating a graph property changes its identity; reordering SQL
columns changes their bound ordinal. Rebuilding the old definition then refuses.
Remove the old Consumer through its normal lifecycle and create a new vector
index definition before admitting reads against the changed target.

Recovery preserves the existing project-authority boundary: a canonical read
transaction retained from a closed database can defer reopening an uncertain
publication with `BranchBusy`. That refusal retains pending evidence and the old
read remains usable. Releasing that transaction allows the ordinary recovery
entrypoint to resolve the selected generation; no vector-specific bypass is used.

## Resource ownership

A registered vector producer retains its source envelope through publication.
Its compatibility snapshot admits compression and encoding scratch before use;
its private artifacts use the existing generation writer under the original
Consumer publication lease, including the original governor admission. This
private producer cannot advance a previously selected root. It is installed and
selected only by the existing Consumer protocol. The producer holds an actual
I/O wave from that governor throughout synchronous private snapshot/artifact
work and releases it before Consumer target preparation. A saturated slot waits
cancellably before writing; staging cleanup shares the producer wave.

Canonical SQL point reads admit key/field/overflow decoding before copying and
retain the allocation owner through projection. The storage allocation controller
composes the query ledger with the original governor controller. Candidate and
canonical execution have disjoint sub-budgets; original vectors are reranked by
the shared distance implementation. Every projection, expression input and sort-key occurrence is admitted before
copying, including repeated wide columns and the empty-input binding probe.
LIKE/ILIKE scratch is admitted separately using the shared matcher's layout:
token slots, one folded-literal buffer, and original Unicode characters. Layout
and execution use the same escape/folding rules and preallocate those buffers;
matching has no growing temporary literal string. Repeated `%` tokens compact,
while alternating `%_` patterns retain their full admitted token cost. Invalid
patterns retain evaluator diagnostics, including short-circuit and NULL behavior.
SQL result delivery checks cancellation and deadlines before and after every
callback, including the final row. Callback errors retain their original cause.
A narrow grant fails before callback delivery
and does not change the selected index or poison the canonical read view.

## Acceptance evidence

Required end-to-end cases cover both SQL and node-property sources: create,
build, explicit query, close, reopen, update/delete and successor publication.
SQL and Cypher must agree on distance and deterministic ties, including opposite
vectors. Test NULL, zero vectors, invalid dimensions, unsupported metrics,
schema/source mismatches, retained old pins, resource boundaries and
cancellation. Feature-disabled builds refuse unavailable index execution.

Build, candidate selection, raw hydration, result retention and decoding must
be admitted before allocation. Budget exhaustion is an error, not successful
truncation. Query evidence identifies backend, candidate budget/count, exact
rerank count, source epoch and generation; it does not claim exhaustive ANN
recall or production Mem acceptance.

The root vector-index unit family is owned by `//:hawdb_unit_test_binary` in
the existing root presubmit lane; Search binding and producer-I/O cases are
owned by `//crates/search:hawdb_search_tests` and applicable existing feature
variants in the crates lane. The canonical point allocation guard belongs to
`//crates/relational:hawdb_relational_tests`. Fixtures use isolated UUID paths,
at most five vectors for semantic cases, and a 256-KiB synthetic text value for
allocation boundaries, including alternating LIKE tokens and Unicode/escape controls. The I/O test uses a channel to observe the acquisition
boundary and cancellation to release a saturated wait; its receive timeout is
a failure ceiling, not a timing assertion. No new CI job, serialization gate or
heavy benchmark is added.

These guards are separate from pre-existing ordinary relational projection and
standalone writer admission gaps, tracked by
https://github.com/nowledge-co/hawdb/issues/1160 and
https://github.com/nowledge-co/hawdb/issues/1161. This vector-index qualification
does not claim those independent paths are repaired.

Exact eligibility acceptance additionally covers an ineligible nearest prefix,
SQL/Cypher parity, duplicate paths and upstream LIMIT, NULL/empty domains,
visibility before ranking, retained snapshots, mismatched domain bindings,
all-rejected source-work exhaustion and cancellation/retry. Work-budget core
and admitted source-read tests run in their existing crate lanes; no new CI
lane is introduced.

Filtered-ANN guards additionally cover zero collection scoring, an exact rerank
window smaller than eligibility, empty-domain validation, missing/ambiguous
mappings, rejected metadata work, typed mapping memory refusal/retry and mapping
lease refunds. Root empty-index retries preserve the mapping error and permit
reuse; they do not measure native file descriptors or scratch cleanup. The
existing captured candidate-spill and Stage cleanup guards independently cover
those resource owners. SQL/Cypher parity, upstream LIMIT, canonical visibility and
update/delete/NULL/reopen cases exercise both eligible modes. Existing crate,
root and Search feature lanes own these cases; no new CI lane is introduced.

Mem App schema/import migration, runtime activation and FreshDiskANN evaluation
remain subsequent roadmap phases.
