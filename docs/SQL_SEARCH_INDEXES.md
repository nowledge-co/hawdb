# SQL search indexes

Tracked by https://github.com/nowledge-co/hawdb/issues/1654.

A SQL search index is a BM25 full-text index over one `TEXT` column of a
RowPage table. HawDB maintains it in the same transaction as the table, keeps
it under the database directory, and restores it on open. A host that only
speaks SQL (the C ABI `hawdb_query_sql`, the Bun binding) can
declare, write and query it. There is no host-side `SearchIndex`, projection
mapper, catch-up call or refresh job.

```sql
CREATE TABLE events (
    id BIGINT PRIMARY KEY,
    stream TEXT NOT NULL,
    seq BIGINT NOT NULL,
    text TEXT
);

CREATE SEARCH INDEX events_text ON events (text) WITH (analyzer = 'cjk_ngram');

INSERT INTO events (id, stream, seq, text)
VALUES (1, 'chat', 1, '上次我们讨论了数据库的全文搜索'),
       (2, 'chat', 2, 'Lexical recall over the event log');

SELECT e.seq, e.stream, score
FROM text_search('events_text', $1, top_k => 20) AS e
WHERE e.stream = $2
ORDER BY score DESC, e.seq;

DROP SEARCH INDEX events_text;
```

## Statements

- `CREATE SEARCH INDEX [IF NOT EXISTS] name ON table (column) [WITH (analyzer = '...')]`.
  The column must be `TEXT`. `analyzer` is optional and defaults to
  `cjk_ngram`, the only analyzer in this release. Creation backfills existing
  rows in the same transaction. The index name shares the relation namespace,
  as PostgreSQL index names do. `IF NOT EXISTS` accepts an existing search
  index of that name without comparing its definition.
- `DROP SEARCH INDEX [IF EXISTS] name`. `DROP MATERIALIZED VIEW` refuses a
  search index, and `DROP SEARCH INDEX` refuses anything else.
- Both work inside explicit SQL transactions and roll back with them.

A search index is not a relation: `SELECT ... FROM events_text`, DML,
`ALTER TABLE` and `CREATE INDEX` on it are refused. `information_schema.tables`
and `pg_tables` omit it; `pg_indexes` lists it against its source table with
its `CREATE SEARCH INDEX` definition.

## `text_search`

```text
text_search(index TEXT, query TEXT, top_k => k BIGINT) [AS alias]
```

Arguments are literals or positional parameters. `top_k` is required and must
be between 1 and 10,000. The call is the only `FROM` item; it yields rows of
the source table, bound under `alias` (or `text_search`), plus a virtual
`score DOUBLE PRECISION` column. `SELECT *` returns the source columns followed
by `score`. A source table with a column named `score` cannot be searched.

Semantics:

1. The query is analyzed with the index's analyzer into distinct terms. A
   document matches if it contains any term (disjunctive match).
2. Matching documents are scored exactly with Okapi BM25 (`k1 = 1.2`,
   `b = 0.75`, `idf = ln(1 + (N - n + 0.5) / (n + 0.5))`), using the corpus
   statistics of the same snapshot. Nothing is approximated.
3. Candidates are taken in descending score order, ties by encoded primary
   key, and hydrated from the same snapshot. `WHERE` is applied to each; the
   first `top_k` rows that satisfy it are the result. Filtering therefore
   happens before the top-k cut: a selective `WHERE e.stream = $2` still returns
   up to `k` rows from that stream.
4. `ORDER BY`, `LIMIT` and `OFFSET` then apply to those rows. Without
   `ORDER BY`, rows arrive in score order.

Joins, grouping, aggregates, `DISTINCT` and locking clauses are refused with
`text_search` in this release; filter on the source columns directly instead
of joining back to the source table. Inside an explicit transaction,
`text_search` reads the transaction's own staged writes, like any other
SELECT. `EXPLAIN` and `EXPLAIN ANALYZE` show the plan below.

## Execution: the `Bm25IndexScan` access path

A `text_search` call is planned as a physical `Bm25IndexScan` over the
search index, followed by a hydration of source rows by primary key, the
residual `WHERE`, projection, `ORDER BY` and `LIMIT`:

```text
LimitExec            (LIMIT/OFFSET)
└─TopNExec           (ORDER BY, when present)
  └─ProjectionExec   (virtual column: score)
    └─SelectionExec  (residual WHERE: post-filter, bounded over-fetch)
      └─SearchHydrationExec   table:events, fetch=primary_key, order=rank
        └─Bm25IndexScan       table:events, search_index:events_text(text)
                              algorithm=block_max_wand, terms=[...], top_k, k
```

**k, and LIMIT pushdown.** The scan ranks exactly `k` documents. `k` is
`top_k`, or `OFFSET + LIMIT` when that is smaller and the statement's output
order is the rank order (no `ORDER BY`, or exactly `ORDER BY score DESC`).
Any other `ORDER BY` needs the whole `top_k` window and gets it.

**Exact top-k with Block-Max WAND.** The scan reads the statistics row, then
for each query term its dictionary row and its block bounds rows (the skip
entries, see [Physical layout](#physical-layout)) in one range scan; no posting
block is read yet. Ranking uses the BM25 scorer and Block-Max WAND kernel in
`hawdb_executor::bm25`, which the graph-side lexical projection also uses for
its scores (one implementation of the formula, so both paths compute the
same bits). Cursors walk each term's postings in ordinal order; their
positions are lower bounds until a block is decoded. With a k-sized heap
(score descending, then encoded key) full, the kernel:

1. picks the WAND pivot: the first document at which the cursors at or
   before it can, by their term-wide bounds, reach the heap's floor;
2. checks the block-max bound: the sum of the bounds of the blocks that hold
   those cursors' postings at the pivot. Below the floor, every document up
   to the end of the first of those blocks (or the next cursor) is skipped
   without decoding the blocks in between;
3. otherwise decodes the blocks at the pivot and scores the document exactly.

A block's bound is the BM25 contribution of its maximum term frequency in a
document of its minimum length, inflated by a relative 1e-12 so that no
rounding can put it below a covered document's score. Pruning is strict (a
bound equal to the floor is still examined, because equal scores are ordered
by key), so the result is exactly the first `k` entries of the full ranking,
with bit-identical scores. Cursors read a decoded block's successors ahead
only while they are consecutive and each block alone can still reach the
floor, so read-ahead does not undo the skipping. Reads are windows (4 to 64
consecutive competitive blocks; 128 lengths rows) because every range read has a fixed cost that
dominates small reads on a snapshot with a large uncheckpointed overlay.

**Filters: bounded iterative over-fetch.** A `WHERE` clause cannot be decided
from the index (the index holds no source columns), so it is applied to
hydrated candidates in rank order. The first round ranks `4k` documents
(`k` without a `WHERE`); when
candidates fail `WHERE` and more are needed, the next round ranks a prefix
four times as long (rank order is total, so each round extends the previous
one exactly; the scan checks it) and hydration continues where it stopped.
A prefix that would reach the queried terms' total postings ranks the whole
match set instead. Rounds stop as soon as enough rows are accepted, or the
match set is exhausted. The total ranking work is at most about 4/3 of the
last round's, and nothing ranks more than the hydration needs, rounded up
by the growth factor. A filter that
rejects every candidate still hydrates every match; pushing filters into the
index would need filter columns stored in it, which this release does not
do.

**Observability.** `EXPLAIN ANALYZE` reports, on the `Bm25IndexScan` row:
`rounds`, `final_k`, `ranked_rows`, `matched_terms` (terms present / query
terms), `postings_visited` (decoded / total postings of the queried terms,
summed over rounds), `blocks_decoded`, `blocks_read` (decoded or read ahead),
`blocks_skipped` (never read), `blocks_total`, `block_skips` (pivots pruned
by block bounds), `documents_scored`, `lengths_rows_read` and
`terminated_early`; the `SearchHydrationExec` row reports the candidates
hydrated. The same counters are returned with profiled SQL reads
(`RelationalSqlReadProfile::search_index_scan`); the row execution evidence
also carries `postings_visited`, `posting_blocks_read` and
`posting_blocks_skipped`, and the query's access path is `SearchIndex`.

**Work and memory.** Every row the scan reads counts against the query's
candidate-work limit as it is read (a block counts its postings, every other
row one), and so does every hydrated candidate. The scan's memory is charged
to its own `Bm25IndexScan` account before each allocation: the query terms,
the skip entries, one decoded block per term, the read-ahead buffers, a
small window of lengths rows and the current ranked-prefix heap. Each lengths
row reserves all decoded slots, including deleted slots, and an upper bound
on its keys; the fixed window directory is admitted separately.

The complete scan is not O(requested `top_k`) memory. Skip metadata grows with
the queried terms' sealed-block counts, including blocks whose payloads are
never read. Residual filtering can grow the ranked prefix to the whole match
set; the previous and new prefixes coexist while a round is replaced. Block
read-ahead and the 128-row lengths window have fixed bounds, but their payload
bytes still count against admission. Exceeding any admitted limit fails the
query; it never returns a truncated ranking or falls back to approximate
scoring. A selective or reject-all filter can therefore be refused even when
its final output is small.

## Analyzer `cjk_ngram`

- Full-width ASCII (U+FF01..U+FF5E) folds to ASCII; U+3000 is a separator.
- Han (including extensions A..H and compatibility ideographs), Hiragana,
  Katakana and Hangul characters form ideographic runs. Every other Unicode
  letter or digit forms words, lowercased with `char::to_lowercase`. Anything
  else separates tokens. There is no stemming and no stop-word list; BM25's
  IDF discounts common terms.
- Ideographic runs index every character as a unigram and every adjacent pair
  as a bigram. A query run of two or more characters searches its bigrams; a
  single-character run searches its unigram. `数据库` therefore matches text
  containing `数据` and `据库`, and `库` matches any text containing `库`.
- Words longer than 128 bytes (hashes, encoded blobs) are not indexed.
- The analyzer is part of the persistent definition. Its output never
  changes; a different analysis would be a new analyzer name.

## Catalog and maintenance

**Catalog.** A search index is an index object in the persistent catalog: the
relational schema encoding records it with its own derived-relation tag
(`2`, distinct from materialized views) and its definition (source table,
column, analyzer, physical layout). Its name lives in the relation namespace,
as PostgreSQL index names do, and `pg_indexes` lists it against its source.
Its postings are stored as rows of a private RowPage relation under that
name, which only the search index access method reads: SQL cannot select
from, write, alter or index it, and `information_schema.tables` and
`pg_tables` omit it. The physical layout is versioned behind that access
method (layout tag `3` below), so it can change without changing the SQL
surface. The in-memory definition reuses the commit-time maintenance hook
that materialized views also use (one point where derived rows are written
inside the base transaction); it does not inherit materialized view
semantics such as selectability or their writer footprint.

**Maintenance** is synchronous: postings are written by the base
transaction itself. A document with a NULL or token-free column is not part
of the corpus.

- **Commit.** For each source key a transaction changed, maintenance analyzes
  the old and new row images and gathers the changed postings for the whole
  commit; then each affected term's dictionary row, and only the posting blocks
  that own changed older documents, are rewritten once. Unchanged text costs
  nothing. Writes go through the same transaction, WAL record and commit
  acknowledgement as the source row; the index is never behind.
- **Recovery.** WAL replay re-runs the same deterministic maintenance, so a
  replayed transaction restores its postings with its rows; an incomplete
  transaction restores neither. Checkpoints publish the postings with the
  rest of the relational row pages. Strict recovery refuses a torn tail with
  the evidence retained; the default `AutoRepairTornTail` drops the incomplete
  transaction as a whole.
- **Open.** Nothing is rebuilt on open: postings are ordinary checkpointed rows
  plus WAL replay. Read-only opens search the same way.
- **Branches.** Postings are branch data. A new branch starts with its
  source's index and diverges with its own writes; `USE BRANCH` switches both.
- **Logical dump.** [Export](LOGICAL_DUMP.md) records each index's
  definition (name, column, analyzer) with its table and never its postings.
  Import creates the index on the empty table and rebuilds the postings as
  it loads the rows, so `text_search` ranks the imported rows exactly as the
  source did.
- **Concurrency.** Postings are derived at commit from the latest committed
  state, and nothing but `text_search` reads them, so a transaction writing a
  source with a search index keeps the ordinary per-row writer footprint of
  its source rows: writers to other tables, and writers of other rows of the
  same table, do not conflict with it. (Materialized views still take a
  database-wide footprint, because their rows are read like tables.) A
  `text_search` read records a read dependency on its source table's domain:
  BM25 statistics depend on every document, so a serializable transaction
  that ranked the corpus conflicts with any concurrent change to the source
  table, and with nothing else.

Admission follows the materialized view rules: index row reads and writes and
the gathered postings share the transaction's `RelationalMutationLimits` budget
(250,000 row work units and 64 MiB of logical payload by default). A document
with `t` distinct terms costs about `t` dictionary-row reads and writes per
insert; a backfill costs about the source row count plus the corpus's distinct
term count, plus a few dozen payload bytes per posting. Analysis is streamed:
each distinct term of a document is charged to the same payload budget (its
bytes plus an estimated map entry) before it is allocated, repeated tokens
allocate nothing, and long analyses reach a cancellation checkpoint every 4,096
tokens, so a large ideographic value is refused part-way through analysis
instead of first expanding into millions of token strings. A transaction that
exceeds the budget is rejected before WAL append with its source rows; load in
smaller transactions, or create the index on a smaller corpus first, when a
backfill is refused.

The same residency requirement applies as for materialized views: index rows
are resident relational rows, and `RelationalIndexMode::Authoritative` refuses a
database containing a search index. Shadow, demand-paged and the default
materialized index modes, and `OutOfCore` residency with materialized postings,
are supported.

## Physical layout

Layout tag `2`. Every row of the hidden relation has the primary key
`(kind BIGINT, name BYTEA, slot BIGINT)` and one `data BYTEA` payload of
unsigned LEB128 varints (canonical spelling only; decoders refuse overlong,
truncated or trailing bytes):

| kind | name | slot | data |
| --- | --- | --- | --- |
| 0 statistics | empty | 0 | live documents, total tokens, next ordinal |
| 1 document | encoded source primary key | 0 | the document's ordinal |
| 2 lengths | empty | ordinal / 64 | per ordinal: token count (0 = none), encoded primary key |
| 3 term dictionary | term (UTF-8) | `i64::MAX` | document frequency, sealed block count, tail start, tail minimum length, tail postings |
| 3 sealed block | term (UTF-8) | fence | posting count, first/last ordinal, maximum term frequency, postings, CRC32C |
| 4 block bounds | term (UTF-8) | fence | last ordinal - fence, posting count, maximum frequency, minimum length, CRC32C |

- **Ordinals.** A document entering the corpus gets the next dense integer
  ordinal, in source-key order within a commit; ordinals are never reused,
  and an update keeps its document's ordinal. WAL replay assigns the same
  ordinals. Posting lists carry ordinals, not primary keys; the lengths rows
  map an ordinal back to its primary key and store each document's length
  once, not once per posting.
- **Posting lists** are a count followed by `(ordinal delta, term frequency)`
  pairs, sorted by ordinal. The first delta is relative to the list's base
  and every later delta is at least 1.
- **Verified block headers.** Every sealed block records its posting count,
  first and last ordinal and maximum term frequency before the posting pairs,
  and a trailing CRC32C covers the header and the complete posting payload.
  A decoded block checks that its postings match its header exactly, and
  that the header matches the block's bounds row (below). Bounds rows end in
  their own CRC32C, so a bound that lets the scan skip a block unread is
  verified before it is trusted; torn or bit-corrupt bytes never pass
  silently.
- **Term dictionary.** One row per term carries its document frequency, how
  many sealed blocks it has, and the tail start; up to 127 postings at or
  above the tail start live inline in the dictionary row. A rare term is
  therefore one row, `text_search` knows each term's document frequency (and
  refuses an over-budget term) before reading any block, and an append
  rewrites only the dictionary row.
- **Sealed blocks.** When the tail reaches 128 postings it is flushed: first
  into the term's last sealed block if deletes left room there (up to 128),
  then into new blocks keyed by their fence (the tail start at the time), and
  the tail start moves past the last flushed ordinal. Block `f` owns the
  term's ordinals in `[f, next fence)`; the first block's fence is 0, and it
  is kept, possibly empty, while the term exists, so every older ordinal has
  an owner.
- **Block bounds (skip entries).** Every sealed block has exactly one bounds
  row under the same fence: its last posting's ordinal, its posting count, the
  maximum term frequency of its postings and a lower bound on the lengths of
  its documents (the retained empty fence-0 block has all zeros). They are
  separate rows so that a ranked scan reads a term's skip entries in one
  range scan without reading any block, and decodes only the blocks that can
  still reach the top-k. The maximum frequency is exact: it is recomputed
  whenever the block is rewritten. The minimum length is conservative: it
  only ever decreases while a block lives (an added posting lowers it to its
  document's length, a split keeps it, a merge takes the smaller one, and a
  removed posting leaves it), so a delete never invalidates it. A document
  that becomes shorter lowers the bound of every block holding one of its
  unchanged postings, by rewriting only those blocks' bounds rows; a longer
  document needs nothing. The dictionary keeps the same conservative minimum
  for its tail (0 exactly when the tail is empty), and a flushed tail carries
  it into its blocks. Readers check the bounds against every decoded block
  (count, last ordinal, maximum frequency) and against the length of every
  scored document, and refuse a violation as a storage-integrity error.
- **Updates and deletes rewrite blocks in place; there are no tombstones.**
  A change to an older document rewrites the one block that owns its ordinal
  (at most 256 postings, a few hundred bytes) and its bounds row, so BM25
  statistics stay exact without a dead-posting filter, nothing accumulates,
  and the cost per commit is bounded by the number of changed postings, not by
  a term's document frequency. A block that grows past 256 postings splits
  into 128-posting blocks; a changed block that shrinks below 32 merges into a
  neighbour when the two fit in 128, so delete-heavy churn cannot leave a run
  of nearly empty blocks. When a term's last posting goes, its rows go; when
  the last document goes, the relation is empty.

Tombstones were rejected: exact BM25 needs live document frequencies and
document counts anyway, so a delete must touch every one of its terms'
dictionaries regardless, and tombstoned postings would need a filter on every
query plus a compaction whose cost grows with a term's frequency.

The ranked scan reads the statistics row, then per query term the dictionary
row and the term's bounds rows (one primary-key range), then only the sealed
blocks Block-Max WAND has to decode, through short primary-key ranges that read
ahead over consecutive competitive blocks, and the lengths rows of the
documents it scores, in forward windows of 128 rows (8,192 ordinals). A term-level and
a block-level bound decide what is skipped; see
[Execution](#execution-the-bm25indexscan-access-path).

### Measurements

Temporary local harness (not committed), repository release profile, macOS
arm64, persistent database with the default `SyncOnEveryWrite`. Each event row
has about 30 English words, 30 distinct `wNNNN` tokens and two short Chinese
sentences, about 70 postings. "Old" is the first revision of this change
(one row per `(term, document)`); "new" is the compact layout 1 (the layout
above without block headers, bounds rows or checksums) with the full-ranking
scan of the first revisions; the next subsection compares the first
released ranking (layout 2, WAND inside `text_search`) with the current
`Bm25IndexScan`. The three
variants ran side by side on a host that other workloads also loaded, so
latencies are comparable within a table, not across tables or machines.

10,000 events, one INSERT per transaction:

| | no index | old layout | new layout |
| --- | --- | --- | --- |
| index relation rows | – | 695,348 | 18,351 |
| estimated resident row bytes, all tables | 5.28 MB | 79.96 MB | 9.14 MB |
| of which the index | – | 74.68 MB | 3.86 MB (19x less) |
| full checkpoint file `relational.N.hawdb` (batched load) | 4.80 MB | 42.14 MB | 7.51 MB (index 2.71 MB, 14x less) |
| database directory after checkpoint | 42.0 MB | 978.3 MB | 189.5 MB |
| of which WAL / other files | 7.4 / 34.7 MB | 71.8 / 906.5 MB | 73.9 / 115.7 MB |
| INSERT mean / p50 / p99 | 19.7 / 15.0 / 35.4 ms | 28.1 / 16.8 / 39.8 ms | 24.2 / 16.5 / 39.2 ms |
| `w42` (rare term) | – | 1.97 ms | 2.60 ms |
| `deploy failed` (thousands of matches) | – | 6.77 ms | 5.09 ms |
| `数据库 搜索` (four common bigrams) | – | 7.84 ms | 4.39 ms |
| `WHERE stream = 's0'` (matches early) | – | 7.40 ms | 6.01 ms |
| `WHERE stream = 's3'` (rejects all 3,334 candidates) | – | 277 ms | 338 ms |
| reopen + first query after checkpoint | 0.40 s | 1.08 s | 0.29 s |
| the same queries after reopen | – | 0.87 / 4.68 / 5.15 / 4.58 / 132 ms | 0.94 / 2.94 / 2.36 / 2.82 / 130 ms |

100,000 events: 99,000 loaded in 500-row transactions, then 1,000 timed
single-row INSERTs. The old layout's load took 40 minutes, so its INSERT and
query phases ran with the other two variants already finished; the INSERT and
query rows below therefore come from runs of each variant alone, and the
unindexed p99 varied between 12 and 29 ms across repeated runs on this host.

| | no index | old layout | new layout |
| --- | --- | --- | --- |
| index relation rows | – | 6,953,348 | 157,520 |
| estimated resident row bytes, all tables | 52.78 MB | 799.60 MB | 88.66 MB |
| of which the index | – | 746.82 MB | 35.87 MB (21x less) |
| full checkpoint file `relational.N.hawdb` (batched load) | 47.98 MB | not measured | 73.87 MB (index 25.9 MB) |
| database directory after checkpoint | 322.2 MB | 41.2 GB | 1.80 GB (23x less) |
| of which WAL / other files | 58.5 / 263.7 MB | 0.44 / 40.8 GB | 0.18 / 1.63 GB |
| 99,000-row batched load | 5.5 s | 2,433 s | 202 s |
| INSERT p50 / p99 (mean) | 6.3 / 28.5 (9.6) ms | 8.4 / 13.0 (55.3) ms | 7.6 / 28.7 (14.8) ms |
| `w42` / `deploy failed` / `数据库 搜索` | – | 16.0 / 68.1 / 118.5 ms | 4.6 / 24.6 / 19.4 ms |
| `WHERE stream = 's0'` / `'s3'` | – | 80.9 ms / 6.30 s | 19.8 ms / 3.22 s |
| reopen + first query after checkpoint | 0.32 s | 9.84 s | 0.48 s |

What the numbers say:

- Resident memory and checkpointed index bytes drop by more than an order of
  magnitude: the index's resident cost at 100,000 events is 36 MB rather than
  747 MB, and the database directory 1.8 GB rather than 41 GB. Queries are
  faster at 100,000 events, and reopen plus the first query takes 0.48 s
  instead of 9.8 s.
- Single-row INSERT p50 and p99 stay within 1.2x of the unindexed table at
  both sizes; means are 1.2x (10,000) and 1.55x (100,000). The means include
  automatic checkpoint pauses, which write a larger relational state when the
  index exists (the old layout's 100,000-event mean is 55 ms for the same
  reason).
- WAL bytes do not shrink for one-row transactions (73.9 MB against 71.8 MB
  at 10,000 events): each WAL record carries the primary keys every relation
  changed, and an insert still changes one index row per distinct term (about
  70 here) in either layout. In batched loads they do shrink (0.18 GB against
  0.44 GB at 100,000 events), because a term's dictionary row changes once per
  transaction rather than once per posting. The other files include the
  checkpoint artifacts also stored under `objects/` and retained earlier
  generations; `reclaim_branch_storage` brings the 100,000-event directory
  (batched load) to 1.00 GB against 0.29 GB without the index.
- Bulk loading in large transactions is the slow path: every index row write
  copies its row page and the page directory, so a 500-row transaction that
  touches about 5,000 terms costs about a second at 100,000 events (the old
  layout wrote about 35,000 rows per such transaction). Reducing that per-row
  write cost is a storage-engine change outside this layout.
- A filtered query whose `WHERE` rejects every candidate still hydrates them
  all; a candidate-side filter remains future work.

### `Bm25IndexScan` versus the in-function WAND

The same harness and release profile on one host, run back to back:
"before" is layout 2 with Block-Max WAND inside the `text_search` table
function (main at `76ff45f9d`, the first released ranking), "after" is
layout 3 with `Bm25IndexScan` (this revision). The host was shared with
other workloads (load average about 20 during the runs), so differences
under about 2x on live (uncheckpointed) queries are noise; reopened queries
were steadier. "Live" queries run after the single-row inserts, before any
checkpoint; "reopened" ones after checkpoint and reopen.

| 100,000 events (99k in 500-row transactions, 1,000 single INSERTs) | before | after |
| --- | --- | --- |
| index relation rows | 157,520 | 208,356 (+50,836 bounds rows) |
| estimated resident row bytes, all tables | 89.10 MB | 94.99 MB |
| 99,000-row batched load | 327 s | 252 s |
| INSERT p50 / p99 | 11.5 / 71.7 ms | 7.7 / 18.3 ms |
| live: `w42` / `deploy failed` / `数据库 搜索` (k = 20) | 3,001 / 136 / 149 ms | 9.9 / 19.3 / 30.5 ms |
| live: `deploy` / `deploy failed` (k = 1) | 109 / 129 ms | 10.9 / 14.6 ms |
| live: `WHERE stream = 's0'` / `'s3'` | 22.6 ms / 3.48 s | 19.1 ms / 4.50 s |
| reopened: `w42` / `deploy failed` / `数据库 搜索` | 17.1 / 62.1 / 83.8 ms | 3.9 / 9.1 / 9.7 ms |
| reopened: `deploy` / `deploy failed` (k = 1) | 61.2 / 65.4 ms | 5.5 / 8.0 ms |
| reopened: `WHERE stream = 's0'` / `'s3'` | 19.3 ms / 1.96 s | 8.7 ms / 1.59 s |

| 10,000 events, one INSERT per transaction | before | after |
| --- | --- | --- |
| index relation rows | 18,351 | 21,424 |
| INSERT p50 / p99 | 9.5 / 46.6 ms | 8.3 / 50.4 ms |
| reopened: `w42` / `deploy failed` / `数据库 搜索` | 2.1 / 5.8 / 6.7 ms | 1.4 / 2.2 / 2.4 ms |
| reopened: `deploy` / `deploy failed` (k = 1) | 4.9 / 5.9 ms | 0.9 / 1.4 ms |
| reopened: `WHERE stream = 's0'` / `'s3'` | 4.3 / 189 ms | 3.2 / 218 ms |

`EXPLAIN ANALYZE` at 100,000 events. Before reports postings decoded and
blocks read (block headers verified, payloads decoded); after reports the
same plus rounds and blocks never read:

| query | before: postings / blocks read | after: rounds, postings / blocks read | after: documents scored |
| --- | --- | --- | --- |
| `w42`, k = 20 | 600 / 5 | 1, 600 / 4 | 600 |
| `deploy failed`, k = 20 | 66,667 / 522 | 1, 66,667 / 520 | 33,433 (terminated early) |
| `数据库 搜索`, k = 20 | 100,002 / 783 | 1, 100,002 / 780 | 33,334 |
| `deploy`, k = 1 | 33,334 / 261 | 1, 33,334 / 260 | 33,334 |
| `数据库` `WHERE stream = 's0'`, k = 10 | 66,668 / 522 (exhaustive) | 1 (k = 40), 66,668 / 520 | 33,334 |
| `数据库` `WHERE stream = 's3'`, k = 10 | 66,668 / 522 (exhaustive) | 6 (k = 40,960), 400,008 / 3,120 over rounds | 200,004 |

What the numbers say:

- This corpus gives Block-Max WAND nothing to skip in either design: every
  event contains each of its English words exactly twice and has almost the
  same length, so every block of a term has the same bound, equal to the best
  score, and both scans decode the same postings. Pruning needs score
  variation across blocks (term-frequency bursts or length variation), as in
  the unit cases: on 100,000 synthetic documents with occasional repeated
  terms a one-row top-k decodes under 5% of the postings
  (`hawdb_executor::bm25`), and on 3,000 SQL rows of varying length most
  blocks are never read (`api::tests::search_indexes::scan`).
- With equal postings decoded, the reopened k = 20 and k = 1 queries are
  1.5x to 11x faster: the scan reads blocks and lengths rows in bounded
  forward windows instead of one block per range read, and ranks into a
  k-entry heap. Live queries on the large uncheckpointed overlay gain the most
  (`w42` from 3.0 s to 10 ms), because every range read there has a large
  fixed cost.
- A filter that accepts early (`s0`) now needs one round of `4k` instead of an
  exhaustive scoring. A filter that rejects every candidate (`s3`) still
  hydrates every match, and its over-fetch rounds add a third of a full
  ranking (4.5 s against 3.5 s live; 1.6 s against 2.0 s reopened).
- The bounds rows add about 32% index rows and 5.9 MB of estimated resident
  bytes at 100,000 events (about 50 bytes per sealed block); insert latency
  and the batched load did not get worse.

## Format

The relational schema encoding tags the derived-relation definition: `0` none,
`1` materialized view (unchanged layout), `2` search index (source table,
column, analyzer tag, physical layout tag; `cjk_ngram` is analyzer `1`, the
layout above is layout `3`). The engine-wide `PERSISTENT_FORMAT_VERSION`
advances from `1` to `2` because layout `3` refuses search indexes created with
layouts `1` or `2`. Databases without those older search indexes remain
readable. Individual codec versions and their strict unsupported-format
refusals remain authoritative.

Before upgrading a database with an older search index, export a
[logical dump](LOGICAL_DUMP.md) with the library that can still open it, then
import with the new library. The dump carries the index definition rather
than postings, so import rebuilds layout `3`. An older database cannot first
be opened by the new library merely to drop its unsupported index. Disposable
development databases may instead be recreated under the development-phase
format policy; no in-place compatibility migration is added.

## Out of scope

- Vector and hybrid scoring (#1471, #293); phrase, prefix, fuzzy and boolean
  query syntax; per-field weights; highlighting and snippets.
- Joins in the same SELECT as `text_search`, a `WHERE`-clause match operator
  over the base table (the scan is reached through `text_search` only), and
  plan reuse for prepared `text_search` statements.
- Filters evaluated inside the index (filter columns stored with the
  postings); `WHERE` is a post-filter with bounded over-fetch.
- Multi-column indexes, non-TEXT columns, strict append source tables, and a
  search index over a materialized view.
- SIMD bit-packing of posting blocks, and impact-ordered or quantized block
  maxima; blocks hold delta varints with exact maximum frequencies and
  conservative minimum lengths.
- A dictionary analyzer (for example the graph search kernel's Jieba-based
  analyzer); `cjk_ngram` is dictionary-free and recall-oriented.
- Background or chunked backfill of large existing tables.

## Tests

`api::tests::search_indexes` (root general unit lane) checks every query
against a brute-force BM25 oracle over `SELECT * FROM events` with the same
analyzer, requiring bit-equal scores, through inserts, updates (including
primary-key moves and NULL), deletes, upserts, drop/re-create backfill,
explicit transactions and rollback, WAL replay, checkpoint reopen, torn WAL
tails (strict refusal and repair), shadow/demand-paged index modes,
out-of-core residency, authoritative refusal, branch isolation, concurrent
writers, admission refusal (including a large pseudo-random CJK document
refused with its row) and coexistence with a materialized view. Layout cases
check exact block shapes as tails seal, changes to older documents at block
boundaries, a block split by one large transaction, many updates of one
document keeping its ordinal, and delete-heavy churn compacting blocks until
the relation is empty; every layout check also verifies each bounds row
against its block and the lengths of its documents.

`api::tests::search_indexes::scan` covers the access path: exact top-k equal
to the oracle (ids and score bits) for a named shortening/deletion case
across a block boundary and a residual filter; an ignored manual campaign
retains the generated Zipf-like corpora, queries, `k`, filters and churn; a one-row top-k over 3,000 documents decoding a small fraction of the
postings and skipping most blocks unread (from `EXPLAIN ANALYZE`); filters by
bounded over-fetch (more than one round, the stream's own top-k, a bounded
number of hydrated candidates, and a filter nothing satisfies); LIMIT
pushdown only in rank order; the EXPLAIN operator tree and ANALYZE fields;
and the concurrency footprint (a source writer does not conflict with writers
of other tables or other rows; a serializable `text_search` read conflicts
with a concurrent source change and with nothing else).

`hawdb_executor::bm25` ordinary units cover empty inputs, zero limits,
gapped postings, conservative length bounds and key-ordered ties. A focused
100,000-document case checks that a one-row top-k decodes under 5% of postings.
The 400-corpus differential campaign (random block sizes, conservative bounds
and keys unrelated to ordinals) is an ignored manual test. The generated SQL
corpora/churn campaign is also ignored; replay commands and seeds are in
[TEST_INDEX](TEST_INDEX.md).

`hawdb-relational` units check admission of the complete scan state. One
512 KiB budget serves a top-10 over both 4,096 and 16,384 matches; this fixture
demonstrates pruning under one budget, not memory independence from match
count. The full-ranking scan needed 56 bytes per match, 917 KB for the larger
fixture. A sparse-after-deletion case checks refusal before decoded lengths
arrays exceed admission. The scan also honours cancellation and never reads
skipped block payloads.
Storage and SQL units cover the analyzer (streamed document terms equal to
the materialized tokens, and an allocation-observed refusal of a
1,000,000-ideograph value before expansion), the layout codec (varint widths
and refused spellings, block/bounds/dictionary/statistics/lengths round trips
and corrupt inputs), the schema codec with unknown analyzer and layout tags,
and the grammar. The Bun suite runs the same SQL through `querySQL` on a
persistent database and a read-only reopen.
