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
2. Every matching document is scored exactly with Okapi BM25 (`k1 = 1.2`,
   `b = 0.75`, `idf = ln(1 + (N - n + 0.5) / (n + 0.5))`), using the corpus
   statistics of the same snapshot. Nothing is approximated.
3. Candidates are visited in descending score order, ties by encoded primary
   key, and hydrated from the same snapshot. `WHERE` is applied to each; the
   first `top_k` rows that satisfy it are the result. Filtering therefore
   happens before the top-k cut: a selective `WHERE e.stream = $2` still returns
   up to `k` rows from that stream.
4. `ORDER BY`, `LIMIT` and `OFFSET` then apply to those rows. Without
   `ORDER BY`, rows arrive in score order.

Joins, grouping, aggregates, `DISTINCT` and locking clauses are refused with
`text_search` in this release; filter on the source columns directly instead
of joining back to the source table. `EXPLAIN` reports the search index access
without executing it; `EXPLAIN ANALYZE` also reports `postings_visited`,
`posting_blocks_read` and `posting_blocks_skipped`. Inside an explicit
transaction, `text_search` reads the transaction's own staged writes, like any
other SELECT.

Without a residual `WHERE`, ranking is a storage-native exact Block-Max WAND
scan once the query terms cover at least 4,096 postings. It keeps one posting
block per term, one lengths block and a `top_k` score heap; it does not build a
score map or materialize every posting. Per-block maximum term frequency gives
a safe BM25 upper bound. Cursors advance only to the earlier of the WAND pivot
and the first current-block boundary, and only a bound valid for all remaining
postings may terminate the scan. Strict comparisons preserve equal-score
primary-key tie breaking. Smaller inputs use exhaustive streaming because its
bound bookkeeping costs more than it saves.

A residual `WHERE` still uses exhaustive scoring in this release, then hydrates
rows in rank order until `top_k` rows pass the predicate. This deliberately
preserves filter-before-top-k semantics; the WAND heap must not be cut before a
source-row predicate has run. Work remains bounded by the ordinary SQL read
limits: decoded postings and hydrated rows count against candidate work, and
query terms, cursor buffers, the score heap and retained identities are charged
to the query memory ledger before allocation. Exceeding a limit fails the
query; it never returns a truncated ranking.

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

## Maintenance, persistence and recovery

The index is a synchronous derived relation built on the
[materialized view](MATERIALIZED_VIEWS.md) maintenance kernel. Its rows live in
a hidden RowPage relation with a compact postings layout (see
[Physical layout](#physical-layout)). A document with a NULL or token-free
column is not part of the corpus.

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
- **Concurrency.** Like other derived relations, a transaction writing a
  source with a search index takes a database-wide version footprint for
  optimistic validation, and `text_search` reads record a database-wide read
  dependency, because BM25 statistics depend on every document.

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
| 3 term dictionary | term (UTF-8) | `i64::MAX` | document frequency, sealed block count, tail start, tail postings |
| 3 sealed block | term (UTF-8) | fence | posting count, first/last ordinal, maximum term frequency, postings, CRC32C |

- **Ordinals.** A document entering the corpus gets the next dense integer
  ordinal, in source-key order within a commit; ordinals are never reused,
  and an update keeps its document's ordinal. WAL replay assigns the same
  ordinals. Posting lists carry ordinals, not primary keys; the lengths rows
  map an ordinal back to its primary key and store each document's length
  once, not once per posting.
- **Posting lists** are a count followed by `(ordinal delta, term frequency)`
  pairs, sorted by ordinal. The first delta is relative to the list's base
  and every later delta is at least 1.
- **Verified block bounds.** Every sealed block records its first and last
  ordinal and maximum term frequency before the posting pairs. A trailing
  CRC32C covers both the bounds and the complete posting payload. WAND can
  therefore trust a checksummed bound and skip posting decoding without letting
  torn or bit-corrupt bytes pass silently; a selected block also checks
  that decoded first/last/max values exactly match its header.
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
- **Updates and deletes rewrite blocks in place; there are no tombstones.**
  A change to an older document rewrites the one block that owns its ordinal
  (at most 256 postings, a few hundred bytes), so BM25 statistics stay exact
  without a dead-posting filter, nothing accumulates, and the cost per commit
  is bounded by the number of changed postings, not by a term's document
  frequency. A block that grows past 256 postings splits into 128-posting
  blocks; a changed block that shrinks below 32 merges into a neighbour when
  the two fit in 128, so delete-heavy churn cannot leave a run of nearly
  empty blocks. When a term's last posting goes, its rows go; when the last
  document goes, the relation is empty.

Tombstones were rejected: exact BM25 needs live document frequencies and
document counts anyway, so a delete must touch every one of its terms'
dictionaries regardless, and tombstoned postings would need a filter on every
query plus a compaction whose cost grows with a term's frequency.

`text_search` reads the statistics and dictionary rows, then advances each term
through ordered sealed-block ranges. It verifies a block header before deciding
whether its posting payload must be decoded, and point-reads one lengths block
at a time as candidate ordinals advance. Scores use the same floating-point
operations in analyzer term order as a brute-force evaluation, so they are
bit-for-bit equal. Decoders check counts, ordering, dictionary bounds, block
bounds and checksums and refuse an inconsistent index as a storage-integrity
error.

### Measurements

These measurements predate layout tag 2 and the WAND executor; they describe
the tag-1 compact-block baseline retained by PR #1670 and must not be read as a
performance claim for the current head. Temporary local harness (not
committed), repository release profile, macOS
arm64, persistent database with the default `SyncOnEveryWrite`. Each event row
has about 30 English words, 30 distinct `wNNNN` tokens and two short Chinese
sentences, about 70 postings. "Old" is the previous revision of this change
(one row per `(term, document)`); "new" is the tag-1 compact-block layout
measured before the tag-2 bounds/checksum addition. The three
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

## Format

The relational schema encoding tags the derived-relation definition: `0` none,
`1` materialized view (unchanged layout), `2` search index (source table,
column, analyzer tag, physical layout tag; `cjk_ngram` is analyzer `1`, the
layout above is layout `2`). Existing databases read unchanged. Older binaries
refuse a schema with tag `2`, and current binaries refuse unknown tags,
analyzer tags or layout tags as an unsupported format rather than guessing.
No codec version changes, following the materialized view MIN/MAX precedent
and HawDB's development-phase format policy.

## Out of scope

- Vector and hybrid scoring (#1471, #293); phrase, prefix, fuzzy and boolean
  query syntax; per-field weights; highlighting and snippets.
- Joins in the same SELECT as `text_search`.
- Multi-column indexes, non-TEXT columns, strict append source tables, and a
  search index over a materialized view.
- SIMD bit-packing of posting blocks and predicate-aware WAND. A residual
  `WHERE` currently uses the exhaustive exact scorer so filtering remains
  before the top-k cut.
- A dictionary analyzer (for example the graph search kernel's Jieba-based
  analyzer); `cjk_ngram` is dictionary-free and recall-oriented.
- Background or chunked backfill of large existing tables.

## Tests

`api::tests::search_indexes` (root general unit lane) checks every query
against a brute-force BM25 oracle over `SELECT * FROM events` with the same
analyzer, requiring bit-equal scores, through inserts, updates (including primary-key moves and NULL),
deletes, upserts, drop/re-create backfill, explicit transactions and rollback,
WAL replay, checkpoint reopen, torn WAL tails (strict refusal and repair),
shadow/demand-paged index modes, out-of-core residency, authoritative refusal,
branch isolation, concurrent writers, admission refusal (including a large
pseudo-random CJK document refused with its row) and coexistence with a
materialized view. Layout cases check exact block shapes as tails seal,
changes to older documents at block boundaries, a block split by one large
transaction, many updates of one document keeping its ordinal, and
delete-heavy churn compacting blocks until the relation is empty. Storage and
SQL units cover the analyzer (streamed document terms equal to the
materialized tokens, and an allocation-observed refusal of a 1,000,000-ideograph
value before expansion), exact WAND/exhaustive score-bit parity across block
boundaries, a bounded top-k heap succeeding under a budget that refused the
former score map, and observable losing-block skips. The layout codec covers
varint widths, checksums, bounds, refused spellings, block/dictionary/statistics/
lengths round trips and corrupt inputs. Further units cover the schema codec
with unknown analyzer and layout tags and the grammar. The Bun suite runs the
same SQL through `querySQL` on a
persistent database and a read-only reopen.
