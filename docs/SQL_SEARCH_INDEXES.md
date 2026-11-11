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
of joining back to the source table. `EXPLAIN` of a `text_search` query is
refused. Inside an explicit transaction, `text_search` reads the transaction's
own staged writes, like any other SELECT.

Work is bounded by the ordinary SQL read limits: postings visited by the
ranking phase and rows hydrated for `WHERE` count against the query's
candidate-work limit, and score state is charged to the query memory ledger.
Exceeding a limit fails the query; it never returns a truncated ranking.

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
a hidden RowPage relation keyed by `(term, encoded primary key)`: one posting
per distinct term per document holding the term frequency and document length,
plus one statistics row with the document count and total token count. A
document with a NULL or token-free column is not part of the corpus.

- **Commit.** For each source key a transaction changed, maintenance analyzes
  the old and new row images, deletes postings of terms that disappeared,
  writes changed postings, and adjusts the statistics row. Unchanged text costs
  no posting writes. Writes go through the same transaction, WAL record and
  commit acknowledgement as the source row; the index is never behind.
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

Admission follows the materialized view rules: posting writes and row reads
share the transaction's `RelationalMutationLimits` budget (250,000 row work
units and 64 MiB of logical payload by default). A document with `t` distinct
terms costs about `t` row units per insert, and a backfill costs about the
source row count plus the total distinct-term count. A transaction that exceeds
the budget is rejected before WAL append with its source rows; create the index
before bulk loading, or load in smaller transactions, when the corpus is large.

The same residency requirement applies as for materialized views: postings are
resident relational rows, and `RelationalIndexMode::Authoritative` refuses a
database containing a search index. Shadow, demand-paged and the default
materialized index modes, and `OutOfCore` residency with materialized postings,
are supported.

## Format

The relational schema encoding tags the derived-relation definition: `0` none,
`1` materialized view (unchanged layout), `2` search index (source table,
column, analyzer tag; `cjk_ngram` is `1`). Existing databases read unchanged.
Older binaries refuse a schema with tag `2`, and current binaries refuse
unknown tags or analyzer tags as an unsupported format rather than guessing.
No codec version changes, following the materialized view MIN/MAX precedent
and HawDB's development-phase format policy.

## Out of scope

- Vector and hybrid scoring (#1471, #293); phrase, prefix, fuzzy and boolean
  query syntax; per-field weights; highlighting and snippets.
- Joins in the same SELECT as `text_search`, and `EXPLAIN` output for it.
- Multi-column indexes, non-TEXT columns, strict append source tables, and a
  search index over a materialized view.
- Compact posting storage. Postings are one row each; resident memory grows
  with the total distinct-term count. A compact layout is the subject of #206.
- A dictionary analyzer (for example the graph search kernel's Jieba-based
  analyzer); `cjk_ngram` is dictionary-free and recall-oriented.
- Background or chunked backfill of large existing tables.

## Tests

`api::tests::search_indexes` (root general unit lane) checks every query
against a brute-force BM25 oracle over `SELECT * FROM events` with the same
analyzer, through inserts, updates (including primary-key moves and NULL),
deletes, upserts, drop/re-create backfill, explicit transactions and rollback,
WAL replay, checkpoint reopen, torn WAL tails (strict refusal and repair),
shadow/demand-paged index modes, out-of-core residency, authoritative refusal,
branch isolation, concurrent writers, admission refusal and coexistence with a
materialized view. Storage and SQL units cover the analyzer, the schema codec
and the grammar. The Bun suite runs the same SQL through `querySQL` on a
persistent database and a read-only reopen.
