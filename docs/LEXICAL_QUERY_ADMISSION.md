# Lexical query score ownership

Lexical query streams and retained scores now use two operation-scoped accounts
under one `QueryMemoryLedger`. The root is capped by the existing configured
lexical query limit and, when supplied, the task's admitted memory reservation.
The score account also honors the task's result limit. Zero working/result
reservations do not become unlimited. Accounts are not created per score or row.

The existing stream envelope is reserved before dictionary I/O and collector
allocation. The lease remains live through cursor/lookup cleanup, delta scoring
and top-k conversion; its existing component formula is not a new measurement
of every allocation performed by those algorithms. Shared-root rejection keeps
already-held owners charged. Component and root rejection remain fail-closed.

The collector pre-admits heap capacity and owned score IDs plus the existing
256-byte per-entry container/conversion allowance. Shared reservation failure
does not change the collector's charged total or replace its previous results.
When a smaller top-k entry replaces a larger one, the previous ID drops before
the excess charge is released. Delta ID copies have separate incoming scratch
admission before cloning, held until the collector takes ownership.

`AdmittedScores` couples the returned map to its lease with data-before-lease
field drop order. It exposes only a borrowed map, not an uncharged consuming
iterator or mutable map. The wrapper is not `Clone`: a new owned map would need
its own reservation. Moving the scores out of `LexicalQueryReport`, dropping the
query root handle, or dropping the projection reader cannot release still-live
score ownership. Both resident-index and out-of-core consumers preserve this
owner while ranking/hydration use the scores. The resident fallback remains a
separate, uninstrumented path; it never mutates an admitted score map.

## Verification

Normal regressions cover ownership beyond report/query/reader drop, exact and
one-short shared roots, repeated scoring while a previous result is retained,
cross-account rejection before dictionary I/O, cancellation/consumer failure,
top-k replacement rollback and shrinking, zero windows, absent terms and zero
task limits. Every finished/error path releases its tracked capacity; the root
retains exactly two account records.

The explicit local `skein_search_lexical_query_memory_fuzz_tests` target runs
512 deterministic base/delta/filter/window cases (seed `0x2065c0e`) against an
independent document-frequency/BM25 oracle. Competing live work remains charged
during exact and one-short retries. Returned results keep their charge until
drop; cancellation also releases actual cache pins. This target is manual and
ignored in ordinary test runs, not an additional CI job.

## Remaining full-issue boundaries

This connects the existing lexical stream/score allowances to a shared owner;
it does not establish complete query memory or RSS coverage. Candidate metadata,
spill/cache buffers and vector allowlists now share that root as described in
`CANDIDATE_QUERY_ADMISSION.md`. Raw vector sidecars and retained vector scores
also share that root (`VECTOR_QUERY_ADMISSION.md`). RaBitQ candidate generation,
predicate pruning/report containers, ranking/fusion copies, hydration and public
result payloads still need complete ownership under the outer query root.
Persistent delta maps and tokenizer/dependency workspaces remain separate owners.
The task reservation describes admitted bytes, not a newly acquired governor
permit; no global host budget is invented here.

Published-reader reopen also remains incomplete. Its current public open entry
points take configuration and a cache but no host memory owner. A temporary
query lease or another independent reader budget is not a substitute for an
explicit host-lifetime ownership contract. No public signature, storage format,
v1 version, dependency, backend, Bazel runtime setting or fuzz CI was changed.
The Jieba maintenance decision, exact-head native verification and measured
representative-corpus posting reduction remain full #206 acceptance gates.
