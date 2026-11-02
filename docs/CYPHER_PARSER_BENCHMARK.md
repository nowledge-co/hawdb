# Cypher parser benchmark

HawDB keeps parser changes evidence-driven. The production benchmark covers
short exact lookup, bounded expansion, aggregate pagination, mutation, and
query-hint shapes while constructing the complete production AST.

Run the production parser benchmark with:

```bash
cargo bench -p hawdb-cypher --bench cypher_parser
```

The retired single-query Yacc experiment has been removed. It did not cover
the production grammar or establish a migration decision. Cypher now defaults
to a grammar compiled by the external `peg` crate for all supported syntax.
Projection and sort items lower directly after matching; other captures lower
to the same production AST. The old grammar builder and selection feature have
been removed. Completing the source migration does not establish performance
readiness. Qualification against the former production parser still requires:

- equal AST and rejection behavior over the production query corpus;
- differential fuzz coverage for valid and invalid inputs;
- no allocation or error-path regression;
- at least 20 percent parser throughput improvement on the weighted corpus;
- a measurable end-to-end query CPU or latency improvement.

SQL uses the [owned token PEG engine](SQL_PEG_PARSER.md); Cypher uses its compiled
`peg` grammar. The handwritten drivers and parser fallback have been removed.
Diagnostic measurements of the compiled grammar improve on the previous shared
matcher, but they do not establish the weighted throughput or end-to-end gates.
Valid wide-expression allocations and overdeep-input diagnostic differences
also remain under qualification. The performance criteria above remain unmet. Track qualification in
[#1375](https://github.com/nowledge-co/hawdb/issues/1375); source routing alone
does not waive those criteria.

The asynchronous runtime prepares each query once. It reuses the prepared AST
and physical plan only while the optimizer schema and statistics epoch match;
otherwise it keeps the AST and replans against the execution snapshot.

End-to-end memory measurements include planning and storage caches as well as
parser buffers. Root graph-algorithm leaves have fixed lowering/cost and no
logical input, so their cache lookup and bound trace refresh use maintained
counts and schema without requesting unrelated property histograms. The trace
reports this choice, physical templates remain reusable, and ordinary graph
queries retain their full-statistics path. This does not establish parser
throughput parity; the isolated RSS ceiling remains unchanged.
