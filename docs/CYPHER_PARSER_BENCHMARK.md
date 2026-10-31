# Cypher parser benchmark

HawDB keeps parser changes evidence-driven. The production benchmark covers
short exact lookup, bounded expansion, aggregate pagination, mutation, and
query-hint shapes while constructing the complete production AST.

Run the production parser benchmark with:

```bash
cargo bench -p hawdb-cypher --bench cypher_parser
```

The retired single-query Yacc experiment has been removed. It did not cover
the production grammar or establish a migration decision. Cypher now uses the
shared token PEG engine for all supported syntax, with its own grammar and AST
transformation. Completing the source migration does not establish performance
readiness. Qualification against the former production parser still requires:

- equal AST and rejection behavior over the production query corpus;
- differential fuzz coverage for valid and invalid inputs;
- no allocation or error-path regression;
- at least 20 percent parser throughput improvement on the weighted corpus;
- a measurable end-to-end query CPU or latency improvement.

SQL and Cypher share the [owned token PEG engine](SQL_PEG_PARSER.md); their
grammars and transformations remain separate. The handwritten parsers and
native matcher callbacks have been removed. Current measurements show a parser
throughput regression against independently built main baselines, so the
performance criteria above remain unmet. Track qualification in
[#1375](https://github.com/nowledge-co/hawdb/issues/1375); source routing alone
does not waive those criteria.

The asynchronous runtime prepares each query once. It reuses the prepared AST
and physical plan only while the optimizer schema and statistics epoch match;
otherwise it keeps the AST and replans against the execution snapshot.
