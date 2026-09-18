# Cypher parser benchmark

HawDB keeps parser changes evidence-driven. The production benchmark covers
short exact lookup, bounded expansion, aggregate pagination, mutation, and
query-hint shapes while constructing the complete production AST.

Run the production parser benchmark with:

```bash
cargo bench -p hawdb-cypher --bench cypher_parser
```

The isolated Yacc experiment compares a generated `lrpar`/`lrlex` parser with
the production parser for a parameterized exact lookup. It rejects every parse
repair and verifies AST equality before collecting timing samples.

```bash
cargo run --release --manifest-path experiments/cypher-yacc/Cargo.toml
```

The experiment is deliberately outside the workspace so parser-generator build
dependencies do not affect production builds. Its result does not justify a
full migration by itself. A migration requires all of the following:

- an equal AST and equal rejection behavior over the production query corpus;
- differential fuzz coverage for valid and invalid inputs;
- no allocation or error-path regression;
- at least 20 percent parser throughput improvement on the weighted corpus;
- a measurable end-to-end query CPU or latency improvement.

The asynchronous runtime prepares each query once. It reuses the prepared AST
and physical plan only while the optimizer schema and statistics epoch match;
otherwise it keeps the AST and replans against the execution snapshot.
