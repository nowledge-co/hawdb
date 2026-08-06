# Cypher Yacc experiment

This standalone package measures a Yacc-generated parser against Skein's
production hand-written parser without adding parser-generator dependencies to
the workspace build.

The experiment intentionally covers one production-shaped hot path:
parameterized exact lookup with a projected property, alias, and result limit.
Both parsers must construct an equal `skein_cypher::Statement` before timing is
reported. Any lexer or parser repair is rejected.

Run it with:

```bash
cargo run --release --manifest-path experiments/cypher-yacc/Cargo.toml
```

This result is not sufficient to migrate the full grammar. A migration requires
a production-weighted corpus, full differential fuzz coverage, no allocation or
error-path regression, at least 20 percent parser throughput improvement, and a
measurable end-to-end query improvement.
