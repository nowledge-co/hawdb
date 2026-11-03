# Cypher diagnostic transition triage

The predicate/punctuation and diagnostic-buffer repair is based on
`a6c936c2a380e17768352c227e9299cca3dd8e0f`. Fresh paired capture preserves all
21,944 independent AST/span/class outcomes and every previously matching error.
It restores1,590 more exact diagnostics, leaving2,523 indexed differences.
Source and full-rendered hashes are in `transition-index.json`.

The prior4113-record derived index was checked against complete current parent
capture before replacement. Each remaining group now has full paired examples.
Buckets guide source review; they are not accepted behavior changes or approval.

| Bucket | Records |
| --- | ---: |
| Admission and validation-order transitions | 1105 |
| Context-specific expectation transitions | 1022 |
| CASE keyword commitment | 180 |
| Unlabelled terminal or alternative expectation | 128 |
| Location-only transitions | 84 |
| Identifier expectation and semantic validation order | 4 |

| Group | Records | Reference | Candidate | Review status |
| --- | ---: | --- | --- | --- |
| D001 | 619 | Cypher parser nesting exceeds limit of 32 | Cypher parser nesting exceeds limit or PEG resource budget | Pending source review |
| D002 | 404 | Cypher parser nesting exceeds limit of 32 | Cypher parser nesting exceeds limit of 32 | Pending source review |
| D003 | 226 | expected '=' | expected '.' | Pending source review |
| D004 | 180 | expected identifier | expected CASE expression | Pending source review |
| D005 | 143 | expected '=' | expected IS | Pending source review |
| D006 | 128 | expected value | expected a Cypher value token | Pending source review |
| D007 | 73 | expected value | expected CASE | Pending source review |
| D008 | 72 | expected identifier | Cypher parser nesting exceeds limit of 32 | Pending source review |
| D009 | 64 | expected ',' | expected ')' | Pending source review |
| D010 | 56 | expected identifier | Cypher parser nesting exceeds limit or PEG resource budget | Pending source review |
| D011 | 52 | expected '=' | unexpected trailing input | Pending source review |
| D012 | 36 | expected identifier | expected shortestPath | Pending source review |
| D013 | 32 | expected '=' | expected '=' | Pending source review |
| D014 | 28 | expected ',' or '}' | expected '.' | Pending source review |
| D015 | 28 | expected := | expected ':' | Pending source review |
| D016 | 24 | arithmetic expression exceeds maximum nesting depth | arithmetic expression exceeds maximum nesting depth | Pending source review |
| D017 | 20 | expected '=' | expected IN | Pending source review |
| D018 | 19 | expected identifier | expected ':' | Pending source review |
| D019 | 18 | COUNT(DISTINCT *) is not supported | COUNT(DISTINCT *) is not supported | Pending source review |
| D020 | 18 | IS NULL in a projection requires a non-aggregate operand | IS NULL in a projection requires a non-aggregate operand | Pending source review |
| D021 | 18 | expected '=' | expected a comparison | Pending source review |
| D022 | 17 | expected '(' | expected INDEX | Pending source review |
| D023 | 16 | expected ')' | expected '.' | Pending source review |
| D024 | 14 | expected ',' | expected '.' | Pending source review |
| D025 | 14 | expected END | expected '.' | Pending source review |
| D026 | 14 | expected THEN | expected '.' | Pending source review |
| D027 | 14 | unterminated string | expected ',' or '}' | Pending source review |
| D028 | 12 | expected ',' or ']' | expected IN | Pending source review |
| D029 | 12 | expected integer digits | expected '.' | Pending source review |
| D030 | 8 | expected '(' | expected a Cypher schema token | Pending source review |
| D031 | 8 | expected ')' | expected a Cypher schema token | Pending source review |
| D032 | 8 | expected identifier | expected ',' or ']' | Pending source review |
| D033 | 6 | expected '(' | expected '.' | Pending source review |
| D034 | 6 | expected MATCH | expected '.' | Pending source review |
| D035 | 6 | expected WHEN | expected '.' | Pending source review |
| D036 | 6 | unexpected trailing input | aggregate list functions do not support postfixes | Pending source review |
| D037 | 6 | unexpected trailing input | expected adjacent tokens | Pending source review |
| D038 | 6 | unterminated string | expected ')' | Pending source review |
| D039 | 5 | expected NODE or RELATIONSHIP | expected NODE | Pending source review |
| D040 | 4 | bounded relationship pattern requires a finite max hop | expected adjacent tokens | Pending source review |
| D041 | 4 | expected ',' or ']' | expected '.' | Pending source review |
| D042 | 4 | expected identifier | expected END | Pending source review |
| D043 | 4 | unexpected trailing input | expected ',' | Pending source review |
| D044 | 3 | bounded relationship pattern requires a finite max hop | bounded relationship pattern requires a finite max hop | Pending source review |
| D045 | 3 | expected ',' | vector search RETURN must be id, score | Pending source review |
| D046 | 3 | vector search RETURN must be id, score | vector search RETURN must be id, score | Pending source review |
| D047 | 2 | Cypher parser nesting exceeds limit of 32 | expected IS | Pending source review |
| D048 | 2 | VECTOR dimension must be a positive u32 | VECTOR dimension must be a positive u32 | Pending source review |
| D049 | 2 | expected ',' | expected identifier | Pending source review |
| D050 | 2 | expected ',' | unsupported procedure option value | Pending source review |
| D051 | 2 | expected ',' or ']' | expected a decimal point | Pending source review |
| D052 | 2 | expected ',' or ']' | expected adjacent tokens | Pending source review |
| D053 | 2 | expected ',' or '}' | expected a decimal point | Pending source review |
| D054 | 2 | expected ',' or '}' | expected adjacent tokens | Pending source review |
| D055 | 2 | expected '=' | expected ')' | Pending source review |
| D056 | 2 | expected ']' | expected adjacent tokens | Pending source review |
| D057 | 2 | expected := | expected '=' | Pending source review |
| D058 | 2 | expected END | expected adjacent tokens | Pending source review |
| D059 | 2 | expected identifier | expected ')' | Pending source review |
| D060 | 2 | expected identifier | expected a graph algorithm | Pending source review |
| D061 | 2 | expected identifier | expected scalar value | Pending source review |
| D062 | 2 | expected string | expected ')' | Pending source review |
| D063 | 2 | expected value | expected ')' | Pending source review |
| D064 | 2 | projected node predicates are not supported | projected node predicates are not supported | Pending source review |
| D065 | 2 | relationship min hop must not exceed max hop | relationship min hop must not exceed max hop | Pending source review |
| D066 | 2 | shortest-path functions do not accept a nested SHORTEST selector | shortest-path functions do not accept a nested SHORTEST selector | Pending source review |
| D067 | 2 | unexpected trailing input | expected a decimal point | Pending source review |
| D068 | 2 | unexpected trailing input | expected the end of a property reference | Pending source review |
| D069 | 2 | unexpected trailing input | unexpected trailing input | Pending source review |
| D070 | 2 | unterminated string | expected string | Pending source review |
| D071 | 2 | vector search RETURN must be id, score | expected identifier | Pending source review |
| D072 | 1 | expected ',' | unexpected trailing input | Pending source review |
| D073 | 1 | expected LABEL | expected a schema table, node label, or relationship type | Pending source review |
| D074 | 1 | expected TYPE | expected a schema table, node label, or relationship type | Pending source review |
| D075 | 1 | expected identifier | expected ',' or '}' | Pending source review |
| D076 | 1 | expected identifier | unexpected trailing input | Pending source review |
| D077 | 1 | expected identifier | vector search RETURN must be id, score | Pending source review |
| D078 | 1 | expected schema object state | expected DELETE_ONLY | Pending source review |
| D079 | 1 | fulltext index expects one property | index expects one property | Pending source review |
| D080 | 1 | range index expects one property | index expects one property | Pending source review |
| D081 | 1 | unexpected procedure RETURN column | expected ',' | Pending source review |
| D082 | 1 | unexpected trailing input | vector search RETURN must be id, score | Pending source review |
| D083 | 1 | vector search YIELD must be id, score | vector search YIELD must feed a MATCH read query | Pending source review |

The unchanged five-query release benchmark is recorded in `parser-timing.json`: eight counterbalanced processes, 11 samples, 20,000 iterations; all five improve, with 24.4622 percent equal-weighted throughput gain against former main. The source hashes bind the measured runtime before documentation additions. Native checks pass, including 254 Cypher tests (one ignored), strict workspace Clippy, full prek, portable14 and minimal WASM strict Clippy. Final-head fuzz/CI/public E2E results are separate from this incremental evidence.
