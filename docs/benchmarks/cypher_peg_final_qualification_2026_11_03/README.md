# Combined PEG qualification candidate

This runtime was measured before commit and is based on `3f9271bc738035fff73d6a2cc890398ecf238b76`.
Runtime source hashes bind every reported Cypher measurement; the MVCC correction
has its own exact source hashes. Native and all eight stack checks now pass. Final throughput, committed-head
fuzz/CI and public-library timing evidence remain pending. This record does not
accept any remaining diagnostic or resource transition.

`heap-summary.json` uses the unchanged native release probes and frozen 163 heap
plus 32 boolean controls. Private Cypher tokens use 8 bytes, with a u32 start and
24-bit length-minus-one for nonempty tokens. EOF keeps its full offset and zero
length. The input cap remains 16 MiB; no shared SQL token or public AST changes.
All 195 controls are nonincreasing against the preceding PEG runtime. Four wide
metrics and sixteen boolean metrics still regress against former main. These
are requested allocator counts/bytes and live bytes, not RSS or a throughput
result. The original probe sources and inputs remain in the preceding compact
measurement directory; no measured workload or allocator was changed.

`diagnostic-invariants.json` independently recomputes byte boundaries, Unicode
line/column and the exact escaped 24-scalar context for all 10,845 Parse errors.
It includes every remaining 2,336 difference, without accepting changed labels,
anchors, validation order or admission. The full paired transition index and
source-review dispositions are in the adjacent diagnostic review directory.
The fifth provenance-bearing fixture restores 187 additional independent-main
outcomes. All 21,944 AST/span/class outcomes and previously matching diagnostics
remain unchanged. `vector-return-controls.json` records another 50 unchanged
controls: no class change or lost reference match, six additional exact control
repairs, and fourteen diagnostic differences. Control sets overlap; do not add
their repair counts to the main corpus count.

`project-graph-string-review.json` explains two retained transitions: the removed
parser accepted an arbitrary next character as a string delimiter. Its error
on a non-string `[1]` option consequently said unterminated string at EOF.
The PEG lowering retains the existing precise string-token rejection instead.
This source-based proposal remains pending maintainer disposition.

`mvcc-baseline-failure.json` preserves a real qualification failure on both the
PEG head and independently built current main `870a6edd77fa0913dd8ff47bbb200d67a80cd364`.
A same-value point UPDATE committed before a stale reader's broad UPDATE, but
table-lock validation missed the retained primary-key write intent. The original
seed42/case147 is checked in under the existing MVCC tool's cases directory.
`mvcc-correction.json` records a focused, unpublished correction: table guards
also validate the existing bounded primary-key version range, retaining all
metadata/stamp admission limits. Seven storage tests and eighteen concurrent API
tests pass; the new API regression failed before the fix. The exact saved replay,
30-second optimistic campaign (56,234 cases), pessimistic campaign (164 cases,
all twelve families and both terminal directions), and range campaign (34,570
cases, all ten families) pass. The stored source revision is the base checkout;
modified file hashes, prototype status and commands bind the actual tested code.
No WAL, checkpoint, persistent format or durability behavior is changed.

`native-and-stack-summary.json` records the exact native commands and all eight
small-stack outcomes at runtime tree `893beda491ac25bf584bcfe05b31b593a80671eb`.
Formatting, strict workspace Clippy, full prek, query15, cache26, Cypher255 (one
ignored), SQL114 (four ignored), SQL-syntax36, storage7, concurrent18, portable14
and documented minimal WASM strict Clippy pass. The token-size assertion also
compiles for WASM; browser execution is not claimed.

`parser-timing-environment-failure.json` records an unqualified attempt. While
other compiler/test processes were active, process observation exceeded the
existing two-second guard before timed samples began. The guard and benchmark
protocol were not weakened; this is neither a throughput pass nor a regression.
The previously published 3f result retains its own source scope. The unchanged
protocol must be repeated on a quiet build/test environment for this runtime.

All 73 remaining source-mechanism reviews and full paired proposal data are in
the adjacent diagnostic review directory. Every maintainer disposition remains
pending. `vector-return-cases.jsonl` also exposes all fifty control inputs and
both rendered outcomes, including fourteen remaining diagnostic differences.
