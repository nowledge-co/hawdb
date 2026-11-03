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

The published `d440b791f4ecc60c69fe29287ec389c04a572ef1` revision has completed
all 98 required local fuzz tests. Eight counterbalanced runs of the unchanged
five-case release benchmark against former-parser main
`fd10958dd37b8878151aeb73ff0477d189b5db5e` show a 17.2598 percent
improvement in equal-weighted throughput. Mean whole-process CPU is
3.45750525 seconds for the former parser and 2.94485875 seconds for PEG.
No concurrent compiler, test or benchmark process was observed during these
runs; other host activity was uncontrolled. This remains below the 20 percent
gate, and aggregate-page parsing still regresses slightly (4073.25 to 4160 ns).

The complete independent rendered-diagnostic comparison covers 21,944 records
and finds 5,621 message or location differences across 102 groups. The earlier
260 overdeep differences came from a narrower comparison. The follow-up repairs
693 independently exported CSV, string, arity, computed-UNWIND and integer-overflow outcomes;
other diagnostic transitions and wide-expression resource regressions remain
open. Frozen full-message fixtures preserve the independent main provenance.

The same main storage baseline and the unpublished follow-up were also compared
through `Database::query_with_params` on a 64-node in-memory fixture. Eight
counterbalanced runs of 11 samples with 2,000 calls per case, after warming the
database, measured exact lookup at 22,050.75 ns former / 22,255.5 ns follow-up,
and aggregate pagination at 61,563 ns former / 62,858.75 ns follow-up.
Complete output columns and row values were checked outside the timed regions.
These default-cache measurements show no end-to-end improvement. They do not
qualify the end-to-end gate or isolate cold query preparation.

A repeat of the same eight-run parser protocol on `b607f4b2f9d0` measures
16.6779 percent equal-weighted throughput improvement (3.46220475 seconds
former process CPU / 2.96796775 seconds PEG). This also remains below the
20 percent requirement. The further diagnostic follow-up restores another
83 full-message and location outcomes from the independent main export.

Rank CASE lowering validates the complete matched reference/literal shape before
allocating its specialized AST. Other shapes retain the existing generic AST
lowering. This avoids constructing a generic CASE tree before normalization.
The wide rank control changes from 168 allocations / 70,174 requested bytes /
31,116 peak live bytes to 51 / 36,541 / 19,709. Former main's peak is 15,153
bytes, so this control still has a peak regression. An additional 2,026
independent main outcomes preserve complete ASTs/spans and rejection classes;
these resource results do not establish the remaining readiness gates.

The shared query-preparation constructor computes identity normalization and the
value-preserving plan-cache key from one scan. The two existing normalization
contracts and digest/hash protocols remain unchanged; 27,280 outcomes match an
independently compiled main reference. Capability and access checks precede this
work. The existing graph-algorithm statistics isolation remains in place.

A [32-process confirmation](benchmarks/cypher_peg_preparation_2026_11_03/public-query-confirmation.json)
uses the unchanged two-query public fixture, 64 rows, normal warm-cache policy,
11 samples and 2,000 calls per sample. Sixteen counterbalanced pairs show point
lookup throughput improving by 3.0720 percent (95 percent t interval 2.2654 to
3.8786); aggregate pagination is unchanged within noise (-0.0241 percent,
interval -0.3708 to 0.3227). Their equal-weighted combined improvement is 0.7676
percent, interval 0.5251 to 1.0102; all sixteen paired combined gains are positive.
Mean process CPU is 1.8459808125 seconds former / 1.8299754375 seconds candidate.
These intervals describe this experiment, not other machines or workloads.
No compiler/test/benchmark interference was observed; other host activity was
uncontrolled. Complete ordered columns and rows are checked outside timing.

The captured runtime tree is `b2e1dee89a37b3fc275be116c47b13a885a3e631`, based on
`91b1ecffb98830be1fb01c3ab00fa09fd1085e32`; the report includes exact hashes of
the two changed runtime files, executables and
[verbatim benchmark source](benchmarks/cypher_peg_preparation_2026_11_03/public-query-source.txt).
The [initial eight-process comparison](benchmarks/cypher_peg_preparation_2026_11_03/public-query-initial.json)
and [comparison with the preceding PEG runtime](benchmarks/cypher_peg_preparation_2026_11_03/preparation-comparison.json)
remain available. Earlier adverse measurements remain evidence for their own
source revisions. Final-head qualification still needs the required suite;
diagnostic transitions, resource admission and allocation regressions remain
open, so these performance results do not make the migration ready by themselves.


Failed value alternatives now report the value rule's public expectation when
none consumes input, while retaining an earlier diagnostic at the same position.
The scalar-primary rule reports its identifier fallback when no alternative
consumes input. Deeper failures and fatal work/capture/nesting admission errors
retain their precedence; these actions do not add a second recognition pass.
An independent 21,944-record comparison preserves complete ASTs/spans and
acceptance/rejection classes. Full rendered comparisons repair 815 additional
main outcomes, reducing the remaining differences from 4,928 to 4,113 without
losing an existing match. Four already-incompatible diagnostics change wording
and remain unresolved: two vector-search projection failures and two malformed
parenthesized COALESCE argument entries. This is incremental compatibility work,
not completion of the diagnostic gate.

The provenance-bearing third repair fixture checks 818 distinct query/entry
pairs, including four procedure-argument controls. The 163 existing allocation
controls introduce no regression against the preceding PEG runtime; wide controls
still have 14 regressing metrics against former main. Neither these controls nor
this diagnostic change establishes complete resource or performance readiness.


The Cypher lexer now owns a private token representation with lossless u32 byte
offsets, justified by the existing 16 MiB admission limit. Named parameters carry
no numeric ordinal in the lexer; AST lowering still reads their matched names.
On the qualified native target, token storage falls from 24 to 12 bytes. The
shared SQL token type and public ASTs are unchanged. Boundary tests cover the
largest admitted offsets, EOF, UTF-8 text and parameter-token boundaries.

The 21,944 independent AST/span/class outcomes match, and all 22,980 full
rendered outputs equal the preceding PEG runtime. The 163 heap controls and 32
additional boolean controls have no parent regressions. Against former main,
wide-query regressing metrics decrease from 14 to 5; boolean regressing metrics
decrease from 22 to 16. The remaining wide cases are a literal IN list (total and
peak bytes), a property-heavy CREATE (allocation count), and rank CASE (total and
peak bytes). Rank CASE changes from 51 allocations / 36,541 requested bytes /
19,709 peak live bytes to 51 / 30,445 / 16,637. Former main is 105 / 26,239 /
15,153, so requested/peak-byte readiness is still incomplete. Existing diagnostic
differences and final-head performance qualification also remain open.


An [eight-process parser comparison](benchmarks/cypher_peg_compact_tokens_2026_11_03/parser-timing.json)
on the compact-token runtime preserves the original five queries, 11 samples and
20,000 iterations. Equal-weighted throughput improves 25.7665 percent over former
main; all five shapes improve. Four paired gains have a mean of 25.7939 percent
and a small-sample 95 percent t interval of 22.2129 to 29.3750 percent. Mean CPU
is 3.4610865 seconds former / 2.765941 seconds candidate. No compiler/test/benchmark
interference was observed; other host activity was uncontrolled. These intervals
describe this experiment. The unchanged-runtime tree and source hashes are bound
in the report; final documentation adds the measurement artifacts.

[Allocation evidence and reproduction sources](benchmarks/cypher_peg_compact_tokens_2026_11_03/README.md)
retain all remaining regressing metrics. Retained AST bytes equal former main
in all eleven wide cases. Cold/warm 256/512 KiB stack outcomes at optimization
levels 1 and 3 match each build's own 8 MiB reference over all 21,944 inputs.
The parser-throughput result does not establish current-main public E2E gains or
complete diagnostic/resource readiness.
