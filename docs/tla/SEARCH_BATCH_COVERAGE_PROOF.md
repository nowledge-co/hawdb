# Search batch coverage protocol model

This is a **design model**, not a refinement proof of an implemented native
coverage API. The current legacy builder has a verified partial-prefix failure.
The [protocol specification](../specs/MVCC_SEARCH_BATCH_COVERAGE_SPEC.md) records
that reproducer, the required API/staging contract and all remaining delivery.

## Invariant and induction

For one immutable source E and complete baseline C, let the cursor identify a
contiguous processed prefix of the retained C..E feed. The private reducer holds
the union of old document identities and the final E rows for visited source
identities. Each successful visit extends that prefix by exactly one admitted
record. A refusal/cancellation makes the session terminal and cannot expose
private rows or a watermark. Repeated source keys still require visits.

Assuming complete capture, disjoint document-ID namespaces per source root and
the stated projection function, processing the entire feed yields the full E
projection: unchanged baseline documents remain valid, every changed old
identity is deleted, and each final live changed row overwrites its identity.
Deletions precede final upserts globally, so rename/ABA cannot erase a final row
merely because its old identity occurs later in enumeration. Native duplicate
business IDs need the explicit host uniqueness/reduction contract; the finite
model does not establish that premise for actual Mem data.

Sealing requires the complete cursor/covered interval. Publication additionally
requires the exact source, host binding incarnation, schema, base and dependency
validation under a retained fence, and the exact reduced stage. The independent
safety predicate uses all intervening writes and authority fields, not the
potentially faulty checks used to permit publication. A write to a root outside
the captured interval's affected roots may commit during preparation without
invalidating the E output. Publication validity concerns E, not equality with a
later forever-changing live source.

## Finite check and controls

`HawDBSearchBatchCoverage` has two initially live roots with distinct two-ID
namespaces, payload bits, rename/presence changes and two committed epochs.
The baseline is C=0. Capture pins a source world and E; one record is processed
per Visit. Authority fields distinguish canonical instance, schema, monotone
host binding incarnation and base selector generation. Mutations can occur
before capture and throughout private work. Validate/Publish retain exclusion;
only the fence-release mutant admits intervening mutations after validation.
A mapper failure or cancellation terminates private work.

TLC 2.19 / TLA+ 1.7.4, Java 21.0.9, jar SHA-256
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`,
four workers, MaxEpoch=2: **109,852 generated / 49,105 distinct states**, depth
13, no invariant failure. The checked invariants are TypeOK,
NoStalePublication and NoPartialVisibility.

Twelve controls each exit 12 with the exact NoStalePublication violation:

| Control | Missing protocol obligation |
| --- | --- |
| PublishPrefix | Complete interval before sealing/publication |
| SkipItem | Actual processing, not only a progressing cursor/count |
| IgnoreMapperError | Sticky failure after partial/failed mapping |
| DropDeletes | Full old-identity reduction |
| WrongSnapshot | Hydration against the captured source |
| SubstituteStage | Receipt bound to the exact staged output |
| IgnoreDependency | Relevant newer writes reject before visibility |
| IgnoreBinding | Host binding/incarnation fence |
| IgnoreInstance | Canonical instance identity |
| IgnoreSchema | Schema identity |
| IgnoreBase | Search base generation CAS |
| ReleaseFence | Exclusion across validation and visible publication |

CompleteWitness and DisjointWitness exit 12 for NoCompleteProgress and
NoDisjointProgress respectively. They show non-vacuous complete publication and
an actual disjoint commit between capture and successful publication. Configs
and expected invariant names are in `mutants/mutants.txt`. Parse/runtime errors
or timeouts are not accepted as counterexamples.

The prefix control models premature authorization of a full E receipt. It is
not an exact implementation model of the legacy builder, which instead labels
mixed-snapshot output B. The native reproducer in the specification independently
shows that B <= E and an operation limit do not establish B coverage.

## Source mapping and limits

| Model seam | Current native state / required implementation |
| --- | --- |
| Immutable source world | Existing PreparedRead pins E; bounded feed cursor/context now implemented; artifact-owning session still missing |
| Visit / covered interval | New bounded cursor/context enumerates inputs; eager Vec/public counts and enumeration alone do not supply output coverage authority |
| Pinned graph hydration | Existing parameterized prepared Cypher helper; batch orchestration and relational/host mapping still required |
| Rows/deletes reducer | Abstract sets only; governed external ordering/dedup and duplicate-ID policy still required |
| Sealed / SubstituteStage | Existing Search candidates are sealed, but no stage-bound batch coverage token exists |
| Authority / Publish | Existing engine source validation and Search base CAS; actual Mem binding/outer-owner integration still required |

The model assumes atomic committed graph writes and durable source/base
prerequisites. It does not model WAL/file barriers, replay, durable private
resume, lost responses, stage GC, allocator/RSS, parser/physical I/O or fairness.
The source-durability/publication/history models retain those separate
obligations; composed fault images remain required. One small abstract event
per visit does not prove bounded native processing of a large transaction.
Arbitrary C baselines, full Mem graph/relational mapping and ID collisions are
not exhausted by this configuration. Native source-linked interleavings and
admission/equivalence regressions are required when the session is implemented.
