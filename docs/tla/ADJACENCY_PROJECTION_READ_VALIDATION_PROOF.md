# Internal-ID and single-hop prepared reads

Prepared Cypher reads can qualify necessary internal-ID EQ/IN bounds reached
only through conjunctions for the same root binding. A complete local node
source is required: bounds do not cross projection, LIMIT, TopN, aggregation or
full-text selection. The collector records every bounded root, including absent
nodes. Other admitted node access paths retain their existing conservative
dependencies; unqualified traversal shapes fail closed.

The traversal qualification covers one hop from those roots, with
outgoing, incoming or undirected expansion and an optional relationship type.
`GraphMatchExec` must have no input/imports and exactly a root plus at most one
expand; optimized `AdjacencyExpandExec` must have a complete local root source.
`AdjacencyExpandExec` may be required or optional when its input is a complete
local internal-ID root; an empty optional expansion produces a null-extended row
from the same captured inputs. Standalone `GraphMatchExec` remains non-optional.
Variable-length traversal, multiple hops, unqualified optional/imported matches and hidden
relationship-existence predicates are not qualified by this collector.

## Conditional completeness argument

Assume the captured canonical instance and schema, complete existing graph write
intents, a single canonical commit order and retained newer stamps:

1. A necessary root-ID bound contains every possible root. Recording GraphNode
   covers root presence, labels and properties, even if the root filter is false.
2. Record directed GraphAdjacency for every bounded root before scanning, even
   when no edge matches. Edge creation/deletion stamps the affected endpoint
   adjacency. Thus a new matching edge or removal cannot escape validation.
   The key covers all types, conservatively invalidating other types at the same
   root; incoming and outgoing scopes remain distinct.
3. Enumerate every edge and neighbor identity in that root/type/direction scope
   before relationship-property or neighbor-label/property filtering. Record
   GraphRelationship and the neighbor's GraphNode. A previously false predicate
   becoming true is covered as well as a returned row changing. Property writes
   need not stamp adjacency because these identity stamps cover their effects.
4. Live posting metadata and canonical descriptor inputs together cover the
   snapshot. Live overrides/tombstones replace canonical entries; the canonical
   scan still charges skipped entries. Missing canonical adjacency qualification
   refuses rather than silently omitting cold edges. Unknown mutation paths keep
   the existing Database barrier and explicit schema operations keep the Schema
   barrier. Implicit catalog additions from data writes need not emit Schema;
   those writes still need their complete graph identity/adjacency footprint.
5. Every change to the qualified query's input therefore touches a recorded key
   or a conservative barrier. Validation rejecting any newer such stamp implies
   unchanged inputs. Local filtering, projection, sort, grouping and limits over
   those inputs cannot introduce another graph source. Optional null extension
   is a pure function of the complete candidate set, including its emptiness.
   This relies on the
   existing closed expression/plan qualification, not arbitrary host callbacks.

Existing graph writers already produce these keys, including before a query is
chosen. The new path adds no shared writer intent, WAL format or version index.
History remains pinned from capture, covering a commit before the first query.
This proves a conditional dependency claim, not serializable transactions: the
underlying transaction contract retains its documented snapshot-isolation limit.

## Work and refusal boundary

At most 1024 root values are admitted. One traversal collection has a shared
1024-unit metadata budget across its roots and directions. Live group visits,
live matching entries and canonical matching entries consume work, including
canonical entries hidden by a live override or tombstone. The token's existing
cumulative entry/byte limits bound retained keys across statements. These are
admission bounds, not measured total allocation/RSS or parser-work bounds.

The metadata prepass constructs identities without materializing relationship
property values or hydrating neighbor properties. Canonical scans still read and
validate complete encoded blocks, including their property bytes, and can finish
validating a block after the callback stops. The callback budget therefore does
not bound physical bytes read, encoded-value validation work or peak block
allocation. Normal query execution still observes its
configured output/runtime limits. Any collector failure makes the token unusable;
an ignored partial scan cannot later validate. Admission errors leave the source
usable, whereas physical descriptor corruption follows storage poisoning rules.

## Finite model and native mapping

`HawDBAdjacencyProjectionReadValidation` contains two roots, four nodes, two
directed edges, initially empty scopes, false node/edge predicates, canonical and
live identity sources, delayed query choice, two commits, broad/schema barriers,
pruning and failed reads. The oracle retains independent full write history and
also compares captured/live query output. With `MaxEpoch = 2`, the main run checks
**5,029,782 generated / 2,143,350 distinct states**, depth 11. Optional queries
include unfiltered, node-filtered and edge-filtered empty/null-extended results.

Ten fault controls omit the root, empty adjacency, incoming adjacency, filtered
neighbors, filtered relationships, canonical identities, live identities,
barriers, pin protection or sticky refusal. Each exits 12 with the exact
`NoStaleAcceptance` violation. `DisjointWitness` exits 12 with
`NoDisjointAcceptance`, showing that acceptance after an unrelated write is
reachable. Parser/runtime failures are not accepted as negative controls.

| Model seam | Native source / regression |
| --- | --- |
| Capture / Choose | `begin_prepared_read`, `dependency_read/graph_match.rs` necessary ID and physical shape qualification |
| ChosenKeys | `store/read_dependencies/adjacency.rs`, live postings plus canonical `scan_endpoint_inputs_control` |
| Node / edge writes | existing GraphNode, GraphRelationship and directed GraphAdjacency write intents |
| Barriers / Prune / Accept | existing `validate_read_dependencies`, source pin and shared publication validator |
| Failed Read | sticky kernel token refusal and `PreparedRead.failed` / `finish` |

Native consumers verify disjoint same-label node writes; root, neighbor and edge
payload changes; empty-adjacency insertion; false-to-true predicates; direction
separation; and checkpointed canonical plus live edges with an actual 1-MiB
relationship payload. Kernel tests verify ignored metadata-work failure refuses
validation without poisoning the source, and identity collection succeeds with
large relationship properties. These tests support the source mapping without
proving all canonical descriptor implementations or allocator behavior.

## Remaining scope

The finite model abstracts IDs, types, descriptor validation, physical I/O and
writer classification. It is not a mechanized Rust refinement or power-loss
proof. Multi-hop/unqualified optional/path predicates, graph range/composite and additional
relational dependency families still need their own qualification.

The existing Search graph-delta builder has not yet been replaced by the new
[prepared graph hydration path](PREPARED_SEARCH_GRAPH_HYDRATION_PROOF.md).
Bounded batch coverage receipts, exact relational hydration, host binding/incarnation,
Mem outer-owner migration and source-validated catch-up remain required. Search
activation I/O, fault composition, retained-source RSS and 1/4/8-writer latency
also remain open under the full #231/#232 acceptance criteria.
