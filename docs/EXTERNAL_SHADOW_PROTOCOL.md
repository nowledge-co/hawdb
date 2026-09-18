# External Shadow Protocol

HawDB uses the external shadow protocol to compare the embedded engine with a
previous local graph wrapper during Nowledge migration gates. The protocol is a
line-delimited JSON request/response stream over child-process stdin/stdout.

This protocol is intentionally small. It covers only the compatibility harness
surface that Nowledge needs for cutover: parameterized Cypher execution, grouped
session execution, and projected graph checks.

## Transport

The parent process starts the shadow engine with piped stdin and stdout. Each
request is one UTF-8 JSON object followed by a newline. Each response must be
one UTF-8 JSON object followed by a newline.

The child process must not write logs or progress messages to stdout. Diagnostic
output belongs on stderr; malformed stdout is treated as a protocol error and
HawDB includes a bounded stdout line tail in the error for local debugging.

`request_id` is a monotonically increasing per-process identifier assigned by
HawDB. It matches the external shadow trace `sequence` value for the same
request.

The child process must keep its graph state for the lifetime of the process.
HawDB sends fixture setup statements and checks to the same process so the
shadow engine can model an embedded database instance.

Every request includes:

```json
{
  "protocol_version": 1,
  "request_id": 1,
  "op": "execute",
  "context": {
    "fixture": "nowledge-memory-core",
    "check": "read title",
    "phase": "statement",
    "statement_index": null
  }
}
```

Unknown top-level fields must be ignored by compatible shadow engines. A shadow
engine should reject unsupported protocol versions with an `execution` error.
`context` is diagnostic metadata for wrapper logs and traces. `phase` is one of
`fixture_setup`, `check_setup`, `statement`, `session`, `effect`, or
`project_graph`; `check` is `null` for fixture-wide setup requests.
`statement_index` is `null` for single-statement requests and zero-based for
statements nested inside `execute_session`.

Responses may include a top-level `request_id` echo. The echo is optional for
backward compatibility, but when present it must match the request `request_id`.
HawDB rejects mismatched response identifiers as an `execution` error because
they indicate a stale, reordered, or misrouted shadow response.

Every response must use exactly one envelope shape. `execute`, `execute_session`,
and `ready` responses must contain exactly one of `ok` or `error`; ambiguous
responses are rejected as protocol errors.

## Values

Cypher parameters and result rows use JSON values:

| JSON value | HawDB value |
| --- | --- |
| `null` | `Null` |
| `true` or `false` | `Bool` |
| integer number | `Int` |
| floating-point number | `Float` |
| string | `String` |
| array | `List` |
| object | `Map` |

Rows are JSON objects keyed by projected column name. Relationship and node
identities used by projected graph output are unsigned integer identifiers from
the corresponding engine.

## `ready`

`ready` is a preflight operation used by the migration gate before the full
fixture set is executed. It runs automatically when `--require-ready` is passed,
and can also be requested independently with `--shadow-ready`.

Request:

```json
{
  "protocol_version": 1,
  "request_id": 1,
  "op": "ready",
  "required_protocol_version": 1,
  "required_capabilities": ["execute", "execute_session", "project_graph"]
}
```

Success response:

```json
{
  "ok": {
    "protocol_version": 1,
    "engine_kind": "previous_wrapper",
    "capabilities": ["execute", "execute_session", "project_graph"]
  }
}
```

`protocol_version` must match the request protocol version. `capabilities` must
include `execute`, `execute_session`, and `project_graph`; an adapter that cannot
materialize projected graph metadata should still advertise `project_graph` when
it can return a valid `primary_only` response for that operation.
`engine_kind` is optional for protocol smoke tests, but migration cutover
evidence requires `previous_wrapper`. The bundled `hawdb-shadow-self` adapter
reports `protocol_smoke`, so it can validate the protocol without being accepted
as previous-wrapper cutover evidence.

Previous-wrapper adapters should not hand-roll the JSON-lines protocol loop.
HawDB exposes `ExternalShadowProtocolBackend` and
`ExternalShadowProtocolServer` for wrapper processes: implement the backend
methods against the existing Kuzu/Ladybug wrapper, return
`engine_kind() == "previous_wrapper"`, then call `run_json_lines` on stdin and
stdout. The server owns protocol-version validation, response envelopes,
capability reporting, JSON-to-`Value` conversion, default ordered
`execute_session` handling, and default `project_graph` primary-only responses.
The compile-checked
`examples/nowledge_previous_wrapper_shadow_adapter.rs` file shows the intended
shape. In Nowledge, the `PreviousWrapperGraph::query` hook should call the
existing Kuzu/Ladybug raw read/write wrapper with the decoded parameters, and
`execute_session` should use the wrapper's single-connection transaction/session
path so fixture setup, mutation, and effect checks observe one mutable state.
For integration work where linking the wrapper into the example binary is too
heavy, the example also supports:

```text
cargo run --example nowledge_previous_wrapper_shadow_adapter -- [--command-timeout-ms <ms>] --command <program> [args...]
cargo run --example nowledge_previous_wrapper_shadow_adapter -- --persistent-command <program> [args...]
```

In this mode the adapter keeps the HawDB external-shadow JSON-lines protocol on
stdin/stdout and delegates each operation to the command as a separate JSON
request on the command's stdin. The command should return one JSON value on
stdout:

- query request:
  `{"op":"query","cypher":"...","parameters":{...}}`
  expects `{"rows":[{...}]}`
- session request:
  `{"op":"execute_session","statements":[{"op":"query","cypher":"...","parameters":{...}}]}`
  expects `{"results":[{"rows":[{...}]}]}`; bare row arrays are also accepted
  per result for small shims
- projected-graph request:
  `{"op":"project_graph","rel_type":"...","expected_incoming_nodes":[...],"include_communities":false,"include_hierarchical_communities":false}`
  expects either `{"primary_only":true,"reason":"..."}` for early wiring or a
  full `{"ok": ...}` projected-graph payload

The command bridge is intentionally process-owned by Nowledge. It lets the real
Kuzu/Ladybug wrapper keep its dependencies and transaction/session handling
outside HawDB while still producing `engine_kind: "previous_wrapper"` shadow
evidence through the shared protocol server. `--command-timeout-ms` bounds each
delegated command invocation so a hung wrapper fails with a direct adapter error
instead of only surfacing as an outer shadow request timeout.
Use `--persistent-command` for real wrapper validation when startup, graph open,
or connection setup is expensive. In this mode the adapter starts one child
process, sends one JSON request per line, and expects one JSON response line per
request. Request timeouts are enforced by the outer shadow command
`--shadow-timeout-ms` gate rather than by respawning the child per operation.

To generate the exact production-shaped fixture contract for a wrapper shim,
use:

```text
hawdb nowledge-fixture-contract [nowledge-memory-core]
```

The command prints `hawdb-nowledge-fixture-contract` JSON with the fixture setup
statements, each check's Cypher statement, parameters, expected rows or row
count, execution mode, effect queries, projected-graph requests, and the command
bridge request/response shapes. It does not open a database or run the shadow
engine. Use it as the machine-readable contract when implementing the Nowledge
Kuzu/Ladybug wrapper process, then validate that process with
`external-shadow-adapter-smoke --require-previous-wrapper` before running the
full migration gate.

For a command shim that already implements the contract bridge shape, run:

```text
hawdb nowledge-fixture-contract-command-check [--require-full-contract] [--stop-after-first-failure] [--start-check <zero-based-index>] [--check-name <name>] [--max-checks <n>] [--command-timeout-ms <ms>] [--allow-primary-only-project-graph] <contract-json> [--persistent-command] <program> [args...]
```

This command reads a `hawdb-nowledge-fixture-contract` file, invokes the command
shim directly with the exported query/session/project-graph requests, and checks
the command's JSON rows against the fixture expectations. It is a wrapper
bring-up diagnostic; production replacement still requires the full
`nowledge-cypher-migration-gate --require-cutover-evidence` path with
previous-wrapper identity, storage recovery evidence, background-maintenance
evidence, and per-family readiness.

Use `--start-check` and `--check-name` to isolate failures while implementing
the real wrapper. The report distinguishes selected-subset readiness from
`full_contract_ready`; partial runs are never full cutover evidence.
Use `--require-full-contract` in CI or release validation when the command must
exit successfully only after the complete exported contract has been selected,
checked, and matched.
The checker report also includes `full_contract_blocker_codes`,
`full_contract_blockers`, `required_contract_blocker_codes`, and
`required_contract_blockers`. These fields distinguish a healthy selected slice
from full-contract readiness with stable codes such as
`full_contract_not_checked` and `selected_subset_not_ready`.
By default the checker spawns the command once per request, which is useful for
small smoke shims. Use `--persistent-command` before the program when validating
the real wrapper against the full contract; the checker keeps one JSON-lines
child process open, sends one request per line, expects one response line per
request, and records `options.command_mode: "persistent"` in the report.
Use `--stop-after-first-failure` when iterating on a failing wrapper to avoid
secondary failures after state divergence. Reports include `failure_summary`
with phase counts, the first failed check index/name, suggested
`--start-check`/`--check-name` values, and whether the checker stopped early.
Each failure also carries a stable `code` such as `row_count_mismatch`,
`project_graph_primary_only`, `command_timeout`, or `command_invalid_json`, and
`failure_summary.failed_code_counts` aggregates those codes so wrapper bring-up
automation does not need to parse human-readable messages.

## `execute`

`execute` runs one Cypher statement against the shadow engine.

Request:

```json
{
  "protocol_version": 1,
  "request_id": 1,
  "op": "execute",
  "role": "read",
  "access": "read",
  "context": {
    "fixture": "nowledge-memory-core",
    "check": "read title",
    "phase": "statement",
    "statement_index": null
  },
  "cypher": "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title",
  "parameters": {
    "id": 1
  }
}
```

Success response:

```json
{
  "ok": {
    "rows": [
      {
        "title": "Graph foundations"
      }
    ]
  }
}
```

Mutation statements should return the same rows the wrapper would expose for
the Cypher statement. For count-only mutation fixtures, an empty row object can
represent one affected row.

## `execute_session`

`execute_session` runs a sequence of Cypher statements inside one logical
session. This is used when a check needs setup, the main statement, and effect
verification to observe the same mutable wrapper state.

Request:

```json
{
  "protocol_version": 1,
  "request_id": 1,
  "op": "execute_session",
  "access": "mutation",
  "context": {
    "fixture": "nowledge-memory-core",
    "check": "update memory title",
    "phase": "session",
    "statement_index": null
  },
  "statements": [
    {
      "cypher": "CREATE (:Memory {id: 1, title: 'Old'})",
      "role": "statement",
      "access": "mutation",
      "context": {
        "fixture": "nowledge-memory-core",
        "check": "update memory title",
        "phase": "statement",
        "statement_index": 0
      },
      "parameters": {}
    },
    {
      "cypher": "MATCH (m:Memory) WHERE m.id = 1 SET m.title = 'New'",
      "role": "statement",
      "access": "mutation",
      "context": {
        "fixture": "nowledge-memory-core",
        "check": "update memory title",
        "phase": "statement",
        "statement_index": 1
      },
      "parameters": {}
    },
    {
      "cypher": "MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title",
      "role": "statement",
      "access": "read",
      "context": {
        "fixture": "nowledge-memory-core",
        "check": "update memory title",
        "phase": "statement",
        "statement_index": 2
      },
      "parameters": {}
    }
  ]
}
```

Success response:

```json
{
  "ok": {
    "outputs": [
      {
        "rows": []
      },
      {
        "rows": [
          {}
        ]
      },
      {
        "rows": [
          {
            "title": "New"
          }
        ]
      }
    ]
  }
}
```

The `outputs` array must have the same length and order as the request
`statements` array. The top-level `access` is `mutation` if any statement in
the session may write; otherwise it is `read`. Per-statement `access` is
advisory but stable: previous-wrapper adapters should use `read` for read-only
Kuzu/Ladybug APIs and `mutation` for serialized write paths. `role` is a
compatibility-harness context label. HawDB reports malformed session outputs
with their zero-based output index so wrapper logs can be aligned with
`statements[*].context.statement_index`.

## `project_graph`

`project_graph` asks the shadow engine for the projected graph metadata needed
by the compatibility harness.

Request:

```json
{
  "protocol_version": 1,
  "request_id": 1,
  "op": "project_graph",
  "context": {
    "fixture": "nowledge-memory-core",
    "check": "mentions projection",
    "phase": "project_graph",
    "statement_index": null
  },
  "rel_type": "MENTIONS",
  "expected_incoming_nodes": [1],
  "include_communities": false,
  "include_hierarchical_communities": false
}
```

Success response:

```json
{
  "ok": {
    "node_count": 2,
    "edge_count": 1,
    "incoming": [
      [1, [0]]
    ],
    "communities": [],
    "hierarchical_communities": [],
    "page_rank_scores": [
      [0, 0.3508773619358619],
      [1, 0.649122638064138]
    ],
    "page_rank_top_node": 1
  }
}
```

`incoming` contains `[node_id, [source_node_id, ...]]` tuples for the requested
`expected_incoming_nodes`. `communities` contains `[node_id, community_id]`
tuples. `hierarchical_communities` contains
`[level, node_id, community_id]` tuples. `page_rank_scores` contains
`[node_id, score]` tuples in deterministic order.

If the previous wrapper cannot expose projected graph metadata, it may respond
with:

```json
{
  "primary_only": true,
  "reason": "projection metadata is not exposed"
}
```

`reason` is optional. When provided, it is included in the cutover report's
`primary_only_reasons` object and in the blocker text. The migration gate treats
primary-only projected graph checks as blockers by default.

Projected graph responses extend the envelope with `primary_only: true`, and
must contain exactly one of `ok`, `error`, or `primary_only: true`.
`primary_only` must be a boolean when present. Ambiguous responses are rejected
as protocol errors instead of being treated as ordinary primary-only coverage
gaps.

## Errors

Any operation can return an error:

```json
{
  "error": {
    "class": "semantic",
    "message": "missing parameter: id"
  }
}
```

`class` must be one of:

- `parse`
- `semantic`
- `storage`
- `execution`

Unknown classes are treated as `execution`. The message is included in the
HawDB-side error with the shadow engine name.

## Adapter Smoke Command

Before running the full Nowledge migration gate, a wrapper can be checked with:

```text
HAWDB_ENABLE_COMPATIBILITY_TOOLS=1 hawdb external-shadow-adapter-smoke [--require-previous-wrapper] [--shadow-trace <path>] [--shadow-timeout-ms <ms>] <shadow-name> <program> [args...]
```

The smoke command sends `ready`, then runs a minimal fixture that exercises
`execute_session` and `project_graph`. It prints a
`hawdb-external-shadow-adapter-smoke` JSON report with the accepted ready
metadata, matched check counts, primary-only projection reasons, request count,
and optional shadow trace summary.

Use `--require-previous-wrapper` when validating the real previous database
wrapper. This rejects protocol-only self-shadow adapters whose `ready`
`engine_kind` is not `previous_wrapper`. A primary-only `project_graph` response
is allowed in this smoke command so early wrapper integration can prove request
routing before projection metadata parity exists. The production cutover gate
below still treats primary-only projected graph checks as blockers.
This command is a quarantined developer/preflight tool. Production serving and
read routing must use embedded library APIs and typed readiness reports instead
of invoking the `hawdb` binary.

## Gate Command

The current migration-gate entry point is:

```text
HAWDB_ENABLE_COMPATIBILITY_TOOLS=1 hawdb nowledge-cypher-migration-gate [--require-ready] [--require-cutover-evidence] [--allow-self-shadow] [--shadow-ready] [--shadow-trace <path>] [--shadow-timeout-ms <ms>] [--require-rollback-evidence] [--rollback-evidence <text>] [--require-storage-recovery-evidence] [--storage-recovery-report-json <path>] [--require-background-maintenance-evidence] [--background-maintenance-report-json <path>] <root> <shadow-name> <program> [args...]
```

It scans the Nowledge source tree, runs the public Nowledge compatibility
fixture through the external shadow process, prints a JSON migration-gate bundle,
and exits with an error when `--require-ready` is set and the final gate is
blocked.

`--require-ready` requires a previous-wrapper shadow by default, sends the
`ready` preflight before fixture setup, and exits with an error unless the final
migration gate decision is `ready`. `hawdb-shadow-self` is allowed only when
`--allow-self-shadow` is passed, and that flag is intended for protocol and CI
smoke tests, not cutover evidence.

The printed migration gate bundle includes a top-level `shadow_run` object with
`shadow_name`, `self_shadow`, and `evidence_kind`. `evidence_kind` is
`previous_wrapper` for normal external wrappers and `protocol_smoke` for
self-shadow runs. Cutover automation must not treat `protocol_smoke` as
previous-wrapper parity evidence.

The bundle also includes a top-level `cutover_evidence` object. `eligible` is
true only when the run used previous-wrapper evidence, executed the ready
preflight, advertised all required ready capabilities, produced at least one
matched shadow check, and the migration gate decision is `ready`.
`requires_ready_capabilities` lists the required capabilities and
`ready_missing_capabilities` lists any capability missing from the ready
response. Its `blockers` array is intended for CI and release gates that need
to reject protocol smoke or incomplete shadow runs without rejoining the rest
of the bundle fields.
If a shadow trace is present, `cutover_evidence` also reports
`shadow_trace_present`, `shadow_trace_complete`,
`shadow_trace_summary_available`, `shadow_trace_request_count_matches`, and
`shadow_trace_pending_request_count`. A present trace is complete only when it
is readable, the reported request count matches request events in the trace,
and no traced request remains pending. An incomplete present trace blocks
cutover evidence; omitting trace logging does not by itself block cutover
eligibility.
When the bundle includes `replacement_readiness_by_query_family`,
`cutover_evidence` also reports
`replacement_readiness_family_report_present`,
`replacement_readiness_min_per_million`,
`replacement_readiness_invalid_family_count`,
`replacement_readiness_blocked_query_families`, and
`replacement_readiness_blockers`. Any query family below full replacement
readiness, or any malformed family entry, blocks cutover evidence, so
automation that primarily reads `cutover_evidence` does not collapse scanner
coverage and shadow parity into a single global ratio.
`--storage-recovery-report-json <path>` attaches a JSON report produced by
`hawdb storage-recovery-report`. When present, or when
`--require-storage-recovery-evidence` is passed, `cutover_evidence` reports
`storage_recovery_required`, `storage_recovery_present`,
`storage_recovery_ready`, `storage_recovery_protocol_matches`,
`storage_recovery_durable`, `storage_recovery_checkpoint_boundary_present`,
`storage_recovery_wal_replay_bounded`,
`storage_recovery_torn_tail_clean`, `storage_recovery_blocker_codes`, and
`storage_recovery_blockers`.
Required storage recovery evidence is ready only when the report protocol
matches `hawdb-storage-recovery-report`, durable recovery was observed, a
checkpoint boundary is present, WAL replay was opened with a configured bound,
and no torn tail was ignored.

The bundle includes a top-level `background_maintenance` object with the local
HawDB maintenance summary after the compatibility fixture run. It reports
candidate counts, admitted/deferred/rejected operation totals, stable work
class/priority/admission strings, reason codes, whether a search projection
delta candidate carries an executable request, and top-level aggregate counts
for executable/admitted/deferred/rejected search-projection graph deltas.
Executable search deltas also report operation, upsert, delete, max-operation,
and complete-through graph commit epoch fields, and the summary aggregates
total/admitted delta operations plus the max complete-through graph commit
epoch so host-owned loops can distinguish precise incremental projection work
from planning-only freshness signals. This object is resource readiness
evidence for host-owned scheduling. When
`--require-background-maintenance-evidence` is passed, `cutover_evidence`
reports `background_maintenance_required`, `background_maintenance_present`,
`background_maintenance_ready`, `background_maintenance_protocol_matches`,
`background_maintenance_total_candidates`, `background_maintenance_ranked_count`,
`background_maintenance_executable_search_projection_graph_delta_count`,
`background_maintenance_admitted_search_projection_graph_delta_count`,
`background_maintenance_deferred_search_projection_graph_delta_count`,
`background_maintenance_rejected_search_projection_graph_delta_count`,
`background_maintenance_executable_search_projection_graph_delta_operations`,
`background_maintenance_admitted_search_projection_graph_delta_operations`,
`background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch`,
`background_maintenance_foreground_ranked_count`,
`background_maintenance_unknown_admission_count`,
`background_maintenance_blocker_codes`, and
`background_maintenance_blockers`. Required background maintenance evidence is
ready only when the summary is present, any declared protocol matches
`hawdb-background-maintenance-report`, it contains non-empty candidate and
ranked-work counts, all ranked work is background priority, and admission values
use the stable `admit`, `defer`, or `reject` strings. Legacy fixture-local
summaries without a `protocol` field remain accepted. Deferred or rejected
background work does not block cutover evidence because resource-constrained
deployments are expected to delay internal work under pressure.

For standalone resource-readiness preflight, run:

```text
hawdb background-maintenance-report [--require-cutover-ready] <database-path>
```

The command opens the database read-only and prints
`hawdb-background-maintenance-report` JSON with the same candidate, ranking,
admission, and search-projection-delta fields used by the migration-gate
`background_maintenance` object. `--require-cutover-ready` applies the same
background-maintenance evidence health rules used by cutover evidence and exits
with an error when stable blocker codes such as `no_candidates`,
`no_ranked_work`, `foreground_ranked_work`, or `unknown_admission` are present.
Pass the resulting JSON to `nowledge-cypher-migration-gate` with
`--background-maintenance-report-json <path>` when cutover evidence should use a
caller-owned durable database preflight instead of the fixture-local
`background_maintenance` summary.

For bounded graph-read evidence, first generate a read report and then compile
it with graph route readiness:

```text
hawdb nowledge-bounded-read-report <database-path> <cypher> > read-report.json
hawdb nowledge-bounded-read-evidence \
  --graph-route-readiness-json graph-route-readiness.json \
  read-report.json > bounded-read-evidence.json
```

`covered-routes.json` may be either a JSON array of route identifiers or an
object with a `covered_routes` string array when callers need to override the
route list. Without that override, `nowledge-bounded-read-evidence` uses
`primary_ready_routes` from `graph-route-readiness.json`. The emitted evidence
includes `covered_routes`, `required_covered_routes`, `missing_covered_routes`,
route primary readiness, query plan/profile evidence readiness, and relationship
property pruning counts. Missing production graph-read routes or missing graph
route readiness add stable blockers and keep bounded-read readiness false, so a
single bounded query probe cannot be mistaken for full route cutover coverage.

The embedded library readiness report also accepts query-family replacement
readiness evidence through `replacement_readiness_by_query_family`. It
recomputes `hawdb-nowledge-query-family-evidence-v1` from the family rows and
publishes it as `query_family_evidence` plus
`readiness_by_area.query_family`. Missing, blocked, or malformed query-family
rows keep library readiness false; callers should pass the same family evidence
that will later feed `nowledge-replacement-summary`.

Rust-only harnesses can generate the same library-level artifact without
manually composing API calls:

```text
hawdb nowledge-mem-library-readiness \
  --bounded-probe-json bounded-probe.json \
  --covered-routes-json covered-routes.json \
  --graph-route-readiness-json graph-route-readiness.json \
  --query-family-evidence-json query-family-evidence.json \
  --primary-search-projection-probe-json lancedb-probe.json \
  --search-projection hawdb-search-index \
  hawdb-graph-db > library-readiness.json
```

The command opens the graph in `shadow_read_only` mode by default, uses the
same `NowledgeMemEmbeddedStore::library_readiness_json` path as embedded
callers, and includes only a sanitized `open_report` instead of local database
paths. Use `--require-ready` in release automation when missing route coverage,
query-family evidence, search projection parity, storage recovery, or
background-maintenance readiness must fail the command.

Nightly Mem replacement bundles should include this command output as the
top-level `library_readiness` object. `nowledge-mem-integration-readiness
--require-ready` treats `hawdb-nowledge-mem-library-readiness-v1` as required
cutover evidence and fails closed unless the graph store, search projection,
query families, bounded reads, storage recovery, and background maintenance are
all ready through the Rust embedded library surface.

Nightly bundles must also include top-level
`search_candidate_shadow_evidence` for LanceDB candidate-read replacement.
The evidence must use route `/search-index/hawdb-shadow/candidate-evidence`,
source `nmem-rust-bridge`, `candidate_primary_engine: "hawdb"`, and a
non-zero `request_count`. Candidate parity is fail-closed unless
`primary_candidate_count == shadow_candidate_count`,
`matched_candidate_count == shadow_candidate_count`, and
`primary_only_candidate_count == 0`.
The nested `candidate_identity` summary must also be ready. It carries only
rolling checksums over per-request sorted candidate IDs for the primary,
shadow, and matched sets; it must not include raw candidate IDs.
Rust bridge code should generate this object with
`nowledge_mem_search_candidate_shadow_evidence_json` and
`NowledgeMemSearchCandidateShadowEvidence` so `ready` and blocker codes are
computed by HawDB instead of handwritten by the caller.
For multi-request bridge runs, prefer
`NowledgeMemSearchCandidateShadowAccumulator::record_compare_candidate_ids`
once per LanceDB/HawDB candidate comparison and emit `accumulator.json()` at
the end. Use `record_compare` only for count-only diagnostics; final
integration readiness requires candidate identity evidence.
CLI-based harnesses can emit the same evidence from a minimal probe:

```text
hawdb nowledge-search-candidate-shadow-evidence \
  --require-ready \
  search-candidate-shadow-probe.json > search-candidate-shadow-evidence.json
```

The probe schema is:

```json
{
  "requests": [
    {
      "primary_candidate_ids": ["mem_1", "mem_2"],
      "shadow_candidate_ids": ["mem_1", "mem_2"]
    }
  ],
  "filter_pushdown": {
    "pushed_predicate_count": 1,
    "fields": [
      "kind",
      "external_id",
      "source_id",
      "space_id",
      "unit_type",
      "importance",
      "confidence",
      "created_at",
      "updated_at",
      "event_start",
      "event_end",
      "is_latest"
    ]
  }
}
```

The CLI does not execute search; it only compiles already-observed LanceDB and
HawDB candidate IDs plus filter-pushdown fields into fail-closed evidence.

`--require-cutover-evidence` runs the same `ready` preflight and exits with an
error unless `cutover_evidence.eligible` is true. Use it for isolated release or
nightly evidence generation that must reject self-shadow smoke runs, missing
shadow parity evidence, or blocked migration gates. Production cutover
automation should consume the resulting artifacts through
`nowledge_mem_final_cutover_preflight` and other typed Rust library APIs rather
than shelling out to this command.

`--shadow-ready` sends the same `ready` preflight without requiring the final
migration gate decision to be `ready`. Use it for previous-wrapper adapter
integration runs when failing fast on protocol version or capability drift is
useful, but the local run still wants a report instead of a hard cutover gate.
When the preflight succeeds, the printed migration gate bundle includes a
top-level `shadow_ready` object with the accepted `protocol_version` and
advertised `capabilities`.

The `migration_gate` object keeps the existing flattened `blockers` list for
human-readable logs and also includes machine-readable
`shadow_total_checks`, `shadow_matched_checks`,
`shadow_primary_only_checks`, `shadow_evidence_present`,
`fixture_mismatch_blockers`, `inventory_blockers`, `shadow_blockers`, and
`rollback_blockers` counts. It also includes matching
`fixture_mismatch_blocker_messages`, `inventory_blocker_messages`,
`shadow_blocker_messages`, and `rollback_blocker_messages` arrays.
The coverage, inventory gate, cutover, and migration gate JSON objects expose
deterministic integer ratios with `*_per_million` fields. The top-level
`replacement_readiness_per_million` is the conservative minimum of inventory
coverage and shadow parity, scaled so `1000000` means all required checks are
covered and matched. These fields are progress and dashboard signals only;
cutover automation must still honor the Ready/Blocked decision and blocker
arrays.
The cutover object and top-level bundle also include `dual_engine_evidence`,
which records `primary_engine`, `shadow_engine`, primary and shadow check
counts, matched check count, primary-only check count, matched ratio, and a
`ready` boolean. This is the stable side-by-side evidence field for release
automation; it makes primary-only or self-shadow protocol smoke visibly
different from real HawDB-vs-previous-wrapper parity.
The coverage and inventory gate objects also include
`coverage_by_query_family`, which groups required inventory checks by their
scanner-assigned query family and reports per-family required, covered, missing,
and `coverage_per_million` values. This lets migration dashboards show which
Nowledge business slice is still blocking replacement without parsing fixture
names.
The top-level bundle also includes `replacement_readiness_by_query_family`,
which combines scanner coverage with shadow parity for each query family. Each
entry reports inventory-missing checks, shadow-matched checks, primary-only
shadow checks, primary-only check names, and per-million coverage, shadow
matched, and replacement-readiness ratios. These per-family readiness values are
progress signals; cutover automation must still honor the gate decisions and
blocker arrays.
`shadow_evidence_present` is true only when at least one check matched through
the shadow engine; primary-only checks do not count as parity evidence. Cutover
automation should use those grouped fields to distinguish scanner coverage gaps,
fixture wiring drift, missing shadow evidence, and shadow parity failures
without parsing blocker strings.

For dashboards and release notes that need one conservative replacement number,
use:

```text
hawdb nowledge-replacement-summary [--require-production-ready] [--compact] [--max-family-items <n>] [--max-blockers <n>] [--search-projection-evidence-json <path>] [--search-projection-shadow-evidence-json <path>] [--search-candidate-shadow-evidence-json <path>] [--bounded-read-evidence-json <path>] [--query-runtime-preflight-json <path>] [--query-family-evidence-json <path>] <migration-gate-json>
```

The command reads an existing migration-gate bundle and prints
`hawdb-nowledge-replacement-summary` JSON. It keeps scanner coverage,
shadow-parity readiness, and production cutover readiness separate. If the
bundle lacks eligible `cutover_evidence`, `production_replacement_per_million`
is `0` even when scanner coverage and shadow matched ratios are complete. If
the bundle includes `dual_engine_evidence`, the summary copies it into the
release-facing output and also requires `dual_engine_evidence.ready == true`
for production readiness. It also requires ready
`search_candidate_shadow_evidence` for the LanceDB/HawDB candidate-read path,
including count parity and redacted candidate identity parity. The summary also
preserves cutover storage/background
evidence, including background search-projection graph-delta aggregate counts,
operation totals, and max complete-through graph commit epoch, so release notes
do not need to parse raw ranked maintenance items. With
`--require-production-ready`, the command exits
with an error unless the summary reports `production_cutover_ready: true`.
`--compact` omits the potentially large `replacement_readiness_by_query_family`
and `blockers` arrays while retaining aggregate family counts, blocked family
names, blocker counts, and omitted-item counts in
`replacement_readiness_family_summary` and `blocker_summary`.
`--max-family-items <n>` keeps only the first `n` family-detail rows and reports
the omitted count. `--max-blockers <n>` keeps only the first `n` blocker strings
and reports the omitted count.
The summary also includes a bounded `next_actions` array. Each entry contains a
stable `action` code, a short `reason`, and the JSON `evidence_fields` that led
to the action. These actions are diagnostic hints for release automation and
dashboards; they do not override the fail-closed `production_cutover_ready`
decision.

When the caller requires rollback proof, the migration gate can also carry
caller-owned rollback evidence through `rollback_required`, `rollback_ready`,
and `rollback_evidence`. HawDB only gates on this supplied evidence; it does not
open or link the previous graph database. The CLI sets `rollback_required` with
`--require-rollback-evidence` and marks rollback ready only when
`--rollback-evidence <text>` is supplied.

`--shadow-trace <path>` writes a JSON-lines transcript of the external shadow
conversation. Each line contains `sequence`, `event`, and `payload`; `event` is
`request`, `response`, or `error`. Error events include the failure message and
the current stderr tail when available. The transcript is intended for
previous-wrapper parity debugging and should be treated as local diagnostic
output because Cypher parameters may contain graph data. When trace logging is
enabled, the printed migration gate bundle includes a top-level `shadow_trace`
object with the local `path`, total `request_count`, and a best-effort trace
summary so the report can be paired with the transcript and its request
sequence. When the trace can be read, `summary_available` is `true` and the
object includes `trace_record_count`, `request_events`, `response_events`,
`error_events`, `invalid_lines`, `completed_request_count`, and
`pending_request_count`. It also includes `request_op_counts`,
`response_op_counts`, `error_op_counts`, and `pending_op_counts` maps keyed by
shadow request operation, such as `ready`, `execute`, `execute_session`, and
`project_graph`, so cutover diagnostics can identify which operation class is
stalled or failing without copying Cypher text or parameter payloads into the
bundle. If the trace cannot be read, `summary_available` is `false` and
`summary_error` carries the local diagnostic; this does not rewrite the
migration gate decision.

Each request waits up to 30000 ms for one stdout response line by default.
`--shadow-timeout-ms <ms>` overrides that per-request timeout. A timeout kills
the shadow process and fails the gate with an `execution` error instead of
letting CI or local cutover runs hang indefinitely.
