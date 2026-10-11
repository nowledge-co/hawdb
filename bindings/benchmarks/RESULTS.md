# Initial host-boundary observations

These are partial measurements for [#976](https://github.com/nowledge-co/hawdb/issues/976)
and the first two stages of the [interchange proposal](../../docs/specs/ZERO_COPY_COLUMNAR_INTERCHANGE_SPEC.md).
They do not complete its representative large-size baseline, qualify ordinary
latency, or establish a strict zero-copy capability. The reproducible commands
and measurement boundaries are in [README.md](README.md); the recorded native
allocation observations and refusal results are in [observations.json](observations.json).

## Environment and source identity

Measurements used macOS 27.0.1, arm64, Apple M3 Max, Rust 1.97.1 and Go 1.27.1,
with all engine dependencies built together using Bazel `-c opt`. Persistent
stores used `SyncOnEveryWrite`; memory stores used the ordinary facade defaults.
The fixture seed is `0x5eed`. All cases use the same parameterized statements,
schema and ordered, type-sensitive checksum in Rust, Python and pure Go.

The frozen base commit was `1519433bb70900bf3f51f5a3ed93de0873072713`.
The staged trees identify the exact uncommitted source used for each run:

| Experiment | Staged tree | Actual result |
| --- | --- | --- |
| Ordinary N=3 wiring | `530ad537b60aa8c206cc02a663a7b572d9bdfc87` | 60/60 runs passed parity |
| Ordinary N=1,000 pilot | `530ad537b60aa8c206cc02a663a7b572d9bdfc87` | 60/60 runs passed parity |
| Instrumented N=1,000 before clone removal | `c11c0b34c1c93c7fb79302182baab761d4048e7f` | 60/60 runs passed parity and native-profile validation |
| Instrumented N=1,000 after clone removal | `1aed93890fb600ca865b6550c923468f74b654d3` | 60/60 runs passed parity and native-profile validation |
| Ordinary N=100,000 select/fill_bulk/wide | `530ad537b60aa8c206cc02a663a7b572d9bdfc87` | 36/36 runs refused by the result-memory budget |

Each 60-run experiment covers five cases, two backends and three languages,
with one discarded and one measured iteration. The supplemental 36-run refusal
experiment covers three cases at N=100,000 using the same iteration policy.
Concurrent host work makes these runs unsuitable for ordinary latency or
throughput comparisons. Instrumentation also changes allocator and tracing
costs. No end-to-end speedup is claimed.

## Python redundant-clone removal

`QueryResult::from_output` previously cloned each engine `Value` before
converting it into an independently owned Python object. It now borrows the
same value during conversion. Python dictionaries, strings, bytes and recursive
containers are still materialized; this is an ordinary binding optimization.

Measured successful Rust allocation requests for the Python operation and its
checksum consumer at N=1,000 were:

| Case / backend | Before | After | Difference |
| --- | ---: | ---: | ---: |
| select / memory | 25,550 | 24,550 | -1,000 |
| select / file | 25,560 | 24,560 | -1,000 |
| point / memory | 7,949,000 | 7,948,000 | -1,000 |
| point / file | 7,959,000 | 7,958,000 | -1,000 |
| fill / memory | 10,815,635 | 10,814,635 | -1,000 |
| fill / file | 10,839,581 | 10,838,602 | -979 |
| fill_bulk / memory | 161,465 | 160,465 | -1,000 |
| fill_bulk / file | 185,428 | 184,428 | -1,000 |
| wide / memory | 235,100 | 202,100 | -33,000 |
| wide / file | 235,110 | 202,110 | -33,000 |

For the 20-column recursive wide fixture this is about 14.04% fewer allocation
requests and 4.59 MB less requested allocation traffic. Allocation traffic is
neither retained memory nor peak RSS. It is not proof of payload-copy identity.
The file-backed point run requested 102,000 more bytes despite 1,000 fewer
allocation calls; the observations retain that variation.

Both backends pass a regression that keeps an owned nested result after
database closure and mutates the original host input. It covers Unicode/NUL,
binary/empty values, lists/maps, UUID, integer bounds, signed zero and a NaN
payload. The benchmark's narrower fixture does not prove arbitrary float or
reserved JSON-tag round trips across every language.

## Large results and user-facing budgets

The N=100,000 experiment returned the same `result_materialization` memory
refusal through Rust, Python and Go, using the original 16 MiB result budget.
The reported next charge exceeded 16,777,216 bytes. Refused runs have no
successful checksum or measured-operation profile. An `execute` error in
`fill_bulk` alone does not establish which statement failed or whether the
preceding mutation committed.

Peak native-process RSS included input parsing, setup and warmup; the observed
wide fixture exceeded 2 GiB before its refusal. Raising the global result cap
alone would not bound this workspace or qualify a user-experience improvement.
This change keeps all default limits and durability policies intact. Future
bounded delivery needs its own producer and retained-memory admission evidence.

The full three-size, three-sample ordinary matrix remains incomplete. Its point
case performs N separate lookups during warmup and again during measurement;
the call count has not been reduced to shorten the experiment. Sampling an
optimized N=100,000 point process found graph projection scanning and owned
property cloning prominent. This is a separate execution-engine investigation,
outside #976's binding-only implementation scope.

## Validation and remaining work

The full original 97-target fuzz/Go run executed all targets: 94 passed and
three timed out. The scan-callback, search-manifest-budget and Go targets all
passed an isolated uncached retry with their original cases and deadlines.
The default Python suite initially timed out; a full uncached retry passed in
42.8 seconds. The Pydantic target passed in 40.2 seconds. No cases, seeds,
deadlines, job limits or fuzz registrations were weakened.

The remaining acceptance work includes the complete large-size baseline,
ordinary latency comparisons and a measured bulk-boundary candidate. Retained
batch production, aggregate byte/slot/handle admission, strict C/Go/Python
views, compatible Arrow exports and the independent execution extensions have
not been implemented by this measurement change. #976 remains open.
