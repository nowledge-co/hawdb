# Indexed retained-node readers and point-write controls

This is a same-source conditional write diagnostic, not comprehensive performance
acceptance for PR #986. It measures ordinary parameterized point `SET` with four
modes: no active reader, closed source buffers, an owned Python node dictionary,
and original source buffers held across writes. All fixtures use the same default
lazy retained governor initialized by an empty indexed query outside timing.

Each mode has an independent fixture. Every ID warms the owned point path; source
modes also warm every ID through the actual retained-node buffer protocol. Mode
order rotates across nine rounds, discarding the first. Each round updates the
same 32 indexed IDs one at a time. Reader acquisition, old-buffer checks, complete
new-node checks and release occur outside write timers. File commits retain
`SyncOnEveryWrite`; budgets, batch sizes and prefetch defaults are unchanged.

## Frozen producers and observations

The ordinary opt extension and existing manual profiling extension were built
at `48a2c8beb881494920e75848b849ee3b8c28982d`. The measured checkout head is
`3f71ac4efcba443279642c550746239b49767304`; its intervening patch changes only
Python tests and documentation. Compiled Rust/module input hashes remain identical.
Frozen packages, complete driver, source/extension hashes, all observations and
failure history are in [the evidence JSON](evidence/README.md#python-retained-node-point-write-20261012).

Eight children succeed, covering 288 raw and 256 measured rounds, or 8192 measured
point-write timings across ordinary and separately instrumented producers. Every
held scalar/payload preserves its old value, each new complete node is verified,
and all retained bytes, handles and owner groups return to zero.

Ordinary producer medians below are milliseconds for 32 point writes. The final
column is the median of eight within-round retained/no-reader ratios, rather than
the ratio of medians. Ratios above one are slower.

| Nodes | Pad bytes | Backend | No reader ms | Closed ms | Owned ms | Held buffer ms | Paired held/no-reader |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1000 | 4096 | memory | 23.977 | 23.456 | 24.106 | 23.587 | 1.0115 |
| 1000 | 4096 | file | 155.660 | 157.695 | 146.588 | 152.714 | 0.9817 |
| 10000 | 64 | memory | 240.509 | 253.514 | 249.095 | 274.840 | 1.0876 |
| 10000 | 64 | file | 378.179 | 382.560 | 387.416 | 405.280 | 1.0512 |

The adverse 10k-node groups remain evidence. Holding 32 rows charges 371744
retained bytes for 4096-byte pads, or 242720 for 64-byte pads, including source
and conservative descriptor capacity. These charges are not whole-process RSS.

The instrumented extension counts all Rust allocator requests on all native
threads. Median allocation traffic for 32 writes increases by 179616 bytes
with retained 4096-byte-pad rows, or 50592 bytes for 64-byte-pad rows. The no-reader,
closed and owned modes have identical median write allocation traffic per fixture.
These counts include runtime/WAL/index work and do not directly count payload copies.
Instrumented timings are separate from ordinary release results.

The ordinary writer itself is expensive: profiled no-reader traffic is 316575448
bytes per 32 memory writes at 1k nodes/4096-byte pads, and 555562968 bytes at
10k nodes/64-byte pads. Source inspection locates `matching_node_ids_bounded` in
`crates/storage/src/store/graph_mutation.rs`: it calls `visit_nodes_owned`, which
clones labels and complete properties before applying the filter. Thus an indexed
point mutation still materializes unrelated candidates in this preflight path.
Removing this path requires its own semantics/admission/recovery regression and
frozen before/after evidence; this diagnostic makes no claim that it is repaired.

## Limits and reproduction

Each mode/group has 256 empirical point timings; the JSON preserves every value
and empirical P50/P95/P99 at sorted index `floor((N - 1) * p)`. These local, nonisolated samples do not
establish production tail latency. Process high-water RSS includes all four
fixtures, setup, warmup and foreign consumers, with no mode-specific attribution.
Native allocator requests exclude Python/foreign allocations, allocator rounding
and external buffers. A held source generation may add COW allocation while other
work dominates timing; neither favorable nor unfavorable timings isolate causality.

The first driver preflight assumed that the lazy governor already existed in
the no-reader mode. All eight children failed during setup without measurements.
The corrected driver initializes every mode identically through the public empty
indexed-query API. Those errors and hashes remain in the evidence JSON.

Use a frozen ordinary or profiling package directory containing `hawdb`:

```console
python3 bindings/benchmarks/retained_node_write_control.py \
  --package /path/to/frozen/package --rows 1000 --pad-bytes 4096 \
  --backend file --hold-rows 32 --samples 8
```

Cross-revision reads/writes/mixed results, full process memory bounds, graph/FTS,
large payloads, million-row coverage and power-loss qualification remain open.
