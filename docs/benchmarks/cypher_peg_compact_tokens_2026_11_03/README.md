# Compact Cypher token measurements

Native macOS arm64, Rust 1.97.1, default Cargo release. The candidate runtime was
validated at tree `b1441e2e7e3800e5b0e016e36a465d859562d76e`, based on
`520d3e968e96025a0435db49e0903a7559d699f6`. Reports bind runtime file hashes;
subsequent documentation and artifact additions do not change those files.

`parser-timing.json` records eight counterbalanced processes with the unchanged
repository `cypher_parser` benchmark: five queries, 11 samples, 20,000 iterations.
All five improve against former main; equal-weighted throughput improves
25.7665 percent. No compiler/test/benchmark interference was observed; other host
activity was uncontrolled. These measurements do not establish public E2E gains.

`heap-summary.json` retains the remaining five wide and sixteen boolean metrics
that regress against former main. Parent comparisons introduce no regression.
The counters measure allocations, requested bytes and peak live requested bytes;
they do not measure RSS. All eleven retained AST byte counts equal former main.

The two probe files and their synthetic input files are archived verbatim.
To reproduce a probe, build `hawdb-cypher` with locked default release settings,
then link the corresponding exact `.rlib` and dependency directory:

```sh
cargo build --locked --release -p hawdb-cypher --lib
rustc --edition=2024 -O -Clto=thin --crate-name wide_probe \
  -L dependency=PATH_TO_RELEASE_DEPS \
  --extern hawdb_cypher=PATH_TO_HAWDB_CYPHER_RLIB wide-probe.txt -o wide_probe
./wide_probe
```

Use separate source/artifact directories for the reference and candidate. For
boolean controls, substitute `boolean-probe.txt` and a distinct executable name.
The input files must remain adjacent to their probe source. Full diagnostic,
resource-admission, final-head fuzz/CI and public E2E readiness remain open.
