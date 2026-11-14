# Selected Raw-Vector Decoding

> **Superseded (#1469).** Layout `HAWDB_SEARCH_OUT_OF_CORE_LAYOUT_V2` stores raw
> vectors as checksummed binary rows, so serving reads and decodes only the
> selected rows and no longer parses decimal text. The text decoder and its
> `vector_selection_tests` were removed with the V1 layout; see
> [STORAGE.md](STORAGE.md#binary-vector-sidecar-layout-v2). The measurements
> below remain historical evidence for the V1 decoder.

Issue #1067 reduces raw-vector preparation before bounded RaBitQ reranking.
Each touched vector frame is still read, checksummed, decompressed and fully
validated. Only embeddings for selected ordinals are materialized. Unselected
components use a finite-decimal check for a narrow normalized subset; every
other representation uses the original float parser. Selected values always
use the original parser, preserving their exact bits.

Segments whose sorted, unique candidates cover every vector ordinal use the
full-decoding entry point directly, avoiding per-row membership checks. The
decoder still validates every row, including ordinal layout and ordering.

The optimization leaves query arithmetic, candidate policy, persistent
formats, read budgets and cancellation checkpoints unchanged. It does not
introduce a validation cache or skip malformed unselected rows. The full raw
scan uses the same validator with every ordinal selected.

## Initial Public Serving-Query Comparison

The control is main `4c32610910cc78771eeb23d729be7107ff769bf6` with the
public-API profiling helper from #1066 at
`3ae50b58614d8b15e7dc0ca5210e091c070c2e10`. The candidate uses the same base
with the selected decoder. The measured runtime source hashes are retained
in the [candidate build record](benchmarks/vector_selected_decode_linux_2026_10_18/query-build.json);
the [original build record](benchmarks/vector_selected_decode_linux_2026_10_18/original-query-build.json)
identifies the frozen control binary. This initial candidate is recorded at
`1e539b3f056daf9fa88028198d2f8914872db3a9`. Its timings remain evidence for
that revision; subsequent full-coverage refinement requires fresh query
measurements, described in the candidate-density qualification below.

Both binaries use pinned Rust 1.97.1, the repository release profile with
`CARGO_PROFILE_RELEASE_DEBUG=1`, root defaults plus `vector-search`, and an
explicit scalar candidate kernel. The fixture is the same immutable
100,000-document, 384-dimension, default 4-bit generation used by #1066.
It uses fixed-seed xorshift64* unit vectors and synthetic document bodies.
The fixture's complete [file hash inventory](benchmarks/vector_selected_decode_linux_2026_10_18/fixture-sha256.json)
was identical before and after all four processes.

The comparison runs control/candidate/candidate/control sequentially. Each
process opens the same fixture, performs three warmups, and then measures
100 queries after a stdin handshake. There is no sampling profiler. No owned
compiler or fuzz process overlaps the timed comparison; lightweight source
inspection and remote CI polling occur during the overall run. The host is
shared Linux x86_64 on an AMD Ryzen 7 7735HS, 16 logical CPUs, about 28 GiB RAM
and NVMe Btrfs. These are warm-cache measurements, not idle-host or OS-cold
qualification.

The query requests ten results and `rank_window = 100`. The serving policy
actually scores 100,000 quantized vectors and reranks 1,024 candidates. The
probe verifies stable final IDs and vector-score bits on every iteration.
Across all processes, its recorded backend, kernel, candidate counts, worker
counts, selected raw-vector bytes and admitted candidate bytes match exactly.
The reported 13 workers sum sequential artifact layers; configured parallelism
is one. Selected raw-vector bytes are not total decompressed or decoded bytes.

| Run | Median ms | P95 ms | Min..max ms | Process peak RSS KiB |
| --- | ---: | ---: | ---: | ---: |
| Control 1 | 1,536.93 | 1,559.53 | 1,521.45..1,567.29 | 40,680 |
| Candidate 1 | 1,152.92 | 1,169.56 | 1,148.46..1,243.33 | 40,748 |
| Candidate 2 | 1,153.93 | 1,169.63 | 1,148.83..1,180.75 | 40,936 |
| Control 2 | 1,500.47 | 1,514.78 | 1,492.43..1,532.39 | 40,792 |

Pooling both blocks gives 200 measured queries per version. Median latency
falls from **1,522.58 ms to 1,153.43 ms**, a **24.25% reduction** in this
fixture. P95 falls from 1,554.69 ms to 1,169.56 ms. Both control blocks are
retained, including their roughly 2.4% median drift; no outlier is removed.
These results do not establish the same gain for every Mem request or corpus.

Peak RSS is the kernel's `wait4` maximum for the entire child process,
including opening and warmups. It remains about 40 MiB; **no process peak-RSS
reduction is established**. Avoiding unselected embedding allocations is an
implementation property, not a substitute for an application residency claim.
CPU totals likewise include the complete child process.

- [All reports and 400 elapsed times](benchmarks/vector_selected_decode_linux_2026_10_18/query-comparison.json)
- [Per-run and pooled summaries](benchmarks/vector_selected_decode_linux_2026_10_18/query-summary.json)
- [Exact local comparison driver](benchmarks/vector_selected_decode_linux_2026_10_18/compare_queries.py)

## Reproduction

For each clean source revision, temporarily place the same #1066 example in
`examples/rabitq_query_profile.rs`, then build it with:

```sh
git show 3ae50b58614d8b15e7dc0ca5210e091c070c2e10:examples/rabitq_query_profile.rs \
  > examples/rabitq_query_profile.rs
CARGO_PROFILE_RELEASE_DEBUG=1 cargo build --locked --release \
  --example rabitq_query_profile --features vector-search
```

Preserve each binary under a separate name. Build the synthetic fixture once
with the control binary's `build` mode; use both binaries' `profile` mode on
that same directory. Send `run` after each ready record, without attaching a
profiler. The retained local driver records the exact order, handshake, source
and binary hashes, child resource usage and fixture-hash checks. Its four path
constants identify the original local run; adjust them to separately preserved
binaries and a new output directory for another host. Existing output
directories are rejected.

The helper is a developer probe from #1066, not a new production dependency of
this change. Remove the temporary copy when it is not part of the checked-out
revision. Neither binary, the fixture nor a profiler capture is checked into
this report.

## Decoder and Preparation Matrix

The ignored `out_of_core::vector_selection_tests::selected_vector_decode_matrix`
uses the original full decoder as an independent reference. Its manual
campaign checks 20,000 generated inputs, including malformed rows and error
precedence. Ordinary CI cases additionally cover every subset of a mixed
floating-point fixture, empty/no-match selection, selected and unselected
corruption, signed zero, finite boundaries, scientific forms, long fractions,
invalid Unicode, ordinal saturation, count/bound/order failures, and explicit
Required-mode checksum, budget and cancellation guards.

The timing matrix uses 1/32/8,192 rows, dimensions 3/384, all/half/approximately
1% selection, one warmup and five measurements. Rows contain repeated
deterministic decimal embeddings and unique IDs. Fixture construction, bit
verification and result destruction are outside the timers; implementation
order alternates between repetitions. This is an in-memory text-decoding
comparison, not storage I/O, query latency or RSS measurement.

Run only the manual entry in release mode:

```sh
cargo test --locked --release -p hawdb-search --all-features --lib \
  out_of_core::vector_selection_tests::selected_vector_decode_matrix \
  -- --exact --ignored --nocapture --test-threads=1
```

The current harness separates two comparable boundaries:

- `selected_preparation` includes decoding and the old post-decode ordinal
  filter, or decoding with selection and the corresponding returned-row
  traversal. Both touch the same selected IDs and embeddings without copying
  embeddings or introducing rerank arithmetic.
- `full_decode` selects every row with a constant predicate, matching the full
  raw-scan caller. Both variants traverse every returned row without an
  ordinal lookup.

The initial decoder-only run is retained separately:
[raw output](benchmarks/vector_selected_decode_linux_2026_10_18/decoder-only.log),
[summary](benchmarks/vector_selected_decode_linux_2026_10_18/decoder-only-summary.json),
and [build provenance](benchmarks/vector_selected_decode_linux_2026_10_18/decoder-only-build.json).
Its old post-decode filter was outside the timer, while the new decoder
included selection. It therefore cannot establish a full preparation gain or
regression: for example, the 8,192-row/3-dimension all-selected decoder-only
median increased from 2.599 ms to 2.861 ms at those different boundaries.
Use the matched preparation/full-decode comparison to assess that case.

Three complete matched runs retain all samples: each has 48 timing groups,
288 timing records and the same 20,000-case differential campaign. The first
matched run showed a noisy 32-row/384-dimension all-selected preparation group
(14.42% higher median, with candidate samples from 0.502 to 0.981 ms). This
prompted two complete repeats of the unchanged frozen binary. The table pools
all 15 measured samples per variant/group, including that first run. Repeating
the deterministic campaign does not increase its number of distinct inputs.

| Phase | Rows | Dimensions | Selection stride | Original median ms | Candidate median ms | Time change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Preparation | 32 | 384 | 1 | 0.503 | 0.509 | +1.16% |
| Preparation | 32 | 384 | 2 | 0.499 | 0.421 | -15.62% |
| Preparation | 32 | 384 | 100 | 0.495 | 0.337 | -31.88% |
| Preparation | 8,192 | 384 | 1 | 86.829 | 87.310 | +0.55% |
| Preparation | 8,192 | 384 | 2 | 87.643 | 73.126 | -16.56% |
| Preparation | 8,192 | 384 | 100 | 87.002 | 58.113 | -33.20% |
| Preparation | 8,192 | 3 | 1 | 2.734 | 2.817 | +3.05% |
| Preparation | 8,192 | 3 | 2 | 2.733 | 2.490 | -8.91% |
| Preparation | 8,192 | 3 | 100 | 2.651 | 1.996 | -24.69% |
| Full decode | 32 | 384 | 1 | 0.497 | 0.503 | +1.23% |
| Full decode | 8,192 | 384 | 1 | 87.061 | 87.475 | +0.48% |
| Full decode | 8,192 | 3 | 1 | 2.572 | 2.672 | +3.89% |

Sparse and half-selected preparation benefit; all-selected decoding can incur
overhead. In particular, the 3-dimensional bulk full decode is 3.89% slower
in this pooled matrix. This tradeoff is retained, not treated as a universal
speedup. One-row cases range from -4.16% to +2.12% (mostly nanosecond-scale
differences); all rows and individual times are available in the
[pooled summary](benchmarks/vector_selected_decode_linux_2026_10_18/matched-pooled.json).
These decoder measurements do not replace an end-to-end dense-query comparison.

The raw runs and frozen binary/source provenance are retained for independent
recalculation:

- Run 2: [output](benchmarks/vector_selected_decode_linux_2026_10_18/matched-2.run.log),
  [summary](benchmarks/vector_selected_decode_linux_2026_10_18/matched-2.summary.json),
  [build](benchmarks/vector_selected_decode_linux_2026_10_18/matched-2.build.json).
- Run 3: [output](benchmarks/vector_selected_decode_linux_2026_10_18/matched-3.run.log),
  [summary](benchmarks/vector_selected_decode_linux_2026_10_18/matched-3.summary.json),
  [build](benchmarks/vector_selected_decode_linux_2026_10_18/matched-3.build.json).
- Run 4: [output](benchmarks/vector_selected_decode_linux_2026_10_18/matched-4.run.log),
  [summary](benchmarks/vector_selected_decode_linux_2026_10_18/matched-4.summary.json),
  [build](benchmarks/vector_selected_decode_linux_2026_10_18/matched-4.build.json).

Tracked log copies omit trailing empty lines to satisfy repository whitespace
checks. All records remain unchanged. [Original and recorded log hashes](benchmarks/vector_selected_decode_linux_2026_10_18/log-normalization.json)
identify that normalization; summary hashes refer to the original logs, which
remain in the local artifact directories.

## Candidate-Density Qualification

The density probe uses the same immutable 100,000-document/384-dimension
fixture and requests ten final hits, with candidate limits of 1,024, 50,000
and 100,000. It sets both `max_vector_candidates` and `rank_window` to that
limit through ordinary typed options. Each case runs control/candidate/
candidate/control, with three warmups and ten measured queries per process.
The two measured blocks retain 20 samples per version and density. Every
sample is kept; the complete fixture inventory is unchanged before and after.

The probe compares the complete `SearchOutOfCoreOutput` Debug representation
on every query, plus explicit final ID/score bits. Cross-process comparison
checks the same complete output and report fields, excluding the process PID.
Both builds use the same Rust compiler and output type definitions; Debug
formatting is a developer probe contract, not a stable production serialization
format. All output validation occurs outside the query timer. Process CPU/RSS
still include opening, warmups and validation.

The first density run exposed a regression in candidate `1e539b3f`:

| Selected candidates | Original median ms | Initial candidate median ms | Time change |
| ---: | ---: | ---: | ---: |
| 1,024 | 1,501.22 | 1,160.31 | -22.71% |
| 50,000 | 2,016.88 | 1,912.74 | -5.16% |
| 100,000 | 2,056.60 | 2,300.22 | +11.85% |

All 120 measured queries preserved complete outputs, but sparse gains did not
justify the all-selected regression. The implementation therefore uses the
existing full-decoding entry point when a segment's selected count equals
its entry count. Candidate ordinals have already been sorted and checked for
duplicates, and the slice contains only ordinals in that segment's range, so
equal counts prove full coverage. This retains validation and final reranked
count checks. Required-mode regressions cover both sparse and full segments,
budget/cancellation/checksum failures, and full-selection score equality with
the raw-vector backend.

The first run remains available without replacing its measurements:

- [All outputs and times](benchmarks/vector_selected_decode_linux_2026_10_18/density-before-comparison.json)
- [Per-run and pooled summaries](benchmarks/vector_selected_decode_linux_2026_10_18/density-before-summary.json)
- [Fixture inventory](benchmarks/vector_selected_decode_linux_2026_10_18/density-fixture-sha256.json)
- [Control build](benchmarks/vector_selected_decode_linux_2026_10_18/density-original-build.json)
  and [initial candidate build](benchmarks/vector_selected_decode_linux_2026_10_18/density-before-build.json)
- [Exact query probe](benchmarks/vector_selected_decode_linux_2026_10_18/query_rerank_qualification.rs)
  and [comparison driver](benchmarks/vector_selected_decode_linux_2026_10_18/compare_query_density.py)

The refined candidate was rebuilt and frozen, then the complete twelve-process
matrix was repeated against the same control binary and fixture. All 120 new
measured queries preserve the complete outputs, including equality with the
first density campaign. The measured production source hashes are in the
[refined build record](benchmarks/vector_selected_decode_linux_2026_10_18/density-refined-build.json).
The two production source hashes still match the final implementation. The
recorded test-source hash predates a test-only correction: checksum injection
now targets the frame containing the returned hit rather than the first frame
in the payload file, which a sparse query may never read. That correction does
not change either measured binary or its production source.

| Selected candidates | Original median ms | Refined candidate median ms | Time change |
| ---: | ---: | ---: | ---: |
| 1,024 | 1,491.06 | 1,169.24 | -21.58% |
| 50,000 | 2,006.34 | 1,799.39 | -10.31% |
| 100,000 | 2,047.34 | 2,063.30 | +0.78% |

Full selection retains a small measured overhead; this is not a claim of zero
cost or a speedup for every density. The observed large regression falls from
11.85% to 0.78%. The 3-dimensional full-decoding overhead in the matched matrix
also remains part of the evidence. Per-process RSS ranges from 39.3 to 46.8 MiB
across these cases and does not show a consistent reduction versus the control.
The fresh comparison ran after all owned compilation, fuzz, artifact downloads
and independent artifact hashing ended. Light source edits and remote CI
polling occurred during the overall run. The host remains shared and cache-warm.

- [All refined outputs and times](benchmarks/vector_selected_decode_linux_2026_10_18/density-refined-comparison.json)
- [Per-run timings, resource measurements and pooled summaries](benchmarks/vector_selected_decode_linux_2026_10_18/density-refined-summary.json)

To reproduce, place the retained query probe temporarily in
`examples/query_rerank_qualification.rs` on each clean source revision, build
with the same release profile, and preserve the binaries separately:

```sh
CARGO_PROFILE_RELEASE_DEBUG=1 cargo build --locked --release \
  --example query_rerank_qualification --features vector-search
python3 docs/benchmarks/vector_selected_decode_linux_2026_10_18/compare_query_density.py \
  --original /path/to/control-binary \
  --candidate /path/to/candidate-binary \
  --fixture /path/to/unchanged-serving-fixture \
  --output /path/to/new-output-directory
```

The driver refuses an existing output directory. Remove the temporary example
after preserving each binary. A preliminary all-selected feasibility probe
overlapped the earlier artifact download; none of its elapsed times contributes
to either performance table.
