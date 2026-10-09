# Exact-count relational access smoke qualification

The default Bazel smoke completed all twenty persisted comparisons with complete
identical IDs and payloads. It uses 256 rows, three warm samples, both 64- and
1,024-byte bodies, and clustered/dispersed membership. This records an earlier
metadata-count candidate before descriptor/bound access charges, at base
`d97c0aab33fb7436bcfc01d948972db88bc2f94b`,
runtime inventory `d287facdb86aa3d26d5ac03ba37714dc27600d2b0a5b01f62d1c0a1a5df587cf`.
The default build and smoke scale differ from the earlier release campaign;
their absolute timings cannot establish a before/after performance improvement.

The [lossless recording](benchmarks/relational_access_macos_2026_10_07_exact_counts_smoke.json)
retains every first profile and all three warm profiles, resident cases, binary
and raw-output hashes, and qualification scope. Expanding `common` with each
`samples` object recovers each complete original warm profile. The
[benchmark contract](RELATIONAL_ACCESS_COST_BENCHMARK.md) owns the full result
and independent cost-selection assertions.

This contributes to [HawDB issue #216](https://github.com/nowledge-co/hawdb/issues/216).
Exact counts correct value-specific estimates and select scans for broad/all-row
prefixes in this matrix. They do not establish that the selected plan is always
faster. Full-scale release calibration, further contextual policy qualification,
Mem runtime acceptance and delivery remain open.

| Body bytes | Layout | Shape | Matches | Operator | Estimate | First selected/scan | Warm selected/scan |
| ---: | --- | --- | ---: | --- | ---: | ---: | ---: |
| 64 | clustered | point | 1 | TablePointGetExec | 1 | 0.976 | 0.182 |
| 64 | clustered | sparse_prefix | 2 | IndexRangeScanExec | 2 | 1.035 | 1.924 |
| 64 | clustered | medium_prefix | 23 | IndexRangeScanExec | 23 | 1.057 | 3.174 |
| 64 | clustered | broad_prefix | 231 | TableFullScanExec | 256 | 1.029 | 1.722 |
| 64 | clustered | all_rows_prefix | 256 | TableFullScanExec | 256 | 1.029 | 1.726 |
| 64 | dispersed | point | 1 | TablePointGetExec | 1 | 0.978 | 0.170 |
| 64 | dispersed | sparse_prefix | 2 | IndexRangeScanExec | 2 | 1.028 | 1.907 |
| 64 | dispersed | medium_prefix | 23 | IndexRangeScanExec | 23 | 1.059 | 3.145 |
| 64 | dispersed | broad_prefix | 231 | TableFullScanExec | 256 | 1.024 | 1.739 |
| 64 | dispersed | all_rows_prefix | 256 | TableFullScanExec | 256 | 1.025 | 1.704 |
| 1024 | clustered | point | 1 | TablePointGetExec | 1 | 0.984 | 0.163 |
| 1024 | clustered | sparse_prefix | 2 | IndexRangeScanExec | 2 | 1.026 | 1.869 |
| 1024 | clustered | medium_prefix | 23 | IndexRangeScanExec | 23 | 1.156 | 3.296 |
| 1024 | clustered | broad_prefix | 231 | TableFullScanExec | 256 | 1.024 | 1.668 |
| 1024 | clustered | all_rows_prefix | 256 | TableFullScanExec | 256 | 1.027 | 1.705 |
| 1024 | dispersed | point | 1 | TablePointGetExec | 1 | 0.984 | 0.167 |
| 1024 | dispersed | sparse_prefix | 2 | IndexRangeScanExec | 2 | 1.056 | 1.317 |
| 1024 | dispersed | medium_prefix | 23 | IndexRangeScanExec | 23 | 1.054 | 2.741 |
| 1024 | dispersed | broad_prefix | 231 | TableFullScanExec | 256 | 1.016 | 1.720 |
| 1024 | dispersed | all_rows_prefix | 256 | TableFullScanExec | 256 | 1.048 | 1.665 |

Ratios above one favor the unindexed scan. Point warm queries favor the point
path. Every non-point selected arm is slower than its unindexed scan in this
small smoke: sparse and medium prefixes retain indexed point-fetch overhead;
broad/all-row selections still pay observable planning metadata although their
execution scans. These regressions are retained rather than discarded. The
first-handle sample also includes uncontrolled OS cache and opening effects;
it is not cold-device proof. Warm p95/p99 are maxima of three samples and do
not characterize stable tail latency.

A selected scan reports zero index row visits while retaining planning index
page/file attribution. An executed index reports complete locator visits and
its exact matching estimate. Profiles, limits and complete results agree; no
assertion, retry, timeout or CI job was relaxed for this qualification.
