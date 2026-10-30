# Resident BM25 query-term statistics, 2026-10-27

Ordinary resident search now retains DF and a reusable TF vector only for the
query terms, using the existing canonical streaming seed counter. It still
analyzes every searchable field and retains complete weighted document lengths,
corpus document count and average length over the **existing filtered domain**.
The complete text seed API retains its distinct full-corpus contract.

The original full-vocabulary `TextCorpusStats` and `bm25_score` are unchanged
apart from becoming test-only. They remain independent correctness/performance
references for this experiment and the existing lexical tests. No public API,
BM25 formula, term-summation order, rank/fusion/window policy or disk format
changes. Ref #1289.

## Procedure and source binding

Baseline algorithm: main `d2511859f0952680eb93cd8393c747e9be72781f`, retained
inside the candidate test binary. Candidate: this PR's working tree on that
base, bound by the hashes below. Both algorithms use identical immutable
fixtures, query term sets, analyzers, Rust compiler and repository test profile.
This compares statistics construction plus scalar scoring, not two separate
end-to-end query executables.

| Source | SHA-256 |
| --- | --- |
| `crates/search/src/lib.rs` | `c41c74a20c5d836956f2ed2610a14c1433bda20d7672334d42d6af74ff203cc4` |
| `crates/search/src/resident_bm25.rs` | `445d2f888369ea726abec79c386cba5f3ed9923b998b45d8d7676e8dd5dc079d` |
| `crates/search/src/resident_bm25/tests.rs` | `ea21db8d90f83ef577c5e6d8593ec35c93e366c49b50e6420ee8b5a7e64975cb` |
| `crates/search/src/resident_text_seed.rs` | `6e8c09450b6647d1e2afa518348277bd59a884d0cb41f0f9042d6ca4654e3e39` |

Frozen default-feature checked test executable SHA-256: `65e6c20e446b6dccde4e33101cedffc9ab538376dcd36d995466bfc83eecaf33`.

```console
cargo test --locked -p hawdb-search --lib resident_bm25_measurement --no-run
# Copy and hash the executable, then invoke the immutable copy:
/path/to/frozen-test-binary resident_bm25::tests::resident_bm25_measurement --exact --ignored --nocapture --test-threads=1
```

Four process invocations, two warmup pairs per fixture, then 21 pairs alternating
legacy/candidate execution order. Each sample includes corpus statistics and
scores for every document. Report the median of the four process-level p50s;
all 21 samples per algorithm per process are retained in
[measurements.json](measurements.json). Source/fixture preparation, query term
creation, filtering, vector work, hydration, result allocation and formatting
are outside these measurements. This task's compilation finished before timing;
other host activity and CPU frequency are uncontrolled.

Environment: macOS arm64, Mac15,8, 16 logical CPUs, 128 GiB RAM, pinned Rust
1.97.1, repository checked Cargo test profile and default Search features.
No optimization overrides, persisted data or durability settings are involved.

Fixtures contain 4/64/128 resident documents. Each has a `graph` title, a
`graph storage` body prefix and 8/64/256 additional distinct alphabetic words,
plus searchable kind/space/external-ID metadata. Generated word names use three
base-26 letters and an x suffix, wrapping after 17,576 names; canonical phrase
and identifier aliases remain enabled. Query sets contain graph/storage plus
zero or thirty absent alphabetic terms. The allocation regression separately
uses 32 documents × 128 extra words and asserts that the legacy corpus DF has
more than 4,096 terms, preventing a trivial vocabulary fixture.

## Results

| Docs / extra words per doc / query terms | Legacy peak bytes | Candidate peak bytes | Legacy p50 µs | Candidate p50 µs | Timing change |
| --- | --- | --- | --- | --- | --- |
| 4 / 8 / 2 | 9,334 | 1,818 | 156.396 | 159.000 | +1.67% |
| 4 / 8 / 32 | 9,334 | 3,234 | 145.500 | 148.375 | +1.98% |
| 64 / 64 / 2 | 654,318 | 11,443 | 12,176.334 | 11,396.208 | -6.41% |
| 128 / 256 / 2 | 2,776,679 | 44,443 | 90,168.459 | 83,672.021 | -7.20% |

The 128 × 256 fixture lowers measured peak requested heap by about 98.4% and
observes about 7.2% lower checked-profile scorer p50. The small controls observe
about 1.7–2.0% higher p50 despite lower allocation peaks. These are local kernel
measurements; they establish neither general end-to-end latency nor remote CI
speedup. No RSS improvement or production memory budget qualification is claimed.

The peak includes the TF/DF maps/vectors and analyzer allocations during both
passes, excluding previously retained inputs. Only the persistent **statistic
tables** shrink to query-term size: canonical per-field analyzer deduplication
still depends on field vocabulary, and ordinary query candidates/results remain
outside this probe. CJK opaque working sets are not qualified by the ASCII
allocation/timing fixture.

## Correctness and ownership

Existing full-vocabulary code is the oracle. Ordinary tests compare corpus
counts, average-length bits, query DF and every raw BM25 score bit, including
empty/unknown terms, 32/128-term sets, weighted/repeated fields, ignored versus
searchable metadata, CJK, custom aliases/stopwords and four deterministic mixed
corpora. A negative control proves that the filtered fixture's scores differ
from full-corpus statistics. Embedded resident text and exact hybrid queries
check tied ranks, offsets, total hit counts, rank windows and fused score bits.
Existing seed tests preserve full-corpus complete mode, cancellation and memory
ownership. The vocabulary-heavy allocation guard requires candidate peak below
half of legacy peak; observed 665,598 → 22,443 bytes on that fixture.

Tests stay in the existing Search feature matrix. The measurement is ignored
and manual, not a CI timing gate. Test/build console logs belong in PR evidence
rather than this benchmark directory.
