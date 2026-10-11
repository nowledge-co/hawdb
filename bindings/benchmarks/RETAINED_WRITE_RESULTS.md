# Source capacity and ordinary-write controls

These observations explain why retained reads can affect writes even though
Arrow export does not participate in WAL, index maintenance or commit fsync.
They do not complete PR #986's performance or memory qualification.

## Mechanism and measurement boundary

An unconsumed cursor retains an immutable node directory and its pages. A write
to a shared page must preserve that old generation through COW; the modified
page's property allocations are cloned with it. A slow consumer extends the old
generation's lifetime. This is a conditional cost: a writer may already share
pages with another reader, and rewriting a previously detached page need not
clone it again. Keeping an old generation alive also increases memory retention
even when no additional copy is necessary for a particular write.

The source-capacity cache adds inline metadata to the shared segmented map,
copies that metadata with snapshots and invalidates it before mutable access.
Ordinary writes never walk the source to recalculate capacity. The first retained
read after mutation performs that cold walk, outside this write timer.

`retained_write_control.py` uses three separate fixtures: no cursor, a cursor
closed before writing, and an unconsumed cursor held through writing. It times
the same ordinary bulk `SET` and result destruction. Fixture setup, source
admission, consumption of the old snapshot and new-value verification remain
outside the timer. No Arrow API is called. All modes verify updated row count and
values; held cursors verify the unchanged old values, successful EOF and source
release. Native retained bytes, owner groups and handles return to zero after
each iteration. Persistent commits keep `SyncOnEveryWrite`.

## Frozen producers

Default `bazel build -c opt` produced the candidate at commit
`c7e8a08e0dc62907c5ae3165b150d69d36130fd7`, tree
`8898b1e48f84d23bbfb5e845c66dc935e908c65d`. Its Python extension SHA-256 is
`df1a610d43eb7a45d518028eddbc03b06aac970d46dd0bfad4ca44e3f36e13d1`.

The uncached source-admission prototype is based on commit
`6aab8299d96ab8727225cac3e63e50d76a0cce8c`, staged tree
`335f77639782140211c9c24d16697e9e900972df`. Its extension SHA-256 is
`2a340b2510635e44c9c17139f073cf9cddbd70ac81f049e0694052c52a774254`.
It is an uncommitted prototype, not a main-branch baseline. The comparison does
not isolate every intervening change or establish cache causality.

Both frozen packages use the identical final write consumer, SHA-256
`4b83e1e954e79f6b7a5d987c0961bddc08fa2ed49cfd56107eaca878f7633de8`,
and CPython 3.14.6 on macOS arm64. Source and artifact guards pass before and
after every child. Producer blocks alternate by group; mode order rotates within
each producer. Each mode has one discarded and seven measured iterations.
All eight children pass: 168 measured and 24 discarded writes.

```console
bazel build -c opt //bindings/benchmarks:retained_write_control
bazel-bin/bindings/benchmarks/retained_write_control \
  --rows 1000 --pad-bytes 4096 --samples 7 --backend file
```

## Observations

Candidate medians in milliseconds:

| Rows | Unrequested string bytes per row | Backend | No cursor | Closed cursor | Open cursor | Open/no-cursor paired median |
| ---: | ---: | --- | ---: | ---: | ---: | ---: |
| 1,000 | 4,096 | memory | 2.148 | 2.065 | 2.222 | 1.049 |
| 10,000 | 64 | memory | 28.694 | 28.401 | 29.171 | 1.013 |
| 1,000 | 4,096 | file | 6.782 | 7.054 | 7.149 | 1.077 |
| 10,000 | 64 | file | 34.411 | 34.574 | 35.252 | 1.016 |

The last column is the median of seven within-iteration time ratios, not the
ratio of the two median times. Full ratios and all samples are preserved in
[the evidence JSON](evidence/source_capacity_controls_20261011.json).
The open-cursor cost is a workload observation, not a universal COW penalty.

Candidate/uncached-prototype ratios of median write times, where above one is
slower:

| Rows | Backend | No cursor | Closed cursor | Open cursor |
| ---: | --- | ---: | ---: | ---: |
| 1,000 | memory | 1.052 | 1.017 | 1.015 |
| 10,000 | memory | 0.926 | 0.924 | 0.918 |
| 1,000 | file | 1.142 | 1.145 | 1.122 |
| 10,000 | file | 0.975 | 0.993 | 1.006 |

The unfavorable 1,000-row file result remains evidence. These are sequential
producer blocks on a shared host, not paired inter-revision timings. Other
compilation processes were present. The mix of faster and slower groups does
not prove that cache maintenance is negligible or that it caused the file
difference. The complete ordinary-write gate remains open.

Every candidate source preflight visits all fixture rows before any source
advancement. The 1,000-row fixture admits 5,230,264 source bytes; the 10,000-row
fixture admits 11,571,968. Each prior mutation invalidates the cache, so this
write-heavy control intentionally does not reuse a warm source bound.

## Separate read observations and refusals

The same frozen candidate completes 84 measured owned/retained/Arrow numeric
operations at 1,000/10,000 rows, both backends and seven rotated rounds.
Arrow's complete-consumption paired medians improve 2.740-2.878x; the minimum
pair is 2.300x. All successful samples preserve checksum/order and release
retained resources. This is execution-buffer-to-host delivery, not source reuse.

Both 100,000-row attempts fail while constructing the ordinary baseline result,
before the measured rounds: `result_materialization` would charge 16,777,386
bytes against its unchanged 16,777,216-byte allowance. These are failed groups,
not speedups or successful large retained-query qualification. Error reports
and tracebacks remain in the evidence JSON. Ordinary source admission and a
budget refusal cannot be substituted for a successful performance baseline.

Main `5a5c4639` also builds with default macOS opt settings but its frozen Python
extension fails import with `mis-aligned LINKEDIT string pool`. Its extension
SHA-256 is `5f4634fe7860b14a893d5a659bcb4ee4433862364aedce523bf9cbfc1e2a86a6`.
Main `3cdb2610` differs only in three test deadline attributes. No global compiler
override or binary repair was used. This outcome does not qualify a current-main
comparison.

The five-case cross-language matrix, single-write and point/wide controls,
source/planner/foreign-memory bounds, cold admission cancellation and whole-query
RSS remain incomplete. Process high-water RSS includes fixture/runtime work;
it does not measure the bytes retained solely by an old page generation.
