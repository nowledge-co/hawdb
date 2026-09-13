# Integration ownership regression fixtures

These fixtures freeze the behavior before the #418 ownership extraction, from
main `27352706f96f2edb04576fdd27d9e6ff299f5644`:

- `src/mem_integration_bundle.rs`, blob `fc8ae48c9e9bec89b8fff0b1c29855f27ada3a5c`.
- The graph-summary unit in `src/replacement_summary.rs`, blob
  `6c39c8fa9b58eef61d981389f34fdd76b5d5b0cb`.

`ready.json` is a complete bundle output. The input reports are intentionally
small: unused query reports and route details are removed, and opaque host
reports are sentinels. It proves alignment behavior, not host activation
readiness. The root integration tests retain their full library-generated
reports and final readiness checks.

`cases.jsonl` contains 1,266 named single-report perturbations. Each row records
input `changes`, the expected derived-output delta, and the expected graph
`summary` delta. A one-element change removes a field; a two-element change
replaces it, including explicit null. Paths are JSON pointers.

All expected outputs were evaluated by the pre-migration production functions,
not by the new owner functions. The original full root ready fixture was also
compared against both implementations before reducing opaque input reports.
To refresh a fixture for an intentional behavior change, evaluate the changed
inputs against an explicitly selected reference revision and review the entire
output delta; do not silently regenerate expectations from the code under test.

The manual campaign composes six independent report groups using 32 fixed seeds
and 128 combinations per seed, then checks every individual corpus row.
Each group may perturb the evidence or its replacement-summary counterpart.
Only one perturbation per group is composed, so expected deltas have disjoint
derived-output ownership; graph alignment and graph parity share one group.
The comparison covers the whole bundle and graph-summary JSON, including array
ordering and blocker multiplicity. No production evaluator is called to build
expected results at test runtime.

Run the complete local campaign with
`bazel test //crates/readiness:skein_integration_bundle_fuzz_tests`.
It is manual and included in `//crates/fuzz:skein_fuzz_tests`, not native CI.
