load("@crate_index//:defs.bzl", "aliases", "all_crate_deps")
load("@rules_rust//rust:defs.bzl", "rust_binary")

HAWDB_BENCHMARKS = [
    "optimizer_smoke",
    "relational_join_execution",
    "relational_join_planning",
    "aggregate_partial_spill",
    "aggregate_hash",
    "canonical_point_lookup",
    "executor_vectorization",
    "index_restart_cost",
    "integrity_checksum",
    "relational_index_access",
    "relational_index_page_cache",
    "relational_monotonic_append",
    "relational_oltp_mix",
    "relational_overflow",
    "relational_row_delta_runs",
    "relational_row_page_lending",
    "search_checkpoint",
    "search_generation",
    "search_tokenization",
    "storage_segment_read",
    "store_cow_feasibility",
    "vector_projection_scan",
    "wal_group_commit",
]

HAWDB_BENCHMARK_TARGETS = [
    ":hawdb_bench_%s" % benchmark
    for benchmark in HAWDB_BENCHMARKS
]

# Local qualification only; these are outside the existing CI smoke dispatch.
HAWDB_MANUAL_BENCHMARKS = ["concurrent_snapshot_reads"]

def hawdb_benchmark_binaries(crate_features):
    benchmark_deps = all_crate_deps(normal = True, normal_dev = True) + [
        ":hawdb",
        "//crates/core:hawdb_core",
        "//crates/executor:hawdb_executor",
        "//crates/integrity:hawdb_integrity",
        "//crates/optimizer:hawdb_optimizer",
        "//crates/qos:hawdb_qos",
        "//crates/storage:hawdb_storage",
        "//crates/vector-projection:hawdb_vector_projection",
    ]
    for benchmark in HAWDB_BENCHMARKS + HAWDB_MANUAL_BENCHMARKS:
        rust_binary(
            name = "hawdb_bench_%s" % benchmark,
            crate_root = "benches/%s.rs" % benchmark,
            crate_features = crate_features,
            srcs = ["benches/%s.rs" % benchmark] + native.glob([
                "benches/%s/**/*.rs" % benchmark,
            ], allow_empty = True),
            edition = "2024",
            tags = ["manual"] if benchmark in HAWDB_MANUAL_BENCHMARKS else [],
            aliases = aliases(
                normal = True,
                normal_dev = True,
                proc_macro = True,
                proc_macro_dev = True,
            ),
            deps = benchmark_deps,
            proc_macro_deps = all_crate_deps(
                proc_macro = True,
                proc_macro_dev = True,
            ),
        )
