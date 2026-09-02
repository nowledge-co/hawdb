load("@crate_index//:defs.bzl", "aliases", "all_crate_deps")
load("@rules_rust//rust:defs.bzl", "rust_binary")

SKEIN_BENCHMARKS = [
    "optimizer_smoke",
    "relational_join_execution",
    "relational_join_planning",
    "aggregate_partial_spill",
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
    "storage_segment_read",
    "store_cow_feasibility",
    "vector_projection_scan",
    "wal_group_commit",
]

SKEIN_BENCHMARK_TARGETS = [
    ":skein_bench_%s" % benchmark
    for benchmark in SKEIN_BENCHMARKS
]

def skein_benchmark_binaries(crate_features):
    benchmark_deps = all_crate_deps(normal = True, normal_dev = True) + [
        ":skein",
        "//crates/core:skein_core",
        "//crates/executor:skein_executor",
        "//crates/integrity:skein_integrity",
        "//crates/optimizer:skein_optimizer",
        "//crates/qos:skein_qos",
        "//crates/storage:skein_storage",
        "//crates/vector-projection:skein_vector_projection",
    ]
    for benchmark in SKEIN_BENCHMARKS:
        rust_binary(
            name = "skein_bench_%s" % benchmark,
            crate_root = "benches/%s.rs" % benchmark,
            crate_features = crate_features,
            srcs = ["benches/%s.rs" % benchmark] + native.glob([
                "benches/%s/**/*.rs" % benchmark,
            ], allow_empty = True),
            edition = "2024",
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
