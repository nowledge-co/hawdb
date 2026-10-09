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
HAWDB_MANUAL_BENCHMARKS = [
    "branch_catalog_inspection",
    "concurrent_snapshot_reads",
    "concurrent_writers",
    "graph_analytics",
    "host_boundary",
    "host_boundary_allocations",
]

def _release_benchmark_transition_impl(_settings, _attr):
    return {"//command_line_option:compilation_mode": "opt"}

_release_benchmark_transition = transition(
    implementation = _release_benchmark_transition_impl,
    inputs = [],
    outputs = ["//command_line_option:compilation_mode"],
)

def _release_benchmark_impl(ctx):
    binary = ctx.attr.binary[0][DefaultInfo]
    executable = ctx.actions.declare_file(ctx.label.name)
    ctx.actions.symlink(
        output = executable,
        target_file = binary.files_to_run.executable,
        is_executable = True,
    )
    return [DefaultInfo(
        executable = executable,
        runfiles = binary.default_runfiles,
    )]

# Configure the existing binary and its dependencies together. Optimizing only
# the benchmark crate would leave the measured database in the smoke profile.
_release_benchmark = rule(
    implementation = _release_benchmark_impl,
    executable = True,
    attrs = {
        "binary": attr.label(
            executable = True,
            cfg = _release_benchmark_transition,
        ),
        "_allowlist_function_transition": attr.label(
            default = "@bazel_tools//tools/allowlists/function_transition_allowlist",
        ),
    },
)

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
        shared_srcs = [
            "benches/host_boundary/workload.rs",
            "benches/host_boundary/checksum.rs",
            "//bindings/benchmarks:native_alloc.rs",
        ] if benchmark == "host_boundary_allocations" else []
        rust_binary(
            name = "hawdb_bench_%s" % benchmark,
            crate_root = "benches/%s.rs" % benchmark,
            crate_features = crate_features,
            srcs = ["benches/%s.rs" % benchmark] + native.glob([
                "benches/%s/**/*.rs" % benchmark,
            ], allow_empty = True) + shared_srcs,
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
    _release_benchmark(
        name = "hawdb_bench_relational_index_access_release",
        binary = ":hawdb_bench_relational_index_access",
        tags = ["manual"],
    )
