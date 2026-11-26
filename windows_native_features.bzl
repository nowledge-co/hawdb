# Copyright 2026 Nowledge
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Exact Cargo --all-features selections, including forwarded dependency features.
# scripts/check-windows-native-features.py checks these against Cargo manifests.
WINDOWS_NATIVE_ALL_FEATURES = {
    "hawdb": ["test-zstd", "acl", "background-maintenance", "default", "full-text-search", "graph-analytics", "loom-tests", "opentelemetry", "qualification", "test-support", "tokio-runtime", "vector-search"],
    "hawdb-search": ["test-zstd", "acl", "background-maintenance", "default", "full-text-search", "graph-analytics", "qualification", "vector-search"],
    "hawdb-storage": ["test-zstd", "default", "artifact-mmap", "test-support"],
    "hawdb-system-sql": ["default", "loom-tests"],
    "hawdb-telemetry": ["default", "opentelemetry"],
}

# Match feature forwarding for each original Cargo package selection.
WINDOWS_NATIVE_FEATURE_PROFILES = {
    "facade_all": {
        "hawdb": ["acl", "background-maintenance", "default", "full-text-search", "graph-analytics", "loom-tests", "opentelemetry", "qualification", "test-support", "test-zstd", "tokio-runtime", "vector-search"],
        "hawdb-search": ["acl", "background-maintenance", "full-text-search", "graph-analytics", "qualification", "vector-search"],
        "hawdb-storage": ["artifact-mmap", "test-support", "test-zstd"],
        "hawdb-system-sql": ["default", "loom-tests"],
        "hawdb-telemetry": ["default", "opentelemetry"],
    },
    "search_all": {
        "hawdb": ["default", "background-maintenance", "full-text-search", "graph-analytics", "tokio-runtime", "vector-search"],
        "hawdb-search": ["acl", "background-maintenance", "default", "full-text-search", "graph-analytics", "qualification", "test-zstd", "vector-search"],
        "hawdb-storage": ["artifact-mmap", "test-zstd"],
        "hawdb-system-sql": ["default"],
        "hawdb-telemetry": ["default"],
    },
    "storage_all": {
        "hawdb": ["default", "background-maintenance", "full-text-search", "graph-analytics", "tokio-runtime", "vector-search"],
        "hawdb-search": ["background-maintenance", "full-text-search", "graph-analytics", "vector-search"],
        "hawdb-storage": ["artifact-mmap", "default", "test-support", "test-zstd"],
        "hawdb-system-sql": ["default"],
        "hawdb-telemetry": ["default"],
    },
    "facade_default": {
        "hawdb": ["background-maintenance", "default", "full-text-search", "graph-analytics", "tokio-runtime", "vector-search"],
        "hawdb-search": ["background-maintenance", "full-text-search", "graph-analytics", "vector-search"],
        "hawdb-storage": ["artifact-mmap", "test-support", "test-zstd"],
        "hawdb-system-sql": ["default"],
        "hawdb-telemetry": ["default"],
    },
    # Linux root lane for tests gated on the root `test-support` feature (#1965).
    "facade_test_support": {
        "hawdb": ["background-maintenance", "default", "full-text-search", "graph-analytics", "test-support", "tokio-runtime", "vector-search"],
        "hawdb-search": ["background-maintenance", "full-text-search", "graph-analytics", "vector-search"],
        "hawdb-storage": ["artifact-mmap", "test-support", "test-zstd"],
        "hawdb-system-sql": ["default"],
        "hawdb-telemetry": ["default"],
    },
    "facade_minimal": {
        "hawdb": [],
        "hawdb-search": [],
        "hawdb-storage": ["test-support", "test-zstd"],
        "hawdb-system-sql": ["default"],
        "hawdb-telemetry": ["default"],
    },
    "component_default": {
        "hawdb": ["default", "background-maintenance", "full-text-search", "graph-analytics", "tokio-runtime", "vector-search"],
        "hawdb-search": ["background-maintenance", "full-text-search", "graph-analytics", "vector-search"],
        "hawdb-storage": ["test-zstd"],
        "hawdb-system-sql": ["default"],
        "hawdb-telemetry": ["default"],
    },
    "bindings": {
        "hawdb": ["background-maintenance", "default", "full-text-search", "graph-analytics", "tokio-runtime", "vector-search"],
        "hawdb-search": ["background-maintenance", "full-text-search", "graph-analytics", "vector-search"],
        "hawdb-storage": ["artifact-mmap"],
        "hawdb-system-sql": ["default"],
        "hawdb-telemetry": ["default"],
    },
}

def _feature_profile_transition_impl(_settings, attr):
    return {"//:windows_native_profile": attr.profile}

# Selects Cargo features only; the compilation profile is unchanged.
_feature_profile_transition = transition(
    implementation = _feature_profile_transition_impl,
    inputs = [],
    outputs = ["//:windows_native_profile"],
)

def _feature_profile_test_binary_impl(ctx):
    binary = ctx.attr.binary[0][DefaultInfo]
    executable = ctx.actions.declare_file(ctx.label.name)
    ctx.actions.symlink(output = executable, target_file = binary.files_to_run.executable, is_executable = True)
    return [DefaultInfo(
        executable = executable,
        files = depset([executable]),
        # The binary's own runfiles would stage it under its original path, over
        # the same binary in another profile. Test binaries here carry no data.
        runfiles = ctx.runfiles(files = [executable]),
    )]

# A test executable of one Cargo feature profile, for a test runner's data.
feature_profile_test_binary = rule(
    implementation = _feature_profile_test_binary_impl,
    attrs = {
        "binary": attr.label(mandatory = True, executable = True, cfg = _feature_profile_transition),
        "profile": attr.string(mandatory = True, values = WINDOWS_NATIVE_FEATURE_PROFILES.keys()),
    },
    executable = True,
)

def windows_native_features(package, default_features):
    choices = {
        "//:windows_native_profile_" + profile: [feature for feature in features[package] if feature != "test-zstd"]
        for profile, features in WINDOWS_NATIVE_FEATURE_PROFILES.items()
    }
    choices["//conditions:default"] = default_features
    test_features = select({
        "//:compression_test_support_enabled": ["test-zstd"],
        "//conditions:default": [],
    }) if package in ["hawdb", "hawdb-search", "hawdb-storage"] else []
    return select(choices) + test_features

def windows_native_feature_dependencies(package, feature, dependencies):
    """Link optional dependencies only when their Cargo feature is selected."""
    choices = {
        "//:windows_native_profile_" + profile: dependencies if feature in features[package] else []
        for profile, features in WINDOWS_NATIVE_FEATURE_PROFILES.items()
    }
    choices["//conditions:default"] = []
    return select(choices)
