#!/usr/bin/env python3
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

"""Reject drift between native Bazel qualification and Cargo feature inventories."""

import ast
from pathlib import Path
import tomllib

root = Path(__file__).resolve().parent.parent
module = ast.parse((root / "windows_native_features.bzl").read_text())
features = next(
    ast.literal_eval(node.value)
    for node in module.body
    if isinstance(node, ast.Assign)
    and any(isinstance(name, ast.Name) and name.id == "WINDOWS_NATIVE_ALL_FEATURES" for name in node.targets)
)
for package, selected in features.items():
    manifest = root / ("Cargo.toml" if package == "hawdb" else f"crates/{package.removeprefix('hawdb-')}/Cargo.toml")
    with manifest.open("rb") as source:
        declared = tomllib.load(source)["features"]
    if len(selected) != len(set(selected)) or set(selected) != set(declared):
        raise SystemExit(f"native feature inventory differs for {package}: selected={selected}, Cargo={sorted(declared)}")
print("PASS | native all-feature inventories match Cargo")

profiles = next(
    ast.literal_eval(node.value)
    for node in module.body
    if isinstance(node, ast.Assign)
    and any(isinstance(name, ast.Name) and name.id == "WINDOWS_NATIVE_FEATURE_PROFILES" for name in node.targets)
)
manifest_paths = {
    name: root / ("Cargo.toml" if name == "hawdb" else f"crates/{name.removeprefix('hawdb-')}/Cargo.toml")
    for name in [*features, "hawdb-vector-projection"]
}
manifests = {name: tomllib.loads(path.read_text()) for name, path in manifest_paths.items()}


def expected_features(initial, development):
    selected = {name: set() for name in manifests}
    # These non-optional facade dependencies are present in every facade mode.
    for name in features:
        dependency = manifests["hawdb"].get("dependencies", {}).get(name)
        if isinstance(dependency, dict):
            selected[name].update(dependency.get("features", []))
            if dependency.get("default-features", True) and "default" in manifests[name].get("features", {}):
                selected[name].add("default")
    if development:
        selected["hawdb-storage"].update(manifests["hawdb"]["dev-dependencies"]["hawdb-storage"].get("features", []))
    for name, values in initial.items():
        selected[name].update(values)
    previous = None
    while previous != selected:
        previous = {name: values.copy() for name, values in selected.items()}
        for name, values in previous.items():
            declarations = manifests[name].get("features", {})
            for value in values:
                for forwarded in declarations.get(value, []):
                    if "/" in forwarded:
                        dependency, feature = forwarded.split("/", 1)
                        dependency = dependency.removesuffix("?")
                        if dependency in selected:
                            selected[dependency].add(feature)
                    elif forwarded in declarations:
                        selected[name].add(forwarded)
    return selected


for name, initial, development in [
    ("facade_all", features["hawdb"], True),
    ("facade_default", ["default"], True),
    ("facade_test_support", ["default", "test-support"], True),
    ("facade_minimal", ["builtin-zstd"], True),
    ("bindings", ["default"], False),
]:
    expected = expected_features({"hawdb": initial}, development)
    for package in features:
        if set(profiles[name][package]) != expected[package]:
            raise SystemExit(f"feature forwarding differs for {name}/{package}: {profiles[name][package]} != {sorted(expected[package])}")
for name, initial, relevant in [
    ("search_all", {"hawdb-search": features["hawdb-search"]}, ["hawdb-search", "hawdb-storage", "hawdb-telemetry"]),
    ("storage_all", {"hawdb-storage": features["hawdb-storage"]}, ["hawdb-storage", "hawdb-telemetry"]),
    ("component_default", {"hawdb-storage": ["builtin-zstd"]}, ["hawdb-storage", "hawdb-telemetry"]),
]:
    expected = expected_features(initial, False)
    for package in relevant:
        if set(profiles[name][package]) != expected[package]:
            raise SystemExit(f"feature forwarding differs for {name}/{package}")
print("PASS | native Cargo feature forwarding and default/minimal boundaries match")

# The optional dependency has an independent Bazel lock; do not let its exact
# spec pin drift when Cargo or Dependabot updates the workspace resolution.
root_lock = tomllib.loads((root / "Cargo.lock").read_text())
telemetry_lock = tomllib.loads((root / "crates/telemetry/Cargo.Bazel.lock").read_text())
resolved = [package for package in root_lock["package"] if package["name"] == "opentelemetry"]
standalone = [package for package in telemetry_lock["package"] if package["name"] == "opentelemetry"]
if len(resolved) != 1 or len(standalone) != 1:
    raise SystemExit("optional telemetry resolution must be unambiguous in both Cargo lockfiles")
for key in ("version", "source", "checksum"):
    if resolved[0].get(key) != standalone[0].get(key):
        raise SystemExit(f"optional telemetry Bazel lock differs from Cargo.lock: {key}")
specs = []
for node in ast.walk(ast.parse((root / "MODULE.bazel").read_text())):
    if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
        continue
    if not isinstance(node.func.value, ast.Name) or node.func.value.id != "crate" or node.func.attr != "spec":
        continue
    if not any(keyword.arg == "package" and isinstance(keyword.value, ast.Constant)
               and keyword.value.value == "opentelemetry" for keyword in node.keywords):
        continue
    keywords = {keyword.arg: ast.literal_eval(keyword.value) for keyword in node.keywords}
    if "telemetry_crates" in keywords.get("repositories", []):
        specs.append(keywords)
if len(specs) != 1 or specs[0].get("version") != "=" + resolved[0]["version"]:
    raise SystemExit("optional telemetry Bazel spec must exactly pin the Cargo.lock version")
print("PASS | optional telemetry Bazel spec and lock match Cargo.lock")
