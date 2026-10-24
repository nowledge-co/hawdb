#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Prepare three isolated, locally patched Bf-Tree comparison workspaces.

Never edits the Cargo registry, the checked-in manifest/lockfile, or production
dependencies. Builds/tests/measurements are deliberately separate steps.
"""

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import tomllib


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def replace_exact(path, old, new, count):
    # Registry sources have CRLF. Preserve it to keep the patch reviewable.
    data = path.read_bytes()
    before = old.encode()
    assert data.count(before) == count, (path, old, data.count(before))
    path.write_bytes(data.replace(before, new.encode()))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", type=Path, required=True, help="new directory")
    args = parser.parse_args()
    project = Path(__file__).resolve().parents[1]
    repo = project.parents[1]
    scratch = args.scratch.resolve()
    scratch.mkdir(parents=True, exist_ok=False)
    metadata = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--locked", "--format-version", "1",
         "--manifest-path", str(project / "Cargo.toml")], cwd=repo, text=True))
    package = next(p for p in metadata["packages"] if p["name"] == "bf-tree")
    assert package["version"] == "0.5.6"
    registry = Path(package["manifest_path"]).parent
    upstream = json.loads((registry / ".cargo_vcs_info.json").read_text())
    assert upstream["git"]["sha1"] == "3998ce8643dec4e98b07f2d11cad67bf2c2d28d1"
    lock = tomllib.loads((project / "Cargo.lock").read_text())
    locked = next(p for p in lock["package"] if p["name"] == "bf-tree")
    archive = registry.parents[2] / "cache" / registry.parent.name / "bf-tree-0.5.6.crate"
    assert sha(archive) == locked["checksum"]
    with tarfile.open(archive) as packed:
        packed_vcs = json.load(packed.extractfile("bf-tree-0.5.6/.cargo_vcs_info.json"))
    assert packed_vcs == upstream
    (scratch / ".cargo").mkdir()
    shutil.copy2(repo / ".cargo/config.toml", scratch / ".cargo/config.toml")
    shutil.copy2(repo / "rust-toolchain.toml", scratch / "rust-toolchain.toml")
    receipts = {"upstream_commit": upstream["git"]["sha1"],
                "registry_checksum": locked["checksum"], "variants": {}}
    for variant in ("upstream", "word", "neon"):
        root = scratch / variant
        dependency = root / "bf-tree"
        root.mkdir()
        with tarfile.open(archive) as packed:
            packed.extractall(root, filter="data")
        (root / "bf-tree-0.5.6").rename(dependency)
        if variant != "upstream":
            nodes = dependency / "src/nodes"
            replace_exact(nodes / "mod.rs", "mod inner_node;",
                          "mod key_compare;\r\nmod inner_node;", 1)
            kernel = (project / "simd/key_compare.rs").read_text()
            kernel += ("\n#[inline]\npub(crate) fn compare(left: &[u8], right: &[u8]) "
                       "-> std::cmp::Ordering {\n    if "
                       + str(variant == "neon").lower()
                       + " { neon(left, right) } else { word(left, right) }\n}\n")
            (nodes / "key_compare.rs").write_text(kernel)
            for file, expression, count in (
                ("leaf_node.rs", "full_key", 1),
                ("leaf_node.rs", "remaining_key", 1),
                ("inner_node.rs", "rest_key", 2),
            ):
                replace_exact(nodes / file,
                              f"cmp = {expression}.cmp(search_key_postfix);",
                              f"cmp = super::key_compare::compare({expression}, search_key_postfix);",
                              count)
        harness = root / "harness"
        harness.mkdir()
        shutil.copytree(project / "src", harness / "src")
        (harness / "simd").mkdir()
        shutil.copy2(project / "simd/key_compare.rs", harness / "simd/key_compare.rs")
        manifest = (project / "Cargo.toml").read_text()
        for name in ("qos", "storage"):
            manifest = manifest.replace(f'"../../crates/{name}"',
                                        json.dumps(str(repo / "crates" / name)))
        manifest += f'\n[patch.crates-io]\nbf-tree = {{ path = {json.dumps(str(dependency))} }}\n'
        (harness / "Cargo.toml").write_text(manifest)
        shutil.copy2(project / "Cargo.lock", harness / "Cargo.lock")
        # Resolve only the registry -> local path replacement, offline. Refuse
        # any unrelated dependency or version change before compiling.
        subprocess.run(["cargo", "metadata", "--offline", "--format-version", "1",
                        "--manifest-path", str(harness / "Cargo.toml")],
                       cwd=scratch, stdout=subprocess.DEVNULL, check=True)
        actual = tomllib.loads((harness / "Cargo.lock").read_text())
        expected = json.loads(json.dumps(lock))
        bf = next(p for p in expected["package"] if p["name"] == "bf-tree")
        del bf["source"]
        del bf["checksum"]
        assert actual == expected, "unrelated lockfile changes"
        files = {str(p.relative_to(root)): sha(p) for p in root.rglob("*")
                 if p.is_file() and (p.suffix == ".rs" or p.name in ("Cargo.toml", "Cargo.lock"))}
        receipts["variants"][variant] = {"manifest": str(harness / "Cargo.toml"),
                                          "source_sha256": files}
    (scratch / "source.json").write_text(json.dumps(receipts, indent=2) + "\n")
    print(f"Prepared upstream/word/neon under {scratch}; source identities in source.json")


if __name__ == "__main__":
    main()
