#!/usr/bin/env python3
"""Local isolated-consumer inventory and native release footprint evidence (#647)."""
import argparse
from collections import deque
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tools/consumer-profiles"


def normal_closure(metadata, include_build=False):
    """Target-filtered closure; optionally include build edges, never dev edges."""
    nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
    root = metadata["resolve"]["root"]
    paths = {root: [root]}
    queue = deque([root])
    while queue:
        current = queue.popleft()
        for dep in nodes[current]["deps"]:
            if not any(kind["kind"] is None or include_build and kind["kind"] == "build"
                       for kind in dep["dep_kinds"]):
                continue
            target = dep["pkg"]
            if target not in paths:
                paths[target] = paths[current] + [target]
                queue.append(target)
    return paths


def check_profile(metadata, profile):
    paths = normal_closure(metadata)
    packages = {package["id"]: package for package in metadata["packages"]}
    names = {packages[key]["name"] for key in paths}
    missing = set(profile["required"]) - names
    compiled_names = {packages[key]["name"] for key in normal_closure(metadata, include_build=True)}
    unexpected = set(profile["excluded"]) & compiled_names
    if missing or unexpected:
        raise ValueError(f'{profile["name"]}: missing={sorted(missing)}, excluded present={sorted(unexpected)}')
    return paths


def capture(args, cwd=None, merge_stderr=False):
    return subprocess.check_output(args, cwd=cwd, text=True,
                                   stderr=subprocess.STDOUT if merge_stderr else None).strip()


def native_libraries(binary):
    if sys.platform == "darwin":
        lines = capture(["otool", "-L", str(binary)]).splitlines()[1:]
        paths = [line.strip().split(" (", 1)[0] for line in lines]
        external = [path for path in paths if not path.startswith(("/usr/lib/", "/System/Library/"))]
    elif sys.platform.startswith("linux"):
        lines = capture(["ldd", str(binary)]).splitlines()
        if any("not found" in line for line in lines):
            raise ValueError("unresolved dynamic dependency")
        paths = sorted(set(re.findall(r"(?:=>\s*)?(/[^\s]+)", "\n".join(lines))))
        external = [path for path in paths if not path.startswith(("/lib/", "/lib64/", "/usr/lib/", "/usr/lib64/"))]
    else:
        raise ValueError("native footprint inspection currently supports macOS and Linux only")
    if external:
        raise ValueError(f"package must include non-system dynamic libraries: {external}")
    return paths


def archive_binary(binary, output):
    payload = binary.read_bytes()
    with output.open("wb") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb", filename="", mtime=0) as zipped:
            with tarfile.open(fileobj=zipped, mode="w") as archive:
                entry = tarfile.TarInfo("hawdb-composition-consumer")
                entry.size = len(payload)
                entry.mode = 0o755
                entry.mtime = 0
                archive.addfile(entry, io.BytesIO(payload))
    return len(payload), hashlib.sha256(payload).hexdigest(), output.stat().st_size


def lock_identities(path):
    identities = set()
    for block in path.read_text().split("[[package]]")[1:]:
        fields = dict(re.findall(r'^(name|version|source) = "([^"]+)"$', block, re.M))
        identities.add((fields["name"], fields["version"], fields.get("source")))
    return identities


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--inventory-only", action="store_true")
    parser.add_argument("--build-cache", type=Path, help="optional reusable Cargo target directory")
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    matrix = json.loads((FIXTURE / "profiles.json").read_text())
    overrides = [key for key, value in os.environ.items() if value and (
        key in {"RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_BUILD_RUSTFLAGS"}
        or key.startswith("CARGO_PROFILE_RELEASE_")
        or key.startswith("CARGO_TARGET_") and key.endswith("_RUSTFLAGS"))]
    if overrides:
        raise ValueError(f"unset build overrides before measuring: {sorted(overrides)}")
    rustc = capture(["rustc", "-vV"], ROOT)
    pinned = re.search(r'channel = "([^"]+)"', (ROOT / "rust-toolchain.toml").read_text()).group(1)
    if not rustc.startswith("rustc " + pinned + " "):
        raise ValueError("recorded compiler does not match the repository release toolchain pin")
    target = next(line.removeprefix("host: ") for line in rustc.splitlines() if line.startswith("host: "))
    baseline_lock = lock_identities(ROOT / "Cargo.lock")
    report = {
        "schema": 1,
        "revision": capture(["git", "rev-parse", "HEAD"], ROOT),
        "tracked_changes": capture(["git", "diff", "--stat"], ROOT),
        "target": target, "platform": platform.platform(), "rustc": rustc,
        "cc": capture(["cc", "--version"]),
        "linker": capture(["xcrun", "ld", "-v"] if sys.platform == "darwin" else ["ld", "--version"], merge_stderr=True),
        "release": {"opt_level": 3, "lto": "thin", "codegen_units": 1, "strip": "none", "debug": False},
        "matrix_sha256": hashlib.sha256((FIXTURE / "profiles.json").read_bytes()).hexdigest(),
        "host_source_sha256": hashlib.sha256((FIXTURE / "src/main.rs").read_bytes()).hexdigest(),
        "workspace_lock_sha256": hashlib.sha256((ROOT / "Cargo.lock").read_bytes()).hexdigest(),
        "profiles": [],
    }
    with tempfile.TemporaryDirectory(prefix="hawdb-external-consumer-") as temporary:
        consumer = Path(temporary)
        shutil.copytree(FIXTURE / "src", consumer / "src")
        shutil.copyfile(ROOT / "rust-toolchain.toml", consumer / "rust-toolchain.toml")
        if capture(["rustc", "-vV"], consumer) != rustc:
            raise ValueError("external consumer toolchain differs from the recorded compiler")
        manifest = (FIXTURE / "Cargo.toml").read_text().replace('path = "../.."', "path = " + json.dumps(str(ROOT)))
        (consumer / "Cargo.toml").write_text(manifest)
        shutil.copyfile(ROOT / "Cargo.lock", consumer / "Cargo.lock")
        for profile in matrix["profiles"]:
            flags = ["--no-default-features"]
            if profile["features"]:
                flags += ["--features", ",".join(profile["features"])]
            # First resolve adds only this standalone host; seeded dependency versions stay pinned.
            metadata = json.loads(capture(["cargo", "metadata", "--offline", "--format-version=1", "--filter-platform", target, *flags], consumer))
            additions = lock_identities(consumer / "Cargo.lock") - baseline_lock
            if additions != {("hawdb-composition-consumer", "0.0.0", None)}:
                raise ValueError(f"consumer dependency resolution drift: {additions}")
            paths = check_profile(metadata, profile)
            compiled_paths = normal_closure(metadata, include_build=True)
            packages = {package["id"]: package for package in metadata["packages"]}
            nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
            label = lambda key: packages[key]["name"] + "@" + packages[key]["version"]
            entry = {
                "name": profile["name"], "consumer_features": profile["features"],
                "normal_dependencies": sorted(label(key) for key in paths),
                "build_only_dependencies": sorted(label(key) for key in compiled_paths if key not in paths),
                "hawdb_features": {packages[key]["name"]: nodes[key]["features"] for key in paths if packages[key]["name"].startswith("hawdb")},
                "witness_paths": {name: [label(key) for key in next((path for key, path in compiled_paths.items() if packages[key]["name"] == name), [])] for name in profile["required"] + profile["excluded"]},
                "consumer_lock_sha256": hashlib.sha256((consumer / "Cargo.lock").read_bytes()).hexdigest(),
            }
            if not args.inventory_only:
                cache = args.build_cache.resolve() if args.build_cache else output / "target"
                command = ["cargo", "build", "--offline", "--locked", "--release", "--target", target, "--target-dir", str(cache), *flags]
                with (output / (profile["name"] + ".build.log")).open("w") as log:
                    subprocess.run(command, cwd=consumer, stdout=log, stderr=subprocess.STDOUT, check=True)
                built = cache / target / "release/hawdb-composition-consumer"
                binary = output / (profile["name"] + "-host")
                shutil.copyfile(built, binary)
                binary.chmod(0o755)
                entry["workload_output"] = capture([str(binary), "measured-host-input"])
                entry["system_dynamic_libraries"] = native_libraries(binary)
                entry["separately_shipped_libraries"] = []
                size, digest, packed = archive_binary(binary, output / (profile["name"] + ".tar.gz"))
                entry.update(executable_bytes=size, executable_sha256=digest, packaged_bytes=packed)
            report["profiles"].append(entry)
            (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
            print(profile["name"], "verified", flush=True)
    print(output / "report.json")


if __name__ == "__main__":
    main()
