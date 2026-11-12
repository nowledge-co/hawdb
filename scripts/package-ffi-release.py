#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Package a built HawDB C ABI library as a versioned release archive.

The archive is ``hawdb-ffi-<version>-<target>.tar.gz`` (``.zip`` on Windows)
with a single top-level directory holding the shared library (and, on macOS
and Linux, the static archive; on Windows, the DLL import library), the C
header, LICENSE and ``manifest.json``. The manifest records the engine
revision, feature profile, ABI expectations and the minimum OS or C library
measured from the packaged binary itself, not assumed from the build image.

macOS dylibs get the install name ``@rpath/libhawdb_ffi.dylib`` and an ad-hoc
signature (rewriting the load command invalidates the linker's signature, and
arm64 macOS refuses unsigned code). Real signing belongs to the embedding app.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
INSTALL_NAME = "@rpath/libhawdb_ffi.dylib"

# Rust target triple -> (platform family, shared library, static library, import library).
TARGETS = {
    "aarch64-apple-darwin": ("macos", "libhawdb_ffi.dylib", "libhawdb_ffi.a", None),
    "x86_64-apple-darwin": ("macos", "libhawdb_ffi.dylib", "libhawdb_ffi.a", None),
    "x86_64-unknown-linux-gnu": ("linux", "libhawdb_ffi.so", "libhawdb_ffi.a", None),
    "aarch64-unknown-linux-gnu": ("linux", "libhawdb_ffi.so", "libhawdb_ffi.a", None),
    # The Windows static archive is not link-qualified; ship the DLL and import library.
    "x86_64-pc-windows-msvc": ("windows", "hawdb_ffi.dll", None, "hawdb_ffi.dll.lib"),
}

ABI_COMPATIBILITY = (
    "Pre-1.0: the ordinary C ABI is versioned with the HawDB release and may change "
    "between minor versions. Compile against the header shipped in the same archive "
    "and load only that library; hawdb_version() reports the engine version. The "
    "hawdb_retained_* exports are experimental."
)


def archive_name(version: str, target: str) -> str:
    family = TARGETS[target][0]
    return f"hawdb-ffi-{version}-{target}." + ("zip" if family == "windows" else "tar.gz")


def max_glibc(objdump_dynamic_symbols: str) -> str | None:
    """Highest GLIBC_x.y[.z] symbol version referenced in ``objdump -T`` output."""
    versions = {
        tuple(int(part) for part in match.split("."))
        for match in re.findall(r"\bGLIBC_(\d+(?:\.\d+)+)\b", objdump_dynamic_symbols)
    }
    if not versions:
        return None
    return ".".join(str(part) for part in max(versions))


def macos_minimum(otool_load_commands: str) -> str | None:
    """Deployment target from ``otool -l`` (LC_BUILD_VERSION or LC_VERSION_MIN_MACOSX)."""
    build = re.search(r"cmd LC_BUILD_VERSION.*?\n\s*minos (\S+)", otool_load_commands, re.S)
    if build:
        return build.group(1)
    legacy = re.search(r"cmd LC_VERSION_MIN_MACOSX.*?\n\s*version (\S+)", otool_load_commands, re.S)
    return legacy.group(1) if legacy else None


def macos_link_flags(otool_dependencies: str, library_name: str) -> list[str]:
    """Static-link flags for the system dylibs and frameworks in ``otool -L`` output."""
    flags = []
    for line in otool_dependencies.splitlines()[1:]:
        path = line.strip().split(" (", 1)[0]
        # The compiler driver always links libSystem; repeating it only warns.
        if not path or path.endswith(library_name) or "/libSystem." in path:
            continue
        framework = re.search(r"/([^/]+)\.framework/", path)
        if framework:
            flags += ["-framework", framework.group(1)]
            continue
        dylib = re.fullmatch(r".*/lib([^/.]+)(?:\.[^/]*)?\.dylib", path)
        if dylib is None:
            raise SystemExit(f"unexpected macOS dependency {path!r}")
        flags.append(f"-l{dylib.group(1)}")
    return flags


def linux_link_flags(readelf_dynamic: str) -> list[str]:
    """Static-link flags for the NEEDED entries in ``readelf -d`` output."""
    flags = []
    for needed in re.findall(r"\(NEEDED\)\s+Shared library: \[([^\]]+)\]", readelf_dynamic):
        if needed.startswith("ld-linux"):
            continue
        library = re.fullmatch(r"lib(.+?)\.so(?:\.\d+)*", needed)
        if library is None:
            raise SystemExit(f"unexpected Linux dependency {needed!r}")
        flags.append(f"-l{library.group(1)}")
    return flags


def default_features() -> list[str]:
    manifest = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))
    return sorted(manifest["features"]["default"])


def run(*command: str) -> str:
    return subprocess.run(command, check=True, capture_output=True, text=True).stdout


def prepare_macos(library: Path) -> dict:
    run("install_name_tool", "-id", INSTALL_NAME, str(library))
    run("codesign", "--force", "--sign", "-", str(library))
    install_name = run("otool", "-D", str(library)).splitlines()[-1].strip()
    if install_name != INSTALL_NAME:
        raise SystemExit(f"install name is {install_name!r}, expected {INSTALL_NAME!r}")
    minimum = macos_minimum(run("otool", "-l", str(library)))
    if minimum is None:
        raise SystemExit("could not read the macOS deployment target")
    return {
        "os": "macos",
        "min_os_version": minimum,
        "install_name": INSTALL_NAME,
        "code_signature": "ad-hoc; sign with your application's identity when embedding",
        "static_link_flags": macos_link_flags(run("otool", "-L", str(library)), library.name),
    }


def prepare_linux(library: Path) -> dict:
    glibc = max_glibc(run("objdump", "-T", str(library)))
    if glibc is None:
        raise SystemExit("could not read the required glibc version")
    return {
        "os": "linux",
        "libc": "glibc",
        "min_glibc": glibc,
        "static_link_flags": linux_link_flags(run("readelf", "-d", str(library))),
    }


def prepare_windows(_library: Path) -> dict:
    return {
        "os": "windows",
        "min_os_version": "10",
        "runtime": "Microsoft Visual C++ runtime (vcruntime140.dll) and the Universal CRT",
    }


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_archive(staging: Path, destination: Path, epoch: int) -> None:
    files = sorted(path for path in staging.rglob("*") if path.is_file())
    if destination.suffix == ".zip":
        with zipfile.ZipFile(destination, "w", zipfile.ZIP_DEFLATED) as archive:
            for path in files:
                info = zipfile.ZipInfo(path.relative_to(staging.parent).as_posix(), (1980, 1, 1, 0, 0, 0))
                info.external_attr = 0o644 << 16
                info.compress_type = zipfile.ZIP_DEFLATED
                archive.writestr(info, path.read_bytes())
        return
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=tarfile.PAX_FORMAT) as archive:
        for path in [staging, *sorted(staging.rglob("*"))]:
            info = archive.gettarinfo(str(path), path.relative_to(staging.parent).as_posix())
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            info.mtime = epoch
            info.mode = 0o755 if path.is_dir() else 0o644
            if path.is_file():
                with path.open("rb") as handle:
                    archive.addfile(info, handle)
            else:
                archive.addfile(info)
    with destination.open("wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=epoch, filename="") as gz:
        gz.write(buffer.getvalue())


def package(args: argparse.Namespace) -> Path:
    family, shared_name, static_name, import_name = TARGETS[args.target]
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    root_name = f"hawdb-ffi-{args.version}-{args.target}"
    with tempfile.TemporaryDirectory() as temporary:
        staging = Path(temporary) / root_name
        shared_dir = staging / ("bin" if family == "windows" else "lib")
        lib_dir = staging / "lib"
        for directory in (shared_dir, lib_dir, staging / "include"):
            directory.mkdir(parents=True, exist_ok=True)
        shared = shared_dir / shared_name
        shutil.copyfile(args.shared, shared)
        if static_name is not None:
            if args.static is None:
                raise SystemExit("--static is required for macOS and Linux targets")
            shutil.copyfile(args.static, lib_dir / static_name)
        if import_name is not None:
            if args.import_lib is None:
                raise SystemExit("--import-lib is required for Windows targets")
            shutil.copyfile(args.import_lib, lib_dir / import_name)
        shutil.copyfile(args.header, staging / "include" / "hawdb_ffi.h")
        shutil.copyfile(args.license, staging / "LICENSE")
        requirements = {"macos": prepare_macos, "linux": prepare_linux, "windows": prepare_windows}[family](shared)
        files = {
            path.relative_to(staging).as_posix(): {"sha256": sha256(path), "size": path.stat().st_size}
            for path in sorted(staging.rglob("*"))
            if path.is_file()
        }
        manifest = {
            "name": "hawdb-ffi",
            "version": args.version,
            "target": args.target,
            "engine_revision": args.revision,
            "feature_profile": {"name": "default", "features": default_features()},
            "build": {
                "system": "bazel",
                "targets": ["//bindings/ffi:hawdb_ffi", "//bindings/ffi:hawdb_ffi_static"],
                "configuration": "--config=prebuilt",
                "runner": args.runner,
            },
            "abi": {
                "header": "include/hawdb_ffi.h",
                "library_version": f"hawdb {args.version}",
                "compatibility": ABI_COMPATIBILITY,
                "retained_abi_version": 1,
            },
            "requirements": requirements,
            "files": files,
        }
        (staging / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        destination = out / archive_name(args.version, args.target)
        write_archive(staging, destination, args.source_date_epoch)
    return destination


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--version", required=True)
    parser.add_argument("--target", required=True, choices=sorted(TARGETS))
    parser.add_argument("--revision", required=True, help="engine source commit")
    parser.add_argument("--runner", required=True, help="build image, e.g. ubuntu-24.04")
    parser.add_argument("--shared", required=True, type=Path)
    parser.add_argument("--static", type=Path, help="static archive; ignored for Windows")
    parser.add_argument("--import-lib", type=Path)
    parser.add_argument("--header", type=Path, default=ROOT / "bindings/ffi/include/hawdb_ffi.h")
    parser.add_argument("--license", type=Path, default=ROOT / "LICENSE")
    parser.add_argument("--source-date-epoch", type=int, default=0)
    parser.add_argument("--out", required=True)
    return parser.parse_args(argv)


if __name__ == "__main__":
    print(package(parse_args(sys.argv[1:])))
