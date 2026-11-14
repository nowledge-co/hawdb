#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Build an iOS static XCFramework and a self-contained Swift package."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import plistlib
import re
import shlex
import shutil
import subprocess
import tomllib

ROOT = Path(__file__).resolve().parents[2]
TARGETS = {
    "aarch64-apple-ios": "iphoneos",
    "aarch64-apple-ios-sim": "iphonesimulator",
    "x86_64-apple-ios": "iphonesimulator",
}
IOS_MINIMUM = "15.0"


def run(*args, **kwargs):
    return subprocess.run(args, cwd=ROOT, check=True, text=True, **kwargs)


def link_settings(output):
    """Refuse unrepresented dependencies instead of guessing static-link flags."""
    matches = re.findall(r"native-static-libs: (.+)", output)
    if not matches:
        raise ValueError("rustc did not report native-static-libs")
    tokens = iter(shlex.split(matches[-1]))
    settings = []
    for token in tokens:
        if token == "-framework":
            name = next(tokens, None)
            if name is None:
                raise ValueError("missing framework name")
            settings.append(("linkedFramework", name))
        elif token.startswith("-l") and len(token) > 2:
            settings.append(("linkedLibrary", token[2:]))
        else:
            raise ValueError(f"unrepresented native link flag: {token}")
    return settings


def package_manifest(settings):
    links = ",\n".join(f"                .{kind}({json.dumps(name)})" for kind, name in settings)
    return f'''// swift-tools-version: 6.0
// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

import PackageDescription

let package = Package(
    name: "HawDB",
    platforms: [.iOS(.v15)],
    products: [.library(name: "HawDB", targets: ["HawDB"])],
    targets: [
        .binaryTarget(name: "CHawDB", path: "CHawDB.xcframework"),
        .target(
            name: "HawDB", dependencies: ["CHawDB"],
            linkerSettings: [
{links}
            ]
        ),
        .testTarget(name: "HawDBTests", dependencies: ["HawDB"]),
    ]
)
'''


def validate_xcframework(path):
    with (path / "Info.plist").open("rb") as source:
        libraries = plistlib.load(source)["AvailableLibraries"]
    expected = {("ios", ""): {"arm64"}, ("ios", "simulator"): {"arm64", "x86_64"}}
    actual = {}
    for library in libraries:
        key = (library["SupportedPlatform"], library.get("SupportedPlatformVariant", ""))
        if key in actual:
            raise ValueError(f"duplicate XCFramework platform: {key}")
        actual[key] = set(library["SupportedArchitectures"])
        directory = path / library["LibraryIdentifier"]
        archive = directory / library["LibraryPath"]
        header = directory / library["HeadersPath"] / "hawdb_ffi.h"
        if not archive.is_file() or archive.stat().st_size == 0:
            raise ValueError(f"missing native archive: {archive}")
        if header.read_bytes() != (ROOT / "bindings/ffi/include/hawdb_ffi.h").read_bytes():
            raise ValueError(f"XCFramework header differs from the canonical C ABI: {header}")
    if actual != expected:
        raise ValueError(f"unexpected XCFramework slices: {actual}")


def build(output):
    # Resolve both SDKs before creating output or compiling. CLT alone is insufficient.
    run("xcrun", "--find", "xcodebuild", capture_output=True)
    sdks = {
        sdk: run("xcrun", "--sdk", sdk, "--show-sdk-path", capture_output=True).stdout.strip()
        for sdk in set(TARGETS.values())
    }
    output.mkdir(parents=True, exist_ok=False)
    work = output / "build"
    work.mkdir()
    cargo_target = ROOT / "target/swift-apple/cargo"
    libraries = {}
    settings = None
    for target, sdk in TARGETS.items():
        environment = dict(os.environ, SDKROOT=sdks[sdk], IPHONEOS_DEPLOYMENT_TARGET=IOS_MINIMUM)
        command = (
            "cargo", "rustc", "--locked", "--color", "never", "--release", "-j", "2",
            "--manifest-path", "bindings/ffi/Cargo.toml", "--lib", "--crate-type", "staticlib",
            "--target", target, "--target-dir", str(cargo_target), "--", "--print=native-static-libs",
        )
        print(f"Building {target}", flush=True)
        process = subprocess.run(
            command, cwd=ROOT, env=environment, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        )
        print(process.stdout, flush=True)
        (work / f"{target}.log").write_text(process.stdout)
        process.check_returncode()
        native_settings = link_settings(process.stdout)
        if settings is not None and settings != native_settings:
            raise ValueError(f"native dependencies differ for {target}: {native_settings}")
        settings = native_settings
        libraries[target] = cargo_target / target / "release/libhawdb_ffi.a"

    headers = work / "headers"
    headers.mkdir()
    shutil.copy2(ROOT / "bindings/ffi/include/hawdb_ffi.h", headers)
    (headers / "module.modulemap").write_text(
        '// Copyright 2026 Nowledge\n// SPDX-License-Identifier: Apache-2.0\n'
        'module CHawDB {\n    header "hawdb_ffi.h"\n    export *\n}\n'
    )
    simulator = work / "libhawdb_ffi.a"
    run("xcrun", "lipo", "-create", str(libraries["aarch64-apple-ios-sim"]),
        str(libraries["x86_64-apple-ios"]), "-output", str(simulator))
    package = output / "HawDB"
    package.mkdir()
    framework = package / "CHawDB.xcframework"
    run("xcodebuild", "-create-xcframework",
        "-library", str(libraries["aarch64-apple-ios"]), "-headers", str(headers),
        "-library", str(simulator), "-headers", str(headers), "-output", str(framework))
    validate_xcframework(framework)
    (package / "Package.swift").write_text(package_manifest(settings))
    shutil.copytree(ROOT / "bindings/swift/Sources/HawDB", package / "Sources/HawDB")
    shutil.copytree(ROOT / "bindings/swift/Tests/HawDBTests", package / "Tests/HawDBTests")
    shutil.copy2(ROOT / "LICENSE", package)
    shutil.copy2(ROOT / "bindings/swift/README.md", package)
    manifest = {
        "version": tomllib.loads((ROOT / "Cargo.toml").read_text())["package"]["version"],
        "revision": run("git", "rev-parse", "HEAD", capture_output=True).stdout.strip(),
        "dirty": bool(run("git", "status", "--porcelain", capture_output=True).stdout),
        "minimum_ios": IOS_MINIMUM,
        "rust_toolchain": tomllib.loads(
            (ROOT / "rust-toolchain.toml").read_text()
        )["toolchain"]["channel"],
        "xcode": run("xcodebuild", "-version", capture_output=True).stdout.strip(),
        "targets": list(TARGETS),
        "profile": "release",
        "features": "default",
        "native_link_settings": settings,
        "files": {
            path.relative_to(package).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(package.rglob("*")) if path.is_file()
        },
    }
    (package / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"iOS Swift package: {package}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "target/swift-apple/package")
    build(parser.parse_args().output.resolve())
