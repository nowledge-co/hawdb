#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Portable refusal controls; simulator behavior is owned by Swift Apple CI."""

import importlib.util
from pathlib import Path
import plistlib
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location(
    "build_apple", Path(__file__).with_name("build-apple.py")
)
BUILD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BUILD)


class ApplePackagingTests(unittest.TestCase):
    def test_native_dependencies_preserve_libraries_frameworks_and_order(self):
        settings = BUILD.link_settings(
            "note: native-static-libs: -lSystem -liconv -framework Security -lSystem\n"
        )
        self.assertEqual(settings, [
            ("linkedLibrary", "System"), ("linkedLibrary", "iconv"),
            ("linkedFramework", "Security"), ("linkedLibrary", "System"),
        ])

    def test_missing_or_unrepresented_native_dependencies_are_refused(self):
        for output in ["", "native-static-libs: -framework", "native-static-libs: -Wl,-something"]:
            with self.subTest(output=output), self.assertRaises(ValueError):
                BUILD.link_settings(output)

    def fixture(self, root):
        libraries = [
            {"LibraryIdentifier": "ios-arm64", "SupportedPlatform": "ios",
             "SupportedArchitectures": ["arm64"]},
            {"LibraryIdentifier": "ios-arm64_x86_64-simulator", "SupportedPlatform": "ios",
             "SupportedPlatformVariant": "simulator",
             "SupportedArchitectures": ["arm64", "x86_64"]},
        ]
        for library in libraries:
            library.update(LibraryPath="libhawdb_ffi.a", HeadersPath="Headers")
            directory = root / library["LibraryIdentifier"]
            (directory / "Headers").mkdir(parents=True)
            (directory / "libhawdb_ffi.a").write_bytes(b"fixture")
            (directory / "Headers/hawdb_ffi.h").write_bytes(
                (BUILD.ROOT / "bindings/ffi/include/hawdb_ffi.h").read_bytes())
        return libraries

    def write_plist(self, root, libraries):
        with (root / "Info.plist").open("wb") as output:
            plistlib.dump({"AvailableLibraries": libraries}, output)

    def test_device_and_both_simulator_architectures_are_required(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            libraries = self.fixture(root)
            self.write_plist(root, libraries)
            BUILD.validate_xcframework(root)
            for refused in [
                libraries[:1],
                [libraries[0], dict(libraries[1], SupportedArchitectures=["arm64"])],
                libraries + [libraries[0]],
            ]:
                self.write_plist(root, refused)
                with self.assertRaises(ValueError):
                    BUILD.validate_xcframework(root)

    def test_missing_archive_and_mismatched_header_are_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            libraries = self.fixture(root)
            self.write_plist(root, libraries)
            archive = root / "ios-arm64/libhawdb_ffi.a"
            archive.write_bytes(b"")
            with self.assertRaises(ValueError):
                BUILD.validate_xcframework(root)
            archive.write_bytes(b"fixture")
            (root / "ios-arm64/Headers/hawdb_ffi.h").write_text("stale header")
            with self.assertRaises(ValueError):
                BUILD.validate_xcframework(root)


if __name__ == "__main__":
    unittest.main()
