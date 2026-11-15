#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Exercise the C ABI release packager with stand-in libraries."""

import importlib.util
import json
import os
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parent.parent
if "TEST_SRCDIR" in os.environ:
    ROOT = Path(os.environ["TEST_SRCDIR"]) / os.environ["TEST_WORKSPACE"]
SPEC = importlib.util.spec_from_file_location("package_ffi_release", ROOT / "scripts/package-ffi-release.py")
packager = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(packager)

OBJDUMP = """
libhawdb_ffi.so:     file format elf64-x86-64

DYNAMIC SYMBOL TABLE:
0000000000000000      DF *UND*  0000000000000000 (GLIBC_2.2.5) free
0000000000000000      DF *UND*  0000000000000000 (GLIBC_2.34) pthread_create
0000000000000000      DF *UND*  0000000000000000 (GLIBC_2.18) __cxa_thread_atexit_impl
0000000000000000      DF *UND*  0000000000000000  GLIBC_2.3.4 __memcpy_chk
0000000000000000  w   DF *UND*  0000000000000000 (GLIBC_PRIVATE) __libc_hidden
"""

OTOOL = """
Load command 9
      cmd LC_BUILD_VERSION
  cmdsize 32
 platform 1
    minos 11.0
      sdk 15.5
"""


class ParsingTests(unittest.TestCase):
    def test_highest_glibc_version_is_numeric_not_lexical(self):
        self.assertEqual(packager.max_glibc(OBJDUMP), "2.34")
        self.assertEqual(packager.max_glibc("(GLIBC_2.9) a\n(GLIBC_2.17) b"), "2.17")
        self.assertIsNone(packager.max_glibc("(GLIBC_PRIVATE) only"))

    def test_macos_deployment_target(self):
        self.assertEqual(packager.macos_minimum(OTOOL), "11.0")
        legacy = "      cmd LC_VERSION_MIN_MACOSX\n  cmdsize 16\n  version 10.12\n      sdk 15.5\n"
        self.assertEqual(packager.macos_minimum(legacy), "10.12")
        self.assertIsNone(packager.macos_minimum("cmd LC_SEGMENT_64"))

    def test_static_link_flags_follow_the_shared_library_dependencies(self):
        otool = (
            "/tmp/libhawdb_ffi.dylib:\n"
            "\t@rpath/libhawdb_ffi.dylib (compatibility version 0.0.0, current version 0.0.0)\n"
            "\t/System/Library/Frameworks/IOKit.framework/Versions/A/IOKit (compatibility version 1.0.0)\n"
            "\t/usr/lib/libiconv.2.dylib (compatibility version 7.0.0, current version 7.0.0)\n"
            "\t/usr/lib/libSystem.B.dylib (compatibility version 1.0.0, current version 1356.0.0)\n"
            "\t/usr/lib/libc++.1.dylib (compatibility version 1.0.0, current version 2000.67.0)\n"
        )
        self.assertEqual(
            packager.macos_link_flags(otool, "libhawdb_ffi.dylib"),
            ["-framework", "IOKit", "-liconv", "-lc++"],
        )
        readelf = (
            " 0x0000000000000001 (NEEDED)             Shared library: [libgcc_s.so.1]\n"
            " 0x0000000000000001 (NEEDED)             Shared library: [libm.so.6]\n"
            " 0x0000000000000001 (NEEDED)             Shared library: [libc.so.6]\n"
            " 0x0000000000000001 (NEEDED)             Shared library: [ld-linux-x86-64.so.2]\n"
            " 0x000000000000000e (SONAME)             Library soname: [libhawdb_ffi.so]\n"
        )
        self.assertEqual(packager.linux_link_flags(readelf), ["-lgcc_s", "-lm", "-lc"])
        with self.assertRaises(SystemExit):
            packager.linux_link_flags(" (NEEDED) Shared library: [weird]")

    def test_archive_names(self):
        self.assertEqual(
            packager.archive_name("0.30.0", "aarch64-unknown-linux-gnu"),
            "hawdb-ffi-0.30.0-aarch64-unknown-linux-gnu.tar.gz",
        )
        self.assertEqual(
            packager.archive_name("0.30.0", "x86_64-pc-windows-msvc"),
            "hawdb-ffi-0.30.0-x86_64-pc-windows-msvc.zip",
        )

    def test_feature_profile_is_the_engine_default(self):
        features = packager.default_features()
        self.assertIn("vector-search", features)
        self.assertEqual(features, sorted(features))


class PackagingTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        for name, payload in [("shared", b"ELF"), ("static", b"!<arch>"), ("import", b"LIB")]:
            (self.root / name).write_bytes(payload)

    def args(self, target, out, **overrides):
        argv = [
            "--version", "0.30.0", "--target", target, "--revision", "a" * 40,
            "--runner", "test-image", "--shared", str(self.root / "shared"),
            "--static", str(self.root / "static"), "--source-date-epoch", "1700000000",
            "--out", str(self.root / out),
        ]
        if overrides.get("import_lib"):
            argv += ["--import-lib", str(self.root / "import")]
        return packager.parse_args(argv)

    def test_linux_archive_layout_manifest_and_reproducibility(self):
        with patch.object(packager, "run", return_value=OBJDUMP):
            first = packager.package(self.args("x86_64-unknown-linux-gnu", "one"))
            second = packager.package(self.args("x86_64-unknown-linux-gnu", "two"))
        self.assertEqual(first.read_bytes(), second.read_bytes())
        prefix = "hawdb-ffi-0.30.0-x86_64-unknown-linux-gnu/"
        with tarfile.open(first) as archive:
            names = sorted(member.name for member in archive.getmembers() if member.isfile())
            self.assertTrue(all(member.uid == 0 and member.mtime == 1700000000 for member in archive.getmembers()))
            manifest = json.load(archive.extractfile(prefix + "manifest.json"))
        self.assertEqual(
            names,
            sorted(prefix + name for name in [
                "LICENSE", "include/hawdb_ffi.h", "lib/libhawdb_ffi.a", "lib/libhawdb_ffi.so", "manifest.json",
            ]),
        )
        self.assertEqual(
            manifest["requirements"],
            {"os": "linux", "libc": "glibc", "min_glibc": "2.34", "static_link_flags": []},
        )
        self.assertEqual(manifest["engine_revision"], "a" * 40)
        self.assertEqual(manifest["abi"]["library_version"], "hawdb 0.30.0")
        self.assertEqual(manifest["feature_profile"]["features"], packager.default_features())
        self.assertEqual(set(manifest["files"]), {
            "LICENSE", "include/hawdb_ffi.h", "lib/libhawdb_ffi.a", "lib/libhawdb_ffi.so",
        })

    def test_windows_zip_needs_and_ships_the_import_library(self):
        with self.assertRaises(SystemExit):
            packager.package(self.args("x86_64-pc-windows-msvc", "missing"))
        destination = packager.package(self.args("x86_64-pc-windows-msvc", "zip", import_lib=True))
        with zipfile.ZipFile(destination) as archive:
            names = sorted(archive.namelist())
            manifest = json.loads(archive.read("hawdb-ffi-0.30.0-x86_64-pc-windows-msvc/manifest.json"))
        self.assertEqual(
            names,
            sorted("hawdb-ffi-0.30.0-x86_64-pc-windows-msvc/" + name for name in [
                "LICENSE", "bin/hawdb_ffi.dll", "include/hawdb_ffi.h", "lib/hawdb_ffi.dll.lib",
                "manifest.json",
            ]),
        )
        self.assertEqual(manifest["requirements"]["os"], "windows")

    def test_macos_rewrites_install_name_and_refuses_a_mismatch(self):
        calls = []

        def fake_run(*command):
            calls.append(command[0])
            if command[:2] == ("otool", "-D"):
                return f"{command[2]}:\n{packager.INSTALL_NAME}\n"
            return OTOOL if command[:2] == ("otool", "-l") else ""

        with patch.object(packager, "run", side_effect=fake_run):
            destination = packager.package(self.args("aarch64-apple-darwin", "mac"))
        self.assertEqual(calls, ["install_name_tool", "codesign", "otool", "otool", "otool"])
        with tarfile.open(destination) as archive:
            manifest = json.load(archive.extractfile("hawdb-ffi-0.30.0-aarch64-apple-darwin/manifest.json"))
        self.assertEqual(manifest["requirements"]["min_os_version"], "11.0")
        self.assertEqual(manifest["requirements"]["install_name"], "@rpath/libhawdb_ffi.dylib")

        with patch.object(packager, "run", side_effect=lambda *command: "/abs/libhawdb_ffi.dylib"):
            with self.assertRaises(SystemExit):
                packager.package(self.args("aarch64-apple-darwin", "bad"))


if __name__ == "__main__":
    unittest.main()
