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

"""Negative controls for the persistent format lock."""

import importlib.util
from pathlib import Path
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("check-persistent-format.py")
SPEC = importlib.util.spec_from_file_location("persistent_format", SCRIPT)
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)


class PersistentFormatLockTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        for directory in CHECK.SCANNED:
            if directory.suffix == ".rs":
                self.write(directory, "pub const SAMPLE_FORMAT: &str = \"sample\";\n")
            else:
                (self.root / directory).mkdir(parents=True)
        self.write(
            CHECK.VERSION_SOURCE,
            "pub const PERSISTENT_FORMAT_VERSION: u32 = 1;\n",
        )
        self.write(
            "crates/storage/src/codec.rs",
            'const PAGE_MAGIC: &[u8; 8] = b"SKPAGE01";\n'
            "pub(crate) const PAGE_VERSION: u16 = 2;\n"
            "const ANALYZER_FORMAT_VERSION: &[u8] =\n    b\"analyzer-v1\";\n"
            "const DEFAULT_MAX_VERSION_BYTES: usize = 4;\n"
            "const UNRELATED: u16 = 7;\n",
        )
        self.write(
            "crates/storage/src/codec/tests.rs",
            'const TEST_MAGIC: &[u8; 8] = b"TESTONLY";\n',
        )
        (self.root / "docs").mkdir()
        self.assertEqual(CHECK.main(["--root", str(self.root), "--update"]), 0)

    def write(self, relative, text):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    def test_lock_records_codec_constants_only(self):
        lock = (self.root / CHECK.LOCK).read_text(encoding="utf-8")
        self.assertIn("persistent_format_version 1\n", lock)
        self.assertIn('PAGE_MAGIC: &[u8; 8] = b"SKPAGE01";', lock)
        self.assertIn("PAGE_VERSION: u16 = 2;", lock)
        self.assertIn('ANALYZER_FORMAT_VERSION: &[u8] = b"analyzer-v1";', lock)
        self.assertNotIn("DEFAULT_MAX_VERSION_BYTES", lock)
        self.assertNotIn("UNRELATED", lock)
        self.assertNotIn("TEST_MAGIC", lock)
        self.assertEqual(CHECK.check(self.root), [])

    def test_codec_change_requires_a_reviewed_lock_update(self):
        self.write(
            "crates/storage/src/codec.rs",
            'const PAGE_MAGIC: &[u8; 8] = b"SKPAGE02";\n',
        )
        problems = CHECK.check(self.root)
        self.assertTrue(problems)
        self.assertTrue(any("SKPAGE02" in problem for problem in problems))
        self.assertTrue(any("bump PERSISTENT_FORMAT_VERSION" in problem for problem in problems))

    def test_version_bump_alone_requires_a_lock_update(self):
        self.write(
            CHECK.VERSION_SOURCE,
            "pub const PERSISTENT_FORMAT_VERSION: u32 = 2;\n",
        )
        self.assertTrue(CHECK.check(self.root))
        CHECK.main(["--root", str(self.root), "--update"])
        self.assertEqual(CHECK.check(self.root), [])
        self.assertEqual(CHECK.format_version(self.root), 2)

    def test_checkout_without_a_version_reports_none(self):
        (self.root / CHECK.VERSION_SOURCE).unlink()
        self.assertEqual(CHECK.main(["--root", str(self.root), "--version"]), 0)


if __name__ == "__main__":
    unittest.main()
