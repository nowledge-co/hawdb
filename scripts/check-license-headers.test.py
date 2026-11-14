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

"""Exercise both HawkEye header forms without changing fixture bytes."""

from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/check-license-headers.py"
FULL = (ROOT / "crates/storage/src/append_table.rs").read_text().split("\n\n", 1)[0]
SHORT = "// Copyright 2026 Nowledge\n// SPDX-License-Identifier: Apache-2.0"


class LicenseHookTests(unittest.TestCase):
    def run_fixture(self, contents, expected, parent=ROOT):
        with tempfile.TemporaryDirectory(prefix=".license-test-", dir=parent) as directory:
            paths = []
            for name, text in contents.items():
                path = Path(directory) / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(text)
                paths.append(path)
            before = {path: path.read_bytes() for path in paths}
            result = subprocess.run(
                [sys.executable, str(SCRIPT), *map(str, paths)],
                cwd=ROOT, capture_output=True, text=True,
            )
            self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
            self.assertEqual({path: path.read_bytes() for path in paths}, before)

    def test_both_forms_and_supported_script_styles(self):
        self.run_fixture({
            "full.rs": FULL + "\n\nfn main() {}\n",
            "short form.rs": SHORT + "\n\nfn main() {}\n",
            "script.py": "#!/usr/bin/env python3\n" + SHORT.replace("//", "#") + "\n\npass\n",
            "script.sh": "#!/usr/bin/env bash\n" + SHORT.replace("//", "#") + "\n\ntrue\n",
        }, 0)

    def test_missing_foreign_incomplete_and_mixed_headers_fail(self):
        for invalid in [
            "fn main() {}\n",
            SHORT.replace("Nowledge", "Someone Else") + "\n\nfn main() {}\n",
            SHORT.replace("Apache-2.0", "MIT") + "\n\nfn main() {}\n",
            "// Copyright 2026 Nowledge\n\nfn main() {}\n",
        ]:
            with self.subTest(header=invalid):
                self.run_fixture({"valid.rs": SHORT + "\n\n", "invalid.rs": invalid}, 1)

    def test_markdown_unsupported_and_toml_excluded(self):
        self.run_fixture({"notes.md": "# Notes\n", "config.toml": "value = 1\n"}, 0)

    def test_moonbit_sources_interfaces_and_manifests(self):
        for name, body in {
            "source.mbt": "///|\nfn main { println(1) }\n",
            "package.mbti": '// Generated using `moon info`, DO NOT EDIT\npackage "example/test"\n',
            "moon.mod": 'name = "example/test"\n',
            "moon.pkg": 'import { "moonbitlang/core/json" }\n',
        }.items():
            for header in (FULL, SHORT):
                with self.subTest(path=name, header=header):
                    self.run_fixture({name: header + "\n\n" + body}, 0)
            with self.subTest(path=name, header="missing"):
                self.run_fixture({name: body}, 1)

    def test_go_line_headers_and_build_constraints(self):
        for header in (FULL, SHORT):
            with self.subTest(header=header):
                self.run_fixture({
                    "source.go": header + "\n\n//go:build example\n\npackage example\n",
                }, 0)
        self.run_fixture({"source.go": "//go:build example\n\npackage example\n"}, 1)

    def test_generated_output_and_dependencies_are_excluded(self):
        self.run_fixture({
            "target/generated.rs": "fn generated() {}\n",
            "_build/generated.mbt": "fn main { println(1) }\n",
            ".mooncakes/dependency/source.mbt": "fn main { println(1) }\n",
        }, 0)

    def test_historical_benchmark_evidence_is_preserved(self):
        self.run_fixture({
            "test.xml": '<testsuites tests="1" />\n',
            "probe.rs": "fn main() {}\n",
            "measure.py": 'print("historical evidence")\n',
        }, 0, parent=ROOT / "docs/benchmarks")


if __name__ == "__main__":
    unittest.main()
