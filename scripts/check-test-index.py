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

"""Check per-family test index entries under docs/test-index/.

Each entry starts with a `# ` title, followed by a blank line and the
`Owner:`, `Lane:` and `Replay:` lines (non-empty). File names are lowercase
`[a-z0-9-]+.md`; titles are unique.
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INDEX_DIR = ROOT / "docs" / "test-index"
NAME = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*\.md$")
FIELDS = ("Owner:", "Lane:", "Replay:")


def check(path: Path) -> list[str]:
    errors = []
    if not NAME.match(path.name):
        errors.append(f"{path.name}: file name must match [a-z0-9-]+.md")
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or not lines[0].startswith("# ") or len(lines[0]) <= 2:
        errors.append(f"{path.name}: first line must be a '# ' title")
        return errors
    if len(lines) < 5 or lines[1] != "":
        errors.append(f"{path.name}: the title must be followed by a blank line and the fields")
        return errors
    for offset, field in enumerate(FIELDS, start=2):
        line = lines[offset] if offset < len(lines) else ""
        if not line.startswith(field) or not line[len(field):].strip():
            errors.append(f"{path.name}: line {offset + 1} must be a non-empty '{field}' field")
    return errors


def main() -> int:
    if not INDEX_DIR.is_dir():
        print(f"missing {INDEX_DIR.relative_to(ROOT)}", file=sys.stderr)
        return 1
    errors = []
    titles = {}
    for path in sorted(INDEX_DIR.glob("*.md")):
        if path.name == "README.md":
            continue
        errors.extend(check(path))
        first = path.read_text(encoding="utf-8").splitlines()[:1]
        if first:
            other = titles.setdefault(first[0], path.name)
            if other != path.name:
                errors.append(f"{path.name}: title duplicates {other}")
    for error in errors:
        print(f"docs/test-index/{error}", file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
