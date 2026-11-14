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

"""Keep the engine-wide persistent format version honest.

`docs/persistent-format.lock` records `PERSISTENT_FORMAT_VERSION` and every
magic/version/format constant declared by the crates that write persistent
files. Changing any of them fails this check until the lock is regenerated,
so the change's diff shows reviewers whether the format version was bumped.

    python3 scripts/check-persistent-format.py            # check
    python3 scripts/check-persistent-format.py --update   # rewrite the lock
    python3 scripts/check-persistent-format.py --version  # print the version
    python3 scripts/check-persistent-format.py --version --root <checkout>
"""

import argparse
import re
import sys
from pathlib import Path

LOCK = Path("docs/persistent-format.lock")
VERSION_SOURCE = Path("crates/storage/src/persistent_format.rs")
# Crates that own persistent codecs, manifests or segment layouts.
SCANNED = [
    Path("crates/storage/src"),
    Path("crates/search/src"),
    Path("crates/vector-projection/src"),
    Path("crates/relational/src"),
    # The format names hosts compare `StorageFormatError::format()` against.
    Path("crates/core/src/error/storage_format.rs"),
]
DECLARATION = re.compile(
    r"^\s*(?:pub(?:\([^)]*\))?\s+)?const\s+([A-Z0-9_]*(?:MAGIC|VERSION|FORMAT)[A-Z0-9_]*)\s*:"
)
# Tunables and qualification pins whose names match but that describe no
# persisted byte layout.
EXCLUDED_PREFIXES = ("DEFAULT_", "MAX_", "QUALIFIED_")
VERSION = re.compile(r"pub const PERSISTENT_FORMAT_VERSION: u32 = (\d+);")
HEADER = (
    "# Persistent format lock: PERSISTENT_FORMAT_VERSION and the persistent codec\n"
    "# constants it covers. Regenerate with\n"
    "#     python3 scripts/check-persistent-format.py --update\n"
    "# after deciding whether the change makes existing databases unreadable; if\n"
    "# it does, bump PERSISTENT_FORMAT_VERSION in " + str(VERSION_SOURCE) + " first.\n"
)


def is_test_source(path):
    name = path.name
    return (
        "tests" in path.parts
        or name == "tests.rs"
        or name.endswith("_tests.rs")
        or name.endswith("_test.rs")
        or name == "test_support.rs"
    )


def statement(lines, start):
    """The declaration starting at `start`, joined up to its semicolon."""
    parts = []
    for line in lines[start:]:
        parts.append(line.strip())
        if line.rstrip().endswith(";"):
            break
    return " ".join(parts)


def scan(root):
    entries = []
    for directory in SCANNED:
        scanned = root / directory
        paths = [scanned] if scanned.suffix == ".rs" else sorted(scanned.rglob("*.rs"))
        for path in paths:
            relative = path.relative_to(root)
            if is_test_source(relative):
                continue
            lines = path.read_text(encoding="utf-8").splitlines()
            for index, line in enumerate(lines):
                match = DECLARATION.match(line)
                if match is None or match.group(1).startswith(EXCLUDED_PREFIXES):
                    continue
                entries.append(f"{relative.as_posix()} {statement(lines, index)}")
    return entries


def format_version(root):
    text = (root / VERSION_SOURCE).read_text(encoding="utf-8")
    match = VERSION.search(text)
    if match is None:
        raise SystemExit(f"{VERSION_SOURCE} does not declare PERSISTENT_FORMAT_VERSION")
    return int(match.group(1))


def render(version, entries):
    body = "".join(f"{entry}\n" for entry in entries)
    return f"{HEADER}persistent_format_version {version}\n{body}"


def check(root):
    """Return a list of human-readable problems; empty means consistent."""
    expected = render(format_version(root), scan(root))
    lock = root / LOCK
    if not lock.exists():
        return [f"{LOCK} is missing; run with --update"]
    actual = lock.read_text(encoding="utf-8")
    if actual == expected:
        return []
    old = set(actual.splitlines())
    new = set(expected.splitlines())
    problems = [f"{LOCK} is out of date:"]
    problems += [f"  removed: {line}" for line in sorted(old - new)]
    problems += [f"  added:   {line}" for line in sorted(new - old)]
    problems.append(
        "If this change makes databases written by the previous release "
        "unreadable, bump PERSISTENT_FORMAT_VERSION in "
        f"{VERSION_SOURCE}; then run python3 scripts/check-persistent-format.py --update."
    )
    return problems


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--update", action="store_true")
    parser.add_argument("--version", action="store_true")
    args = parser.parse_args(argv)
    root = args.root
    if args.version:
        if not (root / VERSION_SOURCE).exists():
            print("none")
            return 0
        print(format_version(root))
        return 0
    if args.update:
        (root / LOCK).write_text(render(format_version(root), scan(root)), encoding="utf-8")
        return 0
    problems = check(root)
    for problem in problems:
        print(problem, file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
