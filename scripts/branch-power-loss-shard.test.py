#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Verify discovery coverage and Python-to-Bash argument transport on Windows."""

import importlib.util
import io
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).with_name("branch-power-loss-shard.py")
if not SCRIPT.exists():
    SCRIPT = (
        Path(os.environ["TEST_SRCDIR"])
        / os.environ["TEST_WORKSPACE"]
        / "scripts/branch-power-loss-shard.py"
    )
sys.path.insert(0, str(SCRIPT.parent))
from vector_recovery_cases import (
    expected_combinations,
    main as recovery_main,
    validate_log,
)
from automatic_checkpoint_cases import (
    expected_combinations as automatic_combinations,
    expected_summaries as automatic_summaries,
    validate_log as validate_automatic,
)

SPEC = importlib.util.spec_from_file_location("power_loss_shard", SCRIPT)
SHARD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SHARD)


class PowerLossShardTests(unittest.TestCase):
    def setUp(self):
        self.names = (
            [SHARD.AUTOMATIC_PREFIX + name for name in SHARD.AUTOMATIC_CASES.values()]
            + list(SHARD.RECOVERY_CASES.values())
            + list(SHARD.PUBLICATION_CASES.values())
            + [SHARD.ABANDONMENT_PREFIX + f"case_{index}" for index in range(8)]
            + [
                SHARD.CONSUMER_PREFIX + "reclamation::case",
                SHARD.CONSUMER_PREFIX + "vector_incremental::case",
                SHARD.SEARCH_PREFIX + "case",
                SHARD.GC_CASE,
                SHARD.GC_CASE + "_with_retained_reader",
                SHARD.PREFIX + "recovery",
                SHARD.PREFIX + "retained_branch",
            ]
        )
        self.discovery = "\n".join(name + ": test" for name in self.names) + "\n"

    def test_windows_text_mode_preserves_bash_arguments_and_complete_membership(self):
        native_open = Path.open

        def windows_open(
            path, mode="r", buffering=-1, encoding=None, errors=None, newline=None
        ):
            if "w" in mode and "b" not in mode and newline is None:
                newline = "\r\n"
            return native_open(path, mode, buffering, encoding, errors, newline)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            discovery = root / "discovery.txt"
            discovery.write_bytes(self.discovery.replace("\n", "\r\n").encode("utf-8"))
            for shard in [
                "full",
                *SHARD.AUTOMATIC_CASES,
                *SHARD.RECOVERY_CASES,
                *SHARD.PUBLICATION_CASES,
                *SHARD.FAMILY_SHARDS,
                "remaining",
            ]:
                with self.subTest(shard=shard):
                    output = root / shard
                    with patch.object(Path, "open", windows_open), patch(
                        "sys.argv",
                        [
                            "selector",
                            "--discovery",
                            str(discovery),
                            "--shard",
                            shard,
                            "--output-dir",
                            str(output),
                        ],
                    ):
                        SHARD.main()
                    for path in output.iterdir():
                        self.assertNotIn(b"\r", path.read_bytes(), path.name)
                    test_filter = (
                        (output / "test-filter.txt").read_bytes().rstrip(b"\n").decode()
                    )
                    # Match Bash read -r: remove LF only, preserving an erroneous CR.
                    arguments = (
                        (output / "test-arguments.txt")
                        .read_bytes()
                        .decode()
                        .split("\n")[:-1]
                    )
                    if (
                        shard in SHARD.AUTOMATIC_CASES
                        or shard in SHARD.RECOVERY_CASES
                        or shard in SHARD.PUBLICATION_CASES
                        or shard == "branch-gc"
                        or shard == "remaining"
                    ):
                        self.assertEqual(arguments[0], "--exact")
                        exact_names = [test_filter, *arguments[1:]]
                        selected = [name for name in self.names if name in exact_names]
                    else:
                        self.assertTrue(
                            all(option == "--skip" for option in arguments[::2])
                        )
                        self.assertEqual(len(arguments) % 2, 0)
                        selected = [
                            name
                            for name in self.names
                            if test_filter in name
                            and not any(prefix in name for prefix in arguments[1::2])
                        ]
                    manifest = json.loads((output / "partition.json").read_text())
                    self.assertEqual(
                        sorted(selected), sorted(manifest["partitions"][shard])
                    )
                    members = [
                        name
                        for group, names in manifest["partitions"].items()
                        if group != "full"
                        for name in names
                    ]
                    self.assertCountEqual(members, self.names)

    def test_invalid_discovery_fails_closed(self):
        invalid = [
            "",
            self.discovery + self.names[0] + ": test\n",
            self.discovery.replace(self.names[0] + ": test\n", ""),
            self.discovery + SHARD.AUTOMATIC_PREFIX + "unexpected: test\n",
            self.discovery.replace(
                next(iter(SHARD.RECOVERY_CASES.values())) + ": test\n", ""
            ),
            self.discovery + SHARD.RECOVERY_PREFIX + "unexpected: test\n",
            self.discovery.replace(
                next(iter(SHARD.PUBLICATION_CASES.values())) + ": test\n", ""
            ),
            self.discovery + SHARD.PUBLICATION_PREFIX + "unexpected: test\n",
            self.discovery.replace(SHARD.ABANDONMENT_PREFIX + "case_0: test\n", ""),
            self.discovery.replace(SHARD.SEARCH_PREFIX + "case: test\n", ""),
            self.discovery.replace(SHARD.GC_CASE + ": test\n", ""),
            "\n".join(
                line
                for line in self.discovery.splitlines()
                if not line.startswith(SHARD.CONSUMER_PREFIX)
                or line.startswith(SHARD.RECOVERY_PREFIX)
            )
            + "\n",
            "\n".join(name + ": test" for name in self.names[:4]) + "\n",
        ]
        for discovery in invalid:
            with self.subTest(discovery=discovery):
                with self.assertRaises(ValueError):
                    SHARD.partition(discovery)

    def test_recovery_selections_cover_actual_rust_cases_and_both_workflows(self):
        root = SCRIPT.parent.parent
        source = (
            root
            / "src/api/tests/power_loss/consumer_vector_incremental/faults/recovery.rs"
        ).read_text()
        actual = {
            SHARD.RECOVERY_PREFIX + name
            for name in re.findall(r"#\[test\]\s+fn ([a-z_]+)\(", source)
        }
        self.assertEqual(actual, set(SHARD.RECOVERY_CASES.values()))
        weekly = (root / ".github/workflows/ci.yml").read_text()
        native = (root / ".github/workflows/native-bindings.yml").read_text()
        self.assertCountEqual(
            re.findall(r"shard: (recovery-[a-z-]+)", weekly), SHARD.RECOVERY_CASES
        )
        self.assertCountEqual(
            re.findall(r"consumers-(recovery-[a-z-]+)", native), SHARD.RECOVERY_CASES
        )
        publication = (
            root / "src/api/tests/power_loss/consumer_vector_incremental/faults.rs"
        ).read_text()
        actual = {
            SHARD.PUBLICATION_PREFIX + name
            for name in re.findall(r"#\[test\]\s+fn ([a-z_]+)\(", publication)
        }
        self.assertEqual(actual, set(SHARD.PUBLICATION_CASES.values()))
        self.assertCountEqual(
            re.findall(r"shard: (publication-[a-z-]+)", weekly), SHARD.PUBLICATION_CASES
        )
        self.assertCountEqual(
            re.findall(r"consumers-(publication-[a-z-]+)", native), SHARD.PUBLICATION_CASES
        )
        automatic = (
            root / "src/api/tests/power_loss/automatic_checkpoint.rs"
        ).read_text()
        actual = {name for name in re.findall(r"#\[test\]\s+fn ([a-z_]+)\(", automatic)}
        self.assertEqual(actual, set(SHARD.AUTOMATIC_CASES.values()))
        self.assertCountEqual(
            re.findall(r"shard: ((?:head|root|wal-sync|manifest)-[a-z-]+)\n", weekly),
            SHARD.AUTOMATIC_CASES,
        )
        for shard in SHARD.FAMILY_SHARDS:
            self.assertEqual(weekly.count(f"shard: {shard}\n"), 1)
        self.assertCountEqual(
            re.findall(r"- os: windows-latest\s+shard: ([a-z-]+)", weekly),
            [
                *SHARD.AUTOMATIC_CASES,
                *SHARD.RECOVERY_CASES,
                *SHARD.PUBLICATION_CASES,
                *SHARD.FAMILY_SHARDS,
                "remaining",
            ],
        )
        self.assertEqual(weekly.count("shard: full\n"), 2)

    def test_automatic_runtime_matrix_requires_all_configurations_and_completion(self):
        all_combinations = []
        for shard in [*SHARD.AUTOMATIC_CASES, "full"]:
            combinations = sorted(automatic_combinations(shard))
            expected_count = 48 if shard == "full" else 6
            self.assertEqual(len(combinations), expected_count)
            if shard != "full":
                all_combinations.extend(combinations)
            lines = [
                f"automatic-power-v4 cut={cut} policy={policy} "
                f"boundary={boundary} generation={generation} path=Some(artifact)"
                for cut, policy, boundary, generation in combinations
            ]
            summaries = [
                f"automatic {cut} qualification: 10 physical crash images, "
                f"3 generations, {policies} durability modes, both boundaries"
                for (cut, policies), count in automatic_summaries(shard).items()
                for _ in range(count)
            ]
            self.assertEqual(
                validate_automatic(shard, "\n".join(lines + summaries))[
                    "configurations"
                ],
                expected_count,
            )
            for invalid in (
                lines[:-1] + summaries,
                lines + lines[:1] + summaries,
                lines + summaries[:-1],
                [
                    line.replace("10 physical", "0 physical")
                    for line in lines + summaries
                ],
            ):
                with self.subTest(shard=shard):
                    with self.assertRaises(ValueError):
                        validate_automatic(shard, "\n".join(invalid))
        self.assertEqual(len(all_combinations), 48)
        self.assertEqual(len(set(all_combinations)), 48)
        self.assertEqual(
            validate_automatic("remaining", "other output")["configurations"], 0
        )
        with self.assertRaises(ValueError):
            validate_automatic("remaining", lines[0])

    def test_runtime_coverage_rejects_missing_duplicate_and_wrong_source_cases(self):
        all_combinations = []
        all_lines = []
        for shard in SHARD.RECOVERY_CASES:
            combinations = sorted(expected_combinations(shard))
            self.assertEqual(len(combinations), 7 if "lost" in shard else 28)
            lines = [
                "canonical-vector-recovery-power-v1 "
                f"origin={origin} cut={cut} durability={durability} graph={graph} "
                f"source_updated={updated} plans=7 distinct_reopens=3"
                for origin, cut, durability, graph, updated in combinations
            ]
            self.assertEqual(
                validate_log(shard, "\n".join(lines))["combinations"], len(lines)
            )
            for invalid in (
                lines[:-1],
                lines + lines[:1],
                [lines[0].replace("source_updated=", "wrong_field=")] + lines[1:],
                [lines[0].replace("origin=", "origin=Wrong")] + lines[1:],
                [lines[0].replace("graph=", "graph=Wrong")] + lines[1:],
            ):
                with self.subTest(shard=shard):
                    with self.assertRaises(ValueError):
                        validate_log(shard, "\n".join(invalid))
            all_combinations.extend(combinations)
            all_lines.extend(lines)
        self.assertEqual(len(all_combinations), 112)
        self.assertEqual(len(set(all_combinations)), 112)
        self.assertEqual(
            validate_log("full", "\n".join(all_lines))["combinations"], 112
        )
        self.assertEqual(
            validate_log("remaining", "other test output")["combinations"], 0
        )
        with self.assertRaises(ValueError):
            validate_log("remaining", all_lines[0])

    def test_recovery_cli_preserves_lf_for_windows_bash(self):
        for arguments in (
            ["--prefix"], ["--publication-prefix"], ["publication-artifacts"],
            ["recovery-selected"], ["recovery-lost-sql-link"],
        ):
            with self.subTest(arguments=arguments):
                buffer = io.BytesIO()
                output = io.TextIOWrapper(buffer, encoding="utf-8", newline="\r\n")
                with patch("sys.stdout", output), patch(
                    "sys.argv", ["selector", *arguments]
                ):
                    recovery_main()
                    output.flush()
                    self.assertNotIn(b"\r", buffer.getvalue())
                    self.assertTrue(buffer.getvalue().endswith(b"\n"))


if __name__ == "__main__":
    unittest.main()
