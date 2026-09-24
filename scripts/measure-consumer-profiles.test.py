#!/usr/bin/env python3
"""Evidence-checker regressions; no compiler/build subprocesses required."""
import importlib.util
import os
from pathlib import Path
import random
import unittest

source = Path(__file__).with_name("measure-consumer-profiles.py")
if not source.exists():
    source = Path(os.environ["TEST_SRCDIR"]) / os.environ["TEST_WORKSPACE"] / "scripts/measure-consumer-profiles.py"
spec = importlib.util.spec_from_file_location("profiles", source)
profiles = importlib.util.module_from_spec(spec)
spec.loader.exec_module(profiles)


def metadata(edges):
    names = {"host"} | {name for edge in edges for name in edge[:2]}
    return {
        "packages": [{"id": name, "name": name} for name in names],
        "resolve": {"root": "host", "nodes": [
            {"id": name, "deps": [{"pkg": target, "dep_kinds": [{"kind": kind}]}
                                  for source, target, kind in edges if source == name]}
            for name in names]},
    }


class ProfileEvidenceTests(unittest.TestCase):
    def test_absent_optional_dependencies_and_present_baseline_are_both_required(self):
        policy = {"name": "minimal", "required": ["hawdb", "jieba"], "excluded": ["simsimd"]}
        edges = [("host", "hawdb", None), ("hawdb", "jieba", None), ("hawdb", "simsimd", "dev")]
        profiles.check_profile(metadata(edges), policy)
        with self.assertRaisesRegex(ValueError, "excluded present"):
            profiles.check_profile(metadata(edges + [("jieba", "simsimd", None)]), policy)
        with self.assertRaisesRegex(ValueError, "missing"):
            profiles.check_profile(metadata(edges[:1]), policy)

    def test_build_only_path_cannot_be_a_runtime_witness(self):
        graph = metadata([("host", "codegen", "build"), ("codegen", "payload", None)])
        self.assertEqual(set(profiles.normal_closure(graph)), {"host"})
        with self.assertRaisesRegex(ValueError, "excluded present"):
            profiles.check_profile(graph, {"name": "minimal", "required": [], "excluded": ["payload"]})

    def test_cycles_have_finite_dependency_witnesses(self):
        graph = metadata([("host", "a", None), ("a", "b", None), ("b", "a", None)])
        self.assertEqual(profiles.normal_closure(graph)["b"], ["host", "a", "b"])

    def test_seeded_closures_match_independent_fixed_point_oracle(self):
        rng = random.Random(647)
        names = ["host"] + [f"package{i}" for i in range(12)]
        for _ in range(128):
            edges = [(rng.choice(names), rng.choice(names), rng.choice([None, "dev", "build"]))
                     for _ in range(40)]
            expected = {"host"}
            while True:
                expanded = expected | {target for source, target, kind in edges
                                       if source in expected and kind is None}
                if expanded == expected:
                    break
                expected = expanded
            paths = profiles.normal_closure(metadata(edges))
            self.assertEqual(set(paths), expected)
            for target, path in paths.items():
                self.assertEqual(path[0], "host")
                self.assertEqual(path[-1], target)
                self.assertEqual(len(path), len(set(path)))
                for source, destination in zip(path, path[1:]):
                    self.assertIn((source, destination, None), edges)


if __name__ == "__main__":
    unittest.main()
