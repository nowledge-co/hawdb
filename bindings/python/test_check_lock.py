# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Regression cases for the release dependency identity gate."""

import unittest

from check_lock import compare_locks


def registry(version="1.0.0", source="registry+crates.io", checksum="original"):
    return {
        "name": "shared", "version": version, "source": source, "checksum": checksum
    }


class LockIdentityTests(unittest.TestCase):
    def test_binding_only_packages_do_not_require_engine_ownership(self):
        root = {"package": [registry()]}
        binding = {"package": [registry(), {"name": "pyo3", "version": "0.30.1"}]}
        self.assertEqual(compare_locks(root, binding), 1)

    def test_version_drift_is_rejected(self):
        with self.assertRaises(ValueError):
            compare_locks(
                {"package": [registry()]}, {"package": [registry(version="1.0.1")]}
            )

    def test_same_version_with_a_different_checksum_is_rejected(self):
        with self.assertRaises(ValueError):
            compare_locks(
                {"package": [registry()]}, {"package": [registry(checksum="changed")]}
            )

    def test_same_name_from_a_different_source_is_rejected(self):
        with self.assertRaises(ValueError):
            compare_locks(
                {"package": [registry()]}, {"package": [registry(source="git+other")]}
            )

    def test_any_reviewed_root_version_can_be_selected(self):
        root = {"package": [registry(), registry(version="2.0.0")]}
        self.assertEqual(
            compare_locks(root, {"package": [registry(version="2.0.0")]}), 1
        )

    def test_local_engine_versions_must_also_match(self):
        root = {"package": [{"name": "hawdb", "version": "0.19.0"}]}
        self.assertEqual(compare_locks(root, root), 1)
        with self.assertRaises(ValueError):
            compare_locks(root, {"package": [{"name": "hawdb", "version": "0.20.0"}]})


if __name__ == "__main__":
    unittest.main()
