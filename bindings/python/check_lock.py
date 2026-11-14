# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Keep the binding's shared dependency identities aligned with the engine."""

import argparse
import hashlib
import tomllib
from pathlib import Path


def compare_locks(root: dict, binding: dict) -> int:
    identities = {}
    for package in root["package"]:
        identity = (
            package["version"], package.get("source"), package.get("checksum")
        )
        identities.setdefault(package["name"], set()).add(identity)

    shared = 0
    for package in binding["package"]:
        allowed = identities.get(package["name"])
        if allowed is None:
            continue
        identity = (
            package["version"], package.get("source"), package.get("checksum")
        )
        if identity not in allowed:
            raise ValueError(
                f"Binding dependency differs from root lock: {package['name']} {identity}"
            )
        shared += 1
    return shared


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("Cargo.lock"))
    parser.add_argument(
        "--binding", type=Path, default=Path("bindings/python/Cargo.lock")
    )
    args = parser.parse_args()
    root = tomllib.loads(args.root.read_text())
    content = args.binding.read_bytes()
    shared = compare_locks(root, tomllib.loads(content.decode()))
    print(f"Verified {shared} shared dependency identities")
    print(f"Binding lock SHA-256: {hashlib.sha256(content).hexdigest()}")


if __name__ == "__main__":
    main()
