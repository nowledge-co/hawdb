#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Select a disjoint Windows power-loss partition from actual test discovery."""

import argparse
import json
from pathlib import Path
import re

from automatic_checkpoint_cases import AUTOMATIC_CASES, AUTOMATIC_PREFIX
from vector_recovery_cases import (
    PUBLICATION_CASES,
    PUBLICATION_PREFIX,
    RECOVERY_CASES,
    RECOVERY_PREFIX,
)

PREFIX = "api::tests::power_loss::"
ABANDONMENT_PREFIX = PREFIX + "branch_abandonment::"
CONSUMER_PREFIX = PREFIX + "consumer_"
SEARCH_PREFIX = PREFIX + "search_projection::"
GC_CASE = PREFIX + "deletion_and_gc_cuts_keep_an_unleased_nested_branch_recoverable"
FAMILY_SHARDS = (
    "abandonment",
    "consumer-publication",
    "search-publication",
    "branch-gc",
)


def partition(discovery: str) -> dict[str, list[str]]:
    discovery = re.sub(r"\x1b\[[0-9;]*m", "", discovery)
    names = re.findall(
        r"^(api::tests::power_loss::[a-zA-Z0-9_:]+): test$", discovery, re.M
    )
    if not names or len(names) != len(set(names)):
        raise ValueError("power-loss discovery must contain distinct, nonempty tests")
    automatic = {name for name in names if name.startswith(AUTOMATIC_PREFIX)}
    expected = {AUTOMATIC_PREFIX + name for name in AUTOMATIC_CASES.values()}
    if automatic != expected:
        raise ValueError(
            f"automatic checkpoint discovery changed: {automatic ^ expected}"
        )
    recovery = {name for name in names if name.startswith(RECOVERY_PREFIX)}
    expected_recovery = set(RECOVERY_CASES.values())
    if recovery != expected_recovery:
        raise ValueError(
            f"vector recovery discovery changed: {recovery ^ expected_recovery}"
        )
    publication = {
        name for name in names if name.startswith(PUBLICATION_PREFIX)
    } - recovery
    expected_publication = set(PUBLICATION_CASES.values())
    if publication != expected_publication:
        raise ValueError(
            f"vector publication discovery changed: {publication ^ expected_publication}"
        )
    abandonment = {name for name in names if name.startswith(ABANDONMENT_PREFIX)}
    if len(abandonment) != 8:
        raise ValueError("branch abandonment discovery must contain all eight cases")
    consumers = (
        {name for name in names if name.startswith(CONSUMER_PREFIX)}
        - recovery
        - publication
    )
    search = {name for name in names if name.startswith(SEARCH_PREFIX)}
    if not consumers or not search or GC_CASE not in names:
        raise ValueError("consumer, search and branch GC discovery must be nonempty")
    partitions = {
        shard: [AUTOMATIC_PREFIX + name] for shard, name in AUTOMATIC_CASES.items()
    }
    partitions.update({shard: [name] for shard, name in RECOVERY_CASES.items()})
    partitions.update({shard: [name] for shard, name in PUBLICATION_CASES.items()})
    partitions["abandonment"] = sorted(abandonment)
    partitions["consumer-publication"] = sorted(consumers)
    partitions["search-publication"] = sorted(search)
    partitions["branch-gc"] = [GC_CASE]
    partitions["remaining"] = sorted(
        set(names)
        - expected
        - expected_recovery
        - expected_publication
        - abandonment
        - consumers
        - search
        - {GC_CASE}
    )
    selected = [name for group in partitions.values() for name in group]
    if (
        not partitions["remaining"]
        or len(selected) != len(set(selected))
        or set(selected) != set(names)
    ):
        raise ValueError(
            "power-loss partitions must be nonempty, disjoint and exhaustive"
        )
    partitions["full"] = sorted(names)
    return partitions


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--discovery", type=Path, required=True)
    parser.add_argument(
        "--shard",
        choices=[
            "full",
            *AUTOMATIC_CASES,
            *RECOVERY_CASES,
            *PUBLICATION_CASES,
            *FAMILY_SHARDS,
            "remaining",
        ],
        required=True,
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    partitions = partition(args.discovery.read_text(encoding="utf-8"))
    selected = partitions[args.shard]
    if (
        args.shard in AUTOMATIC_CASES
        or args.shard in RECOVERY_CASES
        or args.shard in PUBLICATION_CASES
        or args.shard == "branch-gc"
    ):
        test_filter, test_arguments = selected[0], ["--exact"]
    elif args.shard == "abandonment":
        test_filter, test_arguments = ABANDONMENT_PREFIX, []
    elif args.shard == "consumer-publication":
        test_filter, test_arguments = CONSUMER_PREFIX, ["--skip", PUBLICATION_PREFIX]
    elif args.shard == "search-publication":
        test_filter, test_arguments = SEARCH_PREFIX, []
    elif args.shard == "remaining":
        # libtest --skip is substring based. Select the discovered names exactly
        # so a future case containing GC_CASE is neither lost nor run twice.
        test_filter, test_arguments = selected[0], ["--exact", *selected[1:]]
    else:
        test_filter, test_arguments = PREFIX, []
    args.output_dir.mkdir(parents=True, exist_ok=True)
    # Bash consumes these files as arguments. Windows text-mode CRLF translation
    # would leave a literal carriage return in the filter and each option.
    outputs = {
        "selected-tests.txt": "\n".join(selected) + "\n",
        "test-filter.txt": test_filter + "\n",
        "test-arguments.txt": "".join(argument + "\n" for argument in test_arguments),
        "partition.json": json.dumps(
            {"shard": args.shard, "partitions": partitions}, indent=2
        )
        + "\n",
    }
    for name, contents in outputs.items():
        with (args.output_dir / name).open(
            "w", encoding="utf-8", newline="\n"
        ) as output:
            output.write(contents)


if __name__ == "__main__":
    main()
