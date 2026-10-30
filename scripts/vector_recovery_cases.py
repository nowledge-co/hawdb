#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Shared vector power-loss selections and complete recovery matrix validation."""

import argparse
from collections import Counter
import itertools
import json
from pathlib import Path
import re
import sys

PUBLICATION_PREFIX = "api::tests::power_loss::consumer_vector_incremental::faults::"
PUBLICATION_CASES = {
    "publication-artifacts": (
        PUBLICATION_PREFIX + "vector_index_incremental_power_loss_retains_the_linked_artifact_closure"
    ),
    "publication-selectors": (
        PUBLICATION_PREFIX + "vector_index_incremental_power_loss_rejects_torn_private_selectors"
    ),
    "publication-barriers": (
        PUBLICATION_PREFIX + "vector_index_incremental_power_loss_recovers_atomic_selector_and_root_barriers"
    ),
}
RECOVERY_PREFIX = PUBLICATION_PREFIX + "recovery::"
CASE_PREFIX = RECOVERY_PREFIX + "vector_index_incremental_power_loss_recovery_publication_"
RECOVERY_CASES = {
    "recovery-births": CASE_PREFIX + "retains_unselected_births",
    "recovery-unselected": CASE_PREFIX + "settles_unselected_intents",
    "recovery-selected": CASE_PREFIX + "completes_selected_intents",
    "recovery-lost-sql-link": (
        CASE_PREFIX + "preserves_lost_relaxed_sql_sources_after_link"
    ),
    "recovery-lost-sql-selector": (
        CASE_PREFIX + "preserves_lost_relaxed_sql_sources_after_selector_write"
    ),
    "recovery-lost-nodes-link": (
        CASE_PREFIX + "preserves_lost_relaxed_node_sources_after_link"
    ),
    "recovery-lost-nodes-selector": (
        CASE_PREFIX + "preserves_lost_relaxed_node_sources_after_selector_write"
    ),
}
CUTS = ("Write", "FileSync(Before)", "FileSync(After)", "Rename(Before)",
        "Rename(After)", "Directory(Before)", "Directory(After)")


def expected_combinations(shard):
    if shard == "full":
        return set().union(*(expected_combinations(key) for key in RECOVERY_CASES))
    if shard == "remaining":
        return set()
    origins = {
        "recovery-births": "Link",
        "recovery-unselected": "SelectorRename(Before)",
        "recovery-selected": "SelectorRename(After)",
    }
    if shard in origins:
        return set(itertools.product(
            [origins[shard]], CUTS,
            ["SyncOnEveryWrite", "SyncOnCheckpoint"], ["false", "true"], ["true"],
        ))
    if shard not in RECOVERY_CASES:
        raise ValueError(f"unknown vector recovery shard: {shard}")
    graph = "true" if shard.startswith("recovery-lost-nodes-") else "false"
    origin = "Link" if shard.endswith("-link") else "SelectorWrite"
    return set(itertools.product(
        [origin], CUTS, ["SyncOnCheckpoint"], [graph], ["false"],
    ))


def validate_log(shard, text):
    expected = Counter({key: 1 for key in expected_combinations(shard)})
    actual = Counter()
    plans = reopens = 0
    for line in text.splitlines():
        if "canonical-vector-recovery-power-v1 " not in line:
            continue
        fields = dict(re.findall(r"([a-z_]+)=(\S+)", line))
        try:
            key = tuple(fields[name] for name in (
                "origin", "cut", "durability", "graph", "source_updated"
            ))
            count, opened = int(fields["plans"]), int(fields["distinct_reopens"])
        except (KeyError, ValueError) as error:
            raise ValueError("incomplete recovery coverage record") from error
        if count <= 0 or opened <= 0:
            raise ValueError("recovery combination has no evaluated physical plans")
        actual[key] += 1
        plans += count
        reopens += opened
    if actual != expected:
        raise ValueError(f"recovery coverage differs: missing={expected - actual}, "
                         f"unexpected={actual - expected}")
    return {"protocol": "hawdb-vector-recovery-coverage-v1", "shard": shard,
            "combinations": sum(actual.values()), "fault_plans": plans,
            "distinct_reopens": reopens}


def main():
    # Git Bash strips LF from command substitution but would retain a CR.
    sys.stdout.reconfigure(newline="\n")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "shard", nargs="?",
        choices=[*PUBLICATION_CASES, *RECOVERY_CASES, "full", "remaining"],
    )
    prefix = parser.add_mutually_exclusive_group()
    prefix.add_argument("--prefix", action="store_true")
    prefix.add_argument("--publication-prefix", action="store_true")
    parser.add_argument("--validate-log", type=Path)
    args = parser.parse_args()
    wants_prefix = args.prefix or args.publication_prefix
    if wants_prefix and not args.shard and not args.validate_log:
        print(PUBLICATION_PREFIX if args.publication_prefix else RECOVERY_PREFIX)
    elif (
        args.shard in [*RECOVERY_CASES, "full", "remaining"]
        and args.validate_log
        and not wants_prefix
    ):
        text = args.validate_log.read_text(encoding="utf-8")
        print(json.dumps(validate_log(args.shard, text)))
    elif args.shard in PUBLICATION_CASES and not args.validate_log and not wants_prefix:
        print(PUBLICATION_CASES[args.shard])
    elif args.shard in RECOVERY_CASES and not args.validate_log and not wants_prefix:
        print(RECOVERY_CASES[args.shard])
    else:
        parser.error("select a power-loss case, a recovery coverage log, or a prefix")


if __name__ == "__main__":
    main()
