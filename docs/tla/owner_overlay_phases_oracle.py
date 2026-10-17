# Copyright 2026 Nowledge
# Licensed under the Apache License, Version 2.0.
"""Conditional phased owner algebra; not native capture or retention evidence.

Run with: python3 docs/tla/owner_overlay_phases_oracle.py
"""

import itertools
import json

from owner_overlay_oracle import documents


OWNERS = (1, 2)
VALUES = (None, ("a", 0, 0), ("b", 0, 0))


def resolve(events):
    """Greater native phase wins; equal-phase states must agree exactly."""
    seen = {}
    latest = {}
    for phase, owner, document in events:
        key = (phase, owner)
        if key in seen and seen[key] != document:
            raise ValueError("conflicting owner state in one pinned phase")
        seen[key] = document
        if owner not in latest or phase > latest[owner][0]:
            latest[owner] = (phase, document)
    return {owner: latest.get(owner, (0, None))[1] for owner in OWNERS}


def schedules(baseline, catchup):
    """Preserve catch-up phase order; allow C rows before/between/after it."""
    if not baseline:
        yield catchup
        return
    if not catchup:
        yield baseline
        return
    for suffix in schedules(baseline[1:], catchup):
        yield baseline[:1] + suffix
    for suffix in schedules(baseline, catchup[1:]):
        yield catchup[:1] + suffix


def selections(before, after):
    changed = {owner for owner in OWNERS if before[owner] != after[owner]}
    for size in range(len(OWNERS) + 1):
        for chosen in itertools.combinations(OWNERS, size):
            if changed <= set(chosen):
                for order in itertools.permutations(chosen):
                    yield order


def main():
    states = [dict(zip(OWNERS, values)) for values in itertools.product(VALUES, repeat=2)]
    baselines = []
    for state in states:
        try:
            documents(state)
        except ValueError:
            continue
        baselines.append(state)
    checked = refused = 0
    for baseline, middle, final in itertools.product(baselines, states, states):
        # Intermediate states may collide: uniqueness is required on final E.
        for c_order in itertools.permutations(OWNERS):
            c = tuple((0, owner, baseline[owner]) for owner in c_order)
            for first in selections(baseline, middle):
                for second in selections(middle, final):
                    e1 = tuple((1, owner, middle[owner]) for owner in first)
                    e2 = tuple((2, owner, final[owner]) for owner in second)
                    for trace in schedules(c, e1 + e2):
                        for events in (trace, trace + trace):
                            actual = resolve(events)
                            assert actual == final
                            try:
                                expected_documents = documents(final)
                            except ValueError:
                                try:
                                    documents(actual)
                                except ValueError:
                                    refused += 1
                                else:
                                    raise AssertionError("duplicate owners hidden")
                            else:
                                assert documents(actual) == expected_documents
                            checked += 1

    # A C row arriving last must not resurrect the owner's removed document.
    delayed_c = ((1, 1, None), (0, 1, VALUES[1]))
    assert resolve(delayed_c)[1] is None
    last_arrival = {owner: document for _, owner, document in delayed_c}
    assert last_arrival[1] != resolve(delayed_c)[1]
    # A transient duplicate in E1 can be repaired by E2; do not reject at E1.
    repaired = ((0, 1, VALUES[1]), (0, 2, VALUES[2]),
                (1, 1, VALUES[2]), (2, 2, VALUES[1]))
    assert documents(resolve(repaired)) == (VALUES[1], VALUES[2])
    # Check all earlier same-phase visits, even after a newer phase supersedes it.
    for first, second in itertools.permutations(
        (None, ("a", 0, 0), ("a", 1, 0), ("a", 0, 1)), 2
    ):
        try:
            resolve(((1, 1, first), (2, 1, None), (1, 1, second)))
        except ValueError:
            pass
        else:
            raise AssertionError("superseded same-phase conflict hidden")
    print(json.dumps({
        "scope": "finite conditional phased algebra; complete intervals and Ends assumed",
        "C_states": len(baselines), "states_per_catchup_phase": len(states),
        "catchup_phases": 2, "scheduled_and_repeated_traces": checked,
        "duplicate_owner_refusals": refused,
        "controls": ["delayed C resurrection", "transient collision repaired",
                     "superseded same-phase conflict: absence/body/vector"],
        "limits": "No native reducer, capture lease, quota, activation or progress proof",
    }, indent=2))


if __name__ == "__main__":
    main()
