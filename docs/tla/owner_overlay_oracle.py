# Copyright 2026 Nowledge
# Licensed under the Apache License, Version 2.0.
"""Finite conditional algebra; not native capture or activation qualification.

Run with: python3 docs/tla/owner_overlay_oracle.py
"""

import itertools
import json


OWNERS = (1, 2)
DOCUMENTS = tuple(itertools.product(("a", "b"), (0, 1), (0, 1)))
VALUES = (None,) + DOCUMENTS


def documents(state):
    """Independent complete-set oracle, preserving the ownership check."""
    rows = [document for document in state.values() if document is not None]
    if len({document[0] for document in rows}) != len(rows):
        raise ValueError("duplicate document owners")
    return tuple(sorted(rows))


def overlay(baseline, events):
    """Resolve owners before emitting document IDs; repeats must be identical."""
    final = {}
    for owner, document in events:
        if owner in final and final[owner] != document:
            raise ValueError("different states for one pinned E owner")
        final[owner] = document
    return documents(baseline | final)


def naive_document_priority(baseline, events):
    """Deliberately wrong: retain old IDs and overwrite by new document ID."""
    rows = {
        document[0]: document
        for document in baseline.values()
        if document is not None
    }
    for _, document in events:
        if document is not None:
            rows[document[0]] = document
    return tuple(sorted(rows.values()))


def main():
    states = [dict(zip(OWNERS, values)) for values in itertools.product(VALUES, repeat=2)]
    baselines = []
    for state in states:
        try:
            documents(state)
        except ValueError:
            continue
        baselines.append(state)
    selections = tuple(
        set(owners)
        for length in range(3)
        for owners in itertools.combinations(OWNERS, length)
    )
    checks = 0
    duplicate_refusals = 0
    witnesses = {}
    for baseline in baselines:
        for final in states:
            changed = {owner for owner in OWNERS if baseline[owner] != final[owner]}
            try:
                expected = documents(final)
                valid = True
            except ValueError:
                expected = None
                valid = False
            for selected in selections:
                if not changed <= selected:
                    continue
                for order in itertools.permutations(sorted(selected)):
                    trace = [(owner, final[owner]) for owner in order]
                    for events in (trace, trace + trace):
                        try:
                            actual = overlay(baseline, events)
                        except ValueError:
                            assert not valid
                            duplicate_refusals += 1
                        else:
                            assert valid and actual == expected
                        checks += 1
                    wrong = naive_document_priority(baseline, trace)
                    if valid and wrong != expected:
                        for label, condition in (
                            ("deleted_owner", any(
                                baseline[owner] is not None and final[owner] is None
                                for owner in OWNERS
                            )),
                            ("renamed_document", any(
                                baseline[owner] is not None
                                and final[owner] is not None
                                and baseline[owner][0] != final[owner][0]
                                for owner in OWNERS
                            )),
                        ):
                            if condition:
                                witnesses.setdefault(label, {
                                    "C": baseline, "E": final, "events": trace,
                                    "incorrect": wrong, "expected": expected,
                                })
                    if not valid:
                        witnesses.setdefault("hidden_duplicate_owner", {
                            "C": baseline, "E": final, "events": trace,
                            "incorrect": wrong, "expected": "refuse",
                        })
            if changed and valid:
                omitted = min(changed)
                events = [(owner, final[owner]) for owner in sorted(changed) if owner != omitted]
                try:
                    wrong = overlay(baseline, events)
                except ValueError:
                    wrong = "refuse"
                if wrong != expected:
                    witnesses.setdefault("omitted_owner_event", {
                        "C": baseline, "E": final, "omitted": omitted,
                        "incorrect": wrong, "expected": expected,
                    })
    assert len(witnesses) == 4
    try:
        overlay({1: None, 2: None}, [(1, ("a", 0, 0)), (1, ("a", 1, 0))])
    except ValueError:
        pass
    else:
        raise AssertionError("mixed E states accepted")
    assert overlay(
        {1: ("a", 0, 0), 2: ("b", 0, 0)},
        [(1, ("b", 1, 1)), (2, ("a", 1, 1))],
    ) == (("a", 1, 1), ("b", 1, 1))
    # E collides with an unchanged C owner, even though the emitted bodies match.
    baseline = {1: None, 2: ("b", 0, 0)}
    final = {1: ("b", 0, 0), 2: ("b", 0, 0)}
    events = [(1, final[1])]
    try:
        overlay(baseline, events)
    except ValueError:
        pass
    else:
        raise AssertionError("cross-phase duplicate owner accepted")
    witnesses["cross_phase_duplicate_owner"] = {
        "C": baseline, "E": final, "events": events,
        "incorrect": naive_document_priority(baseline, events), "expected": "refuse",
    }
    print(json.dumps({
        "scope": "finite conditional algebra; no native or capture completeness claim",
        "canonical_C_states": len(baselines), "E_states": len(states),
        "ordered_and_repeated_input_cases": checks,
        "duplicate_owner_refusals": duplicate_refusals,
        "counterexamples": witnesses,
    }, indent=2))


if __name__ == "__main__":
    main()
