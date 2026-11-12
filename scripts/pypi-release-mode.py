#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Decide whether the PyPI workflow may build and publish a release.

AGENTS.md keeps HawDB out of every stable or GA release during the current
development phase: only nightly or development builds may compile, bundle or
ship it. The `hawdb` sdist bundles HawDB source and its wheels are HawDB
binaries, so Publish Python to PyPI accepts only a development prerelease:

- a non-draft GitHub Release marked as a prerelease, and
- a tag `v<version>` whose Cargo version is `<major>.<minor>.<patch>-dev.<n>`.

maturin maps that Cargo version to the PEP 440 development release
`<major>.<minor>.<patch>.dev<n>`, which installers skip unless asked for a
prerelease or an exact pin, so it never enters a stable dependency graph.
`-nightly.<n>` has no PEP 440 form (maturin refuses to build it) and is
refused here; the Native Release workflow covers nightly channels. Requiring
both the GitHub flag and the version suffix means a stable version number is
refused even when its release is flagged as a prerelease, and promoting a
development prerelease to stable afterwards still names a `.dev` version.

    pypi-release-mode.py --event release --tag v0.31.0-dev.1 \
        --version 0.31.0-dev.1 --release release.json

``release.json`` is ``gh release view <tag> --json tagName,isDraft,isPrerelease``.
Prints ``mode`` and ``python_version`` as ``key=value`` lines for
``$GITHUB_OUTPUT``; exits 1 with a workflow error annotation when refused.
This mirrors the Native Release gate (scripts/native-release-mode.py) with the
version forms PyPI can represent.
"""

import argparse
import json
import re
import sys

RELEASE_EVENTS = ("release", "workflow_dispatch")
DEVELOPMENT_VERSION = re.compile(
    r"(?P<release>(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*))"
    r"-dev\.(?P<number>0|[1-9][0-9]*)"
)
POLICY = (
    "HawDB must not appear in a stable or GA release (AGENTS.md, Mem release modes); "
    "PyPI publishes only development prereleases"
)


class Refused(Exception):
    pass


def resolve(event, version, tag, release):
    """Return the workflow outputs for this release, or raise Refused."""
    if event not in RELEASE_EVENTS:
        raise Refused(f"unsupported event {event!r}")
    if tag != f"v{version}":
        raise Refused(f"release tag {tag!r} does not match the source version v{version}")
    if release is None:
        raise Refused(f"no GitHub Release found for {tag}")
    if release.get("tagName") != tag:
        raise Refused(f"GitHub Release metadata is for {release.get('tagName')!r}, not {tag}")
    if release.get("isDraft") is not False:
        raise Refused(f"{tag} is a draft release")
    if release.get("isPrerelease") is not True:
        raise Refused(f"{tag} is a stable release. {POLICY}")
    match = DEVELOPMENT_VERSION.fullmatch(version)
    if match is None:
        raise Refused(f"{version} is not a development version (<x.y.z>-dev.<n>). {POLICY}")
    return {
        "mode": "development",
        "python_version": f"{match.group('release')}.dev{match.group('number')}",
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--event", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--release", help="gh release view JSON")
    arguments = parser.parse_args(argv)
    release = None
    if arguments.release:
        with open(arguments.release, encoding="utf-8") as handle:
            release = json.load(handle)
    try:
        outputs = resolve(arguments.event, arguments.version, arguments.tag, release)
    except Refused as refusal:
        print(f"::error::{refusal}", file=sys.stderr)
        return 1
    for key in ("mode", "python_version"):
        print(f"{key}={outputs[key]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
