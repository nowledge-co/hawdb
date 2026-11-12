#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Decide whether the Native Release workflow may build and publish.

AGENTS.md keeps HawDB out of every stable or GA release during the current
development phase: only nightly or development builds may compile, bundle or
ship it. The Native Release workflow therefore has exactly two modes:

- ``pull-request``: a pull request that changes the pipeline builds and tests
  every archive and uploads or publishes nothing;
- ``development``: a GitHub Release that is marked as a prerelease *and* whose
  version is a development prerelease, ``<major>.<minor>.<patch>-dev.<n>`` or
  ``-nightly.<n>``. Its archives are attached to that prerelease and its npm
  packages are published under the ``dev`` or ``nightly`` dist-tag, never
  ``latest``.

Everything else, including any stable (non-prerelease) release and a
prerelease flag on a stable version number, is refused. Requiring both the
GitHub flag and the version suffix means promoting the GitHub Release to
stable afterwards still cannot produce a stable-numbered artifact.

    native-release-mode.py --event pull_request --version 0.31.0
    native-release-mode.py --event release --tag v0.31.0-dev.1 \
        --version 0.31.0-dev.1 --release release.json

``release.json`` is ``gh release view <tag> --json tagName,isDraft,isPrerelease``.
Prints ``mode``, ``tag`` and ``npm_tag`` as ``key=value`` lines for
``$GITHUB_OUTPUT``; exits 1 with a workflow error annotation when refused.
"""

import argparse
import json
import re
import sys

MODES = ("pull-request", "development")
RELEASE_EVENTS = ("release", "workflow_dispatch")
DEVELOPMENT_VERSION = re.compile(
    r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
    r"-(?P<channel>dev|nightly)(\.[0-9A-Za-z-]+)+"
)
POLICY = (
    "HawDB must not appear in a stable or GA release (AGENTS.md, Mem release modes); "
    "Native Release builds and publishes only development prereleases"
)


class Refused(Exception):
    pass


def resolve(event, version, tag=None, release=None):
    """Return the workflow mode for this event, or raise Refused."""
    if event == "pull_request":
        return {"mode": "pull-request", "tag": f"v{version}", "npm_tag": ""}
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
        raise Refused(
            f"{version} is not a development version (<x.y.z>-dev.<n> or <x.y.z>-nightly.<n>). {POLICY}"
        )
    return {"mode": "development", "tag": tag, "npm_tag": match.group("channel")}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--event", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--tag")
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
    for key in ("mode", "tag", "npm_tag"):
        print(f"{key}={outputs[key]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
