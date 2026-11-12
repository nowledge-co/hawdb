#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Decide whether the Native Release workflow may build and publish.

HawDB's own release artifacts may ship as stable releases (AGENTS.md, Mem
release modes). The Native Release workflow has two modes:

- ``pull-request``: a pull request that changes the pipeline builds and tests
  every archive and uploads or publishes nothing;
- ``release``: a published, non-draft GitHub Release whose prerelease flag
  agrees with its version. A stable release has version
  ``<major>.<minor>.<patch>`` and publishes its npm packages under ``latest``;
  a prerelease has version ``<major>.<minor>.<patch>-dev.<n>`` or
  ``-nightly.<n>`` and publishes under ``dev`` or ``nightly``. Its archives are
  attached to that GitHub Release.

A draft, a tag that differs from the source version, a stable release with a
prerelease version, and a prerelease with a stable or other prerelease version
(for example ``-rc.1``) are refused, so npm ``latest`` never receives a
prerelease version.

    native-release-mode.py --event pull_request --version 0.31.0
    native-release-mode.py --event release --tag v0.31.0 \
        --version 0.31.0 --release release.json

``release.json`` is ``gh release view <tag> --json tagName,isDraft,isPrerelease``.
Prints ``mode``, ``tag`` and ``npm_tag`` as ``key=value`` lines for
``$GITHUB_OUTPUT``; exits 1 with a workflow error annotation when refused.
"""

import argparse
import json
import re
import sys

MODES = ("pull-request", "release")
RELEASE_EVENTS = ("release", "workflow_dispatch")
CORE = r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
STABLE_VERSION = re.compile(CORE)
DEVELOPMENT_VERSION = re.compile(CORE + r"-(?P<channel>dev|nightly)(\.[0-9A-Za-z-]+)+")


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
    prerelease = release.get("isPrerelease")
    if prerelease is False:
        if STABLE_VERSION.fullmatch(version) is None:
            raise Refused(
                f"{tag} is a stable release but {version} is not a stable version (<x.y.z>); "
                "npm latest takes only stable versions"
            )
        return {"mode": "release", "tag": tag, "npm_tag": "latest"}
    if prerelease is not True:
        raise Refused(f"{tag} has no prerelease flag")
    match = DEVELOPMENT_VERSION.fullmatch(version)
    if match is None:
        raise Refused(
            f"{tag} is a prerelease but {version} is not a development version "
            "(<x.y.z>-dev.<n> or <x.y.z>-nightly.<n>)"
        )
    return {"mode": "release", "tag": tag, "npm_tag": match.group("channel")}


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
