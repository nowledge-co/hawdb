#!/usr/bin/env python3
"""Select platform-specific link inputs for the MoonBit conformance executable.

Run after moon check and before moon run. The native Rust adapter is a
platform-native static library, not the generic HawDB C FFI library.
"""
from __future__ import annotations

import argparse
from pathlib import Path

SOURCE = "native/target/release/libhawdb_moonbit.a -ldl -lpthread -lm"
FLAGS = {
    "linux": SOURCE,
    "macos": SOURCE + " -framework CoreFoundation -framework IOKit",
    "windows": (
        "native/target/release/hawdb_moonbit.lib"
        " ws2_32.lib userenv.lib bcrypt.lib advapi32.lib ntdll.lib"
        " shell32.lib secur32.lib crypt32.lib iphlpapi.lib ole32.lib"
    ),
}


def configure(pkg: Path, system: str, sdk_prefix: str = "") -> None:
    source = SOURCE.replace("native/target/", f"{sdk_prefix}native/target/")
    replacement = FLAGS[system].replace("native/target/", f"{sdk_prefix}native/target/")
    original = pkg.read_text(encoding="utf-8")
    if original.count(source) != 1:
        raise ValueError(f"Expected exactly one unconfigured link flag field in {pkg}")
    pkg.write_text(original.replace(source, replacement), encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--os", choices=sorted(FLAGS), required=True)
    parser.add_argument("--pkg", type=Path, default=Path("tests/conformance/moon.pkg"))
    parser.add_argument("--sdk-prefix", default="")
    args = parser.parse_args()
    configure(args.pkg, args.os, args.sdk_prefix)
    print(f"LINK_PLATFORM {args.os}")
