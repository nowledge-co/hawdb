/*
 * Copyright 2026 Nowledge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  PLATFORM_PACKAGES,
  UnsupportedPlatformError,
  type ReadAt,
  detectLibc,
  elfInterpreter,
  platformPackageName,
  resolvePrebuiltLibrary,
  selectPlatformPackage,
} from "../src/platform.ts";

describe("prebuilt platform selection", () => {
  test.each([
    [{ platform: "darwin", arch: "arm64" }, "@nowledge/hawdb-ffi-darwin-arm64", "libhawdb_ffi.dylib"],
    [{ platform: "darwin", arch: "x64" }, "@nowledge/hawdb-ffi-darwin-x64", "libhawdb_ffi.dylib"],
    [{ platform: "linux", arch: "x64", libc: "glibc" }, "@nowledge/hawdb-ffi-linux-x64-gnu", "libhawdb_ffi.so"],
    [{ platform: "linux", arch: "arm64", libc: "glibc" }, "@nowledge/hawdb-ffi-linux-arm64-gnu", "libhawdb_ffi.so"],
    [{ platform: "win32", arch: "x64" }, "@nowledge/hawdb-ffi-win32-x64-msvc", "hawdb_ffi.dll"],
  ] as const)("%o selects %s", (host, name, library) => {
    const selected = selectPlatformPackage(host);
    expect(platformPackageName(selected)).toBe(name);
    expect(selected.library).toBe(library);
  });

  test.each([
    { platform: "linux", arch: "x64", libc: "musl" },
    { platform: "linux", arch: "x64", libc: null },
    { platform: "linux", arch: "riscv64", libc: "glibc" },
    { platform: "win32", arch: "arm64" },
    { platform: "freebsd", arch: "x64" },
  ] as const)("refuses unsupported host %o with a source-build hint", (host) => {
    expect(() => selectPlatformPackage(host)).toThrow(UnsupportedPlatformError);
    expect(() => selectPlatformPackage(host)).toThrow(/loadHawDB\(path\)/);
  });

  test("each package is unique and maps to one release target", () => {
    const ids = PLATFORM_PACKAGES.map((platform) => platform.id);
    const targets = PLATFORM_PACKAGES.map((platform) => platform.target);
    expect(new Set(ids).size).toBe(ids.length);
    expect(new Set(targets).size).toBe(targets.length);
    for (const platform of PLATFORM_PACKAGES) {
      expect(platform.libc !== undefined).toBe(platform.os === "linux");
    }
  });

  test("the release workflow builds every published platform", () => {
    const workflow = readFileSync(
      join(import.meta.dir, "../../../.github/workflows/native-release.yml"),
      "utf8",
    );
    for (const platform of PLATFORM_PACKAGES) {
      expect(workflow).toContain(`target: ${platform.target}`);
      expect(workflow).toContain(`npm: ${platform.id}`);
    }
  });
});

// A minimal little-endian ELF64 image: a header, a PT_LOAD and optionally a
// PT_INTERP program header, then the interpreter path.
function elf(interpreter: string | null, elfClass = 2): Uint8Array {
  const path = interpreter === null ? new Uint8Array() : new TextEncoder().encode(`${interpreter}\0`);
  const entries = interpreter === null ? 1 : 2;
  const image = new Uint8Array(64 + 56 * entries + path.length);
  image.set([0x7f, 0x45, 0x4c, 0x46, elfClass, 1, 1]);
  const view = new DataView(image.buffer);
  view.setBigUint64(0x20, 64n, true);
  view.setUint16(0x36, 56, true);
  view.setUint16(0x38, entries, true);
  view.setUint32(64, 1, true); // PT_LOAD
  if (interpreter !== null) {
    const base = 64 + 56;
    view.setUint32(base, 3, true); // PT_INTERP
    view.setBigUint64(base + 8, BigInt(64 + 56 * entries), true);
    view.setBigUint64(base + 32, BigInt(path.length), true);
    image.set(path, 64 + 56 * entries);
  }
  return image;
}

function reader(image: Uint8Array): ReadAt {
  return (offset, length) => image.subarray(offset, offset + length);
}

describe("Linux C library detection", () => {
  test("reads the running executable's program interpreter", () => {
    expect(elfInterpreter(reader(elf("/lib64/ld-linux-x86-64.so.2")))).toBe("/lib64/ld-linux-x86-64.so.2");
    expect(elfInterpreter(reader(elf(null)))).toBeNull();
    expect(elfInterpreter(reader(elf("/lib/ld-musl-x86_64.so.1", 1)))).toBeNull();
    expect(elfInterpreter(reader(new TextEncoder().encode("#!/bin/sh\n")))).toBeNull();
  });

  test("selects glibc for a glibc process on a host that also has musl installed", () => {
    // Debian's musl package installs /usr/lib/ld-musl-x86_64.so.1 beside glibc.
    // Only the running process's interpreter decides; loader files are not read.
    for (const interpreter of [
      "/lib64/ld-linux-x86-64.so.2",
      "/lib/ld-linux-aarch64.so.1",
      "/nix/store/0000-glibc-2.40/lib/ld-linux-x86-64.so.2",
    ]) {
      const libc = detectLibc("linux", reader(elf(interpreter)));
      expect(libc).toBe("glibc");
      expect(selectPlatformPackage({ platform: "linux", arch: "x64", libc }).id).toBe("linux-x64-gnu");
    }
  });

  test("keeps refusing a musl process", () => {
    for (const interpreter of ["/lib/ld-musl-x86_64.so.1", "/lib/ld-musl-aarch64.so.1"]) {
      const libc = detectLibc("linux", reader(elf(interpreter)));
      expect(libc).toBe("musl");
      expect(() => selectPlatformPackage({ platform: "linux", arch: "x64", libc })).toThrow(UnsupportedPlatformError);
    }
  });

  test("returns null when the process libc cannot be determined", () => {
    expect(detectLibc("linux", reader(elf(null)))).toBeNull();
    expect(detectLibc("linux", reader(elf("/system/bin/linker64")))).toBeNull();
    expect(detectLibc("linux", reader(new Uint8Array(8)))).toBeNull();
  });

  test("returns null off Linux without reading the executable", () => {
    expect(
      detectLibc("darwin", () => {
        throw new Error("must not read");
      }),
    ).toBeNull();
  });

  test.if(process.platform === "linux")("detects this Bun process's libc", () => {
    const libc = detectLibc();
    expect(libc === "glibc" || libc === "musl").toBe(true);
  });
});

describe("installed library resolution", () => {
  const host = { platform: "linux", arch: "arm64", libc: "glibc" } as const;
  const readManifest = (version: string) => () =>
    JSON.stringify({ name: "@nowledge/hawdb-ffi-linux-arm64-gnu", version });

  test("joins the platform package directory and its library name", () => {
    const requested: string[] = [];
    const path = resolvePrebuiltLibrary({
      host,
      resolve: (specifier) => {
        requested.push(specifier);
        return "/app/node_modules/@nowledge/hawdb-ffi-linux-arm64-gnu/package.json";
      },
      exists: () => true,
      readManifest: readManifest("0.30.0"),
      expectedVersion: "0.30.0",
    });
    expect(requested).toEqual(["@nowledge/hawdb-ffi-linux-arm64-gnu/package.json"]);
    expect(path).toBe(join("/app/node_modules/@nowledge/hawdb-ffi-linux-arm64-gnu", "libhawdb_ffi.so"));
  });

  test("names the missing package and the explicit-path alternative", () => {
    expect(() =>
      resolvePrebuiltLibrary({
        host,
        resolve: () => {
          throw new Error("Cannot find module");
        },
      }),
    ).toThrow(/@nowledge\/hawdb-ffi-linux-arm64-gnu is not installed.*loadHawDB\(path\)/);
  });

  test("refuses a platform package without its library", () => {
    expect(() =>
      resolvePrebuiltLibrary({
        host,
        resolve: () => "/app/node_modules/@nowledge/hawdb-ffi-linux-arm64-gnu/package.json",
        exists: () => false,
        readManifest: readManifest("0.30.0"),
        expectedVersion: "0.30.0",
      }),
    ).toThrow(/has no libhawdb_ffi\.so/);
  });

  test("refuses a resolved platform package from another release", () => {
    // The optionalDependencies pin does not constrain resolution: an older
    // package left in the tree resolves and must not be loaded.
    for (const read of [readManifest("0.29.0"), readManifest("0.30.1"), () => "{}", () => "not json"]) {
      expect(() =>
        resolvePrebuiltLibrary({
          host,
          resolve: () => "/app/node_modules/@nowledge/hawdb-ffi-linux-arm64-gnu/package.json",
          exists: () => {
            throw new Error("must not check the library of a mismatched package");
          },
          readManifest: read,
          expectedVersion: "0.30.0",
        }),
      ).toThrow(/hawdb-ffi-linux-arm64-gnu is .*but @nowledge\/hawdb-bun is 0\.30\.0.*loadHawDB\(path\)/);
    }
  });

  test("expects this package's own version by default", () => {
    const version = JSON.parse(readFileSync(join(import.meta.dir, "../package.json"), "utf8")).version;
    expect(() =>
      resolvePrebuiltLibrary({
        host,
        resolve: () => "/app/node_modules/@nowledge/hawdb-ffi-linux-arm64-gnu/package.json",
        exists: () => true,
        readManifest: readManifest("0.0.1"),
      }),
    ).toThrow(`@nowledge/hawdb-bun is ${version}`);
  });

  test("refuses an unsupported host before resolving anything", () => {
    expect(() =>
      resolvePrebuiltLibrary({
        host: { platform: "linux", arch: "x64", libc: "musl" },
        resolve: () => {
          throw new Error("must not resolve");
        },
      }),
    ).toThrow(UnsupportedPlatformError);
  });
});
