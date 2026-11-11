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
  detectLibc,
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

describe("Linux C library detection", () => {
  test("recognizes a musl loader in either directory", () => {
    expect(detectLibc("linux", (directory) => (directory === "/lib" ? ["ld-musl-x86_64.so.1"] : []))).toBe("musl");
    expect(detectLibc("linux", (directory) => (directory === "/usr/lib" ? ["ld-musl-aarch64.so.1"] : []))).toBe(
      "musl",
    );
  });

  test("treats a loader directory without musl as glibc", () => {
    expect(detectLibc("linux", () => ["ld-linux-x86-64.so.2", "libc.so.6"])).toBe("glibc");
    expect(detectLibc("linux", () => [])).toBe("glibc");
  });

  test("returns null off Linux without reading the filesystem", () => {
    expect(
      detectLibc("darwin", () => {
        throw new Error("must not list");
      }),
    ).toBeNull();
  });
});

describe("installed library resolution", () => {
  const host = { platform: "linux", arch: "arm64", libc: "glibc" } as const;

  test("joins the platform package directory and its library name", () => {
    const requested: string[] = [];
    const path = resolvePrebuiltLibrary({
      host,
      resolve: (specifier) => {
        requested.push(specifier);
        return "/app/node_modules/@nowledge/hawdb-ffi-linux-arm64-gnu/package.json";
      },
      exists: () => true,
    });
    expect(requested).toEqual(["@nowledge/hawdb-ffi-linux-arm64-gnu/package.json"]);
    expect(path).toBe("/app/node_modules/@nowledge/hawdb-ffi-linux-arm64-gnu/libhawdb_ffi.so");
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
      }),
    ).toThrow(/has no libhawdb_ffi\.so/);
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
