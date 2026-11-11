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

import { existsSync, readdirSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";

/** npm scope shared by the Bun package and its prebuilt C ABI packages. */
export const NPM_SCOPE = "@nowledge";

export type Libc = "glibc" | "musl";

/** One published prebuilt C ABI package and the hosts it serves. */
export interface PlatformPackage {
  /** Suffix of `@nowledge/hawdb-ffi-<id>`. */
  readonly id: string;
  /** `process.platform` and the npm `os` field. */
  readonly os: NodeJS.Platform;
  /** `process.arch` and the npm `cpu` field. */
  readonly cpu: NodeJS.Architecture;
  /** npm `libc` field; Linux only. */
  readonly libc?: Libc;
  /** Release archive target name. */
  readonly target: string;
  /** Shared-library file name inside the package. */
  readonly library: string;
}

// Keep this table, the release workflow matrix and docs/PREBUILT_LIBRARIES.md
// in the same order. Adding a row publishes a new npm package name, whose
// trusted publisher must be configured before its first CI publish.
export const PLATFORM_PACKAGES: readonly PlatformPackage[] = [
  {
    id: "darwin-arm64",
    os: "darwin",
    cpu: "arm64",
    target: "aarch64-apple-darwin",
    library: "libhawdb_ffi.dylib",
  },
  {
    id: "darwin-x64",
    os: "darwin",
    cpu: "x64",
    target: "x86_64-apple-darwin",
    library: "libhawdb_ffi.dylib",
  },
  {
    id: "linux-x64-gnu",
    os: "linux",
    cpu: "x64",
    libc: "glibc",
    target: "x86_64-unknown-linux-gnu",
    library: "libhawdb_ffi.so",
  },
  {
    id: "linux-arm64-gnu",
    os: "linux",
    cpu: "arm64",
    libc: "glibc",
    target: "aarch64-unknown-linux-gnu",
    library: "libhawdb_ffi.so",
  },
  {
    id: "win32-x64-msvc",
    os: "win32",
    cpu: "x64",
    target: "x86_64-pc-windows-msvc",
    library: "hawdb_ffi.dll",
  },
];

export interface Host {
  readonly platform: string;
  readonly arch: string;
  /** Linux C library; ignored on other platforms. */
  readonly libc?: Libc | null;
}

export function platformPackageName(platform: PlatformPackage): string {
  return `${NPM_SCOPE}/hawdb-ffi-${platform.id}`;
}

export class UnsupportedPlatformError extends Error {
  constructor(public readonly host: Host) {
    const libc = host.platform === "linux" ? ` (${host.libc ?? "unknown libc"})` : "";
    super(
      `HawDB has no prebuilt library for ${host.platform}-${host.arch}${libc}. ` +
        "Build //bindings/ffi:hawdb_ffi from source and pass its path to loadHawDB(path).",
    );
    this.name = "UnsupportedPlatformError";
  }
}

/** Select the prebuilt package for a host, or throw UnsupportedPlatformError. */
export function selectPlatformPackage(host: Host): PlatformPackage {
  const selected = PLATFORM_PACKAGES.find(
    (candidate) =>
      candidate.os === host.platform &&
      candidate.cpu === host.arch &&
      (candidate.libc === undefined || candidate.libc === host.libc),
  );
  if (selected === undefined) {
    throw new UnsupportedPlatformError(host);
  }
  return selected;
}

/**
 * Detect the Linux C library from the dynamic loader on disk. musl installs
 * `ld-musl-<arch>.so.1`; glibc never does. Returns null off Linux.
 */
export function detectLibc(
  platform: string = process.platform,
  listLoaderDirectory: (directory: string) => readonly string[] = listDirectory,
): Libc | null {
  if (platform !== "linux") {
    return null;
  }
  for (const directory of ["/lib", "/usr/lib"]) {
    if (listLoaderDirectory(directory).some((name) => name.startsWith("ld-musl-"))) {
      return "musl";
    }
  }
  return "glibc";
}

function listDirectory(directory: string): readonly string[] {
  try {
    return readdirSync(directory);
  } catch {
    return [];
  }
}

export function currentHost(): Host {
  return { platform: process.platform, arch: process.arch, libc: detectLibc() };
}

export interface ResolveOptions {
  readonly host?: Host;
  /** Module resolution, defaulting to Node resolution from this package. */
  readonly resolve?: (specifier: string) => string;
  readonly exists?: (path: string) => boolean;
}

const requireFromHere = createRequire(import.meta.url);

/**
 * Locate the shared library shipped by the installed platform package that
 * matches this host. Only package resolution decides the path; no environment
 * variable or search path is consulted.
 */
export function resolvePrebuiltLibrary(options: ResolveOptions = {}): string {
  const host = options.host ?? currentHost();
  const resolve = options.resolve ?? ((specifier: string) => requireFromHere.resolve(specifier));
  const exists = options.exists ?? existsSync;
  const platform = selectPlatformPackage(host);
  const name = platformPackageName(platform);
  let manifest: string;
  try {
    manifest = resolve(`${name}/package.json`);
  } catch {
    throw new Error(
      `HawDB's prebuilt library package ${name} is not installed. ` +
        "Reinstall @nowledge/hawdb-bun without disabling optional dependencies, " +
        "or pass an explicit library path to loadHawDB(path).",
    );
  }
  const library = join(dirname(manifest), platform.library);
  if (!exists(library)) {
    throw new Error(`HawDB's prebuilt library package ${name} has no ${platform.library}`);
  }
  return library;
}
