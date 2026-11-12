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

import { closeSync, existsSync, openSync, readFileSync, readSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join, posix } from "node:path";

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

/** Reads `length` bytes at `offset`; may return fewer at end of file. */
export type ReadAt = (offset: number, length: number) => Uint8Array;

const PT_INTERP = 3;
const MAX_PROGRAM_HEADERS = 4096;
const MAX_INTERPRETER_BYTES = 4096;

/**
 * The `PT_INTERP` program interpreter of a little-endian 64-bit ELF image, or
 * null when the image is not one or names no interpreter (a static binary).
 */
export function elfInterpreter(read: ReadAt): string | null {
  const header = read(0, 64);
  if (
    header.length < 64 ||
    header[0] !== 0x7f ||
    header[1] !== 0x45 ||
    header[2] !== 0x4c ||
    header[3] !== 0x46 ||
    header[4] !== 2 || // ELFCLASS64
    header[5] !== 1 // ELFDATA2LSB
  ) {
    return null;
  }
  const fields = new DataView(header.buffer, header.byteOffset, header.byteLength);
  const tableOffset = Number(fields.getBigUint64(0x20, true));
  const entrySize = fields.getUint16(0x36, true);
  const entries = fields.getUint16(0x38, true);
  if (entrySize < 56 || entries === 0 || entries > MAX_PROGRAM_HEADERS) {
    return null;
  }
  const table = read(tableOffset, entrySize * entries);
  if (table.length < entrySize * entries) {
    return null;
  }
  const programs = new DataView(table.buffer, table.byteOffset, table.byteLength);
  for (let entry = 0; entry < entries; entry += 1) {
    const base = entry * entrySize;
    if (programs.getUint32(base, true) !== PT_INTERP) {
      continue;
    }
    const offset = Number(programs.getBigUint64(base + 8, true));
    const size = Number(programs.getBigUint64(base + 32, true));
    if (size === 0 || size > MAX_INTERPRETER_BYTES) {
      return null;
    }
    const bytes = read(offset, size);
    if (bytes.length < size) {
      return null;
    }
    const end = bytes.indexOf(0);
    return new TextDecoder().decode(end === -1 ? bytes : bytes.subarray(0, end));
  }
  return null;
}

/** Classify a program interpreter path: musl's `ld-musl-*` or glibc's `ld-linux*`. */
export function libcFromInterpreter(interpreter: string | null): Libc | null {
  if (interpreter === null) {
    return null;
  }
  const name = posix.basename(interpreter);
  if (name.startsWith("ld-musl-")) {
    return "musl";
  }
  if (name.startsWith("ld-linux")) {
    return "glibc";
  }
  return null;
}

/**
 * Detect the C library of the running process from the program interpreter
 * of its own executable (`/proc/self/exe`). A musl loader installed beside
 * glibc does not change which one this process uses, so loader files on disk
 * are not consulted. Returns null off Linux, and on Linux when the process
 * libc cannot be determined.
 */
export function detectLibc(platform: string = process.platform, read?: ReadAt): Libc | null {
  if (platform !== "linux") {
    return null;
  }
  if (read !== undefined) {
    return libcFromInterpreter(elfInterpreter(read));
  }
  let descriptor: number;
  try {
    descriptor = openSync("/proc/self/exe", "r");
  } catch {
    return null;
  }
  try {
    return libcFromInterpreter(
      elfInterpreter((offset, length) => {
        const bytes = new Uint8Array(length);
        return bytes.subarray(0, readSync(descriptor, bytes, 0, length, offset));
      }),
    );
  } catch {
    return null;
  } finally {
    closeSync(descriptor);
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
  /** Reads a resolved package manifest as UTF-8. */
  readonly readManifest?: (path: string) => string;
  /** The release the platform package must match; defaults to this package's version. */
  readonly expectedVersion?: string;
}

const requireFromHere = createRequire(import.meta.url);

function wrapperVersion(): string {
  const manifest = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));
  if (typeof manifest.version !== "string") {
    throw new Error("@nowledge/hawdb-bun has no version in its package.json");
  }
  return manifest.version;
}

/**
 * Locate the shared library shipped by the installed platform package that
 * matches this host. Only package resolution decides the path; no environment
 * variable or search path is consulted. The resolved package must be from the
 * same release as this package: the exact pin in `optionalDependencies` does
 * not constrain resolution, so an older copy left in the tree is refused.
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
  const readManifest = options.readManifest ?? ((path: string) => readFileSync(path, "utf8"));
  const expected = options.expectedVersion ?? wrapperVersion();
  let installed: unknown;
  try {
    installed = JSON.parse(readManifest(manifest)).version;
  } catch {
    installed = undefined;
  }
  if (installed !== expected) {
    const found = typeof installed === "string" ? `version ${installed}` : "an unreadable version";
    throw new Error(
      `HawDB's prebuilt library package ${name} is ${found}, but @nowledge/hawdb-bun is ${expected}. ` +
        `Reinstall @nowledge/hawdb-bun with optional dependencies so ${name}@${expected} is installed, ` +
        "or pass an explicit library path to loadHawDB(path).",
    );
  }
  const library = join(dirname(manifest), platform.library);
  if (!exists(library)) {
    throw new Error(`HawDB's prebuilt library package ${name} has no ${platform.library}`);
  }
  return library;
}
