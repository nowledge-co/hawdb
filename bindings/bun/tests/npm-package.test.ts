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

// Pack the publishable npm packages from the test harness's built library,
// install the tarballs into a fresh node_modules tree and load HawDB there
// through the zero-argument platform-package path.

import { afterAll, expect, test } from "bun:test";
import { cpSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { assemble } from "../scripts/assemble-npm.ts";
import { currentHost, platformPackageName, selectPlatformPackage } from "../src/platform.ts";

const libraryPath = process.env.HAWDB_LIBRARY;
if (!libraryPath) {
  throw new Error("HAWDB_LIBRARY must name the Bazel-built HawDB C ABI library");
}
const version = JSON.parse(readFileSync(join(import.meta.dir, "../package.json"), "utf8")).version as string;
const platform = selectPlatformPackage(currentHost());
const root = mkdtempSync(join(tmpdir(), "hawdb-npm-"));

afterAll(() => rmSync(root, { recursive: true, force: true }));

function run(command: string[], cwd: string): string {
  const child = Bun.spawnSync(command, { cwd, stdout: "pipe", stderr: "pipe", timeout: 60_000 });
  const stderr = new TextDecoder().decode(child.stderr);
  if (child.exitCode !== 0) {
    throw new Error(`${command.join(" ")} failed (${child.exitCode}): ${stderr}`);
  }
  return new TextDecoder().decode(child.stdout);
}

function pack(directory: string, destination: string): string {
  const before = new Set(readdirSync(destination));
  run([process.execPath, "pm", "pack", "--destination", destination, "--quiet"], directory);
  const created = readdirSync(destination).filter((name) => !before.has(name));
  expect(created).toHaveLength(1);
  return join(destination, created[0]);
}

function listTarball(tarball: string): string[] {
  return run(["tar", "-tzf", tarball], root)
    .split("\n")
    .filter((line) => line.length > 0 && !line.endsWith("/"))
    .map((line) => line.replace(/^package\//, ""))
    .sort();
}

// Git for Windows puts a GNU tar that misreads drive-letter paths first on PATH,
// so Windows hosts install the assembled directories; the tarball contents are
// checked on the Unix hosts that run the same assembler.
const packTarballs = process.platform !== "win32";

function install(source: string, name: string, consumer: string): void {
  const directory = join(consumer, "node_modules", name);
  mkdirSync(directory, { recursive: true });
  if (packTarballs) {
    run(["tar", "-xzf", source, "--strip-components=1", "-C", directory], root);
  } else {
    cpSync(source, directory, { recursive: true });
  }
}

test("packed packages load HawDB without an explicit library path", () => {
  const summary = assemble({
    version,
    out: join(root, "packages"),
    libraries: new Map([[platform.id, libraryPath]]),
  });
  expect(summary.placeholders).toEqual([]);
  expect(summary.publish.map((entry) => entry.kind)).toEqual(["platform", "main"]);

  const [platformPackage, mainPackage] = summary.publish;
  const mainManifest = JSON.parse(readFileSync(join(mainPackage.directory, "package.json"), "utf8"));
  expect(mainManifest.private).toBeUndefined();
  expect(mainManifest.devDependencies).toBeUndefined();
  expect(mainManifest.optionalDependencies).toEqual({ [platformPackageName(platform)]: version });
  const platformManifest = JSON.parse(readFileSync(join(platformPackage.directory, "package.json"), "utf8"));
  expect(platformManifest.os).toEqual([platform.os]);
  expect(platformManifest.cpu).toEqual([platform.cpu]);
  expect(platformManifest.libc).toEqual(platform.libc === undefined ? undefined : [platform.libc]);

  let platformSource = platformPackage.directory;
  let mainSource = mainPackage.directory;
  if (packTarballs) {
    const tarballs = join(root, "tarballs");
    mkdirSync(tarballs);
    platformSource = pack(platformPackage.directory, tarballs);
    mainSource = pack(mainPackage.directory, tarballs);
    expect(listTarball(platformSource)).toEqual(
      ["LICENSE", "README.md", "hawdb_ffi.h", platform.library, "package.json"].sort(),
    );
    expect(listTarball(mainSource)).toEqual(
      ["LICENSE", "README.md", "package.json", "src/index.ts", "src/json.ts", "src/native.ts", "src/platform.ts"].sort(),
    );
  }

  const consumer = join(root, "consumer");
  mkdirSync(consumer);
  writeFileSync(join(consumer, "package.json"), JSON.stringify({ name: "consumer", private: true, type: "module" }));
  install(platformSource, platformPackage.name, consumer);
  install(mainSource, mainPackage.name, consumer);
  writeFileSync(
    join(consumer, "check.ts"),
    [
      'import { loadHawDB } from "@nowledge/hawdb-bun";',
      "const hawdb = loadHawDB();",
      "const database = hawdb.openInMemory();",
      'const rows = database.query("UNWIND [41] AS x RETURN x + 1 AS answer").dicts();',
      "database.close();",
      "const version = hawdb.version();",
      "hawdb.close();",
      "console.log(JSON.stringify({ version, rows }));",
    ].join("\n"),
  );
  const output = JSON.parse(run([process.execPath, "check.ts"], consumer));
  expect(output.version).toBe(`hawdb ${version}`);
  expect(output.rows).toEqual([{ answer: 42 }]);
});

test("refuses an older platform package resolved in place of the pinned one", () => {
  // Reproduces installing the main package with optional dependencies omitted
  // beside a directly installed platform package from an earlier release:
  // module resolution finds it although optionalDependencies pins `version`.
  const older = "0.29.0";
  expect(version).not.toBe(older);
  const summary = assemble({
    version,
    out: join(root, "mismatched"),
    libraries: new Map([[platform.id, libraryPath]]),
  });
  const [platformPackage, mainPackage] = summary.publish;
  const manifestPath = join(platformPackage.directory, "package.json");
  const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
  writeFileSync(manifestPath, `${JSON.stringify({ ...manifest, version: older }, null, 2)}\n`);

  const consumer = join(root, "mismatched-consumer");
  mkdirSync(consumer);
  writeFileSync(join(consumer, "package.json"), JSON.stringify({ name: "consumer", private: true, type: "module" }));
  cpSync(platformPackage.directory, join(consumer, "node_modules", platformPackage.name), { recursive: true });
  cpSync(mainPackage.directory, join(consumer, "node_modules", mainPackage.name), { recursive: true });
  writeFileSync(
    join(consumer, "check.ts"),
    [
      'import { loadHawDB, resolvePrebuiltLibrary } from "@nowledge/hawdb-bun";',
      "for (const attempt of [() => resolvePrebuiltLibrary(), () => loadHawDB()]) {",
      "  try {",
      "    attempt();",
      '    console.log("LOADED");',
      "  } catch (error) {",
      "    console.log(String((error as Error).message));",
      "  }",
      "}",
    ].join("\n"),
  );
  const child = Bun.spawnSync([process.execPath, "check.ts"], {
    cwd: consumer,
    stdout: "pipe",
    stderr: "pipe",
    timeout: 60_000,
  });
  expect(new TextDecoder().decode(child.stderr)).toBe("");
  const lines = new TextDecoder().decode(child.stdout).trim().split("\n");
  expect(lines).toHaveLength(2);
  for (const line of lines) {
    expect(line).toContain(
      `${platformPackageName(platform)} is version ${older}, but @nowledge/hawdb-bun is ${version}`,
    );
  }
});
