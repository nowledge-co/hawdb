// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

import assert from "node:assert/strict";
import { copyFileSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

export function command(binary, args, cwd) {
  // npm.cmd cannot be execFile'd directly on Windows without a shell.
  // Run npm's JavaScript entrypoint with the current Node instead.
  if (binary === "npm") {
    const cli = process.env.npm_execpath;
    if (!cli)
      throw new Error(
        "Run this test through npm so npm_execpath identifies its CLI",
      );
    return execFileSync(process.execPath, [cli, ...args], {
      cwd,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
      timeout: 120000,
    });
  }
  return execFileSync(binary, args, { cwd, encoding: "utf8", timeout: 120000 });
}

export function installConsumer(directory) {
  const assembly = join(directory, "assembly");
  const platform =
    process.platform === "linux"
      ? `linux-${process.arch}-gnu`
      : process.platform === "darwin"
        ? `darwin-${process.arch}`
        : "win32-x64-msvc";
  const summary = JSON.parse(
    command(
      process.execPath,
      [
        join(root, "scripts/assemble.mjs"),
        "--out",
        assembly,
        "--addon",
        `${platform}=${join(root, "dist/hawdb.node")}`,
      ],
      root,
    ),
  );
  const tarballs = summary.publish.map((entry) => {
    const packed = JSON.parse(
      command("npm", ["pack", "--ignore-scripts", "--json"], entry.directory),
    );
    return join(entry.directory, packed[0].filename);
  });
  const consumer = join(directory, "consumer");
  mkdirSync(consumer);
  writeFileSync(
    join(consumer, "package.json"),
    JSON.stringify({
      name: "hawdb-node-consumer",
      version: "1.0.0",
      private: true,
      main: "main.cjs",
    }),
  );
  command(
    "npm",
    [
      "install",
      "--ignore-scripts",
      "--offline",
      "--no-audit",
      "--no-fund",
      ...tarballs,
    ],
    consumer,
  );
  const installed = JSON.parse(
    readFileSync(
      join(consumer, "node_modules/@nowledge/hawdb-node/package.json"),
    ),
  );
  assert.equal(installed.private, undefined);
  assert.equal(Object.keys(installed.optionalDependencies).length, 1);
  copyFileSync(
    join(root, "tests/electron-app.cjs"),
    join(consumer, "main.cjs"),
  );
  copyFileSync(
    join(root, "examples/electron/worker.cjs"),
    join(consumer, "worker.cjs"),
  );
  return consumer;
}
