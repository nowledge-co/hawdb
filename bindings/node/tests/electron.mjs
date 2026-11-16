// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

import assert from "node:assert/strict";
import { mkdtempSync, rmSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFileSync } from "node:child_process";
import electron from "electron";
import { packager } from "@electron/packager";
import { statFile } from "@electron/asar";
import { installConsumer } from "./consumer.mjs";

const directory = mkdtempSync(join(tmpdir(), "hawdb-node-electron-"));
try {
  const consumer = installConsumer(directory);
  const env = { ...process.env };
  delete env.ELECTRON_RUN_AS_NODE;
  function run(binary, args) {
    const output = execFileSync(
      binary,
      [...(process.platform === "linux" ? ["--no-sandbox"] : []), ...args],
      {
        env,
        encoding: "utf8",
        timeout: 120000,
      },
    );
    console.log(output);
    assert.match(output, /HAWDB_ELECTRON_OK/);
    return output;
  }
  assert.match(run(electron, [consumer]), /"packaged":false/);
  const [application] = await packager({
    dir: consumer,
    out: join(directory, "packaged"),
    name: "HawDBNodeSmoke",
    electronVersion: JSON.parse(
      readFileSync(
        new URL("../node_modules/electron/package.json", import.meta.url),
      ),
    ).version,
    asar: { unpack: "**/*.node" },
  });
  const resources =
    process.platform === "darwin"
      ? join(application, "HawDBNodeSmoke.app/Contents/Resources")
      : join(application, "resources");
  const installed = JSON.parse(
    readFileSync(
      join(consumer, "node_modules/@nowledge/hawdb-node/package.json"),
    ),
  );
  const [platformPackage] = Object.keys(installed.optionalDependencies);
  assert.equal(
    statFile(
      join(resources, "app.asar"),
      `node_modules/${platformPackage}/hawdb.node`,
    ).unpacked,
    true,
  );
  const binary =
    process.platform === "darwin"
      ? join(application, "HawDBNodeSmoke.app/Contents/MacOS/HawDBNodeSmoke")
      : join(
          application,
          process.platform === "win32"
            ? "HawDBNodeSmoke.exe"
            : "HawDBNodeSmoke",
        );
  assert.match(run(binary, []), /"packaged":true/);
  rmSync(directory, { recursive: true, force: true });
} catch (error) {
  console.error(`Preserved packaged Electron fixture: ${directory}`);
  throw error;
}
