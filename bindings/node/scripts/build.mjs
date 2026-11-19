// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

import {
  copyFileSync,
  mkdirSync,
  readFileSync,
  renameSync,
  writeFileSync,
} from "node:fs";
import { resolve, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { execFileSync } from "node:child_process";
import { createRequire } from "node:module";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const { values } = parseArgs({ options: { native: { type: "string" } } });
const dist = join(root, "dist");
mkdirSync(dist, { recursive: true });
for (const file of ["index.js", "native.js"]) {
  copyFileSync(join(root, "src", file), join(dist, file));
}
// These modules contain no Bun runtime dependency. Keep one source of truth
// for lossless values and supported hosts across the two JavaScript bindings.
const require = createRequire(import.meta.url);
const compiler = join(
  dirname(require.resolve("typescript/package.json")),
  "bin/tsc",
);
execFileSync(
  process.execPath,
  [
    compiler,
    "--ignoreConfig",
    "--target",
    "es2022",
    "--module",
    "esnext",
    "--noEmitOnError",
    "--skipLibCheck",
    "--types",
    "node",
    "--typeRoots",
    join(root, "node_modules/@types"),
    "--outDir",
    dist,
    join(root, "../bun/src/json.ts"),
    join(root, "../bun/src/platform.ts"),
  ],
  { stdio: "inherit" },
);
renameSync(join(dist, "platform.js"), join(dist, "platforms.js"));
const { version } = JSON.parse(
  readFileSync(join(root, "package.json"), "utf8"),
);
writeFileSync(
  join(dist, "metadata.js"),
  `export const version = ${JSON.stringify(version)};\n`,
);
if (values.native !== undefined) {
  copyFileSync(resolve(values.native), join(dist, "hawdb.node"));
}
