// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

import {
  copyFileSync,
  cpSync,
  existsSync,
  mkdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { PLATFORM_PACKAGES } from "../dist/platforms.js";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const source = JSON.parse(readFileSync(join(root, "package.json"), "utf8"));
const repository = {
  type: "git",
  url: "git+https://github.com/nowledge-co/hawdb.git",
  directory: "bindings/node",
};
const { values } = parseArgs({
  options: {
    out: { type: "string" },
    addon: { type: "string", multiple: true },
  },
});
if (values.out === undefined || !values.addon?.length)
  throw new Error("--out and --addon <platform>=<file> are required");
const out = resolve(values.out);
if (existsSync(out))
  throw new Error(`Assembly destination already exists: ${out}`);
const addons = new Map();
for (const value of values.addon) {
  const separator = value.indexOf("=");
  const id = value.slice(0, separator);
  const path = resolve(value.slice(separator + 1));
  if (
    separator < 1 ||
    !PLATFORM_PACKAGES.some((platform) => platform.id === id) ||
    addons.has(id) ||
    !existsSync(path)
  ) {
    throw new Error(`Invalid, duplicate or missing addon: ${value}`);
  }
  addons.set(id, path);
}
const publish = [];
function manifest(directory, value, kind) {
  copyFileSync(join(root, "../../LICENSE"), join(directory, "LICENSE"));
  writeFileSync(
    join(directory, "package.json"),
    `${JSON.stringify(
      {
        version: source.version,
        license: "Apache-2.0",
        repository,
        publishConfig: { access: "public" },
        ...value,
      },
      null,
      2,
    )}\n`,
  );
  publish.push({ name: value.name, version: source.version, directory, kind });
}
for (const platform of PLATFORM_PACKAGES.filter((platform) =>
  addons.has(platform.id),
)) {
  const directory = join(out, `hawdb-node-${platform.id}`);
  mkdirSync(directory, { recursive: true });
  copyFileSync(addons.get(platform.id), join(directory, "hawdb.node"));
  manifest(
    directory,
    {
      name: `@nowledge/hawdb-node-${platform.id}`,
      main: "hawdb.node",
      os: [platform.os],
      cpu: [platform.cpu],
      ...(platform.libc === undefined ? {} : { libc: [platform.libc] }),
      files: ["hawdb.node", "LICENSE"],
    },
    "platform",
  );
}
const directory = join(out, "hawdb-node");
mkdirSync(join(directory, "src"), { recursive: true });
cpSync(join(root, "dist"), join(directory, "dist"), { recursive: true });
// The root package must be portable; only optional platform packages own addons.
rmSync(join(directory, "dist/hawdb.node"), { force: true });
copyFileSync(join(root, "src/index.d.ts"), join(directory, "src/index.d.ts"));
copyFileSync(join(root, "README.md"), join(directory, "README.md"));
manifest(
  directory,
  {
    name: source.name,
    type: source.type,
    exports: source.exports,
    engines: source.engines,
    files: ["dist", "src/index.d.ts", "README.md", "LICENSE"],
    optionalDependencies: Object.fromEntries(
      publish.map((entry) => [entry.name, source.version]),
    ),
  },
  "main",
);
console.log(
  JSON.stringify(
    { version: source.version, publish, placeholders: [] },
    null,
    2,
  ),
);
