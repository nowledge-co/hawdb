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

import { existsSync, readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { detectLibc, selectPlatformPackage } from "./platforms.js";
import { version } from "./metadata.js";

const require = createRequire(import.meta.url);
let native;

export function getNative() {
  if (native !== undefined) return native;
  const local = fileURLToPath(new URL("./hawdb.node", import.meta.url));
  let addon;
  if (existsSync(local)) {
    // Local builds only: the package assembler removes this host-specific file.
    addon = local;
  } else {
    let platform;
    try {
      platform = selectPlatformPackage({
        platform: process.platform,
        arch: process.arch,
        libc: detectLibc(),
      });
    } catch (error) {
      throw new Error(
        `No HawDB Node-API addon for ${process.platform}-${process.arch}; build bindings/node from source on a supported host`,
        { cause: error },
      );
    }
    const name = `@nowledge/hawdb-node-${platform.id}`;
    let manifest;
    try {
      manifest = require.resolve(`${name}/package.json`);
    } catch (error) {
      throw new Error(
        `Missing ${name}@${version}; install optional dependencies for this host`,
        { cause: error },
      );
    }
    const installed = JSON.parse(readFileSync(manifest, "utf8"));
    if (installed.name !== name || installed.version !== version) {
      throw new Error(
        `Expected ${name}@${version}, found ${installed.name}@${installed.version}`,
      );
    }
    addon = join(dirname(manifest), "hawdb.node");
  }
  const loaded = require(addon);
  if (loaded.version() !== `hawdb ${version}`) {
    throw new Error(
      `HawDB addon version mismatch: expected hawdb ${version}, found ${loaded.version()}`,
    );
  }
  native = loaded;
  return native;
}
