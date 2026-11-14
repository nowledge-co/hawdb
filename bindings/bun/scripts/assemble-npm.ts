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

// Assemble the publishable npm packages from built C ABI libraries:
//
//   bun scripts/assemble-npm.ts --version 0.33.0 --out ../../dist/npm \
//     --library darwin-arm64=/path/to/libhawdb_ffi.dylib [--library <id>=<path> ...] \
//     [--placeholders 0.0.0-placeholder.0]
//
// Writes one directory per platform package plus `hawdb-bun/` and prints a JSON
// summary whose `publish` list is the required publish order (platform packages
// before the main package that pins them). The main package's
// optionalDependencies name exactly the platform packages assembled with a
// library, at the same version. `--placeholders` additionally assembles
// library-free packages for every other platform at the given prerelease
// version, only to reserve their names on npm before trusted publishing.

import { copyFileSync, cpSync, existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { PLATFORM_PACKAGES, platformPackageName, type PlatformPackage } from "../src/platform.ts";

const bindingRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const repositoryRoot = resolve(bindingRoot, "../..");
const repository = {
  type: "git",
  url: "git+https://github.com/nowledge-co/hawdb.git",
};

export interface AssembleOptions {
  readonly version: string;
  readonly out: string;
  /** Platform id to built shared-library path. */
  readonly libraries: ReadonlyMap<string, string>;
  /** Prerelease version for name-reservation packages; omitted to assemble none. */
  readonly placeholderVersion?: string;
}

export interface AssembledPackage {
  readonly name: string;
  readonly version: string;
  readonly directory: string;
  readonly kind: "platform" | "placeholder" | "main";
}

export interface AssembleSummary {
  readonly version: string;
  /** Publish order: platform packages, then the main package. */
  readonly publish: readonly AssembledPackage[];
  /** Name-reservation packages; publish only during the one-time bootstrap. */
  readonly placeholders: readonly AssembledPackage[];
}

function writeJson(path: string, value: unknown): void {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
}

function platformManifest(platform: PlatformPackage, version: string, placeholder: boolean) {
  const files = placeholder ? ["README.md", "LICENSE"] : [platform.library, "hawdb_ffi.h", "README.md", "LICENSE"];
  return {
    name: platformPackageName(platform),
    version,
    description: placeholder
      ? `Reserved name for the HawDB C ABI library on ${platform.target}`
      : `Prebuilt HawDB C ABI shared library for ${platform.target}`,
    license: "Apache-2.0",
    repository: { ...repository, directory: "bindings/ffi" },
    homepage: "https://github.com/nowledge-co/hawdb/blob/main/docs/PREBUILT_LIBRARIES.md",
    os: [platform.os],
    cpu: [platform.cpu],
    ...(platform.libc === undefined ? {} : { libc: [platform.libc] }),
    files,
    publishConfig: { access: "public" },
  };
}

function platformReadme(platform: PlatformPackage, version: string, placeholder: boolean): string {
  const name = platformPackageName(platform);
  if (placeholder) {
    return `# ${name}\n\nThis version only reserves the package name. It contains no library.\n`;
  }
  return [
    `# ${name}`,
    "",
    `The HawDB ${version} C ABI shared library (\`${platform.library}\`) and its`,
    `header for \`${platform.target}\`. Install \`@nowledge/hawdb-bun\` instead of`,
    "depending on this package directly; it selects the matching platform package.",
    "",
    "See https://github.com/nowledge-co/hawdb/blob/main/docs/PREBUILT_LIBRARIES.md.",
    "",
  ].join("\n");
}

function assemblePlatform(
  platform: PlatformPackage,
  version: string,
  out: string,
  library: string | undefined,
): AssembledPackage {
  const placeholder = library === undefined;
  const directory = join(out, basename(platformPackageName(platform)));
  rmSync(directory, { recursive: true, force: true });
  mkdirSync(directory, { recursive: true });
  if (library !== undefined) {
    copyFileSync(library, join(directory, platform.library));
    copyFileSync(join(repositoryRoot, "bindings/ffi/include/hawdb_ffi.h"), join(directory, "hawdb_ffi.h"));
  }
  copyFileSync(join(repositoryRoot, "LICENSE"), join(directory, "LICENSE"));
  writeFileSync(join(directory, "README.md"), platformReadme(platform, version, placeholder));
  writeJson(join(directory, "package.json"), platformManifest(platform, version, placeholder));
  return { name: platformPackageName(platform), version, directory, kind: placeholder ? "placeholder" : "platform" };
}

function assembleMain(version: string, out: string, platforms: readonly PlatformPackage[]): AssembledPackage {
  const source = JSON.parse(readFileSync(join(bindingRoot, "package.json"), "utf8"));
  const directory = join(out, "hawdb-bun");
  rmSync(directory, { recursive: true, force: true });
  mkdirSync(directory, { recursive: true });
  cpSync(join(bindingRoot, "src"), join(directory, "src"), { recursive: true });
  copyFileSync(join(bindingRoot, "README.md"), join(directory, "README.md"));
  copyFileSync(join(repositoryRoot, "LICENSE"), join(directory, "LICENSE"));
  writeJson(join(directory, "package.json"), {
    name: source.name,
    version,
    description: "Embed the HawDB database in Bun through its C ABI",
    license: "Apache-2.0",
    repository: { ...repository, directory: "bindings/bun" },
    homepage: "https://github.com/nowledge-co/hawdb/tree/main/bindings/bun#readme",
    type: source.type,
    exports: source.exports,
    engines: source.engines,
    files: ["src", "README.md", "LICENSE"],
    optionalDependencies: Object.fromEntries(platforms.map((platform) => [platformPackageName(platform), version])),
    publishConfig: { access: "public" },
  });
  return { name: source.name, version, directory, kind: "main" };
}

export function assemble(options: AssembleOptions): AssembleSummary {
  const source = JSON.parse(readFileSync(join(bindingRoot, "package.json"), "utf8"));
  if (source.version !== options.version) {
    throw new Error(`bindings/bun/package.json is ${source.version}, not the requested ${options.version}`);
  }
  if (options.libraries.size === 0) {
    throw new Error("at least one --library <platform>=<path> is required");
  }
  for (const [id, library] of options.libraries) {
    if (!PLATFORM_PACKAGES.some((platform) => platform.id === id)) {
      throw new Error(`unknown platform ${id}; expected one of ${PLATFORM_PACKAGES.map((p) => p.id).join(", ")}`);
    }
    if (!existsSync(library)) {
      throw new Error(`library for ${id} does not exist: ${library}`);
    }
  }
  if (options.placeholderVersion !== undefined && !options.placeholderVersion.includes("-")) {
    throw new Error("placeholder versions must be prereleases so they never become a release");
  }
  mkdirSync(options.out, { recursive: true });
  const built = PLATFORM_PACKAGES.filter((platform) => options.libraries.has(platform.id));
  const publish = built.map((platform) =>
    assemblePlatform(platform, options.version, options.out, options.libraries.get(platform.id)),
  );
  const placeholders =
    options.placeholderVersion === undefined
      ? []
      : PLATFORM_PACKAGES.filter((platform) => !options.libraries.has(platform.id)).map((platform) =>
          assemblePlatform(platform, options.placeholderVersion as string, options.out, undefined),
        );
  publish.push(assembleMain(options.version, options.out, built));
  return { version: options.version, publish, placeholders };
}

if (import.meta.main) {
  const { values } = parseArgs({
    options: {
      version: { type: "string" },
      out: { type: "string" },
      library: { type: "string", multiple: true },
      placeholders: { type: "string" },
    },
    strict: true,
  });
  if (values.version === undefined || values.out === undefined) {
    throw new Error("--version and --out are required");
  }
  const libraries = new Map<string, string>();
  for (const entry of values.library ?? []) {
    const separator = entry.indexOf("=");
    if (separator <= 0) {
      throw new Error(`--library expects <platform>=<path>, got ${entry}`);
    }
    const id = entry.slice(0, separator);
    if (libraries.has(id)) {
      throw new Error(`--library ${id} given twice`);
    }
    libraries.set(id, resolve(entry.slice(separator + 1)));
  }
  const summary = assemble({
    version: values.version,
    out: resolve(values.out),
    libraries,
    placeholderVersion: values.placeholders,
  });
  console.log(JSON.stringify(summary, null, 2));
}
