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

import { afterAll, describe, expect, test } from "bun:test";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { HawDBError, loadHawDB } from "../src/index.ts";

const libraryPath = process.env.HAWDB_LIBRARY;
if (!libraryPath) {
  throw new Error("HAWDB_LIBRARY must name the Bazel-built HawDB C ABI library");
}
const library = loadHawDB(libraryPath);
const root = mkdtempSync(join(tmpdir(), "hawdb-bun-dump-"));

afterAll(() => {
  library.close();
  rmSync(root, { recursive: true, force: true });
});

function thrown(action: () => unknown): HawDBError {
  try {
    action();
  } catch (error) {
    expect(error).toBeInstanceOf(HawDBError);
    return error as HawDBError;
  }
  throw new Error("expected a HawDBError");
}

describe("logical dump", () => {
  test("reports the format versions", () => {
    expect(library.formatVersion()).toBeGreaterThanOrEqual(1);
    expect(library.dumpFormatVersion()).toBeGreaterThanOrEqual(1);
  });

  test("exports and imports across a fresh directory", () => {
    const source = library.open(join(root, "source"));
    source.exec("CREATE (n:Memory {id: 1, title: '日志 🚀'})");
    source.querySQL("CREATE TABLE t (id BIGINT PRIMARY KEY, body TEXT)");
    source.querySQL("INSERT INTO t (id, body) VALUES ($1, $2)", [9223372036854775807n, "最大"]);
    const dump = join(root, "dump");
    const exported = source.export(dump);
    expect(exported.dump_format_version).toBe(library.dumpFormatVersion());
    expect(exported.persistent_format_version).toBe(library.formatVersion());
    expect(exported.node_count).toBe(1);
    expect(exported.branch).toBe("main");
    expect(exported.manifest_sha256).toMatch(/^[0-9a-f]{64}$/);
    source.close();

    const target = join(root, "imported");
    const imported = library.import(dump, target);
    expect(imported.node_count).toBe(1);
    expect(imported.table_row_count).toBe(exported.table_row_count);
    expect(imported.manifest_sha256).toBe(exported.manifest_sha256);
    const database = library.open(target);
    expect(database.query("MATCH (n:Memory) RETURN n.title").rows).toEqual([["日志 🚀"]]);
    expect(database.querySQL("SELECT id, body FROM t").rows).toEqual([[9223372036854775807n, "最大"]]);

    expect(thrown(() => library.import(dump, target)).kind).toBe("storage");
    database.close();
    expect(thrown(() => database.export(join(root, "closed"))).kind).toBe("closed");
  });

  test("refuses a newer dump without publishing", () => {
    const source = library.openInMemory();
    const dump = join(root, "newer");
    source.export(dump);
    source.close();
    const manifest = join(dump, "manifest.json");
    writeFileSync(
      manifest,
      readFileSync(manifest, "utf8").replace('"dump_format_version": 1', '"dump_format_version": 999'),
    );
    const target = join(root, "newer-imported");
    expect(thrown(() => library.import(dump, target)).kind).toBe("unsupported_storage_format");
    expect(existsSync(target)).toBe(false);
  });

  test("revalidates the handle after converting the destination", () => {
    const database = library.openInMemory();
    const destination = {
      toString() {
        database.close();
        return join(root, "revalidated");
      },
    } as unknown as string;
    expect(thrown(() => database.export(destination)).kind).toBe("invalid_handle");
    expect(existsSync(join(root, "revalidated"))).toBe(false);
  });
});
