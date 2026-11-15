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

import { afterAll, afterEach, describe, expect, test } from "bun:test";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type Database, HawDBError, loadHawDB, type OpenOptions } from "../src/index.ts";

const libraryPath = process.env.HAWDB_LIBRARY;
if (!libraryPath) {
  throw new Error("HAWDB_LIBRARY must name the Bazel-built HawDB C ABI library");
}
const library = loadHawDB(libraryPath);
afterAll(() => library.close());

const directories: string[] = [];
afterEach(() => {
  for (const directory of directories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

function databasePath(): string {
  const directory = mkdtempSync(join(tmpdir(), "hawdb-bun-options-"));
  directories.push(directory);
  return join(directory, "database");
}

function durability(database: Database): unknown {
  return database.querySQL("SHOW CURRENT BRANCH").dicts()[0].durability;
}

function nativeError(action: () => unknown): HawDBError {
  try {
    action();
  } catch (error) {
    expect(error).toBeInstanceOf(HawDBError);
    return error as HawDBError;
  }
  throw new Error("expected a HawDBError");
}

describe("Bun open options", () => {
  test("defaults stay power-loss safe and durability is selected per open", () => {
    const path = databasePath();
    const relaxed = library.open(path, { durability: "sync_on_checkpoint" });
    expect(durability(relaxed)).toBe("sync_on_checkpoint");
    relaxed.close();

    const defaults = library.open(path);
    expect(durability(defaults)).toBe("sync_on_every_write");
    defaults.close();

    const undefinedKeys = library.open(path, { durability: undefined, readOnly: undefined });
    expect(durability(undefinedKeys)).toBe("sync_on_every_write");
    undefinedKeys.close();
  }, 30_000);

  test("read row and payload budgets refuse instead of truncating", () => {
    const path = databasePath();
    const writer = library.open(path);
    writer.querySQL("CREATE TABLE events (id BIGINT PRIMARY KEY, body TEXT)");
    for (const id of [1, 2, 3]) {
      writer.querySQL("INSERT INTO events (id, body) VALUES ($1, $2)", [id, "x".repeat(4096)]);
    }
    writer.close();

    const rows = library.open(path, { maxReadResultRows: 2 });
    expect(rows.querySQL("SELECT id FROM events WHERE id <= 2 ORDER BY id").rows).toEqual([[1], [2]]);
    const rowError = nativeError(() => rows.querySQL("SELECT id FROM events ORDER BY id"));
    expect(rowError.kind).toBe("resource_budget_exceeded");
    rows.close();

    const payload = library.open(path, { maxReadResultPayloadBytes: 1024n });
    expect(payload.querySQL("SELECT id FROM events ORDER BY id").rows).toEqual([[1], [2], [3]]);
    const payloadError = nativeError(() => payload.querySQL("SELECT body FROM events ORDER BY id"));
    expect(payloadError.kind).toBe("read_budget_exceeded");
    payload.close();

    const unlimited = library.open(path, { maxReadResultRows: null });
    expect(unlimited.querySQL("SELECT id FROM events ORDER BY id").rows).toHaveLength(3);
    unlimited.close();
  }, 30_000);

  test("the monotonic append fast path keeps insert semantics across reopen", () => {
    const path = databasePath();
    const writer = library.open(path, {
      durability: "sync_on_checkpoint",
      relationalMonotonicAppendFastPath: true,
    });
    writer.querySQL("CREATE TABLE log (id BIGINT PRIMARY KEY, body TEXT)");
    for (let id = 1; id <= 64; id += 1) {
      writer.querySQL("INSERT INTO log (id, body) VALUES ($1, $2)", [id, `event ${id}`]);
    }
    writer.querySQL("INSERT INTO log (id, body) VALUES (0, 'early')");
    expect(() => writer.querySQL("INSERT INTO log (id, body) VALUES (7, 'again')")).toThrow(HawDBError);
    writer.close();

    const reader = library.open(path, { readOnly: true });
    expect(reader.querySQL("SELECT COUNT(*) AS n, MIN(id) AS lo, MAX(id) AS hi FROM log").rows).toEqual([
      [65, 0, 64],
    ]);
    reader.close();
  }, 30_000);

  test("unknown keys and invalid values are rejected before the database is created", () => {
    const path = databasePath();
    for (const options of [
      { monotonicAppendFastPath: true },
      { read_only: true },
      { groupCommit: true },
    ]) {
      const error = nativeError(() => library.open(path, options as OpenOptions));
      expect(error.kind).toBe("invalid_argument");
      expect(error.message).toContain("unknown HawDB open option");
    }
    for (const [options, needle] of [
      [{ durability: "relaxed" }, '"durability" must be'],
      [{ readOnly: "yes" }, '"read_only" must be a bool'],
      [{ maxReadResultRows: -1 }, '"max_read_result_rows" must be'],
      [{ maxReadResultPayloadBytes: 0 }, '"max_read_result_payload_bytes" must be a positive integer'],
      [{ relationalMonotonicAppendFastPath: 1 }, '"relational_monotonic_append_fast_path" must be a bool'],
    ] as const) {
      const error = nativeError(() => library.open(path, options as unknown as OpenOptions));
      expect(error.kind).toBe("invalid_argument");
      expect(error.message).toContain(needle);
    }
    expect(existsSync(path)).toBe(false);
  });
});
