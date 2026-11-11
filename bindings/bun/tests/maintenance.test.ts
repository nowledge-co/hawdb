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
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";
import { type Database, HawDBError, loadHawDB } from "../src/index.ts";

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

function workspace(): string {
  const directory = mkdtempSync(join(tmpdir(), "hawdb-bun-maintenance-"));
  directories.push(directory);
  return directory;
}

function seeded(path: string): Database {
  const database = library.open(path);
  database.querySQL("CREATE TABLE events (id BIGINT PRIMARY KEY, body TEXT)");
  database.exec("CREATE (:Memory {id: 1, title: 'Scrub me'})");
  for (const id of [1, 2, 3]) {
    database.querySQL("INSERT INTO events (id, body) VALUES ($1, $2)", [id, `event ${id}`]);
  }
  return database;
}

function ids(database: Database) {
  return database.querySQL("SELECT id FROM events ORDER BY id").rows;
}

function files(root: string): string[] {
  return readdirSync(root).flatMap((name) => {
    const path = join(root, name);
    return statSync(path).isDirectory() ? files(path) : [path];
  });
}

// The canonical checkpoint file a handle reads: beside the storage manifest in
// a backup, under branches/<branch>/data/ in an opened project.
function canonicalCheckpoint(root: string): string {
  const all = files(root);
  const generation = all
    .filter((path) => basename(path) === "manifest.hawdb")
    .map((path) => /^checkpoint_generation\t(\d+)$/m.exec(readFileSync(path, "utf8"))?.[1])
    .find((value) => value !== undefined);
  expect(generation).toBeDefined();
  const candidates = all.filter((path) => basename(path) === `canonical.${generation}.hawdb`);
  return candidates.find((path) => basename(dirname(path)) === "data") ?? candidates[0];
}

function flipMiddleByte(path: string): void {
  const bytes = readFileSync(path);
  bytes[bytes.length >> 1] ^= 0xff;
  writeFileSync(path, bytes);
}

function failure(action: () => unknown): HawDBError {
  try {
    action();
  } catch (error) {
    expect(error).toBeInstanceOf(HawDBError);
    return error as HawDBError;
  }
  throw new Error("expected a HawDBError");
}

describe("Bun backup, restore and scrub", () => {
  test("backs up while open, restores into a fresh directory and reopens", () => {
    const root = workspace();
    const database = seeded(join(root, "database"));
    const backup = database.backup(join(root, "backup"));
    expect(Number(backup.file_count)).toBeGreaterThan(0);
    expect(Number(backup.total_bytes)).toBeGreaterThan(0);
    database.querySQL("INSERT INTO events (id, body) VALUES (4, 'after backup')");
    expect(ids(database)).toEqual([[1], [2], [3], [4]]);
    expect(Number(database.scrub().checked_file_count)).toBeGreaterThan(0);

    const restored = library.restore(join(root, "backup"), join(root, "restored"));
    expect(restored).toEqual(backup);
    const copy = library.open(join(root, "restored"));
    expect(ids(copy)).toEqual([[1], [2], [3]]);
    expect(copy.query("MATCH (m:Memory) RETURN m.title AS title").rows).toEqual([["Scrub me"]]);
    const healthy = copy.scrub();
    expect(healthy.generation).toEqual(backup.generation);
    expect(Number(healthy.checked_bytes)).toBeGreaterThan(0);
    copy.close();
    database.close();
  }, 60_000);

  test("scrub reports a damaged copy and that handle refuses statements", () => {
    const root = workspace();
    const database = seeded(join(root, "database"));
    database.backup(join(root, "backup"));
    database.close();
    library.restore(join(root, "backup"), join(root, "damaged"));

    const copy = library.open(join(root, "damaged"));
    copy.scrub();
    flipMiddleByte(canonicalCheckpoint(join(root, "damaged")));
    const damage = failure(() => copy.scrub());
    expect(damage.message).toMatch(/(CRC32C|SHA-256) mismatch during scrub/);
    expect(failure(() => ids(copy)).message).toContain("close and reopen");
    copy.close();

    const original = library.open(join(root, "database"), { readOnly: true });
    expect(Number(original.scrub().checked_file_count)).toBeGreaterThan(0);
    expect(ids(original)).toEqual([[1], [2], [3]]);
    original.close();
  }, 60_000);

  test("refuses damaged backups, existing destinations and unsupported handles", () => {
    const root = workspace();
    const database = seeded(join(root, "database"));
    mkdirSync(join(root, "occupied"));
    expect(failure(() => database.backup(join(root, "occupied"))).message).toContain("already exists");
    database.backup(join(root, "backup"));
    expect(failure(() => library.restore(join(root, "backup"), join(root, "occupied"))).message).toContain(
      "already exists",
    );
    database.close();

    flipMiddleByte(canonicalCheckpoint(join(root, "backup")));
    failure(() => library.restore(join(root, "backup"), join(root, "restored")));
    expect(existsSync(join(root, "restored"))).toBe(false);

    const memory = library.openInMemory();
    failure(() => memory.backup(join(root, "from-memory")));
    failure(() => memory.scrub());
    memory.close();
    expect(existsSync(join(root, "from-memory"))).toBe(false);
    expect(failure(() => memory.scrub()).kind).toBe("closed");
  }, 60_000);
});
