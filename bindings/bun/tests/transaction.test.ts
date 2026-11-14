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
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { type Database, HawDBError, loadHawDB, type Transaction } from "../src/index.ts";

const libraryPath = process.env.HAWDB_LIBRARY;
if (!libraryPath) {
  throw new Error("HAWDB_LIBRARY must name the Bazel-built HawDB C ABI library");
}
const library = loadHawDB(libraryPath);
afterAll(() => library.close());

const crashFixture = fileURLToPath(new URL("./fixtures/transaction-crash.ts", import.meta.url));
const directories: string[] = [];
afterEach(() => {
  for (const directory of directories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

function databasePath(): string {
  const directory = mkdtempSync(join(tmpdir(), "hawdb-bun-transaction-"));
  directories.push(directory);
  return join(directory, "database");
}

function withSchema(path: string): Database {
  const database = library.open(path);
  database.querySQL("CREATE TABLE events (id BIGINT PRIMARY KEY, kind TEXT)");
  database.querySQL("CREATE TABLE projection (id BIGINT PRIMARY KEY, total BIGINT)");
  database.querySQL("INSERT INTO projection (id, total) VALUES (1, 0)");
  return database;
}

function state(database: Database) {
  return {
    events: database.querySQL("SELECT id, kind FROM events ORDER BY id").rows,
    total: database.querySQL("SELECT total FROM projection WHERE id = 1").rows,
    audits: database.query("MATCH (a:Audit) RETURN a.event AS event").rows,
  };
}

function kindOf(action: () => unknown): string {
  try {
    action();
  } catch (error) {
    expect(error).toBeInstanceOf(HawDBError);
    return (error as HawDBError).kind;
  }
  throw new Error("expected a HawDBError");
}

describe("Bun transactions", () => {
  test("commits every statement on return and returns the body's value", () => {
    const path = databasePath();
    const database = withSchema(path);
    const seen = database.transaction((transaction) => {
      transaction.querySQL("INSERT INTO events (id, kind) VALUES ($1, $2)", [1, "deposit"]);
      transaction.querySQL("UPDATE projection SET total = total + 5 WHERE id = 1");
      transaction.exec("CREATE (:Audit {event: $event})", { event: 1 });
      return transaction.querySQL("SELECT total FROM projection WHERE id = 1").rows;
    });
    expect(seen).toEqual([[5]]);
    expect(state(database)).toEqual({ events: [[1, "deposit"]], total: [[5]], audits: [[1]] });
    database.close();

    const reopened = library.open(path, { readOnly: true });
    expect(state(reopened)).toEqual({ events: [[1, "deposit"]], total: [[5]], audits: [[1]] });
    reopened.close();
  }, 30_000);

  test("rolls back every statement when the body throws", () => {
    const database = withSchema(databasePath());
    const failure = new Error("projection update failed");
    expect(() =>
      database.transaction((transaction) => {
        transaction.querySQL("INSERT INTO events (id, kind) VALUES (1, 'lost')");
        transaction.querySQL("UPDATE projection SET total = 99 WHERE id = 1");
        throw failure;
      }),
    ).toThrow(failure);
    // A failing native statement rolls back the earlier ones too.
    expect(() =>
      database.transaction((transaction) => {
        transaction.querySQL("INSERT INTO events (id, kind) VALUES (2, 'lost')");
        transaction.querySQL("INSERT INTO events (id, kind) VALUES (2, 'duplicate')");
      }),
    ).toThrow(HawDBError);
    expect(state(database)).toEqual({ events: [], total: [[0]], audits: [] });
    database.close();
  }, 30_000);

  test("reports misuse with clear errors and keeps the database usable", () => {
    const database = withSchema(databasePath());
    let escaped: Transaction | undefined;
    database.transaction((transaction) => {
      escaped = transaction;
      expect(kindOf(() => database.transaction(() => undefined))).toBe("transaction_open");
      expect(kindOf(() => database.querySQL("SELECT COUNT(*) FROM events"))).toBe("transaction_open");
      expect(kindOf(() => database.query("RETURN 1 AS one"))).toBe("transaction_open");
      expect(kindOf(() => database.close())).toBe("transaction_open");
      transaction.querySQL("INSERT INTO events (id, kind) VALUES (1, 'kept')");
    });
    // A transaction object cannot be used after its body returns.
    expect(kindOf(() => escaped!.querySQL("INSERT INTO events (id, kind) VALUES (2, 'late')"))).toBe(
      "no_transaction",
    );
    expect(() => database.transaction(async () => undefined)).toThrow("must be synchronous");
    expect(() => database.querySQL("BEGIN")).toThrow(HawDBError);
    expect(state(database).events).toEqual([[1, "kept"]]);
    database.close();
    expect(kindOf(() => database.transaction(() => undefined))).toBe("closed");
  }, 30_000);

  const crashes: Array<[string, string, ReturnType<typeof state>]> = [
    ["uncommitted", "STAGED", { events: [], total: [[0]], audits: [] }],
    ["committed", "COMMITTED", { events: [[1, "deposit"]], total: [[5]], audits: [[1]] }],
  ];
  for (const [scenario, marker, expected] of crashes) {
    test(`recovers no partial transaction after a crash: ${scenario}`, () => {
      const path = databasePath();
      withSchema(path).close();
      const child = Bun.spawnSync([process.execPath, crashFixture, libraryPath, path, scenario], {
        stdout: "pipe",
        stderr: "pipe",
        timeout: 30_000,
      });
      expect(new TextDecoder().decode(child.stdout).trim()).toBe(marker);
      if (process.platform === "win32") {
        // Windows has no signals: Bun ends the process with TerminateProcess,
        // which reports a nonzero exit code and no signal.
        expect(child.signalCode).toBeUndefined();
        expect(child.exitCode).not.toBe(0);
        expect(child.exitCode).not.toBeNull();
      } else {
        expect(child.signalCode).toBe("SIGKILL");
      }
      const reopened = library.open(path);
      expect(state(reopened)).toEqual(expected);
      reopened.close();
    }, 60_000);
  }
});
