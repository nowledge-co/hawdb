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
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { HawDBError, loadHawDB } from "../src/index.ts";

const libraryPath = process.env.HAWDB_LIBRARY;
if (!libraryPath) {
  throw new Error("HAWDB_LIBRARY must name the Bazel-built HawDB C ABI library");
}
const library = loadHawDB(libraryPath);

afterAll(() => library.close());

describe("Bun FFI binding", () => {
  test("reports the loaded HawDB version", () => {
    expect(library.version()).toMatch(/^hawdb \d+\.\d+\.\d+/);
  });

  test("runs Cypher with exact values and owns native buffers", () => {
    const database = library.openInMemory();
    try {
      const maximum = 9223372036854775807n;
      const minimum = -9223372036854775808n;
      database.exec(
        "CREATE (:Value {safe: $safe, maximum: $maximum, minimum: $minimum, text: $text, binary: $binary})",
        {
          safe: 42,
          maximum,
          minimum,
          text: "雪\u0000hawdb",
          binary: new Uint8Array([0, 1, 2, 255]),
        },
      );
      const result = database.query(
        "MATCH (n:Value) RETURN n.safe AS safe, n.maximum AS maximum, n.minimum AS minimum, n.text AS text, n.binary AS binary",
      );
      expect(result.columns).toEqual(["safe", "maximum", "minimum", "text", "binary"]);
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0][0]).toBe(42);
      expect(result.rows[0][1]).toBe(maximum);
      expect(result.rows[0][2]).toBe(minimum);
      expect(result.rows[0][3]).toBe("雪\u0000hawdb");
      expect(result.rows[0][4]).toEqual(new Uint8Array([0, 1, 2, 255]));
      expect(result.dicts()[0].maximum).toBe(maximum);
    } finally {
      database.close();
    }
  });

  test("runs SQL and preserves typed native errors", () => {
    const database = library.openInMemory();
    try {
      expect(database.querySQL("SELECT $1 AS value", [7]).rows).toEqual([[7]]);
      expect(() => database.query("NOT VALID CYPHER")).toThrow(HawDBError);
      try {
        database.query("NOT VALID CYPHER");
      } catch (error) {
        expect(error).toBeInstanceOf(HawDBError);
        expect((error as HawDBError).kind.length).toBeGreaterThan(0);
      }
    } finally {
      database.close();
    }
  });

  test("persists data and rejects use after close", () => {
    const directory = mkdtempSync(join(tmpdir(), "hawdb-bun-"));
    const path = join(directory, "database");
    try {
      const writer = library.open(path);
      writer.exec("CREATE (:Item {id: 9})");
      writer.close();

      const reader = library.open(path, { readOnly: true });
      expect(reader.query("MATCH (n:Item) RETURN n.id AS id").rows).toEqual([[9]]);
      reader.close();
      reader.close();
      expect(() => reader.query("RETURN 1 AS value")).toThrow("HawDB database is closed");
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  }, 30_000);

  test("does not unload the library while a database is live", () => {
    const database = library.openInMemory();
    expect(() => library.close()).toThrow("live database handle");
    database.close();
  });

  test("rejects unsafe numeric inputs before crossing FFI", () => {
    const database = library.openInMemory();
    try {
      expect(() => database.query("RETURN $value AS value", { value: 9007199254740992 })).toThrow(
        "must use bigint",
      );
      expect(() =>
        database.query("RETURN $value AS value", { value: 9223372036854775808n }),
      ).toThrow("signed 64-bit range");
    } finally {
      database.close();
    }
  });
});
