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

  test("searches SQL rows with an engine-owned BM25 index across reopen", () => {
    const directory = mkdtempSync(join(tmpdir(), "hawdb-bun-search-"));
    const path = join(directory, "database");
    const search =
      "SELECT e.id, e.seq, score FROM text_search('events_text', $1, top_k => $2) AS e " +
      "WHERE e.stream = $3 ORDER BY score DESC, e.id";
    try {
      const writer = library.open(path);
      try {
        writer.querySQL(
          "CREATE TABLE events (id BIGINT PRIMARY KEY, stream TEXT NOT NULL, seq BIGINT NOT NULL, text TEXT)",
        );
        writer.querySQL(
          "CREATE SEARCH INDEX events_text ON events (text) WITH (analyzer = 'cjk_ngram')",
        );
        const rows: [number, string, number, string][] = [
          [1, "chat", 1, "上次我们讨论了数据库的全文搜索"],
          [2, "chat", 2, "Lexical recall over the event log"],
          [3, "task", 3, "数据库迁移任务完成"],
          [4, "chat", 4, "unrelated note"],
        ];
        for (const [id, stream, seq, text] of rows) {
          writer.querySQL(
            "INSERT INTO events (id, stream, seq, text) VALUES ($1, $2, $3, $4)",
            [id, stream, seq, text],
          );
        }
        const chinese = writer.querySQL(search, ["数据库", 10, "chat"]);
        expect(chinese.columns).toEqual(["id", "seq", "score"]);
        expect(chinese.rows.map((row) => row[0])).toEqual([1]);
        expect(chinese.rows[0][2]).toBeGreaterThan(0);
        writer.querySQL("UPDATE events SET text = $1 WHERE id = 4", ["数据库 recall note"]);
        writer.querySQL("DELETE FROM events WHERE id = 2");
      } finally {
        writer.close();
      }

      const reader = library.open(path, { readOnly: true });
      try {
        const reopened = reader.querySQL(search, ["数据库", 10, "chat"]).rows;
        expect(reopened.map((row) => row[0]).sort()).toEqual([1, 4]);
        expect(reopened[0][2]).toBeGreaterThanOrEqual(reopened[1][2] as number);
        expect(
          reader.querySQL(search, ["lexical recall", 10, "chat"]).rows.map((row) => row[0]),
        ).toEqual([4]);
        expect(reader.querySQL(search, ["数据库", 10, "task"]).rows.map((row) => row[1])).toEqual([
          3,
        ]);
      } finally {
        reader.close();
      }
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
