// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { once } from "node:events";
import { Worker } from "node:worker_threads";
import {
  open,
  openInMemory,
  version,
  formatVersion,
  restore,
  importDump,
  verifyDump,
  HawDBError,
} from "../dist/index.js";

function workspace(t) {
  const root = mkdtempSync(join(tmpdir(), "hawdb-node-"));
  const handles = [];
  t.after(() => {
    for (const database of handles) database.close();
    rmSync(root, { recursive: true, force: true });
  });
  return {
    root,
    open(name, options) {
      const database = open(join(root, name), options);
      handles.push(database);
      return database;
    },
  };
}

test("worker environment cleanup releases native database ownership", async (t) => {
  const fixture = workspace(t);
  const worker = new Worker(
    new URL("./lifecycle-worker.mjs", import.meta.url),
    {
      workerData: { path: join(fixture.root, "database") },
    },
  );
  const [code] = await once(worker, "exit");
  assert.equal(code, 0);
  const database = fixture.open("database");
  assert.deepEqual(
    database.query("MATCH (n:Note) RETURN n.body AS body").rows,
    [["worker"]],
  );
});

test("parameterized SQL/Cypher preserve bigint, binary, tagged and NULL values", (t) => {
  const db = openInMemory();
  t.after(() => db.close());
  const values = {
    id: 9223372036854775807n,
    bytes: new Uint8Array([0, 128, 255]),
    decimal: { $decimal: "12.34" },
    nil: null,
  };
  assert.deepEqual(
    db
      .query(
        "UNWIND [1] AS seed RETURN $id AS id, $bytes AS bytes, $decimal AS decimal, $nil AS nil",
        values,
      )
      .dicts(),
    [values],
  );
  assert.deepEqual(db.querySQL("SELECT $1 AS id", [values.id]).rows, [
    [values.id],
  ]);
  db.exec("CREATE (:Note {id: $id, body: $body})", { id: 1, body: "hello" });
  assert.deepEqual(db.query("MATCH (n:Note) RETURN n.body AS body").dicts(), [
    { body: "hello" },
  ]);
  assert.match(version(), /^hawdb \d+\.\d+\.\d+/);
  assert.equal(typeof formatVersion(), "number");
  assert.throws(
    () =>
      db.query("UNWIND [1] AS seed RETURN $n AS n", {
        n: 9223372036854775808n,
      }),
    /64|range/,
  );
  assert.throws(() => db.querySQL("SELECT $1", [undefined]), /undefined/);
  assert.throws(
    () => db.querySQL("SELECT $1", new Array(1)),
    /dense|hole|sparse/,
  );
  assert.throws(
    () =>
      db.query("UNWIND [1] AS seed RETURN $n AS n", {
        n: Number.MAX_SAFE_INTEGER + 1,
      }),
    /safe|bigint/,
  );
});

test("callback transactions commit together, roll back and expire their scope", (t) => {
  const db = openInMemory();
  t.after(() => db.close());
  db.querySQL("CREATE TABLE notes (id BIGINT PRIMARY KEY, body TEXT)");
  let stale;
  assert.equal(
    db.transaction((tx) => {
      stale = tx;
      tx.querySQL("INSERT INTO notes (id, body) VALUES ($1, $2)", [1, "kept"]);
      tx.exec("CREATE (:Note {body: 'kept'})");
      assert.throws(() => db.querySQL("SELECT * FROM notes"), {
        kind: "transaction_open",
      });
      assert.throws(() => db.close(), { kind: "transaction_open" });
      assert.throws(() => db.transaction(() => {}), {
        kind: "transaction_open",
      });
      return 42;
    }),
    42,
  );
  assert.throws(() => stale.querySQL("SELECT * FROM notes"), {
    kind: "no_transaction",
  });
  const original = new Error("rollback control");
  assert.throws(
    () =>
      db.transaction((tx) => {
        tx.querySQL("INSERT INTO notes (id, body) VALUES ($1, $2)", [
          2,
          "discarded",
        ]);
        tx.exec("CREATE (:Note {body: 'discarded'})");
        throw original;
      }),
    (error) => error === original,
  );
  assert.deepEqual(db.querySQL("SELECT id FROM notes").rows, [[1]]);
  assert.deepEqual(db.query("MATCH (n:Note) RETURN n.body AS body").rows, [
    ["kept"],
  ]);
  assert.throws(
    () =>
      db.transaction((tx) => {
        tx.querySQL("INSERT INTO notes (id, body) VALUES (3, 'discarded')");
        return Promise.resolve(1);
      }),
    /promise|thenable/,
  );
  assert.deepEqual(db.querySQL("SELECT id FROM notes").rows, [[1]]);
  assert.throws(
    () => db.querySQL("INSERT INTO notes (id, body) VALUES (1, 'duplicate')"),
    (error) => error instanceof HawDBError && error.kind === "constraint",
  );
});

test("persistent reopen, backup/restore and logical dump use the same database", (t) => {
  const fixture = workspace(t);
  const { root } = fixture;
  let db = fixture.open("database");
  db.exec("CREATE (:Note {body: 'durable'})");
  db.close();
  db.close();
  assert.throws(() => db.query("UNWIND [1] AS seed RETURN 1 AS n"), {
    kind: "closed",
  });
  db = fixture.open("database");
  assert.deepEqual(db.query("MATCH (n:Note) RETURN n.body AS body").rows, [
    ["durable"],
  ]);
  const backup = join(root, "backup");
  assert.ok(db.backup(backup).file_count > 0);
  assert.ok(db.scrub().checked_file_count > 0);
  restore(backup, join(root, "restored"));
  const restored = fixture.open("restored");
  assert.deepEqual(
    restored.query("MATCH (n:Note) RETURN n.body AS body").rows,
    [["durable"]],
  );
  const dump = join(root, "dump");
  db.export(dump);
  assert.equal(verifyDump(dump).node_count, 1);
  importDump(dump, join(root, "imported"));
  const imported = fixture.open("imported");
  assert.deepEqual(
    imported.query("MATCH (n:Note) RETURN n.body AS body").rows,
    [["durable"]],
  );
  assert.throws(() => importDump(dump, join(root, "imported")));
});

test("configured result budgets refuse atomically and preserve error kinds", (t) => {
  const fixture = workspace(t);
  const { root } = fixture;
  const db = fixture.open("bounded", {
    maxReadResultRows: 1,
    maxReadResultPayloadBytes: 1024,
  });
  assert.throws(
    () => db.query("UNWIND [1, 2] AS n RETURN n"),
    (error) =>
      error instanceof HawDBError &&
      ["read_budget_exceeded", "resource_budget_exceeded"].includes(error.kind),
  );
  assert.deepEqual(db.query("UNWIND [1] AS seed RETURN 1 AS n").rows, [[1]]);
  assert.throws(() => open(join(root, "bad"), { misspelled: true }), {
    kind: "invalid_argument",
  });
  assert.throws(
    () => open(join(root, "bad"), { maxReadResultPayloadBytes: 0 }),
    { kind: "invalid_argument" },
  );
});

test("argument getters cannot dispatch through a handle they closed", () => {
  const db = openInMemory();
  const values = {
    get value() {
      db.close();
      return 1;
    },
  };
  assert.throws(
    () => db.query("UNWIND [1] AS seed RETURN $value AS value", values),
    { kind: "closed" },
  );
  db.close();
});
