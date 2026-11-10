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

import { mock } from "bun:test";
import assert from "node:assert/strict";
import type { Database } from "../../src/index.ts";

let live = false;
let unloaded = false;
let queryCalls = 0;
let openCalls = 0;
let closeCalls = 0;
let freeCalls = 0;
const address = 100;
const payload = '{"columns":["value"],"rows":[[1]]}';

function open() {
  openCalls += 1;
  assert.equal(unloaded, false, "opened an unloaded library");
  live = true;
  // Every open deliberately reuses an address, without allocating native memory.
  return address;
}

function query(
  handle: number,
  _statement: Uint8Array,
  _statementLength: Uint8Array,
  _parameters: Uint8Array,
  _parametersLength: Uint8Array,
  result: BigUint64Array,
) {
  queryCalls += 1;
  assert.equal(unloaded, false, "queried an unloaded library");
  assert.equal(live, true, "queried a closed database");
  assert.equal(handle, address);
  result[0] = 1n;
  result[1] = BigInt(payload.length);
  return true;
}

mock.module("../../src/native.ts", () => ({
  CString: class extends String {
    constructor() {
      super(payload);
    }
  },
  openNativeLibrary: () => ({
    symbols: {
      hawdb_open: open,
      hawdb_open_in_memory: open,
      hawdb_close: (handle: number) => {
        assert.equal(live, true, "closed a database twice");
        assert.equal(handle, address);
        live = false;
        closeCalls += 1;
      },
      hawdb_query: query,
      hawdb_query_sql: query,
      hawdb_buffer_free: (buffer: BigUint64Array) => {
        assert.equal(buffer[0], 1n);
        buffer.fill(0n);
        freeCalls += 1;
      },
    },
    close: () => {
      assert.equal(live, false, "unloaded a library with a live database");
      unloaded = true;
    },
  }),
}));

const { HawDBError, loadHawDB } = await import("../../src/index.ts");
const scenario = process.argv[2];
const library = loadHawDB("instrumented native bridge");

if (scenario === "open-library-close") {
  assert.throws(
    () =>
      library.open("unused", {
        get readOnly() {
          library.close();
          return false;
        },
      }),
    (error) => error instanceof HawDBError && error.kind === "closed",
  );
  assert.equal(openCalls, 0);
} else {
  const database = library.openInMemory();
  let replacement: Database | undefined;
  try {
    if (scenario === "healthy") {
      assert.deepEqual(database.query("RETURN 1").rows, [[1]]);
      assert.equal(queryCalls, 1);
      assert.equal(freeCalls, 1);
      assert.throws(() => library.close(), (error) => error instanceof HawDBError);
      database.close();
      database.close();
      assert.equal(closeCalls, 1);
      assert.throws(() => database.query("RETURN 1"), (error) => error instanceof HawDBError);
    } else {
      assert.ok(
        ["cypher-close", "sql-close", "cypher-reuse", "sql-reuse", "query-library-close"].includes(scenario),
      );
      const value = () => {
        database.close();
        if (scenario.endsWith("reuse")) replacement = library.openInMemory();
        if (scenario === "query-library-close") library.close();
        return 1;
      };
      const parameters = {
        get value() {
          return value();
        },
      };
      const sqlParameters: number[] = [];
      Object.defineProperty(sqlParameters, "0", { get: value, enumerable: true });
      assert.throws(
        () =>
          scenario.startsWith("sql")
            ? database.querySQL("SELECT $1", sqlParameters)
            : database.query("RETURN $value", parameters),
        (error) =>
          error instanceof HawDBError && error.kind === (unloaded ? "closed" : "invalid_handle"),
      );
      assert.equal(queryCalls, 0);
      if (replacement) assert.equal(live, true);
    }
  } finally {
    database.close();
    replacement?.close();
    library.close();
  }
}

library.close();
console.log(`PASS ${scenario}`);
