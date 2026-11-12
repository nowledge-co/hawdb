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
let maintenanceCalls = 0;
const address = 100;
const queryPayload = '{"columns":["value"],"rows":[[1]]}';
const reportPayload =
  '{"generation":1,"checkpoint_commit_epoch":1,"file_count":1,"total_bytes":1,"manifest_checksum":1}';
let payload = queryPayload;

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
  payload = queryPayload;
  result[0] = 1n;
  result[1] = BigInt(payload.length);
  return true;
}

function report(output: BigUint64Array) {
  payload = reportPayload;
  output[0] = 1n;
  output[1] = BigInt(payload.length);
  return true;
}

function backup(handle: number, _path: Uint8Array, _pathLength: Uint8Array, output: BigUint64Array) {
  maintenanceCalls += 1;
  assert.equal(unloaded, false, "backed up through an unloaded library");
  assert.equal(live, true, "backed up a closed database");
  assert.equal(handle, address);
  return report(output);
}

function restore(
  _backup: Uint8Array,
  _backupLength: Uint8Array,
  _destination: Uint8Array,
  _destinationLength: Uint8Array,
  output: BigUint64Array,
) {
  maintenanceCalls += 1;
  assert.equal(unloaded, false, "restored through an unloaded library");
  return report(output);
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
      hawdb_backup: backup,
      hawdb_restore: restore,
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
} else if (scenario === "restore-backup-library-close" || scenario === "restore-destination-library-close") {
  // A boxed path's conversion runs after the entry check; it must not reach native code.
  const path = Object.assign(new String("path"), {
    [Symbol.toPrimitive]() {
      library.close();
      return "path";
    },
  }) as unknown as string;
  assert.throws(
    () =>
      scenario === "restore-backup-library-close"
        ? library.restore(path, "restored")
        : library.restore("backup", path),
    (error) => error instanceof HawDBError && error.kind === "closed",
  );
  assert.equal(maintenanceCalls, 0);
} else if (scenario === "maintenance-healthy") {
  const database = library.openInMemory();
  const path = Object.assign(new String("backup"), {
    [Symbol.toPrimitive]: () => "backup",
  });
  assert.equal(database.backup(path as unknown as string).file_count, 1);
  assert.equal(library.restore("backup", path as unknown as string).generation, 1);
  assert.equal(maintenanceCalls, 2);
  database.close();
} else if (scenario === "backup-close" || scenario === "backup-reuse") {
  const database = library.openInMemory();
  let replacement: Database | undefined;
  const path = Object.assign(new String("backup"), {
    [Symbol.toPrimitive]() {
      database.close();
      if (scenario === "backup-reuse") replacement = library.openInMemory();
      return "backup";
    },
  });
  try {
    assert.throws(
      () => database.backup(path as unknown as string),
      (error) => error instanceof HawDBError && error.kind === "invalid_handle",
    );
    assert.equal(maintenanceCalls, 0);
    // The replacement shares the freed address but is a different lifetime.
    if (scenario === "backup-reuse") assert.equal(live, true);
  } finally {
    database.close();
    replacement?.close();
  }
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
