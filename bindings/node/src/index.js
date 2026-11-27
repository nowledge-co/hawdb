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

import { getNative } from "./native.js";
import { parseLosslessJson, stringifyParameters } from "./json.js";

const constructorToken = Symbol("HawDB database");
const optionKeys = {
  readOnly: "read_only",
  durability: "durability",
  maxReadResultRows: "max_read_result_rows",
  maxReadIntermediateRows: "max_read_intermediate_rows",
  maxReadResultPayloadBytes: "max_read_result_payload_bytes",
  relationalMonotonicAppendFastPath: "relational_monotonic_append_fast_path",
};

export class HawDBError extends Error {
  constructor(kind, message, details = {}) {
    super(message);
    this.name = "HawDBError";
    this.kind = kind;
    this.details = details;
  }
}

function call(operation) {
  try {
    return operation();
  } catch (error) {
    let envelope;
    try {
      envelope = parseLosslessJson(error.message);
    } catch {
      throw error;
    }
    if (
      !envelope ||
      typeof envelope.kind !== "string" ||
      typeof envelope.message !== "string"
    )
      throw error;
    const { kind, message, ...details } = envelope;
    throw new HawDBError(kind, message, details);
  }
}

function text(value, name) {
  if (typeof value !== "string" || value.length === 0)
    throw new TypeError(`${name} must be a non-empty string`);
  return value;
}

function normalize(value) {
  if (Array.isArray(value)) return value.map(normalize);
  if (value === null || typeof value !== "object") return value;
  const entries = Object.entries(value);
  if (
    entries.length === 1 &&
    entries[0][0] === "$binary" &&
    typeof entries[0][1] === "string"
  ) {
    return new Uint8Array(Buffer.from(entries[0][1], "base64"));
  }
  return Object.fromEntries(
    entries.map(([key, child]) => [key, normalize(child)]),
  );
}

// The C ABI's "counters" keys, by QueryCounters field.
const counterKeys = {
  nodesCreated: "nodes_created",
  nodesDeleted: "nodes_deleted",
  relationshipsCreated: "relationships_created",
  relationshipsDeleted: "relationships_deleted",
  propertiesSet: "properties_set",
  labelsAdded: "labels_added",
  labelsRemoved: "labels_removed",
  indexesAdded: "indexes_added",
  indexesRemoved: "indexes_removed",
  constraintsAdded: "constraints_added",
  constraintsRemoved: "constraints_removed",
};

export class QueryCounters {
  constructor(values = {}) {
    for (const name of Object.keys(counterKeys)) this[name] = values[name] ?? 0;
    Object.freeze(this);
  }
  containsUpdates() {
    return Object.keys(counterKeys).some((name) => this[name] !== 0);
  }
}

function counters(value) {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new TypeError("HawDB returned a malformed result envelope");
  const values = {};
  for (const [name, key] of Object.entries(counterKeys)) {
    const count = value[key];
    if (!(typeof count === "number" || typeof count === "bigint") || count < 0)
      throw new TypeError("HawDB returned a malformed result envelope");
    values[name] = count;
  }
  return new QueryCounters(values);
}

export class QueryResult {
  constructor(columns, rows, counters = new QueryCounters()) {
    this.columns = columns;
    this.rows = rows;
    this.counters = counters;
  }
  dicts() {
    return this.rows.map((row) =>
      Object.fromEntries(this.columns.map((name, index) => [name, row[index]])),
    );
  }
}

function result(json) {
  const parsed = parseLosslessJson(json);
  if (
    !parsed ||
    !Array.isArray(parsed.columns) ||
    !Array.isArray(parsed.rows) ||
    !parsed.columns.every((name) => typeof name === "string") ||
    !parsed.rows.every(
      (row) => Array.isArray(row) && row.length === parsed.columns.length,
    )
  ) {
    throw new TypeError("HawDB returned a malformed result envelope");
  }
  return new QueryResult(
    parsed.columns,
    parsed.rows.map((row) => row.map(normalize)),
    counters(parsed.counters),
  );
}

function parameters(values, sql) {
  if (
    sql
      ? !Array.isArray(values)
      : values === null || typeof values !== "object" || Array.isArray(values)
  ) {
    throw new TypeError(
      sql
        ? "SQL parameters must be an array"
        : "Cypher parameters must be an object",
    );
  }
  return stringifyParameters(values);
}

export class Database {
  #native;
  #closed = false;
  #transaction = null;

  constructor(native, token) {
    if (token !== constructorToken)
      throw new TypeError("Use open() or openInMemory() to create a database");
    this.#native = native;
  }

  #assertOpen() {
    if (this.#closed)
      throw new HawDBError("closed", "HawDB database is closed");
  }

  #assertOutsideTransaction() {
    this.#assertOpen();
    if (this.#transaction !== null)
      throw new HawDBError(
        "transaction_open",
        "Use the transaction argument while its callback is running",
      );
  }

  #query(statement, values, sql, token = null) {
    this.#assertOpen();
    if (
      token !== null ? token !== this.#transaction : this.#transaction !== null
    ) {
      throw new HawDBError(
        token === null ? "transaction_open" : "no_transaction",
        "Statement is outside its transaction scope",
      );
    }
    const query = text(statement, "Statement");
    const encoded = parameters(values, sql);
    // Parameter getters can reenter JavaScript, including closing this handle.
    this.#assertOpen();
    if (token !== this.#transaction)
      throw new HawDBError("no_transaction", "Transaction has finished");
    return result(
      call(() => this.#native.query(query, encoded, sql, token !== null)),
    );
  }

  query(statement, values = {}) {
    return this.#query(statement, values, false);
  }
  querySQL(statement, values = []) {
    return this.#query(statement, values, true);
  }
  exec(statement, values = {}) {
    this.query(statement, values);
  }

  transaction(body) {
    this.#assertOutsideTransaction();
    if (typeof body !== "function")
      throw new TypeError("Transaction body must be a synchronous function");
    call(() => this.#native.begin());
    const token = Symbol("HawDB transaction");
    this.#transaction = token;
    const transaction = Object.freeze({
      query: (statement, values = {}) =>
        this.#query(statement, values, false, token),
      querySQL: (statement, values = []) =>
        this.#query(statement, values, true, token),
      exec: (statement, values = {}) => {
        this.#query(statement, values, false, token);
      },
    });
    let committing = false;
    try {
      const value = body(transaction);
      if (
        value !== null &&
        (typeof value === "object" || typeof value === "function") &&
        typeof value.then === "function"
      ) {
        throw new TypeError(
          "Transaction body must not return a promise or thenable",
        );
      }
      committing = true;
      call(() => this.#native.commit());
      return value;
    } catch (error) {
      // Commit closes the native transaction even on an uncertain publication.
      if (!committing) {
        try {
          call(() => this.#native.rollback());
        } catch (rollbackError) {
          throw new AggregateError(
            [error, rollbackError],
            "Transaction and rollback both failed",
          );
        }
      }
      throw error;
    } finally {
      this.#transaction = null;
    }
  }

  backup(destination) {
    this.#assertOutsideTransaction();
    return parseLosslessJson(
      call(() => this.#native.backup(text(destination, "Destination"))),
    );
  }

  export(destination) {
    this.#assertOutsideTransaction();
    return parseLosslessJson(
      call(() => this.#native.export(text(destination, "Destination"))),
    );
  }

  scrub() {
    this.#assertOutsideTransaction();
    return parseLosslessJson(call(() => this.#native.scrub()));
  }

  close() {
    if (this.#closed) return;
    this.#assertOutsideTransaction();
    call(() => this.#native.close());
    this.#closed = true;
  }

  [Symbol.dispose]() {
    this.close();
  }
}

export function open(path, options = {}) {
  text(path, "Database path");
  if (options === null || typeof options !== "object" || Array.isArray(options))
    throw new TypeError("Open options must be an object");
  const nativeOptions = {};
  for (const key of Object.keys(options)) {
    if (!Object.hasOwn(optionKeys, key))
      throw new HawDBError("invalid_argument", `Unknown open option ${key}`);
    const value = options[key];
    if (value !== undefined) nativeOptions[optionKeys[key]] = value;
  }
  const encoded = stringifyParameters(nativeOptions);
  return new Database(
    call(() => getNative().NativeDatabase.open(path, encoded)),
    constructorToken,
  );
}

export function openInMemory() {
  return new Database(
    call(() => getNative().NativeDatabase.openInMemory()),
    constructorToken,
  );
}

export function version() {
  return call(() => getNative().version());
}
export function formatVersion() {
  return call(() => getNative().formatVersion());
}
export function dumpFormatVersion() {
  return call(() => getNative().dumpFormatVersion());
}
export function restore(source, destination) {
  return parseLosslessJson(
    call(() =>
      getNative().restore(
        text(source, "Backup source"),
        text(destination, "Destination"),
        false,
      ),
    ),
  );
}
export function importDump(source, destination) {
  return parseLosslessJson(
    call(() =>
      getNative().restore(
        text(source, "Dump source"),
        text(destination, "Destination"),
        true,
      ),
    ),
  );
}
export function verifyDump(path) {
  return parseLosslessJson(
    call(() => getNative().verifyDump(text(path, "Dump path"))),
  );
}
export function confirmDumpPublication(path) {
  call(() => getNative().confirmDumpPublication(text(path, "Destination")));
}
