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

import { CString, openNativeLibrary, type NativeLibrary, type NativePointer } from "./native.ts";
import { parseLosslessJson, stringifyParameters } from "./json.ts";

const encoder = new TextEncoder();
const databaseConstructorToken = Symbol("HawDB Database constructor");

export type TaggedHawDBValue =
  | { $decimal: string }
  | { $uuid: string }
  | { $date: string }
  | { $localtime: string }
  | { $localdatetime: string }
  | { $datetime: string }
  | { $duration: string }
  | { $vector: number[] };

export type HawDBValue =
  | null
  | boolean
  | number
  | bigint
  | string
  | Uint8Array
  | TaggedHawDBValue
  | HawDBValue[]
  | { [key: string]: HawDBValue };

export type HawDBParameter = HawDBValue;

/** WAL synchronization policy; see the C ABI `hawdb_open` documentation. */
export type Durability = "sync_on_every_write" | "sync_on_checkpoint";

/**
 * Options for `HawDBLibrary.open`. Each key mirrors one `hawdb_open` option;
 * an absent or `undefined` key keeps the native default (power-loss-safe
 * `sync_on_every_write`, no row ceiling, a 64 MiB read payload budget and the
 * monotonic-append fast path off). The native library validates every value.
 */
export interface OpenOptions {
  readOnly?: boolean;
  durability?: Durability;
  /** Host-wide read row ceiling; `0` and `null` mean no row ceiling. */
  maxReadResultRows?: number | bigint | null;
  /** Host-wide read result payload budget in bytes; must be positive. */
  maxReadResultPayloadBytes?: number | bigint;
  relationalMonotonicAppendFastPath?: boolean;
}

const openOptionKeys: Record<keyof OpenOptions, string> = {
  readOnly: "read_only",
  durability: "durability",
  maxReadResultRows: "max_read_result_rows",
  maxReadResultPayloadBytes: "max_read_result_payload_bytes",
  relationalMonotonicAppendFastPath: "relational_monotonic_append_fast_path",
};

function nativeOpenOptions(options: OpenOptions): Record<string, HawDBParameter> {
  if (options === null || typeof options !== "object" || Array.isArray(options)) {
    throw new TypeError("HawDB open options must be an object");
  }
  const native: Record<string, HawDBParameter> = {};
  for (const key of Object.keys(options)) {
    if (!Object.hasOwn(openOptionKeys, key)) {
      throw new HawDBError(
        "invalid_argument",
        `unknown HawDB open option "${key}"; supported options are ${Object.keys(openOptionKeys).join(", ")}`,
      );
    }
    const value = (options as Record<string, unknown>)[key];
    if (value !== undefined) {
      native[openOptionKeys[key as keyof OpenOptions]] = value as HawDBParameter;
    }
  }
  return native;
}

/** An unsigned 64-bit count: `number` when safe, otherwise `bigint`. */
export type DumpCount = number | bigint;

/** `database.export` result; see docs/LOGICAL_DUMP.md. */
export interface ExportReport {
  dump_format_version: number;
  persistent_format_version: number;
  hawdb_version: string;
  branch: string | null;
  commit_epoch: DumpCount;
  table_count: DumpCount;
  table_row_count: DumpCount;
  node_count: DumpCount;
  relationship_count: DumpCount;
  file_count: DumpCount;
  total_bytes: DumpCount;
  manifest_sha256: string;
}

/** `library.import` result; see docs/LOGICAL_DUMP.md. */
export interface ImportReport {
  dump_format_version: number;
  source_hawdb_version: string;
  source_persistent_format_version: number;
  persistent_format_version: number;
  table_count: DumpCount;
  table_row_count: DumpCount;
  node_count: DumpCount;
  relationship_count: DumpCount;
  file_count: DumpCount;
  total_bytes: DumpCount;
  manifest_sha256: string;
}

const exportReportFields = {
  counts: [
    "dump_format_version",
    "persistent_format_version",
    "commit_epoch",
    "table_count",
    "table_row_count",
    "node_count",
    "relationship_count",
    "file_count",
    "total_bytes",
  ],
  strings: ["hawdb_version", "manifest_sha256"],
} as const;

const importReportFields = {
  counts: [
    "dump_format_version",
    "source_persistent_format_version",
    "persistent_format_version",
    "table_count",
    "table_row_count",
    "node_count",
    "relationship_count",
    "file_count",
    "total_bytes",
  ],
  strings: ["source_hawdb_version", "manifest_sha256"],
} as const;

function parseDumpReport<T>(
  text: string | null,
  fields: { counts: readonly string[]; strings: readonly string[] },
): T {
  if (text === null) {
    throw new HawDBError("ffi", "HawDB dump call returned no report");
  }
  const report = parseLosslessJson(text);
  if (
    report === null ||
    Array.isArray(report) ||
    typeof report !== "object" ||
    !fields.counts.every(
      (field) => typeof report[field] === "number" || typeof report[field] === "bigint",
    ) ||
    !fields.strings.every((field) => typeof report[field] === "string")
  ) {
    throw new TypeError("HawDB returned a malformed dump report");
  }
  return report as T;
}

export class QueryResult {
  constructor(
    public readonly columns: string[],
    public readonly rows: HawDBValue[][],
  ) {}

  dicts(): Array<Record<string, HawDBValue>> {
    return this.rows.map((row) =>
      Object.fromEntries(this.columns.map((column, index) => [column, row[index] ?? null])),
    );
  }
}

export class HawDBError extends Error {
  public readonly details: Record<string, unknown>;

  constructor(
    public readonly kind: string,
    message: string,
    details: Record<string, unknown> = {},
  ) {
    super(message);
    this.name = "HawDBError";
    this.details = details;
  }
}

function outputBuffer(): BigUint64Array {
  // HawdbBuffer is { char *ptr; uintptr_t len }. Bun is 64-bit on every
  // supported host, and BigUint64Array supplies the required alignment.
  return new BigUint64Array(2);
}

function decodeBase64(encoded: string): Uint8Array {
  const binary = atob(encoded);
  return Uint8Array.from(binary, (character) => character.charCodeAt(0));
}

function normalizeValue(value: unknown): HawDBValue {
  if (
    value === null ||
    typeof value === "boolean" ||
    typeof value === "number" ||
    typeof value === "bigint" ||
    typeof value === "string"
  ) {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map(normalizeValue);
  }
  if (typeof value !== "object") {
    throw new TypeError("HawDB returned an unsupported JSON value");
  }
  const entries = Object.entries(value);
  if (entries.length === 1 && entries[0][0] === "$binary" && typeof entries[0][1] === "string") {
    return decodeBase64(entries[0][1]);
  }
  return Object.fromEntries(entries.map(([key, child]) => [key, normalizeValue(child)]));
}

function parseResult(text: string): QueryResult {
  const envelope = parseLosslessJson(text);
  if (envelope === null || Array.isArray(envelope) || typeof envelope !== "object") {
    throw new TypeError("HawDB returned a malformed result envelope");
  }
  const columns = envelope.columns;
  const rows = envelope.rows;
  if (!Array.isArray(columns) || !Array.isArray(rows)) {
    throw new TypeError("HawDB returned a malformed result envelope");
  }
  const normalizedColumns = columns.map((column) => {
    if (typeof column !== "string") {
      throw new TypeError("HawDB returned a malformed result envelope");
    }
    return column;
  });
  const normalizedRows = rows.map((row) => {
    if (!Array.isArray(row) || row.length !== normalizedColumns.length) {
      throw new TypeError("HawDB returned a malformed result envelope");
    }
    return row.map(normalizeValue);
  });
  return new QueryResult(normalizedColumns, normalizedRows);
}

export class HawDBLibrary {
  private readonly native: NativeLibrary;
  private readonly finalizer: FinalizationRegistry<NativePointer>;
  private readonly handles = new Map<NativePointer, symbol>();
  private closed = false;

  private constructor(path: string) {
    this.native = openNativeLibrary(path);
    this.finalizer = new FinalizationRegistry((handle) => {
      if (!this.closed) {
        this.native.symbols.hawdb_close(handle);
        this.handles.delete(handle);
      }
    });
  }

  static load(path: string): HawDBLibrary {
    if (path.length === 0) {
      throw new TypeError("HawDB shared-library path must not be empty");
    }
    return new HawDBLibrary(path);
  }

  private assertOpen(): void {
    if (this.closed) {
      throw new HawDBError("closed", "HawDB shared library is closed");
    }
  }

  private takeBuffer(buffer: BigUint64Array): string | null {
    const address = buffer[0];
    const length = buffer[1];
    if (address === 0n) {
      if (length !== 0n) {
        throw new TypeError("HawDB returned a NULL buffer with a non-zero length");
      }
      return null;
    }
    try {
      if (length > BigInt(Number.MAX_SAFE_INTEGER)) {
        throw new RangeError("HawDB returned a buffer too large for JavaScript");
      }
      return String(new CString(address, 0, Number(length)));
    } finally {
      this.native.symbols.hawdb_buffer_free(buffer);
    }
  }

  private errorFrom(buffer: BigUint64Array, fallback: string): HawDBError {
    const text = this.takeBuffer(buffer);
    if (text === null) {
      return new HawDBError("ffi", fallback);
    }
    try {
      const value = parseLosslessJson(text);
      if (value !== null && !Array.isArray(value) && typeof value === "object") {
        const kind = typeof value.kind === "string" ? value.kind : "ffi";
        const message = typeof value.message === "string" ? value.message : fallback;
        return new HawDBError(kind, message, value as Record<string, unknown>);
      }
    } catch {
      // Preserve the native payload below when an older library did not return JSON.
    }
    return new HawDBError("ffi", text);
  }

  private opened(handle: NativePointer | null): Database {
    if (!handle) {
      throw new HawDBError("ffi", "HawDB returned a NULL database without an error");
    }
    const database = new Database(this, handle, databaseConstructorToken);
    this.handles.set(handle, Symbol("HawDB database lifetime"));
    this.finalizer.register(database, handle, database);
    return database;
  }

  open(path: string, options: OpenOptions = {}): Database {
    this.assertOpen();
    if (path.length === 0) {
      throw new TypeError("HawDB database path must not be empty");
    }
    const pathBytes = encoder.encode(path);
    const optionsBytes = encoder.encode(stringifyParameters(nativeOpenOptions(options)));
    const error = outputBuffer();
    this.assertOpen();
    const handle = this.native.symbols.hawdb_open(
      pathBytes,
      pathBytes,
      optionsBytes,
      optionsBytes,
      error,
    );
    if (!handle) {
      throw this.errorFrom(error, "HawDB open failed");
    }
    const unexpected = this.takeBuffer(error);
    if (unexpected !== null) {
      this.native.symbols.hawdb_close(handle);
      throw new HawDBError("ffi", `HawDB open succeeded with an error payload: ${unexpected}`);
    }
    return this.opened(handle);
  }

  openInMemory(): Database {
    this.assertOpen();
    const error = outputBuffer();
    const handle = this.native.symbols.hawdb_open_in_memory(error);
    if (!handle) {
      throw this.errorFrom(error, "HawDB in-memory open failed");
    }
    const unexpected = this.takeBuffer(error);
    if (unexpected !== null) {
      this.native.symbols.hawdb_close(handle);
      throw new HawDBError("ffi", `HawDB in-memory open succeeded with an error payload: ${unexpected}`);
    }
    return this.opened(handle);
  }

  version(): string {
    this.assertOpen();
    const output = outputBuffer();
    this.native.symbols.hawdb_version(output);
    const version = this.takeBuffer(output);
    if (version === null) {
      throw new HawDBError("ffi", "HawDB returned no version string");
    }
    return version;
  }

  query(
    handle: NativePointer,
    sql: boolean,
    statement: string,
    parameters: Record<string, HawDBParameter> | HawDBParameter[],
  ): QueryResult {
    this.assertOpen();
    const lifetime = this.handles.get(handle);
    if (lifetime === undefined) {
      throw new HawDBError("invalid_handle", "HawDB database handle is not live");
    }
    if (statement.length === 0) {
      throw new TypeError("HawDB statement must not be empty");
    }
    const statementBytes = encoder.encode(statement);
    const parameterBytes = encoder.encode(stringifyParameters(parameters));
    const result = outputBuffer();
    const error = outputBuffer();
    const call = sql ? this.native.symbols.hawdb_query_sql : this.native.symbols.hawdb_query;
    // Getters may close this database and open another at the same address.
    this.assertOpen();
    if (this.handles.get(handle) !== lifetime) {
      throw new HawDBError("invalid_handle", "HawDB database handle is not live");
    }
    const ok = call(
      handle,
      statementBytes,
      statementBytes,
      parameterBytes,
      parameterBytes,
      result,
      error,
    );
    if (!ok) {
      this.takeBuffer(result);
      throw this.errorFrom(error, "HawDB query failed");
    }
    const unexpected = this.takeBuffer(error);
    const text = this.takeBuffer(result);
    if (unexpected !== null) {
      throw new HawDBError("ffi", `HawDB query succeeded with an error payload: ${unexpected}`);
    }
    if (text === null) {
      throw new HawDBError("ffi", "HawDB query returned no result payload");
    }
    return parseResult(text);
  }

  /** The engine-wide persistent format version of the loaded library. */
  formatVersion(): number {
    this.assertOpen();
    return this.native.symbols.hawdb_format_version();
  }

  /** The newest logical dump format version the loaded library reads. */
  dumpFormatVersion(): number {
    this.assertOpen();
    return this.native.symbols.hawdb_dump_format_version();
  }

  private dumpCall<T>(
    fields: { counts: readonly string[]; strings: readonly string[] },
    action: string,
    call: (report: BigUint64Array, error: BigUint64Array) => boolean,
  ): T {
    const report = outputBuffer();
    const error = outputBuffer();
    const ok = call(report, error);
    if (!ok) {
      this.takeBuffer(report);
      throw this.errorFrom(error, `HawDB ${action} failed`);
    }
    const unexpected = this.takeBuffer(error);
    const text = this.takeBuffer(report);
    if (unexpected !== null) {
      throw new HawDBError("ffi", `HawDB ${action} succeeded with an error payload: ${unexpected}`);
    }
    return parseDumpReport<T>(text, fields);
  }

  exportDatabase(handle: NativePointer, destination: string): ExportReport {
    this.assertOpen();
    const lifetime = this.handles.get(handle);
    if (lifetime === undefined) {
      throw new HawDBError("invalid_handle", "HawDB database handle is not live");
    }
    const destinationBytes = encoder.encode(destination);
    if (destinationBytes.length === 0) {
      throw new TypeError("HawDB export destination must not be empty");
    }
    // Converting the path can run its toString/Symbol.toPrimitive, which may
    // close this database, open another at the same address, or unload the
    // library. Recheck both immediately before native dispatch.
    this.assertOpen();
    if (this.handles.get(handle) !== lifetime) {
      throw new HawDBError("invalid_handle", "HawDB database handle is not live");
    }
    return this.dumpCall<ExportReport>(exportReportFields, "export", (report, error) =>
      this.native.symbols.hawdb_export(handle, destinationBytes, destinationBytes, report, error),
    );
  }

  /**
   * Build a new database at `destination`, which must not exist yet, from the
   * logical dump at `source`. Nothing is published unless every file
   * verifies; open the result with `open`. Needs no open database.
   */
  import(source: string, destination: string): ImportReport {
    this.assertOpen();
    const sourceBytes = encoder.encode(source);
    const destinationBytes = encoder.encode(destination);
    if (sourceBytes.length === 0 || destinationBytes.length === 0) {
      throw new TypeError("HawDB import paths must not be empty");
    }
    // Path conversion may have unloaded the library.
    this.assertOpen();
    return this.dumpCall<ImportReport>(importReportFields, "import", (report, error) =>
      this.native.symbols.hawdb_import(
        sourceBytes,
        sourceBytes,
        destinationBytes,
        destinationBytes,
        report,
        error,
      ),
    );
  }

  closeDatabase(database: Database, handle: NativePointer): void {
    this.finalizer.unregister(database);
    if (!this.closed && this.handles.delete(handle)) {
      this.native.symbols.hawdb_close(handle);
    }
  }

  close(): void {
    if (this.closed) return;
    if (this.handles.size !== 0) {
      throw new HawDBError(
        "database_in_use",
        `cannot close HawDB shared library with ${this.handles.size} live database handle(s)`,
      );
    }
    this.native.close();
    this.closed = true;
  }

  [Symbol.dispose](): void {
    this.close();
  }
}

export class Database {
  private handle: NativePointer | null;

  constructor(
    private readonly library: HawDBLibrary,
    handle: NativePointer,
    token: typeof databaseConstructorToken,
  ) {
    if (token !== databaseConstructorToken) {
      throw new TypeError("Database handles must be created by HawDBLibrary");
    }
    this.handle = handle;
  }

  private liveHandle(): NativePointer {
    if (this.handle === null) {
      throw new HawDBError("closed", "HawDB database is closed");
    }
    return this.handle;
  }

  query(
    cypher: string,
    parameters: Record<string, HawDBParameter> = {},
  ): QueryResult {
    if (Array.isArray(parameters) || parameters === null || typeof parameters !== "object") {
      throw new TypeError("Cypher parameters must be an object");
    }
    return this.library.query(this.liveHandle(), false, cypher, parameters);
  }

  querySQL(sql: string, parameters: HawDBParameter[] = []): QueryResult {
    if (!Array.isArray(parameters)) {
      throw new TypeError("SQL parameters must be an array");
    }
    return this.library.query(this.liveHandle(), true, sql, parameters);
  }

  exec(cypher: string, parameters: Record<string, HawDBParameter> = {}): void {
    this.query(cypher, parameters);
  }

  /**
   * Write this database's current snapshot as a logical dump directory at
   * `destination`, which must not exist yet. The dump outlives the storage
   * format: a later library imports it with `library.import`.
   */
  export(destination: string): ExportReport {
    return this.library.exportDatabase(this.liveHandle(), destination);
  }

  close(): void {
    const handle = this.handle;
    if (handle === null) return;
    this.handle = null;
    this.library.closeDatabase(this, handle);
  }

  [Symbol.dispose](): void {
    this.close();
  }
}

export function loadHawDB(path: string): HawDBLibrary {
  return HawDBLibrary.load(path);
}
