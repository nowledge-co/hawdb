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
export type Parameters = Record<string, HawDBValue>;
export type Report = Record<string, string | number | bigint | null>;
export interface OpenOptions {
  readOnly?: boolean;
  durability?: "sync_on_every_write" | "sync_on_checkpoint";
  maxReadResultRows?: number | bigint | null;
  maxReadIntermediateRows?: number | bigint | null;
  maxReadResultPayloadBytes?: number | bigint;
  relationalMonotonicAppendFastPath?: boolean;
}
export class HawDBError extends Error {
  readonly kind: string;
  readonly details: Record<string, unknown>;
  constructor(kind: string, message: string, details?: Record<string, unknown>);
}
/** An unsigned 64-bit count: `number` when safe, otherwise `bigint`. */
export type Count = number | bigint;
export type QueryCounterName =
  | "nodesCreated"
  | "nodesDeleted"
  | "relationshipsCreated"
  | "relationshipsDeleted"
  | "propertiesSet"
  | "labelsAdded"
  | "labelsRemoved"
  | "indexesAdded"
  | "indexesRemoved"
  | "constraintsAdded"
  | "constraintsRemoved";
/**
 * How many write operations one Cypher statement executed. Counts writes
 * executed, not data changed: `SET n.p = v` counts 1 in `propertiesSet` even
 * when `n.p` already equals `v`. A read, a pattern that matches nothing,
 * every SQL statement and a transaction commit report all zeros, but
 * `propertiesSet === 0` does not imply that nothing matched.
 * `labelsRemoved`, `indexesRemoved` and `constraintsRemoved` stay 0 until
 * the Cypher dialect parses `REMOVE` and `DROP`.
 */
export class QueryCounters {
  readonly nodesCreated: Count;
  readonly nodesDeleted: Count;
  readonly relationshipsCreated: Count;
  readonly relationshipsDeleted: Count;
  readonly propertiesSet: Count;
  readonly labelsAdded: Count;
  readonly labelsRemoved: Count;
  readonly indexesAdded: Count;
  readonly indexesRemoved: Count;
  readonly constraintsAdded: Count;
  readonly constraintsRemoved: Count;
  /** Absent fields are 0; the result is frozen. */
  constructor(values?: Partial<Record<QueryCounterName, Count>>);
  /** Whether the statement executed any write counted here. */
  containsUpdates(): boolean;
}
export class QueryResult {
  columns: string[];
  rows: HawDBValue[][];
  /** The write operations the statement executed; all zero for a read. */
  counters: QueryCounters;
  constructor(
    columns: string[],
    rows: HawDBValue[][],
    counters?: QueryCounters,
  );
  dicts(): Record<string, HawDBValue>[];
}
/** Valid only during the synchronous transaction callback. */
export interface Transaction {
  query(cypher: string, parameters?: Parameters): QueryResult;
  querySQL(sql: string, parameters?: HawDBValue[]): QueryResult;
  exec(cypher: string, parameters?: Parameters): void;
}
export class Database implements Transaction {
  private constructor();
  query(cypher: string, parameters?: Parameters): QueryResult;
  querySQL(sql: string, parameters?: HawDBValue[]): QueryResult;
  exec(cypher: string, parameters?: Parameters): void;
  /** Promises/thenables are refused and cause rollback. */
  transaction<T>(body: (transaction: Transaction) => T): T;
  backup(destination: string): Report;
  export(destination: string): Report;
  scrub(): Report;
  close(): void;
  [Symbol.dispose](): void;
}
export function open(path: string, options?: OpenOptions): Database;
export function openInMemory(): Database;
export function version(): string;
export function formatVersion(): number;
export function dumpFormatVersion(): number;
export function restore(source: string, destination: string): Report;
export function importDump(source: string, destination: string): Report;
export function verifyDump(path: string): Report;
export function confirmDumpPublication(path: string): void;
