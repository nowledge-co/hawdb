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

import { CString, dlopen, type Pointer } from "bun:ffi";

export { CString };
export type NativePointer = bigint | Pointer;
export type NativeLibrary = ReturnType<typeof openNativeLibrary>;

const symbols = {
  hawdb_open: {
    args: ["buffer", "buffer_length", "buffer", "buffer_length", "buffer"],
    returns: "ptr",
  },
  hawdb_open_in_memory: { args: ["buffer"], returns: "ptr" },
  hawdb_close: { args: ["ptr"], returns: "void" },
  hawdb_query: {
    args: [
      "ptr",
      "buffer",
      "buffer_length",
      "buffer",
      "buffer_length",
      "buffer",
      "buffer",
    ],
    returns: "bool",
  },
  hawdb_query_sql: {
    args: [
      "ptr",
      "buffer",
      "buffer_length",
      "buffer",
      "buffer_length",
      "buffer",
      "buffer",
    ],
    returns: "bool",
  },
  hawdb_begin_transaction: { args: ["ptr", "buffer"], returns: "bool" },
  hawdb_transaction_query: {
    args: [
      "ptr",
      "buffer",
      "buffer_length",
      "buffer",
      "buffer_length",
      "buffer",
      "buffer",
    ],
    returns: "bool",
  },
  hawdb_transaction_query_sql: {
    args: [
      "ptr",
      "buffer",
      "buffer_length",
      "buffer",
      "buffer_length",
      "buffer",
      "buffer",
    ],
    returns: "bool",
  },
  hawdb_commit_transaction: { args: ["ptr", "buffer", "buffer"], returns: "bool" },
  hawdb_rollback_transaction: { args: ["ptr", "buffer"], returns: "bool" },
  hawdb_backup: {
    args: ["ptr", "buffer", "buffer_length", "buffer", "buffer"],
    returns: "bool",
  },
  hawdb_restore: {
    args: ["buffer", "buffer_length", "buffer", "buffer_length", "buffer", "buffer"],
    returns: "bool",
  },
  hawdb_scrub: { args: ["ptr", "buffer", "buffer"], returns: "bool" },
  hawdb_buffer_free: { args: ["buffer"], returns: "void" },
  hawdb_version: { args: ["buffer"], returns: "void" },
} as const;

export function openNativeLibrary(path: string) {
  return dlopen(path, symbols);
}
