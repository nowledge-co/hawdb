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

import {
  open,
  openInMemory,
  type HawDBValue,
  type Transaction,
} from "../src/index.js";

using memory = openInMemory();
const value: HawDBValue = memory.querySQL("SELECT $1 AS id", [
  9223372036854775807n,
]).rows[0][0];
const persisted = open("/unused/typecheck-only", { maxReadResultRows: 1n });
persisted.transaction((transaction: Transaction) =>
  transaction.exec("CREATE (:Note {id: $id})", { id: value }),
);
persisted.close();
// @ts-expect-error Cypher uses named parameters, not SQL positional arrays.
memory.query("MATCH (n) RETURN n", [1]);
// @ts-expect-error A misspelt budget option must not become part of the API.
open("/unused/typecheck-only", { maxReadRows: 1 });
