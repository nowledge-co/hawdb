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

// One owner per database path. The renderer receives values, never native handles.
const binding = import("@nowledge/hawdb-node");
let database;

process.parentPort.on("message", async ({ data: request }) => {
  try {
    const hawdb = await binding;
    let value;
    switch (request.operation) {
      case "open":
        if (database) throw new Error("Database is already open");
        database = hawdb.open(request.path);
        value = hawdb.version();
        break;
      case "initialize":
        database.querySQL(
          "CREATE TABLE notes (id BIGINT PRIMARY KEY, body TEXT)",
        );
        break;
      case "put":
        database.transaction((tx) => {
          tx.querySQL("INSERT INTO notes (id, body) VALUES ($1, $2)", [
            request.id,
            request.body,
          ]);
          tx.exec("CREATE (:Note {id: $id, body: $body})", {
            id: request.id,
            body: request.body,
          });
        });
        break;
      case "list":
        value = database
          .querySQL("SELECT id, body FROM notes ORDER BY id LIMIT 50")
          .dicts();
        break;
      case "graph":
        value = database
          .query(
            "MATCH (n:Note) RETURN n.id AS id, n.body AS body ORDER BY id LIMIT 50",
          )
          .dicts();
        break;
      case "close":
        database?.close();
        database = undefined;
        break;
      default:
        throw new Error(`Unknown database operation ${request.operation}`);
    }
    process.parentPort.postMessage({ requestId: request.requestId, value });
  } catch (error) {
    process.parentPort.postMessage({
      requestId: request.requestId,
      error: { kind: error.kind, message: error.message },
    });
  }
});
