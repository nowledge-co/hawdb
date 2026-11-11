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

// Child process for the transaction crash/reopen tests: stages or commits one
// transaction and then kills itself with SIGKILL, so no close, destructor or
// rollback runs. The parent reopens the database and checks what recovered.
import { loadHawDB } from "../../src/index.ts";

const [libraryPath, databasePath, scenario] = process.argv.slice(2);
const library = loadHawDB(libraryPath);
const database = library.open(databasePath);

function stage(transaction: import("../../src/index.ts").Transaction): void {
  transaction.querySQL("INSERT INTO events (id, kind) VALUES ($1, $2)", [1, "deposit"]);
  transaction.querySQL("UPDATE projection SET total = total + 5 WHERE id = 1");
  transaction.exec("CREATE (:Audit {event: 1})");
}

if (scenario === "uncommitted") {
  database.transaction((transaction) => {
    stage(transaction);
    console.log("STAGED");
    process.kill(process.pid, "SIGKILL");
  });
} else if (scenario === "committed") {
  database.transaction(stage);
  console.log("COMMITTED");
  process.kill(process.pid, "SIGKILL");
} else {
  throw new Error(`unknown scenario ${scenario}`);
}
