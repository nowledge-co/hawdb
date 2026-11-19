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

const { app, utilityProcess } = require("electron");
const assert = require("node:assert/strict");
const { mkdtempSync, rmSync } = require("node:fs");
const { tmpdir } = require("node:os");
const { join } = require("node:path");

app
  .whenReady()
  .then(async () => {
    const root = mkdtempSync(join(tmpdir(), "hawdb-electron-runtime-"));
    const child = utilityProcess.fork(join(__dirname, "worker.cjs"), [], {
      serviceName: "HawDB",
    });
    const pending = new Map();
    let requestId = 0;
    child.on("message", (message) => {
      const request = pending.get(message.requestId);
      if (!request) return;
      pending.delete(message.requestId);
      if (message.error)
        request.reject(new Error(JSON.stringify(message.error)));
      else request.resolve(message.value);
    });
    child.on("exit", (code) => {
      for (const request of pending.values())
        request.reject(new Error(`Database process exited: ${code}`));
      pending.clear();
    });
    const request = (operation, values = {}) =>
      new Promise((resolve, reject) => {
        const id = ++requestId;
        pending.set(id, { resolve, reject });
        child.postMessage({ requestId: id, operation, ...values });
      });
    try {
      const path = join(root, "database");
      const version = await request("open", { path });
      assert.match(version, /^hawdb \d+\.\d+\.\d+/);
      await request("initialize");
      await request("put", { id: 9223372036854775807n, body: "from Electron" });
      const expected = [{ id: 9223372036854775807n, body: "from Electron" }];
      assert.deepEqual(await request("list"), expected);
      assert.deepEqual(await request("graph"), expected);
      await request("close");
      await request("open", { path });
      assert.deepEqual(await request("list"), expected);
      assert.deepEqual(await request("graph"), expected);
      await request("close");
      console.log(
        "HAWDB_ELECTRON_OK",
        JSON.stringify({ packaged: app.isPackaged, version }),
      );
      child.kill();
      rmSync(root, { recursive: true, force: true });
      app.exit(0);
    } catch (error) {
      console.error(error);
      child.kill();
      // Preserve the database on failure for diagnosis.
      console.error(`Preserved Electron fixture: ${root}`);
      app.exit(1);
    }
  })
  .catch((error) => {
    console.error(error);
    app.exit(1);
  });
