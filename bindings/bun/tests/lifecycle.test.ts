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

import { expect, test } from "bun:test";
import { fileURLToPath } from "node:url";

const fixture = fileURLToPath(new URL("./fixtures/lifecycle.ts", import.meta.url));

for (const scenario of [
  "cypher-close",
  "sql-close",
  "cypher-reuse",
  "sql-reuse",
  "query-library-close",
  "open-library-close",
  "healthy",
]) {
  test(`guards native lifetime: ${scenario}`, () => {
    // Module mocks must not replace the native bridge in the real-engine suite.
    const child = Bun.spawnSync([process.execPath, fixture, scenario], {
      stdout: "pipe",
      stderr: "pipe",
      timeout: 5_000,
    });
    expect(new TextDecoder().decode(child.stderr)).toBe("");
    expect(child.exitCode).toBe(0);
    expect(new TextDecoder().decode(child.stdout).trim()).toBe(`PASS ${scenario}`);
  });
}
