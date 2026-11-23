// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

import assert from "node:assert/strict";
import { mkdtempSync, rmSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { command, installConsumer } from "./consumer.mjs";

test("a packed, installed consumer resolves its optional prebuilt addon", (t) => {
  const directory = mkdtempSync(join(tmpdir(), "hawdb-node-package-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const consumer = installConsumer(directory);
  writeFileSync(
    join(consumer, "check.mjs"),
    `
    import assert from 'node:assert/strict';
    import { openInMemory } from '@nowledge/hawdb-node';
    const db = openInMemory();
    assert.deepEqual(db.query('UNWIND [1] AS seed RETURN $value AS value', { value: 9223372036854775807n }).rows, [[9223372036854775807n]]);
    db.close();
    console.log('HAWDB_NODE_PACKAGE_OK');
  `,
  );
  assert.match(
    command(process.execPath, ["check.mjs"], consumer),
    /HAWDB_NODE_PACKAGE_OK/,
  );
  const main = JSON.parse(
    readFileSync(
      join(consumer, "node_modules/@nowledge/hawdb-node/package.json"),
    ),
  );
  const [name] = Object.keys(main.optionalDependencies);
  const manifestPath = join(consumer, "node_modules", name, "package.json");
  const manifest = JSON.parse(readFileSync(manifestPath));
  manifest.version = "0.0.0-mismatch";
  writeFileSync(manifestPath, JSON.stringify(manifest));
  assert.throws(
    () => command(process.execPath, ["check.mjs"], consumer),
    /0.0.0-mismatch/,
  );
  rmSync(join(consumer, "node_modules", name), { recursive: true });
  assert.throws(
    () => command(process.execPath, ["check.mjs"], consumer),
    /install optional dependencies/,
  );
});
