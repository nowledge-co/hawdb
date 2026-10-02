import assert from "node:assert/strict";
import test from "node:test";
import { createWorkerClient } from "./client.js";
import { createQueryHandler } from "./worker-runtime.js";

test("initialization and queries share one queue; ordinary errors preserve the instance", async () => {
  const calls = [];
  const output = [];
  let finishInitialization;
  const initialized = new Promise((resolve) => { finishInitialization = resolve; });
  let instances = 0;
  const handle = createQueryHandler({
    initialize: () => initialized,
    createBridge: () => {
      instances += 1;
      return { execute(query) {
        calls.push(query);
        return JSON.stringify(query === "invalid"
          ? { status: "error", error: { kind: "parse", message: "Invalid query." } }
          : { status: "ok", rows: [], columns: [] });
      } };
    },
    postMessage: (message) => output.push(message),
  });
  const queued = [
    { id: 1, type: "initialize" },
    { id: 2, type: "execute", query: "write" },
    { id: 3, type: "execute", query: "invalid" },
    { id: 4, type: "unsupported" },
    { id: 5, type: "execute", query: "read" },
  ].map((data) => handle({ data }));
  await Promise.resolve();
  assert.deepEqual(calls, []);
  finishInitialization();
  await Promise.all(queued);
  assert.equal(instances, 1);
  assert.deepEqual(calls, ["write", "invalid", "read"]);
  assert.deepEqual(output.map((message) => message.id), [1, 2, 3, 4, 5]);
  assert.equal(output[2].error.kind, "parse");
  assert.equal(output[3].error.kind, "protocol");
  assert.equal(output[4].status, "ok");
  assert.ok(output.every((message) => !message.restart_required));
});

test("a bridge trap permanently stops queued and later queries without resetting data", async () => {
  const output = [];
  let calls = 0;
  let instances = 0;
  const handle = createQueryHandler({
    initialize: () => {},
    createBridge: () => {
      instances += 1;
      return { execute() { calls += 1; throw new WebAssembly.RuntimeError("unreachable"); } };
    },
    postMessage: (message) => output.push(message),
  });
  await Promise.all([1, 2].map((id) => handle({ data: { id, type: "execute", query: "write" } })));
  await handle({ data: { id: 3, type: "initialize" } });
  assert.equal(calls, 1);
  assert.equal(instances, 1);
  assert.deepEqual(output.map((message) => message.error.kind), ["worker", "worker_unavailable", "worker_unavailable"]);
  assert.ok(output.every((message) => message.restart_required));
  assert.match(output[0].error.message, /outcome may be unknown/);
});

test("initialization failure is reported and never constructs a database", async () => {
  const output = [];
  const handle = createQueryHandler({
    initialize: () => { throw new Error("Cannot load WASM."); },
    createBridge: () => { assert.fail("Must not construct after failed initialization."); },
    postMessage: (message) => output.push(message),
  });
  await handle({ data: { id: 1, type: "initialize" } });
  await handle({ data: { id: 2, type: "execute", query: "write" } });
  assert.ok(output.every((message) => message.restart_required && message.status === "error"));
});

function fakeWorker() {
  return {
    sent: [],
    terminated: false,
    postMessage(message) { this.sent.push(message); },
    terminate() { this.terminated = true; },
  };
}

test("the shared client matches ids, ignores unknown responses and drains on fatal responses", async () => {
  const worker = fakeWorker();
  const received = [];
  const client = createWorkerClient(worker, { onResponse: (data) => received.push(data.id) });
  const first = client.request({ id: 999, type: "initialize" });
  const second = client.request({ type: "execute", query: "read" });
  const abandoned = client.request({ type: "execute", query: "later" });
  const abandonedCheck = assert.rejects(abandoned, /trap/);
  assert.deepEqual(worker.sent.map((data) => data.id), [1, 2, 3]);
  worker.onmessage({ data: { id: 999, status: "ok" } });
  worker.onmessage({ data: { id: 2, status: "ok" } });
  worker.onmessage({ data: { id: 1, status: "error", restart_required: true, error: { message: "trap" } } });
  assert.equal((await second).status, "ok");
  assert.equal((await first).restart_required, true);
  await abandonedCheck;
  assert.deepEqual(received, [2, 1]);
  assert.ok(client.failed && worker.terminated);
  await assert.rejects(client.request({ type: "execute", query: "never" }), /trap/);
  assert.equal(worker.sent.length, 3);
});

for (const event of ["onerror", "onmessageerror"]) {
  test(`the shared client stops and drains pending requests on ${event}`, async () => {
    const worker = fakeWorker();
    const client = createWorkerClient(worker);
    const pending = client.request({ type: "initialize" });
    const rejected = assert.rejects(pending, /failed/);
    worker[event]({ message: "Worker failed." });
    await rejected;
    assert.ok(client.failed && worker.terminated);
    await assert.rejects(client.request({ type: "initialize" }), /failed/);
  });
}

test("postMessage failure rejects the affected request and prevents future sends", async () => {
  const worker = fakeWorker();
  worker.postMessage = () => { throw new Error("Cannot clone request."); };
  const client = createWorkerClient(worker);
  await assert.rejects(client.request({ type: "initialize" }), /Cannot clone/);
  assert.ok(client.failed && worker.terminated);
  await assert.rejects(client.request({ type: "initialize" }), /Cannot clone/);
});
