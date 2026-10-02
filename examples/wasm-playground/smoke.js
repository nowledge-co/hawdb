// Run in a real browser after building the WASM artifact:
// await (await import("./smoke.js")).runSmoke()
export async function runSmoke() {
  const worker = new Worker(new URL("./worker.js", import.meta.url), { type: "module" });
  const pending = new Map();
  const responseOrder = [];
  let nextId = 0;
  const assert = (condition, message) => { if (!condition) throw new Error(message); };
  const timeout = setTimeout(() => {
    for (const entry of pending.values()) entry.reject(new Error("Browser smoke timed out."));
    pending.clear();
  }, 30000);
  worker.onmessage = ({ data }) => {
    responseOrder.push(data.id);
    const entry = pending.get(data.id);
    pending.delete(data.id);
    entry?.resolve(data);
  };
  worker.onerror = (event) => {
    for (const entry of pending.values()) entry.reject(new Error(event.message));
    pending.clear();
  };
  const send = (message) => new Promise((resolve, reject) => {
    const id = ++nextId;
    pending.set(id, { resolve, reject });
    worker.postMessage({ id, ...message });
  });
  const query = (text) => send({ type: "execute", query: text });
  const value = (output, name) => output.rows[0][output.columns.indexOf(name)];
  try {
    // Submit before initialization completes to exercise the actual queue.
    const [ready, write, read, invalid, traversal] = await Promise.all([
      send({ type: "initialize" }),
      query("CREATE (:Memory {id: 9223372036854775807, title: 'Browser memory'})-[:MENTIONS]->(:Entity {name: 'HawDB'})"),
      query("MATCH (m:Memory) RETURN m.id AS id, m.title AS title"),
      query("this is not Cypher"),
      query("MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN e.name AS name"),
    ]);
    assert(ready.status === "ready" && write.status === "ok", "Initialization/write failed.");
    assert(JSON.stringify(responseOrder) === "[1,2,3,4,5]", "Worker did not serialize requests.");
    assert(value(read, "id").value === "9223372036854775807", "int64 precision was lost.");
    assert(value(read, "id").type === "int64", "int64 type was lost.");
    assert(invalid.status === "error" && invalid.error.kind === "parse", "Missing parse error.");
    assert(value(traversal, "name").value === "HawDB", "Traversal/error recovery failed.");
    const graph = await query("MATCH (m:Memory) RETURN m AS node, NULL AS missing, true AS enabled, 1.5 AS ratio");
    assert(graph.status === "ok", "Graph/scalar query failed.");
    assert(value(graph, "node").type === "map", "Graph value did not serialize.");
    assert(value(graph, "missing").value === null, "Null value did not serialize.");
    assert(value(graph, "enabled").value === true, "Boolean value did not serialize.");
    assert(value(graph, "ratio").value === "1.5", "Float display value was lost.");
    const protocol = await send({ type: "unsupported" });
    assert(protocol.error.kind === "protocol", "Missing protocol error.");
    const oversized = await query("x".repeat(64 * 1024 + 1));
    assert(oversized.error.kind === "query_limit", "Missing input limit error.");
    for (let i = 0; i < 257; i++) {
      const output = await query(`CREATE (:Limit {id: ${i}})`);
      assert(output.status === "ok", "Limit fixture write failed.");
    }
    const limited = await query("MATCH (m:Limit) RETURN m.id AS id");
    assert(limited.status === "error" && !limited.rows, "Result cap returned partial rows.");
    assert(limited.error.message.includes("max_read_result_rows"), "Wrong result cap error.");
    const recovered = await query("MATCH (m:Limit) RETURN m.id AS id LIMIT 1");
    assert(recovered.status === "ok" && recovered.rows.length === 1, "Result cap poisoned state.");
    return {
      status: "passed",
      browser: navigator.userAgent,
      requests: nextId,
      checks: ["queued initialization", "serial writes/reads", "int64 precision", "traversal", "graph/scalar/null values", "parse/protocol recovery", "query input limit", "result limit without partial rows"],
    };
  } finally {
    clearTimeout(timeout);
    worker.terminate();
  }
}
