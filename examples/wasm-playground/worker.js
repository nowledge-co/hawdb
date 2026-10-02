import initialize, { QueryBridge } from "./pkg/wasm_playground.js";

// One instance and one queue preserve write/read order, including during init.
const ready = initialize().then(() => new QueryBridge());
let queue = Promise.resolve();

self.onmessage = ({ data }) => {
  queue = queue.then(async () => {
    try {
      const bridge = await ready;
      if (data.type === "initialize") {
        self.postMessage({ id: data.id, status: "ready" });
      } else if (data.type === "execute" && typeof data.query === "string") {
        self.postMessage({ id: data.id, ...JSON.parse(bridge.execute(data.query)) });
      } else {
        self.postMessage({ id: data.id, status: "error", error: { kind: "protocol", message: "Expected an execute request with query text." } });
      }
    } catch (error) {
      self.postMessage({ id: data.id, status: "error", error: { kind: "worker", message: String(error) } });
    }
  });
};
