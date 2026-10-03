import initialize, { QueryBridge } from "./pkg/wasm_playground.js";
import { createQueryHandler } from "./worker-runtime.js";

// One instance and one queue preserve write/read order, including during init.
self.onmessage = createQueryHandler({
  initialize,
  createBridge: () => new QueryBridge(),
  postMessage: (message) => self.postMessage(message),
});
