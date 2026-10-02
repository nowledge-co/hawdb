// Shared by the page and browser smoke so they exercise the same wire protocol.
export function createWorkerClient(worker, { onResponse = () => {}, onFailure = () => {} } = {}) {
  const pending = new Map();
  let nextId = 0;
  let failure;

  function fail(error) {
    if (failure) return;
    failure = error instanceof Error ? error : new Error(String(error));
    worker.terminate();
    for (const entry of pending.values()) entry.reject(failure);
    pending.clear();
    onFailure(failure);
  }

  worker.onmessage = ({ data }) => {
    const entry = pending.get(data.id);
    if (!entry) return;
    pending.delete(data.id);
    onResponse(data);
    entry.resolve(data);
    if (data.restart_required) fail(new Error(data.error.message));
  };
  const workerFailure = (event) => fail(new Error(event.message || "Worker failed. Reload to restart."));
  worker.onerror = workerFailure;
  worker.onmessageerror = workerFailure;

  return {
    get failed() { return Boolean(failure); },
    request(message) {
      if (failure) return Promise.reject(failure);
      return new Promise((resolve, reject) => {
        const id = ++nextId;
        pending.set(id, { resolve, reject });
        try {
          worker.postMessage({ ...message, id });
        } catch (error) {
          fail(error);
        }
      });
    },
    close(error = new Error("Worker closed.")) { fail(error); },
  };
}
