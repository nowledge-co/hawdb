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

export function createQueryHandler({ initialize, createBridge, postMessage }) {
  let failure;
  let queue = Promise.resolve();
  const ready = Promise.resolve().then(initialize).then(createBridge).catch((error) => {
    failure = new Error(String(error));
    return null;
  });

  function stopped(id, kind) {
    postMessage({
      id,
      status: "error",
      restart_required: true,
      error: {
        kind,
        message: `Worker stopped: ${String(failure)}. Query outcome may be unknown. Reload to restart; in-memory data will be lost.`,
      },
    });
  }

  return ({ data }) => {
    queue = queue.then(async () => {
      if (failure) return stopped(data?.id, "worker_unavailable");
      try {
        const bridge = await ready;
        if (failure) return stopped(data?.id, "worker_unavailable");
        if (data?.type === "initialize") {
          postMessage({ id: data.id, status: "ready" });
        } else if (data?.type === "execute" && typeof data.query === "string") {
          postMessage({ id: data.id, ...JSON.parse(bridge.execute(data.query)) });
        } else {
          postMessage({ id: data?.id, status: "error", error: { kind: "protocol", message: "Expected an execute request with query text." } });
        }
      } catch (error) {
        // A WASM trap or unexpected bridge exception leaves database invariants
        // uncertain. Never serve another query from this instance or silently reset.
        failure = new Error(String(error));
        stopped(data?.id, "worker");
      }
    });
    return queue;
  };
}
