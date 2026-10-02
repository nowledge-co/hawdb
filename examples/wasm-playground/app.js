const worker = new Worker(new URL("./worker.js", import.meta.url), { type: "module" });
const form = document.querySelector("form");
const query = document.querySelector("textarea");
const button = document.querySelector("button[type=submit]");
const status = document.querySelector("#status");
const result = document.querySelector("#result");
const pending = new Map();
let nextId = 0;
let failed = false;

function request(message) {
  return new Promise((resolve, reject) => {
    if (failed) return reject(new Error("Reload to restart the query Worker."));
    const id = ++nextId;
    pending.set(id, { resolve, reject });
    worker.postMessage({ id, ...message });
  });
}

worker.onmessage = ({ data }) => {
  const entry = pending.get(data.id);
  if (!entry) return;
  pending.delete(data.id);
  entry.resolve(data);
};

function workerFailure(event) {
  failed = true;
  button.disabled = true;
  status.textContent = "Worker unavailable. Reload to restart.";
  for (const entry of pending.values()) entry.reject(new Error(event.message || "Worker failed."));
  pending.clear();
}
worker.onerror = workerFailure;
worker.onmessageerror = workerFailure;

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (button.disabled) return;
  button.disabled = true;
  status.textContent = "Running locally in your browser…";
  try {
    const output = await request({ type: "execute", query: query.value });
    status.textContent = output.status === "ok" ? `Complete · ${output.rows.length} result rows` : "Query failed";
    result.textContent = JSON.stringify(output, null, 2);
  } catch (error) {
    result.textContent = String(error);
  } finally {
    button.disabled = failed;
  }
});

for (const example of document.querySelectorAll("[data-query]")) {
  example.addEventListener("click", () => {
    query.value = example.dataset.query;
    query.focus();
  });
}

try {
  const output = await request({ type: "initialize" });
  if (output.status !== "ready") throw new Error(output.error.message);
  status.textContent = "Ready · empty in-memory database";
  button.disabled = false;
} catch (error) {
  failed = true;
  status.textContent = "Could not load HawDB";
  result.textContent = String(error);
}
