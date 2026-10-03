import { createWorkerClient } from "./client.js";

const worker = new Worker(new URL("./worker.js", import.meta.url), { type: "module" });
const form = document.querySelector("form");
const query = document.querySelector("textarea");
const button = document.querySelector("button[type=submit]");
const status = document.querySelector("#status");
const result = document.querySelector("#result");
const client = createWorkerClient(worker, {
  onFailure() {
    button.disabled = true;
    status.textContent = "Worker unavailable. Reload to restart; data will be cleared.";
  },
});

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (button.disabled) return;
  button.disabled = true;
  status.textContent = "Running locally in your browser…";
  try {
    const output = await client.request({ type: "execute", query: query.value });
    if (!client.failed) status.textContent = output.status === "ok" ? `Complete · ${output.rows.length} result rows` : "Query failed";
    result.textContent = JSON.stringify(output, null, 2);
  } catch (error) {
    result.textContent = String(error);
  } finally {
    button.disabled = client.failed;
  }
});

for (const example of document.querySelectorAll("[data-query]")) {
  example.addEventListener("click", () => {
    query.value = example.dataset.query;
    query.focus();
  });
}

try {
  const output = await client.request({ type: "initialize" });
  if (output.status !== "ready") throw new Error(output.error.message);
  status.textContent = "Ready · empty in-memory database";
  button.disabled = false;
} catch (error) {
  client.close(error);
  status.textContent = "Could not load HawDB";
  result.textContent = String(error);
}
