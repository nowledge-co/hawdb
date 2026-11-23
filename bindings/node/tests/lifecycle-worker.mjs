// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

import { workerData } from "node:worker_threads";
import { open } from "../dist/index.js";

// Deliberately omit close: Node-API must finalize this environment's handle.
const database = open(workerData.path);
database.exec("CREATE (:Note {body: 'worker'})");
