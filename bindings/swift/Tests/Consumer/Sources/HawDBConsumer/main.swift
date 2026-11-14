// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

import HawDB

let database = try Database.inMemory()
let result = try database.query(
  "UNWIND [0] AS ignored RETURN $answer AS answer", parameters: ["answer": .int(42)])
precondition(result.columns == ["answer"] && result.rows == [[.int(42)]])
try database.close()
print("HawDB Swift consumer: OK")
