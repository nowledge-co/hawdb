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

/*
 * Minimal host linked against a prebuilt HawDB C ABI archive. See
 * docs/PREBUILT_LIBRARIES.md for the compile and link commands. The release
 * workflow builds and runs it against every packaged library.
 */

#include <stdio.h>
#include <string.h>

#include "hawdb_ffi.h"

static int fail(const char *what, struct HawdbBuffer *error) {
  fprintf(stderr, "%s failed: %.*s\n", what, (int)error->len, error->ptr ? error->ptr : "");
  hawdb_buffer_free(error);
  return 1;
}

int main(void) {
  struct HawdbBuffer version = {0};
  hawdb_version(&version);
  printf("%.*s\n", (int)version.len, version.ptr);
  hawdb_buffer_free(&version);

  struct HawdbBuffer error = {0};
  struct HawdbDatabase *database = hawdb_open_in_memory(&error);
  if (database == NULL) {
    return fail("hawdb_open_in_memory", &error);
  }

  const char *cypher = "UNWIND [41] AS x RETURN x + 1 AS answer";
  struct HawdbBuffer result = {0};
  if (!hawdb_query(database, cypher, strlen(cypher), NULL, 0, &result, &error)) {
    hawdb_close(database);
    return fail("hawdb_query", &error);
  }
  printf("%.*s\n", (int)result.len, result.ptr);
  hawdb_buffer_free(&result);
  hawdb_close(database);
  return 0;
}
