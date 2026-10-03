#!/usr/bin/env bash
# Copyright 2026 Nowledge
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

test_output="$(mktemp)"
trap 'rm -f "$test_output"' EXIT

# Cargo succeeds for empty filters and fully ignored suites. A required stage
# must execute a passing test as well as preserve Cargo's failure status.
cargo test "$@" 2>&1 | tee "$test_output"
if ! awk '
  /^test result: ok\. [1-9][0-9]* passed;/ { found = 1 }
  END { exit !found }
' "$test_output"; then
  echo "required Cargo test selection executed no passing tests" >&2
  exit 1
fi
