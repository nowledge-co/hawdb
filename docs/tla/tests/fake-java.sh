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
if [[ "$1" == "-version" ]]; then
  printf 'fixture Java runtime\n' >&2
  exit
fi
test "$1" = -XX:+UseParallelGC
test "$2" = -jar
shift 3
test "$1" = -cleanup
test "$2" = -workers
test "$3" = auto
shift 3
final_liveness=false
if [[ "$1" == -lncheck ]]; then
  test "$2" = final
  final_liveness=true
  shift 2
fi
test "$1" = -metadir
test "$3" = -config
test -f "$4"
test -f "$5"
if [[ "$5" == */HawDBCowPagePublication.tla ]]; then
  test "$final_liveness" = true
  printf '%s\n' 'Checking temporal properties for the complete state space with 2 total distinct states'
else
  test "$final_liveness" = false
fi
printf '%s\n' "$5" >> "$TLA_TEST_INVOCATIONS"
if [[ "${TLA_TEST_FAIL:-false}" == true ]]; then
  printf 'Error: fixture failure\n'
  exit 9
fi
printf '%s\n' 'Model checking completed. No error has been found.' 'Finished in 00s'
