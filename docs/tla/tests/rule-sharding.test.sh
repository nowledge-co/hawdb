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

index=-1
counts=(0 0 0)
seen=" "
for path in "$@"; do
  case "$path" in
    --shard=[012]) index="${path#--shard=}"; continue ;;
  esac
  case "$index:${path##*/}" in
    0:positive_check.run.tlc-evidence | 0:deadlock_check.run.tlc-evidence | \
      1:invariant_check.run.tlc-evidence | 2:temporal_check.run.tlc-evidence) ;;
    *) printf 'unexpected shard assignment: %s:%s\n' "$index" "$path" >&2; exit 1 ;;
  esac
  [[ "$seen" != *" $path "* ]]
  seen+="$path "
  test -s "${TEST_SRCDIR}/${TEST_WORKSPACE}/$path/result.txt"
  counts[$index]=$((${counts[$index]} + 1))
done
test "${counts[*]}" = '2 1 1'
