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

if [[ $# -eq 0 ]]; then
  echo "usage: $0 <query-result>..." >&2
  exit 2
fi

for query_result in "$@"; do
  if [[ ! -f "$query_result" || ! -r "$query_result" ]]; then
    echo "missing or unreadable query result: $query_result" >&2
    exit 1
  fi
  if [[ -s "$query_result" ]]; then
    echo "forbidden dependency path:" >&2
    cat "$query_result" >&2
    exit 1
  fi
done
