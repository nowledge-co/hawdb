#!/usr/bin/env bash

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
