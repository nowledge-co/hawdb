#!/usr/bin/env bash

set -euo pipefail

if [[ $# -eq 0 ]]; then
  echo "usage: $0 <query-result>..." >&2
  exit 2
fi

for query_result in "$@"; do
  if [[ -s "$query_result" ]]; then
    echo "forbidden dependency path:" >&2
    cat "$query_result" >&2
    exit 1
  fi
done
