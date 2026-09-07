#!/usr/bin/env bash
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
