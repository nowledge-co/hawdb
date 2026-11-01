#!/usr/bin/env bash
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
if [[ $# -lt 2 ]]; then
  echo "usage: $0 <unit-test-binary> <exact-case> [libtest arguments]" >&2
  exit 64
fi
unit_binary="$1"
endpoint_case="$2"
shift 2
inventory="$(mktemp "${TEST_TMPDIR:-${TMPDIR:-/tmp}}/hawdb-endpoint-inventory.XXXXXX")"
trap 'rm -f "$inventory"' EXIT
"$unit_binary" --list >"$inventory"
arguments=("$unit_binary" "$endpoint_case" --exact --test-threads=1 "$@")
owned_cases=0
general_cases=0
while IFS= read -r entry; do
  case "$entry" in
    *": test") case_name="${entry%: test}" ;;
    *) continue ;;
  esac
  if [[ "$case_name" == "$endpoint_case" ]]; then
    owned_cases=$((owned_cases + 1))
  else
    # The ordinary target's --skip is a substring filter. Refuse an inventory
    # where it would suppress another case rather than silently losing coverage.
    if [[ "$case_name" == *"$endpoint_case"* ]]; then
      echo "ambiguous endpoint case: $case_name contains $endpoint_case" >&2
      exit 1
    fi
    general_cases=$((general_cases + 1))
  fi
done <"$inventory"
if [[ $owned_cases -ne 1 || $general_cases -eq 0 ]]; then
  echo "endpoint inventory must contain exactly one selected case and nonempty general coverage" >&2
  exit 1
fi
# Validate the effective native selection, including caller filters. This keeps
# the command line bounded and prevents ORed filters from escaping isolation.
"${arguments[@]}" --list >"$inventory"
selected_cases=0
while IFS= read -r entry; do
  case "$entry" in
    *": test") case_name="${entry%: test}" ;;
    *) continue ;;
  esac
  if [[ "$case_name" != "$endpoint_case" ]]; then
    echo "caller filters escape endpoint ownership: $case_name" >&2
    exit 1
  fi
  selected_cases=$((selected_cases + 1))
done <"$inventory"
if [[ $selected_cases -ne 1 ]]; then
  echo "endpoint filters must select exactly one case" >&2
  exit 1
fi
rm -f "$inventory"
trap - EXIT
exec "${arguments[@]}"
