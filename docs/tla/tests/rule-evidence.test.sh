#!/usr/bin/env bash
set -euo pipefail

expected=(ok invariant_violation temporal_violation deadlock temporal_violation)
index=0
for path in "$@"; do
  evidence="${TEST_SRCDIR}/${TEST_WORKSPACE}/$path"
  test -s "$evidence/java-version.txt"
  test -s "$evidence/module.tla"
  test -s "$evidence/model.cfg"
  test "$(< "$evidence/tla2tools.sha256")" = 936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
  expected_args=$'-cleanup\n-workers\n2'
  if [[ "$index" -eq 4 ]]; then
    expected_args+=$'\n-lncheck\nfinal'
    grep -Fq 'Checking temporal properties for the complete state space' "$evidence/tlc.log"
    test "$(tail -n 1 "$evidence/result.txt")" = 13
  fi
  test "$(< "$evidence/tlc-args.txt")" = "$expected_args"
  test "$(head -n 1 "$evidence/result.txt")" = "${expected[$index]}"
  grep -q '^Finished in ' "$evidence/tlc.log"
  if [[ "$index" -eq 0 ]]; then
    test "$(tail -n 1 "$evidence/result.txt")" = 0
    grep -Fq 'Model checking completed. No error has been found.' "$evidence/tlc.log"
    ! grep -q '^Error:' "$evidence/tlc.log"
  else
    test "$(tail -n 1 "$evidence/result.txt")" != 0
    grep -q '^Error:' "$evidence/tlc.log"
  fi
  index=$((index + 1))
done
test "$index" -eq 5
