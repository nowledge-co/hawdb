#!/usr/bin/env bash
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
if [[ "$5" == */SkeinCowPagePublication.tla ]]; then
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
