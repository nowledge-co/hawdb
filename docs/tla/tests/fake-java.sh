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
test "$2" = -metadir
shift 3
test "$1" = -workers
test "$2" = auto
test "$3" = -config
test -f "$4"
test -f "$5"
printf '%s\n' "$5" >> "$TLA_TEST_INVOCATIONS"
if [[ "${TLA_TEST_FAIL:-false}" == true ]]; then
  printf 'Error: fixture failure\n'
  exit 9
fi
printf '%s\n' 'Model checking completed. No error has been found.' 'Finished in 00s'
