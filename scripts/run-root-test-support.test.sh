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

runner="${TEST_SRCDIR:-}/${TEST_WORKSPACE:-}/scripts/run-root-test-support.sh"
if [[ ! -f "${runner}" ]]; then
  runner="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/run-root-test-support.sh"
fi
fixture="$(mktemp -d "${TEST_TMPDIR:-${TMPDIR:-/tmp}}/hawdb-test-support-fixture.XXXXXX")"
trap 'rm -rf "${fixture}"' EXIT

# binary <name> <case>...: lists its cases, or prints the arguments it ran with.
binary() {
  local path="${fixture}/$1"
  shift
  {
    echo '#!/usr/bin/env bash'
    echo 'if [[ "${1:-}" == --list ]]; then'
    printf '  echo %q\n' "${@/%/: test}"
    echo 'else printf "%s\n" "$@"; exit "${FIXTURE_STATUS:-0}"; fi'
  } >"${path}"
  chmod +x "${path}"
}

reads=api::tests::immutable_retirement_reads::cold
search=api::search_projection_consumer::tests::stationary::search_artifact_reads_survive_a_paused_immutable_retirement
leftovers=api::tests::power_loss::checkpoint_leftovers::manual
temporaries=api::tests::power_loss::checkpoint_temporaries::manual
excluded=api::tests::power_loss::interrupted_admission_alias_mount_keeps_authoritative_schema_and_data
new=api::tests::future::gated
shared=api::tests::ordinary

binary default "${shared}"
binary gated "${shared}" "${reads}" "${search}" "${leftovers}" "${temporaries}" "${excluded}" "${new}"
"${runner}" "${fixture}/gated" "${fixture}/default" --nocapture >"${fixture}/out"
expected="$(printf '%s\n' "${reads}" "${search}" "${leftovers}" "${temporaries}" "${new}" | LC_ALL=C sort)"
actual="$(grep '::' "${fixture}/out" | LC_ALL=C sort)"
[[ "${actual}" == "${expected}" ]] || { echo "selected: ${actual}" >&2; exit 1; }
grep -qx -- --exact "${fixture}/out"
grep -qx -- --nocapture "${fixture}/out"

# A failing case fails the lane.
status=0
FIXTURE_STATUS=101 "${runner}" "${fixture}/gated" "${fixture}/default" >/dev/null || status=$?
[[ "${status}" == 101 ]]

# Each required family must be present; so must a difference at all.
for missing in "${reads}" "${search}" "${leftovers}"; do
  remaining=()
  for case_name in "${reads}" "${search}" "${leftovers}"; do
    [[ "${case_name}" == "${missing}" ]] || remaining+=("${case_name}")
  done
  binary partial "${shared}" "${remaining[@]}"
  if "${runner}" "${fixture}/partial" "${fixture}/default" 2>"${fixture}/error"; then exit 1; fi
  grep -q 'test-support lane has no case of' "${fixture}/error"
done
if "${runner}" "${fixture}/default" "${fixture}/default" 2>/dev/null; then exit 1; fi

# An executable that cannot list its cases is an error, not an empty lane.
printf '#!/usr/bin/env bash\nexit 3\n' >"${fixture}/broken"
chmod +x "${fixture}/broken"
if "${runner}" "${fixture}/gated" "${fixture}/broken" 2>/dev/null; then exit 1; fi
echo PASS
