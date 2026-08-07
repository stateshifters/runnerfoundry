#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/runnerfoundry-jit-launcher"

fail() {
  printf 'FAIL: %s\n' "$1" >&2
  exit 1
}

captured_environment=''
captured_parent_environment=''
captured_arguments=()
runuser() {
  captured_environment="${ACTIONS_RUNNER_INPUT_JITCONFIG-}"
  captured_parent_environment="${RUNNERFOUNDRY_TEST_MARKER-}"
  captured_arguments=("$@")
}

readonly secret='test-jit-secret'
export RUNNERFOUNDRY_TEST_MARKER='inherited-value'
run_runner "$secret"

[[ "$captured_environment" == "$secret" ]] || fail 'the runner did not inherit the JIT environment variable'
[[ "$captured_parent_environment" == 'inherited-value' ]] || fail 'the runner did not inherit the parent environment'
for argument in "${captured_arguments[@]}"; do
  [[ "$argument" != *"$secret"* ]] || fail 'the JIT configuration appeared in runner arguments'
done
[[ "${ACTIONS_RUNNER_INPUT_JITCONFIG+x}" != x ]] || fail 'the launcher leaked the JIT variable into its parent environment'

cleanup_calls=0
poweroff_calls=0
cleanup_runtime() {
  cleanup_calls=$((cleanup_calls + 1))
  return 7
}
power_off() {
  poweroff_calls=$((poweroff_calls + 1))
  return 9
}

set +e
finish 23
finish_status=$?
set -e

[[ "$finish_status" -eq 23 ]] || fail 'cleanup replaced the runner failure status'
[[ "$cleanup_calls" -eq 1 ]] || fail 'runtime cleanup was not attempted exactly once'
[[ "$poweroff_calls" -eq 1 ]] || fail 'power-off was not attempted after cleanup failed'

printf 'Ubuntu JIT launcher tests passed.\n'
