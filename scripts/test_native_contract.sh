#!/usr/bin/env bash

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ANDROID_DIR="${ROOT_DIR}/android"
ANDROID_TEST_AVD="${IRIS_ANDROID_QA_AVD:-}"
PACKAGE_NAME="to.iris.chat.debug"
TEST_PACKAGE_NAME="${ANDROID_TEST_PACKAGE_NAME:-to.iris.chat.test}"
CONTRACT_CLASSES="to.iris.chat.core.AppManagerContractTest"
SMOKE_CLASSES="to.iris.chat.PikaLikeUiTest,to.iris.chat.account.AndroidKeystoreSecretStoreTest"

contains_line() {
  local needle="$1"
  local value
  while IFS= read -r value; do
    [[ "$value" == "$needle" ]] && return 0
  done
  return 1
}

select_android_test_avd() {
  if [[ -n "${ANDROID_TEST_AVD}" ]]; then
    printf '%s\n' "${ANDROID_TEST_AVD}"
    return 0
  fi

  local installed preferred avd
  installed="$("${ROOT_DIR}/scripts/run_android_emulators.sh" --list)"
  local preferred_avds=(
    Medium_Phone_API_36.1
    Pixel_9a
    Pixel_Fold
    GroupHardening_API_36_C
    GroupHardening_API_36_D
    SenderKey_API_36
    SenderKey_API_36_B
  )
  for preferred in "${preferred_avds[@]}"; do
    if contains_line "$preferred" <<<"$installed"; then
      printf '%s\n' "$preferred"
      return 0
    fi
  done
  while IFS= read -r avd; do
    if [[ -n "$avd" ]]; then
      printf '%s\n' "$avd"
      return 0
    fi
  done <<<"$installed"

  echo "Need an Android AVD for qa-native-contract; none are installed." >&2
  exit 1
}

resolve_serial() {
  if [[ -n "${IRIS_ANDROID_SERIAL:-}" ]]; then
    printf '%s\n' "${IRIS_ANDROID_SERIAL}"
    return 0
  fi
  if [[ -n "${ANDROID_SERIAL:-}" ]]; then
    printf '%s\n' "${ANDROID_SERIAL}"
    return 0
  fi

  local sdk_dir adb_path attached_serial
  sdk_dir="${ANDROID_HOME:-${ANDROID_SDK_ROOT:-}}"
  if [[ -z "${sdk_dir}" && -f "${ANDROID_DIR}/local.properties" ]]; then
    sdk_dir="$(sed -n 's/^sdk\.dir=//p' "${ANDROID_DIR}/local.properties" | tail -n 1)"
  fi
  adb_path="${sdk_dir}/platform-tools/adb"
  if [[ -n "${sdk_dir}" && -x "${adb_path}" ]]; then
    attached_serial="$("${adb_path}" devices -l 2>/dev/null |
      awk 'NR > 1 && $2 == "device" && $1 !~ /^emulator-/ { print $1; exit }')"
    if [[ -n "${attached_serial}" ]]; then
      printf '%s\n' "${attached_serial}"
      return 0
    fi
  fi

  local boot_output selected_avd
  selected_avd="$(select_android_test_avd)"
  boot_output="$("${ROOT_DIR}/scripts/run_android_emulators.sh" "${selected_avd}")"
  printf '%s\n' "${boot_output}" | awk 'NR == 1 { print $2 }'
}

android_sdk_dir() {
  local sdk_dir
  sdk_dir="${ANDROID_HOME:-${ANDROID_SDK_ROOT:-}}"
  if [[ -z "${sdk_dir}" && -f "${ANDROID_DIR}/local.properties" ]]; then
    sdk_dir="$(sed -n 's/^sdk\.dir=//p' "${ANDROID_DIR}/local.properties" | tail -n 1)"
  fi
  printf '%s\n' "${sdk_dir}"
}

reset_android_app_state() {
  local serial="$1"
  local sdk_dir adb_path
  sdk_dir="$(android_sdk_dir)"
  adb_path="${sdk_dir}/platform-tools/adb"
  if [[ -z "${sdk_dir}" || ! -x "${adb_path}" ]]; then
    return 0
  fi

  "${adb_path}" -s "${serial}" shell am force-stop "${PACKAGE_NAME}" >/dev/null 2>&1 || true
  "${adb_path}" -s "${serial}" shell am force-stop "${TEST_PACKAGE_NAME}" >/dev/null 2>&1 || true
  "${adb_path}" -s "${serial}" shell pm clear "${PACKAGE_NAME}" >/dev/null 2>&1 || true
  "${adb_path}" -s "${serial}" shell pm clear "${TEST_PACKAGE_NAME}" >/dev/null 2>&1 || true
}

run_filtered_android_test() {
  local serial="$1"
  local classes="$2"

  reset_android_app_state "${serial}"
  if ! (
    cd "${ANDROID_DIR}"
    ANDROID_SERIAL="${serial}" \
      ./gradlew \
      :app:connectedDebugAndroidTest \
      -Pandroid.testInstrumentationRunnerArguments.class="${classes}"
  ); then
    echo "Android instrumentation failed for ${classes}; resetting app state and retrying once." >&2
    reset_android_app_state "${serial}"
    (
      cd "${ANDROID_DIR}"
      ANDROID_SERIAL="${serial}" \
        ./gradlew \
        :app:connectedDebugAndroidTest \
        -Pandroid.testInstrumentationRunnerArguments.class="${classes}"
    )
  fi
  reset_android_app_state "${serial}"
}

if [[ "${IRIS_SKIP_FAST:-0}" != "1" ]]; then
  "${ROOT_DIR}/scripts/test_fast.sh"
fi

ANDROID_SERIAL_VALUE="$(resolve_serial)"
if [[ -z "${ANDROID_SERIAL_VALUE}" ]]; then
  echo "Failed to resolve an Android emulator serial for qa-native-contract." >&2
  exit 1
fi

run_filtered_android_test "${ANDROID_SERIAL_VALUE}" "${CONTRACT_CLASSES}"
run_filtered_android_test "${ANDROID_SERIAL_VALUE}" "${SMOKE_CLASSES}"
