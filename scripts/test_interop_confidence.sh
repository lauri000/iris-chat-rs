#!/usr/bin/env bash

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

split_words() {
  local value="$1"
  local word
  for word in ${value}; do
    printf '%s\n' "$word"
  done
}

contains_line() {
  local needle="$1"
  local value
  while IFS= read -r value; do
    [[ "$value" == "$needle" ]] && return 0
  done
  return 1
}

add_unique() {
  local item="$1"
  local existing
  [[ -n "$item" ]] || return 0
  for existing in "${SELECTED_AVDS[@]:-}"; do
    [[ "$existing" == "$item" ]] && return 0
  done
  SELECTED_AVDS+=("$item")
}

select_interop_avds() {
  local installed preferred avd
  installed="$("${ROOT_DIR}/scripts/run_android_emulators.sh" --list)"
  SELECTED_AVDS=()

  if [[ -n "${IRIS_ANDROID_INTEROP_AVDS:-}" ]]; then
    while IFS= read -r avd; do
      add_unique "$avd"
    done < <(split_words "$IRIS_ANDROID_INTEROP_AVDS")
  else
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
        add_unique "$preferred"
      fi
      [[ ${#SELECTED_AVDS[@]} -ge 3 ]] && break
    done
    while [[ ${#SELECTED_AVDS[@]} -lt 3 ]] && IFS= read -r avd; do
      add_unique "$avd"
    done <<<"$installed"
  fi

  if [[ ${#SELECTED_AVDS[@]} -lt 3 ]]; then
    echo "Need at least three Android AVDs for interop confidence; found ${#SELECTED_AVDS[@]}." >&2
    echo "Installed AVDs:" >&2
    printf '%s\n' "$installed" >&2
    exit 1
  fi
}

boot_interop_android_devices() {
  local boot_output line serial
  select_interop_avds
  boot_output="$("${ROOT_DIR}/scripts/run_android_emulators.sh" --headless "${SELECTED_AVDS[@]:0:3}")"
  printf '%s\n' "$boot_output"

  BOOTED_SERIALS=()
  while IFS= read -r line; do
    serial="$(awk '{print $2}' <<<"$line")"
    [[ -n "$serial" ]] && BOOTED_SERIALS+=("$serial")
  done <<<"$boot_output"

  if [[ ${#BOOTED_SERIALS[@]} -lt 3 ]]; then
    echo "Expected three booted Android serials; got ${#BOOTED_SERIALS[@]}." >&2
    exit 1
  fi
}

if [[ -z "${ANDROID_ADMIN_SERIAL:-}" ||
      -z "${ANDROID_MEMBER_SERIAL:-}" ||
      -z "${PRIMARY_SERIAL:-}" ||
      -z "${LINKED_SERIAL:-}" ||
      -z "${ADMIN_SERIAL:-}" ||
      -z "${SERIAL_A:-}" ||
      -z "${SERIAL_B:-}" ||
      -z "${SERIAL_C:-}" ]]; then
  boot_interop_android_devices
  export ANDROID_ADMIN_SERIAL="${ANDROID_ADMIN_SERIAL:-${BOOTED_SERIALS[0]}}"
  export ANDROID_MEMBER_SERIAL="${ANDROID_MEMBER_SERIAL:-${BOOTED_SERIALS[1]}}"
  export PRIMARY_SERIAL="${PRIMARY_SERIAL:-${BOOTED_SERIALS[0]}}"
  export LINKED_SERIAL="${LINKED_SERIAL:-${BOOTED_SERIALS[1]}}"
  export ADMIN_SERIAL="${ADMIN_SERIAL:-${BOOTED_SERIALS[2]}}"
  export SERIAL_A="${SERIAL_A:-${BOOTED_SERIALS[0]}}"
  export SERIAL_B="${SERIAL_B:-${BOOTED_SERIALS[1]}}"
  export SERIAL_C="${SERIAL_C:-${BOOTED_SERIALS[2]}}"
fi

"${ROOT_DIR}/scripts/mixed_platform_group_chat_matrix.sh"
"${ROOT_DIR}/scripts/group_chat_restore_smoke.sh"
"${ROOT_DIR}/scripts/linked_device_relay_matrix.sh"
