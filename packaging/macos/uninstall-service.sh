#!/bin/bash
set -euo pipefail

# Unloads the SpareRunner LaunchDaemon and removes only the property list this
# installation published. Enrollment state, the runner journal, the package
# cache, and the dedicated non-login runner account and group are deliberately
# retained: a node identity is durable credential material, and this script can
# prove what the package installed but not what an operator still needs.
#
# Removing that state is a separate, explicit operator decision; the paths are
# listed in README.md.

readonly label="com.genm.sparerunner.agent"
readonly runner_account="sparerunner-runner-0"
readonly runner_group="sparerunner-runner-0"
readonly state_root_path="/Library/Application Support/SpareRunner"
readonly cache_parent_path="/Library/Caches/com.genm.sparerunner"
readonly plist_target_path="/Library/LaunchDaemons/${label}.plist"
readonly plist_source_arg="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)/launchd/${label}.plist}"

readonly test_root="${SPARERUNNER_MACOS_INSTALL_TEST_ROOT:-}"
readonly test_tools="${SPARERUNNER_MACOS_INSTALL_TEST_TOOLS:-}"
readonly test_enabled="${SPARERUNNER_MACOS_INSTALL_TESTING:-}"
if [[ -n "$test_root" || -n "$test_tools" || -n "$test_enabled" ]]; then
  if [[ "$test_enabled" != "1" ||
        "$EUID" -eq 0 ||
        "$test_root" != /* ||
        "$test_root" == "/" ||
        "$test_root" == */ ||
        "$test_root" == *"/../"* ||
        "$test_root" == *"/./"* ||
        ! -d "$test_root" ||
        -L "$test_root" ||
        ! -f "${test_root}/.sparerunner-installer-test-root" ||
        "$test_tools" != /* ||
        ! -d "$test_tools" ||
        -L "$test_tools" ]]; then
    echo "invalid macOS installer test boundary; root never accepts test indirection" >&2
    exit 1
  fi
fi

fail() {
  echo "$1" >&2
  exit 1
}

run_tool() {
  local name="$1"
  shift
  if [[ "$test_enabled" == "1" ]]; then
    "${test_tools}/${name}" "$@"
    local rc=$?
    return "$rc"
  fi
  case "$name" in
    cmp) /usr/bin/cmp "$@" ;;
    id) /usr/bin/id "$@" ;;
    launchctl) /bin/launchctl "$@" ;;
    rm) /bin/rm "$@" ;;
    stat) /usr/bin/stat "$@" ;;
    *)
      echo "unknown uninstaller tool: $name" >&2
      return 1
      ;;
  esac
}

rooted_path() {
  local logical_path="$1"
  if [[ "$test_enabled" == "1" ]]; then
    printf '%s%s' "$test_root" "$logical_path"
    return
  fi
  printf '%s' "$logical_path"
}

plist_target="$(rooted_path "$plist_target_path")"
readonly plist_target

if [[ "$(run_tool id -u)" != "0" || "$(run_tool id -g)" != "0" ]]; then
  fail "uninstall-service.sh must run as root:wheel"
fi

if [[ "$plist_source_arg" != /* ||
      "$plist_source_arg" == *"//"* ||
      "$plist_source_arg" == *"/../"* ||
      "$plist_source_arg" == *"/.." ||
      "$plist_source_arg" == *"/./"* ||
      "$plist_source_arg" == *"/." ||
      "$plist_source_arg" == */ ]]; then
  fail "launchd property list path must be canonical and absolute"
fi
if [[ ! -f "$plist_source_arg" || -L "$plist_source_arg" ]]; then
  fail "missing or unsafe package file: $plist_source_arg"
fi
readonly plist_source="$plist_source_arg"

stat_contract() {
  run_tool stat -f '%u:%g:%p' "$1"
}

plist_matches_package() {
  local path="$1"
  local actual links
  [[ -f "$path" && ! -L "$path" ]] || return 1
  actual="$(stat_contract "$path" 2>/dev/null)" || return 1
  links="$(run_tool stat -f '%l' "$path" 2>/dev/null)" || return 1
  [[ "$actual" == "0:0:100600" &&
    "$links" =~ ^[0-9]+$ &&
    "$links" -ge 1 &&
    "$links" -le 2 ]] || return 1
  run_tool cmp -s "$plist_source" "$path"
}

if [[ ! -e "$plist_target" && ! -L "$plist_target" ]]; then
  echo "no SpareRunner package file from this package is installed"
  exit 0
fi

# Refuse before the first mutation if the installed property list is not
# exactly this package. A locally modified plist is an operator decision this
# script must not silently discard, and it may not describe a service that is
# actually running under this label.
plist_matches_package "$plist_target" ||
  fail "installed launchd property list differs from this package and is not removed: $plist_target"

if run_tool launchctl print "system/${label}" > /dev/null 2>&1; then
  run_tool launchctl bootout "system/${label}"
fi

# Re-verify immediately before removal so a concurrent operator edit between
# the preflight and this check is never discarded.
plist_matches_package "$plist_target" ||
  fail "installed launchd property list changed during uninstall and is not removed: $plist_target"
run_tool rm "$plist_target"

echo "removed the SpareRunner LaunchDaemon property list"
echo "retained node state; remove \"${state_root_path}\" and \"${cache_parent_path}\" deliberately to discard this node identity"
echo "the dedicated ${runner_account} account and ${runner_group} group are also retained; delete them only after removing the state above, with:"
echo "  sudo dscl . -delete /Users/${runner_account}"
echo "  sudo dscl . -delete /Groups/${runner_group}"
