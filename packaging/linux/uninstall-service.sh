#!/bin/bash
set -euo pipefail

# Stops the SpareRunner services and removes only the package files this
# installation published. Enrollment state, the runner journal, the package
# cache, the service accounts, and the service directories are deliberately
# retained: a node identity is durable credential material, and this script can
# prove what the package installed but not what an operator still needs.
#
# Removing that state is a separate, explicit operator decision; the paths are
# listed in README.md.

readonly marker_name=".sparerunner-install-ownership-v1"
readonly agent_unit="sparerunner-agent.service"
readonly supervisor_unit="sparerunner-supervisor.service"
readonly unit_directory_path="/usr/lib/systemd/system"
readonly sysusers_path="/usr/lib/sysusers.d/sparerunner.conf"
readonly tmpfiles_path="/usr/lib/tmpfiles.d/sparerunner.conf"
readonly supervisor_state_path="/var/lib/sparerunner-supervisor"

readonly package_source_arg="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)}"

readonly test_root="${SPARERUNNER_LINUX_INSTALL_TEST_ROOT:-}"
readonly test_tools="${SPARERUNNER_LINUX_INSTALL_TEST_TOOLS:-}"
readonly test_enabled="${SPARERUNNER_LINUX_INSTALL_TESTING:-}"
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
    echo "invalid Linux installer test boundary; root never accepts test indirection" >&2
    exit 1
  fi
fi

fail() {
  echo "$1" >&2
  exit 1
}

resolve_tool() {
  local name="$1"
  local candidate
  for candidate in \
    "/usr/bin/${name}" \
    "/bin/${name}" \
    "/usr/sbin/${name}" \
    "/sbin/${name}" \
    "/usr/lib/systemd/${name}"; do
    if [[ -f "$candidate" && ! -L "$candidate" && -x "$candidate" ]]; then
      printf '%s' "$candidate"
      return 0
    fi
  done
  return 1
}

run_tool() {
  local name="$1"
  shift
  if [[ "$test_enabled" == "1" ]]; then
    "${test_tools}/${name}" "$@"
    local rc=$?
    return "$rc"
  fi
  local resolved
  case "$name" in
    cmp | id | rm | stat | systemctl)
      resolved="$(resolve_tool "$name")" ||
        fail "required system tool is unavailable: $name"
      ;;
    *)
      echo "unknown installer tool: $name" >&2
      return 1
      ;;
  esac
  "$resolved" "$@"
}

rooted_path() {
  local logical_path="$1"
  if [[ "$test_enabled" == "1" ]]; then
    printf '%s%s' "$test_root" "$logical_path"
    return
  fi
  printf '%s' "$logical_path"
}

unit_directory="$(rooted_path "$unit_directory_path")"
sysusers_target="$(rooted_path "$sysusers_path")"
tmpfiles_target="$(rooted_path "$tmpfiles_path")"
supervisor_state="$(rooted_path "$supervisor_state_path")"
readonly unit_directory sysusers_target tmpfiles_target supervisor_state
readonly agent_unit_target="${unit_directory}/${agent_unit}"
readonly supervisor_unit_target="${unit_directory}/${supervisor_unit}"
readonly marker="${supervisor_state}/${marker_name}"

if [[ "$(run_tool id -u)" != "0" || "$(run_tool id -g)" != "0" ]]; then
  fail "uninstall-service.sh must run as root"
fi

if [[ "$package_source_arg" != /* || "$package_source_arg" == */ ]]; then
  fail "package source directory must be canonical and absolute"
fi
resolved_package_source="$(cd "$package_source_arg" 2>/dev/null && pwd -P)" ||
  fail "package source directory is unavailable"
if [[ "$resolved_package_source" != "$package_source_arg" ]]; then
  fail "package source directory crosses a symlinked ancestor"
fi
readonly package_source="$resolved_package_source"
readonly agent_unit_source="${package_source}/systemd/${agent_unit}"
readonly supervisor_unit_source="${package_source}/systemd/${supervisor_unit}"
readonly sysusers_source="${package_source}/sysusers.d/sparerunner.conf"
readonly tmpfiles_source="${package_source}/tmpfiles.d/sparerunner.conf"

for source in \
  "$agent_unit_source" \
  "$supervisor_unit_source" \
  "$sysusers_source" \
  "$tmpfiles_source"; do
  if [[ ! -f "$source" || -L "$source" ]]; then
    fail "missing or unsafe package file: $source"
  fi
done

matches_package() {
  local path="$1"
  local source="$2"
  local contract links
  [[ -f "$path" && ! -L "$path" ]] || return 1
  contract="$(run_tool stat -c '%u:%g:%04a' "$path" 2>/dev/null)" || return 1
  links="$(run_tool stat -c '%h' "$path" 2>/dev/null)" || return 1
  [[ "$contract" == "0:0:0644" && "$links" == "1" ]] || return 1
  run_tool cmp -s "$source" "$path"
}

# Refuse before the first mutation if any installed file is not exactly this
# package. A locally modified unit is an operator decision this script must not
# silently discard, and it may not describe the services that are running.
removable=()
removable_sources=()
for pair in \
  "${agent_unit_target}|${agent_unit_source}" \
  "${supervisor_unit_target}|${supervisor_unit_source}" \
  "${sysusers_target}|${sysusers_source}" \
  "${tmpfiles_target}|${tmpfiles_source}"; do
  target="${pair%%|*}"
  source="${pair##*|}"
  if [[ ! -e "$target" && ! -L "$target" ]]; then
    continue
  fi
  matches_package "$target" "$source" ||
    fail "installed file differs from this package and is not removed: $target"
  removable+=("$target")
  removable_sources+=("$source")
done

if [[ "${#removable[@]}" -eq 0 ]]; then
  echo "no SpareRunner package file from this package is installed"
  exit 0
fi

run_tool systemctl disable --now "$agent_unit" "$supervisor_unit"

for index in "${!removable[@]}"; do
  # Re-verify immediately before removal so a concurrent operator edit between
  # the preflight and this loop is never discarded.
  matches_package "${removable[$index]}" "${removable_sources[$index]}" ||
    fail "installed file changed during uninstall and is not removed: ${removable[$index]}"
  run_tool rm "${removable[$index]}"
done

run_tool systemctl daemon-reload

echo "removed the SpareRunner systemd units and package definitions"
if [[ -e "$marker" || -L "$marker" ]]; then
  echo "retained node state; remove ${supervisor_state_path}, /var/lib/sparerunner-agent, /var/cache/sparerunner-agent, /var/lib/sparerunner-runtime, and /var/lib/sparerunner-runner deliberately to discard this node identity"
fi
