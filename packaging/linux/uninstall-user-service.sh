#!/bin/bash
set -euo pipefail

# Stops the sudo-free user service and removes the unit file if and only if it
# is byte-identical to this package. Node state (~/.config/sparerunner) and the
# runner roots (~/.local/share/sparerunner) are deliberately retained: the node
# credential is durable material this script cannot decide to destroy.

readonly unit_name="sparerunner-agent.service"
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
    echo "invalid Linux installer test boundary" >&2
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
    "/sbin/${name}"; do
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
    cmp | id | rm | systemctl)
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

if [[ "$(run_tool id -u)" == "0" ]]; then
  fail "uninstall-user-service.sh must not run as root"
fi

if [[ -z "${HOME:-}" || "$HOME" != /* ]]; then
  fail "HOME must be an absolute path"
fi
config_home="${XDG_CONFIG_HOME:-${HOME}/.config}"
readonly unit_target="${config_home}/systemd/user/${unit_name}"

if [[ "$package_source_arg" != /* || "$package_source_arg" == */ ]]; then
  fail "package source directory must be canonical and absolute"
fi
resolved_package_source="$(cd "$package_source_arg" 2>/dev/null && pwd -P)" ||
  fail "package source directory is unavailable"
readonly unit_source="${resolved_package_source}/systemd/user/${unit_name}"
if [[ ! -f "$unit_source" || -L "$unit_source" ]]; then
  fail "missing or unsafe package file: $unit_source"
fi

if [[ ! -e "$unit_target" && ! -L "$unit_target" ]]; then
  echo "no SpareRunner user service from this package is installed"
  exit 0
fi
if [[ -L "$unit_target" ]] || ! run_tool cmp -s "$unit_source" "$unit_target"; then
  fail "installed unit differs from this package and is not removed: $unit_target"
fi

run_tool systemctl --user disable --now "$unit_name"

# Re-verify immediately before removal so a concurrent edit is never discarded.
if [[ -L "$unit_target" ]] || ! run_tool cmp -s "$unit_source" "$unit_target"; then
  fail "installed unit changed during uninstall and is not removed: $unit_target"
fi
run_tool rm "$unit_target"
run_tool systemctl --user daemon-reload

echo "removed the SpareRunner user service"
echo "retained node state; remove ${config_home}/sparerunner and ~/.local/share/sparerunner deliberately to discard this node identity"
