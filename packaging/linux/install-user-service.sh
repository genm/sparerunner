#!/bin/bash
set -euo pipefail

# Installs the sudo-free shared-runner-identity node service for the invoking
# user: it publishes the packaged `systemd --user` unit and starts it. No part
# of this script runs as root, mutates a system path, or creates an account.
#
# The mode it installs deliberately shares one Unix identity between the Agent
# and the job. Its remaining prerequisites exist because SpareRunner proves
# descendant termination and verified cleanup before re-advertising capacity;
# the checks below fail before the first mutation when the host cannot ever
# construct that proof.

readonly unit_name="sparerunner-agent.service"
readonly cgroup_root_path="/sys/fs/cgroup"
readonly linger_root_path="/var/lib/systemd/linger"
readonly minimum_kernel_major=5
readonly minimum_kernel_minor=14

readonly package_source_arg="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)}"

# The test indirection is accepted only with an explicit marker under a
# canonical alternate root, exactly like the root installer's contract.
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
    cmp | id | install | ln | loginctl | mkdir | rm | stat | systemctl | uname)
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

if [[ "$(run_tool id -u)" == "0" ]]; then
  fail "install-user-service.sh installs the sudo-free mode and must not run as root; use install-service.sh for the root Supervisor mode"
fi
invoking_uid="$(run_tool id -u)"
[[ "$invoking_uid" =~ ^[0-9]+$ ]] || fail "cannot determine the invoking user ID"
readonly invoking_uid

if [[ -z "${HOME:-}" || "$HOME" != /* ]]; then
  fail "HOME must be an absolute path"
fi
config_home="${XDG_CONFIG_HOME:-${HOME}/.config}"
if [[ "$config_home" != /* ]]; then
  fail "XDG_CONFIG_HOME must be an absolute path"
fi
readonly config_home
readonly unit_directory="${config_home}/systemd/user"
readonly unit_target="${unit_directory}/${unit_name}"
readonly binary="${HOME}/.local/bin/sparerunner-agent"
readonly agent_state="${config_home}/sparerunner/agent"
cgroup_root="$(rooted_path "$cgroup_root_path")"
linger_root="$(rooted_path "$linger_root_path")"
readonly cgroup_root linger_root

if [[ "$package_source_arg" != /* || "$package_source_arg" == */ ]]; then
  fail "package source directory must be canonical and absolute"
fi
resolved_package_source="$(cd "$package_source_arg" 2>/dev/null && pwd -P)" ||
  fail "package source directory is unavailable"
if [[ "$resolved_package_source" != "$package_source_arg" ]]; then
  fail "package source directory crosses a symlinked ancestor"
fi
readonly unit_source="${resolved_package_source}/systemd/user/${unit_name}"
if [[ ! -f "$unit_source" || -L "$unit_source" ]]; then
  fail "missing or unsafe package file: $unit_source"
fi

stat_contract() {
  run_tool stat -c '%u:%g:%04a' "$1"
}

# The unit file and the agent binary are execution authority for this user, so
# their ownership contract mirrors what the shared-identity launcher itself
# verifies at construction: owned by the invoking user or root, and never
# writable by group or other on any path component.
require_safe_component() {
  local path="$1"
  local actual mode uid
  if [[ -L "$path" ]]; then
    fail "unsafe symlinked installer path component: $path"
  fi
  actual="$(stat_contract "$path")" ||
    fail "cannot inspect installer path component: $path"
  uid="${actual%%:*}"
  mode="${actual##*:}"
  if [[ ( "$uid" != "0" && "$uid" != "$invoking_uid" ) ||
        ! "$mode" =~ ^[0-7]{4}$ ||
        $((8#$mode & 8#022)) -ne 0 ]]; then
    fail "installer path component is not owner-held and write-safe: $path"
  fi
}

# Like the Go launcher, the chain is checked on the physically resolved parent
# so a benign platform symlink (macOS /tmp in the test harness) does not mask
# the real ownership question, while the leaf itself must not be a symlink.
require_safe_chain() {
  local leaf="$1"
  if [[ -L "$leaf" ]]; then
    fail "unsafe symlinked installer path: $leaf"
  fi
  require_safe_component "$leaf"
  local resolved_parent
  resolved_parent="$(cd "$(dirname "$leaf")" 2>/dev/null && pwd -P)" ||
    fail "cannot resolve installer path parent: $leaf"
  local current="$resolved_parent"
  while :; do
    require_safe_component "$current"
    [[ "$current" == "/" ]] && break
    current="$(dirname "$current")"
  done
}

require_supported_host() {
  local release major minor
  release="$(run_tool uname -r)" || fail "cannot read the running kernel release"
  if [[ ! "$release" =~ ^([0-9]{1,4})\.([0-9]{1,4}) ]]; then
    fail "cannot parse the running kernel release: $release"
  fi
  major="$((10#${BASH_REMATCH[1]}))"
  minor="$((10#${BASH_REMATCH[2]}))"
  if [[ "$major" -lt "$minimum_kernel_major" ||
        ( "$major" -eq "$minimum_kernel_major" && "$minor" -lt "$minimum_kernel_minor" ) ]]; then
    fail "SpareRunner requires Linux ${minimum_kernel_major}.${minimum_kernel_minor} or newer for delegated cgroup.kill; found ${release}"
  fi
  if [[ ! -f "${cgroup_root}/cgroup.controllers" || -L "${cgroup_root}/cgroup.controllers" ]]; then
    fail "the unified cgroup v2 hierarchy is not mounted at ${cgroup_root_path}"
  fi
  # systemd delegates the user@UID.service subtree to the user; without it the
  # agent can never own a containment boundary and would only ever advertise
  # zero capacity. Its cgroup.controllers file is the read-only witness.
  local delegated="${cgroup_root}/user.slice/user-${invoking_uid}.slice/user@${invoking_uid}.service/cgroup.controllers"
  if [[ ! -f "$delegated" ]]; then
    fail "no delegated systemd user subtree for uid ${invoking_uid}; the systemd user manager is not running or does not delegate"
  fi
  run_tool systemctl --user show-environment > /dev/null ||
    fail "the systemd user manager is unreachable; run this from a session with systemd --user"
}

unit_is_active() {
  run_tool systemctl --user is-active --quiet "$unit_name"
}

directory_is_empty() {
  local directory="$1"
  local empty=0
  shopt -s nullglob dotglob
  # shellcheck disable=SC2034 # the loop only observes whether a match exists
  for _entry in "${directory}"/*; do
    empty=1
    break
  done
  shopt -u nullglob dotglob
  [[ "$empty" -eq 0 ]]
}

install_committed=0
transaction_active=0
created_unit=0
created_unit_temporary=0
reloaded_units=0
enabled_service=0

unit_matches_package() {
  local path="$1"
  [[ -f "$path" && ! -L "$path" ]] || return 1
  run_tool cmp -s "$unit_source" "$path"
}

rollback_install() {
  local original_exit="$?"
  local rollback_failed=0
  trap - EXIT
  if [[ "$install_committed" -eq 1 || "$transaction_active" -eq 0 ]]; then
    return "$original_exit"
  fi
  set +e
  if [[ "$enabled_service" -eq 1 ]]; then
    if unit_matches_package "$unit_target"; then
      run_tool systemctl --user disable --now "$unit_name" || rollback_failed=1
    else
      rollback_failed=1
    fi
  fi
  if [[ "$created_unit" -eq 1 && ( -e "$unit_target" || -L "$unit_target" ) ]]; then
    if unit_matches_package "$unit_target"; then
      run_tool rm "$unit_target" || rollback_failed=1
    else
      rollback_failed=1
    fi
  fi
  if [[ "$created_unit_temporary" -eq 1 &&
        ( -e "${unit_target}.sparerunner-install-tmp" || -L "${unit_target}.sparerunner-install-tmp" ) ]]; then
    run_tool rm "${unit_target}.sparerunner-install-tmp" || rollback_failed=1
  fi
  if [[ "$reloaded_units" -eq 1 ]]; then
    run_tool systemctl --user daemon-reload || rollback_failed=1
  fi
  if [[ "$rollback_failed" -ne 0 ]]; then
    echo "user-service install failed and verified rollback was incomplete; inspect ${unit_target} before retrying" >&2
  fi
  if [[ "$original_exit" -eq 0 ]]; then
    original_exit=1
  fi
  exit "$original_exit"
}

# Preflight: everything below is read-only until the transaction begins.
require_supported_host
if [[ ! -f "$binary" || -L "$binary" || ! -x "$binary" ]]; then
  fail "install the agent binary at ${binary} first (no root needed): install -m 0755 ./sparerunner-agent ${binary}"
fi
require_safe_chain "$binary"

if unit_is_active; then
  fail "${unit_name} is already running for this user; use the documented upgrade flow"
fi

unit_state="new"
if [[ -e "$unit_target" || -L "$unit_target" ]]; then
  unit_matches_package "$unit_target" ||
    fail "existing ${unit_target} differs from this package"
  require_safe_chain "$unit_target"
  unit_state="owned"
fi

transaction_active=1
trap rollback_install EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ "$unit_state" == "new" ]]; then
  for component in "${config_home}" "${config_home}/systemd" "$unit_directory"; do
    if [[ ! -e "$component" && ! -L "$component" ]]; then
      run_tool mkdir -m 0755 "$component"
    fi
  done
  require_safe_chain "$unit_directory"
  temporary="${unit_target}.sparerunner-install-tmp"
  if [[ -e "$temporary" || -L "$temporary" ]]; then
    fail "refusing to replace installer staging state: $temporary"
  fi
  created_unit_temporary=1
  run_tool install -m 0644 "$unit_source" "$temporary"
  run_tool cmp -s "$unit_source" "$temporary" ||
    fail "staged unit changed during publication"
  created_unit=1
  run_tool ln "$temporary" "$unit_target"
  run_tool rm "$temporary"
  created_unit_temporary=0
  unit_matches_package "$unit_target" ||
    fail "installed unit changed during publication"
fi

reloaded_units=1
run_tool systemctl --user daemon-reload
enabled_service=1
run_tool systemctl --user enable --now "$unit_name"

# Installation precedes enrollment, exactly like the root flow: an agent that
# exits not-initialized and is restarted by the user manager is a pending step,
# never a failed install. Once node state exists, a dead agent is a failure.
if directory_is_empty "$agent_state"; then
  echo "installed; ${unit_name} stays not-initialized until this node is enrolled"
  echo "next: sprun join <join-code>"
  echo "then: systemctl --user restart ${unit_name}"
else
  unit_is_active ||
    fail "${unit_name} did not start with existing node state; inspect journalctl --user -u ${unit_name}"
  echo "installed; ${unit_name} is running against the existing node state"
fi

# Linger is what keeps the service alive after logout on a machine that serves
# full time. It is not required for a session-bound contribution, so its absence
# is guidance rather than a failure.
linger_state="$(run_tool loginctl show-user --property=Linger --value "$invoking_uid" 2>/dev/null || true)"
if [[ "$linger_state" != "yes" && ! -e "${linger_root}/${USER:-}" ]]; then
  echo "note: lingering is disabled, so this service stops at logout"
  echo "      for a full-time machine run: loginctl enable-linger"
fi

install_committed=1
