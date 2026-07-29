#!/bin/bash
set -euo pipefail

# Upgrades the installed sudo-free shared-runner-identity user service to this
# package. The operator replaces the agent binary first, exactly like the
# initial install; this script then stops the user unit, replaces it only when
# it is provably a SpareRunner package file, and restarts it. No part of this
# script runs as root or touches node state.
#
# Provenance follows uninstall-user-service.sh: a unit byte-identical to this
# package needs no replacement, so a binary-only upgrade requires nothing but
# this package. A unit from an older release must match the extracted old
# package passed with `--previous`; release archives are reproducible and
# checksummed, so the old package can always be re-downloaded to supply that
# proof. A unit that matches neither is an operator edit this script must not
# discard.

readonly unit_name="sparerunner-agent.service"
readonly cgroup_root_path="/sys/fs/cgroup"
readonly minimum_kernel_major=5
readonly minimum_kernel_minor=14

package_source_arg=""
previous_source_arg=""
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --previous)
      [[ "$#" -ge 2 ]] || {
        echo "--previous requires the extracted previous package directory" >&2
        exit 1
      }
      previous_source_arg="$2"
      shift 2
      ;;
    --*)
      echo "unknown upgrade option: $1" >&2
      exit 1
      ;;
    *)
      if [[ -n "$package_source_arg" ]]; then
        echo "upgrade-user-service.sh accepts one package source directory" >&2
        exit 1
      fi
      package_source_arg="$1"
      shift
      ;;
  esac
done
if [[ -z "$package_source_arg" ]]; then
  package_source_arg="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
fi
readonly package_source_arg previous_source_arg

# The test indirection is accepted only with an explicit marker under a
# canonical alternate root, exactly like the installer's contract.
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
    cmp | id | install | ln | rm | stat | systemctl | uname)
      resolved="$(resolve_tool "$name")" ||
        fail "required system tool is unavailable: $name"
      ;;
    *)
      echo "unknown upgrade tool: $name" >&2
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
  fail "upgrade-user-service.sh upgrades the sudo-free mode and must not run as root; use upgrade-service.sh for the root Supervisor mode"
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
readonly cgroup_root

resolve_package_directory() {
  local argument="$1"
  local description="$2"
  local resolved
  if [[ "$argument" != /* || "$argument" == */ ]]; then
    fail "${description} must be canonical and absolute"
  fi
  resolved="$(cd "$argument" 2>/dev/null && pwd -P)" ||
    fail "${description} is unavailable"
  if [[ "$resolved" != "$argument" ]]; then
    fail "${description} crosses a symlinked ancestor"
  fi
  printf '%s' "$resolved"
}

package_source="$(resolve_package_directory "$package_source_arg" "package source directory")"
readonly package_source
previous_source=""
if [[ -n "$previous_source_arg" ]]; then
  previous_source="$(resolve_package_directory "$previous_source_arg" "previous package source directory")"
fi
readonly previous_source

readonly unit_source="${package_source}/systemd/user/${unit_name}"
if [[ ! -f "$unit_source" || -L "$unit_source" ]]; then
  fail "missing or unsafe package file: $unit_source"
fi
previous_unit_source=""
if [[ -n "$previous_source" ]]; then
  previous_unit_source="${previous_source}/systemd/user/${unit_name}"
  if [[ ! -f "$previous_unit_source" || -L "$previous_unit_source" ]]; then
    fail "missing or unsafe package file: $previous_unit_source"
  fi
fi
readonly previous_unit_source

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

unit_matches_source() {
  local path="$1"
  local source="$2"
  [[ -f "$path" && ! -L "$path" ]] || return 1
  run_tool cmp -s "$source" "$path"
}

readonly previous_staging="${unit_target}.sparerunner-upgrade-prev"
readonly temporary_staging="${unit_target}.sparerunner-install-tmp"

upgrade_committed=0
transaction_active=0
stopped_service=0
staged_previous=0
staged_temporary=0
published_new=0
reloaded_units=0
started_service=0

rollback_upgrade() {
  local original_exit="$?"
  local rollback_failed=0
  trap - EXIT
  if [[ "$upgrade_committed" -eq 1 || "$transaction_active" -eq 0 ]]; then
    return "$original_exit"
  fi
  set +e

  if [[ "$started_service" -eq 1 ]]; then
    run_tool systemctl --user stop "$unit_name" || rollback_failed=1
  fi
  if [[ "$published_new" -eq 1 && ( -e "$unit_target" || -L "$unit_target" ) ]]; then
    if unit_matches_source "$unit_target" "$unit_source"; then
      run_tool rm "$unit_target" || rollback_failed=1
    else
      rollback_failed=1
    fi
  fi
  if [[ "$staged_temporary" -eq 1 && ( -e "$temporary_staging" || -L "$temporary_staging" ) ]]; then
    run_tool rm "$temporary_staging" || rollback_failed=1
  fi
  if [[ "$staged_previous" -eq 1 && ( -e "$previous_staging" || -L "$previous_staging" ) ]]; then
    if [[ ! -e "$unit_target" && ! -L "$unit_target" ]]; then
      if unit_matches_source "$previous_staging" "$previous_unit_source"; then
        run_tool ln "$previous_staging" "$unit_target" || rollback_failed=1
      else
        rollback_failed=1
      fi
    fi
    if [[ -e "$unit_target" && ! -L "$unit_target" ]] &&
      run_tool cmp -s "$previous_staging" "$unit_target"; then
      run_tool rm "$previous_staging" || rollback_failed=1
    else
      rollback_failed=1
    fi
  fi
  if [[ "$reloaded_units" -eq 1 ]]; then
    run_tool systemctl --user daemon-reload || rollback_failed=1
  fi
  if [[ "$stopped_service" -eq 1 ]]; then
    # The service was running before this upgrade began; a failed upgrade must
    # hand back the running previous installation, not a stopped node.
    run_tool systemctl --user start "$unit_name" || rollback_failed=1
  fi

  if [[ "$rollback_failed" -ne 0 ]]; then
    echo "user-service upgrade failed and verified rollback was incomplete; inspect ${previous_staging} before retrying" >&2
  else
    echo "user-service upgrade failed; the previous installation was restored and restarted" >&2
  fi
  if [[ "$original_exit" -eq 0 ]]; then
    original_exit=1
  fi
  exit "$original_exit"
}

# Preflight: everything below is read-only until the transaction begins.
require_supported_host
if [[ ! -f "$binary" || -L "$binary" || ! -x "$binary" ]]; then
  fail "install the new agent binary at ${binary} first (no root needed): install -m 0755 ./sparerunner-agent ${binary}"
fi
require_safe_chain "$binary"

if [[ ! -e "$unit_target" && ! -L "$unit_target" ]]; then
  fail "no installed ${unit_name} to upgrade; run install-user-service.sh first"
fi
require_safe_chain "$unit_target"

needs_replacement=0
if ! unit_matches_source "$unit_target" "$unit_source"; then
  if [[ -z "$previous_source" ]]; then
    fail "installed ${unit_name} differs from this package; pass --previous <extracted-previous-package> to prove its provenance"
  fi
  unit_matches_source "$unit_target" "$previous_unit_source" ||
    fail "installed ${unit_name} matches neither this package nor the previous package and is not replaced"
  needs_replacement=1
fi

if [[ -e "$previous_staging" || -L "$previous_staging" ||
      -e "$temporary_staging" || -L "$temporary_staging" ]]; then
  fail "refusing to replace upgrade staging state: $unit_target"
fi

transaction_active=1
trap rollback_upgrade EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

stopped_service=1
run_tool systemctl --user stop "$unit_name"

if [[ "$needs_replacement" -eq 1 ]]; then
  staged_previous=1
  run_tool ln "$unit_target" "$previous_staging"
  run_tool cmp -s "$previous_unit_source" "$previous_staging" ||
    fail "staged previous unit changed during upgrade"
  staged_temporary=1
  run_tool install -m 0644 "$unit_source" "$temporary_staging"
  run_tool cmp -s "$unit_source" "$temporary_staging" ||
    fail "staged unit changed during publication"
  run_tool rm "$unit_target"
  published_new=1
  run_tool ln "$temporary_staging" "$unit_target"
  run_tool rm "$temporary_staging"
  staged_temporary=0
  unit_matches_source "$unit_target" "$unit_source" ||
    fail "installed unit changed during publication"
fi

reloaded_units=1
run_tool systemctl --user daemon-reload
started_service=1
run_tool systemctl --user start "$unit_name"

# Upgrade normally follows enrollment, but an installed-and-never-enrolled node
# is legal: mirror the installer's gate so a not-initialized agent restarting
# under the user manager is a pending step, never a failed upgrade.
agent_pending=0
if directory_is_empty "$agent_state"; then
  agent_pending=1
else
  unit_is_active ||
    fail "${unit_name} did not start with existing node state; inspect journalctl --user -u ${unit_name}"
fi

# Past this point the new installation is verified and running; the staged
# previous unit is recovery material that no longer describes this user's
# service, so its removal must not trigger a rollback.
upgrade_committed=1
trap - EXIT
staging_retained=0
if [[ "$staged_previous" -eq 1 ]]; then
  run_tool rm "$previous_staging" || staging_retained=1
fi

if [[ "$needs_replacement" -eq 1 ]]; then
  echo "upgraded; replaced ${unit_name} and restarted the service"
else
  echo "upgraded; ${unit_name} already matches this package, service restarted"
fi
if [[ "$agent_pending" -eq 1 ]]; then
  echo "${unit_name} stays not-initialized until this node is enrolled"
fi
if [[ "$staging_retained" -eq 1 ]]; then
  echo "upgrade succeeded but staging cleanup was incomplete; remove ${previous_staging} before the next upgrade" >&2
  exit 1
fi
