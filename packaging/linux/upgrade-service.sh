#!/bin/bash
set -euo pipefail

# Upgrades an installed SpareRunner root-Supervisor service to this package.
# The operator publishes the new agent binary first, exactly like the initial
# install; this script then stops the services, replaces only package files it
# can prove SpareRunner published, re-declares accounts and directories, and
# restarts the services. It never downloads, builds, or relocates an
# executable, and never touches node state.
#
# Provenance is proven the same way uninstall-service.sh proves it: a file is
# replaced only when it is byte-identical to a package this project shipped.
# An installed file that already matches this package needs no replacement, so
# a binary-only upgrade requires nothing but this package. An installed file
# from an older release must match the extracted old package passed with
# `--previous`; release archives are reproducible and checksummed, so the old
# package can always be re-downloaded to supply that proof. A file that
# matches neither is an operator edit this script must not discard.

readonly marker_name=".sparerunner-install-ownership-v1"
readonly marker_version="1"
readonly agent_unit="sparerunner-agent.service"
readonly supervisor_unit="sparerunner-supervisor.service"
readonly binary_path="/usr/bin/sparerunner-agent"
readonly unit_directory_path="/usr/lib/systemd/system"
readonly sysusers_path="/usr/lib/sysusers.d/sparerunner.conf"
readonly tmpfiles_path="/usr/lib/tmpfiles.d/sparerunner.conf"
readonly supervisor_state_path="/var/lib/sparerunner-supervisor"
readonly agent_state_path="/var/lib/sparerunner-agent"
readonly socket_directory_path="/run/sparerunner-supervisor"
readonly socket_path="/run/sparerunner-supervisor/supervisor.sock"
readonly cgroup_root_path="/sys/fs/cgroup"
readonly minimum_kernel_major=5
readonly minimum_kernel_minor=14
readonly socket_attempts=30

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
        echo "upgrade-service.sh accepts one package source directory" >&2
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
    # Preserve the helper's exit status explicitly so test indirection has the
    # same fail-closed behavior as the production command surface.
    return "$rc"
  fi
  local resolved
  case "$name" in
    cmp | getent | id | install | ln | rm | sleep | stat | systemctl | uname)
      resolved="$(resolve_tool "$name")" ||
        fail "required system tool is unavailable: $name"
      ;;
    systemd-sysusers | systemd-tmpfiles)
      resolved="$(resolve_tool "$name")" ||
        fail "systemd is required but its tools are unavailable: $name"
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

binary="$(rooted_path "$binary_path")"
unit_directory="$(rooted_path "$unit_directory_path")"
sysusers_target="$(rooted_path "$sysusers_path")"
tmpfiles_target="$(rooted_path "$tmpfiles_path")"
supervisor_state="$(rooted_path "$supervisor_state_path")"
agent_state="$(rooted_path "$agent_state_path")"
socket_directory="$(rooted_path "$socket_directory_path")"
socket="$(rooted_path "$socket_path")"
cgroup_root="$(rooted_path "$cgroup_root_path")"
readonly binary unit_directory sysusers_target tmpfiles_target
readonly supervisor_state agent_state socket_directory socket cgroup_root
readonly agent_unit_target="${unit_directory}/${agent_unit}"
readonly supervisor_unit_target="${unit_directory}/${supervisor_unit}"
readonly marker="${supervisor_state}/${marker_name}"

if [[ "$(run_tool id -u)" != "0" || "$(run_tool id -g)" != "0" ]]; then
  fail "upgrade-service.sh must run as root"
fi

require_canonical_path() {
  local path="$1"
  local description="$2"
  if [[ "$path" != /* ||
        "$path" == *"//"* ||
        "$path" == *"/../"* ||
        "$path" == *"/.." ||
        "$path" == *"/./"* ||
        "$path" == *"/." ||
        "$path" == */ ]]; then
    fail "${description} must be canonical and absolute"
  fi
}

resolve_package_directory() {
  local argument="$1"
  local description="$2"
  local resolved
  require_canonical_path "$argument" "$description"
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

require_package_file() {
  local path="$1"
  if [[ ! -f "$path" || -L "$path" ]]; then
    fail "missing or unsafe package file: $path"
  fi
}

stat_contract() {
  local path="$1"
  run_tool stat -c '%u:%g:%04a' "$path"
}

require_regular_contract() {
  local path="$1"
  local expected="$2"
  if [[ ! -f "$path" || -L "$path" ]]; then
    fail "unsafe or missing regular file: $path"
  fi
  local actual
  actual="$(stat_contract "$path")" ||
    fail "cannot inspect regular file: $path"
  if [[ "$actual" != "$expected" ||
        "$(run_tool stat -c '%h' "$path")" != "1" ]]; then
    fail "regular file does not match its ownership contract: $path"
  fi
}

require_directory_contract() {
  local path="$1"
  local expected="$2"
  if [[ ! -d "$path" || -L "$path" ]]; then
    fail "unsafe or missing service directory: $path"
  fi
  local actual
  actual="$(stat_contract "$path")" ||
    fail "cannot inspect service directory: $path"
  if [[ "$actual" != "$expected" ]]; then
    fail "service directory does not match its ownership contract: $path"
  fi
}

require_safe_ancestor() {
  local logical_path="$1"
  local path
  path="$(rooted_path "$logical_path")"
  if [[ ! -d "$path" || -L "$path" ]]; then
    fail "unsafe or missing installer ancestor: $logical_path"
  fi
  local actual mode numeric_mode
  actual="$(stat_contract "$path")" ||
    fail "cannot inspect installer ancestor: $logical_path"
  mode="${actual##*:}"
  if [[ "${actual%%:*}" != "0" || ! "$mode" =~ ^[0-7]{4}$ ]]; then
    fail "installer ancestor is not root-owned and write-safe: $logical_path"
  fi
  numeric_mode="$((8#$mode))"
  if [[ $((numeric_mode & 8#022)) -ne 0 ]]; then
    fail "installer ancestor is not root-owned and write-safe: $logical_path"
  fi
}

require_safe_ancestor_chain() {
  local logical_path="$1"
  local remainder="${logical_path#/}"
  local current=""
  local component
  local components=()
  local old_ifs="$IFS"
  require_safe_ancestor "/"
  IFS='/'
  read -r -a components <<< "$remainder"
  IFS="$old_ifs"
  for component in "${components[@]}"; do
    [[ -n "$component" ]] ||
      fail "installer path is not canonical: $logical_path"
    current="${current}/${component}"
    require_safe_ancestor "$current"
  done
}

# The declared layout is read from a packaged tmpfiles definition rather than
# restated here, so a packaging change can never drift from the contract this
# upgrade verifies before systemd-tmpfiles adopts an existing tree.
declared_paths=()
declared_modes=()
declared_users=()
declared_groups=()

read_declared_layout() {
  local definition="$1"
  local line kind path mode user group remainder
  declared_paths=()
  declared_modes=()
  declared_users=()
  declared_groups=()
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -n "$line" && "$line" != "#"* ]] || continue
    read -r kind path mode user group remainder <<< "$line"
    if [[ "$kind" != "d" ]]; then
      fail "packaged tmpfiles definition uses an unsupported type: $line"
    fi
    if [[ "$remainder" != "-" ]]; then
      fail "packaged tmpfiles definition has an unexpected age field: $line"
    fi
    require_canonical_path "$path" "packaged tmpfiles path"
    if [[ ! "$mode" =~ ^0[0-7]{3}$ ]]; then
      fail "packaged tmpfiles definition has a non-canonical mode: $line"
    fi
    declared_paths+=("$path")
    declared_modes+=("$mode")
    declared_users+=("$user")
    declared_groups+=("$group")
  done < "$definition"
  if [[ "${#declared_paths[@]}" -eq 0 ]]; then
    fail "packaged tmpfiles definition declares no service directory"
  fi
}

resolve_uid() {
  local name="$1"
  if [[ "$name" == "root" ]]; then
    printf '0'
    return 0
  fi
  local record
  record="$(run_tool getent passwd "$name")" || return 1
  local fields=()
  local old_ifs="$IFS"
  IFS=':'
  read -r -a fields <<< "$record"
  IFS="$old_ifs"
  [[ "${#fields[@]}" -ge 3 && "${fields[2]}" =~ ^[0-9]{1,10}$ ]] || return 1
  printf '%s' "$((10#${fields[2]}))"
}

resolve_gid() {
  local name="$1"
  if [[ "$name" == "root" ]]; then
    printf '0'
    return 0
  fi
  local record
  record="$(run_tool getent group "$name")" || return 1
  local fields=()
  local old_ifs="$IFS"
  IFS=':'
  read -r -a fields <<< "$record"
  IFS="$old_ifs"
  [[ "${#fields[@]}" -ge 3 && "${fields[2]}" =~ ^[0-9]{1,10}$ ]] || return 1
  printf '%s' "$((10#${fields[2]}))"
}

verify_declared_layout() {
  local require_present="$1"
  local index path mode user group uid gid
  for index in "${!declared_paths[@]}"; do
    path="$(rooted_path "${declared_paths[$index]}")"
    mode="${declared_modes[$index]}"
    user="${declared_users[$index]}"
    group="${declared_groups[$index]}"
    if [[ ! -e "$path" && ! -L "$path" ]]; then
      if [[ "$require_present" -eq 1 ]]; then
        fail "declared service directory was not created: ${declared_paths[$index]}"
      fi
      continue
    fi
    uid="$(resolve_uid "$user")" ||
      fail "declared service directory exists without its service identity: ${declared_paths[$index]}"
    gid="$(resolve_gid "$group")" ||
      fail "declared service directory exists without its service group: ${declared_paths[$index]}"
    require_directory_contract "$path" "${uid}:${gid}:${mode}"
  done
}

marker_contents() {
  printf 'version=%s\nrole=%s\npath=%s\n' \
    "$marker_version" \
    "supervisor-state" \
    "$supervisor_state_path"
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
}

unit_is_active() {
  local unit="$1"
  run_tool systemctl is-active --quiet "$unit"
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

file_matches_source() {
  local path="$1"
  local source="$2"
  local maximum_links="${3:-1}"
  local actual links
  [[ -f "$path" && ! -L "$path" ]] || return 1
  actual="$(stat_contract "$path" 2>/dev/null)" || return 1
  links="$(run_tool stat -c '%h' "$path" 2>/dev/null)" || return 1
  [[ "$actual" == "0:0:0644" &&
    "$links" =~ ^[0-9]+$ &&
    "$links" -ge 1 &&
    "$links" -le "$maximum_links" ]] || return 1
  run_tool cmp -s "$source" "$path"
}

require_socket_contract() {
  local attempt agent_gid actual
  agent_gid="$(resolve_gid "sparerunner-agent")" ||
    fail "the sparerunner-agent group is absent after upgrade"
  require_directory_contract "$socket_directory" "0:${agent_gid}:0750"
  for ((attempt = 0; attempt < socket_attempts; attempt++)); do
    if [[ -S "$socket" && ! -L "$socket" ]]; then
      actual="$(stat_contract "$socket")" ||
        fail "cannot inspect the Supervisor socket"
      if [[ "$actual" != "0:${agent_gid}:0660" ]]; then
        fail "the Supervisor socket does not match its ownership contract"
      fi
      return 0
    fi
    run_tool sleep 1
  done
  fail "the Supervisor did not publish ${socket_path}; inspect journalctl -u ${supervisor_unit}"
}

# Every installed package file, its package sources, and its transaction flags
# share one index across these arrays.
target_paths=(
  "$agent_unit_target"
  "$supervisor_unit_target"
  "$sysusers_target"
  "$tmpfiles_target"
)
new_sources=(
  "${package_source}/systemd/${agent_unit}"
  "${package_source}/systemd/${supervisor_unit}"
  "${package_source}/sysusers.d/sparerunner.conf"
  "${package_source}/tmpfiles.d/sparerunner.conf"
)
previous_sources=()
if [[ -n "$previous_source" ]]; then
  previous_sources=(
    "${previous_source}/systemd/${agent_unit}"
    "${previous_source}/systemd/${supervisor_unit}"
    "${previous_source}/sysusers.d/sparerunner.conf"
    "${previous_source}/tmpfiles.d/sparerunner.conf"
  )
fi
needs_replacement=(0 0 0 0)
staged_previous=(0 0 0 0)
staged_temporary=(0 0 0 0)
published_new=(0 0 0 0)

upgrade_committed=0
transaction_active=0
stopped_services=0
reloaded_units=0
started_services=0

previous_staging() {
  printf '%s.sparerunner-upgrade-prev' "$1"
}

temporary_staging() {
  printf '%s.sparerunner-install-tmp' "$1"
}

rollback_upgrade() {
  local original_exit="$?"
  local rollback_failed=0
  trap - EXIT
  if [[ "$upgrade_committed" -eq 1 || "$transaction_active" -eq 0 ]]; then
    return "$original_exit"
  fi
  set +e

  if [[ "$started_services" -eq 1 ]]; then
    run_tool systemctl stop "$agent_unit" "$supervisor_unit" || rollback_failed=1
  fi

  local index target previous temporary
  for index in "${!target_paths[@]}"; do
    target="${target_paths[$index]}"
    previous="$(previous_staging "$target")"
    temporary="$(temporary_staging "$target")"
    if [[ "${published_new[$index]}" -eq 1 && ( -e "$target" || -L "$target" ) ]]; then
      if file_matches_source "$target" "${new_sources[$index]}" 2; then
        run_tool rm "$target" || rollback_failed=1
      else
        rollback_failed=1
      fi
    fi
    if [[ "${staged_temporary[$index]}" -eq 1 && ( -e "$temporary" || -L "$temporary" ) ]]; then
      run_tool rm "$temporary" || rollback_failed=1
    fi
    if [[ "${staged_previous[$index]}" -eq 1 && ( -e "$previous" || -L "$previous" ) ]]; then
      if [[ ! -e "$target" && ! -L "$target" ]]; then
        if file_matches_source "$previous" "${previous_sources[$index]}" 2; then
          run_tool ln "$previous" "$target" || rollback_failed=1
        else
          rollback_failed=1
        fi
      fi
      if [[ -e "$target" && ! -L "$target" ]] &&
        run_tool cmp -s "$previous" "$target"; then
        run_tool rm "$previous" || rollback_failed=1
      else
        rollback_failed=1
      fi
    fi
  done

  if [[ "$reloaded_units" -eq 1 ]]; then
    run_tool systemctl daemon-reload || rollback_failed=1
  fi
  if [[ "$stopped_services" -eq 1 ]]; then
    # The services were running before this upgrade began; a failed upgrade
    # must hand back a running previous installation, not a stopped machine.
    if run_tool systemctl start "$supervisor_unit" "$agent_unit"; then
      unit_is_active "$supervisor_unit" || rollback_failed=1
    else
      rollback_failed=1
    fi
  fi

  if [[ "$rollback_failed" -ne 0 ]]; then
    echo "Linux upgrade failed and verified rollback was incomplete; inspect the staged .sparerunner-upgrade-prev files before retrying" >&2
  else
    echo "Linux upgrade failed; the previous installation was restored and restarted" >&2
  fi
  if [[ "$original_exit" -eq 0 ]]; then
    original_exit=1
  fi
  exit "$original_exit"
}

# Validate every authority and target before the first filesystem or systemd
# mutation.
for source in "${new_sources[@]}"; do
  require_package_file "$source"
done
if [[ -n "$previous_source" ]]; then
  for source in "${previous_sources[@]}"; do
    require_package_file "$source"
  done
fi
require_safe_ancestor_chain "$unit_directory_path"
require_safe_ancestor_chain "/usr/lib/sysusers.d"
require_safe_ancestor_chain "/usr/lib/tmpfiles.d"
require_safe_ancestor_chain "/usr/bin"
require_safe_ancestor_chain "/var/lib"
require_supported_host
require_regular_contract "$binary" "0:0:0755"

if [[ ! -e "$marker" && ! -L "$marker" ]]; then
  fail "no owned SpareRunner installation to upgrade; run install-service.sh first"
fi
require_regular_contract "$marker" "0:0:0600"
[[ "$(<"$marker")" == "$(marker_contents)" ]] ||
  fail "the Supervisor state root has a foreign or tampered ownership marker"

# Every installed file must be provably a SpareRunner package file before any
# service is stopped: byte-identical to this package (no replacement needed),
# or byte-identical to the explicit previous package (replaced). Anything else
# is an operator edit or a broken installation this script must not touch.
changed_names=()
for index in "${!target_paths[@]}"; do
  target="${target_paths[$index]}"
  if [[ ! -e "$target" && ! -L "$target" ]]; then
    fail "installed package file is missing; repair with install-service.sh: $target"
  fi
  if file_matches_source "$target" "${new_sources[$index]}"; then
    continue
  fi
  if [[ -z "$previous_source" ]]; then
    fail "installed file differs from this package; pass --previous <extracted-previous-package> to prove its provenance: $target"
  fi
  file_matches_source "$target" "${previous_sources[$index]}" ||
    fail "installed file matches neither this package nor the previous package and is not replaced: $target"
  needs_replacement[index]=1
  changed_names+=("${target##*/}")
done

for index in "${!target_paths[@]}"; do
  previous="$(previous_staging "${target_paths[$index]}")"
  temporary="$(temporary_staging "${target_paths[$index]}")"
  if [[ -e "$previous" || -L "$previous" || -e "$temporary" || -L "$temporary" ]]; then
    fail "refusing to replace upgrade staging state: ${target_paths[$index]}"
  fi
done

# The existing directory layout is verified against the installed tmpfiles
# definition (which the provenance check above just proved is a package file),
# then re-verified against this package's definition after redeclaration.
read_declared_layout "$tmpfiles_target"
verify_declared_layout 0

transaction_active=1
trap rollback_upgrade EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

stopped_services=1
run_tool systemctl stop "$agent_unit" "$supervisor_unit"

for index in "${!target_paths[@]}"; do
  [[ "${needs_replacement[$index]}" -eq 1 ]] || continue
  target="${target_paths[$index]}"
  previous="$(previous_staging "$target")"
  temporary="$(temporary_staging "$target")"
  staged_previous[index]=1
  run_tool ln "$target" "$previous"
  run_tool cmp -s "${previous_sources[$index]}" "$previous" ||
    fail "staged previous package file changed during upgrade: $target"
  staged_temporary[index]=1
  run_tool install -o root -g root -m 0644 "${new_sources[$index]}" "$temporary"
  run_tool cmp -s "${new_sources[$index]}" "$temporary" ||
    fail "staged package file changed during publication: $target"
  run_tool rm "$target"
  published_new[index]=1
  run_tool ln "$temporary" "$target"
  run_tool rm "$temporary"
  staged_temporary[index]=0
  file_matches_source "$target" "${new_sources[$index]}" 2 ||
    fail "installed package file changed during publication: $target"
done

# The installed definitions are byte-identical to this package here, so the
# declarative tools run against the same files systemd itself would read.
run_tool systemd-sysusers "$sysusers_target"
run_tool systemd-tmpfiles --create "$tmpfiles_target"
read_declared_layout "${new_sources[3]}"
verify_declared_layout 1

reloaded_units=1
run_tool systemctl daemon-reload
started_services=1
run_tool systemctl start "$supervisor_unit" "$agent_unit"

unit_is_active "$supervisor_unit" ||
  fail "${supervisor_unit} did not start; inspect journalctl -u ${supervisor_unit}"
require_socket_contract

# Upgrade normally follows enrollment, but an installed-and-never-enrolled node
# is legal: mirror the installer's gate so a not-initialized Agent restarting
# under systemd is a pending step, never a failed upgrade.
agent_pending=0
if directory_is_empty "$agent_state"; then
  agent_pending=1
else
  unit_is_active "$agent_unit" ||
    fail "${agent_unit} did not start with existing node state; inspect journalctl -u ${agent_unit}"
fi

# Past this point the new installation is verified and running; the staged
# previous files are recovery material that no longer describes the machine,
# so their removal must not trigger a rollback.
upgrade_committed=1
trap - EXIT
staging_retained=0
for index in "${!target_paths[@]}"; do
  [[ "${staged_previous[$index]}" -eq 1 ]] || continue
  previous="$(previous_staging "${target_paths[$index]}")"
  run_tool rm "$previous" || staging_retained=1
done

if [[ "${#changed_names[@]}" -gt 0 ]]; then
  echo "upgraded; replaced ${changed_names[*]} and restarted the services"
else
  echo "upgraded; every package file already matches this package, services restarted"
fi
if [[ "$agent_pending" -eq 1 ]]; then
  echo "${agent_unit} stays not-initialized until this node is enrolled"
fi
if [[ "$staging_retained" -eq 1 ]]; then
  echo "upgrade succeeded but staging cleanup was incomplete; remove the remaining .sparerunner-upgrade-prev files before the next upgrade" >&2
  exit 1
fi
