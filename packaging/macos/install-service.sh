#!/bin/bash
set -euo pipefail

readonly label="com.genm.tewake.agent"
readonly runner_account="tewake-runner-0"
readonly runner_group="tewake-runner-0"
readonly marker_name=".tewake-install-ownership-v1"
readonly marker_version="1"
readonly binary_path="/usr/local/libexec/tewake-agent"
readonly plist_target_path="/Library/LaunchDaemons/${label}.plist"
readonly state_root_path="/Library/Application Support/Tewake"
readonly cache_parent_path="/Library/Caches/com.genm.tewake"
readonly cache_root_path="${cache_parent_path}/runner"
readonly plist_source_arg="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)/launchd/${label}.plist}"

# The test indirection is accepted only with an explicit marker under a
# canonical alternate root. Production calls use fixed absolute tools and
# paths; no environment variable can redirect one path independently.
readonly test_root="${TEWAKE_MACOS_INSTALL_TEST_ROOT:-}"
readonly test_tools="${TEWAKE_MACOS_INSTALL_TEST_TOOLS:-}"
readonly test_enabled="${TEWAKE_MACOS_INSTALL_TESTING:-}"
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
        ! -f "${test_root}/.tewake-installer-test-root" ||
        "$test_tools" != /* ||
        ! -d "$test_tools" ||
        -L "$test_tools" ]]; then
    echo "invalid macOS installer test boundary; root never accepts test indirection" >&2
    exit 1
  fi
fi

run_tool() {
  local name="$1"
  shift
  if [[ "$test_enabled" == "1" ]]; then
    "${test_tools}/${name}" "$@"
    return
  fi
  case "$name" in
    cmp) /usr/bin/cmp "$@" ;;
    dscl) /usr/bin/dscl "$@" ;;
    id) /usr/bin/id "$@" ;;
    install) /usr/bin/install "$@" ;;
    launchctl) /bin/launchctl "$@" ;;
    ln) /bin/ln "$@" ;;
    mkdir) /bin/mkdir "$@" ;;
    plutil) /usr/bin/plutil "$@" ;;
    rm) /bin/rm "$@" ;;
    rmdir) /bin/rmdir "$@" ;;
    stat) /usr/bin/stat "$@" ;;
    uuidgen) /usr/bin/uuidgen "$@" ;;
    *)
      echo "unknown installer tool: $name" >&2
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

binary="$(rooted_path "$binary_path")"
plist_target="$(rooted_path "$plist_target_path")"
state_root="$(rooted_path "$state_root_path")"
cache_parent="$(rooted_path "$cache_parent_path")"
cache_root="$(rooted_path "$cache_root_path")"
readonly binary plist_target state_root cache_parent cache_root
readonly state_marker="${state_root}/${marker_name}"
readonly cache_marker="${cache_parent}/${marker_name}"

install_committed=0
transaction_active=0
created_state_root=0
created_agent_directory=0
created_fences_directory=0
created_runtime_directory=0
created_executions_directory=0
created_cache_parent=0
created_cache_root=0
created_state_marker=0
created_state_marker_temporary=0
created_cache_marker=0
created_cache_marker_temporary=0
created_group=0
created_user=0
created_plist_temporary=0
created_plist_target=0
bootstrap_attempted=0
loaded_service=0

fail() {
  echo "$1" >&2
  exit 1
}

if [[ "$(run_tool id -u)" != "0" || "$(run_tool id -g)" != "0" ]]; then
  fail "install-service.sh must run as root:wheel"
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
plist_source_parent="${plist_source_arg%/*}"
[[ -n "$plist_source_parent" ]] || plist_source_parent="/"
plist_source_name="${plist_source_arg##*/}"
resolved_plist_source_parent="$(
  cd "$plist_source_parent" 2>/dev/null && pwd -P
)" || fail "launchd property list parent is unavailable"
if [[ "${resolved_plist_source_parent%/}/${plist_source_name}" != "$plist_source_arg" ]]; then
  fail "launchd property list path crosses a symlinked ancestor"
fi
readonly plist_source="$plist_source_arg"

stat_contract() {
  local path="$1"
  run_tool stat -f '%u:%g:%p' "$path"
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
        "$(run_tool stat -f '%l' "$path")" != "1" ]]; then
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
  local actual uid_gid mode numeric_mode
  actual="$(stat_contract "$path")" ||
    fail "cannot inspect installer ancestor: $logical_path"
  uid_gid="${actual%:*}"
  mode="${actual##*:}"
  if [[ "${uid_gid%%:*}" != "0" ||
        ! "$mode" =~ ^[0-7]{5,6}$ ]]; then
    fail "installer ancestor is not root-owned and write-safe: $logical_path"
  fi
  numeric_mode="$((8#$mode))"
  if [[ $((numeric_mode & 8#170000)) -ne $((8#040000)) ]]; then
    fail "installer ancestor is not root-owned and write-safe: $logical_path"
  fi
  if [[ $((numeric_mode & 8#022)) -ne 0 ]]; then
    if [[ "$logical_path" != "/Library/Caches" ||
          $((numeric_mode & 8#01000)) -eq 0 ]]; then
      fail "installer ancestor is not root-owned and write-safe: $logical_path"
    fi
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

require_only_children() {
  local directory="$1"
  shift
  local entry name allowed expected
  shopt -s nullglob dotglob
  for entry in "${directory}"/*; do
    name="${entry##*/}"
    allowed=0
    for expected in "$@"; do
      if [[ "$name" == "$expected" ]]; then
        allowed=1
        break
      fi
    done
    if [[ "$allowed" -ne 1 ]]; then
      fail "service directory contains foreign content: $entry"
    fi
  done
  shopt -u nullglob dotglob
}

require_empty_directory() {
  require_only_children "$1"
}

valid_install_id() {
  [[ "$1" =~ ^[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}$ ]]
}

marker_contents() {
  local install_id="$1"
  local role="$2"
  local logical_path="$3"
  printf 'version=%s\ninstall_id=%s\nrole=%s\npath=%s\n' \
    "$marker_version" \
    "$install_id" \
    "$role" \
    "$logical_path"
}

read_marker_install_id() {
  local marker="$1"
  local expected_role="$2"
  local expected_path="$3"
  require_regular_contract "$marker" "0:0:100600"
  local line count=0 install_id=""
  while IFS= read -r line || [[ -n "$line" ]]; do
    count=$((count + 1))
    case "$count" in
      1) [[ "$line" == "version=${marker_version}" ]] || return 1 ;;
      2)
        [[ "$line" == install_id=* ]] || return 1
        install_id="${line#install_id=}"
        valid_install_id "$install_id" || return 1
        ;;
      3) [[ "$line" == "role=${expected_role}" ]] || return 1 ;;
      4) [[ "$line" == "path=${expected_path}" ]] || return 1 ;;
      *) return 1 ;;
    esac
  done < "$marker"
  [[ "$count" -eq 4 ]] || return 1
  printf '%s' "$install_id"
}

validate_owned_layout() {
  require_directory_contract "$state_root" "0:0:40711"
  require_directory_contract "${state_root}/agent" "0:0:40700"
  require_directory_contract "${state_root}/fences" "0:0:40700"
  require_directory_contract "${state_root}/runtime" "0:0:40711"
  require_directory_contract "${state_root}/runtime/executions" "0:0:40711"
  require_directory_contract "$cache_parent" "0:0:40700"
  require_directory_contract "$cache_root" "0:0:40700"
  require_only_children "$state_root" \
    "$marker_name" \
    "agent" \
    "fences" \
    "runtime"
  require_only_children "${state_root}/runtime" "executions"
  require_only_children "$cache_parent" "$marker_name" "runner"
  require_empty_directory "${state_root}/agent"
  require_empty_directory "${state_root}/fences"
  require_empty_directory "${state_root}/runtime/executions"
  require_empty_directory "$cache_root"
}

create_directory() {
  local path="$1"
  local mode="$2"
  local created_flag="$3"
  if [[ -e "$path" || -L "$path" ]]; then
    fail "refusing to replace service path: $path"
  fi
  printf -v "$created_flag" '%s' 1
  run_tool mkdir -m "$mode" "$path"
  require_directory_contract "$path" "0:0:40${mode#0}"
}

create_marker() {
  local marker="$1"
  local install_id="$2"
  local role="$3"
  local logical_path="$4"
  local temporary="${marker}.tmp-${install_id}"
  if [[ -e "$marker" || -L "$marker" ||
        -e "$temporary" || -L "$temporary" ]]; then
    fail "refusing to replace an ownership marker: $marker"
  fi
  if [[ "$marker" == "$state_marker" ]]; then
    created_state_marker_temporary=1
  else
    created_cache_marker_temporary=1
  fi
  (
    umask 077
    set -o noclobber
    marker_contents "$install_id" "$role" "$logical_path" > "$temporary"
  )
  require_regular_contract "$temporary" "0:0:100600"
  if [[ "$marker" == "$state_marker" ]]; then
    created_state_marker=1
  else
    created_cache_marker=1
  fi
  run_tool ln "$temporary" "$marker"
  run_tool rm "$temporary"
  if [[ "$marker" == "$state_marker" ]]; then
    created_state_marker_temporary=0
  else
    created_cache_marker_temporary=0
  fi
  local persisted_id
  persisted_id="$(
    read_marker_install_id "$marker" "$role" "$logical_path"
  )" || fail "ownership marker failed validation: $marker"
  [[ "$persisted_id" == "$install_id" ]] ||
    fail "ownership marker changed during publication: $marker"
}

create_layout() {
  local install_id="$1"
  create_directory "$state_root" "0711" created_state_root
  create_directory "${state_root}/agent" "0700" created_agent_directory
  create_directory "${state_root}/fences" "0700" created_fences_directory
  create_directory "${state_root}/runtime" "0711" created_runtime_directory
  create_directory \
    "${state_root}/runtime/executions" \
    "0711" \
    created_executions_directory
  create_directory "$cache_parent" "0700" created_cache_parent
  create_directory "$cache_root" "0700" created_cache_root
  create_marker "$state_marker" "$install_id" "agent-state" "$state_root_path"
  create_marker "$cache_marker" "$install_id" "agent-cache" "$cache_parent_path"
  validate_owned_layout
}

record_state() {
  local record="$1"
  local output exit_code
  if output="$(run_tool dscl . -read "$record" 2>&1)"; then
    printf 'present'
    return
  else
    exit_code="$?"
  fi
  # dscl reports an absent record with both this exit code and Directory
  # Services symbol. Any other read failure is unknown authority, never absence.
  if [[ "$exit_code" -eq 56 &&
        "$output" == *"-14136 (eDSRecordNotFound)"* ]]; then
    printf 'not-found'
    return
  fi
  fail "cannot determine whether Directory Services record exists: $record"
}

read_attribute() {
  local record="$1"
  local attribute="$2"
  local output prefix
  output="$(run_tool dscl . -read "$record" "$attribute")" || return 1
  prefix="${attribute}: "
  [[ "$output" == "${prefix}"* && "$output" != *$'\n'* ]] || return 1
  printf '%s' "${output#"$prefix"}"
}

directory_id_snapshot() {
  local users groups name value
  users="$(run_tool dscl . -list /Users UniqueID)" || return 1
  groups="$(run_tool dscl . -list /Groups PrimaryGroupID)" || return 1
  while IFS=$' \t' read -r name value; do
    [[ -n "$name" ]] && printf 'user %s %s\n' "$name" "$value"
  done <<< "$users"
  while IFS=$' \t' read -r name value; do
    [[ -n "$name" ]] && printf 'group %s %s\n' "$name" "$value"
  done <<< "$groups"
}

require_unique_directory_id() {
  local expected_kind="$1"
  local expected_name="$2"
  local expected_id="$3"
  local snapshot kind name value numeric_value matches=0
  snapshot="$(directory_id_snapshot)" ||
    fail "cannot enumerate Directory Services IDs"
  while IFS=' ' read -r kind name value; do
    [[ "$value" =~ ^-?[0-9]{1,10}$ ]] ||
      fail "Directory Services returned a non-numeric ID"
    if [[ "$value" == -* ]]; then
      continue
    fi
    numeric_value="$((10#$value))"
    if [[ "$numeric_value" == "$expected_id" ]]; then
      matches=$((matches + 1))
      if [[ "$kind" != "$expected_kind" || "$name" != "$expected_name" ]]; then
        fail "runner Directory Services ID is not unique"
      fi
    fi
  done <<< "$snapshot"
  [[ "$matches" -eq 1 ]] ||
    fail "runner Directory Services ID is not unique"
}

next_directory_ids() {
  local snapshot kind name value numeric_value max=499
  snapshot="$(directory_id_snapshot)" ||
    fail "cannot enumerate Directory Services IDs"
  while IFS=' ' read -r kind name value; do
    [[ "$value" =~ ^-?[0-9]{1,10}$ ]] ||
      fail "Directory Services returned a non-numeric ID"
    if [[ "$value" == -* ]]; then
      continue
    fi
    numeric_value="$((10#$value))"
    if [[ "$numeric_value" -ge 500 &&
          "$numeric_value" -lt 2147483647 &&
          "$numeric_value" -gt "$max" ]]; then
      max="$numeric_value"
    fi
  done <<< "$snapshot"
  if [[ "$max" -ge 2147483645 ]]; then
    fail "no safe Directory Services IDs remain"
  fi
  printf '%s %s' "$((max + 1))" "$((max + 2))"
}

validate_group() {
  local install_id="$1"
  local expected_real_name="Tewake native runner slot 0 [${install_id}]"
  local runner_gid group_real_name group_password
  runner_gid="$(read_attribute "/Groups/${runner_group}" PrimaryGroupID)" ||
    fail "runner group has no PrimaryGroupID"
  group_real_name="$(read_attribute "/Groups/${runner_group}" RealName)" ||
    fail "runner group has no RealName"
  group_password="$(read_attribute "/Groups/${runner_group}" Password)" ||
    fail "runner group has no disabled password"
  if [[ ! "$runner_gid" =~ ^[0-9]{1,10}$ ]]; then
    fail "existing runner group does not match the owned slot contract"
  fi
  runner_gid="$((10#$runner_gid))"
  if [[ "$runner_gid" -lt 500 ||
        "$runner_gid" -ge 2147483647 ||
        "$group_real_name" != "$expected_real_name" ||
        "$group_password" != "*" ]]; then
    fail "existing runner group does not match the owned slot contract"
  fi
  require_unique_directory_id "group" "$runner_group" "$runner_gid"
  printf '%s' "$runner_gid"
}

validate_user() {
  local install_id="$1"
  local runner_gid="$2"
  local expected_real_name="Tewake native runner slot 0 [${install_id}]"
  local runner_uid configured_gid configured_real_name configured_shell
  local configured_hidden configured_home configured_auth configured_password
  runner_uid="$(read_attribute "/Users/${runner_account}" UniqueID)" ||
    fail "runner account has no UniqueID"
  configured_gid="$(read_attribute "/Users/${runner_account}" PrimaryGroupID)" ||
    fail "runner account has no PrimaryGroupID"
  configured_real_name="$(read_attribute "/Users/${runner_account}" RealName)" ||
    fail "runner account has no RealName"
  configured_shell="$(read_attribute "/Users/${runner_account}" UserShell)" ||
    fail "runner account has no UserShell"
  configured_hidden="$(read_attribute "/Users/${runner_account}" IsHidden)" ||
    fail "runner account has no hidden flag"
  configured_home="$(read_attribute "/Users/${runner_account}" NFSHomeDirectory)" ||
    fail "runner account has no home contract"
  configured_auth="$(read_attribute "/Users/${runner_account}" AuthenticationAuthority)" ||
    fail "runner account has no disabled authentication authority"
  configured_password="$(read_attribute "/Users/${runner_account}" Password)" ||
    fail "runner account has no disabled password"
  if [[ ! "$runner_uid" =~ ^[0-9]{1,10}$ ]]; then
    fail "existing runner account does not match the owned non-login slot contract"
  fi
  runner_uid="$((10#$runner_uid))"
  if [[ "$runner_uid" -lt 500 ||
        "$runner_uid" -ge 2147483647 ||
        "$configured_gid" != "$runner_gid" ||
        "$configured_real_name" != "$expected_real_name" ||
        "$configured_shell" != "/usr/bin/false" ||
        "$configured_hidden" != "1" ||
        "$configured_home" != "/var/empty" ||
        "$configured_auth" != ";DisabledUser;" ||
        "$configured_password" != "*" ]]; then
    fail "existing runner account does not match the owned non-login slot contract"
  fi
  require_unique_directory_id "user" "$runner_account" "$runner_uid"
}

create_group() {
  local install_id="$1"
  local runner_gid="$2"
  local real_name="Tewake native runner slot 0 [${install_id}]"
  # Create the record and its random ownership identity in one dscl operation.
  # Rollback never deletes a record unless this exact identity remains present.
  created_group=1
  run_tool dscl . -create "/Groups/${runner_group}" RealName "$real_name"
  run_tool dscl . -create "/Groups/${runner_group}" PrimaryGroupID "$runner_gid"
  run_tool dscl . -create "/Groups/${runner_group}" Password "*"
}

create_user() {
  local install_id="$1"
  local runner_uid="$2"
  local runner_gid="$3"
  local real_name="Tewake native runner slot 0 [${install_id}]"
  created_user=1
  run_tool dscl . -create "/Users/${runner_account}" RealName "$real_name"
  run_tool dscl . -create "/Users/${runner_account}" UniqueID "$runner_uid"
  run_tool dscl . -create "/Users/${runner_account}" PrimaryGroupID "$runner_gid"
  run_tool dscl . -create "/Users/${runner_account}" NFSHomeDirectory "/var/empty"
  run_tool dscl . -create "/Users/${runner_account}" UserShell "/usr/bin/false"
  run_tool dscl . -create "/Users/${runner_account}" IsHidden 1
  run_tool dscl . -create "/Users/${runner_account}" AuthenticationAuthority ";DisabledUser;"
  run_tool dscl . -create "/Users/${runner_account}" Password "*"
}

regular_contract_matches() {
  local path="$1"
  local expected="$2"
  local maximum_links="${3:-1}"
  local actual links
  [[ -f "$path" && ! -L "$path" ]] || return 1
  actual="$(stat_contract "$path" 2>/dev/null)" || return 1
  links="$(run_tool stat -f '%l' "$path" 2>/dev/null)" || return 1
  [[ "$actual" == "$expected" &&
    "$links" =~ ^[0-9]+$ &&
    "$links" -ge 1 &&
    "$links" -le "$maximum_links" ]]
}

directory_contract_matches() {
  local path="$1"
  local expected="$2"
  local actual
  [[ -d "$path" && ! -L "$path" ]] || return 1
  actual="$(stat_contract "$path" 2>/dev/null)" || return 1
  [[ "$actual" == "$expected" ]]
}

marker_matches() {
  local marker="$1"
  local expected_role="$2"
  local expected_path="$3"
  local actual expected
  regular_contract_matches "$marker" "0:0:100600" 2 || return 1
  actual="$(<"$marker")" || return 1
  expected="$(marker_contents "$install_id" "$expected_role" "$expected_path")"
  [[ "$actual" == "$expected" ]]
}

plist_matches_package() {
  local path="$1"
  regular_contract_matches "$path" "0:0:100600" 2 &&
    run_tool cmp -s "$plist_source" "$path"
}

rollback_remove_marker() {
  local created="$1"
  local marker="$2"
  local expected_role="$3"
  local expected_path="$4"
  [[ "$created" -eq 1 ]] || return 0
  [[ -e "$marker" || -L "$marker" ]] || return 0
  marker_matches "$marker" "$expected_role" "$expected_path" || return 1
  run_tool rm "$marker"
}

rollback_remove_plist() {
  local created="$1"
  local path="$2"
  [[ "$created" -eq 1 ]] || return 0
  [[ -e "$path" || -L "$path" ]] || return 0
  plist_matches_package "$path" || return 1
  run_tool rm "$path"
}

rollback_remove_record() {
  local created="$1"
  local record="$2"
  local expected_real_name="$3"
  local actual output exit_code
  [[ "$created" -eq 1 ]] || return 0
  if ! actual="$(read_attribute "$record" RealName)"; then
    if output="$(run_tool dscl . -read "$record" 2>&1)"; then
      return 1
    else
      exit_code="$?"
    fi
    if [[ "$exit_code" -eq 56 &&
          "$output" == *"-14136 (eDSRecordNotFound)"* ]]; then
      return 0
    fi
    return 1
  fi
  [[ "$actual" == "$expected_real_name" ]] || return 1
  run_tool dscl . -delete "$record"
}

rollback_remove_directory() {
  local created="$1"
  local path="$2"
  local expected="$3"
  [[ "$created" -eq 1 ]] || return 0
  [[ -e "$path" || -L "$path" ]] || return 0
  directory_contract_matches "$path" "$expected" || return 1
  run_tool rmdir "$path"
}

rollback_install() {
  local original_exit="$?"
  local rollback_failed=0
  local state_temporary="${state_marker}.tmp-${install_id:-unknown}"
  local cache_temporary="${cache_marker}.tmp-${install_id:-unknown}"
  local expected_real_name="Tewake native runner slot 0 [${install_id:-unknown}]"
  trap - EXIT
  if [[ "$install_committed" -eq 1 || "$transaction_active" -eq 0 ]]; then
    return "$original_exit"
  fi
  set +e

  if [[ ( "$bootstrap_attempted" -eq 1 || "$loaded_service" -eq 1 ) ]] &&
    run_tool launchctl print "system/${label}" >/dev/null 2>&1; then
    if plist_matches_package "$plist_target"; then
      run_tool launchctl bootout "system/${label}" ||
        rollback_failed=1
    else
      rollback_failed=1
    fi
  fi
  rollback_remove_plist "$created_plist_target" "$plist_target" ||
    rollback_failed=1
  rollback_remove_plist "$created_plist_temporary" "${plist_target}.tmp-${install_id}" ||
    rollback_failed=1
  rollback_remove_record "$created_user" "/Users/${runner_account}" "$expected_real_name" ||
    rollback_failed=1
  rollback_remove_record "$created_group" "/Groups/${runner_group}" "$expected_real_name" ||
    rollback_failed=1
  rollback_remove_marker \
    "$created_cache_marker" \
    "$cache_marker" \
    "agent-cache" \
    "$cache_parent_path" ||
    rollback_failed=1
  rollback_remove_marker \
    "$created_cache_marker_temporary" \
    "$cache_temporary" \
    "agent-cache" \
    "$cache_parent_path" ||
    rollback_failed=1
  rollback_remove_marker \
    "$created_state_marker" \
    "$state_marker" \
    "agent-state" \
    "$state_root_path" ||
    rollback_failed=1
  rollback_remove_marker \
    "$created_state_marker_temporary" \
    "$state_temporary" \
    "agent-state" \
    "$state_root_path" ||
    rollback_failed=1

  rollback_remove_directory \
    "$created_cache_root" \
    "$cache_root" \
    "0:0:40700" ||
    rollback_failed=1
  rollback_remove_directory \
    "$created_cache_parent" \
    "$cache_parent" \
    "0:0:40700" ||
    rollback_failed=1
  rollback_remove_directory \
    "$created_executions_directory" \
    "${state_root}/runtime/executions" \
    "0:0:40711" ||
    rollback_failed=1
  rollback_remove_directory \
    "$created_runtime_directory" \
    "${state_root}/runtime" \
    "0:0:40711" ||
    rollback_failed=1
  rollback_remove_directory \
    "$created_fences_directory" \
    "${state_root}/fences" \
    "0:0:40700" ||
    rollback_failed=1
  rollback_remove_directory \
    "$created_agent_directory" \
    "${state_root}/agent" \
    "0:0:40700" ||
    rollback_failed=1
  rollback_remove_directory \
    "$created_state_root" \
    "$state_root" \
    "0:0:40711" ||
    rollback_failed=1

  if [[ "$rollback_failed" -ne 0 ]]; then
    echo "macOS install failed and verified rollback was incomplete; inspect owned partial state before retrying" >&2
  fi
  if [[ "$original_exit" -eq 0 ]]; then
    original_exit=1
  fi
  exit "$original_exit"
}

# Validate every authority and target before the first filesystem, Directory
# Services, plist, or launchd mutation.
require_safe_ancestor_chain "/usr/local/libexec"
require_safe_ancestor_chain "/Library/LaunchDaemons"
require_safe_ancestor_chain "/Library/Application Support"
require_safe_ancestor_chain "/Library/Caches"
require_regular_contract "$binary" "0:0:100755"
if [[ ! -f "$plist_source" || -L "$plist_source" ]]; then
  fail "launchd property list is missing or unsafe"
fi
run_tool plutil -lint "$plist_source" >/dev/null
if run_tool launchctl print "system/${label}" >/dev/null 2>&1; then
  fail "${label} is already loaded; use the documented upgrade flow"
fi

layout_state=""
install_id=""
if [[ ! -e "$state_root" && ! -L "$state_root" &&
      ! -e "$cache_parent" && ! -L "$cache_parent" ]]; then
  layout_state="new"
  install_id="$(run_tool uuidgen)"
  valid_install_id "$install_id" ||
    fail "uuidgen returned an invalid install identity"
elif [[ -d "$state_root" && ! -L "$state_root" &&
        -d "$cache_parent" && ! -L "$cache_parent" ]]; then
  layout_state="owned"
  validate_owned_layout
  install_id="$(
    read_marker_install_id "$state_marker" "agent-state" "$state_root_path"
  )" || fail "state root has no valid Tewake ownership marker"
  cache_install_id="$(
    read_marker_install_id "$cache_marker" "agent-cache" "$cache_parent_path"
  )" || fail "cache root has no valid Tewake ownership marker"
  [[ "$cache_install_id" == "$install_id" ]] ||
    fail "Tewake ownership markers do not share one install identity"
else
  fail "refusing foreign or partial Tewake service roots"
fi

group_present=0
user_present=0
group_state="$(record_state "/Groups/${runner_group}")"
user_state="$(record_state "/Users/${runner_account}")"
[[ "$group_state" == "present" ]] && group_present=1
[[ "$user_state" == "present" ]] && user_present=1
if [[ "$layout_state" == "new" && ( "$group_present" -ne 0 || "$user_present" -ne 0 ) ]]; then
  fail "refusing pre-existing runner account or group without an owned layout"
fi
if [[ "$group_present" -eq 0 && "$user_present" -ne 0 ]]; then
  fail "runner account exists without its owned primary group"
fi

runner_gid=""
runner_uid=""
if [[ "$group_present" -eq 1 ]]; then
  runner_gid="$(validate_group "$install_id")"
fi
if [[ "$user_present" -eq 1 ]]; then
  validate_user "$install_id" "$runner_gid"
fi
allocated_ids=""
if [[ "$group_present" -eq 0 ]]; then
  allocated_ids="$(next_directory_ids)" ||
    fail "cannot allocate runner Directory Services IDs"
  read -r runner_gid runner_uid <<< "$allocated_ids"
elif [[ "$user_present" -eq 0 ]]; then
  allocated_ids="$(next_directory_ids)" ||
    fail "cannot allocate runner Directory Services ID"
  read -r runner_uid _ <<< "$allocated_ids"
fi

plist_state="new"
if [[ -e "$plist_target" || -L "$plist_target" ]]; then
  if [[ "$layout_state" != "owned" ]]; then
    fail "refusing pre-existing launchd property list without an owned layout"
  fi
  require_regular_contract "$plist_target" "0:0:100600"
  run_tool cmp -s "$plist_source" "$plist_target" ||
    fail "existing launchd property list differs from this package"
  plist_state="owned"
fi

transaction_active=1
trap rollback_install EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ "$layout_state" == "new" ]]; then
  create_layout "$install_id"
fi
if [[ "$group_present" -eq 0 ]]; then
  create_group "$install_id" "$runner_gid"
  runner_gid="$(validate_group "$install_id")"
fi
if [[ "$user_present" -eq 0 ]]; then
  create_user "$install_id" "$runner_uid" "$runner_gid"
  validate_user "$install_id" "$runner_gid"
fi
if [[ "$plist_state" == "new" ]]; then
  plist_temporary="${plist_target}.tmp-${install_id}"
  if [[ -e "$plist_temporary" || -L "$plist_temporary" ]]; then
    fail "refusing to replace launchd staging state: $plist_temporary"
  fi
  created_plist_temporary=1
  run_tool install -o root -g wheel -m 0600 "$plist_source" "$plist_temporary"
  require_regular_contract "$plist_temporary" "0:0:100600"
  run_tool cmp -s "$plist_source" "$plist_temporary" ||
    fail "launchd staging property list changed during publication"
  created_plist_target=1
  run_tool ln "$plist_temporary" "$plist_target"
  run_tool rm "$plist_temporary"
  created_plist_temporary=0
  require_regular_contract "$plist_target" "0:0:100600"
  run_tool cmp -s "$plist_source" "$plist_target" ||
    fail "installed launchd property list changed during publication"
fi
bootstrap_attempted=1
run_tool launchctl bootstrap system "$plist_target"
loaded_service=1
install_committed=1
