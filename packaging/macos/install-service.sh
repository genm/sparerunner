#!/bin/bash
set -euo pipefail

readonly label="com.genm.tewake.agent"
readonly runner_account="tewake-runner-0"
readonly runner_group="tewake-runner-0"
readonly binary="/usr/local/libexec/tewake-agent"
readonly plist_source="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/launchd/${label}.plist}"
readonly plist_target="/Library/LaunchDaemons/${label}.plist"
readonly state_root="/Library/Application Support/Tewake"
readonly cache_root="/Library/Caches/com.genm.tewake/runner"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "install-service.sh must run as root" >&2
  exit 1
fi
if [[ ! -f "$binary" || -L "$binary" || ! -x "$binary" ]]; then
  echo "install the root-owned tewake-agent binary at $binary first" >&2
  exit 1
fi
if [[ "$(stat -f '%u:%g:%Mp%Lp' "$binary")" != "0:0:100755" ]]; then
  echo "$binary must be root:wheel 0755" >&2
  exit 1
fi
if [[ ! -f "$plist_source" || -L "$plist_source" ]]; then
  echo "launchd property list is missing or unsafe" >&2
  exit 1
fi
/usr/bin/plutil -lint "$plist_source" >/dev/null
if /bin/launchctl print "system/${label}" >/dev/null 2>&1; then
  echo "${label} is already loaded; use the documented upgrade flow" >&2
  exit 1
fi

next_directory_id() {
  /usr/bin/dscl . -list /Users UniqueID
  /usr/bin/dscl . -list /Groups PrimaryGroupID
}

if ! /usr/bin/dscl . -read "/Groups/${runner_group}" >/dev/null 2>&1; then
  next_id="$(
    next_directory_id |
      /usr/bin/awk '
        $NF ~ /^[0-9]+$/ && $NF >= 500 && $NF < 2147483647 {
          if ($NF > max) max = $NF
        }
        END {
          if (max == 0) max = 500
          print max + 1
        }
      '
  )"
  /usr/bin/dscl . -create "/Groups/${runner_group}"
  /usr/bin/dscl . -create "/Groups/${runner_group}" PrimaryGroupID "$next_id"
  /usr/bin/dscl . -create "/Groups/${runner_group}" RealName "Tewake native runner slot 0"
fi
runner_gid="$(/usr/bin/dscl . -read "/Groups/${runner_group}" PrimaryGroupID | /usr/bin/awk '{print $2}')"
if [[ ! "$runner_gid" =~ ^[0-9]+$ || "$runner_gid" -le 0 ]]; then
  echo "runner group has an invalid PrimaryGroupID" >&2
  exit 1
fi

if ! /usr/bin/dscl . -read "/Users/${runner_account}" >/dev/null 2>&1; then
  next_id="$(
    next_directory_id |
      /usr/bin/awk '
        $NF ~ /^[0-9]+$/ && $NF >= 500 && $NF < 2147483647 {
          if ($NF > max) max = $NF
        }
        END {
          if (max == 0) max = 500
          print max + 1
        }
      '
  )"
  /usr/bin/dscl . -create "/Users/${runner_account}"
  /usr/bin/dscl . -create "/Users/${runner_account}" UniqueID "$next_id"
  /usr/bin/dscl . -create "/Users/${runner_account}" PrimaryGroupID "$runner_gid"
  /usr/bin/dscl . -create "/Users/${runner_account}" RealName "Tewake native runner slot 0"
  /usr/bin/dscl . -create "/Users/${runner_account}" NFSHomeDirectory "/var/empty"
  /usr/bin/dscl . -create "/Users/${runner_account}" UserShell "/usr/bin/false"
  /usr/bin/dscl . -create "/Users/${runner_account}" IsHidden 1
  /usr/bin/dscl . -create "/Users/${runner_account}" AuthenticationAuthority ";DisabledUser;"
  /usr/bin/dscl . -create "/Users/${runner_account}" Password "*"
fi

runner_uid="$(/usr/bin/dscl . -read "/Users/${runner_account}" UniqueID | /usr/bin/awk '{print $2}')"
configured_gid="$(/usr/bin/dscl . -read "/Users/${runner_account}" PrimaryGroupID | /usr/bin/awk '{print $2}')"
configured_shell="$(/usr/bin/dscl . -read "/Users/${runner_account}" UserShell | /usr/bin/awk '{print $2}')"
configured_hidden="$(/usr/bin/dscl . -read "/Users/${runner_account}" IsHidden | /usr/bin/awk '{print $2}')"
configured_home="$(/usr/bin/dscl . -read "/Users/${runner_account}" NFSHomeDirectory | /usr/bin/awk '{print $2}')"
configured_auth="$(/usr/bin/dscl . -read "/Users/${runner_account}" AuthenticationAuthority | /usr/bin/awk '{$1=""; sub(/^ /, ""); print}')"
configured_password="$(/usr/bin/dscl . -read "/Users/${runner_account}" Password | /usr/bin/awk '{print $2}')"
if [[ ! "$runner_uid" =~ ^[0-9]+$ || "$runner_uid" -le 0 ||
      "$configured_gid" != "$runner_gid" ||
      "$configured_shell" != "/usr/bin/false" ||
      "$configured_hidden" != "1" ||
      "$configured_home" != "/var/empty" ||
      "$configured_auth" != ";DisabledUser;" ||
      "$configured_password" != "*" ]]; then
  echo "existing runner account does not match the non-login slot contract" >&2
  exit 1
fi

ensure_directory() {
  local directory="$1"
  if [[ -L "$directory" || ( -e "$directory" && ! -d "$directory" ) ]]; then
    echo "refusing unsafe service directory: $directory" >&2
    exit 1
  fi
  /bin/mkdir -p "$directory"
  if [[ -L "$directory" || ! -d "$directory" ]]; then
    echo "service directory changed during creation: $directory" >&2
    exit 1
  fi
}

ensure_directory "$state_root"
ensure_directory "${state_root}/agent"
ensure_directory "${state_root}/fences"
ensure_directory "${state_root}/runtime/executions"
ensure_directory "/Library/Caches/com.genm.tewake"
ensure_directory "$cache_root"
/usr/sbin/chown root:wheel \
  "$state_root" \
  "${state_root}/agent" \
  "${state_root}/fences" \
  "${state_root}/runtime" \
  "${state_root}/runtime/executions" \
  "/Library/Caches/com.genm.tewake" \
  "$cache_root"
/bin/chmod 0700 "${state_root}/agent" "${state_root}/fences" "$cache_root"
/bin/chmod 0711 "$state_root" "${state_root}/runtime" "${state_root}/runtime/executions"

/usr/bin/install -o root -g wheel -m 0600 "$plist_source" "$plist_target"
/usr/bin/plutil -lint "$plist_target" >/dev/null
/bin/launchctl bootstrap system "$plist_target"
