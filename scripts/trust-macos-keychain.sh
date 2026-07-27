#!/usr/bin/env bash
set -euo pipefail

# Clear the partition list of Tewake's macOS Keychain items so a locally rebuilt
# binary can read them without a login-password prompt.
#
# The store already creates its items with a trust-all decrypt ACL (see
# newNativeDarwinCredentialStore in internal/enroll/persistence_darwin.go), but
# macOS additionally records a partition_id ACL naming the creating process. Go
# links host binaries with an ad-hoc signature and no team identifier, so the
# partition entry ends up as `cdhash:<hash>` and every rebuild changes that hash.
# The mismatch — not the decrypt ACL — is what raises the prompt on each run.
#
# Emptying the partition list keeps access within the boundary the trust-all ACL
# already grants: any process of the same user. It does not widen access to the
# separate runner UID, which is the boundary the design relies on.
#
# Re-run this after an enrollment creates new Keychain items.

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "$0: macOS only" >&2
  exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source_file="$repo_root/internal/enroll/persistence_darwin.go"

# Read the service name from the store itself so this script cannot drift from
# the constant that actually names the items.
service="$(sed -n 's/^[[:space:]]*darwinKeychainService[[:space:]]*=[[:space:]]*"\(.*\)".*/\1/p' "$source_file")"
if [[ -z "$service" ]]; then
  echo "$0: could not read darwinKeychainService from $source_file" >&2
  exit 1
fi

list_accounts() {
  security dump-keychain 2>/dev/null | awk -v svc="$service" '
    /^keychain: /                { acct = "" }
    /"acct"<blob>="/             { s = $0; sub(/^.*"acct"<blob>="/, "", s); sub(/".*$/, "", s); acct = s }
    /"svce"<blob>="/             { s = $0; sub(/^.*"svce"<blob>="/, "", s); sub(/".*$/, "", s)
                                   if (s == svc && acct != "") print acct }
  '
}

# Built with a read loop rather than mapfile: macOS ships bash 3.2, which the
# justfile also uses as its shell.
accounts=()
while IFS= read -r account; do
  accounts+=("$account")
done < <(list_accounts)

if [[ ${#accounts[@]} -eq 0 ]]; then
  echo "$0: no Keychain items for service $service" >&2
  echo "$0: nothing to trust — enroll this host first" >&2
  exit 1
fi

echo "service: $service"
echo "items:   ${#accounts[@]}"
echo "macOS asks for your login password once per item."
echo

for account in "${accounts[@]}"; do
  # -S "" removes every partition entry, so the partition check no longer
  # narrows the trust-all decrypt ACL to one binary hash.
  if ! security set-generic-password-partition-list \
    -S "" -s "$service" -a "$account" >/dev/null; then
    echo "$0: failed to clear the partition list for $account" >&2
    exit 1
  fi
  echo "cleared: $account"
done

echo
echo "partition entries now recorded for these items:"
# Printed rather than asserted: the exact rendering of an emptied partition ACL
# is security(1)'s own, so an operator reads the real state instead of trusting
# a pattern match here.
security dump-keychain -a 2>/dev/null | awk -v svc="$service" '
  /^keychain: /    { svce = ""; acct = ""; in_partition = 0 }
  /"acct"<blob>="/ { s = $0; sub(/^.*"acct"<blob>="/, "", s); sub(/".*$/, "", s); acct = s }
  /"svce"<blob>="/ { s = $0; sub(/^.*"svce"<blob>="/, "", s); sub(/".*$/, "", s); svce = s }
  /partition_id/   { in_partition = 1; next }
  in_partition && /description: / {
    in_partition = 0
    if (svce == svc) {
      s = $0
      sub(/^[[:space:]]*description: /, "", s)
      printf "  %s -> %s\n", acct, s
    }
  }
'
