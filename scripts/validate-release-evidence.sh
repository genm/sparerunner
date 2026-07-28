#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 || -z "$1" ]]; then
  echo "usage: $0 <manifest.json>" >&2
  exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Resolve the module from the script location so the release gate remains
# executable when an operator invokes it from a separate evidence directory.
cd "$repo_root"
exec go run ./cmd/sprun evidence validate --file "$1"
