#!/usr/bin/env bash
set -euo pipefail

output_dir="${1:-licenses}"
if [[ -e "$output_dir" ]]; then
  printf 'license bundle destination already exists: %s\n' "$output_dir" >&2
  exit 1
fi

temporary_root="$(mktemp -d "${TMPDIR:-/tmp}/sparerunner-licenses.XXXXXX")"
trap 'rm -rf "$temporary_root"' EXIT

module="github.com/google/go-licenses/v2@v2.0.1"
mkdir -p "$temporary_root/bundle"
go run "$module" report ./... > "$temporary_root/bundle/THIRD_PARTY_LICENSES.csv"
go run "$module" save ./... --save_path="$temporary_root/bundle/THIRD_PARTY_LICENSES"

{
  printf 'SpareRunner third-party notices\n'
  printf '================================\n\n'
  printf 'The following project license applies to SpareRunner itself:\n\n'
  cat LICENSE
  printf '\n\nThird-party dependency license files are under THIRD_PARTY_LICENSES/.\n'
  printf 'The machine-readable dependency report is THIRD_PARTY_LICENSES.csv.\n'
} > "$temporary_root/bundle/NOTICE"

mv "$temporary_root/bundle" "$output_dir"
trap - EXIT
