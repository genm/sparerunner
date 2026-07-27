#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
go_contract="${repo_root}/internal/api/gen/openapi.gen.go"
typescript_contract="${repo_root}/web/src/api/generated/schema.ts"

if [[ ! -f "${go_contract}" || ! -f "${typescript_contract}" ]]; then
  echo "generated API contracts are missing; run 'just generate-api'" >&2
  exit 1
fi

scratch_dir="$(mktemp -d "${TMPDIR:-/tmp}/tewake-api-contract.XXXXXX")"
trap 'rm -rf "${scratch_dir}"' EXIT
cp "${go_contract}" "${scratch_dir}/openapi.gen.go"
cp "${typescript_contract}" "${scratch_dir}/schema.ts"

"${repo_root}/scripts/generate-api.sh"

if ! cmp -s "${scratch_dir}/openapi.gen.go" "${go_contract}"; then
  echo "Go API contract is stale; run 'just generate-api'" >&2
  diff -u "${scratch_dir}/openapi.gen.go" "${go_contract}" || true
  exit 1
fi
if ! cmp -s "${scratch_dir}/schema.ts" "${typescript_contract}"; then
  echo "TypeScript API contract is stale; run 'just generate-api'" >&2
  diff -u "${scratch_dir}/schema.ts" "${typescript_contract}" || true
  exit 1
fi
