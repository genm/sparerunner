#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

mkdir -p internal/api/gen web/src/api/generated
go tool oapi-codegen --config api/oapi-codegen.yaml api/openapi.yaml
pnpm --dir api/codegen generate
pnpm --dir web exec oxfmt src/api/generated/schema.ts
