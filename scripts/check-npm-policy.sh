#!/usr/bin/env bash
set -euo pipefail

# Proves that the npm supply-chain policy is actually in effect for every pnpm
# project, not merely written down.
#
# pnpm reads the .npmrc of the directory it installs in. Every install in this
# repository runs as `pnpm --dir <project>`, and without a pnpm workspace pnpm
# never walks up to the repository root — so a policy that lives only in the root
# .npmrc is silently off. This check fails closed when that happens again.
#
# The root .npmrc is the single source of truth. Each project's effective
# configuration must match it key for key.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
root_npmrc="${repo_root}/.npmrc"

# Settings that must hold wherever dependencies are resolved. Root-only keys such
# as prefer-workspace-packages are excluded: they are meaningless without a pnpm
# workspace and are not part of the supply-chain contract.
enforced_keys=(
  engine-strict
  minimum-release-age
  strict-peer-dependencies
)

projects=(
  web
  api/codegen
)

if [[ ! -f "${root_npmrc}" ]]; then
  echo "root .npmrc is missing; it is the source of truth for npm policy" >&2
  exit 1
fi

failed=0

for key in "${enforced_keys[@]}"; do
  expected="$(grep -E "^${key}=" "${root_npmrc}" | head -n 1 | cut -d= -f2- || true)"
  if [[ -z "${expected}" ]]; then
    echo "root .npmrc does not set ${key}; add it or remove it from this check" >&2
    failed=1
    continue
  fi
  for project in "${projects[@]}"; do
    actual="$(pnpm --dir "${repo_root}/${project}" config get "${key}")"
    if [[ "${actual}" != "${expected}" ]]; then
      printf '%s: %s is %s, expected %s from the root .npmrc\n' \
        "${project}" "${key}" "${actual}" "${expected}" >&2
      printf '  fix: add "%s=%s" to %s/.npmrc\n' \
        "${key}" "${expected}" "${project}" >&2
      failed=1
    fi
  done
done

if [[ "${failed}" -ne 0 ]]; then
  echo "npm supply-chain policy is not in effect for every pnpm project" >&2
  exit 1
fi

echo "npm supply-chain policy is in effect for: ${projects[*]}"
