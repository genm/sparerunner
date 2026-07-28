#!/usr/bin/env bash
# Run every fuzz target in the repository for a short local budget.
#
# `go test` already runs each target's committed seed corpus, so this script is
# about generating new inputs. The nightly deep-verification workflow runs the
# same targets one job each with a much larger budget; a test in
# test/verification asserts that its matrix lists every target this script
# discovers, so neither surface can silently lose one.
set -euo pipefail

fuzz_time="${1:-30s}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

# Read into a plain array rather than with mapfile, because the macOS system
# bash is 3.2 and does not have it.
targets=()
while IFS= read -r target; do
  targets+=("${target}")
done < <(
  grep -rhoE '^func Fuzz[A-Za-z0-9_]*\(' --include='*_test.go' cmd internal test |
    sed -E 's/^func //; s/\($//' |
    sort -u
)

if [[ "${#targets[@]}" -eq 0 ]]; then
  echo "no fuzz targets found" >&2
  exit 1
fi

for target in "${targets[@]}"; do
  package="$(
    grep -rlE "^func ${target}\(" --include='*_test.go' cmd internal test |
      head -n 1 |
      xargs dirname
  )"
  echo "==> ${target} in ./${package} for ${fuzz_time}"
  go test -run '^$' -fuzz "^${target}$" -fuzztime "${fuzz_time}" "./${package}"
done
