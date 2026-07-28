#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
result_dir="${repo_root}/output/test-results"
result_file="${result_dir}/linux-platform-root.json"
go_image="docker.io/library/golang:1.26.5-bookworm@sha256:1ecb7edf62a0408027bd5729dfd6b1b8766e578e8df93995b225dfd0944eb651"

mkdir -p "${result_dir}"

# Fail closed with the actual prerequisite rather than a confusing "command not
# found" three lines into the run. Never skip: a boundary test that silently
# reports success is worse than no boundary test.
if ! command -v docker >/dev/null 2>&1; then
  cat >&2 <<'MESSAGE'
docker is required for the privileged Linux runner boundary test.

This test runs the root ownership-handoff path inside a disposable, networkless
container, so it needs a Docker-compatible runtime (Docker Desktop, OrbStack, or
Colima) with a running daemon.

It is not part of `just check-push`, so you can push without it; required CI runs
it on every pull request.
MESSAGE
  exit 1
fi

# Resolve every module before disabling the container network. The root test
# process receives source and dependencies read-only, so it can exercise
# ownership handoff without being able to rewrite the checkout or fetch code.
go mod download
sparerunner_module_cache="$(go env GOMODCACHE)"
if [[ ! -d "${sparerunner_module_cache}" ]]; then
  echo "Go module cache is unavailable: ${sparerunner_module_cache}" >&2
  exit 1
fi

# Go executes ephemeral test binaries from /tmp inside this disposable,
# networkless container. Keep nosuid/nodev while allowing those binaries.
docker run --rm \
  --network none \
  --mount "type=bind,src=${repo_root},dst=/src,readonly" \
  --mount "type=bind,src=${sparerunner_module_cache},dst=/go/pkg/mod,readonly" \
  --tmpfs /tmp:rw,nosuid,nodev,exec,mode=1777 \
  --workdir /src \
  "${go_image}" \
  go test -race -json -count=1 -tags=sparerunner_privileged \
    ./internal/platform/linux \
    ./internal/runner \
    ./packaging/linux \
  | tee "${result_file}"
