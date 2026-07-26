#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
target_arch="$(go env GOARCH)"
case "${target_arch}" in
  amd64 | arm64) ;;
  *)
    echo "unsupported Docker test architecture: ${target_arch}" >&2
    exit 1
    ;;
esac

output_dir="${repo_root}/output/test-binaries"
mkdir -p "${output_dir}"
CGO_ENABLED=0 GOOS=linux GOARCH="${target_arch}" \
  go test -c -o "${output_dir}/runner-linux-${target_arch}.test" \
  "${repo_root}/internal/runner"
CGO_ENABLED=0 GOOS=linux GOARCH="${target_arch}" \
  go test -c -o "${output_dir}/runner-integration-linux-${target_arch}.test" \
  "${repo_root}/test/integration/runner"

docker run --rm \
  --network none \
  --read-only \
  --cap-drop ALL \
  --security-opt no-new-privileges \
  --user 65532:65532 \
  --tmpfs /tmp:rw,noexec,nosuid,nodev,mode=0700,uid=65532,gid=65532 \
  --mount "type=bind,src=${output_dir}/runner-linux-${target_arch}.test,dst=/opt/runner.test,readonly" \
  --mount "type=bind,src=${output_dir}/runner-integration-linux-${target_arch}.test,dst=/opt/runner-integration.test,readonly" \
  alpine@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce \
  /bin/sh -eu -c '
    /opt/runner.test -test.v
    /opt/runner-integration.test -test.v
  '
