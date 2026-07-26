#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
test_root="$(mktemp -d)"
cleanup() {
  rm -rf "${test_root}"
}
trap cleanup EXIT

target_arch="$(go env GOARCH)"
case "${target_arch}" in
  amd64 | arm64) ;;
  *)
    echo "unsupported Docker test architecture: ${target_arch}" >&2
    exit 1
    ;;
esac

CGO_ENABLED=0 GOOS=linux GOARCH="${target_arch}" \
  go build -trimpath -o "${test_root}/tewake" "${repo_root}/cmd/tewake"
CGO_ENABLED=0 GOOS=linux GOARCH="${target_arch}" \
  go build -trimpath -o "${test_root}/tewake-agent" "${repo_root}/cmd/tewake-agent"

docker run --rm \
  --network none \
  --read-only \
  --cap-drop ALL \
  --security-opt no-new-privileges \
  --user 65532:65532 \
  --tmpfs /state:rw,noexec,nosuid,nodev,mode=0700,uid=65532,gid=65532 \
  --tmpfs /tmp:rw,noexec,nosuid,nodev,mode=0700,uid=65532,gid=65532 \
  --mount "type=bind,src=${test_root}/tewake,dst=/opt/tewake,readonly" \
  --mount "type=bind,src=${test_root}/tewake-agent,dst=/opt/tewake-agent,readonly" \
  alpine@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce \
  /bin/sh -eu -c '
    controller_pid=
    agent_pid=
    stop_processes() {
      if [ -n "${agent_pid}" ]; then
        kill -TERM "${agent_pid}" 2>/dev/null || true
        wait "${agent_pid}" 2>/dev/null || true
      fi
      if [ -n "${controller_pid}" ]; then
        kill -TERM "${controller_pid}" 2>/dev/null || true
        wait "${controller_pid}" 2>/dev/null || true
      fi
    }
    trap stop_processes EXIT INT TERM

    init_output="$(/opt/tewake init \
      --state-dir /state/controller \
      --hint https://127.0.0.1:17443)"
    join_code="$(printf "%s\n" "${init_output}" | awk "/^tewake join / { print \$3 }")"
    test -n "${join_code}"

    /opt/tewake serve \
      --state-dir /state/controller \
      --agent-listen 127.0.0.1:17443 \
      --admin-listen "" \
      --mdns=false >/tmp/controller.log 2>&1 &
    controller_pid=$!

    ready=false
    for _ in $(seq 1 100); do
      if nc -z 127.0.0.1 17443; then
        ready=true
        break
      fi
      kill -0 "${controller_pid}"
      sleep 0.05
    done
    test "${ready}" = true

    /opt/tewake join "${join_code}" \
      --state-dir /state/agent \
      --controller https://127.0.0.1:17443 >/tmp/join.log 2>&1
    test -s /state/agent/node.json
    test -s /state/agent/node-private-key.pem

    /opt/tewake-agent serve \
      --state-dir /state/agent \
      --connection-timeout 2s \
      --reconnect-delay 50ms >/tmp/agent.log 2>&1 &
    agent_pid=$!

    connected=false
    for _ in $(seq 1 100); do
      if grep -q "connection established" /tmp/agent.log; then
        connected=true
        break
      fi
      kill -0 "${agent_pid}"
      sleep 0.05
    done
    test "${connected}" = true

    if grep -R -F "${join_code}" \
      /state/agent /tmp/controller.log /tmp/join.log /tmp/agent.log; then
      echo "join code leaked outside explicit init output" >&2
      exit 1
    fi

    kill -TERM "${agent_pid}"
    wait "${agent_pid}"
    agent_pid=
    kill -TERM "${controller_pid}"
    wait "${controller_pid}"
    controller_pid=
  '
