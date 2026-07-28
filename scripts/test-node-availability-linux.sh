#!/usr/bin/env bash
# Prove node availability control end to end against real controller, agent, and
# CLI binaries: durable intent, capacity withheld while stopped, pending resume
# until the controller confirms it, and stop applied without a controller.
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
  go build -trimpath -o "${test_root}/sprun" "${repo_root}/cmd/sprun"
CGO_ENABLED=0 GOOS=linux GOARCH="${target_arch}" \
  go build -trimpath -o "${test_root}/sparerunner-agent" "${repo_root}/cmd/sparerunner-agent"

docker run --rm \
  --network none \
  --read-only \
  --cap-drop ALL \
  --security-opt no-new-privileges \
  --user 65532:65532 \
  --tmpfs /state:rw,noexec,nosuid,nodev,mode=0700,uid=65532,gid=65532 \
  --tmpfs /tmp:rw,noexec,nosuid,nodev,mode=0700,uid=65532,gid=65532 \
  --mount "type=bind,src=${test_root}/sprun,dst=/opt/sprun,readonly" \
  --mount "type=bind,src=${test_root}/sparerunner-agent,dst=/opt/sparerunner-agent,readonly" \
  alpine@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce \
  /bin/sh -eu -c '
    controller_pid=
    agent_pid=
    stop_agent() {
      if [ -n "${agent_pid}" ]; then
        kill -TERM "${agent_pid}" 2>/dev/null || true
        wait "${agent_pid}" 2>/dev/null || true
        agent_pid=
      fi
    }
    stop_processes() {
      stop_agent
      if [ -n "${controller_pid}" ]; then
        kill -TERM "${controller_pid}" 2>/dev/null || true
        wait "${controller_pid}" 2>/dev/null || true
        controller_pid=
      fi
    }
    trap stop_processes EXIT INT TERM

    field() {
      # Read one field from the versioned CLI document without a JSON tool.
      sed -n "s/.*\"$1\": *\([^,]*\).*/\1/p" | tr -d "\" "
    }

    status_field() {
      /opt/sprun node status --state-dir /state/agent --json | field "$1"
    }

    await_endpoint() {
      # The local control endpoint answers without a controller session, so a
      # restart in the offline part of this test is waited on directly.
      i=0
      while [ "${i}" -lt 100 ]; do
        if /opt/sprun node status --state-dir /state/agent --json >/dev/null 2>&1; then
          return 0
        fi
        i=$((i + 1))
        sleep 0.1
      done
      echo "timed out waiting for the local control endpoint" >&2
      return 1
    }

    # eligibleTargets is refreshed on the heartbeat acknowledgement rather than
    # at connect time, so it is polled the same way await_field polls a scalar
    # field instead of being read once right after controllerConnected turns
    # true.
    await_eligible_targets_empty() {
      i=0
      while [ "${i}" -lt 100 ]; do
        if /opt/sprun node status --state-dir /state/agent --json 2>/dev/null \
          | grep -q "\"eligibleTargets\": \[\]"; then
          return 0
        fi
        i=$((i + 1))
        sleep 0.1
      done
      echo "timed out waiting for an empty eligibleTargets heartbeat echo" >&2
      /opt/sprun node status --state-dir /state/agent --json >&2 || true
      return 1
    }

    await_field() {
      name="$1"
      want="$2"
      i=0
      while [ "${i}" -lt 100 ]; do
        if [ "$(status_field "${name}")" = "${want}" ]; then
          return 0
        fi
        i=$((i + 1))
        sleep 0.1
      done
      echo "timed out waiting for ${name}=${want}" >&2
      /opt/sprun node status --state-dir /state/agent --json >&2 || true
      return 1
    }

    init_output="$(/opt/sprun init \
      --state-dir /state/controller \
      --hint https://127.0.0.1:17443)"
    join_code="$(printf "%s\n" "${init_output}" | awk "/^sprun join / { print \$3 }")"
    test -n "${join_code}"

    /opt/sprun serve \
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

    /opt/sprun join "${join_code}" \
      --state-dir /state/agent \
      --controller https://127.0.0.1:17443 >/tmp/join.log 2>&1

    # An agent without the local control surface must refuse desktop clients
    # rather than answering from an implicit endpoint.
    /opt/sparerunner-agent serve \
      --state-dir /state/agent \
      --connection-timeout 2s \
      --reconnect-delay 50ms >/tmp/agent-nocontrol.log 2>&1 &
    agent_pid=$!
    sleep 0.5
    if /opt/sprun node status --state-dir /state/agent --json >/tmp/nocontrol.json 2>/dev/null; then
      echo "status succeeded without --local-control" >&2
      exit 1
    fi
    # The failure document is the only thing on stdout, so a launcher can parse
    # exactly one stream in both the success and failure cases.
    test "$(field ok </tmp/nocontrol.json)" = false
    test "$(field errorClass </tmp/nocontrol.json)" = endpoint_unavailable
    stop_agent

    /opt/sparerunner-agent serve \
      --state-dir /state/agent \
      --local-control \
      --connection-timeout 2s \
      --reconnect-delay 50ms >/tmp/agent.log 2>&1 &
    agent_pid=$!

    await_field controllerConnected true
    await_field pendingResume false
    test "$(status_field intent)" = accepting

    # Absent-vs-empty: this rig configures no GitHub Target, so the first
    # heartbeat acknowledgement must report an explicit empty eligible list
    # rather than omitting the field. That distinguishes "no eligible
    # targets for this node" from "no refresh yet", and confirms the empty
    # list alone never breaks status rendering.
    await_eligible_targets_empty
    /opt/sprun node status --state-dir /state/agent --json >/tmp/status-first.json
    grep -q "\"eligibleTargets\": \[\]" /tmp/status-first.json

    # Stopping withholds capacity and records the requesting surface.
    /opt/sprun node pause --state-dir /state/agent --source raycast --json >/tmp/pause.json
    test "$(field intent </tmp/pause.json)" = stopped
    test "$(field intentChangedBy </tmp/pause.json)" = raycast
    test "$(field intentExplicit </tmp/pause.json)" = true

    # A stopped computer stays stopped across an agent service restart.
    stop_agent
    /opt/sparerunner-agent serve \
      --state-dir /state/agent \
      --local-control \
      --connection-timeout 2s \
      --reconnect-delay 50ms >/tmp/agent-restart.log 2>&1 &
    agent_pid=$!
    await_field controllerConnected true
    test "$(status_field intent)" = stopped

    # Resuming adds capacity, so it is pending until the controller confirms it,
    # and then becomes effective.
    /opt/sprun node resume --state-dir /state/agent --source tray --json >/tmp/resume.json
    test "$(field intent </tmp/resume.json)" = accepting
    test "$(field pendingResume </tmp/resume.json)" = true
    await_field pendingResume false

    # Losing the controller withdraws confirmation instead of retaining a stale
    # acceptance, and stopping still applies locally.
    kill -TERM "${controller_pid}"
    wait "${controller_pid}" 2>/dev/null || true
    controller_pid=
    await_field controllerConnected false
    test "$(status_field pendingResume)" = true
    /opt/sprun node pause --state-dir /state/agent --source cli --json >/tmp/offline-pause.json
    test "$(field intent </tmp/offline-pause.json)" = stopped

    # Per-Target exclusion. No GitHub target exists in this offline rig, so the
    # scenarios below are exactly the ones that do not need one: excluding an
    # unknown Target is a deliberate safe no-op rendered as not-currently-
    # eligible, and it must be just as durable as the global intent.
    /opt/sprun node targets --state-dir /state/agent \
      --exclude spr-e2e-unknown-target --source tray --json >/tmp/exclude.json
    grep -q "unknownExclusions" /tmp/exclude.json
    grep -q "spr-e2e-unknown-target" /tmp/exclude.json

    # Excluding is subtractive, so it is durable the instant it is recorded and
    # survives an agent service restart exactly like a stop.
    stop_agent
    /opt/sparerunner-agent serve \
      --state-dir /state/agent \
      --local-control \
      --connection-timeout 2s \
      --reconnect-delay 50ms >/tmp/agent-targets.log 2>&1 &
    agent_pid=$!
    await_endpoint
    /opt/sprun node targets --state-dir /state/agent --json >/tmp/targets.json
    grep -q "spr-e2e-unknown-target" /tmp/targets.json

    # The text surface renders it as not-currently-eligible rather than as an
    # error, because the owner may legitimately exclude a Target this node has
    # never been told about.
    /opt/sprun node targets --state-dir /state/agent >/tmp/targets.txt
    grep -q "not currently eligible" /tmp/targets.txt

    # Including removes it again.
    /opt/sprun node targets --state-dir /state/agent \
      --include spr-e2e-unknown-target --source cli --json >/tmp/include.json
    if grep -q "spr-e2e-unknown-target" /tmp/include.json; then
      echo "include did not remove the exclusion" >&2
      exit 1
    fi

    # An ambiguous invocation is refused rather than silently resolved.
    if /opt/sprun node targets --state-dir /state/agent \
      --exclude --include spr-e2e-unknown-target >/tmp/ambiguous.log 2>&1; then
      echo "ambiguous exclude/include invocation was accepted" >&2
      exit 1
    fi

    # A malformed identifier is rejected at the control boundary with a
    # machine-readable class, so garbage never reaches SQLite or the wire.
    if /opt/sprun node targets --state-dir /state/agent \
      --exclude " padded-target " --json >/tmp/bad-target.json 2>/dev/null; then
      echo "a malformed target identifier was accepted" >&2
      exit 1
    fi
    test "$(field errorClass </tmp/bad-target.json)" = invalid_request

    # The exclusion set is bounded and fails closed at the boundary instead of
    # silently truncating the owner deny-list.
    i=0
    while [ "${i}" -lt 256 ]; do
      /opt/sprun node targets --state-dir /state/agent \
        --exclude "spr-e2e-cap-${i}" >/dev/null
      i=$((i + 1))
    done
    if /opt/sprun node targets --state-dir /state/agent \
      --exclude spr-e2e-cap-overflow --json >/tmp/cap.json 2>/dev/null; then
      echo "a full exclusion set accepted another entry" >&2
      exit 1
    fi
    test "$(field ok </tmp/cap.json)" = false
    test "$(field errorClass </tmp/cap.json)" = invalid_request

    # The control surface is non-secret: no join code or credential material may
    # appear in its documents.
    if grep -R -F "${join_code}" /tmp/status-first.json /tmp/pause.json /tmp/resume.json \
      /tmp/offline-pause.json /tmp/exclude.json /tmp/targets.json /tmp/include.json; then
      echo "join code leaked into the node control documents" >&2
      exit 1
    fi

    echo "node availability control verified"
  '
