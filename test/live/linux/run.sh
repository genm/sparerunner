#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
live_build_dir=""
live_source_dir=""
injector_config=""
injector_armed=false
live_controller_pid=""

cleanup() {
  local original_status=$?
  local cleanup_status=0
  trap - EXIT
  set +e
  if [[ -n "$live_controller_pid" ]] && kill -0 "$live_controller_pid" 2>/dev/null; then
    kill -TERM "$live_controller_pid" 2>/dev/null
    sleep 1
    kill -KILL "$live_controller_pid" 2>/dev/null
    wait "$live_controller_pid" 2>/dev/null
  fi
  if [[ -n "$injector_config" ]]; then
    if [[ "$injector_armed" == true ]]; then
      "$live_build_dir/sparerunner-live-linux" exec-injector \
        --config "$injector_config" \
        --operation disarm
      cleanup_status=$?
    fi
    "$live_build_dir/sparerunner-live-linux" cleanup-injector \
      --config "$injector_config"
    injector_config=""
    injector_armed=false
  fi
  if [[ -n "$live_build_dir" && "$live_build_dir" == /run/sparerunner-live-linux.* && -d "$live_build_dir" ]]; then
    rm -rf -- "$live_build_dir"
  fi
  if [[ -n "$live_source_dir" && "$live_source_dir" == /run/sparerunner-live-source.* && -d "$live_source_dir" ]]; then
    rm -rf -- "$live_source_dir"
  fi
  if (( original_status != 0 )); then
    exit "$original_status"
  fi
  exit "$cleanup_status"
}
trap cleanup EXIT

fail() {
  printf 'sparerunner Linux live acceptance: %s\n' "$1" >&2
  exit 1
}

require_absolute_file() {
  local path="$1"
  [[ "$path" == /* && -f "$path" && ! -L "$path" ]] ||
    fail "expected an absolute regular file: $path"
}

require_absolute_directory() {
  local path="$1"
  [[ "$path" == /* && -d "$path" && ! -L "$path" ]] ||
    fail "expected an absolute directory: $path"
}

build_harness() {
  if [[ -n "$live_build_dir" ]]; then
    return
  fi
  command -v git >/dev/null || fail "git is required"
  command -v mise >/dev/null || fail "mise is required"
  local source_head source_root go_binary build_revision build_modified
  source_head="$(git -C "$repo_root" rev-parse --verify 'HEAD^{commit}')" ||
    fail "repository HEAD is unavailable"
  [[ -z "$(git -C "$repo_root" status --porcelain=v1 --untracked-files=all)" ]] ||
    fail "live acceptance requires a clean checkout"

  source_root="$repo_root"
  if [[ ! -d "$repo_root/.git" ]]; then
    # Go's VCS discovery recognizes a .git directory, not the .git pointer file
    # used by linked worktrees. An in-repository worktree can otherwise inherit
    # the parent checkout's unrelated HEAD while still reporting
    # vcs.modified=false. Build from a detached, non-hardlinked local clone so
    # the embedded provenance is the exact clean worktree commit.
    live_source_dir="$(mktemp -d /run/sparerunner-live-source.XXXXXX)"
    git clone --quiet --no-hardlinks --no-checkout "$repo_root" "$live_source_dir" ||
      fail "could not create isolated live-build source"
    git -C "$live_source_dir" checkout --quiet --detach "$source_head" ||
      fail "could not select the live-build commit"
    [[ -z "$(git -C "$live_source_dir" status --porcelain=v1 --untracked-files=all)" ]] ||
      fail "isolated live-build source is not clean"
    source_root="$live_source_dir"
  fi

  go_binary="$(cd "$repo_root" && mise which go)"
  [[ "$go_binary" == /* && -x "$go_binary" ]] ||
    fail "mise did not resolve the pinned Go executable"
  live_build_dir="$(mktemp -d /run/sparerunner-live-linux.XXXXXX)"
  chmod 0700 "$live_build_dir"
  (
    cd "$source_root"
    "$go_binary" build -trimpath -o "$live_build_dir/sparerunner-live-linux" ./test/live/linux
  )
  build_revision="$(
    "$go_binary" version -m "$live_build_dir/sparerunner-live-linux" |
      awk '$1 == "build" && $2 ~ /^vcs.revision=/ {
        sub(/^vcs.revision=/, "", $2)
        print $2
      }'
  )"
  build_modified="$(
    "$go_binary" version -m "$live_build_dir/sparerunner-live-linux" |
      awk '$1 == "build" && $2 ~ /^vcs.modified=/ {
        sub(/^vcs.modified=/, "", $2)
        print $2
      }'
  )"
  [[ "$build_revision" == "$source_head" && "$build_modified" == "false" ]] ||
    fail "built harness provenance does not match the clean checkout"
}

config_value() {
  local config="$1"
  local expression="$2"
  jq -er "$expression | strings | select(length > 0)" "$config"
}

prepare_private_repository_proof() {
  local config="$1"
  command -v gh >/dev/null || fail "gh is required for the private-repository preflight"
  command -v jq >/dev/null || fail "jq is required for strict live-driver parsing"
  local config_url evidence_dir proof_file repository
  config_url="$(config_value "$config" '.github.configUrl')"
  evidence_dir="$(config_value "$config" '.evidenceDirectory')"
  proof_file="$(config_value "$config" '.github.privateRepositoryProofFile')"
  [[ "$config_url" == https://github.com/*/* ]] ||
    fail "github.configUrl must identify one repository"
  repository="${config_url#https://github.com/}"
  [[ "$repository" != */*/* ]] ||
    fail "github.configUrl must identify one repository"
  if [[ -e "$evidence_dir" ]]; then
    require_absolute_directory "$evidence_dir"
    [[ "$(stat -c '%a' "$evidence_dir")" == "700" ]] ||
      fail "evidence directory must have mode 0700"
  else
    [[ "$evidence_dir" == /* ]] || fail "evidence directory must be absolute"
    install -d -m 0700 "$evidence_dir"
  fi
  [[ "$proof_file" == "$evidence_dir/"* && "$(dirname "$proof_file")" == "$evidence_dir" ]] ||
    fail "repository proof must be a direct child of the evidence directory"

  local repository_json visibility name_with_owner repository_url temporary
  # Pin the authority host explicitly. An operator may have GH_HOST configured
  # for GHES; an unqualified owner/repo lookup could otherwise prove a private
  # repository on that host while the configured target is public on github.com.
  repository_json="$(gh api --hostname github.com "repos/$repository")"
  visibility="$(jq -er '.visibility | ascii_upcase' <<<"$repository_json")"
  name_with_owner="$(jq -er '.full_name' <<<"$repository_json")"
  repository_url="$(jq -er '.html_url' <<<"$repository_json")"
  [[ "${name_with_owner,,}" == "${repository,,}" &&
    "${repository_url,,}" == "https://github.com/${repository,,}" ]] ||
    fail "GitHub repository proof did not match the configured github.com target"
  [[ "$visibility" == "PRIVATE" ]] ||
    fail "public and internal repositories are refused by this live gate"
  temporary="$(mktemp "$evidence_dir/.private-repository-proof.XXXXXX")"
  chmod 0600 "$temporary"
  jq -n \
    --arg repository "$name_with_owner" \
    '{version: 1, repository: $repository, visibility: "PRIVATE"}' \
    >"$temporary"
  mv -f -- "$temporary" "$proof_file"
  chmod 0600 "$proof_file"
}

require_fresh_scenario_evidence() {
  local config="$1"
  local evidence_dir
  evidence_dir="$(config_value "$config" '.evidenceDirectory')"
  for name in \
    result.json \
    controller-replay.json \
    processes-before.json \
    processes-after.json \
    processes-running-before-restart.json \
    processes-running-after-restart.json \
    filesystem-after.json \
    agent-restart-started.json \
    authority.json \
    provenance.json \
    injector.json; do
    [[ ! -e "$evidence_dir/$name" ]] ||
      fail "scenario requires fresh state/journal/evidence; found $name"
  done
}

prepare_scenario() {
  local config="$1"
  require_absolute_file "$config"
  build_harness
  # The shell must not derive or mutate any path from the config until the Go
  # boundary has opened it through the trusted-owner/non-writable path policy.
  "$live_build_dir/sparerunner-live-linux" validate-config --config "$config"
  prepare_private_repository_proof "$config"
  require_fresh_scenario_evidence "$config"
  "$live_build_dir/sparerunner-live-linux" capture-authority \
    --config "$config" \
    --repo-root "$repo_root"
}

run_controller_process() {
  local mode="$1"
  local config="$2"
  "$live_build_dir/sparerunner-live-linux" controller --config "$config" --mode "$mode"
}

validate_scenario_evidence() {
  local mode="$1"
  local config="$2"
  "$live_build_dir/sparerunner-live-linux" validate-evidence --config "$config" --mode "$mode"
}

scenario_preflight() {
  local config="$1"
  node_preflight "$config"
}

scenario_postflight() {
  local config="$1"
  node_postflight "$config"
}

run_normal() {
  local config="$1"
  prepare_scenario "$config"
  scenario_preflight "$config"
  run_controller_process normal "$config"
  scenario_postflight "$config"
  validate_scenario_evidence normal "$config"
}

run_commit_before_ack() {
  local config="$1"
  prepare_scenario "$config"
  scenario_preflight "$config"
  local evidence_dir replay_file controller_pid status
  evidence_dir="$(config_value "$config" '.evidenceDirectory')"
  replay_file="$evidence_dir/controller-replay.json"

  "$live_build_dir/sparerunner-live-linux" controller \
    --config "$config" \
    --mode commit-before-ack &
  controller_pid=$!
  live_controller_pid="$controller_pid"
  for _ in $(seq 1 1500); do
    if [[ -f "$replay_file" ]] &&
      jq -e '.version == 1 and .phase == "committed_before_ack"' "$replay_file" >/dev/null; then
      break
    fi
    if ! kill -0 "$controller_pid" 2>/dev/null; then
      set +e
      wait "$controller_pid"
      set -e
      live_controller_pid=""
      fail "controller exited before the durable pre-ack marker"
    fi
    sleep 0.2
  done
  if [[ ! -f "$replay_file" ]] ||
    ! jq -e '.version == 1 and .phase == "committed_before_ack"' "$replay_file" >/dev/null; then
    fail "timed out waiting for the durable pre-ack marker"
  fi

  kill -KILL "$controller_pid"
  set +e
  wait "$controller_pid"
  status=$?
  set -e
  live_controller_pid=""
  [[ "$status" -eq 137 ]] ||
    fail "controller did not terminate through the expected SIGKILL boundary"
  "$live_build_dir/sparerunner-live-linux" record-ack-gate-kill --config "$config"
  jq -e \
    '.version == 1 and
     .phase == "killed_before_ack" and
     .killExitStatus == 137 and
     (.killedBeforeAckObservedAt | strings | length > 0)' \
    "$replay_file" >/dev/null ||
    fail "SIGKILL boundary was not durably recorded"

  prepare_private_repository_proof "$config"
  run_controller_process normal "$config"
  jq -e '.version == 1 and .phase == "redelivered_same_execution"' "$replay_file" >/dev/null ||
    fail "GitHub message was not proven to redeliver to the same execution"
  scenario_postflight "$config"
  validate_scenario_evidence commit-before-ack "$config"
}

run_cleanup_failure() {
  local config="$1"
  local injector="$2"
  prepare_scenario "$config"
  scenario_preflight "$config"
  "$live_build_dir/sparerunner-live-linux" prepare-injector \
    --config "$config" \
    --source "$injector"
  injector_config="$config"
  if ! "$live_build_dir/sparerunner-live-linux" exec-injector \
    --config "$config" \
    --operation arm; then
    fail "cleanup-failure injector could not be armed"
  fi
  injector_armed=true
  run_controller_process cleanup-failure "$config"
  if ! "$live_build_dir/sparerunner-live-linux" exec-injector \
    --config "$config" \
    --operation disarm; then
    fail "cleanup-failure injector could not be disarmed"
  fi
  injector_armed=false
  validate_scenario_evidence cleanup-failure "$config"
  "$live_build_dir/sparerunner-live-linux" cleanup-injector --config "$config"
  injector_config=""
}

capture_running_processes() {
  local phase="$1"
  local config="$2"
  "$live_build_dir/sparerunner-live-linux" capture-node \
    --phase "$phase" \
    --config "$config"
}

node_cleanup_postflight() {
  local config="$1"
  "$live_build_dir/sparerunner-live-linux" capture-node \
    --phase after \
    --config "$config"
}

run_agent_restart() {
  local config="$1"
  prepare_scenario "$config"
  scenario_preflight "$config"
  local evidence_dir marker controller_pid status before_file after_file run_timeout wait_iterations attempt
  evidence_dir="$(config_value "$config" '.evidenceDirectory')"
  run_timeout="$(jq -er '.runTimeoutSeconds | numbers | select(. >= 1 and . <= 7200)' "$config")"
  wait_iterations=$((run_timeout * 5))
  marker="$evidence_dir/agent-restart-started.json"
  before_file="$evidence_dir/processes-running-before-restart.json"
  after_file="$evidence_dir/processes-running-after-restart.json"

  run_controller_process agent-restart "$config" &
  controller_pid=$!
  live_controller_pid="$controller_pid"
  for ((attempt = 1; attempt <= wait_iterations; attempt++)); do
    if [[ -f "$marker" ]] &&
      jq -e '.version == 1 and .runnerRequestId > 0 and (.observedAt | strings | length > 0)' "$marker" >/dev/null; then
      break
    fi
    kill -0 "$controller_pid" 2>/dev/null ||
      fail "controller exited before the JobStarted marker"
    sleep 0.2
  done
  [[ -f "$marker" ]] ||
    fail "timed out waiting for the JobStarted marker"

  capture_running_processes running-before-restart "$config"
  systemctl restart sparerunner-agent.service
  for _ in $(seq 1 60); do
    if systemctl is-active --quiet sparerunner-agent.service; then
      break
    fi
    sleep 0.5
  done
  systemctl is-active --quiet sparerunner-agent.service ||
    fail "sparerunner-agent did not recover while the job was running"
  capture_running_processes running-after-restart "$config"

  local before_agent after_agent before_supervisor after_supervisor before_listener after_listener
  before_agent="$(jq -er '[.processes[] | select(.role == "agent") | .pid] | if length == 1 then .[0] else error("agent count") end' "$before_file")"
  after_agent="$(jq -er '[.processes[] | select(.role == "agent") | .pid] | if length == 1 then .[0] else error("agent count") end' "$after_file")"
  before_supervisor="$(jq -er '[.processes[] | select(.role == "supervisor") | .pid] | if length == 1 then .[0] else error("supervisor count") end' "$before_file")"
  after_supervisor="$(jq -er '[.processes[] | select(.role == "supervisor") | .pid] | if length == 1 then .[0] else error("supervisor count") end' "$after_file")"
  before_listener="$(jq -er '[.processes[] | select(.role == "runner_listener") | .pid] | if length == 1 then .[0] else error("listener count") end' "$before_file")"
  after_listener="$(jq -er '[.processes[] | select(.role == "runner_listener") | .pid] | if length == 1 then .[0] else error("listener count") end' "$after_file")"
  [[ "$before_agent" != "$after_agent" ]] ||
    fail "agent service restart did not replace the Agent process"
  [[ "$before_supervisor" == "$after_supervisor" ]] ||
    fail "agent restart unexpectedly replaced the Supervisor"
  [[ "$before_listener" == "$after_listener" ]] ||
    fail "running runner listener did not survive the Agent restart"

  set +e
  wait "$controller_pid"
  status=$?
  set -e
  live_controller_pid=""
  [[ "$status" -eq 0 ]] ||
    fail "controller failed after the Agent restart"
  node_cleanup_postflight "$config"
  validate_scenario_evidence agent-restart "$config"
}

node_preflight() {
  local config="$1"
  [[ "$(stat -fc '%T' /sys/fs/cgroup)" == "cgroup2fs" ]] ||
    fail "cgroup v2 is required"
  systemctl is-active --quiet sparerunner-agent.service
  systemctl is-active --quiet sparerunner-supervisor.service
  [[ -S /run/sparerunner-supervisor/supervisor.sock ]] ||
    fail "native supervisor socket is unavailable"
  build_harness
  "$live_build_dir/sparerunner-live-linux" capture-node \
    --phase before \
    --config "$config"
}

node_postflight() {
  local config="$1"
  systemctl restart sparerunner-agent.service
  for _ in $(seq 1 60); do
    if systemctl is-active --quiet sparerunner-agent.service; then
      break
    fi
    sleep 0.5
  done
  systemctl is-active --quiet sparerunner-agent.service ||
    fail "sparerunner-agent did not recover after restart"
  build_harness
  "$live_build_dir/sparerunner-live-linux" capture-node \
    --phase after \
    --config "$config"
}

usage() {
  printf '%s\n' \
    'usage:' \
    '  run.sh build-provenance' \
    '  run.sh normal ABSOLUTE_CONFIG' \
    '  run.sh commit-before-ack ABSOLUTE_CONFIG' \
    '  run.sh cleanup-failure ABSOLUTE_CONFIG ABSOLUTE_ROOT_OWNED_INJECTOR' \
    '  run.sh agent-restart ABSOLUTE_CONFIG' \
    '  run.sh node-preflight ABSOLUTE_CONFIG' \
    '  run.sh node-postflight ABSOLUTE_CONFIG' >&2
  exit 2
}

case "${1:-}" in
build-provenance)
  [[ $# -eq 1 ]] || usage
  build_harness
  ;;
normal)
  [[ $# -eq 2 ]] || usage
  run_normal "$2"
  ;;
commit-before-ack)
  [[ $# -eq 2 ]] || usage
  run_commit_before_ack "$2"
  ;;
cleanup-failure)
  [[ $# -eq 3 ]] || usage
  run_cleanup_failure "$2" "$3"
  ;;
agent-restart)
  [[ $# -eq 2 ]] || usage
  run_agent_restart "$2"
  ;;
node-preflight)
  [[ $# -eq 2 ]] || usage
  node_preflight "$2"
  ;;
node-postflight)
  [[ $# -eq 2 ]] || usage
  node_postflight "$2"
  ;;
*)
  usage
  ;;
esac
