# Linux private-sandbox acceptance

This directory owns the SPR-007 live release gate. It is deliberately separate
from `sparerunner serve`: the harness composes the real Controller, Agent broker,
GitHub App client, pinned scale-set adapter, message session, JIT lifecycle, and
single-slot coordinator in one process without introducing the future SPR-012
Target/config API early.

The gate requires a protected trusted commit, a private repository, an installed
GitHub App, a pre-created `sparerunner-linux` scale set, and one real
systemd+cgroup-v2 Linux node. It must never run for a public fork pull request.

## Safety boundary

Evidence authority is fail-closed and layered. The live config names candidates
only; it is not proof that the installed services use them. The preflight must
bind the effective systemd `ExecStart`, `MainPID`, control group, service UID,
boot identity, native runtime root, installed binary digest, and the complete
effective service argv into `authority.json`. A separate `provenance.json`
binds the exact clean checkout commit, harness binary, installed service binary,
effective unit fragments/drop-ins, and official runner archive. Process and
filesystem captures are accepted only when they match that authority. Config,
runtime, injector, and evidence paths must have a trusted non-symlink parent
chain and descriptor identity; argv basenames and shell substring matches are
never final authority.

- The versioned config is non-secret and rejects unknown fields, duplicate
  fields, trailing JSON, organization-level targets, non-canonical paths, and
  labels other than exactly `sparerunner-linux`.
- The driver uses `gh api --hostname github.com` immediately before each
  Controller start, rejoins the returned repository name and URL to the
  configured target, and emits a mode-`0600`, five-minute proof that the exact
  repository is `PRIVATE`. Public and `INTERNAL` visibility fail closed.
- The GitHub App key is read only from the config's absolute Linux credential
  file. The file must be regular, non-symlink, owned by the live process user,
  and mode `0600`; its parent chain must satisfy `enroll.LoadPrivateMaterial`.
  The read byte slice is cleared as soon as it is wrapped by the opaque GitHub
  adapter. The key must be outside the Controller state and evidence trees.
- The selected Node must be enrolled and `active`. Its authenticated snapshot
  must be Linux `amd64`/`arm64`, `nativeRunnerReady: true`, and have no command,
  observation, or cleanup-tombstone history.
- `nativeRunnerReady` is meaningful here because the packaged systemd unit
  starts the Agent with `--require-native-runner`. Every scenario verifies that
  unit flag, the root supervisor, its local socket, cgroup v2, and the absence
  of an idle runner listener.
- Evidence uses fixed Go structs and an explicit filename allowlist. Repository
  proof contains only repository identity and visibility. No JIT body,
  authorization header, GitHub App key, node key, join code, raw process
  argument, repository job name, or runner output has an evidence field.

Native mode is only for trusted private workflows. It is not a hostile-code
sandbox.

## Scenario isolation

Each scenario below requires its own freshly initialized Controller state,
freshly enrolled Agent state/journal, private repository or otherwise isolated
queue, and mode-`0700` evidence directory:

1. `normal`
2. `commit-before-ack`
3. `cleanup-failure`
4. `agent-restart`

Do not reuse a successful normal or cleanup-failure state for another scenario.
The harness intentionally rejects any pre-existing execution or reservation.
The only accepted non-fresh Controller state is the exact `reserved` execution
recorded by the first half of `commit-before-ack`; on restart, the epoch must
advance and the Agent journal must still be empty. A completed replay marker is
also refused on a later run.

This constraint keeps the evidence attributable to one job and makes duplicate
execution/listener detection unambiguous.

## Configuration

Copy [`config.example.json`](./config.example.json) to an absolute path outside
the repository and replace every placeholder. Important values:

- `controllerStateDirectory`: state created by `sparerunner init`.
- `agentListenAddress`: the exact stable endpoint already used by the enrolled
  Agent.
- `evidenceDirectory`: a dedicated absolute mode-`0700` directory.
- `runtimeRoot`: the absolute native-runner runtime root inspected before and
  after the scenario.
- `provenance.expectedCommitSha`: the exact clean `HEAD` used to build the
  harness. Untracked files and every staged or unstaged change fail the gate.
- `provenance.expectedInstalledAgentSha256`: SHA-256 of the installed
  `/usr/local/bin/sparerunner-agent`. Both this binary and the harness must contain
  Go build metadata whose `vcs.revision` equals `expectedCommitSha` and whose
  `vcs.modified` is false.
- `provenance.expectedAgentUnitFragmentPath` and
  `expectedSupervisorUnitFragmentPath`: the exact root-owned, non-writable
  installed fragment paths (the packaged default is under
  `/usr/lib/systemd/system`).
- `provenance.expectedAgentUnitSha256` and
  `expectedSupervisorUnitSha256`: SHA-256 of the respective
  `systemctl cat --no-pager UNIT` output, including every effective fragment
  and drop-in.
- `provenance.expectedRunnerPackageSha256`: the pinned checksum for the current
  OS/architecture. The path is not configurable: the harness derives
  `<runtimeRoot>/.sparerunner-official/<official-cache-key>/archive` and requires a
  root-owned, mode-`0400`, single-link file with the pinned size and SHA-256.
  The example contains the Linux `amd64` checksum; use the pinned `arm64`
  checksum from `internal/runner/package.go` on an ARM node.
- `agentRestartMinimumRunningSeconds`: minimum Started-to-Completed interval
  required by the Agent-restart gate. Use a trusted private workflow step that
  remains running for at least this duration; a job that finishes before the
  restart evidence is captured fails closed.
- `github.configUrl`: exactly `https://github.com/OWNER/PRIVATE-REPOSITORY`;
  organization-only targets are rejected.
- `github.privateKeyFile`: separate absolute mode-`0600` credential file.
- `github.privateRepositoryProofFile`: the reserved direct child
  `evidenceDirectory/private-repository-proof.json`; the driver replaces only
  this dedicated proof after querying GitHub;
  `run.sh` owns and refreshes it.
- `github.runnerGroupId`: the group containing the pre-created scale set.
- `nodeId`: the exact 32-character lowercase enrolled Node ID.

The live process user must be able to read the Controller state and must own the
key file. Do not put the App key in JSON, SQLite, an environment variable, a
command argument, or the evidence directory.

The existing GitHub object must match all of these fields exactly:

```text
name:          sparerunner-linux
labels:        [sparerunner-linux]
runnerGroupId: config value
disableUpdate: config value
```

The harness only calls `GetScaleSet`; it never creates, updates, or deletes a
scale set.

## Reproducible driver

Run from a trusted checkout. The driver builds the harness with the repository's
mise-pinned Go toolchain. It requires `mise`, `gh`, and `jq`.

Linked Git worktrees use a `.git` pointer file. Go's build-VCS discovery does
not treat that file as a repository root and, for an in-repository worktree,
can stamp the containing checkout's unrelated `HEAD` while still reporting
`vcs.modified=false`. The driver detects this layout, verifies the worktree is
clean, builds from a detached non-hardlinked local clone of its exact commit,
and rejects the result unless the embedded `vcs.revision` and `vcs.modified`
match that source. The isolated source is removed on exit.

The build-only provenance preflight performs that check without reading
credentials or contacting GitHub:

```bash
sudo ./test/live/linux/run.sh build-provenance
```

Populate the explicit provenance values immediately before a protected run:

```bash
git rev-parse --verify HEAD
git status --porcelain=v1 --untracked-files=all
sha256sum /usr/local/bin/sparerunner-agent
go version -m /usr/local/bin/sparerunner-agent
systemctl cat --no-pager sparerunner-agent.service | sha256sum
systemctl cat --no-pager sparerunner-supervisor.service | sha256sum
```

The second command must produce no output. The live harness independently
repeats all comparisons and refuses a dirty checkout, wrong commit, changed
installed binary, changed effective unit/drop-in, or changed runner archive.
`go version -m` must show the expected `vcs.revision` and
`vcs.modified=false`; release builds that strip VCS metadata cannot satisfy
this gate.

Run each scenario on the Linux Node as root (or with equivalent systemd and
`/proc` access). Each primary command performs the node preflight, Controller
run, applicable postflight, and final scenario-specific manifest validation.

Run one normal job:

```bash
./test/live/linux/run.sh normal /absolute/live-config.json
```

Exercise the durable commit/ack crash window:

```bash
./test/live/linux/run.sh commit-before-ack /absolute/live-config.json
```

The driver waits until `controller-replay.json` proves that the one slot,
desired execution, and reservation are durable, sends `SIGKILL` before
`DeleteMessage`, verifies exit status 137, records that kill boundary, then
restarts the same composition. The final artifact retains the original commit
epoch/time, verified SIGKILL time/status, and newer redelivery epoch/time. The
second process must observe the same message, runner request, and Execution ID
before it may pass; redelivery itself proves the first process did not
acknowledge the message.
The first durable Available time is carried across the process boundary, so
redelivery cannot reset the 60-second warm-runner gate.

Exercise cleanup quarantine with an environment-owned, root-only injector:

```bash
./test/live/linux/run.sh cleanup-failure \
  /absolute/live-config.json \
  /absolute/root-owned-cleanup-fault-injector
```

The injector is an explicit two-operation contract:

```text
injector arm
injector disarm
```

It and every ancestor must be root-owned, non-symlink, and have no group/world
write bits (sticky directories are not exempt). Before use, the harness opens
the source by descriptor, copies it once to a root-owned mode-`0500` directory
under `/run`, fsyncs the file and directory, and records source/copy SHA-256,
device, inode, owner, mode, and size. Every `arm`, `disarm`, and exit-cleanup
invocation reopens and verifies that exact copy, then executes its open
descriptor through `/proc/self/fd`; a source rename, copy replacement, or byte
mutation fails closed. `arm` must make deletion of the one real execution workspace fail (for example,
an environment-owned bind-mount fixture); it must not replace the runner or
Controller with a mock. The driver calls `disarm` after the scenario and from
its exit cleanup path if an earlier command fails. The harness
passes only after the execution is `cleanup_failed`/`quarantined`, the Node is
`quarantined`, the slot reservation remains, and the same GitHub job has
Available, Started, and successful Completed observations.

Exercise Agent service recovery while a real runner owns the job:

```bash
./test/live/linux/run.sh agent-restart /absolute/live-config.json
```

This scenario waits for the durable JobStarted marker, captures exactly one
Agent, Supervisor, and `Runner.Listener`, restarts only
`sparerunner-agent.service`, and captures the running state again. It requires a new
Agent PID, the same Supervisor PID, and the same single runner-listener PID.
The job must then complete successfully and the final capture must contain no
runner listener. The Supervisor is deliberately not restarted because it owns
the native runner process.

The listener identity must be proved without trusting its argv basename.
`authority.json` supplies the root Supervisor `ControlGroup` and the numeric
`sparerunner-runner-0` UID. For a running scenario, `/proc/<listener-pid>/cgroup`
must contain exactly one unified entry whose path is
`<Supervisor ControlGroup>/sparerunner/sparerunner-<64 lowercase hex>`, and the real UID
from `/proc/<listener-pid>/status` must equal `authority.json.runnerUid`.
Both captures around an Agent-only restart must retain that exact PID, UID, and
cgroup path. A Supervisor shutdown instead must leave no process in any owned
`sparerunner/sparerunner-*` child cgroup.
The same-PID requirement is live proof that the listener remained running; it
is not the durable ownership primitive. Recovery authority is the exact fence
token plus cgroup. If that cgroup is already empty, or only descendants remain
after the listener exits, `Alive` is false and the journaled runtime is adopted
only long enough to finish `Wait`/`Destroy` cleanup.

The standalone capture commands remain available only for diagnostics:

```bash
./test/live/linux/run.sh node-preflight /absolute/live-config.json
./test/live/linux/run.sh node-postflight /absolute/live-config.json
```

The normal and commit-before-ack commands automatically restart
`sparerunner-agent.service`, then verify one non-root Agent, one root Supervisor, no
idle `Runner.Listener`, an empty execution root, and absence of `_work`,
`.runner`, `.credentials`, `.credentials_rsaparams`, symlinks, and the fixed
`.sparerunner-jit-canary` filename. The intentionally quarantined cleanup-failure
scenario does not run this cleanup-success postflight.

## Pass criteria and evidence

The Controller loop is explicitly:

```text
PollOnce (commit then ack)
  → record message observation with a monotonic time.Time
  → DriveNext
```

UTC renderings of the Available and Started observation times and
`availableToStartedMillis` are written to `result.json`. A negative duration or
duration greater than 60,000 ms fails the gate. The monotonic component is used
for the calculation; UTC is only the portable evidence representation.

Shutdown is bounded and ordered: cancel the poll loop, close the GitHub message
session with a 10-second background context, wait for the loop, cancel/close the
Controller server, wait for it, then close SQLite.

The final validator rejects missing, stale, cross-scenario, or contradictory
artifacts. Required files are scenario-specific:

```text
normal:
  authority.json, provenance.json, result.json, processes-before.json, processes-after.json,
  filesystem-after.json, no controller-replay.json

commit-before-ack:
  authority.json, provenance.json, result.json, processes-before.json, processes-after.json,
  filesystem-after.json, controller-replay.json

cleanup-failure:
  authority.json, provenance.json, injector.json, result.json,
  processes-before.json, no processes-after.json,
  no filesystem-after.json, no controller-replay.json

agent-restart:
  authority.json, provenance.json, result.json, processes-before.json,
  agent-restart-started.json,
  processes-running-before-restart.json,
  processes-running-after-restart.json, processes-after.json,
  filesystem-after.json, no controller-replay.json
```

The private repository proof is an additional short-lived preflight artifact.
All Controller results require one completed runner request and a GitHub result
of `succeeded`. Normal and replay additionally require `released`, an active
Node, no reservation, and cleanup-success evidence. The Agent-restart result
also requires the configured minimum running interval and process-identity
continuity across the restart. Cleanup failure requires a quarantined Node and
the reservation retained.

Unit tests are credential-free:

```bash
mise exec -- go test -count=1 ./test/live/linux
mise exec -- go test -race -count=1 ./test/live/linux
```

The real private-sandbox runs remain manual/protected release gates and are not
substituted by those unit tests.
