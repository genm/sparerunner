# macOS native-runner live acceptance

This directory owns the macOS-specific TWK-008 evidence contract. It validates
the packaged system LaunchDaemon, the dedicated non-login runner identity,
Keychain-backed node material, the pinned official runner archive, the local
runner journal, the boot epoch, process ownership, and cleanup residue.

It does not replace TWK-014's three-OS/two-installation GitHub fleet gate. A
protected private-repository workflow must drive the real runner lifecycle;
mock runner output is not accepted as live evidence.

Native mode is for trusted private workflows. A dedicated UID and process group
limit accidental residue but are not a hostile-code sandbox.

## Safety and evidence authority

- Run only from an exact clean commit. The installed Agent and harness must both
  retain Go `vcs.revision` metadata for that commit and `vcs.modified=false`.
- Build the harness, install it root-owned at the exact configured path, and
  record its SHA-256. The harness reopens and hashes itself by descriptor before
  it is allowed to probe the runner account.
- The installed Agent must be root-owned mode `0755`; the LaunchDaemon plist
  must be root-owned mode `0600`. Its effective label, program, user, and group
  are checked with `plutil`, while `launchctl print` supplies the live PID and
  state.
- The configured runner checksum must equal the code-pinned official package
  for the selected architecture. The cached archive's owner, mode, exact size,
  SHA-256, and single-link identity are revalidated.
- Agent SQLite is opened read-only and checked before the one configured
  Execution ID is projected. JIT digests, fence tokens, command payloads,
  arguments, environment, runner output, and private material are never emitted.
- The root service must load the node key through `enroll.LoadPrivateMaterial`.
  A copy of this exact harness then drops to `tewake-runner-0` and must fail to
  load it. The locator itself must not contain PEM/private-key bytes.
- Evidence files are allowlisted by phase, mode `0600`, write-once, and stored
  in a mode-`0700` directory outside Agent state, runtime, fence, and cache
  roots. A second capture cannot replace the first result.

The service-load check fails closed when Keychain is locked, denied, missing,
or inconsistent. The runner-denial child is considered valid only when it
starts successfully under the dedicated UID and returns the expected denial
status; an exec failure is not synthetic success.

## Prepare a protected build

From a clean checkout, build both arm64 and amd64 artifacts. Install only the
artifact matching the live host:

```bash
git status --porcelain=v1 --untracked-files=all
git rev-parse --verify HEAD
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -o /tmp/tewake-macos-live-arm64 ./test/live/macos
GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 go build -o /tmp/tewake-macos-live-amd64 ./test/live/macos
sudo install -o root -g wheel -m 0755 /tmp/tewake-macos-live-arm64 /usr/local/libexec/tewake-macos-live
go version -m /usr/local/libexec/tewake-agent
go version -m /usr/local/libexec/tewake-macos-live
shasum -a 256 /usr/local/libexec/tewake-agent
shasum -a 256 /usr/local/libexec/tewake-macos-live
shasum -a 256 /Library/LaunchDaemons/com.genm.tewake.agent.plist
```

The first command must print nothing. Create a fresh root-owned evidence parent
and a separate config for every scenario:

```bash
sudo install -d -o root -g wheel -m 0700 /var/db/tewake-macos-live
sudo cp test/live/macos/config.example.json /var/db/tewake-macos-live/normal.json
sudo chown root:wheel /var/db/tewake-macos-live/normal.json
sudo chmod 0600 /var/db/tewake-macos-live/normal.json
```

Replace every placeholder and set the exact Controller Execution ID. For
`amd64`, use the code-pinned Darwin amd64 checksum, not the arm64 example.
Validate before starting a workflow:

```bash
sudo ./test/live/macos/run.sh \
  /usr/local/libexec/tewake-macos-live \
  validate-config \
  /var/db/tewake-macos-live/normal.json
```

## Normal cleanup

Use a fresh Agent/Controller state and private workflow queue for the scenario.

```bash
sudo ./test/live/macos/run.sh /usr/local/libexec/tewake-macos-live capture /var/db/tewake-macos-live/normal.json before
# Trigger the protected private workflow and wait until its runner journal is running.
sudo ./test/live/macos/run.sh /usr/local/libexec/tewake-macos-live capture /var/db/tewake-macos-live/normal.json running
# Wait for the job and asynchronous fence garbage collection to finish.
sudo ./test/live/macos/run.sh /usr/local/libexec/tewake-macos-live capture /var/db/tewake-macos-live/normal.json after
sudo ./test/live/macos/run.sh /usr/local/libexec/tewake-macos-live validate /var/db/tewake-macos-live/normal.json normal
```

The manifest requires one LaunchDaemon Agent, the journal PID as process-group
leader while running, every process retaining the dedicated UID, one workspace
and one fence while running, then a `released` journal with zero runner
processes, workspaces, and fences.

## Sleep/wake

Capture the same running execution immediately around a real sleep cycle:

```bash
sudo ./test/live/macos/run.sh /usr/local/libexec/tewake-macos-live capture /var/db/tewake-macos-live/sleep.json running-before-sleep
sudo pmset sleepnow
# After wake and Controller connectivity has recovered:
sudo ./test/live/macos/run.sh /usr/local/libexec/tewake-macos-live capture /var/db/tewake-macos-live/sleep.json running-after-wake
# After the workflow completes:
sudo ./test/live/macos/run.sh /usr/local/libexec/tewake-macos-live capture /var/db/tewake-macos-live/sleep.json after
sudo ./test/live/macos/run.sh /usr/local/libexec/tewake-macos-live validate /var/db/tewake-macos-live/sleep.json sleep
```

The boot epoch, Agent PID, journal PID, and dedicated-UID PID set must remain
identical across sleep/wake; the terminal capture must prove cleanup.

## Reboot recovery

Capture a real running execution before reboot. Reboot is an explicit operator
action and is never initiated by the harness.

```bash
sudo ./test/live/macos/run.sh /usr/local/libexec/tewake-macos-live capture /var/db/tewake-macos-live/reboot.json pre-reboot
sudo shutdown -r now
# After launchd starts the Agent and reconciliation reaches released:
sudo ./test/live/macos/run.sh /usr/local/libexec/tewake-macos-live capture /var/db/tewake-macos-live/reboot.json post-reboot
sudo ./test/live/macos/run.sh /usr/local/libexec/tewake-macos-live validate /var/db/tewake-macos-live/reboot.json reboot
```

The manifest requires a changed boot epoch, exactly one live Agent, no process
under the runner UID, a released journal, and no workspace/fence residue.

## Remaining live gates

Repository-local tests validate the evidence parser and fail-closed invariants,
but they are not real reboot, sleep, Keychain ACL, Intel hardware, or private
GitHub job evidence. TWK-008 remains `in_progress` until protected arm64 and
amd64 hosts produce these manifests and Controller-side evidence confirms the
post-wake/post-reboot authenticated session rather than only local launchd
recovery.
