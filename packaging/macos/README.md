# macOS native agent packaging

Status: local adapter, package, launchd contract, and process-table fault tests
pass on macOS. A reboot/sleep cycle, root LaunchDaemon run, Keychain ACL
inspection, and private GitHub job remain live acceptance gates.

## Ownership boundary

The packaged `com.genm.tewake.agent` job is a system LaunchDaemon. The macOS
adapter needs root only to enter the fixed `tewake-runner-0` identity, inspect
the Darwin process table, and remove the runner-owned workspace. The official
runner itself is always launched as the dedicated non-login
`tewake-runner-0` account.

macOS does not provide Linux cgroup v2 or a Windows Job Object. A bare process
group is insufficient because a child can create a new session. Tewake combines:

1. a fresh process group for the official listener;
2. a dedicated real/effective UID for slot 0;
3. admission only while that UID has no processes;
4. cleanup that kills the recorded group and every remaining process with the
   slot UID, then verifies both are empty.

The UID is one-slot authority. Do not log in as `tewake-runner-0`, run another
service under it, or reuse it for a second concurrent slot.

The start fence is root-owned and durable. The listener helper blocks before
receiving JIT material until the launched PID is committed to that fence.
Agent restart can therefore adopt one running listener or clean an ambiguous
fence; it cannot start a second listener to regain liveness.

## Files

| Path | Owner/mode | Purpose |
|---|---|---|
| `/usr/local/libexec/tewake-agent` | `root:wheel 0755` | Agent and fixed launcher helper |
| `/Library/LaunchDaemons/com.genm.tewake.agent.plist` | `root:wheel 0600` | boot/restart policy |
| `/Library/Application Support/Tewake/agent` | `root:wheel 0700` | non-secret locators, certificates, and Agent SQLite |
| `/Library/Application Support/Tewake/fences` | `root:wheel 0700` | durable start/stop fences |
| `/Library/Application Support/Tewake/runtime` | `root:wheel 0711` | execution-root parent |
| `/Library/Application Support/Tewake/runtime/executions/<digest>` | runner UID `0700` | one disposable runner tree |
| `/Library/Caches/com.genm.tewake/runner` | `root:wheel 0700` | verified official runner packages |

Private material uses macOS Keychain through the shared enrollment persistence
boundary. Files below the Agent state directory contain only a versioned,
non-secret Keychain item locator. The runner account has neither the root
Keychain context nor access to the root-only locator directory. A missing,
locked, denied, or mismatched Keychain item fails native admission.

## Installation

The release installer in `twk-015` invokes the checked
`install-service.sh`. For a development package:

```bash
sudo install -o root -g wheel -m 0755 \
  ./tewake-agent /usr/local/libexec/tewake-agent
sudo ./packaging/macos/install-service.sh
```

Enrollment must be completed in the packaged root service context before
starting a private runner target. Do not copy a node private key into the state
directory or pass it through an environment variable.

Inspect the non-secret service state with:

```bash
sudo launchctl print system/com.genm.tewake.agent
```

## Sleep and reboot

`RunAtLoad` restores the Agent after reboot and `KeepAlive` restarts only failed
exits. The Agent's existing reconnect loop handles network loss across sleep.
Running process groups use the dedicated slot UID rather than the Agent's
launchd process group, so an Agent-only restart does not create a duplicate.
After a full reboot the boot epoch changes; stale PIDs are never signalled, and
the journal converges through verified workspace cleanup before capacity
returns.

## Native isolation limitation

This is not a hostile-code sandbox. A trusted workflow can deliberately invoke
privileged host services or a set-user-ID executable. Tewake's UID/process-group
boundary is cleanup ownership for trusted private workflows, not proof that the
host is pristine.

Primary platform references:

- [Creating launch daemons and agents](https://developer.apple.com/library/archive/documentation/MacOSX/Conceptual/BPSystemStartup/Chapters/CreatingLaunchdJobs.html)
- [Keychain services](https://developer.apple.com/documentation/security/keychain-services)
- [Restricting keychain item accessibility](https://developer.apple.com/documentation/security/restricting-keychain-item-accessibility)

