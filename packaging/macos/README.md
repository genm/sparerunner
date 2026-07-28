# macOS native agent packaging

Status: local adapter, package, launchd contract, and process-table fault tests
pass on macOS. A reboot/sleep cycle, root LaunchDaemon run, Keychain ACL
inspection, and private GitHub job remain live acceptance gates.

## Ownership boundary

The packaged `com.genm.sparerunner.agent` job is a system LaunchDaemon. The macOS
adapter needs root only to enter the fixed `sparerunner-runner-0` identity, inspect
the Darwin process table, and remove the runner-owned workspace. The official
runner itself is always launched as the dedicated non-login
`sparerunner-runner-0` account.

macOS does not provide Linux cgroup v2 or a Windows Job Object. A bare process
group is insufficient because a child can create a new session. SpareRunner combines:

1. a fresh process group for the official listener;
2. a dedicated real/effective UID for slot 0;
3. admission only while that UID has no processes;
4. cleanup that kills the recorded group and every remaining process with the
   slot UID, then verifies both are empty.

The UID is one-slot authority. Do not log in as `sparerunner-runner-0`, run another
service under it, or reuse it for a second concurrent slot.

The first macOS release therefore has a hard capability of `maxRunners: 1`.
The current Agent readiness observation maps to that one concrete slot 0; a
Controller rejects the node configuration before its slot ledger can create a
second macOS slot. Supporting `maxRunners > 1` requires a separate non-login
identity and durable process authority for every slot (for example,
`sparerunner-runner-1`) plus an explicit capacity contract. Reusing this UID is not a
supported concurrency mechanism.

The current Controller configuration validator is the capacity SSOT because the
version-1 Agent snapshot exposes only `nativeRunnerReady`. When the protocol
gains a numeric `nativeRunnerCapacity`, the authenticated, reconciled snapshot
becomes the physical-capability SSOT; Controller configuration and slot topology
must remain bounded by that observation.

The start fence is root-owned and durable. The listener helper blocks before
receiving JIT material until the launched PID is committed to that fence.
Agent restart can therefore adopt one running listener or clean an ambiguous
fence; it cannot start a second listener to regain liveness.

## Files

| Path | Owner/mode | Purpose |
|---|---|---|
| `/usr/local/libexec/sparerunner-agent` | `root:wheel 0755` | Agent and fixed launcher helper |
| `/Library/LaunchDaemons/com.genm.sparerunner.agent.plist` | `root:wheel 0600` | boot/restart policy |
| `/Library/Application Support/SpareRunner/.sparerunner-install-ownership-v1` | `root:wheel 0600` | Versioned, non-secret Agent-state ownership marker |
| `/Library/Application Support/SpareRunner/agent` | `root:wheel 0700` | non-secret locators, certificates, and Agent SQLite |
| `/Library/Application Support/SpareRunner/fences` | `root:wheel 0700` | durable start/stop fences |
| `/Library/Application Support/SpareRunner/runtime` | `root:wheel 0711` | execution-root parent |
| `/Library/Application Support/SpareRunner/runtime/executions/<digest>` | runner UID `0700` | one disposable runner tree |
| `/Library/Caches/com.genm.sparerunner/.sparerunner-install-ownership-v1` | `root:wheel 0600` | Matching cache ownership marker |
| `/Library/Caches/com.genm.sparerunner/runner` | `root:wheel 0700` | verified official runner packages |

The initial installer validates every fixed ancestor and destination before its
first mutation. Existing roots are accepted only when both markers have the
same install ID, the expected role and logical path, exact root-owned modes,
the complete empty layout, and no foreign entries. A missing, partial, changed,
symlinked, or unmarked root is rejected; the installer never repairs it with
`chown` or `chmod`. Fixed ancestors must be root-owned and not writable by
another user; the standard root-owned sticky `/Library/Caches` is the only
writable shape accepted. Existing runner user and group records must also carry
the same install ID in their exact RealName contract, have globally unique
numeric IDs, and retain all non-login attributes. A matching name alone is
never adopted.

After preflight, the installer tracks each newly created directory, marker,
Directory Services record, plist, and loaded service. A normal command failure
removes them in reverse order only after revalidating the install ID,
owner/mode, and package contents; then the same clean install converges on
retry. If ownership changed or rollback itself fails, it leaves the resource in
place and reports incomplete rollback instead of deleting foreign state. A
power loss or `SIGKILL` can still leave fail-closed partial state and requires
the future recovery flow.

Private material uses macOS Keychain through the shared enrollment persistence
boundary. Files below the Agent state directory contain only a versioned,
non-secret Keychain item locator. The runner account has neither the root
Keychain context nor access to the root-only locator directory. A missing,
locked, denied, or mismatched Keychain item fails native admission.

Credential creation first writes an account-specific, non-secret recovery
locator and removes it only after the main locator is durable. If main locator
publication and Keychain deletion both fail, the recovery locator remains so a
later `RemovePrivateMaterial` can retry; startup treats a recovery-only state as
an error and never generates a replacement node key over it.

The adapter calls Security.framework directly and never passes a secret through
`/usr/bin/security` or a process argument. It deliberately creates the item with
the Keychain `TrustAll` access mode: any process running in the same root service
user context can read it without a prompt. Code signing does not narrow this
mode to the SpareRunner executable. The security boundary is therefore the root
service context plus its root-only locator directory versus the dedicated
runner UID; compromise of another root process is outside native mode's trusted
workflow threat model.

The packaged LaunchDaemon uses the root service context's default Keychain. If
that Keychain cannot be opened headlessly, is locked, or requires interaction,
`OpenAgent` fails and launchd restarts the required service; a failure detected
after startup makes `CredentialReady` false and advertises zero capacity. There
is no plaintext fallback. Root LaunchDaemon default-Keychain availability,
same-user access, and runner-UID denial remain real-host release gates.
Repository tests use an injected credential store and prove only lifecycle and
fail-closed behavior; they do not prove native Keychain access or ACL behavior.

## Installation

The release installer in `spr-015` invokes the checked
`install-service.sh`. For a development package:

```bash
sudo install -o root -g wheel -m 0755 \
  ./sprun /usr/local/bin/sprun
sudo install -o root -g wheel -m 0755 \
  ./sparerunner-agent /usr/local/libexec/sparerunner-agent
sudo ./packaging/macos/install-service.sh
sudo /usr/local/bin/sprun join spr_... \
  --state-dir "/Library/Application Support/SpareRunner/agent"
sudo /bin/launchctl kickstart -k system/com.genm.sparerunner.agent
```

Installation intentionally precedes enrollment. The LaunchDaemon may restart
with an explicit not-initialized error until the root-context `sprun join`
creates the Agent state above; it must not synthesize empty state. Enrollment
must complete before creating a private runner target. Do not copy a node
private key into the state directory or pass it through an environment
variable. This packaged join path prints the launchd activation command above;
it does not tell the operator to start a second `sparerunner-agent serve` process.

This is an initial-install contract, not an upgrade or uninstall mechanism. If
preflight reports foreign, partial, or changed state, inspect it and use the
future documented upgrade/recovery flow; do not add a force-adopt option or
manually rewrite the ownership marker.

Inspect the non-secret service state with:

```bash
sudo launchctl print system/com.genm.sparerunner.agent
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
privileged host services or a set-user-ID executable. SpareRunner's UID/process-group
boundary is cleanup ownership for trusted private workflows, not proof that the
host is pristine.

Primary platform references:

- [Creating launch daemons and agents](https://developer.apple.com/library/archive/documentation/MacOSX/Conceptual/BPSystemStartup/Chapters/CreatingLaunchdJobs.html)
- [Keychain services](https://developer.apple.com/documentation/security/keychain-services)
- [Restricting keychain item accessibility](https://developer.apple.com/documentation/security/restricting-keychain-item-accessibility)
