# Linux agent packaging

Status: the local implementation and root-container fault tests pass. The
private GitHub and real systemd acceptance described in
[`test/live/linux`](../../../test/live/linux) remains a separate release gate.

## Supported host boundary

The native Linux adapter requires:

- systemd with `Delegate=yes`;
- the unified cgroup v2 hierarchy;
- a kernel that exposes `cgroup.kill` for delegated non-root cgroups (Linux
  5.14 or newer);
- an unprivileged non-login `tewake-agent` account;
- a separate root Supervisor service and a non-login account for each concrete
  runner slot. The initial package declares `tewake-runner-0`.

Unsupported or partially delegated cgroups fail closed: the node may stay
connected for diagnostics, but it must not advertise or start a native runner.

The Supervisor service deliberately runs with both its primary user and group
set to `root`. systemd changes a delegated service cgroup's ownership to the
configured service user and group; using the Agent group there would make the
trusted cgroup boundary writable by that unprivileged group and would correctly
fail Supervisor startup. The package's tmpfiles rule independently creates
`/run/tewake-supervisor` as `root:tewake-agent 0750` so the Agent can reach the
peer-authenticated local socket. The unit must not use `RuntimeDirectory=`:
systemd reapplies the service's primary `root:root` ownership after
`ExecStartPre=`, which would make the socket directory fail its exact ownership
check. The delegated cgroup itself remains `root:root`.

The network-facing Agent parses mTLS and GitHub-derived messages without root
privileges. The Supervisor accepts a versioned fixed-operation protocol only on
`/run/tewake-supervisor/supervisor.sock`, verifies the peer UID with
`SO_PEERCRED`, and derives all paths, arguments, and slot credentials from its
own service configuration. The Agent cannot ask it to execute an arbitrary
command or select a UID.

The Supervisor creates one child cgroup per execution. It records a durable
start fence before launching the downgraded helper directly into that cgroup
with `CLONE_INTO_CGROUP`, verifies the workspace identity, and only then passes
the one-job JIT material. Cleanup revokes the fence first, uses `cgroup.kill`,
waits until `cgroup.events` reports `populated 0`, removes the execution root,
and verifies absence.

The durable fence becomes `launched` before the Supervisor returns the listener
PID. Once launched, loss or restart of only the Agent preserves that runner;
the restarted Agent can inspect and stop the same containment but cannot launch
a second listener. Loss of an uncommitted session gets a bounded 20-second
revoke and cgroup-empty attempt. Stopping the Supervisor revokes and empties
every cgroup it owns. The packaged unit uses `TimeoutStopSec=30s`, leaving the
outer service cgroup as the final shutdown owner. A timeout is never reported as
clean: the durable runner journal remains cleanup-required and the Agent
quarantines the Node until reconciliation proves the containment and workspace
are gone.

For non-secret runtime inspection, let `CG` be
`systemctl show --property=ControlGroup --value tewake-supervisor.service`.
Every admitted listener and descendant has the unified `/proc/<pid>/cgroup`
path `CG/tewake/tewake-<sha256(executionID)>`, where the digest is 64 lowercase
hex characters. The listener's real UID is exactly the packaged
`tewake-runner-0` UID. A matching argv basename alone is not ownership evidence.
`Alive` first validates the exact durable fence and cgroup, then reports true
only when the journaled listener PID is present in that cgroup's
`cgroup.procs`. An exact launched cgroup that is empty, or still populated only
after that listener PID exited, reports false without weakening ownership; the
Agent adopts it only as cleanup-required state so `Wait`/`Destroy` can converge
the complete descendant boundary.

## Filesystem ownership

| Path | Owner/mode | Purpose |
|---|---|---|
| `/run/tewake-supervisor` | `root:tewake-agent 0750` | volatile parent of the peer-authenticated local socket |
| `/var/lib/tewake-agent` | `tewake-agent 0700` | enrollment state and Agent SQLite |
| `/var/cache/tewake-agent` | `tewake-agent 0700` | verified official runner package cache |
| `/var/lib/tewake-supervisor/fences` | `root:root 0700` | durable start/stop fences |
| `/var/lib/tewake-runtime` | `root:root 0711` | pinned persistent execution-root parent |
| `/var/lib/tewake-runtime/executions/<digest>` | slot user `0700` | one execution; removed after verified cleanup |
| `/var/lib/tewake-runner/0` | `tewake-runner-0 0700` | unused non-login account metadata home; jobs receive an execution-local `HOME` |

The shared package cache is never traversable by a slot user and is not package
authority for the privileged boundary. The root Supervisor fixes the expected
official package from its own platform and configuration, opens the Agent-owned
cache archive, and copies it into a root-owned execution-local file while
independently checking exact size and SHA-256. It extracts only from that same
root-owned descriptor, deletes the temporary archive, creates the
execution-local `HOME`/XDG/`TMPDIR`, and hands the completed tree to the slot
user last. An Agent-created tree, an archive path reopened after verification,
or a persistent runner home is never launch authority.

## Installation layout

Distribution packages install:

```text
/usr/local/bin/tewake-agent
/usr/lib/systemd/system/tewake-agent.service
/usr/lib/systemd/system/tewake-supervisor.service
/usr/lib/sysusers.d/tewake.conf
/usr/lib/tmpfiles.d/tewake.conf
```

Then they run:

```bash
systemd-sysusers /usr/lib/sysusers.d/tewake.conf
systemd-tmpfiles --create /usr/lib/tmpfiles.d/tewake.conf
systemctl daemon-reload
systemctl enable --now tewake-supervisor.service tewake-agent.service
```

Enrollment material must already exist in `/var/lib/tewake-agent`; installation
does not synthesize credentials or copy a private key from another user.

## Native isolation limitation

This service reduces accidental host exposure but is not a sandbox for malicious
workflow code. It is only for private repositories and trusted workflow authors,
actions, and dependencies.

`HOME`, XDG cache/config paths, and `TMPDIR` are disposable per execution. The
systemd `PrivateTmp` namespace belongs to the Supervisor service, not to one job;
a workflow that ignores `TMPDIR` and writes an absolute `/tmp` path can therefore
leave data visible to a later trusted job. Tewake does not claim job-level mount
isolation in native mode.

The initial package provisions one slot and therefore admits one native runner at
a time. Raising `node.maxRunners` requires provisioning the matching fixed
`tewake-runner-N` accounts and homes first; the Agent must not advertise a slot
whose dedicated identity is absent. Reusing one UID for multiple concurrent slots
is forbidden because the official runner receives JIT material through its
transient command line and writes credentials beneath its execution root.

Primary platform references:

- [systemd `Delegate=`](https://www.freedesktop.org/software/systemd/man/latest/systemd.resource-control.html#Delegate=)
- [systemd execution directories and hardening](https://www.freedesktop.org/software/systemd/man/latest/systemd.exec.html)
- [Linux cgroup v2 `cgroup.kill` and `cgroup.events`](https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html)
