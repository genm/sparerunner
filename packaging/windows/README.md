# Windows agent packaging

Status: implementation and cross-compilation are available, but SPR-009 remains
`in_progress` until the real Windows acceptance matrix below has been captured.
Cross-compilation is not evidence for SCM, DPAPI, Job Object, locked-file, sleep,
or reboot behavior.

## Supported boundary

The packaged Agent uses two Windows services:

| Service | Account | Responsibility |
|---|---|---|
| `SpareRunnerAgent` | `LocalSystem` | mTLS session, DPAPI-owned node state, runner journal, package verification, Job Object ownership |
| `SpareRunnerRunnerIdentity` | `NT SERVICE\SpareRunnerRunnerIdentity` | inert service whose distinct primary token is duplicated for one-job runner processes |

The runner-identity service executes no network listener and accepts no
commands. The Agent refuses native capacity if that service is stopped, its
process token changes SID, or its SID equals the Agent SID. A runner is created
suspended with `CreateProcessAsUser`, assigned to a named Job Object with
`JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`, and only then resumed. SpareRunner launches
`bin\Runner.Listener.exe` directly; it does not invoke `cmd.exe` or PowerShell
for a workflow.

The Agent process owns Job Object handles. A Controller or WebSocket disconnect
does not close those handles, so an assigned job continues. An Agent service
crash or machine shutdown closes the handles and Windows terminates the owned
process tree. On recovery, the durable execution fence and journal require
cleanup/reconciliation before capacity can return.

This boundary reduces accidental host exposure. It is **not a sandbox for
malicious workflow code**. Native Windows runners are only for private
repositories and trusted workflow authors, actions, and dependencies.

## Installation

Run an elevated Windows PowerShell 5.1 or newer session from an unpacked release:

```powershell
.\packaging\windows\install.ps1 `
  -AgentBinary (Resolve-Path .\sparerunner-agent.exe) `
  -CliBinary (Resolve-Path .\sparerunner.exe)
```

The installer:

- refuses both install and data roots if either already exists; the first
  release has no in-place upgrade path and never claims or rewrites a foreign
  directory;
- atomically publishes a protected ownership marker in each newly created
  root. Both markers bind the same random installation ID to an exact canonical
  path and distinct `install`/`data` roles;
- copies the binaries with a staging rename and refuses to clobber an existing
  installation;
- creates protected, non-reparse directories under `%ProgramFiles%\SpareRunner` and
  `%ProgramData%\SpareRunner`;
- grants the runner service SID read/execute on the installed Agent and runtime
  root, but no access to Agent state or the verified package cache;
- configures both services for automatic restart and makes the Agent depend on
  the runner identity service;
- puts no join code, node key, controller credential, or JIT material in an SCM
  argument, environment variable, file, or installer log.

The service starts without enrollment state and exposes exactly one local named
pipe instance. In the same elevated session, run:

```powershell
& "$env:ProgramFiles\SpareRunner\sparerunner.exe" join spr_...
```

On Windows, `sparerunner join` does not write Agent state as the interactive user. It
verifies that `\\.\pipe\SpareRunnerEnroll` belongs to the running LocalSystem
`SpareRunnerAgent` SCM PID, submits one versioned request, and reports success only
after the service has durably persisted and reloaded the node credential. If
that durable join succeeds but the acknowledgement cannot reach the CLI, the
Agent exits non-zero so SCM recovery restarts from the durable state; it never
reports bootstrap success or performs a second join. The server pipe:

- has a protected DACL for LocalSystem and elevated Administrators only;
- rejects remote clients;
- verifies the client PID has an elevated Administrator or LocalSystem token;
- rejects unknown/duplicate fields, an unsupported version, malformed join
  codes, oversized frames, disconnects, timeouts, and a second pipe instance;
- returns fixed failure classes and never serializes upstream errors or secret
  material.

The node private key and certificate configuration are DPAPI user-scope
ciphertexts owned by the final LocalSystem service identity. Plaintext is never
stored in SQLite. Stopping or changing the service identity withdraws native
capacity instead of falling back to plaintext.

## Filesystem authority

| Path | Protected ACL |
|---|---|
| `%ProgramFiles%\SpareRunner` | LocalSystem/Administrators full; runner service SID read/execute |
| `%ProgramData%\SpareRunner` | LocalSystem/Administrators full; runner service SID read/execute for traversal |
| `%ProgramData%\SpareRunner\agent-state` | LocalSystem/Administrators only |
| `%ProgramData%\SpareRunner\cache` | LocalSystem/Administrators only |
| `%ProgramData%\SpareRunner\runtime` | LocalSystem/Administrators full; runner service SID read/execute |
| `runtime\executions\<digest>` | runner service SID owner/full; LocalSystem/Administrators full |

Every authoritative path rejects reparse points. Workspace cleanup validates
the volume/file ID and protected DACL before removal. A sharing violation from a
locked file is a cleanup failure: the locator remains in the Agent journal and
the Node becomes quarantined. SpareRunner never reports an empty/healthy workspace
in that state. Durable execution fences live below `runtime\.sparerunner-fences`
with a separate LocalSystem/Administrators-only DACL; the runner identity can
read its execution tree but cannot inspect or modify its cleanup authority.

## Service inspection

These commands expose non-secret effective state:

```powershell
Get-CimInstance Win32_Service -Filter "Name='SpareRunnerAgent'" |
  Select-Object Name, State, StartName, ProcessId, PathName
Get-CimInstance Win32_Service -Filter "Name='SpareRunnerRunnerIdentity'" |
  Select-Object Name, State, StartName, ProcessId, PathName
Get-Acl "$env:ProgramData\SpareRunner\agent-state" |
  Select-Object Owner, AreAccessRulesProtected
```

Use [`test/live/windows/run.ps1`](../../test/live/windows/run.ps1) to capture the
machine-readable platform tests, service preflight, DPAPI cross-identity
rejection, controlled service recovery, and two-phase reboot evidence. The
live harness parses each SCM `PathName` with Windows `CommandLineToArgvW` and
requires the exact executable, service name, role, state/cache/runtime paths,
runner identity, and native-runner flag; duplicate, missing, reordered, or
unknown arguments fail.

## Uninstall

The default uninstall removes services and installed binaries but preserves
enrollment state, the journal, cache, and quarantined workspaces:

```powershell
.\packaging\windows\uninstall.ps1
```

Permanent data removal is a separate, explicitly confirmed action:

```powershell
.\packaging\windows\uninstall.ps1 -PurgeData
```

The primary confirmation names the effective services and verified install
root that will be removed. `-PurgeData` remains a second independent
confirmation for enrollment state, journal, cache, and quarantined workspaces.

The uninstaller rejects paths outside the owning Program Files/ProgramData
trees. Before it changes SCM state, it validates each marker as a regular,
non-reparse, LocalSystem/Administrators-only file with canonical version, role,
path, and cross-root installation ID. A normal uninstall removes the install
root but deliberately retains the data marker, so a later `-PurgeData` can
establish authority without the binary root. Foreign, cross-role,
cross-installation, or tampered markers fail without stopping services or
deleting files.

Owned-root publication also fails closed across the atomic rename boundary. If
post-publish validation fails, the installer removes only a destination whose
exact marker, role, path, installation ID, and ACL can be re-established. A
changed marker, changed ACL, or any extra directory content makes the
destination ambiguous; it is retained for operator inspection rather than
being deleted under an unproven rollback authority.

After ownership is established, the uninstaller inventories every descendant
that would be removed and rejects any junction, symlink, or other reparse
point. Removal then deletes explicit files and directories bottom-up without a
recursive filesystem API; an entry raced into the tree makes removal fail
closed.

## Release acceptance still required

The tagged release remains blocked until artifacts from real, clean Windows
hosts prove all of the following:

- Windows 11/Server on `amd64` and Windows 11 on `arm64`;
- install, elevated one-command join, service restart/recovery, sleep/wake, and
  a real reboot;
- same-identity runner rejection and DPAPI decryptability only in the
  `SpareRunnerAgent` service identity;
- a real descendant process tree is gone after Job Object termination;
- a locked workspace produces `cleanup_failed`/`quarantined`, retains its
  locator, and only returns to service after the lock is removed and cleanup is
  verified;
- one private GitHub job, workspace/JIT cleanup, and no idle
  `Runner.Listener.exe`;
- installed binary/provenance matches the clean tested commit.

GitHub-hosted Windows unit tests and cross-builds cover contracts but do not
replace these host-level gates.
