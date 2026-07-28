---
name: sprun-cli
description: Operate a SpareRunner fleet from the sprun CLI — initialize and serve a controller, connect a GitHub App, enroll nodes with join codes, control node availability and per-Target exclusion, export and apply configuration, and validate release evidence. Use when running sprun or sparerunner-agent, reading their JSON output, or diagnosing enrollment, GitHub App, or availability problems.
---

# Operating SpareRunner with `sprun`

SpareRunner runs official GitHub Actions runner processes on persistent computers
the administrator already owns. `sprun` is the operator CLI and the controller
binary. `sparerunner-agent` is the node service. Both names are deliberate: the
CLI is short because it is typed, the Agent is explicit because it is read in
process lists and logs.

## Before running anything

**Only ever point a fleet at private repositories and workflows you trust.** A
node runs jobs as real processes on a real computer. Native mode is not a
sandbox. If a request involves a public repository, a fork pull request, or
untrusted workflow code, stop and say so rather than configuring it.

**Join codes are credentials.** They start with `spr_` and enroll a computer into
the fleet. Never paste one into a commit, an issue, a log, or a chat message.
When showing command output that contains one, redact it.

**A GitHub App private key is never a flag value.** `sprun github connect` reads
it from a file path and hands it to the host credential store. Do not try to pass
key material inline.

## Which computer am I on?

Almost every mistake in this CLI comes from running a command on the wrong host.

| Command | Runs on | Reads/writes |
| --- | --- | --- |
| `init`, `serve`, `node add`, `github …`, `ui authorize`, `config …` | the controller host | controller state directory |
| `join` | the computer being enrolled | agent state directory |
| `node status`, `node pause`, `node resume`, `node targets` | the node itself | that node's local control endpoint |
| `doctor` | the computer being diagnosed | controller state when present, and this computer's local agent endpoint |

`node status` and its siblings talk to the local agent over a same-host socket
(Unix domain socket, or a named pipe on Windows). They are not remote commands
and cannot inspect a different node.

`--state-dir` defaults to the OS user config directory. When a node's agent runs
as an OS service, its state lives under the service's directory, so an
interactive `sprun node status` needs an explicit `--state-dir` pointing there —
otherwise it reads a different, probably uninitialized, state directory and
reports that no agent is present.

## Command surface

This is the whole surface. Do not invent subcommands; if a task seems to need one
that is not listed, say it does not exist.

```
sprun init                       initialize a controller
sprun serve                      run the controller and the embedded Web UI
sprun join [join-code]           enroll this computer into a fleet
sprun node add                   create a one-time join code
sprun node status                does this computer accept new jobs?
sprun node pause                 stop accepting new jobs (running jobs continue)
sprun node resume                accept new jobs again
sprun node targets [targetId]    list, --exclude, or --include a GitHub Target
sprun github connect             store credentials of a GitHub App you created
sprun github installations       list accounts the App is installed into
sprun ui authorize <code>        authorize one browser handoff to the console
sprun config export              export non-secret configuration as YAML
sprun config apply <file|->      apply a versioned configuration document
sprun doctor                     read-only diagnosis of controller, GitHub, and this computer's agent
sprun evidence validate --file   validate a cross-platform evidence manifest
sprun version                    print version and provenance
```

## Bring up a controller

```bash
sprun init
sprun serve
```

`init` creates the controller CA and identity, its database, and management
credentials. `serve` binds a loopback management listener on `127.0.0.1:7442` and
an mTLS agent listener on `:7443`. The management console is loopback-only by
design — do not try to expose it by binding another address.

The console cannot create its own administrator session. It displays a code, and
you authorize it from the controller host:

```bash
sprun ui authorize '<handoff-code>'
```

## Connect a GitHub App

Create the App yourself on GitHub, then hand its credentials to the controller:

```bash
sprun github connect \
  --app-id 1234567 \
  --client-id Iv1.0123456789abcdef \
  --private-key-file ./private-key.pem
sprun github installations
```

`installations` lists the accounts the App is installed into. A Target can only
be created against an installation that verifies as private.

## Enroll a node

On the controller host:

```bash
sprun node add
```

This prints a one-time join code and the exact command to run. On the computer
being enrolled:

```bash
sprun join spr_...
```

The code carries endpoint hints and a pinned controller fingerprint. Discovery
over mDNS only supplies candidate endpoints; the fingerprint is what makes the
connection trustworthy, so a node never trusts a discovered controller that does
not match.

If enrollment fails, check in this order: the code has not already been used
(they are one-time), the node can reach the controller's agent listener on 7443,
and the controller is actually serving.

## Control a node's availability

These run on the node. Use `--json` whenever you need to act on the result —
the text output is for humans and is not a stable contract.

```bash
sprun node status --json
sprun node pause --source cli
sprun node resume --source cli
```

`pause` stops the node accepting *new* jobs; a job already running is not
cancelled. This is the safe way to reclaim a computer you are also using.

`--source` records which surface made the change (`cli`, `tray`, or `raycast`)
and shows up in the audit trail. Leave it as `cli` unless you are reproducing
what another surface did.

Per-Target exclusion lets a node keep serving the rest of the fleet while
dropping one scope:

```bash
sprun node targets --json
sprun node targets <targetId> --exclude
sprun node targets <targetId> --include
```

## Configuration

```bash
sprun config export > fleet.yaml
sprun config apply fleet.yaml
```

`export` emits non-secret configuration only; secrets stay in the host
credential store and are never written to the file. `apply` is atomic and
revision-checked, so a stale document is rejected rather than silently merged.
When `apply` reports a revision conflict, re-export, re-apply your intent on top
of the current revision, and apply again — do not retry the stale file.

## Diagnose a computer

```bash
sprun doctor --json
```

`doctor` is read-only and composes the surfaces above into one report:
controller state, the management session, GitHub authority, and this computer's
agent endpoint. Absence is not failure — a computer with no controller state or
no installed agent reports `unavailable` findings and exits zero. The exit code
is non-zero exactly when a check fails, so scripts can gate on it. Findings
carry the same machine-readable error classes the underlying surfaces emit.

## Reading failures

The CLI fails closed and says what to run next. Two error shapes are worth
recognizing:

- *"is not an initialized controller state directory; run `sprun init` first"* —
  the `--state-dir` points somewhere that `init` never created. A hand-made
  directory is not equivalent.
- *"no GitHub App is connected; run `sprun github connect` first"* — the
  controller has no App credential, so Target verification cannot proceed.

Neither is fixed by creating files by hand. Run the command the error names.

## Development

Inside this repository, build and run from source rather than installing:

```bash
go run ./cmd/sprun --help
just build            # writes bin/sprun
just check            # the full gate: lint, typecheck, test, build
```

The Linux vertical tests drive the real binaries in containers:

```bash
just test-enrollment-cli-linux
just test-node-availability-linux
```
