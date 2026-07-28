# SpareRunner

[![CI](https://github.com/genm/sparerunner/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/genm/sparerunner/actions/workflows/ci.yml)
[![CodeQL](https://github.com/genm/sparerunner/actions/workflows/codeql.yml/badge.svg?branch=main)](https://github.com/genm/sparerunner/actions/workflows/codeql.yml)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/genm/sparerunner/badge)](https://scorecard.dev/viewer/?uri=github.com/genm/sparerunner)
[![Go Reference](https://pkg.go.dev/badge/github.com/genm/sparerunner.svg)](https://pkg.go.dev/github.com/genm/sparerunner)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![Status: pre-alpha](https://img.shields.io/badge/status-pre--alpha-orange)](#status)

**Flexible GitHub Actions runners from the machines you already own — on demand
or full-time.**

SpareRunner turns trusted Windows, macOS, and Linux computers you already own
into a small private GitHub Actions fleet. Join a machine with one command, then
let a local controller schedule one-job runner processes across repositories and
organizations—without Kubernetes, cloud instances, or mandatory containers.

The capacity is spare because you already own the hardware, not because it is
short-lived. One fleet holds both kinds of machine at once:

- the computer you work on, which contributes while you are willing to lend it
  and which you reclaim in one click;
- the surplus computer with nothing else to do, which serves full time.

It fills the gap between registering runners by hand and running dedicated
runner infrastructure:

```text
GitHub-hosted runners
  ↓
a self-hosted runner registered by hand on each machine, for each scope
  ↓
SpareRunner
  ↓
ARC / Kubernetes / commercial runner platforms
```

> [!WARNING]
> SpareRunner is not a public-code sandbox. Native mode provides an ephemeral
> GitHub runner identity and disposable work directory, not a disposable machine.
> Workflow code runs with the local runner account's privileges. Use it only with
> private repositories and trusted workflows, contributors, actions, and
> dependencies. Lending a computer you also use yourself does not change this.

## Status

SpareRunner is under active pre-alpha development. The repository currently
contains the accepted specification, durable domain/store foundations, the
isolated GitHub scale-set adapter, and a Linux-capable controller/node
enrollment path. `sprun init`, `sprun serve`, `sprun join`,
`sprun node add`, and `sparerunner-agent serve` work for development,
including pinned enrollment, mTLS WebSockets, reconnect, the signed GitHub App
Manifest flow, installation discovery across multiple private org/repo scopes,
private Target verification, generated management UI/API, node-affined
scheduling, native runner lifecycle, verified cleanup, node availability
control, per-Target exclusion, the desktop tray, and the Raycast extension. The
production controller now starts a per-Target runner-coordinator fleet against
real GitHub scale sets: each Target with a verified runtime binding gets its own
message session and dispatch loop, and runner executions are created from
GitHub's assigned-job statistic rather than from `JobAvailable` alone, which is
what the scale-set protocol actually requires.

End-to-end job execution is not yet proven live: the last verified live run
received, committed, and acknowledged every message correctly while the workflow
still sat `queued`, and the fix for that has deterministic coverage but no live
confirmation. Those core paths have local and fault-injection coverage, but OS
service installation, a real private GitHub job, and live three-OS sandbox
evidence remain release gates; the Windows local control endpoint fails closed.
Restart and disconnect reconciliation against live GitHub state (task-011),
node-affined scheduling's remaining live multi-Target evidence (task-010),
per-Target availability's restart and reboot evidence (task-019), and
macOS/Windows platform support (task-008/task-009) are still in progress. A tag now
produces a draft, checksummed six-platform bundle with CycloneDX SBOM and GitHub
attestation steps; it is not a supported release until the real-machine gate and
platform signing prerequisites pass. Do not install it on a production runner
fleet.

The specification is the project source of truth:

- [Requirements](spec/sparerunner/requirements.md)
- [Design](spec/sparerunner/design.md)
- [Task graph](spec/sparerunner/tasks.yaml)

## Intended experience

```bash
sprun init
sprun serve
```

Then, on another computer:

```bash
sprun join spr_...
```

Connect a GitHub App you own, then point a Target at a private scope. This needs
no browser session with the controller — see
[Connecting a GitHub App](docs/github-app.md):

```bash
sprun github connect --app-id 1234567 --client-id Iv1.… \
  --private-key-file ~/Downloads/your-app.private-key.pem
sprun github installations
sprun config apply fleet.yaml
```

Keep the App private key outside any repository working directory, and delete it
once `sprun github connect` has stored it. It mints installation tokens for every
installation of that App.

Workflows then select the fleet with:

```yaml
runs-on: sparerunner
```

OS-specific labels are `sparerunner-linux`, `sparerunner-macos`, and
`sparerunner-windows`.

On a computer you also use yourself, an optional tray client sits in the system
tray or menu bar. It shows every private org/repo scope that computer currently
waits on and what is running right now, and stops or resumes accepting new jobs
in one click. The owner can also exclude a single scope while the machine keeps
serving the rest. Stopping never cancels the job already running, and the same
switch is available from Raycast on macOS, and as `sprun node pause` /
`sprun node resume`:

```bash
sparerunner-agent serve --local-control
sprun node status
```

## Product boundary

- **vs manual self-hosted runners:** automated enrollment, per-job lifecycle, fleet
  scheduling, and multi-organization visibility
- **vs GARM:** schedules on existing enrolled hosts instead of provisioning and
  deleting infrastructure instances
- **vs ARC:** no Kubernetes; native Windows, macOS, and Linux hosts
- **built on GitHub scale sets:** SpareRunner uses GitHub's official scale-set
  client and runner instead of replacing GitHub's scheduling protocol

The thing SpareRunner competes with is not Kubernetes. It is the manual loop of
SSHing into each machine, downloading a runner, registering it per repository,
installing a service, adding a second runner for a second job, updating it,
stopping it, and re-registering it when it breaks.

The first release deliberately excludes public repositories and external forks,
cloud providers, Kubernetes, controller HA, enterprise RBAC, VMs, external plugins,
WAN/NAT traversal, GPU scheduling, and Docker mode. Directions the product may
take later are recorded under Future Direction in the
[requirements](spec/sparerunner/requirements.md).

## Development

Prerequisites are pinned by `mise`, and `just bootstrap` installs all of them:

```bash
mise trust
just bootstrap
just check
```

`just check` also needs a Docker-compatible runtime for the privileged Linux
runner boundary test. [CONTRIBUTING.md](CONTRIBUTING.md) covers setup in full,
including what to do without one.

`just --list` shows every recipe. The ones you need most:

```bash
just check       # formatting, lint, tests, and builds
just check-ci    # everything the required CI gate proves, including the race detector
just lint        # Go, Web, workflows, shell, and committed secrets
just build-all   # cross-compile controller and agent
just check-release-artifacts dist # verify a GoReleaser bundle, checksums, and SBOMs
just dev         # Vite Web UI only through Process Compose
```

Every required CI check has a `just` recipe that runs the same thing locally, so
a red pull request can always be reproduced without pushing again:

| CI job | Local command |
| --- | --- |
| Static analysis | `just lint-workflows`, `just lint-shell`, `just lint-secrets` |
| Generated contracts | `just generate-api-check`, `just generate-web-check` |
| Go quality | `just fmt-check`, `just test-go` |
| Lint Go (per GOOS) | `just lint-go` (host only; set `GOOS` for another target) |
| Go race detector | `just test-race` |
| Go vulnerability scan | `just vulncheck` |
| Privileged Linux runner boundary | `just test-platform-linux` |
| Web quality | `just lint-web`, `pnpm --dir web typecheck`, `pnpm --dir web test:ci`, `just check-npm-policy` |
| Web component tests | `pnpm --dir web test:ct` |
| Raycast extension | `just lint-raycast` |
| Management journey | `just test-management-ui-linux`, `just smoke-embedded-ui-linux` |
| Cross-build | `just build-all` |

`just test-enrollment-cli-linux` runs the actual controller and agent CLI binaries
inside a restricted Linux container and verifies join, reconnect, and join-code
non-persistence. `just dev` still starts only the Web UI; it does not imply that
the management API or controller-backed UI flows are available.

Tests write machine-readable results below `output/test-results/`, which is ignored
by Git.

Pull requests also run CodeQL, OpenSSF Scorecard, and dependency review. See
[.github/workflows](.github/workflows) for what each one gates on.

## Security

Please read [SECURITY.md](SECURITY.md) before reporting a vulnerability. The native
runner isolation contract and threat model are documented, while their three-OS live
evidence and signing prerequisites remain release gates. Until those gates pass, this
repository is development software and has no security-support promise.

The boundary itself is written down, not implied:

- [Security contract](docs/security/SECURITY_CONTRACT.md) — what SpareRunner
  promises and what it explicitly does not.
- [Threat model](docs/security/THREAT_MODEL.md) — the adversaries in scope and
  the ones deliberately out of scope.
- [Native isolation](docs/security/NATIVE_ISOLATION.md) — what a runner process
  can and cannot reach on the host.
- [Support matrix](docs/security/SUPPORT_MATRIX.md) — the OS and configuration
  surface each claim has been proven on.

## Contributing and support

- [CONTRIBUTING.md](CONTRIBUTING.md) — how a change flows from specification to
  task to pull request.
- [SUPPORT.md](SUPPORT.md) — where to take a question, a bug, or a proposal.
- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) — the behavior expected in this
  project's spaces.
- [Connecting a GitHub App](docs/github-app.md) and
  [the GitHub sandbox contract](docs/github-sandbox.md) — the two integration
  documents an operator or contributor needs first.
- [docs/RELEASE.md](docs/RELEASE.md) — the operator checklist a release must pass.

## License

Apache License 2.0. See [LICENSE](LICENSE).
