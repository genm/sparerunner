# Tewake

Tewake turns trusted Windows, macOS, and Linux computers you already own into a
small private GitHub Actions fleet. Join a machine with one command, then let a
local controller schedule one-job runner processes across repositories and
organizations—without Kubernetes, cloud instances, or mandatory containers.

> [!WARNING]
> Tewake is not a public-code sandbox. Native mode provides an ephemeral GitHub
> runner identity and disposable work directory, not a disposable machine.
> Workflow code runs with the local runner account's privileges. Use it only with
> private repositories and trusted workflows, contributors, actions, and
> dependencies.

## Status

Tewake is under active pre-alpha development. The repository currently contains
the accepted specification, durable domain/store foundations, the isolated GitHub
scale-set adapter, and a Linux-capable controller/node enrollment path. `tewake
init`, `tewake serve`, `tewake join`, `tewake node add`, and `tewake-agent serve`
work for development, including pinned enrollment, mTLS WebSockets, reconnect,
the signed GitHub App Manifest flow, installation discovery, private Target
verification, generated management UI/API, node-affined scheduling, native runner
lifecycle, and verified cleanup. Those core paths have local and fault-injection
coverage, but OS service installation, a real private GitHub job, the desktop
tray client, the Raycast extension, and live three-OS sandbox evidence remain
release gates. A tag now produces a draft, checksummed six-platform bundle with
CycloneDX SBOM and GitHub attestation steps; it is not a supported release until
the real-machine gate and platform signing prerequisites pass. Do not install it
on a production runner fleet.

The specification is the project source of truth:

- [Requirements](spec/tewake/requirements.md)
- [Design](spec/tewake/design.md)
- [Task graph](spec/tewake/tasks.yaml)

## Intended experience

```bash
tewake init
tewake serve
```

Then, on another computer:

```bash
tewake join twk_...
```

After connecting private GitHub scopes, workflows select the fleet with:

```yaml
runs-on: tewake
```

OS-specific labels are `tewake-linux`, `tewake-macos`, and `tewake-windows`.

On a computer you also use yourself, an optional tray client sits in the system tray
or menu bar. It shows what that computer is running and stops or resumes accepting
new jobs in one click. Stopping never cancels the job already running, and the same
switch is available from Raycast on macOS, as `tewake node pause` / `tewake node
resume`, and in the Web UI.

## Product boundary

- **vs manual self-hosted runners:** automated enrollment, per-job lifecycle, fleet
  scheduling, and multi-organization visibility
- **vs GARM:** schedules on existing enrolled hosts instead of provisioning and
  deleting infrastructure instances
- **vs ARC:** no Kubernetes; native Windows, macOS, and Linux hosts
- **built on GitHub scale sets:** Tewake uses GitHub's official scale-set client and
  runner instead of replacing GitHub's scheduling protocol

The first release deliberately excludes public repositories and external forks,
cloud providers, Kubernetes, controller HA, enterprise RBAC, VMs, external plugins,
Iroh, GPU scheduling, and Docker mode.

## Development

Prerequisites are pinned by `mise`:

```bash
mise trust
mise install
pnpm --dir web install --frozen-lockfile
lefthook install
just check
```

Common commands:

```bash
just check       # formatting, lint, tests, and builds
just build-all   # cross-compile controller and agent
just check-release-artifacts dist # verify a GoReleaser bundle, checksums, and SBOMs
just dev         # Vite Web UI only through Process Compose
```

`just test-enrollment-cli-linux` runs the actual controller and agent CLI binaries
inside a restricted Linux container and verifies join, reconnect, and join-code
non-persistence. `just dev` still starts only the Web UI; it does not imply that
the management API or controller-backed UI flows are available.

Tests write machine-readable results below `output/test-results/`, which is ignored
by Git.

## Security

Please read [SECURITY.md](SECURITY.md) before reporting a vulnerability. The native
runner isolation contract and threat model will be completed before the first public
tag; until then, this repository is development software and has no security-support
promise.

## License

Apache License 2.0. See [LICENSE](LICENSE).
