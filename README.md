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
the accepted specification and bootstrap scaffolding; unimplemented commands return
an explicit error. Do not install it on a production runner fleet yet.

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
just dev         # controller and Vite through Process Compose
```

Tests write machine-readable results below `output/test-results/`, which is ignored
by Git.

## Security

Please read [SECURITY.md](SECURITY.md) before reporting a vulnerability. The native
runner isolation contract and threat model will be completed before the first public
tag; until then, this repository is development software and has no security-support
promise.

## License

Apache License 2.0. See [LICENSE](LICENSE).
