# Contributing to SpareRunner

SpareRunner is pre-alpha. Design changes start in the specification, not in an issue title
or implementation patch.

## Before coding

1. Read `spec/sparerunner/requirements.md` and `spec/sparerunner/design.md`.
2. Select the next dependency-ready task in `spec/sparerunner/tasks.yaml`.
3. Keep the change within that task's declared paths and acceptance criteria.
4. If behavior changes, update requirements first, then design, then the task graph.

## Local setup

```bash
mise trust
just bootstrap
just check
```

`just bootstrap` installs every pinned tool, all three JavaScript projects'
dependencies, the Playwright browser that component tests need, and the Git
hooks. It is the only setup command; running the individual `pnpm install` lines
by hand leaves the browser missing and `just check` failing on a step you did not
change.

One prerequisite is not installable by `mise`: `just check` runs the privileged
Linux runner boundary test inside a disposable, networkless container, so it needs
a Docker-compatible runtime (Docker Desktop, OrbStack, or Colima) with a running
daemon. Without one that step fails with an explicit message rather than being
skipped. It is not part of the pre-push hook, so you can still push; required CI
runs it on every pull request.

`mise` pins every other tool the checks need, including `golangci-lint`,
`shellcheck`, `actionlint`, and `gitleaks`. CI installs them from the same
`.mise.toml`, so a version never drifts between your machine and the required
gate.

`just --list` shows every recipe:

- `just check-push` is what the pre-push hook enforces. It is everything that
  works after a clean `just bootstrap` with no daemon running.
- `just check` adds the privileged Linux boundary test and Playwright component
  tests.
- `just check-ci` adds the race detector and the vulnerability scan, which is
  what required CI blocks on.

The README lists the local command for each CI job, so a red pull request can be
reproduced without pushing again.

Do not use `.env` files. Development-only non-secret settings belong in documented
CLI flags or versioned configuration; credentials use the OS secret-store boundary
defined by the design.

### macOS: Keychain prompts on every rebuild

macOS records a `partition_id` ACL naming the process that created a Keychain
item, and the trust-all decrypt ACL the store sets does not cover that check. Go
links host binaries with an ad-hoc signature and no team identifier, so the entry
is a `cdhash:` value that changes with every rebuild and the rebuilt binary is
asked for the login password on each run. Empty the partition list once per
enrolled item to stop it, using the accounts listed by `security dump-keychain`
for the service:

```bash
security set-generic-password-partition-list \
  -S "" -s com.genm.sparerunner.private-material.v1 -a "$account"
```

That keeps access within the boundary the trust-all ACL already grants — any
process of the same user, still not the separate runner UID.

## Pull requests

- Use one task ID per pull request and open it as Draft. Task IDs are `spr-NNN`.
  Anything merged before the SpareRunner rename (spr-022) uses the old `twk-NNN`
  prefix; the mapping is positional, so `twk-007` is `spr-007`.
- Describe which acceptance clauses are proven and attach machine-readable or live
  evidence where relevant.
- Use English Conventional Commit messages. Pull request titles, descriptions, and
  review comments are English too, as are issue titles and bodies.
- Do not add generated-by or AI co-author trailers.
- Do not send public fork PR workloads to SpareRunner self-hosted nodes.
- Do not reduce types, skip tests, or change expected output merely to make CI green.

The pull request template asks for the acceptance clauses you proved, the clauses
you did not, and the names of the normal-path and failure-path tests. "Not yet
proven" is a valid and useful answer; a missing answer is not.

Required CI runs on GitHub-hosted runners only, and it does not skip draft pull
requests — the draft state is where evidence is produced. Alongside it, CodeQL,
OpenSSF Scorecard, and dependency review report on every pull request. A
dependency review failure on a licence is a product decision, not a lint nit:
release archives redistribute dependencies under Apache-2.0, so an exception is
added to `.github/workflows/dependency-review.yml` deliberately or the dependency
is not added.

New static-analysis findings belong to the change that introduced them.
`.golangci.yml` records, with measured counts, which linters are not yet enabled
and what each would take; enabling one is its own change with its own remediation,
not a drive-by suppression.

## Release changes

Release changes must keep `.goreleaser.yaml`, the tag workflow, the artifact
checker, and [docs/RELEASE.md](docs/RELEASE.md) consistent. A local snapshot or
hosted build is not live three-OS evidence. Keep the release draft until the
SPR-014 manifest validates and Apple/Windows signing prerequisites are available.

## Security-sensitive changes

Enrollment, authentication, certificates, GitHub App credentials, JIT delivery,
process cleanup, updater/release code, and persistence invariants require both a
normal-path test and a fail-closed test. Follow `SECURITY.md` for private disclosure;
do not open a public issue for a suspected vulnerability.
