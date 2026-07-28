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

## Branches

`main` is the only long-lived branch. There is no `develop` and no `release/*`:
those exist to maintain several shipped versions at once, and this project has
none. A maintenance branch can be cut from a tag later if a released version
ever needs a fix, so nothing is lost by not carrying one now.

Branch from the current `main` and name the branch after what owns the change:

| Change | Branch |
| --- | --- |
| A specification task | `task-NNN-short-slug` |
| A fix no task owns | `fix/short-slug` |
| Documentation only | `docs/short-slug` |
| CI, tooling, or repository hygiene | `chore/short-slug` |

The slug is a human affordance and may be reworded freely; the task ID is the
part that must stay accurate. `dependabot/*` belongs to Dependabot — do not
create branches in that namespace by hand.

When a task's `depends_on` is not yet merged, branch from the dependency instead
of `main` and target the dependency's pull request as the base. That makes the
task graph visible in GitHub rather than in the author's head. After the parent
merges, rebase onto `main`:

```bash
git rebase --onto main task-011-restart-reconciliation task-014-live-gate
```

A branch is short-lived by contract. Rebase on `main` rather than letting a
branch drift, and delete it once it merges or is abandoned — in the same change
that abandons it. Long-running branches are how a task quietly stops matching
the specification it claims to implement.

Agent and parallel work belongs in a Git worktree outside the repository, under
`~/.claude/worktrees/sparerunner/<branch>` or `~/.codex/worktrees/sparerunner/<branch>`.
Whoever creates a worktree removes it with `git worktree remove` in the same
task. Do not run a full dependency install in a worktree that only needs to read
or edit files.

## Merging

Pull requests are squash-merged, so `main` keeps one commit per task and its
history stays bisectable. Merge commits and rebase merges are not used, and
`main` is never force-pushed.

Merge through the merge queue: required CI runs the queued combination, which is
what stops two individually-green pull requests from breaking `main` together.
`.github/workflows/ci.yml` already handles `merge_group`.

`main` requires:

- required status checks, including `ci/required`, before merge;
- linear history, matching squash-only merges;
- no force push and no deletion;
- automatic branch deletion after merge.

Real SpareRunner nodes only ever run on protected branches and release
workflows. That guarantee depends on `main` actually being protected, so the
protection rules above are a security control, not a preference.

## Releases

Tags are cut from `main` and nowhere else, and the tag is the only release
trigger. Pre-alpha tags are `v0.Y.Z-alpha.N`. See [docs/RELEASE.md](docs/RELEASE.md)
for the gates a tag must clear before its draft can be published.

## Pull requests

- Use one task ID per pull request and open it as Draft. Task IDs are `task-NNN`
  and are permanent, opaque handles: the number is mint order, never execution
  order, and it is never reused or renumbered. Execution order is `depends_on`.
  Repository-hygiene changes that no task owns declare `none` with a reason.
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
task-014 manifest validates and Apple/Windows signing prerequisites are available.

## Security-sensitive changes

Enrollment, authentication, certificates, GitHub App credentials, JIT delivery,
process cleanup, updater/release code, and persistence invariants require both a
normal-path test and a fail-closed test. Follow `SECURITY.md` for private disclosure;
do not open a public issue for a suspected vulnerability.
