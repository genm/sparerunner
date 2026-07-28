# SpareRunner repository instructions

These instructions supplement the user-scope rules. The closest specification under
`spec/sparerunner/` is authoritative for product behavior.

## Language

- Write all repository content in English: source code, identifiers, comments,
  commit messages, `spec/sparerunner/` documents, other Markdown docs, and GitHub-facing
  content such as issue titles/bodies, pull request titles/descriptions, and review
  comments.
- This applies regardless of the contributor's or reviewer's working language.

## Product boundary

- SpareRunner schedules official GitHub Actions runner processes on persistent computers
  that the administrator already owns.
- Spare describes where the capacity comes from, not how long it lasts. A lent
  daily-use computer and a full-time surplus computer are the same product, with
  the same concepts and the same install.
- Native mode is for private, trusted workflows. It is not a sandbox. Never weaken
  public-scope rejection or describe ephemeral runner identity as an ephemeral host.
  Any wording that invites lending a personal computer carries that boundary with it.
- Keep the user-facing concepts limited to Node, GitHub Target, and Runner Profile
  unless the requirements change. `Machine` is prose for a Node, never an identifier.
- Do not add arbitrary node, payload, history, or concurrency limits. Follow
  the product-constraint rule that every limit needs a platform contract,
  demonstrated security/integrity risk, or measured resource boundary.

## Source of truth and task flow

- Requirements: `spec/sparerunner/requirements.md`
- Design and ownership boundaries: `spec/sparerunner/design.md`
- Dependency graph and executable acceptance: `spec/sparerunner/tasks.yaml`
- Update upstream spec files before changing downstream task behavior.
- Implement one task as one mergeable Draft PR. Merge bottom-up by `depends_on`.
- Mark a task `done` only after its acceptance evidence passes.
- Task IDs are `task-NNN`: permanent, opaque, never reused or renumbered, and never
  re-derived from the product name. The number is mint order, not execution order;
  `depends_on` is the only ordering. Gaps mean an abandoned task, not a missing one.

## Ownership boundaries

- `internal/domain` has no database, network, OS, or GitHub SDK imports.
- Only `internal/github` may import `github.com/actions/scaleset`.
- Only `internal/store` owns SQLite schemas and migrations.
- Controller SQLite owns desired state; the agent journal and OS runtime own observed
  local state. Reconciliation must not silently overwrite either authority.
- JIT configuration is an opaque transit secret. SpareRunner never persists or logs the
  body. The official runner receives it through `--jitconfig` and materializes
  configuration files in its execution-specific root; Agent cleanup must remove and
  verify those files before releasing capacity.
- Web and CLI mutations use the same `/api/v1` contract.
- Generated OpenAPI clients are never edited by hand; change `api/openapi.yaml` and
  run the canonical generation command.

## Commands and verification

- Set up with `just bootstrap`, which installs the pinned tools, every JavaScript
  project's dependencies, the Playwright browser, and the Git hooks.
- Use `just fmt`, `just lint`, `just test`, `just build`, and `just check`.
  `just check-push` is the pre-push subset; `just check-ci` is the full required
  gate including the race detector and the vulnerability scan. `just --list`
  shows every recipe and the README maps each CI job to its local command.
- `just check` must emit machine-readable test evidence under
  `output/test-results/`.
- Enabling a new linter is its own change with its own remediation.
  `.golangci.yml` and `web/.oxlintrc.json` record, with measured counts, which
  rules are off and why; a drive-by suppression is not an acceptable fix for a
  new finding.
- Every meaningful behavior change needs a normal-path test and at least one
  failure, invalid-input, timeout, permission, or degraded-path test.
- UI components use Playwright Component Testing as the primary visual behavior
  surface once introduced. Capture and self-review screenshots for UI changes.
- Required pull-request CI uses GitHub-hosted runners only. Real SpareRunner nodes are
  limited to trusted protected-branch and release workflows.

## Security invariants

- Fail closed for enrollment, certificates, App credentials, JIT delivery,
  scheduling capacity, cleanup, configuration mutation, and release signing.
- Retain last-known external state and mark it stale; never replace a GitHub failure
  with an empty healthy response.
- Cleanup is part of execution correctness. A cleanup verification failure
  quarantines the node before its slot can be reused.
- Do not expose App keys, join secrets, JIT material, node private keys, session
  secrets, or authorization headers through SQLite, logs, metrics, UI, YAML, or
  diagnostics.
- Test data uses RFC 6761 reserved domains such as `example.test`.

## Git and release

- `main` is the only long-lived branch. Branch names are `task-NNN-slug`, or `fix/`,
  `docs/`, `chore/` when no task owns the change. `dependabot/*` is not hand-authored.
- Branch an unmerged `depends_on` from its dependency and base the PR on that PR,
  then `git rebase --onto main` once the parent merges.
- Pull requests are squash-merged through the merge queue. `main` is never
  force-pushed, and tags are cut from `main` only.
- Worktrees live outside the repository under `~/.claude/worktrees/sparerunner/<branch>`
  or `~/.codex/worktrees/sparerunner/<branch>`, and their creator removes them in the
  same task.
- Commit messages are English Conventional Commits and do not contain AI trailers.
- Do not bypass lefthook. Fix the owning failure.
- Actions are pinned to full commit SHAs.
- CI artifacts always set short explicit `retention-days`; traces, video, and
  coverage upload only on failure.
- Release artifacts require checksums, SBOM, and provenance. Never claim notarized or
  Authenticode-signed output without live verification.
