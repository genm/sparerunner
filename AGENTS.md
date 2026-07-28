# Tewake repository instructions

These instructions supplement the user-scope rules. The closest specification under
`spec/tewake/` is authoritative for product behavior.

## Language

- Write all repository content in English: source code, identifiers, comments,
  commit messages, `spec/tewake/` documents, other Markdown docs, and GitHub-facing
  content such as issue titles/bodies, pull request titles/descriptions, and review
  comments.
- This applies regardless of the contributor's or reviewer's working language, and
  regardless of the language a request or conversation is conducted in. A pull
  request description is repository content, not a reply. Write it in English the
  first time rather than translating it afterwards.

## Product boundary

- Tewake schedules official GitHub Actions runner processes on persistent computers
  that the administrator already owns.
- Native mode is for private, trusted workflows. It is not a sandbox. Never weaken
  public-scope rejection or describe ephemeral runner identity as an ephemeral host.
- Keep the user-facing concepts limited to Node, GitHub Target, and Runner Profile
  unless the requirements change.
- Do not add arbitrary node, payload, history, or concurrency limits. Follow
  the product-constraint rule that every limit needs a platform contract,
  demonstrated security/integrity risk, or measured resource boundary.

## Source of truth and task flow

- Requirements: `spec/tewake/requirements.md`
- Design and ownership boundaries: `spec/tewake/design.md`
- Dependency graph and executable acceptance: `spec/tewake/tasks.yaml`
- Update upstream spec files before changing downstream task behavior.
- Implement one task as one mergeable Draft PR. Merge bottom-up by `depends_on`.
- Mark a task `done` only after its acceptance evidence passes.

## Ownership boundaries

- `internal/domain` has no database, network, OS, or GitHub SDK imports.
- Only `internal/github` may import `github.com/actions/scaleset`.
- Only `internal/store` owns SQLite schemas and migrations.
- Controller SQLite owns desired state; the agent journal and OS runtime own observed
  local state. Reconciliation must not silently overwrite either authority.
- JIT configuration is an opaque transit secret. Tewake never persists or logs the
  body. The official runner receives it through `--jitconfig` and materializes
  configuration files in its execution-specific root; Agent cleanup must remove and
  verify those files before releasing capacity.
- Web and CLI mutations use the same `/api/v1` contract.
- Generated OpenAPI clients are never edited by hand; change `api/openapi.yaml` and
  run the canonical generation command.

## Commands and verification

- Install pinned tools with `mise install`.
- Use `just fmt`, `just lint`, `just test`, `just build`, and `just check`.
- `just check` must emit machine-readable test evidence under
  `output/test-results/`.
- Every meaningful behavior change needs a normal-path test and at least one
  failure, invalid-input, timeout, permission, or degraded-path test.
- UI components use Playwright Component Testing as the primary visual behavior
  surface once introduced. Capture and self-review screenshots for UI changes.
- Required pull-request CI uses GitHub-hosted runners only. Real Tewake nodes are
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

- Commit messages are English Conventional Commits and do not contain AI trailers.
- Do not bypass lefthook. Fix the owning failure.
- Actions are pinned to full commit SHAs.
- CI artifacts always set short explicit `retention-days`; traces, video, and
  coverage upload only on failure.
- Release artifacts require checksums, SBOM, and provenance. Never claim notarized or
  Authenticode-signed output without live verification.
