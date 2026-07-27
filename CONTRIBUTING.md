# Contributing to Tewake

Tewake is pre-alpha. Design changes start in the specification, not in an issue title
or implementation patch.

## Before coding

1. Read `spec/tewake/requirements.md` and `spec/tewake/design.md`.
2. Select the next dependency-ready task in `spec/tewake/tasks.yaml`.
3. Keep the change within that task's declared paths and acceptance criteria.
4. If behavior changes, update requirements first, then design, then the task graph.

## Local setup

```bash
mise trust
mise install
pnpm --dir web install --frozen-lockfile
lefthook install
just check
```

Do not use `.env` files. Development-only non-secret settings belong in documented
CLI flags or versioned configuration; credentials use the OS secret-store boundary
defined by the design.

## Pull requests

- Use one task ID per pull request and open it as Draft.
- Describe which acceptance clauses are proven and attach machine-readable or live
  evidence where relevant.
- Use English Conventional Commit messages.
- Do not add generated-by or AI co-author trailers.
- Do not send public fork PR workloads to Tewake self-hosted nodes.
- Do not reduce types, skip tests, or change expected output merely to make CI green.

## Release changes

Release changes must keep `.goreleaser.yaml`, the tag workflow, the artifact
checker, and [docs/RELEASE.md](docs/RELEASE.md) consistent. A local snapshot or
hosted build is not live three-OS evidence. Keep the release draft until the
TWK-014 manifest validates and Apple/Windows signing prerequisites are available.

## Security-sensitive changes

Enrollment, authentication, certificates, GitHub App credentials, JIT delivery,
process cleanup, updater/release code, and persistence invariants require both a
normal-path test and a fail-closed test. Follow `SECURITY.md` for private disclosure;
do not open a public issue for a suspected vulnerability.
