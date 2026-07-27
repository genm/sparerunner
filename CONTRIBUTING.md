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
  -S "" -s com.genm.tewake.private-material.v1 -a "$account"
```

That keeps access within the boundary the trust-all ACL already grants — any
process of the same user, still not the separate runner UID.

## Pull requests

- Use one task ID per pull request and open it as Draft.
- Describe which acceptance clauses are proven and attach machine-readable or live
  evidence where relevant.
- Use English Conventional Commit messages. Pull request titles, descriptions, and
  review comments are English too, as are issue titles and bodies.
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
