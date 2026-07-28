# Security Policy

## Supported versions

SpareRunner is pre-alpha and has no supported release yet. Security fixes target the
default branch until the first tagged support policy is published.

## Reporting a vulnerability

Report privately, through GitHub's private vulnerability reporting for this
repository:

**<https://github.com/genm/sparerunner/security/advisories/new>**

The same form is reachable from the repository's Security tab under "Report a
vulnerability". It is enabled today, and it is the only channel for a suspected
vulnerability. Do not open a public issue, and do not include secrets, exploit
payloads, or sensitive runner logs anywhere public.

Include:

- affected commit or release
- operating system and architecture
- controller/agent topology
- minimal reproduction
- expected and observed trust boundary
- whether GitHub App, join, node, JIT, workflow, or host secrets may be exposed

If the advisory form is unavailable to you, contact the maintainer through their
GitHub profile. Do not open a public issue, even a minimal one — the existence of
a report against a named component is itself a disclosure.

## Security boundary

Native mode executes trusted private workflow code under a dedicated local OS
account. It is not a sandbox for hostile code and does not make the underlying
computer disposable. Public repositories and external forks are outside the first
release's supported threat model.

The following are release-blocking invariants:

- public or unverifiable scopes are rejected;
- mDNS never authenticates a controller;
- JIT, join, App, node-key, and session secrets are not persisted or logged;
- a slot is not released until cleanup is verified;
- cleanup failure quarantines the node;
- transient external errors remain visible as stale/degraded state;
- unsigned or unverifiable release material is never represented as trusted.

Release artifacts are draft-only until checksums, the GitHub attestation,
CycloneDX SBOMs, real three-OS evidence, and platform signatures are verified.
See [docs/RELEASE.md](docs/RELEASE.md) for the operator checklist.

The boundary is documented in full, and the same documents ship inside every
release archive:

- [Security contract](docs/security/SECURITY_CONTRACT.md)
- [Threat model](docs/security/THREAT_MODEL.md)
- [Native isolation](docs/security/NATIVE_ISOLATION.md)
- [Support matrix](docs/security/SUPPORT_MATRIX.md)

What those documents do not yet have is live three-OS conformance evidence. That
evidence is tracked by `task-014` and must exist before the first public tag.
