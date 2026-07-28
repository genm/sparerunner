# Security Policy

## Supported versions

SpareRunner is pre-alpha and has no supported release yet. Security fixes target the
default branch until the first tagged support policy is published.

## Reporting a vulnerability

Please use GitHub private vulnerability reporting for `genm/sparerunner` when it is
available. Do not include secrets, exploit payloads, or sensitive runner logs in a
public issue.

Include:

- affected commit or release
- operating system and architecture
- controller/agent topology
- minimal reproduction
- expected and observed trust boundary
- whether GitHub App, join, node, JIT, workflow, or host secrets may be exposed

If private reporting is unavailable, open a minimal public issue asking the
maintainer for a private contact channel without disclosing the vulnerability.

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

The detailed threat model and platform conformance results are tracked by
`spr-014` and must exist before the first public tag.
