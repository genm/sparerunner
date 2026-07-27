# Release runbook

The release workflow is intentionally tag-driven and creates a GitHub draft. A
draft is not a supported release: publish it only after the TWK-014 live gate
has accepted a real Linux, macOS, and Windows node plus two private GitHub App
installations.

## Local artifact check

GoReleaser builds controller and Agent archives for Linux, macOS, and Windows on
amd64 and arm64. The archive contains both binaries, the Apache-2.0 license,
security contracts, and all platform service/install files.

```bash
# Put the pinned Syft binary on PATH before running the snapshot. The same
# command is used by the tag workflow after its Syft download step.
syft version
goreleaser release --snapshot --clean
# GoReleaser creates the dependency license/NOTICE bundle under licenses/ and
# invokes Syft to generate one CycloneDX JSON document beside every archive.
just check-release-artifacts dist
```

`scripts/check-release-artifacts.sh` verifies all six archives, the SHA-256
`checksums.txt`, the license/NOTICE bundle inside every archive, and a CycloneDX
JSON document beside every archive. It fails if an archive, license bundle, or
SBOM is missing; it never creates a synthetic pass.

## Tagged workflow

Pushing `v*` runs [.github/workflows/release.yml](../.github/workflows/release.yml):

1. GoReleaser builds a draft release and deterministic `checksums.txt`.
2. Syft generates CycloneDX SBOMs for each archive and attaches them to the draft.
3. The GitHub artifact attestation is created for `checksums.txt`.
4. A failed check uploads `dist/` for three days only.

Verify an installed artifact from a trusted checkout with:

```bash
sha256sum --check checksums.txt
gh attestation verify --owner genm checksums.txt
```

On macOS, Apple Developer ID signing/notarization is an external release
prerequisite. On Windows, Authenticode signing is likewise external. Until
those signatures and the real-machine evidence exist, the repository must keep
the release as a draft and document the unsupported combinations.

The workflow uses only GitHub-hosted runners. Tewake nodes must never execute
release or fork-PR jobs.
