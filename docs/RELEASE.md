# Release runbook

The release workflow is intentionally tag-driven and creates a GitHub draft. A
draft is not a supported release: publish it only after the task-014 live gate
has accepted a real Linux, macOS, and Windows node plus two private GitHub App
installations.

Commit messages and pull request titles merged under the earlier `twk-NNN` and
`spr-NNN` prefixes are not rewritten. `spec/sparerunner/tasks.yaml` holds the
single mapping; do not restate it here.

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

1. The workflow refuses to proceed unless the tagged commit is already an
   ancestor of `main`. A draft release page stays unpublished, but the Sigstore
   attestation in step 4 is minted immediately and is permanently, publicly bound
   to this repository — so a tag pushed from any other branch would produce
   artifacts that pass the verification documented below.
2. GoReleaser builds a draft release and deterministic `checksums.txt`.
3. Syft generates CycloneDX SBOMs for each archive and attaches them to the draft.
4. A GitHub artifact attestation is created *from* `checksums.txt` *for* each
   archive listed in it, and the workflow then verifies that attestation before
   the job can succeed.
5. A failed check uploads `dist/` for three days only.

Before pushing the tag, the release owner must validate the separately captured
task-014 manifest from the trusted Linux/macOS/Windows sandbox:

```bash
just validate-release-evidence output/release-evidence/task-014.json
```

This command must return zero on the exact trusted commit that will be tagged.
The checked-in validator rejects a missing, partial, stale, or secret-bearing
manifest; a green unit or hosted CI run is not a substitute for this live gate.

Verify an installed artifact from a trusted checkout with:

```bash
sha256sum --check checksums.txt
gh attestation verify \
  --repo genm/sparerunner \
  --signer-workflow genm/sparerunner/.github/workflows/release.yml \
  sparerunner_<version>_linux_amd64.tar.gz
```

The subject is the **archive**, not `checksums.txt`. `actions/attest` is given
`checksums.txt` through its `subject-checksums` input, which means the attested
subjects are the files listed inside it; `checksums.txt` itself is not a subject
and verifying it finds nothing.

`--repo` and `--signer-workflow` are both load-bearing. `--owner genm` would
accept an attestation minted by any workflow in any repository that account owns,
which is a much weaker claim than "this artifact came out of this release
pipeline".

## Reproduce a released binary

The release footer tells consumers to verify the checksums, which is only worth
anything if a third party can independently produce the same bytes. Both builds
in [.goreleaser.yaml](../.goreleaser.yaml) pass `-trimpath`, pin `mod_timestamp`
to the commit timestamp, and inject `.CommitDate` rather than wall-clock build
time, so the same tag produces the same archive on any host with the same Go
toolchain:

```bash
git checkout vX.Y.Z
goreleaser build --clean --single-target
sha256sum dist/*/sprun
```

Compare that digest against the corresponding entry in the published
`checksums.txt`. A mismatch means either the toolchain differs from the one the
release job used — check `go.mod` and the pinned GoReleaser version in the
workflow — or the artifact is not what this tag builds.

On macOS, Apple Developer ID signing/notarization is an external release
prerequisite. On Windows, Authenticode signing is likewise external. Until
those signatures and the real-machine evidence exist, the repository must keep
the release as a draft and document the unsupported combinations.

The workflow uses only GitHub-hosted runners. SpareRunner nodes must never execute
release or fork-PR jobs.
