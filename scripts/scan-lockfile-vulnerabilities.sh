#!/usr/bin/env bash
# Report known vulnerabilities in every dependency lock file in the repository.
#
# `just vulncheck` covers Go, and only advisories that reach code this module
# calls. Nothing covered the JavaScript side: the Web console, the API codegen
# toolchain, and the Raycast extension. Dependency review sees a lock file only
# when a pull request changes it, so an advisory published against a dependency
# nobody touched is invisible to both.
#
# OSV-Scanner is installed from the Go module proxy and verified against the
# checksum database, the same way `just vulncheck` installs govulncheck, so this
# adds no third-party Action. The pinned version lives here so a local run and
# the nightly workflow scan with the same tool.
set -euo pipefail

OSV_SCANNER_VERSION="v2.4.0"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

go run "github.com/google/osv-scanner/v2/cmd/osv-scanner@${OSV_SCANNER_VERSION}" \
  scan source --recursive .
