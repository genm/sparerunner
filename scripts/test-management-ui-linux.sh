#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  printf 'usage: %s <sprun-binary>\n' "$0" >&2
  exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
binary="$1"
if [[ "${binary}" != /* ]]; then
  binary="${repo_root}/${binary}"
fi
runtime_parent="${TMPDIR:-/tmp}"
runtime_parent="${runtime_parent%/}"
if [[ -z "${runtime_parent}" || "${runtime_parent}" != /* || ! -d "${runtime_parent}" ]]; then
  printf 'temporary directory parent is invalid\n' >&2
  exit 1
fi
sensitive_output="$(mktemp -d "${runtime_parent}/sparerunner-management-e2e-output.XXXXXX")"

cleanup() {
  # Playwright can create an automatic DOM context even when trace, screenshot,
  # and video are disabled. Never retain that credential-bearing directory.
  if [[ -d "${sensitive_output}" ]]; then
    case "${sensitive_output}" in
      "${runtime_parent}"/sparerunner-management-e2e-output.*)
        find "${sensitive_output}" -depth -delete
        ;;
    esac
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ "$(uname -s)" != Linux ]]; then
  printf 'management UI E2E requires Linux\n' >&2
  exit 1
fi
if [[ ! -x "${binary}" ]]; then
  printf 'sprun binary is not executable\n' >&2
  exit 1
fi
playwright="${repo_root}/web/node_modules/.bin/playwright"
if [[ ! -x "${playwright}" ]]; then
  printf 'Playwright is not installed in web/node_modules\n' >&2
  exit 1
fi

mkdir -p "${repo_root}/output/test-results"
cd "${repo_root}/web"
SPARERUNNER_E2E_BINARY="${binary}" \
  SPARERUNNER_E2E_SENSITIVE_OUTPUT_DIR="${sensitive_output}" \
  "${playwright}" test -c playwright-e2e.config.ts
