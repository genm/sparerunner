#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
embedded_assets="${repo_root}/internal/webui/assets"
runtime_parent="${TMPDIR:-/tmp}"
runtime_parent="${runtime_parent%/}"
if [[ -z "${runtime_parent}" || "${runtime_parent}" != /* || ! -d "${runtime_parent}" ]]; then
  echo "temporary directory parent is invalid" >&2
  exit 1
fi
scratch_dir="$(mktemp -d "${runtime_parent}/sparerunner-generated-web.XXXXXX")"

cleanup() {
  case "${scratch_dir}" in
    "${runtime_parent}"/sparerunner-generated-web.*)
      if [[ -d "${scratch_dir}" ]]; then
        find "${scratch_dir}" -depth -delete
      fi
      ;;
    *)
      echo "refusing to remove unexpected scratch directory: ${scratch_dir}" >&2
      return 1
      ;;
  esac
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ ! -f "${embedded_assets}/index.html" ]]; then
  echo "embedded Web UI is missing; run 'just generate-web'" >&2
  exit 1
fi

pnpm --dir "${repo_root}/web" exec vite build \
  --outDir "${scratch_dir}/assets" \
  --emptyOutDir

if ! diff -ru "${embedded_assets}" "${scratch_dir}/assets"; then
  echo "embedded Web UI is stale; run 'just generate-web'" >&2
  exit 1
fi
