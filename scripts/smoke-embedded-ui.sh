#!/usr/bin/env bash
set -euo pipefail

if [[ $# -gt 2 ]]; then
  printf 'usage: %s [sprun-binary] [result-json]\n' "$0" >&2
  exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
binary="${1:-${repo_root}/bin/sprun}"
result_file="${2:-${repo_root}/output/test-results/embedded-ui-smoke.json}"
runtime_parent="${TMPDIR:-/tmp}"
runtime_parent="${runtime_parent%/}"
if [[ -z "${runtime_parent}" || "${runtime_parent}" != /* || ! -d "${runtime_parent}" ]]; then
  printf 'temporary directory parent is invalid\n' >&2
  exit 1
fi
runtime_dir="$(mktemp -d "${runtime_parent}/sparerunner-embedded-ui.XXXXXX")"
serve_pid=""

cleanup() {
  if [[ -n "${serve_pid}" ]]; then
    kill "${serve_pid}" 2>/dev/null || true
    wait "${serve_pid}" 2>/dev/null || true
  fi

  # Only remove the exact mktemp directory owned by this invocation.
  if [[ -d "${runtime_dir}" ]]; then
    case "${runtime_dir}" in
      "${runtime_parent}"/sparerunner-embedded-ui.*) find "${runtime_dir}" -depth -delete ;;
    esac
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ ! -x "${binary}" ]]; then
  printf 'sprun binary is not executable: %s\n' "${binary}" >&2
  exit 1
fi

umask 077
mkdir -p "${runtime_dir}/empty-path" "$(dirname "${result_file}")"

# The production binary receives an empty PATH. This proves the embedded UI
# does not require Node.js or another host executable after build time.
if env PATH="${runtime_dir}/empty-path" /bin/sh -c 'command -v node' >/dev/null 2>&1; then
  printf 'Node.js unexpectedly resolves in the isolated runtime PATH\n' >&2
  exit 1
fi

env PATH="${runtime_dir}/empty-path" "${binary}" init \
  --state-dir "${runtime_dir}/controller" >"${runtime_dir}/init.out"
chmod 600 "${runtime_dir}/init.out"

env PATH="${runtime_dir}/empty-path" "${binary}" serve \
  --state-dir "${runtime_dir}/controller" \
  --agent-listen 127.0.0.1:0 \
  --admin-listen 127.0.0.1:0 \
  --mdns=false >"${runtime_dir}/serve.out" 2>"${runtime_dir}/serve.err" &
serve_pid="$!"

ui_url=""
ready=false
for _ in {1..100}; do
  ui_url="$(awk '/^Web UI: / {print $3; exit}' "${runtime_dir}/serve.out")"
  if [[ -n "${ui_url}" ]] &&
    curl -fsS -o /dev/null "${ui_url}/" >/dev/null 2>&1; then
    ready=true
    break
  fi
  if ! kill -0 "${serve_pid}" 2>/dev/null; then
    printf 'controller exited before serving the embedded Web UI\n' >&2
    exit 1
  fi
  sleep 0.05
done

if [[ "${ready}" != true ]]; then
  printf 'controller did not publish the embedded Web UI endpoint\n' >&2
  exit 1
fi
case "${ui_url}" in
  http://127.0.0.1:*) ;;
  *)
    printf 'controller published a non-loopback Web UI endpoint\n' >&2
    exit 1
    ;;
esac

curl -fsS -D "${runtime_dir}/index.headers" \
  -o "${runtime_dir}/index.html" "${ui_url}/"
tr -d '\r' <"${runtime_dir}/index.headers" >"${runtime_dir}/index.headers.clean"

grep -Fq 'HTTP/1.1 200 OK' "${runtime_dir}/index.headers.clean"
grep -Fiq 'Cache-Control: no-store' "${runtime_dir}/index.headers.clean"
grep -Fiq "Content-Security-Policy: default-src 'self'" "${runtime_dir}/index.headers.clean"
grep -Fq '<div id="root"></div>' "${runtime_dir}/index.html"

asset_path="$(
  grep -o 'src="/assets/[^"]*\.js"' "${runtime_dir}/index.html" |
    sed 's/^src="//;s/"$//' |
    head -1
)"
case "${asset_path}" in
  /assets/*.js) ;;
  *)
    printf 'embedded HTML did not reference a hashed JavaScript asset\n' >&2
    exit 1
    ;;
esac

curl -fsS -D "${runtime_dir}/asset.headers" \
  -o "${runtime_dir}/asset.js" "${ui_url}${asset_path}"
tr -d '\r' <"${runtime_dir}/asset.headers" >"${runtime_dir}/asset.headers.clean"
grep -Fq 'HTTP/1.1 200 OK' "${runtime_dir}/asset.headers.clean"
grep -Fiq 'Cache-Control: no-store' "${runtime_dir}/asset.headers.clean"

printf '%s\n' \
  '{"status":"passed","runtimePath":"isolated-empty","nodeAvailableToRuntime":false,"indexStatus":200,"assetStatus":200,"contentSecurityPolicy":"present","cacheControl":"no-store"}' \
  >"${result_file}"
printf 'embedded Web UI release-binary smoke passed\n'
