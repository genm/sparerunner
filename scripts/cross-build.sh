#!/usr/bin/env bash
# Cross-compile every supported controller and agent target from one reproducible entrypoint.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_dir="${1:-bin}"

cd "$repo_root"
mkdir -p "$output_dir"

for os in linux darwin windows; do
  extension=""
  if [[ "$os" == "windows" ]]; then
    extension=".exe"
  fi

  for arch in amd64 arm64; do
    for command in tewake tewake-agent; do
      target="$output_dir/$command-$os-$arch$extension"
      printf 'Building %s/%s: %s\n' "$os" "$arch" "$command"
      CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" \
        go build -trimpath -o "$target" "./cmd/$command"
    done
  done
done
