#!/usr/bin/env bash
set -euo pipefail

dist_dir="${1:-dist}"
checksums="$dist_dir/checksums.txt"

if [[ ! -d "$dist_dir" || ! -f "$checksums" ]]; then
  printf 'release artifact directory or checksums.txt is missing: %s\n' "$dist_dir" >&2
  exit 1
fi

if ! grep -Eq '^[0-9a-fA-F]{64}[[:space:]]{2}.+\.(tar\.gz|zip)$' "$checksums"; then
  printf 'checksums.txt has no archive entries\n' >&2
  exit 1
fi

(cd "$dist_dir" && sha256sum --check checksums.txt)

for pattern in \
  'tewake_*_linux_amd64.tar.gz' \
  'tewake_*_linux_arm64.tar.gz' \
  'tewake_*_darwin_amd64.tar.gz' \
  'tewake_*_darwin_arm64.tar.gz' \
  'tewake_*_windows_amd64.zip' \
  'tewake_*_windows_arm64.zip'; do
  compgen -G "$dist_dir/$pattern" >/dev/null || {
    printf 'required archive is missing: %s\n' "$pattern" >&2
    exit 1
  }
done

archive_count=0
for archive in "$dist_dir"/*.tar.gz "$dist_dir"/*.zip; do
  [[ -f "$archive" ]] || continue
  archive_count=$((archive_count + 1))
  sbom="$archive.cyclonedx.json"
  [[ -s "$sbom" ]] || {
    printf 'SBOM is missing for %s\n' "$archive" >&2
    exit 1
  }
  if ! grep -q '"bomFormat"[[:space:]]*:[[:space:]]*"CycloneDX"' "$sbom"; then
    printf 'SBOM is not CycloneDX JSON: %s\n' "$sbom" >&2
    exit 1
  fi
  if [[ "$archive" == *.tar.gz ]]; then
    tar -tzf "$archive" | grep -Fx 'licenses/NOTICE' >/dev/null || {
      printf 'NOTICE bundle is missing from %s\n' "$archive" >&2
      exit 1
    }
    tar -tzf "$archive" | grep -Fx 'licenses/THIRD_PARTY_LICENSES.csv' >/dev/null || {
      printf 'third-party license report is missing from %s\n' "$archive" >&2
      exit 1
    }
  else
    unzip -Z1 "$archive" | grep -Fx 'licenses/NOTICE' >/dev/null || {
      printf 'NOTICE bundle is missing from %s\n' "$archive" >&2
      exit 1
    }
    unzip -Z1 "$archive" | grep -Fx 'licenses/THIRD_PARTY_LICENSES.csv' >/dev/null || {
      printf 'third-party license report is missing from %s\n' "$archive" >&2
      exit 1
    }
  fi
done

(( archive_count >= 6 )) || {
  printf 'expected six platform archives, found %d\n' "$archive_count" >&2
  exit 1
}

printf 'Release artifacts verified: %d archives, checksums, and SBOMs\n' "$archive_count"
