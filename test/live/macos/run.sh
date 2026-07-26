#!/bin/bash
set -euo pipefail

if [[ "$#" -lt 3 ]]; then
  echo "usage: run.sh /absolute/harness validate-config /absolute/config" >&2
  echo "       run.sh /absolute/harness capture /absolute/config PHASE" >&2
  echo "       run.sh /absolute/harness validate /absolute/config SCENARIO" >&2
  exit 64
fi

readonly harness="$1"
readonly operation="$2"
readonly config="$3"

if [[ "$harness" != /* || "$config" != /* || ! -x "$harness" ]]; then
  echo "harness and config must be explicit absolute paths" >&2
  exit 64
fi

case "$operation" in
  validate-config)
    if [[ "$#" -ne 3 ]]; then
      exit 64
    fi
    exec "$harness" validate-config --config "$config"
    ;;
  capture)
    if [[ "$#" -ne 4 ]]; then
      exit 64
    fi
    exec "$harness" capture --config "$config" --phase "$4"
    ;;
  validate)
    if [[ "$#" -ne 4 ]]; then
      exit 64
    fi
    exec "$harness" validate --config "$config" --scenario "$4"
    ;;
  *)
    echo "unknown operation" >&2
    exit 64
    ;;
esac
