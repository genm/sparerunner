set dotenv-load := false
set shell := ["bash", "-euo", "pipefail", "-c"]

default:
  @just --list

bootstrap:
  mise install
  pnpm --dir web install --frozen-lockfile
  pnpm --dir api/codegen install --frozen-lockfile
  lefthook install

generate-api:
  ./scripts/generate-api.sh

generate-api-check:
  ./scripts/check-generated-api.sh

generate-web:
  pnpm --dir web build

generate-web-check:
  ./scripts/check-generated-web.sh

fmt:
  gofmt -w cmd internal packaging test
  pnpm --dir web format

fmt-check:
  test -z "$(gofmt -l cmd internal packaging test)"
  pnpm --dir web format:check

lint:
  go vet ./...
  pnpm --dir web lint
  actionlint

test:
  mkdir -p output/test-results
  go test -json ./... > output/test-results/go-test.json
  pnpm --dir web test:ci
  pnpm --dir web test:ct

test-enrollment-cli-linux:
  ./scripts/test-enrollment-cli-linux.sh

test-runner-linux:
  ./scripts/test-runner-linux.sh

test-node-availability-linux:
  ./scripts/test-node-availability-linux.sh

test-platform-linux:
  ./scripts/test-platform-linux.sh

validate-release-evidence file='output/release-evidence/twk-014.json':
  ./scripts/validate-release-evidence.sh "{{file}}"

check-release-artifacts dist='dist':
  ./scripts/check-release-artifacts.sh "{{dist}}"

test-management-ui-linux: build
  ./scripts/test-management-ui-linux.sh bin/sparerunner

build:
  mkdir -p bin
  pnpm --dir web build
  go build -trimpath -o bin/sparerunner ./cmd/sparerunner
  go build -trimpath -o bin/sparerunner-agent ./cmd/sparerunner-agent

# The optional desktop tray needs cgo and a native toolchain, so it stays out of
# `just build` and the cross-build matrix.
build-tray:
  mkdir -p bin
  CGO_ENABLED=1 go build -trimpath -o bin/sparerunner-tray ./cmd/sparerunner-tray

build-all:
  ./scripts/cross-build.sh

smoke-embedded-ui-linux: build
  test "$(go env GOOS)" = linux
  ./scripts/smoke-embedded-ui.sh bin/sparerunner

check: generate-api-check generate-web-check fmt-check lint test test-platform-linux build

check-quick: generate-api-check generate-web-check fmt-check
  go test ./...
  pnpm --dir web typecheck

dev:
  process-compose up
