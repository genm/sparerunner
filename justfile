set dotenv-load := false
set shell := ["bash", "-euo", "pipefail", "-c"]

default:
  @just --list

bootstrap:
  mise install
  pnpm --dir web install --frozen-lockfile
  lefthook install

fmt:
  gofmt -w cmd internal spec/tewake
  pnpm --dir web format

fmt-check:
  test -z "$(gofmt -l cmd internal spec/tewake)"
  pnpm --dir web format:check

lint:
  go vet ./...
  pnpm --dir web lint
  actionlint

test:
  mkdir -p output/test-results
  go test -json ./... > output/test-results/go-test.json
  pnpm --dir web test:ci

test-enrollment-cli-linux:
  ./scripts/test-enrollment-cli-linux.sh

test-runner-linux:
  ./scripts/test-runner-linux.sh

build:
  mkdir -p bin
  go build -trimpath -o bin/tewake ./cmd/tewake
  go build -trimpath -o bin/tewake-agent ./cmd/tewake-agent
  pnpm --dir web build

build-all:
  ./scripts/cross-build.sh

check: fmt-check lint test build

check-quick: fmt-check
  go test ./...
  pnpm --dir web typecheck

dev:
  process-compose up
