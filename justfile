set dotenv-load := false
set shell := ["bash", "-euo", "pipefail", "-c"]

default:
  @just --list

bootstrap:
  mise install
  pnpm --dir web install --frozen-lockfile
  lefthook install

fmt:
  gofmt -w cmd internal
  pnpm --dir web format

fmt-check:
  test -z "$(gofmt -l cmd internal)"
  pnpm --dir web format:check

lint:
  go vet ./...
  pnpm --dir web lint
  actionlint

test:
  mkdir -p output/test-results
  go test -json ./... > output/test-results/go-test.json
  pnpm --dir web test:ci

build:
  mkdir -p bin
  go build -trimpath -o bin/tewake ./cmd/tewake
  go build -trimpath -o bin/tewake-agent ./cmd/tewake-agent
  pnpm --dir web build

build-all:
  mkdir -p bin
  for os in linux darwin windows; do \
    extension=""; \
    if [ "$os" = windows ]; then extension=".exe"; fi; \
    for arch in amd64 arm64; do \
      CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" \
        go build -trimpath -o "bin/tewake-$os-$arch$extension" ./cmd/tewake; \
      CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" \
        go build -trimpath -o "bin/tewake-agent-$os-$arch$extension" ./cmd/tewake-agent; \
    done; \
  done

check: fmt-check lint test build

check-quick: fmt-check
  go test ./...
  pnpm --dir web typecheck

dev:
  process-compose up
