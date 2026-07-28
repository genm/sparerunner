set dotenv-load := false
set shell := ["bash", "-euo", "pipefail", "-c"]

default:
  @just --list

# Everything `just check` needs. Playwright's browser download is included
# because component tests are part of the gate; `--with-deps` is deliberately
# omitted, since it needs sudo apt on Linux and does not exist on macOS.
[doc("Install every tool and dependency the checks need")]
bootstrap:
  mise install
  pnpm --dir web install --frozen-lockfile
  pnpm --dir api/codegen install --frozen-lockfile
  npm --prefix extensions/raycast ci --ignore-scripts
  pnpm --dir web exec playwright install chromium
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

lint: lint-go lint-web lint-raycast lint-workflows lint-shell lint-secrets check-npm-policy

# golangci-lint's package list excludes cmd/sparerunner-tray for the same
# reason `just build-tray` and cross-build.sh keep it out of their default
# scope: its systray dependency needs cgo and each OS's own native toolchain,
# which cross-compiling GOOS cannot provide. Matches the CI go-lint job's list,
# so setting GOOS locally reproduces that job exactly. `go vet` is left
# unscoped: it is only ever run natively (both here and in CI's go-quality
# job), where tray's cgo dependency resolves fine.
lint-go:
  go vet ./...
  golangci-lint run ./cmd/sprun/... ./cmd/sparerunner-agent/... ./internal/... \
    ./packaging/... ./test/... ./spec/...

lint-web:
  pnpm --dir web lint

# `ray lint` and `ray build` need the Raycast app on macOS. Typechecking does not,
# and it is what catches the extension drifting from the node control contract it
# speaks. Run `just bootstrap` first; its dependencies come from npm, matching the
# Raycast publishing pipeline.
[doc("Typecheck the Raycast extension")]
lint-raycast:
  npm --prefix extensions/raycast run typecheck

# actionlint checks that a workflow is well formed; zizmor checks what a
# well-formed workflow is permitted to do. CI runs zizmor through its own
# Action, pinned to the version below, so the two cannot drift.
lint-workflows:
  actionlint
  zizmor .

lint-shell:
  shellcheck --severity=style scripts/*.sh packaging/macos/*.sh \
    test/live/linux/run.sh test/live/macos/run.sh

# Scans the whole history, not just the working tree, so it catches a secret that
# was committed and then removed. Allowlisted fixtures are justified one by one
# in .gitleaks.toml.
[doc("Scan the repository history for committed secrets")]
lint-secrets:
  gitleaks git --redact --exit-code 1 --config .gitleaks.toml

# pnpm reads the .npmrc of the directory it installs in, so a policy written only
# at the repository root is silently off for every `pnpm --dir` install.
[doc("Prove the npm supply-chain policy is in effect for every pnpm project")]
check-npm-policy:
  ./scripts/check-npm-policy.sh

# This is what CI blocks on. Needs network access to the Go vulnerability
# database.
[doc("Report Go vulnerabilities reachable from this module's own code")]
vulncheck:
  go run golang.org/x/vuln/cmd/govulncheck@v1.1.4 ./...

test:
  mkdir -p output/test-results
  just test-go
  pnpm --dir web test:ci
  pnpm --dir web test:ct

# gotestsum prints a readable per-package result and expands the failing test,
# while writing the machine-readable evidence AGENTS.md requires. Plain
# `go test -json > file` leaves a failed run showing nothing but the exit code.
[doc("Run the Go tests, printing failures and writing machine-readable evidence")]
test-go:
  mkdir -p output/test-results
  go tool gotestsum --format pkgname-and-test-fails --format-hide-empty-pkg \
    --junitfile output/test-results/go-junit.xml \
    --jsonfile output/test-results/go-test.json -- ./...

# Scoped deliberately, and the scope is measured. These five packages own the
# controller's concurrency and cost about 45s together under the detector.
# internal/app (471s) and internal/store (240s even without -race) are excluded:
# a whole-module race run blows Go's 10-minute default per-package timeout.
# Bringing those two under the detector is worth its own task.
[doc("Run the race detector over the concurrency-owning packages, as CI does")]
test-race:
  mkdir -p output/test-results
  go tool gotestsum --format pkgname-and-test-fails --format-hide-empty-pkg \
    --junitfile output/test-results/go-race-junit.xml \
    --jsonfile output/test-results/go-race.json -- -race -timeout=15m \
    ./internal/reconcile/... ./internal/scheduler/... ./internal/transport/... \
    ./internal/nodectl/... ./internal/github/...

# `just test` already runs every fuzz target's committed seed corpus. This
# generates new inputs, which is the part that needs time rather than a gate.
# The nightly deep-verification workflow runs the same targets one job each with
# a much larger budget, and a test in test/verification fails if its matrix and
# this script ever disagree about which targets exist.
[doc("Fuzz every target for a short local budget")]
fuzz time='30s':
  ./scripts/fuzz.sh "{{time}}"

test-enrollment-cli-linux:
  ./scripts/test-enrollment-cli-linux.sh

test-runner-linux:
  ./scripts/test-runner-linux.sh

test-node-availability-linux:
  ./scripts/test-node-availability-linux.sh

test-platform-linux:
  ./scripts/test-platform-linux.sh

validate-release-evidence file='output/release-evidence/task-014.json':
  ./scripts/validate-release-evidence.sh "{{file}}"

check-release-artifacts dist='dist':
  ./scripts/check-release-artifacts.sh "{{dist}}"

test-management-ui-linux: build
  ./scripts/test-management-ui-linux.sh bin/sprun

build:
  mkdir -p bin
  pnpm --dir web build
  go build -trimpath -o bin/sprun ./cmd/sprun
  go build -trimpath -o bin/sparerunner-agent ./cmd/sparerunner-agent

# It needs cgo and a native toolchain, so it stays out of `just build` and the
# cross-build matrix.
[doc("Build the optional desktop tray")]
build-tray:
  mkdir -p bin
  CGO_ENABLED=1 go build -trimpath -o bin/sparerunner-tray ./cmd/sparerunner-tray

build-all:
  ./scripts/cross-build.sh

smoke-embedded-ui-linux: build
  test "$(go env GOOS)" = linux
  ./scripts/smoke-embedded-ui.sh bin/sprun

check: generate-api-check generate-web-check fmt-check lint test test-platform-linux build

check-quick: generate-api-check generate-web-check fmt-check
  go test ./...
  pnpm --dir web typecheck

# What the pre-push hook runs. Everything it leaves out — the privileged Linux
# boundary test, which needs a Docker daemon, and Playwright component tests,
# which need a browser download — is still blocking in required CI, which is free
# and unlimited on this public repository. The point is that a contributor with a
# correct `just bootstrap` can always push; bypassing the hook is never the fix.
[doc("Everything the pre-push hook enforces")]
check-push: generate-api-check generate-web-check fmt-check lint build
  go test ./...
  pnpm --dir web typecheck
  pnpm --dir web test:ci

# Slower than `just check` because of the race detector and the vulnerability
# database lookup.
[doc("Prove locally everything the required CI gate proves")]
check-ci: generate-api-check generate-web-check fmt-check lint vulncheck test-race test-platform-linux build
  pnpm --dir web test:ci
  pnpm --dir web test:ct

dev:
  process-compose up
