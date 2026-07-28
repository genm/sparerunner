package runner

import (
	"context"
	"crypto/sha256"
	"io"
	"runtime"
)

const OfficialRunnerVersion = "2.336.0"

// Platform is intentionally narrower than runtime.GOOS/GOARCH: SpareRunner supports
// only the official packages pinned below, not arbitrary release asset names.
type Platform struct {
	OS   string
	Arch string
}

func CurrentPlatform() Platform { return Platform{OS: runtime.GOOS, Arch: runtime.GOARCH} }

type ArchiveFormat string

const (
	ArchiveTarGz ArchiveFormat = "tar.gz"
	ArchiveZIP   ArchiveFormat = "zip"
)

// Package is a fully pinned official release asset. Checksum is part of the
// identity, so neither callers nor redirects can substitute a different build.
type Package struct {
	Version  string
	Platform Platform
	Asset    string
	Checksum string
	Size     int64
	Format   ArchiveFormat
}

func (p Package) URL() string {
	return "https://github.com/actions/runner/releases/download/v" + p.Version + "/" + p.Asset
}

func (p Package) key() string {
	return p.Version + "-" + p.Platform.OS + "-" + p.Platform.Arch + "-" + p.Checksum
}

// CacheKey returns the fixed cache entry name only for an audited official
// package. Privileged consumers use it to derive a path without accepting an
// Agent-provided asset name or arbitrary relative path.
func (p Package) CacheKey() (string, error) {
	if !p.valid() {
		return "", ErrInvalidRequest
	}
	return p.key(), nil
}

func (p Package) valid() bool {
	if p.Version != OfficialRunnerVersion || len(p.Checksum) != sha256.Size*2 || p.Asset == "" || p.Size <= 0 {
		return false
	}
	for _, c := range p.Checksum {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	expected, err := OfficialPackage(p.Platform)
	return err == nil && expected == p
}

// OfficialPackage returns the six supported v2.336.0 assets. The values are
// copied from the official GitHub release checksum section on 2026-07-26.
func OfficialPackage(platform Platform) (Package, error) {
	os := platform.OS
	if os == "macos" {
		os = "darwin"
	}
	key := os + "/" + platform.Arch
	packages := map[string]Package{
		"windows/amd64": {OfficialRunnerVersion, Platform{"windows", "amd64"}, "actions-runner-win-x64-2.336.0.zip", "d59123a43003e357b0805b5d0f611d0bd2f65ab67d51bd070dd4e7a0f685c162", 103253740, ArchiveZIP},
		"windows/arm64": {OfficialRunnerVersion, Platform{"windows", "arm64"}, "actions-runner-win-arm64-2.336.0.zip", "b3799e9cf754fe4dfcb3d220c9701c924829737ee815dbeb674f8bd076794504", 94445234, ArchiveZIP},
		"darwin/amd64":  {OfficialRunnerVersion, Platform{"darwin", "amd64"}, "actions-runner-osx-x64-2.336.0.tar.gz", "f79c43232761ca495fc18df550bb2865aa99984b37c173c0aa1f8c09d0d548fe", 131517013, ArchiveTarGz},
		"darwin/arm64":  {OfficialRunnerVersion, Platform{"darwin", "arm64"}, "actions-runner-osx-arm64-2.336.0.tar.gz", "8e8839c49b7060b6b2154f4931f815df330c27f167d53ef2239ee3dfce28b079", 127389671, ArchiveTarGz},
		"linux/amd64":   {OfficialRunnerVersion, Platform{"linux", "amd64"}, "actions-runner-linux-x64-2.336.0.tar.gz", "04cf0be1aff4c3ec3554466c39124ca250e3effd8873bb7e8d68535aa9505d5d", 226035903, ArchiveTarGz},
		"linux/arm64":   {OfficialRunnerVersion, Platform{"linux", "arm64"}, "actions-runner-linux-arm64-2.336.0.tar.gz", "58b758e420b87093fbd4bfddd368074960053e2f1388f01848c82624b90f27d1", 138824064, ArchiveTarGz},
	}
	p, ok := packages[key]
	if !ok {
		return Package{}, ErrUnsupportedPlatform
	}
	return p, nil
}

// Fetcher is injectable so tests never make a network request. It must return
// only the package body; production redirect and credential policy is owned by
// HTTPFetcher rather than spread through callers.
type Fetcher interface {
	Fetch(context.Context, Package) (io.ReadCloser, error)
}
