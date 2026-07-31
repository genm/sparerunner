// Package verification_test asserts that the repository's own verification
// configuration stays consistent with the code it is supposed to verify.
package verification_test

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

var fuzzTargetPattern = regexp.MustCompile(`(?m)^func (Fuzz[A-Za-z0-9_]*)\(`)

// TestNightlyFuzzMatrixCoversEveryFuzzTarget keeps the nightly matrix and the
// code in agreement. A fuzz target that exists but is not listed still runs its
// seed corpus in required CI, which passes, so nothing else reports that it is
// never actually fuzzed. Silent coverage loss is the failure mode this test
// exists to prevent.
func TestNightlyFuzzMatrixCoversEveryFuzzTarget(t *testing.T) {
	root := repositoryRoot(t)

	declared := declaredFuzzTargets(t, root)
	if len(declared) == 0 {
		t.Fatal("no fuzz targets found, so the matrix guard would pass vacuously")
	}
	scheduled := scheduledFuzzTargets(t, root)

	for target, packagePath := range declared {
		scheduledPackage, ok := scheduled[target]
		if !ok {
			t.Errorf(
				"fuzz target %s in %s is not scheduled in .github/workflows/deep-verification.yml",
				target,
				packagePath,
			)
			continue
		}
		if scheduledPackage != packagePath {
			t.Errorf(
				"fuzz target %s is declared in %s but scheduled against %s",
				target,
				packagePath,
				scheduledPackage,
			)
		}
	}
	for target := range scheduled {
		if _, ok := declared[target]; !ok {
			t.Errorf(
				"the nightly matrix schedules %s, which no package declares",
				target,
			)
		}
	}
}

// declaredFuzzTargets maps every fuzz target in the repository to the package
// path the nightly matrix would have to name.
func declaredFuzzTargets(t *testing.T, root string) map[string]string {
	t.Helper()

	targets := make(map[string]string)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		// test/live/linux creates its own scratch tree inside the repository
		// (TestMain in security_path_test.go) and removes it when that
		// package's tests finish. go test ./... runs packages concurrently, so
		// this walk can observe a directory entry that is already gone by the
		// time it descends into it or reads it. That vanished path was never a
		// stable part of the tree this test needs to reason about, so it is
		// skipped rather than treated as a walk failure. A permission error or
		// anything else still fails the test.
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "node_modules", "output", "bin", "dist", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}
		source, err := os.ReadFile(path)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		matches := fuzzTargetPattern.FindAllSubmatch(source, -1)
		if len(matches) == 0 {
			return nil
		}
		relative, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			return err
		}
		for _, match := range matches {
			targets[string(match[1])] = "./" + filepath.ToSlash(relative)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk repository: %v", err)
	}
	return targets
}

type deepVerificationWorkflow struct {
	Jobs struct {
		Fuzz struct {
			Strategy struct {
				Matrix struct {
					Include []struct {
						Package string `yaml:"package"`
						Target  string `yaml:"target"`
					} `yaml:"include"`
				} `yaml:"matrix"`
			} `yaml:"strategy"`
		} `yaml:"fuzz"`
	} `yaml:"jobs"`
}

func scheduledFuzzTargets(t *testing.T, root string) map[string]string {
	t.Helper()

	path := filepath.Join(root, ".github", "workflows", "deep-verification.yml")
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read nightly workflow: %v", err)
	}
	var workflow deepVerificationWorkflow
	if err := yaml.Unmarshal(source, &workflow); err != nil {
		t.Fatalf("decode nightly workflow: %v", err)
	}
	scheduled := make(map[string]string)
	for _, entry := range workflow.Jobs.Fuzz.Strategy.Matrix.Include {
		if entry.Target == "" || entry.Package == "" {
			t.Fatalf("nightly fuzz matrix entry is incomplete: %+v", entry)
		}
		if previous, duplicate := scheduled[entry.Target]; duplicate {
			t.Fatalf(
				"nightly fuzz matrix schedules %s twice, against %s and %s",
				entry.Target,
				previous,
				entry.Package,
			)
		}
		scheduled[entry.Target] = entry.Package
	}
	return scheduled
}

// repositoryRoot walks up from the test's working directory to the module root
// so the test does not depend on where `go test` was invoked from.
func repositoryRoot(t *testing.T) string {
	t.Helper()

	directory, err := os.Getwd()
	if err != nil {
		t.Fatalf("resolve working directory: %v", err)
	}
	for {
		module := filepath.Join(directory, "go.mod")
		source, err := os.ReadFile(module)
		if err == nil && bytes.Contains(source, []byte("module github.com/genm/sparerunner\n")) {
			return directory
		}
		if err != nil && !errors.Is(err, fs.ErrNotExist) && !errors.Is(err, io.EOF) {
			t.Fatalf("read %s: %v", module, err)
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("module root not found above the test working directory")
		}
		directory = parent
	}
}
