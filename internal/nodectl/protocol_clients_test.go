package nodectl_test

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"testing"

	"github.com/genm/sparerunner/internal/nodectl"
)

// ProtocolVersion is exact-matched by both ends of the node-local control
// contract, and the Raycast extension is a second implementation of the client
// side in a language the Go compiler never sees. Bumping the Go constant without
// bumping the TypeScript one leaves every check in this repository green while
// every Raycast user gets protocol_mismatch, so the drift is proven here instead.
func TestRaycastClientPinsTheSameProtocolVersion(t *testing.T) {
	source := repositoryFile(t, "extensions", "raycast", "src", "node.ts")

	pattern := regexp.MustCompile(`(?m)^export const PROTOCOL_VERSION = (\d+);$`)
	match := pattern.FindSubmatch(source)
	if match == nil {
		t.Fatalf(
			"extensions/raycast/src/node.ts no longer declares PROTOCOL_VERSION in the "+
				"expected form; update this test and %s together",
			"internal/nodectl/protocol.go",
		)
	}

	declared, err := strconv.Atoi(string(match[1]))
	if err != nil {
		t.Fatalf("parse the Raycast PROTOCOL_VERSION literal %q: %v", match[1], err)
	}
	if declared != nodectl.ProtocolVersion {
		t.Fatalf(
			"Raycast PROTOCOL_VERSION = %d, want %d; bump "+
				"extensions/raycast/src/node.ts alongside internal/nodectl/protocol.go",
			declared,
			nodectl.ProtocolVersion,
		)
	}
}

// repositoryFile resolves a path relative to the repository root from this
// package's own source location, so the test does not depend on the working
// directory `go test` was invoked from.
func repositoryFile(t *testing.T, elements ...string) []byte {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve this test file's own path")
	}
	root := filepath.Join(filepath.Dir(thisFile), "..", "..")

	contents, err := os.ReadFile(filepath.Join(append([]string{root}, elements...)...))
	if err != nil {
		t.Fatalf("read %v from the repository root: %v", elements, err)
	}
	return contents
}
