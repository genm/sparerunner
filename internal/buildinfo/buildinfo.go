package buildinfo

import "fmt"

var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

// Version and Commit expose the same linker-injected values String formats, so
// provider clients can report an exact build identity instead of a hardcoded
// placeholder. Both keep their non-empty defaults on an untagged build.
func Version() string { return version }

// Commit returns the linker-injected commit SHA, or "unknown".
func Commit() string { return commit }

// String deliberately omits host and user information so diagnostics remain portable.
func String() string {
	return fmt.Sprintf("sparerunner %s (commit=%s, built=%s)", version, commit, date)
}
