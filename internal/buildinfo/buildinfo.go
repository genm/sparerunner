package buildinfo

import "fmt"

var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

// String deliberately omits host and user information so diagnostics remain portable.
func String() string {
	return fmt.Sprintf("tewake %s (commit=%s, built=%s)", version, commit, date)
}
