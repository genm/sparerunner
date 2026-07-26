package buildinfo

import (
	"strings"
	"testing"
)

func TestStringIncludesBuildMetadata(t *testing.T) {
	got := String()
	for _, want := range []string{"tewake ", "commit=", "built="} {
		if !strings.Contains(got, want) {
			t.Fatalf("String() = %q, want it to contain %q", got, want)
		}
	}
}
