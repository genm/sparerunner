//go:build unix

package nodectl_test

import "os"

func currentUID() int { return os.Geteuid() }
