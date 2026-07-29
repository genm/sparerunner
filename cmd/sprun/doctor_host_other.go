//go:build !linux

package main

// Only Linux exposes operator-satisfiable native runner host prerequisites;
// macOS and Windows gate on installed services doctor already reports through
// the agent endpoint check.
func diagnoseNodeHost() []doctorFinding { return nil }
