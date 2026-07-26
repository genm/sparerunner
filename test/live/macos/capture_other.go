//go:build !darwin

package main

import (
	"context"
)

const privateMaterialProbeArgument = "--tewake-live-private-material-probe"

func captureMacOSNode(
	context.Context,
	macOSLiveConfig,
	capturePhase,
) (nodeEvidence, error) {
	return nodeEvidence{}, errMacOSEvidenceInvalid
}

func runPrivateMaterialProbe(args []string) (bool, int) {
	if len(args) > 0 && args[0] == privateMaterialProbeArgument {
		return true, 1
	}
	return false, 0
}

func classifyMacOSLiveError(error) string {
	return "unsupported_platform"
}
