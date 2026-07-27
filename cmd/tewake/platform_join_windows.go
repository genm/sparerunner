//go:build windows

package main

import (
	"context"

	"github.com/genm/tewake/internal/app"
	"github.com/genm/tewake/internal/enroll"
	platformwindows "github.com/genm/tewake/internal/platform/windows"
)

func platformJoinAgent(
	ctx context.Context,
	options app.JoinOptions,
) (string, error) {
	// Preserve the cross-platform CLI contract and reject malformed
	// capabilities before probing the privileged local service.
	if _, err := enroll.DecodeJoinCode(options.JoinCode); err != nil {
		return "", err
	}
	return platformwindows.SubmitBootstrapJoin(ctx, platformwindows.BootstrapJoinOptions{
		JoinCode:          options.JoinCode,
		Controller:        options.Controller,
		DiscoveryTimeout:  options.DiscoveryTimeout,
		ConnectionTimeout: options.ConnectionTimeout,
	})
}
