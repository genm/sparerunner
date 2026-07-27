//go:build !windows

package main

import (
	"context"

	"github.com/genm/tewake/internal/app"
)

func platformJoinAgent(
	ctx context.Context,
	options app.JoinOptions,
) (string, error) {
	return app.JoinAgent(ctx, options)
}
