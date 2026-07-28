//go:build !windows

package main

import (
	"context"

	"github.com/genm/sparerunner/internal/app"
)

func platformJoinAgent(
	ctx context.Context,
	options app.JoinOptions,
) (string, error) {
	return app.JoinAgent(ctx, options)
}
