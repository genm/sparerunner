package main

import (
	"context"
	"errors"

	"github.com/genm/sparerunner/internal/runner"
)

// nativeRunnerLifecycle is the complete restart-safe runtime authority expected
// from every supported OS adapter. Embedding it preserves Recover while adding
// platform credential-store readiness as an admission prerequisite.
type nativeRunnerLifecycle interface {
	Ready(context.Context) error
	Recover(context.Context, string) (runner.Snapshot, error)
	EnsurePrepared(context.Context, runner.Preparation) (runner.Snapshot, error)
	EnsureRunning(context.Context, runner.Start) (runner.Snapshot, error)
	Inspect(context.Context, string) (runner.Snapshot, error)
	Wait(context.Context, string) (runner.Snapshot, error)
	Destroy(context.Context, string) (runner.Snapshot, error)
}

type credentialBoundRunnerLifecycle struct {
	nativeRunnerLifecycle
	credentialReady func(context.Context) error
}

func bindNativeRunnerCredential(
	lifecycle nativeRunnerLifecycle,
	credentialReady func(context.Context) error,
) (nativeRunnerLifecycle, error) {
	if lifecycle == nil || credentialReady == nil {
		return nil, errors.New("native runner admission authority is incomplete")
	}
	return &credentialBoundRunnerLifecycle{
		nativeRunnerLifecycle: lifecycle,
		credentialReady:       credentialReady,
	}, nil
}

func (lifecycle *credentialBoundRunnerLifecycle) Ready(ctx context.Context) error {
	if lifecycle == nil || lifecycle.nativeRunnerLifecycle == nil ||
		lifecycle.credentialReady == nil || ctx == nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	if err := lifecycle.nativeRunnerLifecycle.Ready(ctx); err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	if err := lifecycle.credentialReady(ctx); err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	return nil
}
