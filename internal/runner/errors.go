// Package runner owns the local, native lifecycle of an official GitHub Actions
// runner. It deliberately exposes classification errors only: runner arguments,
// filesystem paths, archive member names, and process output may contain secrets.
package runner

import "errors"

var (
	ErrInvalidRequest             = errors.New("runner: invalid request")
	ErrUnsupportedPlatform        = errors.New("runner: unsupported platform")
	ErrPackageIntegrity           = errors.New("runner: package integrity check failed")
	ErrDownloadPolicy             = errors.New("runner: download policy rejected response")
	ErrUnsafeArchive              = errors.New("runner: unsafe archive")
	ErrExecutionConflict          = errors.New("runner: execution specification conflict")
	ErrExecutionNotFound          = errors.New("runner: execution not found")
	ErrReconciliationRequired     = errors.New("runner: reconciliation required")
	ErrQuarantined                = errors.New("runner: node is quarantined")
	ErrCleanupFailed              = errors.New("runner: cleanup verification failed")
	ErrWorkspaceChanged           = errors.New("runner: workspace identity changed")
	ErrStrongOwnershipUnavailable = errors.New("runner: strong descendant ownership unavailable")
	ErrJournal                    = errors.New("runner: journal operation failed")
)
