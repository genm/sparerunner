package github

import (
	"context"
	"errors"
	"fmt"

	"github.com/actions/scaleset"
)

// RunnerLifecycle is the non-secret registration surface needed to reconcile
// ambiguous JIT generation or Agent delivery. JITConfig remains an opaque,
// in-memory-only value.
type RunnerLifecycle interface {
	GenerateJITConfig(context.Context, JITRequest) (JITConfig, error)
	QueryRunner(context.Context, RunnerQuery) (*RunnerReference, error)
	RemoveRunner(context.Context, RunnerReference) error
}

var _ RunnerLifecycle = (*Client)(nil)

// RunnerQuery binds a provider read to one durable JIT attempt. GitHub runner
// records do not expose RunnerRequestID, so callers still pass it to prevent a
// name-only lookup from becoming an unscoped reconciliation API.
type RunnerQuery struct {
	ScaleSetID      ScaleSetID
	RunnerRequestID int64
	Name            string
	ExpectedID      int
}

func (query RunnerQuery) validate() error {
	if query.ScaleSetID <= 0 ||
		query.RunnerRequestID <= 0 ||
		!isGitHubPathPart(query.Name) ||
		query.ExpectedID < 0 {
		return ErrInvalidPreviewResponse
	}
	return nil
}

// QueryRunner verifies known registrations by both ID and deterministic name.
// A pre-generation ambiguity has no ID; an exact name miss is one provider
// observation only. The caller must persist it and require a later observation
// before treating absence as authoritative because the preview API does not
// document read-after-write consistency for GenerateJIT.
func (c *Client) QueryRunner(
	ctx context.Context,
	query RunnerQuery,
) (*RunnerReference, error) {
	if err := query.validate(); err != nil {
		return nil, err
	}
	byName, err := c.GetRunnerByName(ctx, query.Name)
	if err != nil {
		return nil, err
	}
	if query.ExpectedID == 0 {
		if byName == nil {
			return nil, nil
		}
		if byName.ScaleSetID != query.ScaleSetID {
			return nil, ErrInvalidPreviewResponse
		}
		return byName, nil
	}

	if c == nil {
		return nil, ErrInvalidPreviewResponse
	}
	get := c.getRunner
	if get == nil {
		if c.client == nil {
			return nil, ErrInvalidPreviewResponse
		}
		get = c.client.GetRunner
	}
	operationContext, statusRecorder := withProviderStatusRecorder(ctx)
	result, idErr := contain(func() (*scaleset.RunnerReference, error) {
		return get(operationContext, query.ExpectedID)
	})
	idAbsent := errors.Is(idErr, scaleset.RunnerNotFoundError)
	if idErr != nil && !idAbsent {
		return nil, fmt.Errorf(
			"getting GitHub runner by ID: %w",
			providerHTTPStatusError(statusRecorder, idErr),
		)
	}
	if idAbsent {
		if byName == nil {
			return nil, nil
		}
		return nil, ErrInvalidPreviewResponse
	}
	if result == nil {
		return nil, ErrInvalidPreviewResponse
	}
	byID := RunnerReference{
		ID:         result.ID,
		Name:       result.Name,
		ScaleSetID: ScaleSetID(result.RunnerScaleSetID),
	}
	if err := validateRunnerReference(byID); err != nil ||
		byID.ID != query.ExpectedID ||
		byID.Name != query.Name ||
		byID.ScaleSetID != query.ScaleSetID ||
		byName == nil ||
		*byName != byID {
		return nil, ErrInvalidPreviewResponse
	}
	return &byID, nil
}

// GetRunnerByName returns nil only when GitHub authoritatively reports that the
// deterministic SpareRunner runner name is absent.
func (c *Client) GetRunnerByName(ctx context.Context, name string) (*RunnerReference, error) {
	if !isGitHubPathPart(name) {
		return nil, errors.New("GitHub runner name is invalid")
	}
	if c == nil {
		return nil, ErrInvalidPreviewResponse
	}
	get := c.getRunnerByName
	if get == nil {
		if c.client == nil {
			return nil, ErrInvalidPreviewResponse
		}
		get = c.client.GetRunnerByName
	}
	operationContext, statusRecorder := withProviderStatusRecorder(ctx)
	result, err := contain(func() (*scaleset.RunnerReference, error) {
		return get(operationContext, name)
	})
	if err != nil {
		return nil, fmt.Errorf(
			"getting GitHub runner by name: %w",
			providerHTTPStatusError(statusRecorder, err),
		)
	}
	if result == nil {
		return nil, nil
	}
	converted := RunnerReference{
		ID:         result.ID,
		Name:       result.Name,
		ScaleSetID: ScaleSetID(result.RunnerScaleSetID),
	}
	if err := validateRunnerReference(converted); err != nil || converted.Name != name {
		return nil, ErrInvalidPreviewResponse
	}
	return &converted, nil
}

// RemoveRunner deletes exactly the validated runner registration observed
// during reconciliation. The caller must re-read by name before concluding
// that an ambiguous registration is absent.
func (c *Client) RemoveRunner(ctx context.Context, runner RunnerReference) error {
	if err := validateRunnerReference(runner); err != nil {
		return err
	}
	if c == nil {
		return ErrInvalidPreviewResponse
	}
	remove := c.removeRunner
	if remove == nil {
		if c.client == nil {
			return ErrInvalidPreviewResponse
		}
		remove = c.client.RemoveRunner
	}
	operationContext, statusRecorder := withProviderStatusRecorder(ctx)
	_, err := contain(func() (struct{}, error) {
		return struct{}{}, remove(operationContext, int64(runner.ID))
	})
	if err != nil {
		return fmt.Errorf(
			"removing GitHub runner: %w",
			providerHTTPStatusError(statusRecorder, err),
		)
	}
	return nil
}

func validateRunnerReference(runner RunnerReference) error {
	if runner.ID <= 0 || !isGitHubPathPart(runner.Name) || runner.ScaleSetID <= 0 {
		return ErrInvalidPreviewResponse
	}
	return nil
}
