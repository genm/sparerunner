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
	GetRunnerByName(context.Context, string) (*RunnerReference, error)
	RemoveRunner(context.Context, RunnerReference) error
}

var _ RunnerLifecycle = (*Client)(nil)

// GetRunnerByName returns nil only when GitHub authoritatively reports that the
// deterministic Tewake runner name is absent.
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
	result, err := contain(func() (*scaleset.RunnerReference, error) {
		return get(ctx, name)
	})
	if err != nil {
		return nil, fmt.Errorf("getting GitHub runner by name: %w", err)
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
	_, err := contain(func() (struct{}, error) {
		return struct{}{}, remove(ctx, int64(runner.ID))
	})
	if err != nil {
		return fmt.Errorf("removing GitHub runner: %w", err)
	}
	return nil
}

func validateRunnerReference(runner RunnerReference) error {
	if runner.ID <= 0 || !isGitHubPathPart(runner.Name) || runner.ScaleSetID <= 0 {
		return ErrInvalidPreviewResponse
	}
	return nil
}
