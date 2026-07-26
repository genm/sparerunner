package github

import (
	"context"
	"errors"
	"testing"

	"github.com/actions/scaleset"
)

func TestRunnerLifecycleValidatesObservedIdentityBeforeRemoval(t *testing.T) {
	removed := int64(0)
	client := &Client{
		getRunnerByName: func(context.Context, string) (*scaleset.RunnerReference, error) {
			return &scaleset.RunnerReference{ID: 17, Name: "tewake-runner", RunnerScaleSetID: 7}, nil
		},
		removeRunner: func(_ context.Context, id int64) error {
			removed = id
			return nil
		},
	}
	runner, err := client.GetRunnerByName(context.Background(), "tewake-runner")
	if err != nil {
		t.Fatal(err)
	}
	if runner == nil || runner.ID != 17 || runner.ScaleSetID != 7 {
		t.Fatalf("runner = %#v", runner)
	}
	if err := client.RemoveRunner(context.Background(), *runner); err != nil {
		t.Fatal(err)
	}
	if removed != 17 {
		t.Fatalf("removed runner = %d, want 17", removed)
	}
}

func TestRunnerLifecycleFailsClosedOnMismatchedOrAmbiguousResponse(t *testing.T) {
	client := &Client{
		getRunnerByName: func(context.Context, string) (*scaleset.RunnerReference, error) {
			return &scaleset.RunnerReference{ID: 17, Name: "other", RunnerScaleSetID: 7}, nil
		},
	}
	if _, err := client.GetRunnerByName(context.Background(), "tewake-runner"); !errors.Is(err, ErrInvalidPreviewResponse) {
		t.Fatalf("mismatched name error = %v, want ErrInvalidPreviewResponse", err)
	}
	if err := client.RemoveRunner(context.Background(), RunnerReference{}); !errors.Is(err, ErrInvalidPreviewResponse) {
		t.Fatalf("invalid removal error = %v, want ErrInvalidPreviewResponse", err)
	}

	panicClient := &Client{
		getRunnerByName: func(context.Context, string) (*scaleset.RunnerReference, error) {
			panic("preview canary")
		},
	}
	if _, err := panicClient.GetRunnerByName(context.Background(), "tewake-runner"); !errors.Is(err, ErrInvalidPreviewResponse) {
		t.Fatalf("panic error = %v, want ErrInvalidPreviewResponse", err)
	}
}
