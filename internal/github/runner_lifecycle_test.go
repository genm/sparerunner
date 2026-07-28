package github

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/actions/scaleset"
)

func TestRunnerLifecycleValidatesObservedIdentityBeforeRemoval(t *testing.T) {
	removed := int64(0)
	client := &Client{
		getRunnerByName: func(context.Context, string) (*scaleset.RunnerReference, error) {
			return &scaleset.RunnerReference{ID: 17, Name: "sparerunner-runner", RunnerScaleSetID: 7}, nil
		},
		removeRunner: func(_ context.Context, id int64) error {
			removed = id
			return nil
		},
	}
	runner, err := client.GetRunnerByName(context.Background(), "sparerunner-runner")
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
	if _, err := client.GetRunnerByName(context.Background(), "sparerunner-runner"); !errors.Is(err, ErrInvalidPreviewResponse) {
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
	if _, err := panicClient.GetRunnerByName(context.Background(), "sparerunner-runner"); !errors.Is(err, ErrInvalidPreviewResponse) {
		t.Fatalf("panic error = %v, want ErrInvalidPreviewResponse", err)
	}
}

func TestRunnerQueryRequiresExactIDNameAndScaleSetEvidence(t *testing.T) {
	query := RunnerQuery{
		ScaleSetID:      7,
		RunnerRequestID: 7001,
		Name:            "sparerunner-runner",
		ExpectedID:      17,
	}
	exact := &Client{
		getRunner: func(context.Context, int) (*scaleset.RunnerReference, error) {
			return &scaleset.RunnerReference{
				ID: 17, Name: query.Name, RunnerScaleSetID: 7,
			}, nil
		},
		getRunnerByName: func(context.Context, string) (*scaleset.RunnerReference, error) {
			return &scaleset.RunnerReference{
				ID: 17, Name: query.Name, RunnerScaleSetID: 7,
			}, nil
		},
	}
	runner, err := exact.QueryRunner(context.Background(), query)
	if err != nil || runner == nil ||
		*runner != (RunnerReference{ID: 17, Name: query.Name, ScaleSetID: 7}) {
		t.Fatalf("exact runner query = (%#v, %v)", runner, err)
	}

	absent := &Client{
		getRunner: func(context.Context, int) (*scaleset.RunnerReference, error) {
			return nil, scaleset.RunnerNotFoundError
		},
		getRunnerByName: func(context.Context, string) (*scaleset.RunnerReference, error) {
			return nil, nil
		},
	}
	runner, err = absent.QueryRunner(context.Background(), query)
	if err != nil || runner != nil {
		t.Fatalf("exact runner absence = (%#v, %v)", runner, err)
	}
}

func TestRunnerQueryReturnsExactNameAbsenceForDurableCallerConfirmation(t *testing.T) {
	base := RunnerQuery{
		ScaleSetID:      7,
		RunnerRequestID: 7001,
		Name:            "sparerunner-runner",
		ExpectedID:      17,
	}
	t.Run("generation ambiguity name miss", func(t *testing.T) {
		calledByID := false
		client := &Client{
			getRunner: func(context.Context, int) (*scaleset.RunnerReference, error) {
				calledByID = true
				return nil, nil
			},
			getRunnerByName: func(context.Context, string) (*scaleset.RunnerReference, error) {
				return nil, nil
			},
		}
		query := base
		query.ExpectedID = 0
		runner, err := client.QueryRunner(context.Background(), query)
		if err != nil || runner != nil {
			t.Fatalf("generation absence = (%#v, %v)", runner, err)
		}
		if calledByID {
			t.Fatal("generation ambiguity queried an unknown runner ID")
		}
	})
}

func TestRunnerQueryFailsClosedOnSplitIdentity(t *testing.T) {
	base := RunnerQuery{
		ScaleSetID:      7,
		RunnerRequestID: 7001,
		Name:            "sparerunner-runner",
		ExpectedID:      17,
	}
	tests := []struct {
		name    string
		byID    *scaleset.RunnerReference
		idErr   error
		byName  *scaleset.RunnerReference
		nameErr error
	}{
		{
			name: "same name different ID",
			byID: &scaleset.RunnerReference{
				ID: 17, Name: base.Name, RunnerScaleSetID: 7,
			},
			byName: &scaleset.RunnerReference{
				ID: 18, Name: base.Name, RunnerScaleSetID: 7,
			},
		},
		{
			name: "wrong scale set",
			byID: &scaleset.RunnerReference{
				ID: 17, Name: base.Name, RunnerScaleSetID: 8,
			},
			byName: &scaleset.RunnerReference{
				ID: 17, Name: base.Name, RunnerScaleSetID: 8,
			},
		},
		{
			name: "ID present name absent",
			byID: &scaleset.RunnerReference{
				ID: 17, Name: base.Name, RunnerScaleSetID: 7,
			},
		},
		{
			name:  "ID absent name present",
			idErr: scaleset.RunnerNotFoundError,
			byName: &scaleset.RunnerReference{
				ID: 18, Name: base.Name, RunnerScaleSetID: 7,
			},
		},
		{
			name:    "name query failed",
			nameErr: errors.New("server unavailable"),
		},
		{
			name:  "ID query failed",
			idErr: errors.New("server unavailable"),
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			client := &Client{
				getRunner: func(context.Context, int) (*scaleset.RunnerReference, error) {
					return testCase.byID, testCase.idErr
				},
				getRunnerByName: func(context.Context, string) (*scaleset.RunnerReference, error) {
					return testCase.byName, testCase.nameErr
				},
			}
			if _, err := client.QueryRunner(
				context.Background(),
				base,
			); err == nil {
				t.Fatal("split runner identity was accepted")
			}
		})
	}
}

func TestRunnerLifecycleExposesFinalProviderHTTPStatus(t *testing.T) {
	vendorError := errors.New(
		"vendor failure https://api.example.test/runners Bearer secret-token")
	query := RunnerQuery{
		ScaleSetID:      7,
		RunnerRequestID: 7001,
		Name:            "sparerunner-runner",
		ExpectedID:      17,
	}
	tests := []struct {
		name       string
		statusCode int
		call       func(*Client) error
		client     *Client
	}{
		{
			name:       "query by name unavailable",
			statusCode: http.StatusServiceUnavailable,
			client: &Client{
				getRunnerByName: func(
					ctx context.Context,
					_ string,
				) (*scaleset.RunnerReference, error) {
					recordResponseStatus(t, ctx, http.StatusServiceUnavailable)
					return nil, vendorError
				},
			},
			call: func(client *Client) error {
				_, err := client.QueryRunner(context.Background(), query)
				return err
			},
		},
		{
			name:       "query by ID forbidden",
			statusCode: http.StatusForbidden,
			client: &Client{
				getRunnerByName: func(
					context.Context,
					string,
				) (*scaleset.RunnerReference, error) {
					return &scaleset.RunnerReference{
						ID: 17, Name: query.Name, RunnerScaleSetID: 7,
					}, nil
				},
				getRunner: func(
					ctx context.Context,
					_ int,
				) (*scaleset.RunnerReference, error) {
					recordResponseStatus(t, ctx, http.StatusForbidden)
					return nil, vendorError
				},
			},
			call: func(client *Client) error {
				_, err := client.QueryRunner(context.Background(), query)
				return err
			},
		},
		{
			name:       "remove rate limited",
			statusCode: http.StatusTooManyRequests,
			client: &Client{
				removeRunner: func(ctx context.Context, _ int64) error {
					recordResponseStatus(t, ctx, http.StatusTooManyRequests)
					return vendorError
				},
			},
			call: func(client *Client) error {
				return client.RemoveRunner(context.Background(), RunnerReference{
					ID: 17, Name: query.Name, ScaleSetID: 7,
				})
			},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			err := testCase.call(testCase.client)
			var statusError *ProviderHTTPStatusError
			if !errors.As(err, &statusError) ||
				statusError.StatusCode != testCase.statusCode ||
				!errors.Is(err, vendorError) {
				t.Fatalf("runner lifecycle provider status error = %#v", err)
			}
			if strings.Contains(err.Error(), "api.example.test") ||
				strings.Contains(err.Error(), "secret-token") {
				t.Fatalf(
					"runner lifecycle error exposed sensitive vendor text: %q",
					err,
				)
			}
		})
	}
}
