package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

func TestStartRequestRedactsJITAcrossFormattingJSONAndStructuredLogs(t *testing.T) {
	const (
		jitCanary       = "jit-canary.example.test"
		directoryCanary = "workspace-canary.example.test"
		identityCanary  = "identity-canary.example.test"
	)
	request := StartRequest{
		Executable:   "/runtime/run",
		Directory:    "/runtime/" + directoryCanary,
		Arguments:    []string{"--ephemeral"},
		WorkspaceRef: WorkspaceRef{Backend: "test-v1", OwnerID: identityCanary},
		Containment:  ContainmentRef{Backend: "test", OwnerID: "owner"},
		jit:          newJITArgument(jitCanary),
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var log bytes.Buffer
	slog.New(slog.NewJSONHandler(&log, nil)).Info("request", "value", request)
	rendered := fmt.Sprintf("%v\n%+v\n%#v\n%s\n%s", request, request, request, encoded, log.String())
	for _, canary := range []string{jitCanary, directoryCanary, identityCanary} {
		if strings.Contains(rendered, canary) {
			t.Fatalf("StartRequest leaked runtime material %q: %s", canary, rendered)
		}
	}
}

func TestJITDeliveryIsOneShotEvenWhenConsumerFails(t *testing.T) {
	request := StartRequest{
		jit:    newJITArgument("one-job-jit.example.test"),
		verify: func(context.Context) error { return nil },
	}
	if err := request.VerifyWorkspaceAtExec(context.Background()); err != nil {
		t.Fatalf("VerifyWorkspaceAtExec error = %v", err)
	}
	calls := 0
	if err := request.DeliverJIT(func(value string) error {
		calls++
		if value != "one-job-jit.example.test" {
			t.Fatalf("delivered JIT = %q", value)
		}
		return errors.New("injected consumer failure")
	}); !errors.Is(err, ErrStartFailed) {
		t.Fatalf("first DeliverJIT error = %v", err)
	}
	if err := request.DeliverJIT(func(string) error {
		calls++
		return nil
	}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("second DeliverJIT error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("delivery calls = %d", calls)
	}
	if request.finishStart() {
		t.Fatal("failed delivery satisfied the platform start contract")
	}
}

func TestJITDeliveryBeforeWorkspaceVerificationFailsWithoutExposure(t *testing.T) {
	verifyCalls := 0
	request := StartRequest{
		jit: newJITArgument("out-of-order-jit.example.test"),
		verify: func(context.Context) error {
			verifyCalls++
			return nil
		},
	}
	deliveries := 0
	if err := request.DeliverJIT(func(string) error {
		deliveries++
		return nil
	}); !errors.Is(err, ErrWorkspaceChanged) {
		t.Fatalf("out-of-order DeliverJIT error = %v", err)
	}
	if err := request.VerifyWorkspaceAtExec(context.Background()); !errors.Is(err, ErrWorkspaceChanged) {
		t.Fatalf("late VerifyWorkspaceAtExec error = %v", err)
	}
	if deliveries != 0 || verifyCalls != 0 || request.finishStart() {
		t.Fatalf("deliveries=%d verifyCalls=%d", deliveries, verifyCalls)
	}
}

func TestWorkspaceMismatchPermanentlyRevokesJIT(t *testing.T) {
	request := StartRequest{
		jit: newJITArgument("mismatch-jit.example.test"),
		verify: func(context.Context) error {
			return errors.New("workspace identity changed")
		},
	}
	if err := request.VerifyWorkspaceAtExec(context.Background()); !errors.Is(err, ErrWorkspaceChanged) {
		t.Fatalf("VerifyWorkspaceAtExec error = %v", err)
	}
	deliveries := 0
	if err := request.DeliverJIT(func(string) error {
		deliveries++
		return nil
	}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("DeliverJIT after mismatch error = %v", err)
	}
	if deliveries != 0 || request.finishStart() {
		t.Fatalf("deliveries=%d", deliveries)
	}
}

func TestFinishStartRevokesRetainedRequestWithoutDelivery(t *testing.T) {
	request := StartRequest{
		jit:    newJITArgument("retained-jit.example.test"),
		verify: func(context.Context) error { return nil },
	}
	if err := request.VerifyWorkspaceAtExec(context.Background()); err != nil {
		t.Fatal(err)
	}
	retained := request
	if request.finishStart() {
		t.Fatal("missing delivery satisfied the platform start contract")
	}
	deliveries := 0
	if err := retained.DeliverJIT(func(string) error {
		deliveries++
		return nil
	}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("retained DeliverJIT error = %v", err)
	}
	if deliveries != 0 {
		t.Fatalf("retained request exposed JIT %d times", deliveries)
	}
}

func TestFinishStartFailsClosedWithoutWaitingForAsyncDelivery(t *testing.T) {
	request := StartRequest{
		jit:    newJITArgument("async-jit.example.test"),
		verify: func(context.Context) error { return nil },
	}
	if err := request.VerifyWorkspaceAtExec(context.Background()); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- request.DeliverJIT(func(string) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	if request.finishStart() {
		t.Fatal("incomplete asynchronous delivery satisfied the start contract")
	}
	close(release)
	if err := <-done; !errors.Is(err, ErrStartFailed) {
		t.Fatalf("asynchronous DeliverJIT error = %v", err)
	}
}
