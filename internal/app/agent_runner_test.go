package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/runner"
	"github.com/genm/tewake/internal/store"
	"github.com/genm/tewake/internal/transport"
)

type fakeRunnerLifecycle struct {
	prepares int
	starts   int
	destroys int
	prepare  func(context.Context, runner.Preparation) (runner.Snapshot, error)
	start    func(context.Context, runner.Start) (runner.Snapshot, error)
	inspect  func(context.Context, string) (runner.Snapshot, error)
	wait     func(context.Context, string) (runner.Snapshot, error)
	destroy  func(context.Context, string) (runner.Snapshot, error)
	ready    func(context.Context) error
}

type fakeRecoveringRunnerLifecycle struct {
	*fakeRunnerLifecycle
	recover func(context.Context, string) (runner.Snapshot, error)
}

func (fake *fakeRunnerLifecycle) Ready(ctx context.Context) error {
	if fake.ready != nil {
		return fake.ready(ctx)
	}
	return nil
}

func (fake *fakeRecoveringRunnerLifecycle) Recover(ctx context.Context, executionID string) (runner.Snapshot, error) {
	if fake.recover == nil {
		return runner.Snapshot{}, runner.ErrReconciliationRequired
	}
	return fake.recover(ctx, executionID)
}

func (fake *fakeRunnerLifecycle) EnsurePrepared(ctx context.Context, request runner.Preparation) (runner.Snapshot, error) {
	fake.prepares++
	if fake.prepare == nil {
		return runner.Snapshot{ExecutionID: request.ExecutionID, State: runner.StatePrepared, Prepared: true}, nil
	}
	return fake.prepare(ctx, request)
}

func (fake *fakeRunnerLifecycle) EnsureRunning(ctx context.Context, request runner.Start) (runner.Snapshot, error) {
	fake.starts++
	return fake.start(ctx, request)
}

func (fake *fakeRunnerLifecycle) Inspect(ctx context.Context, executionID string) (runner.Snapshot, error) {
	if fake.inspect == nil {
		return runner.Snapshot{ExecutionID: executionID, State: runner.StatePrepared, Prepared: true}, nil
	}
	return fake.inspect(ctx, executionID)
}

func (fake *fakeRunnerLifecycle) Wait(ctx context.Context, executionID string) (runner.Snapshot, error) {
	if fake.wait == nil {
		return runner.Snapshot{ExecutionID: executionID, State: runner.StateRunning, Running: true}, runner.ErrStrongOwnershipUnavailable
	}
	return fake.wait(ctx, executionID)
}

func (fake *fakeRunnerLifecycle) Destroy(ctx context.Context, executionID string) (runner.Snapshot, error) {
	fake.destroys++
	return fake.destroy(ctx, executionID)
}

func openAgentCommandStore(t *testing.T) *store.AgentStore {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	agentStore, err := store.OpenAgent(context.Background(), filepath.Join(directory, "agent.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return agentStore
}

func currentRunnerPackage(t *testing.T) runner.Package {
	t.Helper()
	pkg, err := runner.OfficialPackage(runner.CurrentPlatform())
	if err != nil {
		t.Fatal(err)
	}
	return pkg
}

func runnerJournalRootName(executionID string) string {
	digest := sha256.Sum256([]byte(executionID))
	return hex.EncodeToString(digest[:])
}

func TestAgentPlatformUsesProtocolDomainNamesAndRejectsUnsupportedValues(t *testing.T) {
	for _, testCase := range []struct {
		goos string
		want domain.OperatingSystem
	}{
		{goos: "linux", want: domain.OSLinux},
		{goos: "darwin", want: domain.OSMacOS},
		{goos: "windows", want: domain.OSWindows},
	} {
		operatingSystem, architecture, err := agentPlatform(testCase.goos, "arm64")
		if err != nil {
			t.Fatalf("%s platform: %v", testCase.goos, err)
		}
		if operatingSystem != testCase.want || architecture != domain.ArchARM64 {
			t.Fatalf("%s platform = %s/%s", testCase.goos, operatingSystem, architecture)
		}
	}
	for _, unsupported := range []struct {
		goos   string
		goarch string
	}{
		{goos: "freebsd", goarch: "amd64"},
		{goos: "linux", goarch: "386"},
	} {
		if _, _, err := agentPlatform(unsupported.goos, unsupported.goarch); !errors.Is(err, runner.ErrUnsupportedPlatform) {
			t.Fatalf("unsupported %s/%s error = %v", unsupported.goos, unsupported.goarch, err)
		}
	}
}

func TestAgentRuntimeReadinessFailsClosedAfterLocalJournalDegradation(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	manager := &fakeRunnerLifecycle{
		inspect: func(context.Context, string) (runner.Snapshot, error) {
			return runner.Snapshot{}, runner.ErrExecutionNotFound
		},
	}
	commandRuntime, err := NewAgentCommandRuntime(
		"node-1",
		agentStore,
		manager,
		currentRunnerPackage(t),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !commandRuntime.Ready(context.Background()) {
		t.Fatal("healthy runtime did not advertise readiness")
	}
	commandRuntime.failMonitor()
	if commandRuntime.Ready(context.Background()) {
		t.Fatal("degraded journal/outbox authority remained ready")
	}
	metadata := transport.CommandMetadata{
		CommandID:       "degraded-monitor-command",
		ControllerEpoch: 1,
		ExecutionID:     "degraded-monitor-execution",
		ExpectedState:   domain.ExecutionReserved,
	}
	payload, err := transport.EncodePrepareCommandPayload(metadata, runner.OfficialRunnerVersion, true)
	if err != nil {
		t.Fatal(err)
	}
	envelope := transport.Envelope{
		ProtocolVersion: transport.ProtocolVersion,
		MessageID:       string(metadata.CommandID),
		Type:            transport.MessagePrepare,
		Payload:         payload,
	}
	if _, err := commandRuntime.Accept(context.Background(), &envelope); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("degraded monitor admission error=%v", err)
	}
	snapshot, err := agentStore.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Commands) != 0 || manager.prepares != 0 {
		t.Fatalf("degraded monitor admitted commands=%d prepares=%d", len(snapshot.Commands), manager.prepares)
	}
}

func TestAgentDegradedReadinessStillAllowsExactReplayAndCancel(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	cancelPhase := false
	platformDegraded := false
	manager := &fakeRunnerLifecycle{
		ready: func(context.Context) error {
			if platformDegraded {
				return runner.ErrStrongOwnershipUnavailable
			}
			return nil
		},
		inspect: func(context.Context, string) (runner.Snapshot, error) {
			if cancelPhase {
				return runner.Snapshot{
					ExecutionID: "degraded-replay-execution",
					State:       runner.StateRunning,
					Running:     true,
				}, nil
			}
			return runner.Snapshot{}, runner.ErrExecutionNotFound
		},
	}
	commandRuntime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	prepareMetadata := transport.CommandMetadata{
		CommandID:       "degraded-replay-prepare",
		ControllerEpoch: 1,
		ExecutionID:     "degraded-replay-execution",
		ExpectedState:   domain.ExecutionReserved,
	}
	newPrepare := func() transport.Envelope {
		payload, encodeErr := transport.EncodePrepareCommandPayload(
			prepareMetadata,
			runner.OfficialRunnerVersion,
			true,
		)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		return transport.Envelope{
			ProtocolVersion: transport.ProtocolVersion,
			MessageID:       string(prepareMetadata.CommandID),
			Type:            transport.MessagePrepare,
			Payload:         payload,
		}
	}
	first := newPrepare()
	accepted, err := commandRuntime.Accept(context.Background(), &first)
	if err != nil {
		t.Fatal(err)
	}
	accepted.Discard()

	commandRuntime.failMonitor()
	platformDegraded = true
	replay := newPrepare()
	accepted, err = commandRuntime.Accept(context.Background(), &replay)
	if err != nil || !accepted.replayed {
		t.Fatalf("exact replay while degraded: replayed=%t err=%v", accepted != nil && accepted.replayed, err)
	}
	accepted.Discard()

	cancelPhase = true
	cancelMetadata := transport.CommandMetadata{
		CommandID:       "degraded-replay-cancel",
		ControllerEpoch: 1,
		ExecutionID:     "degraded-replay-execution",
		ExpectedState:   domain.ExecutionRunning,
	}
	cancelPayload, err := transport.EncodeCancelCommandPayload(cancelMetadata)
	if err != nil {
		t.Fatal(err)
	}
	cancelEnvelope := transport.Envelope{
		ProtocolVersion: transport.ProtocolVersion,
		MessageID:       string(cancelMetadata.CommandID),
		Type:            transport.MessageCancel,
		Payload:         cancelPayload,
	}
	cancel, err := commandRuntime.Accept(context.Background(), &cancelEnvelope)
	if err != nil {
		t.Fatalf("cancel while degraded: %v", err)
	}
	cancel.Discard()
}

func TestAgentRejectsNewRunnerAdmissionWhenPlatformReadinessDegrades(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	manager := &fakeRunnerLifecycle{
		ready: func(context.Context) error {
			return runner.ErrStrongOwnershipUnavailable
		},
		inspect: func(context.Context, string) (runner.Snapshot, error) {
			return runner.Snapshot{}, runner.ErrExecutionNotFound
		},
	}
	commandRuntime, err := NewAgentCommandRuntime(
		"node-1",
		agentStore,
		manager,
		currentRunnerPackage(t),
	)
	if err != nil {
		t.Fatal(err)
	}
	metadata := transport.CommandMetadata{
		CommandID:       "degraded-prepare-command",
		ControllerEpoch: 1,
		ExecutionID:     "degraded-prepare-execution",
		ExpectedState:   domain.ExecutionReserved,
	}
	payload, err := transport.EncodePrepareCommandPayload(
		metadata,
		runner.OfficialRunnerVersion,
		true,
	)
	if err != nil {
		t.Fatal(err)
	}
	wire := append([]byte(nil), payload...)
	envelope := transport.Envelope{
		ProtocolVersion: transport.ProtocolVersion,
		MessageID:       string(metadata.CommandID),
		Type:            transport.MessagePrepare,
		Payload:         wire,
	}
	if _, err := commandRuntime.Accept(context.Background(), &envelope); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("Accept error=%v", err)
	}
	if envelope.Payload != nil || !bytes.Equal(wire, make([]byte, len(wire))) {
		t.Fatal("rejected command retained raw payload")
	}
	snapshot, err := agentStore.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Commands) != 0 || manager.prepares != 0 {
		t.Fatalf("commands=%d prepares=%d", len(snapshot.Commands), manager.prepares)
	}
}

func TestAgentCommandAdmissionTimeoutReleasesExecutionLock(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	manager := &fakeRunnerLifecycle{
		ready: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
		inspect: func(context.Context, string) (runner.Snapshot, error) {
			return runner.Snapshot{}, runner.ErrExecutionNotFound
		},
	}
	commandRuntime, err := NewAgentCommandRuntime(
		"node-1",
		agentStore,
		manager,
		currentRunnerPackage(t),
	)
	if err != nil {
		t.Fatal(err)
	}
	newEnvelope := func(commandID string) transport.Envelope {
		t.Helper()
		metadata := transport.CommandMetadata{
			CommandID:       domain.CommandID(commandID),
			ControllerEpoch: 1,
			ExecutionID:     "stalled-readiness-execution",
			ExpectedState:   domain.ExecutionReserved,
		}
		payload, encodeErr := transport.EncodePrepareCommandPayload(
			metadata,
			runner.OfficialRunnerVersion,
			true,
		)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		return transport.Envelope{
			ProtocolVersion: transport.ProtocolVersion,
			MessageID:       commandID,
			Type:            transport.MessagePrepare,
			Payload:         payload,
		}
	}

	stalled := newEnvelope("stalled-readiness-command")
	started := time.Now()
	if err := dispatchAgentCommand(
		context.Background(),
		20*time.Millisecond,
		commandRuntime,
		&stalled,
		func(string) error { return nil },
	); err == nil {
		t.Fatal("stalled readiness admitted a command")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("stalled readiness blocked for %s", elapsed)
	}
	snapshot, err := agentStore.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	commandRuntime.locksMu.Lock()
	lockedExecutions := len(commandRuntime.locks)
	commandRuntime.locksMu.Unlock()
	if len(snapshot.Commands) != 0 || manager.prepares != 0 || lockedExecutions != 0 {
		t.Fatalf(
			"stalled admission left state: commands=%d prepares=%d locks=%d",
			len(snapshot.Commands),
			manager.prepares,
			lockedExecutions,
		)
	}

	manager.ready = nil
	retry := newEnvelope("stalled-readiness-retry")
	acknowledged := false
	if err := dispatchAgentCommand(
		context.Background(),
		time.Second,
		commandRuntime,
		&retry,
		func(string) error {
			acknowledged = true
			return nil
		},
	); err != nil {
		t.Fatalf("retry after readiness recovery: %v", err)
	}
	if !acknowledged || manager.prepares != 1 {
		t.Fatalf("retry acknowledged=%t prepares=%d", acknowledged, manager.prepares)
	}
}

func TestAgentMonitorFailureLinearizesBeforeCommandRecord(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	probeStarted := make(chan struct{})
	releaseProbe := make(chan struct{})
	manager := &fakeRunnerLifecycle{
		ready: func(ctx context.Context) error {
			close(probeStarted)
			select {
			case <-releaseProbe:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		inspect: func(context.Context, string) (runner.Snapshot, error) {
			return runner.Snapshot{}, runner.ErrExecutionNotFound
		},
	}
	commandRuntime, err := NewAgentCommandRuntime(
		"node-1",
		agentStore,
		manager,
		currentRunnerPackage(t),
	)
	if err != nil {
		t.Fatal(err)
	}
	metadata := transport.CommandMetadata{
		CommandID:       "monitor-race-command",
		ControllerEpoch: 1,
		ExecutionID:     "monitor-race-execution",
		ExpectedState:   domain.ExecutionReserved,
	}
	payload, err := transport.EncodePrepareCommandPayload(metadata, runner.OfficialRunnerVersion, true)
	if err != nil {
		t.Fatal(err)
	}
	envelope := transport.Envelope{
		ProtocolVersion: transport.ProtocolVersion,
		MessageID:       string(metadata.CommandID),
		Type:            transport.MessagePrepare,
		Payload:         payload,
	}
	result := make(chan error, 1)
	go func() {
		accepted, acceptErr := commandRuntime.Accept(context.Background(), &envelope)
		if accepted != nil {
			accepted.Discard()
		}
		result <- acceptErr
	}()
	select {
	case <-probeStarted:
	case <-time.After(time.Second):
		t.Fatal("readiness probe did not start")
	}
	commandRuntime.failMonitor()
	close(releaseProbe)
	select {
	case err := <-result:
		if !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
			t.Fatalf("Accept error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("admission did not resolve after readiness probe")
	}
	snapshot, err := agentStore.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Commands) != 0 || manager.prepares != 0 {
		t.Fatalf("monitor failure admitted commands=%d prepares=%d", len(snapshot.Commands), manager.prepares)
	}
}

func TestAgentPrepareCommandPersistsBeforeMaterialization(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	manager := &fakeRunnerLifecycle{
		prepare: func(_ context.Context, request runner.Preparation) (runner.Snapshot, error) {
			snapshot, err := agentStore.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshot.Commands) != 1 {
				t.Fatalf("commands before materialization = %d", len(snapshot.Commands))
			}
			return runner.Snapshot{ExecutionID: request.ExecutionID, State: runner.StatePrepared, Prepared: true}, nil
		},
		inspect: func(context.Context, string) (runner.Snapshot, error) {
			return runner.Snapshot{}, runner.ErrExecutionNotFound
		},
		start: func(context.Context, runner.Start) (runner.Snapshot, error) {
			return runner.Snapshot{}, runner.ErrStartFailed
		},
		destroy: func(context.Context, string) (runner.Snapshot, error) {
			return runner.Snapshot{}, runner.ErrExecutionNotFound
		},
	}
	commandRuntime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	metadata := transport.CommandMetadata{
		CommandID:       "prepare-command",
		ControllerEpoch: 1,
		ExecutionID:     "prepare-execution",
		ExpectedState:   domain.ExecutionReserved,
	}
	payload, err := transport.EncodePrepareCommandPayload(metadata, runner.OfficialRunnerVersion, true)
	if err != nil {
		t.Fatal(err)
	}
	envelope := transport.Envelope{ProtocolVersion: 1, MessageID: string(metadata.CommandID), Type: transport.MessagePrepare, Payload: payload}
	accepted, err := commandRuntime.Accept(context.Background(), &envelope)
	if err != nil {
		t.Fatal(err)
	}
	update, err := accepted.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if update.State != domain.ExecutionPreparing || manager.prepares != 1 || manager.starts != 0 {
		t.Fatalf("prepare update=%#v prepares=%d starts=%d", update, manager.prepares, manager.starts)
	}
}

func TestAgentCommandAcceptPersistsBeforeStartAndErasesWireJIT(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	manager := &fakeRunnerLifecycle{
		start: func(_ context.Context, request runner.Start) (runner.Snapshot, error) {
			var delivered string
			if err := request.JIT.Deliver(func(value string) error {
				delivered = value
				return nil
			}); err != nil {
				return runner.Snapshot{}, err
			}
			if delivered != "jit-canary.example.test" {
				t.Fatalf("delivered JIT = %q", delivered)
			}
			return runner.Snapshot{ExecutionID: request.ExecutionID, State: runner.StateRunning, Running: true}, nil
		},
		destroy: func(context.Context, string) (runner.Snapshot, error) {
			return runner.Snapshot{}, runner.ErrExecutionNotFound
		},
	}
	runtime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	metadata := transport.CommandMetadata{
		CommandID:       "command-1",
		ControllerEpoch: 1,
		ExecutionID:     "execution-1",
		ExpectedState:   domain.ExecutionPreparing,
	}
	payload, err := transport.EncodeStartCommandPayload(metadata, runner.OfficialRunnerVersion, true, "jit-canary.example.test")
	if err != nil {
		t.Fatal(err)
	}
	wire := append([]byte(nil), payload...)
	envelope := transport.Envelope{
		ProtocolVersion: transport.ProtocolVersion,
		MessageID:       string(metadata.CommandID),
		Type:            transport.MessageStart,
		Payload:         wire,
	}
	accepted, err := runtime.Accept(context.Background(), &envelope)
	if err != nil {
		t.Fatal(err)
	}
	if envelope.Payload != nil || !bytes.Equal(wire, make([]byte, len(wire))) {
		t.Fatal("accepted start retained the raw payload")
	}
	before, err := agentStore.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(before.Commands) != 1 || manager.starts != 0 {
		t.Fatalf("before execute: commands=%d starts=%d", len(before.Commands), manager.starts)
	}
	update, err := accepted.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if update.State != domain.ExecutionRunning || update.ErrorCode != transport.ExecutionErrorNone || manager.starts != 1 {
		t.Fatalf("update=%#v starts=%d", update, manager.starts)
	}
}

func TestAgentCommandReplayMismatchFailsBeforeRunner(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	manager := &fakeRunnerLifecycle{
		start: func(_ context.Context, request runner.Start) (runner.Snapshot, error) {
			return runner.Snapshot{ExecutionID: request.ExecutionID, State: runner.StateRunning}, nil
		},
		destroy: func(context.Context, string) (runner.Snapshot, error) {
			return runner.Snapshot{}, runner.ErrExecutionNotFound
		},
	}
	runtime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	metadata := transport.CommandMetadata{
		CommandID:       "command-replay",
		ControllerEpoch: 1,
		ExecutionID:     "execution-replay",
		ExpectedState:   domain.ExecutionPreparing,
	}
	for index, jit := range []string{"first.example.test", "different.example.test"} {
		payload, err := transport.EncodeStartCommandPayload(metadata, runner.OfficialRunnerVersion, false, jit)
		if err != nil {
			t.Fatal(err)
		}
		envelope := transport.Envelope{ProtocolVersion: 1, MessageID: string(metadata.CommandID), Type: transport.MessageStart, Payload: payload}
		accepted, acceptErr := runtime.Accept(context.Background(), &envelope)
		if index == 0 && acceptErr != nil {
			t.Fatal(acceptErr)
		}
		if accepted != nil {
			accepted.Discard()
		}
		if index == 1 && !errors.Is(acceptErr, store.ErrReplayMismatch) {
			t.Fatalf("mismatched replay error = %v", acceptErr)
		}
	}
	if manager.starts != 0 {
		t.Fatalf("runner started before accepted command execution: %d", manager.starts)
	}
}

func TestAgentCleanupFailurePersistsQuarantineTombstone(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	manager := &fakeRunnerLifecycle{
		start: func(context.Context, runner.Start) (runner.Snapshot, error) {
			return runner.Snapshot{}, runner.ErrStartFailed
		},
		inspect: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			return runner.Snapshot{ExecutionID: executionID, State: runner.StateRunning, Running: true}, nil
		},
		destroy: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			return runner.Snapshot{ExecutionID: executionID, State: runner.StateCleanupFailed, Quarantined: true}, runner.ErrQuarantined
		},
	}
	runtime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	metadata := transport.CommandMetadata{
		CommandID:       "command-cancel",
		ControllerEpoch: 1,
		ExecutionID:     "execution-cancel",
		ExpectedState:   domain.ExecutionRunning,
	}
	payload, err := transport.EncodeCancelCommandPayload(metadata)
	if err != nil {
		t.Fatal(err)
	}
	envelope := transport.Envelope{ProtocolVersion: 1, MessageID: string(metadata.CommandID), Type: transport.MessageCancel, Payload: payload}
	accepted, err := runtime.Accept(context.Background(), &envelope)
	if err != nil {
		t.Fatal(err)
	}
	update, err := accepted.Execute(context.Background())
	if !errors.Is(err, runner.ErrQuarantined) || update.State != domain.ExecutionCleanupFailed || update.ErrorCode != transport.ExecutionErrorQuarantined {
		t.Fatalf("cleanup update = %#v, %v", update, err)
	}
	snapshot, err := agentStore.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.CleanupTombstones) != 1 || snapshot.CleanupTombstones[0].ExecutionID != metadata.ExecutionID {
		t.Fatalf("cleanup tombstones = %#v", snapshot.CleanupTombstones)
	}
}

func TestAgentStartFailureDestroysPreparedWorkspaceBeforeReportingFailed(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	manager := &fakeRunnerLifecycle{
		start: func(context.Context, runner.Start) (runner.Snapshot, error) {
			return runner.Snapshot{}, runner.ErrStrongOwnershipUnavailable
		},
		inspect: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			return runner.Snapshot{ExecutionID: executionID, State: runner.StatePrepared, Prepared: true}, nil
		},
		destroy: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			return runner.Snapshot{ExecutionID: executionID, State: runner.StateReleased}, nil
		},
	}
	commandRuntime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	metadata := transport.CommandMetadata{
		CommandID:       "failed-start-cleanup-command",
		ControllerEpoch: 1,
		ExecutionID:     "failed-start-cleanup-execution",
		ExpectedState:   domain.ExecutionPreparing,
	}
	payload, err := transport.EncodeStartCommandPayload(metadata, runner.OfficialRunnerVersion, false, "failed-start-cleanup-canary.example.test")
	if err != nil {
		t.Fatal(err)
	}
	envelope := transport.Envelope{ProtocolVersion: 1, MessageID: string(metadata.CommandID), Type: transport.MessageStart, Payload: payload}
	accepted, err := commandRuntime.Accept(context.Background(), &envelope)
	if err != nil {
		t.Fatal(err)
	}
	update, err := accepted.Execute(context.Background())
	if !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("start error = %v", err)
	}
	if update.State != domain.ExecutionFailed ||
		update.ErrorCode != domain.ExecutionErrorPlatform ||
		manager.destroys != 1 {
		t.Fatalf("failed start update=%#v destroys=%d", update, manager.destroys)
	}
	pending, err := agentStore.PendingExecutionUpdates(context.Background())
	if err != nil || len(pending) != 1 || pending[0].Update != storeExecutionUpdate(update) {
		t.Fatalf("failed start outbox = %#v, %v", pending, err)
	}
	snapshot, err := agentStore.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.CleanupTombstones) != 0 ||
		len(snapshot.Observations) != 1 ||
		snapshot.Observations[0].State != domain.ExecutionFailed {
		t.Fatalf("failed start snapshot = %#v", snapshot)
	}
}

func TestAgentStartCleanupFailureQueuesActualQuarantineOutcome(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	manager := &fakeRunnerLifecycle{
		start: func(context.Context, runner.Start) (runner.Snapshot, error) {
			return runner.Snapshot{}, runner.ErrStrongOwnershipUnavailable
		},
		inspect: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			return runner.Snapshot{ExecutionID: executionID, State: runner.StatePrepared, Prepared: true}, nil
		},
		destroy: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			return runner.Snapshot{
				ExecutionID: executionID,
				State:       runner.StateCleanupFailed,
				Quarantined: true,
			}, runner.ErrQuarantined
		},
	}
	commandRuntime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	metadata := transport.CommandMetadata{
		CommandID:       "failed-start-quarantine-command",
		ControllerEpoch: 1,
		ExecutionID:     "failed-start-quarantine-execution",
		ExpectedState:   domain.ExecutionPreparing,
	}
	payload, err := transport.EncodeStartCommandPayload(metadata, runner.OfficialRunnerVersion, false, "failed-start-quarantine-canary.example.test")
	if err != nil {
		t.Fatal(err)
	}
	envelope := transport.Envelope{ProtocolVersion: 1, MessageID: string(metadata.CommandID), Type: transport.MessageStart, Payload: payload}
	accepted, err := commandRuntime.Accept(context.Background(), &envelope)
	if err != nil {
		t.Fatal(err)
	}
	update, err := accepted.Execute(context.Background())
	if !errors.Is(err, runner.ErrQuarantined) ||
		update.State != domain.ExecutionCleanupFailed ||
		update.ErrorCode != domain.ExecutionErrorQuarantined {
		t.Fatalf("quarantined start update = %#v, %v", update, err)
	}
	deliverable, err := commandRuntime.PendingUpdates(context.Background())
	if err != nil || len(deliverable) != 1 || deliverable[0].Update != storeExecutionUpdate(update) {
		t.Fatalf("quarantine outcome is not deliverable: %#v, %v", deliverable, err)
	}
	pending, err := agentStore.PendingExecutionUpdates(context.Background())
	if err != nil || len(pending) != 1 || pending[0].Update.State != domain.ExecutionCleanupFailed {
		t.Fatalf("quarantined start outbox = %#v, %v", pending, err)
	}
	snapshot, err := agentStore.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.CleanupTombstones) != 1 ||
		snapshot.CleanupTombstones[0].ExecutionID != metadata.ExecutionID ||
		len(snapshot.Observations) != 1 ||
		snapshot.Observations[0].State != domain.ExecutionCleanupFailed {
		t.Fatalf("quarantined start snapshot = %#v", snapshot)
	}
}

func TestAgentStartCleanupFailureWithoutDurableTombstoneDoesNotQueueFailed(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	manager := &fakeRunnerLifecycle{
		start: func(context.Context, runner.Start) (runner.Snapshot, error) {
			return runner.Snapshot{}, runner.ErrStrongOwnershipUnavailable
		},
		inspect: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			return runner.Snapshot{ExecutionID: executionID, State: runner.StatePrepared, Prepared: true}, nil
		},
		destroy: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			return runner.Snapshot{
				ExecutionID: executionID,
				State:       runner.StateCleanupFailed,
				Quarantined: true,
			}, runner.ErrQuarantined
		},
	}
	commandRuntime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	metadata := transport.CommandMetadata{
		CommandID:       "missing-tombstone-command",
		ControllerEpoch: 1,
		ExecutionID:     "missing-tombstone-execution",
		ExpectedState:   domain.ExecutionPreparing,
	}
	payload, err := transport.EncodeStartCommandPayload(metadata, runner.OfficialRunnerVersion, false, "missing-tombstone-canary.example.test")
	if err != nil {
		t.Fatal(err)
	}
	envelope := transport.Envelope{ProtocolVersion: 1, MessageID: string(metadata.CommandID), Type: transport.MessageStart, Payload: payload}
	accepted, err := commandRuntime.Accept(context.Background(), &envelope)
	if err != nil {
		t.Fatal(err)
	}
	if err := agentStore.Close(); err != nil {
		t.Fatal(err)
	}
	update, err := accepted.Execute(context.Background())
	if err == nil ||
		update.State != domain.ExecutionCleanupFailed ||
		update.ErrorCode != domain.ExecutionErrorJournal {
		t.Fatalf("missing tombstone update = %#v, %v", update, err)
	}
	if manager.destroys != 1 {
		t.Fatalf("missing tombstone cleanup attempts = %d", manager.destroys)
	}
}

func TestAgentCommandRejectsStaleCancelWithoutRecordingIt(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	manager := &fakeRunnerLifecycle{
		start: func(context.Context, runner.Start) (runner.Snapshot, error) {
			return runner.Snapshot{}, runner.ErrStartFailed
		},
		inspect: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			return runner.Snapshot{ExecutionID: executionID, State: runner.StateReleased}, runner.ErrExecutionConflict
		},
		destroy: func(context.Context, string) (runner.Snapshot, error) {
			return runner.Snapshot{}, runner.ErrExecutionConflict
		},
	}
	commandRuntime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	metadata := transport.CommandMetadata{
		CommandID:       "stale-cancel",
		ControllerEpoch: 1,
		ExecutionID:     "stale-execution",
		ExpectedState:   domain.ExecutionRunning,
	}
	for range 2 {
		payload, err := transport.EncodeCancelCommandPayload(metadata)
		if err != nil {
			t.Fatal(err)
		}
		envelope := transport.Envelope{ProtocolVersion: 1, MessageID: string(metadata.CommandID), Type: transport.MessageCancel, Payload: payload}
		if _, err := commandRuntime.Accept(context.Background(), &envelope); !errors.Is(err, errAgentCommandExpectedState) {
			t.Fatalf("stale cancel error = %v", err)
		}
	}
	snapshot, err := agentStore.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Commands) != 0 || manager.destroys != 0 {
		t.Fatalf("stale cancel changed state: commands=%d destroys=%d", len(snapshot.Commands), manager.destroys)
	}
}

func TestAgentCommandInvalidFirstReplayMustPassStateAdmission(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	var stateMu sync.Mutex
	localState := runner.StateRunning
	manager := &fakeRunnerLifecycle{
		start: func(_ context.Context, request runner.Start) (runner.Snapshot, error) {
			if err := request.JIT.Deliver(func(string) error { return nil }); err != nil {
				return runner.Snapshot{}, err
			}
			return runner.Snapshot{ExecutionID: request.ExecutionID, State: runner.StateRunning, Running: true}, nil
		},
		inspect: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			stateMu.Lock()
			defer stateMu.Unlock()
			if localState == "" {
				return runner.Snapshot{}, runner.ErrExecutionNotFound
			}
			return runner.Snapshot{ExecutionID: executionID, State: localState, Running: localState == runner.StateRunning}, nil
		},
		destroy: func(context.Context, string) (runner.Snapshot, error) {
			return runner.Snapshot{}, runner.ErrExecutionConflict
		},
	}
	commandRuntime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	metadata := transport.CommandMetadata{
		CommandID:       "invalid-first-start",
		ControllerEpoch: 1,
		ExecutionID:     "invalid-first-execution",
		ExpectedState:   domain.ExecutionPreparing,
	}
	accept := func() (*acceptedAgentCommand, error) {
		payload, err := transport.EncodeStartCommandPayload(metadata, runner.OfficialRunnerVersion, true, "invalid-first-canary.example.test")
		if err != nil {
			t.Fatal(err)
		}
		envelope := transport.Envelope{ProtocolVersion: 1, MessageID: string(metadata.CommandID), Type: transport.MessageStart, Payload: payload}
		return commandRuntime.Accept(context.Background(), &envelope)
	}
	for range 2 {
		if _, err := accept(); !errors.Is(err, errAgentCommandExpectedState) {
			t.Fatalf("invalid first/replay error = %v", err)
		}
	}
	before, err := agentStore.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(before.Commands) != 0 || manager.starts != 0 {
		t.Fatalf("invalid command gained replay admission: commands=%d starts=%d", len(before.Commands), manager.starts)
	}
	stateMu.Lock()
	localState = runner.StatePrepared
	stateMu.Unlock()
	accepted, err := accept()
	if err != nil {
		t.Fatal(err)
	}
	if accepted.replayed {
		t.Fatal("first state-valid admission was marked as a replay")
	}
	accepted.Discard()
}

func TestAgentCommandAckFailureStillExecutesCommittedCommand(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	startCompleted := make(chan struct{})
	manager := &fakeRunnerLifecycle{
		start: func(_ context.Context, request runner.Start) (runner.Snapshot, error) {
			if err := request.JIT.Deliver(func(value string) error {
				if value != "ack-failure-canary.example.test" {
					t.Fatalf("delivered JIT = %q", value)
				}
				return nil
			}); err != nil {
				return runner.Snapshot{}, err
			}
			close(startCompleted)
			return runner.Snapshot{ExecutionID: request.ExecutionID, State: runner.StateRunning, Running: true}, nil
		},
		destroy: func(context.Context, string) (runner.Snapshot, error) {
			return runner.Snapshot{}, runner.ErrExecutionNotFound
		},
	}
	commandRuntime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	metadata := transport.CommandMetadata{
		CommandID:       "ack-failure-start",
		ControllerEpoch: 1,
		ExecutionID:     "ack-failure-execution",
		ExpectedState:   domain.ExecutionPreparing,
	}
	payload, err := transport.EncodeStartCommandPayload(metadata, runner.OfficialRunnerVersion, false, "ack-failure-canary.example.test")
	if err != nil {
		t.Fatal(err)
	}
	ackFailure := errors.New("acknowledgement failed")
	envelope := transport.Envelope{ProtocolVersion: 1, MessageID: string(metadata.CommandID), Type: transport.MessageStart, Payload: payload}
	if err := dispatchAgentCommand(context.Background(), DefaultAgentReadinessTimeout, commandRuntime, &envelope, func(string) error {
		return ackFailure
	}); !errors.Is(err, ackFailure) {
		t.Fatalf("ack failure = %v", err)
	}
	select {
	case <-startCompleted:
	case <-time.After(time.Second):
		t.Fatal("committed command did not execute after ACK failure")
	}
	deadline := time.Now().Add(time.Second)
	for {
		commandRuntime.locksMu.Lock()
		lockedExecutions := len(commandRuntime.locks)
		commandRuntime.locksMu.Unlock()
		if lockedExecutions == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("executor did not release command ownership")
		}
		goruntime.Gosched()
	}
	pending, err := agentStore.PendingExecutionUpdates(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].Update.State != domain.ExecutionRunning || manager.starts != 1 {
		t.Fatalf("ACK failure result: pending=%#v starts=%d", pending, manager.starts)
	}
	snapshot, err := agentStore.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Commands) != 1 {
		t.Fatalf("committed commands = %d", len(snapshot.Commands))
	}
}

func TestAgentCommandExactReplayRemainsIdempotentAfterStateChanges(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	startCalls := 0
	manager := &fakeRunnerLifecycle{
		start: func(_ context.Context, request runner.Start) (runner.Snapshot, error) {
			startCalls++
			if startCalls == 1 {
				if err := request.JIT.Deliver(func(string) error { return nil }); err != nil {
					return runner.Snapshot{}, err
				}
				return runner.Snapshot{ExecutionID: request.ExecutionID, State: runner.StateRunning, Running: true}, nil
			}
			return runner.Snapshot{ExecutionID: request.ExecutionID, State: runner.StateReleased}, runner.ErrExecutionConflict
		},
		inspect: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			return runner.Snapshot{ExecutionID: executionID, State: runner.StatePrepared, Prepared: true}, nil
		},
		destroy: func(context.Context, string) (runner.Snapshot, error) {
			return runner.Snapshot{}, runner.ErrExecutionConflict
		},
	}
	commandRuntime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	metadata := transport.CommandMetadata{
		CommandID:       "state-change-replay",
		ControllerEpoch: 1,
		ExecutionID:     "state-change-execution",
		ExpectedState:   domain.ExecutionPreparing,
	}
	for index := range 2 {
		payload, err := transport.EncodeStartCommandPayload(metadata, runner.OfficialRunnerVersion, false, "state-change-canary.example.test")
		if err != nil {
			t.Fatal(err)
		}
		envelope := transport.Envelope{ProtocolVersion: 1, MessageID: string(metadata.CommandID), Type: transport.MessageStart, Payload: payload}
		accepted, err := commandRuntime.Accept(context.Background(), &envelope)
		if err != nil {
			t.Fatal(err)
		}
		update, err := accepted.Execute(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if update.Replayed != (index == 1) {
			t.Fatalf("iteration %d replayed = %t", index, update.Replayed)
		}
		wantState := domain.ExecutionRunning
		if index == 1 {
			wantState = domain.ExecutionReleased
		}
		if update.State != wantState || update.ErrorCode != domain.ExecutionErrorNone {
			t.Fatalf("iteration %d update = %#v", index, update)
		}
	}
	if manager.starts != 2 {
		t.Fatalf("idempotent lifecycle calls = %d", manager.starts)
	}
}

func TestAgentPrepareExactReplayNormalizesReleasedConflict(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	prepareCalls := 0
	manager := &fakeRunnerLifecycle{
		prepare: func(_ context.Context, request runner.Preparation) (runner.Snapshot, error) {
			prepareCalls++
			if prepareCalls == 1 {
				return runner.Snapshot{ExecutionID: request.ExecutionID, State: runner.StatePrepared, Prepared: true}, nil
			}
			return runner.Snapshot{ExecutionID: request.ExecutionID, State: runner.StateReleased}, runner.ErrExecutionConflict
		},
		inspect: func(context.Context, string) (runner.Snapshot, error) {
			return runner.Snapshot{}, runner.ErrExecutionNotFound
		},
		start: func(context.Context, runner.Start) (runner.Snapshot, error) {
			return runner.Snapshot{}, runner.ErrStartFailed
		},
		destroy: func(context.Context, string) (runner.Snapshot, error) {
			return runner.Snapshot{}, runner.ErrExecutionNotFound
		},
	}
	commandRuntime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	metadata := transport.CommandMetadata{
		CommandID:       "released-prepare-replay",
		ControllerEpoch: 1,
		ExecutionID:     "released-prepare-execution",
		ExpectedState:   domain.ExecutionReserved,
	}
	for index := range 2 {
		payload, err := transport.EncodePrepareCommandPayload(metadata, runner.OfficialRunnerVersion, false)
		if err != nil {
			t.Fatal(err)
		}
		envelope := transport.Envelope{ProtocolVersion: 1, MessageID: string(metadata.CommandID), Type: transport.MessagePrepare, Payload: payload}
		accepted, err := commandRuntime.Accept(context.Background(), &envelope)
		if err != nil {
			t.Fatal(err)
		}
		update, err := accepted.Execute(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		wantState := domain.ExecutionPreparing
		if index == 1 {
			wantState = domain.ExecutionReleased
		}
		if update.State != wantState ||
			update.ErrorCode != domain.ExecutionErrorNone ||
			update.Replayed != (index == 1) {
			t.Fatalf("iteration %d prepare replay update = %#v", index, update)
		}
	}
}

func TestAgentCommandSerializesConcurrentExecutionOrdering(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	var stateMu sync.Mutex
	localState := runner.StatePrepared
	inspectCalls := make(chan struct{}, 2)
	manager := &fakeRunnerLifecycle{
		start: func(_ context.Context, request runner.Start) (runner.Snapshot, error) {
			if err := request.JIT.Deliver(func(string) error { return nil }); err != nil {
				return runner.Snapshot{}, err
			}
			stateMu.Lock()
			localState = runner.StateRunning
			stateMu.Unlock()
			return runner.Snapshot{ExecutionID: request.ExecutionID, State: runner.StateRunning, Running: true}, nil
		},
		inspect: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			inspectCalls <- struct{}{}
			stateMu.Lock()
			defer stateMu.Unlock()
			if localState == "" {
				return runner.Snapshot{}, runner.ErrExecutionNotFound
			}
			return runner.Snapshot{ExecutionID: executionID, State: localState, Running: localState == runner.StateRunning}, nil
		},
		destroy: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			stateMu.Lock()
			localState = runner.StateReleased
			stateMu.Unlock()
			return runner.Snapshot{ExecutionID: executionID, State: runner.StateReleased}, nil
		},
	}
	commandRuntime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	startMetadata := transport.CommandMetadata{
		CommandID:       "ordered-start",
		ControllerEpoch: 1,
		ExecutionID:     "ordered-execution",
		ExpectedState:   domain.ExecutionPreparing,
	}
	startPayload, err := transport.EncodeStartCommandPayload(startMetadata, runner.OfficialRunnerVersion, false, "ordered-canary.example.test")
	if err != nil {
		t.Fatal(err)
	}
	startEnvelope := transport.Envelope{ProtocolVersion: 1, MessageID: string(startMetadata.CommandID), Type: transport.MessageStart, Payload: startPayload}
	start, err := commandRuntime.Accept(context.Background(), &startEnvelope)
	if err != nil {
		t.Fatal(err)
	}
	<-inspectCalls

	cancelMetadata := transport.CommandMetadata{
		CommandID:       "ordered-cancel",
		ControllerEpoch: 1,
		ExecutionID:     startMetadata.ExecutionID,
		ExpectedState:   domain.ExecutionRunning,
	}
	cancelPayload, err := transport.EncodeCancelCommandPayload(cancelMetadata)
	if err != nil {
		t.Fatal(err)
	}
	cancelEnvelope := transport.Envelope{ProtocolVersion: 1, MessageID: string(cancelMetadata.CommandID), Type: transport.MessageCancel, Payload: cancelPayload}
	type acceptResult struct {
		command *acceptedAgentCommand
		err     error
	}
	cancelResult := make(chan acceptResult, 1)
	go func() {
		command, err := commandRuntime.Accept(context.Background(), &cancelEnvelope)
		cancelResult <- acceptResult{command: command, err: err}
	}()
	deadline := time.Now().Add(time.Second)
	for {
		commandRuntime.locksMu.Lock()
		references := commandRuntime.locks[startMetadata.ExecutionID].references
		commandRuntime.locksMu.Unlock()
		if references == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("concurrent cancel did not reach the execution admission lock")
		}
		goruntime.Gosched()
	}
	select {
	case <-inspectCalls:
		t.Fatal("cancel inspected state before the accepted start completed")
	default:
	}
	if _, err := start.Execute(context.Background()); err != nil {
		t.Fatal(err)
	}
	var cancel *acceptedAgentCommand
	select {
	case result := <-cancelResult:
		if result.err != nil {
			t.Fatal(result.err)
		}
		cancel = result.command
	case <-time.After(time.Second):
		t.Fatal("cancel remained blocked after start completion")
	}
	select {
	case <-inspectCalls:
	default:
		t.Fatal("cancel did not inspect post-start state")
	}
	update, err := cancel.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if update.State != domain.ExecutionReleased || manager.starts != 1 || manager.destroys != 1 {
		t.Fatalf("ordered update=%#v starts=%d destroys=%d", update, manager.starts, manager.destroys)
	}
}

func TestAgentCancelOwnsTerminalOutboxWhenCompletionMonitorWasWaiting(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	var stateMu sync.Mutex
	localState := runner.StatePrepared
	waitEntered := make(chan struct{})
	runnerExited := make(chan struct{})
	manager := &fakeRunnerLifecycle{
		start: func(_ context.Context, request runner.Start) (runner.Snapshot, error) {
			if err := request.JIT.Deliver(func(string) error { return nil }); err != nil {
				return runner.Snapshot{}, err
			}
			stateMu.Lock()
			localState = runner.StateRunning
			stateMu.Unlock()
			return runner.Snapshot{
				ExecutionID: request.ExecutionID,
				State:       runner.StateRunning,
				Running:     true,
			}, nil
		},
		inspect: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			stateMu.Lock()
			defer stateMu.Unlock()
			return runner.Snapshot{
				ExecutionID: executionID,
				State:       localState,
				Running:     localState == runner.StateRunning,
			}, nil
		},
		wait: func(ctx context.Context, executionID string) (runner.Snapshot, error) {
			close(waitEntered)
			select {
			case <-runnerExited:
				return runner.Snapshot{
					ExecutionID: executionID,
					State:       runner.StateReleased,
				}, nil
			case <-ctx.Done():
				return runner.Snapshot{}, ctx.Err()
			}
		},
		destroy: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			stateMu.Lock()
			localState = runner.StateReleased
			stateMu.Unlock()
			return runner.Snapshot{ExecutionID: executionID, State: runner.StateReleased}, nil
		},
	}
	commandRuntime, err := NewAgentCommandRuntime(
		"node-1",
		agentStore,
		manager,
		currentRunnerPackage(t),
	)
	if err != nil {
		t.Fatal(err)
	}
	lifetime, stop := context.WithCancel(context.Background())
	defer stop()
	if err := commandRuntime.Start(lifetime); err != nil {
		t.Fatal(err)
	}
	executionID := domain.ExecutionID("cancel-monitor-execution")
	startMetadata := transport.CommandMetadata{
		CommandID:       "cancel-monitor-start",
		ControllerEpoch: 1,
		ExecutionID:     executionID,
		ExpectedState:   domain.ExecutionPreparing,
	}
	startPayload, err := transport.EncodeStartCommandPayload(
		startMetadata,
		runner.OfficialRunnerVersion,
		false,
		"cancel-monitor-canary.example.test",
	)
	if err != nil {
		t.Fatal(err)
	}
	startEnvelope := transport.Envelope{
		ProtocolVersion: 1,
		MessageID:       string(startMetadata.CommandID),
		Type:            transport.MessageStart,
		Payload:         startPayload,
	}
	start, err := commandRuntime.Accept(lifetime, &startEnvelope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := start.Execute(lifetime); err != nil {
		t.Fatal(err)
	}
	<-waitEntered

	cancelMetadata := transport.CommandMetadata{
		CommandID:       "cancel-monitor-cancel",
		ControllerEpoch: 1,
		ExecutionID:     executionID,
		ExpectedState:   domain.ExecutionRunning,
	}
	cancelPayload, err := transport.EncodeCancelCommandPayload(cancelMetadata)
	if err != nil {
		t.Fatal(err)
	}
	cancelEnvelope := transport.Envelope{
		ProtocolVersion: 1,
		MessageID:       string(cancelMetadata.CommandID),
		Type:            transport.MessageCancel,
		Payload:         cancelPayload,
	}
	cancel, err := commandRuntime.Accept(lifetime, &cancelEnvelope)
	if err != nil {
		t.Fatal(err)
	}
	cancelUpdate, err := cancel.Execute(lifetime)
	if err != nil || cancelUpdate.State != domain.ExecutionReleased {
		t.Fatalf("cancel update = (%#v, %v)", cancelUpdate, err)
	}
	close(runnerExited)

	deadline := time.Now().Add(time.Second)
	for {
		commandRuntime.lifetimeMu.Lock()
		monitorCount := len(commandRuntime.monitors)
		commandRuntime.lifetimeMu.Unlock()
		if monitorCount == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("completion monitor did not converge after cancel")
		}
		goruntime.Gosched()
	}
	pending, err := agentStore.PendingExecutionUpdates(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 ||
		pending[0].Update.State != domain.ExecutionRunning ||
		pending[1].Update.State != domain.ExecutionReleased ||
		pending[1].Update.CommandID != cancelMetadata.CommandID {
		t.Fatalf("cancel/monitor outbox = %#v", pending)
	}
	if manager.destroys != 1 {
		t.Fatalf("Destroy calls = %d, want 1", manager.destroys)
	}
}

func TestAgentCompletionMonitorCleansAndQueuesUpdatesWithoutSession(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	var stateMu sync.Mutex
	localState := runner.StatePrepared
	waitEntered := make(chan struct{})
	runnerExited := make(chan struct{})
	destroyed := make(chan struct{})
	destroyEvidence := make(chan error, 1)
	manager := &fakeRunnerLifecycle{
		start: func(_ context.Context, request runner.Start) (runner.Snapshot, error) {
			if err := request.JIT.Deliver(func(string) error { return nil }); err != nil {
				return runner.Snapshot{}, err
			}
			stateMu.Lock()
			localState = runner.StateRunning
			stateMu.Unlock()
			return runner.Snapshot{ExecutionID: request.ExecutionID, State: runner.StateRunning, Running: true}, nil
		},
		inspect: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			stateMu.Lock()
			defer stateMu.Unlock()
			return runner.Snapshot{
				ExecutionID: executionID,
				State:       localState,
				Prepared:    localState == runner.StatePrepared || localState == runner.StateRunning,
				Running:     localState == runner.StateRunning,
			}, nil
		},
		wait: func(ctx context.Context, executionID string) (runner.Snapshot, error) {
			close(waitEntered)
			select {
			case <-runnerExited:
				return runner.Snapshot{ExecutionID: executionID, State: runner.StateRunning, Running: true}, nil
			case <-ctx.Done():
				return runner.Snapshot{ExecutionID: executionID, State: runner.StateRunning, Running: true}, ctx.Err()
			}
		},
		destroy: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			pending, err := agentStore.PendingExecutionUpdates(context.Background())
			if err != nil {
				destroyEvidence <- err
			} else if len(pending) != 1 || pending[0].Update.State != domain.ExecutionRunning {
				destroyEvidence <- fmt.Errorf("pre-Destroy outbox = %#v", pending)
			} else {
				destroyEvidence <- nil
			}
			stateMu.Lock()
			localState = runner.StateReleased
			stateMu.Unlock()
			close(destroyed)
			return runner.Snapshot{ExecutionID: executionID, State: runner.StateReleased}, nil
		},
	}
	commandRuntime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	lifetime, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := commandRuntime.Start(lifetime); err != nil {
		t.Fatal(err)
	}
	metadata := transport.CommandMetadata{
		CommandID:       "monitored-start",
		ControllerEpoch: 1,
		ExecutionID:     "monitored-execution",
		ExpectedState:   domain.ExecutionPreparing,
	}
	payload, err := transport.EncodeStartCommandPayload(metadata, runner.OfficialRunnerVersion, false, "monitor-canary.example.test")
	if err != nil {
		t.Fatal(err)
	}
	envelope := transport.Envelope{ProtocolVersion: 1, MessageID: string(metadata.CommandID), Type: transport.MessageStart, Payload: payload}
	accepted, err := commandRuntime.Accept(lifetime, &envelope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := accepted.Execute(lifetime); err != nil {
		t.Fatal(err)
	}
	<-waitEntered
	close(runnerExited)
	select {
	case <-destroyed:
	case <-time.After(time.Second):
		t.Fatal("completion monitor did not destroy the finished runner")
	}
	if err := <-destroyEvidence; err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		pending, err := agentStore.PendingExecutionUpdates(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(pending) == 2 {
			states := []domain.ExecutionState{
				pending[0].Update.State,
				pending[1].Update.State,
			}
			want := []domain.ExecutionState{
				domain.ExecutionRunning,
				domain.ExecutionReleased,
			}
			for index := range want {
				if states[index] != want[index] {
					t.Fatalf("outbox states = %#v", states)
				}
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("completion outbox did not reach terminal state: %#v", pending)
		}
		goruntime.Gosched()
	}
}

func TestAgentCompletionWaitFailureStillCleansWithDeliverableUpdates(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	var stateMu sync.Mutex
	localState := runner.StatePrepared
	destroyed := make(chan struct{})
	manager := &fakeRunnerLifecycle{
		start: func(_ context.Context, request runner.Start) (runner.Snapshot, error) {
			if err := request.JIT.Deliver(func(string) error { return nil }); err != nil {
				return runner.Snapshot{}, err
			}
			stateMu.Lock()
			localState = runner.StateRunning
			stateMu.Unlock()
			return runner.Snapshot{ExecutionID: request.ExecutionID, State: runner.StateRunning, Running: true}, nil
		},
		inspect: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			stateMu.Lock()
			defer stateMu.Unlock()
			return runner.Snapshot{
				ExecutionID: executionID,
				State:       localState,
				Prepared:    true,
				Running:     localState == runner.StateRunning,
			}, nil
		},
		wait: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			return runner.Snapshot{ExecutionID: executionID, State: runner.StateRunning, Running: true}, runner.ErrReconciliationRequired
		},
		destroy: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			stateMu.Lock()
			localState = runner.StateReleased
			stateMu.Unlock()
			close(destroyed)
			return runner.Snapshot{ExecutionID: executionID, State: runner.StateReleased}, nil
		},
	}
	commandRuntime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	lifetime, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := commandRuntime.Start(lifetime); err != nil {
		t.Fatal(err)
	}
	metadata := transport.CommandMetadata{
		CommandID:       "wait-failure-start",
		ControllerEpoch: 1,
		ExecutionID:     "wait-failure-execution",
		ExpectedState:   domain.ExecutionPreparing,
	}
	payload, err := transport.EncodeStartCommandPayload(metadata, runner.OfficialRunnerVersion, false, "wait-failure-canary.example.test")
	if err != nil {
		t.Fatal(err)
	}
	envelope := transport.Envelope{ProtocolVersion: 1, MessageID: string(metadata.CommandID), Type: transport.MessageStart, Payload: payload}
	accepted, err := commandRuntime.Accept(lifetime, &envelope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := accepted.Execute(lifetime); err != nil {
		t.Fatal(err)
	}
	select {
	case <-destroyed:
	case <-time.After(time.Second):
		t.Fatal("Wait failure prevented Destroy")
	}
	deadline := time.Now().Add(time.Second)
	for {
		pending, err := agentStore.PendingExecutionUpdates(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(pending) == 2 {
			if pending[1].Update.State != domain.ExecutionReleased ||
				pending[1].Update.ErrorCode != domain.ExecutionErrorNone {
				t.Fatalf("Wait failure updates = %#v", pending)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Wait failure updates did not become deliverable: %#v", pending)
		}
		goruntime.Gosched()
	}
	commandRuntime.lifetimeMu.Lock()
	waitFailures := commandRuntime.waitFailures
	commandRuntime.lifetimeMu.Unlock()
	if waitFailures != 1 || manager.destroys != 1 {
		t.Fatalf("Wait failure observations=%d destroys=%d", waitFailures, manager.destroys)
	}
}

func TestAgentLifetimeCancellationLeavesRunningForReconciliation(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	waitEntered := make(chan struct{})
	manager := &fakeRunnerLifecycle{
		start: func(_ context.Context, request runner.Start) (runner.Snapshot, error) {
			if err := request.JIT.Deliver(func(string) error { return nil }); err != nil {
				return runner.Snapshot{}, err
			}
			return runner.Snapshot{ExecutionID: request.ExecutionID, State: runner.StateRunning, Running: true}, nil
		},
		wait: func(ctx context.Context, executionID string) (runner.Snapshot, error) {
			close(waitEntered)
			<-ctx.Done()
			return runner.Snapshot{ExecutionID: executionID, State: runner.StateRunning, Running: true}, ctx.Err()
		},
		destroy: func(context.Context, string) (runner.Snapshot, error) {
			return runner.Snapshot{}, errors.New("destroy must not run during Agent shutdown")
		},
	}
	commandRuntime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	lifetime, cancel := context.WithCancel(context.Background())
	if err := commandRuntime.Start(lifetime); err != nil {
		t.Fatal(err)
	}
	metadata := transport.CommandMetadata{
		CommandID:       "shutdown-start",
		ControllerEpoch: 1,
		ExecutionID:     "shutdown-execution",
		ExpectedState:   domain.ExecutionPreparing,
	}
	payload, err := transport.EncodeStartCommandPayload(metadata, runner.OfficialRunnerVersion, false, "shutdown-canary.example.test")
	if err != nil {
		t.Fatal(err)
	}
	envelope := transport.Envelope{ProtocolVersion: 1, MessageID: string(metadata.CommandID), Type: transport.MessageStart, Payload: payload}
	accepted, err := commandRuntime.Accept(lifetime, &envelope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := accepted.Execute(lifetime); err != nil {
		t.Fatal(err)
	}
	<-waitEntered
	cancel()
	deadline := time.Now().Add(time.Second)
	for {
		commandRuntime.lifetimeMu.Lock()
		monitors := len(commandRuntime.monitors)
		commandRuntime.lifetimeMu.Unlock()
		if monitors == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cancelled completion monitor did not stop")
		}
		goruntime.Gosched()
	}
	if manager.destroys != 0 {
		t.Fatalf("Agent shutdown destroyed a running job: %d", manager.destroys)
	}
	pending, err := agentStore.PendingExecutionUpdates(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].Update.State != domain.ExecutionRunning {
		t.Fatalf("shutdown outbox = %#v", pending)
	}
}

func TestAgentCompletionOutboxFailureStillDestroysRunner(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	waitEntered := make(chan struct{})
	runnerExited := make(chan struct{})
	destroyed := make(chan struct{})
	var stateMu sync.Mutex
	localState := runner.StatePrepared
	manager := &fakeRunnerLifecycle{
		start: func(_ context.Context, request runner.Start) (runner.Snapshot, error) {
			if err := request.JIT.Deliver(func(string) error { return nil }); err != nil {
				return runner.Snapshot{}, err
			}
			stateMu.Lock()
			localState = runner.StateRunning
			stateMu.Unlock()
			return runner.Snapshot{ExecutionID: request.ExecutionID, State: runner.StateRunning, Running: true}, nil
		},
		inspect: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			stateMu.Lock()
			defer stateMu.Unlock()
			return runner.Snapshot{
				ExecutionID: executionID,
				State:       localState,
				Prepared:    true,
				Running:     localState == runner.StateRunning,
			}, nil
		},
		wait: func(ctx context.Context, executionID string) (runner.Snapshot, error) {
			close(waitEntered)
			select {
			case <-runnerExited:
				return runner.Snapshot{ExecutionID: executionID, State: runner.StateRunning, Running: true}, nil
			case <-ctx.Done():
				return runner.Snapshot{}, ctx.Err()
			}
		},
		destroy: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			close(destroyed)
			return runner.Snapshot{ExecutionID: executionID, State: runner.StateReleased}, nil
		},
	}
	commandRuntime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	lifetime, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := commandRuntime.Start(lifetime); err != nil {
		t.Fatal(err)
	}
	metadata := transport.CommandMetadata{
		CommandID:       "outbox-failure-start",
		ControllerEpoch: 1,
		ExecutionID:     "outbox-failure-execution",
		ExpectedState:   domain.ExecutionPreparing,
	}
	payload, err := transport.EncodeStartCommandPayload(metadata, runner.OfficialRunnerVersion, false, "outbox-failure-canary.example.test")
	if err != nil {
		t.Fatal(err)
	}
	envelope := transport.Envelope{ProtocolVersion: 1, MessageID: string(metadata.CommandID), Type: transport.MessageStart, Payload: payload}
	accepted, err := commandRuntime.Accept(lifetime, &envelope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := accepted.Execute(lifetime); err != nil {
		t.Fatal(err)
	}
	<-waitEntered
	if err := agentStore.Close(); err != nil {
		t.Fatal(err)
	}
	close(runnerExited)
	select {
	case <-destroyed:
	case <-time.After(time.Second):
		t.Fatal("outbox failure prevented runner cleanup")
	}
	deadline := time.Now().Add(time.Second)
	for {
		commandRuntime.lifetimeMu.Lock()
		failed := commandRuntime.monitorFailed
		commandRuntime.lifetimeMu.Unlock()
		if failed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("outbox failure was not surfaced")
		}
		goruntime.Gosched()
	}
	if _, err := commandRuntime.PendingUpdates(context.Background()); !errors.Is(err, ErrAgentRuntimeDegraded) {
		t.Fatalf("degraded completion monitor pending error = %v", err)
	}
}

func TestAgentRunningUpdateJournalFailureStillStartsCleanupMonitor(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	waitEntered := make(chan struct{})
	runnerExited := make(chan struct{})
	destroyed := make(chan struct{})
	var stateMu sync.Mutex
	localState := runner.StatePrepared
	manager := &fakeRunnerLifecycle{
		start: func(_ context.Context, request runner.Start) (runner.Snapshot, error) {
			if err := request.JIT.Deliver(func(string) error { return nil }); err != nil {
				return runner.Snapshot{}, err
			}
			stateMu.Lock()
			localState = runner.StateRunning
			stateMu.Unlock()
			return runner.Snapshot{ExecutionID: request.ExecutionID, State: runner.StateRunning, Running: true}, nil
		},
		inspect: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			stateMu.Lock()
			defer stateMu.Unlock()
			return runner.Snapshot{
				ExecutionID: executionID,
				State:       localState,
				Prepared:    true,
				Running:     localState == runner.StateRunning,
			}, nil
		},
		wait: func(ctx context.Context, executionID string) (runner.Snapshot, error) {
			close(waitEntered)
			select {
			case <-runnerExited:
				return runner.Snapshot{ExecutionID: executionID, State: runner.StateRunning, Running: true}, nil
			case <-ctx.Done():
				return runner.Snapshot{}, ctx.Err()
			}
		},
		destroy: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			stateMu.Lock()
			localState = runner.StateReleased
			stateMu.Unlock()
			close(destroyed)
			return runner.Snapshot{ExecutionID: executionID, State: runner.StateReleased}, nil
		},
	}
	commandRuntime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	lifetime, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := commandRuntime.Start(lifetime); err != nil {
		t.Fatal(err)
	}
	metadata := transport.CommandMetadata{
		CommandID:       "running-journal-failure-start",
		ControllerEpoch: 1,
		ExecutionID:     "running-journal-failure-execution",
		ExpectedState:   domain.ExecutionPreparing,
	}
	payload, err := transport.EncodeStartCommandPayload(metadata, runner.OfficialRunnerVersion, false, "running-journal-failure-canary.example.test")
	if err != nil {
		t.Fatal(err)
	}
	envelope := transport.Envelope{ProtocolVersion: 1, MessageID: string(metadata.CommandID), Type: transport.MessageStart, Payload: payload}
	accepted, err := commandRuntime.Accept(lifetime, &envelope)
	if err != nil {
		t.Fatal(err)
	}
	if err := agentStore.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := accepted.Execute(lifetime); err == nil {
		t.Fatal("running update unexpectedly persisted after journal close")
	}
	select {
	case <-waitEntered:
	case <-time.After(time.Second):
		t.Fatal("running journal failure did not start completion monitoring")
	}
	close(runnerExited)
	select {
	case <-destroyed:
	case <-time.After(time.Second):
		t.Fatal("running journal failure stranded the runner")
	}
}

func TestAgentCompletionMonitorClassifiesObservedCleanupFailure(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	manager := &fakeRunnerLifecycle{
		inspect: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			return runner.Snapshot{
				ExecutionID: executionID,
				State:       runner.StateCleanupFailed,
				Quarantined: true,
			}, runner.ErrQuarantined
		},
		wait: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			return runner.Snapshot{ExecutionID: executionID, State: runner.StateRunning, Running: true}, nil
		},
		destroy: func(context.Context, string) (runner.Snapshot, error) {
			return runner.Snapshot{}, errors.New("terminal cleanup failure must not be destroyed again")
		},
	}
	commandRuntime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	metadata := transport.CommandMetadata{
		CommandID:       "observed-cleanup-failure-command",
		ControllerEpoch: 1,
		ExecutionID:     "observed-cleanup-failure-execution",
		ExpectedState:   domain.ExecutionPreparing,
	}
	commandRuntime.monitorCompletion(context.Background(), metadata, false)
	pending, err := agentStore.PendingExecutionUpdates(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 ||
		pending[0].Update.State != domain.ExecutionCleanupFailed ||
		pending[0].Update.ErrorCode != domain.ExecutionErrorQuarantined {
		t.Fatalf("observed cleanup failure update = %#v", pending)
	}
	if manager.destroys != 0 {
		t.Fatalf("observed terminal state was destroyed again: %d", manager.destroys)
	}
}

func TestAgentStartupRecoveryPublishesAcceptedPrepareWithoutRuntimeAsFailed(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	accepted := store.AcceptedAgentCommand{
		Type: domain.CommandPrepare,
		Command: domain.Command{
			ID:              "startup-prepare-command",
			ControllerEpoch: 1,
			ExecutionID:     "startup-prepare-execution",
			ExpectedState:   domain.ExecutionReserved,
			PayloadDigest:   strings.Repeat("a", 64),
		},
	}
	if replayed, err := agentStore.RecordTypedCommand(context.Background(), accepted); err != nil || replayed {
		t.Fatalf("record accepted Prepare = (%t, %v)", replayed, err)
	}
	manager := &fakeRunnerLifecycle{
		start: func(context.Context, runner.Start) (runner.Snapshot, error) {
			return runner.Snapshot{}, errors.New("startup recovery must not start a runner")
		},
		destroy: func(context.Context, string) (runner.Snapshot, error) {
			return runner.Snapshot{}, errors.New("missing Prepare runtime must not be destroyed")
		},
	}
	lifetime, cancel := context.WithCancel(context.Background())
	defer cancel()
	commandRuntime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := commandRuntime.Start(lifetime); err != nil {
		t.Fatal(err)
	}
	pending, err := agentStore.PendingExecutionUpdates(context.Background())
	if err != nil || len(pending) != 1 {
		t.Fatalf("recovered Prepare outbox = (%#v, %v)", pending, err)
	}
	if update := pending[0].Update; update.CommandID != accepted.Command.ID ||
		update.State != domain.ExecutionFailed ||
		update.ErrorCode != domain.ExecutionErrorReconciliation {
		t.Fatalf("recovered Prepare update = %#v", update)
	}

	// Reopening the runtime is an exact recovery replay, not a second outbox
	// result for the same accepted command.
	secondLifetime, secondCancel := context.WithCancel(context.Background())
	defer secondCancel()
	secondRuntime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := secondRuntime.Start(secondLifetime); err != nil {
		t.Fatal(err)
	}
	pending, err = agentStore.PendingExecutionUpdates(context.Background())
	if err != nil || len(pending) != 1 {
		t.Fatalf("replayed Prepare recovery outbox = (%#v, %v)", pending, err)
	}
}

func TestAgentStartupRecoveryRejectsAcceptedStartWithoutRuntime(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	accepted := store.AcceptedAgentCommand{
		Type: domain.CommandStart,
		Command: domain.Command{
			ID:              "startup-missing-start-command",
			ControllerEpoch: 1,
			ExecutionID:     "startup-missing-start-execution",
			ExpectedState:   domain.ExecutionPreparing,
			PayloadDigest:   strings.Repeat("b", 64),
		},
	}
	if replayed, err := agentStore.RecordTypedCommand(context.Background(), accepted); err != nil || replayed {
		t.Fatalf("record accepted Start = (%t, %v)", replayed, err)
	}
	manager := &fakeRunnerLifecycle{
		start: func(context.Context, runner.Start) (runner.Snapshot, error) {
			return runner.Snapshot{}, errors.New("startup recovery must not regenerate JIT")
		},
		destroy: func(context.Context, string) (runner.Snapshot, error) {
			return runner.Snapshot{}, errors.New("missing Start runtime has no attributable root")
		},
	}
	commandRuntime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := commandRuntime.Start(context.Background()); !errors.Is(err, runner.ErrJournal) {
		t.Fatalf("missing accepted Start runtime error = %v", err)
	}
	pending, err := agentStore.PendingExecutionUpdates(context.Background())
	if err != nil || len(pending) != 0 {
		t.Fatalf("missing Start invented outbox evidence = (%#v, %v)", pending, err)
	}
}

func TestAgentStartupRecoveryCleansAcceptedStartWhoseJITWasLost(t *testing.T) {
	ctx := context.Background()
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	executionID := "startup-lost-jit-execution"
	record := runner.Record{
		ExecutionID: executionID,
		SpecDigest:  strings.Repeat("c", 64),
		State:       runner.StatePrepared,
		RootName:    runnerJournalRootName(executionID),
		WorkspaceRef: runner.WorkspaceRef{
			Backend: "test-workspace",
			OwnerID: "workspace-owner",
		},
	}
	if _, created, err := agentStore.RunnerJournal().Create(ctx, strings.Repeat("d", 32), record); err != nil || !created {
		t.Fatalf("create prepared runtime = (%t, %v)", created, err)
	}
	accepted := store.AcceptedAgentCommand{
		Type: domain.CommandStart,
		Command: domain.Command{
			ID:              "startup-lost-jit-command",
			ControllerEpoch: 1,
			ExecutionID:     domain.ExecutionID(executionID),
			ExpectedState:   domain.ExecutionPreparing,
			PayloadDigest:   strings.Repeat("e", 64),
		},
	}
	if replayed, err := agentStore.RecordTypedCommand(ctx, accepted); err != nil || replayed {
		t.Fatalf("record accepted Start = (%t, %v)", replayed, err)
	}
	base := &fakeRunnerLifecycle{
		start: func(context.Context, runner.Start) (runner.Snapshot, error) {
			return runner.Snapshot{}, errors.New("startup recovery must not replay JIT")
		},
		destroy: func(_ context.Context, got string) (runner.Snapshot, error) {
			if got != executionID {
				t.Fatalf("destroy execution = %q", got)
			}
			return runner.Snapshot{ExecutionID: got, State: runner.StateReleased}, nil
		},
	}
	manager := &fakeRecoveringRunnerLifecycle{
		fakeRunnerLifecycle: base,
		recover: func(_ context.Context, got string) (runner.Snapshot, error) {
			if got != executionID {
				t.Fatalf("recover execution = %q", got)
			}
			return runner.Snapshot{ExecutionID: got, State: runner.StatePrepared, Prepared: true}, nil
		},
	}
	lifetime, cancel := context.WithCancel(ctx)
	defer cancel()
	commandRuntime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := commandRuntime.Start(lifetime); err != nil {
		t.Fatal(err)
	}
	pending, err := agentStore.PendingExecutionUpdates(ctx)
	if err != nil || len(pending) != 1 {
		t.Fatalf("lost JIT recovery outbox = (%#v, %v)", pending, err)
	}
	if update := pending[0].Update; update.State != domain.ExecutionReleased ||
		update.ErrorCode != domain.ExecutionErrorNone ||
		update.CommandID != accepted.Command.ID {
		t.Fatalf("lost JIT recovery update = %#v", update)
	}
	if base.starts != 0 || base.destroys != 1 {
		t.Fatalf("lost JIT recovery starts=%d destroys=%d", base.starts, base.destroys)
	}
}

func TestAgentStartupRecoveryPersistsRunningBeforeImmediateCompletion(t *testing.T) {
	ctx := context.Background()
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	executionID := "startup-running-execution"
	record := runner.Record{
		ExecutionID: executionID,
		SpecDigest:  strings.Repeat("1", 64),
		JITDigest:   strings.Repeat("2", 64),
		State:       runner.StateRunning,
		RootName:    runnerJournalRootName(executionID),
		PID:         123,
		WorkspaceRef: runner.WorkspaceRef{
			Backend: "test-workspace",
			OwnerID: "workspace-owner",
		},
		Containment: runner.ContainmentRef{
			Backend:    "test-containment",
			OwnerID:    "containment-owner",
			FenceToken: strings.Repeat("3", 32),
		},
	}
	if _, created, err := agentStore.RunnerJournal().Create(ctx, strings.Repeat("4", 32), record); err != nil || !created {
		t.Fatalf("create running runtime = (%t, %v)", created, err)
	}
	accepted := store.AcceptedAgentCommand{
		Type: domain.CommandStart,
		Command: domain.Command{
			ID:              "startup-running-command",
			ControllerEpoch: 1,
			ExecutionID:     domain.ExecutionID(executionID),
			ExpectedState:   domain.ExecutionPreparing,
			PayloadDigest:   strings.Repeat("5", 64),
		},
	}
	if replayed, err := agentStore.RecordTypedCommand(ctx, accepted); err != nil || replayed {
		t.Fatalf("record accepted Start = (%t, %v)", replayed, err)
	}
	base := &fakeRunnerLifecycle{
		start: func(context.Context, runner.Start) (runner.Snapshot, error) {
			return runner.Snapshot{}, errors.New("running recovery must not start another runner")
		},
		inspect: func(_ context.Context, got string) (runner.Snapshot, error) {
			return runner.Snapshot{ExecutionID: got, State: runner.StateRunning, Running: true}, nil
		},
		wait: func(_ context.Context, got string) (runner.Snapshot, error) {
			return runner.Snapshot{ExecutionID: got, State: runner.StateRunning, Running: true}, nil
		},
		destroy: func(_ context.Context, got string) (runner.Snapshot, error) {
			return runner.Snapshot{ExecutionID: got, State: runner.StateReleased}, nil
		},
	}
	manager := &fakeRecoveringRunnerLifecycle{
		fakeRunnerLifecycle: base,
		recover: func(_ context.Context, got string) (runner.Snapshot, error) {
			return runner.Snapshot{ExecutionID: got, State: runner.StateRunning, Running: true}, nil
		},
	}
	lifetime, cancel := context.WithCancel(ctx)
	defer cancel()
	commandRuntime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := commandRuntime.Start(lifetime); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	var pending []store.PendingExecutionUpdate
	for {
		pending, err = agentStore.PendingExecutionUpdates(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(pending) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("running recovery outbox = %#v", pending)
		}
		goruntime.Gosched()
	}
	if pending[0].Update.State != domain.ExecutionRunning ||
		pending[1].Update.State != domain.ExecutionReleased {
		t.Fatalf("running recovery ordering = %#v", pending)
	}
	if base.starts != 0 || base.destroys != 1 {
		t.Fatalf("running recovery starts=%d destroys=%d", base.starts, base.destroys)
	}
}

func TestAgentStartupRecoveryResumesAcceptedCancelBeforePublishingState(t *testing.T) {
	ctx := context.Background()
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	executionID := "startup-cancel-execution"
	record := runner.Record{
		ExecutionID: executionID,
		SpecDigest:  strings.Repeat("6", 64),
		JITDigest:   strings.Repeat("7", 64),
		State:       runner.StateRunning,
		RootName:    runnerJournalRootName(executionID),
		PID:         456,
		WorkspaceRef: runner.WorkspaceRef{
			Backend: "test-workspace",
			OwnerID: "workspace-owner",
		},
		Containment: runner.ContainmentRef{
			Backend:    "test-containment",
			OwnerID:    "containment-owner",
			FenceToken: strings.Repeat("8", 32),
		},
	}
	if _, created, err := agentStore.RunnerJournal().Create(ctx, strings.Repeat("9", 32), record); err != nil || !created {
		t.Fatalf("create running runtime = (%t, %v)", created, err)
	}
	accepted := store.AcceptedAgentCommand{
		Type: domain.CommandCancel,
		Command: domain.Command{
			ID:              "startup-cancel-command",
			ControllerEpoch: 1,
			ExecutionID:     domain.ExecutionID(executionID),
			ExpectedState:   domain.ExecutionRunning,
			PayloadDigest:   strings.Repeat("a", 64),
		},
	}
	if replayed, err := agentStore.RecordTypedCommand(ctx, accepted); err != nil || replayed {
		t.Fatalf("record accepted Cancel = (%t, %v)", replayed, err)
	}
	base := &fakeRunnerLifecycle{
		start: func(context.Context, runner.Start) (runner.Snapshot, error) {
			return runner.Snapshot{}, errors.New("cancel recovery must not start a runner")
		},
		wait: func(ctx context.Context, _ string) (runner.Snapshot, error) {
			<-ctx.Done()
			return runner.Snapshot{}, ctx.Err()
		},
		destroy: func(_ context.Context, got string) (runner.Snapshot, error) {
			if got != executionID {
				t.Fatalf("destroy execution = %q", got)
			}
			return runner.Snapshot{ExecutionID: got, State: runner.StateReleased}, nil
		},
	}
	manager := &fakeRecoveringRunnerLifecycle{
		fakeRunnerLifecycle: base,
		recover: func(_ context.Context, got string) (runner.Snapshot, error) {
			return runner.Snapshot{ExecutionID: got, State: runner.StateRunning, Running: true}, nil
		},
	}
	lifetime, cancel := context.WithCancel(ctx)
	defer cancel()
	commandRuntime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := commandRuntime.Start(lifetime); err != nil {
		t.Fatal(err)
	}
	pending, err := agentStore.PendingExecutionUpdates(ctx)
	if err != nil || len(pending) != 1 {
		t.Fatalf("cancel recovery outbox = (%#v, %v)", pending, err)
	}
	if update := pending[0].Update; update.State != domain.ExecutionReleased ||
		update.ErrorCode != domain.ExecutionErrorNone ||
		update.CommandID != accepted.Command.ID {
		t.Fatalf("cancel recovery update = %#v", update)
	}
	if base.starts != 0 || base.destroys != 1 {
		t.Fatalf("cancel recovery starts=%d destroys=%d", base.starts, base.destroys)
	}
}

func TestAgentSessionPersistsRunningOutcomeBeforeCommandAck(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	startCompleted := make(chan struct{})
	manager := &fakeRunnerLifecycle{
		start: func(_ context.Context, request runner.Start) (runner.Snapshot, error) {
			if err := request.JIT.Deliver(func(string) error { return nil }); err != nil {
				return runner.Snapshot{}, err
			}
			close(startCompleted)
			return runner.Snapshot{ExecutionID: request.ExecutionID, State: runner.StateRunning, Running: true}, nil
		},
		destroy: func(context.Context, string) (runner.Snapshot, error) {
			return runner.Snapshot{}, runner.ErrExecutionNotFound
		},
	}
	commandRuntime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	metadata := transport.CommandMetadata{
		CommandID:       "command-session",
		ControllerEpoch: 1,
		ExecutionID:     "execution-session",
		ExpectedState:   domain.ExecutionPreparing,
	}
	commandPayload, err := transport.EncodeStartCommandPayload(metadata, runner.OfficialRunnerVersion, true, "session-jit.example.test")
	if err != nil {
		t.Fatal(err)
	}

	serverResult := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			serverResult <- err
			return
		}
		defer connection.CloseNow()
		ctx, cancel := context.WithTimeout(request.Context(), 5*time.Second)
		defer cancel()
		for _, expected := range []transport.MessageType{transport.MessageHello, transport.MessageSnapshot} {
			envelope, err := transport.ReadEnvelope(ctx, connection)
			if err != nil || envelope.Type != expected {
				serverResult <- fmt.Errorf("initial %s message: %#v, %w", expected, envelope, err)
				return
			}
			if err := writeAgentAck(ctx, connection, envelope.MessageID); err != nil {
				serverResult <- err
				return
			}
		}
		if err := transport.WriteEnvelope(ctx, connection, transport.Envelope{
			ProtocolVersion: transport.ProtocolVersion,
			MessageID:       string(metadata.CommandID),
			Type:            transport.MessageStart,
			Payload:         commandPayload,
		}); err != nil {
			serverResult <- err
			return
		}
		ack, err := transport.ReadEnvelope(ctx, connection)
		if err != nil || ack.Type != transport.MessageAck {
			serverResult <- fmt.Errorf("command acknowledgement: %#v, %w", ack, err)
			return
		}
		snapshot, err := agentStore.Snapshot(ctx)
		if err != nil || len(snapshot.Commands) != 1 {
			serverResult <- fmt.Errorf("state at command acknowledgement: commands=%d error=%w", len(snapshot.Commands), err)
			return
		}
		pending, err := agentStore.PendingExecutionUpdates(ctx)
		if err != nil || len(pending) != 1 ||
			pending[0].Update.State != domain.ExecutionRunning {
			serverResult <- fmt.Errorf("durable outcome at command acknowledgement: pending=%#v error=%w", pending, err)
			return
		}
		select {
		case <-startCompleted:
		default:
			serverResult <- errors.New("command was acknowledged before runner start completed")
			return
		}
		updateEnvelope, err := transport.ReadEnvelope(ctx, connection)
		if err != nil || updateEnvelope.Type != transport.MessageExecutionUpdate {
			serverResult <- fmt.Errorf("execution update: %#v, %w", updateEnvelope, err)
			return
		}
		update, err := transport.DecodeExecutionUpdate(updateEnvelope.Payload)
		if err != nil || update.State != domain.ExecutionRunning {
			serverResult <- fmt.Errorf("execution update payload: %#v error=%w", update, err)
			return
		}
		select {
		case <-startCompleted:
		default:
			serverResult <- errors.New("execution update arrived before runner start completed")
			return
		}
		if err := writeAgentAck(ctx, connection, updateEnvelope.MessageID); err != nil {
			serverResult <- err
			return
		}
		serverResult <- nil
		_ = connection.Close(websocket.StatusNormalClosure, "complete")
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	sessionErr := runAgentSession(ctx, connection, &AgentState{NodeID: "node-1", Store: agentStore}, commandRuntime)
	if status := websocket.CloseStatus(sessionErr); status != websocket.StatusNormalClosure {
		t.Fatalf("session error = %v", sessionErr)
	}
	if err := <-serverResult; err != nil {
		t.Fatal(err)
	}
	pending, err := agentStore.PendingExecutionUpdates(context.Background())
	if err != nil || len(pending) != 0 {
		t.Fatalf("acknowledged session outbox = %#v, %v", pending, err)
	}
}

func TestAgentSessionReplaysUnacknowledgedOutboxAfterDisconnect(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	manager := &fakeRunnerLifecycle{
		start: func(_ context.Context, request runner.Start) (runner.Snapshot, error) {
			if err := request.JIT.Deliver(func(string) error { return nil }); err != nil {
				return runner.Snapshot{}, err
			}
			return runner.Snapshot{ExecutionID: request.ExecutionID, State: runner.StateRunning, Running: true}, nil
		},
		destroy: func(context.Context, string) (runner.Snapshot, error) {
			return runner.Snapshot{}, runner.ErrExecutionNotFound
		},
	}
	commandRuntime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	metadata := transport.CommandMetadata{
		CommandID:       "outbox-replay-command",
		ControllerEpoch: 1,
		ExecutionID:     "outbox-replay-execution",
		ExpectedState:   domain.ExecutionPreparing,
	}
	payload, err := transport.EncodeStartCommandPayload(metadata, runner.OfficialRunnerVersion, false, "outbox-replay-canary.example.test")
	if err != nil {
		t.Fatal(err)
	}
	envelope := transport.Envelope{ProtocolVersion: 1, MessageID: string(metadata.CommandID), Type: transport.MessageStart, Payload: payload}
	accepted, err := commandRuntime.Accept(context.Background(), &envelope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := accepted.Execute(context.Background()); err != nil {
		t.Fatal(err)
	}

	var firstMessageID string
	runSession := func(acknowledge bool) error {
		serverResult := make(chan error, 1)
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			connection, err := websocket.Accept(writer, request, nil)
			if err != nil {
				serverResult <- err
				return
			}
			defer connection.CloseNow()
			ctx, cancel := context.WithTimeout(request.Context(), 5*time.Second)
			defer cancel()
			for _, expected := range []transport.MessageType{transport.MessageHello, transport.MessageSnapshot} {
				initial, err := transport.ReadEnvelope(ctx, connection)
				if err != nil || initial.Type != expected {
					serverResult <- fmt.Errorf("initial %s: %#v, %w", expected, initial, err)
					return
				}
				if expected == transport.MessageSnapshot {
					snapshot, decodeErr := transport.DecodeAgentSnapshot(initial.Payload)
					if decodeErr != nil || len(snapshot.Commands) != 1 ||
						!snapshot.NativeRunnerReady ||
						snapshot.Commands[0].ID != metadata.CommandID {
						serverResult <- fmt.Errorf("reconciliation snapshot: %#v, %v", snapshot, decodeErr)
						return
					}
				}
				if err := writeAgentAck(ctx, connection, initial.MessageID); err != nil {
					serverResult <- err
					return
				}
			}
			update, err := transport.ReadEnvelope(ctx, connection)
			if err != nil || update.Type != transport.MessageExecutionUpdate {
				serverResult <- fmt.Errorf("outbox update: %#v, %w", update, err)
				return
			}
			if firstMessageID == "" {
				firstMessageID = update.MessageID
			} else if update.MessageID != firstMessageID {
				serverResult <- fmt.Errorf("outbox message ID changed: %s != %s", update.MessageID, firstMessageID)
				return
			}
			if acknowledge {
				if err := writeAgentAck(ctx, connection, update.MessageID); err != nil {
					serverResult <- err
					return
				}
			}
			serverResult <- nil
			_ = connection.Close(websocket.StatusNormalClosure, "complete")
		}))
		defer server.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
		if err != nil {
			return err
		}
		defer connection.CloseNow()
		sessionErr := runAgentSession(ctx, connection, &AgentState{NodeID: "node-1", Store: agentStore}, commandRuntime)
		if status := websocket.CloseStatus(sessionErr); status != websocket.StatusNormalClosure {
			return sessionErr
		}
		return <-serverResult
	}
	if err := runSession(false); err != nil {
		t.Fatal(err)
	}
	pending, err := agentStore.PendingExecutionUpdates(context.Background())
	if err != nil || len(pending) != 1 {
		t.Fatalf("unacknowledged outbox = %#v, %v", pending, err)
	}
	if err := runSession(true); err != nil {
		t.Fatal(err)
	}
	pending, err = agentStore.PendingExecutionUpdates(context.Background())
	if err != nil || len(pending) != 0 {
		t.Fatalf("acknowledged replay outbox = %#v, %v", pending, err)
	}
}

func TestAgentSessionReturnsFatalRuntimeDegradationInsteadOfReconnectError(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	if err := agentStore.Close(); err != nil {
		t.Fatal(err)
	}
	serverResult := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			serverResult <- err
			return
		}
		defer connection.CloseNow()
		ctx, cancel := context.WithTimeout(request.Context(), 5*time.Second)
		defer cancel()
		hello, err := transport.ReadEnvelope(ctx, connection)
		if err != nil || hello.Type != transport.MessageHello {
			serverResult <- fmt.Errorf("hello: %#v, %w", hello, err)
			return
		}
		if err := writeAgentAck(ctx, connection, hello.MessageID); err != nil {
			serverResult <- err
			return
		}
		serverResult <- nil
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	err = runAgentSession(ctx, connection, &AgentState{NodeID: "node-1", Store: agentStore}, nil)
	if !errors.Is(err, ErrAgentRuntimeDegraded) {
		t.Fatalf("session degradation = %v, want ErrAgentRuntimeDegraded", err)
	}
	if err := <-serverResult; err != nil {
		t.Fatal(err)
	}
}

func TestAgentSessionInitialSnapshotFailsClosedWhenRuntimeProbeFails(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	commandRuntime, err := NewAgentCommandRuntime("node-1", agentStore, &fakeRunnerLifecycle{
		ready: func(context.Context) error {
			return runner.ErrStrongOwnershipUnavailable
		},
	}, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}

	serverResult := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			serverResult <- err
			return
		}
		defer connection.CloseNow()
		ctx, cancel := context.WithTimeout(request.Context(), time.Second)
		defer cancel()
		hello, err := transport.ReadEnvelope(ctx, connection)
		if err != nil || hello.Type != transport.MessageHello {
			serverResult <- fmt.Errorf("hello: %#v, %w", hello, err)
			return
		}
		if err := writeAgentAck(ctx, connection, hello.MessageID); err != nil {
			serverResult <- err
			return
		}
		envelope, err := transport.ReadEnvelope(ctx, connection)
		if err != nil || envelope.Type != transport.MessageSnapshot {
			serverResult <- fmt.Errorf("snapshot: %#v, %w", envelope, err)
			return
		}
		snapshot, err := transport.DecodeAgentSnapshot(envelope.Payload)
		if err != nil || snapshot.NativeRunnerReady {
			serverResult <- fmt.Errorf("readiness snapshot: %#v, %v", snapshot, err)
			return
		}
		if err := writeAgentAck(ctx, connection, envelope.MessageID); err != nil {
			serverResult <- err
			return
		}
		serverResult <- nil
		_ = connection.Close(websocket.StatusNormalClosure, "complete")
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	sessionErr := runAgentSessionWithOptions(
		ctx,
		connection,
		&AgentState{NodeID: "node-1", Store: agentStore},
		commandRuntime,
		agentSessionOptions{heartbeatInterval: time.Second, readinessTimeout: 20 * time.Millisecond},
	)
	if status := websocket.CloseStatus(sessionErr); status != websocket.StatusNormalClosure {
		t.Fatalf("session error = %v", sessionErr)
	}
	if err := <-serverResult; err != nil {
		t.Fatal(err)
	}
}

func TestAgentSessionDisconnectsWhenHeartbeatAcknowledgementIsMissing(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	heartbeatSeen := make(chan transport.AgentHeartbeat, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.CloseNow()
		ctx := request.Context()
		for _, expected := range []transport.MessageType{transport.MessageHello, transport.MessageSnapshot} {
			envelope, readErr := transport.ReadEnvelope(ctx, connection)
			if readErr != nil || envelope.Type != expected {
				return
			}
			if writeAgentAck(ctx, connection, envelope.MessageID) != nil {
				return
			}
		}
		envelope, readErr := transport.ReadEnvelope(ctx, connection)
		if readErr != nil || envelope.Type != transport.MessageHeartbeat {
			return
		}
		heartbeat, decodeErr := transport.DecodeAgentHeartbeat(envelope.Payload)
		if decodeErr != nil {
			return
		}
		heartbeatSeen <- heartbeat
		<-ctx.Done()
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	err = runAgentSessionWithOptions(
		ctx,
		connection,
		&AgentState{NodeID: "node-1", Store: agentStore},
		nil,
		agentSessionOptions{heartbeatInterval: 20 * time.Millisecond, readinessTimeout: 5 * time.Millisecond},
	)
	if err == nil || !strings.Contains(err.Error(), "heartbeat acknowledgement missing") {
		t.Fatalf("missing heartbeat acknowledgement error = %v", err)
	}
	select {
	case heartbeat := <-heartbeatSeen:
		if heartbeat.NodeID != "node-1" || heartbeat.NativeRunnerReady {
			t.Fatalf("heartbeat = %#v", heartbeat)
		}
	default:
		t.Fatal("heartbeat was not observed by controller")
	}
}

func TestAgentSessionHeartbeatTurnsFalseWhenRuntimeProbeStops(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	var runtimeReady atomic.Bool
	runtimeReady.Store(true)
	commandRuntime, err := NewAgentCommandRuntime("node-1", agentStore, &fakeRunnerLifecycle{
		ready: func(context.Context) error {
			if runtimeReady.Load() {
				return nil
			}
			return runner.ErrStrongOwnershipUnavailable
		},
	}, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}

	serverResult := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			serverResult <- err
			return
		}
		defer connection.CloseNow()
		ctx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
		defer cancel()
		hello, err := transport.ReadEnvelope(ctx, connection)
		if err != nil || hello.Type != transport.MessageHello {
			serverResult <- fmt.Errorf("hello: %#v, %w", hello, err)
			return
		}
		if err := writeAgentAck(ctx, connection, hello.MessageID); err != nil {
			serverResult <- err
			return
		}
		initial, err := transport.ReadEnvelope(ctx, connection)
		if err != nil || initial.Type != transport.MessageSnapshot {
			serverResult <- fmt.Errorf("snapshot: %#v, %w", initial, err)
			return
		}
		snapshot, err := transport.DecodeAgentSnapshot(initial.Payload)
		if err != nil || !snapshot.NativeRunnerReady {
			serverResult <- fmt.Errorf("initial readiness: %#v, %v", snapshot, err)
			return
		}
		runtimeReady.Store(false)
		if err := writeAgentAck(ctx, connection, initial.MessageID); err != nil {
			serverResult <- err
			return
		}
		envelope, err := transport.ReadEnvelope(ctx, connection)
		if err != nil || envelope.Type != transport.MessageHeartbeat {
			serverResult <- fmt.Errorf("heartbeat: %#v, %w", envelope, err)
			return
		}
		heartbeat, err := transport.DecodeAgentHeartbeat(envelope.Payload)
		if err != nil || heartbeat.NativeRunnerReady {
			serverResult <- fmt.Errorf("degraded heartbeat: %#v, %v", heartbeat, err)
			return
		}
		if err := writeAgentAck(ctx, connection, envelope.MessageID); err != nil {
			serverResult <- err
			return
		}
		serverResult <- nil
		_ = connection.Close(websocket.StatusNormalClosure, "complete")
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	sessionErr := runAgentSessionWithOptions(
		ctx,
		connection,
		&AgentState{NodeID: "node-1", Store: agentStore},
		commandRuntime,
		agentSessionOptions{heartbeatInterval: 20 * time.Millisecond, readinessTimeout: 5 * time.Millisecond},
	)
	if status := websocket.CloseStatus(sessionErr); status != websocket.StatusNormalClosure {
		t.Fatalf("session error = %v", sessionErr)
	}
	if err := <-serverResult; err != nil {
		t.Fatal(err)
	}
}

func TestAgentSessionKeepsHeartbeatAndOutboxAcknowledgementsDistinct(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	const updateMessageID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if _, _, err := agentStore.QueueExecutionUpdate(context.Background(), updateMessageID, store.ExecutionUpdateRecord{
		NodeID:      "node-1",
		CommandID:   "command-1",
		ExecutionID: "execution-1",
		State:       domain.ExecutionRunning,
	}); err != nil {
		t.Fatal(err)
	}
	commandRuntime, err := NewAgentCommandRuntime(
		"node-1",
		agentStore,
		&fakeRunnerLifecycle{},
		currentRunnerPackage(t),
	)
	if err != nil {
		t.Fatal(err)
	}

	serverResult := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			serverResult <- err
			return
		}
		defer connection.CloseNow()
		ctx, cancel := context.WithTimeout(request.Context(), 5*time.Second)
		defer cancel()
		for _, expected := range []transport.MessageType{transport.MessageHello, transport.MessageSnapshot} {
			envelope, readErr := transport.ReadEnvelope(ctx, connection)
			if readErr != nil || envelope.Type != expected {
				serverResult <- fmt.Errorf("initial %s: %#v, %w", expected, envelope, readErr)
				return
			}
			if err := writeAgentAck(ctx, connection, envelope.MessageID); err != nil {
				serverResult <- err
				return
			}
		}

		var update, heartbeat transport.Envelope
		for update.Type == "" || heartbeat.Type == "" {
			envelope, readErr := transport.ReadEnvelope(ctx, connection)
			if readErr != nil {
				serverResult <- readErr
				return
			}
			switch envelope.Type {
			case transport.MessageExecutionUpdate:
				update = envelope
			case transport.MessageHeartbeat:
				heartbeat = envelope
			default:
				serverResult <- fmt.Errorf("unexpected message type %s", envelope.Type)
				return
			}
		}
		if update.MessageID != updateMessageID {
			serverResult <- fmt.Errorf("outbox message ID = %q", update.MessageID)
			return
		}
		if err := writeAgentAck(ctx, connection, heartbeat.MessageID); err != nil {
			serverResult <- err
			return
		}
		time.Sleep(10 * time.Millisecond)
		pending, err := agentStore.PendingExecutionUpdates(ctx)
		if err != nil || len(pending) != 1 {
			serverResult <- fmt.Errorf("heartbeat ACK altered outbox: %#v, %w", pending, err)
			return
		}
		if err := writeAgentAck(ctx, connection, update.MessageID); err != nil {
			serverResult <- err
			return
		}
		deadline := time.Now().Add(2 * time.Second)
		for {
			pending, err = agentStore.PendingExecutionUpdates(ctx)
			if err != nil {
				serverResult <- err
				return
			}
			if len(pending) == 0 {
				break
			}
			if time.Now().After(deadline) {
				serverResult <- fmt.Errorf("update ACK was not applied: %#v", pending)
				return
			}
			time.Sleep(time.Millisecond)
		}
		serverResult <- nil
		_ = connection.Close(websocket.StatusNormalClosure, "complete")
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	sessionErr := runAgentSessionWithOptions(
		ctx,
		connection,
		&AgentState{NodeID: "node-1", Store: agentStore},
		commandRuntime,
		agentSessionOptions{heartbeatInterval: time.Second, readinessTimeout: 20 * time.Millisecond},
	)
	if status := websocket.CloseStatus(sessionErr); status != websocket.StatusNormalClosure {
		t.Fatalf("session error = %v", sessionErr)
	}
	if err := <-serverResult; err != nil {
		t.Fatal(err)
	}
	pending, err := agentStore.PendingExecutionUpdates(context.Background())
	if err != nil || len(pending) != 0 {
		t.Fatalf("outbox after distinct acknowledgements = %#v, %v", pending, err)
	}
}
