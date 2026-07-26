package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/runner"
	"github.com/genm/tewake/internal/store"
	"github.com/genm/tewake/internal/transport"
)

var errAgentCommandExpectedState = errors.New("agent command expected state mismatch")

type runnerLifecycle interface {
	Ready(context.Context) error
	EnsurePrepared(context.Context, runner.Preparation) (runner.Snapshot, error)
	EnsureRunning(context.Context, runner.Start) (runner.Snapshot, error)
	Inspect(context.Context, string) (runner.Snapshot, error)
	Wait(context.Context, string) (runner.Snapshot, error)
	Destroy(context.Context, string) (runner.Snapshot, error)
}

type runnerLifecycleRecovery interface {
	Recover(context.Context, string) (runner.Snapshot, error)
}

// AgentCommandRuntime connects authenticated protocol commands to the durable
// local runner boundary. Accept persists the replay identity before the caller
// acknowledges a command; Execute performs the separately replay-safe lifecycle.
type AgentCommandRuntime struct {
	nodeID  domain.NodeID
	store   *store.AgentStore
	manager runnerLifecycle
	pkg     runner.Package
	locksMu sync.Mutex
	locks   map[domain.ExecutionID]*executionCommandLock

	lifetimeMu    sync.Mutex
	lifetime      context.Context
	monitors      map[domain.ExecutionID]struct{}
	monitorFailed bool
	waitFailures  uint64
	updateReady   chan struct{}
}

type executionCommandLock struct {
	mu         sync.Mutex
	references int
}

func NewAgentCommandRuntime(nodeID string, agentStore *store.AgentStore, manager runnerLifecycle, pkg runner.Package) (*AgentCommandRuntime, error) {
	if nodeID == "" || agentStore == nil || manager == nil {
		return nil, errors.New("agent runner dependencies are incomplete")
	}
	expected, err := runner.OfficialPackage(runner.CurrentPlatform())
	if err != nil || pkg != expected {
		return nil, runner.ErrUnsupportedPlatform
	}
	return &AgentCommandRuntime{
		nodeID:      domain.NodeID(nodeID),
		store:       agentStore,
		manager:     manager,
		pkg:         pkg,
		locks:       make(map[domain.ExecutionID]*executionCommandLock),
		monitors:    make(map[domain.ExecutionID]struct{}),
		updateReady: make(chan struct{}, 1),
	}, nil
}

// Ready performs a live, non-secret platform admission probe. The returned
// error is classification-only and must never be copied into heartbeat payloads.
func (runtime *AgentCommandRuntime) Ready(ctx context.Context) bool {
	if runtime == nil || runtime.manager == nil {
		return false
	}
	runtime.lifetimeMu.Lock()
	degraded := runtime.monitorFailed
	runtime.lifetimeMu.Unlock()
	if degraded {
		return false
	}
	return runtime.manager.Ready(ctx) == nil
}

// Start binds background completion monitoring to the Agent process lifetime,
// not to any one controller connection. Rebinding would make ownership
// ambiguous and therefore fails closed.
func (runtime *AgentCommandRuntime) Start(ctx context.Context) error {
	if runtime == nil || ctx == nil {
		return errors.New("agent runner lifetime is unavailable")
	}
	runtime.lifetimeMu.Lock()
	if runtime.lifetime != nil {
		runtime.lifetimeMu.Unlock()
		return errors.New("agent runner lifetime is already bound")
	}
	runtime.lifetime = ctx
	runtime.lifetimeMu.Unlock()
	if err := runtime.recoverStartup(ctx); err != nil {
		runtime.failMonitor()
		return err
	}
	return nil
}

func (runtime *AgentCommandRuntime) recoverStartup(ctx context.Context) error {
	records, err := runtime.store.RunnerJournalRecords(ctx)
	if err != nil {
		return err
	}
	commands, err := runtime.store.AcceptedAgentCommands(ctx)
	if err != nil {
		return err
	}
	pending, err := runtime.store.PendingExecutionUpdates(ctx)
	if err != nil {
		return err
	}
	pendingByExecution := make(map[domain.ExecutionID][]store.PendingExecutionUpdate, len(pending))
	for _, update := range pending {
		executionID := update.Update.ExecutionID
		pendingByExecution[executionID] = append(pendingByExecution[executionID], update)
	}
	recordByExecution := make(map[domain.ExecutionID]struct{}, len(records))
	for _, record := range records {
		recordByExecution[domain.ExecutionID(record.ExecutionID)] = struct{}{}
	}
	commandByExecution := make(map[domain.ExecutionID]store.AcceptedAgentCommand)
	for _, command := range commands {
		current, found := commandByExecution[command.Command.ExecutionID]
		if !found || recoveryCommandPrecedes(current, command) {
			commandByExecution[command.Command.ExecutionID] = command
		}
	}

	// A crash immediately after accepting a secret-free Prepare can precede
	// runner journal creation. Publish a terminal classified result so the
	// Controller releases the slot and GitHub may reassign the job. Start and
	// Cancel require a pre-existing local runtime and fail closed if it vanished.
	for executionID, command := range commandByExecution {
		if _, found := recordByExecution[executionID]; found {
			continue
		}
		if command.Type != domain.CommandPrepare {
			return runner.ErrJournal
		}
		metadata := metadataFromAcceptedCommand(command.Command)
		update := transport.ExecutionUpdate{
			NodeID:      runtime.nodeID,
			CommandID:   metadata.CommandID,
			ExecutionID: metadata.ExecutionID,
			State:       domain.ExecutionFailed,
			Replayed:    true,
			ErrorCode:   transport.ExecutionErrorReconciliation,
		}
		if pendingContainsRecoveryUpdate(pendingByExecution[executionID], update) {
			continue
		}
		if _, err := runtime.persistLifecycleUpdate(ctx, runner.Snapshot{
			ExecutionID: string(executionID),
			State:       runner.StateFailed,
		}, update); err != nil {
			return err
		}
	}

	recovery, ok := runtime.manager.(runnerLifecycleRecovery)
	if len(records) > 0 && !ok {
		return runner.ErrStrongOwnershipUnavailable
	}
	for _, record := range records {
		executionID := domain.ExecutionID(record.ExecutionID)
		command, found := commandByExecution[executionID]
		if !found {
			// A runtime without an authenticated command cannot be attributed to
			// Controller desired state. Keep the Agent offline for remediation.
			return runner.ErrJournal
		}
		metadata := metadataFromAcceptedCommand(command.Command)
		snapshot, recoverErr := recovery.Recover(ctx, record.ExecutionID)
		if command.Type == domain.CommandCancel &&
			snapshot.State != runner.StateReleased &&
			snapshot.State != runner.StateFailed &&
			snapshot.State != runner.StateCleanupFailed {
			// A durable Cancel is desired local teardown, not merely an
			// observation. Resume it after a process crash instead of adopting
			// the still-running boundary until natural job completion.
			snapshot, recoverErr = runtime.manager.Destroy(ctx, record.ExecutionID)
		} else if snapshot.State == runner.StatePrepared && command.Type == domain.CommandStart {
			// The Start was accepted, but the Agent crashed before the JIT crossed
			// the durable Starting fence. The one-shot secret is gone, so clean
			// the prepared root and let GitHub reassign instead of regenerating.
			snapshot, recoverErr = runtime.manager.Destroy(ctx, record.ExecutionID)
		}

		monitorRunning := snapshot.State == runner.StateRunning
		state := runnerStateToDomain(snapshot.State)
		if state == "" {
			return runner.ErrReconciliationRequired
		}
		update := transport.ExecutionUpdate{
			NodeID:      runtime.nodeID,
			CommandID:   metadata.CommandID,
			ExecutionID: executionID,
			State:       state,
			Replayed:    true,
			ErrorCode:   classifyRunnerError(recoverErr),
		}
		if state == domain.ExecutionReleased {
			update.ErrorCode = transport.ExecutionErrorNone
		} else if state == domain.ExecutionFailed &&
			update.ErrorCode == transport.ExecutionErrorNone {
			// A durable failed journal record without a surviving raw error must
			// remain visibly failed after restart.
			update.ErrorCode = transport.ExecutionErrorReconciliation
		}
		if state == domain.ExecutionPreparing && command.Type != domain.CommandPrepare {
			return runner.ErrReconciliationRequired
		}
		if !pendingContainsRecoveryUpdate(pendingByExecution[executionID], update) {
			if _, err := runtime.persistLifecycleUpdate(ctx, snapshot, update); err != nil {
				return err
			}
		}
		if monitorRunning {
			// Persist the Running observation before starting a waiter that may
			// immediately observe completion and publish Released.
			runtime.startCompletionMonitor(metadata, true)
		}
	}
	return nil
}

func pendingContainsRecoveryUpdate(
	pending []store.PendingExecutionUpdate,
	want transport.ExecutionUpdate,
) bool {
	for _, item := range pending {
		update := item.Update
		if update.CommandID == want.CommandID &&
			update.ExecutionID == want.ExecutionID &&
			update.State == want.State &&
			update.ErrorCode == want.ErrorCode {
			return true
		}
	}
	return false
}

func recoveryCommandPrecedes(current, candidate store.AcceptedAgentCommand) bool {
	if current.Command.ControllerEpoch != candidate.Command.ControllerEpoch {
		return current.Command.ControllerEpoch < candidate.Command.ControllerEpoch
	}
	priority := func(commandType domain.CommandType) int {
		switch commandType {
		case domain.CommandCancel:
			return 3
		case domain.CommandStart:
			return 2
		case domain.CommandPrepare:
			return 1
		default:
			return 0
		}
	}
	currentPriority, candidatePriority := priority(current.Type), priority(candidate.Type)
	if currentPriority != candidatePriority {
		return currentPriority < candidatePriority
	}
	return current.Command.ID < candidate.Command.ID
}

func metadataFromAcceptedCommand(command domain.Command) transport.CommandMetadata {
	return transport.CommandMetadata{
		CommandID:       command.ID,
		ControllerEpoch: command.ControllerEpoch,
		ExecutionID:     command.ExecutionID,
		ExpectedState:   command.ExpectedState,
	}
}

func (runtime *AgentCommandRuntime) UpdateReady() <-chan struct{} {
	if runtime == nil {
		return nil
	}
	return runtime.updateReady
}

func (runtime *AgentCommandRuntime) PendingUpdates(ctx context.Context) ([]store.PendingExecutionUpdate, error) {
	if runtime == nil {
		return nil, transport.ErrInvalidCommand
	}
	runtime.lifetimeMu.Lock()
	failed := runtime.monitorFailed
	runtime.lifetimeMu.Unlock()
	if failed {
		return nil, ErrAgentRuntimeDegraded
	}
	return runtime.store.PendingExecutionUpdates(ctx)
}

func (runtime *AgentCommandRuntime) AcknowledgeUpdate(ctx context.Context, messageID string) error {
	if runtime == nil {
		return transport.ErrInvalidCommand
	}
	return runtime.store.AcknowledgeExecutionUpdate(ctx, messageID)
}

type acceptedAgentCommand struct {
	runtime     *AgentCommandRuntime
	message     transport.MessageType
	prepare     transport.PrepareCommand
	start       transport.StartCommand
	cancel      transport.CancelCommand
	replayed    bool
	release     func()
	lifecycleMu sync.Mutex
	state       acceptedCommandState
	cleanupOnce sync.Once
}

type acceptedCommandState uint8

const (
	acceptedCommandOpen acceptedCommandState = iota
	acceptedCommandExecuting
	acceptedCommandClosed
)

func (runtime *AgentCommandRuntime) Accept(ctx context.Context, envelope *transport.Envelope) (*acceptedAgentCommand, error) {
	if runtime == nil || envelope == nil ||
		(envelope.Type != transport.MessagePrepare && envelope.Type != transport.MessageStart && envelope.Type != transport.MessageCancel) {
		return nil, transport.ErrInvalidCommand
	}
	// The envelope owns the only raw JSON copy on the Agent. Decode first, then
	// erase it on every path so the JIT body cannot survive in a session buffer.
	payload := envelope.Payload
	payloadErased := false
	erasePayload := func() {
		if payloadErased {
			return
		}
		clear(payload)
		envelope.Payload = nil
		payloadErased = true
	}
	defer erasePayload()

	accepted := &acceptedAgentCommand{runtime: runtime, message: envelope.Type}
	var replayIdentity domain.Command
	switch envelope.Type {
	case transport.MessagePrepare:
		command, err := transport.DecodePrepareCommand(payload)
		if err != nil || string(command.Metadata().CommandID) != envelope.MessageID {
			return nil, transport.ErrInvalidCommand
		}
		accepted.prepare = command
		replayIdentity = command.ReplayIdentity(payload)
	case transport.MessageStart:
		command, err := transport.DecodeStartCommand(payload)
		if err != nil || string(command.Metadata().CommandID) != envelope.MessageID {
			return nil, transport.ErrInvalidCommand
		}
		accepted.start = command
		replayIdentity = command.ReplayIdentity(payload)
	case transport.MessageCancel:
		command, err := transport.DecodeCancelCommand(payload)
		if err != nil || string(command.Metadata().CommandID) != envelope.MessageID {
			return nil, transport.ErrInvalidCommand
		}
		accepted.cancel = command
		replayIdentity = command.ReplayIdentity(payload)
	}

	// The exact authenticated bytes have served their only purpose. Erase them
	// before waiting behind another command for this execution.
	erasePayload()
	accepted.release = runtime.lockExecution(replayIdentity.ExecutionID)
	keep := false
	defer func() {
		if !keep {
			accepted.Discard()
		}
	}()

	acceptedType, err := agentDomainCommandType(envelope.Type)
	if err != nil {
		return nil, err
	}
	typedIdentity := store.AcceptedAgentCommand{Type: acceptedType, Command: replayIdentity}
	replayed, err := runtime.store.LookupTypedCommand(ctx, typedIdentity)
	if err != nil {
		return nil, err
	}
	if !replayed {
		if err := runtime.validateExpectedState(ctx, envelope.Type, replayIdentity); err != nil {
			return nil, err
		}
		replayed, err = runtime.store.RecordTypedCommand(ctx, typedIdentity)
		if err != nil {
			return nil, err
		}
	}
	accepted.replayed = replayed
	keep = true
	return accepted, nil
}

func agentDomainCommandType(messageType transport.MessageType) (domain.CommandType, error) {
	switch messageType {
	case transport.MessagePrepare:
		return domain.CommandPrepare, nil
	case transport.MessageStart:
		return domain.CommandStart, nil
	case transport.MessageCancel:
		return domain.CommandCancel, nil
	default:
		return "", transport.ErrInvalidCommand
	}
}

func (accepted *acceptedAgentCommand) Execute(ctx context.Context) (transport.ExecutionUpdate, error) {
	if accepted == nil || accepted.runtime == nil || !accepted.beginExecute() {
		return transport.ExecutionUpdate{}, transport.ErrInvalidCommand
	}
	defer accepted.finishExecute()
	var (
		metadata transport.CommandMetadata
		snapshot runner.Snapshot
		runErr   error
	)
	switch accepted.message {
	case transport.MessagePrepare:
		metadata = accepted.prepare.Metadata()
		snapshot, runErr = accepted.runtime.manager.EnsurePrepared(ctx, runner.Preparation{
			ExecutionID:   string(metadata.ExecutionID),
			Package:       accepted.runtime.pkg,
			DisableUpdate: accepted.prepare.DisableUpdate(),
		})
	case transport.MessageStart:
		metadata = accepted.start.Metadata()
		snapshot, runErr = accepted.runtime.manager.EnsureRunning(ctx, runner.Start{
			Preparation: runner.Preparation{
				ExecutionID:   string(metadata.ExecutionID),
				Package:       accepted.runtime.pkg,
				DisableUpdate: accepted.start.DisableUpdate(),
			},
			JIT: accepted.start,
		})
		if accepted.replayed && snapshot.State == runner.StateReleased && errors.Is(runErr, runner.ErrExecutionConflict) {
			// The original Start already reached a cleaned terminal runtime.
			// Treat it as an exact idempotent replay before the failed-Start
			// cleanup path considers creating quarantine evidence.
			runErr = nil
		}
		if runErr != nil {
			// Start can fail before a process exists while a prepared credential
			// workspace still survives. Do not publish a terminal command result
			// until the local runtime has been destroyed or the node has been
			// durably quarantined.
			var cleanupDegraded bool
			snapshot, cleanupDegraded = accepted.runtime.cleanupAfterFailedStart(ctx, metadata.ExecutionID)
			if cleanupDegraded {
				// Cleanup failure is an independently durable runtime outcome,
				// not merely the original Start classification.
				runErr = runner.ErrQuarantined
			}
		}
	case transport.MessageCancel:
		metadata = accepted.cancel.Metadata()
		snapshot, runErr = accepted.runtime.manager.Destroy(ctx, string(metadata.ExecutionID))
	default:
		return transport.ExecutionUpdate{}, transport.ErrInvalidCommand
	}
	state := runnerStateToDomain(snapshot.State)
	if accepted.replayed && state == domain.ExecutionReleased && errors.Is(runErr, runner.ErrExecutionConflict) {
		// A completed runtime is the idempotent success observation for an exact
		// Prepare/Start replay whose original ACK was lost. Preserve Released
		// without attaching a failure code that would make the typed update
		// internally inconsistent.
		runErr = nil
	}
	if state == "" {
		// A failure before the runner journal created an observation is still a
		// classified command failure, never an invented healthy state.
		state = domain.ExecutionFailed
	}
	update := transport.ExecutionUpdate{
		NodeID:      accepted.runtime.nodeID,
		CommandID:   metadata.CommandID,
		ExecutionID: metadata.ExecutionID,
		State:       state,
		Replayed:    accepted.replayed,
		ErrorCode:   classifyRunnerError(runErr),
	}
	// A live runner must always gain an Agent-lifetime completion owner, even
	// when the lifecycle update journal is already degraded. Otherwise an
	// outbox failure would strand the process and its credential workspace.
	if accepted.message == transport.MessageStart && snapshot.State == runner.StateRunning && runErr == nil {
		accepted.runtime.startCompletionMonitor(metadata, accepted.replayed)
	}
	persisted, err := accepted.runtime.persistLifecycleUpdate(ctx, snapshot, update)
	if err != nil {
		accepted.runtime.failMonitor()
		update.ErrorCode = transport.ExecutionErrorJournal
		return update, err
	}
	update = persisted
	return update, runErr
}

func (runtime *AgentCommandRuntime) cleanupAfterFailedStart(
	ctx context.Context,
	executionID domain.ExecutionID,
) (runner.Snapshot, bool) {
	failed := runner.Snapshot{ExecutionID: string(executionID), State: runner.StateFailed}
	observed, inspectErr := runtime.manager.Inspect(ctx, string(executionID))
	if errors.Is(inspectErr, runner.ErrExecutionNotFound) {
		return failed, false
	}
	switch observed.State {
	case runner.StateReleased, runner.StateFailed:
		// These states prove the Manager has no live process or credential
		// workspace requiring another destructive transition.
		return failed, false
	case runner.StateCleanupFailed:
		return observed, true
	}

	terminal, destroyErr := runtime.manager.Destroy(ctx, string(executionID))
	if destroyErr == nil && (terminal.State == runner.StateReleased || terminal.State == runner.StateFailed) {
		return failed, false
	}
	if terminal.State != runner.StateCleanupFailed {
		terminal = runner.Snapshot{
			ExecutionID: string(executionID),
			State:       runner.StateCleanupFailed,
			Quarantined: true,
		}
	}
	// persistLifecycleUpdate commits the cleanup tombstone, observation, and
	// outbox entry in one SQLite transaction.
	return terminal, true
}

// Discard closes an accepted command without running it. The JIT value and the
// per-execution admission lock are both released. Transport ownership must call
// Execute before acknowledging; an ACK failure never calls Discard.
func (accepted *acceptedAgentCommand) Discard() {
	if accepted == nil {
		return
	}
	accepted.lifecycleMu.Lock()
	if accepted.state != acceptedCommandOpen {
		accepted.lifecycleMu.Unlock()
		return
	}
	accepted.state = acceptedCommandClosed
	accepted.lifecycleMu.Unlock()
	accepted.cleanup()
}

func (accepted *acceptedAgentCommand) beginExecute() bool {
	accepted.lifecycleMu.Lock()
	defer accepted.lifecycleMu.Unlock()
	if accepted.state != acceptedCommandOpen {
		return false
	}
	accepted.state = acceptedCommandExecuting
	return true
}

func (accepted *acceptedAgentCommand) finishExecute() {
	accepted.lifecycleMu.Lock()
	accepted.state = acceptedCommandClosed
	accepted.lifecycleMu.Unlock()
	accepted.cleanup()
}

func (accepted *acceptedAgentCommand) cleanup() {
	accepted.cleanupOnce.Do(func() {
		accepted.start.Discard()
		if accepted.release != nil {
			accepted.release()
		}
	})
}

func (runtime *AgentCommandRuntime) lockExecution(executionID domain.ExecutionID) func() {
	runtime.locksMu.Lock()
	lock := runtime.locks[executionID]
	if lock == nil {
		lock = &executionCommandLock{}
		runtime.locks[executionID] = lock
	}
	lock.references++
	runtime.locksMu.Unlock()

	lock.mu.Lock()
	var once sync.Once
	return func() {
		once.Do(func() {
			lock.mu.Unlock()
			runtime.locksMu.Lock()
			lock.references--
			if lock.references == 0 {
				delete(runtime.locks, executionID)
			}
			runtime.locksMu.Unlock()
		})
	}
}

func (runtime *AgentCommandRuntime) validateExpectedState(ctx context.Context, message transport.MessageType, command domain.Command) error {
	snapshot, inspectErr := runtime.manager.Inspect(ctx, string(command.ExecutionID))
	if errors.Is(inspectErr, runner.ErrExecutionNotFound) {
		if message == transport.MessagePrepare && command.ExpectedState == domain.ExecutionReserved {
			return nil
		}
		return errAgentCommandExpectedState
	}
	actual := runnerStateToDomain(snapshot.State)
	if actual == "" {
		if inspectErr != nil {
			return inspectErr
		}
		return runner.ErrReconciliationRequired
	}
	if actual != command.ExpectedState {
		return errAgentCommandExpectedState
	}
	if inspectErr != nil {
		return inspectErr
	}
	switch message {
	case transport.MessagePrepare:
		// A new preparation is admitted only when no local execution exists.
		// Exact replays have already passed their original admission.
		return errAgentCommandExpectedState
	case transport.MessageStart:
		if actual != domain.ExecutionPreparing {
			return errAgentCommandExpectedState
		}
		return nil
	case transport.MessageCancel:
		if actual != domain.ExecutionPreparing &&
			actual != domain.ExecutionRunning &&
			actual != domain.ExecutionCleaning {
			return errAgentCommandExpectedState
		}
		return nil
	default:
		return transport.ErrInvalidCommand
	}
}

func (runtime *AgentCommandRuntime) persistLifecycleUpdate(
	ctx context.Context,
	snapshot runner.Snapshot,
	update transport.ExecutionUpdate,
) (transport.ExecutionUpdate, error) {
	if snapshot.ExecutionID == "" || domain.ExecutionID(snapshot.ExecutionID) != update.ExecutionID {
		return update, runner.ErrJournal
	}
	payload, err := transport.EncodeExecutionUpdate(update)
	if err != nil {
		return update, err
	}
	digest := sha256.Sum256(payload)
	messageID := hex.EncodeToString(digest[:])
	clear(payload)
	commit := store.ExecutionLifecycleCommit{
		Observation: store.Observation{
			ExecutionID: update.ExecutionID,
			State:       update.State,
		},
		MessageID: messageID,
		Update:    storeExecutionUpdate(update),
	}
	if snapshot.State == runner.StateCleanupFailed ||
		update.State == domain.ExecutionCleanupFailed ||
		update.State == domain.ExecutionQuarantined {
		commit.CleanupTombstone = &store.CleanupTombstone{
			ExecutionID: update.ExecutionID,
			FailureCode: store.CleanupVerificationFailed,
		}
	}
	pending, _, err := runtime.store.CommitExecutionLifecycle(ctx, commit)
	if err != nil {
		return update, err
	}
	runtime.notifyUpdate()
	encoded := transportExecutionUpdate(pending.Update)
	if encoded.Validate() != nil {
		return update, runner.ErrJournal
	}
	return encoded, nil
}

func (runtime *AgentCommandRuntime) notifyUpdate() {
	select {
	case runtime.updateReady <- struct{}{}:
	default:
	}
}

func (runtime *AgentCommandRuntime) startCompletionMonitor(metadata transport.CommandMetadata, replayed bool) {
	runtime.lifetimeMu.Lock()
	ctx := runtime.lifetime
	if ctx == nil {
		runtime.lifetimeMu.Unlock()
		return
	}
	if _, exists := runtime.monitors[metadata.ExecutionID]; exists {
		runtime.lifetimeMu.Unlock()
		return
	}
	runtime.monitors[metadata.ExecutionID] = struct{}{}
	runtime.lifetimeMu.Unlock()
	go runtime.monitorCompletion(ctx, metadata, replayed)
}

func (runtime *AgentCommandRuntime) monitorCompletion(ctx context.Context, metadata transport.CommandMetadata, replayed bool) {
	defer func() {
		runtime.lifetimeMu.Lock()
		delete(runtime.monitors, metadata.ExecutionID)
		runtime.lifetimeMu.Unlock()
	}()

	_, waitErr := runtime.manager.Wait(ctx, string(metadata.ExecutionID))
	if ctx.Err() != nil {
		// Agent shutdown leaves the durable Running record intact. A later
		// reconciliation must re-adopt the exact platform boundary.
		return
	}
	if waitErr != nil {
		// Preserve only a stable counter; raw platform wait errors may expose
		// paths or process detail and never enter logs or the outbox.
		runtime.lifetimeMu.Lock()
		runtime.waitFailures++
		runtime.lifetimeMu.Unlock()
	}

	release := runtime.lockExecution(metadata.ExecutionID)
	defer release()
	if ctx.Err() != nil {
		return
	}
	terminalPending, pendingErr := runtime.terminalUpdatePending(
		ctx,
		metadata.ExecutionID,
	)
	if pendingErr != nil {
		// Losing the outbox authority must make the Agent visibly unavailable,
		// but it must not strand a completed native runner or its workspace.
		// Inspect/Destroy remain idempotent local ownership operations, and any
		// later persistence failure keeps the runtime degraded for recovery.
		runtime.failMonitor()
	} else if terminalPending {
		// A Cancel admitted while Wait was blocked owns the terminal lifecycle
		// update for this execution. Publishing another terminal update under
		// the Start command would make sequential outbox acknowledgement
		// impossible and would try to release the same Controller slot twice.
		return
	}

	// An explicit cancel may have completed while Wait was observing the same
	// descendant boundary. Publish that terminal observation without regressing
	// the local journal back to Cleaning.
	observed, inspectErr := runtime.manager.Inspect(ctx, string(metadata.ExecutionID))
	observedState := runnerStateToDomain(observed.State)
	if observedState == domain.ExecutionReleased || observedState == domain.ExecutionFailed ||
		observedState == domain.ExecutionCleanupFailed {
		update := transport.ExecutionUpdate{
			NodeID:      runtime.nodeID,
			CommandID:   metadata.CommandID,
			ExecutionID: metadata.ExecutionID,
			State:       observedState,
			Replayed:    replayed,
		}
		if observedState != domain.ExecutionReleased {
			update.ErrorCode = classifyRunnerError(inspectErr)
			if update.ErrorCode == transport.ExecutionErrorNone {
				// A failed runner journal state without a corresponding typed
				// error is inconsistent evidence. Classify it conservatively
				// instead of inventing a successful terminal observation.
				update.ErrorCode = transport.ExecutionErrorReconciliation
			}
		}
		if _, err := runtime.persistLifecycleUpdate(ctx, observed, update); err != nil {
			runtime.failMonitor()
		}
		return
	}
	_ = inspectErr

	// Manager.Destroy first commits its local Cleaning intent, then performs
	// teardown and commits the terminal runner journal state. Publishing a
	// synthetic Cleaning observation before that CAS would let an Agent crash
	// with Controller=Cleaning while its local runtime SSOT remains Running.
	terminal, destroyErr := runtime.manager.Destroy(ctx, string(metadata.ExecutionID))
	state := runnerStateToDomain(terminal.State)
	if state == "" {
		state = domain.ExecutionCleanupFailed
	}
	update := transport.ExecutionUpdate{
		NodeID:      runtime.nodeID,
		CommandID:   metadata.CommandID,
		ExecutionID: metadata.ExecutionID,
		State:       state,
		Replayed:    replayed,
		ErrorCode:   classifyRunnerError(destroyErr),
	}
	if _, err := runtime.persistLifecycleUpdate(ctx, terminal, update); err != nil {
		runtime.failMonitor()
	}
}

func (runtime *AgentCommandRuntime) terminalUpdatePending(
	ctx context.Context,
	executionID domain.ExecutionID,
) (bool, error) {
	pending, err := runtime.store.PendingExecutionUpdates(ctx)
	if err != nil {
		return false, err
	}
	for _, item := range pending {
		if item.Update.ExecutionID != executionID {
			continue
		}
		switch item.Update.State {
		case domain.ExecutionReleased, domain.ExecutionFailed,
			domain.ExecutionCleanupFailed, domain.ExecutionQuarantined:
			return true, nil
		}
	}
	return false, nil
}

func (runtime *AgentCommandRuntime) failMonitor() {
	runtime.lifetimeMu.Lock()
	runtime.monitorFailed = true
	runtime.lifetimeMu.Unlock()
	runtime.notifyUpdate()
}

func storeExecutionUpdate(update transport.ExecutionUpdate) store.ExecutionUpdateRecord {
	return store.ExecutionUpdateRecord{
		NodeID:      update.NodeID,
		CommandID:   update.CommandID,
		ExecutionID: update.ExecutionID,
		State:       update.State,
		Replayed:    update.Replayed,
		ErrorCode:   update.ErrorCode,
	}
}

func transportExecutionUpdate(update store.ExecutionUpdateRecord) transport.ExecutionUpdate {
	return transport.ExecutionUpdate{
		NodeID:      update.NodeID,
		CommandID:   update.CommandID,
		ExecutionID: update.ExecutionID,
		State:       update.State,
		Replayed:    update.Replayed,
		ErrorCode:   update.ErrorCode,
	}
}

func runnerStateToDomain(state runner.State) domain.ExecutionState {
	switch state {
	case runner.StatePreparing, runner.StatePrepared, runner.StateStarting:
		return domain.ExecutionPreparing
	case runner.StateRunning:
		return domain.ExecutionRunning
	case runner.StateCleaning:
		return domain.ExecutionCleaning
	case runner.StateReleased:
		return domain.ExecutionReleased
	case runner.StateFailed:
		return domain.ExecutionFailed
	case runner.StateCleanupFailed:
		return domain.ExecutionCleanupFailed
	default:
		return ""
	}
}

func classifyRunnerError(err error) transport.ExecutionErrorCode {
	switch {
	case err == nil:
		return transport.ExecutionErrorNone
	case errors.Is(err, runner.ErrExecutionConflict):
		return transport.ExecutionErrorConflict
	case errors.Is(err, runner.ErrReconciliationRequired):
		return transport.ExecutionErrorReconciliation
	case errors.Is(err, runner.ErrQuarantined):
		return transport.ExecutionErrorQuarantined
	case errors.Is(err, runner.ErrCleanupFailed):
		return transport.ExecutionErrorCleanup
	case errors.Is(err, runner.ErrStrongOwnershipUnavailable), errors.Is(err, runner.ErrUnsupportedPlatform):
		return transport.ExecutionErrorPlatform
	case errors.Is(err, runner.ErrJournal):
		return transport.ExecutionErrorJournal
	default:
		return transport.ExecutionErrorStart
	}
}
