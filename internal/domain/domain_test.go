package domain

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
)

func testNode(id NodeID, maxRunners int) Node {
	return Node{
		ID:                  id,
		DisplayName:         string(id),
		OS:                  OSLinux,
		Architecture:        ArchAMD64,
		MaxRunners:          maxRunners,
		AdministrativeState: NodeActive,
		ObservedState:       NodeOnline,
	}
}

func TestSlotLedgerRandomizedProperties(t *testing.T) {
	for seed := uint64(1); seed <= 40; seed++ {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			ledger, err := NewSlotLedger([]Node{
				testNode("node-a", 2),
				testNode("node-b", 3),
			}, intPointer(4))
			if err != nil {
				t.Fatal(err)
			}
			rng := rand.New(rand.NewPCG(seed, seed+1))
			owners := make(map[SlotKey]SlotOwner)
			keys := slotKeys(ledger)
			for step := 0; step < 1_000; step++ {
				key := keys[rng.IntN(len(keys))]
				if owner, claimed := owners[key]; claimed && rng.IntN(2) == 0 {
					if err := ledger.Release(key, owner); err != nil {
						t.Fatalf("release step %d: %v", step, err)
					}
					delete(owners, key)
				} else if !claimed {
					owner := SlotOwner{TargetID: TargetID(fmt.Sprintf("target-%d", step))}
					err := ledger.Claim(key, owner)
					if ledger.Claimed() < ledger.FleetMaximum() && err != nil {
						t.Fatalf("claim step %d with remaining capacity: %v", step, err)
					}
					if err == nil {
						owners[key] = owner
					}
				}
				assertLedgerInvariant(t, ledger, owners)
			}
		})
	}
}

func TestSlotLedgerConcurrentClaimHasOneOwner(t *testing.T) {
	ledger, err := NewSlotLedger([]Node{testNode("node-a", 1)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	key := SlotKey{NodeID: "node-a", Index: 0}
	var successes int
	var mu sync.Mutex
	var group sync.WaitGroup
	for attempt := 0; attempt < 32; attempt++ {
		group.Add(1)
		go func(attempt int) {
			defer group.Done()
			if err := ledger.Claim(key, SlotOwner{TargetID: TargetID(fmt.Sprintf("target-%d", attempt))}); err == nil {
				mu.Lock()
				successes++
				mu.Unlock()
			}
		}(attempt)
	}
	group.Wait()
	if successes != 1 {
		t.Fatalf("successful claims = %d, want 1", successes)
	}
	assertLedgerInvariant(t, ledger, nil)
}

func TestSlotLedgerRejectsCapacityOverflowAndMismatchedRelease(t *testing.T) {
	ledger, err := NewSlotLedger([]Node{testNode("node-a", 2)}, intPointer(1))
	if err != nil {
		t.Fatal(err)
	}
	first := SlotKey{NodeID: "node-a", Index: 0}
	second := SlotKey{NodeID: "node-a", Index: 1}
	owner := SlotOwner{TargetID: "target-a"}
	if err := ledger.Claim(first, owner); err != nil {
		t.Fatal(err)
	}
	assertValidationCode(t, ledger.Claim(second, SlotOwner{TargetID: "target-b"}), "fleet_capacity_exhausted")
	assertValidationCode(t, ledger.Release(first, SlotOwner{TargetID: "target-b"}), "slot_owner_mismatch")
	if got := ledger.Claimed(); got != 1 {
		t.Fatalf("claimed after rejected operations = %d, want 1", got)
	}
}

func TestSlotLedgerRejectsMacOSConfigurationAbovePhysicalCapacity(t *testing.T) {
	mac := testNode("mac-node", MacOSNativeRunnerMaxRunners+1)
	mac.OS = OSMacOS
	if _, err := NewSlotLedger([]Node{mac}, nil); err == nil {
		t.Fatal("slot ledger accepted a second macOS native runner slot")
	} else {
		assertValidationCode(t, err, "native_runner_capacity_exceeded")
	}

	// Multiple slots remain a valid configured input on platforms whose
	// adapters provide independently owned slot authorities.
	linux := testNode("linux-node", 2)
	if _, err := NewSlotLedger([]Node{linux}, nil); err != nil {
		t.Fatalf("slot ledger rejected valid multi-slot Linux node: %v", err)
	}
}

func TestExecutionInvalidTransitionsDoNotMutateState(t *testing.T) {
	tests := []struct {
		name string
		from ExecutionState
		to   ExecutionState
	}{
		{name: "pending skips reservation", from: ExecutionPending, to: ExecutionRunning},
		{name: "reserved skips preparing", from: ExecutionReserved, to: ExecutionRunning},
		{name: "running skips cleanup", from: ExecutionRunning, to: ExecutionReleased},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			execution := restoredExecution(t, test.from)
			assertValidationCode(t, execution.Transition(test.to), "invalid_execution_transition")
			if got := execution.CurrentState(); got != test.from {
				t.Fatalf("state after rejected transition = %q, want %q", got, test.from)
			}
		})
	}
}

func TestTerminalExecutionStatesCannotBecomeActive(t *testing.T) {
	activeStates := []ExecutionState{ExecutionPending, ExecutionReserved, ExecutionPreparing, ExecutionRunning, ExecutionCleaning}
	terminalStates := []ExecutionState{ExecutionReleased, ExecutionFailed, ExecutionQuarantined}
	for _, terminal := range terminalStates {
		for _, active := range activeStates {
			t.Run(string(terminal)+"-to-"+string(active), func(t *testing.T) {
				execution := restoredExecution(t, terminal)
				assertValidationCode(t, execution.Transition(active), "invalid_execution_transition")
				if got := execution.CurrentState(); got != terminal {
					t.Fatalf("terminal state changed to %q", got)
				}
			})
		}
	}
}

func TestExecutionHappyPathAndCleanupFailure(t *testing.T) {
	execution, err := NewExecution("execution-1", "target-1", SlotKey{NodeID: "node-1", Index: 0})
	if err != nil {
		t.Fatal(err)
	}
	for _, next := range []ExecutionState{ExecutionReserved, ExecutionPreparing, ExecutionRunning, ExecutionCleaning, ExecutionReleased} {
		if err := execution.Transition(next); err != nil {
			t.Fatalf("transition to %q: %v", next, err)
		}
	}
	if !execution.IsTerminal() {
		t.Fatal("released execution must be terminal")
	}

	failure, err := NewExecution("execution-2", "target-1", SlotKey{NodeID: "node-1", Index: 0})
	if err != nil {
		t.Fatal(err)
	}
	for _, next := range []ExecutionState{ExecutionReserved, ExecutionPreparing, ExecutionRunning, ExecutionCleaning, ExecutionCleanupFailed, ExecutionQuarantined} {
		if err := failure.Transition(next); err != nil {
			t.Fatalf("cleanup failure transition to %q: %v", next, err)
		}
	}
	if got := failure.CurrentState(); got != ExecutionQuarantined {
		t.Fatalf("cleanup failure final state = %q, want %q", got, ExecutionQuarantined)
	}
}

func TestExecutionReachabilityAllowsOmittedIntermediateObservationsButNeverRegression(t *testing.T) {
	tests := []struct {
		name string
		from ExecutionState
		to   ExecutionState
		want bool
	}{
		{name: "same observation", from: ExecutionRunning, to: ExecutionRunning, want: true},
		{name: "offline agent skips to released", from: ExecutionPreparing, to: ExecutionReleased, want: true},
		{name: "offline agent skips to cleanup failure", from: ExecutionRunning, to: ExecutionCleanupFailed, want: true},
		{name: "cleanup failure quarantines", from: ExecutionCleanupFailed, to: ExecutionQuarantined, want: true},
		{name: "terminal state cannot regress", from: ExecutionReleased, to: ExecutionRunning, want: false},
		{name: "cleaning cannot return to preparing", from: ExecutionCleaning, to: ExecutionPreparing, want: false},
		{name: "unknown source fails closed", from: ExecutionState("unknown"), to: ExecutionReleased, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := CanReachExecutionState(test.from, test.to); got != test.want {
				t.Fatalf("CanReachExecutionState(%q, %q) = %t, want %t", test.from, test.to, got, test.want)
			}
		})
	}
}

func TestCommandReplayRejectsMismatchedPayload(t *testing.T) {
	replay := NewCommandReplay()
	command := Command{
		ID:              "command-1",
		ControllerEpoch: 1,
		ExecutionID:     "execution-1",
		ExpectedState:   ExecutionReserved,
		PayloadDigest:   PayloadDigest([]byte("payload-a")),
	}
	if replayed, err := replay.Record(command); err != nil || replayed {
		t.Fatalf("first record = (%t, %v), want (false, nil)", replayed, err)
	}
	if replayed, err := replay.Record(command); err != nil || !replayed {
		t.Fatalf("exact replay = (%t, %v), want (true, nil)", replayed, err)
	}
	command.PayloadDigest = PayloadDigest([]byte("payload-b"))
	assertValidationCode(t, recordError(replay, command), "command_payload_mismatch")
}

func TestCommandReplayRejectsDifferentIdentity(t *testing.T) {
	replay := NewCommandReplay()
	command := Command{
		ID:              "command-1",
		ControllerEpoch: 1,
		ExecutionID:     "execution-1",
		ExpectedState:   ExecutionReserved,
		PayloadDigest:   PayloadDigest([]byte("payload")),
	}
	if _, err := replay.Record(command); err != nil {
		t.Fatal(err)
	}
	command.ExpectedState = ExecutionPreparing
	assertValidationCode(t, recordError(replay, command), "command_replay_mismatch")
}

func TestGitHubTargetFailsClosedWithoutPrivateVisibilityOrSafeRunnerGroup(t *testing.T) {
	target := GitHubTarget{
		ID:                    "target-1",
		InstallationID:        "installation-1",
		ScopeKind:             TargetRepository,
		Scope:                 "owner/repository",
		Visibility:            TargetPrivate,
		RunnerGroupAccessSafe: true,
		ScaleSetName:          "sparerunner",
		RunnerProfileID:       "profile-1",
	}
	if err := target.Validate(); err != nil {
		t.Fatalf("valid private target: %v", err)
	}
	target.Visibility = "unknown"
	assertValidationCode(t, target.Validate(), "target_not_private")
	target.Visibility = TargetPrivate
	target.RunnerGroupAccessSafe = false
	assertValidationCode(t, target.Validate(), "unsafe_runner_group_access")
}

func TestCommandRejectsUnknownExpectedState(t *testing.T) {
	command := Command{
		ID:              "command-1",
		ControllerEpoch: 1,
		ExecutionID:     "execution-1",
		ExpectedState:   "unknown",
		PayloadDigest:   PayloadDigest([]byte("payload")),
	}
	assertValidationCode(t, command.Validate(), "invalid_execution_state")
}

func recordError(replay *CommandReplay, command Command) error {
	_, err := replay.Record(command)
	return err
}

func assertLedgerInvariant(t *testing.T, ledger *SlotLedger, expected map[SlotKey]SlotOwner) {
	t.Helper()
	if err := ledger.Validate(); err != nil {
		t.Fatal(err)
	}
	owners := make(map[SlotKey]SlotOwner)
	for _, slot := range ledger.Slots() {
		if slot.Owner != nil {
			if _, exists := owners[slot.Key]; exists {
				t.Fatalf("slot %v has duplicate owners", slot.Key)
			}
			owners[slot.Key] = *slot.Owner
		}
	}
	if got, want := len(owners), ledger.Claimed(); got != want {
		t.Fatalf("owned slots = %d, claimed = %d", got, want)
	}
	if ledger.Claimed() > ledger.FleetMaximum() {
		t.Fatalf("claimed = %d exceeds fleet maximum %d", ledger.Claimed(), ledger.FleetMaximum())
	}
	if expected != nil {
		if len(owners) != len(expected) {
			t.Fatalf("owned slots = %d, expected = %d", len(owners), len(expected))
		}
		for key, owner := range expected {
			if owners[key] != owner {
				t.Fatalf("owner for %v = %+v, want %+v", key, owners[key], owner)
			}
		}
	}
}

func assertValidationCode(t *testing.T, err error, want string) {
	t.Helper()
	var validationErr *ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("error = %v, want ValidationError code %q", err, want)
	}
	if validationErr.Code != want {
		t.Fatalf("error code = %q, want %q", validationErr.Code, want)
	}
}

func slotKeys(ledger *SlotLedger) []SlotKey {
	slots := ledger.Slots()
	keys := make([]SlotKey, 0, len(slots))
	for _, slot := range slots {
		keys = append(keys, slot.Key)
	}
	return keys
}

func restoredExecution(t *testing.T, state ExecutionState) *Execution {
	t.Helper()
	execution, err := RestoreExecution(ExecutionSnapshot{
		ID:       "execution-1",
		TargetID: "target-1",
		Slot:     SlotKey{NodeID: "node-1", Index: 0},
		State:    state,
	})
	if err != nil {
		t.Fatal(err)
	}
	return execution
}

func intPointer(value int) *int {
	return &value
}
