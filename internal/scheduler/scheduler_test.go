package scheduler

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/genm/sparerunner/internal/domain"
)

const gibibyte = uint64(1024 * 1024 * 1024)

func TestGrantNextFiltersIneligibleNodes(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*NodeSnapshot, *TargetSpec)
	}{
		{
			name: "draining",
			mutate: func(node *NodeSnapshot, _ *TargetSpec) {
				node.Node.AdministrativeState = domain.NodeDraining
			},
		},
		{
			name: "quarantined",
			mutate: func(node *NodeSnapshot, _ *TargetSpec) {
				node.Node.AdministrativeState = domain.NodeQuarantined
			},
		},
		{
			name: "revoked",
			mutate: func(node *NodeSnapshot, _ *TargetSpec) {
				node.Node.AdministrativeState = domain.NodeRevoked
			},
		},
		{
			name: "offline",
			mutate: func(node *NodeSnapshot, _ *TargetSpec) {
				node.Node.ObservedState = domain.NodeOffline
			},
		},
		{
			name: "stale",
			mutate: func(node *NodeSnapshot, _ *TargetSpec) {
				node.Node.ObservedState = domain.NodeStale
			},
		},
		{
			name: "reconciling",
			mutate: func(node *NodeSnapshot, _ *TargetSpec) {
				node.Node.ObservedState = domain.NodeReconciling
			},
		},
		{
			name: "not reconciled",
			mutate: func(node *NodeSnapshot, _ *TargetSpec) {
				node.Reconciled = false
			},
		},
		{
			name: "native backend unavailable",
			mutate: func(node *NodeSnapshot, _ *TargetSpec) {
				node.NativeReady = false
			},
		},
		{
			name: "operating system mismatch",
			mutate: func(_ *NodeSnapshot, target *TargetSpec) {
				target.Profile.OS = operatingSystemPointer(domain.OSMacOS)
			},
		},
		{
			name: "architecture mismatch",
			mutate: func(_ *NodeSnapshot, target *TargetSpec) {
				target.Profile.Architecture = architecturePointer(domain.ArchARM64)
			},
		},
		{
			name: "minimum memory unavailable",
			mutate: func(node *NodeSnapshot, _ *TargetSpec) {
				node.AvailableMemoryBytes = 3 * gibibyte
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node := testNodeSnapshot("node-a", domain.OSLinux, domain.ArchAMD64, 1)
			target := testTargetSpec("target-a", operatingSystemPointer(domain.OSLinux), architecturePointer(domain.ArchAMD64))
			target.Profile.MinAvailableMemoryBytes = 4 * gibibyte
			test.mutate(&node, &target)

			scheduler := newTestScheduler(t, []NodeSnapshot{node}, []TargetSpec{target}, nil)
			if grant, ok, err := scheduler.GrantNext([]domain.TargetID{target.Target.ID}); err != nil {
				t.Fatalf("GrantNext: %v", err)
			} else if ok {
				t.Fatalf("ineligible node received grant: %+v", grant)
			}
			assertSchedulerInvariant(t, scheduler)
		})
	}
}

func TestGrantNextRanksEligibleSlotsDeterministically(t *testing.T) {
	tests := []struct {
		name      string
		nodes     []NodeSnapshot
		packageID string
		want      domain.SlotKey
	}{
		{
			name: "available memory before cache",
			nodes: []NodeSnapshot{
				withNodeResources(testNodeSnapshot("node-a", domain.OSLinux, domain.ArchAMD64, 1), 8*gibibyte),
				withNodeResources(testNodeSnapshot("node-b", domain.OSLinux, domain.ArchAMD64, 1), 4*gibibyte, "runner-v1"),
			},
			packageID: "runner-v1",
			want:      domain.SlotKey{NodeID: "node-a", Index: 0},
		},
		{
			name: "exact package cache after equal load and memory",
			nodes: []NodeSnapshot{
				withNodeResources(testNodeSnapshot("node-a", domain.OSLinux, domain.ArchAMD64, 1), 8*gibibyte, "runner-v0"),
				withNodeResources(testNodeSnapshot("node-b", domain.OSLinux, domain.ArchAMD64, 1), 8*gibibyte, "runner-v1"),
			},
			packageID: "runner-v1",
			want:      domain.SlotKey{NodeID: "node-b", Index: 0},
		},
		{
			name: "immutable node id breaks node tie",
			nodes: []NodeSnapshot{
				withNodeResources(testNodeSnapshot("node-b", domain.OSLinux, domain.ArchAMD64, 1), 8*gibibyte),
				withNodeResources(testNodeSnapshot("node-a", domain.OSLinux, domain.ArchAMD64, 1), 8*gibibyte),
			},
			want: domain.SlotKey{NodeID: "node-a", Index: 0},
		},
		{
			name: "slot index breaks same node tie",
			nodes: []NodeSnapshot{
				withNodeResources(testNodeSnapshot("node-a", domain.OSLinux, domain.ArchAMD64, 2), 8*gibibyte),
			},
			want: domain.SlotKey{NodeID: "node-a", Index: 0},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			target := testTargetSpec("target-a", nil, nil)
			target.RunnerPackage = test.packageID
			scheduler := newTestScheduler(t, test.nodes, []TargetSpec{target}, nil)

			grant, ok, err := scheduler.GrantNext([]domain.TargetID{target.Target.ID})
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatal("GrantNext returned no capacity")
			}
			if grant.Slot != test.want {
				t.Fatalf("slot = %+v, want %+v", grant.Slot, test.want)
			}
		})
	}
}

func TestBackedActiveExecutionRanksAsNodeLoad(t *testing.T) {
	nodeA := testNodeSnapshot("node-a", domain.OSLinux, domain.ArchAMD64, 2)
	nodeA.ActiveExecutions = []domain.ExecutionID{"execution-existing"}
	nodeB := testNodeSnapshot("node-b", domain.OSLinux, domain.ArchAMD64, 1)
	targets := []TargetSpec{
		testTargetSpec("target-existing", nil, nil),
		testTargetSpec("target-demand", nil, nil),
	}
	scheduler, err := NewWithReservations(
		[]NodeSnapshot{nodeA, nodeB},
		targets,
		nil,
		[]RestoredReservation{{
			TargetID:    "target-existing",
			Slot:        domain.SlotKey{NodeID: "node-a", Index: 0},
			ExecutionID: "execution-existing",
		}},
	)
	if err != nil {
		t.Fatal(err)
	}

	grant, ok, err := scheduler.GrantNext([]domain.TargetID{"target-demand"})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("GrantNext returned no capacity")
	}
	if grant.Slot != (domain.SlotKey{NodeID: "node-b", Index: 0}) {
		t.Fatalf("slot = %+v, want least-loaded node-b", grant.Slot)
	}
	assertSchedulerInvariant(t, scheduler)
}

func TestUnbackedStartupExecutionIsNotAdvertisedAndConsumesFleetCapacity(t *testing.T) {
	nodeA := testNodeSnapshot("node-a", domain.OSLinux, domain.ArchAMD64, 2)
	nodeA.ActiveExecutions = []domain.ExecutionID{"execution-unbacked"}
	nodeB := testNodeSnapshot("node-b", domain.OSLinux, domain.ArchAMD64, 1)
	fleetMaximum := 2
	scheduler := newTestScheduler(t,
		[]NodeSnapshot{nodeA, nodeB},
		[]TargetSpec{testTargetSpec("target-a", nil, nil)},
		&fleetMaximum,
	)
	if got := scheduler.OccupiedCapacity(); got != 1 {
		t.Fatalf("startup occupied capacity = %d, want 1", got)
	}

	grant, ok, err := scheduler.GrantNext([]domain.TargetID{"target-a"})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("eligible node-b should receive the one remaining fleet slot")
	}
	if grant.Slot.NodeID != "node-b" {
		t.Fatalf("unbacked node received grant: %+v", grant)
	}
	if got := scheduler.OccupiedCapacity(); got != fleetMaximum {
		t.Fatalf("occupied capacity = %d, want fleet maximum %d", got, fleetMaximum)
	}
	if extra, ok, err := scheduler.GrantNext([]domain.TargetID{"target-a"}); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatalf("unbacked runtime was double-counted as free capacity: %+v", extra)
	}
	assertCapacity(t, scheduler, "target-a", 1)
	assertSchedulerInvariant(t, scheduler)
}

func TestRestoredReservationBacksStartupExecutionBeforeCapacityReturns(t *testing.T) {
	node := testNodeSnapshot("node-a", domain.OSLinux, domain.ArchAMD64, 2)
	node.ActiveExecutions = []domain.ExecutionID{"execution-existing"}
	targets := []TargetSpec{
		testTargetSpec("target-existing", nil, nil),
		testTargetSpec("target-demand", nil, nil),
	}
	fleetMaximum := 2
	scheduler, err := NewWithReservations(
		[]NodeSnapshot{node},
		targets,
		&fleetMaximum,
		[]RestoredReservation{{
			TargetID:    "target-existing",
			Slot:        domain.SlotKey{NodeID: "node-a", Index: 0},
			ExecutionID: "execution-existing",
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	assertCapacity(t, scheduler, "target-existing", 1)

	grant, ok, err := scheduler.GrantNext([]domain.TargetID{"target-demand"})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || grant.Slot != (domain.SlotKey{NodeID: "node-a", Index: 1}) {
		t.Fatalf("remaining restored node slot = (%+v, %t), want node-a/1", grant, ok)
	}
	if got := scheduler.OccupiedCapacity(); got != fleetMaximum {
		t.Fatalf("occupied capacity = %d, want %d", got, fleetMaximum)
	}
	assertSchedulerInvariant(t, scheduler)
}

func TestRestoreReservationsRejectsAmbiguousOwnership(t *testing.T) {
	nodes := []NodeSnapshot{testNodeSnapshot("node-a", domain.OSLinux, domain.ArchAMD64, 2)}
	targets := []TargetSpec{testTargetSpec("target-a", nil, nil)}
	fleetMaximum := 2
	tests := []struct {
		name         string
		reservations []RestoredReservation
		fleetMaximum *int
		wantCode     string
	}{
		{
			name: "duplicate physical slot",
			reservations: []RestoredReservation{
				{TargetID: "target-a", Slot: domain.SlotKey{NodeID: "node-a", Index: 0}, ExecutionID: "execution-a"},
				{TargetID: "target-a", Slot: domain.SlotKey{NodeID: "node-a", Index: 0}, ExecutionID: "execution-b"},
			},
			fleetMaximum: &fleetMaximum,
			wantCode:     "duplicate_restored_slot",
		},
		{
			name: "duplicate execution",
			reservations: []RestoredReservation{
				{TargetID: "target-a", Slot: domain.SlotKey{NodeID: "node-a", Index: 0}, ExecutionID: "execution-a"},
				{TargetID: "target-a", Slot: domain.SlotKey{NodeID: "node-a", Index: 1}, ExecutionID: "execution-a"},
			},
			fleetMaximum: &fleetMaximum,
			wantCode:     "duplicate_restored_execution",
		},
		{
			name: "unknown target",
			reservations: []RestoredReservation{
				{TargetID: "target-unknown", Slot: domain.SlotKey{NodeID: "node-a", Index: 0}, ExecutionID: "execution-a"},
			},
			fleetMaximum: &fleetMaximum,
			wantCode:     "target_not_found",
		},
		{
			name: "unknown slot",
			reservations: []RestoredReservation{
				{TargetID: "target-a", Slot: domain.SlotKey{NodeID: "node-a", Index: 9}, ExecutionID: "execution-a"},
			},
			fleetMaximum: &fleetMaximum,
			wantCode:     "slot_not_found",
		},
		{
			name: "claims exceed fleet maximum",
			reservations: []RestoredReservation{
				{TargetID: "target-a", Slot: domain.SlotKey{NodeID: "node-a", Index: 0}, ExecutionID: "execution-a"},
				{TargetID: "target-a", Slot: domain.SlotKey{NodeID: "node-a", Index: 1}, ExecutionID: "execution-b"},
			},
			fleetMaximum: intPointer(1),
			wantCode:     "restored_capacity_exceeds_fleet_maximum",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheduler, err := NewWithReservations(nodes, targets, test.fleetMaximum, test.reservations)
			if scheduler != nil {
				t.Fatalf("scheduler returned after rejected restoration: %+v", scheduler)
			}
			assertSchedulerCode(t, err, test.wantCode)
		})
	}
}

func TestGrantSequenceIsIndependentOfInputOrder(t *testing.T) {
	nodes := []NodeSnapshot{
		withNodeResources(testNodeSnapshot("node-a", domain.OSLinux, domain.ArchAMD64, 2), 8*gibibyte, "runner-v1"),
		withNodeResources(testNodeSnapshot("node-b", domain.OSLinux, domain.ArchAMD64, 2), 4*gibibyte, "runner-v1"),
	}
	targets := []TargetSpec{
		testTargetSpec("target-a", nil, nil),
		testTargetSpec("target-b", nil, nil),
	}
	for index := range targets {
		targets[index].RunnerPackage = "runner-v1"
	}

	first := newTestScheduler(t, nodes, targets, nil)
	second := newTestScheduler(t,
		[]NodeSnapshot{nodes[1], nodes[0]},
		[]TargetSpec{targets[1], targets[0]},
		nil,
	)
	firstSequence := collectGrantSequence(t, first, []domain.TargetID{"target-b", "target-a"}, 4)
	secondSequence := collectGrantSequence(t, second, []domain.TargetID{"target-a", "target-b"}, 4)
	if fmt.Sprint(firstSequence) != fmt.Sprint(secondSequence) {
		t.Fatalf("grant sequence depends on input order:\nfirst:  %v\nsecond: %v", firstSequence, secondSequence)
	}
}

func TestRoundRobinServesEveryContinuouslyEligibleTarget(t *testing.T) {
	node := testNodeSnapshot("node-a", domain.OSLinux, domain.ArchAMD64, 1)
	targets := []TargetSpec{
		testTargetSpec("target-c", nil, nil),
		testTargetSpec("target-a", nil, nil),
		testTargetSpec("target-b", nil, nil),
	}
	scheduler := newTestScheduler(t, []NodeSnapshot{node}, targets, nil)
	want := []domain.TargetID{"target-a", "target-b", "target-c"}

	for iteration := 0; iteration < 30; iteration++ {
		grant, ok, err := scheduler.GrantNext([]domain.TargetID{"target-c", "target-a", "target-b"})
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("iteration %d returned no grant", iteration)
		}
		if grant.TargetID != want[iteration%len(want)] {
			t.Fatalf("iteration %d target = %q, want %q", iteration, grant.TargetID, want[iteration%len(want)])
		}
		if err := scheduler.Release(grant.Ref()); err != nil {
			t.Fatalf("release iteration %d: %v", iteration, err)
		}
	}
}

func TestCapacityCountsOnlySlotsOwnedByEachTarget(t *testing.T) {
	nodes := []NodeSnapshot{
		testNodeSnapshot("node-a", domain.OSLinux, domain.ArchAMD64, 2),
		testNodeSnapshot("node-b", domain.OSLinux, domain.ArchAMD64, 1),
	}
	targets := []TargetSpec{
		testTargetSpec("target-a", nil, nil),
		testTargetSpec("target-b", nil, nil),
	}
	scheduler := newTestScheduler(t, nodes, targets, nil)

	grants := collectGrants(t, scheduler, []domain.TargetID{"target-a", "target-b"}, 3)
	assertCapacity(t, scheduler, "target-a", 2)
	assertCapacity(t, scheduler, "target-b", 1)

	bound, err := scheduler.BindExecution(grants[0].Ref(), "execution-a")
	if err != nil {
		t.Fatal(err)
	}
	if bound.ExecutionID != "execution-a" {
		t.Fatalf("bound execution = %q", bound.ExecutionID)
	}
	assertCapacity(t, scheduler, bound.TargetID, 2)

	if err := scheduler.Release(grants[1].Ref()); err != nil {
		t.Fatal(err)
	}
	assertCapacity(t, scheduler, grants[1].TargetID, 0)

	capacities := scheduler.Capacities()
	if len(capacities) != 2 ||
		capacities[0] != (TargetCapacity{TargetID: "target-a", Advertised: 2}) ||
		capacities[1] != (TargetCapacity{TargetID: "target-b", Advertised: 0}) {
		t.Fatalf("capacities = %+v", capacities)
	}
	assertSchedulerInvariant(t, scheduler)
}

func TestBindIsIdempotentButMismatchesAndDuplicateReleaseFailClosed(t *testing.T) {
	scheduler := newTestScheduler(t,
		[]NodeSnapshot{testNodeSnapshot("node-a", domain.OSLinux, domain.ArchAMD64, 2)},
		[]TargetSpec{testTargetSpec("target-a", nil, nil), testTargetSpec("target-b", nil, nil)},
		nil,
	)
	grant := collectGrants(t, scheduler, []domain.TargetID{"target-a"}, 1)[0]

	first, err := scheduler.BindExecution(grant.Ref(), "execution-a")
	if err != nil {
		t.Fatal(err)
	}
	replay, err := scheduler.BindExecution(grant.Ref(), "execution-a")
	if err != nil {
		t.Fatalf("exact bind replay: %v", err)
	}
	if replay != first {
		t.Fatalf("bind replay = %+v, want %+v", replay, first)
	}
	assertSchedulerCode(t, bindError(scheduler, grant.Ref(), "execution-b"), "grant_execution_mismatch")

	wrongTarget := grant.Ref()
	wrongTarget.TargetID = "target-b"
	assertSchedulerCode(t, bindError(scheduler, wrongTarget, "execution-a"), "grant_reference_mismatch")

	second := collectGrants(t, scheduler, []domain.TargetID{"target-b"}, 1)[0]
	assertSchedulerCode(t, bindError(scheduler, second.Ref(), "execution-a"), "execution_already_bound")
	if err := scheduler.Release(second.Ref()); err != nil {
		t.Fatal(err)
	}

	assertSchedulerCode(t, scheduler.Release(grant.Ref()), "grant_reference_mismatch")
	if got := scheduler.ActiveGrantCount(); got != 1 {
		t.Fatalf("stale pre-bind release changed active grant count to %d", got)
	}
	if err := scheduler.Release(first.Ref()); err != nil {
		t.Fatal(err)
	}
	assertSchedulerCode(t, scheduler.Release(first.Ref()), "grant_not_active")

	replacement := collectGrants(t, scheduler, []domain.TargetID{"target-a"}, 1)[0]
	if replacement.ID == grant.ID {
		t.Fatal("grant identifiers must never be reused")
	}
	assertSchedulerCode(t, scheduler.Release(grant.Ref()), "grant_not_active")
	if got := scheduler.ActiveGrantCount(); got != 1 {
		t.Fatalf("stale release changed active grant count to %d", got)
	}
	assertSchedulerInvariant(t, scheduler)
}

func TestGrantNextRejectsInvalidDemandWithoutMutatingCapacity(t *testing.T) {
	scheduler := newTestScheduler(t,
		[]NodeSnapshot{testNodeSnapshot("node-a", domain.OSLinux, domain.ArchAMD64, 1)},
		[]TargetSpec{testTargetSpec("target-a", nil, nil)},
		nil,
	)

	if grant, ok, err := scheduler.GrantNext(nil); err != nil || ok || grant != (Grant{}) {
		t.Fatalf("empty demand = (%+v, %t, %v), want zero, false, nil", grant, ok, err)
	}
	assertSchedulerCode(t, grantError(scheduler, []domain.TargetID{"target-a", "target-a"}), "duplicate_demand_target")
	assertSchedulerCode(t, grantError(scheduler, []domain.TargetID{"unknown"}), "target_not_found")
	if got := scheduler.ActiveGrantCount(); got != 0 {
		t.Fatalf("invalid demand changed active grant count to %d", got)
	}
}

func TestUpdateNodeChangesEligibilityButCannotChangeSlotTopology(t *testing.T) {
	node := testNodeSnapshot("node-a", domain.OSLinux, domain.ArchAMD64, 1)
	scheduler := newTestScheduler(t,
		[]NodeSnapshot{node},
		[]TargetSpec{testTargetSpec("target-a", nil, nil)},
		nil,
	)

	node.Node.AdministrativeState = domain.NodeDraining
	if err := scheduler.UpdateNode(node); err != nil {
		t.Fatal(err)
	}
	if grant, ok, err := scheduler.GrantNext([]domain.TargetID{"target-a"}); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatalf("draining updated node received grant: %+v", grant)
	}

	node.Node.MaxRunners = 2
	assertSchedulerCode(t, scheduler.UpdateNode(node), "node_topology_mismatch")
	unknown := testNodeSnapshot("node-unknown", domain.OSLinux, domain.ArchAMD64, 1)
	assertSchedulerCode(t, scheduler.UpdateNode(unknown), "node_not_found")
}

func TestUpdateNodeOwnershipDriftBlocksAdmissionAndActiveRelease(t *testing.T) {
	node := testNodeSnapshot("node-a", domain.OSLinux, domain.ArchAMD64, 2)
	fleetMaximum := 2
	scheduler := newTestScheduler(t,
		[]NodeSnapshot{node},
		[]TargetSpec{testTargetSpec("target-a", nil, nil)},
		&fleetMaximum,
	)
	grant := collectGrants(t, scheduler, []domain.TargetID{"target-a"}, 1)[0]
	bound, err := scheduler.BindExecution(grant.Ref(), "execution-a")
	if err != nil {
		t.Fatal(err)
	}

	node.ActiveExecutions = []domain.ExecutionID{"execution-a", "execution-unbacked"}
	if err := scheduler.UpdateNode(node); err != nil {
		t.Fatal(err)
	}
	if got := scheduler.OccupiedCapacity(); got != fleetMaximum {
		t.Fatalf("drift occupied capacity = %d, want %d", got, fleetMaximum)
	}
	assertSchedulerCode(t, scheduler.Release(bound.Ref()), "execution_still_active")
	if extra, ok, err := scheduler.GrantNext([]domain.TargetID{"target-a"}); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatalf("ownership drift admitted another runner: %+v", extra)
	}
	assertCapacity(t, scheduler, "target-a", 1)

	node.ActiveExecutions = nil
	if err := scheduler.UpdateNode(node); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Release(bound.Ref()); err != nil {
		t.Fatal(err)
	}
	assertSchedulerInvariant(t, scheduler)
}

func TestConcurrentGrantNeverSharesOnePhysicalSlot(t *testing.T) {
	fleetMaximum := 1
	scheduler := newTestScheduler(t,
		[]NodeSnapshot{testNodeSnapshot("node-a", domain.OSLinux, domain.ArchAMD64, 1)},
		[]TargetSpec{testTargetSpec("target-a", nil, nil), testTargetSpec("target-b", nil, nil)},
		&fleetMaximum,
	)

	var successes atomic.Int32
	var group sync.WaitGroup
	errors := make(chan error, 64)
	for attempt := 0; attempt < 64; attempt++ {
		group.Add(1)
		go func() {
			defer group.Done()
			_, ok, err := scheduler.GrantNext([]domain.TargetID{"target-b", "target-a"})
			if err != nil {
				errors <- err
				return
			}
			if ok {
				successes.Add(1)
			}
		}()
	}
	group.Wait()
	close(errors)
	for err := range errors {
		t.Errorf("concurrent GrantNext: %v", err)
	}
	if got := successes.Load(); got != 1 {
		t.Fatalf("successful grants = %d, want 1", got)
	}
	if grants := scheduler.Grants(); len(grants) != 1 || grants[0].Slot != (domain.SlotKey{NodeID: "node-a", Index: 0}) {
		t.Fatalf("grants = %+v", grants)
	}
	assertSchedulerInvariant(t, scheduler)
}

func TestRandomizedHistoriesPreserveCapacityAndOwnership(t *testing.T) {
	nodes := []NodeSnapshot{
		withNodeResources(testNodeSnapshot("node-a", domain.OSLinux, domain.ArchAMD64, 2), 8*gibibyte, "runner-v1"),
		withNodeResources(testNodeSnapshot("node-b", domain.OSLinux, domain.ArchARM64, 2), 6*gibibyte, "runner-v1"),
		withNodeResources(testNodeSnapshot("node-c", domain.OSMacOS, domain.ArchARM64, 1), 4*gibibyte, "runner-v1"),
	}
	targets := []TargetSpec{
		testTargetSpec("target-any", nil, nil),
		testTargetSpec("target-linux", operatingSystemPointer(domain.OSLinux), nil),
		testTargetSpec("target-macos", operatingSystemPointer(domain.OSMacOS), architecturePointer(domain.ArchARM64)),
	}
	for index := range targets {
		targets[index].RunnerPackage = "runner-v1"
	}
	fleetMaximum := 4

	for seed := uint64(1); seed <= 32; seed++ {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			scheduler := newTestScheduler(t, nodes, targets, &fleetMaximum)
			rng := rand.New(rand.NewPCG(seed, seed+1))
			executionSequence := 0

			for step := 0; step < 1_000; step++ {
				grants := scheduler.Grants()
				action := rng.IntN(3)
				if len(grants) == 0 {
					action = 0
				}
				switch action {
				case 0:
					demand := randomizedDemand(rng)
					if _, _, err := scheduler.GrantNext(demand); err != nil {
						t.Fatalf("step %d grant: %v", step, err)
					}
				case 1:
					grant := grants[rng.IntN(len(grants))]
					if grant.ExecutionID == "" {
						executionSequence++
						executionID := domain.ExecutionID(fmt.Sprintf("execution-%d", executionSequence))
						if _, err := scheduler.BindExecution(grant.Ref(), executionID); err != nil {
							t.Fatalf("step %d bind: %v", step, err)
						}
						if _, err := scheduler.BindExecution(grant.Ref(), executionID); err != nil {
							t.Fatalf("step %d bind replay: %v", step, err)
						}
					}
				case 2:
					grant := grants[rng.IntN(len(grants))]
					if err := scheduler.Release(grant.Ref()); err != nil {
						t.Fatalf("step %d release: %v", step, err)
					}
				}
				assertRandomizedInvariant(t, scheduler, nodes, fleetMaximum)
			}
		})
	}
}

func TestRandomizedStartupActiveAndNewGrantsNeverExceedBounds(t *testing.T) {
	for seed := uint64(1); seed <= 64; seed++ {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, seed+1))
			nodes := make([]NodeSnapshot, 0, 4)
			slots := make([]domain.SlotKey, 0, 12)
			totalSlots := 0
			for index := 0; index < 4; index++ {
				maxRunners := 1 + rng.IntN(3)
				nodeID := domain.NodeID(fmt.Sprintf("node-%d", index))
				nodes = append(nodes, testNodeSnapshot(nodeID, domain.OSLinux, domain.ArchAMD64, maxRunners))
				for slotIndex := 0; slotIndex < maxRunners; slotIndex++ {
					slots = append(slots, domain.SlotKey{NodeID: nodeID, Index: slotIndex})
				}
				totalSlots += maxRunners
			}
			fleetMaximum := 1 + rng.IntN(totalSlots)
			rng.Shuffle(len(slots), func(i, j int) {
				slots[i], slots[j] = slots[j], slots[i]
			})
			activeCount := rng.IntN(fleetMaximum + 1)
			reservations := make([]RestoredReservation, 0, activeCount)
			nodeIndexes := make(map[domain.NodeID]int, len(nodes))
			for index, node := range nodes {
				nodeIndexes[node.Node.ID] = index
			}
			for index := 0; index < activeCount; index++ {
				executionID := domain.ExecutionID(fmt.Sprintf("execution-%d", index))
				slot := slots[index]
				nodeIndex := nodeIndexes[slot.NodeID]
				nodes[nodeIndex].ActiveExecutions = append(nodes[nodeIndex].ActiveExecutions, executionID)
				reservations = append(reservations, RestoredReservation{
					TargetID:    "target-existing",
					Slot:        slot,
					ExecutionID: executionID,
				})
			}
			targets := []TargetSpec{
				testTargetSpec("target-existing", nil, nil),
				testTargetSpec("target-demand", nil, nil),
			}
			scheduler, err := NewWithReservations(nodes, targets, &fleetMaximum, reservations)
			if err != nil {
				t.Fatal(err)
			}
			assertRandomizedInvariant(t, scheduler, nodes, fleetMaximum)

			for {
				grant, ok, err := scheduler.GrantNext([]domain.TargetID{"target-demand"})
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				if grant.TargetID != "target-demand" {
					t.Fatalf("new grant target = %q", grant.TargetID)
				}
				assertRandomizedInvariant(t, scheduler, nodes, fleetMaximum)
			}
			if got := scheduler.OccupiedCapacity(); got != fleetMaximum {
				t.Fatalf("occupied capacity after filling = %d, want %d", got, fleetMaximum)
			}
		})
	}
}

func randomizedDemand(rng *rand.Rand) []domain.TargetID {
	all := []domain.TargetID{"target-any", "target-linux", "target-macos"}
	demand := make([]domain.TargetID, 0, len(all))
	for _, targetID := range all {
		if rng.IntN(2) == 0 {
			demand = append(demand, targetID)
		}
	}
	if len(demand) == 0 {
		demand = append(demand, all[rng.IntN(len(all))])
	}
	return demand
}

func assertRandomizedInvariant(t *testing.T, scheduler *Scheduler, nodes []NodeSnapshot, fleetMaximum int) {
	t.Helper()
	grants := scheduler.Grants()
	if len(grants) > fleetMaximum {
		t.Fatalf("active grants = %d, fleet maximum = %d", len(grants), fleetMaximum)
	}
	if occupied := scheduler.OccupiedCapacity(); occupied > fleetMaximum {
		t.Fatalf("occupied capacity = %d, fleet maximum = %d", occupied, fleetMaximum)
	}
	nodeMaxima := make(map[domain.NodeID]int, len(nodes))
	for _, node := range nodes {
		nodeMaxima[node.Node.ID] = node.Node.MaxRunners
	}
	slots := make(map[domain.SlotKey]GrantID, len(grants))
	perNode := make(map[domain.NodeID]int)
	perTarget := make(map[domain.TargetID]int)
	for _, grant := range grants {
		if previous, duplicate := slots[grant.Slot]; duplicate {
			t.Fatalf("slot %+v owned by grants %d and %d", grant.Slot, previous, grant.ID)
		}
		slots[grant.Slot] = grant.ID
		perNode[grant.Slot.NodeID]++
		perTarget[grant.TargetID]++
	}
	for nodeID, count := range perNode {
		if count > nodeMaxima[nodeID] {
			t.Fatalf("node %q grants = %d, max = %d", nodeID, count, nodeMaxima[nodeID])
		}
	}
	for _, capacity := range scheduler.Capacities() {
		if capacity.Advertised != perTarget[capacity.TargetID] {
			t.Fatalf("target %q capacity = %d, owned slots = %d", capacity.TargetID, capacity.Advertised, perTarget[capacity.TargetID])
		}
	}
	assertSchedulerInvariant(t, scheduler)
}

func testNodeSnapshot(id domain.NodeID, operatingSystem domain.OperatingSystem, architecture domain.Architecture, maxRunners int) NodeSnapshot {
	return NodeSnapshot{
		Node: domain.Node{
			ID:                  id,
			DisplayName:         string(id),
			OS:                  operatingSystem,
			Architecture:        architecture,
			MaxRunners:          maxRunners,
			AdministrativeState: domain.NodeActive,
			ObservedState:       domain.NodeOnline,
		},
		Reconciled:           true,
		NativeReady:          true,
		AvailableMemoryBytes: 8 * gibibyte,
	}
}

func testTargetSpec(id domain.TargetID, operatingSystem *domain.OperatingSystem, architecture *domain.Architecture) TargetSpec {
	profileID := domain.RunnerProfileID("profile-" + id)
	return TargetSpec{
		Target: domain.GitHubTarget{
			ID:                    id,
			InstallationID:        "installation-" + string(id),
			ScopeKind:             domain.TargetRepository,
			Scope:                 "owner/" + string(id),
			Visibility:            domain.TargetPrivate,
			RunnerGroupAccessSafe: true,
			ScaleSetName:          "sparerunner-" + string(id),
			RunnerProfileID:       profileID,
		},
		Profile: domain.RunnerProfile{
			ID:            profileID,
			Label:         "sparerunner-" + string(id),
			OS:            operatingSystem,
			Architecture:  architecture,
			VersionPolicy: domain.RunnerVersionPinned,
			Runtime:       domain.RuntimeNative,
		},
	}
}

func withNodeResources(node NodeSnapshot, availableMemory uint64, packages ...string) NodeSnapshot {
	node.AvailableMemoryBytes = availableMemory
	node.CachedRunnerPackages = packages
	return node
}

func operatingSystemPointer(value domain.OperatingSystem) *domain.OperatingSystem {
	return &value
}

func architecturePointer(value domain.Architecture) *domain.Architecture {
	return &value
}

func intPointer(value int) *int {
	return &value
}

func newTestScheduler(t *testing.T, nodes []NodeSnapshot, targets []TargetSpec, fleetMaximum *int) *Scheduler {
	t.Helper()
	scheduler, err := New(nodes, targets, fleetMaximum)
	if err != nil {
		t.Fatal(err)
	}
	return scheduler
}

func collectGrantSequence(t *testing.T, scheduler *Scheduler, demand []domain.TargetID, count int) []string {
	t.Helper()
	grants := collectGrants(t, scheduler, demand, count)
	sequence := make([]string, 0, len(grants))
	for _, grant := range grants {
		sequence = append(sequence, fmt.Sprintf("%s/%s/%d", grant.TargetID, grant.Slot.NodeID, grant.Slot.Index))
	}
	return sequence
}

func collectGrants(t *testing.T, scheduler *Scheduler, demand []domain.TargetID, count int) []Grant {
	t.Helper()
	grants := make([]Grant, 0, count)
	for index := 0; index < count; index++ {
		grant, ok, err := scheduler.GrantNext(demand)
		if err != nil {
			t.Fatalf("grant %d: %v", index, err)
		}
		if !ok {
			t.Fatalf("grant %d returned no capacity", index)
		}
		grants = append(grants, grant)
	}
	return grants
}

func assertCapacity(t *testing.T, scheduler *Scheduler, targetID domain.TargetID, want int) {
	t.Helper()
	got, err := scheduler.Capacity(targetID)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("target %q capacity = %d, want %d", targetID, got, want)
	}
}

func assertSchedulerInvariant(t *testing.T, scheduler *Scheduler) {
	t.Helper()
	if err := scheduler.Validate(); err != nil {
		t.Fatalf("scheduler invariant: %v", err)
	}
}

func assertSchedulerCode(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected scheduler error code %q", want)
	}
	schedulerError, ok := err.(*Error)
	if !ok {
		t.Fatalf("error = %T %v, want *scheduler.Error", err, err)
	}
	if schedulerError.Code != want {
		t.Fatalf("error code = %q, want %q (%v)", schedulerError.Code, want, err)
	}
}

func bindError(scheduler *Scheduler, ref GrantRef, executionID domain.ExecutionID) error {
	_, err := scheduler.BindExecution(ref, executionID)
	return err
}

func grantError(scheduler *Scheduler, demand []domain.TargetID) error {
	_, _, err := scheduler.GrantNext(demand)
	return err
}
