package store

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/enroll"
)

func TestManagementConfigurationDefaultsExistingNodeAndAppliesAtomically(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "management-configuration.db")
	defer controller.Close()
	nodeID, _ := enrollControllerAgentNode(t, controller, 1)

	initial, err := controller.ReadManagementConfiguration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if initial.Revision != 1 ||
		initial.FleetMaxRunners != nil ||
		len(initial.Nodes) != 1 ||
		initial.Nodes[0] != (ManagementNodeConfiguration{
			NodeID:      domain.NodeID(nodeID),
			DisplayName: nodeID,
			MaxRunners:  domain.DefaultMaxRunners,
		}) {
		t.Fatalf("initial management configuration = %#v", initial)
	}

	desired, verified := managementConfigurationFixture(domain.NodeID(nodeID))
	applied, err := controller.ApplyManagementConfiguration(
		ctx,
		initial.Revision,
		desired,
		verified,
		managementConfigurationAudit(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if applied.Revision != 2 ||
		applied.FleetMaxRunners == nil ||
		*applied.FleetMaxRunners != 4 ||
		len(applied.Nodes) != 1 ||
		len(applied.RunnerProfiles) != 1 ||
		len(applied.GitHubTargets) != 1 {
		t.Fatalf("applied management configuration = %#v", applied)
	}
	readBack, err := controller.ReadManagementConfiguration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(readBack, applied) {
		t.Fatalf("management readback = %#v, want %#v", readBack, applied)
	}

	restart, err := controller.RestartSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if restart.FleetMaxRunners == nil ||
		*restart.FleetMaxRunners != 4 ||
		len(restart.NodeTopology) != 1 ||
		restart.NodeTopology[0].DisplayName != "Build Desk" ||
		restart.NodeTopology[0].MaxRunners != 3 {
		t.Fatalf("restart topology did not use management settings: %#v", restart)
	}

	profile, found, err := controller.ReadRunnerProfile(ctx, "profile-linux")
	if err != nil || !found {
		t.Fatalf("runtime profile projection = %#v, %t, %v", profile, found, err)
	}
	if profile.VersionPolicy != domain.RunnerVersionAutoUpdate ||
		profile.RunnerVersion != "2.321.0" {
		t.Fatalf("runtime profile projection = %#v", profile)
	}
	binding, found, err := controller.ReadGitHubTargetRuntimeBinding(ctx, "target-private")
	if err != nil || !found {
		t.Fatalf("runtime target projection = %#v, %t, %v", binding, found, err)
	}
	if binding.ScaleSetID != 41 || binding.ProfileID != "profile-linux" {
		t.Fatalf("runtime target projection = %#v", binding)
	}

	events, err := controller.ReadAuditEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 ||
		events[0].Sequence != 1 ||
		events[0].Revision != 2 ||
		events[0].OccurredAtUnixNano != 100_000_000_000 ||
		events[0].Record != managementConfigurationAudit() {
		t.Fatalf("configuration audit = %#v", events)
	}
}

func TestEnrollmentAdvancesManagementRevisionAndStalesPriorConfiguration(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "management-enrollment-revision.db")
	defer controller.Close()
	before, err := controller.ReadManagementConfiguration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if before.Revision != 0 || len(before.Nodes) != 0 {
		t.Fatalf("pre-enrollment configuration = %#v", before)
	}

	nodeID, _ := enrollControllerAgentNode(t, controller, 2)
	after, err := controller.ReadManagementConfiguration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.Revision != 1 || len(after.Nodes) != 1 ||
		after.Nodes[0].NodeID != domain.NodeID(nodeID) {
		t.Fatalf("post-enrollment configuration = %#v", after)
	}
	_, err = controller.ApplyManagementConfiguration(
		ctx,
		before.Revision,
		DesiredManagementConfiguration{},
		ManagementVerifiedAuthorities{},
		managementConfigurationAudit(),
	)
	var stale *StaleManagementRevisionError
	if !errors.As(err, &stale) ||
		stale.Expected != before.Revision ||
		stale.Actual != after.Revision {
		t.Fatalf("pre-enrollment writer error = %#v", err)
	}
	events, err := controller.ReadAuditEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("stale pre-enrollment writer appended audit: %#v", events)
	}
}

func TestManagementConfigurationStaleRevisionIsTypedAndDoesNotMutate(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "management-stale.db")
	defer controller.Close()
	nodeID, _ := enrollControllerAgentNode(t, controller, 2)
	first, verified := managementConfigurationFixture(domain.NodeID(nodeID))
	if _, err := controller.ApplyManagementConfiguration(
		ctx, 1, first, verified, managementConfigurationAudit(),
	); err != nil {
		t.Fatal(err)
	}

	staleDesired := first
	staleDesired.Nodes = append([]ManagementNodeConfiguration(nil), first.Nodes...)
	staleDesired.Nodes[0].DisplayName = "Stale Writer"
	_, err := controller.ApplyManagementConfiguration(
		ctx,
		1,
		staleDesired,
		ManagementVerifiedAuthorities{},
		managementConfigurationAudit(),
	)
	var stale *StaleManagementRevisionError
	if !errors.As(err, &stale) || stale.Expected != 1 || stale.Actual != 2 ||
		!errors.Is(err, ErrStaleManagementRevision) {
		t.Fatalf("stale apply error = %#v", err)
	}
	if !controller.ManagementAuditHealthy() {
		t.Fatal("stale revision degraded audit persistence health")
	}
	readBack, readErr := controller.ReadManagementConfiguration(ctx)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if readBack.Revision != 2 || readBack.Nodes[0].DisplayName != "Build Desk" {
		t.Fatalf("stale writer mutated configuration: %#v", readBack)
	}
	events, auditErr := controller.ReadAuditEvents(ctx)
	if auditErr != nil {
		t.Fatal(auditErr)
	}
	if len(events) != 1 {
		t.Fatalf("stale writer appended audit events: %#v", events)
	}
}

func TestManagementAuditInsertFailureRollsBackConfiguration(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "management-audit-fault.db")
	defer controller.Close()
	auditChanged := controller.ManagementAuditChange()
	select {
	case <-auditChanged:
		t.Fatal("healthy audit authority exposed a closed change channel")
	default:
	}
	nodeID, _ := enrollControllerAgentNode(t, controller, 3)
	if _, err := controller.db.ExecContext(ctx, `CREATE TRIGGER fail_management_audit_insert
		BEFORE INSERT ON management_audit_events
		BEGIN
			SELECT RAISE(ABORT, 'injected management audit failure');
		END`); err != nil {
		t.Fatal(err)
	}

	desired, verified := managementConfigurationFixture(domain.NodeID(nodeID))
	_, err := controller.ApplyManagementConfiguration(
		ctx,
		1,
		desired,
		verified,
		managementConfigurationAudit(),
	)
	if err == nil {
		t.Fatal("audit insert failure did not fail configuration apply")
	}
	if controller.ManagementAuditHealthy() {
		t.Fatal("audit insert failure did not close audit health authority")
	}
	select {
	case <-auditChanged:
	default:
		t.Fatal("audit insert failure did not signal long-lived admission loops")
	}
	readBack, readErr := controller.ReadManagementConfiguration(ctx)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if readBack.Revision != 1 ||
		len(readBack.RunnerProfiles) != 0 ||
		len(readBack.GitHubTargets) != 0 ||
		readBack.Nodes[0].DisplayName != nodeID ||
		readBack.Nodes[0].MaxRunners != domain.DefaultMaxRunners {
		t.Fatalf("audit failure partially mutated configuration: %#v", readBack)
	}
}

func TestManagementMutationCommitLinearizesWithAuditDegradation(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "management-audit-linearized.db")
	defer controller.Close()
	nodeID, _ := enrollControllerAgentNode(t, controller, 3)
	desired, verified := managementConfigurationFixture(domain.NodeID(nodeID))

	commitEntered := make(chan struct{})
	releaseCommit := make(chan struct{})
	controller.beforeManagementMutationCommit = func() {
		close(commitEntered)
		<-releaseCommit
	}
	commitResult := make(chan error, 1)
	go func() {
		_, err := controller.ApplyManagementConfiguration(
			ctx,
			1,
			desired,
			verified,
			managementConfigurationAudit(),
		)
		commitResult <- err
	}()
	select {
	case <-commitEntered:
	case <-time.After(time.Second):
		t.Fatal("management mutation did not reach the audit-gated commit point")
	}

	degradeStarted := make(chan struct{})
	degradeDone := make(chan struct{})
	go func() {
		close(degradeStarted)
		controller.degradeManagementAudit()
		close(degradeDone)
	}()
	<-degradeStarted
	select {
	case <-degradeDone:
		t.Fatal("audit degradation crossed an in-progress management commit")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseCommit)
	if err := <-commitResult; err != nil {
		t.Fatalf("mutation linearized before audit degradation = %v", err)
	}
	select {
	case <-degradeDone:
	case <-time.After(time.Second):
		t.Fatal("audit degradation did not complete after prior mutation")
	}
	controller.beforeManagementMutationCommit = nil
	if controller.ManagementAuditHealthy() {
		t.Fatal("audit authority remained healthy after degradation")
	}
	committed, err := controller.ReadManagementConfiguration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if committed.Revision != 2 ||
		committed.Nodes[0].DisplayName != "Build Desk" {
		t.Fatalf("first management commit = %#v", committed)
	}

	rejected := cloneDesiredForTest(desired)
	rejected.Nodes[0].DisplayName = "Must not commit"
	if _, err := controller.ApplyManagementConfiguration(
		ctx,
		2,
		rejected,
		ManagementVerifiedAuthorities{},
		managementConfigurationAudit(),
	); !errors.Is(err, ErrManagementAuditPersistence) {
		t.Fatalf("post-degradation management mutation error = %v", err)
	}
	after, err := controller.ReadManagementConfiguration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.Revision != 2 || after.Nodes[0].DisplayName != "Build Desk" {
		t.Fatalf("post-degradation mutation committed: %#v", after)
	}
}

func TestManagementApplyPreservesVerifiedAuthorityAndRejectsUnverifiedTarget(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "management-verified-authority.db")
	defer controller.Close()
	nodeID, _ := enrollControllerAgentNode(t, controller, 4)
	first, verified := managementConfigurationFixture(domain.NodeID(nodeID))
	if _, err := controller.ApplyManagementConfiguration(
		ctx, 1, first, verified, managementConfigurationAudit(),
	); err != nil {
		t.Fatal(err)
	}

	second := first
	second.RunnerProfiles = append([]domain.RunnerProfile(nil), first.RunnerProfiles...)
	second.RunnerProfiles[0].Label = "tewake-linux-updated"
	applied, err := controller.ApplyManagementConfiguration(
		ctx,
		2,
		second,
		ManagementVerifiedAuthorities{},
		managementConfigurationAudit(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if applied.RunnerProfiles[0].RunnerVersion != "2.321.0" ||
		applied.GitHubTargets[0].Target.Visibility != domain.TargetPrivate ||
		!applied.GitHubTargets[0].Target.RunnerGroupAccessSafe ||
		applied.GitHubTargets[0].ScaleSetID != 41 {
		t.Fatalf("verified authority was not preserved: %#v", applied)
	}

	unverified := second
	unverified.GitHubTargets = append(
		[]DesiredManagementGitHubTarget(nil),
		second.GitHubTargets...,
	)
	unverified.GitHubTargets = append(
		unverified.GitHubTargets,
		DesiredManagementGitHubTarget{
			ID:              "target-unverified",
			InstallationID:  "installation-42",
			ScopeKind:       domain.TargetOrganization,
			Scope:           "example-org",
			ScaleSetName:    "tewake-linux-second",
			RunnerProfileID: "profile-linux",
		},
	)
	if _, err := controller.ApplyManagementConfiguration(
		ctx,
		3,
		unverified,
		ManagementVerifiedAuthorities{},
		managementConfigurationAudit(),
	); !errors.Is(err, ErrManagementConfiguration) {
		t.Fatalf("unverified target error = %v", err)
	}
	readBack, err := controller.ReadManagementConfiguration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if readBack.Revision != 3 || len(readBack.GitHubTargets) != 1 {
		t.Fatalf("unverified target partially mutated configuration: %#v", readBack)
	}
}

func TestManagementApplyRejectsCapacityReductionAndActiveTargetDeletion(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "management-active-capacity.db")
	defer controller.Close()
	nodeID, _ := enrollControllerAgentNode(t, controller, 4)
	desired, verified := managementConfigurationFixture(domain.NodeID(nodeID))
	if _, err := controller.ApplyManagementConfiguration(
		ctx,
		1,
		desired,
		verified,
		managementConfigurationAudit(),
	); err != nil {
		t.Fatal(err)
	}
	for index, slot := range []int{0, 2} {
		assignment := testAssignment(
			MessageID(600+index),
			"management-capacity-execution-"+string(rune('a'+index)),
			nodeID,
			slot,
		)
		assignment.Execution.TargetID = "target-private"
		if _, replayed, err := controller.Assign(ctx, assignment); err != nil || replayed {
			t.Fatalf("assign slot %d = (%t, %v)", slot, replayed, err)
		}
	}

	nodeReduction := cloneDesiredForTest(desired)
	nodeReduction.Nodes[0].MaxRunners = 2
	if _, err := controller.ApplyManagementConfiguration(
		ctx,
		2,
		nodeReduction,
		ManagementVerifiedAuthorities{},
		managementConfigurationAudit(),
	); !errors.Is(err, ErrManagementConfiguration) {
		t.Fatalf("node capacity reduction error = %v", err)
	}

	fleetReduction := cloneDesiredForTest(desired)
	fleetMaximum := 1
	fleetReduction.FleetMaxRunners = &fleetMaximum
	if _, err := controller.ApplyManagementConfiguration(
		ctx,
		2,
		fleetReduction,
		ManagementVerifiedAuthorities{},
		managementConfigurationAudit(),
	); !errors.Is(err, ErrManagementConfiguration) {
		t.Fatalf("fleet capacity reduction error = %v", err)
	}

	targetDeletion := cloneDesiredForTest(desired)
	targetDeletion.GitHubTargets = nil
	if _, err := controller.ApplyManagementConfiguration(
		ctx,
		2,
		targetDeletion,
		ManagementVerifiedAuthorities{},
		managementConfigurationAudit(),
	); !errors.Is(err, ErrManagementConfiguration) {
		t.Fatalf("active target deletion error = %v", err)
	}

	readBack, err := controller.ReadManagementConfiguration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	events, err := controller.ReadAuditEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if readBack.Revision != 2 ||
		readBack.Nodes[0].MaxRunners != 3 ||
		*readBack.FleetMaxRunners != 4 ||
		len(readBack.GitHubTargets) != 1 ||
		len(events) != 1 ||
		!controller.ManagementAuditHealthy() {
		t.Fatalf(
			"rejected active reduction mutated authority: config=%#v events=%#v health=%t",
			readBack,
			events,
			controller.ManagementAuditHealthy(),
		)
	}
}

func TestManagementAuditIsAllowlistedAndAppendOnly(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "management-audit-append-only.db")
	defer controller.Close()

	if _, err := controller.AppendAuditEvent(ctx, AuditRecord{
		Actor:        AuditActor("Authorization: secret-canary"),
		Action:       AuditActionAuthenticationFailed,
		Outcome:      AuditOutcomeRejected,
		ResourceKind: AuditResourceController,
		ErrorCode:    AuditErrorAuthenticationFailed,
		RequestID:    "req_11111111111111111111111111111111",
	}); err == nil {
		t.Fatal("non-allowlisted audit actor was persisted")
	}
	if !controller.ManagementAuditHealthy() {
		t.Fatal("invalid caller record degraded audit persistence health")
	}
	if _, err := controller.AppendAuditEvent(ctx, AuditRecord{
		Actor:        AuditActorAnonymous,
		Action:       AuditActionAuthenticationFailed,
		Outcome:      AuditOutcomeRejected,
		ResourceKind: AuditResourceController,
		ErrorCode:    AuditErrorAuthenticationFailed,
		RequestID:    "req_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
	}); err == nil {
		t.Fatal("non-canonical request ID was persisted")
	}
	if !controller.ManagementAuditHealthy() {
		t.Fatal("invalid request ID degraded audit persistence health")
	}
	event, err := controller.AppendAuditEvent(ctx, AuditRecord{
		Actor:        AuditActorAnonymous,
		Action:       AuditActionAuthenticationFailed,
		Outcome:      AuditOutcomeRejected,
		ResourceKind: AuditResourceController,
		ErrorCode:    AuditErrorAuthenticationFailed,
		RequestID:    "req_22222222222222222222222222222222",
	})
	if err != nil {
		t.Fatal(err)
	}
	if event.Sequence != 1 || event.Revision != 0 {
		t.Fatalf("appended audit event = %#v", event)
	}
	if _, err := controller.db.ExecContext(ctx,
		"UPDATE management_audit_events SET outcome = 'failed' WHERE sequence = 1",
	); err == nil {
		t.Fatal("audit UPDATE was accepted")
	}
	if _, err := controller.db.ExecContext(ctx,
		"DELETE FROM management_audit_events WHERE sequence = 1",
	); err == nil {
		t.Fatal("audit DELETE was accepted")
	}
	events, err := controller.ReadAuditEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0] != event {
		t.Fatalf("append-only audit contents = %#v, want %#v", events, event)
	}
}

func TestManagementAuditPageUsesExclusiveCursorAndBoundedLookahead(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "management-audit-page.db")
	defer controller.Close()

	var appended []AuditEvent
	for index := 0; index < 5; index++ {
		event, err := controller.AppendAuditEvent(ctx, AuditRecord{
			Actor:        AuditActorAnonymous,
			Action:       AuditActionAuthenticationFailed,
			Outcome:      AuditOutcomeRejected,
			ResourceKind: AuditResourceController,
			ErrorCode:    AuditErrorAuthenticationFailed,
			RequestID:    fmt.Sprintf("req_%032x", index+1),
		})
		if err != nil {
			t.Fatal(err)
		}
		appended = append(appended, event)
	}

	first, err := controller.ReadAuditEventsPage(ctx, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first.Events, appended[:2]) ||
		first.NextAfter == nil ||
		*first.NextAfter != appended[1].Sequence ||
		first.ResumeAfter == nil ||
		*first.ResumeAfter != appended[1].Sequence {
		t.Fatalf("first audit page = %#v", first)
	}

	second, err := controller.ReadAuditEventsPage(ctx, *first.NextAfter, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(second.Events, appended[2:4]) ||
		second.NextAfter == nil ||
		*second.NextAfter != appended[3].Sequence ||
		second.ResumeAfter == nil ||
		*second.ResumeAfter != appended[3].Sequence {
		t.Fatalf("second audit page = %#v", second)
	}

	last, err := controller.ReadAuditEventsPage(ctx, *second.NextAfter, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(last.Events, appended[4:]) ||
		last.NextAfter != nil ||
		last.ResumeAfter == nil ||
		*last.ResumeAfter != appended[4].Sequence {
		t.Fatalf("last audit page = %#v", last)
	}

	empty, err := controller.ReadAuditEventsPage(ctx, appended[4].Sequence, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(empty.Events) != 0 ||
		empty.NextAfter != nil ||
		empty.ResumeAfter == nil ||
		*empty.ResumeAfter != appended[4].Sequence {
		t.Fatalf("empty audit page = %#v", empty)
	}
}

func TestManagementAuditPageRejectsUnboundedLimits(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "management-audit-page-limits.db")
	defer controller.Close()

	for _, limit := range []int{-1, 0, MaximumAuditPageSize + 1} {
		if _, err := controller.ReadAuditEventsPage(ctx, 0, limit); !errors.Is(
			err,
			ErrInvalidAuditPage,
		) {
			t.Fatalf("limit %d error = %v, want %v", limit, err, ErrInvalidAuditPage)
		}
	}
}

func TestManagementRejectionAuditFailureClosesHealth(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "management-rejection-audit-fault.db")
	defer controller.Close()
	if !controller.ManagementAuditHealthy() {
		t.Fatal("new management audit authority is unhealthy")
	}
	if _, err := controller.db.ExecContext(ctx, `CREATE TRIGGER fail_rejection_audit_insert
		BEFORE INSERT ON management_audit_events
		BEGIN
			SELECT RAISE(ABORT, 'injected rejection audit failure');
		END`); err != nil {
		t.Fatal(err)
	}
	_, err := controller.AppendAuditEvent(ctx, AuditRecord{
		Actor:        AuditActorAnonymous,
		Action:       AuditActionAuthenticationFailed,
		Outcome:      AuditOutcomeRejected,
		ResourceKind: AuditResourceController,
		ErrorCode:    AuditErrorMutationRejected,
		RequestID:    "req_33333333333333333333333333333333",
	})
	if !errors.Is(err, ErrManagementAuditPersistence) {
		t.Fatalf("rejection audit persistence error = %v", err)
	}
	if controller.ManagementAuditHealthy() {
		t.Fatal("rejection audit failure did not close audit health authority")
	}
}

func TestManagementAuditBeginFailureClosesHealth(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "management-audit-begin-fault.db")
	if err := controller.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := controller.AppendAuditEvent(ctx, AuditRecord{
		Actor:        AuditActorAnonymous,
		Action:       AuditActionAuthenticationFailed,
		Outcome:      AuditOutcomeRejected,
		ResourceKind: AuditResourceController,
		ErrorCode:    AuditErrorAuthenticationFailed,
		RequestID:    "req_34343434343434343434343434343434",
	})
	if err == nil {
		t.Fatal("closed database accepted rejection audit")
	}
	if controller.ManagementAuditHealthy() {
		t.Fatal("audit begin failure did not close audit health authority")
	}
}

func TestManagementAuditSupportsSessionJoinAndNodeActions(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "management-audit-actions.db")
	defer controller.Close()
	records := []AuditRecord{
		{
			Actor:        AuditActorAnonymous,
			Action:       AuditActionEnrollmentRejected,
			Outcome:      AuditOutcomeRejected,
			ResourceKind: AuditResourceController,
			ErrorCode:    AuditErrorEnrollmentRateLimited,
			RequestID:    "req_41414141414141414141414141414141",
		},
		{
			Actor:        AuditActorAnonymous,
			Action:       AuditActionEnrollmentUnavailable,
			Outcome:      AuditOutcomeFailed,
			ResourceKind: AuditResourceController,
			ErrorCode:    AuditErrorEnrollmentUnavailable,
			RequestID:    "req_42424242424242424242424242424242",
		},
		{
			Actor:        AuditActorNode,
			Action:       AuditActionAgentSessionRejected,
			Outcome:      AuditOutcomeRejected,
			ResourceKind: AuditResourceNode,
			ResourceID:   "00112233445566778899aabbccddeeff",
			ErrorCode:    AuditErrorAgentProtocolRejected,
			RequestID:    "req_43434343434343434343434343434343",
		},
		{
			Actor:        AuditActorSingleAdmin,
			Action:       AuditActionBrowserHandoffAuthorized,
			Outcome:      AuditOutcomeSucceeded,
			ResourceKind: AuditResourceController,
			RequestID:    "req_40404040404040404040404040404040",
		},
		{
			Actor:        AuditActorSingleAdmin,
			Action:       AuditActionSessionEnded,
			Outcome:      AuditOutcomeSucceeded,
			ResourceKind: AuditResourceController,
			RequestID:    "req_44444444444444444444444444444444",
		},
		{
			Actor:        AuditActorSingleAdmin,
			Action:       AuditActionJoinCodeCreated,
			Outcome:      AuditOutcomeSucceeded,
			ResourceKind: AuditResourceJoinCode,
			ResourceID:   "55555555555555555555555555555555",
			RequestID:    "req_55555555555555555555555555555555",
		},
		{
			Actor:        AuditActorSingleAdmin,
			Action:       AuditActionJoinCodeCancelled,
			Outcome:      AuditOutcomeSucceeded,
			ResourceKind: AuditResourceJoinCode,
			ResourceID:   "55555555555555555555555555555555",
			RequestID:    "req_66666666666666666666666666666666",
		},
		{
			Actor:        AuditActorSingleAdmin,
			Action:       AuditActionNodeDrained,
			Outcome:      AuditOutcomeSucceeded,
			ResourceKind: AuditResourceNode,
			ResourceID:   "node-1",
			RequestID:    "req_77777777777777777777777777777777",
		},
		{
			Actor:        AuditActorSingleAdmin,
			Action:       AuditActionNodeResumed,
			Outcome:      AuditOutcomeSucceeded,
			ResourceKind: AuditResourceNode,
			ResourceID:   "node-1",
			RequestID:    "req_88888888888888888888888888888888",
		},
		{
			Actor:        AuditActorSingleAdmin,
			Action:       AuditActionNodeRevoked,
			Outcome:      AuditOutcomeSucceeded,
			ResourceKind: AuditResourceNode,
			ResourceID:   "node-1",
			RequestID:    "req_99999999999999999999999999999999",
		},
	}
	for _, record := range records {
		if _, err := controller.AppendAuditEvent(ctx, record); err != nil {
			t.Fatalf("append %q audit: %v", record.Action, err)
		}
	}
	events, err := controller.ReadAuditEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != len(records) {
		t.Fatalf("management action audit events = %#v", events)
	}
	for index := range events {
		if events[index].Record != records[index] {
			t.Fatalf(
				"management action event %d = %#v, want %#v",
				index,
				events[index].Record,
				records[index],
			)
		}
	}
}

func TestManagementJoinTokenMutationsCommitWithAudit(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "management-audited-token.db")
	defer controller.Close()
	token := enrollmentToken(10, 1)
	tokenID := hex.EncodeToString(token.ID[:])
	if err := controller.CreateTokenWithAudit(
		ctx,
		token,
		joinCodeAudit(
			AuditActionJoinCodeCreated,
			tokenID,
			"req_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		),
	); err != nil {
		t.Fatal(err)
	}
	if err := controller.CancelTokenWithAudit(
		ctx,
		token.ID,
		joinCodeAudit(
			AuditActionJoinCodeCancelled,
			tokenID,
			"req_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		),
	); err != nil {
		t.Fatal(err)
	}
	if err := controller.CancelToken(ctx, token.ID); !errors.Is(err, enroll.ErrTokenNotFound) {
		t.Fatalf("cancelled audited token remains available: %v", err)
	}
	events, err := controller.ReadAuditEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 ||
		events[0].Revision != 0 ||
		events[1].Revision != 0 ||
		events[0].Record.Action != AuditActionJoinCodeCreated ||
		events[1].Record.Action != AuditActionJoinCodeCancelled {
		t.Fatalf("audited join token events = %#v", events)
	}
}

func TestManagementJoinTokenAuditFailureRollsBackCreation(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "management-audited-token-fault.db")
	defer controller.Close()
	if _, err := controller.db.ExecContext(ctx, `CREATE TRIGGER fail_token_audit_insert
		BEFORE INSERT ON management_audit_events
		BEGIN
			SELECT RAISE(ABORT, 'injected token audit failure');
		END`); err != nil {
		t.Fatal(err)
	}
	token := enrollmentToken(11, 1)
	tokenID := hex.EncodeToString(token.ID[:])
	err := controller.CreateTokenWithAudit(
		ctx,
		token,
		joinCodeAudit(
			AuditActionJoinCodeCreated,
			tokenID,
			"req_cccccccccccccccccccccccccccccccc",
		),
	)
	if !errors.Is(err, ErrManagementAuditPersistence) {
		t.Fatalf("join token audit failure = %v", err)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM enrollment_tokens", 0)
	if controller.ManagementAuditHealthy() {
		t.Fatal("join token audit failure did not close audit health")
	}
}

func TestManagementEnrollmentCommitsWithAuditExactlyOnce(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "management-audited-enrollment.db")
	defer controller.Close()
	token := enrollmentToken(12, 1)
	tokenID := hex.EncodeToString(token.ID[:])
	if err := controller.CreateTokenWithAudit(
		ctx,
		token,
		joinCodeAudit(
			AuditActionJoinCodeCreated,
			tokenID,
			"req_14141414141414141414141414141414",
		),
	); err != nil {
		t.Fatal(err)
	}
	nodeID := enrollmentNodeID(1)
	node := enrollmentNodeRecord(nodeID, "a1", time.Unix(100, 0))
	audit := nodeEnrollmentAudit(
		nodeID,
		"req_15151515151515151515151515151515",
	)

	wrongActor := audit
	wrongActor.Actor = AuditActorSingleAdmin
	if err := controller.ConsumeEnrollmentWithAudit(
		ctx,
		token,
		node,
		wrongActor,
	); !errors.Is(err, ErrManagementAuditRecord) {
		t.Fatalf("single-admin enrollment audit = %v", err)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM enrollment_tokens", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM enrolled_nodes", 0)

	if err := controller.ConsumeEnrollmentWithAudit(
		ctx,
		token,
		node,
		audit,
	); err != nil {
		t.Fatal(err)
	}
	configuration, err := controller.ReadManagementConfiguration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if configuration.Revision != 1 ||
		len(configuration.Nodes) != 1 ||
		configuration.Nodes[0].NodeID != domain.NodeID(nodeID) {
		t.Fatalf("post-enrollment configuration = %#v", configuration)
	}
	replayed, err := controller.ReplayEnrollment(
		ctx,
		token,
		node.PublicKeyDigest,
	)
	if err != nil || !reflect.DeepEqual(replayed, node) {
		t.Fatalf("enrollment replay = %#v, %v", replayed, err)
	}
	events, err := controller.ReadAuditEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 ||
		events[0].Revision != 0 ||
		events[1].Revision != 1 ||
		events[1].Record != audit {
		t.Fatalf("enrollment audit events = %#v", events)
	}

	if err := controller.ConsumeEnrollmentWithAudit(
		ctx,
		token,
		node,
		audit,
	); !errors.Is(err, enroll.ErrTokenNotFound) {
		t.Fatalf("duplicate enrollment = %v", err)
	}
	afterReplay, err := controller.ReplayEnrollment(
		ctx,
		token,
		node.PublicKeyDigest,
	)
	if err != nil || !reflect.DeepEqual(afterReplay, node) {
		t.Fatalf("duplicate enrollment replay = %#v, %v", afterReplay, err)
	}
	configuration, err = controller.ReadManagementConfiguration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	events, err = controller.ReadAuditEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if configuration.Revision != 1 || len(events) != 2 {
		t.Fatalf(
			"duplicate enrollment mutated authority: revision=%d events=%#v",
			configuration.Revision,
			events,
		)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM enrollment_tokens", 0)
	assertCount(t, controller.db, "SELECT count(*) FROM enrolled_nodes", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM enrollment_replays", 1)
}

func TestManagementEnrollmentAuditDegradationRejectsWithoutMutation(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "management-audited-enrollment-degraded.db")
	defer controller.Close()
	token := enrollmentToken(13, 1)
	tokenID := hex.EncodeToString(token.ID[:])
	if err := controller.CreateTokenWithAudit(
		ctx,
		token,
		joinCodeAudit(
			AuditActionJoinCodeCreated,
			tokenID,
			"req_16161616161616161616161616161616",
		),
	); err != nil {
		t.Fatal(err)
	}
	controller.degradeManagementAudit()
	nodeID := enrollmentNodeID(2)
	node := enrollmentNodeRecord(nodeID, "a2", time.Unix(100, 0))
	err := controller.ConsumeEnrollmentWithAudit(
		ctx,
		token,
		node,
		nodeEnrollmentAudit(
			nodeID,
			"req_17171717171717171717171717171717",
		),
	)
	if !errors.Is(err, ErrManagementAuditPersistence) {
		t.Fatalf("degraded enrollment audit = %v", err)
	}
	assertEnrollmentAuditRollback(t, controller, 1)
}

func TestManagementEnrollmentAuditInsertFailureRollsBack(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "management-audited-enrollment-insert-fault.db")
	defer controller.Close()
	auditChanged := controller.ManagementAuditChange()
	token := enrollmentToken(14, 1)
	tokenID := hex.EncodeToString(token.ID[:])
	if err := controller.CreateTokenWithAudit(
		ctx,
		token,
		joinCodeAudit(
			AuditActionJoinCodeCreated,
			tokenID,
			"req_18181818181818181818181818181818",
		),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.db.ExecContext(ctx, `CREATE TRIGGER fail_enrollment_audit_insert
		BEFORE INSERT ON management_audit_events
		WHEN NEW.action = 'node_enrolled'
		BEGIN
			SELECT RAISE(ABORT, 'injected enrollment audit failure');
		END`); err != nil {
		t.Fatal(err)
	}
	nodeID := enrollmentNodeID(3)
	node := enrollmentNodeRecord(nodeID, "a3", time.Unix(100, 0))
	err := controller.ConsumeEnrollmentWithAudit(
		ctx,
		token,
		node,
		nodeEnrollmentAudit(
			nodeID,
			"req_19191919191919191919191919191919",
		),
	)
	if !errors.Is(err, ErrManagementAuditPersistence) {
		t.Fatalf("enrollment audit insert failure = %v", err)
	}
	if controller.ManagementAuditHealthy() {
		t.Fatal("enrollment audit insert failure did not close audit health")
	}
	select {
	case <-auditChanged:
	default:
		t.Fatal("enrollment audit insert failure did not signal audit degradation")
	}
	assertEnrollmentAuditRollback(t, controller, 1)
}

func TestManagementEnrollmentCommitFailureRollsBack(t *testing.T) {
	controller := openController(t, "management-audited-enrollment-commit-fault.db")
	defer controller.Close()
	token := enrollmentToken(15, 1)
	tokenID := hex.EncodeToString(token.ID[:])
	if err := controller.CreateTokenWithAudit(
		context.Background(),
		token,
		joinCodeAudit(
			AuditActionJoinCodeCreated,
			tokenID,
			"req_20202020202020202020202020202020",
		),
	); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	controller.beforeManagementMutationCommit = cancel
	nodeID := enrollmentNodeID(4)
	node := enrollmentNodeRecord(nodeID, "a4", time.Unix(100, 0))
	err := controller.ConsumeEnrollmentWithAudit(
		ctx,
		token,
		node,
		nodeEnrollmentAudit(
			nodeID,
			"req_21212121212121212121212121212121",
		),
	)
	controller.beforeManagementMutationCommit = nil
	if err == nil {
		t.Fatal("canceled enrollment transaction committed")
	}
	if controller.ManagementAuditHealthy() {
		t.Fatal("enrollment commit failure did not close audit health")
	}
	assertEnrollmentAuditRollback(t, controller, 1)
}

func TestManagementEnrollmentCommitLinearizesWithAuditDegradation(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "management-audited-enrollment-linearized.db")
	defer controller.Close()
	token := enrollmentToken(16, 1)
	tokenID := hex.EncodeToString(token.ID[:])
	if err := controller.CreateTokenWithAudit(
		ctx,
		token,
		joinCodeAudit(
			AuditActionJoinCodeCreated,
			tokenID,
			"req_22222222222222222222222222222222",
		),
	); err != nil {
		t.Fatal(err)
	}
	nodeID := enrollmentNodeID(5)
	node := enrollmentNodeRecord(nodeID, "a5", time.Unix(100, 0))
	commitEntered := make(chan struct{})
	releaseCommit := make(chan struct{})
	controller.beforeManagementMutationCommit = func() {
		close(commitEntered)
		<-releaseCommit
	}
	commitResult := make(chan error, 1)
	go func() {
		commitResult <- controller.ConsumeEnrollmentWithAudit(
			ctx,
			token,
			node,
			nodeEnrollmentAudit(
				nodeID,
				"req_23232323232323232323232323232323",
			),
		)
	}()
	select {
	case <-commitEntered:
	case <-time.After(time.Second):
		t.Fatal("enrollment did not reach the audit-gated commit point")
	}

	degradeStarted := make(chan struct{})
	degradeDone := make(chan struct{})
	go func() {
		close(degradeStarted)
		controller.degradeManagementAudit()
		close(degradeDone)
	}()
	<-degradeStarted
	select {
	case <-degradeDone:
		t.Fatal("audit degradation crossed an in-progress enrollment commit")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseCommit)
	if err := <-commitResult; err != nil {
		t.Fatalf("enrollment linearized before audit degradation = %v", err)
	}
	select {
	case <-degradeDone:
	case <-time.After(time.Second):
		t.Fatal("audit degradation did not complete after enrollment")
	}
	controller.beforeManagementMutationCommit = nil
	if controller.ManagementAuditHealthy() {
		t.Fatal("audit authority remained healthy after degradation")
	}
	configuration, err := controller.ReadManagementConfiguration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	events, err := controller.ReadAuditEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if configuration.Revision != 1 ||
		len(configuration.Nodes) != 1 ||
		len(events) != 2 ||
		events[1].Record.Action != AuditActionNodeEnrolled {
		t.Fatalf(
			"linearized enrollment authority: configuration=%#v events=%#v",
			configuration,
			events,
		)
	}
}

func TestManagementJoinTokenCancellationRejectsConsumedTokenWithoutRevocation(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "management-audited-issued-token.db")
	defer controller.Close()
	token := enrollmentToken(7, 1)
	tokenID := hex.EncodeToString(token.ID[:])
	if err := controller.CreateTokenWithAudit(
		ctx,
		token,
		joinCodeAudit(
			AuditActionJoinCodeCreated,
			tokenID,
			"req_71717171717171717171717171717171",
		),
	); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0)
	nodeID := enrollmentNodeID(7)
	node := enrollmentNodeRecord(nodeID, "a7", now)
	if err := controller.ConsumeEnrollment(ctx, token, node); err != nil {
		t.Fatal(err)
	}
	var revocationNotified bool
	controller.SetCredentialRevocationHook(func(revoked enroll.Credential) {
		revocationNotified = true
	})
	err := controller.CancelTokenWithAudit(
		ctx,
		token.ID,
		joinCodeAudit(
			AuditActionJoinCodeCancelled,
			tokenID,
			"req_72727272727272727272727272727272",
		),
	)
	if !errors.Is(err, enroll.ErrTokenNotFound) {
		t.Fatalf("consumed join token cancellation = %v", err)
	}
	if revocationNotified {
		t.Fatal("consumed join token cancellation notified credential revocation")
	}
	if err := controller.AuthorizeCredential(
		ctx,
		node.Credential,
		now,
	); err != nil {
		t.Fatalf("consumed join credential was revoked: %v", err)
	}
	var state domain.NodeAdministrativeState
	if err := controller.db.QueryRowContext(
		ctx,
		`SELECT administrative_state
		 FROM node_administrative_states WHERE node_id = ?`,
		nodeID,
	).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != domain.NodeActive {
		t.Fatalf("consumed-token cancellation node state = %q", state)
	}
	events, err := controller.ReadAuditEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Record.Action != AuditActionJoinCodeCreated {
		t.Fatalf("consumed-token cancellation audit events = %#v", events)
	}
	if !controller.ManagementAuditHealthy() {
		t.Fatal("consumed-token rejection degraded audit health")
	}
}

func TestManagementNodeStateAndCredentialRevocationCommitWithAudit(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "management-audited-node.db")
	defer controller.Close()
	nodeID, _ := enrollControllerAgentNode(t, controller, 5)
	credential, err := controller.CurrentCredential(ctx, nodeID)
	if err != nil {
		t.Fatal(err)
	}
	var revokedCredential enroll.Credential
	controller.SetCredentialRevocationHook(func(revoked enroll.Credential) {
		revokedCredential = revoked
	})
	revision, err := controller.SetNodeAdministrativeStateWithAudit(
		ctx,
		domain.NodeID(nodeID),
		domain.NodeDraining,
		1,
		nodeAudit(
			AuditActionNodeDrained,
			nodeID,
			"req_dddddddddddddddddddddddddddddddd",
		),
	)
	if err != nil || revision != 2 {
		t.Fatalf("drain = (%d, %v)", revision, err)
	}
	if _, err := controller.SetNodeAdministrativeStateWithAudit(
		ctx,
		domain.NodeID(nodeID),
		domain.NodeActive,
		1,
		nodeAudit(
			AuditActionNodeResumed,
			nodeID,
			"req_eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		),
	); !errors.Is(err, ErrStaleManagementRevision) {
		t.Fatalf("stale resume = %v", err)
	}
	if !controller.ManagementAuditHealthy() {
		t.Fatal("stale node mutation degraded audit health")
	}
	revision, err = controller.SetNodeAdministrativeStateWithAudit(
		ctx,
		domain.NodeID(nodeID),
		domain.NodeActive,
		2,
		nodeAudit(
			AuditActionNodeResumed,
			nodeID,
			"req_ffffffffffffffffffffffffffffffff",
		),
	)
	if err != nil || revision != 3 {
		t.Fatalf("resume = (%d, %v)", revision, err)
	}
	revision, err = controller.SetNodeAdministrativeStateWithAudit(
		ctx,
		domain.NodeID(nodeID),
		domain.NodeRevoked,
		3,
		nodeAudit(
			AuditActionNodeRevoked,
			nodeID,
			"req_12121212121212121212121212121212",
		),
	)
	if err != nil || revision != 4 {
		t.Fatalf("revoke = (%d, %v)", revision, err)
	}
	if revokedCredential != credential {
		t.Fatalf("revocation hook credential = %#v, want %#v", revokedCredential, credential)
	}
	if err := controller.AuthorizeCredential(
		ctx,
		credential,
		time.Unix(100, 0),
	); !errors.Is(err, enroll.ErrCredentialRejected) {
		t.Fatalf("revoked credential authorization = %v", err)
	}
	restart, err := controller.RestartSnapshot(ctx)
	if err != nil {
		t.Fatalf("restart snapshot after audited revocation: %v", err)
	}
	if len(restart.NodeTopology) != 1 ||
		restart.NodeTopology[0].NodeID != domain.NodeID(nodeID) ||
		restart.NodeTopology[0].AdministrativeState != domain.NodeRevoked {
		t.Fatalf("revoked restart topology = %#v", restart.NodeTopology)
	}
	events, err := controller.ReadAuditEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 ||
		events[0].Revision != 2 ||
		events[1].Revision != 3 ||
		events[2].Revision != 4 {
		t.Fatalf("audited node events = %#v", events)
	}
}

func TestManagementNodeAuditFailureRollsBackStateAndRevision(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "management-audited-node-fault.db")
	defer controller.Close()
	nodeID, _ := enrollControllerAgentNode(t, controller, 6)
	if _, err := controller.db.ExecContext(ctx, `CREATE TRIGGER fail_node_audit_insert
		BEFORE INSERT ON management_audit_events
		BEGIN
			SELECT RAISE(ABORT, 'injected node audit failure');
		END`); err != nil {
		t.Fatal(err)
	}
	_, err := controller.SetNodeAdministrativeStateWithAudit(
		ctx,
		domain.NodeID(nodeID),
		domain.NodeDraining,
		1,
		nodeAudit(
			AuditActionNodeDrained,
			nodeID,
			"req_13131313131313131313131313131313",
		),
	)
	if !errors.Is(err, ErrManagementAuditPersistence) {
		t.Fatalf("node audit failure = %v", err)
	}
	var state domain.NodeAdministrativeState
	if err := controller.db.QueryRowContext(
		ctx,
		`SELECT administrative_state
		 FROM node_administrative_states WHERE node_id = ?`,
		nodeID,
	).Scan(&state); err != nil {
		t.Fatal(err)
	}
	configuration, readErr := controller.ReadManagementConfiguration(ctx)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if state != domain.NodeActive || configuration.Revision != 1 {
		t.Fatalf(
			"failed node audit left state=%q revision=%d",
			state,
			configuration.Revision,
		)
	}
	if controller.ManagementAuditHealthy() {
		t.Fatal("node audit failure did not close audit health")
	}
}

func managementConfigurationFixture(
	nodeID domain.NodeID,
) (DesiredManagementConfiguration, ManagementVerifiedAuthorities) {
	operatingSystem := domain.OSLinux
	architecture := domain.ArchAMD64
	fleetMaximum := 4
	profile := domain.RunnerProfile{
		ID:                      "profile-linux",
		Label:                   "tewake-linux",
		OS:                      &operatingSystem,
		Architecture:            &architecture,
		MinAvailableMemoryBytes: 4 << 30,
		VersionPolicy:           domain.RunnerVersionAutoUpdate,
		Runtime:                 domain.RuntimeNative,
	}
	target := DesiredManagementGitHubTarget{
		ID:              "target-private",
		InstallationID:  "installation-41",
		ScopeKind:       domain.TargetOrganization,
		Scope:           "example-org",
		ScaleSetName:    "tewake-linux",
		RunnerProfileID: "profile-linux",
	}
	desired := DesiredManagementConfiguration{
		FleetMaxRunners: &fleetMaximum,
		Nodes: []ManagementNodeConfiguration{{
			NodeID:      nodeID,
			DisplayName: "Build Desk",
			MaxRunners:  3,
		}},
		RunnerProfiles: []domain.RunnerProfile{profile},
		GitHubTargets:  []DesiredManagementGitHubTarget{target},
	}
	verified := ManagementVerifiedAuthorities{
		RunnerProfiles: []ManagementRunnerProfile{{
			Profile:       profile,
			RunnerVersion: "2.321.0",
		}},
		GitHubTargets: []ManagementGitHubTarget{{
			Target: domain.GitHubTarget{
				ID:                    target.ID,
				InstallationID:        target.InstallationID,
				ScopeKind:             target.ScopeKind,
				Scope:                 target.Scope,
				Visibility:            domain.TargetPrivate,
				RunnerGroupAccessSafe: true,
				ScaleSetName:          target.ScaleSetName,
				RunnerProfileID:       target.RunnerProfileID,
			},
			ScaleSetID: 41,
		}},
	}
	return desired, verified
}

func cloneDesiredForTest(
	configuration DesiredManagementConfiguration,
) DesiredManagementConfiguration {
	clone := configuration
	clone.FleetMaxRunners = cloneOptionalInt(configuration.FleetMaxRunners)
	clone.Nodes = append(
		[]ManagementNodeConfiguration(nil),
		configuration.Nodes...,
	)
	clone.RunnerProfiles = append(
		[]domain.RunnerProfile(nil),
		configuration.RunnerProfiles...,
	)
	clone.GitHubTargets = append(
		[]DesiredManagementGitHubTarget(nil),
		configuration.GitHubTargets...,
	)
	return clone
}

func joinCodeAudit(
	action AuditAction,
	tokenID string,
	requestID string,
) AuditRecord {
	return AuditRecord{
		Actor:        AuditActorSingleAdmin,
		Action:       action,
		Outcome:      AuditOutcomeSucceeded,
		ResourceKind: AuditResourceJoinCode,
		ResourceID:   tokenID,
		RequestID:    requestID,
	}
}

func nodeAudit(
	action AuditAction,
	nodeID string,
	requestID string,
) AuditRecord {
	return AuditRecord{
		Actor:        AuditActorSingleAdmin,
		Action:       action,
		Outcome:      AuditOutcomeSucceeded,
		ResourceKind: AuditResourceNode,
		ResourceID:   nodeID,
		RequestID:    requestID,
	}
}

func nodeEnrollmentAudit(nodeID string, requestID string) AuditRecord {
	return AuditRecord{
		Actor:        AuditActorJoinCode,
		Action:       AuditActionNodeEnrolled,
		Outcome:      AuditOutcomeSucceeded,
		ResourceKind: AuditResourceNode,
		ResourceID:   nodeID,
		RequestID:    requestID,
	}
}

func assertEnrollmentAuditRollback(
	t *testing.T,
	controller *ControllerStore,
	expectedAuditEvents int,
) {
	t.Helper()
	assertCount(t, controller.db, "SELECT count(*) FROM enrollment_tokens", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM enrolled_nodes", 0)
	assertCount(t, controller.db, "SELECT count(*) FROM enrollment_replays", 0)
	configuration, err := controller.ReadManagementConfiguration(
		context.Background(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if configuration.Revision != 0 || len(configuration.Nodes) != 0 {
		t.Fatalf("rolled-back enrollment configuration = %#v", configuration)
	}
	events, err := controller.ReadAuditEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != expectedAuditEvents {
		t.Fatalf("rolled-back enrollment audit events = %#v", events)
	}
}

func managementConfigurationAudit() AuditRecord {
	return AuditRecord{
		Actor:        AuditActorSingleAdmin,
		Action:       AuditActionConfigurationApplied,
		Outcome:      AuditOutcomeSucceeded,
		ResourceKind: AuditResourceConfiguration,
		RequestID:    "req_00000000000000000000000000000000",
	}
}
