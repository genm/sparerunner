package github_test

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strconv"
	"testing"
	"time"

	githubadapter "github.com/genm/sparerunner/internal/github"
)

func TestPollerCommitsBeforeAcknowledgement(t *testing.T) {
	message := githubadapter.Message{ScaleSetID: 9, ID: 101, Statistics: githubadapter.Statistics{TotalAssignedJobs: 3}}
	source := &fakeMessageSource{messages: []*githubadapter.Message{&message}}
	handler := &durableFakeHandler{source: source}
	poller := newPoller(t, source, handler)

	got, err := poller.PollOnce(context.Background(), 4)
	if err != nil {
		t.Fatalf("PollOnce() error = %v", err)
	}
	if got == nil || got.ID != message.ID {
		t.Fatalf("PollOnce() message = %#v, want message %d", got, message.ID)
	}
	if want := []string{"demand:9/session-1", "poll:0/4", "commit:9/101", "ack:101"}; !slices.Equal(source.events, want) {
		t.Fatalf("events = %v, want %v", source.events, want)
	}
	if got := poller.LastAcknowledgedMessageID(); got != 101 {
		t.Fatalf("LastAcknowledgedMessageID() = %d, want 101", got)
	}
}

func TestPollerDoesNotAcknowledgeWhenDurableCommitFailsAndReplayIsIdempotent(t *testing.T) {
	message := githubadapter.Message{ScaleSetID: 9, ID: 101}
	source := &fakeMessageSource{messages: []*githubadapter.Message{&message, &message}}
	handler := &durableFakeHandler{source: source, failCommits: 1}
	poller := newPoller(t, source, handler)

	if _, err := poller.PollOnce(context.Background(), 4); err == nil {
		t.Fatal("first PollOnce() succeeded, want durable commit failure")
	}
	if got := source.acks; len(got) != 0 {
		t.Fatalf("acknowledgements after failed commit = %v, want none", got)
	}
	if got := poller.LastAcknowledgedMessageID(); got != 0 {
		t.Fatalf("cursor after failed commit = %d, want 0", got)
	}

	if _, err := poller.PollOnce(context.Background(), 4); err != nil {
		t.Fatalf("replayed PollOnce() error = %v", err)
	}
	if got := source.acks; !slices.Equal(got, []int{101}) {
		t.Fatalf("acknowledgements = %v, want [101]", got)
	}
	if got := handler.createdExecutions; got != 1 {
		t.Fatalf("created executions = %d, want 1 after replay", got)
	}
	if want := []string{"demand:9/session-1", "poll:0/4", "commit:9/101", "poll:0/4", "commit:9/101", "ack:101"}; !slices.Equal(source.events, want) {
		t.Fatalf("events = %v, want %v", source.events, want)
	}
}

func TestPollerRetriesRedeliveryAfterAcknowledgementFailure(t *testing.T) {
	message := githubadapter.Message{ScaleSetID: 9, ID: 101}
	source := &fakeMessageSource{messages: []*githubadapter.Message{&message, &message}, failAcks: 1}
	handler := &durableFakeHandler{source: source}
	poller := newPoller(t, source, handler)

	if _, err := poller.PollOnce(context.Background(), 4); err == nil {
		t.Fatal("first PollOnce() succeeded, want acknowledgement failure")
	}
	if got := poller.LastAcknowledgedMessageID(); got != 0 {
		t.Fatalf("cursor after acknowledgement failure = %d, want 0", got)
	}
	if _, err := poller.PollOnce(context.Background(), 4); err != nil {
		t.Fatalf("replayed PollOnce() error = %v", err)
	}
	if got := handler.createdExecutions; got != 1 {
		t.Fatalf("created executions = %d, want 1 after acknowledgement replay", got)
	}
	if got := source.acks; !slices.Equal(got, []int{101, 101}) {
		t.Fatalf("acknowledgements = %v, want [101 101]", got)
	}
}

func TestPollerDoesNotCommitOrAcknowledgeInvalidStatistics(t *testing.T) {
	message := githubadapter.Message{
		ScaleSetID: 9,
		ID:         101,
		Statistics: githubadapter.Statistics{
			TotalAssignedJobs: 1,
			TotalRunningJobs:  2,
		},
	}
	source := &fakeMessageSource{messages: []*githubadapter.Message{&message}}
	handler := &durableFakeHandler{source: source}
	poller := newPoller(t, source, handler)

	if _, err := poller.PollOnce(context.Background(), 4); !errors.Is(err, githubadapter.ErrInvalidStatistics) {
		t.Fatalf("PollOnce() error = %v, want ErrInvalidStatistics", err)
	}
	if got := source.acks; len(got) != 0 {
		t.Fatalf("acknowledgements after invalid statistics = %v, want none", got)
	}
	if got := handler.createdExecutions; got != 0 {
		t.Fatalf("created executions = %d, want no durable commit", got)
	}
	if len(handler.healthy) != 0 {
		t.Fatalf("invalid message marked session healthy: %#v", handler.healthy)
	}
	if got := poller.LastAcknowledgedMessageID(); got != 0 {
		t.Fatalf("cursor after invalid statistics = %d, want 0", got)
	}
	if want := []string{"demand:9/session-1", "poll:0/4"}; !slices.Equal(source.events, want) {
		t.Fatalf("events = %v, want %v", source.events, want)
	}
}

func TestPollerRejectsInvalidJobIdentityAndStateBeforeHealthCommitOrAcknowledgement(t *testing.T) {
	tests := []struct {
		name string
		job  githubadapter.JobMessage
	}{
		{
			name: "available missing runner request ID",
			job:  githubadapter.JobMessage{Type: githubadapter.MessageTypeJobAvailable, WorkflowRunID: 51},
		},
		{
			name: "started negative runner request ID",
			job:  githubadapter.JobMessage{Type: githubadapter.MessageTypeJobStarted, RunnerRequestID: -1, RunnerID: 33, RunnerName: "tewake-runner", WorkflowRunID: 51},
		},
		{
			name: "started missing runner ID",
			job:  githubadapter.JobMessage{Type: githubadapter.MessageTypeJobStarted, RunnerRequestID: 7001, RunnerName: "tewake-runner", WorkflowRunID: 51},
		},
		{
			name: "started missing runner name",
			job:  githubadapter.JobMessage{Type: githubadapter.MessageTypeJobStarted, RunnerRequestID: 7001, RunnerID: 33, WorkflowRunID: 51},
		},
		{
			name: "started negative workflow run ID",
			job:  githubadapter.JobMessage{Type: githubadapter.MessageTypeJobStarted, RunnerRequestID: 7001, RunnerID: 33, RunnerName: "tewake-runner", WorkflowRunID: -1},
		},
		{
			name: "completed negative runner request ID",
			job:  githubadapter.JobMessage{Type: githubadapter.MessageTypeJobCompleted, RunnerRequestID: -1, RunnerID: 33, RunnerName: "tewake-runner", Result: githubadapter.JobResultSucceeded, WorkflowRunID: 51},
		},
		{
			name: "completed missing runner ID",
			job:  githubadapter.JobMessage{Type: githubadapter.MessageTypeJobCompleted, RunnerRequestID: 7001, RunnerName: "tewake-runner", WorkflowRunID: 51},
		},
		{
			name: "completed missing runner name",
			job:  githubadapter.JobMessage{Type: githubadapter.MessageTypeJobCompleted, RunnerRequestID: 7001, RunnerID: 33, WorkflowRunID: 51},
		},
		{
			name: "completed missing result",
			job:  githubadapter.JobMessage{Type: githubadapter.MessageTypeJobCompleted, RunnerRequestID: 7001, RunnerID: 33, RunnerName: "tewake-runner", WorkflowRunID: 51},
		},
		{
			name: "completed noncanonical result",
			job:  githubadapter.JobMessage{Type: githubadapter.MessageTypeJobCompleted, RunnerRequestID: 7001, RunnerID: 33, RunnerName: "tewake-runner", Result: "Succeeded", WorkflowRunID: 51},
		},
		{
			name: "completed unknown result",
			job:  githubadapter.JobMessage{Type: githubadapter.MessageTypeJobCompleted, RunnerRequestID: 7001, RunnerID: 33, RunnerName: "tewake-runner", Result: "unknown", WorkflowRunID: 51},
		},
		{
			name: "available carries runner identity",
			job:  githubadapter.JobMessage{Type: githubadapter.MessageTypeJobAvailable, RunnerRequestID: 7001, RunnerID: 33, RunnerName: "tewake-runner", WorkflowRunID: 51},
		},
		{
			name: "assigned carries runner identity",
			job:  githubadapter.JobMessage{Type: githubadapter.MessageTypeJobAssigned, RunnerRequestID: 7001, RunnerID: 33, RunnerName: "tewake-runner", WorkflowRunID: 51},
		},
		{
			name: "unknown event type",
			job:  githubadapter.JobMessage{Type: "JobUnknown", RunnerRequestID: 7001, WorkflowRunID: 51},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			message := githubadapter.Message{
				ScaleSetID: 9,
				ID:         101,
				Jobs:       []githubadapter.JobMessage{testCase.job},
			}
			source := &fakeMessageSource{messages: []*githubadapter.Message{&message}}
			handler := &durableFakeHandler{source: source}
			poller := newPoller(t, source, handler)

			_, err := poller.PollOnce(context.Background(), 4)
			var providerFailure *githubadapter.ProviderFailure
			if !errors.As(err, &providerFailure) ||
				providerFailure.Operation != githubadapter.ProviderValidateResponse ||
				!errors.Is(err, githubadapter.ErrInvalidJobMessage) {
				t.Fatalf("PollOnce() error = %v, want invalid-response ProviderFailure wrapping ErrInvalidJobMessage", err)
			}
			if len(handler.healthy) != 0 {
				t.Fatalf("invalid job marked session healthy: %#v", handler.healthy)
			}
			if handler.createdExecutions != 0 {
				t.Fatalf("invalid job created executions = %d, want 0", handler.createdExecutions)
			}
			if len(source.acks) != 0 {
				t.Fatalf("invalid job acknowledgements = %v, want none", source.acks)
			}
			if poller.LastAcknowledgedMessageID() != 0 {
				t.Fatalf("invalid job advanced cursor to %d", poller.LastAcknowledgedMessageID())
			}
		})
	}
}

func TestPollerAcceptsStoreCompatibleJobIdentityStates(t *testing.T) {
	message := githubadapter.Message{
		ScaleSetID: 9,
		ID:         101,
		Jobs: []githubadapter.JobMessage{
			{Type: githubadapter.MessageTypeJobAvailable, RunnerRequestID: 7001, WorkflowRunID: 0},
			{Type: githubadapter.MessageTypeJobAssigned, RunnerRequestID: 7002, WorkflowRunID: 51},
			// Live GitHub sends JobAssigned with no RunnerRequestID at all. This
			// exact shape was rejected by both the adapter and the store, so a
			// real queued job redelivered forever and never started. Nothing
			// downstream reads the field for an assignment.
			{Type: githubadapter.MessageTypeJobAssigned, RunnerRequestID: 0, RepositoryName: "tewake-runner-smoke", OwnerName: "arieal", JobID: "b595668a-9118-582e-abbd-9413df4dc0b5", WorkflowRunID: 54},
			// Live GitHub also cancels a never-picked-up job with no runner
			// identity at all. This shape was rejected too, so a cancellation
			// could never be acknowledged and blocked the whole queue behind it.
			{Type: githubadapter.MessageTypeJobCompleted, RunnerRequestID: 0, Result: githubadapter.JobResultCanceled, RepositoryName: "tewake-runner-smoke", OwnerName: "arieal", WorkflowRunID: 55},
			{Type: githubadapter.MessageTypeJobStarted, RunnerRequestID: 7003, RunnerID: 33, RunnerName: "tewake-started", WorkflowRunID: 52},
			{Type: githubadapter.MessageTypeJobStarted, RunnerRequestID: 0, RunnerID: 36, RunnerName: "tewake-started-zero-request", WorkflowRunID: 52},
			{Type: githubadapter.MessageTypeJobCompleted, RunnerRequestID: 7004, RunnerID: 34, RunnerName: "tewake-completed", Result: githubadapter.JobResultSucceeded, WorkflowRunID: 53},
			{Type: githubadapter.MessageTypeJobCompleted, RunnerRequestID: 7005, RunnerID: 35, RunnerName: "tewake-canceled", Result: githubadapter.JobResultCanceled, WorkflowRunID: 54},
			{Type: githubadapter.MessageTypeJobCompleted, RunnerRequestID: 0, RunnerID: 37, RunnerName: "tewake-completed-zero-request", Result: githubadapter.JobResultFailed, WorkflowRunID: 55},
		},
	}
	source := &fakeMessageSource{messages: []*githubadapter.Message{&message}}
	handler := &durableFakeHandler{source: source}
	poller := newPoller(t, source, handler)

	if _, err := poller.PollOnce(context.Background(), 4); err != nil {
		t.Fatalf("PollOnce() error = %v", err)
	}
	if len(handler.healthy) != 1 || handler.createdExecutions != 1 {
		t.Fatalf("healthy commits/executions = %d/%d, want 1/1", len(handler.healthy), handler.createdExecutions)
	}
	if !slices.Equal(source.acks, []int{101}) {
		t.Fatalf("acknowledgements = %v, want [101]", source.acks)
	}
}

func TestPollerCommitsInitialSessionDemandBeforeEmptyPollAndUsesDynamicCapacity(t *testing.T) {
	source := &fakeMessageSource{snapshot: githubadapter.SessionSnapshot{ScaleSetID: 9, ID: "session-1", Statistics: githubadapter.Statistics{TotalAssignedJobs: 2}}}
	handler := &durableFakeHandler{source: source}
	poller := newPoller(t, source, handler)

	if message, err := poller.PollOnce(context.Background(), 1); err != nil || message != nil {
		t.Fatalf("first PollOnce() = (%#v, %v), want empty success", message, err)
	}
	source.snapshot.ID = "session-2" // models an upstream token refresh with new statistics.
	if message, err := poller.PollOnce(context.Background(), 0); err != nil || message != nil {
		t.Fatalf("second PollOnce() = (%#v, %v), want empty success", message, err)
	}
	if want := []string{"demand:9/session-1", "poll:0/1", "demand:9/session-2", "poll:0/0"}; !slices.Equal(source.events, want) {
		t.Fatalf("events = %v, want %v", source.events, want)
	}
}

func TestPollerCommitsSameSessionIDWhenStatisticsChangeAfterEmptyPoll(t *testing.T) {
	source := &fakeMessageSource{snapshot: githubadapter.SessionSnapshot{ScaleSetID: 9, ID: "session-1"}, refreshStatsOnPoll: true}
	handler := &durableFakeHandler{source: source}
	poller := newPoller(t, source, handler)
	if _, err := poller.PollOnce(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if want := []string{"demand:9/session-1", "poll:0/1", "demand:9/session-1"}; !slices.Equal(source.events, want) {
		t.Fatalf("events=%v", source.events)
	}
	if len(handler.healthy) != 1 ||
		handler.healthy[0].Statistics.TotalAssignedJobs != 1 {
		t.Fatalf("healthy snapshots = %#v, want refreshed session", handler.healthy)
	}
}

func TestPollerCommitsHealthForEveryCompletePollEvenWhenDemandIsUnchanged(t *testing.T) {
	source := &fakeMessageSource{
		snapshot: githubadapter.SessionSnapshot{
			ScaleSetID: 9,
			ID:         "session-1",
		},
	}
	handler := &durableFakeHandler{source: source}
	poller := newPoller(t, source, handler)

	for range 2 {
		if message, err := poller.PollOnce(context.Background(), 0); err != nil || message != nil {
			t.Fatalf("PollOnce() = (%#v, %v), want empty success", message, err)
		}
	}
	if len(handler.healthy) != 2 {
		t.Fatalf("healthy commits = %d, want 2", len(handler.healthy))
	}
	if want := []string{"demand:9/session-1", "poll:0/0", "poll:0/0"}; !slices.Equal(source.events, want) {
		t.Fatalf("events = %v, want %v", source.events, want)
	}
}

func TestPollerClassifiesProviderOwnedFailuresWithoutMisclassifyingStoreFailure(t *testing.T) {
	tests := []struct {
		name      string
		source    *fakeMessageSource
		handler   *durableFakeHandler
		operation githubadapter.ProviderOperation
	}{
		{
			name:      "initial session read",
			source:    &fakeMessageSource{snapshotErrors: []error{errors.New("read failed")}},
			operation: githubadapter.ProviderReadSession,
		},
		{
			name:      "poll",
			source:    &fakeMessageSource{pollErr: errors.New("poll failed")},
			operation: githubadapter.ProviderPollMessages,
		},
		{
			name: "refreshed session read",
			source: &fakeMessageSource{
				snapshotErrors: []error{nil, errors.New("refresh failed")},
			},
			operation: githubadapter.ProviderRefreshSession,
		},
		{
			name: "invalid refreshed session",
			source: &fakeMessageSource{
				invalidateSnapshotOnPoll: true,
			},
			operation: githubadapter.ProviderValidateResponse,
		},
		{
			name: "acknowledgement",
			source: &fakeMessageSource{
				messages: []*githubadapter.Message{{
					ScaleSetID: 9,
					ID:         101,
				}},
				failAcks: 1,
			},
			operation: githubadapter.ProviderAcknowledge,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			handler := testCase.handler
			if handler == nil {
				handler = &durableFakeHandler{source: testCase.source}
			}
			poller := newPoller(t, testCase.source, handler)
			_, err := poller.PollOnce(context.Background(), 0)
			var providerFailure *githubadapter.ProviderFailure
			if !errors.As(err, &providerFailure) {
				t.Fatalf("error = %v, want ProviderFailure", err)
			}
			if providerFailure.Operation != testCase.operation {
				t.Fatalf("operation = %q, want %q", providerFailure.Operation, testCase.operation)
			}
		})
	}

	source := &fakeMessageSource{}
	handler := &durableFakeHandler{
		source:          source,
		healthCommitErr: errors.New("SQLite unavailable"),
	}
	poller := newPoller(t, source, handler)
	_, err := poller.PollOnce(context.Background(), 0)
	var providerFailure *githubadapter.ProviderFailure
	if errors.As(err, &providerFailure) {
		t.Fatalf("store failure was classified as provider failure: %v", err)
	}
	if !errors.Is(err, handler.healthCommitErr) {
		t.Fatalf("store failure = %v, want health commit error", err)
	}
}

func TestPollerRejectsScaleSetDriftWithoutCommitOrAck(t *testing.T) {
	message := githubadapter.Message{ScaleSetID: 10, ID: 101}
	source := &fakeMessageSource{messages: []*githubadapter.Message{&message}, refreshOnPoll: true, driftOnPoll: true}
	handler := &durableFakeHandler{source: source}
	poller := newPoller(t, source, handler)
	if _, err := poller.PollOnce(context.Background(), 1); !errors.Is(err, githubadapter.ErrInvalidSession) {
		t.Fatalf("error=%v", err)
	}
	if len(source.acks) != 0 || handler.createdExecutions != 0 {
		t.Fatal("drift committed or acknowledged")
	}
}

func TestPollerCommitsRefreshedSessionDemandBeforeMessageCommit(t *testing.T) {
	message := githubadapter.Message{ScaleSetID: 9, ID: 101}
	source := &fakeMessageSource{messages: []*githubadapter.Message{&message}, refreshOnPoll: true}
	handler := &durableFakeHandler{source: source}
	poller := newPoller(t, source, handler)
	if _, err := poller.PollOnce(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if want := []string{"demand:9/session-1", "poll:0/1", "demand:9/session-2", "commit:9/101", "ack:101"}; !slices.Equal(source.events, want) {
		t.Fatalf("events = %v, want %v", source.events, want)
	}
}

func TestPollerBoundsAcknowledgementWithoutBoundingLongPoll(t *testing.T) {
	message := githubadapter.Message{ScaleSetID: 9, ID: 101}
	source := &fakeMessageSource{messages: []*githubadapter.Message{&message}}
	handler := &durableFakeHandler{source: source}
	poller := newPoller(t, source, handler)

	if _, err := poller.PollOnce(context.Background(), 1); err != nil {
		t.Fatalf("PollOnce() error = %v", err)
	}
	if source.pollHasDeadline {
		t.Fatalf("long poll received deadline %v", source.pollDeadline)
	}
	if !source.ackHasDeadline || !source.ackDeadline.After(time.Now()) {
		t.Fatalf("acknowledgement deadline = (%v, %t), want a future finite deadline", source.ackDeadline, source.ackHasDeadline)
	}
}

func TestPollerSurfacesAcknowledgementContextTimeoutWithoutAdvancingCursor(t *testing.T) {
	message := githubadapter.Message{ScaleSetID: 9, ID: 101}
	source := &fakeMessageSource{
		messages:                 []*githubadapter.Message{&message},
		blockAckUntilContextDone: true,
	}
	handler := &durableFakeHandler{source: source}
	poller := newPoller(t, source, handler)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	_, err := poller.PollOnce(ctx, 1)
	var providerFailure *githubadapter.ProviderFailure
	if !errors.Is(err, context.DeadlineExceeded) ||
		!errors.As(err, &providerFailure) ||
		providerFailure.Operation != githubadapter.ProviderAcknowledge {
		t.Fatalf("PollOnce() error = %v, want acknowledgement ProviderFailure wrapping context deadline", err)
	}
	if poller.LastAcknowledgedMessageID() != 0 {
		t.Fatalf("timeout advanced cursor to %d", poller.LastAcknowledgedMessageID())
	}
	if handler.createdExecutions != 1 || len(handler.healthy) != 1 {
		t.Fatalf("durable commit/health before timeout = %d/%d, want 1/1", handler.createdExecutions, len(handler.healthy))
	}
}

func newPoller(t *testing.T, source *fakeMessageSource, handler *durableFakeHandler) *githubadapter.Poller {
	t.Helper()
	poller, err := githubadapter.NewPoller(source, handler, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewPoller() error = %v", err)
	}
	return poller
}

type fakeMessageSource struct {
	messages                 []*githubadapter.Message
	events                   []string
	acks                     []int
	failAcks                 int
	pollErr                  error
	snapshotErrors           []error
	snapshot                 githubadapter.SessionSnapshot
	refreshOnPoll            bool
	refreshStatsOnPoll       bool
	driftOnPoll              bool
	invalidateSnapshotOnPoll bool
	blockAckUntilContextDone bool
	pollDeadline             time.Time
	pollHasDeadline          bool
	ackDeadline              time.Time
	ackHasDeadline           bool
}

func (s *fakeMessageSource) Poll(ctx context.Context, cursor, capacity int) (*githubadapter.Message, error) {
	s.events = append(s.events, "poll:"+strconv.Itoa(cursor)+"/"+strconv.Itoa(capacity))
	s.pollDeadline, s.pollHasDeadline = ctx.Deadline()
	if s.pollErr != nil {
		return nil, s.pollErr
	}
	if s.refreshOnPoll {
		s.snapshot.ID = "session-2"
		s.refreshOnPoll = false
	}
	if s.refreshStatsOnPoll {
		s.snapshot.Statistics.TotalAssignedJobs++
		s.refreshStatsOnPoll = false
	}
	if s.driftOnPoll {
		s.snapshot.ScaleSetID = 10
		s.driftOnPoll = false
	}
	if s.invalidateSnapshotOnPoll {
		s.snapshot.Statistics.TotalAssignedJobs = 1
		s.snapshot.Statistics.TotalRunningJobs = 2
		s.invalidateSnapshotOnPoll = false
	}
	if len(s.messages) == 0 {
		return nil, nil
	}
	message := s.messages[0]
	s.messages = s.messages[1:]
	return message, nil
}

func (s *fakeMessageSource) Snapshot() (githubadapter.SessionSnapshot, error) {
	if len(s.snapshotErrors) > 0 {
		err := s.snapshotErrors[0]
		s.snapshotErrors = s.snapshotErrors[1:]
		if err != nil {
			return githubadapter.SessionSnapshot{}, err
		}
	}
	if s.snapshot.ID == "" {
		s.snapshot = githubadapter.SessionSnapshot{ScaleSetID: 9, ID: "session-1"}
	}
	return s.snapshot, nil
}

func (s *fakeMessageSource) DeleteMessage(ctx context.Context, messageID int) error {
	s.events = append(s.events, "ack:"+strconv.Itoa(messageID))
	s.acks = append(s.acks, messageID)
	s.ackDeadline, s.ackHasDeadline = ctx.Deadline()
	if s.blockAckUntilContextDone {
		<-ctx.Done()
		return ctx.Err()
	}
	if s.failAcks > 0 {
		s.failAcks--
		return errors.New("simulated acknowledgement outage")
	}
	return nil
}

type durableFakeHandler struct {
	source            *fakeMessageSource
	failCommits       int
	healthCommitErr   error
	healthy           []githubadapter.SessionSnapshot
	committed         map[string]struct{}
	createdExecutions int
}

func (h *durableFakeHandler) CommitSessionDemand(_ context.Context, snapshot githubadapter.SessionSnapshot) error {
	h.source.events = append(h.source.events, "demand:"+strconv.Itoa(int(snapshot.ScaleSetID))+"/"+snapshot.ID)
	return nil
}

func (h *durableFakeHandler) CommitSessionHealthy(
	_ context.Context,
	snapshot githubadapter.SessionSnapshot,
) error {
	if h.healthCommitErr != nil {
		return h.healthCommitErr
	}
	h.healthy = append(h.healthy, snapshot)
	return nil
}

func (h *durableFakeHandler) CommitMessage(_ context.Context, message githubadapter.Message) error {
	h.source.events = append(h.source.events, "commit:"+strconv.Itoa(int(message.ScaleSetID))+"/"+strconv.Itoa(message.ID))
	if h.failCommits > 0 {
		h.failCommits--
		return errors.New("simulated transaction rollback")
	}
	if h.committed == nil {
		h.committed = make(map[string]struct{})
	}
	key := strconv.Itoa(int(message.ScaleSetID)) + "/" + strconv.Itoa(message.ID)
	if _, exists := h.committed[key]; exists {
		return nil
	}
	h.committed[key] = struct{}{}
	h.createdExecutions++
	return nil
}
