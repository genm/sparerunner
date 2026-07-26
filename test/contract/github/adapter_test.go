package github_test

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strconv"
	"testing"

	githubadapter "github.com/genm/tewake/internal/github"
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
	if got := poller.LastAcknowledgedMessageID(); got != 0 {
		t.Fatalf("cursor after invalid statistics = %d, want 0", got)
	}
	if want := []string{"demand:9/session-1", "poll:0/4"}; !slices.Equal(source.events, want) {
		t.Fatalf("events = %v, want %v", source.events, want)
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

func newPoller(t *testing.T, source *fakeMessageSource, handler *durableFakeHandler) *githubadapter.Poller {
	t.Helper()
	poller, err := githubadapter.NewPoller(source, handler, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewPoller() error = %v", err)
	}
	return poller
}

type fakeMessageSource struct {
	messages      []*githubadapter.Message
	events        []string
	acks          []int
	failAcks      int
	snapshot      githubadapter.SessionSnapshot
	refreshOnPoll bool
}

func (s *fakeMessageSource) Poll(_ context.Context, cursor, capacity int) (*githubadapter.Message, error) {
	s.events = append(s.events, "poll:"+strconv.Itoa(cursor)+"/"+strconv.Itoa(capacity))
	if s.refreshOnPoll {
		s.snapshot.ID = "session-2"
		s.refreshOnPoll = false
	}
	if len(s.messages) == 0 {
		return nil, nil
	}
	message := s.messages[0]
	s.messages = s.messages[1:]
	return message, nil
}

func (s *fakeMessageSource) Snapshot() (githubadapter.SessionSnapshot, error) {
	if s.snapshot.ID == "" {
		s.snapshot = githubadapter.SessionSnapshot{ScaleSetID: 9, ID: "session-1"}
	}
	return s.snapshot, nil
}

func (s *fakeMessageSource) DeleteMessage(_ context.Context, messageID int) error {
	s.events = append(s.events, "ack:"+strconv.Itoa(messageID))
	s.acks = append(s.acks, messageID)
	if s.failAcks > 0 {
		s.failAcks--
		return errors.New("simulated acknowledgement outage")
	}
	return nil
}

type durableFakeHandler struct {
	source            *fakeMessageSource
	failCommits       int
	committed         map[string]struct{}
	createdExecutions int
}

func (h *durableFakeHandler) CommitSessionDemand(_ context.Context, snapshot githubadapter.SessionSnapshot) error {
	h.source.events = append(h.source.events, "demand:"+strconv.Itoa(int(snapshot.ScaleSetID))+"/"+snapshot.ID)
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
