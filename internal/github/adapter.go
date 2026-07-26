// Package github isolates the GitHub Actions scale-set client from Tewake.
//
// The package intentionally exposes Tewake-owned transport values rather than
// github.com/actions/scaleset values. This keeps the public-preview dependency
// from leaking into domain code and makes the commit-before-ack boundary testable.
package github

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
)

var (
	// ErrInvalidScaleSetID prevents an unbound session from being used for
	// durable deduplication, whose key is (scale-set ID, message ID).
	ErrInvalidScaleSetID = errors.New("github scale set ID must be positive")
	ErrInvalidMessageID  = errors.New("github message ID must be positive")
	ErrInvalidCapacity   = errors.New("github max capacity must not be negative")
	ErrInvalidStatistics = errors.New("github scale-set statistics are missing or inconsistent")
	ErrInvalidSession    = errors.New("github message session is invalid")
	ErrNilMessageSource  = errors.New("github message source is required")
	ErrNilMessageHandler = errors.New("github durable message handler is required")
)

// SessionSnapshot is the non-secret scaling signal supplied when a GitHub
// message session is created or refreshed. It deliberately excludes queue URLs
// and access tokens. Durable consumers deduplicate by (ScaleSetID, ID).
type SessionSnapshot struct {
	ScaleSetID ScaleSetID
	ID         string
	Statistics Statistics
}

// ScaleSetID identifies one GitHub runner scale set.
type ScaleSetID int

// Statistics is GitHub's current scaling signal. TotalAssignedJobs, rather
// than a count of individual messages, must drive capacity decisions because
// GitHub can truncate large message backlogs.
type Statistics struct {
	TotalAvailableJobs     int
	TotalAcquiredJobs      int
	TotalAssignedJobs      int
	TotalRunningJobs       int
	TotalRegisteredRunners int
	TotalBusyRunners       int
	TotalIdleRunners       int
}

// MessageType identifies an event in a polled GitHub message batch.
type MessageType string

const (
	MessageTypeJobAvailable MessageType = "JobAvailable"
	MessageTypeJobAssigned  MessageType = "JobAssigned"
	MessageTypeJobStarted   MessageType = "JobStarted"
	MessageTypeJobCompleted MessageType = "JobCompleted"
)

// JobMessage is GitHub job metadata. It deliberately contains no credentials
// or JIT configuration; callers must not format it into logs without applying
// their own allowlist.
type JobMessage struct {
	Type            MessageType
	RunnerRequestID int64
	RunnerID        int
	RunnerName      string
	Result          string
	RepositoryName  string
	OwnerName       string
	JobID           string
	WorkflowRunID   int64
}

// Message is one scale-set queue message. Durable consumers deduplicate using
// the stable (ScaleSetID, ID) pair before creating a desired execution.
type Message struct {
	ScaleSetID ScaleSetID
	ID         int
	Statistics Statistics
	Jobs       []JobMessage
}

// MessageSource owns one authenticated GitHub message session. Poll returns
// nil, nil when the long poll has no message. DeleteMessage is the GitHub
// acknowledgement operation and must only run after DurableMessageHandler
// returns successfully.
type MessageSource interface {
	Poll(ctx context.Context, lastAcknowledgedMessageID, maxCapacity int) (*Message, error)
	DeleteMessage(ctx context.Context, messageID int) error
	Snapshot() (SessionSnapshot, error)
}

// DurableMessageHandler commits the message, associated reservation, and
// desired execution atomically. It must be idempotent for a replay of the same
// (scaleSetID, messageID) pair. A non-nil error means no acknowledgement is
// sent, allowing GitHub to redeliver the message.
type DurableMessageHandler interface {
	CommitMessage(ctx context.Context, message Message) error
	CommitSessionDemand(ctx context.Context, snapshot SessionSnapshot) error
}

// Poller processes a single message session serially. lastAcknowledgedMessageID
// advances only after DeleteMessage succeeds, so a crash or acknowledgement
// failure naturally polls the same message again and relies on durable
// deduplication instead of losing or double-starting work.
type Poller struct {
	mu                        sync.Mutex
	source                    MessageSource
	handler                   DurableMessageHandler
	lastAcknowledgedMessageID int
	boundScaleSetID           ScaleSetID
	lastSessionSnapshot       SessionSnapshot
	hasSessionSnapshot        bool
	logger                    *slog.Logger
}

// NewPoller creates a commit-before-ack message processor. A nil logger is
// replaced with a discard logger because GitHub errors may carry untrusted
// response content; callers receive the wrapped error without the adapter
// serializing it into logs.
func NewPoller(source MessageSource, handler DurableMessageHandler, logger *slog.Logger) (*Poller, error) {
	if source == nil {
		return nil, ErrNilMessageSource
	}
	if handler == nil {
		return nil, ErrNilMessageHandler
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	return &Poller{
		source:  source,
		handler: handler,
		logger:  logger,
	}, nil
}

// LastAcknowledgedMessageID returns the durable cursor for the next poll. It
// intentionally does not advance after a merely fetched or merely committed
// message, because GitHub must be allowed to redeliver until acknowledgement.
func (p *Poller) LastAcknowledgedMessageID() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastAcknowledgedMessageID
}

// PollOnce runs exactly one poll -> durable commit -> acknowledgement sequence.
// It never invokes DeleteMessage if validation or durable commit fails.
func (p *Poller) PollOnce(ctx context.Context, maxCapacity int) (*Message, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if maxCapacity < 0 {
		return nil, ErrInvalidCapacity
	}

	snapshot, err := p.source.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("reading GitHub message session snapshot: %w", err)
	}
	if err := validateSessionSnapshot(snapshot); err != nil {
		return nil, err
	}
	if err := p.commitSessionDemand(ctx, snapshot); err != nil {
		return nil, err
	}

	message, err := p.source.Poll(ctx, p.lastAcknowledgedMessageID, maxCapacity)
	if err != nil {
		p.logger.Warn("github_message_poll_failed", slog.String("component", "github"))
		return nil, fmt.Errorf("polling GitHub scale-set message: %w", err)
	}
	// GetMessage may refresh the upstream session token before returning a
	// message. Re-read its snapshot before the message can be committed or acked.
	refreshed, err := p.source.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("reading refreshed GitHub message session snapshot: %w", err)
	}
	if err := validateSessionSnapshot(refreshed); err != nil {
		return nil, err
	}
	if err := p.commitSessionDemand(ctx, refreshed); err != nil {
		return nil, err
	}
	if message == nil {
		p.logger.Debug("github_message_empty", slog.String("component", "github"))
		return nil, nil
	}
	if message.ScaleSetID != p.boundScaleSetID {
		return nil, ErrInvalidSession
	}
	if message.ScaleSetID <= 0 {
		p.logger.Warn("github_message_invalid", slog.String("component", "github"), slog.String("reason", "scale_set_id"))
		return nil, ErrInvalidScaleSetID
	}
	if message.ID <= 0 {
		p.logger.Warn("github_message_invalid", slog.String("component", "github"), slog.String("reason", "message_id"))
		return nil, ErrInvalidMessageID
	}
	if err := validateStatistics(message.Statistics); err != nil {
		p.logger.Warn("github_message_invalid", slog.String("component", "github"), slog.String("reason", "statistics"))
		return nil, err
	}

	if err := p.handler.CommitMessage(ctx, *message); err != nil {
		p.logger.Warn(
			"github_message_commit_failed",
			slog.String("component", "github"),
			slog.Int("scale_set_id", int(message.ScaleSetID)),
			slog.Int("message_id", message.ID),
		)
		return nil, fmt.Errorf("durably committing GitHub scale-set message: %w", err)
	}
	p.logger.Info(
		"github_message_committed",
		slog.String("component", "github"),
		slog.Int("scale_set_id", int(message.ScaleSetID)),
		slog.Int("message_id", message.ID),
	)

	if err := p.source.DeleteMessage(ctx, message.ID); err != nil {
		p.logger.Warn(
			"github_message_ack_failed",
			slog.String("component", "github"),
			slog.Int("scale_set_id", int(message.ScaleSetID)),
			slog.Int("message_id", message.ID),
		)
		return nil, fmt.Errorf("acknowledging GitHub scale-set message: %w", err)
	}
	p.lastAcknowledgedMessageID = message.ID
	p.logger.Info(
		"github_message_acknowledged",
		slog.String("component", "github"),
		slog.Int("scale_set_id", int(message.ScaleSetID)),
		slog.Int("message_id", message.ID),
	)

	return message, nil
}

func (p *Poller) commitSessionDemand(ctx context.Context, snapshot SessionSnapshot) error {
	if !p.hasSessionSnapshot {
		p.boundScaleSetID = snapshot.ScaleSetID
		p.hasSessionSnapshot = true
	}
	if snapshot.ScaleSetID != p.boundScaleSetID {
		return ErrInvalidSession
	}
	if p.hasSessionSnapshot && snapshot == p.lastSessionSnapshot {
		return nil
	}
	if err := p.handler.CommitSessionDemand(ctx, snapshot); err != nil {
		return fmt.Errorf("durably committing GitHub session demand: %w", err)
	}
	p.lastSessionSnapshot = snapshot
	return nil
}

func validateSessionSnapshot(snapshot SessionSnapshot) error {
	if snapshot.ScaleSetID <= 0 || snapshot.ID == "" {
		return ErrInvalidSession
	}
	return validateStatistics(snapshot.Statistics)
}
