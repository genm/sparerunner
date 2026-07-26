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
	ErrNilMessageSource  = errors.New("github message source is required")
	ErrNilMessageHandler = errors.New("github durable message handler is required")
)

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
}

// DurableMessageHandler commits the message, associated reservation, and
// desired execution atomically. It must be idempotent for a replay of the same
// (scaleSetID, messageID) pair. A non-nil error means no acknowledgement is
// sent, allowing GitHub to redeliver the message.
type DurableMessageHandler interface {
	CommitMessage(ctx context.Context, message Message) error
}

// Poller processes a single message session serially. lastAcknowledgedMessageID
// advances only after DeleteMessage succeeds, so a crash or acknowledgement
// failure naturally polls the same message again and relies on durable
// deduplication instead of losing or double-starting work.
type Poller struct {
	mu                        sync.Mutex
	source                    MessageSource
	handler                   DurableMessageHandler
	maxCapacity               int
	lastAcknowledgedMessageID int
	logger                    *slog.Logger
}

// NewPoller creates a commit-before-ack message processor. A nil logger is
// replaced with a discard logger because GitHub errors may carry untrusted
// response content; callers receive the wrapped error without the adapter
// serializing it into logs.
func NewPoller(source MessageSource, handler DurableMessageHandler, maxCapacity int, logger *slog.Logger) (*Poller, error) {
	if source == nil {
		return nil, ErrNilMessageSource
	}
	if handler == nil {
		return nil, ErrNilMessageHandler
	}
	if maxCapacity < 0 {
		return nil, ErrInvalidCapacity
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	return &Poller{
		source:      source,
		handler:     handler,
		maxCapacity: maxCapacity,
		logger:      logger,
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
func (p *Poller) PollOnce(ctx context.Context) (*Message, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	message, err := p.source.Poll(ctx, p.lastAcknowledgedMessageID, p.maxCapacity)
	if err != nil {
		p.logger.Warn("github_message_poll_failed", slog.String("component", "github"))
		return nil, fmt.Errorf("polling GitHub scale-set message: %w", err)
	}
	if message == nil {
		p.logger.Debug("github_message_empty", slog.String("component", "github"))
		return nil, nil
	}
	if message.ScaleSetID <= 0 {
		p.logger.Warn("github_message_invalid", slog.String("component", "github"), slog.String("reason", "scale_set_id"))
		return nil, ErrInvalidScaleSetID
	}
	if message.ID <= 0 {
		p.logger.Warn("github_message_invalid", slog.String("component", "github"), slog.String("reason", "message_id"))
		return nil, ErrInvalidMessageID
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
