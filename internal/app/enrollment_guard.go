package app

import (
	"net"
	"sync"
	"time"

	"github.com/genm/tewake/internal/transport"
)

const (
	enrollmentRequestWindow     = time.Minute
	enrollmentRequestsPerWindow = 60
	enrollmentRequestsPerSource = 12
	enrollmentTrackedSources    = 256
)

type enrollmentAuditKind uint8

const (
	enrollmentAuditRejected enrollmentAuditKind = iota + 1
	enrollmentAuditUnavailable
)

// enrollmentRequestGuard bounds the unauthenticated work performed before a
// join token is verified. Source identities and counters remain process-local;
// only one secret-free aggregate audit marker per outcome and window is
// persisted.
type enrollmentRequestGuard struct {
	mu              sync.Mutex
	now             func() time.Time
	windowStartedAt time.Time
	total           int
	bySource        map[string]int
	audited         map[enrollmentAuditKind]bool
}

type agentSessionAuditKey struct {
	nodeID string
	kind   transport.AgentSessionRejectionKind
}

type agentSessionAuditGuard struct {
	mu              sync.Mutex
	now             func() time.Time
	windowStartedAt time.Time
	claimed         map[agentSessionAuditKey]struct{}
}

func newEnrollmentRequestGuard(now func() time.Time) *enrollmentRequestGuard {
	if now == nil {
		now = time.Now
	}
	return &enrollmentRequestGuard{
		now:      now,
		bySource: make(map[string]int),
		audited:  make(map[enrollmentAuditKind]bool),
	}
}

func newAgentSessionAuditGuard(now func() time.Time) *agentSessionAuditGuard {
	if now == nil {
		now = time.Now
	}
	return &agentSessionAuditGuard{
		now:     now,
		claimed: make(map[agentSessionAuditKey]struct{}),
	}
}

func (guard *enrollmentRequestGuard) admit(remoteAddress string) bool {
	if guard == nil || guard.now == nil {
		return false
	}
	now := guard.now()
	if now.IsZero() {
		return false
	}
	source := enrollmentSource(remoteAddress)
	guard.mu.Lock()
	defer guard.mu.Unlock()
	guard.rollWindow(now)
	if guard.total >= enrollmentRequestsPerWindow {
		return false
	}
	if guard.bySource[source] >= enrollmentRequestsPerSource {
		return false
	}
	if _, tracked := guard.bySource[source]; !tracked &&
		len(guard.bySource) >= enrollmentTrackedSources {
		// The global ceiling still bounds CPU when the source table is full.
		// Refuse to grow attacker-controlled memory any further.
		return false
	}
	guard.total++
	guard.bySource[source]++
	return true
}

func (guard *enrollmentRequestGuard) claimAudit(kind enrollmentAuditKind) bool {
	if guard == nil || guard.now == nil {
		return false
	}
	now := guard.now()
	if now.IsZero() {
		return false
	}
	guard.mu.Lock()
	defer guard.mu.Unlock()
	guard.rollWindow(now)
	if guard.audited[kind] {
		return false
	}
	guard.audited[kind] = true
	return true
}

func (guard *enrollmentRequestGuard) rollWindow(now time.Time) {
	if guard.windowStartedAt.IsZero() ||
		now.Before(guard.windowStartedAt) ||
		!now.Before(guard.windowStartedAt.Add(enrollmentRequestWindow)) {
		guard.windowStartedAt = now
		guard.total = 0
		clear(guard.bySource)
		clear(guard.audited)
	}
}

func enrollmentSource(remoteAddress string) string {
	host, _, err := net.SplitHostPort(remoteAddress)
	if err != nil {
		return "unknown"
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return "unknown"
	}
	return ip.String()
}

func (guard *agentSessionAuditGuard) claim(
	nodeID string,
	kind transport.AgentSessionRejectionKind,
) bool {
	if guard == nil || guard.now == nil || nodeID == "" {
		return false
	}
	now := guard.now()
	if now.IsZero() {
		return false
	}
	key := agentSessionAuditKey{nodeID: nodeID, kind: kind}
	guard.mu.Lock()
	defer guard.mu.Unlock()
	if guard.windowStartedAt.IsZero() ||
		now.Before(guard.windowStartedAt) ||
		!now.Before(guard.windowStartedAt.Add(enrollmentRequestWindow)) {
		guard.windowStartedAt = now
		clear(guard.claimed)
	}
	if _, exists := guard.claimed[key]; exists ||
		len(guard.claimed) >= enrollmentRequestsPerWindow {
		return false
	}
	guard.claimed[key] = struct{}{}
	return true
}
