package app

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/genm/sparerunner/internal/buildinfo"
	"github.com/genm/sparerunner/internal/domain"
	"github.com/genm/sparerunner/internal/github"
	"github.com/genm/sparerunner/internal/store"
)

var (
	// ErrControllerFleetConfig reports an incomplete fleet composition. It is a
	// programming error at construction time, never a runtime provider state.
	ErrControllerFleetConfig = errors.New("controller fleet configuration is invalid")
	// ErrControllerFleetScaleSet reports that the durable runtime binding no
	// longer names the scale set GitHub reports for this Target. Driving it
	// anyway could consume another installation's queue, so the Target runs no
	// coordinator until an operator reprovisions it.
	ErrControllerFleetScaleSet = errors.New("GitHub scale set does not match the stored runtime binding")
)

const (
	controllerFleetRetryInitial = 1 * time.Second
	controllerFleetRetryMaximum = 5 * time.Minute
	// controllerFleetCloseBudget bounds provider session teardown so a wedged
	// GitHub endpoint cannot hold controller shutdown open indefinitely.
	controllerFleetCloseBudget = 10 * time.Second
	// controllerFleetResyncInterval is a safety net behind the management change
	// signal, not the primary trigger. Durable state can advance through a path
	// that never touches the in-memory projection, and a Target must not stay
	// unscheduled until the next operator action in that case.
	controllerFleetResyncInterval = 1 * time.Minute
)

// ControllerFleetTarget is one fully resolved unit of coordination: a Target
// whose configuration, committed runtime binding, and provisioning evidence all
// agree. It is comparable on purpose, so a configuration change is detected by
// value rather than by re-deriving intent.
type ControllerFleetTarget struct {
	TargetID        domain.TargetID
	ScaleSetID      github.ScaleSetID
	ScaleSetName    string
	RunnerGroupID   int
	InstallationID  int64
	Scope           string
	ScopeKind       domain.TargetScopeKind
	RunnerProfileID domain.RunnerProfileID
	VersionPolicy   domain.RunnerVersionPolicy
}

// ControllerFleetSession is the provider surface one Target's coordinator owns
// for the lifetime of a single attempt.
type ControllerFleetSession interface {
	controllerRunnerMessageSession
	Close(ctx context.Context) error
}

// ControllerFleetProvider opens the exact GitHub surface for one Target. The
// production implementation lives below; tests substitute it so fleet
// supervision can be exercised without a live provider.
type ControllerFleetProvider interface {
	Open(context.Context, ControllerFleetTarget) (ControllerFleetSession, ControllerRunnerLifecycle, error)
}

// ControllerFleet supervises exactly one runner coordinator per bound Target.
// A GitHub scale set exposes a single message queue, so two sessions or two
// pollers over one Target would race and double-consume it; the fleet is the
// component that makes "one per Target" a structural property.
type ControllerFleet struct {
	state    *ControllerState
	provider ControllerFleetProvider
	logger   *slog.Logger

	// resyncInterval and retry bounds are fields so tests can drive the same
	// code paths without real-time waits.
	resyncInterval time.Duration
	retryInitial   time.Duration
	retryMaximum   time.Duration
	closeBudget    time.Duration

	mu      sync.Mutex
	workers map[domain.TargetID]*controllerFleetWorker

	errorsMu sync.Mutex
	first    error
}

type controllerFleetWorker struct {
	target ControllerFleetTarget
	cancel context.CancelFunc
	done   chan struct{}
}

// NewControllerFleet composes the production fleet. A controller without GitHub
// App authority has no provider and must not be given a fleet at all; that is a
// normal disconnected state handled by the caller, so it fails closed here.
func NewControllerFleet(
	state *ControllerState,
	provider ControllerFleetProvider,
	logger *slog.Logger,
) (*ControllerFleet, error) {
	if state == nil || state.Store == nil || state.AgentBroker == nil ||
		state.Reconciler == nil || provider == nil {
		return nil, ErrControllerFleetConfig
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &ControllerFleet{
		state:          state,
		provider:       provider,
		logger:         logger,
		resyncInterval: controllerFleetResyncInterval,
		retryInitial:   controllerFleetRetryInitial,
		retryMaximum:   controllerFleetRetryMaximum,
		closeBudget:    controllerFleetCloseBudget,
		workers:        make(map[domain.TargetID]*controllerFleetWorker),
	}, nil
}

// Run supervises the coordinator set until ctx is cancelled. It never returns
// because one Target failed: a broken Target is retried with bounded backoff so
// it can never take down the agent or admin listeners.
func (fleet *ControllerFleet) Run(ctx context.Context) error {
	if fleet == nil {
		return ErrControllerFleetConfig
	}
	defer fleet.stopAll()
	// Reuse the management invalidation signal rather than polling blindly: the
	// same projection change that refreshes the management UI is what adds or
	// removes a Target here.
	for {
		changed := fleet.state.Reconciler.Change()
		if err := fleet.reconcile(ctx); err != nil {
			if ctx.Err() != nil {
				return fleet.shutdownResult()
			}
			fleet.logFailure("controller_fleet_configuration_unreadable", "", err)
		}
		resync := time.NewTimer(fleet.resyncInterval)
		select {
		case <-ctx.Done():
			resync.Stop()
			return fleet.shutdownResult()
		case <-changed:
		case <-resync.C:
		}
		resync.Stop()
	}
}

// DesiredTargets resolves the coordinator set the current durable state calls
// for. A Target without a committed runtime binding, without a runner profile,
// or without provisioning evidence is deliberately absent: fail closed at zero
// capacity and let the existing github_target_runtime_unverified condition
// surface it, rather than fabricating a binding.
func (fleet *ControllerFleet) DesiredTargets(
	ctx context.Context,
) ([]ControllerFleetTarget, error) {
	configuration, err := fleet.state.Store.ReadManagementConfiguration(ctx)
	if err != nil {
		return nil, err
	}
	profiles := make(map[domain.RunnerProfileID]store.ManagementRunnerProfile, len(configuration.RunnerProfiles))
	for _, profile := range configuration.RunnerProfiles {
		profiles[profile.Profile.ID] = profile
	}
	desired := make([]ControllerFleetTarget, 0, len(configuration.GitHubTargets))
	for _, target := range configuration.GitHubTargets {
		resolved, ok, err := fleet.resolveTarget(ctx, target, profiles)
		if err != nil {
			return nil, err
		}
		if ok {
			desired = append(desired, resolved)
		}
	}
	slices.SortFunc(desired, func(first, second ControllerFleetTarget) int {
		switch {
		case first.TargetID < second.TargetID:
			return -1
		case first.TargetID > second.TargetID:
			return 1
		default:
			return 0
		}
	})
	return desired, nil
}

func (fleet *ControllerFleet) resolveTarget(
	ctx context.Context,
	target store.ManagementGitHubTarget,
	profiles map[domain.RunnerProfileID]store.ManagementRunnerProfile,
) (ControllerFleetTarget, bool, error) {
	binding, found, err := fleet.state.Store.ReadGitHubTargetRuntimeBinding(
		ctx, target.Target.ID)
	if err != nil {
		return ControllerFleetTarget{}, false, err
	}
	if !found {
		fleet.logSkip(target.Target.ID, "runtime_binding_missing")
		return ControllerFleetTarget{}, false, nil
	}
	if binding.ScaleSetID != target.ScaleSetID ||
		binding.ProfileID != target.Target.RunnerProfileID {
		fleet.logSkip(target.Target.ID, "runtime_binding_mismatch")
		return ControllerFleetTarget{}, false, nil
	}
	profile, hasProfile := profiles[binding.ProfileID]
	if !hasProfile {
		fleet.logSkip(target.Target.ID, "runner_profile_missing")
		return ControllerFleetTarget{}, false, nil
	}
	installationID, parseErr := strconv.ParseInt(target.Target.InstallationID, 10, 64)
	if parseErr != nil || installationID <= 0 {
		fleet.logSkip(target.Target.ID, "installation_invalid")
		return ControllerFleetTarget{}, false, nil
	}
	intent, err := fleet.state.Store.ReadGitHubTargetProvisioning(
		ctx, string(target.Target.ID))
	if err != nil {
		// A committed Target with no readable provisioning evidence cannot name
		// the runner group GitHub needs to confirm the scale set. Skip it rather
		// than guessing.
		fleet.logSkip(target.Target.ID, "provisioning_intent_unreadable")
		return ControllerFleetTarget{}, false, nil
	}
	if intent.State != "committed" || intent.RunnerGroupID == nil ||
		*intent.RunnerGroupID <= 0 || intent.ScaleSetID == nil ||
		store.ScaleSetID(*intent.ScaleSetID) != binding.ScaleSetID {
		fleet.logSkip(target.Target.ID, "provisioning_not_committed")
		return ControllerFleetTarget{}, false, nil
	}
	return ControllerFleetTarget{
		TargetID:        target.Target.ID,
		ScaleSetID:      github.ScaleSetID(binding.ScaleSetID),
		ScaleSetName:    target.Target.ScaleSetName,
		RunnerGroupID:   int(*intent.RunnerGroupID),
		InstallationID:  installationID,
		Scope:           target.Target.Scope,
		ScopeKind:       target.Target.ScopeKind,
		RunnerProfileID: binding.ProfileID,
		VersionPolicy:   profile.Profile.VersionPolicy,
	}, true, nil
}

func (fleet *ControllerFleet) reconcile(ctx context.Context) error {
	desired, err := fleet.DesiredTargets(ctx)
	if err != nil {
		return err
	}
	wanted := make(map[domain.TargetID]ControllerFleetTarget, len(desired))
	for _, target := range desired {
		wanted[target.TargetID] = target
	}
	fleet.mu.Lock()
	stale := make([]*controllerFleetWorker, 0)
	for _, targetID := range slices.Sorted(maps.Keys(fleet.workers)) {
		worker := fleet.workers[targetID]
		if target, keep := wanted[targetID]; keep && target == worker.target {
			continue
		}
		delete(fleet.workers, targetID)
		stale = append(stale, worker)
	}
	fleet.mu.Unlock()
	// Stop outside the lock, and before starting the replacement: one scale set
	// must never have two live message sessions, not even briefly.
	for _, worker := range stale {
		worker.cancel()
		<-worker.done
	}
	if ctx.Err() != nil {
		return nil
	}
	fleet.mu.Lock()
	defer fleet.mu.Unlock()
	for _, target := range desired {
		if _, running := fleet.workers[target.TargetID]; running {
			continue
		}
		workerContext, cancel := context.WithCancel(ctx)
		worker := &controllerFleetWorker{
			target: target,
			cancel: cancel,
			done:   make(chan struct{}),
		}
		fleet.workers[target.TargetID] = worker
		go func() {
			defer close(worker.done)
			fleet.superviseTarget(workerContext, target)
		}()
	}
	return nil
}

// superviseTarget keeps one Target's coordinator alive. Every failure is
// classified, logged, and retried with bounded exponential backoff, so a Target
// that cannot reach GitHub degrades to a slow retry instead of a hot loop.
func (fleet *ControllerFleet) superviseTarget(
	ctx context.Context,
	target ControllerFleetTarget,
) {
	attempt := 0
	for {
		if ctx.Err() != nil {
			return
		}
		err := fleet.runTargetOnce(ctx, target)
		if ctx.Err() != nil {
			return
		}
		attempt++
		if err != nil {
			fleet.logFailure(
				"controller_fleet_target_failed",
				target.TargetID,
				err,
			)
		} else {
			// A clean return without cancellation still means this Target has no
			// live session any more; treat it as a restartable stop.
			fleet.logFailure(
				"controller_fleet_target_stopped",
				target.TargetID,
				nil,
			)
		}
		if !fleet.waitBackoff(ctx, attempt) {
			return
		}
	}
}

func (fleet *ControllerFleet) runTargetOnce(
	ctx context.Context,
	target ControllerFleetTarget,
) (returnedErr error) {
	session, lifecycle, err := fleet.provider.Open(ctx, target)
	if err != nil {
		return err
	}
	defer func() {
		closeContext, cancel := context.WithTimeout(
			context.WithoutCancel(ctx), fleet.closeBudget)
		defer cancel()
		if closeErr := session.Close(closeContext); closeErr != nil {
			fleet.recordError(closeErr)
			if returnedErr == nil {
				returnedErr = closeErr
			}
		}
	}()
	coordinator, err := NewControllerRunnerCoordinator(
		fleet.state.Store,
		session,
		fleet.state.AgentBroker,
		lifecycle,
		ControllerRunnerConfig{
			ScaleSetID:      target.ScaleSetID,
			TargetID:        target.TargetID,
			Scope:           target.Scope,
			ScopeKind:       target.ScopeKind,
			RunnerProfileID: target.RunnerProfileID,
			VersionPolicy:   target.VersionPolicy,
			ControllerEpoch: domain.ControllerEpoch(fleet.state.Epoch),
			Reconciler:      fleet.state.Reconciler,
		},
		fleet.logger,
	)
	if err != nil {
		return err
	}
	return coordinator.Run(ctx)
}

func (fleet *ControllerFleet) waitBackoff(ctx context.Context, attempt int) bool {
	if attempt < 1 {
		attempt = 1
	}
	delay := fleet.retryInitial
	for range attempt - 1 {
		if delay >= fleet.retryMaximum/2 {
			delay = fleet.retryMaximum
			break
		}
		delay *= 2
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (fleet *ControllerFleet) stopAll() {
	fleet.mu.Lock()
	workers := slices.Collect(maps.Values(fleet.workers))
	clear(fleet.workers)
	fleet.mu.Unlock()
	for _, worker := range workers {
		worker.cancel()
	}
	for _, worker := range workers {
		<-worker.done
	}
}

func (fleet *ControllerFleet) recordError(err error) {
	if err == nil {
		return
	}
	fleet.errorsMu.Lock()
	if fleet.first == nil {
		fleet.first = err
	}
	fleet.errorsMu.Unlock()
}

func (fleet *ControllerFleet) shutdownResult() error {
	fleet.errorsMu.Lock()
	defer fleet.errorsMu.Unlock()
	return fleet.first
}

func (fleet *ControllerFleet) logSkip(targetID domain.TargetID, reason string) {
	fleet.logger.Info(
		"controller_fleet_target_unbound",
		slog.String("component", "github"),
		slog.String("target_id", string(targetID)),
		slog.String("reason", reason),
	)
}

func (fleet *ControllerFleet) logFailure(
	message string,
	targetID domain.TargetID,
	err error,
) {
	attributes := []any{
		slog.String("component", "github"),
		slog.String("target_id", string(targetID)),
	}
	if err != nil {
		attributes = append(attributes, slog.String(
			"failure_class",
			string(ClassifyGitHubObservationFailure(err)),
		))
	}
	fleet.logger.Warn(message, attributes...)
}

// githubAuthorityFleetProvider is the production provider. It keeps the App
// private key inside internal/github by asking Authority for a ready client
// instead of for a credential.
type githubAuthorityFleetProvider struct {
	authority *github.Authority
}

// NewGitHubAuthorityFleetProvider returns nil when the controller has no GitHub
// App authority. A disconnected controller is a normal state, so the caller
// simply runs no fleet.
func NewGitHubAuthorityFleetProvider(authority *github.Authority) ControllerFleetProvider {
	if authority == nil {
		return nil
	}
	return githubAuthorityFleetProvider{authority: authority}
}

func (provider githubAuthorityFleetProvider) Open(
	ctx context.Context,
	target ControllerFleetTarget,
) (ControllerFleetSession, ControllerRunnerLifecycle, error) {
	client, err := provider.authority.InstallationClient(
		ctx,
		target.InstallationID,
		string(target.ScopeKind),
		target.Scope,
		buildinfo.Version(),
		buildinfo.Commit(),
	)
	if err != nil {
		return nil, nil, err
	}
	// The stored scale set ID is durable SpareRunner state, not provider truth.
	// Confirm it before opening a session so a stale binding can never drive
	// somebody else's queue.
	scaleSet, err := client.GetScaleSet(ctx, target.RunnerGroupID, target.ScaleSetName)
	if err != nil {
		return nil, nil, err
	}
	if scaleSet == nil || scaleSet.ID != target.ScaleSetID {
		return nil, nil, ErrControllerFleetScaleSet
	}
	lifecycle, err := NewGitHubClientRunnerLifecycle(client)
	if err != nil {
		return nil, nil, err
	}
	owner := target.Scope
	if target.ScopeKind == domain.TargetRepository {
		for index, character := range owner {
			if character == '/' {
				owner = owner[:index]
				break
			}
		}
	}
	session, err := client.OpenMessageSession(ctx, target.ScaleSetID, owner)
	if err != nil {
		return nil, nil, err
	}
	return session, lifecycle, nil
}
