package github

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/actions/scaleset"
)

// AppClientConfig identifies a single GitHub App installation. A controller
// creates one Client per installation so tokens, sessions, and target state are
// never accidentally shared between installations.
type AppClientConfig struct {
	GitHubConfigURL string
	ClientID        string
	InstallationID  int64
	PrivateKey      AppPrivateKey
	System          string
	Version         string
	CommitSHA       string
	Subsystem       string
}

// Client is the only production implementation that imports the official
// github.com/actions/scaleset v0.4.0 dependency.
type Client struct {
	client *scaleset.Client
}

// NewAppClient creates a low-level client for one GitHub App installation.
// PrivateKey remains opaque to callers and is passed directly to the official
// client without being logged or persisted by this package.
func NewAppClient(config AppClientConfig) (*Client, error) {
	if err := validateGitHubConfigURL(config.GitHubConfigURL); err != nil {
		return nil, err
	}
	if config.ClientID == "" {
		return nil, errors.New("GitHub App client ID is required")
	}
	if config.InstallationID <= 0 {
		return nil, errors.New("GitHub App installation ID must be positive")
	}
	if config.PrivateKey.value == "" {
		return nil, errors.New("GitHub App private key is required")
	}

	client, err := scaleset.NewClientWithGitHubApp(scaleset.ClientWithGitHubAppConfig{
		GitHubConfigURL: config.GitHubConfigURL,
		GitHubAppAuth: scaleset.GitHubAppAuth{
			ClientID:       config.ClientID,
			InstallationID: config.InstallationID,
			PrivateKey:     config.PrivateKey.value,
		},
		SystemInfo: scaleset.SystemInfo{
			System:    config.System,
			Version:   config.Version,
			CommitSHA: config.CommitSHA,
			Subsystem: config.Subsystem,
		},
	}, scaleset.WithRetryMax(0))
	if err != nil {
		return nil, fmt.Errorf("creating GitHub scale-set client: %w", err)
	}

	return &Client{client: client}, nil
}

// ScaleSet describes the lifecycle fields owned by the GitHub scale-set API.
type ScaleSet struct {
	ID            ScaleSetID
	Name          string
	RunnerGroupID int
	Labels        []string
	DisableUpdate bool
	// Statistics is nil when GitHub did not include statistics. Unknown state is
	// deliberately distinct from an observed all-zero healthy scale set.
	Statistics *Statistics
}

// CreateScaleSet creates one scale set in this Client's installation.
func (c *Client) CreateScaleSet(ctx context.Context, scaleSet ScaleSet) (ScaleSet, error) {
	if err := validateRequestedScaleSet(scaleSet, false); err != nil {
		return ScaleSet{}, err
	}
	created, err := contain(func() (*scaleset.RunnerScaleSet, error) {
		return c.client.CreateRunnerScaleSet(ctx, toScaleSet(scaleSet))
	})
	if err != nil {
		return ScaleSet{}, fmt.Errorf("creating GitHub runner scale set: %w", err)
	}
	if created == nil {
		return ScaleSet{}, ErrInvalidPreviewResponse
	}
	converted, err := fromScaleSet(*created)
	if err != nil {
		return ScaleSet{}, fmt.Errorf("validating created GitHub runner scale set: %w", err)
	}
	if err := validateExpectedScaleSet(converted, scaleSet); err != nil {
		return ScaleSet{}, err
	}
	return converted, nil
}

// GetScaleSet returns nil when GitHub has no scale set with that name in the
// given runner group.
func (c *Client) GetScaleSet(ctx context.Context, runnerGroupID int, name string) (*ScaleSet, error) {
	if runnerGroupID <= 0 || !isGitHubPathPart(name) {
		return nil, ErrInvalidPreviewResponse
	}
	result, err := contain(func() (*scaleset.RunnerScaleSet, error) { return c.client.GetRunnerScaleSet(ctx, runnerGroupID, name) })
	if err != nil {
		return nil, fmt.Errorf("getting GitHub runner scale set: %w", err)
	}
	if result == nil {
		return nil, nil
	}

	converted, err := fromScaleSet(*result)
	if err != nil {
		return nil, fmt.Errorf("validating GitHub runner scale set: %w", err)
	}
	if converted.Name != name || converted.RunnerGroupID != runnerGroupID {
		return nil, ErrInvalidPreviewResponse
	}
	return &converted, nil
}

// UpdateScaleSet updates an existing GitHub scale set by its immutable ID.
func (c *Client) UpdateScaleSet(ctx context.Context, scaleSet ScaleSet) (ScaleSet, error) {
	if err := validateRequestedScaleSet(scaleSet, true); err != nil {
		return ScaleSet{}, err
	}
	updated, err := contain(func() (*scaleset.RunnerScaleSet, error) {
		return c.client.UpdateRunnerScaleSet(ctx, int(scaleSet.ID), toScaleSet(scaleSet))
	})
	if err != nil {
		return ScaleSet{}, fmt.Errorf("updating GitHub runner scale set: %w", err)
	}
	if updated == nil {
		return ScaleSet{}, ErrInvalidPreviewResponse
	}
	converted, err := fromScaleSet(*updated)
	if err != nil {
		return ScaleSet{}, fmt.Errorf("validating updated GitHub runner scale set: %w", err)
	}
	if err := validateExpectedScaleSet(converted, scaleSet); err != nil {
		return ScaleSet{}, err
	}
	return converted, nil
}

// DeleteScaleSet deletes one GitHub scale set by immutable ID.
func (c *Client) DeleteScaleSet(ctx context.Context, scaleSetID ScaleSetID) error {
	if scaleSetID <= 0 {
		return ErrInvalidScaleSetID
	}
	if err := c.client.DeleteRunnerScaleSet(ctx, int(scaleSetID)); err != nil {
		return fmt.Errorf("deleting GitHub runner scale set: %w", err)
	}
	return nil
}

// JITRequest contains only the non-secret request parameters GitHub needs to
// create an opaque JIT configuration.
type JITRequest struct {
	ScaleSetID ScaleSetID
	Name       string
	WorkFolder string
}

// GenerateJITConfig creates an in-memory-only JIT configuration. Its encoded
// body is unavailable to logs, JSON, and durable storage by type design.
func (c *Client) GenerateJITConfig(ctx context.Context, request JITRequest) (JITConfig, error) {
	if request.ScaleSetID <= 0 {
		return JITConfig{}, ErrInvalidScaleSetID
	}
	if request.Name == "" {
		return JITConfig{}, errors.New("GitHub JIT runner name is required")
	}
	if request.WorkFolder == "" {
		return JITConfig{}, errors.New("GitHub JIT work folder is required")
	}

	result, err := contain(func() (*scaleset.RunnerScaleSetJitRunnerConfig, error) {
		return c.client.GenerateJitRunnerConfig(ctx, &scaleset.RunnerScaleSetJitRunnerSetting{
			Name:       request.Name,
			WorkFolder: request.WorkFolder,
		}, int(request.ScaleSetID))
	})
	if err != nil {
		return JITConfig{}, fmt.Errorf("generating GitHub JIT configuration: %w", err)
	}
	if err := validateJITResult(result, request); err != nil {
		return JITConfig{}, ErrInvalidPreviewResponse
	}

	return newJITConfig(result.EncodedJITConfig, RunnerReference{
		ID:         result.Runner.ID,
		Name:       result.Runner.Name,
		ScaleSetID: ScaleSetID(result.Runner.RunnerScaleSetID),
	}), nil
}

func validateJITResult(result *scaleset.RunnerScaleSetJitRunnerConfig, request JITRequest) error {
	if result == nil || result.Runner == nil || result.EncodedJITConfig == "" || result.Runner.ID <= 0 || result.Runner.Name != request.Name || ScaleSetID(result.Runner.RunnerScaleSetID) != request.ScaleSetID {
		return ErrInvalidPreviewResponse
	}
	return nil
}

// MessageSession is a low-level, per-scale-set GitHub message source.
type MessageSession struct {
	client     *scaleset.MessageSessionClient
	scaleSetID ScaleSetID
}

var _ MessageSource = (*MessageSession)(nil)

// OpenMessageSession opens one message session. It is intentionally separate
// from Poller creation so controller code owns the source's lifecycle.
func (c *Client) OpenMessageSession(ctx context.Context, scaleSetID ScaleSetID, owner string) (*MessageSession, error) {
	if scaleSetID <= 0 {
		return nil, ErrInvalidScaleSetID
	}
	if owner == "" {
		return nil, errors.New("GitHub message session owner is required")
	}

	session, err := contain(func() (*scaleset.MessageSessionClient, error) {
		return c.client.MessageSessionClient(ctx, int(scaleSetID), owner, scaleset.WithRetryMax(0))
	})
	if err != nil {
		return nil, fmt.Errorf("opening GitHub message session: %w", err)
	}
	return &MessageSession{client: session, scaleSetID: scaleSetID}, nil
}

// Snapshot returns only the values needed for durable demand handling. Queue
// endpoint and bearer token remain encapsulated in the official client.
func (s *MessageSession) Snapshot() (SessionSnapshot, error) {
	session := s.client.Session()
	statistics, err := fromStatistics(session.Statistics)
	if err != nil || statistics == nil || session.SessionID.String() == "00000000-0000-0000-0000-000000000000" {
		return SessionSnapshot{}, ErrInvalidPreviewResponse
	}
	return SessionSnapshot{ScaleSetID: s.scaleSetID, ID: session.SessionID.String(), Statistics: *statistics}, nil
}

// Poll implements MessageSource with the pinned v0.4.0 GetMessage operation.
func (s *MessageSession) Poll(ctx context.Context, lastAcknowledgedMessageID, maxCapacity int) (*Message, error) {
	if maxCapacity < 0 {
		return nil, ErrInvalidCapacity
	}
	message, err := s.client.GetMessage(ctx, lastAcknowledgedMessageID, maxCapacity)
	if err != nil {
		return nil, fmt.Errorf("getting GitHub scale-set message: %w", err)
	}
	if message == nil {
		return nil, nil
	}
	converted, err := fromMessage(s.scaleSetID, message)
	if err != nil {
		return nil, fmt.Errorf("validating GitHub scale-set message: %w", err)
	}
	return converted, nil
}

// DeleteMessage implements MessageSource with the pinned v0.4.0 acknowledgement
// operation. Poller controls its ordering relative to durable commit.
func (s *MessageSession) DeleteMessage(ctx context.Context, messageID int) error {
	if messageID <= 0 {
		return ErrInvalidMessageID
	}
	if err := s.client.DeleteMessage(ctx, messageID); err != nil {
		return fmt.Errorf("deleting GitHub scale-set message: %w", err)
	}
	return nil
}

// AcquireJobs exposes the distinct low-level job acquisition operation. It is
// intentionally not hidden inside Poller because acquisition has its own later
// durable scheduling contract.
func (s *MessageSession) AcquireJobs(ctx context.Context, requestIDs []int64) ([]int64, error) {
	ids, err := s.client.AcquireJobs(ctx, requestIDs)
	if err != nil {
		return nil, fmt.Errorf("acquiring GitHub scale-set jobs: %w", err)
	}
	return ids, nil
}

// Close ends the GitHub message session.
func (s *MessageSession) Close(ctx context.Context) error {
	if err := s.client.Close(ctx); err != nil {
		return fmt.Errorf("closing GitHub message session: %w", err)
	}
	return nil
}

func toScaleSet(scaleSet ScaleSet) *scaleset.RunnerScaleSet {
	labels := make([]scaleset.Label, 0, len(scaleSet.Labels))
	for _, label := range scaleSet.Labels {
		// v0.4.0's example builds labels with Name only; its CreateRunnerScaleSet
		// applies the upstream default Type rather than requiring callers to set it.
		labels = append(labels, scaleset.Label{Name: label})
	}
	return &scaleset.RunnerScaleSet{
		ID:            int(scaleSet.ID),
		Name:          scaleSet.Name,
		RunnerGroupID: scaleSet.RunnerGroupID,
		Labels:        labels,
		RunnerSetting: scaleset.RunnerSetting{DisableUpdate: scaleSet.DisableUpdate},
	}
}

func fromScaleSet(scaleSet scaleset.RunnerScaleSet) (ScaleSet, error) {
	statistics, err := fromStatistics(scaleSet.Statistics)
	if err != nil {
		return ScaleSet{}, err
	}
	labels := make([]string, 0, len(scaleSet.Labels))
	for _, label := range scaleSet.Labels {
		labels = append(labels, label.Name)
	}
	converted := ScaleSet{
		ID:            ScaleSetID(scaleSet.ID),
		Name:          scaleSet.Name,
		RunnerGroupID: scaleSet.RunnerGroupID,
		Labels:        labels,
		DisableUpdate: scaleSet.RunnerSetting.DisableUpdate,
		Statistics:    statistics,
	}
	if err := validateScaleSet(converted); err != nil {
		return ScaleSet{}, err
	}
	return converted, nil
}

var ErrInvalidPreviewResponse = errors.New("invalid GitHub scale-set preview response")

func contain[T any](call func() (T, error)) (result T, err error) {
	defer func() {
		if recover() != nil {
			var zero T
			result = zero
			err = ErrInvalidPreviewResponse
		}
	}()
	return call()
}

func validateScaleSet(scaleSet ScaleSet) error {
	if scaleSet.ID <= 0 || scaleSet.Name == "" || scaleSet.RunnerGroupID <= 0 || len(scaleSet.Labels) == 0 {
		return ErrInvalidPreviewResponse
	}
	for _, label := range scaleSet.Labels {
		if !isGitHubPathPart(label) {
			return ErrInvalidPreviewResponse
		}
	}
	return nil
}

func validateRequestedScaleSet(scaleSet ScaleSet, requireID bool) error {
	if requireID && scaleSet.ID <= 0 {
		return ErrInvalidScaleSetID
	}
	if scaleSet.Name == "" || scaleSet.RunnerGroupID <= 0 || len(scaleSet.Labels) == 0 {
		return ErrInvalidPreviewResponse
	}
	for _, label := range scaleSet.Labels {
		if !isGitHubPathPart(label) {
			return ErrInvalidPreviewResponse
		}
	}
	return nil
}

func validateExpectedScaleSet(actual, requested ScaleSet) error {
	if actual.Name != requested.Name || actual.RunnerGroupID != requested.RunnerGroupID {
		return ErrInvalidPreviewResponse
	}
	if len(actual.Labels) != len(requested.Labels) {
		return ErrInvalidPreviewResponse
	}
	for index, label := range requested.Labels {
		if actual.Labels[index] != label {
			return ErrInvalidPreviewResponse
		}
	}
	return nil
}

func fromMessage(scaleSetID ScaleSetID, message *scaleset.RunnerScaleSetMessage) (*Message, error) {
	if message == nil {
		return nil, errors.New("GitHub scale-set message is required")
	}
	statistics, err := fromStatistics(message.Statistics)
	if err != nil {
		return nil, err
	}
	if statistics == nil {
		return nil, ErrInvalidStatistics
	}
	result := &Message{
		ScaleSetID: scaleSetID,
		ID:         message.MessageID,
		Statistics: *statistics,
	}
	for _, job := range message.JobAvailableMessages {
		result.Jobs = append(result.Jobs, fromJobMessage(MessageTypeJobAvailable, job.JobMessageBase, 0, "", ""))
	}
	for _, job := range message.JobAssignedMessages {
		result.Jobs = append(result.Jobs, fromJobMessage(MessageTypeJobAssigned, job.JobMessageBase, 0, "", ""))
	}
	for _, job := range message.JobStartedMessages {
		result.Jobs = append(result.Jobs, fromJobMessage(MessageTypeJobStarted, job.JobMessageBase, job.RunnerID, job.RunnerName, ""))
	}
	for _, job := range message.JobCompletedMessages {
		result.Jobs = append(result.Jobs, fromJobMessage(MessageTypeJobCompleted, job.JobMessageBase, job.RunnerID, job.RunnerName, job.Result))
	}
	return result, nil
}

func fromJobMessage(messageType MessageType, job scaleset.JobMessageBase, runnerID int, runnerName, result string) JobMessage {
	return JobMessage{
		Type:            messageType,
		RunnerRequestID: job.RunnerRequestID,
		RunnerID:        runnerID,
		RunnerName:      runnerName,
		Result:          result,
		RepositoryName:  job.RepositoryName,
		OwnerName:       job.OwnerName,
		JobID:           job.JobID,
		WorkflowRunID:   job.WorkflowRunID,
	}
}

func fromStatistics(statistics *scaleset.RunnerScaleSetStatistic) (*Statistics, error) {
	if statistics == nil {
		return nil, nil
	}
	converted := Statistics{
		TotalAvailableJobs:     statistics.TotalAvailableJobs,
		TotalAcquiredJobs:      statistics.TotalAcquiredJobs,
		TotalAssignedJobs:      statistics.TotalAssignedJobs,
		TotalRunningJobs:       statistics.TotalRunningJobs,
		TotalRegisteredRunners: statistics.TotalRegisteredRunners,
		TotalBusyRunners:       statistics.TotalBusyRunners,
		TotalIdleRunners:       statistics.TotalIdleRunners,
	}
	if err := validateStatistics(converted); err != nil {
		return nil, err
	}
	return &converted, nil
}

func validateStatistics(statistics Statistics) error {
	values := []int{
		statistics.TotalAvailableJobs,
		statistics.TotalAcquiredJobs,
		statistics.TotalAssignedJobs,
		statistics.TotalRunningJobs,
		statistics.TotalRegisteredRunners,
		statistics.TotalBusyRunners,
		statistics.TotalIdleRunners,
	}
	for _, value := range values {
		if value < 0 {
			return ErrInvalidStatistics
		}
	}
	if statistics.TotalAssignedJobs < statistics.TotalRunningJobs {
		return ErrInvalidStatistics
	}
	return nil
}

func validateGitHubConfigURL(configURL string) error {
	if configURL == "" {
		return errors.New("GitHub config URL is required")
	}
	parsed, err := url.ParseRequestURI(configURL)
	if err != nil {
		return fmt.Errorf("invalid GitHub config URL: %w", err)
	}
	if parsed.Scheme != "https" {
		return errors.New("GitHub config URL must use https")
	}
	if !strings.EqualFold(parsed.Hostname(), "github.com") || parsed.Host != parsed.Hostname() {
		return errors.New("GitHub config URL host must be github.com without a port")
	}
	if parsed.User != nil {
		return errors.New("GitHub config URL must not include userinfo")
	}
	if parsed.RawQuery != "" || parsed.ForceQuery {
		return errors.New("GitHub config URL must not include a query")
	}
	if parsed.Fragment != "" || strings.Contains(configURL, "#") {
		return errors.New("GitHub config URL must not include a fragment")
	}
	if parsed.Opaque != "" {
		return errors.New("GitHub config URL must be hierarchical")
	}

	parts := strings.Split(strings.Trim(parsed.EscapedPath(), "/"), "/")
	if len(parts) < 1 || len(parts) > 2 || parts[0] == "" {
		return errors.New("GitHub config URL path must identify an organization or repository")
	}
	for _, part := range parts {
		if !isGitHubPathPart(part) {
			return errors.New("GitHub config URL path must identify an organization or repository")
		}
	}
	return nil
}

func isGitHubPathPart(part string) bool {
	if part == "" || part == "." || part == ".." {
		return false
	}
	for _, character := range part {
		if !((character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == '-' || character == '_' || character == '.') {
			return false
		}
	}
	return true
}
