package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"

	managementapi "github.com/genm/tewake/internal/api"
	"github.com/genm/tewake/internal/api/gen"
	"github.com/genm/tewake/internal/config"
	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/github"
	"github.com/genm/tewake/internal/store"
)

func (backend *managementBackend) StartGitHubAppManifest(_ context.Context, callbackURL, account string) (gen.GitHubManifestStart, error) {
	if backend == nil || backend.state == nil || backend.state.GitHubAuthority == nil {
		return gen.GitHubManifestStart{}, managementUnavailable()
	}
	start, err := backend.state.GitHubAuthority.StartManifest(callbackURL, account)
	if err != nil {
		return gen.GitHubManifestStart{}, managementUnavailable()
	}
	return gen.GitHubManifestStart{ActionUrl: start.ActionURL, Manifest: start.Manifest, State: start.State, ExpiresAt: start.ExpiresAt}, nil
}

func (backend *managementBackend) CompleteGitHubAppManifest(ctx context.Context, code, state string) error {
	if backend == nil || backend.state == nil || backend.state.GitHubAuthority == nil {
		return managementUnavailable()
	}
	if err := backend.state.GitHubAuthority.CompleteManifest(ctx, code, state); err != nil {
		if err == github.ErrManifestStateConsumed || err == github.ErrManifestStateProcessing {
			return managementapi.ErrGitHubCallbackConflict
		}
		return managementUnavailable()
	}
	return nil
}

func (backend *managementBackend) ListGitHubInstallations(ctx context.Context) (gen.GitHubInstallationList, error) {
	if backend == nil || backend.state == nil || backend.state.GitHubAuthority == nil {
		return gen.GitHubInstallationList{}, managementUnavailable()
	}
	installations, err := backend.state.GitHubAuthority.ListInstallations(ctx)
	if err != nil {
		return gen.GitHubInstallationList{}, managementUnavailable()
	}
	durable := make([]store.GitHubInstallation, 0, len(installations))
	result := gen.GitHubInstallationList{Installations: make([]gen.GitHubInstallation, 0, len(installations))}
	for _, installation := range installations {
		durable = append(durable, store.GitHubInstallation{ID: installation.ID, AccountLogin: installation.AccountLogin, AccountType: installation.AccountType, RepositorySelection: installation.RepositorySelection})
		result.Installations = append(result.Installations, gen.GitHubInstallation{
			Id: strconv.FormatInt(installation.ID, 10), AccountLogin: installation.AccountLogin,
			AccountType: gen.GitHubInstallationAccountType(installation.AccountType), RepositorySelection: installation.RepositorySelection,
		})
	}
	if err := backend.state.Store.ReplaceGitHubInstallations(ctx, durable); err != nil {
		return gen.GitHubInstallationList{}, managementUnavailable()
	}
	return result, nil
}

func (backend *managementBackend) CreateGitHubTarget(ctx context.Context, expected uint64, input gen.CreateGitHubTargetRequest, requestID string) (gen.GitHubTargetMutation, error) {
	if backend == nil || backend.state == nil {
		return gen.GitHubTargetMutation{}, managementUnavailable()
	}
	current, err := backend.state.Store.ReadManagementConfiguration(ctx)
	if err != nil {
		return gen.GitHubTargetMutation{}, err
	}
	document, err := managementConfigurationDocument(current)
	if err != nil {
		return gen.GitHubTargetMutation{}, err
	}
	profileID := domain.RunnerProfileID(input.RunnerProfileId)
	profileFound := false
	for _, profile := range document.RunnerProfiles {
		if profile.ID == profileID {
			profileFound = true
			break
		}
	}
	if !profileFound {
		// The first Target should be usable from Setup without a separate hidden
		// profile wizard. It is still a normal native profile and remains visible
		// in the canonical configuration returned to the browser.
		document.RunnerProfiles = append(document.RunnerProfiles, config.RunnerProfileConfiguration{
			ID: profileID, Label: input.ScaleSetName,
			VersionPolicy: domain.RunnerVersionAutoUpdate, Runtime: domain.RuntimeNative,
			MinAvailableMemoryBytes: 0,
		})
	}
	identity := input.InstallationId + "\x00" + string(input.ScopeKind) + "\x00" + input.Scope + "\x00" + input.ScaleSetName + "\x00" + input.RunnerProfileId
	digest := sha256.Sum256([]byte(identity))
	targetID := domain.TargetID("target-" + hex.EncodeToString(digest[:8]))
	document.Targets = append(document.Targets, config.GitHubTargetConfiguration{
		ID: targetID, InstallationID: input.InstallationId,
		ScopeKind: domain.TargetScopeKind(input.ScopeKind), Scope: input.Scope,
		ScaleSetName: input.ScaleSetName, RunnerProfileID: profileID,
	})
	encoded, err := config.EncodeJSON(document)
	if err != nil {
		return gen.GitHubTargetMutation{}, managementConfigurationError(err)
	}
	result, err := backend.ApplyConfiguration(ctx, expected, "application/json", encoded, requestID)
	if err != nil {
		return gen.GitHubTargetMutation{}, err
	}
	targets, _, targetsErr := backend.Targets(ctx)
	if targetsErr != nil {
		currentRevision, _ := strconv.ParseUint(string(result.Revision), 10, 64)
		return gen.GitHubTargetMutation{}, &managementapi.CommittedMutationError{Current: currentRevision}
	}
	for _, target := range targets {
		if target.Id == string(targetID) {
			return gen.GitHubTargetMutation{Target: target, ConfigurationRevision: result.Revision}, nil
		}
	}
	return gen.GitHubTargetMutation{}, &managementapi.CommittedMutationError{Current: expected + 1}
}

func managementUnavailable() error { return managementapi.ErrBackendUnavailable }
