package app

import (
	"context"
	"errors"
	"strconv"

	managementapi "github.com/genm/tewake/internal/api"
	"github.com/genm/tewake/internal/config"
	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/github"
	"github.com/genm/tewake/internal/store"
)

// githubTargetVerifier is the application-owned bridge between operator
// configuration and provider authority. The provider adapter must prove the
// private scope and safe runner group before this object adds those fields to
// the domain value accepted by SQLite.
type githubTargetVerifier struct {
	authority *github.Authority
	store     *store.ControllerStore
}

func newGitHubTargetVerifier(authority *github.Authority, controllerStore *store.ControllerStore) ManagementTargetVerifier {
	if authority == nil {
		return nil
	}
	return githubTargetVerifier{authority: authority, store: controllerStore}
}

func (verifier githubTargetVerifier) VerifyManagementTarget(
	ctx context.Context,
	target config.GitHubTargetConfiguration,
) (store.ManagementGitHubTarget, error) {
	if verifier.authority == nil {
		return store.ManagementGitHubTarget{}, managementapi.ErrBackendUnavailable
	}
	installationID, parseErr := strconv.ParseInt(target.InstallationID, 10, 64)
	if parseErr != nil {
		return store.ManagementGitHubTarget{}, &managementapi.ValidationError{Violations: []managementapi.FieldViolation{{Field: "targets.installationId", Code: "invalid", Message: "GitHub installation ID is invalid."}}}
	}
	if verifier.store != nil {
		if err := verifier.store.BeginGitHubTargetProvisioning(ctx, store.GitHubTargetProvisioningIntent{TargetID: string(target.ID), InstallationID: installationID, ScopeKind: string(target.ScopeKind), Scope: target.Scope, ScaleSetName: target.ScaleSetName, ProfileID: string(target.RunnerProfileID)}); err != nil {
			return store.ManagementGitHubTarget{}, managementapi.ErrBackendUnavailable
		}
	}
	verified, err := verifier.authority.VerifyAndProvisionTarget(ctx, github.TargetRequest{
		TargetID:           string(target.ID),
		InstallationID:     target.InstallationID,
		ScopeKind:          string(target.ScopeKind),
		Scope:              target.Scope,
		ScaleSetName:       target.ScaleSetName,
		RunnerProfileID:    string(target.RunnerProfileID),
		RunnerProfileLabel: target.ScaleSetName,
	})
	if err != nil {
		if verifier.store != nil {
			state, code := "rolled_back", "provider_rejected"
			if errors.Is(err, github.ErrGitHubProvisioningAmbiguous) {
				state, code = "ambiguous", "provider_provisioning_ambiguous"
			}
			_ = verifier.store.UpdateGitHubTargetProvisioning(ctx, string(target.ID), state, code, nil, nil)
		}
		if errors.Is(err, github.ErrGitHubTargetInvalid) ||
			errors.Is(err, github.ErrGitHubInstallation) ||
			errors.Is(err, github.ErrGitHubTargetNotPrivate) ||
			errors.Is(err, github.ErrGitHubRunnerGroupUnsafe) ||
			errors.Is(err, github.ErrGitHubTargetConflict) {
			return store.ManagementGitHubTarget{}, &managementapi.ValidationError{Violations: []managementapi.FieldViolation{{
				Field: "targets", Code: "provider_rejected", Message: "GitHub did not authorize this private target.",
			}}}
		}
		return store.ManagementGitHubTarget{}, managementapi.ErrBackendUnavailable
	}
	if verifier.store != nil {
		scaleSetID := int64(verified.ScaleSetID)
		runnerGroupID := int64(verified.RunnerGroupID)
		if err := verifier.store.UpdateGitHubTargetProvisioning(ctx, string(target.ID), "ready", "", &scaleSetID, &runnerGroupID); err != nil {
			_ = verifier.store.UpdateGitHubTargetProvisioning(ctx, string(target.ID), "ambiguous", "intent_update_failed", &scaleSetID, &runnerGroupID)
			return store.ManagementGitHubTarget{}, managementapi.ErrBackendUnavailable
		}
	}
	return store.ManagementGitHubTarget{Target: domain.GitHubTarget{
		ID:                    domain.TargetID(verified.TargetID),
		InstallationID:        verified.InstallationID,
		ScopeKind:             domain.TargetScopeKind(verified.ScopeKind),
		Scope:                 verified.Scope,
		Visibility:            domain.TargetPrivate,
		RunnerGroupAccessSafe: true,
		ScaleSetName:          verified.ScaleSetName,
		RunnerProfileID:       domain.RunnerProfileID(verified.RunnerProfileID),
	}, ScaleSetID: store.ScaleSetID(verified.ScaleSetID)}, nil
}

func (verifier githubTargetVerifier) MarkManagementTargetCommitted(ctx context.Context, target store.ManagementGitHubTarget) error {
	if verifier.store == nil {
		return nil
	}
	scaleSetID := int64(target.ScaleSetID)
	return verifier.store.UpdateGitHubTargetProvisioning(ctx, string(target.Target.ID), "committed", "", &scaleSetID, nil)
}

func (verifier githubTargetVerifier) MarkManagementTargetAmbiguous(ctx context.Context, target store.ManagementGitHubTarget, code string) error {
	if verifier.store == nil {
		return nil
	}
	scaleSetID := int64(target.ScaleSetID)
	return verifier.store.UpdateGitHubTargetProvisioning(ctx, string(target.Target.ID), "ambiguous", code, &scaleSetID, nil)
}
