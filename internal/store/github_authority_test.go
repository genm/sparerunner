package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestGitHubAuthorityPersistenceKeepsSecretsOutOfSQLite(t *testing.T) {
	ctx := context.Background()
	controller, err := OpenController(ctx, filepath.Join(privateTestDir(t), "github-authority.db"), Options{Now: func() time.Time { return time.Unix(100, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if err := controller.ReplaceGitHubInstallations(ctx, []GitHubInstallation{{ID: 42, AccountLogin: "acme", AccountType: "Organization", RepositorySelection: "all"}}); err != nil {
		t.Fatal(err)
	}
	installations, err := controller.ReadGitHubInstallations(ctx)
	if err != nil || len(installations) != 1 || installations[0].ID != 42 {
		t.Fatalf("installations = %#v, err=%v", installations, err)
	}
	if err := controller.BeginGitHubTargetProvisioning(ctx, GitHubTargetProvisioningIntent{TargetID: "target-private", InstallationID: 42, ScopeKind: "repository", Scope: "acme/private", ScaleSetName: "sparerunner", ProfileID: "profile-sparerunner"}); err != nil {
		t.Fatal(err)
	}
	scaleSetID, groupID := int64(99), int64(7)
	if err := controller.UpdateGitHubTargetProvisioning(ctx, "target-private", "ready", "", &scaleSetID, &groupID); err != nil {
		t.Fatal(err)
	}
	if err := controller.UpdateGitHubTargetProvisioning(ctx, "target-private", "committed", "", &scaleSetID, nil); err != nil {
		t.Fatal(err)
	}
	intent, err := controller.ReadGitHubTargetProvisioning(ctx, "target-private")
	if err != nil || intent.State != "committed" || intent.ScaleSetID == nil || *intent.ScaleSetID != 99 || intent.RunnerGroupID == nil || *intent.RunnerGroupID != 7 {
		t.Fatalf("intent = %#v, err=%v", intent, err)
	}
	if err := controller.ReplaceGitHubInstallations(ctx, []GitHubInstallation{{ID: 43, AccountLogin: "genm", AccountType: "User", RepositorySelection: "selected"}}); err != nil {
		t.Fatal(err)
	}
	if current, err := controller.ReadGitHubInstallations(ctx); err != nil || len(current) != 1 || current[0].ID != 43 {
		t.Fatalf("replacement = %#v, err=%v", current, err)
	}
	if err := controller.UpdateGitHubTargetProvisioning(ctx, "missing", "committed", "", nil, nil); !errors.Is(err, ErrGitHubProvisioningIntent) {
		t.Fatalf("missing intent error = %v", err)
	}
}
