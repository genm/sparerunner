package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
)

type GitHubInstallation struct {
	ID                  int64
	AccountLogin        string
	AccountType         string
	RepositorySelection string
	ObservedAtUnixNano  int64
}

type GitHubTargetProvisioningIntent struct {
	TargetID          string
	InstallationID    int64
	ScopeKind         string
	Scope             string
	ScaleSetName      string
	ProfileID         string
	State             string
	ScaleSetID        *int64
	RunnerGroupID     *int64
	ErrorCode         string
	CreatedAtUnixNano int64
	UpdatedAtUnixNano int64
}

var ErrGitHubProvisioningIntent = errors.New("GitHub target provisioning intent is invalid")

func (s *ControllerStore) ReplaceGitHubInstallations(ctx context.Context, installations []GitHubInstallation) error {
	if err := s.requireReady(); err != nil {
		return err
	}
	now, err := storeUnixNano(s.now())
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	seen := make(map[int64]struct{}, len(installations))
	for _, installation := range installations {
		if installation.ID <= 0 || installation.AccountLogin == "" || (installation.AccountType != "User" && installation.AccountType != "Organization") || (installation.RepositorySelection != "all" && installation.RepositorySelection != "selected" && installation.RepositorySelection != "none") {
			return ErrGitHubProvisioningIntent
		}
		if installation.ObservedAtUnixNano == 0 {
			installation.ObservedAtUnixNano = now
		}
		if _, exists := seen[installation.ID]; exists {
			return ErrGitHubProvisioningIntent
		}
		seen[installation.ID] = struct{}{}
		if _, err := tx.ExecContext(ctx, `INSERT INTO github_app_installations(installation_id, account_login, account_type, repository_selection, observed_at_unix_nano)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(installation_id) DO UPDATE SET account_login=excluded.account_login, account_type=excluded.account_type, repository_selection=excluded.repository_selection, observed_at_unix_nano=excluded.observed_at_unix_nano`, installation.ID, installation.AccountLogin, installation.AccountType, installation.RepositorySelection, installation.ObservedAtUnixNano); err != nil {
			return err
		}
	}
	deleteQuery := `DELETE FROM github_app_installations`
	deleteArgs := []any{}
	if len(seen) > 0 {
		placeholders := make([]string, 0, len(seen))
		for id := range seen {
			placeholders = append(placeholders, "?")
			deleteArgs = append(deleteArgs, id)
		}
		deleteQuery += ` WHERE installation_id NOT IN (` + strings.Join(placeholders, ",") + ")"
	}
	if _, err := tx.ExecContext(ctx, deleteQuery, deleteArgs...); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *ControllerStore) ReadGitHubInstallations(ctx context.Context) ([]GitHubInstallation, error) {
	if err := s.requireReady(); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT installation_id, account_login, account_type, repository_selection, observed_at_unix_nano FROM github_app_installations ORDER BY installation_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]GitHubInstallation, 0)
	for rows.Next() {
		var item GitHubInstallation
		if err := rows.Scan(&item.ID, &item.AccountLogin, &item.AccountType, &item.RepositorySelection, &item.ObservedAtUnixNano); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *ControllerStore) BeginGitHubTargetProvisioning(ctx context.Context, intent GitHubTargetProvisioningIntent) error {
	if err := validateProvisioningIntent(intent); err != nil {
		return err
	}
	if err := s.requireReady(); err != nil {
		return err
	}
	now, err := storeUnixNano(s.now())
	if err != nil {
		return err
	}
	if intent.CreatedAtUnixNano == 0 {
		intent.CreatedAtUnixNano = now
	}
	intent.UpdatedAtUnixNano = now
	_, err = s.db.ExecContext(ctx, `INSERT INTO github_target_provisioning_intents(target_id, installation_id, scope_kind, scope, scale_set_name, profile_id, state, scale_set_id, runner_group_id, error_code, created_at_unix_nano, updated_at_unix_nano) VALUES (?, ?, ?, ?, ?, ?, 'creating', NULL, NULL, '', ?, ?) ON CONFLICT(target_id) DO UPDATE SET installation_id=excluded.installation_id, scope_kind=excluded.scope_kind, scope=excluded.scope, scale_set_name=excluded.scale_set_name, profile_id=excluded.profile_id, state='creating', scale_set_id=NULL, runner_group_id=NULL, error_code='', updated_at_unix_nano=excluded.updated_at_unix_nano`, intent.TargetID, intent.InstallationID, intent.ScopeKind, intent.Scope, intent.ScaleSetName, intent.ProfileID, intent.CreatedAtUnixNano, intent.UpdatedAtUnixNano)
	return err
}

func (s *ControllerStore) UpdateGitHubTargetProvisioning(ctx context.Context, targetID, state, errorCode string, scaleSetID, runnerGroupID *int64) error {
	if targetID == "" || (state != "creating" && state != "ready" && state != "committed" && state != "rolled_back" && state != "ambiguous") || len(errorCode) > 100 {
		return ErrGitHubProvisioningIntent
	}
	if err := s.requireReady(); err != nil {
		return err
	}
	now, err := storeUnixNano(s.now())
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, `UPDATE github_target_provisioning_intents SET state=?, scale_set_id=COALESCE(?, scale_set_id), runner_group_id=COALESCE(?, runner_group_id), error_code=?, updated_at_unix_nano=? WHERE target_id=?`, state, scaleSetID, runnerGroupID, errorCode, now, targetID)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil || count != 1 {
		return ErrGitHubProvisioningIntent
	}
	return nil
}

func (s *ControllerStore) ReadGitHubTargetProvisioning(ctx context.Context, targetID string) (GitHubTargetProvisioningIntent, error) {
	if err := s.requireReady(); err != nil {
		return GitHubTargetProvisioningIntent{}, err
	}
	var intent GitHubTargetProvisioningIntent
	var scaleSetID, runnerGroupID sql.NullInt64
	err := s.db.QueryRowContext(ctx, `SELECT target_id, installation_id, scope_kind, scope, scale_set_name, profile_id, state, scale_set_id, runner_group_id, error_code, created_at_unix_nano, updated_at_unix_nano FROM github_target_provisioning_intents WHERE target_id=?`, targetID).Scan(&intent.TargetID, &intent.InstallationID, &intent.ScopeKind, &intent.Scope, &intent.ScaleSetName, &intent.ProfileID, &intent.State, &scaleSetID, &runnerGroupID, &intent.ErrorCode, &intent.CreatedAtUnixNano, &intent.UpdatedAtUnixNano)
	if errors.Is(err, sql.ErrNoRows) {
		return GitHubTargetProvisioningIntent{}, ErrGitHubProvisioningIntent
	}
	if err != nil {
		return GitHubTargetProvisioningIntent{}, err
	}
	if scaleSetID.Valid {
		intent.ScaleSetID = &scaleSetID.Int64
	}
	if runnerGroupID.Valid {
		intent.RunnerGroupID = &runnerGroupID.Int64
	}
	return intent, nil
}

func validateProvisioningIntent(intent GitHubTargetProvisioningIntent) error {
	if intent.TargetID == "" || intent.InstallationID <= 0 || (intent.ScopeKind != "repository" && intent.ScopeKind != "organization") || intent.Scope == "" || intent.ScaleSetName == "" || intent.ProfileID == "" {
		return ErrGitHubProvisioningIntent
	}
	return nil
}
