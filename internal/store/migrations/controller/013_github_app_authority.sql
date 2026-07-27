-- Provider identity and external provisioning intent are durable, secret-free
-- records. App private keys and installation tokens remain in the credential
-- store/in-memory request scope; this schema stores only IDs, safe metadata,
-- and a digest-free lifecycle marker for restart reconciliation.
CREATE TABLE github_app_installations (
    installation_id INTEGER PRIMARY KEY CHECK (installation_id > 0),
    account_login TEXT NOT NULL CHECK (length(account_login) > 0),
    account_type TEXT NOT NULL CHECK (account_type IN ('User', 'Organization')),
    repository_selection TEXT NOT NULL CHECK (repository_selection IN ('all', 'selected', 'none')),
    observed_at_unix_nano INTEGER NOT NULL CHECK (observed_at_unix_nano > 0)
);

CREATE TABLE github_target_provisioning_intents (
    target_id TEXT PRIMARY KEY CHECK (length(target_id) > 0),
    installation_id INTEGER NOT NULL CHECK (installation_id > 0),
    scope_kind TEXT NOT NULL CHECK (scope_kind IN ('repository', 'organization')),
    scope TEXT NOT NULL CHECK (length(scope) > 0),
    scale_set_name TEXT NOT NULL CHECK (length(scale_set_name) > 0),
    profile_id TEXT NOT NULL CHECK (length(profile_id) > 0),
    state TEXT NOT NULL CHECK (state IN ('creating', 'ready', 'committed', 'rolled_back', 'ambiguous')),
    scale_set_id INTEGER CHECK (scale_set_id IS NULL OR scale_set_id > 0),
    runner_group_id INTEGER CHECK (runner_group_id IS NULL OR runner_group_id > 0),
    error_code TEXT NOT NULL CHECK (length(error_code) <= 100),
    created_at_unix_nano INTEGER NOT NULL CHECK (created_at_unix_nano > 0),
    updated_at_unix_nano INTEGER NOT NULL CHECK (updated_at_unix_nano > 0)
);

CREATE INDEX github_target_provisioning_intents_state
    ON github_target_provisioning_intents(state);
