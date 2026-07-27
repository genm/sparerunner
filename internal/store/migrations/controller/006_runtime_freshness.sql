-- Runner profiles are the durable authority for update behavior. The revision
-- provides optimistic configuration identity without persisting any package URL
-- or credential-bearing provider data.
CREATE TABLE runner_profile_update_policies (
    profile_id TEXT PRIMARY KEY CHECK (length(profile_id) > 0),
    version_policy TEXT NOT NULL CHECK (version_policy IN ('auto_update', 'pinned')),
    runner_version TEXT NOT NULL DEFAULT '',
    revision INTEGER NOT NULL CHECK (revision > 0)
);

-- A target may be bound exactly once, and a provider scale set may not silently
-- route two targets. Replaying the same mapping is handled by the store API;
-- every differing mapping is a configuration conflict.
CREATE TABLE github_target_runtime_bindings (
    target_id TEXT PRIMARY KEY CHECK (length(target_id) > 0),
    scale_set_id INTEGER NOT NULL UNIQUE CHECK (scale_set_id > 0),
    profile_id TEXT NOT NULL REFERENCES runner_profile_update_policies(profile_id)
        ON DELETE RESTRICT ON UPDATE RESTRICT
);

-- There is one global GitHub Actions runner release feed. A failed fetch keeps
-- the last successful observation intact and records only an allowlisted class,
-- never the raw provider error or response body.
CREATE TABLE github_runner_release_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    latest_version TEXT,
    latest_released_at_unix_nano INTEGER,
    observed_at_unix_nano INTEGER,
    freshness TEXT NOT NULL CHECK (freshness IN ('unknown', 'fresh', 'stale')),
    failure_class TEXT NOT NULL CHECK (failure_class IN (
        '', 'timeout', 'network', 'provider_auth', 'provider_429',
        'provider_5xx', 'invalid_response'
    )),
    failure_at_unix_nano INTEGER,
    generation INTEGER NOT NULL CHECK (generation > 0),
    CHECK (
        (freshness = 'unknown'
            AND latest_version IS NULL AND latest_released_at_unix_nano IS NULL
            AND observed_at_unix_nano IS NULL
            AND length(failure_class) > 0 AND failure_at_unix_nano > 0)
        OR
        (freshness = 'fresh'
            AND length(latest_version) > 0 AND latest_released_at_unix_nano > 0
            AND observed_at_unix_nano >= latest_released_at_unix_nano
            AND failure_class = '' AND failure_at_unix_nano IS NULL)
        OR
        (freshness = 'stale'
            AND length(latest_version) > 0 AND latest_released_at_unix_nano > 0
            AND observed_at_unix_nano >= latest_released_at_unix_nano
            AND length(failure_class) > 0 AND failure_at_unix_nano > 0)
    )
);

-- Health is per scale set. `unknown` is intentionally distinct from a healthy
-- empty value: only a successful session observation can make it fresh.
CREATE TABLE github_scale_set_session_health (
    scale_set_id INTEGER PRIMARY KEY CHECK (scale_set_id > 0),
    freshness TEXT NOT NULL CHECK (freshness IN ('unknown', 'fresh', 'stale')),
    last_success_at_unix_nano INTEGER,
    failure_class TEXT NOT NULL CHECK (failure_class IN (
        '', 'timeout', 'network', 'provider_auth', 'provider_429',
        'provider_5xx', 'invalid_response'
    )),
    failure_at_unix_nano INTEGER,
    transition_generation INTEGER NOT NULL CHECK (transition_generation > 0),
    CHECK (
        (freshness = 'unknown'
            AND last_success_at_unix_nano IS NULL
            AND length(failure_class) > 0 AND failure_at_unix_nano > 0)
        OR
        (freshness = 'fresh'
            AND last_success_at_unix_nano > 0
            AND failure_class = '' AND failure_at_unix_nano IS NULL)
        OR
        (freshness = 'stale'
            AND last_success_at_unix_nano > 0
            AND length(failure_class) > 0 AND failure_at_unix_nano > 0)
    )
);

-- Existing agent snapshots predate runner version observations. Preserve their
-- meaning while making the absence explicit until the agent protocol supplies it.
ALTER TABLE agent_session_snapshots
    ADD COLUMN runner_version TEXT NOT NULL DEFAULT '';
