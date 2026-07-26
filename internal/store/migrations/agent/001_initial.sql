CREATE TABLE store_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT INTO store_metadata (key, value) VALUES ('role', 'agent');

CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at_unix_nano INTEGER NOT NULL
);

CREATE TABLE command_replays (
    command_id TEXT PRIMARY KEY,
    controller_epoch INTEGER NOT NULL,
    execution_id TEXT NOT NULL,
    expected_state TEXT NOT NULL,
    payload_digest TEXT NOT NULL
);

CREATE TABLE execution_observations (
    execution_id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    observed_at_unix_nano INTEGER NOT NULL
);

CREATE TABLE cleanup_tombstones (
    execution_id TEXT PRIMARY KEY,
    reason TEXT NOT NULL,
    recorded_at_unix_nano INTEGER NOT NULL
);
