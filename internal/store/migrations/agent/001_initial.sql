CREATE TABLE store_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT INTO store_metadata (key, value) VALUES ('role', 'agent');
INSERT INTO store_metadata (key, value) VALUES ('max_controller_epoch', '0');

CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY CHECK (version > 0),
    applied_at_unix_nano INTEGER NOT NULL CHECK (applied_at_unix_nano > 0)
);

CREATE TABLE command_replays (
    command_id TEXT PRIMARY KEY CHECK (length(command_id) > 0),
    controller_epoch INTEGER NOT NULL CHECK (controller_epoch > 0),
    execution_id TEXT NOT NULL CHECK (length(execution_id) > 0),
    expected_state TEXT NOT NULL CHECK (expected_state IN ('pending', 'reserved', 'preparing', 'running', 'cleaning', 'released', 'failed', 'cleanup_failed', 'quarantined')),
    payload_digest TEXT NOT NULL CHECK (length(payload_digest) = 64 AND payload_digest NOT GLOB '*[^0-9a-f]*')
);

CREATE TABLE execution_observations (
    execution_id TEXT PRIMARY KEY CHECK (length(execution_id) > 0),
    state TEXT NOT NULL CHECK (state IN ('pending', 'reserved', 'preparing', 'running', 'cleaning', 'released', 'failed', 'cleanup_failed', 'quarantined')),
    observed_at_unix_nano INTEGER NOT NULL CHECK (observed_at_unix_nano > 0)
);

CREATE TABLE cleanup_tombstones (
    execution_id TEXT PRIMARY KEY CHECK (length(execution_id) > 0),
    failure_code TEXT NOT NULL CHECK (failure_code IN ('cleanup_verification_failed', 'process_residue', 'workspace_removal_failed')),
    recorded_at_unix_nano INTEGER NOT NULL CHECK (recorded_at_unix_nano > 0)
);
