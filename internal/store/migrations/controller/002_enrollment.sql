CREATE TABLE enrollment_tokens (
    token_id BLOB PRIMARY KEY CHECK (length(token_id) = 16),
    secret_digest BLOB NOT NULL CHECK (length(secret_digest) = 32),
    controller_epoch INTEGER NOT NULL CHECK (controller_epoch > 0)
);

CREATE TABLE enrolled_nodes (
    node_id TEXT PRIMARY KEY CHECK (length(node_id) = 32),
    current_serial TEXT NOT NULL CHECK (length(current_serial) > 0),
    credential_epoch INTEGER NOT NULL CHECK (credential_epoch > 0),
    not_before_unix_nano INTEGER NOT NULL,
    not_after_unix_nano INTEGER NOT NULL,
    revoked INTEGER NOT NULL CHECK (revoked IN (0, 1)),
    CHECK (not_after_unix_nano > not_before_unix_nano)
);
