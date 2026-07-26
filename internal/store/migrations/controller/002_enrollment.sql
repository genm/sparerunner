CREATE TABLE enrollment_tokens (
    token_id BLOB PRIMARY KEY CHECK (length(token_id) = 16 AND token_id != zeroblob(16)),
    secret_digest BLOB NOT NULL CHECK (length(secret_digest) = 32 AND secret_digest != zeroblob(32)),
    controller_epoch INTEGER NOT NULL CHECK (controller_epoch > 0)
);

CREATE TABLE enrolled_nodes (
    node_id TEXT PRIMARY KEY CHECK (length(node_id) = 32 AND node_id NOT GLOB '*[^0-9a-f]*'),
    current_serial TEXT NOT NULL CHECK (length(current_serial) > 0 AND current_serial NOT GLOB '*[^0-9a-f]*'),
    credential_epoch INTEGER NOT NULL CHECK (credential_epoch > 0),
    not_before_unix_nano INTEGER NOT NULL,
    not_after_unix_nano INTEGER NOT NULL,
    revoked INTEGER NOT NULL CHECK (revoked IN (0, 1)),
    enrollment_token_id BLOB NOT NULL UNIQUE CHECK (length(enrollment_token_id) = 16),
    enrollment_secret_digest BLOB NOT NULL CHECK (length(enrollment_secret_digest) = 32),
    public_key_digest BLOB NOT NULL CHECK (length(public_key_digest) = 32),
    certificate_der BLOB NOT NULL CHECK (length(certificate_der) > 0),
    ca_der BLOB NOT NULL CHECK (length(ca_der) > 0),
    CHECK (not_after_unix_nano > not_before_unix_nano)
);
