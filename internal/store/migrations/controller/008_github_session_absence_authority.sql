-- Provider absence confirmation is valid only while the same scale-set session
-- transition generation remains current. Existing in-flight rows receive zero,
-- which cannot authorize a later provider observation and is replaced on retry.
ALTER TABLE github_jit_snapshot_authority
    ADD COLUMN github_session_generation INTEGER NOT NULL DEFAULT 0
    CHECK (github_session_generation >= 0);
