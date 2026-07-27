-- A fresh JobAvailable message can prove that a runner which exited before
-- pickup must be replaced, but the old GitHub registration must be removed
-- before the replacement is allowed to acquire the job. Keep that authority
-- durable across acknowledgement and Controller restart.
CREATE TABLE github_jit_snapshot_authority_v9 (
    scale_set_id INTEGER NOT NULL CHECK (scale_set_id > 0),
    runner_request_id INTEGER NOT NULL CHECK (runner_request_id > 0),
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    snapshot_digest TEXT NOT NULL CHECK (
        length(snapshot_digest) = 64
        AND snapshot_digest NOT GLOB '*[^0-9a-f]*'
    ),
    controller_epoch INTEGER NOT NULL CHECK (controller_epoch > 0),
    decision TEXT NOT NULL CHECK (decision IN (
        'agent_accepted', 'generation_absence_pending',
        'runner_removal_issued', 'runner_absence_pending',
        'lost_jit_removal_issued', 'lost_jit_absence_pending',
        'unpicked_requeue_removal_issued',
        'unpicked_requeue_absence_pending'
    )),
    updated_at_unix_nano INTEGER NOT NULL CHECK (
        updated_at_unix_nano > 0
    ),
    github_session_generation INTEGER NOT NULL
        CHECK (github_session_generation >= 0),
    PRIMARY KEY (scale_set_id, runner_request_id, attempt),
    FOREIGN KEY (scale_set_id, runner_request_id, attempt)
        REFERENCES github_jit_attempts(
            scale_set_id, runner_request_id, attempt
        )
        ON DELETE RESTRICT ON UPDATE RESTRICT
);

INSERT INTO github_jit_snapshot_authority_v9(
    scale_set_id, runner_request_id, attempt, snapshot_digest,
    controller_epoch, decision, updated_at_unix_nano,
    github_session_generation
)
SELECT scale_set_id, runner_request_id, attempt, snapshot_digest,
    controller_epoch, decision, updated_at_unix_nano,
    github_session_generation
FROM github_jit_snapshot_authority;

DROP TABLE github_jit_snapshot_authority;
ALTER TABLE github_jit_snapshot_authority_v9
    RENAME TO github_jit_snapshot_authority;

CREATE TABLE github_unpicked_requeue_intents (
    scale_set_id INTEGER NOT NULL CHECK (scale_set_id > 0),
    runner_request_id INTEGER NOT NULL CHECK (runner_request_id > 0),
    jit_attempt INTEGER NOT NULL CHECK (jit_attempt > 0),
    old_execution_id TEXT NOT NULL,
    replacement_execution_id TEXT NOT NULL UNIQUE
        CHECK (length(replacement_execution_id) > 0),
    source_message_id INTEGER NOT NULL CHECK (source_message_id > 0),
    source_event_index INTEGER NOT NULL CHECK (source_event_index >= 0),
    controller_epoch INTEGER NOT NULL CHECK (controller_epoch > 0),
    created_at_unix_nano INTEGER NOT NULL CHECK (created_at_unix_nano > 0),
    updated_at_unix_nano INTEGER NOT NULL CHECK (
        updated_at_unix_nano >= created_at_unix_nano
    ),
    PRIMARY KEY (scale_set_id, runner_request_id),
    FOREIGN KEY (scale_set_id, runner_request_id)
        REFERENCES github_job_claims(scale_set_id, runner_request_id)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (scale_set_id, runner_request_id, jit_attempt)
        REFERENCES github_jit_attempts(scale_set_id, runner_request_id, attempt)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (scale_set_id, source_message_id, source_event_index)
        REFERENCES github_message_jobs(scale_set_id, message_id, event_index)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (old_execution_id)
        REFERENCES executions(id)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CHECK (old_execution_id <> replacement_execution_id)
);
