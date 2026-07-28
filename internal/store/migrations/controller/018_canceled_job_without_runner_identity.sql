-- A job canceled before any runner ever picked it up carries no runner identity
-- at all, and live GitHub sends exactly that shape. The v17 CHECK still demanded
-- a runner ID and name for every JobCompleted, so a cancellation could never be
-- committed or acknowledged and blocked the whole queue behind it.
--
-- Accepting it cannot weaken pickup proof: that query matches only
-- succeeded/failed completions against an exact non-zero runner ID and name, so
-- an identity-less canceled completion can never satisfy it. A completion that
-- carries partial identity, or a succeeded/failed one without identity, stays
-- rejected.

CREATE TABLE github_message_jobs_v18 (
    scale_set_id INTEGER NOT NULL CHECK (scale_set_id > 0),
    message_id INTEGER NOT NULL CHECK (message_id > 0),
    event_index INTEGER NOT NULL CHECK (event_index >= 0),
    event_type TEXT NOT NULL CHECK (event_type IN (
        'JobAvailable', 'JobAssigned', 'JobStarted', 'JobCompleted'
    )),
    runner_request_id INTEGER NOT NULL CHECK (runner_request_id >= 0),
    runner_id INTEGER NOT NULL CHECK (runner_id >= 0),
    runner_name TEXT NOT NULL,
    result TEXT NOT NULL,
    repository_name TEXT NOT NULL,
    owner_name TEXT NOT NULL,
    job_id TEXT NOT NULL,
    workflow_run_id INTEGER NOT NULL CHECK (workflow_run_id >= 0),
    PRIMARY KEY (scale_set_id, message_id, event_index),
    FOREIGN KEY (scale_set_id, message_id)
        REFERENCES github_queue_messages(scale_set_id, message_id)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CHECK (
        (
            event_type = 'JobAvailable'
            AND runner_request_id > 0
            AND runner_id = 0
            AND runner_name = ''
        )
        OR
        (
            event_type = 'JobAssigned'
            AND runner_id = 0
            AND runner_name = ''
        )
        OR
        (
            event_type IN ('JobStarted', 'JobCompleted')
            AND runner_id > 0
            AND length(runner_name) > 0
        )
        OR
        (
            event_type = 'JobCompleted'
            AND result = 'canceled'
            AND runner_id = 0
            AND runner_name = ''
        )
    )
);

INSERT INTO github_message_jobs_v18(
    scale_set_id, message_id, event_index, event_type, runner_request_id,
    runner_id, runner_name, result, repository_name, owner_name, job_id,
    workflow_run_id
)
SELECT scale_set_id, message_id, event_index, event_type, runner_request_id,
    runner_id, runner_name, result, repository_name, owner_name, job_id,
    workflow_run_id
FROM github_message_jobs;

-- Rebuild the only child of github_message_jobs alongside it so foreign-key
-- enforcement remains enabled for the whole transactional migration.
CREATE TABLE github_unpicked_requeue_intents_v18 (
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
        REFERENCES github_message_jobs_v18(scale_set_id, message_id, event_index)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (old_execution_id)
        REFERENCES executions(id)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CHECK (old_execution_id <> replacement_execution_id)
);

INSERT INTO github_unpicked_requeue_intents_v18(
    scale_set_id, runner_request_id, jit_attempt, old_execution_id,
    replacement_execution_id, source_message_id, source_event_index,
    controller_epoch, created_at_unix_nano, updated_at_unix_nano
)
SELECT scale_set_id, runner_request_id, jit_attempt, old_execution_id,
    replacement_execution_id, source_message_id, source_event_index,
    controller_epoch, created_at_unix_nano, updated_at_unix_nano
FROM github_unpicked_requeue_intents;

DROP TABLE github_unpicked_requeue_intents;
DROP TABLE github_message_jobs;

ALTER TABLE github_message_jobs_v18 RENAME TO github_message_jobs;
ALTER TABLE github_unpicked_requeue_intents_v18
    RENAME TO github_unpicked_requeue_intents;
