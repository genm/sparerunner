-- GitHub lifecycle messages can omit runnerRequestId while retaining the exact
-- provider runner ID and name. Availability and assignment still require the
-- request identity used by AcquireJobs.
CREATE TABLE github_message_jobs_v10 (
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
            event_type IN ('JobAvailable', 'JobAssigned')
            AND runner_request_id > 0
            AND runner_id = 0
            AND runner_name = ''
        )
        OR
        (
            event_type IN ('JobStarted', 'JobCompleted')
            AND runner_id > 0
            AND length(runner_name) > 0
        )
    )
);

INSERT INTO github_message_jobs_v10(
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
CREATE TABLE github_unpicked_requeue_intents_v10 (
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
        REFERENCES github_message_jobs_v10(scale_set_id, message_id, event_index)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (old_execution_id)
        REFERENCES executions(id)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CHECK (old_execution_id <> replacement_execution_id)
);

INSERT INTO github_unpicked_requeue_intents_v10(
    scale_set_id, runner_request_id, jit_attempt, old_execution_id,
    replacement_execution_id, source_message_id, source_event_index,
    controller_epoch, created_at_unix_nano, updated_at_unix_nano
)
SELECT scale_set_id, runner_request_id, jit_attempt, old_execution_id,
    replacement_execution_id, source_message_id, source_event_index,
    controller_epoch, created_at_unix_nano, updated_at_unix_nano
FROM github_unpicked_requeue_intents;

-- `reconciled_pending` is created only after the ACKed replacement
-- availability and the provider absence fences have both been consumed. It
-- distinguishes restart-safe immediate dispatch from a normal pre-ACK pending
-- claim, which must still poll for redelivery first.
CREATE TABLE github_acquire_attempts_v10 (
    scale_set_id INTEGER NOT NULL CHECK (scale_set_id > 0),
    runner_request_id INTEGER NOT NULL CHECK (runner_request_id > 0),
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    evidence_message_id INTEGER NOT NULL CHECK (evidence_message_id > 0),
    controller_epoch INTEGER NOT NULL CHECK (controller_epoch > 0),
    state TEXT NOT NULL CHECK (state IN (
        'pending', 'reconciled_pending', 'dispatching', 'acquired'
    )),
    created_at_unix_nano INTEGER NOT NULL CHECK (created_at_unix_nano > 0),
    updated_at_unix_nano INTEGER NOT NULL CHECK (
        updated_at_unix_nano >= created_at_unix_nano
    ),
    PRIMARY KEY (scale_set_id, runner_request_id, attempt),
    UNIQUE (scale_set_id, runner_request_id, evidence_message_id),
    FOREIGN KEY (scale_set_id, runner_request_id)
        REFERENCES github_job_claims(scale_set_id, runner_request_id)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (scale_set_id, evidence_message_id)
        REFERENCES github_queue_messages(scale_set_id, message_id)
        ON DELETE RESTRICT ON UPDATE RESTRICT
);

INSERT INTO github_acquire_attempts_v10(
    scale_set_id, runner_request_id, attempt, evidence_message_id,
    controller_epoch, state, created_at_unix_nano, updated_at_unix_nano
)
SELECT scale_set_id, runner_request_id, attempt, evidence_message_id,
    controller_epoch, state, created_at_unix_nano, updated_at_unix_nano
FROM github_acquire_attempts;

DROP TABLE github_unpicked_requeue_intents;
DROP TABLE github_message_jobs;
DROP TABLE github_acquire_attempts;

ALTER TABLE github_message_jobs_v10 RENAME TO github_message_jobs;
ALTER TABLE github_unpicked_requeue_intents_v10
    RENAME TO github_unpicked_requeue_intents;
ALTER TABLE github_acquire_attempts_v10 RENAME TO github_acquire_attempts;

-- A zero-request lifecycle event may fall back to runner identity only when
-- that provider identity names exactly one durable JIT attempt.
CREATE UNIQUE INDEX github_jit_attempts_provider_runner_identity
    ON github_jit_attempts(scale_set_id, runner_id)
    WHERE runner_id IS NOT NULL;
