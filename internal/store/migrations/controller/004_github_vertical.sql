-- GitHub queue messages are an external event stream, not execution identities.
-- A single message can contain available, assigned, started, and completed
-- events for different runner requests, so the durable replay key is independent
-- from the optional single-slot claim created while committing that message.
CREATE TABLE github_session_demand (
    scale_set_id INTEGER PRIMARY KEY CHECK (scale_set_id > 0),
    session_id TEXT NOT NULL CHECK (length(session_id) > 0),
    total_available_jobs INTEGER NOT NULL CHECK (total_available_jobs >= 0),
    total_acquired_jobs INTEGER NOT NULL CHECK (total_acquired_jobs >= 0),
    total_assigned_jobs INTEGER NOT NULL CHECK (total_assigned_jobs >= 0),
    total_running_jobs INTEGER NOT NULL CHECK (
        total_running_jobs >= 0 AND total_running_jobs <= total_assigned_jobs
    ),
    total_registered_runners INTEGER NOT NULL CHECK (total_registered_runners >= 0),
    total_busy_runners INTEGER NOT NULL CHECK (total_busy_runners >= 0),
    total_idle_runners INTEGER NOT NULL CHECK (total_idle_runners >= 0),
    observed_at_unix_nano INTEGER NOT NULL CHECK (observed_at_unix_nano > 0)
);

CREATE TABLE github_queue_messages (
    scale_set_id INTEGER NOT NULL CHECK (scale_set_id > 0),
    message_id INTEGER NOT NULL CHECK (message_id > 0),
    message_digest TEXT NOT NULL CHECK (
        length(message_digest) = 64 AND message_digest NOT GLOB '*[^0-9a-f]*'
    ),
    committed_at_unix_nano INTEGER NOT NULL CHECK (committed_at_unix_nano > 0),
    PRIMARY KEY (scale_set_id, message_id)
);

CREATE TABLE github_message_jobs (
    scale_set_id INTEGER NOT NULL CHECK (scale_set_id > 0),
    message_id INTEGER NOT NULL CHECK (message_id > 0),
    event_index INTEGER NOT NULL CHECK (event_index >= 0),
    event_type TEXT NOT NULL CHECK (event_type IN (
        'JobAvailable', 'JobAssigned', 'JobStarted', 'JobCompleted'
    )),
    runner_request_id INTEGER NOT NULL CHECK (runner_request_id > 0),
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
        (event_type IN ('JobAvailable', 'JobAssigned') AND runner_id = 0 AND runner_name = '')
        OR
        (event_type IN ('JobStarted', 'JobCompleted') AND runner_id > 0 AND length(runner_name) > 0)
    )
);

CREATE TABLE github_job_claims (
    scale_set_id INTEGER NOT NULL CHECK (scale_set_id > 0),
    runner_request_id INTEGER NOT NULL CHECK (runner_request_id > 0),
    source_message_id INTEGER NOT NULL CHECK (source_message_id > 0),
    execution_id TEXT NOT NULL UNIQUE REFERENCES executions(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    state TEXT NOT NULL CHECK (state IN (
        'pending', 'acquire_ambiguous', 'acquired', 'preparing',
        'prepare_failed', 'jit_intent', 'jit_generation_ambiguous',
        'jit_generated', 'start_dispatching', 'start_ambiguous',
        'running', 'reconciliation_required'
    )),
    current_jit_attempt INTEGER NOT NULL CHECK (current_jit_attempt >= 0),
    created_at_unix_nano INTEGER NOT NULL CHECK (created_at_unix_nano > 0),
    updated_at_unix_nano INTEGER NOT NULL CHECK (updated_at_unix_nano >= created_at_unix_nano),
    PRIMARY KEY (scale_set_id, runner_request_id),
    FOREIGN KEY (scale_set_id, source_message_id)
        REFERENCES github_queue_messages(scale_set_id, message_id)
        ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE INDEX github_job_claims_actionable
    ON github_job_claims(scale_set_id, state, created_at_unix_nano, runner_request_id);

-- Persist only runner registration identity and the digest of the opaque JIT
-- body. The body itself never enters SQLite.
CREATE TABLE github_jit_attempts (
    scale_set_id INTEGER NOT NULL CHECK (scale_set_id > 0),
    runner_request_id INTEGER NOT NULL CHECK (runner_request_id > 0),
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    controller_epoch INTEGER NOT NULL CHECK (controller_epoch > 0),
    runner_name TEXT NOT NULL CHECK (length(runner_name) > 0),
    state TEXT NOT NULL CHECK (state IN (
        'intent', 'generation_ambiguous', 'generated', 'start_dispatching',
        'start_ambiguous', 'started', 'agent_accepted', 'removal_pending',
        'reconciled_absent'
    )),
    runner_id INTEGER CHECK (runner_id > 0),
    jit_digest TEXT CHECK (
        jit_digest IS NULL OR (
            length(jit_digest) = 64 AND jit_digest NOT GLOB '*[^0-9a-f]*'
        )
    ),
    start_command_id TEXT NOT NULL,
    created_at_unix_nano INTEGER NOT NULL CHECK (created_at_unix_nano > 0),
    updated_at_unix_nano INTEGER NOT NULL CHECK (updated_at_unix_nano >= created_at_unix_nano),
    PRIMARY KEY (scale_set_id, runner_request_id, attempt),
    FOREIGN KEY (scale_set_id, runner_request_id)
        REFERENCES github_job_claims(scale_set_id, runner_request_id)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CHECK (
        (state IN ('intent', 'generation_ambiguous', 'reconciled_absent')
            AND runner_id IS NULL AND jit_digest IS NULL AND start_command_id = '')
        OR
        (state IN (
            'generated', 'start_dispatching', 'start_ambiguous', 'started',
            'agent_accepted'
        ) AND runner_id IS NOT NULL AND jit_digest IS NOT NULL
            AND length(start_command_id) > 0)
        OR
        (state = 'removal_pending' AND runner_id IS NOT NULL)
    )
);
