-- GitHub's scale-set protocol drives runner creation from
-- Statistics.TotalAssignedJobs, not from JobAvailable. An assigned job is
-- delivered as JobAssigned with no runner request ID at all, so the whole
-- lifecycle could never be keyed on one: every durable table below used
-- (scale_set_id, runner_request_id) as its identity and enforced positivity.
--
-- The fix separates identity from provider correlation.
--
--   claim_key         SpareRunner-owned durable identity for one runner lifecycle.
--                     Positive when the claim came from a JobAvailable offer, in
--                     which case it is exactly the provider request ID, so every
--                     existing row keeps its identity and every child FK keeps
--                     its value. Negative when the claim came from assigned
--                     demand, allocated by the Controller from a namespace that
--                     is disjoint from GitHub's positive request IDs by sign.
--   runner_request_id Provider correlation only. NULL when GitHub never offered
--                     a request ID. Never synthesized: AcquireJobs and provider
--                     correlation read this column, so a fabricated value could
--                     collide with a real one.
--
-- A synthetic positive key was rejected for exactly that reason, and a nullable
-- identity column was rejected because SQLite does not enforce a composite
-- foreign key whose child columns contain NULL — the five child tables below
-- would have silently lost referential integrity for every assigned-demand row.
--
-- source_message_id becomes nullable for the same reason the feature exists: an
-- empty long poll carries no message, yet the reference listener still
-- re-evaluates desired runner count from the last observed statistics, so a
-- claim can legitimately be created with no owning queue message.

CREATE TABLE github_job_claims_v19 (
    scale_set_id INTEGER NOT NULL CHECK (scale_set_id > 0),
    claim_key INTEGER NOT NULL CHECK (claim_key <> 0),
    origin TEXT NOT NULL CHECK (origin IN ('job_available', 'assigned_demand')),
    runner_request_id INTEGER CHECK (
        runner_request_id IS NULL OR runner_request_id > 0
    ),
    source_message_id INTEGER CHECK (
        source_message_id IS NULL OR source_message_id > 0
    ),
    execution_id TEXT NOT NULL UNIQUE
        REFERENCES executions(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    state TEXT NOT NULL CHECK (state IN (
        'pending', 'acquire_ambiguous', 'acquired', 'preparing',
        'prepare_failed', 'jit_intent', 'jit_generation_ambiguous',
        'jit_generated', 'start_dispatching', 'start_ambiguous',
        'running', 'reconciliation_required'
    )),
    current_jit_attempt INTEGER NOT NULL CHECK (current_jit_attempt >= 0),
    created_at_unix_nano INTEGER NOT NULL CHECK (created_at_unix_nano > 0),
    updated_at_unix_nano INTEGER NOT NULL CHECK (
        updated_at_unix_nano >= created_at_unix_nano
    ),
    PRIMARY KEY (scale_set_id, claim_key),
    -- SQLite treats NULLs as distinct here, so assigned-demand claims coexist
    -- while an offered request ID can still only ever be claimed once.
    UNIQUE (scale_set_id, runner_request_id),
    FOREIGN KEY (scale_set_id, source_message_id)
        REFERENCES github_queue_messages(scale_set_id, message_id)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CHECK (
        (
            origin = 'job_available'
            AND claim_key > 0
            AND runner_request_id = claim_key
            AND source_message_id IS NOT NULL
        )
        OR
        (
            origin = 'assigned_demand'
            AND claim_key < 0
            AND runner_request_id IS NULL
        )
    )
);

INSERT INTO github_job_claims_v19(
    scale_set_id, claim_key, origin, runner_request_id, source_message_id,
    execution_id, state, current_jit_attempt, created_at_unix_nano,
    updated_at_unix_nano
)
SELECT scale_set_id, runner_request_id, 'job_available', runner_request_id,
    source_message_id, execution_id, state, current_jit_attempt,
    created_at_unix_nano, updated_at_unix_nano
FROM github_job_claims;

CREATE TABLE github_jit_attempts_v19 (
    scale_set_id INTEGER NOT NULL CHECK (scale_set_id > 0),
    claim_key INTEGER NOT NULL CHECK (claim_key <> 0),
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
    updated_at_unix_nano INTEGER NOT NULL CHECK (
        updated_at_unix_nano >= created_at_unix_nano
    ),
    PRIMARY KEY (scale_set_id, claim_key, attempt),
    FOREIGN KEY (scale_set_id, claim_key)
        REFERENCES github_job_claims_v19(scale_set_id, claim_key)
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

INSERT INTO github_jit_attempts_v19(
    scale_set_id, claim_key, attempt, controller_epoch, runner_name, state,
    runner_id, jit_digest, start_command_id, created_at_unix_nano,
    updated_at_unix_nano
)
SELECT scale_set_id, runner_request_id, attempt, controller_epoch, runner_name,
    state, runner_id, jit_digest, start_command_id, created_at_unix_nano,
    updated_at_unix_nano
FROM github_jit_attempts;

CREATE TABLE github_acquire_attempts_v19 (
    scale_set_id INTEGER NOT NULL CHECK (scale_set_id > 0),
    -- Acquisition only ever applies to an offered job, so this child keeps the
    -- positive-key invariant its parent relaxed.
    claim_key INTEGER NOT NULL CHECK (claim_key > 0),
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
    PRIMARY KEY (scale_set_id, claim_key, attempt),
    UNIQUE (scale_set_id, claim_key, evidence_message_id),
    FOREIGN KEY (scale_set_id, claim_key)
        REFERENCES github_job_claims_v19(scale_set_id, claim_key)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (scale_set_id, evidence_message_id)
        REFERENCES github_queue_messages(scale_set_id, message_id)
        ON DELETE RESTRICT ON UPDATE RESTRICT
);

INSERT INTO github_acquire_attempts_v19(
    scale_set_id, claim_key, attempt, evidence_message_id, controller_epoch,
    state, created_at_unix_nano, updated_at_unix_nano
)
SELECT scale_set_id, runner_request_id, attempt, evidence_message_id,
    controller_epoch, state, created_at_unix_nano, updated_at_unix_nano
FROM github_acquire_attempts;

CREATE TABLE github_jit_snapshot_authority_v19 (
    scale_set_id INTEGER NOT NULL CHECK (scale_set_id > 0),
    claim_key INTEGER NOT NULL CHECK (claim_key <> 0),
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
    PRIMARY KEY (scale_set_id, claim_key, attempt),
    FOREIGN KEY (scale_set_id, claim_key, attempt)
        REFERENCES github_jit_attempts_v19(scale_set_id, claim_key, attempt)
        ON DELETE RESTRICT ON UPDATE RESTRICT
);

INSERT INTO github_jit_snapshot_authority_v19(
    scale_set_id, claim_key, attempt, snapshot_digest, controller_epoch,
    decision, updated_at_unix_nano, github_session_generation
)
SELECT scale_set_id, runner_request_id, attempt, snapshot_digest,
    controller_epoch, decision, updated_at_unix_nano,
    github_session_generation
FROM github_jit_snapshot_authority;

CREATE TABLE github_unpicked_requeue_intents_v19 (
    scale_set_id INTEGER NOT NULL CHECK (scale_set_id > 0),
    claim_key INTEGER NOT NULL CHECK (claim_key <> 0),
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
    PRIMARY KEY (scale_set_id, claim_key),
    FOREIGN KEY (scale_set_id, claim_key)
        REFERENCES github_job_claims_v19(scale_set_id, claim_key)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (scale_set_id, claim_key, jit_attempt)
        REFERENCES github_jit_attempts_v19(scale_set_id, claim_key, attempt)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (scale_set_id, source_message_id, source_event_index)
        REFERENCES github_message_jobs(scale_set_id, message_id, event_index)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (old_execution_id)
        REFERENCES executions(id)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CHECK (old_execution_id <> replacement_execution_id)
);

INSERT INTO github_unpicked_requeue_intents_v19(
    scale_set_id, claim_key, jit_attempt, old_execution_id,
    replacement_execution_id, source_message_id, source_event_index,
    controller_epoch, created_at_unix_nano, updated_at_unix_nano
)
SELECT scale_set_id, runner_request_id, jit_attempt, old_execution_id,
    replacement_execution_id, source_message_id, source_event_index,
    controller_epoch, created_at_unix_nano, updated_at_unix_nano
FROM github_unpicked_requeue_intents;

DROP TABLE github_unpicked_requeue_intents;
DROP TABLE github_jit_snapshot_authority;
DROP TABLE github_acquire_attempts;
DROP TABLE github_jit_attempts;
DROP TABLE github_job_claims;

ALTER TABLE github_job_claims_v19 RENAME TO github_job_claims;
ALTER TABLE github_jit_attempts_v19 RENAME TO github_jit_attempts;
ALTER TABLE github_acquire_attempts_v19 RENAME TO github_acquire_attempts;
ALTER TABLE github_jit_snapshot_authority_v19
    RENAME TO github_jit_snapshot_authority;
ALTER TABLE github_unpicked_requeue_intents_v19
    RENAME TO github_unpicked_requeue_intents;

CREATE INDEX github_job_claims_actionable
    ON github_job_claims(scale_set_id, state, created_at_unix_nano, claim_key);

-- A zero-request lifecycle event may fall back to runner identity only when
-- that provider identity names exactly one durable JIT attempt.
CREATE UNIQUE INDEX github_jit_attempts_provider_runner_identity
    ON github_jit_attempts(scale_set_id, runner_id)
    WHERE runner_id IS NOT NULL;
