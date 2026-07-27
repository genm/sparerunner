-- The full authenticated Agent snapshot is the compare-and-swap authority for
-- actions derived from presence or absence. Historical observation rows remain
-- useful for audit, but only this current digest may authorize a mutation.
CREATE TABLE agent_snapshot_authority (
    node_id TEXT PRIMARY KEY
        REFERENCES enrolled_nodes(node_id) ON DELETE CASCADE,
    revision INTEGER NOT NULL CHECK (revision > 0),
    snapshot_digest TEXT NOT NULL CHECK (
        length(snapshot_digest) = 64
        AND snapshot_digest NOT GLOB '*[^0-9a-f]*'
    ),
    accepted_by_controller_epoch INTEGER NOT NULL
        CHECK (accepted_by_controller_epoch > 0),
    committed_at_unix_nano INTEGER NOT NULL
        CHECK (committed_at_unix_nano > 0)
);

-- Historical snapshot rows remain append/update audit evidence. These tables
-- are the exact membership of the latest committed full snapshot and are
-- replaced transactionally whenever its digest advances.
CREATE TABLE agent_current_snapshot_commands (
    node_id TEXT NOT NULL,
    command_id TEXT NOT NULL,
    snapshot_digest TEXT NOT NULL CHECK (
        length(snapshot_digest) = 64
        AND snapshot_digest NOT GLOB '*[^0-9a-f]*'
    ),
    PRIMARY KEY (node_id, command_id),
    FOREIGN KEY (node_id, command_id)
        REFERENCES agent_snapshot_commands(node_id, command_id)
        ON DELETE CASCADE ON UPDATE RESTRICT
);

CREATE TABLE agent_current_snapshot_observations (
    node_id TEXT NOT NULL,
    execution_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN (
        'preparing', 'running', 'cleaning', 'released', 'failed',
        'cleanup_failed', 'quarantined'
    )),
    agent_observed_at_unix_nano INTEGER NOT NULL
        CHECK (agent_observed_at_unix_nano > 0),
    snapshot_digest TEXT NOT NULL CHECK (
        length(snapshot_digest) = 64
        AND snapshot_digest NOT GLOB '*[^0-9a-f]*'
    ),
    PRIMARY KEY (node_id, execution_id),
    FOREIGN KEY (node_id, execution_id)
        REFERENCES agent_snapshot_observations(node_id, execution_id)
        ON DELETE CASCADE ON UPDATE RESTRICT
);

CREATE TABLE agent_current_snapshot_tombstones (
    node_id TEXT NOT NULL,
    execution_id TEXT NOT NULL,
    failure_code TEXT NOT NULL CHECK (failure_code IN (
        'cleanup_verification_failed', 'process_residue',
        'workspace_removal_failed'
    )),
    agent_recorded_at_unix_nano INTEGER NOT NULL
        CHECK (agent_recorded_at_unix_nano > 0),
    snapshot_digest TEXT NOT NULL CHECK (
        length(snapshot_digest) = 64
        AND snapshot_digest NOT GLOB '*[^0-9a-f]*'
    ),
    PRIMARY KEY (node_id, execution_id),
    FOREIGN KEY (node_id, execution_id)
        REFERENCES agent_snapshot_cleanup_tombstones(node_id, execution_id)
        ON DELETE CASCADE ON UPDATE RESTRICT
);

-- Reconciliation teardown commands are Controller-authorized recovery actions,
-- not ordinary desired-state transitions. The marker lets execution-update
-- commits distinguish an already-terminal desired execution from a normal
-- active slot without weakening the ordinary command precondition.
CREATE TABLE reconciliation_agent_commands (
    command_id TEXT PRIMARY KEY
        REFERENCES agent_commands(command_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    snapshot_digest TEXT NOT NULL CHECK (
        length(snapshot_digest) = 64
        AND snapshot_digest NOT GLOB '*[^0-9a-f]*'
    )
);

-- AcquireJobs is an external write whose response can be lost. Each network
-- call therefore has a durable source message and Controller epoch token. A
-- newly committed (not replayed) JobAvailable message may create the next
-- pending attempt, but no message replay or other event can re-arm acquisition.
CREATE TABLE github_acquire_attempts (
    scale_set_id INTEGER NOT NULL CHECK (scale_set_id > 0),
    runner_request_id INTEGER NOT NULL CHECK (runner_request_id > 0),
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    evidence_message_id INTEGER NOT NULL CHECK (evidence_message_id > 0),
    controller_epoch INTEGER NOT NULL CHECK (controller_epoch > 0),
    state TEXT NOT NULL CHECK (state IN (
        'pending', 'dispatching', 'acquired'
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

-- Provider reconciliation may remove or clear a registration only while the
-- same current authenticated Agent snapshot remains the authority for the
-- decision. That decision may prove the exact Start was unaccepted, or that an
-- accepted Start ended locally without any durable Running/Cleaning evidence.
-- A newer safe snapshot may replace this authority, but historical rows never
-- can authorize a destructive provider operation.
CREATE TABLE github_jit_snapshot_authority (
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
        'lost_jit_removal_issued', 'lost_jit_absence_pending'
    )),
    updated_at_unix_nano INTEGER NOT NULL CHECK (
        updated_at_unix_nano > 0
    ),
    PRIMARY KEY (scale_set_id, runner_request_id, attempt),
    FOREIGN KEY (scale_set_id, runner_request_id, attempt)
        REFERENCES github_jit_attempts(
            scale_set_id, runner_request_id, attempt
        )
        ON DELETE RESTRICT ON UPDATE RESTRICT
);
