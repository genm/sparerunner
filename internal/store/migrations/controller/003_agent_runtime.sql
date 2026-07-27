-- Executions are immutable historical identities after completion, while slot
-- reservations are active leases. The lease points at its execution so a
-- terminal execution can remain after the lease is safely released.
CREATE TABLE executions_v3 (
    id TEXT PRIMARY KEY CHECK (length(id) > 0),
    target_id TEXT NOT NULL CHECK (length(target_id) > 0),
    node_id TEXT NOT NULL CHECK (length(node_id) > 0),
    slot_index INTEGER NOT NULL CHECK (slot_index >= 0),
    state TEXT NOT NULL CHECK (state IN (
        'reserved', 'preparing', 'running', 'cleaning', 'released', 'failed',
        'cleanup_failed', 'quarantined'
    )),
    created_at_unix_nano INTEGER NOT NULL CHECK (created_at_unix_nano > 0),
    UNIQUE (id, node_id),
    UNIQUE (id, node_id, slot_index, target_id)
);

INSERT INTO executions_v3(id, target_id, node_id, slot_index, state, created_at_unix_nano)
SELECT id, target_id, node_id, slot_index, state, created_at_unix_nano
FROM executions;

CREATE TABLE slot_reservations_v3 (
    node_id TEXT NOT NULL CHECK (length(node_id) > 0),
    slot_index INTEGER NOT NULL CHECK (slot_index >= 0),
    target_id TEXT NOT NULL CHECK (length(target_id) > 0),
    execution_id TEXT NOT NULL UNIQUE CHECK (length(execution_id) > 0),
    PRIMARY KEY (node_id, slot_index),
    FOREIGN KEY (execution_id, node_id, slot_index, target_id)
        REFERENCES executions_v3(id, node_id, slot_index, target_id)
        ON DELETE RESTRICT ON UPDATE RESTRICT
);

INSERT INTO slot_reservations_v3(node_id, slot_index, target_id, execution_id)
SELECT node_id, slot_index, target_id, execution_id
FROM slot_reservations;

CREATE TABLE processed_messages_v3 (
    scale_set_id INTEGER NOT NULL CHECK (scale_set_id > 0),
    message_id INTEGER NOT NULL CHECK (message_id > 0),
    message_digest TEXT NOT NULL CHECK (
        length(message_digest) = 64 AND message_digest NOT GLOB '*[^0-9a-f]*'
    ),
    execution_id TEXT NOT NULL UNIQUE CHECK (length(execution_id) > 0),
    created_at_unix_nano INTEGER NOT NULL CHECK (created_at_unix_nano > 0),
    PRIMARY KEY (scale_set_id, message_id),
    FOREIGN KEY (execution_id) REFERENCES executions_v3(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

INSERT INTO processed_messages_v3(scale_set_id, message_id, message_digest, execution_id, created_at_unix_nano)
SELECT scale_set_id, message_id, message_digest, execution_id, created_at_unix_nano
FROM processed_messages;

DROP TABLE processed_messages;
DROP TABLE executions;
DROP TABLE slot_reservations;
ALTER TABLE executions_v3 RENAME TO executions;
ALTER TABLE slot_reservations_v3 RENAME TO slot_reservations;
ALTER TABLE processed_messages_v3 RENAME TO processed_messages;

-- An active execution owns its slot until the same transaction has durably
-- recorded a terminal state. This prevents manual or accidental lease deletion
-- from making capacity appear free while a runner can still exist.
CREATE TRIGGER active_execution_reservation_guard
BEFORE DELETE ON slot_reservations
WHEN EXISTS (
    SELECT 1
    FROM executions
    WHERE id = OLD.execution_id
      AND state NOT IN ('released', 'failed')
)
BEGIN
    SELECT RAISE(ABORT, 'active execution reservation cannot be deleted');
END;

-- Administrative state is Controller authority, distinct from the Agent's
-- observed online/stale state and from credential revocation. Enrollment and
-- revocation triggers keep legacy credential mutations on the same invariant.
CREATE TABLE node_administrative_states (
    node_id TEXT PRIMARY KEY REFERENCES enrolled_nodes(node_id) ON DELETE CASCADE,
    administrative_state TEXT NOT NULL CHECK (administrative_state IN (
        'active', 'draining', 'quarantined', 'revoked'
    ))
);

INSERT INTO node_administrative_states(node_id, administrative_state)
SELECT node_id, CASE revoked WHEN 1 THEN 'revoked' ELSE 'active' END
FROM enrolled_nodes;

CREATE TRIGGER enrolled_node_administrative_state
AFTER INSERT ON enrolled_nodes
BEGIN
    INSERT INTO node_administrative_states(node_id, administrative_state)
    VALUES (NEW.node_id, CASE NEW.revoked WHEN 1 THEN 'revoked' ELSE 'active' END);
END;

CREATE TRIGGER enrolled_node_administrative_revocation
AFTER UPDATE OF revoked ON enrolled_nodes
WHEN NEW.revoked = 1
BEGIN
    UPDATE node_administrative_states
    SET administrative_state = 'revoked'
    WHERE node_id = NEW.node_id;
END;

-- Controller-issued commands contain only metadata and an exact digest of the
-- authenticated type+payload tuple. Secret-bearing payload bodies are excluded.
CREATE TABLE agent_commands (
    command_id TEXT PRIMARY KEY CHECK (length(command_id) > 0),
    node_id TEXT NOT NULL REFERENCES enrolled_nodes(node_id) ON DELETE RESTRICT,
    command_type TEXT NOT NULL CHECK (command_type IN ('prepare', 'start', 'cancel')),
    controller_epoch INTEGER NOT NULL CHECK (controller_epoch > 0),
    execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE RESTRICT,
    expected_state TEXT NOT NULL CHECK (expected_state IN (
        'reserved', 'preparing', 'running', 'cleaning'
    )),
    payload_digest TEXT NOT NULL CHECK (
        length(payload_digest) = 64 AND payload_digest NOT GLOB '*[^0-9a-f]*'
    ),
    issued_at_unix_nano INTEGER NOT NULL CHECK (issued_at_unix_nano > 0),
    UNIQUE (
        node_id, command_id, controller_epoch, execution_id,
        expected_state, payload_digest
    ),
    UNIQUE (command_id, node_id, execution_id)
);

CREATE TABLE agent_session_snapshots (
    node_id TEXT PRIMARY KEY REFERENCES enrolled_nodes(node_id) ON DELETE CASCADE,
    operating_system TEXT NOT NULL CHECK (operating_system IN ('linux', 'macos', 'windows')),
    architecture TEXT NOT NULL CHECK (architecture IN ('amd64', 'arm64')),
    native_runner_ready INTEGER NOT NULL CHECK (native_runner_ready IN (0, 1)),
    max_controller_epoch INTEGER NOT NULL CHECK (max_controller_epoch >= 0),
    received_at_unix_nano INTEGER NOT NULL CHECK (received_at_unix_nano > 0)
);

CREATE TABLE agent_snapshot_commands (
    node_id TEXT NOT NULL REFERENCES enrolled_nodes(node_id) ON DELETE CASCADE,
    command_id TEXT NOT NULL CHECK (length(command_id) > 0),
    controller_epoch INTEGER NOT NULL CHECK (controller_epoch > 0),
    execution_id TEXT NOT NULL CHECK (length(execution_id) > 0),
    expected_state TEXT NOT NULL CHECK (expected_state IN (
        'reserved', 'preparing', 'running', 'cleaning'
    )),
    payload_digest TEXT NOT NULL CHECK (
        length(payload_digest) = 64 AND payload_digest NOT GLOB '*[^0-9a-f]*'
    ),
    PRIMARY KEY (node_id, command_id),
    FOREIGN KEY (
        node_id, command_id, controller_epoch, execution_id,
        expected_state, payload_digest
    ) REFERENCES agent_commands(
        node_id, command_id, controller_epoch, execution_id,
        expected_state, payload_digest
    ) ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE TABLE agent_snapshot_observations (
    node_id TEXT NOT NULL REFERENCES enrolled_nodes(node_id) ON DELETE CASCADE,
    execution_id TEXT NOT NULL CHECK (length(execution_id) > 0),
    state TEXT NOT NULL CHECK (state IN (
        'preparing', 'running', 'cleaning', 'released', 'failed',
        'cleanup_failed', 'quarantined'
    )),
    agent_observed_at_unix_nano INTEGER NOT NULL CHECK (agent_observed_at_unix_nano > 0),
    received_at_unix_nano INTEGER NOT NULL CHECK (received_at_unix_nano > 0),
    PRIMARY KEY (node_id, execution_id),
    FOREIGN KEY (execution_id, node_id)
        REFERENCES executions(id, node_id)
        ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE TABLE agent_snapshot_cleanup_tombstones (
    node_id TEXT NOT NULL REFERENCES enrolled_nodes(node_id) ON DELETE CASCADE,
    execution_id TEXT NOT NULL CHECK (length(execution_id) > 0),
    failure_code TEXT NOT NULL CHECK (failure_code IN (
        'cleanup_verification_failed', 'process_residue', 'workspace_removal_failed'
    )),
    agent_recorded_at_unix_nano INTEGER NOT NULL CHECK (agent_recorded_at_unix_nano > 0),
    received_at_unix_nano INTEGER NOT NULL CHECK (received_at_unix_nano > 0),
    PRIMARY KEY (node_id, execution_id),
    FOREIGN KEY (execution_id, node_id)
        REFERENCES executions(id, node_id)
        ON DELETE RESTRICT ON UPDATE RESTRICT
);

-- Envelope message ID is the durable outbox event identity. Duplicate delivery
-- is accepted only when every typed field and the exact payload digest match.
CREATE TABLE agent_execution_updates (
    node_id TEXT NOT NULL REFERENCES enrolled_nodes(node_id) ON DELETE RESTRICT,
    message_id TEXT NOT NULL CHECK (length(message_id) > 0),
    command_id TEXT NOT NULL REFERENCES agent_commands(command_id) ON DELETE RESTRICT,
    execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE RESTRICT,
    state TEXT NOT NULL CHECK (state IN (
        'preparing', 'running', 'cleaning', 'released', 'failed',
        'cleanup_failed', 'quarantined'
    )),
    replayed INTEGER NOT NULL CHECK (replayed IN (0, 1)),
    error_code TEXT NOT NULL CHECK (error_code IN (
        '', 'execution_conflict', 'reconciliation_required', 'quarantined',
        'cleanup_failed', 'start_failed', 'platform_unavailable',
        'journal_failed', 'command_rejected'
    )),
    payload_digest TEXT NOT NULL CHECK (
        length(payload_digest) = 64 AND payload_digest NOT GLOB '*[^0-9a-f]*'
    ),
    received_at_unix_nano INTEGER NOT NULL CHECK (received_at_unix_nano > 0),
    PRIMARY KEY (node_id, message_id),
    FOREIGN KEY (command_id, node_id, execution_id)
        REFERENCES agent_commands(command_id, node_id, execution_id)
        ON DELETE RESTRICT ON UPDATE RESTRICT
);
