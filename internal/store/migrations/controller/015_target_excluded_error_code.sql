-- The classified failure vocabulary gains the node owner's own refusal. An
-- agent that re-reads its durable per-Target exclusion set at the exec boundary
-- publishes target_excluded through the same durable outbox as every other
-- classified runner failure, so the controller must be able to persist it.
-- SQLite cannot widen a CHECK in place, so the table is rebuilt with its exact
-- existing shape plus the new code and every stored row is carried over.
PRAGMA defer_foreign_keys = ON;

CREATE TABLE agent_execution_updates_target_excluded (
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
        'journal_failed', 'command_rejected', 'target_excluded'
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

INSERT INTO agent_execution_updates_target_excluded (
    node_id, message_id, command_id, execution_id, state, replayed,
    error_code, payload_digest, received_at_unix_nano
)
SELECT node_id, message_id, command_id, execution_id, state, replayed,
       error_code, payload_digest, received_at_unix_nano
FROM agent_execution_updates;

DROP TABLE agent_execution_updates;

ALTER TABLE agent_execution_updates_target_excluded RENAME TO agent_execution_updates;
