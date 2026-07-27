-- The node owner's per-Target deny-list. It is durable so an exclusion survives
-- agent restart and reboot, and it is owner-editable local authority: the
-- controller adopts it, never invents it. Unknown target IDs are accepted on
-- purpose, so an owner can exclude a scope while offline or before the first
-- eligible-target list has arrived.
CREATE TABLE node_target_exclusions (
    target_id TEXT PRIMARY KEY CHECK (
        length(target_id) > 0
        AND length(target_id) <= 128
        AND target_id = trim(target_id)
        AND target_id NOT GLOB '*[' || char(1) || '-' || char(31) || char(127) || ']*'
    ),
    changed_at_unix_nano INTEGER NOT NULL CHECK (changed_at_unix_nano > 0),
    changed_by TEXT NOT NULL CHECK (length(changed_by) > 0)
);

-- Target attribution for local executions. It is a side table rather than
-- columns on execution_observations because attribution is written once at
-- prepare admission while the observation row changes state repeatedly; a
-- missing row is simply an unattributed execution, never a corrupt one.
CREATE TABLE execution_targets (
    execution_id TEXT PRIMARY KEY CHECK (length(execution_id) > 0),
    target_id TEXT NOT NULL CHECK (length(target_id) > 0 AND length(target_id) <= 128),
    scope TEXT NOT NULL CHECK (length(scope) > 0),
    scope_kind TEXT NOT NULL CHECK (scope_kind IN ('repository', 'organization'))
);

-- The classified failure vocabulary gains the owner's own refusal. SQLite
-- cannot widen a CHECK in place, so the outbox is rebuilt with its exact
-- existing shape plus the new code; in-flight rows are carried over verbatim.
CREATE TABLE execution_update_outbox_target_excluded (
    sequence INTEGER PRIMARY KEY CHECK (sequence > 0),
    message_id TEXT NOT NULL UNIQUE CHECK (length(message_id) = 64 AND message_id NOT GLOB '*[^0-9a-f]*'),
    node_id TEXT NOT NULL CHECK (length(node_id) > 0),
    command_id TEXT NOT NULL CHECK (length(command_id) > 0),
    execution_id TEXT NOT NULL CHECK (length(execution_id) > 0),
    state TEXT NOT NULL CHECK (state IN ('pending', 'reserved', 'preparing', 'running', 'cleaning', 'released', 'failed', 'cleanup_failed', 'quarantined')),
    replayed INTEGER NOT NULL CHECK (replayed IN (0, 1)),
    error_code TEXT NOT NULL CHECK (error_code IN ('', 'execution_conflict', 'reconciliation_required', 'quarantined', 'cleanup_failed', 'start_failed', 'platform_unavailable', 'journal_failed', 'command_rejected', 'target_excluded'))
);

INSERT INTO execution_update_outbox_target_excluded (
    sequence, message_id, node_id, command_id, execution_id, state, replayed, error_code
)
SELECT sequence, message_id, node_id, command_id, execution_id, state, replayed, error_code
FROM execution_update_outbox;

DROP TABLE execution_update_outbox;

ALTER TABLE execution_update_outbox_target_excluded RENAME TO execution_update_outbox;
