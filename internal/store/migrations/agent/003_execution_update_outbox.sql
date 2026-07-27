-- Lifecycle updates are acknowledged by the Controller only after its durable
-- consumer accepts them. Keep only the typed, classified update while it is in
-- flight so a transport disconnect cannot erase cleanup evidence.
CREATE TABLE execution_update_outbox (
    sequence INTEGER PRIMARY KEY CHECK (sequence > 0),
    message_id TEXT NOT NULL UNIQUE CHECK (length(message_id) = 64 AND message_id NOT GLOB '*[^0-9a-f]*'),
    node_id TEXT NOT NULL CHECK (length(node_id) > 0),
    command_id TEXT NOT NULL CHECK (length(command_id) > 0),
    execution_id TEXT NOT NULL CHECK (length(execution_id) > 0),
    state TEXT NOT NULL CHECK (state IN ('pending', 'reserved', 'preparing', 'running', 'cleaning', 'released', 'failed', 'cleanup_failed', 'quarantined')),
    replayed INTEGER NOT NULL CHECK (replayed IN (0, 1)),
    error_code TEXT NOT NULL CHECK (error_code IN ('', 'execution_conflict', 'reconciliation_required', 'quarantined', 'cleanup_failed', 'start_failed', 'platform_unavailable', 'journal_failed', 'command_rejected'))
);
