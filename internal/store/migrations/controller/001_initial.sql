CREATE TABLE store_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT INTO store_metadata (key, value) VALUES ('role', 'controller');
INSERT INTO store_metadata (key, value) VALUES ('controller_epoch', '0');

CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at_unix_nano INTEGER NOT NULL
);

CREATE TABLE slot_reservations (
    node_id TEXT NOT NULL,
    slot_index INTEGER NOT NULL CHECK (slot_index >= 0),
    target_id TEXT NOT NULL,
    execution_id TEXT NOT NULL UNIQUE,
    PRIMARY KEY (node_id, slot_index)
);

CREATE TABLE executions (
    id TEXT PRIMARY KEY,
    target_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    slot_index INTEGER NOT NULL CHECK (slot_index >= 0),
    state TEXT NOT NULL,
    created_at_unix_nano INTEGER NOT NULL,
    FOREIGN KEY (node_id, slot_index) REFERENCES slot_reservations(node_id, slot_index)
        ON DELETE RESTRICT ON UPDATE RESTRICT
);

-- This partial index is the final race-safe guard: an active execution owns one slot.
CREATE UNIQUE INDEX active_execution_per_slot ON executions (node_id, slot_index)
WHERE state IN ('pending', 'reserved', 'preparing', 'running', 'cleaning', 'cleanup_failed');

CREATE TABLE processed_messages (
    scale_set_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    message_digest TEXT NOT NULL,
    target_id TEXT NOT NULL,
    execution_id TEXT NOT NULL UNIQUE,
    node_id TEXT NOT NULL,
    slot_index INTEGER NOT NULL CHECK (slot_index >= 0),
    created_at_unix_nano INTEGER NOT NULL,
    PRIMARY KEY (scale_set_id, message_id),
    FOREIGN KEY (execution_id) REFERENCES executions(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);
