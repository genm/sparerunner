CREATE TABLE store_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT INTO store_metadata (key, value) VALUES ('role', 'controller');
INSERT INTO store_metadata (key, value) VALUES ('controller_epoch', '0');

CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY CHECK (version > 0),
    applied_at_unix_nano INTEGER NOT NULL CHECK (applied_at_unix_nano > 0)
);

CREATE TABLE slot_reservations (
    node_id TEXT NOT NULL CHECK (length(node_id) > 0),
    slot_index INTEGER NOT NULL CHECK (slot_index >= 0),
    target_id TEXT NOT NULL CHECK (length(target_id) > 0),
    execution_id TEXT NOT NULL UNIQUE CHECK (length(execution_id) > 0),
    PRIMARY KEY (node_id, slot_index),
    UNIQUE (node_id, slot_index, target_id, execution_id)
);

CREATE TABLE executions (
    id TEXT PRIMARY KEY CHECK (length(id) > 0),
    target_id TEXT NOT NULL CHECK (length(target_id) > 0),
    node_id TEXT NOT NULL CHECK (length(node_id) > 0),
    slot_index INTEGER NOT NULL CHECK (slot_index >= 0),
    state TEXT NOT NULL CHECK (state = 'reserved'),
    created_at_unix_nano INTEGER NOT NULL CHECK (created_at_unix_nano > 0),
    FOREIGN KEY (node_id, slot_index, target_id, id)
        REFERENCES slot_reservations(node_id, slot_index, target_id, execution_id)
        ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE TABLE processed_messages (
    scale_set_id INTEGER NOT NULL CHECK (scale_set_id > 0),
    message_id INTEGER NOT NULL CHECK (message_id > 0),
    message_digest TEXT NOT NULL CHECK (
        length(message_digest) = 64 AND message_digest NOT GLOB '*[^0-9a-f]*'
    ),
    execution_id TEXT NOT NULL UNIQUE CHECK (length(execution_id) > 0),
    created_at_unix_nano INTEGER NOT NULL CHECK (created_at_unix_nano > 0),
    PRIMARY KEY (scale_set_id, message_id),
    FOREIGN KEY (execution_id) REFERENCES executions(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);
