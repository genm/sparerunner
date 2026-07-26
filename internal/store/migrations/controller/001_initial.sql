CREATE TABLE store_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT INTO store_metadata (key, value) VALUES ('role', 'controller');
INSERT INTO store_metadata (key, value) VALUES ('controller_epoch', '0');

CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY CHECK (version > 0),
    applied_at_unix_nano INTEGER NOT NULL
);

CREATE TABLE slot_reservations (
    node_id TEXT NOT NULL,
    slot_index INTEGER NOT NULL CHECK (slot_index >= 0),
    target_id TEXT NOT NULL,
    execution_id TEXT NOT NULL UNIQUE,
    PRIMARY KEY (node_id, slot_index),
    UNIQUE (node_id, slot_index, target_id, execution_id)
);

CREATE TABLE executions (
    id TEXT PRIMARY KEY,
    target_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    slot_index INTEGER NOT NULL CHECK (slot_index >= 0),
    state TEXT NOT NULL CHECK (state = 'reserved'),
    created_at_unix_nano INTEGER NOT NULL,
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
    execution_id TEXT NOT NULL UNIQUE,
    created_at_unix_nano INTEGER NOT NULL,
    PRIMARY KEY (scale_set_id, message_id),
    FOREIGN KEY (execution_id) REFERENCES executions(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);
