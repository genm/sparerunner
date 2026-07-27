-- Command payloads remain absent. The type is required only so Agent startup
-- recovery can distinguish a secret-free Prepare from a Start whose one-shot
-- JIT body was intentionally not persisted.
CREATE TABLE accepted_command_types (
    command_id TEXT PRIMARY KEY
        REFERENCES command_replays(command_id) ON DELETE CASCADE,
    command_type TEXT NOT NULL CHECK (command_type IN ('prepare', 'start', 'cancel'))
);
