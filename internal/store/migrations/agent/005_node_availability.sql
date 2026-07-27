-- The node owner's local availability intent. It is durable so a stopped
-- computer stays stopped across agent restart and reboot, and it is a single
-- row because it describes this node, not an execution.
CREATE TABLE node_availability (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    intent TEXT NOT NULL CHECK (intent IN ('accepting', 'stopped')),
    changed_at_unix_nano INTEGER NOT NULL CHECK (changed_at_unix_nano > 0),
    changed_by TEXT NOT NULL
);
