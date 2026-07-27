-- Runner journal records retain only replay-safe lifecycle metadata. JIT bodies,
-- argv, environment, paths, and process output must never enter this database.
CREATE TABLE runner_journal_records (
    execution_id TEXT PRIMARY KEY CHECK (length(execution_id) > 0),
    spec_digest TEXT NOT NULL CHECK (length(spec_digest) = 64 AND spec_digest NOT GLOB '*[^0-9a-f]*'),
    jit_digest TEXT NOT NULL CHECK (jit_digest = '' OR (length(jit_digest) = 64 AND jit_digest NOT GLOB '*[^0-9a-f]*')),
    state TEXT NOT NULL CHECK (state IN ('preparing', 'prepared', 'starting', 'running', 'cleaning', 'released', 'failed', 'cleanup_failed')),
    root_name TEXT NOT NULL CHECK (length(root_name) = 64 AND root_name NOT GLOB '*[^0-9a-f]*'),
    pid INTEGER NOT NULL CHECK (pid >= 0),
    tombstone INTEGER NOT NULL CHECK (tombstone IN (0, 1)),
    containment_backend TEXT NOT NULL,
    containment_owner_id TEXT NOT NULL,
    containment_scope TEXT NOT NULL,
    containment_host_epoch TEXT NOT NULL,
    containment_invocation_id TEXT NOT NULL,
    containment_fence_token TEXT NOT NULL CHECK (containment_fence_token = '' OR (length(containment_fence_token) = 32 AND containment_fence_token NOT GLOB '*[^0-9a-f]*')),
    workspace_backend TEXT NOT NULL,
    workspace_owner_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision > 0),
    mutation_token TEXT NOT NULL CHECK (length(mutation_token) = 32 AND mutation_token NOT GLOB '*[^0-9a-f]*')
);
