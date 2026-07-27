-- The management contract owns one revision across the complete structured
-- desired configuration. A compare-and-swap apply updates every related row and
-- its audit event in one transaction.
CREATE TABLE management_configuration_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    revision INTEGER NOT NULL CHECK (revision >= 0),
    fleet_max_runners INTEGER CHECK (
        fleet_max_runners IS NULL OR fleet_max_runners >= 1
    )
);

INSERT INTO management_configuration_state(
    singleton, revision, fleet_max_runners
) VALUES (1, 0, NULL);

CREATE TABLE management_node_configurations (
    node_id TEXT PRIMARY KEY
        REFERENCES enrolled_nodes(node_id) ON DELETE CASCADE ON UPDATE RESTRICT,
    display_name TEXT NOT NULL CHECK (length(display_name) > 0),
    max_runners INTEGER NOT NULL CHECK (max_runners >= 1)
);

-- Existing enrolled nodes keep the established one-runner behavior. A node
-- enrolled after this migration receives the same durable default immediately,
-- so restart topology never depends on an in-memory fallback.
INSERT INTO management_node_configurations(node_id, display_name, max_runners)
SELECT node_id, node_id, 1
FROM enrolled_nodes;

CREATE TRIGGER enrolled_node_management_configuration
AFTER INSERT ON enrolled_nodes
BEGIN
    INSERT INTO management_node_configurations(node_id, display_name, max_runners)
    VALUES (NEW.node_id, NEW.node_id, 1);
END;

CREATE TABLE management_runner_profiles (
    profile_id TEXT PRIMARY KEY CHECK (length(profile_id) > 0),
    label TEXT NOT NULL CHECK (length(label) > 0),
    operating_system TEXT CHECK (
        operating_system IS NULL
        OR operating_system IN ('linux', 'macos', 'windows')
    ),
    architecture TEXT CHECK (
        architecture IS NULL
        OR architecture IN ('amd64', 'arm64')
    ),
    min_available_memory_bytes INTEGER NOT NULL
        CHECK (min_available_memory_bytes >= 0),
    version_policy TEXT NOT NULL
        CHECK (version_policy IN ('auto_update', 'pinned')),
    runner_version TEXT NOT NULL CHECK (length(runner_version) > 0),
    runtime TEXT NOT NULL CHECK (runtime = 'native')
);

CREATE TABLE management_github_targets (
    target_id TEXT PRIMARY KEY CHECK (length(target_id) > 0),
    installation_id TEXT NOT NULL CHECK (length(installation_id) > 0),
    scope_kind TEXT NOT NULL
        CHECK (scope_kind IN ('repository', 'organization')),
    scope TEXT NOT NULL CHECK (length(scope) > 0),
    visibility TEXT NOT NULL CHECK (visibility = 'private'),
    runner_group_access_safe INTEGER NOT NULL
        CHECK (runner_group_access_safe = 1),
    scale_set_name TEXT NOT NULL CHECK (length(scale_set_name) > 0),
    profile_id TEXT NOT NULL
        REFERENCES management_runner_profiles(profile_id)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    scale_set_id INTEGER NOT NULL UNIQUE CHECK (scale_set_id > 0)
);

-- Audit rows deliberately contain only typed allowlisted fields. Request
-- bodies, headers, join secrets, credential values, and other raw payloads have
-- no storage column at this boundary.
CREATE TABLE management_audit_events (
    sequence INTEGER PRIMARY KEY CHECK (sequence > 0),
    occurred_at_unix_nano INTEGER NOT NULL
        CHECK (occurred_at_unix_nano > 0),
    actor TEXT NOT NULL CHECK (
        actor IN ('anonymous', 'join_code', 'node', 'single_admin')
    ),
    action TEXT NOT NULL CHECK (action IN (
        'authentication_succeeded', 'authentication_failed',
        'enrollment_rejected', 'enrollment_unavailable',
        'agent_session_rejected',
        'session_ended', 'join_code_created', 'join_code_cancelled',
        'configuration_applied', 'node_enrolled',
        'node_drained', 'node_resumed', 'node_revoked'
    )),
    outcome TEXT NOT NULL CHECK (outcome IN (
        'succeeded', 'rejected', 'failed'
    )),
    resource_kind TEXT NOT NULL CHECK (resource_kind IN (
        'controller', 'configuration', 'join_code', 'node'
    )),
    resource_id TEXT NOT NULL,
    error_code TEXT NOT NULL CHECK (error_code IN (
        '', 'authentication_failed', 'enrollment_rejected',
        'enrollment_rate_limited', 'enrollment_unavailable',
        'node_credential_rejected', 'agent_protocol_rejected',
        'host_rejected',
        'event_stream_rejected', 'mutation_rejected',
        'management_unavailable', 'configuration_revision_conflict',
        'validation_failed', 'join_code_not_found', 'state_conflict',
        'invalid_body', 'invalid_precondition', 'precondition_required',
        'request_forbidden', 'misdirected_host', 'method_not_allowed',
        'payload_too_large', 'unsupported_media_type',
        'session_unavailable', 'invalid_session_request'
    )),
    request_id TEXT NOT NULL CHECK (
        request_id = 'req_unavailable'
        OR (
            length(request_id) = 36
            AND substr(request_id, 1, 4) = 'req_'
            AND substr(request_id, 5) NOT GLOB '*[^0-9a-f]*'
        )
    ),
    revision INTEGER NOT NULL CHECK (revision >= 0),
    CHECK (
        (outcome = 'succeeded' AND error_code = '')
        OR (outcome IN ('rejected', 'failed') AND error_code != '')
    ),
    CHECK (
        (
            action = 'authentication_succeeded'
            AND actor = 'single_admin'
            AND outcome = 'succeeded'
        )
        OR (
            action = 'authentication_failed'
            AND actor = 'anonymous'
            AND outcome IN ('rejected', 'failed')
        )
        OR (
            action = 'enrollment_rejected'
            AND actor = 'anonymous'
            AND outcome = 'rejected'
        )
        OR (
            action = 'enrollment_unavailable'
            AND actor = 'anonymous'
            AND outcome = 'failed'
        )
        OR (
            action = 'agent_session_rejected'
            AND actor = 'node'
            AND outcome = 'rejected'
        )
        OR (
            action = 'node_enrolled'
            AND actor = 'join_code'
            AND outcome = 'succeeded'
        )
        OR (
            action NOT IN (
                'authentication_succeeded', 'authentication_failed',
                'enrollment_rejected', 'enrollment_unavailable',
                'agent_session_rejected', 'node_enrolled'
            )
            AND actor = 'single_admin'
        )
    ),
    CHECK (
        (
            action IN (
                'authentication_succeeded', 'authentication_failed',
                'enrollment_rejected', 'enrollment_unavailable',
                'session_ended'
            )
            AND resource_kind = 'controller'
            AND resource_id = ''
        )
        OR (
            action = 'configuration_applied'
            AND resource_kind = 'configuration'
            AND resource_id = ''
        )
        OR (
            action IN ('join_code_created', 'join_code_cancelled')
            AND resource_kind = 'join_code'
            AND length(resource_id) > 0
            AND trim(resource_id) = resource_id
        )
        OR (
            action IN (
                'node_enrolled', 'node_drained', 'node_resumed',
                'node_revoked', 'agent_session_rejected'
            )
            AND resource_kind = 'node'
            AND length(resource_id) > 0
            AND trim(resource_id) = resource_id
        )
    )
);

CREATE TRIGGER management_audit_events_no_update
BEFORE UPDATE ON management_audit_events
BEGIN
    SELECT RAISE(ABORT, 'management audit events are append-only');
END;

CREATE TRIGGER management_audit_events_no_delete
BEFORE DELETE ON management_audit_events
BEGIN
    SELECT RAISE(ABORT, 'management audit events are append-only');
END;
